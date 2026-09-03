/**
 * ESP32 Watch Backend
 * -----------------------------------------------------------------------
 * A single WebSocket relay that handles two things:
 *
 *   1. CALLS   - relays audio frames (binary) between two connected
 *                clients (e.g. two watches, or a watch + a browser bridge)
 *   2. DEVICES - relays JSON "commands" from a watch to any other
 *                connected device (e.g. "turn on lamp1")
 *
 * Every client (watch or controllable device) connects to:
 *
 *   wss://<your-app>.onrender.com/ws?id=<UNIQUE_ID>&role=<watch|device>&token=<AUTH_TOKEN>
 *
 * - id    : a unique string identifying this client (e.g. "watch-001", "lamp-1")
 * - role  : "watch" or "device" (informational, used for the /devices listing)
 * - token : optional shared-secret, required if AUTH_TOKEN env var is set
 *
 * This is intentionally an in-memory relay (no database). State resets on
 * restart/redeploy. That's fine for a prototype; see README for notes on
 * making it durable later.
 * -----------------------------------------------------------------------
 */

const express = require("express");
const http = require("http");
const { WebSocketServer } = require("ws");
const { randomUUID } = require("crypto");

const PORT = process.env.PORT || 3000;
const AUTH_TOKEN = process.env.AUTH_TOKEN || null; // set this in Render's env vars for basic protection

const app = express();
app.use(express.json());

// ---- REST endpoints -------------------------------------------------

// Render (and you) can hit this to confirm the service is alive.
app.get("/health", (req, res) => {
  res.json({ status: "ok", clients: clients.size, calls: calls.size });
});

// Debug/admin: see who's currently connected.
app.get("/devices", (req, res) => {
  const list = [...clients.values()].map((c) => ({ id: c.id, role: c.role }));
  res.json({ count: list.length, clients: list });
});

const server = http.createServer(app);
const wss = new WebSocketServer({ server, path: "/ws" });

// ---- In-memory state --------------------------------------------------

// id -> { id, role, ws }
const clients = new Map();

// callId -> { a: id, b: id }
const calls = new Map();

// ---- Helpers ------------------------------------------------------------

function send(ws, obj) {
  if (ws.readyState === ws.OPEN) ws.send(JSON.stringify(obj));
}

function findClient(id) {
  return clients.get(id);
}

// Find the call a given client id is currently part of.
function findCallFor(id) {
  for (const [callId, call] of calls.entries()) {
    if (call.a === id || call.b === id) return { callId, ...call };
  }
  return null;
}

function otherParty(call, id) {
  return call.a === id ? call.b : call.a;
}

function broadcastDeviceList() {
  const list = [...clients.values()].map((c) => ({ id: c.id, role: c.role }));
  for (const c of clients.values()) {
    send(c.ws, { type: "device_list", devices: list });
  }
}

// ---- Connection handling -------------------------------------------------

wss.on("connection", (ws, req) => {
  const url = new URL(req.url, `http://${req.headers.host}`);
  const id = url.searchParams.get("id");
  const role = url.searchParams.get("role") || "device";
  const token = url.searchParams.get("token");

  if (AUTH_TOKEN && token !== AUTH_TOKEN) {
    send(ws, { type: "error", message: "invalid or missing token" });
    ws.close(4001, "unauthorized");
    return;
  }

  if (!id) {
    send(ws, { type: "error", message: "missing ?id= query param" });
    ws.close(4000, "missing id");
    return;
  }

  if (clients.has(id)) {
    // Same id reconnecting - drop the old socket, keep the new one.
    const old = clients.get(id);
    try {
      old.ws.close(4002, "replaced by new connection");
    } catch (_) {}
  }

  clients.set(id, { id, role, ws });
  console.log(`[connect] ${id} (${role}) — ${clients.size} total connected`);

  send(ws, {
    type: "welcome",
    id,
    connected: [...clients.keys()],
  });
  broadcastDeviceList();

  ws.on("message", (data, isBinary) => {
    const self = clients.get(id);
    if (!self) return;

    // ---- Binary frames: audio during an active call ----
    if (isBinary) {
      const call = findCallFor(id);
      if (!call) return; // not in a call, drop the frame
      const target = findClient(otherParty(call, id));
      if (target && target.ws.readyState === target.ws.OPEN) {
        target.ws.send(data, { binary: true });
      }
      return;
    }

    // ---- Text frames: JSON control messages ----
    let msg;
    try {
      msg = JSON.parse(data.toString());
    } catch (err) {
      send(ws, { type: "error", message: "invalid JSON" });
      return;
    }

    switch (msg.type) {
      // ---------------- Device control ----------------
      // Watch -> server -> target device
      case "device_command": {
        const target = findClient(msg.target);
        if (!target) {
          send(ws, { type: "error", message: `device '${msg.target}' not connected` });
          return;
        }
        send(target.ws, {
          type: "device_command",
          from: id,
          action: msg.action,
          payload: msg.payload || {},
        });
        send(ws, { type: "command_ack", target: msg.target, action: msg.action });
        break;
      }

      // ---------------- Call signaling ----------------
      case "call_invite": {
        const target = findClient(msg.target);
        if (!target) {
          send(ws, { type: "call_failed", target: msg.target, reason: "not connected" });
          return;
        }
        const callId = randomUUID();
        calls.set(callId, { a: id, b: msg.target });
        send(target.ws, { type: "call_incoming", from: id, callId });
        send(ws, { type: "call_ringing", target: msg.target, callId });
        break;
      }

      case "call_accept": {
        const call = calls.get(msg.callId);
        if (!call) {
          send(ws, { type: "error", message: "no such call" });
          return;
        }
        const caller = findClient(otherParty(call, id));
        if (caller) send(caller.ws, { type: "call_accepted", callId: msg.callId });
        send(ws, { type: "call_accepted", callId: msg.callId });
        break;
      }

      case "call_reject": {
        const call = calls.get(msg.callId);
        if (!call) return;
        const caller = findClient(otherParty(call, id));
        if (caller) send(caller.ws, { type: "call_rejected", callId: msg.callId });
        calls.delete(msg.callId);
        break;
      }

      case "call_end": {
        const call = calls.get(msg.callId);
        if (!call) return;
        const other = findClient(otherParty(call, id));
        if (other) send(other.ws, { type: "call_ended", callId: msg.callId });
        send(ws, { type: "call_ended", callId: msg.callId });
        calls.delete(msg.callId);
        break;
      }

      case "list_devices": {
        send(ws, {
          type: "device_list",
          devices: [...clients.values()].map((c) => ({ id: c.id, role: c.role })),
        });
        break;
      }

      case "ping": {
        send(ws, { type: "pong", t: Date.now() });
        break;
      }

      default:
        send(ws, { type: "error", message: `unknown message type '${msg.type}'` });
    }
  });

  ws.on("close", () => {
    // Only remove if this socket is still the one registered for this id
    // (avoids a race where a reconnect already replaced it).
    const current = clients.get(id);
    if (current && current.ws === ws) {
      clients.delete(id);
    }
    // End any call this client was part of.
    const call = findCallFor(id);
    if (call) {
      const other = findClient(otherParty(call, id));
      if (other) send(other.ws, { type: "call_ended", callId: call.callId, reason: "peer disconnected" });
      calls.delete(call.callId);
    }
    console.log(`[disconnect] ${id} — ${clients.size} total connected`);
    broadcastDeviceList();
  });

  ws.on("error", (err) => {
    console.error(`[ws error] ${id}:`, err.message);
  });
});

server.listen(PORT, () => {
  console.log(`Watch backend listening on port ${PORT}`);
});
