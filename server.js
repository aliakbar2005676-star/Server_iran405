// IRANIAN ISLAND - game server (Stage 1: players see each other, 10 max)
// Run: npm install && node server.js      (Render sets PORT by itself)
// HTTP : GET /health  -> "ok"            (Render health check, also wakes the server)
//        GET /info    -> {name,players,max,map}   (the lobby reads this for the server list)
// WS   : client -> {t:"join",name} {t:"p",x,y,z,yaw,pitch} {t:"ping",ts} {t:"chat",m}
//        server -> {t:"welcome",id,max,players:[..]} {t:"join"} {t:"leave"} {t:"s",p:[[id,x,y,z,yaw,pitch],..]}
//                  {t:"pong",ts,n} {t:"chat",id,name,m} {t:"full"}
const http = require("http");
const { WebSocketServer } = require("ws");

const PORT = process.env.PORT || 8080;
const MAX_PLAYERS = parseInt(process.env.MAX_PLAYERS || "10", 10);
const SERVER_NAME = process.env.SERVER_NAME || "Iranian Island #1";
const MAP_NAME = process.env.MAP_NAME || "Iranian Island";
const TICK_MS = 100;            // 10 state packets per second (keeps the Render bandwidth small)
const MAX_MSGS_PER_SEC = 40;
const WORLD_LIMIT = 20000;      // positions outside +-20 km are ignored

const players = new Map();      // id -> {id, ws, name, ci, x,y,z,yaw,pitch, dirty, msgCount, msgWindow}
let nextId = 1;

const server = http.createServer((req, res) => {
  const url = (req.url || "/").split("?")[0];
  if (url === "/info") {
    res.writeHead(200, { "Content-Type": "application/json", "Access-Control-Allow-Origin": "*" });
    res.end(JSON.stringify({ name: SERVER_NAME, players: players.size, max: MAX_PLAYERS, map: MAP_NAME }));
  } else if (url === "/health" || url === "/") {
    res.writeHead(200, { "Content-Type": "text/plain" });
    res.end("ok");
  } else {
    res.writeHead(404);
    res.end();
  }
});

const wss = new WebSocketServer({ server, maxPayload: 2048 });

function send(ws, obj) {
  if (ws.readyState === 1) ws.send(JSON.stringify(obj));
}
function broadcast(obj, exceptId) {
  const s = JSON.stringify(obj);
  for (const p of players.values()) {
    if (p.id !== exceptId && p.ws.readyState === 1) p.ws.send(s);
  }
}
function num(v) {
  v = Number(v);
  return Number.isFinite(v) ? v : null;
}
function cleanName(n) {
  n = String(n || "Player").replace(/[^\p{L}\p{N} _\-\.]/gu, "").trim().slice(0, 16);
  return n || "Player";
}
function freeColor() {
  const used = new Set([...players.values()].map((p) => p.ci));
  for (let i = 0; i < 64; i++) if (!used.has(i)) return i;
  return 0;
}
function pub(p) {
  return { id: p.id, name: p.name, ci: p.ci, x: p.x, y: p.y, z: p.z, yaw: p.yaw, pitch: p.pitch, has: p.has };
}

wss.on("connection", (ws) => {
  let me = null;
  ws.isAlive = true;
  ws.on("pong", () => { ws.isAlive = true; });

  ws.on("message", (data) => {
    let m;
    try { m = JSON.parse(data.toString()); } catch (e) { return; }
    if (!m || typeof m.t !== "string") return;

    if (me) {                                   // flood protection
      const now = Date.now();
      if (now - me.msgWindow > 1000) { me.msgWindow = now; me.msgCount = 0; }
      if (++me.msgCount > MAX_MSGS_PER_SEC) return;
    }

    if (m.t === "join" && !me) {
      if (players.size >= MAX_PLAYERS) {
        send(ws, { t: "full" });
        ws.close(1000, "full");
        return;
      }
      me = { id: nextId++, ws, name: cleanName(m.name), ci: freeColor(), x: 0, y: 0, z: 0, yaw: 0, pitch: 0, has: false, dirty: false, msgCount: 0, msgWindow: Date.now() };
      send(ws, { t: "welcome", id: me.id, max: MAX_PLAYERS, players: [...players.values()].map(pub) });
      players.set(me.id, me);
      broadcast({ t: "join", id: me.id, name: me.name, ci: me.ci }, me.id);
      console.log(`+ ${me.name} (#${me.id})  ${players.size}/${MAX_PLAYERS}`);
      return;
    }
    if (!me) return;

    if (m.t === "p") {
      const x = num(m.x), y = num(m.y), z = num(m.z), yaw = num(m.yaw), pitch = num(m.pitch);
      if (x === null || y === null || z === null || yaw === null || pitch === null) return;
      if (Math.abs(x) > WORLD_LIMIT || Math.abs(z) > WORLD_LIMIT || Math.abs(y) > 5000) return;
      me.x = x; me.y = y; me.z = z; me.yaw = yaw; me.pitch = pitch; me.has = true; me.dirty = true;
    } else if (m.t === "ping") {
      send(ws, { t: "pong", ts: m.ts, n: players.size });
    } else if (m.t === "chat") {
      const text = String(m.m || "").slice(0, 120).trim();
      if (text) broadcast({ t: "chat", id: me.id, name: me.name, m: text });
    }
  });

  ws.on("close", () => {
    if (me && players.get(me.id) === me) {
      players.delete(me.id);
      broadcast({ t: "leave", id: me.id });
      console.log(`- ${me.name} (#${me.id})  ${players.size}/${MAX_PLAYERS}`);
    }
  });
  ws.on("error", () => {});
});

// state tick: one packet per player with everybody else's position (only when something moved)
setInterval(() => {
  const moved = [...players.values()].filter((p) => p.dirty);
  if (moved.length === 0) return;
  for (const p of players.values()) {
    const list = [];
    for (const o of moved) {
      if (o.id !== p.id) list.push([o.id, +o.x.toFixed(2), +o.y.toFixed(2), +o.z.toFixed(2), +o.yaw.toFixed(3), +o.pitch.toFixed(3)]);
    }
    if (list.length) send(p.ws, { t: "s", p: list });
  }
  for (const o of moved) o.dirty = false;
}, TICK_MS);

// drop dead connections (phone lost network)
setInterval(() => {
  for (const ws of wss.clients) {
    if (ws.isAlive === false) { ws.terminate(); continue; }
    ws.isAlive = false;
    ws.ping();
  }
}, 25000);

server.listen(PORT, () => console.log(`Iranian Island server on port ${PORT}, max ${MAX_PLAYERS} players`));
