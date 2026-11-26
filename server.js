// IRANIAN ISLAND - game server (Stage 1: players see each other, 10 max)
// Run: npm install && node server.js      (Render sets PORT by itself)
// HTTP : GET /health  -> "ok"            (Render health check, also wakes the server)
//        GET /info    -> {name,players,max,map}   (the lobby reads this for the server list)
// WS   : client -> {t:"join",name} {t:"p",x,y,z,yaw,pitch} {t:"ping",ts} {t:"chat",m}
//        server -> {t:"welcome",id,max,players:[..]} {t:"join"} {t:"leave"} {t:"s",p:[[id,x,y,z,yaw,pitch],..]}
//                  {t:"pong",ts,n} {t:"chat",id,name,m} {t:"full"}
// Stage 2 (PvP + shared world):
//   client -> {t:"hit",id,d,w} {t:"die",by} {t:"spawn"} {t:"rm",id,k,x,z,s} {t:"lt",id,s}
//             {t:"bag",x,y,z,items} {t:"bagtake",bid}
//   server -> {t:"dmg",from,name,d,w} {t:"die",id,name,by,byName} {t:"spawn",id} {t:"rm",...} {t:"lt",...}
//             {t:"bag",bid,x,y,z,items} {t:"bagrm",bid} {t:"bagok",bid,items}
//   welcome also carries: res:[[id,k,x,z,secsLeft]], loot:[[id,secsLeft]], bags:[{bid,x,y,z,items}]
const http = require("http");
const { WebSocketServer } = require("ws");

const PORT = process.env.PORT || 8080;
const MAX_PLAYERS = parseInt(process.env.MAX_PLAYERS || "10", 10);
const SERVER_NAME = process.env.SERVER_NAME || "Iranian Island #1";
const MAP_NAME = process.env.MAP_NAME || "Iranian Island";
const TICK_MS = 100;            // 10 state packets per second (keeps the Render bandwidth small)
const MAX_MSGS_PER_SEC = 40;
const WORLD_LIMIT = 20000;      // positions outside +-20 km are ignored

const players = new Map();      // id -> {id, ws, name, ci, x,y,z,yaw,pitch, alive, dirty, msgCount, msgWindow}
let nextId = 1;

// ---- shared world (only the CHANGES are stored: the map itself is built from the same seed on every phone)
const res = new Map();          // resource id -> {k, x, z, until}   (felled trees, mined rocks ... they grow back)
const loot = new Map();         // container id -> until (ms)        (looted crates / barrels: back after the time)
const bags = new Map();         // bag id -> {bid, x, y, z, items, until}   (what a dead player dropped)
let nextBag = 1;
const MAX_RES = 30000;
const WEAPON_DMG = { "Sniper": 100, "Assault Rifle": 24, "Revolver": 50, "Shotgun": 112, "melee": 25, "Arrow": 35, "Rocket": 60 };
const WEAPON_RANGE = { "Sniper": 175, "Assault Rifle": 110, "Revolver": 80, "Shotgun": 45, "melee": 6, "Arrow": 55, "Rocket": 90 };
function clamp(v, a, b) { return Math.max(a, Math.min(b, v)); }
function secsLeft(until) { return Math.max(1, Math.round((until - Date.now()) / 1000)); }
function dist3(a, b) { return Math.hypot(a.x - b.x, a.y - b.y, a.z - b.z); }

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

const wss = new WebSocketServer({ server, maxPayload: 16384 });

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
  return { id: p.id, name: p.name, ci: p.ci, x: p.x, y: p.y, z: p.z, yaw: p.yaw, pitch: p.pitch, has: p.has, alive: p.alive };
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
      me = { id: nextId++, ws, name: cleanName(m.name), ci: freeColor(), x: 0, y: 0, z: 0, yaw: 0, pitch: 0, has: false, alive: true, dirty: false, msgCount: 0, msgWindow: Date.now() };
      send(ws, {
        t: "welcome", id: me.id, max: MAX_PLAYERS, players: [...players.values()].map(pub),
        res: [...res.entries()].map(([id, e]) => [id, e.k, e.x, e.z, secsLeft(e.until)]),
        loot: [...loot.entries()].map(([id, u]) => [id, secsLeft(u)]),
        bags: [...bags.values()].map((b) => ({ bid: b.bid, x: b.x, y: b.y, z: b.z, items: b.items })),
      });
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
    } else if (m.t === "hit") {
      // PvP: the shooter says "I hit player id for d damage with weapon w". The server checks the numbers and
      // forwards the damage to the victim only. (The victim's phone keeps the health.)
      const v = players.get(num(m.id));
      const w = String(m.w || "");
      const d = num(m.d);
      if (!v || v.id === me.id || !v.alive || !me.alive || d === null || d <= 0) return;
      if (!Object.prototype.hasOwnProperty.call(WEAPON_DMG, w)) return;
      if (d > WEAPON_DMG[w] + 0.5) return;
      if (me.has && v.has && dist3(me, v) > WEAPON_RANGE[w] + (w === "melee" ? 3 : 12)) return;
      send(v.ws, { t: "dmg", from: me.id, name: me.name, d: Math.round(d * 10) / 10, w });
    } else if (m.t === "die") {
      if (!me.alive) return;
      me.alive = false;
      const by = players.get(num(m.by));
      broadcast({ t: "die", id: me.id, name: me.name, by: by ? by.id : -1, byName: by ? by.name : "" });
      console.log(`x ${me.name} died${by ? " (killed by " + by.name + ")" : ""}`);
    } else if (m.t === "spawn") {
      me.alive = true;
      broadcast({ t: "spawn", id: me.id }, me.id);
    } else if (m.t === "rm") {
      const id = String(m.id || "").slice(0, 80);
      if (!id || res.size >= MAX_RES) return;
      const s2 = clamp(num(m.s) || 600, 30, 1800);
      const e = { k: String(m.k || "").slice(0, 12), x: num(m.x) || 0, z: num(m.z) || 0, until: Date.now() + s2 * 1000 };
      res.set(id, e);
      broadcast({ t: "rm", id, k: e.k, x: e.x, z: e.z, s: Math.round(s2) }, me.id);
    } else if (m.t === "lt") {
      const id = String(m.id || "").slice(0, 80);
      if (!id || loot.size >= MAX_RES) return;
      const s2 = clamp(num(m.s) || 600, 30, 1800);
      loot.set(id, Date.now() + s2 * 1000);
      broadcast({ t: "lt", id, s: Math.round(s2) }, me.id);
    } else if (m.t === "bag") {
      // a dead player's backpack: everybody sees it, the first one who takes it gets the items
      const items = {};
      let n = 0;
      if (m.items && typeof m.items === "object") {
        for (const key of Object.keys(m.items)) {
          const q = Math.floor(Number(m.items[key]));
          if (n >= 80 || key.length > 40 || !Number.isFinite(q) || q < 1 || q > 100000) continue;
          items[key] = q;
          n++;
        }
      }
      const x = num(m.x), y = num(m.y), z = num(m.z);
      if (n === 0 || x === null || y === null || z === null || bags.size >= 400) return;
      const b = { bid: nextBag++, x, y, z, items, until: Date.now() + 30 * 60 * 1000 };
      bags.set(b.bid, b);
      broadcast({ t: "bag", bid: b.bid, x, y, z, items });
    } else if (m.t === "bagtake") {
      const b = bags.get(num(m.bid));
      if (!b) { send(ws, { t: "bagrm", bid: num(m.bid) }); return; }
      if (me.has && dist3(me, b) > 12) return;
      bags.delete(b.bid);
      send(ws, { t: "bagok", bid: b.bid, items: b.items });
      broadcast({ t: "bagrm", bid: b.bid }, me.id);
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

// forget what has grown back / expired
setInterval(() => {
  const now = Date.now();
  for (const [id, e] of res) if (e.until <= now) res.delete(id);
  for (const [id, u] of loot) if (u <= now) loot.delete(id);
  for (const [id, b] of bags) if (b.until <= now) bags.delete(id);
}, 10000);

server.listen(PORT, () => console.log(`Iranian Island server on port ${PORT}, max ${MAX_PLAYERS} players`));
