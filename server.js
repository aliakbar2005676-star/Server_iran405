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
  const used = new Set([...players.values()].map((p) => p
