const http = require('http');
const { WebSocketServer } = require('ws');

const MAX = 10;
const players = new Map();
let nextId = 1;

const server = http.createServer((req, res) => {
  if (req.url === '/info') {
    res.writeHead(200, {
      'Content-Type': 'application/json',
      'Access-Control-Allow-Origin': '*'
    });
    res.end(JSON.stringify({ name: 'Iranian Island #1', players: players.size, max: MAX }));
    return;
  }
  res.writeHead(200);
  res.end('ok');
});

const wss = new WebSocketServer({ server });

function broadcast(obj, except) {
  const s = JSON.stringify(obj);
  for (const ws of players.keys()) {
    if (ws !== except && ws.readyState === 1) ws.send(s);
  }
}

wss.on('connection', (ws) => {
  if (players.size >= MAX) {
    ws.send(JSON.stringify({ type: 'full' }));
    ws.close();
    return;
  }
  const id = nextId++;
  players.set(ws, { id });
  ws.send(JSON.stringify({
    type: 'welcome',
    id,
    players: [...players.values()]
  }));
  broadcast({ type: 'join', id }, ws);

  ws.on('message', (data) => {
    let m;
    try { m = JSON.parse(data); } catch { return; }
    if (m.type === 'ping') {
      ws.send(JSON.stringify({ type: 'pong', t: m.t }));
      return;
    }
    m.id = id;
    if (m.type === 'state') Object.assign(players.get(ws), m);
    broadcast(m, ws);
  });

  ws.on('close', () => {
    players.delete(ws);
    broadcast({ type: 'leave', id });
  });
});

server.listen(process.env.PORT || 3000);
