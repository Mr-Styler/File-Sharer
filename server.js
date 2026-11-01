const express = require('express'), http = require('http'), os = require('os');
const { WebSocketServer } = require('ws');
const QR = require('qrcode');

const PORT = process.env.PORT || 3000;
const app = express();
const fs = require('fs'), path = require('path');
// Find index.html: prefer ./public/index.html, fall back to ./index.html next to server.js
const PUB = path.join(__dirname, 'public');
const INDEX = [path.join(PUB, 'index.html'), path.join(__dirname, 'index.html')].find(fs.existsSync);
if (!INDEX) { console.error('\nERROR: index.html not found. Put it in a "public" folder next to server.js.\n'); process.exit(1); }
if (fs.existsSync(PUB)) app.use(express.static(PUB));
const page = (q, r) => r.sendFile(INDEX);

// Page routes: the app is a single page, each path opens the matching view
app.get('/', page);
app.get(['/dashboard', '/send', '/receive', '/history', '/transfer'], (q, r) => r.redirect('/#' + q.path.slice(1)));

function lan() {
  for (const list of Object.values(os.networkInterfaces()))
    for (const i of list) if (i.family === 'IPv4' && !i.internal) return i.address;
  return 'localhost';
}
app.get('/api/info', (q, r) => r.json({ ip: lan(), url: `http://${lan()}:${PORT}` }));
app.get('/qr.svg', async (q, r) => {
  r.type('image/svg+xml').send(await QR.toString(String(q.query.t || ''), { type: 'svg', margin: 1 }));
});

app.use(page); // anything else loads the app instead of "Cannot GET"

const server = http.createServer(app);
const wss = new WebSocketServer({ server, maxPayload: 8 * 1024 * 1024 });
const peers = new Map();      // id -> {ws, name, type, ip}
const transfers = new Map();  // tid -> {from, to}

const send = (ws, m) => ws.readyState === 1 && ws.send(JSON.stringify(m));
const fwd = (id, m) => peers.has(id) && send(peers.get(id).ws, m);
const broadcast = () => {
  const list = [...peers].map(([id, p]) => ({ id, name: p.name, type: p.type, ip: p.ip }));
  for (const p of peers.values()) send(p.ws, { t: 'peers', peers: list });
};

wss.on('connection', (ws, req) => {
  const id = Math.random().toString(36).slice(2, 8);
  peers.set(id, { ws, name: 'Device', type: 'laptop', ip: req.socket.remoteAddress.replace('::ffff:', '') });
  send(ws, { t: 'hi', id });
  broadcast();

  ws.on('message', (data, isBinary) => {
    if (isBinary) { // frame = 8 byte transfer id + 4 byte file index + payload
      const x = transfers.get(data.subarray(0, 8).toString());
      const p = x && peers.get(x.to);
      if (p && p.ws.readyState === 1) p.ws.send(data, { binary: true });
      return;
    }
    let m; try { m = JSON.parse(data); } catch { return; }
    const me = peers.get(id);
    if (m.t === 'hello') { me.name = String(m.name).slice(0, 40); me.type = m.type; broadcast(); return; }
    if (m.t === 'offer') { transfers.set(m.tid, { from: id, to: m.to }); fwd(m.to, { ...m, from: id }); return; }
    const x = transfers.get(m.tid);
    if (!x) return;
    fwd(id === x.from ? x.to : x.from, { ...m, from: id });
    if (['decline', 'done', 'cancel'].includes(m.t)) transfers.delete(m.tid);
  });
  ws.on('close', () => { peers.delete(id); broadcast(); });
});

server.listen(PORT, '0.0.0.0', () => {
  console.log(`\nLocalShare running.\nOpen on any device on the same Wi-Fi:  http://${lan()}:${PORT}\n`);
});