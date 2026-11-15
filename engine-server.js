// engine-server.js : the FIXED bidding engine (WebSocket + atomic bids + batched broadcast)
// Run: node engine-server.js [portCount] [broadcastMs]
//   node engine-server.js 4 500  -> four ports, price broadcast every 500 ms (default 100)
//   node engine-server.js        -> one port  (3000)
//   node engine-server.js 4      -> four ports (3000-3003), ONE shared auction state
// Why several ports? One TCP port can only accept ~16k-28k connections from a single
// client machine (ephemeral port limit). Several ports lift that limit for load testing.
const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');

const BASE_PORT = Number(process.env.PORT) || 3000;
const PORT_COUNT = Number(process.argv[2]) || 1;
const CFG = {
  minIncrement: 1,
  snipeWindowMs: 3000,   // a bid in the last 3 s ...
  snipeExtendMs: 3000,   // ... resets the clock to 3 s remaining
  maxExtensions: 5,
  maxBidsPerSec: 5,      // per connection (token bucket)
  broadcastMs: Number(process.argv[3]) || 100,  // batch updates: one broadcast per interval (default 10 per second)
  maxBuffered: 64 * 1024 // backpressure: skip clients whose send buffer is full
};

const wsServers = [];
const totalConnections = () => wsServers.reduce((n, w) => n + w.clients.size, 0);

// ---------- Auction state (single writer: Node's event loop) ----------
const seen = new Map(); // bidId -> earlier result (idempotency)
const auction = {};
function resetAuction(seconds) {
  seen.clear();
  Object.assign(auction, {
    high: 0, winner: null, endsAt: Date.now() + seconds * 1000,
    extensions: 0, closed: false, dirty: true,
    accepted: 0, rejected: 0, duplicates: 0, rateLimited: 0, skippedSends: 0,
    peakConnections: totalConnections(),
    acceptedLog: [],
  });
}
resetAuction(60);

const auditLog = fs.createWriteStream(path.join(__dirname, 'bids.log'), { flags: 'a' }); // async audit trail

// ---------- THE ENGINE: this function runs start-to-finish with no interleaving ----------
// JS is single-threaded and there is no `await` inside, so no other bid can run
// between "read high" and "write high". That is what removes the race condition.
function placeBid({ bidId, userId, amount }) {
  if (typeof bidId !== 'string' || typeof userId !== 'string' ||
      !Number.isSafeInteger(amount) || amount <= 0) {
    return { accepted: false, reason: 'invalid', price: auction.high };
  }
  if (seen.has(bidId)) {                       // retry of an earlier bid
    auction.duplicates++;
    return seen.get(bidId);
  }

  const now = Date.now();
  let result;
  if (auction.closed || now >= auction.endsAt) {
    result = { accepted: false, reason: 'closed' };
    auction.rejected++;
  } else if (amount < auction.high + CFG.minIncrement) {
    result = { accepted: false, reason: 'too_low' };
    auction.rejected++;
  } else {
    auction.high = amount;                     // atomic check-and-update
    auction.winner = userId;
    auction.accepted++;
    auction.dirty = true;
    auction.acceptedLog.push(amount);
    auditLog.write(`${now},${userId},${bidId},${amount}\n`);
    // anti-sniping
    if (auction.endsAt - now < CFG.snipeWindowMs && auction.extensions < CFG.maxExtensions) {
      auction.endsAt = now + CFG.snipeExtendMs;
      auction.extensions++;
    }
    result = { accepted: true };
  }
  result.price = auction.high;
  seen.set(bidId, result);
  return result;
}

// ---------- Rate limiting (token bucket per connection) ----------
function allow(ws) {
  const now = Date.now();
  ws.tokens = Math.min(CFG.maxBidsPerSec, ws.tokens + ((now - ws.last) / 1000) * CFG.maxBidsPerSec);
  ws.last = now;
  if (ws.tokens >= 1) { ws.tokens -= 1; return true; }
  return false;
}

// ---------- HTTP (serves the page + stats + reset) ----------
function httpHandler(req, res) {
  const url = new URL(req.url, 'http://x');
  if (url.pathname === '/') {
    res.writeHead(200, { 'Content-Type': 'text/html' });
    return res.end(fs.readFileSync(path.join(__dirname, 'client.html')));
  }
  if (url.pathname === '/reset') {
    resetAuction(Number(url.searchParams.get('seconds')) || 60);
    res.writeHead(200); return res.end('ok');
  }
  if (url.pathname === '/stats') {
    const log = auction.acceptedLog;
    const strictlyIncreasing = log.every((v, i) => i === 0 || v > log[i - 1]);
    res.writeHead(200, { 'Content-Type': 'application/json' });
    return res.end(JSON.stringify({
      high: auction.high, winner: auction.winner, accepted: auction.accepted,
      rejected: auction.rejected, duplicates: auction.duplicates,
      rateLimited: auction.rateLimited, skippedSends: auction.skippedSends,
      extensions: auction.extensions, strictlyIncreasing,
      connections: totalConnections(), peakConnections: auction.peakConnections, ports: PORT_COUNT,
      broadcastMs: CFG.broadcastMs,
    }));
  }
  res.writeHead(404); res.end();
}

// ---------- WebSocket gateway ----------
const stateMsg = () => JSON.stringify({
  type: 'state', price: auction.high, winner: auction.winner,
  endsAt: auction.endsAt, now: Date.now(), closed: auction.closed,
  bidders: totalConnections(),
});

function onConnection(ws) {
  ws.tokens = CFG.maxBidsPerSec;
  ws.last = Date.now();
  const n = totalConnections();
  if (n > auction.peakConnections) auction.peakConnections = n;
  ws.send(stateMsg());                                   // new client gets current state at once

  ws.on('error', () => {});                              // a broken socket must never crash the server
  ws.on('message', (data) => {
    let m;
    try { m = JSON.parse(data); } catch { return; }
    if (m.type !== 'bid') return;
    if (!allow(ws)) {
      auction.rateLimited++;
      return ws.send(JSON.stringify({ type: 'result', bidId: m.bidId, accepted: false, reason: 'rate_limited' }));
    }
    const r = placeBid(m);
    ws.send(JSON.stringify({ type: 'result', bidId: m.bidId, ...r }));   // instant answer to the bidder
  });
}

for (let i = 0; i < PORT_COUNT; i++) {
  const port = BASE_PORT + i;
  const server = http.createServer(httpHandler);
  const wss = new WebSocketServer({ server, maxPayload: 1024 });
  wss.on('connection', onConnection);
  wsServers.push(wss);
  server.listen(port, () => console.log(`Auction engine listening on http://localhost:${port} (broadcast every ${CFG.broadcastMs} ms)`));
}

// ---------- Batched broadcast: one tick every 100 ms, only if something changed ----------
setInterval(() => {
  if (!auction.closed && Date.now() >= auction.endsAt) { auction.closed = true; auction.dirty = true; }
  if (!auction.dirty) return;
  auction.dirty = false;
  const msg = stateMsg();
  for (const wss of wsServers) {
    for (const c of wss.clients) {
      if (c.readyState !== 1) continue;
      if (c.bufferedAmount > CFG.maxBuffered) { auction.skippedSends++; continue; } // slow client: backpressure
      c.send(msg);
    }
  }
}, CFG.broadcastMs);
