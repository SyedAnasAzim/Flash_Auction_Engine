// loadtest-worker.js : started automatically by loadtest.js (do not run by hand).
// Each worker simulates a share of the bidders and reports back to the coordinator.
const WebSocket = require('ws');

let cfg;
const clients = [];
const sent = new Map();            // bidId -> { t, amount }
const latencies = [];
const acceptedAmounts = [];
const reasons = {};
const errors = {};
let results = 0, accepted = 0, failed = 0, totalSent = 0;

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function connect(i) {              // i = global client index
  return new Promise((resolve) => {
    const port = cfg.basePort + (i % cfg.portCount);          // spread clients over the server ports
    const ws = new WebSocket(`ws://${cfg.host}:${port}`, { handshakeTimeout: 30000 });
    let opened = false;
    ws.lastPrice = 0;
    ws.userId = 'bot' + i;
    ws.on('open', () => { opened = true; resolve(ws); });
    ws.on('error', (err) => {
      if (opened) return;
      failed++;
      const key = err.code || err.message;
      errors[key] = (errors[key] || 0) + 1;
      resolve(null);
    });
    ws.on('message', (data) => {
      const m = JSON.parse(data);
      if (m.type === 'state') ws.lastPrice = m.price;          // bots only know the last broadcast (may be stale)
      else if (m.type === 'result') {
        const s = sent.get(m.bidId);
        if (!s) return;
        results++;
        latencies.push(Date.now() - s.t);
        if (m.accepted) { accepted++; acceptedAmounts.push(s.amount); }
        else reasons[m.reason] = (reasons[m.reason] || 0) + 1;
      }
    });
  });
}

async function connectAll() {
  for (let k = 0; k < cfg.clients; k += 250) {                 // connect in batches of 250
    const n = Math.min(250, cfg.clients - k);
    const batch = await Promise.all(Array.from({ length: n }, (_, j) => connect(cfg.offset + k + j)));
    for (const ws of batch) if (ws) clients.push(ws);
  }
}

async function burst() {
  for (const ws of clients) {
    for (let b = 0; b < cfg.bids; b++) {
      setTimeout(() => {
        if (ws.readyState !== 1) return;
        const bidId = ws.userId + '-' + b + '-' + Math.random().toString(36).slice(2);
        const amount = ws.lastPrice + 1 + Math.floor(Math.random() * 50);
        sent.set(bidId, { t: Date.now(), amount });
        ws.send(JSON.stringify({ type: 'bid', bidId, userId: ws.userId, amount }));
        totalSent++;
      }, Math.random() * cfg.window * 1000);
    }
  }
  await sleep(cfg.window * 1000);
  const deadline = Date.now() + 30000;                          // wait (max 30 s) until every bid is answered,
  while (results < totalSent && Date.now() < deadline) await sleep(200);   // so slow answers are not miscounted
  await sleep(500);
}

process.on('message', async (m) => {
  if (m.type === 'config') {
    cfg = m;
    await connectAll();
    process.send({ type: 'ready', connected: clients.length, failed, errors });
  } else if (m.type === 'go') {
    await burst();
    process.send({ type: 'done', totalSent, results, accepted, reasons, latencies, acceptedAmounts });
  } else if (m.type === 'exit') {
    clients.forEach((c) => c.terminate());
    process.exit(0);
  }
});
