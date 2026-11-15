// naive-server.js : the BROKEN baseline (plain REST, read -> check -> write)
// Run: node naive-server.js   (listens on port 3001)
const http = require('http');

const fresh = () => ({ high: 0, winner: null, accepted: 0, rejected: 0, lostUpdates: 0 });
let state = fresh();

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const json = (res, obj) => {
  res.writeHead(200, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify(obj));
};

const server = http.createServer(async (req, res) => {
  if (req.method === 'GET' && req.url === '/stats') return json(res, state);
  if (req.method === 'POST' && req.url === '/reset') {
    state = fresh();
    return json(res, { ok: true });
  }

  if (req.method === 'POST' && req.url === '/bid') {
    let body = '';
    for await (const chunk of req) body += chunk;
    const { userId, amount } = JSON.parse(body);

    const seen = state.high;               // 1. READ the current price
    await sleep(5 + Math.random() * 10);   //    simulated database round trip (5-15 ms)

    if (amount > seen) {                   // 2. CHECK against a possibly STALE value
      if (state.high > amount) state.lostUpdates++; // we are about to overwrite a HIGHER bid
      state.high = amount;                 // 3. WRITE
      state.winner = userId;
      state.accepted++;
      return json(res, { accepted: true });
    }
    state.rejected++;
    return json(res, { accepted: false, price: state.high });
  }

  res.writeHead(404);
  res.end();
});

server.listen(3001, () => console.log('Naive REST server on http://localhost:3001'));
