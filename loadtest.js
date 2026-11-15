// loadtest.js : coordinator. Starts worker processes, runs a synchronized burst, verifies correctness.
// Run: node loadtest.js [clients] [bidsPerClient] [windowSeconds] [workers] [portCount]
//   node loadtest.js 5000 10 10            -> 5,000 clients, 1 worker, 1 port
//   node loadtest.js 30000 3 10 6 4        -> 30,000 clients, 6 workers, server started with 4 ports
// (start the server first with the same port count:  node engine-server.js 4)
//
// Source addresses: an OS can only give ~16k outgoing connections per source IP address.
// When testing against this machine, each worker therefore uses its OWN loopback address
// (127.0.0.10, 127.0.0.11, ...) so the limit applies per worker, not to the whole test.
// The coordinator uses 127.0.0.2 for its own requests so it never competes for those ports.
const { fork } = require('child_process');
const http = require('http');
const path = require('path');

const N = Number(process.argv[2]) || 2000;
const BIDS = Number(process.argv[3]) || 3;
const WINDOW = Number(process.argv[4]) || 10;
const WORKERS = Number(process.argv[5]) || Math.max(1, Math.ceil(N / 5000));
const PORT_COUNT = Number(process.argv[6]) || 1;
const HOST = !process.env.HOST || process.env.HOST === 'localhost' ? '127.0.0.1' : process.env.HOST;
const BASE_PORT = Number(process.env.PORT) || 3000;
const LOCAL = HOST.startsWith('127.');           // testing against this machine?

const pct = (arr, p) => (arr.length ? arr[Math.min(arr.length - 1, Math.floor(arr.length * p))] : 0);

// plain HTTP GET with its own source address and no connection reuse
const getText = (p) => new Promise((resolve, reject) => {
  const opts = { host: HOST, port: BASE_PORT, path: p, agent: false };
  if (LOCAL) opts.localAddress = '127.0.0.2';
  http.get(opts, (res) => {
    let b = '';
    res.on('data', (c) => (b += c));
    res.on('end', () => resolve(b));
  }).on('error', reject);
});
const getJson = async (p) => JSON.parse(await getText(p));

// resolves with the next message of the given type (or {crashed:true} if the worker dies first)
const waitFor = (child, type) => new Promise((resolve) => {
  const onMsg = (m) => { if (m.type === type) { cleanup(); resolve(m); } };
  const onExit = () => { cleanup(); resolve({ type, crashed: true }); };
  const cleanup = () => { child.off('message', onMsg); child.off('exit', onExit); };
  child.on('message', onMsg);
  child.on('exit', onExit);
});

(async () => {
  await getText('/reset?seconds=600');

  // split clients between workers
  const per = Math.floor(N / WORKERS), extra = N % WORKERS;
  const children = [];
  let offset = 0;
  console.log(`Starting ${WORKERS} worker(s) for ${N} clients across ${PORT_COUNT} port(s)...`);
  const readyP = [];
  for (let w = 0; w < WORKERS; w++) {
    const clients = per + (w < extra ? 1 : 0);
    const child = fork(path.join(__dirname, 'loadtest-worker.js'));
    children.push(child);
    readyP.push(waitFor(child, 'ready'));
    child.send({
      type: 'config', clients, offset, bids: BIDS, window: WINDOW, host: HOST,
      basePort: BASE_PORT, portCount: PORT_COUNT,
      localAddress: LOCAL ? `127.0.0.${10 + w}` : undefined,   // own source IP per worker
    });
    offset += clients;
  }
  const ready = await Promise.all(readyP);
  const connected = ready.reduce((s, r) => s + (r.connected || 0), 0);
  const failed = ready.reduce((s, r) => s + (r.failed || 0), 0);
  const errors = {};
  ready.forEach((r) => Object.entries(r.errors || {}).forEach(([k, v]) => (errors[k] = (errors[k] || 0) + v)));
  console.log(`Connected: ${connected} / ${N} (failed: ${failed}${failed ? ' ' + JSON.stringify(errors) : ''}). Starting ${WINDOW}s burst...`);

  // synchronized start
  const doneP = children.map((c) => waitFor(c, 'done'));
  children.forEach((c) => c.send({ type: 'go' }));
  const done = await Promise.all(doneP);

  let stats;
  try {
    stats = await getJson('/stats');                       // clients are still connected at this moment
  } catch (e) {
    console.log('Could not fetch server stats:', e.code || e.message);
    children.forEach((c) => c.send({ type: 'exit' }));
    setTimeout(() => process.exit(1), 1000);
    return;
  }

  // aggregate
  let totalSent = 0, results = 0, accepted = 0, lat = [], acc = [];
  const reasons = {};
  for (const d of done) {
    totalSent += d.totalSent || 0; results += d.results || 0; accepted += d.accepted || 0;
    if (d.latencies) lat = lat.concat(d.latencies);
    if (d.acceptedAmounts) acc = acc.concat(d.acceptedAmounts);
    Object.entries(d.reasons || {}).forEach(([k, v]) => (reasons[k] = (reasons[k] || 0) + v));
  }
  lat.sort((a, b) => a - b);
  const maxAccepted = acc.reduce((m, v) => (v > m ? v : m), 0);
  const unique = new Set(acc).size === acc.length;
  const crashed = done.filter((d) => d.crashed).length;

  console.log('\n=== ENGINE LOAD TEST RESULT ===');
  console.log(`Clients connected (peak, server side): ${stats.peakConnections}  (ports: ${stats.ports}, workers: ${WORKERS})`);
  console.log(`Broadcast interval    : ${stats.broadcastMs} ms`);
  console.log(`Bids sent / answered  : ${totalSent} / ${results}`);
  console.log(`Accepted              : ${accepted}   Rejected: ${JSON.stringify(reasons)}`);
  console.log(`Offered load          : ${(totalSent / WINDOW).toFixed(0)} bids/s`);
  console.log(`Bid latency (ms)      : p50=${pct(lat, 0.5)}  p95=${pct(lat, 0.95)}  p99=${pct(lat, 0.99)}`);
  console.log(`Server skipped sends  : ${stats.skippedSends} (backpressure)   Rate-limited: ${stats.rateLimited}`);
  if (crashed) console.log(`WARNING: ${crashed} worker(s) crashed (likely out of memory)`);
  console.log('\n--- Correctness checks ---');
  const checks = [
    ['Accepted prices strictly increasing (server log)', stats.strictlyIncreasing],
    ['No two accepted bids share a price', unique],
    ['Final price == highest accepted bid', stats.high === maxAccepted],
    ['Accepted count matches server count', accepted === stats.accepted],
  ];
  for (const [name, ok] of checks) console.log(`${ok ? 'PASS' : 'FAIL'}  ${name}`);

  children.forEach((c) => c.send({ type: 'exit' }));
  setTimeout(() => process.exit(0), 1500);
})();
