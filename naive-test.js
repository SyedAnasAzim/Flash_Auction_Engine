// naive-test.js : fires N simultaneous bids at the naive server and checks the result
// Run: node naive-test.js 500
const N = Number(process.argv[2]) || 500;
const HOST = 'localhost:3001';

(async () => {
  await fetch(`http://${HOST}/reset`, { method: 'POST' });

  const bids = Array.from({ length: N }, (_, i) => ({
    userId: 'user' + i,
    amount: 1 + Math.floor(Math.random() * 1_000_000),
  }));
  const maxSent = Math.max(...bids.map((b) => b.amount));
  const highestBidder = bids.find((b) => b.amount === maxSent).userId;

  await Promise.all(
    bids.map((b) =>
      fetch(`http://${HOST}/bid`, { method: 'POST', body: JSON.stringify(b) }).then((r) => r.json())
    )
  );

  const s = await (await fetch(`http://${HOST}/stats`)).json();
  const wrong = s.high !== maxSent;

  console.log('\n=== NAIVE REST RESULT ===');
  console.log(`Bids sent simultaneously : ${N}`);
  console.log(`Highest bid actually sent: ${maxSent} (by ${highestBidder})`);
  console.log(`Server says winner       : ${s.high} (by ${s.winner})`);
  console.log(`Bids accepted            : ${s.accepted}`);
  console.log(`Higher bids overwritten  : ${s.lostUpdates}`);
  console.log(wrong ? '\nRESULT: WRONG WINNER (race condition reproduced)' : '\nRESULT: correct this time (try a larger N)');
})();
