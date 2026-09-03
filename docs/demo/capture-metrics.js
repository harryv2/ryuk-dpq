const { chromium } = require('playwright-core');
const path = require('path');

const OUT = process.argv[2];
const BASE = 'http://localhost:8090';
const CHROME = '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome';
const AUTH = { 'Content-Type': 'application/json', Authorization: 'Bearer acme-token' };
const VIEW = { width: 1920, height: 968 };
const Q = 'orders';

let n = 17;                                  // continues the first phase
async function shot(page, name) {
  n += 1;
  const f = path.join(OUT, `${String(n).padStart(3, '0')}_${name}.png`);
  await page.screenshot({ path: f });
  console.log(`${String(n).padStart(3, '0')} ${name}`);
}

const send = (payload, priority, groupId) =>
  fetch(`${BASE}/v1/queues/${Q}/messages`, {
    method: 'POST', headers: AUTH, body: JSON.stringify({ payload, priority, groupId }),
  }).catch(() => {});

async function take(max) {
  const r = await fetch(`${BASE}/v1/queues/${Q}/messages/dequeue`, {
    method: 'POST', headers: AUTH, body: JSON.stringify({ maxMessages: max }),
  }).catch(() => null);
  if (!r || r.status !== 200) return [];
  return (await r.json()).messages ?? [];
}

const ack = (receipt) =>
  fetch(`${BASE}/v1/queues/${Q}/messages/ack`, {
    method: 'POST', headers: AUTH, body: JSON.stringify({ receipt }),
  }).catch(() => {});

// One tick of traffic. Producing faster than consuming makes the queue grow;
// flipping the ratio drains it. Both are visible on the charts.
async function tick(i, inPer, ackPer) {
  for (let k = 0; k < inPer; k++) {
    const p = ['HIGH', 'MEDIUM', 'LOW'][(i + k) % 3];
    await send(`load ${i}-${k}`, p, `g${(i + k) % 6}`);
  }
  for (const m of await take(ackPer)) await ack(m.receipt);
}

(async () => {
  const browser = await chromium.launch({ executablePath: CHROME });
  const page = await browser.newPage({ viewport: VIEW, deviceScaleFactor: 2 });

  await page.goto(`${BASE}/queue/metrics?name=${Q}`, { waitUntil: 'networkidle' });
  await page.waitForTimeout(3000);
  await shot(page, 'metrics_quiet');

  // ---- producers outrun consumers: depth climbs ----
  let i = 0;
  for (let frame = 0; frame < 11; frame++) {
    const until = Date.now() + 5000;
    while (Date.now() < until) { await tick(i++, 5, 1); await new Promise(r => setTimeout(r, 350)); }
    await shot(page, `metrics_growing_${String(frame).padStart(2, '0')}`);
  }

  // ---- consumers catch up: depth falls ----
  for (let frame = 0; frame < 9; frame++) {
    const until = Date.now() + 5000;
    while (Date.now() < until) { await tick(i++, 1, 6); await new Promise(r => setTimeout(r, 350)); }
    await shot(page, `metrics_draining_${String(frame).padStart(2, '0')}`);
  }

  // ---- the total across every priority, and a longer window ----
  await page.click('.chart-filters button:has-text("total")');
  await page.waitForTimeout(1400);
  await shot(page, 'metrics_total');
  await page.click('.chart-filters button:has-text("high")');
  await page.waitForTimeout(1400);
  await shot(page, 'metrics_filtered');
  await page.click('.chart-filters button:has-text("high")');
  await page.click('.window-pick button:text-is("1h")');
  await page.waitForTimeout(1800);
  await shot(page, 'metrics_window');

  await browser.close();
})();
