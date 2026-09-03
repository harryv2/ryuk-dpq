const { chromium } = require('playwright-core');
const path = require('path');

const OUT = process.argv[2];
const BASE = 'http://localhost:8090';
const CHROME = '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome';
const AUTH = { 'Content-Type': 'application/json', Authorization: 'Bearer acme-token' };
const VIEW = { width: 1920, height: 968 };   // the composition's shot area exactly
const Q = 'orders';

let n = 0;
async function shot(page, name) {
  n += 1;
  const f = path.join(OUT, `${String(n).padStart(3, '0')}_${name}.png`);
  await page.screenshot({ path: f });
  console.log(`${String(n).padStart(3, '0')} ${name}`);
}

const api = {
  send: (q, payload, priority, groupId) =>
    fetch(`${BASE}/v1/queues/${q}/messages`, {
      method: 'POST', headers: AUTH,
      body: JSON.stringify({ payload, priority, groupId }),
    }).catch(() => {}),
  take: async (q, max) => {
    const r = await fetch(`${BASE}/v1/queues/${q}/messages/dequeue`, {
      method: 'POST', headers: AUTH, body: JSON.stringify({ maxMessages: max }),
    }).catch(() => null);
    if (!r || r.status !== 200) return [];
    return (await r.json()).messages ?? [];
  },
  ack: (q, receipt) =>
    fetch(`${BASE}/v1/queues/${q}/messages/ack`, {
      method: 'POST', headers: AUTH, body: JSON.stringify({ receipt }),
    }).catch(() => {}),
  stats: async (q) => {
    const r = await fetch(`${BASE}/v1/queues/${q}/stats`, { headers: AUTH }).catch(() => null);
    return r && r.status === 200 ? r.json() : null;
  },
};

// Traffic driven from the side, so the metrics page has something moving while
// it is being photographed.
function load({ inPerTick, ackPerTick }) {
  let running = true;
  (async () => {
    let i = 0;
    while (running) {
      i += 1;
      for (let k = 0; k < inPerTick; k++) {
        const p = ['HIGH', 'MEDIUM', 'LOW'][(i + k) % 3];
        await api.send(Q, `load ${i}-${k}`, p, `g${(i + k) % 6}`);
      }
      for (const m of await api.take(Q, ackPerTick)) await api.ack(Q, m.receipt);
      await new Promise((r) => setTimeout(r, 700));
    }
  })();
  return () => { running = false; };
}

(async () => {
  const browser = await chromium.launch({ executablePath: CHROME });
  const page = await browser.newPage({ viewport: VIEW, deviceScaleFactor: 2 });

  // ---------- 1. the cluster it runs on ----------
  await page.goto(`${BASE}/cluster`, { waitUntil: 'networkidle' });
  await page.waitForTimeout(2000);
  await shot(page, 'cluster');
  await page.mouse.wheel(0, 820);
  await page.waitForTimeout(900);
  await shot(page, 'cluster_nodes');

  // ---------- 2. the queue list ----------
  await page.goto(BASE, { waitUntil: 'networkidle' });
  await page.waitForTimeout(1600);
  await shot(page, 'queues');

  // ---------- 3. create ----------
  await page.click('text=New queue');
  await page.waitForTimeout(800);
  await shot(page, 'new_blank');
  await page.fill('input[placeholder="orders"]', Q);
  await page.waitForTimeout(300);
  await shot(page, 'new_named');
  await page.click('button:has-text("Create queue")');
  await page.waitForTimeout(2000);
  await shot(page, 'created');

  // ---------- 4. four messages: 2 HIGH, 1 MEDIUM, 1 LOW ----------
  const send = async (payload, priority) => {
    await page.fill('textarea', payload);
    await page.selectOption('.fields select', priority);
    await page.click('form button.primary');   // "Send" also names the tab
    await page.waitForTimeout(900);
  };
  await send('ship order #1  (high)', 'HIGH');
  await shot(page, 'sent_one');
  await send('ship order #2  (high)', 'HIGH');
  await send('audit log entry  (medium)', 'MEDIUM');
  await send('nightly report  (low)', 'LOW');
  await page.waitForTimeout(1500);
  await shot(page, 'sent_four');

  // ---------- 5. poll one at a time ----------
  await page.click('.tabs button:has-text("Poll")');
  await page.waitForTimeout(700);
  await page.fill('.card input[type="number"]', '1');
  await page.waitForTimeout(300);
  await shot(page, 'poll_ready');

  const poll = async () => {
    await page.click('.card button:text-is("Poll")');
    await page.waitForTimeout(1500);
  };
  const rowAck = async () => {
    await page.click('table button:text-is("Ack")>>nth=0');   // Nack contains "ack"
    await page.waitForTimeout(1600);
  };
  const rowNack = async () => {
    await page.click('table button:text-is("Nack")>>nth=0');
    await page.waitForTimeout(1600);
  };

  await poll();  await shot(page, 'poll_high1');
  await rowAck(); await shot(page, 'ack_high1');
  await poll();  await shot(page, 'poll_high2');
  await rowNack(); await shot(page, 'nack_high2');
  await poll();  await shot(page, 'poll_high2_again');
  await rowAck();
  await poll();  await shot(page, 'poll_medium');
  await rowAck();
  await poll();  await shot(page, 'poll_low');
  await rowAck(); await shot(page, 'drained');

  await browser.close();
})();
