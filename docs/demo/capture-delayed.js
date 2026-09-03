const { chromium } = require('playwright-core');
const path = require('path');

const OUT = process.argv[2];
const BASE = 'http://localhost:8090';
const CHROME = '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome';
const VIEW = { width: 1920, height: 968 };
const Q = 'scheduled';

let n = 63;
async function shot(page, name) {
  n += 1;
  const f = path.join(OUT, `${String(n).padStart(3, '0')}_${name}.png`);
  await page.screenshot({ path: f });
  console.log(`${String(n).padStart(3, '0')} ${name}`);
}

(async () => {
  const browser = await chromium.launch({ executablePath: CHROME });
  const page = await browser.newPage({ viewport: VIEW, deviceScaleFactor: 2 });

  await fetch(`${BASE}/v1/queues/${Q}`, {
    method: 'DELETE', headers: { Authorization: 'Bearer acme-token' },
  }).catch(() => {});
  await page.waitForTimeout(1500);

  // a fresh queue, so the delayed message is the only thing on screen
  await page.goto(`${BASE}/queues/new`, { waitUntil: 'networkidle' });
  await page.waitForTimeout(800);
  await page.fill('input[placeholder="orders"]', Q);
  await page.click('button:has-text("Create queue")');
  await page.waitForTimeout(2200);

  const send = async (payload, deliverAfter) => {
    await page.fill('textarea', payload);
    await page.fill('input[placeholder="e.g. 30s"]', deliverAfter);
    await page.waitForTimeout(400);
  };

  // one for now
  await send('process refund', '');
  await page.click('form button.primary');
  await page.waitForTimeout(1200);

  // and one for later
  await send('send the 09:00 digest', '25s');
  await shot(page, 'delay_form');
  await page.click('form button.primary');
  await page.waitForTimeout(2000);
  await shot(page, 'delay_sent');

  // the delayed one is held back: a poll only gets the other
  await page.click('.tabs button:has-text("Poll")');
  await page.waitForTimeout(700);
  await page.fill('.card input[type="number"]', '5');
  await page.click('.card button:text-is("Poll")');
  await page.waitForTimeout(1800);
  await shot(page, 'delay_held_back');

  await page.click('table button:text-is("Ack")>>nth=0');
  await page.waitForTimeout(1800);
  await shot(page, 'delay_only_delayed');

  // nothing to take while it waits
  await page.click('.card button:text-is("Poll")');
  await page.waitForTimeout(1800);
  await shot(page, 'delay_nothing_yet');

  // ...until its time arrives
  await page.waitForTimeout(20000);
  await shot(page, 'delay_released');
  await page.click('.card button:text-is("Poll")');
  await page.waitForTimeout(1800);
  await shot(page, 'delay_arrived');

  await browser.close();
})();
