const { chromium } = require('playwright-core');
const path = require('path');
const { execSync } = require('child_process');

const OUT = process.argv[2];
const BASE = 'http://localhost:8090';
const CHROME = '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome';
const VIEW = { width: 1920, height: 968 };

let n = 41;
async function shot(page, name) {
  n += 1;
  const f = path.join(OUT, `${String(n).padStart(3, '0')}_${name}.png`);
  await page.screenshot({ path: f });
  console.log(`${String(n).padStart(3, '0')} ${name}`);
}
const sh = (cmd) => execSync(cmd, { encoding: 'utf8' }).trim();

(async () => {
  const browser = await chromium.launch({ executablePath: CHROME });
  const page = await browser.newPage({ viewport: VIEW, deviceScaleFactor: 2 });

  // ---------- the distributed queue ----------
  await page.goto(`${BASE}/queue?name=events`, { waitUntil: 'networkidle' });
  await page.waitForTimeout(1800);
  await shot(page, 'events_queue');
  await page.goto(`${BASE}/queue/metrics?name=events`, { waitUntil: 'networkidle' });
  await page.waitForTimeout(4000);
  await shot(page, 'events_metrics');
  await page.goto(`${BASE}/cluster`, { waitUntil: 'networkidle' });
  await page.waitForTimeout(2000);
  await shot(page, 'placement');

  // ---------- add two nodes and watch the slots move ----------
  sh('cd /Users/harish/Documents/code/keychain-assignment-dpq/deploy && docker compose up -d --no-recreate --scale node=6 2>&1 | tail -1');
  for (let i = 0; i < 8; i++) {
    await page.waitForTimeout(7000);
    await page.reload({ waitUntil: 'networkidle' });
    await page.waitForTimeout(1200);
    await shot(page, `scaling_${String(i).padStart(2, '0')}`);
  }

  // ---------- take a node down ----------
  const ids = sh('cd /Users/harish/Documents/code/keychain-assignment-dpq/deploy && docker compose ps -q node').split('\n');
  const victim = ids[0].slice(0, 12);
  console.log('   stopping ' + victim);
  sh(`docker stop ${victim}`);

  for (let i = 0; i < 4; i++) {
    await page.waitForTimeout(6000);
    await page.reload({ waitUntil: 'networkidle' });
    await page.waitForTimeout(1200);
    await shot(page, `node_down_${String(i).padStart(2, '0')}`);
  }
  await page.goto(`${BASE}/queue?name=events`, { waitUntil: 'networkidle' });
  await page.waitForTimeout(2500);
  await shot(page, 'events_degraded');

  sh(`docker start ${victim}`);
  await page.waitForTimeout(14000);
  await page.goto(`${BASE}/cluster`, { waitUntil: 'networkidle' });
  await page.waitForTimeout(2500);
  await shot(page, 'node_back');

  // ---------- tenancy and theme ----------
  await page.goto(BASE, { waitUntil: 'networkidle' });
  await page.waitForTimeout(1500);
  await shot(page, 'acme');
  await page.selectOption('header select', { label: 'Globex' });
  await page.waitForTimeout(2000);
  await shot(page, 'globex');
  await page.selectOption('header select', { label: 'Acme' });
  await page.waitForTimeout(1600);
  await page.click('button[aria-label="Toggle theme"]');
  await page.waitForTimeout(1200);
  await shot(page, 'dark_queues');
  await page.goto(`${BASE}/queue/metrics?name=orders`, { waitUntil: 'networkidle' });
  await page.waitForTimeout(4000);
  await shot(page, 'dark_metrics');
  await page.goto(`${BASE}/cluster`, { waitUntil: 'networkidle' });
  await page.waitForTimeout(2000);
  await shot(page, 'dark_cluster');

  await browser.close();
})();
