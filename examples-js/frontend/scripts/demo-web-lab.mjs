import { chromium } from "playwright";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const repoRoot = path.resolve(__dirname, "../../..");
const pdfPath = path.join(
  repoRoot,
  "src/test/resources/court-corpus/cafc-janich-v-collins.pdf"
);
const shotsDir = path.join(repoRoot, "examples-js/frontend/demo-screenshots");

const steps = [
  { name: "01-loaded", action: "loaded" },
  { name: "02-pdf-selected", action: "select" },
  { name: "03-linearized", action: "linearize" },
  { name: "04-fetch-sim", action: "fetch" }
];

async function main() {
  const browser = await chromium.launch({
    headless: false,
    channel: "chrome",
    args: ["--start-maximized"]
  });
  const context = await browser.newContext({ viewport: { width: 1440, height: 960 } });
  const page = await context.newPage();

  await page.goto("http://127.0.0.1:5173/", { waitUntil: "networkidle" });
  await page.screenshot({ path: path.join(shotsDir, `${steps[0].name}.png`), fullPage: true });

  await page.locator("#file-input").setInputFiles(pdfPath);
  await page.waitForSelector("#web-lab:not([hidden])", { timeout: 15000 });
  await page.waitForSelector("#preview-stage[data-state='ready']", { timeout: 60000 });
  await page.screenshot({ path: path.join(shotsDir, `${steps[1].name}.png`), fullPage: true });

  await page.locator("#run-linearize").click();
  await page.waitForSelector("#web-lab[data-state='complete']", { timeout: 120000 });
  await page.waitForSelector("#lab-download:not([hidden])", { timeout: 5000 });
  await page.screenshot({ path: path.join(shotsDir, `${steps[2].name}.png`), fullPage: true });

  await page.locator("#simulate-fetch").click();
  await page.waitForTimeout(2200);
  await page.screenshot({ path: path.join(shotsDir, `${steps[3].name}.png`), fullPage: true });

  const metrics = await page.evaluate(() => ({
    source: document.querySelector("#lab-source-bytes")?.textContent ?? "",
    linearized: document.querySelector("#lab-linearized-bytes")?.textContent ?? "",
    prefix: document.querySelector("#lab-prefix-bytes")?.textContent ?? "",
    savings: document.querySelector("#lab-savings")?.textContent ?? "",
    status: document.querySelector("#lab-status")?.textContent ?? ""
  }));

  console.log(JSON.stringify({ shotsDir, metrics }, null, 2));
  console.log("Chrome demo complete — window stays open for 45s.");
  await page.waitForTimeout(45000);
  await browser.close();
}

await main();
