// design-shots.cjs <out dir> <shots dir>: serves the static site from <out dir>, passes /api/* through to the live API,
// and photographs the view tabs as pill buttons: the Blobs list's Blobs | Namespaces and a validator's Latest checks |
// Endpoint history, each view selected in turn, at 1440 in both themes. Runs only in the Design shots workflow, on a
// throwaway branch that is never merged.
const http = require("http");
const https = require("https");
const fs = require("fs");
const path = require("path");
const { chromium } = require(path.join(process.env.PW_DIR, "node_modules", "playwright-core"));

const [root, out] = process.argv.slice(2).map((p) => path.resolve(p));
const LIVE = "tensile.huginn.tech";
const PORT = 4173;
const BASE = `http://127.0.0.1:${PORT}`;
const TYPES = { ".html": "text/html; charset=utf-8", ".js": "text/javascript", ".css": "text/css", ".json": "application/json", ".svg": "image/svg+xml", ".png": "image/png", ".ico": "image/x-icon", ".woff2": "font/woff2", ".txt": "text/plain" };

const server = http.createServer((req, res) => {
  if (req.url.startsWith("/api/")) {
    const up = https.request({ host: LIVE, path: req.url, method: req.method, headers: { accept: req.headers.accept || "*/*", "user-agent": "tensile-design-shots" } }, (r) => {
      res.writeHead(r.statusCode, r.headers);
      r.pipe(res);
    });
    up.on("error", () => { res.writeHead(502); res.end(); });
    req.on("close", () => up.destroy());
    return up.end();
  }
  let p = decodeURIComponent(req.url.split("?")[0]);
  if (p.endsWith("/")) p += "index.html";
  const f = path.join(root, p);
  if (!f.startsWith(root) || !fs.existsSync(f) || fs.statSync(f).isDirectory()) { res.writeHead(404); return res.end(); }
  res.writeHead(200, { "content-type": TYPES[path.extname(f)] || "application/octet-stream" });
  fs.createReadStream(f).pipe(res);
});

// ITRocket: four endpoint registrations, so the history tab has rows
const VAL = "924090b949a3a3a43aee52c4dae342332c57b684";
const W = 1440, H = 1100;
const notes = [];

async function shoot(browser, theme) {
  const ctx = await browser.newContext({ viewport: { width: W, height: H }, deviceScaleFactor: 2, colorScheme: theme });
  await ctx.addInitScript((t) => { try { localStorage.setItem("theme", t); } catch {} }, theme);
  const page = await ctx.newPage();
  page.on("pageerror", (e) => notes.push(`${theme} page error: ${e.message}`));

  /** the list head and the first rows under it, from the list's left edge to its right */
  async function crop(file, head, rows) {
    await page.evaluate(() => document.fonts.ready);
    await page.waitForTimeout(1200);
    const b = await page.evaluate(({ head, rows }) => {
      const h = document.querySelector(head);
      if (!h) return null;
      h.scrollIntoView({ block: "start" });
      scrollBy(0, -40);
      const hb = h.getBoundingClientRect();
      const t = document.querySelector(rows);
      const bottom = t ? Math.min(t.getBoundingClientRect().bottom, hb.top + 360) : hb.bottom;
      return { x: hb.left, y: hb.top, w: hb.width, b: bottom };
    }, { head, rows });
    if (!b) { notes.push(`${file}: no ${head}`); return; }
    const x = Math.max(0, Math.floor(b.x - 24)), y = Math.max(0, Math.floor(b.y - 24));
    await page.screenshot({ path: path.join(out, file), animations: "disabled", clip: { x, y, width: Math.ceil(b.w + 48), height: Math.ceil(b.b - y + 16) } });
  }

  await page.goto(`${BASE}/blobs/`, { waitUntil: "domcontentloaded" });
  try { await page.waitForSelector("#list .lg-t tbody tr.row td.c-p .lg-who", { timeout: 45000 }); } catch { notes.push("blobs: rows did not appear"); }
  await crop(`blobs-tab-blobs-${theme}.png`, "#list .list-head", "#list .lg-t");
  await page.click("#list .tabs button:nth-child(2)");
  try { await page.waitForSelector("#list .lg-nss tbody tr", { timeout: 20000 }); } catch { notes.push("namespaces: rows did not appear"); }
  await crop(`blobs-tab-namespaces-${theme}.png`, "#list .list-head", "#list .lg-nss");

  await page.goto(`${BASE}/validator/?addr=${VAL}`, { waitUntil: "domcontentloaded" });
  try { await page.waitForSelector(".vd-tabs", { timeout: 45000 }); } catch { notes.push("validator: tabs did not appear"); }
  try { await page.waitForSelector("#evidence .lg-t tbody tr", { timeout: 30000 }); } catch { notes.push("validator: checks did not appear"); }
  await crop(`validator-tab-checks-${theme}.png`, "#evidence .list-head", "#evidence .lg-t");
  await page.click("#tab-endpoints");
  try { await page.waitForSelector(".eh-t tbody tr", { timeout: 20000 }); } catch { notes.push("validator: history did not appear"); }
  await crop(`validator-tab-endpoints-${theme}.png`, "#evidence .list-head", ".eh-t");
  // the tab not shown, hovered
  await page.hover("#tab-checks");
  await page.waitForTimeout(300);
  await crop(`validator-tab-hover-${theme}.png`, "#evidence .list-head", ".eh-t");
  await ctx.close();
}

(async () => {
  fs.mkdirSync(out, { recursive: true });
  await new Promise((r) => server.listen(PORT, "127.0.0.1", r));
  const browser = await chromium.launch({ executablePath: process.env.CHROME || "/usr/bin/google-chrome", args: ["--hide-scrollbars"] });
  for (const theme of ["dark", "light"]) await shoot(browser, theme);
  await browser.close();
  server.close();
  fs.writeFileSync(path.join(out, "notes.txt"), notes.join("\n") + "\n");
  console.log(notes.length ? notes.join("\n") : "no notes");
})().catch((e) => { console.error(e); process.exit(1); });
