// design-shots.cjs <out dir> <shots dir>: serves the static site from <out dir>,
// passes /api/* through to the live API, and photographs the overview's map
// at desktop widths in both themes. Runs only in the Design shots workflow.
const http = require("http");
const https = require("https");
const fs = require("fs");
const path = require("path");
const { chromium } = require(path.join(process.env.PW_DIR, "node_modules", "playwright-core"));

const [root, out] = process.argv.slice(2).map((p) => path.resolve(p));
const LIVE = "tensile.huginn.tech";
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

(async () => {
  await new Promise((ok) => server.listen(4173, "127.0.0.1", ok));
  fs.mkdirSync(out, { recursive: true });
  const browser = await chromium.launch({ executablePath: process.env.CHROME || "/usr/bin/google-chrome", args: ["--hide-scrollbars"] });
  const notes = [];
  for (const width of [1440, 1280]) {
    for (const theme of ["dark", "light"]) {
      const ctx = await browser.newContext({ viewport: { width, height: 900 }, deviceScaleFactor: 2, colorScheme: theme });
      await ctx.addInitScript((t) => { try { localStorage.setItem("theme", t); } catch {} }, theme);
      const page = await ctx.newPage();
      page.on("pageerror", (e) => notes.push(`${width} ${theme} page error: ${e.message}`));
      await page.goto("http://127.0.0.1:4173/", { waitUntil: "domcontentloaded" });
      try {
        await page.waitForFunction(() => document.querySelectorAll(".cm-pin").length >= 5, null, { timeout: 45000 });
      } catch { notes.push(`${width} ${theme}: fewer than 5 places after 45 s`); }
      await page.evaluate(() => document.fonts.ready);
      await page.waitForTimeout(2500);
      const tag = `${width}-${theme}`;
      const deck = page.locator(".deck").first();
      await deck.screenshot({ path: path.join(out, `deck-${tag}.png`), animations: "disabled" });
      const atlas = page.locator(".cm-atlas").first();
      await atlas.screenshot({ path: path.join(out, `map-${tag}.png`), animations: "disabled" });
      // Southeast and East Asia, where the smallest hosted places are
      const b = await atlas.boundingBox();
      if (b) await page.screenshot({ path: path.join(out, `asia-${tag}.png`), animations: "disabled", clip: { x: b.x + b.width * 0.58, y: b.y + b.height * 0.12, width: b.width * 0.3, height: b.height * 0.62 } });
      // the east edge, where the detail windows stand, with the places they detail
      if (b) await page.screenshot({ path: path.join(out, `east-${tag}.png`), animations: "disabled", clip: { x: b.x + b.width * 0.64, y: b.y, width: b.width * 0.36, height: b.height } });
      // a pointer on the last detail window: its place opens on the world
      const inset = page.locator(".cm-inset").last();
      if (await inset.count()) {
        await inset.hover();
        await page.waitForTimeout(700);
        await atlas.screenshot({ path: path.join(out, `open-${tag}.png`), animations: "disabled" });
        await page.mouse.move(1, 1);
      }
      await ctx.close();
    }
  }
  await browser.close();
  server.close();
  fs.writeFileSync(path.join(out, "notes.txt"), notes.join("\n") + "\n");
  console.log(fs.readdirSync(out).join("\n"));
  if (notes.length) console.log(notes.join("\n"));
})().catch((e) => { console.error(e); process.exit(1); });
