// design-shots.cjs <out dir> <shots dir>: serves the static site from <out dir>, passes /api/* through to the live API,
// and photographs the Blobs list as the production build of design A draws it, with a failed blob payment among its
// rows: at 1440 in both themes (the list's head and its first six rows), the hover on Failed, the pager, at 1180 (the
// band where the publisher's chip drops its prefix), at 1100 (where the time drops its age), and on a 375px phone in
// both themes, and one publisher's page, which keeps its table. Every view's heads and cells are measured (scrollWidth
// against clientWidth, every clipping box inside a cell too), with the columns' widths, where the status words and dots
// stand, and any sideways scroll; all of it goes to notes.txt.
//
// The live API does not know include_failed yet, so the browser's reads of the list are the live answer as the new
// route gives it: each blob row with "status":"success", failed_total, and on the first page, unfiltered, ONE failed
// blob payment between its first two rows, counted in total and failed_total: a STUB, see FAILED below. Runs only in
// the Design shots workflow, on a throwaway branch that is never merged.
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

const notes = [];

// ---------------------------------------------------------------- the stub
// STUB: one failed blob payment (a MsgPayForFibre that failed in its block), made up for these shots, in the row the
// API gives (failedblobs.go): its transaction and promise hashes are invented, its height lies between the live list's
// first two rows (its time in step), its namespace and publisher are the first row's, its reason "Out of gas". Set once,
// from the first live answer, and put back in its place in every later one.
const FAILED_TX = "9b23e1d4c07a5f3e8b6d2a91c4f07e35b8a2d6c19e4f03a7b5d8c2e61f097c4a";
const FAILED_PROMISE = "4f0c2a7e9d13b58c6a2e0f71d94b3c85e2a6f1d07b9c4e38a5d2f6c0e1b7a493";
for (const h of [FAILED_TX, FAILED_PROMISE]) if (!/^[0-9a-f]{64}$/.test(h)) throw new Error("a stub hash is not 64 hex characters");
let FAILED = null;

function stub(rows) {
  if (FAILED || rows.length < 2) return;
  const [a, b] = rows;
  const ha = a.settlement_height, hb = b.settlement_height;
  const h = ha - hb >= 2 ? Math.round((ha + hb) / 2) : hb;
  const ta = Date.parse(a.settlement_time), tb = Date.parse(b.settlement_time);
  const t = ha === hb ? tb : tb + Math.round(((ta - tb) * (h - hb)) / (ha - hb) / 1000) * 1000;
  FAILED = {
    status: "failed", promise_hash: FAILED_PROMISE, namespace: a.namespace, publisher: a.publisher || a.signer,
    settlement_height: h, settlement_tx_index: 0, settlement_tx_hash: FAILED_TX, settlement_time: new Date(Math.floor(t / 1000) * 1000).toISOString().replace(/\.000Z$/, "Z"),
    code: 11, codespace: "sdk", reason: "Out of gas", final: true,
  };
  notes.push(`stub: failed blob payment at #${h} (${FAILED.settlement_time}) between #${ha} and #${hb}, tx ${FAILED_TX}`);
}

/** /v1/blobs?include_failed=1 as the new route answers it: blob rows with their status, the stub in its place on the first page */
function withFailed(j, q) {
  if (!j || !Array.isArray(j.blobs) || q.get("include_failed") !== "1") return j;
  let rows = j.blobs.filter((r) => r.settlement_tx_hash !== FAILED_TX).map((r) => (r.status ? r : { status: "success", ...r }));
  let failed = 0;
  if (!q.get("namespace") && !q.get("publisher") && !q.get("before_height") && (Number(q.get("offset")) || 0) === 0) {
    stub(rows);
    if (FAILED) {
      let i = rows.findIndex((r) => r.settlement_height <= FAILED.settlement_height);
      if (i < 0) i = rows.length;
      rows.splice(i, 0, FAILED);
      failed = 1;
    }
  }
  const limit = Number(q.get("limit")) || rows.length;
  rows = rows.slice(0, limit);
  return { ...j, blobs: rows, total: (Number(j.total) || 0) + failed, failed_total: failed };
}

// ---------------------------------------------------------------- photographing
async function open(browser, theme, width, height) {
  const ctx = await browser.newContext({ viewport: { width, height }, deviceScaleFactor: 2, colorScheme: theme });
  await ctx.addInitScript((t) => { try { localStorage.setItem("theme", t); } catch {} }, theme);
  await ctx.route((u) => u.pathname === "/api/v1/blobs", async (route) => {
    const u = new URL(route.request().url());
    let res;
    try { res = await route.fetch(); } catch { return route.abort(); }
    const type = res.headers()["content-type"] || "";
    if (!res.ok() || !type.includes("json")) return route.fulfill({ response: res });
    let j;
    try { j = await res.json(); } catch { return route.fulfill({ response: res }); }
    return route.fulfill({ response: res, json: withFailed(j, u.searchParams) });
  });
  const page = await ctx.newPage();
  page.on("pageerror", (e) => notes.push(`${theme} ${width} page error: ${e.message}`));
  return { ctx, page, width, height };
}

async function go(v, url, ready) {
  await v.page.goto(BASE + url, { waitUntil: "domcontentloaded" });
  try { await v.page.waitForSelector(ready, { timeout: 45000 }); } catch { notes.push(`${url} at ${v.width}: ${ready} did not appear in 45 s`); }
  await v.page.evaluate(() => document.fonts.ready);
  await v.page.waitForTimeout(1500);
}

/** the union of the elements' boxes, in the viewport's coordinates */
async function box(page, sels) {
  const rs = [];
  for (const s of sels) {
    const r = await page.evaluate((sel) => {
      const el = document.querySelector(sel);
      if (!el) return null;
      const b = el.getBoundingClientRect();
      return { x: b.left, y: b.top, r: b.right, b: b.bottom };
    }, s);
    if (r) rs.push(r); else notes.push(`no element for ${s}`);
  }
  if (!rs.length) return null;
  return { x: Math.min(...rs.map((r) => r.x)), y: Math.min(...rs.map((r) => r.y)), r: Math.max(...rs.map((r) => r.r)), b: Math.max(...rs.map((r) => r.b)) };
}

/** scrolls an element near the window's top */
async function reveal(page, sel, above = 40) {
  await page.evaluate(({ sel, above }) => { const h = document.querySelector(sel); if (h) { h.scrollIntoView({ block: "start" }); scrollBy(0, -above); } }, { sel, above });
  await page.waitForTimeout(300);
}

/** the elements' area with room around it, from the window as it is */
async function crop(v, file, sels, pad = 24) {
  const b = await box(v.page, sels);
  if (!b) { notes.push(`${file}: nothing to crop`); return; }
  const x = Math.max(0, Math.floor(b.x - pad)), y = Math.max(0, Math.floor(b.y - pad));
  const width = Math.min(v.width - x, Math.ceil(b.r + pad) - x), height = Math.min(v.height - y, Math.ceil(b.b + 16) - y);
  await v.page.screenshot({ path: path.join(out, file), animations: "disabled", clip: { x, y, width, height } });
}

/**
 * the browser's own tooltip for an element's title: a headless browser paints no native tooltip, so the shot draws it
 * in the platform's plain style, beside the hovered element inside its own row (right), level with the row, so it never
 * stands over another row. What it covers is written to the notes.
 */
async function tip(v, sel, theme) {
  const el = v.page.locator(sel).first();
  if (!(await el.count())) { notes.push(`tip: no element for ${sel}`); return; }
  await el.hover();
  await v.page.waitForTimeout(250);
  const covered = await v.page.evaluate(({ sel, dark }) => {
    const e = document.querySelector(sel);
    const t = e && (e.getAttribute("title") || e.closest("[title]")?.getAttribute("title"));
    if (!t) return ["no title"];
    const r = e.getBoundingClientRect();
    const tr = e.closest("tr");
    const band = (tr || e).getBoundingClientRect();
    const d = document.createElement("div");
    d.className = "shot-tip";
    d.textContent = t;
    Object.assign(d.style, {
      position: "fixed", left: "0px", top: "0px", zIndex: 99,
      maxWidth: "480px", padding: "4px 8px", font: "12px/1.35 system-ui, -apple-system, 'Segoe UI', sans-serif",
      color: dark ? "#f2f2f2" : "#1d1d1d", background: dark ? "#3b3b3b" : "#ffffff", border: `1px solid ${dark ? "#5c5c5c" : "#a0a0a0"}`,
      borderRadius: "3px", boxShadow: "0 2px 6px rgba(0,0,0,.22)", whiteSpace: "nowrap", pointerEvents: "none",
    });
    document.body.appendChild(d);
    const h = d.offsetHeight;
    d.style.left = `${Math.round(r.right + 10)}px`;
    d.style.top = `${Math.round(band.top + (band.height - h) / 2)}px`;
    const tb = d.getBoundingClientRect();
    const o = [`title "${t}"`];
    if (tb.top < band.top || tb.bottom > band.bottom) o.push(`OUT OF ITS ROW (${Math.round(tb.top)}–${Math.round(tb.bottom)} in ${Math.round(band.top)}–${Math.round(band.bottom)})`);
    if (tb.right > innerWidth || tb.left < 0) o.push("OFF THE WINDOW");
    return o;
  }, { sel, dark: theme === "dark" });
  notes.push(`${theme} tip on ${sel}: ${covered.join(", ")}`);
}
const untip = async (v) => { await v.page.evaluate(() => document.querySelectorAll(".shot-tip").forEach((d) => d.remove())); await v.page.mouse.move(5, 5); await v.page.waitForTimeout(200); };

// ---------------------------------------------------------------- measuring
/** every head and cell of the list, and every clipping box in a cell: scrollWidth over clientWidth is clipped text */
async function clips(v, label) {
  const r = await v.page.evaluate(() => {
    const t = document.querySelector("#list .lg-t");
    if (!t) return { bad: ["no table"], n: 0 };
    const o = [];
    let n = 0;
    const cls = (el) => (typeof el.className === "string" && el.className ? el.className.split(" ")[0] : el.tagName.toLowerCase());
    const check = (el, where) => { n++; if (el.clientWidth > 0 && el.scrollWidth > el.clientWidth) o.push(`${where}: ${el.scrollWidth} > ${el.clientWidth} ("${el.textContent.trim().slice(0, 40)}")`); };
    t.querySelectorAll("thead th").forEach((th) => { if (getComputedStyle(th).display !== "none") check(th, `head ${cls(th)}`); });
    [...t.querySelectorAll("tbody tr.row")].forEach((tr, i) => {
      [...tr.children].forEach((td) => {
        if (getComputedStyle(td).display === "none") return;
        const where = `row ${i + 1}${tr.classList.contains("xf") ? " (failed)" : ""} ${cls(td)}`;
        check(td, where);
        td.querySelectorAll("*").forEach((el) => { const s = getComputedStyle(el); if (s.overflowX !== "visible") check(el, `${where} ${cls(el)}`); });
      });
    });
    // the pager's count, whole on its line
    const count = document.querySelector("#list .pager .count");
    if (count) check(count, "pager count");
    return { bad: o, n };
  });
  r.bad.forEach((b) => notes.push(`CLIP ${label} ${b}`));
  if (!r.bad.length) notes.push(`${label}: every head and cell whole (${r.n} boxes measured)`);
}

/** sideways scroll: the page wider than its window */
async function sideways(v, label) {
  const r = await v.page.evaluate(() => ({ doc: document.documentElement.scrollWidth, body: document.body.scrollWidth, win: innerWidth }));
  notes.push(`${label}: ${r.doc > r.win || r.body > r.win ? "SCROLLS SIDEWAYS" : "no sideways scroll"} (document ${r.doc}, body ${r.body}, window ${r.win})`);
}

/** the columns' widths, and where the status words and dots stand against their head and each other */
async function layout(v, label) {
  const r = await v.page.evaluate(() => {
    const t = document.querySelector("#list .lg-t");
    const ths = [...t.querySelectorAll("thead th")];
    const widths = ths.map((th) => `${th.className.split(" ")[0]} ${Math.round(th.getBoundingClientRect().width * 10) / 10}`).join(" · ");
    const L = (el) => Math.round(el.getBoundingClientRect().left * 10) / 10;
    const textLeft = (el) => { for (const n of el.childNodes) if (n.nodeType === 3 && n.nodeValue.trim()) { const g = document.createRange(); g.selectNodeContents(n); return Math.round(g.getBoundingClientRect().left * 10) / 10; } return null; };
    const st = t.querySelector("thead th.c-st");
    const head = st ? (() => { const g = document.createRange(); g.selectNodeContents(st); return Math.round(g.getBoundingClientRect().left * 10) / 10; })() : null;
    const dots = [...t.querySelectorAll("tbody td.c-st .dot")].map(L);
    const words = [...t.querySelectorAll("tbody td.c-st .st")].map(textLeft);
    let room = null;
    const cell = t.querySelector("tbody tr.row:not(.xf) td.c-st");
    if (cell) {
      const s = getComputedStyle(cell), cb = cell.getBoundingClientRect();
      const pr = parseFloat(s.paddingRight);
      const right = Math.max(...[...t.querySelectorAll("tbody td.c-st .st")].map((el) => { const tn = [...el.childNodes].find((n) => n.nodeType === 3 && n.nodeValue.trim()); const g = document.createRange(); g.selectNodeContents(tn || el); return g.getBoundingClientRect().right; }));
      const dot = cell.querySelector(".dot");
      room = { w: cb.width, right: (cb.right - pr - right).toFixed(1), dot: dot ? (dot.getBoundingClientRect().left - cb.left).toFixed(1) : "-" };
    }
    const meter = t.querySelector("tbody .em");
    return { widths, table: Math.round(t.getBoundingClientRect().width), head, dots: [...new Set(dots)], words: [...new Set(words)], room, meter: meter ? meter.getBoundingClientRect().width : null };
  });
  notes.push(`${label} table ${r.table}px, columns: ${r.widths}`);
  if (r.head != null) notes.push(`${label} status: head's word left ${r.head}; words' left ${r.words.join(", ") || "-"}; dots' left ${r.dots.join(", ") || "-"}; meter ${r.meter}px`);
  if (r.room) notes.push(`${label} status cell ${r.room.w}px: room after the widest word ${r.room.right}px, the dot ${r.room.dot}px from the cell's left edge`);
}

/** the failed row's cells as the reader sees them, left to right */
async function failedRow(v, label) {
  const r = await v.page.evaluate(() => {
    const tr = document.querySelector("#list .lg-t tr.xf");
    if (!tr) return null;
    return [...tr.children].filter((td) => getComputedStyle(td).display !== "none").map((td) => `${td.className.split(" ")[0]}="${td.textContent.trim().replace(/\s+/g, " ")}"`).join(" ");
  });
  notes.push(`${label} failed row: ${r ?? "NONE"}`);
}

// ---------------------------------------------------------------- the shots
const rowSel = (n) => `#list .lg-t tbody tr:nth-child(${n})`;
const LIST = "#list .lg-t tbody tr.xf";

(async () => {
  fs.mkdirSync(out, { recursive: true });
  await new Promise((r) => server.listen(PORT, "127.0.0.1", r));
  const browser = await chromium.launch({ executablePath: process.env.CHROME || "/usr/bin/google-chrome", args: ["--hide-scrollbars"] });
  let publisher = null;
  for (const theme of ["dark", "light"]) {
    // the page's width: the list's head and six rows, the hover, the pager
    const v = await open(browser, theme, 1440, 1100);
    await go(v, "/blobs/", LIST);
    await reveal(v.page, "#list .list-head");
    await clips(v, `1440 ${theme}`);
    await sideways(v, `1440 ${theme}`);
    if (theme === "dark") { await layout(v, "1440"); await failedRow(v, "1440"); }
    await crop(v, `a-1440-${theme}.png`, ["#list .list-head", rowSel(6)]);
    if (theme === "dark") {
      await tip(v, "#list .lg-t tr.xf td.c-st .st.f", theme);
      await crop(v, `a-1440-hover-${theme}.png`, ["#list .list-head", rowSel(6), ".shot-tip"]);
      await untip(v);
      await reveal(v.page, "#list .pager", 260);
      await crop(v, `pager-1440-${theme}.png`, [rowSel(23), "#list .pager"]);
      publisher = await v.page.evaluate(() => document.querySelector("#list .lg-t tr.row:not(.xf) td.c-p .lg-who")?.getAttribute("href")?.split("addr=")[1] ?? null);
    }
    await v.ctx.close();
  }
  // the bands below the page's width, dark: the publisher's chip without its prefix (1180), the time without its age
  // (1100), the narrowest table (1081)
  for (const w of [1180, 1100, 1081]) {
    const v = await open(browser, "dark", w, 1000);
    await go(v, "/blobs/", LIST);
    await reveal(v.page, "#list .list-head");
    await clips(v, `${w} dark`);
    await sideways(v, `${w} dark`);
    await layout(v, `${w}`);
    await failedRow(v, `${w}`);
    if (w !== 1081) await crop(v, `a-${w}-dark.png`, ["#list .list-head", rowSel(6)]);
    await v.ctx.close();
  }
  // the phone, both themes: the list's head and six cards, the pager
  for (const theme of ["dark", "light"]) {
    const v = await open(browser, theme, 375, 812);
    await go(v, "/blobs/", LIST);
    await reveal(v.page, "#list .list-head", 16);
    await clips(v, `375 ${theme}`);
    await sideways(v, `375 ${theme}`);
    if (theme === "dark") await failedRow(v, "375");
    await crop(v, `a-375-${theme}.png`, ["#list .list-head", rowSel(6)], 16);
    if (theme === "dark") {
      await reveal(v.page, "#list .pager", 300);
      await crop(v, `pager-375-${theme}.png`, [rowSel(24), "#list .pager"], 16);
    }
    await v.ctx.close();
  }
  // one publisher's page keeps its table: no Status column, today's gutters and meter
  if (publisher) {
    const v = await open(browser, "dark", 1440, 1100);
    await go(v, `/publisher/?addr=${publisher}`, "#list .lg-t tbody tr.row:not(.sk)");
    await reveal(v.page, "#list");
    const cols = await v.page.evaluate(() => [...document.querySelectorAll("#list .lg-t thead th")].map((th) => `${th.className.split(" ")[0]} ${Math.round(th.getBoundingClientRect().width)}`).join(" · "));
    const st = await v.page.evaluate(() => !!document.querySelector("#list .lg-t th.c-st, #list .lg-t td.c-st, #list .lg-t.lg-st"));
    notes.push(`publisher page columns: ${cols}; Status column ${st ? "PRESENT" : "absent"}`);
    await clips(v, "publisher 1440 dark");
    await crop(v, "publisher-1440-dark.png", ["#list .lg-t thead", rowSel(6)]);
    await v.ctx.close();
  } else notes.push("no publisher found for the publisher page");
  await browser.close();
  server.close();
  fs.writeFileSync(path.join(out, "notes.txt"), notes.join("\n") + "\n");
  console.log(notes.join("\n"));
})().catch((e) => { console.error(e); process.exit(1); });
