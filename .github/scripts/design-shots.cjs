// design-shots.cjs <out dir> <shots dir>: serves the static site from <out dir>, passes /api/* through to the live API,
// and photographs the lower tables of one publisher's page, which show its successful AND failed Fibre transactions, in
// three alternatives (?pv=1|2|3), at 1440 in both themes: each of the All, Blobs and Escrow tabs from the tabs down
// through ~8 rows, the hover on a Failed status, and the page's top once (unchanged). The live API has no failed rows
// for this page and no route for an account's transactions yet: the browser's reads of the proposed route
// /v1/publishers/{addr}/txs and its first page of /v1/blobs?publisher= are answered here, from the live answers and the
// rows marked STUB below. Everything else is the live answer. Every head and cell is measured for clipping, the heads
// against their cells and the status marks against each other: notes.txt. Runs only in the Design shots workflow, on a
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

/** one live answer, as JSON */
async function live(p) {
  const r = await fetch(`https://${LIVE}/api${p}`, { headers: { "user-agent": "tensile-design-shots" } });
  if (!r.ok) throw new Error(`${p}: ${r.status}`);
  return r.json();
}

const notes = [];

// ---------------------------------------------------------------- the rows
const PUB = "celestia1jw8afsj3j0c23fxs09nu8pq5asxwes5e3kkxdx";
const TIA = 1000000;

// STUB (for this page: the API serves no failed row per account yet). The two failed escrow transactions are this
// account's real failed records on chain (hash, height, time, reason), as the ftx shots used them: a deposit that ran
// out of gas and a withdrawal request of more than the escrow held. The failed blob payment is invented (the ftx shots'
// F_PFF), between the blobs at #1,497,140 and #1,497,108; its namespace is its newer neighbour's.
const F_DEPOSIT = { kind: "deposit", status: "failed", height: 1509598, tx_index: 0, msg_index: 0, time: "2026-10-08T23:17:45.331354784Z", tx_hash: "f5ac69d972e0d707e6f535e92912c8ee6a6ddbf8c56ce920c6751764cbb1581b", reason: "Out of gas" };
const F_WITHDRAWAL = { kind: "withdrawal_request", status: "failed", height: 1509587, tx_index: 0, msg_index: 0, time: "2026-10-08T23:17:14.030516463Z", tx_hash: "a4bd0b1855044d09c2f84ae43f6504d9365a470810b6f67d30a6ec11d2afdb92", reason: "Insufficient funds" };
const F_PFF = { kind: "settlement", status: "failed", height: 1497121, tx_index: 1, msg_index: 0, time: "2026-10-08T13:25:05.700Z", tx_hash: "9b3e7a41d0c25f86e4a7b1d9c03f5e2a8d6b4c1f7e9a0d3b5c8f2e6a1d4b7c90", reason: "Invalid request" };
// the account's two real deposits (its whole escrow history on chain, with its blobs' fees)
const DEP_1 = { kind: "deposit", status: "success", height: 1366494, tx_index: 0, msg_index: 0, time: "2026-10-04T05:51:46.491278Z", tx_hash: "7f7e66d16ec7337c82ffc65148bf3e42bef39b2adef3f930862455494ddc3d89", amount_utia: 1000 * TIA };
const DEP_2 = { kind: "deposit", status: "success", height: 1107337, tx_index: 0, msg_index: 0, time: "2026-09-25T16:10:56.562358Z", tx_hash: "b241a96f9f221d4e9f03205ada95a5587b3f702c5345ad7b16c3c62da16d8f4c", amount_utia: 4 * TIA };
// STUB: two successful withdrawal requests, so both payout states show: one paid out (a payout on record, a day after
// it: the chain's 24 h withdrawal delay), one still waiting; and a deposit of the paid one's amount before it, so the
// statement still closes on the live balance. Hashes invented; heights from the live blobs' times (heightAt).
const D_STUB = { kind: "deposit", status: "success", tx_index: 0, msg_index: 0, time: "2026-10-05T09:14:27.512Z", tx_hash: "3e8a1c5f907b2d64a1f3e9c0b7d5a2e8c4f6b1d93a7e0c2f5b8d1a4e6c9f0b27", amount_utia: 25 * TIA };
const W_PAID = { kind: "withdrawal_request", status: "success", tx_index: 0, msg_index: 0, time: "2026-10-06T16:02:11.274Z", tx_hash: "d07c4e9a2b5f8136e0a4c7d2f9b3e6a1c5d8f0b4e7a2c9d6f1b3e5a8c0d4f7e2", amount_utia: 25 * TIA,
  payout: { state: "paid", available_at: "2026-10-07T16:02:11.274Z", paid_at: "2026-10-07T16:02:13.981Z", paid_utia: 25 * TIA, payout_delay_s: 86403 } };
const W_PENDING = { kind: "withdrawal_request", status: "success", tx_index: 0, msg_index: 0, time: "2026-10-09T09:47:20.118Z", tx_hash: "6a2f9d4c1e7b3a05f8c2d6e9b1a4f7c3e0d5b8a2f6c9e1d4b7a3f0c6e2d9b5a8", amount_utia: 50 * TIA,
  payout: { state: "pending", available_at: "2026-10-10T09:47:20.118Z" } };
for (const t of [F_DEPOSIT, F_WITHDRAWAL, F_PFF, DEP_1, DEP_2, D_STUB, W_PAID, W_PENDING]) if (!/^[0-9a-f]{64}$/.test(t.tx_hash)) throw new Error(`not a hash: ${t.tx_hash}`);

/** every transaction of the account, newest first (view all), its escrow's alone (view escrow), and the escrow's sums */
let ALL = [], ESCROW = [], SUMS = null, PFF_BLOB = null;
const newestFirst = (a, b) => b.height - a.height || b.tx_index - a.tx_index || b.msg_index - a.msg_index;

async function prepare() {
  const blobs = [];
  for (let o = 0; o < 2000; o += 100) {
    const j = await live(`/v1/blobs?publisher=${PUB}&limit=100&offset=${o}`);
    blobs.push(...j.blobs);
    if (blobs.length >= j.total || j.blobs.length === 0) break;
  }
  // a height for a time: between the two live anchors around it, or past the newest at the rate of the last day
  const anchors = [...blobs.map((b) => ({ h: b.settlement_height, t: Date.parse(b.settlement_time) })), { h: F_DEPOSIT.height, t: Date.parse(F_DEPOSIT.time) }]
    .sort((a, b) => a.t - b.t);
  const heightAt = (iso) => {
    const t = Date.parse(iso);
    let i = anchors.findIndex((a) => a.t >= t);
    if (i === 0) return anchors[0].h;
    if (i < 0) {
      const a = anchors[anchors.length - 1], b = anchors.find((x) => a.t - x.t < 2 * 86400000) || anchors[0];
      return Math.round(a.h + ((t - a.t) * (a.h - b.h)) / (a.t - b.t));
    }
    const a = anchors[i - 1], b = anchors[i];
    return Math.round(a.h + ((t - a.t) * (b.h - a.h)) / (b.t - a.t || 1));
  };
  for (const t of [D_STUB, W_PAID, W_PENDING]) t.height = heightAt(t.time);
  W_PAID.payout.paid_height = heightAt(W_PAID.payout.paid_at);
  notes.push(`stub heights: deposit #${D_STUB.height}, paid request #${W_PAID.height} (paid out #${W_PAID.payout.paid_height}), pending request #${W_PENDING.height}`);

  const settled = blobs.map((b) => ({ kind: "settlement", status: "success", height: b.settlement_height, tx_index: b.settlement_tx_index ?? 0, msg_index: 0, time: b.settlement_time,
    tx_hash: b.settlement_tx_hash, promise_hash: b.promise_hash, amount_utia: b.charge ? b.charge.fee_utia : 0 }));
  ALL = [F_DEPOSIT, F_WITHDRAWAL, F_PFF, DEP_1, DEP_2, D_STUB, W_PAID, W_PENDING, ...settled].sort(newestFirst);
  ESCROW = ALL.filter((t) => t.kind !== "settlement");
  const ok = (k) => ESCROW.filter((t) => t.status === "success" && t.kind === k);
  SUMS = {
    deposited_utia: ok("deposit").reduce((s, t) => s + t.amount_utia, 0),
    withdrawn_utia: ok("withdrawal_request").filter((t) => t.payout && t.payout.state === "paid").reduce((s, t) => s + t.payout.paid_utia, 0),
    charged_utia: 0,
  };
  notes.push(`All: ${ALL.length} rows (${settled.length} live blob payments, ${ESCROW.length} escrow transactions, ${ALL.filter((t) => t.status === "failed").length} failed); Escrow: ${ESCROW.length}`);

  // the failed blob payment as /v1/blobs would carry it (Ledger's FailedRow): no blob field
  const newer = blobs.filter((b) => b.settlement_height > F_PFF.height).sort((a, b) => a.settlement_height - b.settlement_height)[0] || blobs[0];
  PFF_BLOB = { status: "failed", reason: F_PFF.reason, settlement_tx_hash: F_PFF.tx_hash, settlement_height: F_PFF.height, settlement_tx_index: F_PFF.tx_index, settlement_time: F_PFF.time,
    namespace: newer.namespace, publisher: PUB, signer: PUB };

  try {
    const p = await live(`/v1/publishers/${PUB}?window=all`);
    const all = p.windows.find((w) => w.window.name === "all");
    const bal = p.publisher.escrow && p.publisher.escrow.balance_utia;
    const left = SUMS.deposited_utia - all.fees_utia - SUMS.charged_utia - SUMS.withdrawn_utia;
    notes.push(left === bal ? `Escrow's statement closes on the live balance (${bal} utia)` : `Escrow's statement does NOT close (${left} vs the live ${bal} utia): no foot in the shots`);
    notes.push(`the live top: settlements ${p.publisher.settlements}, fees ${all.fees_utia} utia, escrow available ${p.publisher.escrow && p.publisher.escrow.available_utia} utia (the STUB pending request is not in it)`);
  } catch (e) { notes.push(`publisher not read: ${e.message}`); }
}

/** the proposed route's answer, from the rows; null for every other path (read live) */
function own(pathname, q) {
  if (pathname !== `/api/v1/publishers/${PUB}/txs`) return null;
  const view = q.get("view") === "escrow" ? "escrow" : "all";
  const limit = Math.min(100, Number(q.get("limit")) || 25), offset = Number(q.get("offset")) || 0;
  const list = view === "escrow" ? ESCROW : ALL;
  const body = { publisher: PUB, view, limit, offset, total: list.length, failed: list.filter((t) => t.status === "failed").length, txs: list.slice(offset, offset + limit) };
  if (view === "escrow") body.sums = SUMS;
  return body;
}

/** the account's first page of blobs with the failed blob payment in its place by height; any other read as it came */
function withFailed(j, q) {
  if (!j || !Array.isArray(j.blobs) || q.get("publisher") !== PUB || (Number(q.get("offset")) || 0) !== 0) return j;
  const ns = q.get("namespace");
  if (ns && ns !== PFF_BLOB.namespace) return j;
  const rows = j.blobs.filter((r) => r.settlement_tx_hash !== F_PFF.tx_hash);
  let i = rows.findIndex((r) => r.settlement_height <= F_PFF.height);
  if (i < 0) i = rows.length;
  rows.splice(i, 0, PFF_BLOB);
  const limit = Number(q.get("limit")) || rows.length;
  return { ...j, blobs: rows.slice(0, limit), total: (Number(j.total) || 0) + 1 };
}

// ---------------------------------------------------------------- photographing
const W = 1440, H = 1200;

async function open(browser, theme) {
  const ctx = await browser.newContext({ viewport: { width: W, height: H }, deviceScaleFactor: 2, colorScheme: theme });
  await ctx.addInitScript((t) => { try { localStorage.setItem("theme", t); } catch {} }, theme);
  await ctx.route("**/api/v1/**", async (route) => {
    const u = new URL(route.request().url());
    const o = own(u.pathname, u.searchParams);
    if (o) return route.fulfill({ status: 200, contentType: "application/json", headers: { "access-control-allow-origin": "*" }, body: JSON.stringify(o) });
    if (u.pathname !== "/api/v1/blobs") return route.continue();
    let res;
    try { res = await route.fetch(); } catch { return route.abort(); }
    const type = res.headers()["content-type"] || "";
    if (!res.ok() || !type.includes("json")) return route.fulfill({ response: res });
    let j;
    try { j = await res.json(); } catch { return route.fulfill({ response: res }); }
    return route.fulfill({ response: res, json: withFailed(j, u.searchParams) });
  });
  const page = await ctx.newPage();
  page.on("pageerror", (e) => notes.push(`${theme} page error: ${e.message}`));
  return { ctx, page };
}

async function go(page, url, ready) {
  await page.goto(BASE + url, { waitUntil: "domcontentloaded" });
  try { await page.waitForSelector(ready, { timeout: 45000 }); } catch { notes.push(`${url}: ${ready} did not appear in 45 s`); }
  await page.evaluate(() => document.fonts.ready);
  await page.waitForTimeout(1500);
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

/** scrolls the element's top near the window's top */
async function reveal(page, sel) {
  await page.evaluate((s) => { const h = document.querySelector(s); if (h) { h.scrollIntoView({ block: "start" }); scrollBy(0, -40); } }, sel);
  await page.waitForTimeout(300);
}

/** the elements' area with room around it, from the window as it is */
async function crop(page, file, sels, pad = 24) {
  const b = await box(page, sels);
  if (!b) { notes.push(`${file}: nothing to crop`); return; }
  const x = Math.max(0, Math.floor(b.x - pad)), y = Math.max(0, Math.floor(b.y - pad));
  const width = Math.min(W - x, Math.ceil(b.r + pad) - x), height = Math.min(H - y, Math.ceil(b.b + 16) - y);
  if (b.b + 16 > H) notes.push(`${file}: taller than the window, cut at its foot`);
  await page.screenshot({ path: path.join(out, file), animations: "disabled", clip: { x, y, width, height } });
}

/**
 * the browser's own tooltip for an element's title: a headless browser paints no native tooltip, so the shot draws it
 * in the platform's plain style, level with the hovered element's row and inside it: to its right when it fits in the
 * window, else to its left. What it covers is written to the notes, row by row.
 */
async function tip(page, sel, theme) {
  const el = page.locator(sel).first();
  if (!(await el.count())) { notes.push(`tip: no element for ${sel}`); return; }
  await el.hover();
  await page.waitForTimeout(250);
  const covered = await page.evaluate(({ sel, dark }) => {
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
    const w = d.offsetWidth, h = d.offsetHeight;
    const left = r.right + 10 + w <= innerWidth - 8 ? r.right + 10 : r.left - 10 - w;
    d.style.left = `${Math.round(left)}px`;
    d.style.top = `${Math.round(band.top + (band.height - h) / 2)}px`;
    const tb = d.getBoundingClientRect();
    const o = [];
    if (tb.top < band.top || tb.bottom > band.bottom) o.push(`OUT OF ITS ROW (${Math.round(tb.top)}–${Math.round(tb.bottom)} in ${Math.round(band.top)}–${Math.round(band.bottom)})`);
    const walk = document.createTreeWalker(document.body, NodeFilter.SHOW_TEXT);
    for (let n = walk.nextNode(); n; n = walk.nextNode()) {
      const p = n.parentElement;
      if (!n.nodeValue.trim() || !p || d.contains(n) || getComputedStyle(p).visibility === "hidden") continue;
      const rg = document.createRange();
      rg.selectNodeContents(n);
      if (![...rg.getClientRects()].some((q) => q.right > tb.left + 1 && q.left < tb.right - 1 && q.bottom > tb.top + 1 && q.top < tb.bottom - 1)) continue;
      o.push(`${tr && tr.contains(n) ? "its own row" : p.closest("thead") ? "the head" : "ANOTHER ROW"} "${n.nodeValue.trim().slice(0, 40)}"`);
    }
    if (tb.right > innerWidth || tb.left < 0) o.push("OFF THE WINDOW");
    return [`"${t}"`, ...o];
  }, { sel, dark: theme === "dark" });
  notes.push(`${theme} tip on ${sel}: ${covered.join(", ")}`);
}
const untip = async (page) => { await page.evaluate(() => document.querySelectorAll(".shot-tip").forEach((d) => d.remove())); await page.mouse.move(5, 5); await page.waitForTimeout(200); };

// ---------------------------------------------------------------- measuring
/** every head and cell of the list (and the list's head), and every clipping box in them: scrollWidth over clientWidth is clipped text */
async function clips(page, label) {
  const bad = await page.evaluate(() => {
    const o = [];
    const cls = (el) => (typeof el.className === "string" && el.className ? el.className.split(" ")[0] : el.tagName.toLowerCase());
    const check = (el, where) => { if (el.clientWidth > 0 && el.scrollWidth > el.clientWidth) o.push(`${where}: ${el.scrollWidth} > ${el.clientWidth} ("${el.textContent.trim().slice(0, 30)}")`); };
    const inside = (root, where) => root.querySelectorAll("*").forEach((el) => { const s = getComputedStyle(el); if (s.display !== "none" && s.overflowX !== "visible") check(el, `${where} ${cls(el)}`); });
    const t = document.querySelector("#list table");
    if (!t) return ["no table"];
    t.querySelectorAll("thead th").forEach((th) => { check(th, `head ${cls(th)}`); inside(th, `head ${cls(th)}`); });
    [...t.querySelectorAll("tbody tr.row, tfoot tr")].forEach((tr, r) => {
      [...tr.children].forEach((td) => {
        if (getComputedStyle(td).display === "none") return;
        const where = `${tr.closest("tfoot") ? "foot" : "row"} ${r + 1}${tr.classList.contains("xf") ? " (failed)" : ""} ${cls(td)}`;
        check(td, where);
        inside(td, where);
      });
    });
    const lh = document.querySelector("#list .list-head");
    if (lh) { inside(lh, "list head"); lh.querySelectorAll("button, h2").forEach((b) => check(b, `list head ${cls(b)} "${b.textContent.trim()}"`)); }
    const pg = document.querySelector("#list .pager");
    if (pg) inside(pg, "pager");
    return o;
  });
  bad.forEach((b) => notes.push(`CLIP ${label} ${b}`));
  if (!bad.length) notes.push(`${label}: every head and cell whole`);
}

/**
 * the table as drawn: its columns' widths; each column's head words against its cells' words by the column's own
 * alignment (left edges, centres or right edges; the largest gap); the status marks (dots' and words' left edges, which
 * must be one each); the amounts' right edges (one: the digits stack); every failed row's amount; the rows' heights
 */
async function layout(page, label) {
  const r = await page.evaluate(() => {
    const t = document.querySelector("#list table");
    const R1 = (v) => Math.round(v * 10) / 10;
    const words = (el) => {
      const w = document.createTreeWalker(el, NodeFilter.SHOW_TEXT);
      let L = Infinity, Rr = -Infinity;
      for (let n = w.nextNode(); n; n = w.nextNode()) {
        if (!n.nodeValue.trim()) continue;
        const g = document.createRange();
        g.selectNodeContents(n);
        for (const q of g.getClientRects()) if (q.width > 0) { L = Math.min(L, q.left); Rr = Math.max(Rr, q.right); }
      }
      return isFinite(L) ? { l: L, r: Rr, c: (L + Rr) / 2 } : null;
    };
    const ths = [...t.querySelectorAll("thead th")];
    const rows = [...t.querySelectorAll("tbody tr.row")];
    const cols = ths.map((th, i) => {
      const name = th.className.split(" ")[0] || `#${i}`;
      const hw = words(th);
      const align = rows[0] ? getComputedStyle(rows[0].children[i]).textAlign : "";
      let gap = 0;
      if (hw && !["gap", "tn"].includes(name)) for (const tr of rows) {
        const td = tr.children[i];
        if (!td || getComputedStyle(td).display === "none") continue;
        const cw = words(td);
        if (!cw) continue;
        const d = align === "center" ? cw.c - hw.c : align === "right" || align === "end" ? cw.r - hw.r : cw.l - hw.l;
        if (Math.abs(d) > Math.abs(gap)) gap = d;
      }
      return `${name} ${R1(th.getBoundingClientRect().width)} ${align || "-"}${hw ? ` Δ${R1(gap)}` : ""}`;
    });
    const L = (el) => R1(el.getBoundingClientRect().left);
    const textLeft = (el) => { for (const n of el.childNodes) if (n.nodeType === 3 && n.nodeValue.trim()) { const g = document.createRange(); g.selectNodeContents(n); return R1(g.getBoundingClientRect().left); } return null; };
    const dots = [...new Set([...t.querySelectorAll("tbody td.c-st .dot")].map(L))];
    const stWords = [...new Set([...t.querySelectorAll("tbody td.c-st .st")].map(textLeft))];
    const st = t.querySelector("thead th.c-st");
    const stHead = st ? words(st) : null;
    const amSel = t.classList.contains("ptx-t") ? "td.c-am" : "td.c-fee";
    const amRight = [...new Set([...t.querySelectorAll(`tbody ${amSel}, tfoot ${amSel}`)].map((td) => words(td)).filter(Boolean).map((w) => R1(w.r)))];
    const failed = [...t.querySelectorAll("tbody tr.xf")].map((tr) => (tr.querySelector(amSel) || {}).textContent);
    const heights = [...new Set(rows.map((tr) => R1(tr.getBoundingClientRect().height)))];
    return { table: R1(t.getBoundingClientRect().width), cls: t.className, cols, dots, stWords, stHead: stHead ? `${R1(stHead.l)}–${R1(stHead.r)}` : "-", amRight, failed, heights };
  });
  notes.push(`--- ${label}: table ${r.table}px (${r.cls})`);
  notes.push(`${label} columns (width, alignment, largest gap of the cells' words from the head's): ${r.cols.join(" · ")}`);
  notes.push(`${label} status: head words ${r.stHead}; dots' left ${r.dots.join(", ") || "-"}; words' left ${r.stWords.join(", ") || "-"}${r.dots.length > 1 || r.stWords.length > 1 ? "  NOT ONE LINE" : ""}`);
  notes.push(`${label} amounts' right edges: ${r.amRight.join(", ") || "-"}${r.amRight.length > 1 ? "  NOT STACKED" : ""}`);
  notes.push(`${label} failed rows' amounts: ${r.failed.map((s) => JSON.stringify(s)).join(", ") || "-"}${r.failed.some((s) => s !== "—") ? "  A FAILED ROW SHOWS AN AMOUNT" : ""}`);
  notes.push(`${label} rows' heights: ${r.heights.join(", ")}`);
}

/** the top's words, for the check that the design leaves it as today's page draws it */
const topText = (page) => page.evaluate(() => [".pb-mast", ".pbd"].map((s) => document.querySelector(s)?.innerText.replace(/\b\d+ (d|h|min|s)\b/g, "")).join("\n"));

// ---------------------------------------------------------------- the shots
const rowSel = (n) => `#list table tbody tr:nth-child(${n})`;
const URL0 = `/publisher/?addr=${PUB}`;

(async () => {
  fs.mkdirSync(out, { recursive: true });
  await prepare();
  await new Promise((r) => server.listen(PORT, "127.0.0.1", r));
  const browser = await chromium.launch({ executablePath: process.env.CHROME || "/usr/bin/google-chrome", args: ["--hide-scrollbars"] });
  for (const theme of ["dark", "light"]) {
    const { ctx, page } = await open(browser, theme);
    // today's page: its top, for the check below
    await go(page, URL0, ".pbd .pbd-v");
    const top0 = await topText(page);
    for (const pv of [1, 2, 3]) {
      // All
      await go(page, `${URL0}&pv=${pv}`, "#list .ptx-t tbody tr.row");
      const top = await topText(page);
      notes.push(`p${pv} ${theme}: the top ${top === top0 ? "is today's, word for word" : "DIFFERS from today's"}`);
      if (pv === 1) {
        await page.evaluate(() => scrollTo(0, 0));
        await page.waitForTimeout(200);
        await crop(page, `p-top-${theme}.png`, [".pb-mast", ".pbd", "#list .list-head"]);
      }
      await reveal(page, "#list .list-head");
      await clips(page, `p${pv} all ${theme}`);
      if (theme === "dark") await layout(page, `p${pv} all`);
      await crop(page, `p${pv}-all-${theme}.png`, ["#list .list-head", rowSel(8)]);
      await tip(page, "#list .ptx-t tr.xf td.c-st .st.f", theme);
      await crop(page, `p${pv}-hover-${theme}.png`, ["#list .list-head", rowSel(8), ".shot-tip"]);
      await untip(page);
      // Blobs
      await go(page, `${URL0}&pv=${pv}&kind=blobs`, "#list .lg-t tbody tr.xf");
      await reveal(page, "#list .list-head");
      await clips(page, `p${pv} blobs ${theme}`);
      if (theme === "dark") await layout(page, `p${pv} blobs`);
      await crop(page, `p${pv}-blobs-${theme}.png`, ["#list .list-head", rowSel(8)]);
      // Escrow
      await go(page, `${URL0}&pv=${pv}&kind=escrow`, "#list .ptx-t tbody tr.row");
      await page.waitForSelector("#list .ptx-t tfoot tr.tot", { timeout: 10000 }).catch(() => notes.push(`p${pv} ${theme} escrow: no statement lines`));
      await reveal(page, "#list .list-head");
      await clips(page, `p${pv} escrow ${theme}`);
      if (theme === "dark") await layout(page, `p${pv} escrow`);
      await crop(page, `p${pv}-escrow-${theme}.png`, ["#list .list-head", "#list .lg-tw", "#list .pager"]);
    }
    await ctx.close();
  }
  await browser.close();
  server.close();
  fs.writeFileSync(path.join(out, "notes.txt"), notes.join("\n") + "\n");
  console.log(fs.readdirSync(out).join("\n"));
  console.log(notes.join("\n"));
})().catch((e) => { console.error(e); try { fs.mkdirSync(out, { recursive: true }); fs.writeFileSync(path.join(out, "notes.txt"), notes.join("\n") + `\nFAILED: ${e.stack}\n`); } catch {} process.exit(1); });
