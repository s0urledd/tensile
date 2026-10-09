// design-shots.cjs <out dir> <shots dir>: serves the static site from <out dir>, passes /api/* through to the live API,
// and photographs the production build of one publisher's page (branch publisher-txs): All, Blobs and Escrow at 1440 in
// both themes, the hover on a Failed status, the page's top once, and All and Escrow at 375 in both themes. The live
// API does not serve /v1/publishers/{addr}/txs or /v1/blobs?include_failed=1 yet (not deployed): the browser's reads of
// both are answered here as the branch's Go code answers them (observer/api/pubtxs.go, failedblobs.go), built from the
// live record (the account's blobs, its two real deposits and its two real failed transactions, read from the live
// /v1/txs) and the rows marked STUB below. Everything else is the live answer. Every head and cell is measured for
// clipping (scrollWidth over clientWidth), the 1440 tables' columns are measured against the approved Alt 3, and the
// 375 page is checked for sideways scroll: notes.txt. Runs only in the Design shots workflow, on a throwaway branch.
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

/** a time in the store's layout (store.TS): nine digits of the second, as payments and failed_txs keep it */
const ts9 = (s) => { const d = new Date(s); const m = /\.(\d+)Z$/.exec(s); const frac = (m ? m[1] : "").padEnd(9, "0").slice(0, 9); return `${d.toISOString().slice(0, 19)}.${frac}Z`; };

const notes = [];

// ---------------------------------------------------------------- the rows
const PUB = "celestia1jw8afsj3j0c23fxs09nu8pq5asxwes5e3kkxdx";
const TIA = 1000000;
// the account's real deposits and real failed transactions, read from the live /v1/txs below
const REAL_DEPOSITS = ["7f7e66d16ec7337c82ffc65148bf3e42bef39b2adef3f930862455494ddc3d89", "b241a96f9f221d4e9f03205ada95a5587b3f702c5345ad7b16c3c62da16d8f4c"];
const REAL_FAILED = ["f5ac69d972e0d707e6f535e92912c8ee6a6ddbf8c56ce920c6751764cbb1581b", "a4bd0b1855044d09c2f84ae43f6504d9365a470810b6f67d30a6ec11d2afdb92"];
// STUB: a failed blob payment (invented), between the blobs at #1,497,140 and #1,497,108, as the approved shots had it;
// its namespace is its newer neighbour's; final, so the pages keep the default cache
const F_PFF = { kind: "settlement", status: "failed", height: 1497121, tx_index: 1, msg_index: 0, time: "2026-10-08T13:25:05.700000000Z",
  tx_hash: "9b3e7a41d0c25f86e4a7b1d9c03f5e2a8d6b4c1f7e9a0d3b5c8f2e6a1d4b7c90", promise_hash: "4c1f7e9a0d3b5c8f2e6a1d4b7c909b3e7a41d0c25f86e4a7b1d9c03f5e2a8d6b",
  code: 18, codespace: "sdk", reason: "Invalid request", final: true };
// STUB: two successful withdrawal requests, so both payout states show: one paid out (a payout on record, a day after
// it: the chain's 24 h withdrawal delay), one still queued; and a deposit of the paid one's amount before it, so the
// statement still closes on the live balance. Hashes invented; heights from the live blobs' times (heightAt).
const D_STUB = { kind: "deposit", status: "success", tx_index: 0, msg_index: 0, time: "2026-10-05T09:14:27.512000000Z", tx_hash: "3e8a1c5f907b2d64a1f3e9c0b7d5a2e8c4f6b1d93a7e0c2f5b8d1a4e6c9f0b27", amount_utia: 25 * TIA };
const W_PAID = { kind: "withdrawal_request", status: "success", tx_index: 0, msg_index: 0, time: "2026-10-06T16:02:11.274000000Z", tx_hash: "d07c4e9a2b5f8136e0a4c7d2f9b3e6a1c5d8f0b4e7a2c9d6f1b3e5a8c0d4f7e2", amount_utia: 25 * TIA,
  payout: { state: "paid", available_at: "2026-10-07T16:02:11.274000000Z", paid_height: 0, paid_at: "2026-10-07T16:02:13.981000000Z", paid_utia: 25 * TIA, payout_delay_s: 86402 } };
const W_PENDING = { kind: "withdrawal_request", status: "success", tx_index: 0, msg_index: 0, time: "2026-10-09T09:47:20.118000000Z", tx_hash: "6a2f9d4c1e7b3a05f8c2d6e9b1a4f7c3e0d5b8a2f6c9e1d4b7a3f0c6e2d9b5a8", amount_utia: 50 * TIA,
  payout: { state: "pending", available_at: "2026-10-10T09:47:20.118000000Z" } };

/** every transaction of the account, newest first (view all), its escrow's alone (view escrow), the escrow's sums, and
 *  the Blobs tab's list (the account's blobs, each status success, the failed blob payment in its place) */
let ALL = [], ESCROW = [], SUMS = null, BLOBS = [];
const newestFirst = (a, b) => b.height - a.height || b.tx_index - a.tx_index || b.msg_index - a.msg_index;
// the field order pubtxs.go writes (pubTx), absent fields left out as omitempty does
const ORDER = ["kind", "status", "height", "tx_index", "msg_index", "time", "tx_hash", "amount_utia", "promise_hash", "code", "codespace", "reason", "final", "payout"];
const asGo = (t) => Object.fromEntries(ORDER.filter((k) => t[k] !== undefined && t[k] !== "").map((k) => [k, t[k]]));

async function prepare() {
  const blobs = [];
  for (let o = 0; o < 2000; o += 100) {
    const j = await live(`/v1/blobs?publisher=${PUB}&limit=100&offset=${o}`);
    blobs.push(...j.blobs);
    if (blobs.length >= j.total || j.blobs.length === 0) break;
  }
  const failed = [];
  for (const h of REAL_FAILED) {
    const x = await live(`/v1/txs/${h}`);
    failed.push({ kind: x.kind, status: "failed", height: x.height, tx_index: x.tx_index, msg_index: 0, time: ts9(x.time), tx_hash: h,
      code: x.failure.code, codespace: x.failure.codespace, reason: x.failure.reason, final: x.final });
  }
  const deposits = [];
  for (const h of REAL_DEPOSITS) {
    const x = await live(`/v1/txs/${h}`);
    deposits.push({ kind: "deposit", status: "success", height: x.height, tx_index: x.tx_index, msg_index: x.messages.find((m) => m.type_url.endsWith("MsgDepositToEscrow")).index, time: ts9(x.time), tx_hash: h, amount_utia: x.effect.amount_utia });
  }
  notes.push(`live rows: ${blobs.length} blobs, deposits ${deposits.map((d) => `#${d.height} ${d.amount_utia / TIA} TIA`).join(", ")}, failed ${failed.map((f) => `#${f.height} ${f.kind} (${f.reason}, final ${f.final})`).join(", ")}`);

  // a height for a time: between the two live anchors around it, or past the newest at the rate of the last day
  const anchors = [...blobs.map((b) => ({ h: b.settlement_height, t: Date.parse(b.settlement_time) })), ...failed.map((f) => ({ h: f.height, t: Date.parse(f.time) }))]
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
  notes.push(`STUB heights: deposit #${D_STUB.height}, paid request #${W_PAID.height} (paid out #${W_PAID.payout.paid_height}), pending request #${W_PENDING.height}`);

  const settled = blobs.map((b) => ({ kind: "settlement", status: "success", height: b.settlement_height, tx_index: b.settlement_tx_index ?? 0, msg_index: 0, time: ts9(b.settlement_time),
    tx_hash: b.settlement_tx_hash.toLowerCase(), amount_utia: b.charge ? b.charge.fee_utia : 0, promise_hash: b.promise_hash }));
  ALL = [...failed, F_PFF, ...deposits, D_STUB, W_PAID, W_PENDING, ...settled].sort(newestFirst).map(asGo);
  ESCROW = ALL.filter((t) => t.kind === "deposit" || t.kind === "withdrawal_request");
  const ok = (k) => ALL.filter((t) => t.status === "success" && t.kind === k);
  let timeouts = 0, charged = 0;
  try {
    const p = await live(`/v1/publishers/${PUB}?window=all`);
    timeouts = p.publisher.timeouts; charged = p.publisher.timed_out_utia;
  } catch (e) { notes.push(`publisher not read: ${e.message}`); }
  SUMS = {
    deposited_utia: ok("deposit").reduce((s, t) => s + t.amount_utia, 0),
    settlements: ok("settlement").length,
    fees_utia: ok("settlement").reduce((s, t) => s + t.amount_utia, 0),
    timeouts, charged_utia: charged,
    withdrawn_utia: ok("withdrawal_request").filter((t) => t.payout && t.payout.state === "paid").reduce((s, t) => s + t.payout.paid_utia, 0),
  };
  notes.push(`All: ${ALL.length} rows (${settled.length} live blob payments, ${ESCROW.length} escrow transactions, ${ALL.filter((t) => t.status === "failed").length} failed); Escrow: ${ESCROW.length}; sums ${JSON.stringify(SUMS)}`);

  // the Blobs tab's list, as failedblobs.go answers it: each blob with status success, the failed blob payment in its
  // place (a failure stands before a blob of the same place) with its record's fields
  const newer = blobs.filter((b) => b.settlement_height > F_PFF.height).sort((a, b) => a.settlement_height - b.settlement_height)[0] || blobs[0];
  const pffRow = { status: "failed", promise_hash: F_PFF.promise_hash, namespace: newer.namespace, publisher: PUB, settlement_height: F_PFF.height, settlement_tx_index: F_PFF.tx_index,
    settlement_tx_hash: F_PFF.tx_hash, settlement_time: F_PFF.time, code: F_PFF.code, codespace: F_PFF.codespace, reason: F_PFF.reason, final: F_PFF.final };
  BLOBS = [...blobs.map((b) => ({ status: "success", ...b })), pffRow]
    .sort((a, b) => b.settlement_height - a.settlement_height || b.settlement_tx_index - a.settlement_tx_index || (a.status === "failed" ? -1 : 1));

  try {
    const p = await live(`/v1/publishers/${PUB}?window=all`);
    const bal = p.publisher.escrow && p.publisher.escrow.balance_utia;
    const left = SUMS.deposited_utia - SUMS.fees_utia - SUMS.charged_utia - SUMS.withdrawn_utia;
    notes.push(left === bal ? `Escrow's statement closes on the live balance (${bal} utia)` : `Escrow's statement does NOT close (${left} vs the live ${bal} utia): no foot in the shots`);
  } catch (e) { notes.push(`publisher not read: ${e.message}`); }
}

/** the branch's route, as pubtxs.go answers it; null for every other path (read live) */
function txs(pathname, q) {
  if (pathname !== `/api/v1/publishers/${PUB}/txs`) return null;
  const view = q.get("view") || "all";
  const limit = Number(q.get("limit") || 25), offset = Number(q.get("offset") || 0);
  const list = view === "escrow" ? ESCROW : ALL;
  const body = { publisher: PUB, view, limit, offset, total: list.length, failed_total: list.filter((t) => t.status === "failed").length, txs: list.slice(offset, offset + limit) };
  if (view === "escrow") body.sums = SUMS;
  return body;
}

/** /v1/blobs?include_failed=1 of the account, as failedblobs.go answers it; null for every other read (live) */
function blobsWithFailed(pathname, q) {
  if (pathname !== "/api/v1/blobs" || q.get("include_failed") !== "1" || q.get("publisher") !== PUB) return null;
  const ns = (q.get("namespace") || "").toLowerCase();
  const list = BLOBS.filter((r) => !ns || r.namespace === ns);
  const limit = Number(q.get("limit") || 50), offset = Number(q.get("offset") || 0);
  const page = list.slice(offset, offset + limit);
  const body = { blobs: page, limit, offset, total: list.length, failed_total: list.filter((r) => r.status === "failed").length, truncated: offset + page.length < list.length, namespace: ns, publisher: PUB };
  if (body.truncated && page.length) { body.next_before_height = page[page.length - 1].settlement_height; body.next_before_tx_index = page[page.length - 1].settlement_tx_index; }
  return body;
}

// ---------------------------------------------------------------- photographing
async function open(browser, theme, width, height, mobile) {
  const ctx = await browser.newContext({ viewport: { width, height }, deviceScaleFactor: 2, colorScheme: theme, isMobile: !!mobile, hasTouch: !!mobile });
  await ctx.addInitScript((t) => { try { localStorage.setItem("theme", t); } catch {} }, theme);
  await ctx.route("**/api/v1/**", async (route) => {
    const u = new URL(route.request().url());
    const o = txs(u.pathname, u.searchParams) || blobsWithFailed(u.pathname, u.searchParams);
    if (o) return route.fulfill({ status: 200, contentType: "application/json", headers: { "access-control-allow-origin": "*", "cache-control": "public, max-age=15" }, body: JSON.stringify(o) });
    return route.continue();
  });
  const page = await ctx.newPage();
  page.on("pageerror", (e) => notes.push(`${theme} ${width} page error: ${e.message}`));
  return { ctx, page, W: width, H: height };
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

/** the elements' area with room around it, from the window as it is; never over the site's header, which stays at the window's top */
async function crop(v, file, sels, pad = 24) {
  const { page, W, H } = v;
  const b = await box(page, sels);
  if (!b) { notes.push(`${file}: nothing to crop`); return; }
  const head = await page.evaluate(() => { const h = document.querySelector("header.top"); return h ? Math.ceil(h.getBoundingClientRect().bottom) : 0; });
  const x = Math.max(0, Math.floor(b.x - pad)), y = Math.max(0, head, Math.floor(b.y - pad));
  const width = Math.min(W - x, Math.ceil(b.r + pad) - x), height = Math.min(H - y, Math.ceil(b.b + 16) - y);
  if (b.b + 16 > H) notes.push(`${file}: taller than the window, cut at its foot`);
  await page.screenshot({ path: path.join(out, file), animations: "disabled", clip: { x, y, width, height } });
}

/**
 * the browser's own tooltip for an element's title: a headless browser paints no native tooltip, so the shot draws it
 * in the platform's plain style, level with the hovered element's row: to its right when it fits in the window, else
 * to its left. What it covers is written to the notes.
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
    const rs = [e, ...e.querySelectorAll("*")].map((x) => x.getBoundingClientRect()).filter((q) => q.width > 0);
    const r = { left: Math.min(...rs.map((q) => q.left)), right: Math.max(...rs.map((q) => q.right)) };
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
/** every head and cell of the list (and the list's head and pager), and every clipping box in them: scrollWidth over
 *  clientWidth is clipped text; and whether the page scrolls sideways */
async function clips(page, label) {
  const bad = await page.evaluate(() => {
    const o = [];
    const cls = (el) => (typeof el.className === "string" && el.className ? el.className.split(" ")[0] : el.tagName.toLowerCase());
    const check = (el, where) => { if (el.clientWidth > 0 && el.scrollWidth > el.clientWidth) o.push(`${where}: ${el.scrollWidth} > ${el.clientWidth} ("${el.textContent.trim().slice(0, 30)}")`); };
    const inside = (root, where) => root.querySelectorAll("*").forEach((el) => { const s = getComputedStyle(el); if (s.display !== "none" && s.overflowX !== "visible") check(el, `${where} ${cls(el)}`); });
    const t = document.querySelector("#list table");
    if (!t) return ["no table"];
    t.querySelectorAll("thead th").forEach((th) => { if (getComputedStyle(th).display !== "none" && getComputedStyle(th.closest("thead")).display !== "none") { check(th, `head ${cls(th)}`); inside(th, `head ${cls(th)}`); } });
    [...t.querySelectorAll("tbody tr.row, tfoot tr")].forEach((tr, r) => {
      [...tr.children].forEach((td) => {
        if (getComputedStyle(td).display === "none") return;
        const where = `${tr.closest("tfoot") ? "foot" : "row"} ${r + 1}${tr.classList.contains("xf") ? " (failed)" : ""} ${cls(td)}`;
        check(td, where);
        inside(td, where);
        // a cell's words past its own box (a grid cell on a phone, which does not clip)
        const b = td.getBoundingClientRect();
        for (const k of td.querySelectorAll("*")) { const q = k.getBoundingClientRect(); if (q.width && (q.right > b.right + 0.5 || q.left < b.left - 0.5) && getComputedStyle(k).position !== "absolute" && !k.classList.contains("cp") && !k.classList.contains("dot")) { o.push(`${where} ${cls(k)} outside its cell (${Math.round(q.left)}–${Math.round(q.right)} in ${Math.round(b.left)}–${Math.round(b.right)})`); break; } }
      });
    });
    const lh = document.querySelector("#list .list-head");
    if (lh) { inside(lh, "list head"); lh.querySelectorAll("button, h2").forEach((b) => check(b, `list head ${cls(b)} "${b.textContent.trim()}"`)); }
    const pg = document.querySelector("#list .pager");
    if (pg) inside(pg, "pager");
    if (document.documentElement.scrollWidth > innerWidth) o.push(`THE PAGE SCROLLS SIDEWAYS: ${document.documentElement.scrollWidth} > ${innerWidth}`);
    return o;
  });
  bad.forEach((b) => notes.push(`CLIP ${label} ${b}`));
  if (!bad.length) notes.push(`${label}: every head and cell whole, nothing sideways`);
}

/**
 * the table as drawn at 1440: its columns' widths; each column's head words against its cells' words by the column's
 * own alignment; the status marks; the amounts' right edges; every failed row's amount; the rows' heights; the room
 * between neighbouring columns' words: the approved Alt 3's notes have the same lines
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
    const amRight = [...new Set([...t.querySelectorAll(`tbody tr.row:not(.xf) ${amSel}, tfoot ${amSel}`)].map((td) => words(td)).filter(Boolean).map((w) => R1(w.r)))];
    const failed = [...t.querySelectorAll("tbody tr.xf")].map((tr) => (tr.querySelector(amSel) || {}).textContent);
    const heights = [...new Set(rows.map((tr) => R1(tr.getBoundingClientRect().height)))];
    const gaps = ths.slice(0, -1).map((th, i) => {
      let lo = Infinity, hi = -Infinity;
      for (const tr of rows) {
        const a = tr.children[i] && words(tr.children[i]), b = tr.children[i + 1] && words(tr.children[i + 1]);
        if (!a || !b) continue;
        lo = Math.min(lo, b.l - a.r); hi = Math.max(hi, b.l - a.r);
      }
      return isFinite(lo) ? `${th.className.split(" ")[0]}→${ths[i + 1].className.split(" ")[0]} ${Math.round(lo)}–${Math.round(hi)}` : null;
    }).filter(Boolean);
    return { table: R1(t.getBoundingClientRect().width), cls: t.className, cols, dots, stWords, stHead: stHead ? `${R1(stHead.l)}–${R1(stHead.r)}` : "-", amRight, failed, heights, gaps };
  });
  notes.push(`--- ${label}: table ${r.table}px (${r.cls})`);
  notes.push(`${label} columns (width, alignment, largest gap of the cells' words from the head's): ${r.cols.join(" · ")}`);
  notes.push(`${label} status: head words ${r.stHead}; dots' left ${r.dots.join(", ") || "-"}; words' left ${r.stWords.join(", ") || "-"}${r.dots.length > 1 || r.stWords.length > 1 ? "  NOT ONE LINE" : ""}`);
  notes.push(`${label} amounts' right edges: ${r.amRight.join(", ") || "-"}${r.amRight.length > 1 ? "  NOT STACKED" : ""}`);
  notes.push(`${label} failed rows' amounts: ${r.failed.map((s) => JSON.stringify(s)).join(", ") || "-"}${r.failed.some((s) => s !== "—") ? "  A FAILED ROW SHOWS AN AMOUNT" : ""}`);
  notes.push(`${label} rows' heights: ${r.heights.join(", ")}`);
  notes.push(`${label} room between the columns' words (least–most over the rows): ${r.gaps.join(" · ")}`);
}

/** a phone's card: each row's lines and what stands on them, for the notes */
async function cards(page, label) {
  const r = await page.evaluate(() => {
    const rows = [...document.querySelectorAll("#list table tbody tr.row")].slice(0, 6);
    return rows.map((tr) => {
      const cells = [...tr.children].filter((td) => getComputedStyle(td).display !== "none").map((td) => ({ c: td.className.split(" ")[0], t: Math.round(td.getBoundingClientRect().top - tr.getBoundingClientRect().top), x: Math.round(td.getBoundingClientRect().left), r: Math.round(td.getBoundingClientRect().right), w: td.innerText.replace(/\s+/g, " ").trim() }));
      return `${Math.round(tr.getBoundingClientRect().height)}px: ${cells.map((c) => `${c.c}@${c.t},${c.x}–${c.r} "${c.w}"`).join(" | ")}`;
    });
  });
  notes.push(`--- ${label} cards`);
  r.forEach((x) => notes.push(`${label} ${x}`));
}

/** the top's words, for the check that the page leaves it as today's live page draws it */
const topText = (page) => page.evaluate(() => [".pb-mast", ".pbd"].map((s) => document.querySelector(s)?.innerText.replace(/\b\d+ (d|h|min|s)\b/g, "")).join("\n"));

// ---------------------------------------------------------------- the shots
const rowSel = (n) => `#list table tbody tr:nth-child(${n})`;
const URL0 = `/publisher/?addr=${PUB}`;
const READY_TX = "#list .ptx-t tbody tr.row:not(.sk)";

(async () => {
  fs.mkdirSync(out, { recursive: true });
  await prepare();
  await new Promise((r) => server.listen(PORT, "127.0.0.1", r));
  const browser = await chromium.launch({ executablePath: process.env.CHROME || "/usr/bin/google-chrome", args: ["--hide-scrollbars"] });

  // today's live page, its top: the branch must draw it word for word
  let top0 = "";
  {
    const ctx = await browser.newContext({ viewport: { width: 1440, height: 1200 } });
    const page = await ctx.newPage();
    await page.goto(`https://${LIVE}${URL0}`, { waitUntil: "domcontentloaded" });
    try { await page.waitForSelector(".pbd .pbd-v", { timeout: 45000 }); await page.waitForTimeout(3000); top0 = await topText(page); } catch { notes.push("the live page's top did not load"); }
    await ctx.close();
  }

  for (const theme of ["dark", "light"]) {
    const v = await open(browser, theme, 1440, 1200);
    const { ctx, page } = v;
    // All
    await go(page, URL0, READY_TX);
    const top = await topText(page);
    notes.push(`${theme}: the top ${top === top0 ? "is the live page's, word for word" : `DIFFERS from the live page's:\n${top}\n-- live --\n${top0}`}`);
    await page.evaluate(() => scrollTo(0, 0));
    await page.waitForTimeout(200);
    await crop(v, `top-${theme}.png`, [".pb-mast", ".pbd", "#list .list-head"]);
    await reveal(page, "#list .list-head");
    await clips(page, `all ${theme}`);
    if (theme === "dark") await layout(page, "all");
    await crop(v, `all-${theme}.png`, ["#list .list-head", rowSel(8)]);
    // Blobs
    await go(page, `${URL0}&kind=blobs`, "#list .lg-t tbody tr.xf");
    await reveal(page, "#list .list-head");
    await clips(page, `blobs ${theme}`);
    if (theme === "dark") await layout(page, "blobs");
    await crop(v, `blobs-${theme}.png`, ["#list .list-head", rowSel(8)]);
    // Escrow
    await go(page, `${URL0}&kind=escrow`, READY_TX);
    await page.waitForSelector("#list .ptx-t tfoot tr.tot", { timeout: 10000 }).catch(() => notes.push(`${theme} escrow: no statement lines`));
    await reveal(page, "#list .list-head");
    await clips(page, `escrow ${theme}`);
    if (theme === "dark") await layout(page, "escrow");
    await crop(v, `escrow-${theme}.png`, ["#list .list-head", "#list .lg-tw", "#list .pager"]);
    // the hover on a Failed status, in Escrow
    await tip(page, "#list .ptx-t tr.xf td.c-st .st.f", theme);
    await crop(v, `hover-${theme}.png`, ["#list .list-head", "#list .ptx-t tbody", ".shot-tip"]);
    await untip(page);
    // the payout line's hover, the paid one
    const paidTitle = await page.evaluate(() => [...document.querySelectorAll("#list .ptx-t .k2")].map((k) => `${k.textContent} → ${k.getAttribute("title")}`).join(" | "));
    notes.push(`${theme} payout lines: ${paidTitle}`);
    await ctx.close();
  }

  // a phone: All and Escrow at 375, both themes
  for (const theme of ["dark", "light"]) {
    const v = await open(browser, theme, 375, 2400, true);
    const { ctx, page } = v;
    await go(page, URL0, READY_TX);
    await reveal(page, "#list .list-head");
    await clips(page, `all 375 ${theme}`);
    if (theme === "dark") await cards(page, "all 375");
    await crop(v, `all-375-${theme}.png`, ["#list .list-head", rowSel(8)], 16);
    await go(page, `${URL0}&kind=escrow`, READY_TX);
    await page.waitForSelector("#list .ptx-t tfoot tr.tot", { timeout: 10000 }).catch(() => notes.push(`${theme} 375 escrow: no statement lines`));
    await reveal(page, "#list .list-head");
    await clips(page, `escrow 375 ${theme}`);
    if (theme === "dark") await cards(page, "escrow 375");
    await crop(v, `escrow-375-${theme}.png`, ["#list .list-head", "#list .lg-tw", "#list .pager"], 16);
    // the Blobs tab on a phone too, for the notes: its failed card
    await go(page, `${URL0}&kind=blobs`, "#list .lg-t tbody tr.xf");
    await reveal(page, "#list .list-head");
    await clips(page, `blobs 375 ${theme}`);
    await crop(v, `blobs-375-${theme}.png`, ["#list .list-head", rowSel(6)], 16);
    await ctx.close();
  }
  await browser.close();
  server.close();
  fs.writeFileSync(path.join(out, "notes.txt"), notes.join("\n") + "\n");
  console.log(fs.readdirSync(out).join("\n"));
  console.log(notes.join("\n"));
})().catch((e) => { console.error(e); try { fs.mkdirSync(out, { recursive: true }); fs.writeFileSync(path.join(out, "notes.txt"), notes.join("\n") + `\nFAILED: ${e.stack}\n`); } catch {} process.exit(1); });
