// design-shots.cjs <out dir> <shots dir>: serves the static site from <out dir>, passes /api/* through to the live API,
// and photographs round 2 of the failed-transaction follow-ups at 1440 in both themes. The browser's requests to the
// two new routes (/v1/publishers/{addr}/txs, /v1/txs/{hash}) are answered from the mock; the validator and blob answers
// are read live and given the keys the proposal adds. Real Mocha values, except rows marked STUB or MOCK. Runs only in
// the Design shots workflow.
const http = require("http");
const https = require("https");
const fs = require("fs");
const path = require("path");
const { chromium } = require(path.join(process.env.PW_DIR, "node_modules", "playwright-core"));

const [root, out] = process.argv.slice(2).map((p) => path.resolve(p));
const LIVE = "tensile.huginn.tech";
const BASE = "http://127.0.0.1:4173";
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

// ---------------------------------------------------------------- the mock data (proposal-v2 §8.4)
const PUB = "celestia1jw8afsj3j0c23fxs09nu8pq5asxwes5e3kkxdx";
const VAL = "celestiavaloper1l0smvx0aaguy64zg8kz9hf0keqptngmp5qrrvj";
const OPER = "celestia1l0smvx0aaguy64zg8kz9hf0keqptngmp3lp665";
const BLOB = "ba80a76e14c028e71ed777aa85b6f6ba3a3ca52916ededd5a9a8a61671c6c496";
const NS = "00000000000000000000000000000000000000000074656e73696c6500";

// STUB: a successful deposit on page 1, beside the failures, so a signed amount stands next to their dashes (All only)
const STUB_DEPOSIT = { kind: "deposit", status: "success", height: 1509603, tx_index: 0, msg_index: 0, time: "2026-10-08T23:18:16.912442Z", tx_hash: "c41d9e07a3b5f2861e0d4c7a9b3f5e2d8a6c1b4f7e0a3d9c5b8f2e1a6d4c7b30", amount_utia: 1000000000 };
const F_DEPOSIT = { kind: "deposit", status: "failed", height: 1509598, tx_index: 0, msg_index: 0, time: "2026-10-08T23:17:45.331354784Z", tx_hash: "f5ac69d972e0d707e6f535e92912c8ee6a6ddbf8c56ce920c6751764cbb1581b", reason: "Out of gas" };
const F_WITHDRAWAL = { kind: "withdrawal_request", status: "failed", height: 1509587, tx_index: 0, msg_index: 0, time: "2026-10-08T23:17:14.030516463Z", tx_hash: "a4bd0b1855044d09c2f84ae43f6504d9365a470810b6f67d30a6ec11d2afdb92", reason: "Insufficient funds" };
// STUB: a failed blob payment between the blobs at #1,497,140 and #1,497,108
const F_PFF = { kind: "settlement", status: "failed", height: 1497121, tx_index: 1, msg_index: 0, time: "2026-10-08T13:25:05.700Z", tx_hash: "9b3e7a41d0c25f86e4a7b1d9c03f5e2a8d6b4c1f7e9a0d3b5c8f2e6a1d4b7c90", reason: "Invalid request" };
const DEP_1 = { kind: "deposit", status: "success", height: 1366494, tx_index: 0, msg_index: 0, time: "2026-10-04T05:51:46.491278Z", tx_hash: "7f7e66d16ec7337c82ffc65148bf3e42bef39b2adef3f930862455494ddc3d89", amount_utia: 1000000000 };
const DEP_2 = { kind: "deposit", status: "success", height: 1107337, tx_index: 0, msg_index: 0, time: "2026-09-25T16:10:56.562358Z", tx_hash: "b241a96f9f221d4e9f03205ada95a5587b3f702c5345ad7b16c3c62da16d8f4c", amount_utia: 4000000 };
const ESCROW = [F_DEPOSIT, F_WITHDRAWAL, DEP_1, DEP_2];
const SUMS = { deposited_utia: 1004000000, withdrawn_utia: 0, charged_utia: 0 };

// the validator's endpoint history: its real changes and the host before Tensile's record; the failed row is MOCK (the
// real failed set-host FB27DDA8…, placed under Unity Nodes; its real signer is not a validator)
const EH = [
  { outcome: "failed", height: 1509592, tx_index: 0, time: "2026-10-08T23:17:28.23537Z", tx_hash: "fb27dda8abf009db6512800b74898bc60410e1139a7fbb8f49e4ff30d3226cdb", host: "203.0.113.10:7980", attempted: "change", reason: "Invalid validator" },
  { outcome: "changed", height: 1496499, tx_index: 0, time: "2026-10-08T12:55:34.318488Z", tx_hash: "e59c8ec3e5538f64f442e968795dd2c1159636ecf9f90d4698317f946374adcf", host: "89.40.226.218:7980", previous_host: "89.40.226.146:7980" },
  { outcome: "changed", height: 1253828, tx_index: 0, time: "2026-09-30T12:17:39.784839Z", tx_hash: "ed68446c853e334de84e6fe95873a6d9de16f2bfdc10d96481464093e8254c52", host: "89.40.226.146:7980", previous_host: "149.86.227.11:7980" },
  { outcome: "before_record", host: "149.86.227.11:7980" },
];
const BLOB_COST = { gas_wanted: 400000, gas_used: 219118, fee: "8000utia", fee_payer: PUB, messages: 1 };

const MSG = (type, signer) => ({ index: 0, type_url: type, fibre: true, signer });
/** the five transaction pages, by hash; avatar is filled from the live validator answer */
const TXS = {
  // T1 blob payment, success (real; its cost from round 1)
  "5da67b2a8865c75a572f5abb3070a2d3377a23baf371f705e1db1b312e21754e": {
    tx_hash: "5da67b2a8865c75a572f5abb3070a2d3377a23baf371f705e1db1b312e21754e", status: "success", height: 1497140, tx_index: 2, time: "2026-10-08T13:25:59.888800317Z", kind: "settlement",
    messages: [MSG("/celestia.fibre.v1.MsgPayForFibre", PUB)],
    cost: { gas_wanted: 400000, gas_used: 219118, fee: "8000utia", fee_payer: PUB },
    effect: { promise_hash: BLOB, commitment: "e188951e6e2e1ac6b145782e6dc2f6ec95dacdeb1fe9a785861562b80daddff7", blob_version: 0, namespace: NS, blob_size: 17039360, fee_paid_utia: 3575000, settled: true, timed_out: false },
    related: { publisher: PUB, blob: { promise_hash: BLOB, commitment: "e188951e6e2e1ac6b145782e6dc2f6ec95dacdeb1fe9a785861562b80daddff7", blob_version: 0 } },
  },
  // T2 deposit, success (real)
  "7f7e66d16ec7337c82ffc65148bf3e42bef39b2adef3f930862455494ddc3d89": {
    tx_hash: "7f7e66d16ec7337c82ffc65148bf3e42bef39b2adef3f930862455494ddc3d89", status: "success", height: 1366494, tx_index: 0, time: "2026-10-04T05:51:46.491278Z", kind: "deposit",
    messages: [MSG("/celestia.fibre.v1.MsgDepositToEscrow", PUB)],
    cost: { gas_wanted: 200000, gas_used: 74215, fee: "4000utia", fee_payer: PUB },
    effect: { amount_utia: 1000000000 }, related: { publisher: PUB },
  },
  // T3 withdrawal request, failed (real: the live failed record of A4BD0B18…; its signer from the record)
  "a4bd0b1855044d09c2f84ae43f6504d9365a470810b6f67d30a6ec11d2afdb92": {
    tx_hash: "a4bd0b1855044d09c2f84ae43f6504d9365a470810b6f67d30a6ec11d2afdb92", status: "failed", final: true, height: 1509587, tx_index: 0, time: "2026-10-08T23:17:14.030516463Z", kind: "withdrawal_request",
    messages: [MSG("/celestia.fibre.v1.MsgRequestWithdrawal", PUB)],
    cost: { gas_wanted: 200000, gas_used: 50219, fee: "800utia" },
    failure: { code: 5, codespace: "sdk", reason: "Insufficient funds", failed_msg_index: 0, log: "failed to execute message; message index: 0: insufficient available balance: have 458805000utia, need 1000000000000000utia: insufficient funds" },
    effect: { requested: "1000000000000000utia", requested_utia: 1000000000000000 }, related: { publisher: PUB },
  },
  // T4 endpoint registration, success (real)
  "e59c8ec3e5538f64f442e968795dd2c1159636ecf9f90d4698317f946374adcf": {
    tx_hash: "e59c8ec3e5538f64f442e968795dd2c1159636ecf9f90d4698317f946374adcf", status: "success", height: 1496499, tx_index: 0, time: "2026-10-08T12:55:34.318488Z", kind: "set_host",
    messages: [MSG("/celestia.valaddr.v1.MsgSetFibreProviderInfo", OPER)],
    cost: { gas_wanted: 200000, gas_used: 54455, fee: "2000utia", fee_payer: OPER },
    effect: { action: "changed", host: "89.40.226.218:7980", previous_host: "89.40.226.146:7980" },
    related: { validator: { operator_address: VAL, moniker: "Unity Nodes" } },
  },
  // T5 endpoint registration, failed (MOCK: FB27DDA8…'s real failure, gas, fee and error, its signer set to Unity Nodes')
  "fb27dda8abf009db6512800b74898bc60410e1139a7fbb8f49e4ff30d3226cdb": {
    tx_hash: "fb27dda8abf009db6512800b74898bc60410e1139a7fbb8f49e4ff30d3226cdb", status: "failed", final: true, height: 1509592, tx_index: 0, time: "2026-10-08T23:17:28.235370721Z", kind: "set_host",
    messages: [MSG("/celestia.valaddr.v1.MsgSetFibreProviderInfo", OPER)],
    cost: { gas_wanted: 200000, gas_used: 49930, fee: "800utia" },
    failure: { code: 2, codespace: "valaddr", reason: "Invalid validator", failed_msg_index: 0, log: "failed to execute message; message index: 0: validator not found: validator does not exist: invalid validator" },
    effect: { requested_host: "203.0.113.10:7980", attempted: "change", host_at_block: "89.40.226.218:7980" },
    related: { validator: { operator_address: VAL, moniker: "Unity Nodes" } },
  },
};

const notes = [];
/** every row of the account's All view, newest first: its real settlements and deposits, the STUB rows, the failures */
let ALL = [];
async function prepare() {
  const blobs = [];
  for (let o = 0; o < 1000; o += 100) {
    const j = await live(`/v1/blobs?publisher=${PUB}&limit=100&offset=${o}`);
    blobs.push(...j.blobs);
    if (blobs.length >= j.total || j.blobs.length === 0) break;
  }
  const settled = blobs.map((b) => ({ kind: "settlement", status: "success", height: b.settlement_height, tx_index: b.settlement_tx_index ?? 0, msg_index: 0, time: b.settlement_time,
    tx_hash: b.settlement_tx_hash, promise_hash: b.promise_hash, amount_utia: b.charge ? b.charge.fee_utia : 0 }));
  ALL = [STUB_DEPOSIT, F_DEPOSIT, F_WITHDRAWAL, F_PFF, DEP_1, DEP_2, ...settled]
    .sort((a, b) => b.height - a.height || b.tx_index - a.tx_index || b.msg_index - a.msg_index);
  notes.push(`All: ${ALL.length} rows (${settled.length} live settlements, 2 real deposits, 1 STUB deposit, 3 failed)`);
  try {
    const v = await live(`/v1/validators/${VAL}?window=24h`);
    for (const t of Object.values(TXS)) if (t.related.validator) t.related.validator.avatar_url = v.validator.avatar_url;
  } catch (e) { notes.push(`validator avatar not read: ${e.message}`); }
  try {
    const p = await live(`/v1/publishers/${PUB}?window=all`);
    const all = p.windows.find((w) => w.window.name === "all");
    const bal = p.publisher.escrow && p.publisher.escrow.balance_utia;
    if (SUMS.deposited_utia - all.fees_utia - SUMS.charged_utia - SUMS.withdrawn_utia !== bal) notes.push(`Escrow foot does not close on the live balance (${bal} utia, fees ${all.fees_utia}): the shot shows no foot`);
  } catch (e) { notes.push(`publisher not read: ${e.message}`); }
}

/** an answer of the two new routes, from the mock; null for every other path (read live) */
function own(pathname, q) {
  if (pathname === `/api/v1/publishers/${PUB}/txs`) {
    const view = q.get("view") === "escrow" ? "escrow" : "all";
    const limit = Math.min(100, Number(q.get("limit")) || 25), offset = Number(q.get("offset")) || 0;
    const list = view === "escrow" ? ESCROW : ALL;
    const body = { publisher: PUB, view, limit, offset, total: list.length, failed: list.filter((t) => t.status === "failed").length, txs: list.slice(offset, offset + limit) };
    if (view === "escrow") body.sums = SUMS;
    return { status: 200, body };
  }
  const m = /^\/api\/v1\/txs\/([0-9a-fA-F]{64})$/.exec(pathname);
  if (m) {
    const t = TXS[m[1].toLowerCase()];
    return t ? { status: 200, body: t } : { status: 404, body: { error: "no Fibre transaction with this hash on record" } };
  }
  return null;
}

/** a live answer with the proposal's keys added */
function mock(pathname, j) {
  if (pathname.startsWith("/api/v1/validators/") && j && j.validator && j.validator.operator_address === VAL) return { ...j, endpoint_history: EH };
  if (pathname === `/api/v1/blobs/${BLOB}`) return { ...j, tx_cost: BLOB_COST };
  return j;
}

// ---------------------------------------------------------------- photographing
const shots = [];
const W = 1440, H = 1300;

async function open(browser, theme) {
  const ctx = await browser.newContext({ viewport: { width: W, height: H }, deviceScaleFactor: 2, colorScheme: theme });
  await ctx.addInitScript((t) => { try { localStorage.setItem("theme", t); } catch {} }, theme);
  await ctx.route("**/api/v1/**", async (route) => {
    const u = new URL(route.request().url());
    const o = own(u.pathname, u.searchParams);
    if (o) return route.fulfill({ status: o.status, contentType: "application/json", headers: { "access-control-allow-origin": "*" }, body: JSON.stringify(o.body) });
    let res;
    try { res = await route.fetch(); } catch (e) { return route.abort(); }
    const type = res.headers()["content-type"] || "";
    if (!res.ok() || !type.includes("json")) return route.fulfill({ response: res });
    let j;
    try { j = await res.json(); } catch { return route.fulfill({ response: res }); }
    return route.fulfill({ response: res, json: mock(u.pathname, j) });
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
  for (const s of [].concat(sels)) {
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

/** room around a crop: one figure for every side, or { t, r, b, l } */
const pads = (p) => (typeof p === "number" ? { t: p, r: p, b: p, l: p } : { t: 24, r: 24, b: 24, l: 24, ...p });

/** scrolls the elements into the window, their top near its top, unless they are in it already */
async function reveal(page, sels, pad = 24) {
  const p = pads(pad);
  const b = await box(page, sels);
  if (!b) return;
  if (b.y - p.t >= 0 && b.b + p.b <= H) return;
  await page.evaluate((dy) => scrollBy(0, dy), Math.round(b.y - p.t - 8));
  await page.waitForTimeout(200);
}

/** the elements' area, with some room around it, from the window as it is (a tip that is open stays: nothing scrolls) */
async function crop(page, file, sels, shows, pad = 24) {
  const popped = await page.evaluate(() => !!document.querySelector(".info-pop, .warn-tip, .shot-tip"));
  if (!popped) await reveal(page, sels, pad);
  const b = await box(page, sels);
  if (!b) { notes.push(`${file}: nothing to crop (${[].concat(sels).join(", ")})`); return; }
  const p = pads(pad);
  const x = Math.max(0, Math.floor(b.x - p.l)), y = Math.max(0, Math.floor(b.y - p.t));
  const width = Math.min(W - x, Math.ceil(b.r + p.r) - x), height = Math.min(H - y, Math.ceil(b.b + p.b) - y);
  if (b.b + p.b > H) notes.push(`${file}: taller than the window, cut at its foot`);
  await page.screenshot({ path: path.join(out, file), animations: "disabled", clip: { x, y, width, height } });
  shots.push({ file, shows });
}

async function full(page, file, shows) {
  await page.evaluate(() => scrollTo(0, 0));
  await page.waitForTimeout(200);
  await page.screenshot({ path: path.join(out, file), fullPage: true, animations: "disabled" });
  shots.push({ file, shows });
}

/**
 * the browser's own tooltip for an element's title, drawn where the browser puts it (under the pointer): a headless
 * browser paints no native tooltip, so the shot draws it in the platform's plain style
 */
async function tip(page, sel, theme, at = "pointer") {
  const el = page.locator(sel).first();
  if (!(await el.count())) { notes.push(`tip: no element for ${sel}`); return; }
  await el.hover();
  await page.waitForTimeout(250);
  await page.evaluate(({ sel, dark, at }) => {
    const e = document.querySelector(sel);
    const t = e && (e.getAttribute("title") || e.closest("[title]")?.getAttribute("title"));
    if (!t) return;
    const r = e.getBoundingClientRect();
    const d = document.createElement("div");
    d.className = "shot-tip";
    d.textContent = t;
    Object.assign(d.style, {
      position: "fixed", left: `${Math.round(at === "start" ? r.left - 4 : r.left + Math.min(r.width / 2, 14))}px`, top: `${Math.round(r.bottom + 18)}px`, zIndex: 99,
      maxWidth: "480px", padding: "4px 8px", font: "12px/1.35 system-ui, -apple-system, 'Segoe UI', sans-serif",
      color: dark ? "#f2f2f2" : "#1d1d1d", background: dark ? "#3b3b3b" : "#ffffff", border: `1px solid ${dark ? "#5c5c5c" : "#a0a0a0"}`,
      borderRadius: "3px", boxShadow: "0 2px 6px rgba(0,0,0,.22)", whiteSpace: "normal", pointerEvents: "none",
    });
    document.body.appendChild(d);
    // a figure at a table's right edge: the tip ends under it, as a browser keeps a tooltip on the screen
    if (at === "end") { d.style.left = "0px"; const w = d.offsetWidth; d.style.left = `${Math.round(r.right - w + 4)}px`; }
  }, { sel, dark: theme === "dark", at });
}
/** hides what stands under a hover shot's area (the site's foot, the sections under a frame), so the crop holds only what it shows */
const hide = (page, sel, on = true) => page.evaluate(({ sel, on }) => document.querySelectorAll(sel).forEach((e) => { e.style.visibility = on ? "hidden" : ""; }), { sel, on });
const untip = async (page) => { await page.evaluate(() => document.querySelectorAll(".shot-tip").forEach((d) => d.remove())); await page.mouse.move(5, 5); await page.waitForTimeout(150); };

/** the n-th row of a table's body (1-based) as a selector */
const row = (scope, n) => `${scope} tbody tr:nth-child(${n})`;

(async () => {
  await prepare();
  await new Promise((ok) => server.listen(4173, "127.0.0.1", ok));
  fs.mkdirSync(out, { recursive: true });
  const browser = await chromium.launch({ executablePath: process.env.CHROME || "/usr/bin/google-chrome", args: ["--hide-scrollbars"] });

  for (const theme of ["dark", "light"]) {
    const dk = theme === "dark";

    // ---- P1. publisher, All: the transaction table, its hovers, its pager
    {
      const { ctx, page } = await open(browser, theme);
      await go(page, `/publisher/?addr=${PUB}`, ".tx-t tbody tr.row:not(.sk)");
      await crop(page, `p1-all-${theme}.png`, ["#list .list-head", row("#list .tx-t", 12)],
        "Publisher, All: one transaction table, TX hash · Type · Time (UTC) · Block · Status · Amount. The STUB deposit's +1,000.000 TIA beside the failed deposit and withdrawal request (Failed, Amount —); the STUB failed blob payment between two blob payments", { b: 0 });
      if (dk) await full(page, `p1-full-${theme}.png`, "Publisher page, whole, All open, for context");
      await reveal(page, ["#list .list-head", row("#list .tx-t", 6)]);
      await tip(page, `${row("#list .tx-t", 3)} .st.f`, theme);
      await crop(page, `p1-tip-failed-${theme}.png`, ["#list .tx-t thead", row("#list .tx-t", 5), ".shot-tip"], "Hover on Failed: \"Failed: Insufficient funds. None of its messages took effect.\"", { t: 20, r: 20, b: 0, l: 20 });
      await untip(page);
      await tip(page, `${row("#list .tx-t", 2)} td.c-am .xd`, theme, "end");
      await crop(page, `p1-tip-dash-${theme}.png`, ["#list .tx-t thead", row("#list .tx-t", 5), ".shot-tip"], "Hover on a failed row's Amount dash: \"Nothing moved: the escrow is as it was.\"", { t: 20, r: 20, b: 0, l: 20 });
      await untip(page);
      await reveal(page, ["#list .pager"], 160);
      await tip(page, "#list .pager .count span[title]", theme);
      await hide(page, "footer");
      await crop(page, `p1-pager-${theme}.png`, [row("#list .tx-t", 24), "#list .pager", ".shot-tip"], "The pager: Showing 1–25 of 115 transactions; its hover \"3 of them failed\"", { t: 0, r: 20, b: 16, l: 20 });
      await hide(page, "footer", false);
      await untip(page);
      await ctx.close();
    }

    // ---- P2. publisher, Blobs: today's blob table, the namespace picker here only
    {
      const { ctx, page } = await open(browser, theme);
      await go(page, `/publisher/?addr=${PUB}&kind=blobs`, "#list .lg-t tbody tr.row:not(.sk)");
      await crop(page, `p2-blobs-${theme}.png`, ["#list .list-head", row("#list .lg-t", 9)], "Publisher, Blobs: today's blob table unchanged (Blob ID, Namespace, Blob size, Amount, Endorsed, Tensile); the Namespace picker shows under Blobs only", { b: 0 });
      await ctx.close();
    }

    // ---- P3. publisher, Escrow: the same five columns, the statement's foot
    {
      const { ctx, page } = await open(browser, theme);
      await go(page, `/publisher/?addr=${PUB}&kind=escrow`, ".tx-t tbody tr.row:not(.sk)");
      await page.waitForSelector(".tx-t tfoot", { timeout: 15000 }).catch(() => notes.push(`${theme} escrow: no foot`));
      await crop(page, `p3-escrow-${theme}.png`, ["#list .list-head", "#list .lg-tw", "#list .pager"], "Publisher, Escrow: All's five columns; the failed deposit and withdrawal request (Failed, —), the two real deposits; the statement foot unchanged and closing (Deposited +1,004 · Fees paid for 109 settlements −545.195 · Escrow now 458.805 TIA)");
      await ctx.close();
    }

    // ---- V1. validator: the panels unchanged, the history area's tabs, Endpoint history open
    {
      const { ctx, page } = await open(browser, theme);
      await go(page, `/validator/?addr=${VAL}&tab=endpoints`, ".eh-t tbody tr");
      await crop(page, `v1-history-${theme}.png`, [".vd-stat", "#evidence .list-head", "#evidence .vd-pager"],
        "Validator Unity Nodes: the panels unchanged; under them the tabs Latest checks | Endpoint history 3, Endpoint history open: Time (UTC) · Block · TX hash · Action · Status · Endpoint. The MOCK failed row (Change requested · Failed · 203.0.113.10:7980 quiet), two real Changed rows (the newer marked current) and the host before Tensile's record", { t: 12 });
      if (dk) await full(page, `v1-full-${theme}.png`, "Validator page, whole, Endpoint history open, for context");
      await reveal(page, ["#evidence .list-head", "#evidence .vd-pager"]);
      await tip(page, ".eh-t tr.xf .st.f", theme);
      await crop(page, `v1-tip-failed-${theme}.png`, ["#evidence .lg-tw", ".shot-tip"], "Hover on Failed: \"Failed: Invalid validator. The endpoint did not change.\"", { t: 8, r: 20, b: 8, l: 20 });
      await untip(page);
      await tip(page, ".eh-t tr.xf .rq", theme);
      await crop(page, `v1-tip-req-${theme}.png`, ["#evidence .lg-tw", ".shot-tip"], "Hover on the requested address: \"Requested; the endpoint did not change.\"", { t: 8, r: 20, b: 8, l: 20 });
      await untip(page);
      await ctx.close();
    }
    // the tabs as the page opens: Latest checks picked, Endpoint history beside it
    {
      const { ctx, page } = await open(browser, theme);
      await go(page, `/validator/?addr=${VAL}`, ".vr-t tbody tr.row");
      await crop(page, `v1-tabs-checks-${theme}.png`, ["#evidence .list-head", row("#evidence .vr-t", 4)], "The history area as the page opens: Latest checks picked (its filter beside it, its table unchanged), Endpoint history 3 beside it, quieter", { b: 0 });
      await ctx.close();
    }

    // ---- T1–T6. the transaction page
    const T = [
      ["t1-blob-payment", "5da67b2a8865c75a572f5abb3070a2d3377a23baf371f705e1db1b312e21754e", "Blob payment, success: Publisher, Blob and Namespace links, Gas 219,118 used of 400,000; Blob size, Fee paid 3.575 TIA from escrow · settled, Transaction fee 0.008 TIA; its message"],
      ["t2-deposit", "7f7e66d16ec7337c82ffc65148bf3e42bef39b2adef3f930862455494ddc3d89", "Deposit, success: Amount +1,000 TIA into the escrow; Transaction fee 0.004 TIA"],
      ["t3-withdrawal-failed", "a4bd0b1855044d09c2f84ae43f6504d9365a470810b6f67d30a6ec11d2afdb92", "Withdrawal request, failed: Requested 1,000,000,000 TIA (not bold) not withdrawn · Escrow Unchanged · Transaction fee 0.0008 TIA; the error (Insufficient funds, sdk 5, the raw log); MsgRequestWithdrawal failed"],
      ["t4-endpoint", "e59c8ec3e5538f64f442e968795dd2c1159636ecf9f90d4698317f946374adcf", "Endpoint registration, success: Validator Unity Nodes; Action Changed; Endpoint 89.40.226.146:7980 → 89.40.226.218:7980; Transaction fee 0.002 TIA"],
      ["t5-endpoint-failed", "fb27dda8abf009db6512800b74898bc60410e1139a7fbb8f49e4ff30d3226cdb", "Endpoint registration, failed (MOCK signer): Requested 203.0.113.10:7980 (quiet) · Endpoint Unchanged, 89.40.226.218:7980 stayed registered; Invalid validator and its raw error"],
    ];
    for (const [name, hash, shows] of T) {
      const { ctx, page } = await open(browser, theme);
      await go(page, `/tx/?hash=${hash}`, ".tx-m tbody tr");
      await crop(page, `${name}-${theme}.png`, [".bd-title", ".tx-ms"], shows);
      if (name === "t3-withdrawal-failed") {
        if (dk) await full(page, `t3-full-${theme}.png`, "Transaction page, whole (a failed withdrawal request), for context");
        await reveal(page, [".bd-title", ".tx-top"]);
        await hide(page, ".tx-err, .tx-ms, footer");
        await tip(page, ".tx-top .bd-figs dd:last-of-type b[title]", theme);
        await crop(page, `t3-tip-fee-${theme}.png`, [".tx-top", ".shot-tip"], "Hover on the transaction fee's figure: the chain's own, \"800 utia\"", { t: 16, r: 20, b: 20, l: 20 });
        await untip(page);
        await tip(page, ".tx-top .bd-figs dt:last-of-type", theme, "start");
        await crop(page, `t3-tip-label-${theme}.png`, [".tx-top", ".shot-tip"], "Hover on Transaction fee: paid from the fee payer's bank balance, never the escrow; it was taken, so this transaction cannot run again", { t: 16, r: 20, b: 20, l: 20 });
        await hide(page, ".tx-err, .tx-ms, footer", false);
        await untip(page);
      }
      await ctx.close();
    }
    {
      const { ctx, page } = await open(browser, theme);
      await go(page, `/blob/?tx=A4BD0B1855044D09C2F84AE43F6504D9365A470810B6F67D30A6EC11D2AFDB92`, ".tx-m tbody tr");
      await crop(page, `t6-blob-tx-${theme}.png`, [".bd-title", ".tx-ms"], "The old failed address /blob/?tx=A4BD0B18… renders the same transaction page");
      await ctx.close();
    }

    // ---- G1. the blob page, kept: Gas left, both fees right, every amount in TIA; the transaction hash now a link
    {
      const { ctx, page } = await open(browser, theme);
      await go(page, `/blob/?hash=${BLOB}`, ".bd-figs");
      await crop(page, `g1-blob-${theme}.png`, [".bd-title", ".bd-top"], "Blob page as round 1's: Gas 219,118 used of 400,000 under the Transaction (its hash now opens the transaction page); Fee paid 3.575 TIA from escrow · settled, and under it Transaction fee 0.008 TIA from the bank balance", { t: 24, b: 14 });
      if (dk) await full(page, `g1-full-${theme}.png`, "Blob page, whole, for context");
      await reveal(page, [".bd-title", ".bd-top"]);
      await hide(page, ".bd-under, .bd-full, footer");
      await tip(page, ".bd-figs dd b[title]", theme);
      await crop(page, `g1-tip-fee-${theme}.png`, [".bd-top", ".shot-tip"], "Hover on the transaction fee's figure: \"8,000 utia\"", { t: 16, r: 20, b: 20, l: 20 });
      await hide(page, ".bd-under, .bd-full, footer", false);
      await untip(page);
      await ctx.close();
    }
  }

  await browser.close();
  server.close();
  fs.writeFileSync(path.join(out, "shots.json"), JSON.stringify(shots, null, 1) + "\n");
  fs.writeFileSync(path.join(out, "notes.txt"), notes.join("\n") + "\n");
  console.log(fs.readdirSync(out).join("\n"));
  if (notes.length) console.log(notes.join("\n"));
})().catch((e) => { console.error(e); process.exit(1); });
