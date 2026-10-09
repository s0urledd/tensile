// design-shots.cjs <out dir> <shots dir>: serves the static site from <out dir>, passes /api/* through to the live API,
// and photographs the failed-transaction follow-ups at 1440 in both themes. The browser's own requests to /api/v1/ are
// read live and given the keys the proposal adds (mock data, real Mocha values except rows marked STUB or MOCK) before
// the page sees them. Runs only in the Design shots workflow.
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

// ---------------------------------------------------------------- the mock data (proposal §6.3)
const PUB = "celestia1jw8afsj3j0c23fxs09nu8pq5asxwes5e3kkxdx";
const VAL = "celestiavaloper1l0smvx0aaguy64zg8kz9hf0keqptngmp5qrrvj";
const BLOB = "ba80a76e14c028e71ed777aa85b6f6ba3a3ca52916ededd5a9a8a61671c6c496";
const FAILED_TX = "A4BD0B1855044D09C2F84AE43F6504D9365A470810B6F67D30A6EC11D2AFDB92";

const FAILED_DEPOSIT = { kind: "deposit", failed: true, height: 1509598, tx_index: 0, msg_index: 0, time: "2026-10-08T23:17:45.331354Z", tx_hash: "f5ac69d972e0d707e6f535e92912c8ee6a6ddbf8c56ce920c6751764cbb1581b", requested: "1utia", requested_utia: 1, reason: "Out of gas" };
const FAILED_WITHDRAWAL = { kind: "withdrawal_request", failed: true, height: 1509587, tx_index: 0, msg_index: 0, time: "2026-10-08T23:17:14.030516Z", tx_hash: "a4bd0b1855044d09c2f84ae43f6504d9365a470810b6f67d30a6ec11d2afdb92", requested: "1000000000000000utia", requested_utia: 1000000000000000, reason: "Insufficient funds" };
// STUB: a failed blob payment between the blobs at #1,497,140 and #1,497,108
const FAILED_PFF = { kind: "settlement", failed: true, height: 1497121, tx_index: 1, msg_index: 0, time: "2026-10-08T13:25:05.700Z", tx_hash: "9b3e7a41d0c25f86e4a7b1d9c03f5e2a8d6b4c1f7e9a0d3b5c8f2e6a1d4b7c90", promise_hash: "4f0a9c2e7b1d5a83c6e9f2b4d7a0c3e5f8b1d4a7c0e3f6b9d2a5c8e1f4b7a0d3", namespace: "00000000000000000000000000000000000000000074656e73696c6500", blob_size: 4456448, reason: "Invalid request" };
const RECENT_MOVES = [
  { kind: "deposit", height: 1366494, tx_index: 0, msg_index: 0, time: "2026-10-04T05:51:46.491278Z", tx_hash: "7f7e66d16ec7337c82ffc65148bf3e42bef39b2adef3f930862455494ddc3d89", amount_utia: 1000000000,
    tx_cost: { gas_wanted: 200000, gas_used: 74215, fee: "4000utia", fee_payer: PUB, messages: 1 } },
  { kind: "deposit", height: 1107337, tx_index: 0, msg_index: 0, time: "2026-09-25T16:10:56.562358Z", tx_hash: "b241a96f9f221d4e9f03205ada95a5587b3f702c5345ad7b16c3c62da16d8f4c", amount_utia: 4000000,
    tx_cost: { gas_wanted: 200000, gas_used: 86844, fee: "4000utia", fee_payer: PUB, messages: 1 } },
];
const OPERATOR_ACCOUNT = "celestia1l0smvx0aaguy64zg8kz9hf0keqptngmp3lp665";
// MOCK failed row: built from the real failed set-host FB27DDA8…, whose signer is not a validator
const EH_FAILED = { outcome: "failed", height: 1509592, tx_index: 0, time: "2026-10-08T23:17:28.23537Z", tx_hash: "fb27dda8abf009db6512800b74898bc60410e1139a7fbb8f49e4ff30d3226cdb", host: "203.0.113.10:7980", reason: "Invalid validator" };
const EH_CHANGED_2 = { outcome: "changed", height: 1496499, tx_index: 0, time: "2026-10-08T12:55:34.318488Z", tx_hash: "e59c8ec3e5538f64f442e968795dd2c1159636ecf9f90d4698317f946374adcf", host: "89.40.226.218:7980", previous_host: "89.40.226.146:7980",
  tx_cost: { gas_wanted: 200000, gas_used: 54455, fee: "2000utia", fee_payer: OPERATOR_ACCOUNT, messages: 1 } };
const EH_CHANGED_1 = { outcome: "changed", height: 1253828, tx_index: 0, time: "2026-09-30T12:17:39.784839Z", tx_hash: "ed68446c853e334de84e6fe95873a6d9de16f2bfdc10d96481464093e8254c52", host: "89.40.226.146:7980", previous_host: "149.86.227.11:7980",
  tx_cost: { gas_wanted: 78351, gas_used: 54455, fee: "5000utia", fee_payer: OPERATOR_ACCOUNT, messages: 1 } };
const EH_REAL = [EH_FAILED, EH_CHANGED_2, EH_CHANGED_1, { outcome: "before_record", host: "149.86.227.11:7980" }];
// the state sheet: STUB rows around the real ones, seven in all
const EH_SHEET = [
  EH_FAILED,
  { outcome: "same", height: 1503210, tx_index: 1, time: "2026-10-08T18:40:11.204117Z", tx_hash: "3c81f0a9d27e45b6c90d1e8a7f3b2c4d5e6f708192a3b4c5d6e7f8091a2b3c4d", host: "89.40.226.218:7980",
    tx_cost: { gas_wanted: 200000, gas_used: 54455, fee: "2000utia", fee_payer: OPERATOR_ACCOUNT, messages: 1 } },
  EH_CHANGED_2,
  { outcome: "failed", height: 1401877, tx_index: 3, time: "2026-10-05T09:02:51.660021Z", tx_hash: "77d0e2b94c1a3f5e6d8b0a2c4e6f8a1b3c5d7e9f0a2b4c6d8e0f1a3b5c7d9e1f", host: "89.40.226.146:7981", reason: "Insufficient funds", other_message_failed: true },
  EH_CHANGED_1,
  { outcome: "after_gap", height: 1180402, host: "149.86.227.11:7980", previous_host: "149.86.227.9:7980" },
  { outcome: "before_record", host: "149.86.227.9:7980" },
];
const BLOB_COST = { gas_wanted: 400000, gas_used: 219118, fee: "8000utia", fee_payer: PUB, messages: 1 };
const STUB_PAYER = "celestia1qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqzzzzzz";

/** the live answer with the proposal's keys added; mode picks the state a shot shows */
function mock(pathname, q, j, mode) {
  if (pathname === "/api/v1/blobs" && q.get("publisher") === PUB && q.get("with_escrow") === "1" && !q.get("namespace")) {
    const first = (Number(q.get("offset")) || 0) === 0;
    return { ...j, escrow: first ? [FAILED_DEPOSIT, FAILED_WITHDRAWAL, FAILED_PFF] : [], escrow_newer: first ? 0 : 3, escrow_total: 5, escrow_more: 0 };
  }
  if (pathname === `/api/v1/publishers/${PUB}`) return { ...j, recent_moves: RECENT_MOVES, recent_failed: [FAILED_DEPOSIT, FAILED_WITHDRAWAL] };
  if (pathname.startsWith("/api/v1/validators/") && j && j.validator && j.validator.operator_address === VAL) {
    return { ...j, endpoint_history: mode.eh === "sheet" ? EH_SHEET : EH_REAL };
  }
  if (pathname === `/api/v1/blobs/${BLOB}`) {
    if (mode.cost === "none") return j;
    return { ...j, tx_cost: mode.cost === "other" ? { ...BLOB_COST, fee_payer: STUB_PAYER } : BLOB_COST };
  }
  return j;
}

// ---------------------------------------------------------------- photographing
const notes = [];
const shots = [];
const W = 1440, H = 1300;

async function open(browser, theme, mode = {}) {
  const ctx = await browser.newContext({ viewport: { width: W, height: H }, deviceScaleFactor: 2, colorScheme: theme });
  await ctx.addInitScript((t) => { try { localStorage.setItem("theme", t); } catch {} }, theme);
  await ctx.route("**/api/v1/**", async (route) => {
    const u = new URL(route.request().url());
    let res;
    try { res = await route.fetch(); } catch (e) { return route.abort(); }
    const type = res.headers()["content-type"] || "";
    if (!res.ok() || !type.includes("json")) return route.fulfill({ response: res });
    let j;
    try { j = await res.json(); } catch { return route.fulfill({ response: res }); }
    return route.fulfill({ response: res, json: mock(u.pathname, u.searchParams, j, mode) });
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
    if (r) rs.push(r);
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

/**
 * the elements' area, with some room around it, from the window as it is (a card or a tip that is open stays open:
 * nothing scrolls while one is; the steps that open one reveal the area first)
 */
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
  await page.screenshot({ path: path.join(out, file), fullPage: true, animations: "disabled" });
  shots.push({ file, shows });
}

/**
 * the browser's own tooltip for an element's title, drawn where the browser puts it (under the pointer): a headless
 * browser paints no native tooltip, so the shot draws it in the platform's plain style
 */
async function tip(page, sel, theme) {
  const el = page.locator(sel).first();
  await el.hover();
  await page.waitForTimeout(250);
  await page.evaluate(({ sel, dark }) => {
    const e = document.querySelector(sel);
    const t = e && (e.getAttribute("title") || e.closest("[title]")?.getAttribute("title"));
    if (!t) return;
    const r = e.getBoundingClientRect();
    const d = document.createElement("div");
    d.className = "shot-tip";
    d.textContent = t;
    Object.assign(d.style, {
      position: "fixed", left: `${Math.round(r.left + Math.min(r.width / 2, 14))}px`, top: `${Math.round(r.bottom + 18)}px`, zIndex: 99,
      maxWidth: "480px", padding: "4px 8px", font: "12px/1.35 system-ui, -apple-system, 'Segoe UI', sans-serif",
      color: dark ? "#f2f2f2" : "#1d1d1d", background: dark ? "#3b3b3b" : "#ffffff", border: `1px solid ${dark ? "#5c5c5c" : "#a0a0a0"}`,
      borderRadius: "3px", boxShadow: "0 2px 6px rgba(0,0,0,.22)", whiteSpace: "normal", pointerEvents: "none",
    });
    document.body.appendChild(d);
  }, { sel, dark: theme === "dark" });
}
const untip = (page) => page.evaluate(() => document.querySelectorAll(".shot-tip").forEach((d) => d.remove()));

/** the n-th row of the list's body (1-based) as a selector */
const row = (scope, n) => `${scope} tbody tr:nth-child(${n})`;

(async () => {
  await new Promise((ok) => server.listen(4173, "127.0.0.1", ok));
  fs.mkdirSync(out, { recursive: true });
  const browser = await chromium.launch({ executablePath: process.env.CHROME || "/usr/bin/google-chrome", args: ["--hide-scrollbars"] });
  const top = (page) => page.evaluate(() => scrollTo(0, 0));

  for (const theme of ["dark", "light"]) {
    const dk = theme === "dark";

    // ---- P1, P3. publisher: All, page 1, the failed rows among the blobs (B, recommended, then A), and their hovers
    for (const v of ["b", "a"]) {
      const { ctx, page } = await open(browser, theme);
      await go(page, `/publisher/?addr=${PUB}${v === "a" ? "&variant=a" : ""}`, "#list tr.xf");
      // cut at a row's own line, never through the next row
      await crop(page, `p1-all-${v}-${theme}.png`, ["#list .list-head", row("#list", 8)],
        v === "b"
          ? "Publisher, All, page 1, B (recommended): the failed deposit and withdrawal request above the newest blob, the STUB failed blob payment between #1,497,140 and #1,497,108; Amount a dash, the request in words under the reason"
          : "Publisher, All, page 1, A (alternative): the same rows, the qualifier ends at the reason and the request is struck in the Amount column, unsigned", { b: 0 });
      if (v === "b") {
        if (dk) { await top(page); await full(page, `p1-full-${theme}.png`, "Publisher page, whole (B), for context"); }
        await reveal(page, ["#list .list-head", row("#list", 5)]);
        await tip(page, `${row("#list", 2)} .xs`, theme);
        await crop(page, `p3-tip-failed-${theme}.png`, ["#list thead", row("#list", 4), ".shot-tip"], "Hover on \"Failed\" in a failed row: the failed page's own words", 20);
        await untip(page);
        await tip(page, `${row("#list", 2)} td.c-fee .xd`, theme);
        await crop(page, `p3-tip-dash-${theme}.png`, ["#list thead", row("#list", 4), ".shot-tip"], "Hover on the Amount dash of a failed row (B): nothing moved, the escrow is as it was", 20);
        await untip(page);
        await reveal(page, ["#list .pager"], 140);
        await tip(page, "#list .pager .count span[title]", theme);
        await crop(page, `p1-pager-${theme}.png`, ["#list .pager", ".shot-tip"], "Publisher, All: the pager counts blobs and escrow rows together; its hover splits them (109 blob settlements, 2 escrow movements and 3 failed transactions)", 20);
        await untip(page);
      } else {
        await reveal(page, ["#list .list-head", row("#list", 5)]);
        await tip(page, `${row("#list", 2)} td.c-fee s`, theme);
        await crop(page, `p3-tip-struck-${theme}.png`, ["#list thead", row("#list", 4), ".shot-tip"], "Hover on the struck request (A): requested, not moved", 20);
        await untip(page);
      }
      await ctx.close();
    }

    // ---- P2, P3. publisher: Escrow, the statement with its failed rows and its foot (B, then A); the card on "Deposit"
    for (const v of ["b", "a"]) {
      const { ctx, page } = await open(browser, theme);
      await go(page, `/publisher/?addr=${PUB}&kind=escrow${v === "a" ? "&variant=a" : ""}`, "#list .pb-st tr.xf");
      await crop(page, `p2-escrow-${v}-${theme}.png`, ["#list .list-head", "#list .pb-st", "#list .pager"],
        `Publisher, Escrow, ${v === "b" ? "B" : "A"}: the failed deposit and withdrawal request, then the two deposits; the foot unchanged (Deposited +1,004 · Fees paid for 109 settlements −545.195 · Escrow now 458.805 TIA)`);
      if (v === "b") {
        await reveal(page, ["#list .list-head", "#list .pb-st"]);
        await page.locator("#list .pb-st button.txw").first().click();
        await page.waitForSelector(".info-pop .txc", { timeout: 5000 }).catch(() => notes.push(`${theme}: the Deposit card did not open`));
        await page.waitForTimeout(300);
        await crop(page, `p3-card-deposit-${theme}.png`, ["#list .pb-st thead", row("#list .pb-st", 4), ".info-pop"], "The card on a successful Deposit's kind: its transaction (copyable), Gas 74,215 used of 200,000, Transaction fee Paid 4,000 utia from the bank balance", 20);
        await page.keyboard.press("Escape");
      }
      await ctx.close();
    }

    // ---- V1, V2. validator: Endpoint history between the figures and the latest checks (recommended), its hovers
    {
      const { ctx, page } = await open(browser, theme);
      await go(page, `/validator/?addr=${VAL}`, ".eh-t tbody tr");
      await crop(page, `v1-history-${theme}.png`, [".vd-stat", "#endpoints", "#evidence .list-head"],
        "Validator Unity Nodes, Endpoint history between the stat panel and Latest checks (recommended): the MOCK failed row (from the real failed set-host FB27DDA8…, whose signer is no validator), its two real changes and the host before Tensile's record");
      await crop(page, `v1-rows-${theme}.png`, ["#endpoints"], "Endpoint history alone: each verb in one slot so the addresses line up; the old address quiet, the new one in weight; the failure's request never bold", 20);
      if (dk) { await top(page); await full(page, `v1-full-${theme}.png`, "Validator page, whole, Endpoint history between the panel and Latest checks, for context"); }
      await reveal(page, ["#endpoints"], 60);
      await page.locator("#endpoints button.eh-v").first().click();
      await page.waitForSelector(".info-pop .txc", { timeout: 5000 }).catch(() => notes.push(`${theme}: the Changed card did not open`));
      await page.waitForTimeout(300);
      await crop(page, `v2-card-changed-${theme}.png`, ["#endpoints .lg-tw", ".info-pop"], "The card on \"Changed\": Gas 54,455 used of 200,000, Transaction fee Paid 2,000 utia from the bank balance (the operator's own account)", 20);
      await page.keyboard.press("Escape");
      await page.mouse.move(5, 5);
      await tip(page, "#endpoints tr.xf .eh-v.f", theme);
      await crop(page, `v2-tip-failed-${theme}.png`, ["#endpoints .lg-tw", ".shot-tip"], "Hover on \"Failed\" in the Endpoint history: the failed page's own words", 20);
      await untip(page);
      await tip(page, `${row("#endpoints", 2)} .cp`, theme);
      await crop(page, `v2-tip-copy-${theme}.png`, ["#endpoints .lg-tw", ".shot-tip"], "Hover on the copy mark after a transaction hash: \"Copy the transaction hash\" (it copies the full upper-case hash)", 20);
      await untip(page);
      await ctx.close();
    }
    // the alternative placement, at the page's end
    if (dk) {
      const { ctx, page } = await open(browser, theme);
      await go(page, `/validator/?addr=${VAL}&variant=end`, ".eh-t tbody tr");
      await crop(page, `v1-end-${theme}.png`, ["#evidence tbody tr:nth-last-child(3)", "#endpoints"], "Alternative placement: Endpoint history at the page's end, under the 50 rows of Latest checks (their last three shown)", 24);
      await top(page);
      await full(page, `v1-end-full-${theme}.png`, "Validator page, whole, Endpoint history at the end (alternative), for context");
      await ctx.close();
    }
    // V3. the state sheet: a same-address registration, a failure whose failing message was another, a record gap (amber)
    {
      const { ctx, page } = await open(browser, theme, { eh: "sheet" });
      await go(page, `/validator/?addr=${VAL}`, ".eh-t tbody tr");
      await crop(page, `v3-sheet-${theme}.png`, ["#endpoints"], "State sheet (STUB rows around the real ones): Registered again · same address, a failure whose failing message was another one, \"Show all 7\"", 20);
      await page.locator("#endpoints .eh-more button").click();
      await page.waitForTimeout(300);
      await reveal(page, ["#endpoints"], 20);
      await page.locator("#endpoints .eh-pre button.warn").first().hover();
      await page.waitForTimeout(300);
      await crop(page, `v3-sheet-all-${theme}.png`, ["#endpoints", ".warn-tip"], "State sheet opened: all seven, the change made in a record gap with its amber dot and its words, the host before Tensile's record, \"Show fewer\"", 20);
      await ctx.close();
    }

    // ---- G1, G2. blob page: Gas, Blob fee from escrow, Transaction fee from the bank balance
    for (const cost of ["own", "other", "none"]) {
      const { ctx, page } = await open(browser, theme, { cost });
      await go(page, `/blob/?hash=${BLOB}`, ".bd-figs");
      const what = {
        own: "Blob page: Gas 219,118 used of 400,000 under the settlement's transaction; Blob fee 3.575 TIA from escrow · settled, and under it Transaction fee Paid 8,000 utia from the bank balance",
        other: "Blob page with a STUB fee payer: Transaction fee Paid 8,000 utia, paid by celestia1…zzzz (the whole address on hover)",
        none: "Blob page with no cost on record (anything before the deploy): Gas and Transaction fee read not recorded",
      }[cost];
      await crop(page, `g${cost === "own" ? 1 : 2}-blob-${cost}-${theme}.png`, [".bd-title", ".bd-top"], what, { t: 24, b: 14 });
      if (cost === "own" && dk) { await top(page); await full(page, `g1-full-${theme}.png`, "Blob page, whole, for context"); }
      await ctx.close();
    }

    // ---- G3. the failed page, as live: the format the success details share
    {
      const { ctx, page } = await open(browser, theme);
      await go(page, `/blob/?tx=${FAILED_TX}`, ".bd-err");
      await crop(page, `g3-failed-${theme}.png`, [".bd-title", ".bd-top"], "Failed page A4BD0B18… as live: Gas 50,219 used of 200,000; Transaction fee Paid 800 utia, the format the success details share", { t: 24, b: 14 });
      if (dk) { await top(page); await full(page, `g3-full-${theme}.png`, "Failed page, whole, for context"); }
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
