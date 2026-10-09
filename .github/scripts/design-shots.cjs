// design-shots.cjs <out dir>: a read-only audit of the LIVE site (https://tensile.huginn.tech), no local server, no
// mocks. Every page below at 1440x1100 and 375x800, dark and light: waits for real content (no skeleton rows, no
// placeholders, no "Loading…"), then records console and page errors, failed or 4xx/5xx requests, sideways scroll,
// clipped table heads and cells, overlapping text, and the words undefined/NaN/null/[object Object] on screen; saves a
// full-page shot and crops of the parts that changed. Then the header search with every identifier typed (no Enter),
// and the click-through checks. Everything goes to findings.json, a short summary to notes.txt. Sequential: one page at
// a time, a pause between loads.
const fs = require("fs");
const path = require("path");
const { chromium } = require(path.join(process.env.PW_DIR, "node_modules", "playwright-core"));

const SITE = "https://tensile.huginn.tech";
const out = path.resolve(process.argv[2] || "shots");
const SIZES = [
  { tag: "1440", width: 1440, height: 1100, dsf: 1, cap: 3 },
  { tag: "375", width: 375, height: 800, dsf: 2, cap: 4 },
];
const THEMES = ["dark", "light"];
const PAUSE = 400;

const TX = {
  blobpay: "1a0045c5c1b2d7fd6eda690aba6fe5b670cef2b00b7a83b422d328baa4aa4994",
  endpoint: "ce2921833517b34f1edf4b2817279e7b507edaebbb9d4caf1efcb8f077f51e47",
  deposit: "0b5273d779cec9d2fc9534077bd1ac45904ac7e82c9af6b9ecb98255756b0b30",
  withdrawal: "6382e0bd921962004e850459f4e075c82f0c14d3e45a93e17856edb62c048306",
  "failed-endpoint": "2f9bb8f9f45408918797f115ba51fc7cedb27bc6531ec371c08dec143e78f5de",
  "failed-withdrawal": "a4bd0b1855044d09c2f84ae43f6504d9365a470810b6f67d30a6ec11d2afdb92",
  "failed-deposit": "f5ac69d972e0d707e6f535e92912c8ee6a6ddbf8c56ce920c6751764cbb1581b",
  random: "7c3e9a51f0d24b86a1e5c9077d3b2f48e6a0c1d95b7f3e2a4c8d6b0f1e9a7c35",
};
const BLOB_ID = "AFlclaUTklUlJ/UkVLQQzGguN2V9Ci5dUhaDviID2jVm";
const PUB_A = "celestia1kh9j9zvpt3kw2ql5fy0wv8qz65h3vasrlzmvmw";
const PUB_B = "celestia1jw8afsj3j0c23fxs09nu8pq5asxwes5e3kkxdx";
const CHAINSAFE = "61c7225d947271edd34f5f17f309ef77393492ab";
const ITROCKET = "924090b949a3a3a43aee52c4dae342332c57b684";
const NEW_BLOB = "d3bb2769ff34b2e3f99cdea0f2e0a9d847d6082ed13cd6ce4dfa7681b0728280";

const findings = { started: new Date().toISOString(), discovered: {}, pages: [], search: [], clicks: [], errors: [] };
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function api(p) {
  const r = await fetch(`${SITE}/api${p}`, { headers: { "user-agent": "tensile-live-audit", accept: "application/json" } });
  if (!r.ok) throw new Error(`${p}: HTTP ${r.status}`);
  return r.json();
}

// ---------------------------------------------------------------- discovery (a few API reads, sequential)
async function discover() {
  const d = findings.discovered;
  try {
    let j = await api("/v1/blobs?limit=1&offset=8500");
    if (!j.blobs || !j.blobs.length) {
      const t = await api("/v1/blobs?limit=1");
      d.blobsTotal = t.total;
      j = await api(`/v1/blobs?limit=1&offset=${Math.max(0, (t.total || 1) - 1)}`);
    }
    const b = j.blobs[0];
    d.septBlob = { promise_hash: b.promise_hash, settlement_time: b.settlement_time, settlement_height: b.settlement_height, total: j.total };
  } catch (e) { findings.errors.push(`discover sept blob: ${e.message}`); }
  await sleep(PAUSE);
  try {
    const vs = (await api("/v1/validators?window=24h")).validators || [];
    d.validators = vs.length;
    // those with no endpoint registered first: the likeliest to have no endpoint history
    const cands = [...vs].sort((a, b) => (a.host ? 1 : 0) - (b.host ? 1 : 0)).slice(0, 14);
    for (const c of cands) {
      const addr = c.operator_address || c.address;
      await sleep(PAUSE);
      try {
        const v = await api(`/v1/validators/${encodeURIComponent(addr)}?window=24h`);
        const n = (v.endpoint_history || []).length;
        if (!n) { d.noEndpointValidator = { addr, moniker: c.moniker, host: c.host || "", address: c.address }; break; }
      } catch (e) { findings.errors.push(`discover validator ${addr}: ${e.message}`); }
    }
  } catch (e) { findings.errors.push(`discover validators: ${e.message}`); }
}

// ---------------------------------------------------------------- in-page checks
/** waits until the page shows real content: no skeleton rows, no placeholders, no "Loading…" in main */
async function settle(page, ms = 35000) {
  const t0 = Date.now();
  let st = null;
  for (;;) {
    st = await page.evaluate(() => {
      const vis = (e) => e.getClientRects().length > 0 && getComputedStyle(e).visibility !== "hidden";
      const main = document.querySelector("main");
      return {
        sk: [...document.querySelectorAll("tr.sk")].filter(vis).length,
        wait: main ? [...main.querySelectorAll(".wait")].filter(vis).length : -1,
        loading: /Loading…|Looking it up…/.test(main?.innerText || "Loading…"),
      };
    }).catch(() => ({ sk: -1, wait: -1, loading: true }));
    if (!st.sk && !st.wait && !st.loading) { st = null; break; }
    if (Date.now() - t0 > ms) break;
    await page.waitForTimeout(500);
  }
  await page.evaluate(() => document.fonts.ready).catch(() => {});
  await page.waitForTimeout(1200);
  return st;
}

function inspectInPage() {
  const W = innerWidth;
  const cs = new Map();
  const S = (e) => { let s = cs.get(e); if (!s) { s = getComputedStyle(e); cs.set(e, s); } return s; };
  const vis = (e) => { if (!e.getClientRects().length) return false; const s = S(e); return s.visibility !== "hidden" && s.display !== "none"; };
  const desc = (e) => {
    const parts = [];
    for (let n = e, k = 0; n && n !== document.body && k < 4; n = n.parentElement, k++) {
      const c = typeof n.className === "string" && n.className.trim() ? "." + n.className.trim().split(/\s+/).slice(0, 3).join(".") : "";
      parts.unshift(n.tagName.toLowerCase() + (n.id ? "#" + n.id : "") + c);
    }
    return parts.join(" > ");
  };
  const txt = (e) => (e.innerText || e.textContent || "").replace(/\s+/g, " ").trim().slice(0, 90);
  const r = { appliedTheme: document.documentElement.dataset.theme, at: location.pathname + location.search };

  // sideways scroll, and what stands past the window's right edge
  r.scrollWidth = document.documentElement.scrollWidth;
  r.sideways = r.scrollWidth > W;
  if (r.sideways) {
    r.sidewaysBy = [];
    for (const e of document.querySelectorAll("body *")) {
      if (r.sidewaysBy.length >= 12) break;
      const b = e.getBoundingClientRect();
      if (!b.width || b.right <= W + 1) continue;
      let inside = false;
      for (let p = e.parentElement; p && p !== document.body; p = p.parentElement) {
        if (S(p).overflowX !== "visible" && p.getBoundingClientRect().right <= W + 1) { inside = true; break; }
        if (p.getBoundingClientRect().right > W + 1) { inside = true; break; } // its parent is reported instead
      }
      if (!inside) r.sidewaysBy.push({ el: desc(e), right: Math.round(b.right), text: txt(e).slice(0, 50) });
    }
  }

  // clipped table heads and cells, and clipping boxes inside cells
  r.clipped = [];
  for (const cell of document.querySelectorAll("th, td")) {
    if (r.clipped.length > 80) break;
    if (!vis(cell) || cell.clientWidth === 0) continue;
    if (cell.scrollWidth > cell.clientWidth) r.clipped.push({ el: desc(cell), sw: cell.scrollWidth, cw: cell.clientWidth, text: txt(cell) });
    for (const e of cell.querySelectorAll("*")) {
      if (!(e instanceof HTMLElement) || !vis(e)) continue;
      const s = S(e);
      if (s.overflowX !== "visible" && e.clientWidth > 0 && e.scrollWidth > e.clientWidth) r.clipped.push({ inner: true, ellipsis: s.textOverflow === "ellipsis", el: desc(e), sw: e.scrollWidth, cw: e.clientWidth, text: txt(e) });
    }
  }

  // boxes in main that scroll sideways
  r.hscroll = [];
  for (const e of document.querySelectorAll("main *")) {
    if (!(e instanceof HTMLElement)) continue;
    const ox = S(e).overflowX;
    if ((ox === "auto" || ox === "scroll") && e.scrollWidth > e.clientWidth + 1 && vis(e)) r.hscroll.push({ el: desc(e), sw: e.scrollWidth, cw: e.clientWidth });
  }

  // the bad words, on screen and in hovers and links
  const BAD = /(^|[^A-Za-z_])(undefined|NaN|null|\[object Object\]|Infinity|Invalid Date)(?![A-Za-z_])/;
  r.badText = [];
  const walk = document.createTreeWalker(document.body, NodeFilter.SHOW_TEXT);
  for (let n = walk.nextNode(); n; n = walk.nextNode()) {
    const p = n.parentElement;
    if (!p || /^(SCRIPT|STYLE|NOSCRIPT)$/.test(p.tagName)) continue;
    if (BAD.test(n.nodeValue) && vis(p)) r.badText.push({ el: desc(p), text: n.nodeValue.trim().slice(0, 160) });
    if (r.badText.length > 30) break;
  }
  r.badAttr = [];
  for (const e of document.querySelectorAll("[title],[aria-label],[href]")) {
    for (const a of ["title", "aria-label", "href"]) {
      const v = e.getAttribute(a);
      if (v && BAD.test(v) && !(a === "href" && /^https?:\/\/(github|docs)/.test(v))) r.badAttr.push({ el: desc(e), attr: a, value: v.slice(0, 160) });
    }
    if (r.badAttr.length > 30) break;
  }

  // text over text: every text line's box, cut to the boxes that clip it, against every other
  const clipOf = new Map();
  const clipRect = (el) => {
    if (clipOf.has(el)) return clipOf.get(el);
    let c = { l: -1e9, t: -1e9, r: 1e9, b: 1e9 };
    const par = el.parentElement;
    if (par && par !== document.documentElement) c = { ...clipRect(par) };
    const s = S(el);
    if (s.overflowX !== "visible" || s.overflowY !== "visible") {
      const b = el.getBoundingClientRect();
      c = { l: Math.max(c.l, b.left), t: Math.max(c.t, b.top), r: Math.min(c.r, b.right), b: Math.min(c.b, b.bottom) };
    }
    clipOf.set(el, c);
    return c;
  };
  const hid = new Map();
  const hidden = (el) => {
    if (!el || el === document.documentElement) return false;
    if (hid.has(el)) return hid.get(el);
    const s = S(el);
    const h = s.display === "none" || s.visibility === "hidden" || +s.opacity === 0 || el.hidden || el.classList.contains("wait") || el.classList.contains("shot-tip") || hidden(el.parentElement);
    hid.set(el, h);
    return h;
  };
  const items = [];
  const w2 = document.createTreeWalker(document.body, NodeFilter.SHOW_TEXT);
  for (let n = w2.nextNode(); n && items.length < 5000; n = w2.nextNode()) {
    if (!n.nodeValue.trim()) continue;
    const p = n.parentElement;
    if (!p || /^(SCRIPT|STYLE|NOSCRIPT)$/.test(p.tagName) || hidden(p)) continue;
    const c = clipRect(p);
    const rg = document.createRange();
    rg.selectNodeContents(n);
    for (const q of rg.getClientRects()) {
      const b = { l: Math.max(q.left, c.l), t: Math.max(q.top, c.t), r: Math.min(q.right, c.r), b: Math.min(q.bottom, c.b) };
      if (b.r - b.l > 1 && b.b - b.t > 1) items.push({ n, p, b });
    }
  }
  r.overlaps = [];
  for (let i = 0; i < items.length && r.overlaps.length < 15; i++) {
    for (let j = i + 1; j < items.length; j++) {
      const A = items[i], B = items[j];
      if (A.n === B.n) continue;
      const ox = Math.min(A.b.r, B.b.r) - Math.max(A.b.l, B.b.l), oy = Math.min(A.b.b, B.b.b) - Math.max(A.b.t, B.b.t);
      if (ox > 2 && oy > 3) {
        r.overlaps.push({ a: desc(A.p), at: A.n.nodeValue.trim().slice(0, 40), b: desc(B.p), bt: B.n.nodeValue.trim().slice(0, 40), ox: Math.round(ox), oy: Math.round(oy), y: Math.round(A.b.t + scrollY) });
        if (r.overlaps.length >= 15) break;
      }
    }
  }

  // the tables' heads, and their copy marks by column
  r.tables = [...document.querySelectorAll("main table")].filter(vis).map((t) => {
    const rows = [...t.querySelectorAll("tbody tr")].filter((tr) => vis(tr) && !tr.classList.contains("sk"));
    const byCol = {};
    for (const b of t.querySelectorAll("tbody button.cp")) {
      const td = b.closest("td");
      const k = td ? td.className.split(" ")[0] || "?" : "?";
      byCol[k] = byCol[k] || { n: 0, visible: 0 };
      byCol[k].n++;
      if (vis(b)) byCol[k].visible++;
    }
    return { table: desc(t), heads: [...t.querySelectorAll("thead th")].filter(vis).map(txt).join(" | "), rows: rows.length, failedRows: rows.filter((x) => x.classList.contains("xf")).length, copyMarks: byCol };
  });
  // a definition with nothing in it
  r.emptyDd = [...document.querySelectorAll("main dd")].filter((d) => vis(d) && !txt(d) && !d.querySelector("svg,img,canvas")).map(desc).slice(0, 10);
  r.h1 = [...document.querySelectorAll("main h1")].map(txt).join(" / ");
  return r;
}

// ---------------------------------------------------------------- the pages
const click = (sel) => async (page) => { await page.locator(sel).first().click(); };
const clickText = (sel, text) => async (page) => { await page.locator(sel, { hasText: text }).first().click(); };

function pages() {
  const d = findings.discovered;
  const P = [
    { name: "home", url: "/", states: [{ s: "base", focus: [] }] },
    { name: "blobs", url: "/blobs/", states: [
      { s: "base", focus: ["#list"] },
      { s: "page2", focus: ["#list"], act: click('#list .pager button[aria-label="next page"]') },
      { s: "namespaces", focus: ["#list"], act: clickText("#list .tabs button", "Namespaces") },
    ] },
    { name: "blobs-blobid", url: `/blobs/?blob=${encodeURIComponent(BLOB_ID)}`, states: [{ s: "base", focus: ["#list"] }] },
    { name: "publishers", url: "/publishers/", states: [{ s: "base", focus: ["#list"] }] },
  ];
  for (const [tag, a] of [["pubA", PUB_A], ["pubB", PUB_B]]) {
    P.push({ name: `publisher-${tag}`, url: `/publisher/?addr=${a}`, states: [
      { s: "all", focus: [".pb-mast", "#list"] },
      { s: "blobs", focus: ["#list"], act: clickText(".pb-kinds button", "Blobs"), optional: ".pb-kinds button" },
      { s: "escrow", focus: ["#list"], act: clickText(".pb-kinds button", "Escrow"), optional: ".pb-kinds button" },
    ] });
  }
  for (const [tag, a] of [["chainsafe", CHAINSAFE], ["itrocket", ITROCKET]]) {
    P.push({ name: `validator-${tag}`, url: `/validator/?addr=${a}`, states: [
      { s: "checks", focus: ["#evidence"] },
      { s: "endpoints", focus: ["#evidence"], act: click("#tab-endpoints"), optional: "#tab-endpoints" },
    ] });
  }
  if (d.noEndpointValidator) P.push({ name: "validator-noep", url: `/validator/?addr=${encodeURIComponent(d.noEndpointValidator.addr)}`, states: [{ s: "base", focus: ["#evidence"] }] });
  P.push({ name: "blob-new", url: `/blob/?hash=${NEW_BLOB}`, states: [{ s: "base", focus: [".bd-mast", ".bd-top"] }] });
  if (d.septBlob) P.push({ name: "blob-sept", url: `/blob/?hash=${d.septBlob.promise_hash}`, states: [{ s: "base", focus: [".bd-mast", ".bd-top"] }] });
  for (const [k, h] of Object.entries(TX)) P.push({ name: `tx-${k}`, url: `/tx/?hash=${h}`, expect404: k === "random" ? [`/api/v1/txs/${h}`] : [], states: [{ s: "base", focus: ["main"] }] });
  P.push({ name: "blob-tx-failed-deposit", url: `/blob/?tx=${TX["failed-deposit"]}`, states: [{ s: "base", focus: ["main"] }] });
  P.push({ name: "api", url: "/api/", states: [{ s: "base", focus: [] }] });
  P.push({ name: "methodology", url: "/methodology/", states: [{ s: "base", focus: [] }] });
  return P;
}

// ---------------------------------------------------------------- driving
let bucket = null;
async function open(browser, size, theme, extra = {}) {
  const ctx = await browser.newContext({ viewport: { width: size.width, height: size.height }, deviceScaleFactor: size.dsf, colorScheme: theme, ...extra });
  await ctx.addInitScript((t) => { try { localStorage.setItem("theme", t); } catch {} }, theme);
  const page = await ctx.newPage();
  page.on("console", (m) => { if (bucket && (m.type() === "error" || m.type() === "warning")) bucket.console.push({ type: m.type(), text: m.text().slice(0, 300), at: m.location()?.url?.slice(0, 160) }); });
  page.on("pageerror", (e) => { if (bucket) bucket.pageErrors.push(String(e && e.message || e).slice(0, 400)); });
  page.on("requestfailed", (r) => { if (bucket) bucket.requests.push({ url: r.url().slice(0, 200), failure: r.failure()?.errorText || "failed", type: r.resourceType() }); });
  page.on("response", (r) => { if (bucket && r.status() >= 400) bucket.requests.push({ url: r.url().slice(0, 200), status: r.status(), type: r.request().resourceType() }); });
  page.on("request", (r) => { if (bucket && /\/api\//.test(r.url())) bucket.api.push(r.url().replace(SITE, "").slice(0, 160)); });
  return { ctx, page };
}

const newBucket = () => ({ console: [], pageErrors: [], requests: [], api: [] });

async function chunks(page, base, sels, size) {
  if (!sels.length) return [];
  const b = await page.evaluate((sels) => {
    let u = null;
    for (const s of sels) {
      const e = document.querySelector(s);
      if (!e) continue;
      const r = e.getBoundingClientRect();
      const x = { l: r.left + scrollX, t: r.top + scrollY, r: r.right + scrollX, b: r.bottom + scrollY };
      u = u ? { l: Math.min(u.l, x.l), t: Math.min(u.t, x.t), r: Math.max(u.r, x.r), b: Math.max(u.b, x.b) } : x;
    }
    return u;
  }, sels);
  if (!b) return [];
  const files = [];
  const x = Math.max(0, Math.floor(b.l - 12));
  const w = Math.min(size.width - x, Math.ceil(b.r - b.l + 24));
  for (let y = Math.max(0, Math.floor(b.t - 12)), k = 0; y < b.b && k < size.cap; y += size.height, k++) {
    const h = Math.min(size.height, Math.ceil(b.b + 12 - y));
    if (h < 8) break;
    const f = `${base}-c${k + 1}.png`;
    try { await page.screenshot({ path: path.join(out, f), fullPage: true, clip: { x, y, width: w, height: h }, animations: "disabled" }); files.push(f); } catch (e) { findings.errors.push(`${f}: ${e.message}`); }
  }
  return files;
}

async function record(page, P, st, size, theme, notSettled, extra = {}) {
  const base = `${P.name}-${st.s}-${size.tag}-${theme}`;
  let r;
  await page.evaluate(() => scrollTo(0, 0)).catch(() => {});
  await page.waitForTimeout(150);
  try { r = await page.evaluate(inspectInPage); } catch (e) { r = { error: e.message }; }
  const full = `${base}.jpg`;
  try { await page.screenshot({ path: path.join(out, full), fullPage: true, scale: "css", type: "jpeg", quality: 78, animations: "disabled" }); } catch (e) { findings.errors.push(`${full}: ${e.message}`); }
  const crops = await chunks(page, base, st.focus, size);
  if (size.tag === "1440" && theme === "dark") {
    const t = await page.evaluate(() => document.querySelector("main")?.innerText || "").catch(() => "");
    fs.mkdirSync(path.join(out, "texts"), { recursive: true });
    fs.writeFileSync(path.join(out, "texts", `${P.name}-${st.s}.txt`), t);
  }
  const b = bucket;
  const expect = P.expect404 || [];
  const reqs = b.requests.map((q) => ({ ...q, expected: expect.some((e) => q.url.includes(e)) }));
  findings.pages.push({ page: P.name, state: st.s, url: P.url, size: size.tag, theme, notSettled, ...extra, shots: [full, ...crops], consoleErrors: b.console, pageErrors: b.pageErrors, requests: reqs, api: [...new Set(b.api)], ...r });
}

async function runPages(browser) {
  const P = pages();
  for (const size of SIZES) {
    for (const theme of THEMES) {
      const { ctx, page } = await open(browser, size, theme);
      for (const p of P) {
        bucket = newBucket();
        try {
          await page.goto(SITE + p.url, { waitUntil: "domcontentloaded", timeout: 60000 });
        } catch (e) { findings.errors.push(`${p.name} ${size.tag} ${theme} goto: ${e.message}`); continue; }
        let ns = await settle(page);
        for (let i = 0; i < p.states.length; i++) {
          const st = p.states[i];
          if (i > 0) {
            bucket = newBucket();
            if (st.optional && !(await page.locator(st.optional).count())) { findings.pages.push({ page: p.name, state: st.s, size: size.tag, theme, skipped: `no ${st.optional}` }); continue; }
            try { await st.act(page); } catch (e) { findings.errors.push(`${p.name}/${st.s} ${size.tag} ${theme} act: ${e.message}`); continue; }
            await page.waitForTimeout(300);
            ns = await settle(page);
          }
          await record(page, p, st, size, theme, ns);
        }
        await page.waitForTimeout(PAUSE);
      }
      await ctx.close();
    }
  }
  bucket = null;
}

// ---------------------------------------------------------------- the header search
function panelInPage() {
  const dd = document.querySelector(".hs-dd");
  if (!dd || dd.hidden) return { open: false };
  const desc = (e) => e.tagName.toLowerCase() + (typeof e.className === "string" && e.className ? "." + e.className.trim().split(/\s+/).join(".") : "");
  const R = (b) => ({ l: Math.round(b.left), t: Math.round(b.top), r: Math.round(b.right), b: Math.round(b.bottom), w: Math.round(b.width), h: Math.round(b.height) });
  const ddr = dd.getBoundingClientRect();
  const items = [...dd.querySelectorAll(".hs-item")].map((it) => {
    const k = it.querySelector(".hs-k"), at = it.querySelector(".hs-at"), st = it.querySelector(".hs-st"), hash = it.querySelector(".hs-hash");
    const kr = k?.getBoundingClientRect(), ar = at?.getBoundingClientRect();
    return {
      text: it.innerText.replace(/\s+/g, " ").trim(), box: R(it.getBoundingClientRect()),
      timeWrapped: kr && ar ? ar.top >= kr.bottom - 1 : null,
      status: st ? st.innerText.trim() : null, statusClass: st ? st.className : null,
      hashCut: hash ? hash.scrollWidth > hash.clientWidth : null,
    };
  });
  const overflow = [];
  for (const e of dd.querySelectorAll("*")) {
    if (e.closest("svg") && e.tagName.toLowerCase() !== "svg") continue;
    const p = e.parentElement;
    const b = e.getBoundingClientRect(), pb = p.getBoundingClientRect();
    if (!b.width && !b.height) continue;
    if (b.right > pb.right + 0.5 || b.left < pb.left - 0.5) overflow.push({ el: desc(e), in: desc(p), box: R(b), parent: R(pb), text: (e.textContent || "").trim().slice(0, 40) });
    if (e instanceof HTMLElement && getComputedStyle(e).overflowX !== "visible" && e.scrollWidth > e.clientWidth + 0 && !e.classList.contains("hs-items")) overflow.push({ el: desc(e), clipped: `${e.scrollWidth} > ${e.clientWidth}`, ellipsis: getComputedStyle(e).textOverflow === "ellipsis", text: (e.textContent || "").trim().slice(0, 40) });
  }
  const items2 = dd.querySelector(".hs-items");
  return {
    open: true, panel: R(ddr), offWindow: ddr.left < 0 || ddr.right > innerWidth, sideways: document.documentElement.scrollWidth > innerWidth,
    listScrollsSideways: items2 ? items2.scrollWidth > items2.clientWidth : false,
    note: dd.querySelector(".hs-note")?.innerText || null, items, overflow,
  };
}

const QUERIES = [...Object.entries(TX).map(([k, v]) => ({ k: `tx-${k}`, v })), { k: "blobid", v: BLOB_ID }, { k: "publisher", v: PUB_A }];

async function runSearch(browser) {
  for (const size of SIZES) {
    for (const theme of THEMES) {
      const { ctx, page } = await open(browser, size, theme);
      bucket = newBucket();
      await page.goto(SITE + "/", { waitUntil: "domcontentloaded", timeout: 60000 });
      await settle(page);
      const inp = page.locator(".hs-field input");
      for (const q of QUERIES) {
        bucket = newBucket();
        try {
          await inp.click();
          await inp.fill("");
          await page.waitForTimeout(250);
          await inp.fill(q.v);
          const t0 = Date.now();
          let state = "wait";
          while (Date.now() - t0 < 30000) {
            state = await page.evaluate(() => {
              const dd = document.querySelector(".hs-dd");
              if (!dd || dd.hidden) return "closed";
              if (dd.querySelector(".hs-item")) return "rows";
              const n = dd.querySelector(".hs-note");
              if (n && !n.querySelector(".hs-wait")) return "note";
              return "wait";
            });
            if (state === "rows" || state === "note") break;
            await page.waitForTimeout(300);
          }
          await page.waitForTimeout(800);
          const res = await page.evaluate(panelInPage);
          const f = `search-${q.k}-${size.tag}-${theme}.png`;
          const clip = await page.evaluate(() => {
            const a = document.querySelector(".hs-field").getBoundingClientRect();
            const dd = document.querySelector(".hs-dd");
            const b = dd && !dd.hidden ? dd.getBoundingClientRect() : a;
            const l = Math.max(0, Math.min(a.left, b.left) - 16), t = Math.max(0, Math.min(a.top, b.top) - 16);
            return { x: l, y: t, width: Math.min(innerWidth - l, Math.max(a.right, b.right) + 16 - l), height: Math.min(innerHeight - t, Math.max(a.bottom, b.bottom) + 16 - t) };
          });
          await page.screenshot({ path: path.join(out, f), clip, animations: "disabled" });
          const expect = q.k === "tx-random" ? ["/api/v1/txs/"] : [];
          findings.search.push({ q: q.k, value: q.v, size: size.tag, theme, state, shot: f, ...res, consoleErrors: bucket.console, pageErrors: bucket.pageErrors, requests: bucket.requests.map((r) => ({ ...r, expected: expect.some((e) => r.url.includes(e)) && r.status === 404 })), api: [...new Set(bucket.api)] });
        } catch (e) { findings.errors.push(`search ${q.k} ${size.tag} ${theme}: ${e.message}`); }
        await page.waitForTimeout(PAUSE);
      }
      await inp.fill("").catch(() => {});
      await ctx.close();
    }
  }
  bucket = null;
}

// ---------------------------------------------------------------- click-throughs
async function runClicks(browser) {
  const size = SIZES[0];
  const { ctx, page } = await open(browser, size, "dark");
  await ctx.grantPermissions(["clipboard-read", "clipboard-write"], { origin: SITE }).catch(() => {});
  const C = (check, ok, detail) => findings.clicks.push({ check, ok, ...detail });
  const goSettle = async (u) => { bucket = newBucket(); await page.goto(SITE + u, { waitUntil: "domcontentloaded", timeout: 60000 }); await settle(page); };

  // a Blobs row opens its blob page
  try {
    await goSettle("/blobs/");
    const row = page.locator("#list tr.row:not(.xf):not(.sk)").first();
    const h = await row.getAttribute("data-h");
    await row.locator("td.c-sz").click();
    await page.waitForURL(/\/blob\/\?hash=/, { timeout: 15000 }).catch(() => {});
    await settle(page);
    const h1 = await page.locator("main h1").first().innerText().catch(() => "");
    C("Blobs row (size cell) opens its blob page", page.url().includes(`hash=${h}`), { url: page.url(), want: h, h1 });
  } catch (e) { C("Blobs row opens its blob page", false, { error: e.message }); }

  // a failed row in the Blobs list (first two pages) opens its transaction page
  try {
    let found = false;
    for (const u of ["/blobs/", "/blobs/?page=2", "/blobs/?page=3"]) {
      await goSettle(u);
      const xf = page.locator("#list tr.row.xf");
      if (await xf.count()) {
        const tx = (await xf.first().locator("td.c-b a").getAttribute("href")) || "";
        await xf.first().locator("td.c-sz").click();
        await page.waitForURL(/\/tx\/\?hash=/, { timeout: 15000 }).catch(() => {});
        await settle(page);
        const chip = await page.locator(".bd-chips .state").first().innerText().catch(() => "");
        C("Failed Blobs row opens /tx/", page.url().includes("/tx/?hash=") && page.url().includes(tx.split("hash=")[1] || "x"), { list: u, href: tx, url: page.url(), chip });
        found = true;
        break;
      }
      await page.waitForTimeout(PAUSE);
    }
    if (!found) C("Failed Blobs row opens /tx/", null, { note: "no failed row on Blobs pages 1-3" });
  } catch (e) { C("Failed Blobs row opens /tx/", false, { error: e.message }); }

  // the copy mark copies the whole transaction hash, and the publisher's
  try {
    await goSettle("/blobs/");
    const row = page.locator("#list tr.row:not(.xf):not(.sk)").first();
    const title = (await row.locator("td.c-b a").getAttribute("title")) || "";
    const want = (title.match(/Transaction ([0-9A-F]{64})/) || [])[1] || "";
    await row.locator("td.c-b button.cp").click();
    await page.waitForTimeout(300);
    const got = await page.evaluate(() => navigator.clipboard.readText()).catch((e) => `ERR ${e.message}`);
    const after = await row.locator("td.c-b button.cp").getAttribute("title");
    C("Blobs TX hash copy mark copies the hash", got === want, { want, got, titleAfter: after });
    await page.waitForTimeout(1500);
    const pt = await row.locator("td.c-p button.cp").count();
    if (pt) {
      await row.locator("td.c-p button.cp").click();
      await page.waitForTimeout(300);
      const g2 = await page.evaluate(() => navigator.clipboard.readText()).catch((e) => `ERR ${e.message}`);
      C("Blobs publisher copy mark copies a celestia1 address", /^celestia1[0-9a-z]{38}$/.test(g2), { got: g2 });
    } else C("Blobs publisher copy mark present", false, {});
  } catch (e) { C("Blobs copy marks", false, { error: e.message }); }

  // Endpoint history: a TX hash link opens /tx/, a failed row opens its failed transaction
  try {
    await goSettle(`/validator/?addr=${CHAINSAFE}`);
    await page.locator("#tab-endpoints").click();
    await page.waitForSelector(".eh-t tbody tr", { timeout: 15000 });
    await page.waitForTimeout(500);
    const rows = await page.evaluate(() => [...document.querySelectorAll(".eh-t tbody tr")].map((tr) => tr.innerText.replace(/\s+/g, " ").trim()));
    const link = page.locator(".eh-t td.c-x a").first();
    const href = await link.getAttribute("href");
    await link.click();
    await page.waitForURL(/\/tx\/\?hash=/, { timeout: 15000 }).catch(() => {});
    await settle(page);
    const chip = await page.locator(".bd-chips").first().innerText().catch(() => "");
    C("Endpoint history TX hash link opens /tx/", page.url().includes(href), { href, url: page.url(), chip: chip.replace(/\s+/g, " "), rows });
    // back: the tab the reader was on
    await page.goBack();
    await page.waitForTimeout(1500);
    await settle(page);
    const sel = await page.locator("#tab-endpoints").getAttribute("aria-selected").catch(() => null);
    C("Back from /tx/ returns to the Endpoint history tab", sel === "true", { ariaSelected: sel, url: page.url() });
    await goSettle(`/validator/?addr=${CHAINSAFE}`);
    await page.locator("#tab-endpoints").click();
    await page.waitForTimeout(800);
    const xf = page.locator(".eh-t tr.xf");
    if (await xf.count()) {
      const h2 = await xf.first().locator("td.c-x a").getAttribute("href");
      await xf.first().locator("td.c-ac").click();
      await page.waitForURL(/\/tx\/\?hash=/, { timeout: 15000 }).catch(() => {});
      await settle(page);
      const chip2 = await page.locator(".bd-chips").first().innerText().catch(() => "");
      C("Failed Endpoint history row opens its failed tx", page.url().includes(h2 || "x") && /Failed/.test(chip2), { href: h2, url: page.url(), chip: chip2.replace(/\s+/g, " ") });
    } else C("Failed Endpoint history row present (ChainSafe)", false, {});
  } catch (e) { C("Endpoint history clicks", false, { error: e.message }); }

  // a link with ?tab=endpoints opens on Endpoint history
  try {
    await goSettle(`/validator/?addr=${ITROCKET}&tab=endpoints`);
    const sel = await page.locator("#tab-endpoints").getAttribute("aria-selected").catch(() => null);
    const n = await page.locator(".eh-t tbody tr").count();
    C("?tab=endpoints opens on Endpoint history (ITRocket)", sel === "true" && n > 0, { ariaSelected: sel, rows: n });
  } catch (e) { C("?tab=endpoints", false, { error: e.message }); }

  // Latest checks: the TX hash link opens the blob
  try {
    await goSettle(`/validator/?addr=${CHAINSAFE}`);
    const a = page.locator(".vr-t td.c-b a").first();
    if (await a.count()) {
      const href = await a.getAttribute("href");
      await a.click();
      await page.waitForURL(/\/blob\/\?hash=/, { timeout: 15000 }).catch(() => {});
      C("Latest checks TX hash link opens the blob page", page.url().includes(href), { href, url: page.url() });
    } else C("Latest checks has a TX hash link", false, {});
  } catch (e) { C("Latest checks click", false, { error: e.message }); }

  // the blob payment's tx page links its blob
  try {
    await goSettle(`/tx/?hash=${TX.blobpay}`);
    const a = page.locator("a.tx-go").first();
    if (await a.count()) {
      const href = await a.getAttribute("href");
      await a.click();
      await page.waitForURL(/\/blob\/\?hash=/, { timeout: 15000 }).catch(() => {});
      await settle(page);
      C("Blob payment tx page's blob link opens the blob", page.url().includes(href), { href, url: page.url() });
    } else C("Blob payment tx page has a blob link", false, {});
  } catch (e) { C("tx blob link", false, { error: e.message }); }

  // a search row picked by click opens its page
  try {
    await goSettle("/");
    const inp = page.locator(".hs-field input");
    await inp.click();
    await inp.fill(TX["failed-endpoint"]);
    await page.waitForSelector(".hs-dd .hs-item", { timeout: 20000 });
    await page.locator(".hs-dd .hs-item").first().click();
    await page.waitForURL(/\/tx\/\?hash=/, { timeout: 15000 }).catch(() => {});
    await settle(page);
    const chip = await page.locator(".bd-chips").first().innerText().catch(() => "");
    C("Search row (failed endpoint registration) opens its /tx/ page", page.url().includes(TX["failed-endpoint"]), { url: page.url(), chip: chip.replace(/\s+/g, " ") });
  } catch (e) { C("Search row click", false, { error: e.message }); }

  await ctx.close();
  bucket = null;
}

// ---------------------------------------------------------------- the summary
function summarize() {
  const L = [];
  const real = (q) => !q.expected && !(q.failure === "net::ERR_ABORTED");
  for (const p of findings.pages) {
    if (p.skipped) { L.push(`SKIP ${p.page}/${p.state} ${p.size} ${p.theme}: ${p.skipped}`); continue; }
    const f = [];
    if (p.notSettled) f.push(`not settled ${JSON.stringify(p.notSettled)}`);
    if (p.error) f.push(`inspect error ${p.error}`);
    if (p.appliedTheme && p.appliedTheme !== p.theme) f.push(`theme ${p.appliedTheme}`);
    if (p.pageErrors?.length) f.push(`pageErrors ${p.pageErrors.length}`);
    const ce = (p.consoleErrors || []).filter((c) => c.type === "error");
    if (ce.length) f.push(`console ${ce.length}`);
    const rq = (p.requests || []).filter(real);
    if (rq.length) f.push(`requests ${rq.map((q) => `${q.status || q.failure} ${q.url.replace(SITE, "")}`).join(", ")}`);
    if (p.sideways) f.push(`SIDEWAYS ${p.scrollWidth}`);
    const cl = (p.clipped || []).filter((c) => !c.ellipsis);
    if (cl.length) f.push(`clipped ${cl.length}`);
    const el = (p.clipped || []).filter((c) => c.ellipsis);
    if (el.length) f.push(`ellipsis ${el.length}`);
    if (p.hscroll?.length) f.push(`hscroll ${p.hscroll.length}`);
    if (p.badText?.length) f.push(`badText ${p.badText.map((b) => b.text.slice(0, 40)).join(" | ")}`);
    if (p.badAttr?.length) f.push(`badAttr ${p.badAttr.length}`);
    if (p.overlaps?.length) f.push(`overlaps ${p.overlaps.length}`);
    if (p.emptyDd?.length) f.push(`emptyDd ${p.emptyDd.length}`);
    if (f.length) L.push(`${p.page}/${p.state} ${p.size} ${p.theme}: ${f.join("; ")}`);
  }
  for (const s of findings.search) {
    const f = [];
    if (s.state !== "rows" && s.state !== "note") f.push(`state ${s.state}`);
    if (s.offWindow) f.push("panel off window");
    if (s.sideways) f.push("page sideways");
    if (s.items?.some((i) => i.timeWrapped)) f.push(`time wrapped ${s.items.filter((i) => i.timeWrapped).length}/${s.items.length}`);
    if (s.overflow?.length) f.push(`overflow ${s.overflow.length}`);
    if (s.pageErrors?.length) f.push(`pageErrors ${s.pageErrors.length}`);
    const rq = (s.requests || []).filter((q) => !q.expected && q.failure !== "net::ERR_ABORTED");
    if (rq.length) f.push(`requests ${rq.length}`);
    L.push(`search ${s.q} ${s.size} ${s.theme}: ${s.state} ${s.items?.length ?? 0} rows${s.items?.[0] ? ` [${s.items[0].text.slice(0, 70)}]` : ""}${s.note ? ` note "${s.note.slice(0, 60)}"` : ""}${f.length ? " :: " + f.join("; ") : ""}`);
  }
  for (const c of findings.clicks) L.push(`click ${c.ok === true ? "OK  " : c.ok === null ? "N/A " : "FAIL"} ${c.check}${c.error ? `: ${c.error}` : ""}`);
  for (const e of findings.errors) L.push(`ERROR ${e}`);
  return L;
}

(async () => {
  fs.mkdirSync(out, { recursive: true });
  const save = () => fs.writeFileSync(path.join(out, "findings.json"), JSON.stringify(findings, null, 1));
  await discover();
  save();
  const browser = await chromium.launch({ executablePath: process.env.CHROME || "/usr/bin/google-chrome", args: ["--hide-scrollbars"] });
  try { await runPages(browser); } catch (e) { findings.errors.push(`pages: ${e.stack || e}`); }
  save();
  try { await runSearch(browser); } catch (e) { findings.errors.push(`search: ${e.stack || e}`); }
  save();
  try { await runClicks(browser); } catch (e) { findings.errors.push(`clicks: ${e.stack || e}`); }
  await browser.close();
  findings.finished = new Date().toISOString();
  save();
  const L = summarize();
  fs.writeFileSync(path.join(out, "notes.txt"), L.join("\n") + "\n");
  console.log(L.join("\n"));
})().catch((e) => { console.error(e); process.exit(1); });
