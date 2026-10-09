// design-shots.cjs <out dir> <shots dir>: serves the static site from <out dir>, passes /api/* through to the live API,
// and photographs how a failed blob payment would stand in the Blobs list, in three variants (?fv=a|b|c), at 1440 in
// both themes: the list's head and its first six rows, and the hover on Failed. The browser's reads of the list
// (/v1/blobs, first page, unfiltered) are the live answer with ONE failed blob payment set between its first and second
// rows: a STUB, see FAILED below. It also measures every column's slack in today's table (its widest content, the
// widest a column is sized for included) and checks every cell and head of every variant for clipping; both go to
// notes.txt. Runs only in the Design shots workflow, on a throwaway branch that is never merged.
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
// STUB: one failed blob payment (a MsgPayForFibre that failed in its block), made up for these shots: its transaction
// hash is invented, its height lies between the live list's first two rows (its time in step), its namespace and
// publisher are its neighbours', its reason "Out of gas". It carries no blob field: no promise hash, commitment, size,
// fee or endorsement. Set once, from the first live answer, and put back in its place in every later one.
const FAILED_TX = "9b23e1d4c07a5f3e8b6d2a91c4f07e35b8a2d6c19e4f03a7b5d8c2e61f097c4a";
if (!/^[0-9a-f]{64}$/.test(FAILED_TX)) throw new Error("the stub's hash is not 64 hex characters");
let FAILED = null;

function stub(rows) {
  if (FAILED || rows.length < 2) return;
  const [a, b] = rows;
  const ha = a.settlement_height, hb = b.settlement_height;
  const h = ha - hb >= 2 ? Math.round((ha + hb) / 2) : hb;
  const ta = Date.parse(a.settlement_time), tb = Date.parse(b.settlement_time);
  const t = ha === hb ? tb : tb + Math.round(((ta - tb) * (h - hb)) / (ha - hb) / 1000) * 1000;
  if (a.namespace !== b.namespace || (a.publisher || a.signer) !== (b.publisher || b.signer)) notes.push("stub: the first two live rows differ in namespace or publisher; the stub takes the first row's");
  FAILED = {
    status: "failed", reason: "Out of gas",
    settlement_tx_hash: FAILED_TX, settlement_height: h, settlement_tx_index: 0, settlement_time: new Date(Math.floor(t / 1000) * 1000).toISOString().replace(/\.000Z$/, "Z"),
    namespace: a.namespace, publisher: a.publisher, signer: a.signer,
  };
  notes.push(`stub: failed blob payment at #${h} (${FAILED.settlement_time}) between #${ha} and #${hb}, tx ${FAILED_TX}`);
}

/** the list's first page, unfiltered, with the stub in its place by height; any other read as it came */
function withFailed(j, q) {
  if (!j || !Array.isArray(j.blobs)) return j;
  if (q.get("namespace") || q.get("publisher") || q.get("tx") || (Number(q.get("offset")) || 0) !== 0) return j;
  stub(j.blobs);
  if (!FAILED) return j;
  const rows = j.blobs.filter((r) => r.settlement_tx_hash !== FAILED_TX);
  let i = rows.findIndex((r) => r.settlement_height <= FAILED.settlement_height);
  if (i < 0) i = rows.length;
  rows.splice(i, 0, FAILED);
  const limit = Number(q.get("limit")) || rows.length;
  return { ...j, blobs: rows.slice(0, limit), total: (Number(j.total) || 0) + 1 };
}

// ---------------------------------------------------------------- photographing
const W = 1440, H = 1100;

async function open(browser, theme) {
  const ctx = await browser.newContext({ viewport: { width: W, height: H }, deviceScaleFactor: 2, colorScheme: theme });
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

/** scrolls the list's head near the window's top */
async function reveal(page) {
  await page.evaluate(() => { const h = document.querySelector("#list .list-head"); h.scrollIntoView({ block: "start" }); scrollBy(0, -40); });
  await page.waitForTimeout(300);
}

/** the elements' area with room around it, from the window as it is */
async function crop(page, file, sels) {
  const b = await box(page, sels);
  if (!b) { notes.push(`${file}: nothing to crop`); return; }
  const x = Math.max(0, Math.floor(b.x - 24)), y = Math.max(0, Math.floor(b.y - 24));
  const width = Math.min(W - x, Math.ceil(b.r + 24) - x), height = Math.min(H - y, Math.ceil(b.b + 16) - y);
  await page.screenshot({ path: path.join(out, file), animations: "disabled", clip: { x, y, width, height } });
}

/**
 * the browser's own tooltip for an element's title: a headless browser paints no native tooltip, so the shot draws it
 * in the platform's plain style, beside the hovered element inside its own row (right), level with the row, so it never
 * stands over another row. What it covers is written to the notes, row by row.
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
    const h = d.offsetHeight;
    d.style.left = `${Math.round(r.right + 10)}px`;
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
    return o;
  }, { sel, dark: theme === "dark" });
  if (covered.length) notes.push(`${theme} tip on ${sel} covers: ${covered.join(", ")}`);
}
const untip = async (page) => { await page.evaluate(() => document.querySelectorAll(".shot-tip").forEach((d) => d.remove())); await page.mouse.move(5, 5); await page.waitForTimeout(200); };

// ---------------------------------------------------------------- measuring
/**
 * today's table: each column's width, its padding, its head's words and its cells' widest content over the live rows
 * (the failed stub left out), and the content a column is sized for at its widest, set in a copy of the first row and
 * measured: the slack is what a column holds beyond the widest of them
 */
async function slack(page, label) {
  const r = await page.evaluate(() => {
    const t = document.querySelector("#list .lg-t");
    const pad = (el) => { const s = getComputedStyle(el); return parseFloat(s.paddingLeft) + parseFloat(s.paddingRight); };
    const cw = (el) => { const g = document.createRange(); g.selectNodeContents(el); return g.getBoundingClientRect().width; };
    const ths = [...t.querySelectorAll("thead th")];
    const rows = [...t.querySelectorAll("tbody tr.row:not(.xf):not(.sk)")];
    const cols = ths.map((th, i) => {
      let max = 0, at = "";
      for (const tr of rows) { const td = tr.children[i]; if (!td) continue; const w = cw(td); if (w > max) { max = w; at = td.textContent.trim(); } }
      return { col: th.className.split(" ")[0] || `#${i}`, w: th.getBoundingClientRect().width, pad: pad(th), padTd: rows[0] ? pad(rows[0].children[i]) : 0, head: cw(th), cells: max, at };
    });
    // the widest contents, set in a copy of the first row
    const probes = [];
    const src = rows[0];
    if (src) {
      const tr = src.cloneNode(true);
      tr.classList.add("probe");
      src.parentElement.appendChild(tr);
      const ix = (c) => ths.findIndex((th) => th.classList.contains(c));
      const td = (c) => tr.children[ix(c)];
      const set = (c, html, label) => { const d = td(c); if (!d) return; const keep = d.innerHTML; d.innerHTML = html; probes.push({ col: c, label, cells: cw(d), inner: d.clientWidth - pad(d), scroll: d.scrollWidth, client: d.clientWidth }); d.innerHTML = keep; };
      const tdT = td("c-t");
      if (tdT) {
        const tm = tdT.querySelector(".tm"), ag = tdT.querySelector(".ag");
        for (const [a, b] of [["May 28 20:48:38", "23 h 59 min"], ["Oct 9 12:11:34", "23 h 59 min"], ["May 28 20:48:38", "29 d 23 h"], ["Oct 9 12:11:34", "41 min"]]) {
          if (tm) tm.textContent = a; if (ag) ag.textContent = b;
          probes.push({ col: "c-t", label: `${a} ${b}`, cells: cw(tdT), inner: tdT.clientWidth - pad(tdT), scroll: tdT.scrollWidth, client: tdT.clientWidth });
        }
      }
      for (const s of ["256.0", "1023.9", "128.0"]) for (const u of ["KiB", "MiB"]) set("c-sz", `${s}<span class="u"> ${u}</span>`, `${s} ${u}`);
      for (const s of ["0.695", "23.69", "99.99", "999.9"]) set("c-fee", `${s}<span class="u"> TIA</span>`, `${s} TIA`);
      const pc = td("c-e")?.querySelector(".pc");
      if (pc) for (const s of ["66.89%", "100%", "99.99%"]) { const keep = pc.textContent; pc.textContent = s; const d = td("c-e"); probes.push({ col: "c-e", label: s, cells: cw(d), inner: d.clientWidth - pad(d), pc: pc.getBoundingClientRect().width, pcText: (() => { const g = document.createRange(); g.selectNodeContents(pc); return g.getBoundingClientRect().width; })(), scroll: d.scrollWidth, client: d.clientWidth }); pc.textContent = keep; }
      const em = td("c-e")?.querySelector(".em");
      if (em) probes.push({ col: "c-e", label: "meter", cells: em.getBoundingClientRect().width, inner: 0 });
      tr.remove();
    }
    const table = t.getBoundingClientRect().width;
    return { cols, probes, table };
  });
  notes.push(`--- ${label}, ${Math.round(r.table)}px: column · width · padding (head / cell) · head's words · widest live cell ("its words") · slack against it`);
  for (const c of r.cols) notes.push(`${c.col.padEnd(6)} ${c.w.toFixed(1).padStart(6)}  pad ${c.pad}/${c.padTd}  head ${c.head.toFixed(1).padStart(6)}  cells ${c.cells.toFixed(1).padStart(6)} ("${c.at.slice(0, 40)}")  slack ${(c.w - c.padTd - Math.max(c.cells, c.head + c.pad - c.padTd)).toFixed(1)}`);
  notes.push("--- the widest contents, set in a copy of the first row: column · content · its width · the cell's inner width · slack");
  for (const p of r.probes) notes.push(`${p.col.padEnd(6)} ${p.label.padEnd(30)} ${p.cells.toFixed(1).padStart(6)}  inner ${(+p.inner).toFixed(1).padStart(6)}  slack ${(p.inner - p.cells).toFixed(1)}${p.pcText != null ? `  (.pc box ${p.pc.toFixed(1)}, its words ${p.pcText.toFixed(1)})` : ""}${p.scroll > p.client ? `  CLIPS ${p.scroll}>${p.client}` : ""}`);
}

/** every head and cell of the list, and every clipping box in a cell: scrollWidth over clientWidth is clipped text */
async function clips(page, label) {
  const bad = await page.evaluate(() => {
    const t = document.querySelector("#list .lg-t");
    const o = [];
    const cls = (el) => (typeof el.className === "string" && el.className ? el.className.split(" ")[0] : el.tagName.toLowerCase());
    const check = (el, where) => { if (el.clientWidth > 0 && el.scrollWidth > el.clientWidth) o.push(`${where}: ${el.scrollWidth} > ${el.clientWidth}`); };
    t.querySelectorAll("thead th").forEach((th) => check(th, `head ${cls(th)}`));
    [...t.querySelectorAll("tbody tr.row")].forEach((tr, r) => {
      [...tr.children].forEach((td) => {
        if (getComputedStyle(td).display === "none") return;
        const where = `row ${r + 1}${tr.classList.contains("xf") ? " (failed)" : ""} ${cls(td)}`;
        check(td, where);
        td.querySelectorAll("*").forEach((el) => { const s = getComputedStyle(el); if (s.overflowX !== "visible") check(el, `${where} ${cls(el)}`); });
      });
    });
    return o;
  });
  bad.forEach((b) => notes.push(`CLIP ${label} ${b}`));
  if (!bad.length) notes.push(`${label}: every head and cell whole`);
}

/** the columns' widths as the variant sets them, and where its status marks stand (their dots' and words' left edges) */
async function layout(page, label) {
  const r = await page.evaluate(() => {
    const t = document.querySelector("#list .lg-t");
    const ths = [...t.querySelectorAll("thead th")];
    const widths = ths.map((th) => `${th.className.split(" ")[0]} ${Math.round(th.getBoundingClientRect().width * 10) / 10}`).join(" · ");
    const L = (el) => Math.round(el.getBoundingClientRect().left * 10) / 10;
    const C = (el) => { const b = el.getBoundingClientRect(); return Math.round((b.left + b.right) * 5) / 10; };
    const textLeft = (el) => { for (const n of el.childNodes) if (n.nodeType === 3 && n.nodeValue.trim()) { const g = document.createRange(); g.selectNodeContents(n); return Math.round(g.getBoundingClientRect().left * 10) / 10; } return null; };
    const st = t.querySelector("thead th.c-st");
    const head = st ? (() => { const g = document.createRange(); g.selectNodeContents(st); const b = g.getBoundingClientRect(); return { l: Math.round(b.left * 10) / 10, c: Math.round((b.left + b.right) * 5) / 10 }; })() : null;
    const dots = [...t.querySelectorAll("tbody td.c-st .dot")].map(L);
    const words = [...t.querySelectorAll("tbody td.c-st .st")].map(textLeft);
    const pills = [...t.querySelectorAll("tbody td.c-st .sp")].map(C);
    const col = st ? C(st) : null;
    // the status cell: its edges, its padding, its widest content (a word's own text, or a pill's box) and the room left
    // at its right before the padding; the dot's room at its left
    let room = null;
    const cell = t.querySelector("tbody tr.row:not(.xf) td.c-st");
    if (cell) {
      const s = getComputedStyle(cell), cb = cell.getBoundingClientRect();
      const pl = parseFloat(s.paddingLeft), pr = parseFloat(s.paddingRight);
      const ws = [...t.querySelectorAll("tbody td.c-st .st, tbody td.c-st .sp")].map((el) => {
        const tn = [...el.childNodes].find((n) => n.nodeType === 3 && n.nodeValue.trim());
        const g = document.createRange();
        if (tn) g.selectNodeContents(tn); else g.selectNodeContents(el);
        const b = el.classList.contains("sp") ? el.getBoundingClientRect() : g.getBoundingClientRect();
        return { w: b.width, right: b.right, text: el.textContent };
      });
      const widest = ws.reduce((a, b) => (b.w > a.w ? b : a), { w: 0, right: 0, text: "" });
      const dot = cell.querySelector(".dot");
      room = { w: cb.width, pl, pr, widest: `${widest.text} ${widest.w.toFixed(1)}`, right: (cb.right - pr - Math.max(...ws.map((x) => x.right))).toFixed(1), dotLeft: dot ? (dot.getBoundingClientRect().left - cb.left).toFixed(1) : "-" };
    }
    return { widths, head, dots: [...new Set(dots)], words: [...new Set(words)], pills: [...new Set(pills)], col, room };
  });
  notes.push(`${label} columns: ${r.widths}`);
  if (r.head) notes.push(`${label} status: head words left ${r.head.l} centre ${r.head.c}; column centre ${r.col}; dots' left ${r.dots.join(", ") || "-"}; words' left ${r.words.join(", ") || "-"}; pills' centres ${r.pills.join(", ") || "-"}`);
  if (r.room) notes.push(`${label} status cell: ${r.room.w}px, padding ${r.room.pl}/${r.room.pr}; widest "${r.room.widest}"; room at its right before the padding ${r.room.right}; the dot ${r.room.dotLeft}px from the cell's left edge`);
}

// ---------------------------------------------------------------- the shots
const VARIANTS = [
  { v: "a", fail: "#list .lg-t tr.xf td.c-st .st.f" },
  { v: "b", fail: "#list .lg-t tr.xf td.c-st .sp.f" },
  { v: "c", fail: "#list .lg-t tr.xf td.c-xf .xw" },
];
const rowSel = (n) => `#list .lg-t tbody tr:nth-child(${n})`;

(async () => {
  fs.mkdirSync(out, { recursive: true });
  await new Promise((r) => server.listen(PORT, "127.0.0.1", r));
  const browser = await chromium.launch({ executablePath: process.env.CHROME || "/usr/bin/google-chrome", args: ["--hide-scrollbars"] });
  for (const theme of ["dark", "light"]) {
    const { ctx, page } = await open(browser, theme);
    if (theme === "dark") {
      await go(page, "/blobs/", "#list .lg-t tbody tr.row:not(.sk) td.c-p .lg-who");
      await slack(page, "today's table");
      await clips(page, "today");
    }
    for (const { v, fail } of VARIANTS) {
      await go(page, `/blobs/?fv=${v}`, "#list .lg-t tbody tr.xf");
      await reveal(page);
      await clips(page, `${v} ${theme}`);
      if (theme === "dark") { await layout(page, v); if (v !== "c") await slack(page, `variant ${v}`); }
      await crop(page, `${v}-${theme}.png`, ["#list .list-head", rowSel(6)]);
      await tip(page, fail, theme);
      await crop(page, `${v}-hover-${theme}.png`, ["#list .list-head", rowSel(6), ".shot-tip"]);
      await untip(page);
    }
    await ctx.close();
  }
  await browser.close();
  server.close();
  fs.writeFileSync(path.join(out, "notes.txt"), notes.join("\n") + "\n");
  console.log(notes.join("\n"));
})().catch((e) => { console.error(e); process.exit(1); });
