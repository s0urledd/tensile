#!/usr/bin/env node
/**
 * Builds the one file the overview's host map draws from. Run once, offline;
 * the output is committed and nothing here runs in the browser.
 *
 *   npm i --no-save d3-geo topojson-server topojson-simplify topojson-client
 *   node web/scripts/build-map.mjs <ne_50m_admin_0_countries.geojson> <ne_10m_admin_0_countries.geojson>
 *
 * Input is Natural Earth 1:50m and 1:10m admin-0 countries (public domain), from
 * https://github.com/nvkelso/natural-earth-vector/tree/master/geojson
 * The four packages are d3-geo and topojson (ISC); they are needed only here.
 *
 * Output: web/src/lib/map/world.json
 *   w, h        the frame in map units (Equal Earth, Antarctica left out)
 *   k, tx, ty   the projection: x = tx + k * X, y = ty - k * Y, where X, Y is
 *               the raw Equal Earth forward (web/src/lib/map/project.ts)
 *   countries   [code, path][]: every country as one SVG path in map units,
 *               relative commands on an integer grid. Code is ISO 3166-1
 *               alpha-2, or "" where Natural Earth has none.
 *   points      code -> [lon, lat], each country's Natural Earth label point
 *   tiny        code -> [x, y, e, path] for every country under TINY_E units
 *               across at 1:50m: its 1:10m outline about its middle x, y (map
 *               units), e its larger side in map units, the path in relative
 *               commands on a grid where the larger side is 100. The map draws a
 *               small hosted country larger from it, so its own shape shows.
 *
 * The countries share their borders in a topology before simplification, so
 * neighbours stay seamless; tiny islands are dropped unless they are the
 * largest piece of their country (Singapore, Malta and Bahrain stay).
 */
import { readFileSync, writeFileSync, mkdirSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { createRequire } from "node:module";

const require = createRequire(join(process.cwd(), "noop.js"));
const { geoEqualEarth, geoPath } = require("d3-geo");
const { topology } = require("topojson-server");
const { presimplify, simplify, quantile } = require("topojson-simplify");
const { feature } = require("topojson-client");

const [countriesPath, detailPath] = process.argv.slice(2);
if (!countriesPath || !detailPath) {
  console.error("usage: build-map.mjs <ne_50m_admin_0_countries.geojson> <ne_10m_admin_0_countries.geojson>");
  process.exit(2);
}
const outDir = join(dirname(fileURLToPath(import.meta.url)), "..", "src", "lib", "map");
mkdirSync(outDir, { recursive: true });

const W = 4800;      // frame width in map units; coordinates are whole units
const KEEP = 0.2;    // share of points kept by simplification (Visvalingam, by triangle area)
const MIN_RING = 5;  // units²: smaller rings go, unless a country's largest
const TINY_E = 24;   // units: a country narrower than this at 1:50m also gets its 1:10m outline (tiny)
const TINY_TOL = 2;  // its simplification, in hundredths of its larger side (Douglas-Peucker)

const src = JSON.parse(readFileSync(countriesPath, "utf8"));
const codeOf = (p) => [p.ISO_A2_EH, p.ISO_A2, p.WB_A2].find((v) => typeof v === "string" && /^[A-Z]{2}$/.test(v)) ?? "";
const feats = src.features.filter((f) => codeOf(f.properties) !== "AQ" && f.properties.ADMIN !== "Antarctica");

// ---- label points ------------------------------------------------------------
const points = {};
for (const f of feats) {
  const p = f.properties, code = codeOf(p);
  if (!code || points[code]) continue; // the first feature for a code is the sovereign mainland
  if (typeof p.LABEL_X === "number" && typeof p.LABEL_Y === "number") points[code] = [+p.LABEL_X.toFixed(2), +p.LABEL_Y.toFixed(2)];
}

// ---- projection, fitted to the land without Antarctica -----------------------
const fc = { type: "FeatureCollection", features: feats };
const proj = geoEqualEarth().precision(0).fitWidth(W, fc);
const [[, y0], [, y1]] = geoPath(proj).bounds(fc);
const PAD = 6;
proj.translate([proj.translate()[0], proj.translate()[1] - y0 + PAD]);
const H = Math.ceil(y1 - y0 + 2 * PAD);

// ---- shared-border topology, simplified before projecting ----------------------
const objs = {};
feats.forEach((f, i) => { objs["f" + i] = { type: "Feature", properties: { code: codeOf(f.properties) }, geometry: f.geometry }; });
const full = topology(objs, 1e6);
const pre = presimplify(topology(objs, 1e6));
const topo = simplify(pre, quantile(pre, KEEP));

// ---- serialise: relative commands on the integer grid ------------------------
function ringsOf(geom) {
  const rings = [];
  let cur = null;
  const ctx = {
    moveTo(x, y) { cur = [[x, y]]; rings.push(cur); },
    lineTo(x, y) { cur.push([x, y]); },
    closePath() {},
    arc() {},
  };
  geoPath(proj, ctx)(geom);
  return rings;
}
const area = (r) => { let a = 0; for (let i = 0, j = r.length - 1; i < r.length; j = i++) a += (r[j][0] + r[i][0]) * (r[j][1] - r[i][1]); return Math.abs(a / 2); };
function ringPath(r) {
  const pts = [];
  for (const [x, y] of r) {
    const p = [Math.round(x), Math.round(y)];
    const last = pts[pts.length - 1];
    if (!last || last[0] !== p[0] || last[1] !== p[1]) pts.push(p);
  }
  if (pts.length > 1 && pts[0][0] === pts[pts.length - 1][0] && pts[0][1] === pts[pts.length - 1][1]) pts.pop();
  if (pts.length < 3) return null;
  return pts;
}
// Numbers joined the way SVG allows: a minus sign separates on its own.
const join2 = (ns) => ns.reduce((s, n, i) => s + (i === 0 || n < 0 ? "" : " ") + n, "");
function ringD(pts, at) {
  const d = [];
  for (let i = 1; i < pts.length; i++) d.push(pts[i][0] - pts[i - 1][0], pts[i][1] - pts[i - 1][1]);
  return at ? "m" + join2([pts[0][0] - at[0], pts[0][1] - at[1]]) + "l" + join2(d) + "z" : "M" + join2(pts[0]) + "l" + join2(d) + "z";
}

// A small country can simplify away entirely (an island is one arc, and an
// arc keeps only its ends); those take their unsimplified outline instead.
const byCode = new Map();
for (const [id, g] of Object.entries(topo.objects)) {
  const f = feature(topo, g);
  const code = f.properties.code;
  let rings = ringsOf(f).map(ringPath).filter(Boolean);
  if (!rings.some((r) => r.length >= 4)) rings = ringsOf(feature(full, full.objects[id])).map(ringPath).filter(Boolean);
  if (!rings.length) continue;
  const list = byCode.get(code) ?? [];
  list.push(...rings);
  byCode.set(code, list);
}
const countries = [];
let totalPts = 0;
for (const [code, rings] of byCode) {
  const big = Math.max(...rings.map(area));
  const kept = rings.filter((r) => area(r) >= MIN_RING || area(r) === big);
  // After the first ring, each ring starts relative to the previous ring's start (z returns there).
  let d = "", at = null;
  for (const r of kept) { d += ringD(r, at); at = r[0]; totalPts += r.length; }
  countries.push([code, d]);
}
countries.sort((a, b) => (a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : 0));

const [k] = [proj.scale()], [tx, ty] = proj.translate();
const sortedPoints = Object.fromEntries(Object.keys(points).sort().map((c) => [c, points[c]]));
// ---- tiny: the 1:10m outline of each small country, for drawing it larger ----
// Its rings near the 1:50m outline and at least 5% of the largest one's area, about their middle, scaled so the
// larger side is 100 and simplified to TINY_TOL of it.
const extent = (rs) => { let x0 = Infinity, y0 = Infinity, x1 = -Infinity, y1 = -Infinity; for (const r of rs) for (const [x, y] of r) { x0 = Math.min(x0, x); y0 = Math.min(y0, y); x1 = Math.max(x1, x); y1 = Math.max(y1, y); } return { x: (x0 + x1) / 2, y: (y0 + y1) / 2, e: Math.max(x1 - x0, y1 - y0) }; };
function dp(pts, tol) {
  const keep = new Uint8Array(pts.length); keep[0] = keep[pts.length - 1] = 1;
  const st = [[0, pts.length - 1]];
  while (st.length) {
    const [a, b] = st.pop(); let md = -1, mi = -1;
    const [ax, ay] = pts[a], [bx, by] = pts[b], dx = bx - ax, dy = by - ay, l = Math.hypot(dx, dy);
    for (let i = a + 1; i < b; i++) { const d = l ? Math.abs(dy * pts[i][0] - dx * pts[i][1] + bx * ay - by * ax) / l : Math.hypot(pts[i][0] - ax, pts[i][1] - ay); if (d > md) { md = d; mi = i; } }
    if (md > tol) { keep[mi] = 1; st.push([a, mi], [mi, b]); }
  }
  return pts.filter((_, i) => keep[i]);
}
// a closed ring is split at its point farthest from the start, and each half simplified
const dpRing = (r, tol) => { let fi = 0, fd = -1; r.forEach((q, i) => { const d = Math.hypot(q[0] - r[0][0], q[1] - r[0][1]); if (d > fd) { fd = d; fi = i; } }); return [...dp(r.slice(0, fi + 1), tol).slice(0, -1), ...dp(r.slice(fi), tol)].slice(0, -1); };
const small = new Map();
for (const [code, rings] of byCode) {
  if (!code) continue;
  const big = Math.max(...rings.map(area)), b = extent(rings.filter((r) => area(r) >= MIN_RING || area(r) === big));
  if (b.e < TINY_E) small.set(code, b);
}
const tiny = {};
for (const f of JSON.parse(readFileSync(detailPath, "utf8")).features) {
  const code = codeOf(f.properties), b = small.get(code);
  if (!b || tiny[code]) continue;
  let rings = ringsOf(f).filter((r) => r.some(([x, y]) => Math.hypot(x - b.x, y - b.y) < Math.max(12, b.e)));
  if (!rings.length) continue;
  const big = Math.max(...rings.map(area));
  rings = rings.filter((r) => area(r) >= big * 0.05);
  const m = extent(rings), n = 100 / m.e;
  let d = "", at = null;
  for (const r of rings) {
    const pts = ringPath(dpRing(r, TINY_TOL / n).map(([x, y]) => [(x - m.x) * n, (y - m.y) * n]));
    if (pts) { d += ringD(pts, at); at = pts[0]; }
  }
  if (d) tiny[code] = [+m.x.toFixed(2), +m.y.toFixed(2), +m.e.toFixed(2), d];
}
const sortedTiny = Object.fromEntries(Object.keys(tiny).sort().map((c) => [c, tiny[c]]));
const out = JSON.stringify({ w: W, h: H, k: +k.toFixed(4), tx: +tx.toFixed(3), ty: +ty.toFixed(3), countries, points: sortedPoints, tiny: sortedTiny });
writeFileSync(join(outDir, "world.json"), out + "\n");
console.log(`${W}x${H}, ${countries.length} countries (${Object.keys(tiny).length} tiny), ${totalPts} points, ${(out.length / 1024).toFixed(1)} KB -> ${outDir}/world.json`);
