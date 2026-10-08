#!/usr/bin/env node
/**
 * Builds the coast patches the host map's detail windows draw from. Run once,
 * offline, after build-map.mjs (it reads that script's world.json); the output
 * is committed and nothing here runs in the browser.
 *
 *   node web/scripts/build-coast.mjs <ne_10m_admin_0_countries.geojson>
 *
 * Input is the Natural Earth 1:10m admin-0 countries that build-map.mjs reads
 * (public domain). No packages.
 *
 * Output: web/src/lib/map/coast.json
 *   g         grid units across a country's size e (world.json's tiny)
 *   hx, hy    how far a patch reaches from its country's middle, across and
 *             down, in grid units
 *   patches   code -> [code, path][] for every country of tiny: the 1:10m land
 *             of each country inside the patch (Johor and Batam beside
 *             Singapore, Shenzhen beside Hong Kong), the patch's own country
 *             last, as SVG paths in relative commands on the integer grid about
 *             the country's middle (tiny's x, y). Code is ISO 3166-1 alpha-2, or
 *             "" where Natural Earth has none.
 *
 * The points are projected as web/src/lib/map/project.ts does, with world.json's
 * k, tx and ty, so a patch lies exactly where its country is on the world. Each
 * ring is cut to the patch, simplified, and dropped when it is a speck.
 */
import { readFileSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const [detailPath] = process.argv.slice(2);
if (!detailPath) {
  console.error("usage: build-coast.mjs <ne_10m_admin_0_countries.geojson>");
  process.exit(2);
}
const mapDir = join(dirname(fileURLToPath(import.meta.url)), "..", "src", "lib", "map");
const world = JSON.parse(readFileSync(join(mapDir, "world.json"), "utf8"));

const G = 250;            // grid units across a country: a window draws one about 90 px across, so a unit is about a third of a px
const HX = 1.0, HY = 0.8; // a patch's reach from its country's middle, in country sizes: more than a window shows of it
const TOL = 1.5;          // simplification, in grid units (Douglas-Peucker)
const MIN_AREA = 12;      // grid units²: smaller rings go (about a px or less)

// ---- the forward projection of web/src/lib/map/project.ts ----------------------
const A1 = 1.340264, A2 = -0.081106, A3 = 0.000893, A4 = 0.003796, M = Math.sqrt(3) / 2;
function project([lon, lat]) {
  const l = (lon * Math.PI) / 180, p = (lat * Math.PI) / 180;
  const t = Math.asin(M * Math.sin(p)), t2 = t * t, t6 = t2 * t2 * t2;
  const x = (l * Math.cos(t)) / (M * (A1 + 3 * A2 * t2 + t6 * (7 * A3 + 9 * A4 * t2)));
  const y = t * (A1 + A2 * t2 + t6 * (A3 + A4 * t2));
  return [world.tx + world.k * x, world.ty - world.k * y];
}

// ---- every ring of the 1:10m countries, projected, with its bounds ---------------
const codeOf = (p) => [p.ISO_A2_EH, p.ISO_A2, p.WB_A2].find((v) => typeof v === "string" && /^[A-Z]{2}$/.test(v)) ?? "";
const rings = [];
for (const f of JSON.parse(readFileSync(detailPath, "utf8")).features) {
  const g = f.geometry, code = codeOf(f.properties);
  const polys = g?.type === "Polygon" ? [g.coordinates] : g?.type === "MultiPolygon" ? g.coordinates : [];
  for (const poly of polys) for (const r of poly) {
    const pts = r.map(project);
    if (pts.length > 1 && pts[0][0] === pts[pts.length - 1][0] && pts[0][1] === pts[pts.length - 1][1]) pts.pop();
    if (pts.length < 3) continue;
    let x0 = Infinity, y0 = Infinity, x1 = -Infinity, y1 = -Infinity;
    for (const [x, y] of pts) { x0 = Math.min(x0, x); y0 = Math.min(y0, y); x1 = Math.max(x1, x); y1 = Math.max(y1, y); }
    rings.push({ code, pts, box: [x0, y0, x1, y1] });
  }
}

// ---- cut, simplify, serialise --------------------------------------------------
// A ring cut to a box, one side at a time (Sutherland-Hodgman); where it leaves and re-enters, the cut runs along the
// box's side, outside what a window shows.
function clip(pts, [x0, y0, x1, y1]) {
  let out = pts;
  for (const [a, v, s] of [[0, x0, 1], [0, x1, -1], [1, y0, 1], [1, y1, -1]]) {
    const inp = out;
    out = [];
    for (let i = 0; i < inp.length; i++) {
      const p = inp[i], q = inp[(i + 1) % inp.length], pIn = s * (p[a] - v) >= 0, qIn = s * (q[a] - v) >= 0;
      if (pIn) out.push(p);
      if (pIn !== qIn) {
        const t = (v - p[a]) / (q[a] - p[a]);
        out.push([p[0] + t * (q[0] - p[0]), p[1] + t * (q[1] - p[1])]);
      }
    }
    if (out.length < 3) return [];
  }
  return out;
}
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
// a ring (not repeating its first point) is split at its point farthest from the start, and each half simplified
function dpRing(r, tol) {
  let fi = 0, fd = -1;
  r.forEach((q, i) => { const d = Math.hypot(q[0] - r[0][0], q[1] - r[0][1]); if (d > fd) { fd = d; fi = i; } });
  return [...dp(r.slice(0, fi + 1), tol).slice(0, -1), ...dp([...r.slice(fi), r[0]], tol).slice(0, -1)];
}
const area = (r) => { let a = 0; for (let i = 0, j = r.length - 1; i < r.length; j = i++) a += (r[j][0] + r[i][0]) * (r[j][1] - r[i][1]); return Math.abs(a / 2); };
// Numbers joined the way SVG allows: a minus sign separates on its own.
const join2 = (ns) => ns.reduce((s, n, i) => s + (i === 0 || n < 0 ? "" : " ") + n, "");
// After the first ring, each ring starts relative to the previous ring's start (z returns there).
function ringD(pts, at) {
  const d = [];
  for (let i = 1; i < pts.length; i++) d.push(pts[i][0] - pts[i - 1][0], pts[i][1] - pts[i - 1][1]);
  return at ? "m" + join2([pts[0][0] - at[0], pts[0][1] - at[1]]) + "l" + join2(d) + "z" : "M" + join2(pts[0]) + "l" + join2(d) + "z";
}

// ---- one patch per small country -------------------------------------------------
const patches = {};
let total = 0;
for (const code of Object.keys(world.tiny).sort()) {
  const [x, y, e] = world.tiny[code];
  const box = [x - HX * e, y - HY * e, x + HX * e, y + HY * e], n = G / e;
  const byCode = new Map();
  for (const r of rings) {
    if (r.box[0] > box[2] || r.box[2] < box[0] || r.box[1] > box[3] || r.box[3] < box[1]) continue;
    const cut = clip(r.pts, box).map(([px, py]) => [(px - x) * n, (py - y) * n]);
    if (cut.length < 3 || area(cut) < MIN_AREA) continue;
    const pts = [];
    for (const [px, py] of dpRing(cut, TOL)) {
      const p = [Math.round(px), Math.round(py)], last = pts[pts.length - 1];
      if (!last || last[0] !== p[0] || last[1] !== p[1]) pts.push(p);
    }
    while (pts.length > 1 && pts[0][0] === pts[pts.length - 1][0] && pts[0][1] === pts[pts.length - 1][1]) pts.pop();
    if (pts.length < 3 || area(pts) < MIN_AREA / 2) continue;
    byCode.set(r.code, [...(byCode.get(r.code) ?? []), pts]);
  }
  if (!byCode.has(code)) continue;
  patches[code] = [...byCode.keys()].filter((c) => c !== code).sort().concat(code).map((c) => {
    let d = "", at = null;
    for (const r of byCode.get(c)) { d += ringD(r, at); at = r[0]; total += r.length; }
    return [c, d];
  });
}
const out = JSON.stringify({ g: G, hx: Math.round(HX * G), hy: Math.round(HY * G), patches });
writeFileSync(join(mapDir, "coast.json"), out + "\n");
console.log(`${Object.keys(patches).length} patches, ${total} points, ${(out.length / 1024).toFixed(1)} KB -> ${mapDir}/coast.json`);
