"use client";
import { useEffect, useLayoutEffect, useMemo, useRef, useState } from "react";
import Link from "next/link";
import { type Validator, API_BASE, ago, utcWord, int } from "@/lib/api";
import { validatorHref } from "@/lib/addr";
import type { Hosting } from "@/lib/hosting";
import { type Coast, FRAME, COUNTRIES, TINY, project, countryPoint, loadCoast } from "@/lib/map/project";
import { countryName } from "@/components/Flag";
import { Eye } from "@/components/Metrics";
import { type EndpointState, endpointState, readiness } from "@/components/Readiness";

/**
 * The overview's host map: every registered Fibre host of the bonded set,
 * placed by the city its address geolocates to (hosting.lat/lon) or, without
 * one, by its country's label point, over Natural Earth country outlines
 * (Equal Earth) drawn as calm solid land. A country with a host is tinted,
 * always at its true size; one too small to show around its badge also gets
 * a detail window at the map's east edge: that piece of the map at a scale
 * where the country shows, its 1:10m coast and its neighbours', with its
 * hosts' badge and its name. The open place's countries are lit.
 *
 * Every place is a rounded square in the accent carrying its count; a lone
 * host is a small square with no figure. Hosts close together on screen share
 * one badge (badges never touch). A badge with an unreachable host carries an
 * amber pip, and one whose hosts all stopped answering turns amber; the map
 * shows reachability only, what was served is on each validator's page. Names
 * appear only on hover, focus or tap, in the place's popover (its unreachable
 * hosts first), and the other badges step back
 * while one is open. A badge that holds several places zooms in on them; the
 * corner buttons, a double click, a pinch on a trackpad and a drag (once
 * zoomed) move the view, each in one eased flight.
 *
 * Across the map's foot, two small pills: the newest event at the left, the
 * three counts in the centre. The map has no heading of its own: the counts say what
 * it shows, and the panel's one heading on the subject is "Current Fibre
 * providers" under it. The box (its shape set in the stylesheet) and the bar
 * are in the prerendered page and the land is drawn as soon as the box is
 * measured, all before the list arrives, so nothing moves when it does.
 */

type Host = { v: Validator; state: EndpointState; share: number; cc: string; city: string; loc: string; lon: number; lat: number; ux: number; uy: number; provider: string };
type Cluster = { id: string; hosts: Host[]; ux: number; uy: number; locs: number; ccs: string[] };
/**
 * a small hosted country's detail window at the home view: its box in px; its land, each country's outline in the
 * window's own units (a coast patch's grid, else map units), the country itself last; the scale (px per unit), the
 * point drawn at the window's middle shifted by dx, dy px; and the country's hosts, as one badge
 */
type Inset = {
  cc: string; x: number; y: number; w: number; h: number; land: [string, string][]; k: number; cx: number; cy: number; dx: number; dy: number;
  n: number; unreach: number; none: number;
};
/** the visible part of the map, in map units: top-left corner and width (height follows the box) */
type View = { x: number; y: number; w: number };

const STATE_WORD: Record<EndpointState, string> = { reachable: "reachable", unreachable: "unreachable", none: "not checked yet" };
const STATE_VAR: Record<EndpointState, string> = { reachable: "var(--accent)", unreachable: "var(--hold)", none: "var(--pending)" };
const ORDER: EndpointState[] = ["reachable", "unreachable", "none"];

/** the home view's top is 72°N, above every city anyone hosts in; a phone's crop runs from there to 50°S */
const TOP = project(0, 72)[1], BOTTOM = project(0, -50)[1];
/**
 * the box's two shapes: a wide box is a low band (about 380 px tall at 1120 px wide), a box under
 * 560 px a taller crop. The stylesheet sets the same two shapes (.cm-atlas, by a container query on
 * .cm), so the box has its size before any script runs.
 */
const WIDE = 0.34, TALL = 0.56;
const MAX_ZOOM = 12;
/** the home view may step back until the world is this much narrower than the box, to keep every badge inside it */
const MAX_BACK = 1.4;
/** the room every badge of the home view keeps from the box's edges, in px: above, beside, and below (or above the bar) */
const ROOM_TOP = 28, ROOM_SIDE = 12, ROOM_FOOT = 8;
const ROOM_TOP_NARROW = 14;

/** a view inside the world; one at least as wide as the world (the home view, stepped back) is centred on it */
function clampView(v: View, a: number, maxW = FRAME.w): View {
  const w = Math.min(maxW, FRAME.h / a, Math.max(FRAME.w / MAX_ZOOM, v.w)), h = w * a;
  const x = w >= FRAME.w ? (FRAME.w - w) / 2 : Math.min(FRAME.w - w, Math.max(0, v.x));
  return { w, x, y: Math.min(FRAME.h - h, Math.max(0, v.y)) };
}
/** the view that shows every point, with room around them */
function fitView(pts: [number, number][], a: number, pad = 0.35, minW = FRAME.w / MAX_ZOOM): View {
  let x0 = Infinity, y0 = Infinity, x1 = -Infinity, y1 = -Infinity;
  for (const [x, y] of pts) { x0 = Math.min(x0, x); y0 = Math.min(y0, y); x1 = Math.max(x1, x); y1 = Math.max(y1, y); }
  const w = Math.max(minW, (x1 - x0) * (1 + 2 * pad), ((y1 - y0) * (1 + 2 * pad)) / a);
  return clampView({ w, x: (x0 + x1) / 2 - w / 2, y: (y0 + y1) / 2 - (w * a) / 2 }, a);
}

/**
 * The home view and its badges. It starts from the whole width of the world (a phone: a crop from
 * 72°N to 50°S, centred on the hosts) with its top at 72°N, and keeps every badge inside the box:
 * ROOM_TOP below the top edge, ROOM_SIDE from either side, and clear of the floating bar (bar: its
 * left, top and width in px, where it lies over the box) or ROOM_FOOT above the bottom. Where the
 * top at 72°N leaves a badge outside, the view moves north or south as far as the others allow;
 * where no position does, it steps back a little and tries again, until the world is MAX_BACK times
 * narrower than the box.
 */
function fitHome(hosts: Host[], a: number, width: number, bar: [number, number, number, number] | null, narrow: boolean): [View, Cluster[]] {
  const height = width * a;
  const w0 = Math.min(FRAME.w, (BOTTOM - TOP) / a);
  const xs = hosts.map((h) => h.ux), cx = xs.length ? (Math.min(...xs) + Math.max(...xs)) / 2 : FRAME.w / 2;
  const top = narrow ? ROOM_TOP_NARROW : ROOM_TOP;
  let last: [View, Cluster[]] | null = null;
  for (let i = 0; i <= 12; i++) {
    const w = Math.min(w0 * MAX_BACK, w0 / (1 - 0.03 * i));
    const s = width / w;
    const x = w >= FRAME.w ? (FRAME.w - w) / 2 : Math.min(FRAME.w - w, Math.max(0, cx - w / 2));
    const cs = cluster(hosts, s, narrow);
    // the band of view tops that keeps every badge in: lo from the badges that must stay above the foot, hi from those below the top
    let lo = -Infinity, hi = Infinity, fits = true;
    for (const c of cs) {
      const px = (c.ux - x) * s, rr = drawn(c.hosts.length, narrow) / 2;
      if (px - rr < ROOM_SIDE || px + rr > width - ROOM_SIDE) fits = false;
      const under = !!bar && px + rr > bar[0] - 6 && px - rr < bar[0] + bar[2] + 6;
      const foot = under ? bar![1] - 6 : height - ROOM_FOOT;
      hi = Math.min(hi, c.uy - (top + rr) / s);
      lo = Math.max(lo, c.uy - (foot - rr) / s);
    }
    const y = Math.min(hi, Math.max(lo, TOP));
    last = [clampView({ w, x, y: lo <= hi ? y : (lo + hi) / 2 }, a, w0 * MAX_BACK), cs];
    if (fits && lo <= hi) break;
  }
  return last!;
}

function providerOf(h?: Hosting): string {
  if (!h || h.status === "unresolved") return "";
  if (h.provider && h.provider !== "Other" && h.provider !== "Unknown") return h.provider;
  const org = (h.as_org ?? "").split(/\s+/)[0].replace(/^AS-/i, "").replace(/-ASN?(-\d+)?$/i, "").replace(/\d+$/, "");
  if (!org) return "";
  return org === org.toUpperCase() && org.length > 3 ? org.charAt(0) + org.slice(1).toLowerCase() : org;
}

const valLink = (v: Validator) => validatorHref(v.operator_address, v.cons_address || v.address);
const name = (v: Validator) => v.moniker || v.operator_address || v.address;
const fmtShare = (s: number) => { const p = s * 100; return p >= 0.1 ? `${p.toFixed(1)}%` : p > 0 ? "<0.1%" : "0%"; };

/** a badge's height in px: a little taller for more hosts, so a crowd reads as one; a lone host is a small square */
const badgeH = (n: number, narrow: boolean) => (n === 1 ? (narrow ? 8 : 10) : narrow ? Math.round(17 + Math.min(6, 1.9 * Math.sqrt(n - 1))) : Math.round(21 + Math.min(8, 2.3 * Math.sqrt(n - 1))));
/** its width: square, or wider for a figure of three digits */
const badgeW = (n: number, narrow: boolean) => (n === 1 ? badgeH(1, narrow) : Math.max(badgeH(n, narrow), Math.round(String(n).length * (narrow ? 6.3 : 7.2) + (narrow ? 9 : 12))));
/** the room a badge takes, for the merging and for keeping clear of the bar */
const drawn = (n: number, narrow: boolean) => Math.max(badgeH(n, narrow), badgeW(n, narrow));

/** hosts -> clusters: one per place (city, else country), then merged while two badges would touch on screen */
function cluster(hosts: Host[], pxPerUnit: number, narrow: boolean): Cluster[] {
  const byLoc = new Map<string, Host[]>();
  for (const h of hosts) byLoc.set(h.loc, [...(byLoc.get(h.loc) ?? []), h]);
  type C = { hosts: Host[]; ux: number; uy: number };
  const mean = (hs: Host[], k: "ux" | "uy") => hs.reduce((s, h) => s + h[k], 0) / hs.length;
  let cs: C[] = [...byLoc.values()].map((hs) => ({ hosts: hs, ux: mean(hs, "ux"), uy: mean(hs, "uy") }));
  for (;;) {
    let best: [number, number, number] | null = null;
    for (let i = 0; i < cs.length; i++) for (let j = i + 1; j < cs.length; j++) {
      const d = Math.hypot(cs[i].ux - cs[j].ux, cs[i].uy - cs[j].uy) * pxPerUnit;
      const need = (drawn(cs[i].hosts.length, narrow) + drawn(cs[j].hosts.length, narrow)) / 2 + 5;
      if (d < need && (!best || d - need < best[2])) best = [i, j, d - need];
    }
    if (!best) break;
    const a = cs[best[0]], b = cs[best[1]], na = a.hosts.length, nb = b.hosts.length;
    const merged: C = { hosts: [...a.hosts, ...b.hosts], ux: (a.ux * na + b.ux * nb) / (na + nb), uy: (a.uy * na + b.uy * nb) / (na + nb) };
    cs = cs.filter((_, i) => i !== best![0] && i !== best![1]).concat(merged);
  }
  // a place's hosts as its popover lists them: the unreachable first, so a badge opened for its amber pip shows them at
  // once, then the rest; each group by voting power
  return cs.map((c) => {
    const hosts = [...c.hosts].sort((x, y) => Number(y.state === "unreachable") - Number(x.state === "unreachable") || (y.v.voting_power || 0) - (x.v.voting_power || 0));
    const n = new Map<string, number>();
    for (const h of hosts) if (h.cc) n.set(h.cc, (n.get(h.cc) ?? 0) + 1);
    const ccs = [...n.entries()].sort((p, q) => q[1] - p[1]).map(([cc]) => cc);
    return { id: hosts.map((h) => h.v.address).sort().join(","), hosts, ux: c.ux, uy: c.uy, locs: new Set(hosts.map((h) => h.loc)).size, ccs };
  }).sort((x, y) => x.ux - y.ux);
}

/**
 * A hosted country too small to show around its badge would vanish under it. On the world it stays at its
 * true size, tinted as every other hosted country; beside the world it gets a detail window: the same
 * map at a scale where the country shows, its coast and its neighbours' (a country of TINY from its
 * 1:10m coast patch, a larger one from the world's own outlines), flat in the same tones, with no frame;
 * its hosts' badge in the window's top-right corner and its name in the bottom-left, clear of its
 * outline. Where a larger hosted country beside it already shows the place blue (Slovenia, the Baltics,
 * South Korea beside Japan), it needs none. Which countries are small is decided at the home view, and
 * the windows stand at the home view only: a zoom hides them.
 */
/** px at the home view: a country at least this wide shows around its badge; a smaller one is hidden by it */
const SHOWN = 30, SHOWN_NARROW = 24;
/** px: a shown hosted country whose land comes this close to a small one's middle already makes that place read blue */
const NEAR = 16, NEAR_NARROW = 12;
/** px: a neighbour's piece smaller than this does not count as beside (India's Andaman and Nicobar Islands beside Singapore) */
const RING = 8;
/**
 * a detail window's size in px (wide box, narrow box); the room its country's outline keeps from its sides and from
 * its top and foot; the badge's and the name's inset from the corners, the name's line, and the gap to the next
 * window. The windows stand in one column at the box's east edge, under the view buttons, each as level with its
 * place as the others allow
 */
const INSET_W = 120, INSET_H = 96, INSET_W_NARROW = 84, INSET_H_NARROW = 64;
const INSET_PAD_X = 16, INSET_PAD_Y = 12, INSET_PAD_NARROW = 8;
const INSET_EDGE = 8, INSET_LINE = 14, INSET_GAP = 16;
/** the country's outline, if it would meet the badge or the name: smaller, and moved by these px, first that clears */
const INSET_FITS = [1, 0.92, 0.84, 0.76];
const INSET_SHIFTS: [number, number][] = [[0, 0], [-4, 0], [0, 4], [4, 0], [0, -4], [-4, 4], [4, -4], [-8, 0], [0, 8], [8, 0], [0, -8]];
/** px: the column's room from the box's east edge (the view buttons' own), and from the top (under the buttons) */
const INSET_SIDE = 16, INSET_TOP = 62, INSET_TOP_NARROW = 10;
/**
 * a country's own shape: its middle and larger side in map units, its path on TINY's grid where e is 100 (unit) or
 * in map units
 */
type Own = { x: number; y: number; e: number; d: string; unit: boolean };
/** an outline's rings as absolute points, from its path (absolute M and L, relative m and l, z) */
function ringsOf(d: string): number[][] {
  let x = 0, y = 0, sx = 0, sy = 0, cmd = "M";
  const rings: number[][] = [];
  const t = d.match(/[MmLlZz]|-?\d+(?:\.\d+)?/g) ?? [];
  for (let i = 0; i < t.length; i++) {
    const k = t[i];
    if (/[MmLlZz]/.test(k)) { cmd = k; if (k === "z" || k === "Z") { x = sx; y = sy; } continue; }
    const a = +k, b = +t[++i];
    if (cmd === "M") { x = a; y = b; sx = x; sy = y; cmd = "L"; rings.push([]); }
    else if (cmd === "m") { x += a; y += b; sx = x; sy = y; cmd = "l"; rings.push([]); }
    else if (cmd === "L") { x = a; y = b; }
    else { x += a; y += b; }
    rings[rings.length - 1].push(x, y);
  }
  return rings;
}
const bounds = (p: number[]): [number, number, number, number] => {
  let x0 = Infinity, y0 = Infinity, x1 = -Infinity, y1 = -Infinity;
  for (let i = 0; i < p.length; i += 2) { x0 = Math.min(x0, p[i]); y0 = Math.min(y0, p[i + 1]); x1 = Math.max(x1, p[i]); y1 = Math.max(y1, p[i + 1]); }
  return [x0, y0, x1, y1];
};
const area = (p: number[]) => {
  let a = 0;
  for (let i = 0, j = p.length - 2; i < p.length; j = i, i += 2) a += (p[j] + p[i]) * (p[j + 1] - p[i + 1]);
  return Math.abs(a / 2);
};
/** an outline's points without its specks: the rings of at least 5% of its largest one's area (as TINY keeps) */
function mainPoints(d: string): number[] {
  const rs = ringsOf(d), as = rs.map(area), big = Math.max(0, ...as);
  return rs.filter((_, i) => as[i] >= big * 0.05).flat();
}
/** the world's outlines with their bounds, for a window drawn from them; read when the first one is */
let worldBoxes: { cc: string; d: string; box: [number, number, number, number] }[] | null = null;
const worldOutlines = () => (worldBoxes ??= COUNTRIES.map(([cc, d]) => ({ cc, d, box: bounds(ringsOf(d).flat()) })));
/**
 * each country's size and its rings (each one's size and points) in map units, and its own shape: its 1:10m
 * outline from TINY, else its largest ring, so far islands (the Azores, Marion Island) neither move its middle
 * nor grow with it
 */
type Shape = { e: number; rings: { e: number; pts: number[] }[]; own: Own };
const SHAPES: Map<string, Shape> = (() => {
  const out = new Map<string, Shape>();
  const extent = (p: number[]) => {
    const [x0, y0, x1, y1] = bounds(p);
    return { x: (x0 + x1) / 2, y: (y0 + y1) / 2, e: Math.max(x1 - x0, y1 - y0) };
  };
  for (const [cc, d] of COUNTRIES) {
    if (!cc) continue;
    const rings = ringsOf(d);
    if (!rings.length) continue;
    const rs = rings.map((pts) => ({ ...extent(pts), pts }));
    const big = rs.reduce((p, q) => (q.e > p.e ? q : p)), tiny = TINY[cc];
    const own: Own = tiny
      ? { x: tiny[0], y: tiny[1], e: tiny[2], d: tiny[3], unit: true }
      : { x: big.x, y: big.y, e: big.e, d: `M${big.pts.slice(0, 2).join(" ")}L${big.pts.slice(2).join(" ")}Z`, unit: false };
    out.set(cc, { e: extent(rings.flat()).e, rings: rs.map(({ e, pts }) => ({ e, pts })), own });
  }
  return out;
})();

/**
 * Where a window draws its country (pts, its outline's points in the window's units; box, their bounds): the scale
 * that fits the box in the window less its padding, from the box's middle shifted by dx, dy px, the largest and
 * least moved that keeps every point inside the window and clear of the badge in the top-right corner (badge: its
 * width and height) and of the name in the bottom-left (name: its width); within a coast patch's reach (units from
 * the patch's middle, across and down), so the window never shows past the patch's land. Where none clears, the one
 * that leaves the fewest points in the way.
 */
function placeIn(pts: number[], box: [number, number, number, number], w: number, h: number, padX: number, padY: number,
  badge: [number, number], name: number, reach: [number, number] | null): { k: number; cx: number; cy: number; dx: number; dy: number } {
  const [x0, y0, x1, y1] = box, cx = (x0 + x1) / 2, cy = (y0 + y1) / 2;
  const k0 = Math.min((w - 2 * padX) / Math.max(1e-6, x1 - x0), (h - 2 * padY) / Math.max(1e-6, y1 - y0));
  // the corners' rooms, in px from the window's top-left: the badge's right and top edges, the name's left and foot
  const bx = w - INSET_EDGE - badge[0] - 4, by = INSET_EDGE + badge[1] + 4;
  const nx = INSET_EDGE + name + 4, ny = h - INSET_EDGE - INSET_LINE - 4;
  let best = { k: k0, cx, cy, dx: 0, dy: 0 }, fewest = Infinity;
  for (const f of INSET_FITS) {
    const k = k0 * f;
    for (const [dx, dy] of INSET_SHIFTS) {
      if (reach && (Math.abs(cx - dx / k) + w / 2 / k > reach[0] || Math.abs(cy - dy / k) + h / 2 / k > reach[1])) continue;
      let hit = 0;
      for (let i = 0; i < pts.length; i += 2) {
        const X = w / 2 + dx + (pts[i] - cx) * k, Y = h / 2 + dy + (pts[i + 1] - cy) * k;
        if (X < 4 || X > w - 4 || Y < 4 || Y > h - 4 || (X > bx && Y < by) || (X < nx && Y > ny)) hit++;
      }
      if (!hit) return { k, cx, cy, dx, dy };
      if (hit < fewest) { fewest = hit; best = { k, cx, cy, dx, dy }; }
    }
  }
  return best;
}

/**
 * The detail windows of the small hosted countries at the home view (view, scale s px per unit), in one column at
 * the box's east edge: each as level with its own places as the others allow, from under the view buttons to
 * above the bar. A window that would meet a badge of the world is left out; one of a TINY country waits for the
 * coast patches (coast).
 */
function layInsets(small: { cc: string; own: Own }[], hosts: Host[], home: View, s: number, width: number, height: number,
  bar: [number, number, number, number] | null, badges: Cluster[], narrow: boolean, coast: Coast | null): Inset[] {
  if (!small.length || !s) return [];
  const w = narrow ? INSET_W_NARROW : INSET_W, h = narrow ? INSET_H_NARROW : INSET_H;
  const px = narrow ? INSET_PAD_NARROW : INSET_PAD_X, py = narrow ? INSET_PAD_NARROW : INSET_PAD_Y;
  const step = h + INSET_GAP, x = width - INSET_SIDE - w;
  const top = narrow ? INSET_TOP_NARROW : INSET_TOP, foot = (bar ? bar[1] : height) - 10;
  const want = small.flatMap(({ cc, own }) => {
    const hs = hosts.filter((q) => q.cc === cc);
    if (!hs.length || (own.unit && !coast)) return [];
    const uy = hs.reduce((t, q) => t + q.uy, 0) / hs.length;
    return [{ cc, own, hs, y: (uy - home.y) * s - h / 2 }];
  }).sort((a, b) => a.y - b.y);
  // stacked in order, each run of windows that would overlap centred on where its windows want to be (sum: each
  // window's wanted top less its place in the run)
  const groups: { n: number; y: number; sum: number }[] = [];
  want.forEach((q) => {
    groups.push({ n: 1, y: q.y, sum: q.y });
    for (;;) {
      const g = groups.length, a = groups[g - 2], b = groups[g - 1];
      if (!a || a.y + a.n * step <= b.y) break;
      const n = a.n + b.n, sum = a.sum + b.sum - b.n * a.n * step;
      groups.splice(g - 2, 2, { n, y: sum / n, sum });
    }
  });
  const ys = groups.flatMap((g) => Array.from({ length: g.n }, (_, i) => g.y + i * step));
  // then into the column's room: down from the top, and up from the foot
  for (let i = 0; i < ys.length; i++) ys[i] = Math.max(ys[i], i ? ys[i - 1] + step : top);
  for (let i = ys.length - 1; i >= 0; i--) ys[i] = Math.min(ys[i], i < ys.length - 1 ? ys[i + 1] - step : foot - h);
  const rooms = badges.map((c) => {
    const r = drawn(c.hosts.length, narrow) / 2 + 6;
    return [(c.ux - home.x) * s - r, (c.uy - home.y) * s - r, 2 * r] as const;
  });
  return want.flatMap(({ cc, own, hs }, i) => {
    const y = ys[i];
    if (y < top - 0.5) return [];
    if (rooms.some(([bx, by, d]) => bx < x + w && bx + d > x && by < y + h && by + d > y)) return [];
    const n = hs.length, unreach = hs.filter((q) => q.state === "unreachable").length, none = hs.filter((q) => q.state === "none").length;
    // the land: a TINY country's coast patch, else (no patch) its own 1:10m outline alone; a larger one the world's
    // outlines round it, in map units
    const patch = own.unit ? coast?.patches[cc] : undefined;
    const mine = patch?.find(([c]) => c === cc)?.[1] ?? own.d;
    const pts = mainPoints(mine);
    if (!pts.length) return [];
    const name = Math.min(w - 2 * INSET_EDGE, countryName(cc).length * 6.2);
    const at = placeIn(pts, bounds(pts), w, h, px, py, [badgeW(n, narrow), badgeH(n, narrow)], name, patch && coast ? [coast.hx, coast.hy] : null);
    let land: [string, string][] = patch ?? [[cc, own.d]];
    if (!own.unit) {
      const vx = at.cx - at.dx / at.k, vy = at.cy - at.dy / at.k, hw = w / 2 / at.k, hh = h / 2 / at.k;
      land = worldOutlines().filter(({ box: [x0, y0, x1, y1] }) => x0 < vx + hw && x1 > vx - hw && y0 < vy + hh && y1 > vy - hh)
        .sort((a, b) => Number(a.cc === cc) - Number(b.cc === cc)).map((o): [string, string] => [o.cc, o.d]);
    }
    return [{ cc, x, y, w, h, land, ...at, n, unreach, none }];
  });
}

/** what a badge is called: its city, its country, or its countries */
function placeLabel(c: Cluster): string {
  if (c.locs === 1 && c.hosts[0].city) return `${c.hosts[0].city}, ${countryName(c.hosts[0].cc)}`;
  if (c.ccs.length === 1) return countryName(c.ccs[0]);
  if (c.ccs.length === 2) return c.ccs.map(countryName).join(", ");
  return `${c.ccs.length} countries`;
}
/** a badge's places, most hosts first: its cities (or countries, where no city is known) */
function cityCounts(c: Cluster): [string, number][] {
  const n = new Map<string, number>();
  for (const h of c.hosts) { const k = h.city || countryName(h.cc); n.set(k, (n.get(k) ?? 0) + 1); }
  return [...n.entries()].sort((p, q) => q[1] - p[1] || p[0].localeCompare(q[0]));
}

/** a list that scrolls says which edges have more: the stylesheet fades those (data-more) */
function scrollFade(el: HTMLElement | null) {
  if (!el) return;
  const more = [el.scrollTop > 1 ? "top" : "", el.scrollTop + el.clientHeight < el.scrollHeight - 1 ? "bottom" : ""].filter(Boolean).join(" ");
  if (el.dataset.more !== more) el.dataset.more = more;
}

function reducedMotion(): boolean {
  try { return window.matchMedia("(prefers-reduced-motion: reduce)").matches; } catch { return false; }
}

// ---- the network feed: host registrations, for the event line before any blob settles ----
type FeedEvent = { addr: string; term: string; at: string };
function useFeedEvents(enabled: boolean): FeedEvent[] {
  const [events, setEvents] = useState<FeedEvent[]>([]);
  useEffect(() => {
    if (!enabled) return;
    let dead = false;
    const load = async () => {
      try {
        const r = await fetch(`${API_BASE}/v1/feed.atom`, { cache: "no-store" });
        if (!r.ok) return;
        const doc = new DOMParser().parseFromString(await r.text(), "application/xml");
        const out: FeedEvent[] = [];
        for (const e of Array.from(doc.getElementsByTagName("entry"))) {
          const term = e.getElementsByTagName("category")[0]?.getAttribute("term") ?? "";
          const href = e.getElementsByTagName("link")[0]?.getAttribute("href") ?? "";
          const addr = /[?&]addr=([^&]+)/.exec(href)?.[1];
          const at = e.getElementsByTagName("published")[0]?.textContent ?? e.getElementsByTagName("updated")[0]?.textContent ?? "";
          if (addr && at) out.push({ addr: decodeURIComponent(addr).toLowerCase(), term, at });
        }
        if (!dead) setEvents(out);
      } catch { /* no feed, no events */ }
    };
    load();
    // not while the tab is hidden, as every stream of the site (lib/api.ts)
    const t = window.setInterval(() => { if (!document.hidden) load(); }, 120000);
    return () => { dead = true; window.clearInterval(t); };
  }, [enabled]);
  return events;
}

const EVENT_WORD: Record<string, string> = { registered: "new Fibre provider", "host-changed": "changed its Fibre host" };
/** "22 h ago" for "22 h 52 min ago": the pill's clock needs the hour, not the minute */
const shortAgo = (t: string) => ago(t).replace(/(\d+ h) \d+ min/, "$1");

export default function HostMap({ rows }: { rows: Validator[] | null }) {
  const loading = !rows;
  const r = readiness(rows ?? []);
  const total = r.total;

  const { hosts, unplaced } = useMemo(() => {
    const hs: Host[] = [];
    let un = 0;
    for (const v of r.registered) {
      const g = v.hosting;
      const cc = (g?.country ?? "").toUpperCase();
      const hasLL = !!g && typeof g.lat === "number" && typeof g.lon === "number";
      const ll = hasLL ? [g!.lon!, g!.lat!] as [number, number] : countryPoint(cc);
      if (!ll) { un++; continue; }
      const [ux, uy] = project(ll[0], ll[1]);
      const city = hasLL ? (g!.city ?? "") : "";
      // One place per named city: DB-IP gives districts of one city (and hosts in it) slightly different points.
      const loc = city ? `${cc}/${city}` : hasLL ? `${ll[1].toFixed(1)},${ll[0].toFixed(1)}` : `cc:${cc}`;
      hs.push({ v, state: endpointState(v), share: total > 0 ? (v.voting_power || 0) / total : 0, cc, city, loc, lon: ll[0], lat: ll[1], ux, uy, provider: providerOf(v.hosting) });
    }
    return { hosts: hs, unplaced: un };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [rows]);

  // ---- size: the box holds its shape in CSS (aspect-ratio), so measuring it never shifts the layout ----
  const box = useRef<HTMLDivElement>(null);
  const [width, setWidth] = useState(0);
  useLayoutEffect(() => {
    const el = box.current;
    if (!el) return;
    // the exact width, as the stylesheet's container query sees it
    const measure = () => setWidth(el.getBoundingClientRect().width);
    measure();
    const ro = new ResizeObserver(measure);
    ro.observe(el);
    return () => ro.disconnect();
  }, []);
  const narrow = width > 0 && width < 560;
  const aspect = narrow ? TALL : WIDE;
  const height = width * aspect;

  // the floating bar, where it lies over the map: measured, so no badge is left under it
  const bar = useRef<HTMLDivElement>(null);
  const [barAt, setBarAt] = useState<[number, number, number, number] | null>(null);
  useLayoutEffect(() => {
    const a = box.current?.getBoundingClientRect(), b = bar.current?.getBoundingClientRect();
    let at: [number, number, number, number] | null = null;
    if (a && b && b.top < a.bottom && b.bottom > a.top) at = [Math.round(b.left - a.left), Math.round(b.top - a.top), Math.round(b.width), Math.round(Math.min(b.bottom, a.bottom) - b.top)];
    if (JSON.stringify(at) !== JSON.stringify(barAt)) setBarAt(at);
  });

  // the home view keeps every badge inside the box and clear of the bar (see fitHome)
  const [home, homeClusters] = useMemo((): [View, Cluster[]] => {
    if (!width) { const w = Math.min(FRAME.w, (BOTTOM - TOP) / aspect); return [clampView({ w, x: (FRAME.w - w) / 2, y: TOP }, aspect), []]; }
    return fitHome(hosts, aspect, width, barAt, narrow);
  }, [hosts, barAt, width, narrow, aspect]);
  /** a view at least as wide as home's, or as the world: the home view itself */
  const isHome = (w: number) => w >= Math.min(home.w, FRAME.w) - 1;

  // ---- view: where the map is looking, flown toward a target ----
  const [view, setView] = useState<View>(home);
  const [target, setTarget] = useState<View>(home);
  const viewRef = useRef(view);
  viewRef.current = view;
  const targetRef = useRef(target);
  targetRef.current = target;
  const raf = useRef(0);
  const homeKey = `${aspect}|${Math.round(home.x)}|${Math.round(home.y)}|${Math.round(home.w)}`;
  useLayoutEffect(() => { cancelAnimationFrame(raf.current); setView(home); setTarget(home); }, [homeKey]); // eslint-disable-line react-hooks/exhaustive-deps
  const go = (to: View) => {
    const t = isHome(to.w) ? home : clampView(to, aspect), from = viewRef.current;
    setTarget(t);
    cancelAnimationFrame(raf.current);
    if (reducedMotion()) { setView(t); return; }
    const t0 = performance.now();
    const fcx = from.x + from.w / 2, fcy = from.y + (from.w * aspect) / 2, tcx = t.x + t.w / 2, tcy = t.y + (t.w * aspect) / 2;
    const step = (now: number) => {
      const p = Math.min(1, (now - t0) / 460), e = p < 0.5 ? 4 * p * p * p : 1 - Math.pow(-2 * p + 2, 3) / 2;
      const w = from.w * Math.pow(t.w / from.w, e), cx = fcx + (tcx - fcx) * e, cy = fcy + (tcy - fcy) * e;
      setView(p < 1 ? { w, x: cx - w / 2, y: cy - (w * aspect) / 2 } : t);
      if (p < 1) raf.current = requestAnimationFrame(step);
    };
    raf.current = requestAnimationFrame(step);
  };
  useEffect(() => () => cancelAnimationFrame(raf.current), []);
  const moving = view !== target;
  const zoomed = target.w < home.w - 1;
  const scale = width > 0 ? width / view.w : 0;
  const toPx = (ux: number, uy: number): [number, number] => [(ux - view.x) * scale, (uy - view.y) * scale];

  // Badges merge by the zoom the view is heading to, so they do not re-merge mid-flight.
  const clusters = useMemo(() => (target.w === home.w ? homeClusters : cluster(hosts, width / target.w, narrow)), [homeClusters, home.w, hosts, width, target.w, narrow]);

  // ---- drag to pan, once zoomed in ----
  const drag = useRef<{ id: number; x: number; y: number; v: View; moved: boolean } | null>(null);
  const onPointerDown = (e: React.PointerEvent) => {
    if (!zoomed || e.button !== 0 || (e.target as Element).closest(".cm-pin, .cm-tools, .cm-inset")) return;
    cancelAnimationFrame(raf.current);
    drag.current = { id: e.pointerId, x: e.clientX, y: e.clientY, v: viewRef.current, moved: false };
    (e.currentTarget as Element).setPointerCapture?.(e.pointerId);
  };
  const onPointerMove = (e: React.PointerEvent) => {
    const d = drag.current;
    if (!d || d.id !== e.pointerId) return;
    const dx = e.clientX - d.x, dy = e.clientY - d.y;
    if (!d.moved && Math.hypot(dx, dy) < 3) return;
    d.moved = true;
    const s = width / d.v.w;
    const nv = clampView({ w: d.v.w, x: d.v.x - dx / s, y: d.v.y - dy / s }, aspect);
    setView(nv); setTarget(nv);
  };
  const onPointerUp = (e: React.PointerEvent) => { if (drag.current?.id === e.pointerId) drag.current = null; };

  /** zoom by f about a point of the box (px), the centre by default */
  const zoomAt = (f: number, px = width / 2, py = height / 2) => {
    const t = targetRef.current, s = width / t.w, ux = t.x + px / s, uy = t.y + py / s, w = t.w / f;
    go({ w, x: ux - (px / width) * w, y: uy - (py / width) * w });
  };
  const onDoubleClick = (e: React.MouseEvent) => {
    if ((e.target as Element).closest(".cm-pin, .cm-tools, .cm-inset")) return;
    const b = box.current!.getBoundingClientRect();
    zoomAt(2, e.clientX - b.left, e.clientY - b.top);
  };
  // a pinch on a trackpad arrives as a wheel with ctrlKey: zoom about the pointer, following the fingers.
  // A plain wheel is left to the page, so the map never takes the page's scroll.
  useEffect(() => {
    const el = box.current;
    if (!el) return;
    const onWheel = (e: WheelEvent) => {
      if (!e.ctrlKey) return;
      e.preventDefault();
      const b = el.getBoundingClientRect(), px = e.clientX - b.left, py = e.clientY - b.top;
      const t = viewRef.current, s = b.width / t.w, ux = t.x + px / s, uy = t.y + py / s;
      const w0 = Math.min(home.w, t.w * Math.exp(e.deltaY * 0.01));
      const nv = isHome(w0) ? home : clampView({ w: w0, x: ux - (px / b.width) * w0, y: uy - (py / b.width) * w0 }, aspect);
      cancelAnimationFrame(raf.current);
      setView(nv); setTarget(nv);
    };
    el.addEventListener("wheel", onWheel, { passive: false });
    return () => el.removeEventListener("wheel", onWheel);
  }, [home, aspect]);

  // ---- the badges are one stop in the tab order: the arrow keys walk them, west to east ----
  const [roving, setRoving] = useState<string | null>(null);
  const walk = (e: React.KeyboardEvent<HTMLButtonElement>) => {
    const step = e.key === "ArrowRight" || e.key === "ArrowDown" ? 1 : e.key === "ArrowLeft" || e.key === "ArrowUp" ? -1 : e.key === "Home" ? -Infinity : e.key === "End" ? Infinity : 0;
    if (!step) return;
    const all = [...(e.currentTarget.closest("ul")?.querySelectorAll<HTMLButtonElement>(".cm-b") ?? [])];
    const i = all.indexOf(e.currentTarget);
    const j = step === -Infinity ? 0 : step === Infinity ? all.length - 1 : Math.min(all.length - 1, Math.max(0, i + step));
    all[j]?.focus();
    e.preventDefault();
  };

  // ---- the open place (hover, focus, tap) ----
  const [open, setOpen] = useState<string | null>(null);
  const closeT = useRef<number | undefined>(undefined);
  const openNow = (id: string) => { window.clearTimeout(closeT.current); setOpen(id); };
  const closeSoon = () => { window.clearTimeout(closeT.current); closeT.current = window.setTimeout(() => setOpen(null), 160); };
  useEffect(() => {
    if (!open) return;
    const onKey = (e: KeyboardEvent) => { if (e.key === "Escape") setOpen(null); };
    const onDown = (e: PointerEvent) => { if (!(e.target as Element)?.closest?.(".cm-pin, .cm-inset")) setOpen(null); };
    document.addEventListener("keydown", onKey);
    document.addEventListener("pointerdown", onDown);
    return () => { document.removeEventListener("keydown", onKey); document.removeEventListener("pointerdown", onDown); };
  }, [open]);
  const canSplit = (c: Cluster) => c.locs > 1 && target.w > FRAME.w / MAX_ZOOM + 1;
  const activate = (c: Cluster) => {
    if (canSplit(c)) {
      // Zoom until the places in this badge come apart: at least twice as close, at most the zoom limit.
      const fit = fitView(c.hosts.map((h) => [h.ux, h.uy]), aspect);
      const w = Math.min(fit.w, target.w / 2);
      setOpen(null);
      go({ w, x: fit.x + fit.w / 2 - w / 2, y: fit.y + (fit.w * aspect) / 2 - (w * aspect) / 2 });
    } else openNow(c.id);
  };

  // ---- the event on the bar: who served most recently, or else the newest host events ----
  const blobs = (rows ?? []).some((v) => (v.obligations?.total ?? 0) > 0);
  type Live = { v: Validator; host?: Host; event?: string; at: string };
  const served: Live[] = useMemo(() => {
    if (!blobs) return [];
    return hosts.filter((h) => !!h.v.last_served_at)
      .sort((a, b) => b.v.last_served_at!.localeCompare(a.v.last_served_at!))
      .slice(0, 5)
      .map((h) => ({ v: h.v, host: h, at: h.v.last_served_at! }));
  }, [hosts, blobs]);
  const feed = useFeedEvents(!loading && served.length === 0);
  const live: Live[] = useMemo(() => {
    if (served.length) return served;
    const byAddr = new Map<string, Validator>();
    for (const v of rows ?? []) {
      byAddr.set(v.address.toLowerCase(), v);
      if (v.operator_address) byAddr.set(v.operator_address.toLowerCase(), v);
    }
    const hostOf = new Map(hosts.map((h) => [h.v.address, h]));
    const ev: Live[] = [];
    for (const e of feed) {
      const v = byAddr.get(e.addr);
      if (v && EVENT_WORD[e.term]) ev.push({ v, host: hostOf.get(v.address), event: EVENT_WORD[e.term], at: e.at });
    }
    for (const h of hosts) {
      const v = h.v;
      if (h.state === "unreachable" && v.last_reachable_at) ev.push({ v, host: h, event: "last reachable", at: v.last_reachable_at });
    }
    ev.sort((a, b) => Date.parse(b.at) - Date.parse(a.at));
    const seen = new Set<string>();
    return ev.filter((e) => { const k = e.v.address + e.event; if (seen.has(k)) return false; seen.add(k); return true; }).slice(0, 5);
  }, [served, feed, rows, hosts]);

  const [li, setLi] = useState(0);
  const [, tick] = useState(0);
  useEffect(() => {
    if (live.length < 2 || reducedMotion()) { const t = window.setInterval(() => tick((n) => n + 1), 30000); return () => window.clearInterval(t); }
    const t = window.setInterval(() => setLi((i) => i + 1), 5000);
    return () => window.clearInterval(t);
  }, [live.length]);
  const cur = live.length ? live[li % live.length] : null;
  const liveCluster = cur?.host ? clusters.find((c) => c.hosts.includes(cur.host!))?.id : undefined;

  // ---- the land: every country once; hosted ones a shade warmer, the open place's lit ----
  const hosted = useMemo(() => new Set(hosts.map((h) => h.cc)), [hosts]);
  const unchecked = hosts.some((h) => h.state === "none");
  const openCcs = useMemo(() => new Set(clusters.find((c) => c.id === open)?.ccs ?? []), [clusters, open]);
  const land = useMemo(() => (
    <>
      {COUNTRIES.map(([cc, d], i) => (
        <path key={cc || i} d={d} className={openCcs.has(cc) ? "hi" : hosted.has(cc) ? "on" : undefined} />
      ))}
    </>
  ), [hosted, openCcs]);

  // each hosted country's size and own shape with, for every other hosted one, that one's size and how near each of its rings comes to this one's middle
  const beside = useMemo(() => [...hosted].flatMap((cc) => {
    const s = SHAPES.get(cc);
    if (!s) return [];
    const { e, own } = s;
    const others = [...hosted].flatMap((o) => {
      const q = o === cc ? undefined : SHAPES.get(o);
      if (!q) return [];
      return [{ e: q.e, rings: q.rings.map((r) => {
        let d = Infinity;
        for (let i = 0; i < r.pts.length; i += 2) d = Math.min(d, Math.hypot(r.pts[i] - own.x, r.pts[i + 1] - own.y));
        return { e: r.e, d };
      }) }];
    });
    return [{ cc, e, own, others }];
  }), [hosted]);
  // the small ones, by the home view's scale: hidden by the badge, with no shown hosted country's land beside; larger first
  const sHome = width > 0 ? width / home.w : 0;
  const small = useMemo(() => {
    if (!sHome) return [];
    const shown = narrow ? SHOWN_NARROW : SHOWN, near = narrow ? NEAR_NARROW : NEAR;
    return beside.filter(({ e, others }) => e * sHome < shown
      && !others.some((o) => o.e * sHome >= shown && o.rings.some((r) => r.e * sHome >= RING && r.d * sHome < near)))
      .sort((a, b) => b.own.e - a.own.e);
  }, [beside, sHome, narrow]);
  // the coast patches of the TINY ones' windows, fetched once one needs them; failing, those windows draw the country alone
  const needCoast = small.some((q) => q.own.unit);
  const [coast, setCoast] = useState<Coast | null>(null);
  useEffect(() => {
    if (!needCoast || coast) return;
    let dead = false;
    loadCoast().then((c) => { if (!dead) setCoast(c); }, () => { if (!dead) setCoast({ g: 100, hx: 0, hy: 0, patches: {} }); });
    return () => { dead = true; };
  }, [needCoast, coast]);
  // their detail windows, at the home view
  const insets = useMemo(() => layInsets(small, hosts, home, sHome, width, height, barAt, homeClusters, narrow, coast),
    [small, hosts, home, sHome, width, height, barAt, homeClusters, narrow, coast]);
  /** the world's badge that holds most of a country's hosts: a window opens that one */
  const worldOf = (cc: string) => {
    let best: Cluster | undefined, most = 0;
    for (const c of clusters) { const n = c.hosts.filter((h) => h.cc === cc).length; if (n > most) { best = c; most = n; } }
    return best;
  };

  // ---- badges on screen ----
  const placed = width > 0 ? clusters.map((c) => {
    const [x, y] = toPx(c.ux, c.uy);
    const n = c.hosts.length, bw = badgeW(n, narrow), bh = badgeH(n, narrow);
    return { c, x, y, bw, bh, hit: Math.max(24, bw + 4, bh + 4) };
  }).filter((p) => p.x >= p.bw / 2 - 2 && p.x <= width - p.bw / 2 + 2 && p.y >= p.bh / 2 - 2 && p.y <= height - p.bh / 2 + 2) : [];

  // ---- counts ----
  const countries = hosted.size - (hosted.has("") ? 1 : 0);
  const providers = new Set(r.registered.map((v) => providerOf(v.hosting)).filter(Boolean)).size;
  const counts: [number, string, string][] = [
    [hosts.length + unplaced, "Fibre providers", "Fibre provider"],
    [countries, "countries", "country"],
    [providers, "hosting providers", "hosting provider"],
  ];

  return (
    <section className="cm" aria-label="Map of Fibre providers">
      <div className={`cm-atlas${zoomed ? " zoomed" : ""}${open ? " has-open" : ""}`} ref={box}
        onPointerDown={onPointerDown} onPointerMove={onPointerMove} onPointerUp={onPointerUp} onPointerCancel={onPointerUp}
        onDoubleClick={onDoubleClick}>
        <div className="cm-vp">
          {/* drawn once the box is measured: the prerendered page holds the box, not the 100 kB of outlines */}
          {width > 0 && (
            <svg className="cm-land" viewBox={`${view.x} ${view.y} ${view.w} ${view.w * aspect}`} preserveAspectRatio="xMidYMid meet" aria-hidden="true" focusable="false">
              {land}
            </svg>
          )}
        </div>
        {/* the small hosted countries' detail windows: a duplicate of their places for the eye, so out of the tab order and
            the reading; a pointer on one opens its place on the world */}
        {insets.length > 0 && (
          <div className={`cm-insets${zoomed || moving ? " off" : ""}`} aria-hidden="true">
            {insets.map((d) => {
              const world = worldOf(d.cc);
              const bw = badgeW(d.n, narrow), bh = badgeH(d.n, narrow);
              const tone = d.unreach === d.n ? "hold" : d.none === d.n ? "none" : "ok";
              return (
                <div key={d.cc} className={`cm-inset${world && open === world.id ? " open" : ""}`} style={{ left: d.x, top: d.y, width: d.w, height: d.h }}
                  onMouseEnter={() => world && openNow(world.id)} onMouseLeave={closeSoon} onClick={() => world && openNow(world.id)}>
                  <svg viewBox={`${-d.w / 2} ${-d.h / 2} ${d.w} ${d.h}`} width={d.w} height={d.h} focusable="false">
                    <g transform={`translate(${d.dx} ${d.dy}) scale(${d.k}) translate(${-d.cx} ${-d.cy})`}>
                      {d.land.map(([cc, p], i) => <path key={cc || i} d={p} className={openCcs.has(cc) ? "hi" : hosted.has(cc) ? "on" : undefined} />)}
                    </g>
                  </svg>
                  <span className={`cm-inset-b${d.n === 1 ? " one" : ""}`}>
                    <span className="cm-badge" data-tone={tone} style={{ width: bw, height: bh }}>{d.n === 1 ? null : d.n}</span>
                    {d.unreach > 0 && tone !== "hold" && <i className="cm-pip hold" style={{ left: bw - 3, top: -2 }} />}
                  </span>
                  <span className="cm-inset-name">{countryName(d.cc)}</span>
                </div>
              );
            })}
          </div>
        )}
        {/* no heading of its own: the counts on the bar say what the map shows, and "Current Fibre providers" below is the one heading */}
        <div className="cm-title">
          <p className="cm-key" title="Observed by Tensile: whether each registered host answered its latest endpoint check">
            <Eye />
            <span><i className="k-ok" />reachable</span>
            <span><i className="k-hold" />unreachable</span>
            {unchecked && <span><i className="k-none" />not checked yet</span>}
          </p>
        </div>
        <ul className="cm-pins" aria-label="Fibre providers by location">
          {placed.map(({ c, x, y, bw, bh, hit }) => {
            const isOpen = open === c.id;
            const place = placeLabel(c);
            const tally = ORDER.map((s) => [s, c.hosts.filter((h) => h.state === s).length] as const).filter(([, n]) => n > 0);
            const unreach = c.hosts.filter((h) => h.state === "unreachable").length;
            const none = c.hosts.filter((h) => h.state === "none").length;
            const n = c.hosts.length;
            const tone = unreach === n ? "hold" : none === n ? "none" : "ok";
            const split = canSplit(c);
            const multi = c.ccs.length > 1;
            const one = n === 1;
            const cities = c.locs > 1 ? cityCounts(c) : [];
            // beside the badge, on the side with room, and ending above the bar, so it never covers the counts: its list scrolls instead
            const below = y <= height * 0.5, edge = Math.max(bh, 24) / 2, floor = barAt ? Math.min(height, barAt[1]) : height;
            const pop: React.CSSProperties = narrow
              ? { left: -x + 8, top: height - y + 14, width: width - 16 }
              : { [x > width * 0.6 ? "right" : "left"]: bw / 2 + 12, [below ? "top" : "bottom"]: -edge, width: 300, maxHeight: Math.max(220, below ? floor - (y - edge) - 8 : y + edge - 10) };
            return (
              <li key={c.id} className={`cm-pin${one ? " one" : ""}${isOpen ? " open" : ""}${liveCluster === c.id ? " live" : ""}`}
                style={{ transform: `translate(${x.toFixed(1)}px, ${y.toFixed(1)}px)`, zIndex: isOpen ? 30 : undefined }}
                onMouseEnter={() => openNow(c.id)} onMouseLeave={closeSoon}>
                <button type="button" className="cm-b" aria-expanded={split ? undefined : isOpen} aria-describedby="cm-pins-keys"
                  aria-label={`${place}${cities.length ? ` (${cities.map(([k, m]) => (m > 1 ? `${k} ${m}` : k)).join(", ")})` : ""}: ${n} Fibre provider${one ? "" : "s"}, ${tally.map(([s, k]) => `${k} ${STATE_WORD[s]}`).join(", ")}${split ? ". Zoom in" : ""}`}
                  style={{ width: hit, height: hit }}
                  tabIndex={c.id === (placed.some((q) => q.c.id === roving) ? roving : placed[0]?.c.id) ? 0 : -1}
                  onFocus={() => { setRoving(c.id); openNow(c.id); }} onKeyDown={walk} onClick={() => activate(c)}>
                  <span className="cm-badge" data-tone={tone} style={{ width: bw, height: bh }}>{one ? null : n}</span>
                  {unreach > 0 && tone !== "hold" && <i className="cm-pip hold" aria-hidden="true" style={{ left: `calc(50% + ${bw / 2 - 3}px)`, top: `calc(50% - ${bh / 2 + 2}px)` }} />}
                </button>
                {isOpen && !moving && (
                  <div className="cm-pop" style={pop} role="group" aria-label={place}>
                    <p className="cm-pop-h">
                      {c.ccs.length > 2 ? <b>{n} Fibre providers</b> : <><b>{place}</b><span>{n} provider{one ? "" : "s"}</span></>}
                    </p>
                    <ul ref={scrollFade} onScroll={(e) => scrollFade(e.currentTarget)}>
                      {c.hosts.map((h) => (
                        <li key={h.v.address}>
                          <i className="cm-dot" style={{ background: STATE_VAR[h.state] }} title={STATE_WORD[h.state]} />
                          <Link href={valLink(h.v)} className="cm-name">{name(h.v)}</Link>
                          <span className="cm-share">{fmtShare(h.share)}</span>
                          <span className="cm-meta">
                            {[multi ? h.cc : "", h.provider, c.locs > 1 && h.city ? h.city : multi && !h.city ? countryName(h.cc) : "", h.state !== "reachable" ? STATE_WORD[h.state] : ""].filter(Boolean).join(" · ")}
                          </span>
                        </li>
                      ))}
                    </ul>
                    {split && <button type="button" className="cm-zoomin" onClick={() => activate(c)}>Zoom in</button>}
                  </div>
                )}
              </li>
            );
          })}
        </ul>
        <span id="cm-pins-keys" className="sr-only">Arrow keys move between places.</span>
        <div className="cm-tools" role="group" aria-label="Map view">
          <button type="button" aria-label="Zoom in" disabled={target.w <= FRAME.w / MAX_ZOOM + 1} onClick={() => zoomAt(2)}>
            <svg viewBox="0 0 12 12" width="12" height="12" aria-hidden="true"><path d="M6 2v8M2 6h8" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" /></svg>
          </button>
          <button type="button" aria-label="Zoom out" disabled={!zoomed} onClick={() => zoomAt(0.5)}>
            <svg viewBox="0 0 12 12" width="12" height="12" aria-hidden="true"><path d="M2 6h8" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" /></svg>
          </button>
          {zoomed && (
            <button type="button" aria-label="Whole map" onClick={() => go(home)}>
              <svg viewBox="0 0 12 12" width="12" height="12" aria-hidden="true"><path d="M2.2 4.4V2.2h2.2M7.6 2.2h2.2v2.2M9.8 7.6v2.2H7.6M4.4 9.8H2.2V7.6" fill="none" stroke="currentColor" strokeWidth="1.4" strokeLinecap="round" strokeLinejoin="round" /></svg>
            </button>
          )}
        </div>
      </div>

      {/* two pills across the map's foot: the newest event at the left, the three counts in the centre */}
      <div className="cm-bar" ref={bar}>
        <p className="cm-ev" key={cur ? `${cur.v.address}|${cur.at}|${cur.event ?? ""}` : "none"}>
          {cur ? (
            <>
              <i className="cm-ev-dot" data-tone={cur.event === "last reachable" ? "hold" : "ok"} aria-hidden="true" />
              <span className="cm-ev-t" title={`${name(cur.v)}${cur.event ? ` · ${cur.event}` : " served its rows"}${cur.host?.cc ? ` · ${countryName(cur.host.cc)}` : ""}${cur.host?.provider ? ` · ${cur.host.provider}` : ""} · ${utcWord(cur.at)}`}>
                <Link href={valLink(cur.v)}>{name(cur.v)}</Link>
                {cur.event ? <> · {cur.event}</> : <> served its rows</>}
              </span>
              <span className="cm-ev-ago">{shortAgo(cur.at)}</span>
            </>
          ) : <span className="cm-ev-t wait">{" "}</span>}
        </p>
        <ul className="cm-counts" aria-busy={loading || undefined}>
          {counts.map(([n, many, one]) => (
            <li key={many}><b className={loading ? "wait" : undefined}>{loading ? "00" : int(n)}</b><span>{n === 1 && !loading ? one : many}</span></li>
          ))}
        </ul>
      </div>
    </section>
  );
}
