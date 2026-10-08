"use client";
import { useEffect, useLayoutEffect, useMemo, useRef, useState } from "react";
import Link from "next/link";
import { type Validator, API_BASE, ago, utcWord, int } from "@/lib/api";
import { validatorHref } from "@/lib/addr";
import type { Hosting } from "@/lib/hosting";
import { FRAME, COUNTRIES, project, countryPoint } from "@/lib/map/project";
import { countryName } from "@/components/Flag";
import { Eye } from "@/components/Metrics";
import { type EndpointState, endpointState, readiness } from "@/components/Readiness";

/**
 * The overview's host map: every registered Fibre host of the bonded set,
 * placed by the city its address geolocates to (hosting.lat/lon) or, without
 * one, by its country's label point, over the land of Natural Earth's
 * country outlines (Equal Earth) drawn as a board of small squares, the
 * recent blobs' own shape: one tone for every country, none marked, so a
 * place as small as Singapore reads as every other.
 *
 * Every place is a rounded square in the accent carrying its count, set on
 * the same board: at the home view it covers a block of the board's cells,
 * edge to edge; a lone host is a small square with no figure. Hosts close
 * together on screen share one badge (badges never touch). A badge with an
 * unreachable host carries an amber pip, and one whose hosts all stopped
 * answering turns amber; the map
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
/** px: the board stops this far inside the box's top and sides (the stylesheet's clip of .cm-land) */
const INSET = 12;
/**
 * px: a wide box's home view sets the land's northernmost coast this far below its top, so the board ends on the
 * Arctic's own coast inside the inset, not on a row cut straight; it steps back for that by at most ARCTIC_BACK
 */
const ARCTIC = 14, ARCTIC_BACK = 0.08;

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
 * narrower than the box. A wide box then sets the land's top ARCTIC px down, stepping back as little
 * more as that takes, with the badges it found (see ARCTIC).
 */
function fitHome(hosts: Host[], a: number, width: number, bar: [number, number, number, number] | null, narrow: boolean): [View, Cluster[]] {
  const height = width * a;
  const w0 = Math.min(FRAME.w, (BOTTOM - TOP) / a);
  const xs = hosts.map((h) => h.ux), cx = xs.length ? (Math.min(...xs) + Math.max(...xs)) / 2 : FRAME.w / 2;
  const top = narrow ? ROOM_TOP_NARROW : ROOM_TOP;
  const left = (w: number) => (w >= FRAME.w ? (FRAME.w - w) / 2 : Math.min(FRAME.w - w, Math.max(0, cx - w / 2)));
  // the band of view tops that keeps every badge in: lo from the badges that must stay above the foot, hi from those
  // below the top; and the largest scale at which every badge still clears the foot with the land's top ARCTIC px down
  const band = (cs: Cluster[], s: number, x: number) => {
    let lo = -Infinity, hi = Infinity, fits = true, arctic = Infinity;
    for (const c of cs) {
      const px = (c.ux - x) * s, rr = room(c.hosts.length, narrow) / 2;
      if (px - rr < ROOM_SIDE || px + rr > width - ROOM_SIDE) fits = false;
      const under = !!bar && px + rr > bar[0] - 6 && px - rr < bar[0] + bar[2] + 6;
      const foot = under ? bar![1] - 6 : height - ROOM_FOOT;
      hi = Math.min(hi, c.uy - (top + rr) / s);
      lo = Math.max(lo, c.uy - (foot - rr) / s);
      if (c.uy > 0) arctic = Math.min(arctic, (foot - rr - ARCTIC) / c.uy);
    }
    return { lo, hi, fits: fits && lo <= hi, arctic };
  };
  let last: [View, Cluster[]] | null = null;
  for (let i = 0; i <= 12; i++) {
    const w = Math.min(w0 * MAX_BACK, w0 / (1 - 0.03 * i));
    const s = width / w, x = left(w);
    const cs = cluster(hosts, s, narrow);
    const { lo, hi, fits, arctic } = band(cs, s, x);
    const y = Math.min(hi, Math.max(lo, TOP));
    last = [clampView({ w, x, y: lo <= hi ? y : (lo + hi) / 2 }, a, w0 * MAX_BACK), cs];
    if (!fits) continue;
    // the land's top (the frame's own, 0) ARCTIC px down, above the frame's edge: the badges stay the ones found here
    const s2 = Math.min(s, arctic), w2 = width / s2;
    if (!narrow && s2 >= s * (1 - ARCTIC_BACK) && w2 <= Math.min(w0 * MAX_BACK, FRAME.h / a)) {
      const x2 = left(w2), b = band(cs, s2, x2);
      if (b.fits) return [{ w: w2, x: x2, y: Math.min(b.hi, Math.max(b.lo, -ARCTIC / s2)) }, cs];
    }
    break;
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

/**
 * The land is a board of small squares: one in every cell of a lattice with some land in it, PITCH px apart at the
 * home view and SQUARE px each, on whole device pixels there, so every square is crisp. No coast is drawn and no
 * country marked. Zoomed in, the lattice halves at each doubling, so the squares keep about their size on screen. The
 * board is cut in tiles of TILE by TILE cells, each read once from the outlines (filled on a small canvas, a cell lit
 * by its coverage) and drawn as one path of squares.
 */
const PITCH = 4, SQUARE = 2, TILE = 64;
/**
 * a cell is land where at least this much of it is (of 255, about 13%), so a thin peninsula (Malaya, Japan, Taiwan,
 * the Philippines) keeps its squares
 */
const LIT = 34;
/** cells: a lit cell with no other within this many is a speck in open sea, not a coast, and is left out */
const SPECK = 2;
/** the halvings of the lattice: at the deepest zoom the squares are some 8 px apart */
const LEVELS = 3;
/** a lattice in map units: the middle of its cell (0, 0), and the step between two cells */
type Lattice = { x: number; y: number; p: number };
/**
 * the board on the screen's own pixels: PITCH and SQUARE rounded to whole device pixels (pd, sd; each square od in
 * from its cell's corner), so at a display scale of 125% or 150% every square falls alike and the board never beats
 * into bands. p and s are the same in css px, and f is the squares' middle from their lattice point, in cells (half
 * a device pixel where the gap is odd)
 */
type Grid = { p: number; s: number; pd: number; sd: number; od: number; f: number };
function grid(dpr: number): Grid {
  const pd = Math.max(2, Math.round(PITCH * dpr)), sd = Math.max(1, Math.round(SQUARE * dpr)), od = Math.floor((pd - sd) / 2);
  return { pd, sd, od, p: pd / dpr, s: sd / dpr, f: (od + sd / 2) / pd - 0.5 };
}
let LAND: Path2D | null = null, RASTER: CanvasRenderingContext2D | null = null;
/**
 * the land cells of one tile, as squares in device pixels from the tile's corner (a cell g.pd): the cell (i, j) is
 * the lattice's (ti * TILE + i, tj * TILE + j). SPECK cells around it are read too, so a cell's neighbours across the
 * tile's edge count.
 */
function boardTile(o: Lattice, ti: number, tj: number, g: Grid): string {
  const n = TILE + 2 * SPECK;
  if (!RASTER) {
    const c = document.createElement("canvas");
    c.width = c.height = n;
    RASTER = c.getContext("2d", { willReadFrequently: true });
    LAND = new Path2D(COUNTRIES.map(([, d]) => d).join(""));
  }
  const r = RASTER, land = LAND;
  if (!r || !land) return "";
  r.setTransform(1, 0, 0, 1, 0, 0);
  r.clearRect(0, 0, n, n);
  // a cell is the canvas's pixel, the lattice point at its middle; the pixel's alpha is how much of the cell is land
  r.setTransform(1 / o.p, 0, 0, 1 / o.p, 0.5 + SPECK - ti * TILE - o.x / o.p, 0.5 + SPECK - tj * TILE - o.y / o.p);
  r.fill(land);
  const a = r.getImageData(0, 0, n, n).data;
  const lit = (i: number, j: number) => a[(j * n + i) * 4 + 3] >= LIT;
  let d = "";
  for (let j = SPECK; j < SPECK + TILE; j++) for (let i = SPECK; i < SPECK + TILE; i++) {
    if (!lit(i, j)) continue;
    let near = false;
    for (let v = j - SPECK; v <= j + SPECK && !near; v++) for (let u = i - SPECK; u <= i + SPECK; u++) if ((u !== i || v !== j) && lit(u, v)) { near = true; break; }
    if (near) d += `M${(i - SPECK) * g.pd + g.od} ${(j - SPECK) * g.pd + g.od}h${g.sd}v${g.sd}h${-g.sd}z`;
  }
  return d;
}

/**
 * a badge's size in the board's cells: a lone host 3, a place 6, ten hosts or more 7 (a figure of three digits 9
 * wide), so at the home view it covers that block of cells edge to edge and the board's gap around it is its edge
 */
const CELLS = (n: number) => (n === 1 ? 3 : n < 10 ? 6 : 7);
const span = (cells: number, g: Grid) => cells * g.p - (g.p - g.s);
/** a badge's height in px: a little taller for a crowd, so it reads as one; a lone host is a small square */
const badgeH = (n: number, narrow: boolean, g: Grid) => (!narrow ? span(CELLS(n), g) : n === 1 ? 8 : Math.round(17 + Math.min(6, 1.9 * Math.sqrt(n - 1))));
/** its width: square, or wider for a figure of three digits */
const badgeW = (n: number, narrow: boolean, g: Grid) => (!narrow ? span(n < 100 ? CELLS(n) : 9, g) : n === 1 ? 8 : Math.max(badgeH(n, narrow, g), Math.round(String(n).length * 6.3 + 9)));
/**
 * the room a badge takes, for the merging and for keeping clear of the bar: a place's badge as it was before the
 * board (none on the board is larger), so the board changes neither which hosts share a badge nor where the home
 * view sits
 */
function room(n: number, narrow: boolean): number {
  if (n === 1) return narrow ? 8 : 10;
  const h = narrow ? Math.round(17 + Math.min(6, 1.9 * Math.sqrt(n - 1))) : Math.round(21 + Math.min(8, 2.3 * Math.sqrt(n - 1)));
  return Math.max(h, Math.round(String(n).length * (narrow ? 6.3 : 7.2) + (narrow ? 9 : 12)));
}
/**
 * a badge's middle set on the board, in px, where the board's cells are p px and a square's middle is at a (px from
 * the box's corner): a block of odd cells is centred on a cell, an even one between two
 */
const onBoard = (v: number, cells: number, a: number, p: number) => a + (cells % 2 ? Math.round((v - a) / p) : Math.round((v - a) / p - 0.5) + 0.5) * p;
type Placed = { c: Cluster; x: number; y: number; bw: number; bh: number; hit: number };
/**
 * badges set on the board keep at least one of its cells between them: where setting them closed a gap down to the
 * board's own, the smaller of the two moves a cell away, on the side where they are already further apart
 */
function apart(ps: Placed[], g: Grid) {
  const min = 2 * g.p - g.s - 0.01;
  for (let k = 0; k < 6; k++) {
    let moved = false;
    for (let i = 0; i < ps.length; i++) for (let j = i + 1; j < ps.length; j++) {
      const a = ps[i], b = ps[j];
      const gx = Math.abs(a.x - b.x) - (a.bw + b.bw) / 2, gy = Math.abs(a.y - b.y) - (a.bh + b.bh) / 2;
      if (gx >= min || gy >= min) continue;
      const m = a.c.hosts.length < b.c.hosts.length ? a : b, o = m === a ? b : a;
      if (gx >= gy) m.x += (m.x >= o.x ? 1 : -1) * g.p;
      else m.y += (m.y >= o.y ? 1 : -1) * g.p;
      moved = true;
    }
    if (!moved) break;
  }
}

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
      const need = (room(cs[i].hosts.length, narrow) + room(cs[j].hosts.length, narrow)) / 2 + 5;
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

  // the screen's pixels per css px, so the board falls on whole device pixels (see grid); read again when it changes
  const [dpr, setDpr] = useState(1);
  useLayoutEffect(() => {
    let mq: MediaQueryList | null = null;
    const read = () => {
      const d = window.devicePixelRatio || 1;
      setDpr(d);
      mq?.removeEventListener("change", read);
      mq = window.matchMedia(`(resolution: ${d}dppx)`);
      mq.addEventListener("change", read);
    };
    read();
    return () => mq?.removeEventListener("change", read);
  }, []);
  const gd = useMemo(() => grid(dpr), [dpr]);

  // the floating bar, where it lies over the map: measured, so no badge is left under it. And the key, the view
  // buttons and the bar's two pills, where they lie over the box: a zoomed view keeps its badges clear of them
  const bar = useRef<HTMLDivElement>(null);
  const keyEl = useRef<HTMLParagraphElement>(null), toolsEl = useRef<HTMLDivElement>(null);
  const [barAt, setBarAt] = useState<[number, number, number, number] | null>(null);
  const [chrome, setChrome] = useState<number[][]>([]);
  useLayoutEffect(() => {
    const a = box.current?.getBoundingClientRect(), b = bar.current?.getBoundingClientRect();
    let at: [number, number, number, number] | null = null;
    if (a && b && b.top < a.bottom && b.bottom > a.top) at = [Math.round(b.left - a.left), Math.round(b.top - a.top), Math.round(b.width), Math.round(Math.min(b.bottom, a.bottom) - b.top)];
    if (JSON.stringify(at) !== JSON.stringify(barAt)) setBarAt(at);
    const els = [keyEl.current, toolsEl.current, ...Array.from(bar.current?.children ?? [])];
    const rs = a ? els.flatMap((el) => {
      const q = el?.getBoundingClientRect();
      return q && q.width > 2 && q.height > 2 ? [[q.left - a.left, q.top - a.top, q.right - a.left, q.bottom - a.top].map(Math.round)] : [];
    }) : [];
    if (JSON.stringify(rs) !== JSON.stringify(chrome)) setChrome(rs);
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
    if (!zoomed || e.button !== 0 || (e.target as Element).closest(".cm-pin, .cm-tools")) return;
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
    if ((e.target as Element).closest(".cm-pin, .cm-tools")) return;
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
    const onDown = (e: PointerEvent) => { if (!(e.target as Element)?.closest?.(".cm-pin")) setOpen(null); };
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

  // ---- the land: the board's tiles that the view shows, on the lattice of its zoom ----
  const hosted = useMemo(() => new Set(hosts.map((h) => h.cc)), [hosts]);
  const unchecked = hosts.some((h) => h.state === "none");
  // at the home view a cell is gd.p px (PITCH on whole device pixels) from the box's corner, so the squares sit on
  // whole pixels; each doubling of the zoom past it halves the cell
  const p0 = width > 0 ? (gd.p * home.w) / width : 0;
  const lat: Lattice = { x: 0, y: 0, p: p0 / 2 ** Math.max(0, Math.min(LEVELS, Math.round(Math.log2(home.w / view.w)))) };
  lat.x = home.x + lat.p / 2;
  lat.y = home.y + lat.p / 2;
  // a tile is read once per lattice; a new home view (another width) or another screen starts the board again
  const tiles = useRef<{ base: string; d: Map<string, string> }>({ base: "", d: new Map() });
  const board: React.ReactNode[] = [];
  if (width > 0) {
    const base = `${p0}|${home.x}|${home.y}|${gd.pd}|${gd.sd}`;
    if (tiles.current.base !== base) tiles.current = { base, d: new Map() };
    const cache = tiles.current.d, p = lat.p;
    const tile = (v: number, o: number) => Math.floor(Math.floor((v - o) / p + 0.5) / TILE);
    for (let tj = tile(view.y, lat.y); tj <= tile(view.y + view.w * aspect, lat.y); tj++) {
      for (let ti = tile(view.x, lat.x); ti <= tile(view.x + view.w, lat.x); ti++) {
        const key = `${p}|${ti}|${tj}`;
        let d = cache.get(key);
        if (d === undefined) { d = boardTile(lat, ti, tj, gd); cache.set(key, d); }
        // the tile's squares in device pixels of a cell, from its first cell's corner
        if (d) board.push(<path key={key} d={d} transform={`translate(${home.x + ti * TILE * p} ${home.y + tj * TILE * p}) scale(${p / gd.pd})`} />);
      }
    }
  }

  // ---- badges on screen: at rest on a cell of gd.p px (the home view, or a zoom by whole doublings) each set on the board ----
  const settled = !narrow && !moving && Math.abs(lat.p * scale - gd.p) < 0.01;
  // a cell on screen, and the middle of the squares of the cell (0, 0), in px from the box's corner
  const cp = lat.p * scale;
  const ax = (lat.x - view.x) * scale + gd.f * cp, ay = (lat.y - view.y) * scale + gd.f * cp;
  const placed: Placed[] = width > 0 ? clusters.map((c) => {
    const [ux, uy] = toPx(c.ux, c.uy);
    const n = c.hosts.length, bw = badgeW(n, narrow, gd), bh = badgeH(n, narrow, gd);
    const x = settled ? onBoard(ux, n < 100 ? CELLS(n) : 9, ax, gd.p) : ux, y = settled ? onBoard(uy, CELLS(n), ay, gd.p) : uy;
    return { c, x, y, bw, bh, hit: Math.max(24, bw + 4, bh + 4) };
  }).filter((p) => p.x >= p.bw / 2 - 2 && p.x <= width - p.bw / 2 + 2 && p.y >= p.bh / 2 - 2 && p.y <= height - p.bh / 2 + 2) : [];
  if (settled) apart(placed, gd);
  // zoomed in, a badge shows only where the view is open: on the board (INSET in from the top and sides) and clear of
  // the key, the view buttons and the two pills. One past them is past the view's edge, a drag away
  const M = 4;
  const shown = !zoomed || narrow ? placed : placed.filter((p) => p.x - p.bw / 2 >= INSET + M && p.x + p.bw / 2 <= width - INSET - M && p.y - p.bh / 2 >= INSET + M
    && !chrome.some(([l, t, rt, b]) => p.x + p.bw / 2 > l - M && p.x - p.bw / 2 < rt + M && p.y + p.bh / 2 > t - M && p.y - p.bh / 2 < b + M));

  // the cells around each badge are land, so none sits on open sea: its block's edge cells, the corners left as the
  // land has them
  let rim = "";
  for (const b of shown) {
    const bx = (b.x - ax) / cp, by = (b.y - ay) / cp, hx = b.bw / 2 / cp, hy = b.bh / 2 / cp;
    for (let j = Math.ceil(by - hy - 1); j <= Math.floor(by + hy + 1); j++) for (let i = Math.ceil(bx - hx - 1); i <= Math.floor(bx + hx + 1); i++) {
      if ((Math.abs(i - bx) > hx) !== (Math.abs(j - by) > hy)) rim += `M${i * gd.pd + gd.od} ${j * gd.pd + gd.od}h${gd.sd}v${gd.sd}h${-gd.sd}z`;
    }
  }

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
          {/* drawn once the box is measured: the prerendered page holds the box, not the board */}
          {width > 0 && (
            <svg className="cm-land" viewBox={`${view.x} ${view.y} ${view.w} ${view.w * aspect}`} preserveAspectRatio="xMidYMid meet" aria-hidden="true" focusable="false">
              {board}
              {rim && <path d={rim} transform={`translate(${home.x} ${home.y}) scale(${lat.p / gd.pd})`} />}
            </svg>
          )}
        </div>
        {/* no heading of its own: the counts on the bar say what the map shows, and "Current Fibre providers" below is the one heading */}
        <div className="cm-title">
          <p className="cm-key" ref={keyEl} title="Observed by Tensile: whether each registered host answered its latest endpoint check">
            <Eye />
            <span><i className="k-ok" />reachable</span>
            <span><i className="k-hold" />unreachable</span>
            {unchecked && <span><i className="k-none" />not checked yet</span>}
          </p>
        </div>
        <ul className="cm-pins" aria-label="Fibre providers by location">
          {shown.map(({ c, x, y, bw, bh, hit }) => {
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
                  tabIndex={c.id === (shown.some((q) => q.c.id === roving) ? roving : shown[0]?.c.id) ? 0 : -1}
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
        <div className="cm-tools" ref={toolsEl} role="group" aria-label="Map view">
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
