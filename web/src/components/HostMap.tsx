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
 * one, by its country's label point, over Natural Earth country outlines
 * (Equal Earth) drawn as one calm land tone on a sea with a faint graticule.
 * No country is marked: the badges alone say where the hosts are, so a
 * country too small to see (Singapore, Hong Kong) needs nothing drawn for it.
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
/** a wide home view sits this many px lower than that fit (as far as the badges at its foot allow), so the far north's coasts have a little sea over them rather than meeting the box's top */
const SEA_TOP = 8;

/**
 * the graticule every 30°, under the land: meridians curve, parallels are straight; the meridians at 180° are the
 * world's own edge. In map units, drawn once
 */
const GRID = (() => {
  const f = (n: number) => n.toFixed(1);
  let d = "";
  for (let lon = -180; lon <= 180; lon += 30) for (let lat = -90; lat <= 90; lat += 2) { const [x, y] = project(lon, lat); d += `${lat === -90 ? "M" : "L"}${f(x)} ${f(y)}`; }
  for (let lat = -60; lat <= 60; lat += 30) { const [x0, y] = project(-180, lat), [x1] = project(180, lat); d += `M${f(x0)} ${f(y)}H${f(x1)}`; }
  return d;
})();

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
 * top at 72°N leaves a badge outside, the view moves north or south as far as the others allow, and
 * a wide view then sits SEA_TOP lower where the foot allows; where no position does, it steps back a little and tries again, until the world is MAX_BACK times
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
    // at 72°N or as near as the badges allow, then a wide view SEA_TOP lower still, as far as the badges at its foot allow
    const y = Math.max(lo, Math.min(hi, Math.max(lo, TOP)) - (narrow ? 0 : SEA_TOP / s));
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

/** a badge's height in px: a little taller for more hosts, so a crowd reads as one (24 to 29 px); a lone host is a small square */
const badgeH = (n: number, narrow: boolean) => (n === 1 ? (narrow ? 8 : 10) : narrow ? Math.round(17 + Math.min(6, 1.9 * Math.sqrt(n - 1))) : Math.round(22 + Math.min(7, 2.3 * Math.sqrt(n - 1))));
/** its width: square, or wider for a figure of three digits */
const badgeW = (n: number, narrow: boolean) => (n === 1 ? badgeH(1, narrow) : Math.max(badgeH(n, narrow), Math.round(String(n).length * (narrow ? 6.3 : 7.8) + (narrow ? 9 : 12))));
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

  // the floating bar, where it lies over the map: measured, so no badge is left under it. Its pills end with the
  // counts (the column after them is empty), so it is measured to there and a badge beyond them may sit lower
  const bar = useRef<HTMLDivElement>(null);
  const [barAt, setBarAt] = useState<[number, number, number, number] | null>(null);
  useLayoutEffect(() => {
    const a = box.current?.getBoundingClientRect(), b = bar.current?.getBoundingClientRect();
    const end = bar.current?.querySelector(".cm-counts")?.getBoundingClientRect().right ?? b?.right ?? 0;
    let at: [number, number, number, number] | null = null;
    if (a && b && b.top < a.bottom && b.bottom > a.top) at = [Math.round(b.left - a.left), Math.round(b.top - a.top), Math.round(end - b.left), Math.round(Math.min(b.bottom, a.bottom) - b.top)];
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

  // ---- the land: the graticule, then every country once, all in one tone ----
  const hosted = useMemo(() => new Set(hosts.map((h) => h.cc)), [hosts]);
  const unchecked = hosts.some((h) => h.state === "none");
  const land = useMemo(() => (
    <>
      <path className="cm-grid" d={GRID} />
      {COUNTRIES.map(([cc, d], i) => <path key={cc || i} d={d} />)}
    </>
  ), []);

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
