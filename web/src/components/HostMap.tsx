"use client";
import { useEffect, useLayoutEffect, useMemo, useRef, useState } from "react";
import Link from "next/link";
import { type Validator, type Probe, API_BASE, useApi, ago, utcWord, int } from "@/lib/api";
import type { Hosting } from "@/lib/hosting";
import { FRAME, COUNTRIES, project, countryPoint } from "@/lib/map/project";
import { verdictDef } from "@/components/Verdict";
import { countryName } from "@/components/Flag";
import Info from "@/components/Info";
import { type EndpointState, endpointState, readiness } from "@/components/Readiness";

/**
 * The overview's host map: every registered Fibre host of the bonded set,
 * placed by the city its address geolocates to (hosting.lat/lon) or, without
 * one, by its country's label point, over Natural Earth country outlines drawn
 * as one dot field, hosted countries lit in the accent. Above it the three
 * counts; in its corner the key of Tensile's endpoint check and the zoom;
 * under it, on a line of its own, the newest event.
 *
 * Every place is a disc carrying its count, sized by it, its ring split by
 * endpoint state; hosts close together on screen share one disc (discs never
 * touch), and in the dark theme a crowd glows in proportion to its count.
 * Every disc is named the same way: its place with the most hosts, and "+n"
 * for the other places it holds, which its popover lists. A name goes beside
 * its disc where it fits, else a step out on a short leader, never nearer to
 * another disc than to its own; one that fits nowhere is left to the popover.
 * A disc that holds several places zooms in on them.
 *
 * Beside the map, the stake gauge, then the latest blob (`aside`).
 */

/** ⅔ set from the text face's own numerals, on its baseline: the font's fraction glyph falls back heavier and off the line */
function Frac() {
  return <b className="ov-frac"><span className="ov-frac-n">2</span><span className="ov-frac-s">⁄</span><span className="ov-frac-d">3</span></b>;
}

/**
 * The stake gauge: the share of voting power with a Fibre provider as the
 * panel's lead figure, then one bar on a ruler of tenths with the ⅔ a blob
 * needs to settle drawn as a needle through it. Both halves are the chain's
 * own records (x/staking, x/valaddr); see Readiness.tsx.
 */
function StakeGauge({ rows }: { rows: Validator[] }) {
  const r = readiness(rows);
  if (r.total === 0) return null;
  const { pct, regPower, quorum, total } = r;
  const at = (n: number) => `${Math.min(100, Math.max(0, (100 * n) / total))}%`;
  const vars = { "--v": at(regPower), "--q": at(quorum) } as React.CSSProperties;
  return (
    <div className="ov-stake">
      <h2 id="readiness-h" className="ov-eyebrow">Stake with a Fibre provider<Info label="Stake with a Fibre provider"><p>Share of the stake held by validators with a Fibre provider. A blob needs signatures from ⅔ of the stake to settle.</p></Info></h2>
      <p className="ov-hero">
        <b className="ov-fig">{pct(regPower)}</b>
        <span className="ov-hero-help"> of voting power · {int(r.registered.length)} of {int(r.bonded.length)} validators</span>
      </p>
      <div className="ov-gauge" style={vars} role="img" aria-label={`${pct(regPower)} of stake with a Fibre provider, ${pct(quorum)} needed`}>
        <span className="ov-needle"><span><Frac /> needed</span></span>
        <span className="ov-track"><i /></span>
        <span className="ov-ruler">{Array.from({ length: 11 }, (_, i) => <i key={i} />)}</span>
      </div>
      <p className="ov-rest">
        <span><i className="ov-sw" /> no Fibre provider {pct(total - regPower)} · {int(r.bonded.length - r.registered.length)}</span>
      </p>
    </div>
  );
}

type Host = { v: Validator; state: EndpointState; share: number; cc: string; city: string; loc: string; lon: number; lat: number; ux: number; uy: number; provider: string };
type Cluster = { id: string; hosts: Host[]; ux: number; uy: number; locs: number; ccs: string[] };
/** the visible part of the map, in map units: top-left corner and width (height follows the frame) */
type View = { x: number; y: number; w: number };

const STATE_WORD: Record<EndpointState, string> = { reachable: "reachable", unreachable: "unreachable", none: "not checked yet" };
// The table's colours: amber for a host that stopped answering.
const STATE_VAR: Record<EndpointState, string> = { reachable: "var(--accent)", unreachable: "var(--hold)", none: "var(--pending)" };
const ORDER: EndpointState[] = ["reachable", "unreachable", "none"];

/**
 * the box's height over its width. On a wide screen the world at a width that crops a
 * little open Pacific at the sides (the edges fade out), and at the bottom cut under
 * the southern capes, near 46°S, where no host sits, so the map holds no empty band
 * below Cape Town. A taller crop on a phone, the whole height.
 */
const HOME_W = Math.min(FRAME.w, FRAME.h / 0.5);
const SOUTH = project(0, -46)[1];
const WIDE = SOUTH / HOME_W, TALL = 0.62;
/** the room a view keeps beside the outermost hosts, as a share of their spread */
const SIDE = 0.1;
/** how wide a view the hosts need: their spread and the room beside it, never more than the home width */
const hostsWidth = (pts: [number, number][]) => {
  if (!pts.length) return HOME_W;
  const xs = pts.map(([x]) => x);
  return Math.min(HOME_W, (Math.max(...xs) - Math.min(...xs)) * (1 + 2 * SIDE));
};
const MAX_ZOOM = 12;

function clampView(v: View, a: number): View {
  const w = Math.min(FRAME.w, FRAME.h / a, Math.max(FRAME.w / MAX_ZOOM, v.w)), h = w * a;
  return { w, x: Math.min(FRAME.w - w, Math.max(0, v.x)), y: Math.min(FRAME.h - h, Math.max(0, v.y)) };
}
/** the view that shows every point, with room around them */
function fitView(pts: [number, number][], a: number, pad = 0.35, minW = FRAME.w / MAX_ZOOM): View {
  let x0 = Infinity, y0 = Infinity, x1 = -Infinity, y1 = -Infinity;
  for (const [x, y] of pts) { x0 = Math.min(x0, x); y0 = Math.min(y0, y); x1 = Math.max(x1, x); y1 = Math.max(y1, y); }
  const w = Math.max(minW, (x1 - x0) * (1 + 2 * pad), ((y1 - y0) * (1 + 2 * pad)) / a);
  return clampView({ w, x: (x0 + x1) / 2 - w / 2, y: (y0 + y1) / 2 - (w * a) / 2 }, a);
}
/**
 * the whole world (to the southern capes), or on a phone the widest crop of it that keeps the
 * hosts in the middle. A wide box taller than that crop (the panel beside it is taller than
 * the map) is filled by closing in on the hosts, never closer than they need.
 */
function homeView(pts: [number, number][], a: number, wide: boolean): View {
  const w = wide ? Math.min(HOME_W, Math.max(hostsWidth(pts), SOUTH / a)) : Math.min(FRAME.w, FRAME.h / a);
  const xs = pts.map(([x]) => x), cx = xs.length ? (Math.min(...xs) + Math.max(...xs)) / 2 : FRAME.w / 2;
  return clampView({ w, x: cx - w / 2, y: 0 }, a);
}

function providerOf(h?: Hosting): string {
  if (!h || h.status === "unresolved") return "";
  if (h.provider && h.provider !== "Other" && h.provider !== "Unknown") return h.provider;
  const org = (h.as_org ?? "").split(/\s+/)[0].replace(/^AS-/i, "").replace(/-ASN?(-\d+)?$/i, "").replace(/\d+$/, "");
  if (!org) return "";
  return org === org.toUpperCase() && org.length > 3 ? org.charAt(0) + org.slice(1).toLowerCase() : org;
}

const valLink = (v: Validator) => `/validator/?addr=${encodeURIComponent(v.cons_address || v.address)}`;
const name = (v: Validator) => v.moniker || v.operator_address || v.address;
const fmtShare = (s: number) => { const p = s * 100; return p >= 0.1 ? `${p.toFixed(1)}%` : p > 0 ? "<0.1%" : "0%"; };

/** badge diameter in px: a little larger for more hosts, so a crowd reads as one */
const badge = (n: number, narrow: boolean) => Math.round((narrow ? 22 : 24) + Math.min(12, 3.2 * Math.sqrt(n - 1)));
/** a lone host is the smallest disc, a step under the smallest group's */
const drawn = (n: number, narrow: boolean) => (n === 1 ? (narrow ? 17 : 18) : badge(n, narrow));
/** a crowd's glow: 0 for one host, rising to 1 at about twenty */
const glowOf = (n: number) => Math.min(1, Math.sqrt((n - 1) / 20));

/** hosts -> clusters: one per place (city, else country), then merged while two discs would touch on screen */
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
      // Two discs as drawn, their rings and halos clear of each other.
      const need = (drawn(cs[i].hosts.length, narrow) + drawn(cs[j].hosts.length, narrow)) / 2 + 4;
      if (d < need && (!best || d - need < best[2])) best = [i, j, d - need];
    }
    if (!best) break;
    const a = cs[best[0]], b = cs[best[1]], na = a.hosts.length, nb = b.hosts.length;
    const merged: C = { hosts: [...a.hosts, ...b.hosts], ux: (a.ux * na + b.ux * nb) / (na + nb), uy: (a.uy * na + b.uy * nb) / (na + nb) };
    cs = cs.filter((_, i) => i !== best![0] && i !== best![1]).concat(merged);
  }
  return cs.map((c) => {
    const hosts = [...c.hosts].sort((x, y) => (y.v.voting_power || 0) - (x.v.voting_power || 0));
    // Countries by host count, so the first is the biggest share of the disc.
    const n = new Map<string, number>();
    for (const h of hosts) if (h.cc) n.set(h.cc, (n.get(h.cc) ?? 0) + 1);
    const ccs = [...n.entries()].sort((p, q) => q[1] - p[1]).map(([cc]) => cc);
    // A stable id whatever order the merges ran in.
    return { id: hosts.map((h) => h.v.address).sort().join(","), hosts, ux: c.ux, uy: c.uy, locs: new Set(hosts.map((h) => h.loc)).size, ccs };
  }).sort((x, y) => x.ux - y.ux);
}

function ring(hosts: Host[]): string {
  const n = hosts.length;
  let at = 0;
  const stops: string[] = [];
  for (const s of ORDER) {
    const k = hosts.filter((h) => h.state === s).length;
    if (!k) continue;
    stops.push(`${STATE_VAR[s]} ${(at / n) * 360}deg ${((at + k) / n) * 360}deg`);
    at += k;
  }
  return `conic-gradient(${stops.join(", ")})`;
}

/** what a disc is called: its city, its country, or its countries */
function placeLabel(c: Cluster): string {
  if (c.locs === 1 && c.hosts[0].city) return `${c.hosts[0].city}, ${countryName(c.hosts[0].cc)}`;
  if (c.ccs.length === 1) return countryName(c.ccs[0]);
  if (c.ccs.length === 2) return c.ccs.map(countryName).join(", ");
  return `${c.ccs.length} countries`;
}
/** a disc's places, most hosts first: its cities (or countries, where no city is known) */
function cityCounts(c: Cluster): [string, number][] {
  const n = new Map<string, number>();
  for (const h of c.hosts) { const k = h.city || countryName(h.cc); n.set(k, (n.get(k) ?? 0) + 1); }
  return [...n.entries()].sort((p, q) => q[1] - p[1] || p[0].localeCompare(q[0]));
}
/** the name beside a disc: its place with the most hosts, and how many other places it holds */
function tagText(c: Cluster): { text: string; more: number } {
  const places = cityCounts(c);
  return places.length ? { text: places[0][0], more: places.length - 1 } : { text: "", more: 0 };
}

let measureCtx: CanvasRenderingContext2D | null | undefined;
function textWidth(s: string): number {
  if (measureCtx === undefined) {
    try { measureCtx = document.createElement("canvas").getContext("2d"); if (measureCtx) measureCtx.font = "500 12px 'IBM Plex Sans', system-ui, sans-serif"; } catch { measureCtx = null; }
  }
  return measureCtx ? measureCtx.measureText(s).width : s.length * 6.6;
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

// ---- the network feed: host registrations, for the line under the map before any blob settles ----
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
    const t = window.setInterval(load, 120000);
    return () => { dead = true; window.clearInterval(t); };
  }, [enabled]);
  return events;
}

const EVENT_WORD: Record<string, string> = { registered: "registered as a Fibre provider", "host-changed": "changed its Fibre host" };

export default function HostMap({ rows, showReadiness, aside }: { rows: Validator[]; showReadiness: boolean; /** shown under the stake panel */ aside?: React.ReactNode }) {
  const r = readiness(rows);
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

  // ---- size: the box keeps at least the crop's shape, so measuring it never shifts the layout ----
  const box = useRef<HTMLDivElement>(null);
  const [width, setWidth] = useState(760);
  // the box's own height: beside the panel it grows to the panel's (CSS), up to what the hosts allow
  const [boxH, setBoxH] = useState(0);
  // the map is drawn once there is a host to place; measure whenever it (re)appears
  const mapShown = total > 0 && (hosts.length > 0 || r.registered.length > 0);
  useLayoutEffect(() => {
    const el = box.current;
    if (!el) return;
    setWidth(el.clientWidth || 760);
    setBoxH(el.clientHeight);
    const ro = new ResizeObserver(() => { setWidth(el.clientWidth || 760); setBoxH(el.clientHeight); });
    ro.observe(el);
    return () => ro.disconnect();
  }, [mapShown]);
  // the key and zoom in the map's corner: measured, so no place name is set under them
  const tools = useRef<HTMLDivElement>(null);
  const [toolsAt, setToolsAt] = useState<[number, number, number, number] | null>(null);
  useLayoutEffect(() => {
    const el = tools.current;
    const at: [number, number, number, number] | null = el ? [el.offsetLeft, el.offsetTop, el.offsetWidth, el.offsetHeight] : null;
    if (JSON.stringify(at) !== JSON.stringify(toolsAt)) setToolsAt(at);
  });
  const narrow = width < 560;
  // A wide box is at least the crop's shape and grows with the panel beside it, at most to the
  // shape at which the whole frame's height still spans the hosts' width
  const tallest = FRAME.h / hostsWidth(hosts.map((h) => [h.ux, h.uy]));
  const aspect = narrow ? TALL : Math.round(1000 * Math.min(Math.max(WIDE, tallest), Math.max(WIDE, boxH / Math.max(1, width)))) / 1000;
  const height = width * aspect;
  const home = useMemo(() => homeView(hosts.map((h) => [h.ux, h.uy]), aspect, !narrow), [hosts, aspect, narrow]);

  // ---- view: where the map is looking, animated toward a target ----
  const [view, setView] = useState<View>(home);
  const [target, setTarget] = useState<View>(home);
  const viewRef = useRef(view);
  viewRef.current = view;
  const raf = useRef(0);
  // A new shape of box starts again from its home view; a data refresh that leaves home where it was does not.
  const homeKey = `${aspect}|${Math.round(home.x)}|${Math.round(home.w)}`;
  useLayoutEffect(() => { cancelAnimationFrame(raf.current); setView(home); setTarget(home); }, [homeKey]); // eslint-disable-line react-hooks/exhaustive-deps
  const go = (to: View) => {
    const t = clampView(to, aspect), from = viewRef.current;
    setTarget(t);
    cancelAnimationFrame(raf.current);
    if (reducedMotion()) { setView(t); return; }
    const t0 = performance.now();
    const fcx = from.x + from.w / 2, fcy = from.y + (from.w * aspect) / 2, tcx = t.x + t.w / 2, tcy = t.y + (t.w * aspect) / 2;
    const step = (now: number) => {
      const p = Math.min(1, (now - t0) / 420), e = 1 - Math.pow(1 - p, 3);
      const w = from.w * Math.pow(t.w / from.w, e), cx = fcx + (tcx - fcx) * e, cy = fcy + (tcy - fcy) * e;
      setView(p < 1 ? { w, x: cx - w / 2, y: cy - (w * aspect) / 2 } : t);
      if (p < 1) raf.current = requestAnimationFrame(step);
    };
    raf.current = requestAnimationFrame(step);
  };
  useEffect(() => () => cancelAnimationFrame(raf.current), []);
  const moving = view !== target;
  const zoomed = target.w < home.w - 1;
  const scale = width / view.w; // px per map unit, now
  const toPx = (ux: number, uy: number): [number, number] => [(ux - view.x) * scale, (uy - view.y) * scale];

  // Discs merge by the zoom the view is heading to, so they do not re-merge mid-flight.
  const clusters = useMemo(() => cluster(hosts, width / target.w, narrow), [hosts, width, target.w, narrow]);

  // ---- drag to pan, once zoomed in ----
  const drag = useRef<{ id: number; x: number; y: number; v: View; moved: boolean } | null>(null);
  const onPointerDown = (e: React.PointerEvent) => {
    if (!zoomed || e.button !== 0 || (e.target as Element).closest(".ov-pin, .ov-map-tools")) return;
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

  const zoomBy = (f: number) => {
    const t = target, cx = t.x + t.w / 2, cy = t.y + (t.w * aspect) / 2, w = t.w / f;
    go({ w, x: cx - w / 2, y: cy - (w * aspect) / 2 });
  };

  // ---- the pins are one stop in the tab order: the arrow keys walk them, west to east ----
  const [roving, setRoving] = useState<string | null>(null);
  const walk = (e: React.KeyboardEvent<HTMLButtonElement>) => {
    const step = e.key === "ArrowRight" || e.key === "ArrowDown" ? 1 : e.key === "ArrowLeft" || e.key === "ArrowUp" ? -1 : e.key === "Home" ? -Infinity : e.key === "End" ? Infinity : 0;
    if (!step) return;
    const all = [...(e.currentTarget.closest("ul")?.querySelectorAll<HTMLButtonElement>(".ov-b") ?? [])];
    const i = all.indexOf(e.currentTarget);
    const j = step === -Infinity ? 0 : step === Infinity ? all.length - 1 : Math.min(all.length - 1, Math.max(0, i + step));
    all[j]?.focus();
    e.preventDefault();
  };

  // ---- open cluster (hover, focus, tap) ----
  const [open, setOpen] = useState<string | null>(null);
  const closeT = useRef<number | undefined>(undefined);
  const openNow = (id: string) => { window.clearTimeout(closeT.current); setOpen(id); };
  const closeSoon = () => { window.clearTimeout(closeT.current); closeT.current = window.setTimeout(() => setOpen(null), 160); };
  useEffect(() => {
    if (!open) return;
    const onKey = (e: KeyboardEvent) => { if (e.key === "Escape") setOpen(null); };
    const onDown = (e: PointerEvent) => { if (!(e.target as Element)?.closest?.(".ov-pin")) setOpen(null); };
    document.addEventListener("keydown", onKey);
    document.addEventListener("pointerdown", onDown);
    return () => { document.removeEventListener("keydown", onKey); document.removeEventListener("pointerdown", onDown); };
  }, [open]);
  const canSplit = (c: Cluster) => c.locs > 1 && target.w > FRAME.w / MAX_ZOOM + 1;
  const activate = (c: Cluster) => {
    if (canSplit(c)) {
      // Zoom until the places in this disc come apart: at least twice as close, at most the zoom limit.
      const fit = fitView(c.hosts.map((h) => [h.ux, h.uy]), aspect);
      const w = Math.min(fit.w, target.w / 2);
      setOpen(null);
      go({ w, x: fit.x + fit.w / 2 - w / 2, y: fit.y + (fit.w * aspect) / 2 - (w * aspect) / 2 });
    } else openNow(c.id);
  };

  // ---- the line under the map: who served most recently, or else the newest host events ----
  const blobs = rows.some((v) => (v.obligations?.total ?? 0) > 0);
  const probes = useApi<{ probes: Probe[] }>(blobs ? "/v1/probes?limit=60" : null, 30000);
  type Live = { v: Validator; host?: Host; event?: string; at: string };
  const served: Live[] = useMemo(() => {
    const byAddr = new Map(hosts.map((h) => [h.v.address.toLowerCase(), h]));
    const seen = new Set<string>();
    const out: Live[] = [];
    for (const p of probes.data?.probes ?? []) {
      const a = p.validator_address.toLowerCase(), h = byAddr.get(a);
      if (!h || seen.has(a) || verdictDef(p.classification || p.outcome).tier !== "kept") continue;
      seen.add(a);
      out.push({ v: h.v, host: h, at: p.started_at });
      if (out.length === 5) break;
    }
    return out;
  }, [hosts, probes.data]);
  const feed = useFeedEvents(served.length === 0);
  const live: Live[] = useMemo(() => {
    if (served.length) return served;
    const byAddr = new Map(rows.map((v) => [v.address.toLowerCase(), v]));
    const hostOf = new Map(hosts.map((h) => [h.v.address.toLowerCase(), h]));
    const ev: Live[] = [];
    for (const e of feed) {
      const v = byAddr.get(e.addr);
      if (v && EVENT_WORD[e.term]) ev.push({ v, host: hostOf.get(e.addr), event: EVENT_WORD[e.term], at: e.at });
    }
    // A registered host that stopped answering, dated by its last successful check.
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

  // ---- the land: every country once; hosted ones lit, the open disc's brighter ----
  const hosted = useMemo(() => new Set(hosts.map((h) => h.cc)), [hosts]);
  // a registered host this observer has not checked yet gets its own entry in the key
  const unchecked = hosts.some((h) => h.state === "none");
  const openCcs = useMemo(() => new Set(clusters.find((c) => c.id === open)?.ccs ?? []), [clusters, open]);
  const land = useMemo(() => (
    <>
      {/* a faint wash under a lit country's dots, so a lit region reads as one shape */}
      {COUNTRIES.map(([cc, d], i) => hosted.has(cc) ? <path key={`w${cc || i}`} d={d} className={`ov-wash${openCcs.has(cc) ? " hi" : ""}`} /> : null)}
      {COUNTRIES.map(([cc, d], i) => (
        <path key={cc || i} d={d} className={openCcs.has(cc) ? "hi" : hosted.has(cc) ? "on" : undefined} />
      ))}
    </>
  ), [hosted, openCcs]);
  // The dots keep their spacing on screen: they grow with the zoom and halve their pitch every doubling.
  const pitch = ((narrow ? 4.8 : 5.4) * home.w) / Math.max(1, width) / Math.pow(2, Math.max(0, Math.round(Math.log2(home.w / view.w))));
  const dots = (["d", "on", "hi"] as const).map((k) => (
    <pattern key={k} id={`ov-dots-${k}`} patternUnits="userSpaceOnUse" width={pitch} height={pitch}>
      <circle className={`ov-dot-${k}`} cx={pitch / 2} cy={pitch / 2} r={pitch * (k === "d" ? 0.26 : 0.31)} />
    </pattern>
  ));

  if (total === 0) return null;
  if (!mapShown) {
    // Nothing to place yet: the readiness answer alone, as before.
    return showReadiness ? (
      <section className="band readiness" aria-labelledby="readiness-h">
        <div className="fm-side ov-solo"><StakeGauge rows={rows} />{aside && <div className="fm-aside">{aside}</div>}</div>
      </section>
    ) : null;
  }

  // ---- discs on screen, and their names where they fit ----
  const placed = clusters.map((c) => {
    const [x, y] = toPx(c.ux, c.uy);
    // d: the hit target, never under 24px; s: the disc that is drawn
    const s = drawn(c.hosts.length, narrow);
    return { c, x, y, d: Math.max(24, s), s };
  }).filter((p) => p.x >= p.s / 2 - 2 && p.x <= width - p.s / 2 + 2 && p.y >= p.s / 2 - 2 && p.y <= height - p.s / 2 + 2);
  type Rect = [number, number, number, number];
  const hit = (a: Rect, b: Rect) => a[0] < b[0] + b[2] && b[0] < a[0] + a[2] && a[1] < b[1] + b[3] && b[1] < a[1] + a[3];
  const grow = (r: Rect, m: number): Rect => [r[0] - m, r[1] - m, r[2] + 2 * m, r[3] + 2 * m];
  const discRect = (p: { x: number; y: number; s: number }): Rect => [p.x - p.s / 2, p.y - p.s / 2, p.s, p.s];
  /** how far a rectangle is from a point */
  const gapTo = (r: Rect, x: number, y: number) => Math.hypot(Math.max(r[0] - x, 0, x - (r[0] + r[2])), Math.max(r[1] - y, 0, y - (r[1] + r[3])));
  // Every name reads the same way: beside its disc on the right, level with
  // it, where it fits; else just above or below on the right; else the same
  // a step out on a leader; only then the left, in the same order. Never
  // straight above or under a disc, where it could name either of two. It
  // may not cover a disc or another name, keeps clear of every other disc,
  // and must sit clearly nearer its own disc than any other; a name that
  // fits nowhere is left to the popover.
  type Tag = { x: number; y: number; w: number; text: string; more: number; lead?: { x: number; y: number; len: number; ang: number } };
  const tags = new Map<string, Tag>();
  if (!moving) {
    const labels: Rect[] = [];
    const NEAR = 7, OUT = 20;
    for (const p of [...placed].sort((a, b) => b.c.hosts.length - a.c.hosts.length || a.x - b.x)) {
      const { text, more } = tagText(p.c);
      if (!text) continue;
      const full = more > 0 ? `${text} +${more}` : text;
      const w = Math.ceil(10 + textWidth(full)), h = 18;
      const others = placed.filter((q) => q !== p);
      const spots: [number, number, number][] = [];
      for (const side of [1, -1]) for (const step of [NEAR, OUT]) {
        const r = p.s / 2 + step, d = r * 0.7;
        const xs = side > 0 ? r : -r - w, xd = side > 0 ? d : -d - w;
        spots.push([step, xs, -h / 2], [step, xd, -d - h], [step, xd, d]);
      }
      for (const [step, dx, dy] of spots) {
        const rect: Rect = [p.x + dx, p.y + dy, w, h];
        if (rect[0] < 2 || rect[0] + w > width - 2 || rect[1] < 2 || rect[1] + h > height - 2) continue;
        if (hit(grow(discRect(p), 3), rect)) continue;
        if (toolsAt && hit(grow(toolsAt, 4), rect)) continue;
        if (others.some((q) => hit(grow(discRect(q), 8), rect))) continue;
        if (labels.some((l) => hit(grow(l, 4), rect))) continue;
        const own = gapTo(rect, p.x, p.y) - p.s / 2;
        if (others.some((q) => gapTo(rect, q.x, q.y) - q.s / 2 < own + 10)) continue;
        labels.push(rect);
        const t: Tag = { x: dx, y: dy, w, text, more };
        if (step > NEAR) {
          // a leader from the disc's rim to the name's nearest edge
          const nx = Math.max(dx, Math.min(0, dx + w)), ny = Math.max(dy, Math.min(0, dy + h));
          const len = Math.hypot(nx, ny), ux = nx / len, uy = ny / len;
          const r0 = p.s / 2 + 2;
          t.lead = { x: ux * r0, y: uy * r0, len: Math.max(0, len - r0 - 2), ang: (Math.atan2(uy, ux) * 180) / Math.PI };
        }
        tags.set(p.c.id, t);
        break;
      }
    }
  }

  // ---- counts ----
  const countries = hosted.size - (hosted.has("") ? 1 : 0);
  const providers = new Set(r.registered.map((v) => providerOf(v.hosting)).filter(Boolean)).size;

  return (
    <section className={`band hostmap${showReadiness ? "" : " solo"}`} aria-labelledby="hostmap-h">
      <div className="ov-map">
        <div className="ov-map-head">
          <h2 id="hostmap-h" className="ov-eyebrow">Fibre providers</h2>
          <p className="ov-counts">
            <span><b>{int(hosts.length + unplaced)}</b> Fibre providers</span>
            <span><b>{int(countries)}</b> countries</span>
            <span><b>{int(providers)}</b> hosting providers</span>
          </p>
        </div>
        <div className={`ov-atlas${zoomed ? " zoomed" : ""}`} ref={box}
          style={{ aspectRatio: `1 / ${narrow ? TALL : WIDE}`, maxHeight: narrow ? undefined : Math.ceil(width * Math.max(WIDE, tallest)) }}
          onPointerDown={onPointerDown} onPointerMove={onPointerMove} onPointerUp={onPointerUp} onPointerCancel={onPointerUp}>
          <div className="ov-vp">
            <svg className="ov-land" viewBox={`${view.x} ${view.y} ${view.w} ${view.w * aspect}`} preserveAspectRatio="xMidYMid meet" aria-hidden="true" focusable="false">
              <defs>{dots}</defs>
              {land}
            </svg>
          </div>
          <ul className="ov-pins" aria-label="Fibre providers by location" aria-describedby="ov-pins-keys">
            {placed.map(({ c, x, y, d, s }) => {
              const isOpen = open === c.id;
              const fault = c.hosts.some((h) => (h.v.obligations?.broken ?? 0) > 0);
              const place = placeLabel(c);
              const counts = ORDER.map((st) => [st, c.hosts.filter((h) => h.state === st).length] as const).filter(([, n]) => n > 0);
              const split = canSplit(c);
              const tag = tags.get(c.id);
              const multi = c.ccs.length > 1;
              const n = c.hosts.length;
              const one = n === 1;
              // a merged disc's places, most hosts first, for the popover and the screen reader alike
              const cities = c.locs > 1 ? cityCounts(c) : [];
              const glow = glowOf(n);
              // The list opens beside the disc on a wide map and under the map on a narrow one.
              const pop: React.CSSProperties = narrow
                ? { left: -x, top: height - y + 10, width }
                : { [x > width * 0.6 ? "right" : "left"]: s / 2 + 10, [y > height * 0.5 ? "bottom" : "top"]: -Math.max(s, 24) / 2, width: 300 };
              return (
                <li key={c.id} className={`ov-pin${one ? " one" : ""}${isOpen ? " open" : ""}${liveCluster === c.id ? " live" : ""}`}
                  style={{ left: x, top: y, zIndex: isOpen ? 30 : undefined }}
                  onMouseEnter={() => openNow(c.id)} onMouseLeave={closeSoon}>
                  {glow > 0 && <span className="ov-glow" aria-hidden="true" style={{ width: s + 76 * glow, height: s + 76 * glow, opacity: 0.35 + 0.65 * glow }} />}
                  <button type="button" className="ov-b" aria-expanded={split ? undefined : isOpen}
                    aria-label={`${place}${cities.length ? ` (${cities.map(([k, m]) => (m > 1 ? `${k} ${m}` : k)).join(", ")})` : ""}: ${n} Fibre provider${one ? "" : "s"}, ${counts.map(([st, k]) => `${k} ${STATE_WORD[st]}`).join(", ")}${split ? ". Zoom in" : ""}`}
                    style={{ width: d, height: d, "--s": `${s}px`, "--ring": ring(c.hosts) } as React.CSSProperties}
                    tabIndex={c.id === (placed.some((q) => q.c.id === roving) ? roving : placed[0]?.c.id) ? 0 : -1}
                    onFocus={() => { setRoving(c.id); openNow(c.id); }} onKeyDown={walk} onClick={() => activate(c)}>
                    <span className="ov-disc"><span>{n}</span></span>
                    {fault && <i className="ov-fault" aria-hidden="true" />}
                  </button>
                  {tag?.lead && <i className="ov-lead" aria-hidden="true" style={{ left: tag.lead.x, top: tag.lead.y, width: tag.lead.len, transform: `rotate(${tag.lead.ang}deg)` }} />}
                  {tag && (
                    <span className="ov-tag" aria-hidden="true" style={{ left: tag.x, top: tag.y, width: tag.w }}>{tag.text}{tag.more > 0 && <i> +{tag.more}</i>}</span>
                  )}
                  {isOpen && (
                    <div className="ov-pop" style={pop} role="group" aria-label={place}>
                      <p className="ov-pop-h">
                        <b>{place}</b>
                        <span>{n} provider{one ? "" : "s"}</span>
                      </p>
                      {cities.length > 0 && <p className="ov-pop-cities">{cities.map(([k, m]) => <span key={k}>{k}{m > 1 && <i> {m}</i>}</span>)}</p>}
                      {/* whole rows, the list fading at an edge that has more (scrollFade) */}
                      <ul ref={scrollFade} onScroll={(e) => scrollFade(e.currentTarget)}>
                        {c.hosts.map((h) => (
                          <li key={h.v.address}>
                            <i className="ov-dot" style={{ background: STATE_VAR[h.state] }} title={STATE_WORD[h.state]} />
                            <Link href={valLink(h.v)} className="ov-name">{name(h.v)}</Link>
                            <span className="ov-share">{fmtShare(h.share)}</span>
                            <span className="ov-meta">
                              {[multi ? h.cc : "", h.provider, c.locs > 1 && h.city ? h.city : multi && !h.city ? countryName(h.cc) : "", h.state !== "reachable" ? STATE_WORD[h.state] : ""].filter(Boolean).join(" · ")}
                              {(h.v.obligations?.broken ?? 0) > 0 && <span className="ov-broken"> · {h.v.obligations.broken} not served</span>}
                            </span>
                          </li>
                        ))}
                      </ul>
                      {split && <button type="button" className="ov-zoomin" onClick={() => activate(c)}>Zoom in</button>}
                    </div>
                  )}
                </li>
              );
            })}
          </ul>
          <span id="ov-pins-keys" className="sr-only">Arrow keys move between places.</span>
          {/* the corner of the map: the quiet key of Tensile's check, and the zoom */}
          <div className="ov-map-tools" ref={tools}>
            <p className="ov-key" aria-hidden="true" title="Observed by Tensile">
              <svg viewBox="0 0 16 16" width="12" height="12"><path d="M1.5 8s2.4-4.5 6.5-4.5S14.5 8 14.5 8 12.1 12.5 8 12.5 1.5 8 1.5 8Z" fill="none" stroke="currentColor" strokeWidth="1.4" strokeLinejoin="round" /><circle cx="8" cy="8" r="2" fill="currentColor" /></svg>
              <span><i style={{ background: STATE_VAR.reachable }} />reachable</span>
              <span><i style={{ background: STATE_VAR.unreachable }} />unreachable</span>
              {unchecked && <span><i style={{ background: STATE_VAR.none }} />not checked yet</span>}
            </p>
            <div className="ov-zoom" role="group" aria-label="Map view">
              <button type="button" aria-label="Zoom in" disabled={target.w <= FRAME.w / MAX_ZOOM + 1} onClick={() => zoomBy(2)}>
                <svg viewBox="0 0 12 12" width="12" height="12" aria-hidden="true"><path d="M6 2v8M2 6h8" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" /></svg>
              </button>
              <button type="button" aria-label="Zoom out" disabled={!zoomed} onClick={() => zoomBy(0.5)}>
                <svg viewBox="0 0 12 12" width="12" height="12" aria-hidden="true"><path d="M2 6h8" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" /></svg>
              </button>
            </div>
          </div>
        </div>
        {/* under the map, on a line of its own: the newest event */}
        <div className="ov-map-foot">
          {cur && (
            <p className="ov-live" key={`${cur.v.address}|${cur.at}|${cur.event ?? ""}`}>
              <i className="ov-dot" style={{ background: cur.event === "last reachable" ? "var(--hold)" : "var(--accent)" }} />
              <span className="ov-who"><Link href={valLink(cur.v)}>{name(cur.v)}</Link>{cur.event && <> {cur.event}</>}</span>
              {cur.host?.cc && <span>{countryName(cur.host.cc)}</span>}
              {!cur.event && cur.host?.provider && <span>{cur.host.provider}</span>}
              <span className="ov-ago" title={utcWord(cur.at)}>{ago(cur.at)}</span>
            </p>
          )}
        </div>
      </div>
      {showReadiness && (
        <div className="fm-side">
          <StakeGauge rows={rows} />
          {aside && <div className="fm-aside">{aside}</div>}
        </div>
      )}
    </section>
  );
}
