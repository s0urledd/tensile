"use client";
import { useEffect, useLayoutEffect, useMemo, useRef, useState } from "react";
import Link from "next/link";
import { type Validator, type Probe, API_BASE, useApi, ago, utcWord } from "@/lib/api";
import type { Hosting } from "@/lib/hosting";
import { FRAME, COUNTRIES, project, countryPoint } from "@/lib/map/project";
import { verdictDef } from "@/components/Verdict";
import { countryName } from "@/components/Flag";
import { type EndpointState, endpointState, readiness, ReadyAnswer } from "@/components/Readiness";

/**
 * The overview's host map: every registered Fibre host of the bonded set,
 * placed by the city its address geolocates to (hosting.lat/lon) or, without
 * one, by its country's label point, over Natural Earth country outlines.
 * Countries with a host are tinted. Hosts close together on screen share one
 * badge whose ring is split by endpoint state; a badge that holds several
 * places zooms in on them, so a dense region comes apart into countries and
 * cities. Beside the badges, a flag and a place name wherever they fit.
 */

type Host = { v: Validator; state: EndpointState; share: number; cc: string; city: string; loc: string; lon: number; lat: number; ux: number; uy: number; provider: string };
type Cluster = { id: string; hosts: Host[]; ux: number; uy: number; locs: number; ccs: string[] };
/** the visible part of the map, in map units: top-left corner and width (height follows the frame) */
type View = { x: number; y: number; w: number };

const STATE_WORD: Record<EndpointState, string> = { reachable: "reachable", unreachable: "unreachable", none: "no host" };
// The table's colours: amber for a host that stopped answering.
const STATE_VAR: Record<EndpointState, string> = { reachable: "var(--accent)", unreachable: "var(--hold)", none: "var(--pending)" };
const ORDER: EndpointState[] = ["reachable", "unreachable"];

/** the box's height over its width: the whole frame on a wide screen, a taller crop of it on a phone */
const WIDE = FRAME.h / FRAME.w, TALL = 0.62;
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
/** the whole world, or on a phone the widest crop of it that keeps the hosts in the middle */
function homeView(pts: [number, number][], a: number): View {
  const w = Math.min(FRAME.w, FRAME.h / a);
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
      // Neighbours may overlap a little before they merge, so a dense region keeps some shape.
      const need = ((badge(cs[i].hosts.length, narrow) + badge(cs[j].hosts.length, narrow)) / 2) * 0.9;
      if (d < need && (!best || d - need < best[2])) best = [i, j, d - need];
    }
    if (!best) break;
    const a = cs[best[0]], b = cs[best[1]], na = a.hosts.length, nb = b.hosts.length;
    const merged: C = { hosts: [...a.hosts, ...b.hosts], ux: (a.ux * na + b.ux * nb) / (na + nb), uy: (a.uy * na + b.uy * nb) / (na + nb) };
    cs = cs.filter((_, i) => i !== best![0] && i !== best![1]).concat(merged);
  }
  return cs.map((c) => {
    const hosts = [...c.hosts].sort((x, y) => (y.v.voting_power || 0) - (x.v.voting_power || 0));
    // Countries by host count, so the first flag is the biggest share of the badge.
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

/** what a badge is called: its city, its country, or its countries */
function placeLabel(c: Cluster): string {
  if (c.locs === 1 && c.hosts[0].city) return `${c.hosts[0].city}, ${countryName(c.hosts[0].cc)}`;
  if (c.ccs.length === 1) return countryName(c.ccs[0]);
  if (c.ccs.length === 2) return c.ccs.map(countryName).join(", ");
  return `${c.ccs.length} countries`;
}
/** the short tag beside a badge: the name of the one place it holds, or nothing */
function tagText(c: Cluster): string {
  // Only a single place is named; a merged badge shows its count, and its places are in the popover.
  if (c.locs !== 1) return "";
  return c.hosts[0].city || (c.ccs.length === 1 ? countryName(c.ccs[0]) : "");
}

let measureCtx: CanvasRenderingContext2D | null | undefined;
function textWidth(s: string): number {
  if (measureCtx === undefined) {
    try { measureCtx = document.createElement("canvas").getContext("2d"); if (measureCtx) measureCtx.font = "500 12px 'IBM Plex Sans', system-ui, sans-serif"; } catch { measureCtx = null; }
  }
  return measureCtx ? measureCtx.measureText(s).width : s.length * 6.6;
}

function reducedMotion(): boolean {
  try { return window.matchMedia("(prefers-reduced-motion: reduce)").matches; } catch { return false; }
}

// ---- the network feed: host registrations, for the pill before any blob settles ----
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

  // ---- size: the box keeps the frame's aspect ratio, so measuring it never shifts the layout ----
  const box = useRef<HTMLDivElement>(null);
  const [width, setWidth] = useState(760);
  useLayoutEffect(() => {
    const el = box.current;
    if (!el) return;
    setWidth(el.clientWidth || 760);
    const ro = new ResizeObserver(() => setWidth(el.clientWidth || 760));
    ro.observe(el);
    return () => ro.disconnect();
  }, []);
  const narrow = width < 560;
  const aspect = narrow ? TALL : WIDE;
  const height = width * aspect;
  const home = useMemo(() => homeView(hosts.map((h) => [h.ux, h.uy]), aspect), [hosts, aspect]);

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

  // Badges merge by the zoom the view is heading to, so they do not re-merge mid-flight.
  const clusters = useMemo(() => cluster(hosts, width / target.w, narrow), [hosts, width, target.w, narrow]);

  // ---- drag to pan, once zoomed in ----
  const drag = useRef<{ id: number; x: number; y: number; v: View; moved: boolean } | null>(null);
  const onPointerDown = (e: React.PointerEvent) => {
    if (!zoomed || e.button !== 0 || (e.target as Element).closest(".fm-c, .fm-ctl")) return;
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

  // ---- open cluster (hover, focus, tap) ----
  const [open, setOpen] = useState<string | null>(null);
  const closeT = useRef<number | undefined>(undefined);
  const openNow = (id: string) => { window.clearTimeout(closeT.current); setOpen(id); };
  const closeSoon = () => { window.clearTimeout(closeT.current); closeT.current = window.setTimeout(() => setOpen(null), 160); };
  useEffect(() => {
    if (!open) return;
    const onKey = (e: KeyboardEvent) => { if (e.key === "Escape") setOpen(null); };
    const onDown = (e: PointerEvent) => { if (!(e.target as Element)?.closest?.(".fm-c")) setOpen(null); };
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

  // ---- live pill: who served most recently, or else the newest host events ----
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

  // ---- the land: every country once; hosted ones tinted, the open badge's stronger ----
  const hosted = useMemo(() => new Set(hosts.map((h) => h.cc)), [hosts]);
  const openCcs = useMemo(() => new Set(clusters.find((c) => c.id === open)?.ccs ?? []), [clusters, open]);
  const land = useMemo(() => COUNTRIES.map(([cc, d], i) => (
    <path key={cc || i} d={d} className={openCcs.has(cc) ? "hi" : hosted.has(cc) ? "on" : undefined} />
  )), [hosted, openCcs]);

  if (total === 0) return null;
  if (hosts.length === 0 && r.registered.length === 0) {
    // Nothing to place yet: the readiness answer alone, as before.
    return showReadiness ? (
      <section className="band readiness" aria-labelledby="readiness-h">
        <div><ReadyAnswer rows={rows} />{aside && <div className="fm-aside">{aside}</div>}</div>
      </section>
    ) : null;
  }

  // ---- badges on screen, and their tags where they fit ----
  const placed = clusters.map((c) => {
    const [x, y] = toPx(c.ux, c.uy);
    return { c, x, y, d: badge(c.hosts.length, narrow) };
  }).filter((p) => p.x >= p.d / 2 - 2 && p.x <= width - p.d / 2 + 2 && p.y >= p.d / 2 - 2 && p.y <= height - p.d / 2 + 2);
  type Rect = [number, number, number, number];
  const hit = (a: Rect, b: Rect) => a[0] < b[0] + b[2] && b[0] < a[0] + a[2] && a[1] < b[1] + b[3] && b[1] < a[1] + a[3];
  const taken: Rect[] = placed.map((p) => [p.x - p.d / 2 - 2, p.y - p.d / 2 - 2, p.d + 4, p.d + 4]);
  taken.push([0, height - 42, 80, 42]); // the zoom controls, bottom left
  // Each tag tries the right of its badge, then the left, then a little higher or lower, then above and below.
  const tags = new Map<string, { x: number; y: number; w: number; text: string }>();
  if (!moving) {
    for (const p of [...placed].sort((a, b) => b.c.hosts.length - a.c.hosts.length)) {
      const text = tagText(p.c);
      if (!text) continue;
      const w = Math.ceil(10 + textWidth(text)), h = 20;
      const r = p.d / 2 + 3, right = r, left = -r - w, mid = -w / 2;
      for (const [dx, dy] of [[right, 0], [left, 0], [right, -11], [right, 11], [left, -11], [left, 11], [mid, -r - h / 2], [mid, r + h / 2]]) {
        const rect: Rect = [p.x + dx, p.y + dy - h / 2, w, h];
        if (rect[0] < 0 || rect[0] + w > width || rect[1] < 0 || rect[1] + h > height) continue;
        if (taken.some((t) => hit(t, rect))) continue;
        taken.push(rect);
        tags.set(p.c.id, { x: dx, y: dy - h / 2, w, text });
        break;
      }
    }
  }

  // ---- counts ----
  const countries = hosted.size - (hosted.has("") ? 1 : 0);
  const providers = new Set(r.registered.map((v) => providerOf(v.hosting)).filter(Boolean)).size;

  return (
    <section className={`band hostmap${showReadiness ? "" : " solo"}`} aria-labelledby="hostmap-h">
      <div className="fm-main">
        <div className="fm-head">
          <h2 id="hostmap-h">Fibre providers</h2>
          <p className="fm-pill fm-stats">
            <span><b>{hosts.length + unplaced}</b> Fibre providers</span>
            <span><b>{countries}</b> countries</span>
            <span><b>{providers}</b> hosting providers</span>
          </p>
        </div>
        <div className={`fm-box${zoomed ? " zoomed" : ""}`} ref={box}
          style={{ aspectRatio: `1 / ${aspect}` }}
          onPointerDown={onPointerDown} onPointerMove={onPointerMove} onPointerUp={onPointerUp} onPointerCancel={onPointerUp}>
          <div className="fm-vp">
            <svg className="fm-land" viewBox={`${view.x} ${view.y} ${view.w} ${view.w * aspect}`} preserveAspectRatio="xMidYMid meet" aria-hidden="true" focusable="false">
              {land}
            </svg>
          </div>
          <ul className="fm-clusters" aria-label="Fibre providers by location">
            {placed.map(({ c, x, y, d }) => {
              const isOpen = open === c.id;
              const fault = c.hosts.some((h) => (h.v.obligations?.broken ?? 0) > 0);
              const place = placeLabel(c);
              const counts = ORDER.map((s) => [s, c.hosts.filter((h) => h.state === s).length] as const).filter(([, n]) => n > 0);
              const split = canSplit(c);
              const tag = tags.get(c.id);
              const multi = c.ccs.length > 1;
              // The list opens beside the badge on a wide map and under the map on a narrow one.
              const pop: React.CSSProperties = narrow
                ? { left: -x, top: height - y + 8, width }
                : { [x > width * 0.6 ? "right" : "left"]: d / 2 + 8, [y > height * 0.5 ? "bottom" : "top"]: -d / 2, width: 300 };
              return (
                <li key={c.id} className={`fm-c${isOpen ? " open" : ""}${liveCluster === c.id ? " live" : ""}`}
                  style={{ left: x, top: y, zIndex: isOpen ? 30 : undefined }}
                  onMouseEnter={() => openNow(c.id)} onMouseLeave={closeSoon}>
                  <button type="button" className="fm-b" aria-expanded={split ? undefined : isOpen}
                    aria-label={`${place}: ${c.hosts.length} Fibre provider${c.hosts.length === 1 ? "" : "s"}, ${counts.map(([s, n]) => `${n} ${STATE_WORD[s]}`).join(", ")}${split ? ". Zoom in" : ""}`}
                    style={{ width: d, height: d, background: ring(c.hosts) }}
                    onFocus={() => openNow(c.id)} onClick={() => activate(c)}>
                    <span>{c.hosts.length}</span>
                    {fault && <i className="fm-fault" aria-hidden="true" />}
                  </button>
                  {tag && (
                    <span className="fm-tag" aria-hidden="true" style={{ left: tag.x, top: tag.y, width: tag.w }}>
                      <span>{tag.text}</span>
                    </span>
                  )}
                  {isOpen && (
                    <div className="fm-pop" style={pop} role="group" aria-label={place}>
                      <p className="fm-pop-h">
                        <b>{place}</b>
                        <span>{c.hosts.length} provider{c.hosts.length === 1 ? "" : "s"}</span>
                      </p>
                      <ul>
                        {c.hosts.map((h) => (
                          <li key={h.v.address}>
                            <i className="fm-dot" style={{ background: STATE_VAR[h.state] }} title={STATE_WORD[h.state]} />
                            <Link href={valLink(h.v)} className="fm-name">{name(h.v)}</Link>
                            <span className="fm-share">{fmtShare(h.share)}</span>
                            <span className="fm-meta">
                              {[multi ? h.cc : "", h.provider, c.locs > 1 && h.city ? h.city : multi && !h.city ? countryName(h.cc) : "", h.state !== "reachable" ? STATE_WORD[h.state] : ""].filter(Boolean).join(" · ")}
                              {(h.v.obligations?.broken ?? 0) > 0 && <span className="fm-broken"> · {h.v.obligations.broken} broken</span>}
                            </span>
                          </li>
                        ))}
                      </ul>
                      {split && <button type="button" className="fm-zoomin" onClick={() => activate(c)}>Zoom in</button>}
                    </div>
                  )}
                </li>
              );
            })}
          </ul>
          <div className="fm-ctl" role="group" aria-label="Map view">
            <span className="fm-seg">
              <button type="button" aria-label="Zoom in" disabled={target.w <= FRAME.w / MAX_ZOOM + 1} onClick={() => zoomBy(2)}>+</button>
              <button type="button" aria-label="Zoom out" disabled={!zoomed} onClick={() => zoomBy(0.5)}>−</button>
            </span>
          </div>
        </div>
        <div className="fm-pills">
          {cur && (
            <p className="fm-pill fm-live" key={`${cur.v.address}|${cur.at}|${cur.event ?? ""}`}>
              <i className="fm-dot" style={{ background: cur.event === "last reachable" ? "var(--hold)" : "var(--accent)" }} />
              <span className="fm-who"><Link href={valLink(cur.v)}>{name(cur.v)}</Link>{cur.event && <> {cur.event}</>}</span>
              {cur.host?.cc && <span>{countryName(cur.host.cc)}</span>}
              {!cur.event && cur.host?.provider && <span>{cur.host.provider}</span>}
              <span className="fm-ago" title={utcWord(cur.at)}>{ago(cur.at)}</span>
            </p>
          )}
          <p className="fm-key" aria-hidden="true">
            <span><i style={{ background: STATE_VAR.reachable }} />reachable</span>
            <span><i style={{ background: STATE_VAR.unreachable }} />unreachable</span>
          </p>
        </div>
      </div>
      {showReadiness && (
        <div className="fm-side">
          <ReadyAnswer rows={rows} />
          {aside && <div className="fm-aside">{aside}</div>}
        </div>
      )}
    </section>
  );
}
