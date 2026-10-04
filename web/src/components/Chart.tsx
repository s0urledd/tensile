"use client";
import { useEffect, useId, useLayoutEffect, useRef, useState, type CSSProperties, type ReactNode } from "react";

/**
 * A column chart with axes, gridlines, a hover readout and, for more than
 * one series, a stack with a legend. Drawn in pixels from the container's
 * measured width, so text never stretches; nothing here scales a viewBox.
 *
 * Rows are the x categories in order (days); every row is drawn, including
 * empty ones, so a quiet week is a quiet week and not a shorter chart.
 *
 * Bars grow from a zero baseline, square at the baseline and rounded only at
 * the data end. Each bucket is its own bar: nothing is drawn between
 * buckets, so no value is suggested where there is none. A single series
 * takes a vertical gradient of its own hue; a stack keeps flat fills so the
 * identity of each segment stays exact, and is laid out in whole pixels:
 * exactly as tall as its total's bar, each gap cut from the segment above
 * it, and no segment thinner than 1px.
 *
 * Every bucket is named on the axis when the names fit; when they do not,
 * a row's `short` form ("22" for "Sep 22") is tried before any is dropped,
 * the first name and the first of each month kept in full.
 *
 * The readout never covers a bar other than the hovered one, nor an axis
 * label: it goes beside the column on whichever side has room, rising over
 * the chart's own head when the bars under it are tall.
 *
 * The plot is focusable: the arrow keys (and Home / End) walk the buckets
 * and the readout follows, the same one the pointer gets.
 *
 * `bare` draws only the bars, their baseline and the axis names, for a chart
 * set beside a figure of its own (the Blobs top): no head, ticks or
 * gridlines, the tallest bar filling the plot exactly with its count over
 * it, flat fills, and any bucket with something in it at least 3px tall. A
 * row's `partial` (an hour or a day not over yet) takes a lighter tint of
 * the series, so half a day is not read as a drop. The tallest bar's bucket
 * is always named on the axis.
 */
export type Series = { key: string; label: string; color: string };
/** `short`: the label without its month, "22" for "Sep 22" */
export type Row = { x: string; label?: string; short?: string; values: Record<string, number>; note?: string; partial?: boolean };

const PAD = { top: 12, right: 4, bottom: 26, left: 0 };
/** the axis text size, --t-tiny */
const AXIS_PX = 11;
/** clear space between two axis labels; short day numbers are lighter and need less */
const LABEL_GAP = 10, SHORT_GAP = 8;
/** the card showing between two stacked segments, cut from the upper one */
const SEG_GAP = 2;
/** clear space kept between the readout and what it must not cover */
const CLEAR = 6;
/** the gridlines of an empty chart, as fractions of the plot's height */
const EMPTY_GRID = [0.25, 0.5, 0.75, 1];

function niceStep(max: number, ticks: number): number {
  if (max <= 0) return 1;
  const raw = max / ticks;
  const mag = Math.pow(10, Math.floor(Math.log10(raw)));
  const norm = raw / mag;
  const step = norm <= 1 ? 1 : norm <= 2 ? 2 : norm <= 2.5 ? 2.5 : norm <= 5 ? 5 : 10;
  return step * mag;
}

/** a column rounded at the top only: the data end, never the baseline */
function column(x: number, y: number, w: number, h: number, r: number): string {
  const rr = Math.max(0, Math.min(r, w / 2, h));
  return `M${x},${y + h}V${y + rr}A${rr},${rr} 0 0 1 ${x + rr},${y}H${x + w - rr}A${rr},${rr} 0 0 1 ${x + w},${y + rr}V${y + h}Z`;
}

// Axis text widths, measured in the page's own face (and measured again once
// the webfont is in), so the label plan is exact rather than guessed.
const widths = new Map<string, number>();
let ctx2d: CanvasRenderingContext2D | null | undefined;
function textWidth(s: string, font: string): number {
  const k = font + "\u0000" + s;
  const hit = widths.get(k);
  if (hit !== undefined) return hit;
  if (ctx2d === undefined) { try { ctx2d = document.createElement("canvas").getContext("2d"); } catch { ctx2d = null; } }
  let w = s.length * 6.4;
  if (ctx2d) { ctx2d.font = font; w = ctx2d.measureText(s).width; }
  widths.set(k, w);
  return w;
}

/**
 * A stack's segments in whole pixels, bottom first: `ext` is the height a
 * segment takes including the gap under it, `gap` that gap. The extents add
 * up to `px`, the total's own bar, so the stack never stands taller than its
 * total; each segment above the first gives its gap out of its own height;
 * a segment too small to see keeps 1px, taken from the larger ones. Only a
 * bar with fewer pixels than segments leaves its smallest undrawn (the
 * readout still lists them).
 */
function stackPx(vals: number[], px: number): { ext: number; gap: number }[] {
  const out = vals.map(() => ({ ext: 0, gap: 0 }));
  let keep = vals.map((v, k) => (v > 0 ? k : -1)).filter((k) => k >= 0);
  if (keep.length === 0 || px <= 0) return out;
  let gap = SEG_GAP;
  if (px < 1 + (keep.length - 1) * (1 + gap)) gap = 0;
  if (px < keep.length) keep = [...keep].sort((a, b) => vals[b] - vals[a]).slice(0, px).sort((a, b) => a - b);
  const min = (k: number) => (k === keep[0] ? 1 : 1 + gap);
  // shares in proportion to value; any below its minimum is pinned there and the rest shared again
  const pinned = new Set<number>();
  let share = new Map<number, number>();
  for (;;) {
    const free = keep.filter((k) => !pinned.has(k));
    let room = px;
    pinned.forEach((k) => { room -= min(k); });
    const sum = free.reduce((a, k) => a + vals[k], 0);
    share = new Map(free.map((k) => [k, sum > 0 ? (vals[k] / sum) * room : 0]));
    const low = free.filter((k) => (share.get(k) ?? 0) < min(k));
    if (low.length === 0) break;
    low.forEach((k) => pinned.add(k));
  }
  // whole pixels, the remainder to the largest fractions
  const ext = new Map<number, number>();
  pinned.forEach((k) => ext.set(k, min(k)));
  let left = px;
  ext.forEach((e) => { left -= e; });
  const free = [...share.keys()];
  free.forEach((k) => { const e = Math.floor(share.get(k) ?? 0); ext.set(k, e); left -= e; });
  free.sort((a, b) => ((share.get(b) ?? 0) % 1) - ((share.get(a) ?? 0) % 1));
  for (let j = 0; left > 0 && free.length > 0; j = (j + 1) % free.length, left--) ext.set(free[j], (ext.get(free[j]) ?? 0) + 1);
  keep.forEach((k) => { out[k] = { ext: ext.get(k) ?? 0, gap: k === keep[0] ? 0 : gap }; });
  return out;
}

export default function Chart({ series, rows, fmt, height = 200, fmtAxis, title, head, sub, figure, figureTitle, empty, bare = false, padTop }: {
  series: Series[];
  rows: Row[];
  fmt: (v: number) => string;
  fmtAxis?: (v: number) => string;
  height?: number;
  title: string;
  /**
   * a head of the caller's own in place of the title and figure (the Blobs
   * deck's eyebrow and live rate); the title still names the plot to
   * assistive tech, and the readout may rise over this head as over its own
   */
  head?: ReactNode;
  /** a line beside the title in place of a figure (use figure to set the figure large) */
  sub?: string;
  /** the period's total, printed large under the title: "1.35 GiB" (the period itself is in the title: "Blob size per day (7d)") */
  figure?: string;
  /** the figure's hover text, when the total needs one sentence of explanation */
  figureTitle?: string;
  empty?: string;
  /** the bars, their baseline and the axis names only (see above) */
  bare?: boolean;
  /** room over the plot, for the tallest bar's count when bare */
  padTop?: number;
}) {
  const ref = useRef<HTMLDivElement>(null);
  const plotRef = useRef<HTMLDivElement>(null);
  const tipRef = useRef<HTMLDivElement>(null);
  const [width, setWidth] = useState(0);
  const [family, setFamily] = useState("sans-serif");
  const [, setFontsIn] = useState(0);
  const [hover, setHover] = useState<number | null>(null);
  const [keyed, setKeyed] = useState(false);
  // the readout's measured size, and the height of the chart's head above the plot
  const [tipBox, setTipBox] = useState({ w: 150, h: 84 });
  const [headH, setHeadH] = useState(64);
  const uid = "c" + useId().replace(/[^a-zA-Z0-9]/g, "");
  useEffect(() => {
    const el = ref.current;
    if (!el) return;
    const ro = new ResizeObserver((es) => setWidth(Math.floor(es[0].contentRect.width)));
    ro.observe(el);
    setWidth(el.clientWidth);
    setFamily(getComputedStyle(el).fontFamily || "sans-serif");
    let live = true;
    document.fonts?.ready.then(() => { if (live) { widths.clear(); setFontsIn((v) => v + 1); } });
    return () => { live = false; ro.disconnect(); };
  }, []);
  useLayoutEffect(() => {
    const p = plotRef.current, t = tipRef.current;
    if (p && p.offsetTop !== headH) setHeadH(p.offsetTop);
    if (t && (t.offsetWidth !== tipBox.w || t.offsetHeight !== tipBox.h)) {
      // a new size is a new place, found before paint: go there, do not glide from where the old size put it
      t.style.transition = "none";
      setTipBox({ w: t.offsetWidth, h: t.offsetHeight });
      requestAnimationFrame(() => requestAnimationFrame(() => { t.style.transition = ""; }));
    }
  });
  const tw = (s: string, weight = 400) => textWidth(s, `${weight} ${AXIS_PX}px ${family}`);

  const axisFmt = fmtAxis ?? fmt;
  const totals = rows.map((r) => series.reduce((a, s) => a + (r.values[s.key] ?? 0), 0));
  const max = Math.max(0, ...totals);
  const hasData = max > 0;
  const step = niceStep(max, 4);
  // bare: the tallest bar fills the plot, its count printed over it in place of a scale
  const top = !hasData ? 1 : bare ? max : Math.ceil(max / step) * step;
  // an empty chart keeps its scale's floor: the 0 tick, and gridlines with nothing to count
  const ticks: number[] = [];
  if (hasData) for (let v = 0; v <= top + 1e-9; v += step) ticks.push(v); else ticks.push(0);
  // the left gutter from the widest tick label, and at least 56px, so charts side by side start their plots on one line
  const left = bare ? 0 : Math.max(56, Math.ceil(Math.max(...ticks.map((t) => tw(axisFmt(t))))) + 10);
  const padT = bare ? padTop ?? PAD.top : PAD.top;
  const plotW = Math.max(0, width - left - PAD.right);
  const plotH = height - padT - PAD.bottom;
  const base = padT + plotH;
  const n = rows.length;
  const slot = n > 0 ? plotW / n : 0;
  const bw = Math.max(2, Math.round(bare ? Math.min(24, slot * 0.56) : Math.min(26, slot * 0.58)));
  const y = (v: number) => padT + plotH * (1 - v / top);
  // bare: a bucket with anything in it stands at least 3px, so it never reads as an empty one
  const barPx = (v: number) => (v > 0 ? Math.max(bare ? 3 : 1, Math.round((plotH * v) / top)) : 0);
  const cxOf = (i: number) => left + slot * i + slot / 2;
  const colX = (i: number) => Math.round(cxOf(i) - bw / 2);
  const single = series.length === 1;
  const lastWithData = (() => { for (let i = n - 1; i >= 0; i--) if (totals[i] > 0) return i; return n - 1; })();
  // the hovered bucket, if it is one of these rows (another period may have fewer)
  const on = hasData && hover !== null && hover < n ? hover : null;

  // The axis names: every bucket in full if they fit; else every bucket in
  // its short form (the first, and the first of a month, in full); else every
  // k-th bucket in full, the smallest k that fits.
  // Bare: every k-th bucket in full, at least 80px apart centre to centre,
  // counted from the tallest bar (the earliest on a tie), so it is always
  // named and the axis keeps one even step through it.
  const full = rows.map((r) => r.label ?? r.x);
  const canShort = n > 0 && rows.every((r, i) => !!r.short && full[i].endsWith(r.short));
  const month = (i: number) => full[i].slice(0, full[i].length - (rows[i].short ?? "").length);
  const peak = bare && hasData ? totals.indexOf(max) : -1;
  const plan = (k: number, short: boolean, from = 0): Map<number, string> | null => {
    const out = new Map<number, string>();
    let prev = -1;
    for (let i = from; i < n; i += k) {
      const s = !short || prev < 0 || month(i) !== month(prev) ? full[i] : (rows[i].short as string);
      if (prev >= 0 && slot * (i - prev) < (tw(out.get(prev) as string) + tw(s)) / 2 + (short ? SHORT_GAP : LABEL_GAP)) return null;
      out.set(i, s);
      prev = i;
    }
    return out;
  };
  let names: Map<number, string> | null = null;
  if (bare) {
    for (let k = Math.max(1, Math.ceil(80 / slot)); !names && k <= n; k++) names = plan(k, false, peak >= 0 ? peak % k : 0);
  } else {
    names = plan(1, false) ?? (canShort ? plan(1, true) : null);
    for (let k = 2; !names && k <= n; k++) names = plan(k, false);
  }
  names = names ?? new Map<number, string>();
  // the hovered bucket is named in full; the names that would touch it make room
  const onW = on !== null ? tw(full[on], 600) : 0;
  const showName = (i: number) => i === on || (names.has(i) && (on === null || Math.abs(i - on) * slot >= (tw(names.get(i) as string) + onW) / 2 + 6));

  // The readout's place: beside the hovered column, on either side, as low as
  // the bars under it allow (up over the chart's head when they are tall),
  // never over another bar or an axis label; if nowhere is clear, wherever it
  // covers least, the hovered bar before any other.
  let tip = { x: 0, y: 0 };
  if (on !== null && width > 0) {
    const w = tipBox.w, h = tipBox.h;
    const yTop = padT - 8;
    const yLow = -headH;
    type Box = { x0: number; x1: number; top: number; weight: number };
    const boxes: Box[] = [{ x0: 0, x1: left - CLEAR, top: y(top) - 8, weight: 50 }];
    // the tallest bar bare carries its count over it: the readout keeps off that too
    for (let i = 0; i < n; i++) if (totals[i] > 0) boxes.push({ x0: colX(i), x1: colX(i) + bw, top: i === peak ? 0 : base - barPx(totals[i]), weight: i === on ? 1 : 5 });
    const hx0 = colX(on), hx1 = hx0 + bw;
    const xs = new Set<number>();
    const clampX = (x: number) => Math.max(0, Math.min(width - w, Math.round(x)));
    for (let x = 0; x <= width - w; x += 4) xs.add(x);
    [width - w, hx1 + 10, hx0 - 10 - w, (hx0 + hx1 - w) / 2].forEach((x) => xs.add(clampX(x)));
    let best = Infinity;
    xs.forEach((x) => {
      const hit = boxes.filter((b) => b.x0 < x + w + CLEAR && b.x1 > x - CLEAR);
      // above the zero stubs (3px over the baseline), whatever the bars
      let ty = Math.min(yTop, base - 3 - CLEAR - h);
      hit.forEach((b) => { ty = Math.min(ty, b.top - CLEAR - h); });
      let cover = 0;
      if (ty < yLow) {
        ty = yLow;
        hit.forEach((b) => {
          const dx = Math.min(x + w + CLEAR, b.x1) - Math.max(x - CLEAR, b.x0);
          const dy = Math.min(ty + h + CLEAR, base) - Math.max(ty - CLEAR, b.top);
          if (dx > 0 && dy > 0) cover += dx * dy * b.weight;
        });
      }
      const away = x >= hx1 ? x - hx1 : x + w <= hx0 ? hx0 - (x + w) : 0;
      const room = x >= hx1 ? width - hx1 : hx0;
      const score = cover * 100 + (yTop - ty) * 0.5 + away - room * 0.001;
      if (score < best) { best = score; tip = { x, y: ty }; }
    });
  }

  const move = (to: number) => { setKeyed(true); setHover(Math.min(n - 1, Math.max(0, to))); };

  return (
    <div className={"chart" + (single ? " chart--single" : "") + (bare ? " chart--bare" : "")} ref={ref} style={single ? ({ "--series": series[0].color } as CSSProperties) : undefined}>
      {bare ? null : head ?? (
        <div className="chart-head">
          <div className="chart-title">{single && <i className="chart-key" aria-hidden="true" />}{title}</div>
          <div className="chart-figure">
            {figure !== undefined
              ? <b className="num" title={figureTitle}>{figure}</b>
              : sub !== undefined ? <span>{sub}</span> : <b aria-hidden="true">&nbsp;</b>}
          </div>
        </div>
      )}
      <div className="chart-plot" ref={plotRef} style={{ height }}>
      {width > 0 && (
        <>
          <svg width={width} height={height} role="img" aria-label={hasData ? title : `${title}: ${empty ?? "nothing recorded in this window"}`}
            tabIndex={n > 0 && hasData ? 0 : -1}
            className={on !== null ? "hovering" : undefined}
            onMouseLeave={() => { if (!keyed) setHover(null); }}
            onMouseMove={(e) => {
              const rect = (e.currentTarget as SVGSVGElement).getBoundingClientRect();
              const px = e.clientX - rect.left - left;
              setKeyed(false);
              if (!hasData || px < 0 || px > plotW || n === 0) { setHover(null); return; }
              setHover(Math.min(n - 1, Math.max(0, Math.floor(px / slot))));
            }}
            onFocus={() => { if (n > 0 && hasData) { setKeyed(true); setHover((h) => h ?? lastWithData); } }}
            onBlur={() => { setKeyed(false); setHover(null); }}
            onKeyDown={(e) => {
              if (n === 0 || !hasData) return;
              const h = hover ?? lastWithData;
              if (e.key === "ArrowRight") { move(h + 1); e.preventDefault(); }
              else if (e.key === "ArrowLeft") { move(h - 1); e.preventDefault(); }
              else if (e.key === "Home") { move(0); e.preventDefault(); }
              else if (e.key === "End") { move(n - 1); e.preventDefault(); }
              else if (e.key === "Escape") { setHover(null); }
            }}>
            <defs>
              {series.map((s, si) => (
                <linearGradient key={s.key} id={`${uid}-g${si}`} x1="0" y1="0" x2="0" y2="1">
                  <stop offset="0" style={{ stopColor: s.color, stopOpacity: 1 }} />
                  <stop offset="1" style={{ stopColor: s.color, stopOpacity: single ? "var(--bar-fade)" : 1 }} />
                </linearGradient>
              ))}
              <linearGradient id={`${uid}-hl`} x1="0" y1="0" x2="0" y2="1">
                <stop offset="0" style={{ stopColor: single ? series[0].color : "var(--accent)", stopOpacity: 0 }} />
                <stop offset="1" style={{ stopColor: single ? series[0].color : "var(--accent)", stopOpacity: "var(--band-op)" }} />
              </linearGradient>
            </defs>
            {bare ? null : hasData
              ? ticks.map((t) => (
                <g key={t}>
                  {t > 0 && <line x1={left} x2={left + plotW} y1={Math.round(y(t)) + 0.5} y2={Math.round(y(t)) + 0.5} className="grid" />}
                  <text x={left - 10} y={y(t) + 3.5} textAnchor="end" className="tick">{axisFmt(t)}</text>
                </g>
              ))
              : <>
                {EMPTY_GRID.map((f) => <line key={f} x1={left} x2={left + plotW} y1={Math.round(base - plotH * f) + 0.5} y2={Math.round(base - plotH * f) + 0.5} className="grid" />)}
                <text x={left - 10} y={base + 3.5} textAnchor="end" className="tick">{axisFmt(0)}</text>
              </>}
            {rows.map((r, i) => {
              const cx = cxOf(i);
              const x0 = colX(i);
              const isOn = on === i;
              let bars: ReactNode = null;
              if (totals[i] > 0) {
                if (single) {
                  const h = barPx(totals[i]);
                  // bare: a flat fill, lighter for a bucket not over yet
                  bars = bare
                    ? <path className="bar" d={column(x0, base - h, bw, h, 3)} style={{ fill: r.partial ? `color-mix(in srgb, ${series[0].color} 55%, var(--paper))` : series[0].color }} />
                    : <path className="bar" d={column(x0, base - h, bw, h, 4)} fill={`url(#${uid}-g0)`} />;
                } else {
                  const vals = series.map((s) => Math.max(0, r.values[s.key] ?? 0));
                  const px = stackPx(vals, barPx(totals[i]));
                  const topK = px.reduce((a, p, k) => (p.ext > 0 ? k : a), -1);
                  let yb = base;
                  bars = series.map((s, si) => {
                    const p = px[si];
                    if (p.ext <= 0) return null;
                    const y1 = yb - p.ext;
                    const h = p.ext - p.gap;
                    yb = y1;
                    return <path key={s.key} className="bar" d={column(x0, y1, bw, h, si === topK ? 4 : 0)} fill={`url(#${uid}-g${si})`} />;
                  });
                }
              }
              return (
                <g key={r.x} className={isOn ? "col hover" : "col"}>
                  {isOn && (bare
                    ? <rect x={left + slot * i + 1} y={padT - 4} width={Math.max(0, slot - 2)} height={plotH + 4} rx={Math.min(8, slot / 3)} style={{ fill: "var(--wash)" }} className="hl" />
                    : <rect x={left + slot * i + 1} y={PAD.top - 4} width={Math.max(0, slot - 2)} height={plotH + 4} rx={Math.min(8, slot / 3)} fill={`url(#${uid}-hl)`} className="hl" />)}
                  {bars}
                  {totals[i] === 0 && !bare && <rect x={Math.round(cx - Math.min(bw, 10) / 2)} y={base - 3} width={Math.min(bw, 10)} height={2} rx={1} className="quiet" />}
                  {showName(i) && (
                    <text x={cx} y={height - (bare ? 9 : 7)} textAnchor="middle" className={isOn ? "tick on" : on !== null ? "tick dim" : "tick"}>{isOn ? full[i] : names.get(i)}</text>
                  )}
                </g>
              );
            })}
            <line x1={left} x2={left + plotW} y1={base + 0.5} y2={base + 0.5} className="axis" />
            {peak >= 0 && (() => {
              // the tallest bar's count over it, kept inside the plot at either edge
              const s = fmt(max), w = textWidth(s, `500 12px ${family}`);
              const x = Math.max(left + w / 2, Math.min(left + plotW - w / 2, cxOf(peak)));
              return <text x={x} y={12} textAnchor="middle" className="peak">{s}</text>;
            })()}
          </svg>
          {!hasData && !bare && (
            <div className="chart-empty" style={{ left, right: PAD.right, bottom: PAD.bottom }}>
              <span className="chart-empty-badge">
                <svg viewBox="0 0 16 16" width="14" height="14" aria-hidden="true"><path d="M2 13.5h12M4 10.5v1M8 8.5v3M12 10.5v1" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" /></svg>
                {empty ?? "nothing recorded in this window"}
              </span>
            </div>
          )}
          {on !== null && rows[on] && (
            <div ref={tipRef} className={"chart-tip" + (single ? " single" : " stack")} style={{ transform: `translate(${Math.round(tip.x)}px, ${Math.round(tip.y)}px)` }} role={keyed ? "status" : undefined}>
              <div className="tip-x">{rows[on].x}</div>
              {single ? (
                <div className="tip-main"><b className="num">{fmt(totals[on])}</b><span>{series[0].label}</span></div>
              ) : (
                <>
                  {[...series].reverse().map((s) => (
                    <div key={s.key} className="tip-row"><i className="sw" style={{ background: s.color }} /><span className="tip-l">{s.label}</span><b className="num">{fmt(rows[on].values[s.key] ?? 0)}</b></div>
                  ))}
                  <div className="tip-row total"><span className="tip-l">total</span><b className="num">{fmt(totals[on])}</b></div>
                </>
              )}
              {rows[on].note && <div className="tip-note">{rows[on].note}</div>}
            </div>
          )}
        </>
      )}
      </div>
      {series.length > 1 && (
        <ul className="chart-legend">
          {series.map((s) => <li key={s.key}><i className="sw" style={{ background: s.color }} />{s.label}</li>)}
        </ul>
      )}
    </div>
  );
}
