"use client";
import { useEffect, useId, useRef, useState, type CSSProperties } from "react";

/**
 * A column chart with axes, gridlines, a hover tooltip and, for more than
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
 * identity of each segment stays exact.
 *
 * The plot is focusable: the arrow keys (and Home / End) walk the buckets
 * and the tooltip follows, the same readout the pointer gets.
 */
export type Series = { key: string; label: string; color: string };
export type Row = { x: string; label?: string; values: Record<string, number>; note?: string };

const PAD = { top: 12, right: 4, bottom: 26, left: 0 };
/** the readout: narrow for one series, wide enough for a publisher name beside its value in a stack */
const TIP_W = 176, TIP_W_STACK = 232;

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

export default function Chart({ series, rows, fmt, height = 200, fmtAxis, title, sub, figure, figureNote, empty }: {
  series: Series[];
  rows: Row[];
  fmt: (v: number) => string;
  fmtAxis?: (v: number) => string;
  height?: number;
  title: string;
  /** a line beside the title: "1.35 GiB in the period" (use figure/figureNote to set the figure large) */
  sub?: string;
  /** the period's total, printed large under the title: "1.35 GiB" */
  figure?: string;
  /** the words after the figure: "in the period" */
  figureNote?: string;
  empty?: string;
}) {
  const ref = useRef<HTMLDivElement>(null);
  const [width, setWidth] = useState(0);
  const [hover, setHover] = useState<number | null>(null);
  const [keyed, setKeyed] = useState(false);
  const uid = "c" + useId().replace(/[^a-zA-Z0-9]/g, "");
  useEffect(() => {
    const el = ref.current;
    if (!el) return;
    const ro = new ResizeObserver((es) => setWidth(Math.floor(es[0].contentRect.width)));
    ro.observe(el);
    setWidth(el.clientWidth);
    return () => ro.disconnect();
  }, []);

  const axisFmt = fmtAxis ?? fmt;
  const totals = rows.map((r) => series.reduce((a, s) => a + (r.values[s.key] ?? 0), 0));
  const max = Math.max(0, ...totals);
  const step = niceStep(max, 4);
  const top = max === 0 ? 1 : Math.ceil(max / step) * step;
  const ticks: number[] = [];
  for (let v = 0; v <= top + 1e-9; v += step) ticks.push(v);
  const hasData = max > 0;
  // Left gutter from the widest tick label, at 11px: ~6.4px a char, and at
  // least 56px, so two charts side by side start their plots on one line.
  const left = hasData ? Math.max(56, PAD.left + Math.max(...ticks.map((t) => axisFmt(t).length)) * 6.4 + 10) : 0;
  const plotW = Math.max(0, width - left - PAD.right);
  const plotH = height - PAD.top - PAD.bottom;
  const n = rows.length;
  const slot = n > 0 ? plotW / n : 0;
  const bw = Math.max(2, Math.min(26, slot * 0.58));
  const y = (v: number) => PAD.top + plotH * (1 - v / top);
  // As many labels as fit at 11px (~5.4px a character, 10px apart); the last
  // bucket is labelled only when it is a full step past the previous label,
  // so two never print over each other.
  const labelW = Math.max(...rows.map((r) => (r.label ?? r.x).length), 1) * 5.4 + 10;
  const every = Math.max(1, Math.ceil(n / Math.max(1, Math.floor(plotW / labelW))));
  const lastDrawn = Math.floor((n - 1) / every) * every;
  const showLabel = (i: number) => i % every === 0 || (i === n - 1 && i - lastDrawn >= every);
  const single = series.length === 1;
  const lastWithData = (() => { for (let i = n - 1; i >= 0; i--) if (totals[i] > 0) return i; return n - 1; })();

  const move = (to: number) => { setKeyed(true); setHover(Math.min(n - 1, Math.max(0, to))); };

  // the readout sits beside the hovered column, never over it
  let tipX = 0;
  const tipW = series.length > 1 ? TIP_W_STACK : TIP_W;
  if (hover !== null) {
    // right of the column if it fits, else left of it, else on the roomier side, kept inside the chart
    const right = left + slot * (hover + 1) + 6;
    const leftX = left + slot * hover - 6 - tipW;
    tipX = right + tipW <= width ? right : leftX >= 0 ? leftX
      : width - right >= left + slot * hover - 6 ? Math.min(right, width - tipW) : Math.max(0, leftX);
  }

  return (
    <div className={"chart" + (single ? " chart--single" : "")} ref={ref} style={single ? ({ "--series": series[0].color } as CSSProperties) : undefined}>
      <div className="chart-head">
        <div className="chart-title">{single && <i className="chart-key" aria-hidden="true" />}{title}</div>
        <div className="chart-figure">
          {figure !== undefined
            ? <><b className="num">{figure}</b>{figureNote && <span>{figureNote}</span>}</>
            : sub !== undefined ? <span>{sub}</span> : <b aria-hidden="true">&nbsp;</b>}
        </div>
      </div>
      {width > 0 && (
        <div className="chart-plot" style={{ height }}>
          <svg width={width} height={height} role="img" aria-label={title} tabIndex={n > 0 ? 0 : -1}
            className={hover !== null && hasData ? "hovering" : undefined}
            onMouseLeave={() => { if (!keyed) setHover(null); }}
            onMouseMove={(e) => {
              const rect = (e.currentTarget as SVGSVGElement).getBoundingClientRect();
              const px = e.clientX - rect.left - left;
              setKeyed(false);
              if (px < 0 || px > plotW || n === 0) { setHover(null); return; }
              setHover(Math.min(n - 1, Math.max(0, Math.floor(px / slot))));
            }}
            onFocus={() => { if (n > 0) { setKeyed(true); setHover((h) => h ?? lastWithData); } }}
            onBlur={() => { setKeyed(false); setHover(null); }}
            onKeyDown={(e) => {
              if (n === 0) return;
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
            {/* an all-zero series has no scale to read: no gridlines or tick labels, the sentence below instead */}
            {hasData && ticks.map((t) => (
              <g key={t}>
                {t > 0 && <line x1={left} x2={left + plotW} y1={Math.round(y(t)) + 0.5} y2={Math.round(y(t)) + 0.5} className="grid" />}
                <text x={left - 10} y={y(t) + 3.5} textAnchor="end" className="tick">{axisFmt(t)}</text>
              </g>
            ))}
            {rows.map((r, i) => {
              const cx = left + slot * i + slot / 2;
              const on = hover === i;
              let acc = 0;
              const segs = series.map((s, si) => {
                const v = r.values[s.key] ?? 0;
                const y0 = y(acc), y1 = y(acc + v);
                acc += v;
                const h = Math.max(0, y0 - y1 - (si > 0 && v > 0 ? 2 : 0));
                if (v <= 0) return null;
                const topmost = Math.abs(acc - totals[i]) < 1e-9;
                return <path key={s.key} className="bar" d={column(cx - bw / 2, y1, bw, Math.max(h, 1), topmost ? 4 : 0)} fill={`url(#${uid}-g${si})`} />;
              });
              return (
                <g key={r.x} className={on ? "col hover" : "col"}>
                  {on && <rect x={left + slot * i + 1} y={PAD.top - 4} width={Math.max(0, slot - 2)} height={plotH + 4} rx={Math.min(8, slot / 3)} fill={`url(#${uid}-hl)`} className="hl" />}
                  {segs}
                  {totals[i] === 0 && <rect x={cx - Math.min(bw, 10) / 2} y={y(0) - 3} width={Math.min(bw, 10)} height={2} rx={1} className="quiet" />}
                  {/* the hovered bucket is always named; the other labels step back while it is, and any that would touch it make room */}
                  {(on || (showLabel(i) && (hover === null || Math.abs(i - hover) * slot >= labelW))) && (
                    <text x={cx} y={height - 7} textAnchor="middle" className={on ? "tick on" : hover !== null ? "tick dim" : "tick"}>{r.label ?? r.x}</text>
                  )}
                </g>
              );
            })}
            <line x1={left} x2={left + plotW} y1={Math.round(y(0)) + 0.5} y2={Math.round(y(0)) + 0.5} className="axis" />
          </svg>
          {!hasData && (
            <div className="chart-empty">
              <span className="chart-empty-badge">
                <svg viewBox="0 0 16 16" width="14" height="14" aria-hidden="true"><path d="M2 13.5h12M4 10.5v1M8 8.5v3M12 10.5v1" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" /></svg>
                {empty ?? "nothing recorded in this window"}
              </span>
            </div>
          )}
          {hover !== null && rows[hover] && (
            <div className={"chart-tip" + (single ? " single" : "")} style={{ width: tipW, transform: `translateX(${Math.round(tipX)}px)` }} role={keyed ? "status" : undefined}>
              <div className="tip-x">{rows[hover].x}</div>
              {single ? (
                <div className="tip-main"><b className="num">{fmt(totals[hover])}</b><span>{series[0].label}</span></div>
              ) : (
                <>
                  {[...series].reverse().map((s) => (
                    <div key={s.key} className="tip-row"><i className="sw" style={{ background: s.color }} /><span className="tip-l">{s.label}</span><b className="num">{fmt(rows[hover].values[s.key] ?? 0)}</b></div>
                  ))}
                  <div className="tip-row total"><span className="tip-l">total</span><b className="num">{fmt(totals[hover])}</b></div>
                </>
              )}
              {rows[hover].note && <div className="tip-note">{rows[hover].note}</div>}
            </div>
          )}
        </div>
      )}
      {series.length > 1 && (
        <ul className="chart-legend">
          {series.map((s) => <li key={s.key}><i className="sw" style={{ background: s.color }} />{s.label}</li>)}
        </ul>
      )}
    </div>
  );
}

/** Every UTC day from start to end inclusive, as YYYY-MM-DD. */
export function calendar(start: Date, end: Date): string[] {
  const out: string[] = [];
  const t0 = Date.UTC(start.getUTCFullYear(), start.getUTCMonth(), start.getUTCDate());
  const t1 = Date.UTC(end.getUTCFullYear(), end.getUTCMonth(), end.getUTCDate());
  for (let t = t0; t <= t1; t += 86400_000) out.push(new Date(t).toISOString().slice(0, 10));
  return out;
}

/** The categorical slots, in fixed order, plus the fold-in for the rest. */
export const CATEGORICAL = ["var(--cat-1)", "var(--cat-2)", "var(--cat-3)", "var(--cat-4)", "var(--cat-5)"];
export const OTHER_COLOR = "var(--cat-other)";
