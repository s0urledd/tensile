"use client";
import { useEffect, useRef, useState } from "react";

/**
 * A column chart with axes, gridlines, a hover tooltip and, for more than
 * one series, a stack with a legend. Drawn in pixels from the container's
 * measured width, so text never stretches; nothing here scales a viewBox.
 *
 * Rows are the x categories in order (days); every row is drawn, including
 * empty ones, so a quiet week is a quiet week and not a shorter chart.
 */
export type Series = { key: string; label: string; color: string };
export type Row = { x: string; label?: string; values: Record<string, number>; note?: string };

const PAD = { top: 10, right: 8, bottom: 22, left: 8 };

function niceStep(max: number, ticks: number): number {
  if (max <= 0) return 1;
  const raw = max / ticks;
  const mag = Math.pow(10, Math.floor(Math.log10(raw)));
  const norm = raw / mag;
  const step = norm <= 1 ? 1 : norm <= 2 ? 2 : norm <= 2.5 ? 2.5 : norm <= 5 ? 5 : 10;
  return step * mag;
}

export default function Chart({ series, rows, fmt, height = 200, fmtAxis, title, sub, empty }: {
  series: Series[];
  rows: Row[];
  fmt: (v: number) => string;
  fmtAxis?: (v: number) => string;
  height?: number;
  title: string;
  /** a figure beside the title: "1.35 GiB in the period" */
  sub?: string;
  empty?: string;
}) {
  const ref = useRef<HTMLDivElement>(null);
  const [width, setWidth] = useState(0);
  const [hover, setHover] = useState<number | null>(null);
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
  // Left gutter from the widest tick label, in a 11px mono: ~6.6px a char.
  const left = PAD.left + Math.max(...ticks.map((t) => axisFmt(t).length)) * 6.6 + 8;
  const plotW = Math.max(0, width - left - PAD.right);
  const plotH = height - PAD.top - PAD.bottom;
  const n = rows.length;
  const slot = n > 0 ? plotW / n : 0;
  const bw = Math.max(2, Math.min(28, slot * 0.62));
  const y = (v: number) => PAD.top + plotH * (1 - v / top);
  // One label per 44px at least; the last day is labelled only when it is
  // a full step past the previous label, so two never print over each other.
  const every = Math.max(1, Math.ceil(n / Math.max(1, Math.floor(plotW / 44))));
  const lastDrawn = Math.floor((n - 1) / every) * every;
  const showLabel = (i: number) => i % every === 0 || (i === n - 1 && i - lastDrawn >= every);
  const hasData = max > 0;

  return (
    <div className="chart" ref={ref}>
      <div className="chart-head"><span className="label">{title}</span>{sub && <span className="chart-sub">{sub}</span>}</div>
      {width > 0 && (
        <div className="chart-plot" style={{ height }}>
          <svg width={width} height={height} role="img" aria-label={title}
            onMouseLeave={() => setHover(null)}
            onMouseMove={(e) => {
              const rect = (e.currentTarget as SVGSVGElement).getBoundingClientRect();
              const px = e.clientX - rect.left - left;
              if (px < 0 || px > plotW || n === 0) { setHover(null); return; }
              setHover(Math.min(n - 1, Math.max(0, Math.floor(px / slot))));
            }}>
            {/* an all-zero series has no scale to read: no gridlines or tick labels, the sentence below instead */}
            {hasData && ticks.map((t) => (
              <g key={t}>
                <line x1={left} x2={left + plotW} y1={y(t)} y2={y(t)} className="grid" />
                <text x={left - 6} y={y(t) + 3.5} textAnchor="end" className="tick">{axisFmt(t)}</text>
              </g>
            ))}
            {rows.map((r, i) => {
              const cx = left + slot * i + slot / 2;
              let acc = 0;
              const segs = series.map((s, si) => {
                const v = r.values[s.key] ?? 0;
                const y0 = y(acc), y1 = y(acc + v);
                acc += v;
                const h = Math.max(0, y0 - y1 - (si > 0 && v > 0 ? 2 : 0));
                return v > 0 ? <rect key={s.key} x={cx - bw / 2} y={y1} width={bw} height={h} rx={acc === totals[i] ? 3 : 0} fill={s.color} /> : null;
              });
              return (
                <g key={r.x} className={hover === i ? "col hover" : "col"}>
                  {hover === i && <rect x={left + slot * i} y={PAD.top} width={slot} height={plotH} className="hl" />}
                  {segs}
                  {totals[i] === 0 && <rect x={cx - bw / 2} y={y(0) - 1.5} width={bw} height={1.5} className="quiet" />}
                  {showLabel(i) && (
                    <text x={cx} y={height - 6} textAnchor="middle" className="tick">{r.label ?? r.x}</text>
                  )}
                </g>
              );
            })}
            <line x1={left} x2={left + plotW} y1={y(0) + 0.5} y2={y(0) + 0.5} className="axis" />
          </svg>
          {!hasData && <div className="chart-empty">{empty ?? "nothing recorded in this window"}</div>}
          {hover !== null && rows[hover] && (
            <div className="chart-tip" style={{ left: Math.min(width - 190, Math.max(0, left + slot * hover + slot / 2 - 90)) }}>
              <div className="tip-x">{rows[hover].x}</div>
              {series.length > 1 && [...series].reverse().map((s) => (
                <div key={s.key} className="tip-row"><i className="sw" style={{ background: s.color }} />{s.label}<b>{fmt(rows[hover].values[s.key] ?? 0)}</b></div>
              ))}
              <div className="tip-row total">{series.length > 1 ? "total" : series[0].label}<b>{fmt(totals[hover])}</b></div>
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
