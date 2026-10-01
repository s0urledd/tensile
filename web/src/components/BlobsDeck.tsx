"use client";
import { useEffect, useRef, useState } from "react";
import Link from "next/link";
import { type Blob, type Market, bytes, int, tia, utc } from "@/lib/api";
import { buckets, type Bucket } from "@/lib/buckets";
import { type WindowName, WINDOWS, periodName, windowLabel } from "@/lib/window";
import { unit } from "@/components/Unit";
import RollNumber from "@/components/RollNumber";
import Chart from "@/components/Chart";

/**
 * The Blobs page's deck: the overview's panel holding the period in two
 * instruments, a hairline between them. On the left the period's blobs as
 * the lead figure, then its blob size, fees and namespaces; on the right one
 * chart, settlements per hour (per day past a day), bars from zero, with the
 * chain's rate over the newest blobs read above it.
 *
 * A period with no settlement folds the deck to one band: when the last blob
 * settled, the period's empty buckets as a flat line, and the shortest
 * longer period that holds that blob, one click away.
 *
 * Every figure is the API's: the period's from /v1/market, the rate and the
 * last blob from the newest rows of /v1/blobs (the page passes them in, so
 * the deck asks for nothing of its own).
 */

/** the rate is the chain's pace only while its newest blob is this recent */
const PACE_FRESH_MS = 10 * 60_000;
const SPAN_MS: Partial<Record<WindowName, number>> = { "24h": 86400_000, "7d": 7 * 86400_000, "30d": 30 * 86400_000 };

/** "12 s", "4 min", "3 h 12 min", "1 d 20 h": how long ago, in its two largest units */
export function age(ms: number): string {
  const s = Math.max(0, Math.floor(ms / 1000));
  if (s < 60) return `${s} s`;
  const m = Math.floor(s / 60);
  if (m < 60) return `${m} min`;
  const h = Math.floor(m / 60);
  if (h < 24) return m % 60 ? `${h} h ${m % 60} min` : `${h} h`;
  const d = Math.floor(h / 24);
  return h % 24 ? `${d} d ${h % 24} h` : `${d} d`;
}

const MON = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
/** "28 Sep", the UTC day; spelled out here, since a locale's short month can be "Sept" */
export function dayMonth(d: Date): string {
  return `${d.getUTCDate()} ${MON[d.getUTCMonth()]}`;
}

/** "28 Sep 20:48:38 UTC" */
export function dayTime(s: string): string {
  const d = new Date(s);
  if (isNaN(d.getTime())) return s;
  return `${dayMonth(d)} ${utc(s).slice(11, 19)} UTC`;
}

/** the observer's clock, read every 15 s: the deck's ages are minutes and days */
function useNow(skew: number): number {
  const [now, setNow] = useState(0);
  const skewRef = useRef(skew);
  skewRef.current = skew;
  useEffect(() => {
    const tick = () => setNow(Date.now() + skewRef.current);
    tick();
    const t = window.setInterval(tick, 15000);
    return () => window.clearInterval(t);
  }, []);
  return now;
}

/** a phone's width: the chart is shorter there */
function useNarrow(): boolean {
  const [narrow, setNarrow] = useState(false);
  useEffect(() => {
    const q = window.matchMedia("(max-width: 720px)");
    const on = () => setNarrow(q.matches);
    on();
    q.addEventListener("change", on);
    return () => q.removeEventListener("change", on);
  }, []);
  return narrow;
}

/** blobs per minute over the newest blobs read (newest first), while the newest is recent: the chain's pace now */
function pace(rows: Blob[], now: number): { per: number; n: number; from: string; to: string } | null {
  if (rows.length < 2 || !now) return null;
  const t0 = Date.parse(rows[0].settlement_time), t1 = Date.parse(rows[rows.length - 1].settlement_time);
  const mins = (t0 - t1) / 60000;
  if (!(mins > 0) || now - t0 > PACE_FRESH_MS) return null;
  return { per: (rows.length - 1) / mins, n: rows.length, from: rows[rows.length - 1].settlement_time, to: rows[0].settlement_time };
}
const fmtPace = (v: number) => (v >= 10 ? String(Math.round(v)) : v.toFixed(1));

/** the shortest period after this one that holds the last blob */
function holding(win: WindowName, last: number, now: number): WindowName | null {
  for (const w of WINDOWS.slice(WINDOWS.indexOf(win) + 1)) {
    const span = SPAN_MS[w];
    if (span === undefined || now - last <= span) return w;
  }
  return null;
}

/** the period's empty buckets as a flat line: a stub on the axis for each, and the axis's times */
function Flat({ cells, hourly }: { cells: Bucket[]; hourly: boolean }) {
  const named = (c: Bucket, i: number): "" | "on" | "alt" => {
    if (hourly) { const h = Number(c.key.slice(11, 13)); return h % 6 === 0 ? "on" : h % 3 === 0 ? "alt" : ""; }
    const back = cells.length - 1 - i;
    if (cells.length <= 8) return back % 2 === 0 ? "on" : "alt";
    return back % 10 === 0 ? "on" : back % 5 === 0 ? "alt" : "";
  };
  return (
    <div className="lg-flat" style={{ gridTemplateColumns: `repeat(${cells.length}, minmax(0, 1fr))` }} aria-hidden="true">
      {cells.map((c, i) => {
        const n = named(c, i);
        return <span key={c.key}><i />{n && <em className={n === "alt" ? "alt" : undefined}>{c.label}</em>}</span>;
      })}
    </div>
  );
}

export default function BlobsDeck({ win, onWin, market, newest, skew }: {
  win: WindowName;
  onWin: (w: WindowName) => void;
  /** /v1/market for the period; the last answer stays while another period loads */
  market: Market | null;
  /** the chain's newest blobs, newest first, whatever the list below is filtered to */
  newest: Blob[] | null;
  skew: number;
}) {
  const now = useNow(skew);
  const narrow = useNarrow();
  const m = market;
  // the answer for another period, kept while this one loads, is not this chart
  const mine = !!m && m.window.name === win;
  const series = m && mine ? buckets(m, win) : [];
  const per = win === "24h" ? "hour" : "day";
  const last = newest?.[0] ?? null;
  const lastAt = last ? Date.parse(last.settlement_time) : 0;

  // ---- a period with no settlement: when the last one was ----
  if (m && mine && m.settlements === 0) {
    const next = last && now ? holding(win, lastAt, now) : null;
    return (
      <section className="deck lg-deck is-quiet" aria-label="The period's settlements">
        <div className="lg-lead">
          <h2 className="ov-eyebrow">Last blob</h2>
          {last
            ? <>
              <p className="lg-fig">{now ? age(now - lastAt) : <span className="wait">0 d 00 h</span>}<small>ago</small></p>
              <p className="lg-help"><Link href={`/blob/?hash=${last.promise_hash}`}>#{int(last.settlement_height)}</Link> · {dayTime(last.settlement_time)}</p>
            </>
            : <p className="lg-help lg-none">{newest ? "No blob on record" : <span className="wait">#0,000,000 · 00 Sep 00:00:00 UTC</span>}</p>}
        </div>
        <div className="lg-plot">
          <h3 className="ov-eyebrow">Settlements per {per} <span className="per">({periodName(win)})</span></h3>
          <Flat cells={series} hourly={win === "24h"} />
          <div className="lg-rest">
            <p>No settlement {win === "all" ? "on record" : `in the last ${{ "24h": "24 hours", "7d": "7 days", "30d": "30 days" }[win]}`}</p>
            {next && (
              <button type="button" className="lg-go" onClick={() => onWin(next)}>
                Show {windowLabel(next)}
                <svg width="12" height="12" viewBox="0 0 12 12" aria-hidden="true"><path d="M2 6h7.5M6.5 2.8 9.7 6 6.5 9.2" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" /></svg>
              </button>
            )}
          </div>
        </div>
      </section>
    );
  }

  // ---- the period's figures and its chart ----
  const p = newest ? pace(newest, now) : null;
  const rate = p
    ? <span className="lg-pace" title={`${int(p.n)} blobs settled between ${utc(p.from).slice(11, 19)} and ${utc(p.to).slice(11, 19)} UTC`}>
      <i className="d" aria-hidden="true" /><b><RollNumber value={Math.round(p.per * 10) / 10} format={fmtPace} /></b>blobs/min
    </span>
    : last && now
      ? <span className="lg-pace" title={dayTime(last.settlement_time)}>Last blob {age(now - lastAt)} ago</span>
      : null;
  return (
    <section className="deck lg-deck" aria-label="The period's settlements">
      <div className="lg-lead">
        {/* a figure keeps the last answer while another period loads, so it names that answer's period */}
        <h2 className="ov-eyebrow">Blobs <span className="per">({periodName(m?.window.name ?? win)})</span></h2>
        <p className="lg-fig" title="Blobs (BlobID) settled in the period.">{m ? <RollNumber value={m.blobs} format={int} /> : <span className="wait">0,000</span>}</p>
        {/* the settlements that paid for them, only where they are not the same count */}
        {m && m.settlements !== m.blobs && <p className="lg-help" title="A blob settled twice counts once as a blob and twice as a settlement."><b>{int(m.settlements)}</b> settlements</p>}
        <dl className="lg-stats">
          <div title="Summed over settlements: a blob settled twice counts twice."><dt>Blob size</dt><dd>{m ? unit(bytes(m.bytes)) : <span className="wait">000.00 GiB</span>}</dd></div>
          <div><dt>Fees paid</dt><dd>{m ? unit(tia(m.fees_settled_utia)) : <span className="wait">00,000 TIA</span>}</dd></div>
          <div title="Namespaces the period's settlements used, of every namespace on record."><dt>Namespaces</dt><dd>{m?.namespaces != null ? <>{int(m.namespaces)}{m.namespaces_total != null && <span className="of"> of {int(m.namespaces_total)}</span>}</> : <span className="wait">0 of 00</span>}</dd></div>
        </dl>
      </div>
      <div className="lg-plot">
        <Chart title={`Settlements per ${per}`} height={narrow ? 150 : 176}
          head={<div className="lg-plot-h"><h3 className="ov-eyebrow">Settlements per {per}</h3>{rate}</div>}
          series={[{ key: "n", label: "settlements", color: "var(--accent)" }]}
          rows={series.map((c) => ({ x: c.title, label: c.label, short: c.short, values: { n: c.settlements }, note: `${bytes(c.bytes)} · ${tia(c.fees)} fees` }))}
          fmt={(v) => int(v)} empty={mine ? "nothing settled" : "loading…"} />
      </div>
    </section>
  );
}
