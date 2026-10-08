"use client";
import { useEffect, useId, useRef, useState } from "react";
import Link from "next/link";
import { type Blob, type Market, bytes, int, tia, utc } from "@/lib/api";
import { buckets, type Bucket } from "@/lib/buckets";
import { type WindowName, WINDOWS, periodName, windowLabel } from "@/lib/window";
import { unit } from "@/components/Unit";
import Chart from "@/components/Chart";
import Warn from "@/components/Warn";

/**
 * The Blobs page's top: how much was settled in the period and when. One frame:
 * the period's figures in a strip, three of them tabs (the settlements, their
 * total size, the fees they paid) over a chart that shows the one picked per
 * hour (per day past a day), titled with what a bar holds; the namespaces close
 * the strip, a figure with no chart.
 *
 * Everything in it is one /v1/market answer: while another period loads, the
 * last answer stays whole under its own period, a step back, and the top
 * changes in one step when the new one lands.
 *
 * A period with no settlement folds the top to one band: the 0, when the last
 * blob settled (from the newest rows of /v1/blobs, which the page passes in),
 * and the shortest longer period that holds it, one click away.
 */

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
function dayMonth(d: Date): string {
  return `${d.getUTCDate()} ${MON[d.getUTCMonth()]}`;
}

/** "28 Sep 20:48:38 UTC" */
export function dayTime(s: string): string {
  const d = new Date(s);
  if (isNaN(d.getTime())) return s;
  return `${dayMonth(d)} ${utc(s).slice(11, 19)} UTC`;
}

/** "Sep 28 20:48:38", in UTC: the day as the site's short dates write it, and the second */
export function monthDayTime(s: string): string {
  const d = new Date(s);
  if (isNaN(d.getTime())) return s;
  return `${MON[d.getUTCMonth()]} ${d.getUTCDate()} ${utc(s).slice(11, 19)}`;
}

/** the observer's clock, read every 15 s: the top's ages are minutes and days */
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

/**
 * The chart's height at the page's width: its bars are 102 px under the figures, as they are on a tablet, 88 on a phone. The
 * page's HTML is drawn for a desktop and the browser takes it over as it is, so the width is read once it has: the
 * chart is measured then too, and draws nothing before.
 */
const HEIGHTS: [string, number][] = [["(max-width: 720px)", 136], ["(max-width: 960px)", 150]];
function useChartHeight(): number {
  const [h, setH] = useState(150);
  useEffect(() => {
    const qs = HEIGHTS.map(([q]) => window.matchMedia(q));
    const on = () => setH(HEIGHTS.find(([q]) => window.matchMedia(q).matches)?.[1] ?? 150);
    on();
    qs.forEach((q) => q.addEventListener("change", on));
    return () => qs.forEach((q) => q.removeEventListener("change", on));
  }, []);
  return h;
}

/** the shortest period after this one that holds the last blob */
export function holding(win: WindowName, last: number, now: number): WindowName | null {
  for (const w of WINDOWS.slice(WINDOWS.indexOf(win) + 1)) {
    const span = SPAN_MS[w];
    if (span === undefined || now - last <= span) return w;
  }
  return null;
}

/** "Show 7d →": a period, as a text button */
export function ShowPeriod({ to, onWin }: { to: WindowName; onWin: (w: WindowName) => void }) {
  return (
    <button type="button" className="tp-go" onClick={() => onWin(to)}>
      Show {windowLabel(to)}
      <svg width="12" height="12" viewBox="0 0 12 12" aria-hidden="true"><path d="M2 6h7.5M6.5 2.8 9.7 6 6.5 9.2" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" /></svg>
    </button>
  );
}

const plural = (n: number, w: string) => `${int(n)} ${w}${n === 1 ? "" : "s"}`;

/** the lead label: the measure in caps, then the answer's own period */
export function Lbl({ name, per }: { name: string; per: string }) {
  return <h2 className="tp-lbl">{name} <span className="per">· {per}</span></h2>;
}

/**
 * The three figures with a bucket of their own: each is a tab over the chart, the chart showing the one picked (the
 * settlements to begin with), titled with what a bar holds and the bucket the period uses.
 */
type Metric = "n" | "bytes" | "fees";
const METRICS: Record<Metric, {
  name: string; chart: string; unit: string; title: (m: Market | null) => string | undefined; wait: string;
  figure: (m: Market) => React.ReactNode; of: (c: Bucket) => number; fmt: (v: number) => string; note: (c: Bucket) => string;
}> = {
  n: {
    name: "Settlements", chart: "Settlements", unit: "settlements", title: () => "Settlements paid in the period: one per blob paid for.", wait: "0,000",
    figure: (m) => <>{int(m.settlements)}{m.blobs !== m.settlements && <span className="beside" title="A blob settled twice counts once as a blob and twice as a settlement."><b>{int(m.blobs)}</b> {m.blobs === 1 ? "blob" : "blobs"}</span>}</>,
    of: (c) => c.settlements, fmt: (v) => int(v), note: (c) => `${bytes(c.bytes)} · ${tia(c.fees)} fees`,
  },
  bytes: {
    name: "Total size", chart: "Blob size", unit: "blob size", title: () => "Summed over settlements: a blob settled twice counts twice.", wait: "000.00 GiB",
    figure: (m) => unit(bytes(m.bytes)), of: (c) => c.bytes, fmt: (v) => bytes(v), note: (c) => `${plural(c.settlements, "settlement")} · ${tia(c.fees)} fees`,
  },
  fees: {
    name: "Fees paid", chart: "Fees paid", unit: "fees paid", title: (m) => (m?.paid_per_mib_utia != null ? `${tia(m.paid_per_mib_utia)} per MiB` : undefined), wait: "00,000 TIA",
    figure: (m) => unit(tia(m.fees_settled_utia)), of: (c) => c.fees, fmt: (v) => tia(v), note: (c) => `${plural(c.settlements, "settlement")} · ${bytes(c.bytes)}`,
  },
};

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
  const height = useChartHeight();
  // what the chart shows: the settlements, until another tab is picked
  const [metric, setMetric] = useState<Metric>("n");
  const uid = useId();
  const tabsRef = useRef<HTMLDivElement>(null);
  const m = market;
  // every figure, bar and label is this one answer's, the period included: while another period loads it stays whole
  const shownWin = ((m?.window.name ?? win) as WindowName);
  const per = periodName(shownWin);
  const last = newest?.[0] ?? null;
  const lastAt = last ? Date.parse(last.settlement_time) : 0;

  // another period is on its way: what is shown stays, a step back, until it lands
  const pending = !!m && m.window.name !== win;
  const cls = (more: string) => `pan tp tp-b${more}${pending ? " is-pending" : ""}`;

  // ---- a period with no settlement: the 0, when the last one was, and the period that holds it ----
  if (m && m.settlements === 0) {
    const next = last && now ? holding(shownWin, lastAt, now) : null;
    return (
      <section className={cls(" is-quiet")} aria-label="The period's settlements" aria-busy={pending || undefined}>
        <div className="tp-lead">
          <Lbl name="Settlements" per={per} />
          <p className="tp-fig zero">0</p>
        </div>
        <div className="tp-plot">
          <h2 className="tp-lbl">Last blob</h2>
          <p className="tp-fig tp-quiet">
            <span className="say">{last
              ? <>settled {now
                ? <Link className="age" href={`/blob/?hash=${last.promise_hash}`} title={`#${int(last.settlement_height)} · ${dayTime(last.settlement_time)}`}>{age(now - lastAt)} ago</Link>
                : <span className="wait">0 d 00 h ago</span>}</>
              : newest ? "None on record" : <span className="wait">settled 0 d 00 h ago</span>}</span>
            {next && <ShowPeriod to={next} onWin={onWin} />}
          </p>
        </div>
      </section>
    );
  }

  // ---- the period's figures, the three that have a chart as tabs over it ----
  const series = m ? buckets(m, shownWin) : [];
  const unitWord = shownWin === "24h" ? "hour" : "day";
  const M = METRICS[metric];
  const keys = Object.keys(METRICS) as Metric[];
  // the tabs as a tab list: one stop for Tab, the arrow keys (and Home / End) moving the choice and the focus with it;
  // a tab is not a <button>, since the fees one may hold the warning dot, which is one
  const pick = (k: Metric) => { setMetric(k); tabsRef.current?.querySelector<HTMLElement>(`[data-k="${k}"]`)?.focus(); };
  const onKey = (e: React.KeyboardEvent<HTMLDivElement>) => {
    const i = keys.indexOf(metric);
    const to = e.key === "ArrowRight" ? keys[(i + 1) % keys.length] : e.key === "ArrowLeft" ? keys[(i + keys.length - 1) % keys.length]
      : e.key === "Home" ? keys[0] : e.key === "End" ? keys[keys.length - 1] : null;
    if (to) { e.preventDefault(); pick(to); }
  };
  return (
    <section className={cls(" tc")} aria-label="The period's settlements" aria-busy={pending || undefined}>
      <div className="tc-tabs" role="tablist" aria-label="What the chart shows" ref={tabsRef} onKeyDown={onKey}>
        {keys.map((k) => {
          const x = METRICS[k];
          return (
            <div key={k} role="tab" id={`${uid}-${k}`} data-k={k} aria-selected={metric === k} aria-controls={`${uid}-plot`} tabIndex={metric === k ? 0 : -1}
              className="tc-tab" onClick={() => setMetric(k)} onKeyDown={(e) => { if (e.target === e.currentTarget && (e.key === "Enter" || e.key === " ")) { e.preventDefault(); setMetric(k); } }} title={x.title(m)}>
              <span className="tc-k">{x.name}{k === "n" && <span className="per"> · {per}</span>}</span>
              <span className="tc-v">{m
                ? <>{x.figure(m)}{k === "fees" && m.timeouts > 0 && <Warn tone="fault" text={`${plural(m.timeouts, "payment promise")} timed out in the period; ${tia(m.timed_out_utia)} charged all the same`} />}</>
                : <span className="wait">{x.wait}</span>}</span>
            </div>
          );
        })}
        <div className="tc-tab tc-static" title="Namespaces the period's settlements used, of every namespace on record.">
          <span className="tc-k">Namespaces</span>
          <span className="tc-v">{m?.namespaces != null ? <>{int(m.namespaces)}{m.namespaces_total != null && <span className="of"> of {int(m.namespaces_total)}</span>}</> : m ? "—" : <span className="wait">0 of 00</span>}</span>
        </div>
      </div>
      <div className="tp-plot" role="tabpanel" id={`${uid}-plot`} aria-labelledby={`${uid}-${metric}`}>
        <h3 className="tp-ct">{M.chart} per {unitWord}<span className="tz">UTC</span></h3>
        <Chart key={metric} bare padTop={22} height={height} title={`${M.chart} per ${unitWord}`}
          series={[{ key: "v", label: M.unit, color: "var(--accent)" }]}
          rows={series.map((c) => ({ x: c.title, label: c.label, short: c.short, values: { v: M.of(c) }, note: M.note(c), partial: c.partial }))}
          fmt={M.fmt} empty={m ? "nothing settled" : "loading…"} />
      </div>
    </section>
  );
}
