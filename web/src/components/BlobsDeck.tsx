"use client";
import { useEffect, useRef, useState } from "react";
import Link from "next/link";
import { type Blob, type Market, bytes, int, tia, utc } from "@/lib/api";
import { buckets } from "@/lib/buckets";
import { type WindowName, WINDOWS, periodName, windowLabel } from "@/lib/window";
import { unit } from "@/components/Unit";
import Chart from "@/components/Chart";
import Warn from "@/components/Warn";

/**
 * The Blobs page's top: one frame holding the period's settlements as a short
 * ledger (the count, a hairline, then blob size, fees and namespaces) and,
 * behind one thin divider, the same settlements per hour (per day past a
 * day) as bare bars. The tallest bar's top is level with the count and its
 * own count sits on the label's line; the dates sit on the last row's line,
 * so the figure and the chart read as one piece.
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
 * The chart's height at the page's width: its bars are 123 px beside the ledger, 102 under it, 88 on a phone. The
 * page's HTML is drawn for a desktop and the browser takes it over as it is, so the width is read once it has: the
 * chart is measured then too, and draws nothing before.
 */
const HEIGHTS: [string, number][] = [["(max-width: 720px)", 136], ["(max-width: 960px)", 150]];
function useChartHeight(): number {
  const [h, setH] = useState(171);
  useEffect(() => {
    const qs = HEIGHTS.map(([q]) => window.matchMedia(q));
    const on = () => setH(HEIGHTS.find(([q]) => window.matchMedia(q).matches)?.[1] ?? 171);
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

  // ---- the period's ledger and its chart ----
  const series = m ? buckets(m, shownWin) : [];
  const unitWord = shownWin === "24h" ? "hour" : "day";
  return (
    <section className={cls("")} aria-label="The period's settlements" aria-busy={pending || undefined}>
      <div className="tp-lead">
        <Lbl name="Settlements" per={per} />
        <p className="tp-fig">
          {m ? <span title="Settlements paid in the period: one per blob paid for.">{int(m.settlements)}</span> : <span className="wait">0,000</span>}
          {/* the blobs they paid for, only where they are not the same count */}
          {m && m.blobs !== m.settlements && <span className="beside" title="A blob settled twice counts once as a blob and twice as a settlement."><b>{int(m.blobs)}</b> blobs</span>}
        </p>
        <hr className="tp-rule" />
        <dl className="tp-rows">
          <div title="Summed over settlements: a blob settled twice counts twice."><dt>Blob size</dt><dd>{m ? unit(bytes(m.bytes)) : <span className="wait">000.00 GiB</span>}</dd></div>
          <div>
            <dt>Fees paid</dt>
            <dd>{m
              ? <>
                <span title={m.paid_per_mib_utia != null ? `${tia(m.paid_per_mib_utia)} per MiB` : undefined}>{unit(tia(m.fees_settled_utia))}</span>
                {m.timeouts > 0 && <Warn tone="fault" text={`${plural(m.timeouts, "payment promise")} timed out in the period; ${tia(m.timed_out_utia)} charged all the same`} />}
              </>
              : <span className="wait">00,000 TIA</span>}</dd>
          </div>
          <div title="Namespaces the period's settlements used, of every namespace on record.">
            <dt>Namespaces</dt>
            <dd>{m?.namespaces != null ? <>{int(m.namespaces)}{m.namespaces_total != null && <span className="of"> of {int(m.namespaces_total)}</span>}</> : m ? "—" : <span className="wait">0 of 00</span>}</dd>
          </div>
        </dl>
      </div>
      <div className="tp-plot">
        <Chart bare padTop={22} height={height} title={`Settlements per ${unitWord}`}
          series={[{ key: "n", label: "settlements", color: "var(--accent)" }]}
          rows={series.map((c) => ({ x: c.title, label: c.label, short: c.short, values: { n: c.settlements }, note: `${bytes(c.bytes)} · ${tia(c.fees)} fees`, partial: c.partial }))}
          fmt={(v) => int(v)} empty={m ? "nothing settled" : "loading…"} />
      </div>
    </section>
  );
}
