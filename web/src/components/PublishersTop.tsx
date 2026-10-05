"use client";
import { useId, useRef, useState, type CSSProperties, type ReactNode } from "react";
import Link from "next/link";
import { type Window, bytes, fmtShare, int, tia, utcWord } from "@/lib/api";
import type { MarketWithQueue, PublisherWithQueue } from "@/lib/withdrawals";
import { type WindowName, periodName } from "@/lib/window";
import { unit } from "@/components/Unit";
import Ident from "@/components/Ident";
import { Who } from "@/components/Ledger";
import Warn from "@/components/Warn";
import { Lbl, ShowPeriod, age, dayTime, holding } from "@/components/BlobsDeck";

/**
 * The Publishers page's top: who published in the period, and how much of it
 * each one holds. The Blobs top's sibling: one frame, the period's figures in
 * a strip of tabs (the publishers active, the blob size, the fees paid, the
 * escrow held now), and under the strip, for the tab picked, the publishers
 * ranked by that figure: a row each, its mark, a bar of its part of the whole
 * on a quiet track, the figure and its share. Every one when they fit, else
 * the five largest and the rest as one row.
 *
 * The strip's figures are one /v1/market answer; the rows are the same
 * period's /v1/publishers answer (the escrow's every account on record, as
 * the escrow is a balance now). While another period loads, the last answers
 * stay, a step back, and the top changes in one step.
 *
 * A period with no blob and no deposit folds the top to one band: the 0, when
 * the last blob settled and who posted it, and the escrow held now.
 */

const plural = (n: number, w: string) => `${int(n)} ${w}${n === 1 ? "" : "s"}`;

/** a publisher as the table names it: its registry label, else the chip with the address's last four; `short` drops the chip's prefix */
function Name({ p, short }: { p: { publisher: string; label?: string }; short?: boolean }) {
  if (p.label) {
    return (
      <Link className="lg-who tp-lab" href={`/publisher/?addr=${p.publisher}`} title={p.publisher}>
        <Ident addr={p.publisher} /><span className="nm">{p.label}</span>
      </Link>
    );
  }
  return <span className={short ? "tp-who short" : "tp-who"}><Who addr={p.publisher} /></span>;
}

/** what a tab ranks the publishers by */
type Metric = "n" | "bytes" | "fees" | "escrow";
type Row = { p: PublisherWithQueue; v: number };
/** the rows shown: every one when they fit, else the five largest and the rest as one */
const TOP = 5;

export default function PublishersTop({ m, win, list, all, now, pre, onWin }: {
  /** /v1/market for the period; the last answer stays while another period loads */
  m: MarketWithQueue | null;
  /** the period selected: the labels' until the first answer lands */
  win: WindowName;
  /** /v1/publishers for the period: the rows of the first three tabs */
  list: { publishers: PublisherWithQueue[]; count: number; window?: Window } | null;
  /** every account on record: the escrow tab's rows, and the newest blob for a quiet period */
  all: { publishers: PublisherWithQueue[] } | null;
  /** the observer's clock */
  now: number;
  /** before activation: every figure is a dash */
  pre: boolean;
  onWin: (w: WindowName) => void;
}) {
  // what the rows rank by: the settlements, until another tab is picked
  const [metric, setMetric] = useState<Metric>("n");
  const uid = useId();
  const tabsRef = useRef<HTMLDivElement>(null);
  const shownWin = (m?.window.name ?? win) as WindowName;
  const per = periodName(shownWin);
  const quiet = !!m && m.settlements === 0;

  // the period's rows only once they are the market answer's period, so the strip and the rows always say one period;
  // until then the last that were stay
  const rowsOf = list && m && list.window?.name === m.window.name ? list.publishers : null;
  const [kept, setKept] = useState<PublisherWithQueue[] | null>(null);
  if (rowsOf && rowsOf !== kept) setKept(rowsOf);
  const pubs = rowsOf ?? kept;

  let activeTitle = "Accounts that paid for at least one blob in the period.";
  if (m && list && list.window?.name === m.window.name && list.count > m.publishers_active) {
    activeTitle = activeTitle.slice(0, -1) + `; ${int(list.count - m.publishers_active)} more only moved escrow.`;
  }

  // ---- the escrow, held now ----
  const held = m ? m.escrow_total_utia ?? m.escrow_held_utia : 0;
  const queued = m?.withdrawal_queue?.pending;
  const accounts = !!m && (m.escrow_total_utia == null || m.escrow_total_utia === m.escrow_held_utia);
  const heldTitle = m ? [
    queued && queued.count > 0 ? `${tia(queued.utia)} queued to withdraw${queued.next_available_at ? `, the next payable from ${utcWord(queued.next_available_at)}` : ""}` : "",
    m.escrow_total_at ? `As of ${utcWord(m.escrow_total_at)}` : "",
  ].filter(Boolean).join("; ") || undefined : undefined;

  // another period is on its way: what is shown stays, a step back, until it lands
  const pending = !!m && m.window.name !== win;
  const cls = (more: string) => `pan tp tp-p${more}${pending ? " is-pending" : ""}`;

  // ---- a period with no blob and no deposit: one band, the 0, when the last blob was, and the escrow held now ----
  if (m && quiet && m.deposits.count === 0) {
    const newest = all
      ? all.publishers.reduce<PublisherWithQueue | null>((a, p) => (p.last_settlement_at && (!a || Date.parse(p.last_settlement_at) > Date.parse(a.last_settlement_at!)) ? p : a), null)
      : null;
    const newestAt = newest?.last_settlement_at ? Date.parse(newest.last_settlement_at) : 0;
    const next = newest && now ? holding(shownWin, newestAt, now) : null;
    return (
      <section className={cls(" is-quiet")} aria-label="The period's publishers" aria-busy={pending || undefined}>
        <div className="tp-g">
          <Lbl name="Publishers" per={per} />
          <p className="tp-fig zero"><span title="Accounts that paid for at least one blob in the period.">0<span className="u"> active</span></span></p>
        </div>
        <div className="tp-g">
          <h2 className="tp-lbl">Last blob</h2>
          <p className="tp-fig tp-quiet">
            {newest
              ? <span className="say">settled {now ? <span className="age" title={dayTime(newest.last_settlement_at!)}>{age(now - newestAt)} ago</span> : <span className="wait">0 d 00 h ago</span>} by <Name p={newest} short /></span>
              : <span className="say">{all ? "None on record" : <span className="wait">settled 0 d 00 h ago by 0000</span>}</span>}
            {next && <ShowPeriod to={next} onWin={onWin} />}
          </p>
        </div>
        <div className="tp-g">
          <Lbl name="Escrow held" per="now" />
          <p className="tp-fig">
            <span title={heldTitle}>{unit(tia(held))}</span>
            {accounts && <span className="beside"><b>{int(m.escrow_accounts)}</b> {m.escrow_accounts === 1 ? "account" : "accounts"}</span>}
          </p>
        </div>
      </section>
    );
  }

  // a payment promise that timed out: a dot on the fees tab (a tab holds no button, so the dot is a mark and its words
  // the tab's description), and the warning itself beside the fees list's title
  const timedOut = m && m.timeouts > 0 ? `${plural(m.timeouts, "payment promise")} timed out in the period; ${tia(m.timed_out_utia)} charged all the same` : null;

  // ---- the tabs: each a figure of the period (the escrow's now), and the measure its rows rank by ----
  const TABS: Record<Metric, {
    name: string; per: string; title: string | undefined; wait: string; figure: (m: MarketWithQueue) => ReactNode;
    list: string; total: (m: MarketWithQueue) => number; of: (p: PublisherWithQueue) => number; fmt: (v: number) => ReactNode;
    share: (p: PublisherWithQueue, total: number) => number | null; hover: (p: PublisherWithQueue) => string | undefined;
  }> = {
    n: {
      name: "Publishers", per, title: m ? `${activeTitle} ${plural(m.settlements, "settlement")} in the period.` : activeTitle, wait: "0 active",
      figure: (m) => <>{int(m.publishers_active)}<span className="u"> active</span></>,
      list: "Settlements by publisher", total: (m) => m.settlements, of: (p) => p.settlements, fmt: (v) => int(v),
      share: (p, t) => (t > 0 ? p.settlements / t : null), hover: (p) => `${plural(p.settlements, "settlement")} · ${bytes(p.bytes)} · ${tia(p.fees_utia)} paid`,
    },
    bytes: {
      name: "Total size", per: "", title: "Summed over settlements: a blob settled twice counts twice.", wait: "000.00 GiB",
      figure: (m) => unit(bytes(m.bytes)),
      list: "Blob size by publisher", total: (m) => m.bytes, of: (p) => p.bytes, fmt: (v) => unit(bytes(v)),
      share: (p) => p.bytes_share, hover: (p) => (p.avg_blob_bytes != null ? `${plural(p.settlements, "settlement")} · average blob ${bytes(Math.round(p.avg_blob_bytes))}` : undefined),
    },
    fees: {
      name: "Fees paid", per: "", wait: "00,000 TIA",
      title: [m?.paid_per_mib_utia != null ? `${tia(m.paid_per_mib_utia)} per MiB` : "", timedOut ?? ""].filter(Boolean).join("; ") || undefined,
      figure: (m) => <>{unit(tia(m.fees_settled_utia))}{timedOut && <span className="pp-dot" aria-hidden="true" />}</>,
      list: "Fees paid by publisher", total: (m) => m.fees_settled_utia, of: (p) => p.fees_utia, fmt: (v) => unit(tia(v)),
      share: (p) => p.fees_share, hover: (p) => (p.paid_per_mib_utia != null ? `${tia(p.paid_per_mib_utia)} per MiB` : undefined),
    },
    escrow: {
      name: "Escrow held", per: "now", wait: "00,000 TIA",
      title: m ? [accounts ? plural(m.escrow_accounts, "account") : "", `${tia(m.deposits.utia)} deposited in the period${m.deposits.count > 0 ? ` (${plural(m.deposits.count, "deposit")})` : ""}`, heldTitle ?? ""].filter(Boolean).join("; ") : undefined,
      figure: (m) => <>{unit(tia(held))}{accounts && <span className="beside"><b>{int(m.escrow_accounts)}</b> {m.escrow_accounts === 1 ? "account" : "accounts"}</span>}</>,
      list: "Escrow held by account", total: () => held, of: (p) => (p.escrow?.found ? p.escrow.balance_utia : 0), fmt: (v) => unit(tia(v)),
      share: (p, t) => (t > 0 && p.escrow?.found ? p.escrow.balance_utia / t : null),
      hover: (p) => (p.escrow?.found ? `${tia(p.escrow.available_utia)} available to spend; as of block #${int(p.escrow.height)}` : undefined),
    },
  };
  const keys = Object.keys(TABS) as Metric[];
  const T = TABS[metric];

  // the rows: the period's publishers that paid for a blob, or for the escrow every account holding any, largest first
  const source = metric === "escrow" ? all?.publishers ?? null : pubs;
  const ranked: Row[] | null = source && m
    ? source.map((p) => ({ p, v: T.of(p) })).filter((r) => r.v > 0).sort((a, b) => b.v - a.v)
    : null;
  const total = m ? T.total(m) : 0;
  const head = ranked ? (ranked.length <= TOP + 1 ? ranked : ranked.slice(0, TOP)) : [];
  const rest = ranked && ranked.length > TOP + 1 ? ranked.slice(TOP) : [];
  const restV = rest.reduce((s, r) => s + r.v, 0);
  // a bar is its part of the whole: the track is the period's total, and anything above zero shows at least a sliver
  const w = (v: number) => (total > 0 ? Math.min(1, v / total) : 0);

  // the tabs as a tab list, as on Blobs: one stop for Tab, the arrow keys (and Home / End) moving the choice and the focus
  const pick = (k: Metric) => { setMetric(k); tabsRef.current?.querySelector<HTMLElement>(`[data-k="${k}"]`)?.focus(); };
  const onKey = (e: React.KeyboardEvent<HTMLDivElement>) => {
    const i = keys.indexOf(metric);
    const to = e.key === "ArrowRight" ? keys[(i + 1) % keys.length] : e.key === "ArrowLeft" ? keys[(i + keys.length - 1) % keys.length]
      : e.key === "Home" ? keys[0] : e.key === "End" ? keys[keys.length - 1] : null;
    if (to) { e.preventDefault(); pick(to); }
  };

  return (
    <section className={cls(" tc pp")} aria-label="The period's publishers" aria-busy={pending || undefined}>
      <div className="tc-tabs pp-tabs" role="tablist" aria-label="What the publishers are ranked by" ref={tabsRef} onKeyDown={onKey}>
        {keys.map((k) => {
          const x = TABS[k];
          const warned = k === "fees" && !!timedOut;
          return (
            <div key={k} role="tab" id={`${uid}-${k}`} data-k={k} aria-selected={metric === k} aria-controls={`${uid}-list`} tabIndex={metric === k ? 0 : -1}
              aria-describedby={warned ? `${uid}-to` : undefined}
              className="tc-tab" onClick={() => setMetric(k)} onKeyDown={(e) => { if (e.key === "Enter" || e.key === " ") { e.preventDefault(); setMetric(k); } }} title={x.title}>
              <span className="tc-k">{x.name}{x.per && <span className="per"> · {x.per}</span>}</span>
              <span className="tc-v">{m ? x.figure(m) : pre ? "—" : <span className="wait">{x.wait}</span>}</span>
              {warned && <span className="sr-only" id={`${uid}-to`}>{timedOut}</span>}
            </div>
          );
        })}
      </div>
      <div className="pp-plot" role="tabpanel" id={`${uid}-list`} aria-labelledby={`${uid}-${metric}`}>
        <h3 className="tp-ct">{T.list}{metric === "escrow" && <span className="tz">now{m && accounts && <span className="pp-acc"> · {plural(m.escrow_accounts, "account")}</span>}</span>}{metric === "fees" && timedOut && <Warn tone="fault" text={timedOut} />}</h3>
        {pre
          ? <p className="pp-none">No publisher has paid for a blob yet</p>
          : !ranked
            ? <ol className="pp-rows is-wait">{[0, 1, 2].map((i) => <li key={i} className="pp-row"><span className="pp-who"><span className="wait">celestia ••• 0000</span></span><span className="pp-bar" /><span className="pp-v"><span className="wait">000.0 TIA</span></span><span className="pp-sh"><span className="wait">00.00%</span></span></li>)}</ol>
            : ranked.length === 0
              ? <p className="pp-none">{metric === "escrow" ? "No escrow held" : `No publisher paid for a blob in ${per}`}</p>
              : <ol className="pp-rows">
                {head.map((r) => (
                  <li key={r.p.publisher} className="pp-row" title={T.hover(r.p)}>
                    <span className="pp-who"><Name p={r.p} /></span>
                    <span className="pp-bar" aria-hidden="true"><i style={{ "--w": w(r.v) } as CSSProperties} /></span>
                    <span className="pp-v">{T.fmt(r.v)}</span>
                    <span className="pp-sh">{fmtShare(T.share(r.p, total))}</span>
                  </li>
                ))}
                {rest.length > 0 && (
                  <li className="pp-row pp-rest" title={rest.map((r) => r.p.label || `celestia ••• ${r.p.publisher.slice(-4)}`).join(", ")}>
                    <span className="pp-who"><span className="pp-n">{int(rest.length)} others</span></span>
                    <span className="pp-bar" aria-hidden="true"><i style={{ "--w": w(restV) } as CSSProperties} /></span>
                    <span className="pp-v">{T.fmt(restV)}</span>
                    <span className="pp-sh">{fmtShare(total > 0 ? restV / total : null)}</span>
                  </li>
                )}
              </ol>}
      </div>
    </section>
  );
}
