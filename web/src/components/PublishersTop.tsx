"use client";
import type { CSSProperties, ReactNode } from "react";
import Link from "next/link";
import { type PublisherShare, type Window, bytes, fmtShare, int, tia, utcWord } from "@/lib/api";
import type { MarketWithQueue, PublisherWithQueue } from "@/lib/withdrawals";
import { buckets } from "@/lib/buckets";
import { type WindowName, periodName } from "@/lib/window";
import { unit } from "@/components/Unit";
import Ident from "@/components/Ident";
import { Who } from "@/components/Ledger";
import Warn from "@/components/Warn";
import { Lbl, ShowPeriod, age, dayTime, holding } from "@/components/BlobsDeck";

/**
 * The Publishers page's top: one frame, three short ledgers side by side,
 * each a figure under a label that names it and its period, a hairline, then
 * two rows. Activity: the publishers that paid for blobs, with the
 * settlements and namespaces. Blob size: the period's total, then how much of
 * it the largest publisher posted, as a bar and in words at both ends. Fees
 * and escrow: the fees paid, then the deposits in the period and the escrow
 * held now.
 *
 * Every figure is one /v1/market answer, labelled with that answer's period:
 * while another period loads, the last answer stays whole and the top
 * changes in one step. The list's count is read only for a hover, and the
 * last blob on record only for a quiet period.
 */

const plural = (n: number, w: string) => `${int(n)} ${w}${n === 1 ? "" : "s"}`;
/** "celestia1las83…e9snr": an address short enough for a sentence */
const shortAddr = (a: string) => (a.length > 24 ? `${a.slice(0, 14)}…${a.slice(-5)}` : a);

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

export default function PublishersTop({ m, win, list, all, now, pre, onWin }: {
  /** /v1/market for the period; the last answer stays while another period loads */
  m: MarketWithQueue | null;
  /** the period selected: the labels' until the first answer lands */
  win: WindowName;
  /** /v1/publishers for the period: its count, for the Publishers hover only */
  list: { count: number; window?: Window } | null;
  /** every account on record: the newest blob, for a quiet period only */
  all: { publishers: PublisherWithQueue[] } | null;
  /** the observer's clock */
  now: number;
  /** before activation: every figure is a dash */
  pre: boolean;
  onWin: (w: WindowName) => void;
}) {
  const ph = (s: string): ReactNode => (pre ? "—" : <span className="wait">{s}</span>);
  const shownWin = (m?.window.name ?? win) as WindowName;
  const per = periodName(shownWin);
  const quiet = !!m && m.settlements === 0;

  // ---- Activity ----
  let activeTitle = "Accounts that paid for at least one blob in the period.";
  if (m && list && list.window?.name === m.window.name && list.count > m.publishers_active) {
    activeTitle = activeTitle.slice(0, -1) + `; ${int(list.count - m.publishers_active)} more only moved escrow.`;
  }
  const spread = m && m.settlements > 0 ? buckets(m, shownWin).filter((b) => b.settlements > 0).length : 0;
  const spreadWords = shownWin === "24h" ? `in ${plural(spread, "hour")}` : `on ${plural(spread, "day")}`;

  // ---- Blob size: the largest publisher's part ----
  const lead: PublisherShare | null = m?.largest_poster ?? null;
  const share = lead?.bytes_share ?? null;
  const others = m ? Math.max(0, m.publishers_active - 1) : 0;
  // exactly one other is named, never counted
  const other = others === 1 ? m?.top_publishers.find((p) => p.publisher !== lead?.publisher) ?? null : null;
  const restShare = share != null ? Math.max(0, 1 - share) : null;
  const leadName = lead ? lead.label || shortAddr(lead.publisher) : "";
  const restWho = other ? other.label || shortAddr(other.publisher) : plural(others, "other publisher");
  const restBytes = m && lead ? Math.max(0, m.bytes - lead.bytes) : 0;

  // ---- a quiet period: the newest blob on record ----
  const newest = quiet && all
    ? all.publishers.reduce<PublisherWithQueue | null>((a, p) => (p.last_settlement_at && (!a || Date.parse(p.last_settlement_at) > Date.parse(a.last_settlement_at!)) ? p : a), null)
    : null;
  const newestAt = newest?.last_settlement_at ? Date.parse(newest.last_settlement_at) : 0;
  const next = newest && now ? holding(shownWin, newestAt, now) : null;

  // ---- Fees and escrow ----
  const held = m ? m.escrow_total_utia ?? m.escrow_held_utia : 0;
  const queued = m?.withdrawal_queue?.pending;
  const accounts = !!m && (m.escrow_total_utia == null || m.escrow_total_utia === m.escrow_held_utia);
  const z = (v: number) => (v === 0 ? " zero" : "");

  return (
    <section className="pan tp tp-p" aria-label="The period's publishers">
      {/* Activity */}
      <div className="tp-g">
        <Lbl name="Publishers" per={per} />
        <p className={"tp-fig" + (m ? z(m.publishers_active) : "")}>
          {m ? <span title={activeTitle}>{int(m.publishers_active)}<span className="u"> active</span></span> : ph("0 active")}
        </p>
        <hr className="tp-rule" />
        <dl className="tp-rows">
          <div title="Settlements paid in the period: one per blob paid for.">
            <dt>Settlements</dt>
            <dd className={m ? z(m.settlements).trim() || undefined : undefined}>{m ? <>{int(m.settlements)}{spread > 0 && <em>{spreadWords}</em>}</> : ph("0,000")}</dd>
          </div>
          <div title="Namespaces the period's settlements used, of every namespace on record.">
            <dt>Namespaces</dt>
            <dd className={m && !m.namespaces ? "zero" : undefined}>{m ? (m.namespaces != null ? <>{int(m.namespaces)}{m.namespaces_total != null && <span className="of"> of {int(m.namespaces_total)}</span>}</> : "—") : ph("0 of 00")}</dd>
          </div>
        </dl>
      </div>

      {/* Blob size, and how much of it the largest publisher posted */}
      <div className="tp-g">
        <Lbl name="Blob size" per={per} />
        <p className={"tp-fig" + (m ? z(m.bytes) : "")}>
          {m ? <span title="Summed over settlements: a blob settled twice counts twice.">{unit(bytes(m.bytes))}</span> : ph("000.00 GiB")}
        </p>
        <hr className="tp-rule" />
        {quiet
          ? <div className="tp-share">
            <p className="tp-last">{newest
              ? <>Last blob settled {now ? <span className="age" title={dayTime(newest.last_settlement_at!)}>{age(now - newestAt)} ago</span> : <span className="wait">0 d 00 h ago</span>}</>
              : all ? "No blob on record" : <span className="wait">Last blob settled 0 d 00 h ago</span>}</p>
            <div className="tp-names">
              <span className="ld">{newest && <Name p={newest} />}</span>
              {next && <ShowPeriod to={next} onWin={onWin} />}
            </div>
          </div>
          : <div className="tp-share">
            {m && lead && share != null
              ? <div className="tp-bar" role="img"
                aria-label={`${leadName} ${fmtShare(share).replace("%", " percent")}${others > 0 && restShare != null ? `; ${restWho} ${fmtShare(restShare).replace("%", " percent")}` : ""}`}>
                <i className="lead" title={`${lead.label || shortAddr(lead.publisher)} posted ${bytes(lead.bytes)} of ${bytes(m.bytes)} in the period.`} />
                {share < 1 && others > 0 && <i className="rest" style={{ "--r": restShare } as CSSProperties} title={`${bytes(restBytes)} from ${restWho}.`} />}
              </div>
              : <div className={"tp-bar" + (m ? " none" : " wait")} />}
            <div className="tp-names">
              {m && lead && share != null
                ? <>
                  <span className="ld"><Name p={lead} /><b className="sh">{fmtShare(share)}</b></span>
                  {others > 0 && restShare != null && (other
                    ? <span className="oth one"><Name p={other} short /><b>{fmtShare(other.bytes_share ?? restShare)}</b></span>
                    : <span className="oth">{int(others)} others<b>{fmtShare(restShare)}</b></span>)}
                </>
                : <span className="ld">{m ? "" : ph("celestia ••• 0000 00.00%")}</span>}
            </div>
          </div>}
      </div>

      {/* Fees paid, then the escrow: deposited in the period, held now */}
      <div className="tp-g">
        <Lbl name="Fees paid" per={per} />
        <p className={"tp-fig" + (m ? z(m.fees_settled_utia) : "")}>
          {m
            ? <>
              <span title={m.paid_per_mib_utia != null ? `${tia(m.paid_per_mib_utia)} per MiB` : undefined}>{unit(tia(m.fees_settled_utia))}</span>
              {m.timeouts > 0 && <Warn tone="fault" text={`${plural(m.timeouts, "payment promise")} timed out in the period; ${tia(m.timed_out_utia)} charged all the same`} />}
            </>
            : ph("00,000 TIA")}
        </p>
        <hr className="tp-rule" />
        <dl className="tp-rows">
          <div title={m ? [`${plural(m.deposits.count, "deposit")} into escrow in the period`, m.withdrawals_executed.utia > 0 ? `${tia(m.withdrawals_executed.utia)} withdrawn (${plural(m.withdrawals_executed.count, "withdrawal")} paid out)` : ""].filter(Boolean).join("; ") : undefined}>
            <dt>Deposited <span className="per">· {per}</span></dt>
            <dd className={m && m.deposits.utia === 0 ? "zero" : undefined}>{m ? <>{unit(tia(m.deposits.utia))}{m.deposits.count > 0 && <em>{plural(m.deposits.count, "deposit")}</em>}</> : ph("00,000 TIA")}</dd>
          </div>
          {/* on hover: any queued withdrawal, and when the chain was read */}
          <div title={m ? [
            queued && queued.count > 0 ? `${tia(queued.utia)} queued to withdraw${queued.next_available_at ? `, the next payable from ${utcWord(queued.next_available_at)}` : ""}` : "",
            m.escrow_total_at ? `Read at ${utcWord(m.escrow_total_at)}` : "",
          ].filter(Boolean).join("; ") || undefined : undefined}>
            <dt>Escrow held <span className="per">· now</span></dt>
            <dd>{m ? <>{unit(tia(held))}{accounts && <em>{plural(m.escrow_accounts, "account")}</em>}</> : ph("00,000 TIA")}</dd>
          </div>
        </dl>
      </div>
    </section>
  );
}
