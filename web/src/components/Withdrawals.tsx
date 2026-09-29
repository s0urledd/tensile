"use client";
import { ago, span, tia, utc } from "@/lib/api";
import { OUTCOME, type PendingQueue, type PublisherWithdrawals, type WithdrawalRow, type WithdrawalQueue as WithdrawalQueueT } from "@/lib/withdrawals";
import { Panel, Cell } from "./Panel";
import { withPeriod } from "@/lib/window";

/**
 * "12.5 TIA in 2 withdrawals · next payable in 3 h" — one account's queue in
 * one line, or the reason there is none to show. null means the queue has
 * never been read for this account, which is not the same as empty.
 */
export function pendingLine(p: PendingQueue | null | undefined): string {
  if (!p) return "not read yet";
  if (p.count === 0) return "none queued";
  const n = `${tia(p.utia)} in ${p.count} withdrawal${p.count === 1 ? "" : "s"}`;
  return p.next_available_at ? `${n} · next payable ${ago(p.next_available_at)}` : n;
}

function delay(s: number | undefined): string {
  return s == null ? "—" : span(s);
}

/**
 * The withdrawal queue of one publisher, as x/fibre state holds it: what is
 * queued now with the moment each becomes payable, and what left the queue
 * and how. Read from the chain's Withdrawals query, because a settlement
 * that finds the available balance short shrinks or deletes queued
 * withdrawals without any event.
 */
export function WithdrawalQueue({ w }: { w: PublisherWithdrawals | null }) {
  if (!w) {
    return (
      <Panel title="Withdrawal queue">
        <p className="muted">This account&rsquo;s queue has not been read yet. The collector reads it with the escrow balance, every few minutes, once x/fibre answers.</p>
      </Panel>
    );
  }
  return (
    <Panel title="Withdrawal queue">
      {w.check && !w.check.consistent && (
        <p className="notice">At height {w.check.height.toLocaleString("en-US")} the escrow says {tia(w.check.balance_minus_available_utia)} is locked (balance minus available) but the queue holds {tia(w.check.pending_utia)}. The module keeps these equal; the two reads disagree and neither is corrected here.</p>
      )}
      <div className="tablewrap">
        <table>
          <thead><tr><th>requested (UTC)</th><th>payable from (UTC)</th><th className="right">amount</th><th className="right">taken by settlements</th><th>first seen</th></tr></thead>
          <tbody>
            {w.pending.length === 0 && <tr><td colSpan={5} className="muted">Nothing queued at the last read.</td></tr>}
            {w.pending.map((r) => (
              <tr key={r.requested_at}>
                <td className="mono">{utc(r.requested_at)}</td>
                <td className="mono" title={utc(r.available_at)}>{utc(r.available_at)} <span className="faint">({ago(r.available_at)})</span></td>
                <td className="right mono">{tia(r.amount_utia)}</td>
                <td className={"right mono" + (r.reduced_utia > 0 ? "" : " faint")} title={r.reduced_utia > 0 ? `first seen at ${tia(r.first_amount_utia)}; a settlement that found the available balance short took the rest` : "no reduction seen"}>{r.reduced_utia > 0 ? `−${tia(r.reduced_utia)}` : "—"}</td>
                <td className="mono faint" title={utc(r.first_seen_at)}>h {r.first_seen_height.toLocaleString("en-US")}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      {w.left_queue.length > 0 && (
        <div className="tablewrap">
          <table>
            <thead><tr><th>requested (UTC)</th><th>left the queue</th><th>outcome</th><th className="right">amount</th><th className="right">request → payout</th></tr></thead>
            <tbody>
              {w.left_queue.map((r: WithdrawalRow) => (
                <tr key={r.requested_at}>
                  <td className="mono">{utc(r.requested_at)}</td>
                  <td className="mono faint" title={r.gone_at ? utc(r.gone_at) : undefined}>by h {r.gone_height?.toLocaleString("en-US")}</td>
                  <td title={r.outcome_reason}>{r.outcome ? OUTCOME[r.outcome] : <span className="faint">waiting for the scanner</span>}</td>
                  <td className="right mono">{tia(r.paid_utia ?? r.amount_utia)}</td>
                  <td className="right mono" title={r.payout_lag_s != null ? `${delay(r.payout_lag_s)} after it became payable` : undefined}>{delay(r.payout_delay_s)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </Panel>
  );
}

/**
 * The market's queue in three figures: what is queued now, what was paid out
 * in the window and how long that took from the request, and what
 * settlements consumed before it could be paid. The last is the one the
 * chain never announces.
 */
export function WithdrawalQueueCells({ q, win }: { q: WithdrawalQueueT; win: string }) {
  const p = q.pending;
  const d = q.payout_delay;
  const other = q.unattributed.count + q.unresolved.count;
  return (
    <Panel title="Withdrawal queue">
      <div className="cells three">
        <Cell label="Queued to withdraw"
          value={tia(p.utia, { unit: false })} unit="TIA"
          tone={p.count === 0 ? "absent" : undefined}
          sub={p.count === 0 ? "nothing queued" : `${p.count} withdrawal${p.count === 1 ? "" : "s"} · ${q.publishers} account${q.publishers === 1 ? "" : "s"}${p.next_available_at ? ` · next payable ${ago(p.next_available_at)}` : ""}`}
          detail={p.reduced_utia > 0 ? `Settlements have already taken ${tia(p.reduced_utia)} out of these while they were queued.` : undefined}
          info={<>
            <p>Escrow a publisher has asked to take back, locked until the withdrawal delay in force at the request has passed, then paid out by the chain in the first block after that.</p>
            <p>Read from the chain&rsquo;s own queue, not from events: when a settlement finds the available balance short, the chain takes the difference out of queued withdrawals, oldest first, and announces nothing.</p>
          </>} />
        <Cell label={withPeriod("Paid out", win)}
          value={q.executed.count.toLocaleString("en-US")}
          tone={q.executed.count === 0 ? "absent" : undefined}
          sub={d.median_s != null ? `median ${span(d.median_s)} request → payout` : "none"}
          detail={d.count > 0 && d.min_s != null && d.max_s != null ? `${tia(q.executed.utia)} over ${d.count} payout${d.count === 1 ? "" : "s"}; request → payout from ${span(d.min_s)} to ${span(d.max_s)}${d.median_lag_s != null ? `, a median ${span(d.median_lag_s)} after becoming payable` : ""}.` : undefined}
          info={<p>Withdrawals that left the queue in the window and are matched to exactly one payout of their last-seen amount. The delay is from the request block to the payout block; only matched withdrawals have one.</p>} />
        <Cell label={withPeriod("Consumed by settlements", win)}
          value={q.consumed.count.toLocaleString("en-US")}
          tone={q.consumed.count === 0 ? "absent" : undefined}
          sub={q.consumed.count > 0 ? `${tia(q.consumed.utia)} never paid out` : "none"}
          detail={other > 0 ? `${q.unattributed.count} more left the queue without a payout this observer could match, ${q.unresolved.count} still waiting for the scanner.` : undefined}
          info={<p>Withdrawals that vanished from the queue before they became payable. Only a settlement or timeout that found the available balance short can remove one then, so these are certain; the chain emits no event for it.</p>} />
      </div>
    </Panel>
  );
}
