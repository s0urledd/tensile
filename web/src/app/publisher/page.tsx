"use client";
import { Suspense } from "react";
import { useWindow, WindowSwitch, windowLabel } from "@/lib/window";
import Link from "next/link";
import { useSearchParams } from "next/navigation";
import { useApi, notFound, badRequest, throttled, hhmm, type Payment, type Blob, type Window, type PriceFormula, utc, ago, bytes, tia, shortHex, shortBech, nsDisplay, fmtShare } from "@/lib/api";
import { Panel, Cell } from "@/components/Panel";
import { WithdrawalQueue, pendingLine } from "@/components/Withdrawals";
import type { PublisherWithQueue, PublisherWithdrawals } from "@/lib/withdrawals";

type Detail = {
  window: Window;
  publisher: PublisherWithQueue;
  /** the escrow withdrawal queue as last read from state; null until read */
  withdrawals: PublisherWithdrawals | null;
  windows: { window: Window; settlements: number; bytes: number; fees_utia: number; timeouts: number; paid_per_mib_utia: number | null }[];
  recent_payments: Payment[];
  recent_blobs: Blob[];
  price_formula: PriceFormula;
  notes: string[];
};

const KIND: Record<Payment["kind"], string> = {
  settlement: "settled",
  timeout: "timed out",
  deposit: "deposit",
  withdrawal_request: "withdrawal requested",
  withdrawal_executed: "withdrawal paid",
};

function Page() {
  const addr = useSearchParams().get("addr") ?? "";
  // 7d like the publishers list this page is opened from, so the figures match the row that was clicked.
  const [win, setWin] = useWindow("24h");
  const pub = useApi<Detail>(addr ? `/v1/publishers/${addr}?window=${win}` : null);
  const { data, error, loading } = pub;
  if (!addr) return <p className="notice err">No publisher address given.</p>;
  if (!data && error) return <p className="notice">{
    notFound(pub) ? <>No publisher with this address is on record.</>
    : badRequest(pub) ? <><span className="mono">{addr}</span> is not a publisher account address ({error}).</>
    : throttled(pub) ? <>The observer API is busy ({error}); the page retries every 30 seconds.</>
    : <>The observer API is not answering ({error}); the page retries every 30 seconds.</>}</p>;
  if (loading || !data) return <p className="muted">Loading…</p>;
  const p = data.publisher;
  return (
    <>
      {/* A refresh that failed keeps the last answer on screen; say so, and
          from when, rather than let it pass for the current one. */}
      {error && <p className="notice">{throttled(pub) ? "The observer API is busy" : "The observer API is not answering"} ({error}). Showing the figures received at {pub.fetchedAt ? `${hhmm(pub.fetchedAt)} (${ago(pub.fetchedAt)})` : "the last refresh"}; the page retries every 30 seconds.</p>}
      <div className="section-head">
        <h1 className="mono" style={{ fontSize: "var(--t-h1)" }}>{p.label ?? shortBech(p.publisher)}</h1>
        {p.label && <span className="chip" title={p.label_source ? `label source: ${p.label_source}` : undefined}>{p.label_source ?? "labelled"}</span>}
        <span className="spacer" />
        <WindowSwitch value={win} onChange={setWin} />
      </div>

      <section className="card kvcard">
        <dl className="kv">
          <dt>account</dt><dd className="mono">{p.publisher}</dd>
          <dt>escrow</dt><dd className="mono">{p.escrow ? (p.escrow.found ? <>{tia(p.escrow.balance_utia)} <span className="muted">· {tia(p.escrow.available_utia)} available</span></> : <span className="muted">no escrow account on chain</span>) : <span className="muted">not polled yet</span>}</dd>
          <dt>pending withdrawals</dt><dd className="mono">{pendingLine(p.pending_withdrawals)}</dd>
          <dt>first seen</dt><dd className="mono">{utc(p.first_seen_at)} <span className="muted">({ago(p.first_seen_at)})</span></dd>
          <dt>last seen</dt><dd className="mono">{utc(p.last_seen_at)} <span className="muted">({ago(p.last_seen_at)})</span></dd>
          {p.label && <dt>label</dt>}{p.label && <dd>{p.label}{p.label_source && <span className="muted"> · {p.label_source}</span>}</dd>}
        </dl>
      </section>

      <Panel title="Activity" right={`${windowLabel(win)} window`}>
      <div className="cells four">
        <Cell label="Fees settled" value={tia(p.fees_utia, { unit: false })} unit="TIA"
          tone={p.settlements === 0 ? "absent" : undefined}
          sub={`${p.settlements.toLocaleString("en-US")} blob${p.settlements === 1 ? "" : "s"} · ${fmtShare(p.fees_share)} of the window`} />
        <Cell label="Bytes" value={bytes(p.bytes)} sub={`${fmtShare(p.bytes_share)} of the window`} />
        <Cell label="Paid per MiB" value={p.paid_per_mib_utia != null ? tia(p.paid_per_mib_utia, { unit: false }) : "—"} unit={p.paid_per_mib_utia != null ? "TIA" : undefined}
          tone={p.paid_per_mib_utia == null ? "absent" : undefined}
          sub={p.avg_blob_bytes != null ? `avg blob ${bytes(p.avg_blob_bytes)} · largest ${bytes(p.largest_blob_bytes)}` : "nothing settled"} />
        <Cell label="Timed out" value={p.timeouts > 0 ? p.timeouts : "none"} tone={p.timeouts > 0 ? "fault" : "absent"}
          sub={p.timeouts > 0 ? `${tia(p.timed_out_utia)} charged` : "none reported"}
          detail={p.timeouts > 0 ? `${tia(p.timed_out_utia)} charged on promises this publisher abandoned.` : "No timeout reported. A floor, not a total: a promise nobody reports leaves no trace on chain."} />
      </div>
      </Panel>

      <Panel title="By window">
      <div className="tablewrap">
        <table>
          <thead><tr><th>window</th><th className="right">blobs</th><th className="right">bytes</th><th className="right">fees</th><th className="right">per MiB</th><th className="right">timed out</th></tr></thead>
          <tbody>
            {data.windows.map((w) => (
              <tr key={w.window.name}>
                <td className="mono">{windowLabel(w.window.name)}</td>
                <td className="right mono">{w.settlements.toLocaleString("en-US")}</td>
                <td className="right mono">{bytes(w.bytes)}</td>
                <td className="right mono">{tia(w.fees_utia)}</td>
                <td className="right mono">{w.paid_per_mib_utia != null ? tia(w.paid_per_mib_utia) : "—"}</td>
                <td className={"right mono" + (w.timeouts > 0 ? " err" : " faint")}>{w.timeouts > 0 ? w.timeouts : "—"}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      </Panel>

      <WithdrawalQueue w={data.withdrawals ?? null} />

      <Panel title="Escrow movements" right={`${data.recent_payments.length} most recent`}>
      <div className="tablewrap">
        <table>
          <thead><tr><th>kind</th><th>time (UTC)</th><th className="right">height</th><th className="right">amount</th><th>promise</th><th className="right">size</th><th>by</th></tr></thead>
          <tbody>
            {data.recent_payments.length === 0 && <tr><td colSpan={7} className="muted">No escrow movement recorded.</td></tr>}
            {data.recent_payments.map((x, i) => (
              <tr key={`${x.height}-${x.tx_hash ?? ""}-${i}`}>
                <td className={x.kind === "timeout" ? "err" : ""}>{KIND[x.kind] ?? x.kind}</td>
                <td className="mono" title={ago(x.time)}>{utc(x.time)}</td>
                <td className="right mono">{x.height.toLocaleString("en-US")}</td>
                <td className="right mono">{x.kind === "deposit" || x.kind === "withdrawal_executed" ? "+" : "−"}{tia(x.amount_utia)}</td>
                <td className="mono">{x.promise_hash ? <Link href={`/blob/?hash=${x.promise_hash}`}>{shortHex(x.promise_hash, 6)}</Link> : <span className="faint">—</span>}{x.namespace && <span className="faint"> {nsDisplay(x.namespace)}</span>}</td>
                <td className="right mono">{x.blob_size ? bytes(x.blob_size) : <span className="faint">—</span>}</td>
                <td className="mono faint" title={x.processor}>{x.processor && x.processor !== x.publisher ? shortHex(x.processor, 6) : x.processor ? "self" : "chain"}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      </Panel>

      {data.recent_blobs.length > 0 && (
        <Panel title="Recent blobs" right={<>{data.recent_blobs.length} most recent · <Link href={`/blobs/`}>all blobs →</Link></>}>
        <div className="tablewrap">
          <table>
            <thead><tr><th>blob</th><th>settled (UTC)</th><th>namespace</th><th className="right">upload size</th><th className="right">fee paid</th><th className="right">validators</th></tr></thead>
            <tbody>
              {data.recent_blobs.map((b) => (
                <tr key={b.promise_hash}>
                  <td className="mono"><Link href={`/blob/?hash=${b.promise_hash}`}>{shortHex(b.promise_hash, 6)}</Link></td>
                  <td className="mono" title={ago(b.settlement_time)}>{utc(b.settlement_time)}</td>
                  <td className="mono" title={b.namespace}>{nsDisplay(b.namespace)}</td>
                  <td className="right mono">{bytes(b.blob_size)}</td>
                  <td className="right mono">{b.charge ? tia(b.charge.fee_utia) : <span className="faint">—</span>}</td>
                  <td className="right mono">{b.validators_with_rows}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        </Panel>
      )}

    </>
  );
}

export default function PublisherPage() {
  return <Suspense fallback={<p className="muted">Loading…</p>}><Page /></Suspense>;
}
