"use client";
import { Suspense } from "react";
import Link from "next/link";
import { useApi, type Meta, fmtShare, bytes, utc, ago, tia, int, shortBech, publisherName } from "@/lib/api";
import { Panel } from "@/components/Panel";
import Chart, { calendar, CATEGORICAL, OTHER_COLOR, type Row, type Series } from "@/components/Chart";
import Info from "@/components/Info";
import { WithdrawalQueueCells, pendingLine } from "@/components/Withdrawals";
import type { MarketWithQueue, PublisherWithQueue } from "@/lib/withdrawals";
import { useWindow, WindowSwitch } from "@/lib/window";
import PreLive, { notLiveOf } from "@/components/PreLive";
import { unit } from "@/components/Unit";
import { Metric, Metrics } from "@/components/Metrics";
import Pager, { usePage } from "@/components/Pager";

/** rows per page of the publisher list */
const SIZE = 25;

/**
 * The publisher side of Fibre: who pays for storage, what it costs, and
 * which promises were abandoned. Nothing on this page was measured by this
 * observer. Every figure is a count of something the chain recorded, and the
 * notes at the bottom say which ones are floors.
 */
function Page() {
  // 7d, not the site's 24h: the page's two charts are per UTC day, and a
  // 24h window draws one or two bars, which is not a chart.
  const [win, setWin] = useWindow("7d");
  const [page, setPage] = usePage();
  const { data: meta } = useApi<Meta>("/v1/meta");
  // Before activation every market figure is a zero of a module that does not exist yet: one line says so, the tiles show a dash.
  const pre = notLiveOf(meta);
  const { data: m, error } = useApi<MarketWithQueue>(`/v1/market?window=${win}`);
  const { data: list } = useApi<{ publishers: PublisherWithQueue[] }>(`/v1/publishers?window=${win}`);
  const pubs = list?.publishers ?? [];
  const shown = pubs.slice((page - 1) * SIZE, page * SIZE);
  const queued = m?.withdrawal_queue?.pending;

  return (
    <>
      <div className="section-head">
        <div><h1>Publishers</h1><p className="sub">Accounts that publish blobs through Fibre and pay for them from escrow.</p></div>
        {m && (
          <Info label="About these figures">
            <p>Everything on this page is a count of something the chain recorded; none of it was measured by this observer.</p>
            <ul>{m.notes.map((n) => <li key={n}>{n}</li>)}</ul>
            <p className="mono">fee = ({m.price_formula.base_gas.toLocaleString("en-US")} + {m.price_formula.gas_per_chunk.toLocaleString("en-US")} × ⌈size / {bytes(m.price_formula.chunk_bytes)}⌉) gas × {m.price_formula.utia_per_gas} utia</p>
          </Info>
        )}
        <span className="spacer" />
        <WindowSwitch value={win} onChange={setWin} />
      </div>
      <PreLive meta={meta} />

      {error && <div className="note hold"><span className="label">Observer</span><p>Cannot reach the observer API: {error}. Nothing below is current.</p></div>}

      <section className="group" id="summary">
        <Metrics>
          <Metric label="Fees" value={pre || !m ? "—" : tia(m.fees_settled_utia)} tone={pre || !m ? "absent" : undefined}
            help={pre || !m ? " " : `${int(m.settlements)} settlement${m.settlements === 1 ? "" : "s"} · ${m.timeouts > 0 ? `${int(m.timeouts)} timed out` : "none timed out"}`}
            title="Paid from the publishers' escrow for the blobs settled in the period; not the settlement transaction's own fee." />
          <Metric label="Bytes published" value={pre || !m ? "—" : bytes(m.bytes)} tone={pre || !m ? "absent" : undefined}
            help={pre || !m ? " " : m.paid_per_mib_utia != null ? `${tia(m.paid_per_mib_utia)} per MiB` : "nothing settled"}
            title="The padded blob size publishers paid for, without parity." />
          <Metric label="Publishers" value={pre || !m ? "—" : int(m.publishers_active)} tone={pre || !m ? "absent" : undefined}
            help={pre || !m ? " " : `with a blob in the period · ${int(m.escrow_accounts)} escrow account${m.escrow_accounts === 1 ? "" : "s"}`}
            title="Escrow owners whose blobs settled in the period." />
          <Metric label="Escrow held" value={pre || !m ? "—" : tia(m.escrow_total_utia ?? m.escrow_held_utia)} tone={pre || !m ? "absent" : undefined}
            help={pre || !m ? " " : queued && queued.count > 0 ? `${tia(queued.utia)} queued to withdraw` : "nothing queued to withdraw"}
            title="Every escrow on the chain, read from the x/fibre module account." />
        </Metrics>
      </section>

      {/* per-day charts need more than one day to say anything: 7d and longer */}
      {m && win !== "24h" && (() => {
        const days = calendar(m.window.start.startsWith("0001-") || m.window.name === "all"
          ? new Date((m.daily[0]?.day ?? m.window.end.slice(0, 10)) + "T00:00:00Z") : new Date(m.window.start), new Date(m.window.end));
        const byDay = new Map(m.daily.map((d) => [d.day, d]));
        const feeRows: Row[] = days.map((d) => {
          const b = byDay.get(d);
          return { x: d, label: d.slice(5), values: { fees: b?.fees_utia ?? 0 },
            note: b ? `${b.settlements} settlement${b.settlements === 1 ? "" : "s"} · ${bytes(b.bytes)}${b.timeouts ? ` · ${b.timeouts} timed out` : ""}` : "nothing settled" };
        });
        const pubs = m.top_publishers.map((p) => p.publisher);
        const series: Series[] = pubs.map((p, i) => ({ key: p, label: publisherName(m.top_publishers[i]), color: CATEGORICAL[i] }));
        if (m.other_publishers) series.push({ key: "", label: `${m.other_publishers.publishers} other`, color: OTHER_COLOR });
        const byteRows: Row[] = days.map((d) => {
          const values: Record<string, number> = {};
          for (const r of m.daily_by_publisher) if (r.day === d) values[r.publisher] = (values[r.publisher] ?? 0) + r.bytes;
          const b = byDay.get(d);
          for (const k of Object.keys(values)) values[k] = values[k] / (1 << 20); // MiB, so the axis steps are round
          return { x: d, label: d.slice(5), values, note: b ? `${b.settlements} settlement${b.settlements === 1 ? "" : "s"}` : "nothing settled" };
        });
        const mib = (v: number) => v >= 1024 ? `${(v / 1024).toFixed(2)} GiB` : v >= 100 ? `${Math.round(v)} MiB` : v >= 10 ? `${v.toFixed(1)} MiB` : `${v.toFixed(2)} MiB`;
        const axisTia = (v: number) => v === 0 ? "0" : v >= 100e6 ? Math.round(v / 1e6).toLocaleString("en-US") : v >= 1e6 ? (v / 1e6).toFixed(v % 1e6 ? 1 : 0) : (v / 1e6).toFixed(2);
        const axisMib = (v: number) => v >= 1024 ? `${(v / 1024).toFixed(v % 1024 ? 1 : 0)} GiB` : `${Number.isInteger(v) ? v : v.toFixed(1)} MiB`;
        return (
          <div className="charts">
            <div className="card">
              <Chart title="Fees per day (TIA)" series={[{ key: "fees", label: "fees", color: "var(--accent)" }]} rows={feeRows}
                fmt={(v) => tia(v)} fmtAxis={axisTia} />
            </div>
            <div className="card">
              <Chart title="Bytes published per day, by publisher" series={series} rows={byteRows} fmt={mib} fmtAxis={axisMib} />
            </div>
          </div>
        );
      })()}

      <Panel title="Publishers" right="by fees">
      <div className="tablewrap">
        <table>
          <thead><tr>
            <th>publisher</th>
            <th className="right">blobs</th>
            <th className="right">bytes</th>
            <th className="right">of bytes</th>
            <th className="right">fees</th>
            <th className="right">of fees</th>
            <th className="right">per MiB</th>
            <th className="right">timed out</th>
            <th className="right">escrow</th>
            <th className="right">queued out</th>
            <th>first seen</th>
            <th>last seen</th>
          </tr></thead>
          <tbody>
            {pubs.length === 0 && <tr><td colSpan={12} className="muted">{list ? "No escrow movement recorded in this window." : "Loading…"}</td></tr>}
            {shown.map((p) => (
              <tr key={p.publisher}>
                <td className="mono">
                  <Link href={`/publisher/?addr=${p.publisher}`} title={p.publisher}>{p.label ? <span className="sans">{p.label}</span> : shortBech(p.publisher)}</Link>
                  {p.label && <span className="faint"> {shortBech(p.publisher)}</span>}
                </td>
                <td className="right mono">{p.settlements.toLocaleString("en-US")}</td>
                <td className="right mono">{unit(bytes(p.bytes))}</td>
                <td className="right mono faint">{fmtShare(p.bytes_share)}</td>
                <td className="right mono">{unit(tia(p.fees_utia))}</td>
                <td className="right mono faint">{fmtShare(p.fees_share)}</td>
                <td className="right mono">{p.paid_per_mib_utia != null ? unit(tia(p.paid_per_mib_utia)) : "—"}</td>
                <td className={"right mono" + (p.timeouts > 0 ? " err" : " faint")} title={p.timeouts > 0 ? `${tia(p.timed_out_utia)} charged on abandoned promises` : "no timeout reported"}>{p.timeouts > 0 ? p.timeouts : "—"}</td>
                <td className="right mono" title={p.escrow ? (p.escrow.found ? `available ${tia(p.escrow.available_utia)} · read at height ${p.escrow.height.toLocaleString("en-US")}, ${ago(p.escrow.updated_at)}` : "no escrow account on chain") : "not polled yet"}>
                  {p.escrow ? (p.escrow.found ? unit(tia(p.escrow.balance_utia)) : <span className="faint">none</span>) : <span className="faint">—</span>}
                </td>
                <td className={"right mono" + (p.pending_withdrawals?.count ? "" : " faint")} title={pendingLine(p.pending_withdrawals)}>{p.pending_withdrawals ? (p.pending_withdrawals.count ? unit(tia(p.pending_withdrawals.utia)) : "none") : "—"}</td>
                <td className="mono faint" title={utc(p.first_seen_at)}>{ago(p.first_seen_at)}</td>
                <td className="mono faint" title={utc(p.last_seen_at)}>{ago(p.last_seen_at)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      {list && <Pager total={pubs.length} page={page} size={SIZE} onPage={setPage} noun={pubs.length === 1 ? "publisher" : "publishers"} />}
      </Panel>

      {/* The withdrawal queue, read from chain state rather than rebuilt from
          events (see WithdrawalQueueCells). Absent until the collector has
          read it, and on a pinned window. */}
      {!pre && m?.withdrawal_queue && <WithdrawalQueueCells q={m.withdrawal_queue} win={win} />}
    </>
  );
}

export default function PublishersPage() {
  return <Suspense fallback={<p className="muted">Loading…</p>}><Page /></Suspense>;
}
