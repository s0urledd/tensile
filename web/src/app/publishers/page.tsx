"use client";
import { Suspense } from "react";
import Link from "next/link";
import { useApi, type Meta, fmtShare, bytes, utc, ago, tia, int, shortBech, publisherName } from "@/lib/api";
import Chart, { calendar, CATEGORICAL, OTHER_COLOR, type Row, type Series } from "@/components/Chart";
import Info from "@/components/Info";
import { WithdrawalQueueCells, pendingLine } from "@/components/Withdrawals";
import type { MarketWithQueue, PublisherWithQueue } from "@/lib/withdrawals";
import { useWindow, WindowSwitch, periodName, withPeriod } from "@/lib/window";
import PreLive, { notLiveOf } from "@/components/PreLive";
import { unit } from "@/components/Unit";
import { Metric, Figures } from "@/components/Metrics";
import Pager, { usePage } from "@/components/Pager";
import { buckets } from "@/lib/buckets";

/** "Sep 21": a chart's UTC day, as the Blobs chart labels it */
const dayLabel = (d: string) => new Date(d + "T00:00:00Z").toLocaleDateString("en-US", { month: "short", day: "numeric", timeZone: "UTC" });

/** rows per page of the publisher list */
const SIZE = 25;

/**
 * The publisher side of Fibre: who pays for storage, what it costs, and
 * which promises were abandoned. Nothing on this page was measured by this
 * observer. Every figure is a count of something the chain recorded, and the
 * notes at the bottom say which ones are floors.
 */
function Page() {
  // 24h, as on the overview and the Blobs page: the charts open on the day by
  // the hour; the longer periods chart by the day.
  const [win, setWinRaw] = useWindow("24h");
  const [page, setPage] = usePage();
  // another period is another list: start from its first page
  const setWin = (w: typeof win) => { setWinRaw(w); setPage(1); };
  const { data: meta } = useApi<Meta>("/v1/meta");
  // Before activation every market figure is a zero of a module that does not exist yet: one line says so, the tiles show a dash.
  const pre = notLiveOf(meta);
  const { data: m, error } = useApi<MarketWithQueue>(`/v1/market?window=${win}`);
  const { data: list } = useApi<{ publishers: PublisherWithQueue[] }>(`/v1/publishers?window=${win}`);
  const pubs = list?.publishers ?? [];
  const shown = pubs.slice((page - 1) * SIZE, page * SIZE);
  // a figure of the period names it in its title: the period of the answer shown
  // (the last one stays while another loads), else the one selected
  const period = periodName(m?.window?.name ?? win);

  return (
    <>
      <div className="page-head">
        <div>
          <div className="h1row">
            <h1>Publishers</h1>
            {m && (
              <Info label="About these figures">
                <p>Every figure on this page is read from the chain; none was measured by Tensile. <Link href="/methodology/#publishers">How each is counted</Link></p>
              </Info>
            )}
          </div>
          <p className="lede">Accounts that publish blobs through Fibre and pay for them from escrow.</p>
        </div>
        <WindowSwitch value={win} onChange={setWin} />
      </div>
      <PreLive meta={meta} />

      {error && <div className="note hold"><span className="label">Observer</span><p>Cannot reach the observer API: {error}. Nothing below is current.</p></div>}

      {/* The escrow side, then who published and which promises timed out; the
          period's fees paid are the total on the fees chart below. */}
      <section className="board board--stack" id="summary">
        <Figures className="row">
          <Metric label="Escrow held" value={pre || !m ? "—" : tia(m.escrow_total_utia ?? m.escrow_held_utia)} tone={pre || !m ? "absent" : undefined}
            help={pre || !m ? " " : `${int(m.escrow_accounts)} escrow account${m.escrow_accounts === 1 ? "" : "s"}`}
            title="Every escrow on the chain, read from the x/fibre module account." />
          <Metric label="Deposits" period={period} value={pre || !m ? "—" : tia(m.deposits.utia)} tone={pre || !m || m.deposits.count === 0 ? "absent" : undefined}
            help={pre || !m ? " " : `${int(m.deposits.count)} deposit${m.deposits.count === 1 ? "" : "s"}`}
            title="TIA paid into escrow accounts in the period." />
          <Metric label="Withdrawals" period={period} value={pre || !m ? "—" : tia(m.withdrawals_requested.utia)} tone={pre || !m || m.withdrawals_requested.count === 0 ? "absent" : undefined}
            help={pre || !m ? " " : `${int(m.withdrawals_requested.count)} requested`}
            title="TIA requested out of escrow in the period. What is still waiting to pay out is under Withdrawal queue." />
          <Metric label="Largest publisher" period={period} value={pre || !m || m.largest_poster?.bytes_share == null ? "—" : fmtShare(m.largest_poster.bytes_share)}
            tone={pre || !m || !m.largest_poster ? "absent" : undefined}
            help={pre || !m ? " " : m.largest_poster ? `of blob size · ${publisherName(m.largest_poster)}` : "nothing settled"}
            title="The publisher with the most blob size in the period, and its share." />
          <Metric label="Publishers" period={period} value={pre || !m ? "—" : int(m.publishers_active)}
            tone={pre || !m || m.publishers_active === 0 ? "absent" : undefined}
            help={pre || !m || m.publishers_active > 0 ? " " : "none"}
            title="Accounts that published blobs in this period." />
          <Metric label="Payment promise timeouts" period={period} value={pre || !m ? "—" : int(m.timeouts)}
            tone={pre || !m ? "absent" : undefined}
            help={pre || !m ? " " : m.timeouts > 0 ? `${tia(m.timed_out_utia)} charged` : "none"}
            title="Payment promises not settled within an hour. The account is charged anyway." />
        </Figures>

      {/* per UTC day; for 24h per UTC hour, as the Blobs charts are (the period of the answer shown decides) */}
      {m && (() => {
        const perHour = m.window.name === "24h";
        // An API from before the hours carried fees and a publisher split sends
        // hours without the split: no chart then, as 24h had none before, rather
        // than empty hours under the period's totals. An hour with a settlement
        // is always in the split, so a period with no hours has nothing to miss.
        if (perHour && m.hourly?.length && !m.hourly_by_publisher) return null;
        const per = perHour ? "hour" : "day";
        // Each bar: its axis names, its bucket (absent when nothing happened in it) and its split by publisher.
        const slots: { x: string; label: string; short?: string; b?: { fees_utia: number; bytes: number; settlements: number; timeouts?: number }; split: { publisher: string; bytes: number }[] }[] = [];
        if (perHour) {
          // every hour of the period, the partial first and last included, named as on the Blobs page
          const byHour = new Map((m.hourly ?? []).map((h) => [h.hour, h]));
          for (const c of buckets(m, "24h")) slots.push({ x: c.title, label: c.label, short: c.short, b: byHour.get(c.key), split: (m.hourly_by_publisher ?? []).filter((r) => r.hour === c.key) });
        } else {
          const days = calendar(m.window.start.startsWith("0001-") || m.window.name === "all"
            ? new Date((m.daily[0]?.day ?? m.window.end.slice(0, 10)) + "T00:00:00Z") : new Date(m.window.start), new Date(m.window.end));
          const byDay = new Map(m.daily.map((d) => [d.day, d]));
          for (const d of days) slots.push({ x: d, label: dayLabel(d), short: String(Number(d.slice(8))), b: byDay.get(d), split: m.daily_by_publisher.filter((r) => r.day === d) });
        }
        const feeRows: Row[] = slots.map(({ x, label, short, b }) => ({ x, label, short, values: { fees: b?.fees_utia ?? 0 },
          note: b ? `${b.settlements} settlement${b.settlements === 1 ? "" : "s"} · ${bytes(b.bytes)}${b.timeouts ? ` · ${b.timeouts} timed out` : ""}` : "nothing settled" }));
        const pubs = m.top_publishers.map((p) => p.publisher);
        const series: Series[] = pubs.map((p, i) => ({ key: p, label: publisherName(m.top_publishers[i]), color: CATEGORICAL[i] }));
        if (m.other_publishers) series.push({ key: "", label: `${m.other_publishers.publishers} other`, color: OTHER_COLOR });
        const byteRows: Row[] = slots.map(({ x, label, short, b, split }) => {
          const values: Record<string, number> = {};
          for (const r of split) values[r.publisher] = (values[r.publisher] ?? 0) + r.bytes;
          for (const k of Object.keys(values)) values[k] = values[k] / (1 << 20); // MiB, so the axis steps are round
          return { x, label, short, values, note: b ? `${b.settlements} settlement${b.settlements === 1 ? "" : "s"}` : "nothing settled" };
        });
        const mib = (v: number) => v >= 1024 ? `${(v / 1024).toFixed(2)} GiB` : v >= 100 ? `${Math.round(v)} MiB` : v >= 10 ? `${v.toFixed(1)} MiB` : `${v.toFixed(2)} MiB`;
        const axisTia = (v: number) => v === 0 ? "0" : v >= 100e6 ? Math.round(v / 1e6).toLocaleString("en-US") : v >= 1e6 ? (v / 1e6).toFixed(v % 1e6 ? 1 : 0) : (v / 1e6).toFixed(2);
        const axisMib = (v: number) => v >= 1024 ? `${(v / 1024).toFixed(v % 1024 ? 1 : 0)} GiB` : `${Number.isInteger(v) ? v : v.toFixed(1)} MiB`;
        return (
          <div className="board-charts">
            <Chart title={withPeriod(`Fees paid per ${per}`, m.window.name)} figure={tia(m.fees_settled_utia)} series={[{ key: "fees", label: "fees", color: "var(--accent)" }]} rows={feeRows}
              fmt={(v) => tia(v)} fmtAxis={axisTia} height={210} />
            <Chart title={withPeriod(`Blob size per ${per}, by publisher`, m.window.name)} figure={bytes(m.bytes)} series={series} rows={byteRows} fmt={mib} fmtAxis={axisMib} height={210} />
          </div>
        );
      })()}
      </section>

      <section className="listing" id="publishers">
      <div className="sec-head sec-head--table">
        <h2>Publishers</h2>
        <span className="sec-note">by fees</span>
      </div>
      <div className="tablewrap framed">
        <table className="pt">
          <thead><tr>
            <th>publisher</th>
            <th className="right">settlements</th>
            <th className="right">blob size</th>
            <th className="right">share</th>
            <th className="right">fees paid</th>
            <th className="right">share</th>
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
      {list && <Pager total={pubs.length} page={page} size={SIZE} onPage={setPage} noun={pubs.length === 1 ? "account" : "accounts"} />}
      </section>

      {/* The withdrawal queue, read from chain state rather than rebuilt from
          events (see WithdrawalQueueCells). Absent until the collector has
          read it, and on a pinned window. */}
      {!pre && m?.withdrawal_queue && <WithdrawalQueueCells q={m.withdrawal_queue} win={m.window.name} />}
    </>
  );
}

export default function PublishersPage() {
  return <Suspense fallback={<p className="muted">Loading…</p>}><Page /></Suspense>;
}
