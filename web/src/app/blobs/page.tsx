"use client";
import { Suspense, useState } from "react";
import Link from "next/link";
import { useSearchParams } from "next/navigation";
import PreLive from "@/components/PreLive";
import { unit } from "@/components/Unit";
import { useApi, type Meta, type Market, type Blob, type NamespaceRow, utc, ago, nsDisplay, bytes, int, pctOf, tia, shortBech } from "@/lib/api";
import { Mark } from "@/components/Verdict";
import { recon } from "@/lib/status";
import Chart from "@/components/Chart";
import { Metric, Figures } from "@/components/Metrics";
import { buckets } from "@/lib/buckets";
import Pager, { usePage } from "@/components/Pager";
import { useWindow, WindowSwitch, periodName, withPeriod } from "@/lib/window";

/** rows per page of the blob list */
const SIZE = 25;

type BlobPage = { blobs: Blob[]; total: number; offset: number; truncated: boolean; namespace?: string };

/** the last page /v1/blobs serves: its offset stops at 100,000 */
const MAX_PAGE = Math.floor(100000 / SIZE) + 1;

function Page() {
  const nsParam = useSearchParams().get("namespace") ?? "";
  const [ns, setNsRaw] = useState(nsParam);
  const [win, setWin] = useWindow("24h");
  const [page, setPage] = usePage();
  const [tab, setTab] = useState<"blobs" | "namespaces">("blobs");
  // a new filter starts from the first page, and lives in the address so a link keeps it
  const setNs = (v: string) => {
    setNsRaw(v);
    setPage(1);
    try {
      const u = new URL(window.location.href);
      if (v.trim()) u.searchParams.set("namespace", v.trim()); else u.searchParams.delete("namespace");
      window.history.replaceState(null, "", u.pathname + u.search + u.hash);
    } catch { /* fine */ }
  };
  const nsq = ns.trim() ? `&namespace=${encodeURIComponent(ns.trim())}` : "";
  const offset = (Math.min(page, MAX_PAGE) - 1) * SIZE;
  const { data, error, loading } = useApi<BlobPage>(`/v1/blobs?limit=${SIZE}&offset=${offset}${nsq}`);
  // the rows of the page asked for, not the last one received while the next loads
  const rows = data && data.offset === offset && (data.namespace ?? "") === ns.trim().toLowerCase() ? data.blobs : null;
  const { data: meta } = useApi<Meta>("/v1/meta");
  const nss = useApi<{ namespaces: NamespaceRow[] }>("/v1/namespaces?limit=100");
  const { data: m } = useApi<Market>(`/v1/market?window=${win}`);
  // the answer for another period, kept while this one loads, is not this chart
  const series = m && m.window.name === win ? buckets(m, win) : [];
  const per = win === "24h" ? "hour" : "day";
  // Figures and charts name their period in the title. A figure keeps the last
  // answer while another period loads, so it names that answer's period; a
  // chart draws only the period selected, so it names that one.
  const period = periodName(m?.window?.name ?? win);
  const mib = (v: number) => (v >= 1024 ? `${(v / 1024).toFixed(2)} GiB` : v >= 10 ? `${Math.round(v)} MiB` : `${v.toFixed(2)} MiB`);
  const axisMib = (v: number) => (v >= 1024 ? `${(v / 1024).toFixed(v % 1024 ? 1 : 0)} GiB` : `${Number.isInteger(v) ? v : v.toFixed(1)} MiB`);
  const nsN = nss.data?.namespaces.length ?? 0;
  const nsMore = !!(nss.data as { truncated?: boolean } | null)?.truncated;

  return (
    <>
      <div className="page-head">
        <div><h1>Blobs</h1><p className="lede">Blobs published through Fibre and settled on chain.</p></div>
      </div>
      <PreLive meta={meta} />
      {error && !data && <p className="notice">The observer API is not answering ({error}); the page retries every 30 seconds. This is an observer outage, not a Fibre network outage.</p>}
      {error && data && <p className="sample">Showing the last list received; the API is not answering right now ({error}).</p>}

      <section className="group" id="chain">
        <div className="sec-head">
          <div><h2>On chain</h2><p className="sub">The period&rsquo;s settlements.</p></div>
          <WindowSwitch value={win} onChange={setWin} />
        </div>
        <div className="board board--rail">
          <Figures className="rail">
            <Metric size="hero" label="Blobs" period={period} value={m ? int(m.blobs) : "—"} tone={m && m.blobs > 0 ? undefined : "absent"}
              help={m ? `${int(m.settlements)} settlement${m.settlements === 1 ? "" : "s"}` : " "}
              title="Blobs (BlobID) settled in the period, and the settlements that paid for them." />
            <Metric label="Namespaces" period={period} value={m?.namespaces != null ? int(m.namespaces) : "—"} tone={m?.namespaces ? undefined : "absent"}
              help={m?.namespaces_total != null ? `${int(m.namespaces_total)} on record` : " "}
              title="Namespaces the period's settlements used." />
          </Figures>
          <div className="board-charts">
            <Chart title={withPeriod(`Upload size per ${per}`, win)} figure={m && series.length ? bytes(m.bytes) : undefined}
              series={[{ key: "bytes", label: "upload size", color: "var(--accent)" }]}
              rows={series.map((c) => ({ x: c.title, label: c.label, short: c.short, values: { bytes: c.bytes / (1 << 20) }, note: `${int(c.settlements)} settlement${c.settlements === 1 ? "" : "s"}` }))}
              fmt={mib} fmtAxis={axisMib} empty={m && m.window.name === win ? "nothing settled" : "loading…"} />
            <Chart title={withPeriod(`Settlements per ${per}`, win)} figure={m && series.length ? int(m.settlements) : undefined}
              series={[{ key: "n", label: "settlements", color: "var(--accent-2)" }]}
              rows={series.map((c) => ({ x: c.title, label: c.label, short: c.short, values: { n: c.settlements }, note: bytes(c.bytes) }))}
              fmt={(v) => int(v)} empty={m && m.window.name === win ? "nothing settled" : "loading…"} />
          </div>
        </div>
      </section>

      <section id="list" className="listing">
        <div className="list-head">
          <div className="tabs" role="group" aria-label="list">
            <button type="button" aria-pressed={tab === "blobs"} onClick={() => setTab("blobs")}>Blobs</button>
            <button type="button" aria-pressed={tab === "namespaces"} onClick={() => setTab("namespaces")}>Namespaces{nsN ? <span className="n"> {int(nsN)}{nsMore ? "+" : ""}</span> : null}</button>
          </div>
          {tab === "blobs" && (
            <label className="field">
              <svg viewBox="0 0 16 16" width="15" height="15" aria-hidden="true"><circle cx="7" cy="7" r="4.75" fill="none" stroke="currentColor" strokeWidth="1.5" /><path d="m10.5 10.5 3.5 3.5" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" /></svg>
              <input className="nsfilter" type="search" placeholder="Filter by namespace (58 hex)" value={ns} onChange={(e) => setNs(e.target.value)} aria-label="namespace filter" />
            </label>
          )}
        </div>

        {tab === "blobs" && (
          <>
            <div className="tablewrap framed">
              <table className="bt blist">
                <thead><tr><th>Blob</th><th>Height</th><th>Settled (UTC)</th><th>Namespace</th><th>Publisher</th><th>Upload size</th><th>Fee paid</th>
                  <th title="Share of voting power whose signature on the settlement verified. A settlement needs ⅔.">Endorsed</th><th>Status</th></tr></thead>
                <tbody>
                  {!rows && (loading || !!data) && <tr><td colSpan={9} className="muted">Loading…</td></tr>}
                  {rows && rows.length === 0 && <tr><td colSpan={9} className="muted">No blob recorded{ns.trim() ? " in this namespace" : ""}.</td></tr>}
                  {rows?.map((b) => {
                    const rc = recon(b);
                    const pub = b.publisher || b.charge?.publisher || b.signer;
                    return (
                      <tr key={b.promise_hash}>
                        <td className="mono" title={b.promise_hash}><Link href={`/blob/?hash=${b.promise_hash}`}>{b.promise_hash.slice(0, 10)}…</Link></td>
                        <td className="mono">{int(b.settlement_height)}</td>
                        <td className="mono" title={`${utc(b.settlement_time)} · ${ago(b.settlement_time)}`}>{utc(b.settlement_time).slice(5, 16)}</td>
                        <td title={b.namespace}><button className="rowlink" onClick={() => setNs(b.namespace)} title="show only this namespace">{nsDisplay(b.namespace)}</button></td>
                        <td title={pub}><Link className="mono" href={`/publisher/?addr=${pub}`}>{shortBech(pub)}</Link></td>
                        <td className="mono">{unit(bytes(b.blob_size))}</td>
                        <td className="mono">{b.charge ? unit(tia(b.charge.fee_utia)) : "—"}</td>
                        <td className="mono">{b.attested_voting_power != null && b.total_voting_power ? pctOf(b.attested_voting_power, b.total_voting_power) : "—"}</td>
                        <td><span className={`verdict verdict--${rc.tier}`} title={rc.title}><Mark tier={rc.tier} /><span className="w">{rc.word}</span></span></td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
            {data && <Pager total={data.total} page={page} size={SIZE} maxPages={MAX_PAGE} onPage={setPage} noun={data.total === 1 ? "settlement on record" : "settlements on record"} />}
          </>
        )}

        {tab === "namespaces" && (
          <div className="tablewrap framed">
            <table className="bt nss">
              <thead><tr><th>Namespace</th><th>Upload size</th><th>Settlements</th><th>Last 24h</th><th>Publishers</th><th>First seen</th><th>Last blob</th></tr></thead>
              <tbody>
                {!nss.data && <tr><td colSpan={7} className="muted">Loading…</td></tr>}
                {nss.data?.namespaces.map((n) => (
                  <tr key={n.namespace}>
                    <td title={n.namespace}><button className="rowlink mono" onClick={() => { setNs(n.namespace); setTab("blobs"); }}>{nsDisplay(n.namespace)}</button></td>
                    <td>{bytes(n.bytes)}</td>
                    <td>{int(n.blobs)}</td>
                    <td>{n.blobs_24h > 0 ? <>{bytes(n.bytes_24h)} <span className="soft">· {int(n.blobs_24h)}</span></> : "—"}</td>
                    <td>{int(n.accounts)}</td>
                    <td title={utc(n.first_seen)}>{ago(n.first_seen)}</td>
                    <td title={utc(n.last_blob)}>{ago(n.last_blob)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </section>
    </>
  );
}

export default function BlobsPage() {
  return <Suspense fallback={<p className="muted">Loading…</p>}><Page /></Suspense>;
}
