"use client";
import { Suspense, useState } from "react";
import Link from "next/link";
import { useSearchParams } from "next/navigation";
import PreLive from "@/components/PreLive";
import { unit } from "@/components/Unit";
import { useApi, type Meta, type Market, type Blob, type NamespaceRow, utc, ago, nsDisplay, bytes, int, tia, shortBech } from "@/lib/api";
import { Mark, type Tier } from "@/components/Verdict";
import { Metric, Metrics } from "@/components/Metrics";
import VolumeChart from "@/components/VolumeChart";
import Pager, { usePage } from "@/components/Pager";
import { useWindow, WindowSwitch } from "@/lib/window";

/** rows per page of the blob list */
const SIZE = 25;

type BlobPage = { blobs: Blob[]; total: number; offset: number; truncated: boolean };

// Retrievability as a mark and a word, in the same channel the verdicts use.
// Retrievable: enough rows were retrieved to reconstruct the blob, in the
// words of celestia-app's own client ("some rows were retrieved, but not
// enough to reconstruct" is its word for the other case).
function recon(b: Blob): { word: string; tier: Tier; title: string } {
  const r = b.reconstructable;
  const over = new Date(b.must_serve_until).getTime() <= Date.now();
  if (b.sampled_out && (!r || r.status === "unknown")) {
    return { word: "sampled out", tier: "gap", title: "The load policy of the time drew this blob out of its sample: not read." };
  }
  if (!r || r.status === "unknown" || (r.status === "pending" && over)) {
    return !over && (!r || r.status !== "unknown")
      ? { word: "in retention window", tier: "gap", title: "Read once, 10 minutes before the retention window ends." }
      : { word: "not read by Tensile", tier: "gap", title: "No reading of this blob was completed. Nothing is counted for or against a validator." };
  }
  if (r.status === "pending") {
    return { word: "in retention window", tier: "gap", title: "Read once, 10 minutes before the retention window ends." };
  }
  const rows = `${int(r.served_distinct_rows)} of ${int(r.total_rows)} rows retrieved, ${int(r.needed_rows)} needed to reconstruct`;
  if (r.status === "yes" || r.status === "degraded") return { word: "retrievable", tier: "kept", title: rows };
  return { word: "not retrievable", tier: "hold", title: `${rows}: not enough.` };
}

function Page() {
  const nsParam = useSearchParams().get("namespace") ?? "";
  const [ns, setNsRaw] = useState(nsParam);
  const [win, setWin] = useWindow("7d");
  const [page, setPage] = usePage();
  const [tab, setTab] = useState<"blobs" | "namespaces">("blobs");
  const [metric, setMetric] = useState<"bytes" | "settlements">("bytes");
  // a new filter starts from the first page
  const setNs = (v: string) => { setNsRaw(v); setPage(1); };
  const nsq = ns.trim() ? `&namespace=${encodeURIComponent(ns.trim())}` : "";
  const { data, error, loading } = useApi<BlobPage>(`/v1/blobs?limit=${SIZE}&offset=${(page - 1) * SIZE}${nsq}`);
  const { data: meta } = useApi<Meta>("/v1/meta");
  const nss = useApi<{ namespaces: NamespaceRow[] }>("/v1/namespaces?limit=100");
  const { data: m } = useApi<Market>(`/v1/market?window=${win}`);
  const nsN = nss.data?.namespaces.length ?? 0;

  return (
    <>
      <div className="head">
        <div><h1>Blobs</h1><p className="sub">Blobs published through Fibre and settled on chain.</p></div>
        <WindowSwitch value={win} onChange={setWin} />
      </div>
      <PreLive meta={meta} />
      {error && !data && <p className="notice">The observer API is not answering ({error}); the page retries every 30 seconds. This is an observer outage, not a Fibre network outage.</p>}

      <section className="group" id="summary">
      <Metrics>
        <Metric label="Blobs" value={m ? int(m.blobs) : "—"} tone={m ? undefined : "absent"}
          help={m ? `${int(m.settlements)} settlement${m.settlements === 1 ? "" : "s"} in the period` : " "} title="Distinct blobs (BlobID) settled in the period." />
        <Metric label="Bytes published" value={m ? bytes(m.bytes) : "—"} tone={m ? undefined : "absent"}
          help="padded size, as charged" title="The padded blob size publishers paid for, without parity." />
        <Metric label="Fees" value={m ? tia(m.fees_settled_utia) : "—"} tone={m ? undefined : "absent"}
          help={m?.paid_per_mib_utia != null ? `${tia(m.paid_per_mib_utia)} per MiB` : "nothing settled"}
          title="Paid from the publishers' escrow for these blobs; not the settlement transaction's own fee." />
        <Metric label="Publishers" value={m ? int(m.publishers_active) : "—"} tone={m ? undefined : "absent"}
          help="with a blob in the period" title="Escrow owners whose blobs settled in the period." />
      </Metrics>
      </section>

      <section className="group" id="published">
        <div className="vhead">
          <div><h2>{metric === "bytes" ? "Bytes published" : "Settlements"}</h2><p className="sub">Per UTC {win === "24h" ? "hour" : "day"}</p></div>
          <div className="seg" role="group" aria-label="chart">
            <button type="button" aria-pressed={metric === "bytes"} onClick={() => setMetric("bytes")}>Bytes</button>
            <button type="button" aria-pressed={metric === "settlements"} onClick={() => setMetric("settlements")}>Settlements</button>
          </div>
        </div>
        <VolumeChart market={m ?? null} win={win} metric={metric} />
      </section>

      <section id="list">
        <div className="vhead">
          <div className="seg" role="group" aria-label="list">
            <button type="button" aria-pressed={tab === "blobs"} onClick={() => setTab("blobs")}>Blobs{data ? <span className="n"> {int(data.total)}</span> : null}</button>
            <button type="button" aria-pressed={tab === "namespaces"} onClick={() => setTab("namespaces")}>Namespaces{nsN ? <span className="n"> {int(nsN)}</span> : null}</button>
          </div>
          <div className="tools">
            {tab === "blobs" && <input className="nsfilter" type="search" placeholder="Filter by namespace (56 hex)" value={ns} onChange={(e) => setNs(e.target.value)} aria-label="namespace filter" />}
          </div>
        </div>

        {tab === "blobs" && (
          <>
            <div className="tablewrap framed">
              <table className="bt blist">
                <thead><tr><th>Blob</th><th>Height</th><th>Settled (UTC)</th><th>Namespace</th><th>Publisher</th><th>Size</th><th>Fee</th>
                  <th title="Share of voting power whose signature on the settlement verified. A settlement needs ⅔.">Endorsed</th><th>Status</th></tr></thead>
                <tbody>
                  {loading && !data && <tr><td colSpan={9} className="muted">Loading…</td></tr>}
                  {data && data.blobs.length === 0 && <tr><td colSpan={9} className="muted">No blob recorded{ns.trim() ? " in this namespace" : ""}.</td></tr>}
                  {data?.blobs.map((b) => {
                    const rc = recon(b);
                    const pub = b.charge?.publisher || b.signer;
                    return (
                      <tr key={b.promise_hash}>
                        <td className="mono" title={b.promise_hash}><Link href={`/blob/?hash=${b.promise_hash}`}>{b.promise_hash.slice(0, 10)}…</Link></td>
                        <td className="mono">{int(b.settlement_height)}</td>
                        <td className="mono" title={`${utc(b.settlement_time)} · ${ago(b.settlement_time)}`}>{utc(b.settlement_time).slice(5, 16)}</td>
                        <td title={b.namespace}><button className="rowlink" onClick={() => setNs(b.namespace)} title="show only this namespace">{nsDisplay(b.namespace)}</button></td>
                        <td title={pub}><Link className="mono" href={`/publisher/?addr=${pub}`}>{shortBech(pub)}</Link></td>
                        <td className="mono">{unit(bytes(b.blob_size))}</td>
                        <td className="mono">{b.charge ? unit(tia(b.charge.fee_utia)) : "—"}</td>
                        <td className="mono">{b.attested_voting_power != null && b.total_voting_power ? `${(100 * b.attested_voting_power / b.total_voting_power).toFixed(1)}%` : "—"}</td>
                        <td><span className={`verdict verdict--${rc.tier}`} title={rc.title}><Mark tier={rc.tier} /><span className="w">{rc.word}</span></span></td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
            {data && <Pager total={data.total} page={page} size={SIZE} onPage={setPage} noun={data.total === 1 ? "settlement" : "settlements"} />}
          </>
        )}

        {tab === "namespaces" && (
          <div className="tablewrap framed">
            <table className="bt nss">
              <thead><tr><th>Namespace</th><th>Data</th><th>Blobs</th><th>Last 24h</th><th>Accounts</th><th>First seen</th><th>Last blob</th></tr></thead>
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
