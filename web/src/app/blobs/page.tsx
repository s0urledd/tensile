"use client";
import { Suspense, useState } from "react";
import Link from "next/link";
import { useSearchParams } from "next/navigation";
import PreLive from "@/components/PreLive";
import { unit } from "@/components/Unit";
import { useApi, type Meta, type Market, type Network, type Blob, type NamespaceRow, utc, ago, nsDisplay, bytes, int, tia, shortBech } from "@/lib/api";
import { Mark, type Tier } from "@/components/Verdict";
import { Metric, Metrics } from "@/components/Metrics";
import VolumeChart from "@/components/VolumeChart";
import Pager, { usePage } from "@/components/Pager";
import { useWindow, WindowSwitch } from "@/lib/window";

/** rows per page of the blob list */
const SIZE = 25;

type BlobPage = { blobs: Blob[]; total: number; offset: number; truncated: boolean; namespace?: string };

/** the last page /v1/blobs serves: its offset stops at 100,000 */
const MAX_PAGE = Math.floor(100000 / SIZE) + 1;

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
  if (!r || r.status === "unknown" || r.status === "pending") {
    return !over
      ? { word: "in retention window", tier: "gap", title: "Read once, 10 minutes before the retention window ends." }
      : { word: "not read by Tensile", tier: "gap", title: "No reading of this blob was completed. Nothing is counted for or against a validator." };
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
  const { data: net } = useApi<Network>(`/v1/network?window=${win}`);
  const bs = m?.blob_stats;
  const rc = net?.reconstructable;
  const pct = (f: number | null | undefined) => (f == null ? "—" : `${(f * 100).toFixed(1)}%`);
  const nsN = nss.data?.namespaces.length ?? 0;
  const nsMore = !!(nss.data as { truncated?: boolean } | null)?.truncated;
  const busy = !m; // figures not in yet: a blank line, not "nothing settled"

  return (
    <>
      <div className="head">
        <div><h1>Blobs</h1><p className="sub">Blobs published through Fibre and settled on chain.</p></div>
      </div>
      <PreLive meta={meta} />
      {error && !data && <p className="notice">The observer API is not answering ({error}); the page retries every 30 seconds. This is an observer outage, not a Fibre network outage.</p>}
      {error && data && <p className="sample">Showing the last list received; the API is not answering right now ({error}).</p>}

      <section className="group" id="chain">
        <div className="vhead">
          <div><h2>On chain</h2><p className="sub">The period&rsquo;s settlements.</p></div>
          <WindowSwitch value={win} onChange={setWin} />
        </div>
        <Metrics>
          <Metric label="Namespaces" value={bs ? int(bs.namespaces) : "—"} tone={bs && bs.namespaces > 0 ? undefined : "absent"}
            help={bs ? `in the period · ${int(bs.namespaces_total)} on record` : " "} title="Namespaces the period's blobs were published in." />
          <Metric label="Upload size" value={bs && bs.upload_size_max > 0 ? bytes(bs.upload_size_median) : "—"} tone={bs && bs.upload_size_max > 0 ? undefined : "absent"}
            help={busy ? " " : bs && bs.upload_size_max > 0 ? `median · largest ${bytes(bs.upload_size_max)}` : "nothing settled"}
            title="The size a blob paid for, with padding and without parity: the median of the period's settlements, and the largest." />
          <Metric label="Endorsed voting power" value={pct(bs?.endorsed_share_median)} tone={bs?.endorsed_share_median != null ? undefined : "absent"}
            help={busy ? " " : bs?.endorsed_share_min != null ? `median · ${pct(bs.endorsed_share_min)} to ${pct(bs.endorsed_share_max)}` : "nothing settled"}
            title="Share of voting power whose signature is on the settlement. A settlement needs ⅔." />
        </Metrics>
        {rc && rc.recoverable.den > 0 && (
          <p className="observed" title="Settlements Tensile read near the end of their retention window whose rows were enough to reconstruct the blob.">
            Observed by Tensile: <b>{int(rc.recoverable.num)} of {int(rc.recoverable.den)}</b> settlements read were retrievable.
          </p>
        )}
      </section>

      <section className="group" id="published">
        <div className="vhead">
          <div><h2>{metric === "bytes" ? "Upload size" : "Settlements"}</h2><p className="sub">Per UTC {win === "24h" ? "hour" : "day"}</p></div>
          <div className="seg" role="group" aria-label="chart">
            <button type="button" aria-pressed={metric === "bytes"} onClick={() => setMetric("bytes")}>Upload size</button>
            <button type="button" aria-pressed={metric === "settlements"} onClick={() => setMetric("settlements")}>Settlements</button>
          </div>
        </div>
        <VolumeChart market={m ?? null} win={win} metric={metric} />
      </section>

      <section id="list">
        <div className="vhead">
          <div className="seg" role="group" aria-label="list">
            <button type="button" aria-pressed={tab === "blobs"} onClick={() => setTab("blobs")}>Blobs</button>
            <button type="button" aria-pressed={tab === "namespaces"} onClick={() => setTab("namespaces")}>Namespaces{nsN ? <span className="n"> {int(nsN)}{nsMore ? "+" : ""}</span> : null}</button>
          </div>
          <div className="tools">
            {tab === "blobs" && <input className="nsfilter" type="search" placeholder="Filter by namespace (58 hex)" value={ns} onChange={(e) => setNs(e.target.value)} aria-label="namespace filter" />}
          </div>
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
                        <td className="mono">{b.attested_voting_power != null && b.total_voting_power ? `${(100 * b.attested_voting_power / b.total_voting_power).toFixed(1)}%` : "—"}</td>
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
