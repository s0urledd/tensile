"use client";
import { Suspense, useState } from "react";
import Link from "next/link";
import { useSearchParams } from "next/navigation";
import PreLive from "@/components/PreLive";
import { unit } from "@/components/Unit";
import { useApi, type Meta, type Market, type Blob, type NamespaceRow, type Tip, utc, ago, nsDisplay, bytes, int, pctOf, tia, shortBech } from "@/lib/api";
import { Mark } from "@/components/Verdict";
import { recon } from "@/lib/status";
import Pager, { usePage } from "@/components/Pager";
import { useWindow, WindowSwitch } from "@/lib/window";
import BlobsDeck from "@/components/BlobsDeck";

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
  // the chain's newest blobs, for the deck's rate and last blob: the list's own while it shows them, else a read of their own
  const plain = page === 1 && !nsq;
  const head = useApi<BlobPage>(plain ? null : `/v1/blobs?limit=${SIZE}`);
  const newest = plain ? rows : head.data?.blobs ?? null;
  const tip = useApi<Tip>("/v1/tip", 4000); // the header's stream: no request of its own
  const skew = tip.data?.server_time && tip.fetchedAt ? Date.parse(tip.data.server_time) - Date.parse(tip.fetchedAt) : 0;
  const { data: meta } = useApi<Meta>("/v1/meta");
  const nss = useApi<{ namespaces: NamespaceRow[] }>("/v1/namespaces?limit=100");
  const { data: m } = useApi<Market>(`/v1/market?window=${win}`);
  const nsN = nss.data?.namespaces.length ?? 0;
  const nsMore = !!(nss.data as { truncated?: boolean } | null)?.truncated;

  return (
    <>
      <div className="page-head lg-head">
        <h1>Blobs</h1>
        <WindowSwitch value={win} onChange={setWin} />
      </div>
      <PreLive meta={meta} />
      {error && !data && <p className="notice">The observer API is not answering ({error}); the page retries every 30 seconds. This is an observer outage, not a Fibre network outage.</p>}
      {error && data && <p className="sample">Showing the last list received; the API is not answering right now ({error}).</p>}

      <BlobsDeck win={win} onWin={setWin} market={m} newest={newest} skew={skew} />

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
                <thead><tr><th>Blob</th><th>Height</th><th>Settled (UTC)</th><th>Namespace</th><th>Publisher</th><th>Blob size</th><th>Fee paid</th>
                  <th title="Share of voting power whose signature on the settlement verified. A settlement needs ⅔.">Endorsed</th><th>Status</th></tr></thead>
                <tbody>
                  {!rows && (loading || !!data) && <tr><td colSpan={9} className="muted">Loading…</td></tr>}
                  {rows && rows.length === 0 && <tr><td colSpan={9} className="muted">No blob recorded{ns.trim() ? " in this namespace" : ""}.</td></tr>}
                  {rows?.map((b) => {
                    const rc = recon(b);
                    const pub = b.publisher || b.signer;
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
              <thead><tr><th>Namespace</th><th>Blob size</th><th>Settlements</th><th>Last 24h</th><th>Publishers</th><th>First seen</th><th>Last blob</th></tr></thead>
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
