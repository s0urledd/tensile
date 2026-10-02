"use client";
import { Suspense, useCallback, useRef, useState } from "react";
import { useSearchParams } from "next/navigation";
import PreLive from "@/components/PreLive";
import { useApi, type Meta, type Market, type Blob, type NamespaceRow, type Publisher, type Tip, int, bytes, nsDisplay, shortHex, utcWord } from "@/lib/api";
import Pager, { usePage } from "@/components/Pager";
import { useWindow, WindowSwitch } from "@/lib/window";
import BlobsDeck, { age, dayTime } from "@/components/BlobsDeck";
import Ledger, { useLedger, type Feed } from "@/components/Ledger";
import Picker, { type Choice } from "@/components/Picker";
import Ident from "@/components/Ident";
import { nsHex, NsName, NS_ICON } from "@/components/Namespace";

/** rows per page of the blob list */
const SIZE = 25;

/** the last page /v1/blobs serves: its offset stops at 100,000 */
const MAX_PAGE = Math.floor(100000 / SIZE) + 1;

const FIND_ICON = <svg width="14" height="14" viewBox="0 0 16 16" aria-hidden="true"><circle cx="7" cy="7" r="4.75" fill="none" stroke="currentColor" strokeWidth="1.5" /><path d="m10.5 10.5 3.5 3.5" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" /></svg>;
/** a promise hash or a commitment as typed or pasted: 64 hex characters, with or without 0x */
const hash64 = (s: string) => { const h = s.trim().toLowerCase().replace(/^0x/, ""); return /^[0-9a-f]{64}$/.test(h) ? h : null; };

/**
 * Find a blob: a promise hash or a commitment, pasted or typed whole. The list then shows only the blobs it names
 * (one for a promise hash; every settlement of a commitment), until the search is cleared.
 */
function FindBlob({ value, onFind }: { value: string; onFind: (h: string) => void }) {
  const [q, setQ] = useState("");
  const [bad, setBad] = useState(false);
  if (value) {
    return (
      <span className="lg-pick on lg-find">
        <span className="lg-chip">{FIND_ICON}<span className="k">Blob</span><span className="v mono">{value.slice(0, 8)}…{value.slice(-6)}</span></span>
        <button type="button" className="lg-x" aria-label="Show every blob" title="Show every blob" onClick={() => onFind("")}>
          <svg width="8" height="8" viewBox="0 0 8 8" aria-hidden="true"><path d="m1.5 1.5 5 5m0-5-5 5" stroke="currentColor" strokeWidth="1.4" strokeLinecap="round" /></svg>
        </button>
      </span>
    );
  }
  const go = (s: string) => { const h = hash64(s); if (h) { setQ(""); setBad(false); onFind(h); } else setBad(!!s.trim()); };
  return (
    <label className={`lg-findbox${bad ? " bad" : ""}`} title={bad ? "A promise hash or a commitment is 64 hex characters" : undefined}>
      {FIND_ICON}
      <input type="search" placeholder="Find a blob · promise hash or commitment" aria-label="Find a blob by its promise hash or its commitment" value={q}
        spellCheck={false} autoComplete="off"
        onChange={(e) => { setQ(e.target.value); setBad(false); if (hash64(e.target.value)) go(e.target.value); }}
        onKeyDown={(e) => { if (e.key === "Enter") go(q); }} />
    </label>
  );
}

const PUB_ICON = <svg width="14" height="14" viewBox="0 0 16 16" aria-hidden="true"><circle cx="8" cy="5.6" r="2.7" fill="none" stroke="currentColor" strokeWidth="1.4" /><path d="M2.8 13.6c.8-2.4 2.8-3.7 5.2-3.7s4.4 1.3 5.2 3.7" fill="none" stroke="currentColor" strokeWidth="1.4" strokeLinecap="round" /></svg>;

/** the namespaces on record, newest settlement first; a row shows its blobs */
function Namespaces({ rows, truncated, onPick }: { rows: NamespaceRow[] | null; truncated: boolean; onPick: (ns: string) => void }) {
  const now = Date.now();
  return (
    <>
      <div className="lg-tw">
        <table className="lg-t lg-nss">
          <thead>
            <tr><th className="n-nm">Namespace</th><th className="n-pub num">Publishers</th><th className="n-n num">Settlements</th><th className="n-sz num">Blob size</th><th className="n-24 num">Last 24h</th><th className="n-first">First seen</th><th className="n-last">Last blob</th></tr>
          </thead>
          <tbody>
            {!rows && <tr className="lg-empty"><td colSpan={7}>Loading…</td></tr>}
            {rows && rows.length === 0 && <tr className="lg-empty"><td colSpan={7}>No namespace on record.</td></tr>}
            {rows?.map((n) => {
              const hex = nsHex(n.namespace);
              const text = nsDisplay(n.namespace) !== shortHex(hex || n.namespace, 6);
              return (
                // the whole row shows the namespace's blobs; its name is the control a keyboard reaches
                <tr key={n.namespace} className="row" onClick={() => onPick(n.namespace)}>
                  <td className="n-nm"><button type="button" className="nsb" title={`${n.namespace} · show its blobs`}><NsName ns={n.namespace} /></button>{text && <span className="hx">{hex.slice(0, 6)}…{hex.slice(-4)}</span>}</td>
                  <td className="n-pub num">{int(n.accounts)}</td>
                  <td className="n-n num">{int(n.blobs)}</td>
                  <td className="n-sz num">{bytes(n.bytes)}</td>
                  <td className="n-24 num">{n.blobs_24h > 0 ? <>{int(n.blobs_24h)}<span className="sub">{bytes(n.bytes_24h)}</span></> : <span className="u">—</span>}</td>
                  <td className="n-first" title={utcWord(n.first_seen)}>{dayTime(n.first_seen).slice(0, -7)}</td>
                  <td className="n-last" title={utcWord(n.last_blob)}>{age(now - Date.parse(n.last_blob))} ago</td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
      {rows && rows.length > 0 && (
        <div className="pager"><span className="count">{truncated ? <>The <b>{int(rows.length)}</b> namespaces with the newest blobs</> : <><b>{int(rows.length)}</b> namespace{rows.length === 1 ? "" : "s"} on record</>}</span></div>
      )}
    </>
  );
}

/**
 * Blobs: the deck with the period's settlements, then the ledger of every
 * blob on record, newest first, with the namespaces as a second view. The
 * period drives the deck; the list is the whole record, which the namespace
 * and publisher filters narrow. The period, the page and the filters live in
 * the address, so a link opens on what its sender was looking at.
 */
function Page() {
  const params = useSearchParams();
  const [win, setWin] = useWindow("24h");
  const [page, setPageRaw] = usePage();
  const [tab, setTab] = useState<"blobs" | "namespaces">("blobs");
  const [ns, setNsRaw] = useState((params.get("namespace") ?? "").trim().toLowerCase());
  const [pub, setPubRaw] = useState((params.get("publisher") ?? "").trim().toLowerCase());
  const [found, setFoundRaw] = useState(hash64(params.get("blob") ?? "") ?? "");
  // a new page opens at the list's top when the reader had scrolled past it
  const setPage = useCallback((p: number) => {
    setPageRaw(p);
    const el = document.getElementById("list");
    if (el && el.getBoundingClientRect().top < 0) el.scrollIntoView();
  }, [setPageRaw]);
  // a new filter starts from the first page, and lives in the address so a link keeps it
  const filterBy = useCallback((key: string, set: (v: string) => void, v: string) => {
    set(v);
    setPageRaw(1);
    try {
      const u = new URL(window.location.href);
      if (v) u.searchParams.set(key, v); else u.searchParams.delete(key);
      window.history.replaceState(null, "", u.pathname + u.search + u.hash);
    } catch { /* fine */ }
  }, [setPageRaw]);
  const setNs = useCallback((v: string) => filterBy("namespace", setNsRaw, v), [filterBy]);
  const setPub = useCallback((v: string) => filterBy("publisher", setPubRaw, v), [filterBy]);
  const setFound = useCallback((v: string) => filterBy("blob", setFoundRaw, v), [filterBy]);
  const showNs = useCallback((v: string) => { setNs(v); setTab("blobs"); }, [setNs]);

  const q = `${ns ? `&namespace=${encodeURIComponent(ns)}` : ""}${pub ? `&publisher=${encodeURIComponent(pub)}` : ""}`;
  const offset = (Math.min(page, MAX_PAGE) - 1) * SIZE;
  const live = page === 1;
  const tip = useApi<Tip>("/v1/tip", 4000); // the header's stream: no request of its own
  const skew = tip.data?.server_time && tip.fetchedAt ? Date.parse(tip.data.server_time) - Date.parse(tip.fetchedAt) : 0;
  const feed = useLedger(`/v1/blobs?limit=${SIZE}&offset=${offset}${q}`, live, tip.data?.height, skew);
  // the chain's newest blobs, for the deck's rate and last blob: the list's own while it shows them, else a read of their own;
  // the last ones read stand while another page or filter loads, so the deck does not blank
  const plain = live && !q;
  const head = useApi<{ blobs: Blob[] }>(plain ? null : `/v1/blobs?limit=${SIZE}`);
  const newestRead = plain ? (feed.loaded ? feed.rows : null) : head.data?.blobs ?? null;
  const newestKept = useRef<Blob[] | null>(null);
  if (newestRead) newestKept.current = newestRead;
  const newest = newestRead ?? newestKept.current;

  const { data: meta } = useApi<Meta>("/v1/meta");
  const m = useApi<Market>(`/v1/market?window=${win}`);
  const nss = useApi<{ namespaces: NamespaceRow[]; truncated?: boolean }>("/v1/namespaces?limit=100");
  // the publishers, once the reader opens their filter
  const [wantPubs, setWantPubs] = useState(false);
  const pubs = useApi<{ publishers: Publisher[] }>(wantPubs ? "/v1/publishers?window=all" : null);

  const nsChoices: Choice[] | null = nss.data
    ? nss.data.namespaces.map((n) => ({ value: n.namespace, label: <NsName ns={n.namespace} />, count: n.blobs, find: `${nsDisplay(n.namespace)} ${nsHex(n.namespace)}`.toLowerCase() }))
    : null;
  const pubChoices: Choice[] | null = pubs.data
    ? pubs.data.publishers.filter((p) => p.settlements > 0).sort((a, b) => b.settlements - a.settlements).map((p) => ({
      value: p.publisher,
      label: p.label ? <><Ident addr={p.publisher} />{p.label}</> : <><Ident addr={p.publisher} /><span className="hd">celestia •••</span><span className="mono">{p.publisher.slice(-4)}</span></>,
      count: p.settlements,
      find: `${p.publisher} ${p.label ?? ""}`.toLowerCase(),
    }))
    : null;
  // a blob found by its promise hash, or the settlements of a commitment: the list shows only those, read once
  const byHash = useApi<{ blob: Blob }>(found ? `/v1/blobs/${found}` : null, 0);
  const byCommit = useApi<{ blobs: Blob[]; total: number }>(found ? `/v1/blobs?commitment=${found}&limit=${SIZE}` : null, 0);
  const foundRows = found ? [...(byHash.data?.blob ? [byHash.data.blob] : []), ...(byCommit.data?.blobs ?? [])].filter((b, i, a) => a.findIndex((x) => x.promise_hash === b.promise_hash) === i) : [];
  const foundFeed: Feed | null = found
    ? { path: `find:${found}`, rows: foundRows, total: foundRows.length, loaded: (!!byHash.data || !!byHash.error) && (!!byCommit.data || !!byCommit.error), error: null, refused: false, lastNewAt: 0 }
    : null;

  const nsN = nss.data?.namespaces.length ?? 0;
  const liveWord = !live || found || feed.refused ? null : feed.error ? "Not answering" : feed.loaded ? "Live" : "Connecting";
  const liveTitle = feed.error
    ? `The observer API did not answer (${feed.error}); the list shows the last read.`
    : "New blobs come in as the chain moves, while this page is open.";

  return (
    <>
      <div className="page-head lg-head">
        <h1>Blobs</h1>
        <WindowSwitch value={win} onChange={setWin} />
      </div>
      <PreLive meta={meta} />
      {m.error && <div className="note hold"><span className="label">Observer</span><p>Cannot reach the observer API: {m.error}. Nothing below is current.</p></div>}
      <BlobsDeck win={win} onWin={setWin} market={m.data} newest={newest} skew={skew} />

      <section id="list" className="listing lg-list">
        <div className="list-head">
          <div className="tabs" role="group" aria-label="list">
            <button type="button" aria-pressed={tab === "blobs"} onClick={() => setTab("blobs")}>Blobs</button>
            <button type="button" aria-pressed={tab === "namespaces"} onClick={() => setTab("namespaces")}>Namespaces{nsN ? <span className="n">{int(nsN)}{nss.data?.truncated ? "+" : ""}</span> : null}</button>
          </div>
          <div className="lg-tools">
            {tab === "blobs" && liveWord && <span className={`lg-live${feed.error ? " down" : ""}`} title={liveTitle}><i aria-hidden="true" />{liveWord}</span>}
          </div>
        </div>

        {/* over the table: the search over the blob columns, the publisher filter over Tensile's lane; a namespace
            picked from a row shows its chip there, to clear it */}
        {tab === "blobs" && (
          <div className="lg-bar">
            <FindBlob value={found} onFind={setFound} />
            {!found && (
              <span className="lg-bar-r">
                {ns && <Picker name="Namespace" icon={NS_ICON} value={ns} text={<NsName ns={ns} />} choices={nsChoices}
                  accept={(s) => (/^[0-9a-f]{58}$/.test(s) ? s : null)} placeholder="Name or hex" onPick={setNs} />}
                <Picker name="Publisher" icon={PUB_ICON} value={pub} text={pub ? <><span className="pre">celestia </span>••• {pub.slice(-4)}</> : null} choices={pubChoices}
                  accept={(s) => (/^celestia1[0-9a-z]{38}$/.test(s) ? s : null)} placeholder="Address" onPick={setPub} onOpen={() => setWantPubs(true)} />
              </span>
            )}
          </div>
        )}

        {tab === "blobs"
          ? (foundFeed
            ? (
              <Ledger feed={foundFeed} size={SIZE} live={false} skew={skew} onNs={(v) => { setFound(""); setNs(v); }}>
                {() => null}
              </Ledger>
            )
            : (
              <Ledger feed={feed} size={SIZE} live={live} skew={skew} onNs={setNs}>
                {(total) => (total === 0 && page === 1 ? null : <Pager total={total} page={page} size={SIZE} maxPages={MAX_PAGE} onPage={setPage}
                  noun={q ? (total === 1 ? "settlement with this filter" : "settlements with this filter") : total === 1 ? "settlement on record" : "settlements on record"} />)}
              </Ledger>
            ))
          : <Namespaces rows={nss.data?.namespaces ?? null} truncated={!!nss.data?.truncated} onPick={showNs} />}
      </section>
    </>
  );
}

export default function BlobsPage() {
  return <Suspense fallback={<p className="muted">Loading…</p>}><Page /></Suspense>;
}
