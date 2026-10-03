"use client";
import { Suspense, useCallback, useEffect, useRef, useState } from "react";
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
import { blobKey, keyText } from "@/lib/blobkey";

/** rows per page of the blob list */
const SIZE = 25;

/** the last page /v1/blobs serves: its offset stops at 100,000 */
const MAX_PAGE = Math.floor(100000 / SIZE) + 1;

/**
 * What the search takes, in words that fit its field: about 270 px of 13 px text, where the field leaves 281 px or more
 * (360 px and wider), and 228 px on a narrower phone, where it leaves 241 at 320. The empty field gives the placeholder
 * the room Chrome and Safari otherwise keep for the search's clear button, about 14 px (globals.css, .lg-findbox).
 */
const FIND_PLACEHOLDER = "Promise hash, commitment, blob ID or tx hash";
const FIND_PLACEHOLDER_NARROW = "Promise hash, commitment, blob ID, tx";
const FIND_NARROW = "(max-width: 359px)";
function useFindPlaceholder(): string {
  const [narrow, setNarrow] = useState(false);
  useEffect(() => {
    const q = window.matchMedia(FIND_NARROW);
    const on = () => setNarrow(q.matches);
    on();
    q.addEventListener("change", on);
    return () => q.removeEventListener("change", on);
  }, []);
  return narrow ? FIND_PLACEHOLDER_NARROW : FIND_PLACEHOLDER;
}

const FIND_ICON = <svg width="14" height="14" viewBox="0 0 16 16" aria-hidden="true"><circle cx="7" cy="7" r="4.75" fill="none" stroke="currentColor" strokeWidth="1.5" /><path d="m10.5 10.5 3.5 3.5" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" /></svg>;
/** what the search and the address keep of a blob identifier (blobKey), or "" for anything that is none */
const findText = (s: string | null) => { const k = blobKey(s); return k ? keyText(k) : ""; };

/**
 * Find a blob: a promise hash, a commitment, the hash of the transaction that settled it, or the client's blob ID,
 * pasted or typed whole. The list then shows only the blobs it names (one for a promise hash or a transaction; every
 * settlement of a commitment or a blob ID), until the search is cleared.
 */
function FindBlob({ value, onFind }: { value: string; onFind: (h: string) => void }) {
  const [q, setQ] = useState("");
  const [bad, setBad] = useState(false);
  const placeholder = useFindPlaceholder();
  if (value) {
    const id = blobKey(value)?.kind === "id";
    return (
      <span className="lg-pick on lg-find">
        <span className="lg-chip" title={value}>{FIND_ICON}<span className="k">{id ? "Blob ID" : "Blob"}</span><span className="v mono">{value.slice(0, 8)}…{value.slice(-6)}</span></span>
        <button type="button" className="lg-x" aria-label="Show every blob" title="Show every blob" onClick={() => onFind("")}>
          <svg width="8" height="8" viewBox="0 0 8 8" aria-hidden="true"><path d="m1.5 1.5 5 5m0-5-5 5" stroke="currentColor" strokeWidth="1.4" strokeLinecap="round" /></svg>
        </button>
      </span>
    );
  }
  const go = (s: string) => { const v = findText(s); if (v) { setQ(""); setBad(false); onFind(v); } else setBad(!!s.trim()); };
  // a paste finds at once; hex being typed waits for its 64th character (or Enter), so 44 characters of it are never
  // read as a blob ID in base64 on the way
  const whole = (s: string) => {
    const k = blobKey(s), t = s.trim();
    return !!k && (k.kind === "hash" || /[^0-9a-f]/i.test(t) || t.length >= 66);
  };
  return (
    <label className={`lg-findbox${bad ? " bad" : ""}`} title={bad ? "A promise hash, a commitment or a transaction hash is 64 hex characters; a blob ID is the client's, in base64" : undefined}>
      {FIND_ICON}
      <input type="search" placeholder={placeholder} aria-label="Find a blob by its promise hash, commitment, blob ID or transaction hash" value={q}
        spellCheck={false} autoComplete="off"
        onChange={(e) => { setQ(e.target.value); setBad(false); if (whole(e.target.value)) go(e.target.value); }}
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
  // the address reads a raw + as a space, which base64 never holds: each goes back before blobKey trims a last one away
  const [found, setFoundRaw] = useState(findText((params.get("blob") ?? "").replace(/ /g, "+")));
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
  // A blob found by its promise hash, the blob a transaction settled, or the settlements of a commitment: the list shows
  // only those, read once. 64 hex characters are asked as all three; a blob ID is its commitment.
  const key = blobKey(found);
  const hex = key?.kind === "hash" ? key.hex : "";
  const byHash = useApi<{ blob: Blob }>(hex ? `/v1/blobs/${hex}` : null, 0);
  const byCommit = useApi<{ blobs: Blob[]; total: number; commitment?: string }>(key ? `/v1/blobs?commitment=${key.hex}&limit=${SIZE}` : null, 0);
  const byTx = useApi<{ blobs: Blob[]; tx?: string }>(hex ? `/v1/blobs?tx=${hex}&limit=${SIZE}` : null, 0);
  // an answer counts only when it says it filtered on this hash: an API from before ?tx= ignores it and answers the
  // newest blobs, and a search just changed still holds the last one's answer
  const commitRows = key && byCommit.data?.commitment === key.hex ? byCommit.data.blobs : [];
  const txRows = hex && byTx.data?.tx === hex ? byTx.data.blobs : [];
  const hashRow = hex && byHash.data?.blob?.promise_hash === hex ? [byHash.data.blob] : [];
  const foundRows = key ? [...hashRow, ...commitRows, ...txRows].filter((b, i, a) => a.findIndex((x) => x.promise_hash === b.promise_hash) === i) : [];
  // in once each answers this search (an API from before ?tx= answers without saying what it filtered on), or fails
  const loaded = (byCommit.data?.commitment === key?.hex || (!!byCommit.error && !byCommit.data))
    && (!hex || ((!!hashRow.length || !!byHash.error) && (byTx.data?.tx === hex || (!!byTx.data && byTx.data.tx === undefined) || !!byTx.error)));
  const foundFeed: Feed | null = key
    ? { path: `find:${found}`, rows: foundRows, total: foundRows.length, loaded, error: null, refused: false, lastNewAt: 0 }
    : null;

  const nsN = nss.data?.namespaces.length ?? 0;

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
          {/* on the tabs line: the publisher filter right beside the search (a namespace picked from a row shows its
              chip before them, to clear it) */}
          {tab === "blobs" && (
            <div className="lg-tools">
              {!found && ns && <Picker name="Namespace" icon={NS_ICON} value={ns} text={<NsName ns={ns} />} choices={nsChoices}
                accept={(s) => (/^[0-9a-f]{58}$/.test(s) ? s : null)} placeholder="Name or hex" onPick={setNs} />}
              {!found && <Picker name="Publisher" icon={PUB_ICON} value={pub} text={pub ? <><span className="pre">celestia </span>••• {pub.slice(-4)}</> : null} choices={pubChoices}
                accept={(s) => (/^celestia1[0-9a-z]{38}$/.test(s) ? s : null)} placeholder="Address" onPick={setPub} onOpen={() => setWantPubs(true)} />}
              <FindBlob value={found} onFind={setFound} />
            </div>
          )}
        </div>

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
