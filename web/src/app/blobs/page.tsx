"use client";
import { Suspense, useCallback, useEffect, useRef, useState } from "react";
import { useSearchParams } from "next/navigation";
import PreLive from "@/components/PreLive";
import { useApi, type Meta, type Market, type Blob, type NamespaceRow, type Publisher, type Tip, int, bytes, nsDisplay, shortHex, shortMid, utcWord, TIP_MS } from "@/lib/api";
import Pager, { usePage } from "@/components/Pager";
import { useWindow, WindowSwitch } from "@/lib/window";
import BlobsDeck, { age, dayTime } from "@/components/BlobsDeck";
import Ledger, { useLedger, type Feed } from "@/components/Ledger";
import Picker, { type Choice } from "@/components/Picker";
import Ident from "@/components/Ident";
import { nsHex, NsName, NS_ICON } from "@/components/Namespace";
import Info from "@/components/Info";
import { blobKey, keyText, type BlobKey } from "@/lib/blobkey";
import { useFind, type Found } from "@/lib/blobfind";

/** rows per page of the blob list */
const SIZE = 25;

/** the last page /v1/blobs serves: its offset stops at 100,000 */
const MAX_PAGE = Math.floor(100000 / SIZE) + 1;

/**
 * The search's words, in its field: the full ones need about 250 px of 13 px text, which the field leaves from 360 px
 * wide; a narrower phone gets the short ones. The empty field gives the placeholder the room Chrome and Safari
 * otherwise keep for the search's clear button (globals.css, .lg-findbox). Every format it takes is under its "i".
 */
const FIND_PLACEHOLDER = "Search by tx hash, blob ID or commitment";
const FIND_PLACEHOLDER_NARROW = "Tx hash, blob ID or commitment";
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

/** every format the search takes, behind the "i" beside it */
const FORMATS = (
  <ul className="lg-formats">
    <li><b>Transaction hash</b>: the MsgPayForFibre transaction that settled the blob, 64 hex characters.</li>
    <li><b>Blob ID</b>: as the Fibre client returns it, in base64 (standard or URL-safe, with or without padding) or in hex, 66 characters starting 00.</li>
    <li><b>Commitment</b>: 64 hex characters, the blob ID without its version byte.</li>
    <li><b>Promise hash</b>: 64 hex characters, the hash of the blob's payment promise.</li>
  </ul>
);

/**
 * The one search for a blob: a transaction hash, a blob ID, a commitment or a promise hash, pasted or typed whole,
 * with no choice of which. What it finds shows here as the list of it, one row or several, until the search is
 * cleared (value, with label for what it matched).
 */
function FindBlob({ value, label, upper, onFind }: { value: string; label: string; upper: boolean; onFind: (h: string) => void }) {
  const [q, setQ] = useState("");
  const [bad, setBad] = useState(false);
  const pasted = useRef(false);
  const placeholder = useFindPlaceholder();
  if (value) {
    // a transaction hash in upper case, as the client prints it, and so a hash that matched nothing, as the list names it
    const shown = upper ? value.toUpperCase() : value;
    return (
      <span className="lg-pick on lg-find">
        <span className="lg-chip" title={shown}>{FIND_ICON}<span className="k">{label}</span><span className="v mono">{shown.slice(0, 8)}…{shown.slice(-6)}</span></span>
        <button type="button" className="lg-x" aria-label="Show every blob" title="Show every blob" onClick={() => onFind("")}>
          <svg width="8" height="8" viewBox="0 0 8 8" aria-hidden="true"><path d="m1.5 1.5 5 5m0-5-5 5" stroke="currentColor" strokeWidth="1.4" strokeLinecap="round" /></svg>
        </button>
      </span>
    );
  }
  const go = (s: string) => { const v = findText(s); if (v) { setQ(""); setBad(false); onFind(v); } else setBad(!!s.trim()); };
  // A paste finds at once. Hex being typed waits until it is whole (or Enter): 44 characters of it are never read as a
  // blob ID in base64 on the way, nor the first 64 of a 66-character blob ID as a hash.
  const whole = (s: string) => {
    const k = blobKey(s), t = s.trim().replace(/^0x/i, "");
    return !!k && (pasted.current || /[^0-9a-f]/i.test(t) || (k.kind === "hash" && !t.startsWith("00")) || t.length >= 66);
  };
  return (
    <span className={`lg-findbox${bad ? " bad" : ""}`} title={bad ? "Not a transaction hash, blob ID, commitment or promise hash: the i beside the field lists each" : undefined}>
      <label>
        {FIND_ICON}
        <input type="search" placeholder={placeholder} aria-label="Search by transaction hash, blob ID, commitment or promise hash" value={q}
          spellCheck={false} autoComplete="off"
          onPaste={() => { pasted.current = true; }}
          onChange={(e) => { setQ(e.target.value); setBad(false); if (whole(e.target.value)) go(e.target.value); pasted.current = false; }}
          onKeyDown={(e) => { pasted.current = false; if (e.key === "Enter") go(q); }} />
      </label>
      <Info label="Search formats">{FORMATS}<p>Hex in either case, with or without 0x. What matches shows in the list. Typed hex starting 00 waits for Enter, since it may be a blob ID in hex.</p></Info>
    </span>
  );
}

/** a hash or a blob ID in a line of words: its ends, in full on hover; a transaction's in upper case, as the client prints it */
function Ident8({ text, tx }: { text: string; tx?: boolean }) {
  const t = tx ? text.toUpperCase() : text;
  return <span className="mono" title={t}>{shortMid(t, 10, 6)}</span>;
}

/**
 * what a search shown as a list matched, in a line over it: several blobs, or the one a lookup found while another
 * did not answer; then that the list holds only the newest, or may be incomplete
 */
function matchedWords(key: BlobKey, value: string, f: Found): React.ReactNode {
  const n = int(f.total);
  const as = f.by.map((b) => `as a ${b === "promise" ? "promise hash" : b === "tx" ? "transaction hash" : b}`);
  const what = f.total === 1
    ? <>One blob matches <Ident8 text={value} tx={f.by[0] === "tx"} />, {key.kind === "id" ? "as a blob ID" : as[0]}.</>
    : key.kind === "id" ? <>The blob ID <Ident8 text={value} /> was settled {n} times.</>
    : f.by.length === 1 && f.by[0] === "tx" ? <>The transaction <Ident8 text={value} tx /> settled {n} blobs.</>
    : f.by.length === 1 && f.by[0] === "commitment" ? <>The commitment <Ident8 text={value} /> was settled {n} times.</>
    : <>{n} blobs match <Ident8 text={value} />, {as.slice(0, -1).join(", ")} and {as[as.length - 1]}.</>;
  return (
    <>
      {what}
      {f.total > f.rows.length && <> Showing the newest {int(f.rows.length)}.</>}
      {f.partial && <> The observer did not answer every lookup ({f.partial}), so there may be more.</>}
    </>
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
  // the search came from the field, not the address: what it found goes into the address once it is known, so a link
  // opens on the same list
  const typed = useRef(false);
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
  const search = useCallback((v: string) => {
    typed.current = !!v;
    if (v) setFoundRaw(v); else setFound("");
  }, [setFound]);
  const showNs = useCallback((v: string) => { setNs(v); setTab("blobs"); }, [setNs]);
  // A search from the header while this page is open, or Back to an earlier one, changes only the address under it: the
  // list follows, from its first page. The page writing its own search into the address changes nothing here, as that
  // is the search shown.
  const asked = params.get("blob");
  const askedSeen = useRef(asked);
  useEffect(() => {
    if (asked === askedSeen.current) return;
    askedSeen.current = asked;
    const v = findText((asked ?? "").replace(/ /g, "+"));
    if (v === found) return;
    typed.current = false;
    setFoundRaw(v);
    setNsRaw((params.get("namespace") ?? "").trim().toLowerCase());
    setPubRaw((params.get("publisher") ?? "").trim().toLowerCase());
    setPageRaw(1);
    setTab("blobs");
  }, [asked]); // eslint-disable-line react-hooks/exhaustive-deps

  const q = `${ns ? `&namespace=${encodeURIComponent(ns)}` : ""}${pub ? `&publisher=${encodeURIComponent(pub)}` : ""}`;
  const offset = (Math.min(page, MAX_PAGE) - 1) * SIZE;
  const live = page === 1;
  const tip = useApi<Tip>("/v1/tip", TIP_MS); // the header's stream: no request of its own
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
  // The search: 64 hex characters asked as a promise hash, a commitment and a transaction hash, a blob ID as its
  // commitment, read once. The list shows what it found, one row or several, or says none matched; a lookup that failed
  // might have found more, so the list says so.
  const key = blobKey(found);
  const hit = useFind(key, { limit: SIZE });
  useEffect(() => {
    if (!hit) return;
    // the address keeps the search, so a link opens on the same list
    if (typed.current) setFound(found);
    typed.current = false;
  }, [hit]); // eslint-disable-line react-hooks/exhaustive-deps
  const tx = !!hit && hit.by.length === 1 && hit.by[0] === "tx";
  const foundLabel = key?.kind === "id" ? "Blob ID" : !hit?.by.length || hit.by.length > 1 ? "Search" : tx ? "Tx" : hit.by[0] === "commitment" ? "Commitment" : "Blob";
  const foundFeed: Feed | null = key
    ? { path: `find:${found}`, rows: hit?.rows ?? [], total: hit?.rows.length ?? 0, loaded: !!hit && !hit.error, error: hit?.error ?? null, refused: false, lastNewAt: 0 }
    : null;
  // nothing matched: Tensile has not indexed it yet, or the transaction carries no blob; never that the chain refused it
  const none = key && hit && !hit.error && hit.total === 0 && (key.kind === "id"
    ? <>Tensile has not indexed a blob with the blob ID <Ident8 text={found} /> yet. A blob appears once Tensile has read the block that settled it.</>
    : <>
      {hit.noTx
        ? <>No blob Tensile has indexed has the promise hash or commitment <Ident8 text={found} />, and this observer does not look transactions up yet.</>
        : <>Tensile has not indexed <Ident8 text={found} tx /> yet, or the transaction carries no Fibre blob.</>}
    </>);
  // the line over a list: what several matches matched, or that the one shown may not be all
  const matched = key && hit && !hit.error && (hit.total > 1 || (!!hit.partial && hit.total > 0)) && matchedWords(key, found, hit);
  // a hash that matched nothing is named as a transaction, in upper case: the chip says it as the line under it does
  const upper = tx || (key?.kind === "hash" && !!hit && !hit.error && hit.total === 0 && !hit.noTx);

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
          {/* the namespaces are the whole record, whatever the period above: said on the tabs line */}
          {tab === "namespaces" && <p className="lg-scope">All namespaces · totals on record</p>}
          {/* on the tabs line: the publisher filter right beside the search (a namespace picked from a row shows its
              chip before them, to clear it) */}
          {tab === "blobs" && (
            <div className="lg-tools">
              {!found && ns && <Picker name="Namespace" icon={NS_ICON} value={ns} text={<NsName ns={ns} />} choices={nsChoices}
                accept={(s) => (/^[0-9a-f]{58}$/.test(s) ? s : null)} placeholder="Name or hex" onPick={setNs} />}
              {!found && <Picker name="Publisher" icon={PUB_ICON} value={pub} text={pub ? <><span className="pre">celestia </span>••• {pub.slice(-4)}</> : null} choices={pubChoices}
                accept={(s) => (/^celestia1[0-9a-z]{38}$/.test(s) ? s : null)} placeholder="Address" onPick={setPub} onOpen={() => setWantPubs(true)} />}
              <FindBlob value={found} label={foundLabel} upper={upper} onFind={search} />
            </div>
          )}
        </div>

        {tab === "blobs"
          ? (foundFeed
            ? (
              <>
                {matched && <p className="lg-matched">{matched}</p>}
                <Ledger feed={foundFeed} size={SIZE} live={false} skew={skew} emptyText={none || undefined} onNs={(v) => { setFound(""); setNs(v); }}>
                  {() => null}
                </Ledger>
              </>
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
