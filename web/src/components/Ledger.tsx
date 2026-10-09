"use client";
import { memo, useCallback, useEffect, useLayoutEffect, useMemo, useRef, useState } from "react";
import Link from "next/link";
import { useRouter } from "next/navigation";
import { API_BASE, type Blob, type FailedPayment, type ListRow, int, bytes, tia, pctOf, nsDisplay, utcWord, isBlob, isFailed, failedTitle } from "@/lib/api";
import { lane } from "@/lib/status";
import { openRow } from "@/lib/row";
import { unit } from "@/components/Unit";
import { Eye } from "@/components/Metrics";
import { Frac } from "@/components/CurrentProviders";
import Ident from "@/components/Ident";
import Info from "@/components/Info";
import { blobIdOf } from "@/lib/blobkey";

/** how many of the settlement transaction hash's first characters the TX hash column shows */
const ID_ENDS = 6;
/** a transaction hash keeps four characters at its end, as the hash is printed elsewhere (5DA67B…754E) */
const TX_END = 4;
import { reducedMotion } from "@/components/RollNumber";
import { age, monthDayTime } from "@/components/BlobsDeck";

/**
 * The Blobs list as a ledger: the blobs, newest first, each a row of its own
 * with its height, when it settled (the day and the second, UTC) and how
 * long ago, whatever block or day it shares with the rows around it.
 * Figures are right-aligned on their digits. Endorsed
 * is the share of voting power with a short meter whose tick is the ⅔ a
 * settlement needs. Tensile's own reading has a lane of its own at the end,
 * empty until Tensile has read the blob. One publisher's list is its
 * transactions: its escrow movements stand between its blobs, by time, and
 * its fee column is the escrow's statement, each amount signed.
 *
 * The Blobs list also says how each blob payment came out on chain, in a
 * Status column after the fee: Success on every blob, and Failed on the
 * payments that failed in their block, which it asks for among the blobs
 * (/v1/blobs?include_failed=1). A failed payment stands where its block put
 * it, with its height, time, transaction, namespace and publisher as a
 * blob's, the reason on hover, and a dash for everything it would have
 * moved; its row opens its transaction.
 *
 * The first page is live. It is read when the chain moves (the header's
 * /v1/tip stream, shared): at most every 5 s while blobs arrive, every 15 s
 * once none has for two minutes, every 30 s if the chain stops moving, and
 * never while the tab is hidden. While the reader is at the top of the list
 * and neither pointing into it nor focused in it, new blobs come in on their
 * own, in one move with one glow (neither under reduced motion). While the
 * reader is in the list, nothing under them moves: the new blobs gather in a
 * pill at the table's edge, and a click on it brings them in. Older pages
 * hold still.
 *
 * While a page loads, the list keeps its size: a page of placeholders at
 * first, then the last page's rows, quieted, until the next one comes.
 */

const FAST_MS = 5000, SLOW_MS = 15000, FALLBACK_MS = 30000;
/** no new blob for this long reads at the slow pace */
const IDLE_AFTER_MS = 120000;
const SLIDE_MS = 420;

export type Feed = {
  /** the request this answer is for: a new page or filter starts a new feed */
  path: string;
  rows: ListRow[];
  /** the settlements the filters select, all of them, and the failed payments among them when the list asks for those */
  total: number;
  /** the failed payments counted in total: none unless the list asks for them */
  failed: number;
  loaded: boolean;
  error: string | null;
  /** the API refused the request itself (a 400: a filter that is no namespace or account), which is no outage and is not asked again */
  refused: boolean;
  /** when the newest row arrived, on the observer's clock (when it settled, on the first read) */
  lastNewAt: number;
};
const empty = (path: string): Feed => ({ path, rows: [], total: 0, failed: 0, loaded: false, error: null, refused: false, lastNewAt: 0 });

/** a row's key: a blob's promise hash; a failed payment, which settled no blob, its place (its block, its index in it) */
const keyOf = (r: ListRow) => (isFailed(r) ? `f${r.settlement_height}:${r.settlement_tx_index}` : r.promise_hash);

/** a failed read, with the status the API answered (0: no answer) */
class ReadError extends Error {
  constructor(message: string, readonly status: number) { super(message); }
}

async function readPage(path: string): Promise<{ blobs: ListRow[]; total: number; failed: number }> {
  const ctl = new AbortController();
  const t = window.setTimeout(() => ctl.abort(), 15000);
  try {
    const r = await fetch(API_BASE + path, { cache: "no-store", signal: ctl.signal });
    if (!r.ok) {
      let msg = String(r.status);
      try { const j = await r.json(); if (j?.error) msg = j.error; } catch { /* keep the status */ }
      throw new ReadError(msg, r.status);
    }
    const j = await r.json();
    return { blobs: Array.isArray(j.blobs) ? j.blobs : [], total: typeof j.total === "number" ? j.total : 0, failed: typeof j.failed_total === "number" ? j.failed_total : 0 };
  } finally {
    window.clearTimeout(t);
  }
}

/**
 * One page of /v1/blobs (path), read once, and again as the chain moves
 * while it is live (height: the tip's, from the shared stream), paced as the
 * comment at the top says. A page that is not live is read again only after
 * an error. skew: the observer's clock minus the reader's.
 */
export function useLedger(path: string, live: boolean, height: number | undefined, skew: number): Feed {
  const [feed, setFeed] = useState<Feed>(() => empty(path));
  const cur = useRef(feed);
  const busy = useRef(false);
  const last = useRef(0);
  const later = useRef<number | undefined>(undefined);
  const pathRef = useRef(path);
  pathRef.current = path;
  const liveRef = useRef(live);
  liveRef.current = live;
  const skewRef = useRef(skew);
  skewRef.current = skew;

  const apply = useCallback((f: Feed) => { cur.current = f; setFeed(f); }, []);
  const gap = () => {
    const f = cur.current;
    return f.error || (f.loaded && Date.now() + skewRef.current - f.lastNewAt > IDLE_AFTER_MS) ? SLOW_MS : FAST_MS;
  };

  const read = useCallback(async () => {
    if (busy.current || document.hidden) return;
    busy.current = true;
    last.current = Date.now();
    const p = pathRef.current;
    try {
      const page = await readPage(p);
      if (p !== pathRef.current) return;
      const before = cur.current.path === p && cur.current.loaded ? cur.current : null;
      const known = new Set(before?.rows.map(keyOf));
      const came = !!before && page.blobs.some((b) => !known.has(keyOf(b)));
      const lastNewAt = !before ? (page.blobs[0] ? Date.parse(page.blobs[0].settlement_time) : 0) : came ? Date.now() + skewRef.current : before.lastNewAt;
      apply({ path: p, rows: page.blobs, total: page.total, failed: page.failed, loaded: true, error: null, refused: false, lastNewAt });
    } catch (e) {
      if (p !== pathRef.current) return;
      const f = cur.current.path === p ? cur.current : empty(p);
      apply({ ...f, error: e instanceof Error ? e.message : String(e), refused: e instanceof ReadError && e.status === 400 });
    } finally {
      busy.current = false;
      // the page or a filter changed while this read was out: read the new one now
      if (p !== pathRef.current) read();
    }
  }, [apply]);

  /** a read now, or at the end of the current gap if the last one was sooner */
  const request = useCallback(() => {
    window.clearTimeout(later.current);
    if (document.hidden || cur.current.refused) return;
    const wait = gap() - (Date.now() - last.current);
    if (wait <= 0) read();
    else later.current = window.setTimeout(read, wait);
  }, [read]); // eslint-disable-line react-hooks/exhaustive-deps

  // a new page or filter: nothing of the old one stands for it, and it is read at once
  useEffect(() => {
    window.clearTimeout(later.current);
    apply(empty(path));
    last.current = 0;
    read();
  }, [path, apply, read]);
  // live: a read each time the chain moves
  useEffect(() => { if (live) request(); }, [height, live, request]);
  // live: a slow read when the chain stops moving, and one when the page comes back into view; any page: another try after an error,
  // and its first read when it was opened in a tab out of view
  useEffect(() => {
    const t = window.setInterval(() => {
      const f = cur.current;
      if (!document.hidden && !f.refused && (liveRef.current || f.error) && Date.now() - last.current >= FALLBACK_MS) read();
    }, 5000);
    const onVis = () => { if (document.hidden) window.clearTimeout(later.current); else if (liveRef.current || !cur.current.loaded) request(); };
    document.addEventListener("visibilitychange", onVis);
    return () => { window.clearInterval(t); window.clearTimeout(later.current); document.removeEventListener("visibilitychange", onVis); };
  }, [read, request]);

  return feed;
}

// ---- what the table shows ----

/** the rows on screen: the feed's, unless the reader holds them; move is the last arrival into them */
type Shown = { path: string; rows: ListRow[]; total: number; failed: number; loaded: boolean; move: { id: number; fresh: Set<string> } | null };

/** the feed onto the screen; the rows it did not show before are the move's */
function take(v: Shown, f: Feed): Shown {
  if (f.path !== v.path || !v.loaded) return { path: f.path, rows: f.rows, total: f.total, failed: f.failed, loaded: f.loaded, move: null };
  if (f.rows === v.rows && f.total === v.total) return v;
  const known = new Set(v.rows.map(keyOf));
  const fresh = new Set(f.rows.filter((b) => !known.has(keyOf(b))).map(keyOf));
  return { path: f.path, rows: f.rows, total: f.total, failed: f.failed, loaded: true, move: fresh.size ? { id: (v.move?.id ?? 0) + 1, fresh } : v.move };
}

/** the publisher of a row: who paid, else who submitted it */
const payer = (b: Blob) => b.publisher || b.signer;

/** a column of amounts keeps one precision, so the digits stack: the finest tia() gives any of them, so a 0.695 TIA fee never rounds to 1 beside a deposit of thousands */
export function decimals(amounts: number[]): number {
  return Math.max(0, ...amounts.filter((a) => a !== 0).map((a) => { const v = Math.abs(a) / 1e6; return v >= 1000 ? 0 : v >= 100 ? 1 : v >= 10 ? 2 : 3; }));
}
const tiaAt = (utia: number, dec: number) => `${(utia / 1e6).toLocaleString("en-US", { minimumFractionDigits: dec, maximumFractionDigits: dec })} TIA`;
const tiaExact = (utia: number) => `${(utia / 1e6).toLocaleString("en-US", { minimumFractionDigits: 6 })} TIA`;
/** an amount of the escrow's statement: + in, − out, no sign for what only moves inside it; every digit on hover */
export function Signed({ sign, utia, dec }: { sign: string; utia: number; dec: number }) {
  return <span title={tiaExact(utia)}>{unit(`${utia ? sign : ""}${tiaAt(utia, dec)}`)}</span>;
}

/** one escrow movement in a publisher's list: a deposit, a withdrawal requested or paid out, a timed-out promise charged */
export type Move = {
  key: string;
  height: number;
  time: string;
  /** what it is, where a blob's hash stands: "Deposit" */
  word: string;
  /** what qualifies it, quietly over the namespace and the size ("payable from Oct 3 12:00:00"), and the shorter form a phone's line takes */
  qual: string;
  short: string;
  sign: "+" | "−" | "";
  utia: number;
  /** "req": money moved into the withdrawal queue; "req hold": a request settlements used up; "fault": a timed-out promise */
  tone: string;
  /** the amount's hover, where it needs more than the amount */
  note?: string;
  /** its place among the account's payments, newest first, which keeps a block's own order */
  idx: number;
};
/** the movements that stand among a page's rows, and the rows and movements that page shows, counted from the list's first */
export type Placed = { list: Move[]; range: [number, number] };
/**
 * One publisher's movements against the rows on screen: place sets them by those rows (the page's path, its rows and
 * the count of all of them, as one read gave them), so a page that holds still keeps its movements in step with its
 * rows; rank is each blob's place among the same payments, which keeps a block's own order.
 */
export type Moves = { place: (path: string, rows: Blob[], total: number) => Placed; rank: Map<string, number> };

/**
 * the columns' heads; one publisher's list names no publisher, its rows are its transactions, and its fee column is the
 * escrow's statement. The statement alone (escrow) heads what its rows hold: the kind of movement and its amount, the
 * blobs' columns left unnamed (they stay, so nothing moves when the kind changes). The Blobs list's (status) names each
 * payment's outcome on chain after the fee
 */
export function LedgerHead({ one, escrow = false, status = false }: { one: boolean; escrow?: boolean; status?: boolean }) {
  if (escrow) {
    return (
      <thead>
        <tr>
          <th className="c-h">Height</th>
          <th className="c-t">Time <span className="per">(UTC)</span></th>
          <th className="c-b">Type</th>
          <th className="c-ns" aria-hidden="true" />
          <th className="c-sz num" aria-hidden="true" />
          <th className="c-fee num" title="What each movement put into the escrow (+) or took out of it (−).">Amount</th>
          <th className="c-e num" aria-hidden="true" />
          <th className="gap" aria-hidden="true" />
          <th className="tn" aria-hidden="true" />
        </tr>
      </thead>
    );
  }
  return (
    <thead>
      <tr>
        <th className="c-h">Height</th>
        <th className="c-t">{one ? "Time" : "Settled"} <span className="per">(UTC)</span></th>
        <th className="c-b">TX hash</th>
        <th className="c-ns">Namespace</th>
        {!one && <th className="c-p">Publisher</th>}
        <th className="c-sz num">Blob size</th>
        {one
          ? <th className="c-fee num" title="What each transaction moved into the escrow (+) or out of it (−): a blob's fee, a deposit, a withdrawal paid out.">Amount</th>
          : <th className="c-fee num">Fee paid</th>}
        {status && <th className="c-st">Status</th>}
        <th className="c-e num" title="Share of voting power whose signature on the settlement verified. A settlement needs ⅔.">Endorsed <Frac /></th>
        <th className="gap" aria-hidden="true" />
        <th className="tn" title="Tensile's own reading of each blob, once, near the end of its retention window."><span><Eye />Tensile</span></th>
      </tr>
    </thead>
  );
}

/**
 * An escrow movement as a row: its height and time as a blob's, its kind where the blob is, what qualifies it over the
 * namespace and the size, its amount signed; no endorsement, nothing for Tensile to read. In the list's two lines, its
 * qualifier and its amount close the second line, as a blob's fee closes its own.
 */
export const MoveRow = memo(function MoveRow({ m, age: ag, dec }: { m: Move; age: string | null; dec: number }) {
  return (
    <tr className={`row mv${m.tone ? ` ${m.tone}` : ""}`} data-m={m.key}>
      <td className="c-h">{int(m.height)}</td>
      <td className="c-t"><span title={utcWord(m.time)}><span className="tm">{monthDayTime(m.time)}</span>{ag && <span className="ag">{ag}</span>}</span></td>
      <td className="c-b"><span className="k" title={m.word}>{m.word}</span><span className="ht">#{int(m.height)}</span></td>
      <td className="c-q" colSpan={2} title={m.qual || undefined}>{m.qual}</td>
      <td className="c-fee num" title={m.note}><Signed sign={m.sign} utia={m.utia} dec={dec} /></td>
      <td className="c-e" /><td className="gap" aria-hidden="true" /><td className="tn" />
      <td className="c-m">
        {m.short && <><span className="q">{m.short}</span><span className="sep">·</span></>}
        <span className="fe" title={m.note ?? tiaExact(m.utia)}>{m.utia ? m.sign : ""}{tiaAt(m.utia, dec)}</span>
      </td>
    </tr>
  );
});

/** a publisher as a chip: its mark, the address's prefix quietly, its last four characters */
export function Who({ addr }: { addr: string }) {
  const i = addr.indexOf("1");
  return (
    <Link className="lg-who" href={`/publisher/?addr=${addr}`} title={addr}>
      <Ident addr={addr} />
      {i > 0 && <span className="hd">{addr.slice(0, i)} •••</span>}
      <span className="tl">{addr.slice(-4)}</span>
    </Link>
  );
}

/**
 * A small button after an identifier that copies the whole of it (a blob's short hash in the list, an endpoint or an
 * address on a validator's page): "Copy <label>" on hover, a tick for a moment once it has.
 */
export function CopyMark({ text, label }: { text: string; label: string }) {
  const [done, setDone] = useState(false);
  return (
    <button type="button" className={`cp${done ? " done" : ""}`} aria-label={`Copy ${label}`} title={done ? "Copied" : `Copy ${label}`}
      onClick={async () => { try { await navigator.clipboard.writeText(text); setDone(true); window.setTimeout(() => setDone(false), 1200); } catch { /* clipboard unavailable */ } }}>
      {done
        ? <svg width="12" height="12" viewBox="0 0 16 16" aria-hidden="true"><path d="m3.5 8.5 3 3 6-7" fill="none" stroke="currentColor" strokeWidth="1.7" strokeLinecap="round" strokeLinejoin="round" /></svg>
        : <svg width="12" height="12" viewBox="0 0 16 16" aria-hidden="true"><rect x="5.5" y="5.5" width="8" height="8" rx="1.8" fill="none" stroke="currentColor" strokeWidth="1.4" /><path d="M10.5 3.6v-.2c0-.9-.7-1.6-1.6-1.6H4.1c-.9 0-1.6.7-1.6 1.6v4.8c0 .9.7 1.6 1.6 1.6h.2" fill="none" stroke="currentColor" strokeWidth="1.4" strokeLinecap="round" /></svg>}
    </button>
  );
}

type RowProps = { b: Blob; age: string | null; fresh: boolean; one: boolean; dec: number; status: boolean; onNs: (ns: string) => void; onOpen: (e: React.MouseEvent, href: string) => void };
/** one blob: the cells of the table, and the second line a phone shows under the first */
const Row = memo(function Row({ b, age: ag, fresh, one, dec, status, onNs, onOpen }: RowProps) {
  const href = `/blob/?hash=${b.promise_hash}`;
  const id = blobIdOf(b.commitment, b.blob_version ?? 0);
  // the settlement transaction, in upper case as an explorer prints it: the reference people pass around
  const tx = b.settlement_tx_hash?.toUpperCase() ?? "";
  const who = payer(b);
  const name = nsDisplay(b.namespace);
  const ln = lane(b);
  const share = b.attested_voting_power != null && b.total_voting_power ? b.attested_voting_power / b.total_voting_power : null;
  return (
    // The whole row opens the blob; links and buttons keep their own.
    <tr className={`row${fresh ? " fresh" : ""}`} data-h={b.promise_hash}
      onClick={(e) => openRow(e, href, onOpen)} onAuxClick={(e) => openRow(e, href, onOpen)}>
      <td className="c-h">{int(b.settlement_height)}</td>
      <td className="c-t"><span title={utcWord(b.settlement_time)}><span className="tm">{monthDayTime(b.settlement_time)}</span>{ag && <span className="ag">{ag}</span>}</span></td>
      <td className="c-b">
        {/* the settlement transaction, its hash's first and last characters; the row opens this settlement (its promise
            hash), the copy is the transaction hash, and the blob ID is on hover */}
        {tx
          ? <><Link href={href} title={`Transaction ${tx}\nBlob ID ${id}\nPromise hash ${b.promise_hash}`} aria-label={`Transaction ${tx.slice(0, 10)}, height ${int(b.settlement_height)}`}>{tx.slice(0, ID_ENDS)}<span className="el">…</span>{tx.slice(-TX_END)}</Link>
            <CopyMark text={tx} label="the transaction hash" /></>
          : <Link href={href} title={`Blob ID ${id}\nPromise hash ${b.promise_hash}`}>—</Link>}
        <span className="ht">#{int(b.settlement_height)}</span>
      </td>
      <td className="c-ns"><button type="button" className="nsb" onClick={() => onNs(b.namespace)} title={`${b.namespace} · show only this namespace`}>{name}</button></td>
      {!one && <td className="c-p">{who ? <><Who addr={who} /><CopyMark text={who} label="the publisher's address" /></> : "—"}</td>}
      <td className="c-sz num">{unit(bytes(b.blob_size))}</td>
      {/* one publisher's list is its escrow's statement: the fee went out of it */}
      <td className="c-fee num">{!b.charge ? "—" : one ? <Signed sign="−" utia={b.charge.fee_utia} dec={dec} /> : unit(tia(b.charge.fee_utia))}</td>
      {/* a settled blob's payment came out on chain */}
      {status && <td className="c-st"><span className="st"><i className="dot ok" />Success</span></td>}
      <td className="c-e num">
        {share == null ? "—" : (
          <span className="en" title={`${b.attested_with_rows != null ? `${int(b.attested_with_rows)} of ${int(b.validators_with_rows)} validators holding rows endorsed it. ` : ""}A settlement needs ⅔ of voting power.`}>
            <span className="pc">{pctOf(b.attested_voting_power!, b.total_voting_power!)}</span>
            <span className="em" aria-hidden="true"><i style={{ width: `${Math.min(100, share * 100).toFixed(2)}%` }} /></span>
          </span>
        )}
      </td>
      <td className="gap" aria-hidden="true" />
      <td className="tn">{ln && <Info label="Tensile" trigger={ln.word} title={ln.title}
        className={`lw${ln.tier === "hold" ? " hold" : ln.tier === "kept" ? " ok" : ""}`}><p>{ln.title}</p></Info>}</td>
      <td className="c-m">
        <span className="nm">{name}</span><span className="sz"><span className="sep">·</span>{bytes(b.blob_size)}</span>
        {/* one publisher's list names the fee where the others name the publisher */}
        {one ? b.charge && <span className="fe"><span className="sep">·</span>−{tiaAt(b.charge.fee_utia, dec)}</span> : who && <><span className="sep">·</span><Who addr={who} /></>}
      </td>
    </tr>
  );
});

type FailedProps = { f: FailedPayment; age: string | null; fresh: boolean; status: boolean; onNs: (ns: string) => void; onOpen: (e: React.MouseEvent, href: string) => void };
/**
 * a blob payment that failed in its block: its height, time, transaction, namespace and publisher as a blob's; Failed
 * where the list says how a payment came out (the Status column, or without one the endorsement), its reason on hover;
 * a quiet dash for its size, fee and endorsement and in Tensile's lane, since it moved nothing and there is nothing to
 * read. The whole row opens its transaction. On a phone, Failed stands where a blob's endorsement does
 */
const FailedRow = memo(function FailedRow({ f, age: ag, fresh, status, onNs, onOpen }: FailedProps) {
  const href = `/tx/?hash=${f.settlement_tx_hash}`;
  const tx = f.settlement_tx_hash.toUpperCase();
  const ns = f.namespace;
  const name = ns ? nsDisplay(ns) : null;
  const who = f.publisher;
  const dash = <span className="na">—</span>;
  const failed = <span className="st f" title={failedTitle(f.reason)}><i className="dot fault" />Failed</span>;
  return (
    // The whole row opens the transaction; links and buttons keep their own.
    <tr className={`row xf${fresh ? " fresh" : ""}`} data-h={keyOf(f)}
      onClick={(e) => openRow(e, href, onOpen)} onAuxClick={(e) => openRow(e, href, onOpen)}>
      <td className="c-h">{int(f.settlement_height)}</td>
      <td className="c-t"><span title={utcWord(f.settlement_time)}><span className="tm">{monthDayTime(f.settlement_time)}</span>{ag && <span className="ag">{ag}</span>}</span></td>
      <td className="c-b">
        <Link href={href} title={`Transaction ${tx}${f.promise_hash ? `\nPromise hash ${f.promise_hash}` : ""}`} aria-label={`Failed transaction ${tx.slice(0, 10)}, height ${int(f.settlement_height)}`}>{tx.slice(0, ID_ENDS)}<span className="el">…</span>{tx.slice(-TX_END)}</Link>
        <CopyMark text={tx} label="the transaction hash" />
        <span className="ht">#{int(f.settlement_height)}</span>
      </td>
      <td className="c-ns">{ns ? <button type="button" className="nsb" onClick={() => onNs(ns)} title={`${ns} · show only this namespace`}>{name}</button> : dash}</td>
      <td className="c-p">{who ? <><Who addr={who} /><CopyMark text={who} label="the publisher's address" /></> : dash}</td>
      <td className="c-sz num">{dash}</td>
      <td className="c-fee num">{dash}</td>
      {status && <td className="c-st">{failed}</td>}
      <td className="c-e num">{status ? dash : failed}</td>
      <td className="gap" aria-hidden="true" />
      <td className="tn">{dash}</td>
      <td className="c-m">
        {name && <span className="nm">{name}</span>}
        {who && <>{name && <span className="sep">·</span>}<Who addr={who} /></>}
      </td>
    </tr>
  );
});

/** the first page's places while it loads: a page of rows, each cell's shape at its size */
function Placeholders({ rows, one, status }: { rows: number; one: boolean; status: boolean }) {
  return (
    <>
      {Array.from({ length: rows }, (_, i) => (
        <tr key={i} className="row sk" aria-hidden="true">
          <td className="c-h"><span className="wait">1,204,085</span></td>
          <td className="c-t"><span className="wait">Sep 28 20:48:38</span></td>
          <td className="c-b"><span className="wait">AJ+0/l…kd8FQ</span><span className="ht"><span className="wait">#1,204,085</span></span></td>
          <td className="c-ns"><span className="wait">sov-niko-a</span></td>
          {!one && <td className="c-p"><span className="wait">celestia ••• 9snr</span></td>}
          <td className="c-sz num"><span className="wait">16.0 MiB</span></td>
          <td className="c-fee num"><span className="wait">3.530 TIA</span></td>
          {status && <td className="c-st"><span className="wait">Success</span></td>}
          <td className="c-e num"><span className="wait">69.88%</span></td>
          <td className="gap" />
          <td className="tn" />
          <td className="c-m"><span className="wait">sov-niko-a · 16.0 MiB · {one ? "3.530 TIA" : "9snr"}</span></td>
        </tr>
      ))}
    </>
  );
}

export default function Ledger({ feed, size, live, skew, onePublisher = false, status = false, moves, emptyText, onNs, children }: {
  feed: Feed;
  /** the rows a page holds: as many places are kept while the first one loads */
  size: number;
  /** the first page: new blobs come in */
  live: boolean;
  /** the observer's clock minus the reader's, for the ages */
  skew: number;
  /** the list is one publisher's, on its page: no Publisher column, which would name it on every row */
  onePublisher?: boolean;
  /** the Blobs list: a Status column, how each payment came out on chain, with the failed ones its feed holds among the blobs */
  status?: boolean;
  /** one publisher's escrow movements, set between its blobs by time */
  moves?: Moves;
  /** what the empty list says, in place of "No blob recorded" */
  emptyText?: React.ReactNode;
  onNs: (ns: string) => void;
  /** the pager, under the table; it counts what the table shows: its rows, any movements placed among them, and the failed payments counted in total */
  children?: (total: number, placed?: Placed, failed?: number) => React.ReactNode;
}) {
  const [motion, setMotion] = useState(true);
  useEffect(() => { setMotion(!reducedMotion()); }, []);

  // ---- holding: while the reader is in the list, what is on screen stays ----
  const wrapRef = useRef<HTMLDivElement>(null);
  const bodyRef = useRef<HTMLTableSectionElement>(null);
  const [pointerIn, setPointerIn] = useState(false);
  const [focusIn, setFocusIn] = useState(false);
  // scrolled past the list's first line: the reader is reading it
  const [away, setAway] = useState(false);
  useEffect(() => {
    let raf = 0;
    const check = () => { raf = 0; const first = bodyRef.current?.firstElementChild; setAway(!!first && first.getBoundingClientRect().top < 0); };
    const on = () => { if (!raf) raf = requestAnimationFrame(check); };
    check();
    window.addEventListener("scroll", on, { passive: true });
    window.addEventListener("resize", on);
    return () => { cancelAnimationFrame(raf); window.removeEventListener("scroll", on); window.removeEventListener("resize", on); };
  }, []);
  // focus on something the list has since taken away (the pill, a replaced row) leaves without a blur in some browsers: it no longer holds
  useEffect(() => {
    if (focusIn && !wrapRef.current?.contains(document.activeElement)) setFocusIn(false);
  }, [feed, focusIn]);
  const hold = live && (pointerIn || focusIn || away);

  const [shown, setShown] = useState<Shown>(() => ({ path: feed.path, rows: feed.rows, total: feed.total, failed: feed.failed, loaded: feed.loaded, move: null }));
  useEffect(() => {
    setShown((v) => {
      // a new page or filter: the rows on screen stay, quieted, until its answer (or its error) comes, so the list keeps its size
      if (feed.path !== v.path) return v.loaded && !feed.loaded && !feed.error ? v : take(v, feed);
      // new rows of the same list wait while the reader holds them
      return !v.loaded || !hold ? take(v, feed) : v;
    });
  }, [feed, hold]);
  const waiting = feed.path !== shown.path;
  // the blobs that came while the list held still: a failed payment is no blob, and comes in with them
  const pending = live && feed.loaded && feed.path === shown.path ? Math.max(0, feed.total - feed.failed - (shown.total - shown.failed)) : 0;
  const bringIn = () => {
    setShown((v) => take(v, feed));
    const top = wrapRef.current?.getBoundingClientRect().top ?? 0;
    if (top < 0) window.scrollBy({ top: top - 12, behavior: motion ? "smooth" : "auto" });
  };

  // ---- the move: the rows that were there slide down by what came in, once, and what came in glows once ----
  useLayoutEffect(() => {
    const tb = bodyRef.current, mv = shown.move;
    if (!tb || !mv || !motion) return;
    const trs = [...tb.children] as HTMLElement[];
    const firstOld = trs.find((tr) => (tr.dataset.h && !mv.fresh.has(tr.dataset.h)) || tr.dataset.m);
    if (!firstOld) return;
    const shift = firstOld.offsetTop - trs[0].offsetTop;
    if (shift <= 0) return;
    const anims = trs.map((tr) => tr.animate([{ transform: `translateY(${-shift}px)` }, { transform: "none" }], { duration: SLIDE_MS, easing: "cubic-bezier(.2, .8, .2, 1)" }));
    return () => anims.forEach((a) => a.cancel());
  }, [shown.move?.id]); // eslint-disable-line react-hooks/exhaustive-deps

  // ---- the ages, on the observer's clock: every second while the newest is under a minute old ----
  const [now, setNow] = useState(0);
  const skewRef = useRef(skew);
  skewRef.current = skew;
  useEffect(() => {
    let t: number | undefined;
    const tick = () => {
      const n = Date.now() + skewRef.current;
      setNow(n);
      const newest = shown.rows[0] ? Date.parse(shown.rows[0].settlement_time) : 0;
      t = window.setTimeout(tick, n - newest < 60000 ? 1000 - (n % 1000) : 15000);
    };
    tick();
    return () => window.clearTimeout(t);
  }, [shown.rows]);

  const router = useRouter();
  const onOpen = useCallback((e: React.MouseEvent, href: string) => {
    // a plain click stays in the app; a modified or middle click is the browser's
    if (e.defaultPrevented || e.button !== 0 || e.metaKey || e.ctrlKey || e.shiftKey || e.altKey) return;
    router.push(href);
  }, [router]);

  // ---- one publisher's list: the namespace column as wide as the longest name on the page, so no empty gap
  // stretches before Blob size; the time, centred, takes what that leaves, and every column after it stays put. The
  // width is set on the list (.lg-list), so the escrow's own statement there keeps the same columns ----
  const tableRef = useRef<HTMLTableElement>(null);
  useLayoutEffect(() => {
    const t = tableRef.current;
    if (!onePublisher || !t) return;
    const host = t.closest<HTMLElement>(".lg-list") ?? t;
    const fit = () => {
      const w = Math.max(0, ...[...t.querySelectorAll<HTMLElement>("td.c-ns .nsb")].map((b) => b.scrollWidth));
      if (w) host.style.setProperty("--ns-w", `${Math.ceil(w)}px`);
    };
    fit();
    // again once the table's face has loaded: the names had the fallback's widths until then
    document.fonts?.ready.then(fit);
  }, [onePublisher, shown.rows]);

  // the rows on screen with the movements of their own stretch of time between them, newest first; a block's own order
  // breaks a tie. They are placed by the rows on screen, so rows held still keep the movements that stand among them.
  const placed = useMemo(() => (moves && shown.loaded ? moves.place(shown.path, shown.rows.filter(isBlob), shown.total) : undefined), [moves, shown]);
  const between = placed?.list;
  const items = useMemo(() => {
    const blobs = shown.rows.map((b) => ({ b, m: null, t: Date.parse(b.settlement_time), i: moves?.rank.get(keyOf(b)) ?? -1 }));
    if (!between?.length) return blobs;
    return [...blobs, ...between.map((m) => ({ b: null, m, t: Date.parse(m.time), i: m.idx }))].sort((x, y) => y.t - x.t || x.i - y.i);
  }, [shown.rows, between, moves?.rank]);
  // the amounts' one precision, over the page
  const dec = onePublisher ? decimals([...shown.rows.map((b) => (isBlob(b) ? b.charge?.fee_utia ?? 0 : 0)), ...(between ?? []).map((m) => m.utia)]) : 3;

  const fresh = motion ? shown.move?.fresh : undefined;
  const cols = (onePublisher ? 9 : 10) + (status ? 1 : 0);
  return (
    <>
      <div className="lg-wrap" ref={wrapRef}
        onPointerEnter={(e) => { if (e.pointerType === "mouse") setPointerIn(true); }}
        onPointerLeave={() => setPointerIn(false)}
        onFocus={() => setFocusIn(true)}
        onBlur={(e) => { if (!e.currentTarget.contains(e.relatedTarget as Node)) setFocusIn(false); }}>
        <div className="lg-newbar">
          {hold && pending > 0 && (
            <button type="button" className="lg-new" onClick={bringIn}>
              <i className="d" aria-hidden="true" /><b>{int(pending)}</b> new blob{pending === 1 ? "" : "s"}
              <svg width="12" height="12" viewBox="0 0 12 12" aria-hidden="true"><path d="M6 10V2.5M2.8 5.5 6 2.3l3.2 3.2" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" /></svg>
            </button>
          )}
        </div>
        <div className={`lg-tw${waiting ? " is-waiting" : ""}`} aria-busy={waiting || !shown.loaded}>
          {!shown.loaded && !feed.error && <span className="sr-only">Loading…</span>}
          <table ref={tableRef} className={`lg-t${onePublisher ? " lg-one" : ""}${status ? " lg-st" : ""}`}>
            <LedgerHead one={onePublisher} status={status} />
            <tbody ref={bodyRef}>
              {!shown.loaded && (feed.error
                ? <tr className="lg-empty"><td colSpan={cols}>{feed.refused ? `${feed.error.charAt(0).toUpperCase()}${feed.error.slice(1)}.` : `The observer API is not answering (${feed.error}).`}</td></tr>
                : <Placeholders rows={size} one={onePublisher} status={status} />)}
              {shown.loaded && shown.rows.length === 0 && <tr className="lg-empty"><td colSpan={cols}>{emptyText ?? <>No blob recorded{shown.path.includes("&namespace=") || (!onePublisher && shown.path.includes("&publisher=")) ? " with this filter" : ""}.</>}</td></tr>}
              {items.map(({ b, m, t }) => b
                ? isFailed(b)
                  ? <FailedRow key={keyOf(b)} f={b} age={now ? age(now - t) : null} fresh={!!fresh?.has(keyOf(b))} status={status} onNs={onNs} onOpen={onOpen} />
                  : <Row key={b.promise_hash} b={b} age={now ? age(now - t) : null} fresh={!!fresh?.has(b.promise_hash)} one={onePublisher} dec={dec} status={status} onNs={onNs} onOpen={onOpen} />
                : <MoveRow key={m!.key} m={m!} age={now ? age(now - t) : null} dec={dec} />)}
            </tbody>
          </table>
        </div>
      </div>
      {shown.loaded
        ? children?.(shown.total, placed, shown.failed)
        : !feed.error && (
          <div className="pager" aria-hidden="true">
            <span className="count"><span className="wait">Showing 1–25 of 0,000 settlements on record</span></span>
            <span className="ctl"><span className="wait lg-ctl-wait">First ‹ Page 1 of 000 › Last</span></span>
          </div>
        )}
    </>
  );
}
