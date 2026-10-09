"use client";
import { memo, useCallback, useEffect, useLayoutEffect, useMemo, useRef, useState } from "react";
import Link from "next/link";
import { useRouter } from "next/navigation";
import { API_BASE, type Blob, type EscrowRow, type TxCost as Cost, int, bytes, tia, pctOf, nsDisplay, utcWord } from "@/lib/api";
import { lane } from "@/lib/status";
import { openRow } from "@/lib/row";
import { unit } from "@/components/Unit";
import { Eye } from "@/components/Metrics";
import { Frac } from "@/components/CurrentProviders";
import Ident from "@/components/Ident";
import Info from "@/components/Info";
import { blobIdOf } from "@/lib/blobkey";
import TxCost from "@/components/TxCost";

/** how many of the blob ID's first and last characters the Blob column shows */
const ID_ENDS = 6;
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
  rows: Blob[];
  /** the settlements the filters select, all of them */
  total: number;
  loaded: boolean;
  error: string | null;
  /** the API refused the request itself (a 400: a filter that is no namespace or account), which is no outage and is not asked again */
  refused: boolean;
  /** when the newest row arrived, on the observer's clock (when it settled, on the first read) */
  lastNewAt: number;
  /** one publisher's list asked with_escrow=1: its escrow rows that stand among this page, and their counts */
  escrow: Escrow | null;
};
/**
 * the escrow rows the API placed among a page of one publisher's blobs, newest first in chain order: rows, how many are
 * newer than the page (newer), all of them (total), and how many past its cap the page leaves out (more)
 */
export type Escrow = { rows: EscrowRow[]; newer: number; total: number; more: number };
const empty = (path: string): Feed => ({ path, rows: [], total: 0, loaded: false, error: null, refused: false, lastNewAt: 0, escrow: null });

/** a failed read, with the status the API answered (0: no answer) */
class ReadError extends Error {
  constructor(message: string, readonly status: number) { super(message); }
}

async function readPage(path: string): Promise<{ blobs: Blob[]; total: number; escrow: Escrow | null }> {
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
    const escrow: Escrow | null = Array.isArray(j.escrow)
      ? { rows: j.escrow, newer: j.escrow_newer ?? 0, total: j.escrow_total ?? j.escrow.length, more: j.escrow_more ?? 0 }
      : null;
    return { blobs: Array.isArray(j.blobs) ? j.blobs : [], total: typeof j.total === "number" ? j.total : 0, escrow };
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
      const known = new Set(before?.rows.map((b) => b.promise_hash));
      const came = !!before && page.blobs.some((b) => !known.has(b.promise_hash));
      const lastNewAt = !before ? (page.blobs[0] ? Date.parse(page.blobs[0].settlement_time) : 0) : came ? Date.now() + skewRef.current : before.lastNewAt;
      apply({ path: p, rows: page.blobs, total: page.total, loaded: true, error: null, refused: false, lastNewAt, escrow: page.escrow });
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
type Shown = { path: string; rows: Blob[]; total: number; loaded: boolean; escrow: Escrow | null; move: { id: number; fresh: Set<string> } | null };

/** the feed onto the screen; the rows it did not show before are the move's */
function take(v: Shown, f: Feed): Shown {
  if (f.path !== v.path || !v.loaded) return { path: f.path, rows: f.rows, total: f.total, loaded: f.loaded, escrow: f.escrow, move: null };
  if (f.rows === v.rows && f.total === v.total && f.escrow === v.escrow) return v;
  const known = new Set(v.rows.map((b) => b.promise_hash));
  const fresh = new Set(f.rows.filter((b) => !known.has(b.promise_hash)).map((b) => b.promise_hash));
  return { path: f.path, rows: f.rows, total: f.total, loaded: true, escrow: f.escrow, move: fresh.size ? { id: (v.move?.id ?? 0) + 1, fresh } : v.move };
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
  /** its place in its block, where the API gives it: a payout's is −1, the chain paid it before the block's transactions */
  txIndex?: number;
  msgIndex?: number;
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
  /** its transaction and what it cost, for the card its kind opens (none for a payout, which no transaction made) */
  hash?: string;
  cost?: Cost;
  /** a transaction that failed: it moved nothing, so it has no amount (utia 0, no sign) */
  fail?: Fail;
};
/**
 * What a failed row says: why (the chain's reason, when Tensile has one), and what it asked for, exact and in words
 * ("requested 1,000,000,000 TIA"; a blob payment's blob size, its namespace on hover); amount: the request alone, for
 * the struck alternative. Its whole row opens the failed page.
 */
export type Fail = { reason?: string; ask: string; askTitle?: string; amount?: string; href: string; aria: string };
/** how a failed row's request reads: B, in words under its reason, the Amount a dash; A, struck in the Amount column */
export type FailStyle = "words" | "struck";
/** the movements that stand among a page's rows, and the rows and movements that page shows, counted from the list's first */
export type Placed = { list: Move[]; range: [number, number]; more?: number; count?: number };
/** what a page shows, as one read gave it: the path, its blobs, the count of all of them, and any escrow rows the API placed among them */
export type PageRead = { path: string; rows: Blob[]; total: number; escrow: Escrow | null };
/**
 * One publisher's movements against the rows on screen: place sets them by those rows (the page's path, its rows, the
 * count of all of them and, from an API that places them, its escrow rows, as one read gave them), so a page that holds
 * still keeps its movements in step with its rows; rank is each blob's place among the same payments, which keeps a
 * block's own order.
 */
export type Moves = { place: (page: PageRead) => Placed; rank: Map<string, number> };

/**
 * the columns' heads; one publisher's list names no publisher, its rows are its transactions, and its fee column is the
 * escrow's statement. The statement alone (escrow) heads what its rows hold: the kind of movement and its amount, the
 * blobs' columns left unnamed (they stay, so nothing moves when the kind changes)
 */
export function LedgerHead({ one, escrow = false }: { one: boolean; escrow?: boolean }) {
  if (escrow) {
    return (
      <thead>
        <tr>
          <th className="c-h">Height</th>
          <th className="c-t">Time <span className="per">(UTC)</span></th>
          <th className="c-b">Type</th>
          <th className="c-ns" aria-hidden="true" />
          <th className="c-sz num" aria-hidden="true" />
          <th className="c-fee num" title="What each movement put into the escrow (+) or took out of it (−). A failed one moved nothing.">Amount</th>
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
        <th className="c-b">Blob ID</th>
        <th className="c-ns">Namespace</th>
        {!one && <th className="c-p">Publisher</th>}
        <th className="c-sz num">Blob size</th>
        {one
          ? <th className="c-fee num" title="What each transaction moved into the escrow (+) or out of it (−): a blob's fee, a deposit, a withdrawal paid out. A failed one moved nothing.">Amount</th>
          : <th className="c-fee num">Fee paid</th>}
        <th className="c-e num" title="Share of voting power whose signature on the settlement verified. A settlement needs ⅔.">Endorsed <Frac /></th>
        <th className="gap" aria-hidden="true" />
        <th className="tn" title="Tensile's own reading of each blob, once, near the end of its retention window."><span><Eye />Tensile</span></th>
      </tr>
    </thead>
  );
}

/** the failed page's own words for a failure, on its mark */
const FAILED_TITLE = "This transaction failed in this block: none of its messages took effect.";

/**
 * An escrow movement as a row: its height and time as a blob's, its kind where the blob is, what qualifies it over the
 * namespace and the size, its amount signed; no endorsement, nothing for Tensile to read. In the list's two lines, its
 * qualifier and its amount close the second line, as a blob's fee closes its own. Its kind opens its transaction's gas
 * and fee, where they are on record. A failed one is a row of its own (FailRow).
 */
export const MoveRow = memo(function MoveRow({ m, age: ag, dec, owner, failStyle = "words" }: { m: Move; age: string | null; dec: number; owner?: string; failStyle?: FailStyle }) {
  if (m.fail) return <FailRow m={m} f={m.fail} age={ag} style={failStyle} />;
  return (
    <tr className={`row mv${m.tone ? ` ${m.tone}` : ""}`} data-m={m.key}>
      <td className="c-h">{int(m.height)}</td>
      <td className="c-t"><span title={utcWord(m.time)}><span className="tm">{monthDayTime(m.time)}</span>{ag && <span className="ag">{ag}</span>}</span></td>
      <td className="c-b">{m.cost
        ? <TxCost word={m.word} label={m.word} cost={m.cost} hash={m.hash} owner={owner} className="k" />
        : <span className="k" title={m.word}>{m.word}</span>}<span className="ht">#{int(m.height)}</span></td>
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

/**
 * A transaction that failed, among the movements: its height and time, its kind in the quieter grey ("Withdrawal
 * request": it never happened), the red mark and why over the namespace and the size, with what it asked for under
 * them; its Amount a dash, since nothing moved. The struck alternative keeps the qualifier to the reason and strikes
 * the request in the Amount column, unsigned. The whole row opens the failed page, where its gas, fee and error are.
 */
function FailRow({ m, f, age: ag, style }: { m: Move; f: Fail; age: string | null; style: FailStyle }) {
  const router = useRouter();
  const go = (e: React.MouseEvent, href: string) => {
    if (e.defaultPrevented || e.button !== 0 || e.metaKey || e.ctrlKey || e.shiftKey || e.altKey) return;
    router.push(href);
  };
  const words = style === "words";
  const mark = <span className="xs" title={FAILED_TITLE}><i className="dot fault" aria-hidden="true" />Failed</span>;
  return (
    <tr className={`row mv xf${words ? " xw" : ""}`} data-f={m.key} aria-label={f.aria}
      onClick={(e) => openRow(e, f.href, go)} onAuxClick={(e) => openRow(e, f.href, go)}>
      <td className="c-h">{int(m.height)}</td>
      <td className="c-t"><span title={utcWord(m.time)}><span className="tm">{monthDayTime(m.time)}</span>{ag && <span className="ag">{ag}</span>}</span></td>
      <td className="c-b"><span className="k">{m.word}</span><span className="ht">#{int(m.height)}</span></td>
      <td className="c-q" colSpan={2}>
        <span className="xl">{mark}{f.reason && <><span className="sep">·</span><span className="xy">{f.reason}</span></>}</span>
        {words && <span className="xa" title={f.askTitle}>{f.ask}</span>}
      </td>
      <td className="c-fee num">{words || !f.amount
        ? <span className="xd" title="Nothing moved: the escrow is as it was.">—</span>
        : <s className="xk" title="Requested, not moved: the escrow is as it was.">{unit(f.amount)}</s>}</td>
      <td className="c-e" /><td className="gap" aria-hidden="true" /><td className="tn" />
      <td className="c-m">
        <span className="q">{mark}{f.reason && <><span className="sep">·</span>{f.reason}</>}</span><span className="sep">·</span>
        <span className="xd" title="Nothing moved: the escrow is as it was.">—</span>
      </td>
    </tr>
  );
}

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

type RowProps = { b: Blob; age: string | null; fresh: boolean; one: boolean; dec: number; onNs: (ns: string) => void; onOpen: (e: React.MouseEvent, href: string) => void };
/** one blob: the cells of the table, and the second line a phone shows under the first */
const Row = memo(function Row({ b, age: ag, fresh, one, dec, onNs, onOpen }: RowProps) {
  const href = `/blob/?hash=${b.promise_hash}`;
  const id = blobIdOf(b.commitment, b.blob_version ?? 0);
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
        {/* the blob as the client names it, its blob ID's first and last characters; the row opens this settlement (its
            promise hash), and the copy is the blob ID */}
        <Link href={href} title={`Blob ID ${id}\nPromise hash ${b.promise_hash}`} aria-label={`Blob ${id.slice(0, 10)}, height ${int(b.settlement_height)}`}>{id.slice(0, ID_ENDS)}<span className="el">…</span>{id.slice(-ID_ENDS)}</Link>
        <CopyMark text={id} label="the blob ID" />
        <span className="ht">#{int(b.settlement_height)}</span>
      </td>
      <td className="c-ns"><button type="button" className="nsb" onClick={() => onNs(b.namespace)} title={`${b.namespace} · show only this namespace`}>{name}</button></td>
      {!one && <td className="c-p">{who ? <Who addr={who} /> : "—"}</td>}
      <td className="c-sz num">{unit(bytes(b.blob_size))}</td>
      {/* one publisher's list is its escrow's statement: the fee went out of it */}
      <td className="c-fee num">{!b.charge ? "—" : one ? <Signed sign="−" utia={b.charge.fee_utia} dec={dec} /> : unit(tia(b.charge.fee_utia))}</td>
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

/** the first page's places while it loads: a page of rows, each cell's shape at its size */
function Placeholders({ rows, one }: { rows: number; one: boolean }) {
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
          <td className="c-e num"><span className="wait">69.88%</span></td>
          <td className="gap" />
          <td className="tn" />
          <td className="c-m"><span className="wait">sov-niko-a · 16.0 MiB · {one ? "3.530 TIA" : "9snr"}</span></td>
        </tr>
      ))}
    </>
  );
}

export default function Ledger({ feed, size, live, skew, onePublisher = false, moves, owner, failStyle, emptyText, onNs, children }: {
  feed: Feed;
  /** the rows a page holds: as many places are kept while the first one loads */
  size: number;
  /** the first page: new blobs come in */
  live: boolean;
  /** the observer's clock minus the reader's, for the ages */
  skew: number;
  /** the list is one publisher's, on its page: no Publisher column, which would name it on every row */
  onePublisher?: boolean;
  /** one publisher's escrow movements, set between its blobs by time */
  moves?: Moves;
  /** the publisher whose list it is, for whose fee a movement's card names: its own, or another account's */
  owner?: string;
  /** how a failed row's request reads (the gallery's two alternatives) */
  failStyle?: FailStyle;
  /** what the empty list says, in place of "No blob recorded" */
  emptyText?: React.ReactNode;
  onNs: (ns: string) => void;
  /** the pager, under the table; it counts what the table shows: its rows, and any movements placed among them */
  children?: (total: number, placed?: Placed) => React.ReactNode;
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

  const [shown, setShown] = useState<Shown>(() => ({ path: feed.path, rows: feed.rows, total: feed.total, loaded: feed.loaded, escrow: feed.escrow, move: null }));
  useEffect(() => {
    setShown((v) => {
      // a new page or filter: the rows on screen stay, quieted, until its answer (or its error) comes, so the list keeps its size
      if (feed.path !== v.path) return v.loaded && !feed.loaded && !feed.error ? v : take(v, feed);
      // new rows of the same list wait while the reader holds them
      return !v.loaded || !hold ? take(v, feed) : v;
    });
  }, [feed, hold]);
  const waiting = feed.path !== shown.path;
  const pending = live && feed.loaded && feed.path === shown.path ? Math.max(0, feed.total - shown.total) : 0;
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
    const firstOld = trs.find((tr) => (tr.dataset.h && !mv.fresh.has(tr.dataset.h)) || tr.dataset.m || tr.dataset.f);
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
  const placed = useMemo(() => (moves && shown.loaded ? moves.place({ path: shown.path, rows: shown.rows, total: shown.total, escrow: shown.escrow }) : undefined), [moves, shown]);
  const between = placed?.list;
  const items = useMemo(() => {
    const blobs = shown.rows.map((b) => ({ b, m: null, t: Date.parse(b.settlement_time), h: b.settlement_height, x: b.settlement_tx_index as number | undefined, g: undefined as number | undefined, i: moves?.rank.get(b.promise_hash) ?? -1 }));
    if (!between?.length) return blobs;
    // chain order, newest first: the block, then the place in it where both have one (a payout's −1 before the block's
    // transactions), then the message; else, as before, the place among the payments
    return [...blobs, ...between.map((m) => ({ b: null, m, t: Date.parse(m.time), h: m.height, x: m.txIndex, g: m.msgIndex, i: m.idx }))]
      .sort((p, q) => q.h - p.h || (p.x != null && q.x != null && q.x - p.x) || (p.g != null && q.g != null && q.g - p.g) || q.t - p.t || p.i - q.i);
  }, [shown.rows, between, moves?.rank]);
  // the amounts' one precision, over the page
  const dec = onePublisher ? decimals([...shown.rows.map((b) => b.charge?.fee_utia ?? 0), ...(between ?? []).filter((m) => !m.fail).map((m) => m.utia)]) : 3;

  const fresh = motion ? shown.move?.fresh : undefined;
  const cols = onePublisher ? 9 : 10;
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
          <table ref={tableRef} className={`lg-t${onePublisher ? " lg-one" : ""}`}>
            <LedgerHead one={onePublisher} />
            <tbody ref={bodyRef}>
              {!shown.loaded && (feed.error
                ? <tr className="lg-empty"><td colSpan={cols}>{feed.refused ? `${feed.error.charAt(0).toUpperCase()}${feed.error.slice(1)}.` : `The observer API is not answering (${feed.error}).`}</td></tr>
                : <Placeholders rows={size} one={onePublisher} />)}
              {shown.loaded && shown.rows.length === 0 && <tr className="lg-empty"><td colSpan={cols}>{emptyText ?? <>No blob recorded{shown.path.includes("&namespace=") || (!onePublisher && shown.path.includes("&publisher=")) ? " with this filter" : ""}.</>}</td></tr>}
              {items.map(({ b, m, t }) => b
                ? <Row key={b.promise_hash} b={b} age={now ? age(now - t) : null} fresh={!!fresh?.has(b.promise_hash)} one={onePublisher} dec={dec} onNs={onNs} onOpen={onOpen} />
                : <MoveRow key={m!.key} m={m!} age={now ? age(now - t) : null} dec={dec} owner={owner} failStyle={failStyle} />)}
            </tbody>
          </table>
        </div>
      </div>
      {shown.loaded
        ? children?.(shown.total, placed)
        : !feed.error && (
          <div className="pager" aria-hidden="true">
            <span className="count"><span className="wait">Showing 1–25 of 0,000 settlements on record</span></span>
            <span className="ctl"><span className="wait lg-ctl-wait">First ‹ Page 1 of 000 › Last</span></span>
          </div>
        )}
    </>
  );
}
