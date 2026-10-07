"use client";
import { Suspense, useCallback, useEffect, useMemo, useState } from "react";
import { useSearchParams } from "next/navigation";
import { API_BASE, useApi, notFound, badRequest, throttled, hhmm, ago, type Blob, type Payment, type PublisherNamespace, type RecentBlob, type Tip, type Window, blobFee, bytes, int, nsDisplay, span, tia, utcWord, TIP_MS } from "@/lib/api";
import type { Params, PublisherWithQueue, PublisherWithdrawals } from "@/lib/withdrawals";
import { lane } from "@/lib/status";
import Ledger, { useLedger, LedgerHead, MoveRow, Signed, decimals, type Move, type Moves, type Placed } from "@/components/Ledger";
import Pager, { usePage } from "@/components/Pager";
import Picker, { type Choice } from "@/components/Picker";
import Ident from "@/components/Ident";
import Copy from "@/components/Copy";
import { unit } from "@/components/Unit";
import { nsHex, NsName, NS_ICON } from "@/components/Namespace";
import { Eye } from "@/components/Metrics";
import { age, monthDayTime } from "@/components/BlobsDeck";
import Warn from "@/components/Warn";

/** the publisher's figures over one of the periods, with the namespaces of its settlements in it (the most used first, at most 10) */
type Span = {
  window: Window; settlements: number; bytes: number; fees_utia: number; timeouts: number; paid_per_mib_utia: number | null;
  namespaces?: PublisherNamespace[]; namespaces_total?: number;
};

type Detail = {
  window: Window;
  publisher: PublisherWithQueue;
  /** the escrow withdrawal queue as last read from state; null until read */
  withdrawals: PublisherWithdrawals | null;
  windows: Span[];
  /** the newest 100 escrow movements of every kind, blob fees included */
  recent_payments: Payment[];
  recent_blobs: RecentBlob[];
};

/** rows per page of the blobs, and of the escrow's statement */
const SIZE = 25;
/** the last page /v1/blobs serves: its offset stops at 100,000 */
const MAX_OFFSET = 100000;
const MAX_PAGE = Math.floor(MAX_OFFSET / SIZE) + 1;
/** recent_payments stops at this many: a history that long may be missing its oldest movements */
const PAYMENTS_CAP = 100;
/** a publisher's blobs read at once to say things of every one of them, where the API does not say them */
const AT_ONCE = 100;

/** what the list shows: every transaction, the blobs alone, or the escrow's movements alone */
type Kind = "all" | "blobs" | "escrow";
const KINDS: [Kind, string][] = [["all", "All"], ["blobs", "Blobs"], ["escrow", "Escrow"]];

/** a page of /v1/blobs, newest first, with the count of every blob its filter matches */
async function blobsAt(query: string): Promise<{ blobs: Blob[]; total: number }> {
  const r = await fetch(`${API_BASE}/v1/blobs?${query}`, { cache: "no-store" });
  if (!r.ok) throw new Error(String(r.status));
  return r.json();
}

/**
 * A publisher's first blob: the last of its blobs, newest first, read at the
 * offset its count gives. A blob that lands between the count and the read
 * moves that row, so the read's own count has to agree, or the read is made
 * again at the offset that count gives. Kept once read: a first blob does
 * not change. Undefined while it is read; null past the last offset the API
 * serves, or when the reads fail or never agree.
 */
function useFirstBlob(addr: string, total: number | null): Blob | null | undefined {
  const [first, setFirst] = useState<Blob | null | undefined>(undefined);
  const ask = total != null && first === undefined;
  useEffect(() => {
    if (!ask) return;
    let gone = false;
    (async () => {
      for (let t = total!, i = 0; i < 3; i++) {
        if (t - 1 > MAX_OFFSET) return null;
        const j = await blobsAt(`publisher=${addr}&limit=1&offset=${t - 1}`);
        if (j.total === t) return j.blobs[0] ?? null;
        t = j.total;
      }
      return null;
    })().then((b) => { if (!gone) setFirst(b); }, () => { if (!gone) setFirst(null); });
    return () => { gone = true; };
  }, [addr, ask]); // eslint-disable-line react-hooks/exhaustive-deps
  return first;
}

const plural = (n: number, w: string) => `${int(n)} ${w}${n === 1 ? "" : "s"}`;

/** "Oct 2 06:42": the minute is enough in a line; the second and UTC are on hover */
const monthDayMin = (s: string) => monthDayTime(s).slice(0, -3);

/** a payment's place in time: when, and in the same second its place among the payments, newest first */
type At = { t: number; i: number };
const olderThan = (a: At, b: At) => a.t < b.t || (a.t === b.t && a.i > b.i);
const atOf = (m: Move): At => ({ t: Date.parse(m.time), i: m.idx });

type Money = {
  /** the movements, newest first */
  list: Move[];
  /** each settlement's place among the payments, by its promise hash */
  rank: Map<string, number>;
  /** for each settlement, by its promise hash, the settlement just newer than it (null: none is) */
  prev: Map<string, At | null>;
};

/**
 * The account's escrow movements, newest first, each in plain words with a quiet qualifier (when a request becomes
 * payable, how long a payout took). The fee settlements are the blob rows and are not among them; rank is each one's
 * place among the payments, by its promise hash, so a movement in the same block as a blob keeps the chain's order,
 * and prev the settlement before each, which bounds the movements a page of blobs takes from above.
 */
function movesOf(d: Detail): Money {
  const left = d.withdrawals?.left_queue ?? [];
  const queue = [...left, ...(d.withdrawals?.pending ?? [])];
  const list: Move[] = [];
  const rank = new Map<string, number>();
  const prev = new Map<string, At | null>();
  let before: At | null = null;
  d.recent_payments.forEach((x, idx) => {
    if (x.kind === "settlement") {
      if (x.promise_hash) { rank.set(x.promise_hash, idx); prev.set(x.promise_hash, before); }
      before = { t: Date.parse(x.time), i: idx };
      return;
    }
    const t = Date.parse(x.time);
    let word: string = x.kind, qual = "", short = "", sign: Move["sign"] = "", tone = "", note: string | undefined;
    if (x.kind === "deposit") { word = "Deposit"; sign = "+"; }
    else if (x.kind === "withdrawal_request") {
      const w = queue.find((q) => Date.parse(q.requested_at) === t);
      const at = w?.available_at ?? x.available_at;
      word = "Withdrawal requested"; tone = "req";
      note = "Moved from available to the withdrawal queue; the balance changes when it is paid out.";
      if (at) { qual = `payable from ${monthDayTime(at)}`; short = `payable ${monthDayMin(at)}`; }
      // a request the account's own settlements used up, whole or in part, before it could be paid out
      if (w?.outcome === "consumed") { qual = "used by settlements, not paid out"; short = "used by settlements"; tone = "req hold"; }
      else if (w && w.reduced_utia > 0) qual = [qual, `${tia(w.reduced_utia)} of it used by settlements`].filter(Boolean).join(" · ");
    } else if (x.kind === "withdrawal_executed") {
      const w = left.find((q) => q.paid_height === x.height);
      word = "Withdrawal paid out"; sign = "−";
      if (w?.payout_delay_s != null) { qual = `${span(w.payout_delay_s)} after the request`; short = `${span(w.payout_delay_s)} after request`; }
    } else if (x.kind === "timeout") { word = "Timed-out promise"; sign = "−"; tone = "fault"; }
    list.push({ key: `${x.height}-${x.tx_hash ?? ""}-${idx}`, height: x.height, time: x.time, word, qual, short, sign, utia: x.amount_utia, tone, note, idx });
  });
  return { list, rank, prev };
}

/**
 * The movements a page of blobs takes, by the rows on it: those older than the settlement just before its first row
 * (the page before's oldest; the first page has none above it) and newer than its own oldest, the page that holds the
 * last row every older one. range: the rows and movements the page shows, counted from the list's first.
 */
function placeMoves(money: Money, path: string, rows: Blob[], total: number): Placed {
  const offset = Number(new URLSearchParams(path.slice(path.indexOf("?") + 1)).get("offset")) || 0;
  const at = (b: Blob): At => ({ t: Date.parse(b.settlement_time), i: money.rank.get(b.promise_hash) ?? -1 });
  // a first row the payments do not place (a blob newer than them) stands as its own bound
  const top = offset > 0 && rows.length > 0 ? money.prev.get(rows[0].promise_hash) ?? at(rows[0]) : null;
  const bottom = rows.length > 0 && offset + rows.length < total ? at(rows[rows.length - 1]) : null;
  const list = money.list.filter((m) => (!top || olderThan(atOf(m), top)) && (!bottom || !olderThan(atOf(m), bottom)));
  const before = top ? money.list.filter((m) => !olderThan(atOf(m), top)).length : 0;
  const from = offset + before + 1;
  return { list, range: [from, from + rows.length + list.length - 1] };
}

/**
 * The escrow's own statement: its movements alone, newest first, a page at a time, in the list's own columns, so
 * nothing moves when the kind changes; then what went in, what the blobs and any timed-out promise cost, what went
 * out, and the escrow that leaves, the labels up to the Amount column and the figures in it.
 */
function Statement({ d, moves, page, onPage, now }: { d: Detail; moves: Move[]; page: number; onPage: (p: number) => void; now: number }) {
  const e = d.publisher.escrow?.found ? d.publisher.escrow : null;
  const all = d.windows.find((w) => w.window.name === "all");

  // shown only when it closes exactly on the balance, a line only when something moved
  const foot: { label: string; utia: number; sign: string; tot?: boolean }[] = [];
  if (e && all) {
    const sum = (k: Payment["kind"]) => d.recent_payments.filter((x) => x.kind === k).reduce((s, x) => s + x.amount_utia, 0);
    const dep = sum("deposit"), out = sum("withdrawal_executed"), charged = sum("timeout");
    if (dep - all.fees_utia - charged - out === e.balance_utia) {
      foot.push({ label: "Deposited", utia: dep, sign: "+" });
      if (all.fees_utia) foot.push({ label: `Fees paid for ${plural(all.settlements, "settlement")}`, utia: all.fees_utia, sign: "−" });
      if (charged) foot.push({ label: "Charged for timed-out promises", utia: charged, sign: "−" });
      if (out) foot.push({ label: "Withdrawn", utia: out, sign: "−" });
      foot.push({ label: "Escrow now", utia: e.balance_utia, sign: "", tot: true });
    }
  }
  const dec = decimals([...moves.map((m) => m.utia), ...foot.map((f) => f.utia)]);
  const shown = moves.slice((page - 1) * SIZE, page * SIZE);
  return (
    <>
      <div className="lg-tw">
        <table className="lg-t lg-one pb-st">
          <LedgerHead one escrow />
          <tbody>
            {moves.length === 0 && <tr className="lg-empty"><td colSpan={9}>No escrow movement on record.</td></tr>}
            {shown.map((m) => <MoveRow key={m.key} m={m} age={age(now - Date.parse(m.time))} dec={dec} />)}
          </tbody>
          {foot.length > 0 && page === Math.ceil(moves.length / SIZE) && (
            <tfoot>
              {foot.map((f) => (
                <tr key={f.label} className={f.tot ? "tot" : undefined}>
                  <td colSpan={5} className="fl">{f.label}</td>
                  <td className="c-fee num"><Signed sign={f.sign} utia={f.utia} dec={dec} /></td>
                  <td className="c-e" /><td className="gap" aria-hidden="true" /><td className="tn" />
                </tr>
              ))}
            </tfoot>
          )}
        </table>
      </div>
      {moves.length > 0 && <Pager total={moves.length} page={page} size={SIZE} onPage={onPage} noun={moves.length === 1 ? "escrow movement" : "escrow movements"} />}
    </>
  );
}

/**
 * One publisher: whose account it is; under it, on the left, its four figures in one frame, a row each (the escrow it
 * has left now, and over its whole record its blobs, what it paid, and how many of its blobs Tensile found available);
 * on the right, in a frame of the same make, when it last and first posted and where; then its transactions, newest
 * first: its blobs as the Blobs list draws them, and its escrow movements between them.
 */
function Publisher({ addr }: { addr: string }) {
  const params = useSearchParams();
  const [page, setPageRaw] = usePage();
  const [kindPick, setKindPick] = useState<Kind>(() => { const k = params.get("kind"); return k === "blobs" || k === "escrow" ? k : "all"; });
  const [ns, setNsRaw] = useState((params.get("namespace") ?? "").trim().toLowerCase());
  // a new page opens at the list's top when the reader had scrolled past it
  const setPage = useCallback((p: number) => {
    setPageRaw(p);
    const el = document.getElementById("list");
    if (el && el.getBoundingClientRect().top < 0) el.scrollIntoView();
  }, [setPageRaw]);
  // a kind or a namespace starts from the first page, and lives in the address so a link keeps it
  const setView = useCallback((v: { kind?: Kind; ns?: string }) => {
    if (v.kind !== undefined) setKindPick(v.kind);
    if (v.ns !== undefined) setNsRaw(v.ns);
    setPageRaw(1);
    try {
      const u = new URL(window.location.href);
      if (v.kind !== undefined) { if (v.kind === "all") u.searchParams.delete("kind"); else u.searchParams.set("kind", v.kind); }
      if (v.ns !== undefined) { if (v.ns) u.searchParams.set("namespace", v.ns); else u.searchParams.delete("namespace"); }
      window.history.replaceState(null, "", u.pathname + u.search + u.hash);
    } catch { /* fine */ }
  }, [setPageRaw]);
  const setNs = useCallback((v: string) => setView({ ns: v }), [setView]);

  // the whole record: every figure on this page is the account's own over all of it, but its escrow, which is now
  const pub = useApi<Detail>(`/v1/publishers/${addr}?window=all`);
  // the price of a blob, for the low-escrow warning: the parameters' own 5-minute stream (PublishersTop's too), asked
  // again soon after a failure, so one failed first answer does not lose the warning for the life of the page
  const pf = useApi<Params>("/v1/params", 300000).data?.price_formula;
  const tip = useApi<Tip>("/v1/tip", TIP_MS); // the header's stream: no request of its own
  const skew = tip.data?.server_time && tip.fetchedAt ? Date.parse(tip.data.server_time) - Date.parse(tip.fetchedAt) : 0;
  const now = Date.now() + skew;

  // The escrow's history is whole while the API did not stop it at its cap: a busy account's deposits can fall out of
  // that cap, and a part of a history is never shown, so its list is its blobs, with a line to the API under them.
  const wholeMoney = !!pub.data && pub.data.recent_payments.length < PAYMENTS_CAP;
  const posted = !!pub.data?.windows.find((w) => w.window.name === "all")?.settlements;
  const money = useMemo(() => (pub.data && wholeMoney ? movesOf(pub.data) : null), [pub.data, wholeMoney]);
  const moveN = money?.list.length ?? 0;
  // the kind can be picked when there is some of each; an account that never posted has only its statement
  const split = posted && moveN > 0;
  const kind: Kind = split ? kindPick : "all";
  const statement = wholeMoney && (!posted || kind === "escrow");
  const offset = (Math.min(page, MAX_PAGE) - 1) * SIZE;
  const live = page === 1;
  const blobsPath = useCallback((o: number) => `/v1/blobs?limit=${SIZE}&offset=${o}&publisher=${addr}${ns ? `&namespace=${encodeURIComponent(ns)}` : ""}`, [addr, ns]);
  const feed = useLedger(blobsPath(offset), live && !statement, tip.data?.height, skew);

  // Under All, each page of blobs takes the movements of its own stretch of time (placeMoves), by the rows the list
  // shows. A namespace is a filter of blobs: the movements step aside while one is set.
  const merging = kind === "all" && !ns && posted && moveN > 0;
  const moves = useMemo<Moves | undefined>(() => (merging && money
    ? { place: (path, rows, total) => placeMoves(money, path, rows, total), rank: money.rank }
    : undefined), [merging, money]);

  // What the API says of all its blobs, whatever the period: its first and last blob, the namespaces of the whole
  // record (the "all" span's), and Tensile's reading of every one. An API from before them, or one that has not counted
  // the readings since it started, leaves the page to read its blobs at once as it used to: the newest, the oldest
  // (past AT_ONCE a read of its own), the namespaces with its settlements in each and what Tensile read, while every
  // one is in hand.
  const firstAt = pub.data?.publisher.first_settlement_at;
  const allSpan = pub.data?.windows.find((w) => w.window.name === "all");
  const apiNs = allSpan?.namespaces;
  const apiRead = pub.data?.publisher.readings ?? null;
  const ask = !!pub.data && (firstAt === undefined || apiNs === undefined || !apiRead);
  const head = useApi<{ blobs: Blob[]; total: number }>(ask ? `/v1/blobs?publisher=${addr}&limit=${AT_ONCE}` : null, 60000);
  const whole = head.data && head.data.blobs.length >= head.data.total ? head.data.blobs : null;
  const oldest = useFirstBlob(addr, firstAt === undefined && head.data && !whole ? head.data.total : null);
  const seen = apiNs === undefined && head.data && !whole ? [...new Set(head.data.blobs.map((b) => b.namespace))] : null;

  const { data, error, loading } = pub;
  if (!data && error) return <p className="notice">{
    notFound(pub) ? <>No publisher with this address is on record.</>
    : badRequest(pub) ? <><span className="mono">{addr}</span> is not a publisher account address ({error}).</>
    : throttled(pub) ? <>The observer API is busy ({error}); the page retries every 30 seconds.</>
    : <>The observer API is not answering ({error}); the page retries every 30 seconds.</>}</p>;
  if (loading || !data) return <p className="muted">Loading…</p>;

  const p = data.publisher;
  const all = data.windows.find((w) => w.window.name === "all");
  // the newest blob: the ledger's, while it shows a newer one than the API's last settlement (or the last read of all of them)
  const newest = [feed.loaded && !ns ? feed.rows[0]?.settlement_time : undefined, head.data?.blobs[0]?.settlement_time, p.last_settlement_at ?? undefined]
    .filter((t): t is string => !!t).sort((a, b) => Date.parse(b) - Date.parse(a))[0] ?? null;
  // the first blob: its time, and the height where the blob itself was read
  const firstBlob = firstAt !== undefined ? null : whole ? whole[whole.length - 1] ?? null : oldest ?? null;
  const first = firstAt !== undefined ? firstAt : firstBlob?.settlement_time ?? null;
  let nss: { ns: string; n: number }[] | null = null;
  let nsMore = 0;
  if (apiNs) {
    nss = apiNs.map((x) => ({ ns: x.namespace, n: x.settlements }));
    nsMore = Math.max(0, (allSpan?.namespaces_total ?? apiNs.length) - apiNs.length);
  } else if (whole) {
    const m = new Map<string, number>();
    for (const b of whole) m.set(b.namespace, (m.get(b.namespace) ?? 0) + 1);
    nss = [...m].map(([ns, n]) => ({ ns, n }));
  }
  // Tensile's reading of each of its blobs, in the Blobs list's own words (lane()): the API's count, or while it has
  // none, the page's own of every blob it read at once
  let read: Record<string, number> | null = null;
  if (apiRead) {
    const r = { unavailable: apiRead.unavailable, available: apiRead.available, "retention window": apiRead.in_retention_window, "not read": apiRead.not_read };
    read = Object.values(r).some((v) => v > 0) ? r : null;
  } else if (whole && whole.length > 0) {
    read = whole.reduce((c, b) => { const w = lane(b).word; c[w] = (c[w] ?? 0) + 1; return c; }, {} as Record<string, number>);
  }
  // Until the reads of an API from before these land, each row they will fill keeps its place under a placeholder, so
  // nothing below the block moves when they do. Which rows will come, its count of settlements says before its blobs
  // are read: all four up to AT_ONCE blobs; past that no reading, and no first blob past the last offset the API serves.
  const n = all?.settlements ?? 0;
  const reading = ask && !head.data && !head.error;
  const firstWait = !first && firstAt === undefined && (reading ? n - 1 <= MAX_OFFSET : !whole && oldest === undefined);
  const nsWait = !nss && apiNs === undefined && reading;
  const readWait = !read && !apiRead && reading && n <= AT_ONCE;

  // the escrow now, and whether it pays for one more blob of the account's average size over every blob it posted
  const e = p.escrow?.found ? p.escrow : null;
  const avg = posted && all ? all.bytes / all.settlements : null;
  const need = avg != null && pf ? blobFee(pf, avg) : null;
  const short = !!e && need != null && e.available_utia < need;
  const queued = e && p.pending_withdrawals && p.pending_withdrawals.count > 0 ? p.pending_withdrawals : null;
  // what the escrow's amber dot says: it cannot pay for one more blob, or settlements took part of a queued withdrawal
  const escWarn = [
    short && `Not enough for one more ${bytes(Math.round(avg!))} blob (${tia(need)})`,
    queued && queued.reduced_utia > 0 && `Settlements used ${tia(queued.reduced_utia)} of a queued withdrawal; that part will not be paid out`,
  ].filter(Boolean).join(". ");
  // its blobs: as many as Tensile's reading counts (a blob settled twice is one blob), or its settlements until it has
  const readN = read ? Object.values(read).reduce((a, b) => a + b, 0) : 0;
  const blobN = readN || p.settlements;
  // the blobs Tensile has read: an available or an unavailable one; one in its retention window, or not read, is neither
  const readDone = read ? (read.available ?? 0) + (read.unavailable ?? 0) : 0;
  // what it paid: the fees of its settlements, and what any timed-out promise was charged as a blob
  const paid = p.fees_utia + p.timed_out_utia;

  const nsChoices: Choice[] | null = nss
    ? nss.map((x) => ({ value: x.ns, label: <NsName ns={x.ns} />, count: x.n, find: `${nsDisplay(x.ns)} ${nsHex(x.ns)}`.toLowerCase() }))
    : seen ? seen.map((x) => ({ value: x, label: <NsName ns={x} />, find: `${nsDisplay(x)} ${nsHex(x)}`.toLowerCase() })) : null;
  const liveWord = !live || feed.refused ? null : feed.error ? "Not answering" : feed.loaded ? "Live" : "Connecting";
  const i = addr.indexOf("1");

  // the list's count: what it holds under the kind and the namespace picked, as its pager counts it
  const blobTotal = feed.loaded ? feed.total : ns ? null : p.settlements;
  const listN = statement ? moveN : blobTotal == null ? null : blobTotal + (merging ? moveN : 0);
  // a long account's escrow history is not whole here: where to find it, quietly, under its blobs
  const someMoves = data.recent_payments.some((x) => x.kind !== "settlement");
  const elsewhere = !wholeMoney && posted && (
    <p className="pb-more">{someMoves ? "Escrow movements" : "Older escrow movements"} are <a href={`${API_BASE}/v1/exports`} title="Every account's payments, deposits and withdrawals included, day by day">in the API →</a></p>
  );

  // The account's four figures, each a label and a value, what qualifies it on hover: the escrow it has left now (an
  // amber dot when it cannot pay for one more blob of its usual size, or settlements took part of a queued
  // withdrawal), then over its whole record its blobs, what it paid (a red dot for a timed-out promise), and how many
  // of its blobs Tensile found available (a red dot for one it did not).
  const escTitle = e ? [
    queued ? `${tia(queued.utia)} queued to withdraw${queued.next_available_at ? `, payable from ${utcWord(queued.next_available_at)}` : ""}; balance ${tia(e.balance_utia)}` : "",
    `${queued ? "as" : "As"} of block #${int(e.height)}, ${utcWord(e.updated_at)}`,
  ].filter(Boolean).join("; ") : p.escrow ? "No escrow account on the chain" : "Not read yet";
  const escVal = e ? <>{unit(tia(e.available_utia))}{escWarn && <Warn text={escWarn} />}</> : <span className="na">—</span>;
  const blobsTitle = posted && newest ? [
    first ? `First ${utcWord(first)}` : "",
    `last ${utcWord(newest)}`,
    readN && readN !== p.settlements ? `${plural(p.settlements, "settlement")}: a blob settled twice is one blob` : "",
  ].filter(Boolean).join("; ") : undefined;
  // a blob settled twice is one blob: its settlements beside it, quietly, when they are more
  const twice = readN > 0 && readN !== p.settlements ? plural(p.settlements, "settlement") : null;
  const paidTitle = paid > 0 ? [
    `${tia(p.fees_utia)} in fees for ${plural(p.settlements, "settlement")}`,
    p.timeouts > 0 ? `${tia(p.timed_out_utia)} charged for ${plural(p.timeouts, "timed-out promise")}` : "",
    p.paid_per_mib_utia != null ? `${tia(p.paid_per_mib_utia)} per MiB` : "",
  ].filter(Boolean).join("; ") : undefined;
  const paidDot = p.timeouts > 0 && <Warn tone="fault" text={`${plural(p.timeouts, "payment promise")} timed out; ${tia(p.timed_out_utia)} charged all the same`} />;
  const paidVal = <>{unit(tia(paid))}{paidDot}</>;
  const readTitle = read ? `Tensile's reading of its ${plural(readN, "blob")}: ${[
    read.available ? `${int(read.available)} available` : "",
    read.unavailable ? `${int(read.unavailable)} unavailable` : "",
    read["retention window"] ? `${int(read["retention window"])} in the retention window` : "",
    read["not read"] ? `${int(read["not read"])} not read` : "",
  ].filter(Boolean).join(", ")}` : posted ? "Tensile's reading of its blobs is not counted yet" : undefined;
  const unreadDot = read && (read.unavailable ?? 0) > 0 && <Warn tone="fault" text={`${plural(read.unavailable, "blob")} unavailable: Tensile could not read ${read.unavailable === 1 ? "it" : "them"} back from the validators`} />;
  // what Tensile read: "none yet" when it has read none, a placeholder while the count is on its way
  const readState: "some" | "none" | "wait" | "na" = read ? (readDone > 0 ? "some" : "none") : readWait ? "wait" : "na";

  // its namespaces, the most used first: each one shows its blobs alone in the list below
  const pickNs = (x: string) => { setView({ ns: x, ...(kind === "escrow" ? { kind: "all" as const } : {}) }); document.getElementById("list")?.scrollIntoView({ block: "start" }); };
  const nsLinks = nss && nss.length > 0 && <>
    {nss.map((x) => (
      <button key={x.ns} type="button" className="nsb" title={`${x.ns} · ${int(x.n)} settlement${x.n === 1 ? "" : "s"} · show only these`} onClick={() => pickNs(x.ns)}>{nsDisplay(x.ns)}</button>
    ))}
    {nsMore > 0 && <span className="more">+{int(nsMore)} more</span>}
  </>;
  const nsWord = `Namespace${nss && nss.length + nsMore === 1 ? "" : "s"}`;
  const showFacts = posted && (!!newest || reading);

  return (
    <>
      {/* A refresh that failed keeps the last answer on screen; say so, and
          from when, rather than let it pass for the current one. */}
      {error && <p className="notice">{throttled(pub) ? "The observer API is busy" : "The observer API is not answering"} ({error}). Showing the figures received at {pub.fetchedAt ? `${hhmm(pub.fetchedAt)} (${ago(pub.fetchedAt)})` : "the last refresh"}; the page retries every 30 seconds.</p>}

      <section className="pb-mast">
        <p className="pb-kind">Publisher</p>
        <div className="pb-id">
          <Ident addr={addr} />
          {p.label
            ? <><h1 className="pb-h1 lab" title={p.label_source ? `Label source: ${p.label_source}` : undefined}>{p.label}</h1><span className="pb-chip" title={addr}><span className="hd">{addr.slice(0, i)} •••</span><span className="tl">{addr.slice(-4)}</span></span></>
            : <h1 className="pb-h1" title={addr} aria-label={addr}><span className="hd">{addr.slice(0, i)}</span><span className="dots" aria-hidden="true">•••</span><span className="tl">{addr.slice(-4)}</span></h1>}
        </div>
        <div className="pb-addr"><span className="mono">{addr}</span><Copy text={addr} label="the address" /></div>
      </section>

      {/* Under its name, two frames side by side, as tall as each other, a row each with its label in a column of its
          own so every value starts at one x: the account on the left, its four figures; its history on the right, when
          it last and first posted level with the figure rows beside them and its namespaces in the room left under
          them. An account that never posted has its figures alone. */}
      <div className="pbd">
        <dl className="pan pbd-acct" aria-label="The account at a glance">
          <dt>Escrow available<span className="per"> · now</span></dt>
          <dd className="pbd-v" title={escTitle}>{escVal}</dd>
          <dt>Blobs<span className="per"> · all time</span></dt>
          <dd className="pbd-v" title={blobsTitle}>{int(blobN)}{twice && <span className="beside">{twice}</span>}</dd>
          <dt>Total paid<span className="per"> · all time</span></dt>
          <dd className="pbd-v" title={paidTitle}>{paidVal}</dd>
          <dt className="tz"><Eye /><span>Available<span className="per"> · all time</span></span></dt>
          {readState === "some"
            ? <dd className="pbd-v" title={readTitle}>{int(read!.available ?? 0)}<span className="beside">of {int(readDone)} read by Tensile</span>{unreadDot}</dd>
            : readState === "none" ? <dd className="pbd-v words" title={readTitle}>none read by Tensile yet</dd>
            : readState === "wait" ? <dd className="pbd-v"><span className="wait">5 of 5 read by Tensile</span></dd>
            : <dd className="pbd-v" title={readTitle}><span className="na">—</span></dd>}
        </dl>
        {showFacts && (
          <dl className="pan pbd-hist" aria-label="When and where it posted">
            <dt>Last blob</dt>
            <dd className="pbd-v">{newest
              ? <>{age(now - Date.parse(newest))} ago<span className="beside" title={utcWord(newest)}>{monthDayTime(newest)} UTC</span></>
              : <span className="wait">2 d 21 h ago Sep 28 20:48:38 UTC</span>}</dd>
            {(first || firstWait) && <><dt>First blob</dt><dd className="pbd-v">{first
              ? <span title={firstBlob ? `${utcWord(first)} · height ${int(firstBlob.settlement_height)}` : utcWord(first)}>{unit(`${monthDayTime(first)} UTC`)}</span>
              : <span className="wait">Sep 28 12:47:32 UTC</span>}</dd></>}
            {nsLinks && <><dt>{nsWord}</dt><dd className="nss">{nsLinks}</dd></>}
            {!nsLinks && nsWait && <><dt>Namespaces</dt><dd className="nss"><span className="wait">sov-niko-a</span></dd></>}
          </dl>
        )}
      </div>

      <section id="list" className="listing lg-list pb-list">
        <div className="list-head">
          <div className="pb-lh">
            <h2 className="pb-th">Transactions{listN != null && <span className="n">{int(listN)}</span>}</h2>
            {split && (
              <div className="seg pb-kinds" role="group" aria-label="kind">
                {KINDS.map(([k, label]) => (
                  <button key={k} type="button" aria-pressed={kind === k} onClick={() => setView(k === "escrow" ? { kind: k, ns: "" } : { kind: k })}>{label}</button>
                ))}
              </div>
            )}
          </div>
          {posted && !statement && (
            <div className="lg-tools">
              <Picker name="Namespace" icon={NS_ICON} value={ns} text={ns ? <NsName ns={ns} /> : null} choices={nsChoices}
                accept={(s) => (/^[0-9a-f]{58}$/.test(s) ? s : null)} placeholder="Name or hex" onPick={setNs} />
              {liveWord && <span className={`lg-live${feed.error ? " down" : ""}`} title={feed.error ? `The observer API did not answer (${feed.error}); the list shows the last read.` : "New blobs come in as the chain moves, while this page is open."}><i aria-hidden="true" />{liveWord}</span>}
            </div>
          )}
        </div>

        {statement
          ? <Statement d={data} moves={money?.list ?? []} page={page} onPage={setPage} now={now} />
          : (
            <Ledger feed={feed} size={SIZE} live={live} skew={skew} onePublisher moves={moves} onNs={setNs}>
              {(n, placed) => {
                // under All, the pages follow the blobs' and each counts its blobs and the movements between them, of
                // every transaction; what they are on hover
                const all = n + moveN;
                return (
                  <>
                    {elsewhere}
                    {n === 0 && page === 1 ? null : <Pager total={n} page={page} size={SIZE} maxPages={MAX_PAGE} onPage={setPage}
                      range={merging ? placed?.range : undefined} of={merging ? all : undefined}
                      noun={ns ? (n === 1 ? "settlement with this filter" : "settlements with this filter")
                        : merging ? <span title={`${plural(n, "blob settlement")} and ${plural(moveN, "escrow movement")}`}>{all === 1 ? "transaction" : "transactions"}</span>
                        : n === 1 ? "settlement" : "settlements"} />}
                  </>
                );
              }}
            </Ledger>
          )}
      </section>
    </>
  );
}

function Page() {
  const addr = (useSearchParams().get("addr") ?? "").trim().toLowerCase();
  if (!addr) return <p className="notice err">No publisher address given.</p>;
  return <Publisher key={addr} addr={addr} />;
}

export default function PublisherPage() {
  return <Suspense fallback={<p className="muted">Loading…</p>}><Page /></Suspense>;
}
