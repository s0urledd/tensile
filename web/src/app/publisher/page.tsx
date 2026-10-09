"use client";
import { Suspense, useCallback, useEffect, useState } from "react";
import { useSearchParams } from "next/navigation";
import { API_BASE, useApi, notFound, badRequest, throttled, hhmm, ago, shortMid, type Blob, type Payment, type PublisherNamespace, type RecentBlob, type Tip, type Window, blobFee, bytes, int, isBlob, nsDisplay, tia, utcWord, TIP_MS } from "@/lib/api";
import type { Params, PublisherWithQueue, PublisherWithdrawals } from "@/lib/withdrawals";
import { lane } from "@/lib/status";
import Ledger, { useLedger } from "@/components/Ledger";
import TxList, { type Foot, type PubTxs } from "@/components/PubTxs";
import Pager, { usePage } from "@/components/Pager";
import Picker, { type Choice } from "@/components/Picker";
import Ident from "@/components/Ident";
import Copy from "@/components/Copy";
import { unit } from "@/components/Unit";
import { nsHex, NsName, NS_ICON } from "@/components/Namespace";
import { Eye } from "@/components/Metrics";
import { age, monthDayTime } from "@/components/BlobsDeck";
import Warn from "@/components/Warn";
import { publisherAddr } from "@/lib/sitefind";

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

/** rows per page of every tab */
const SIZE = 25;
/** the last page /v1/blobs serves: its offset stops at 100,000 */
const MAX_OFFSET = 100000;
const MAX_PAGE = Math.floor(MAX_OFFSET / SIZE) + 1;
/** a publisher's blobs read at once to say things of every one of them, where the API does not say them */
const AT_ONCE = 100;

/** what the list shows: every transaction of the account, its blob payments alone, or its escrow's transactions alone */
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
        const j = await blobsAt(`publisher=${encodeURIComponent(addr)}&limit=1&offset=${t - 1}`);
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

/**
 * One publisher: whose account it is; under it, on the left, its four figures in one frame, a row each (the escrow it
 * has left now, and over its whole record its blobs, what it paid, and how many of its blobs Tensile found available);
 * on the right, in a frame of the same make, when it last and first posted and where; then its transactions, newest
 * first, under three tabs: All, every Fibre transaction of the account, successful and failed; Blobs, its blob
 * payments as the Blobs list draws them, the failed ones among them; Escrow, its deposits and withdrawal requests, and
 * the escrow's statement under them. No figure above counts a failed transaction: it moved nothing.
 */
function Publisher({ addr }: { addr: string }) {
  const params = useSearchParams();
  const [page, setPageRaw] = usePage();
  // the tab, from the address; a namespace is a filter of the blobs, so a link with one and no tab opens Blobs
  const [kindPick, setKindPick] = useState<Kind>(() => {
    const k = params.get("kind");
    return k === "blobs" || k === "escrow" ? k : (params.get("namespace") ?? "").trim() ? "blobs" : "all";
  });
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
  const pub = useApi<Detail>(`/v1/publishers/${encodeURIComponent(addr)}?window=all`);
  // the price of a blob, for the low-escrow warning: the parameters' own 5-minute stream (PublishersTop's too), asked
  // again soon after a failure, so one failed first answer does not lose the warning for the life of the page
  const pf = useApi<Params>("/v1/params", 300000).data?.price_formula;
  const tip = useApi<Tip>("/v1/tip", TIP_MS); // the header's stream: no request of its own
  const skew = tip.data?.server_time && tip.fetchedAt ? Date.parse(tip.data.server_time) - Date.parse(tip.fetchedAt) : 0;
  const now = Date.now() + skew;

  // An account that posted has the three tabs; one that never posted has only its escrow's transactions and statement.
  const posted = !!pub.data?.windows.find((w) => w.window.name === "all")?.settlements;
  const kind: Kind = posted ? kindPick : "escrow";
  const blobs = kind === "blobs";
  const offset = (Math.min(page, MAX_PAGE) - 1) * SIZE;
  const live = page === 1;
  // Blobs: a page of the account's blobs with its failed blob payments among them (the Blobs list's own rows), the
  // first page live. Under the other tabs the first page, unfiltered, is read once, for the newest blob in the frame
  // above: the page Blobs opens on.
  const blobsPath = (o: number, n: string) => `/v1/blobs?limit=${SIZE}&offset=${o}&publisher=${encodeURIComponent(addr)}&include_failed=1${n ? `&namespace=${encodeURIComponent(n)}` : ""}`;
  const feed = useLedger(blobs ? blobsPath(offset, ns) : blobsPath(0, ""), blobs && live, tip.data?.height, skew);
  // All and Escrow: the account's transactions, successful and failed, a page at a time over the whole record; the
  // first page asked again as often as the API's own cache turns
  const txs = useApi<PubTxs>(pub.data && !blobs ? `/v1/publishers/${encodeURIComponent(addr)}/txs?view=${kind}&limit=${SIZE}&offset=${offset}` : null, live ? 15000 : 30000);

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
  const head = useApi<{ blobs: Blob[]; total: number }>(ask ? `/v1/blobs?publisher=${encodeURIComponent(addr)}&limit=${AT_ONCE}` : null, 60000);
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
  // the newest blob: the ledger's, while it shows a newer one than the API's last settlement (or the last read of all of
  // them); a failed blob payment among its rows settled no blob
  const newest = [feed.loaded && !ns ? feed.rows.find(isBlob)?.settlement_time : undefined, head.data?.blobs[0]?.settlement_time, p.last_settlement_at ?? undefined]
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
  // Tensile's readings of it: one per settlement, since each promise is read on its own (a blob paid for twice is read twice)
  const readN = read ? Object.values(read).reduce((a, b) => a + b, 0) : 0;
  // the blobs Tensile has read: an available or an unavailable one; one in its retention window, or not read, is neither
  const readDone = read ? (read.available ?? 0) + (read.unavailable ?? 0) : 0;
  // what it paid: the fees of its settlements, and what any timed-out promise was charged as a blob
  const paid = p.fees_utia + p.timed_out_utia;

  const nsChoices: Choice[] | null = nss
    ? nss.map((x) => ({ value: x.ns, label: <NsName ns={x.ns} />, count: x.n, find: `${nsDisplay(x.ns)} ${nsHex(x.ns)}`.toLowerCase() }))
    : seen ? seen.map((x) => ({ value: x, label: <NsName ns={x} />, find: `${nsDisplay(x)} ${nsHex(x)}`.toLowerCase() })) : null;
  const liveWord = !live || feed.refused ? null : feed.error ? "Not answering" : feed.loaded ? "Live" : "Connecting";
  const i = addr.indexOf("1");

  // the list's count: what the tab holds, as its pager counts it (Blobs: under the namespace picked)
  const txsHere = txs.data && txs.data.view === kind ? txs.data : null;
  const listN = blobs ? (feed.loaded ? feed.total : ns ? null : p.settlements) : txsHere ? txsHere.total : null;

  // Escrow's statement under its last page: what went in, what the blobs and any timed-out promise cost, what was paid
  // out, and the escrow that leaves, over the whole record; shown only when it closes exactly on the balance, a line
  // only when something moved
  const foot: Foot = [];
  const sums = txsHere?.sums;
  if (e && sums && sums.deposited_utia - sums.fees_utia - sums.charged_utia - sums.withdrawn_utia === e.balance_utia) {
    foot.push({ label: "Deposited", utia: sums.deposited_utia, sign: "+" });
    if (sums.fees_utia) foot.push({ label: `Fees paid for ${plural(sums.settlements, "settlement")}`, utia: sums.fees_utia, sign: "−" });
    if (sums.charged_utia) foot.push({ label: "Charged for timed-out promises", utia: sums.charged_utia, sign: "−" });
    if (sums.withdrawn_utia) foot.push({ label: "Withdrawn", utia: sums.withdrawn_utia, sign: "−" });
    foot.push({ label: "Escrow now", utia: e.balance_utia, sign: "", tot: true });
  }

  // The account's four figures, each a label and a value, what qualifies it on hover: the escrow it has left now (an
  // amber dot when it cannot pay for one more blob of its usual size, or settlements took part of a queued
  // withdrawal), then over its whole record its settlements, what it paid (a red dot for a timed-out promise), and how
  // many of them Tensile found available (a red dot for one it did not).
  const escTitle = e ? [
    queued ? `${tia(queued.utia)} queued to withdraw${queued.next_available_at ? `, payable from ${utcWord(queued.next_available_at)}` : ""}; balance ${tia(e.balance_utia)}` : "",
    `${queued ? "as" : "As"} of block #${int(e.height)}, ${utcWord(e.updated_at)}`,
  ].filter(Boolean).join("; ") : p.escrow ? "No escrow account on the chain" : "Not read yet";
  const escVal = e ? <>{unit(tia(e.available_utia))}{escWarn && <Warn text={escWarn} />}</> : <span className="na">—</span>;
  const settledTitle = posted && newest ? [first ? `First ${utcWord(first)}` : "", `last ${utcWord(newest)}`].filter(Boolean).join("; ") : undefined;
  const paidTitle = paid > 0 ? [
    `${tia(p.fees_utia)} in fees for ${plural(p.settlements, "settlement")}`,
    p.timeouts > 0 ? `${tia(p.timed_out_utia)} charged for ${plural(p.timeouts, "timed-out promise")}` : "",
    p.paid_per_mib_utia != null ? `${tia(p.paid_per_mib_utia)} per MiB` : "",
  ].filter(Boolean).join("; ") : undefined;
  const paidDot = p.timeouts > 0 && <Warn tone="fault" text={`${plural(p.timeouts, "payment promise")} timed out; ${tia(p.timed_out_utia)} charged all the same`} />;
  const paidVal = <>{unit(tia(paid))}{paidDot}</>;
  const readTitle = read ? `Tensile's reading of its ${plural(readN, "settlement")}: ${[
    read.available ? `${int(read.available)} available` : "",
    read.unavailable ? `${int(read.unavailable)} unavailable` : "",
    read["retention window"] ? `${int(read["retention window"])} in the retention window` : "",
    read["not read"] ? `${int(read["not read"])} not read` : "",
  ].filter(Boolean).join(", ")}` : posted ? "Tensile's reading of its blobs is not counted yet" : undefined;
  const unreadDot = read && (read.unavailable ?? 0) > 0 && <Warn tone="fault" text={`${plural(read.unavailable, "blob")} unavailable: Tensile could not read ${read.unavailable === 1 ? "it" : "them"} back from the validators`} />;
  // what Tensile read: "none yet" when it has read none, a placeholder while the count is on its way
  const readState: "some" | "none" | "wait" | "na" = read ? (readDone > 0 ? "some" : "none") : readWait ? "wait" : "na";

  // its namespaces, the most used first: each one shows its blobs alone under Blobs
  const pickNs = (x: string) => { setView({ ns: x, kind: "blobs" }); document.getElementById("list")?.scrollIntoView({ block: "start" }); };
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
          <dt>Settlements<span className="per"> · all time</span></dt>
          <dd className="pbd-v" title={settledTitle}>{int(p.settlements)}</dd>
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
            {posted && (
              <div className="seg pb-kinds" role="group" aria-label="kind">
                {KINDS.map(([k, label]) => (
                  <button key={k} type="button" aria-pressed={kind === k} onClick={() => setView(k === "blobs" ? { kind: k } : { kind: k, ns: "" })}>{label}</button>
                ))}
              </div>
            )}
          </div>
          {/* the namespace and the live word are the blobs' */}
          {blobs && (
            <div className="lg-tools">
              <Picker name="Namespace" icon={NS_ICON} value={ns} text={ns ? <NsName ns={ns} /> : null} choices={nsChoices}
                accept={(s) => (/^[0-9a-f]{58}$/.test(s) ? s : null)} placeholder="Name or hex" onPick={setNs} />
              {liveWord && <span className={`lg-live${feed.error ? " down" : ""}`} title={feed.error ? `The observer API did not answer (${feed.error}); the list shows the last read.` : "New blobs come in as the chain moves, while this page is open."}><i aria-hidden="true" />{liveWord}</span>}
            </div>
          )}
        </div>

        {blobs
          ? (
            // its blobs as the Blobs list draws them, a failed blob payment among them in its place; the pager names its
            // rows blob payments once a failed one is among them, as the Blobs list does
            <Ledger feed={feed} size={SIZE} live={live} skew={skew} onePublisher status onNs={setNs}>
              {(n, _, failed = 0) => (n === 0 && page === 1 ? null : <Pager total={n} page={page} size={SIZE} maxPages={MAX_PAGE} onPage={setPage}
                noun={`${failed === 0 ? "settlement" : "blob payment"}${n === 1 ? "" : "s"}${ns ? " with this filter" : ""}`} />)}
            </Ledger>
          )
          : <TxList view={kind === "escrow" ? "escrow" : "all"} offset={offset} data={txs.data} error={txs.error} page={page} size={SIZE} onPage={setPage} now={now} foot={foot} />}
      </section>
    </>
  );
}

function Page() {
  const asked = (useSearchParams().get("addr") ?? "").trim();
  if (!asked) return <p className="notice err">No publisher address given.</p>;
  // a value that is no account address never reaches the API's path
  const addr = publisherAddr(asked);
  if (!addr) return <p className="notice"><span className="mono">{shortMid(asked, 16, 6)}</span> is not a publisher account address: one is a <code>celestia1…</code> address.</p>;
  return <Publisher key={addr} addr={addr} />;
}

export default function PublisherPage() {
  return <Suspense fallback={<p className="muted">Loading…</p>}><Page /></Suspense>;
}
