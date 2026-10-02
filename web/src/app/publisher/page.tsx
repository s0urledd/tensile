"use client";
import { Suspense, useCallback, useEffect, useState, type CSSProperties } from "react";
import { useSearchParams } from "next/navigation";
import { API_BASE, useApi, notFound, badRequest, throttled, hhmm, ago, type Blob, type Payment, type PublisherNamespace, type RecentBlob, type Tip, type Window, blobFee, bytes, int, nsDisplay, span, tia, utcWord } from "@/lib/api";
import type { Params, PublisherWithQueue, PublisherWithdrawals, WithdrawalRow } from "@/lib/withdrawals";
import { useWindow, WindowSwitch, windowLabel } from "@/lib/window";
import { lane } from "@/lib/status";
import Ledger, { useLedger } from "@/components/Ledger";
import Pager, { usePage } from "@/components/Pager";
import Picker, { type Choice } from "@/components/Picker";
import Ident from "@/components/Ident";
import Copy from "@/components/Copy";
import { unit } from "@/components/Unit";
import { nsHex, NsName, NS_ICON } from "@/components/Namespace";
import { PanelFig } from "@/components/Metrics";
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

/** rows per page of the blobs, and of the deposits and withdrawals */
const SIZE = 25;
/** the last page /v1/blobs serves: its offset stops at 100,000 */
const MAX_OFFSET = 100000;
const MAX_PAGE = Math.floor(MAX_OFFSET / SIZE) + 1;
/** recent_payments stops at this many: a history that long may be missing its oldest movements */
const PAYMENTS_CAP = 100;
/** a publisher's blobs read at once to say things of every one of them, where the API does not say them */
const AT_ONCE = 100;

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

/** "Oct 2 06:42": the minute is enough in a line; the second and UTC are on hover */
const monthDayMin = (s: string) => monthDayTime(s).slice(0, -3);

/** a column of amounts keeps one precision, the one tia() gives its largest, so the digits stack: never "1.000" under "1,127" */
function decimals(amounts: number[]): number {
  const v = Math.max(0, ...amounts.map((a) => Math.abs(a))) / 1e6;
  return v >= 1000 ? 0 : v >= 100 ? 1 : v >= 10 ? 2 : 3;
}
const tiaAt = (utia: number, dec: number) => `${(utia / 1e6).toLocaleString("en-US", { minimumFractionDigits: dec, maximumFractionDigits: dec })} TIA`;
const tiaExact = (utia: number) => `${(utia / 1e6).toLocaleString("en-US", { minimumFractionDigits: 6 })} TIA`;

/**
 * Deposits and withdrawals: the account's movements of money, newest first,
 * each in plain words with a quiet qualifier (when a request becomes payable,
 * how long a payout took), then a statement that adds up to the escrow now.
 * The blob fees are the Blobs list's rows and are not repeated here.
 */
function Movements({ d, now }: { d: Detail; now: number }) {
  const [page, setPage] = useState(1);
  const money = d.recent_payments.filter((x) => x.kind !== "settlement");
  const left = d.withdrawals?.left_queue ?? [];
  const queue = [...left, ...(d.withdrawals?.pending ?? [])];
  const e = d.publisher.escrow?.found ? d.publisher.escrow : null;
  const all = d.windows.find((w) => w.window.name === "all");

  // what went in, what the blobs and any timed-out promise cost, what went out, and the escrow that leaves;
  // shown only when it closes exactly on the balance, a line only when something moved
  const foot: { label: string; utia: number; sign: string; tot?: boolean }[] = [];
  if (e && all) {
    const sum = (k: Payment["kind"]) => money.filter((x) => x.kind === k).reduce((s, x) => s + x.amount_utia, 0);
    const dep = sum("deposit"), out = sum("withdrawal_executed"), charged = sum("timeout");
    if (dep - all.fees_utia - charged - out === e.balance_utia) {
      foot.push({ label: "Deposited", utia: dep, sign: "+" });
      if (all.fees_utia) foot.push({ label: `Fees paid for ${int(all.settlements)} settlement${all.settlements === 1 ? "" : "s"}`, utia: all.fees_utia, sign: "−" });
      if (charged) foot.push({ label: "Charged for timed-out promises", utia: charged, sign: "−" });
      if (out) foot.push({ label: "Withdrawn", utia: out, sign: "−" });
      foot.push({ label: "Escrow now", utia: e.balance_utia, sign: "", tot: true });
    }
  }
  const dec = decimals([...money.map((x) => x.amount_utia), ...foot.map((f) => f.utia)]);
  const amt = (sign: string, utia: number) => <span title={tiaExact(utia)}>{unit(`${utia ? sign : ""}${tiaAt(utia, dec)}`)}</span>;

  const shown = money.slice((page - 1) * SIZE, page * SIZE);
  return (
    <>
      <div className="lg-tw">
        <table className="lg-t pb-mv">
          <thead>
            <tr>
              <th className="c-h">Height</th>
              <th className="c-t">Time <span className="per">(UTC)</span></th>
              <th className="c-k">Movement</th>
              <th className="c-amt num">Amount</th>
            </tr>
          </thead>
          <tbody>
            {money.length === 0 && <tr className="lg-empty"><td colSpan={4}>No deposit or withdrawal on record.</td></tr>}
            {shown.map((x, i) => {
              const t = Date.parse(x.time);
              let word: string = x.kind, qual = "", short = "", sign = "", cls = "";
              if (x.kind === "deposit") { word = "Deposit"; sign = "+"; }
              else if (x.kind === "withdrawal_request") {
                const w: WithdrawalRow | undefined = queue.find((q) => Date.parse(q.requested_at) === t);
                const at = w?.available_at ?? x.available_at;
                word = "Withdrawal requested"; cls = "req";
                if (at) { qual = `payable from ${monthDayTime(at)}`; short = `payable ${monthDayMin(at)}`; }
                // a request the account's own settlements used up, whole or in part, before it could be paid out
                if (w?.outcome === "consumed") { qual = "used by settlements, not paid out"; short = "used by settlements"; cls = "req hold"; }
                else if (w && w.reduced_utia > 0) qual += ` · ${tia(w.reduced_utia)} of it used by settlements`;
              } else if (x.kind === "withdrawal_executed") {
                const w = left.find((q) => q.paid_height === x.height);
                word = "Withdrawal paid out"; sign = "−";
                if (w?.payout_delay_s != null) { qual = `${span(w.payout_delay_s)} after the request`; short = `${span(w.payout_delay_s)} after request`; }
              } else if (x.kind === "timeout") { word = "Timed-out promise charged"; sign = "−"; cls = "fault"; }
              return (
                <tr key={`${x.height}-${x.tx_hash ?? ""}-${i}`} className={`row ${cls}`}>
                  <td className="c-h">{int(x.height)}</td>
                  <td className="c-t"><span title={utcWord(x.time)}><span className="tm">{monthDayTime(x.time)}</span><span className="ag">{age(now - t)}</span></span></td>
                  <td className="c-k"><span className="k">{word}</span>{qual && <span className="q">{qual}</span>}</td>
                  <td className="c-amt num" title={x.kind === "withdrawal_request" ? "Moved from available to the withdrawal queue; the balance changes when it is paid out." : undefined}>{amt(sign, x.amount_utia)}</td>
                  <td className={`c-m2${short ? " hq" : ""}`}><span className="h">#{int(x.height)}</span>{short && <><span className="sep">·</span><span className="q">{short}</span></>}</td>
                </tr>
              );
            })}
          </tbody>
          {foot.length > 0 && page === Math.ceil(money.length / SIZE) && (
            <tfoot>
              {foot.map((f) => (
                <tr key={f.label} className={f.tot ? "tot" : undefined}>
                  <td colSpan={3} className="fl">{f.label}</td>
                  <td className="c-amt num">{amt(f.sign, f.utia)}</td>
                </tr>
              ))}
            </tfoot>
          )}
        </table>
      </div>
      {money.length > 0 && <Pager total={money.length} page={page} size={SIZE} onPage={setPage} noun={money.length === 1 ? "deposit or withdrawal" : "deposits and withdrawals"} />}
    </>
  );
}

/**
 * One publisher: whose account it is, what it posted and when, then one
 * framed panel with its escrow as it stands now and the period's activity,
 * then its blobs as the Blobs list draws them, and its deposits and
 * withdrawals.
 */
function Publisher({ addr }: { addr: string }) {
  const params = useSearchParams();
  const [win, setWin] = useWindow("24h");
  const [page, setPageRaw] = usePage();
  const [tabPick, setTab] = useState<"blobs" | "money" | null>(null);
  const [ns, setNsRaw] = useState((params.get("namespace") ?? "").trim().toLowerCase());
  // a new page opens at the list's top when the reader had scrolled past it
  const setPage = useCallback((p: number) => {
    setPageRaw(p);
    const el = document.getElementById("list");
    if (el && el.getBoundingClientRect().top < 0) el.scrollIntoView();
  }, [setPageRaw]);
  // a namespace starts from the first page, and lives in the address so a link keeps it
  const setNs = useCallback((v: string) => {
    setNsRaw(v);
    setPageRaw(1);
    try {
      const u = new URL(window.location.href);
      if (v) u.searchParams.set("namespace", v); else u.searchParams.delete("namespace");
      window.history.replaceState(null, "", u.pathname + u.search + u.hash);
    } catch { /* fine */ }
  }, [setPageRaw]);

  const pub = useApi<Detail>(`/v1/publishers/${addr}?window=${win}`);
  const pf = useApi<Params>("/v1/params", 0).data?.price_formula;
  const tip = useApi<Tip>("/v1/tip", 4000); // the header's stream: no request of its own
  const skew = tip.data?.server_time && tip.fetchedAt ? Date.parse(tip.data.server_time) - Date.parse(tip.fetchedAt) : 0;
  const now = Date.now() + skew;

  // The history is whole while the API did not stop it at its cap: a busy account's deposits can fall out of that cap,
  // and its Deposits and withdrawals wait for the API to page them. An account that never posted opens on them.
  const wholeMoney = !!pub.data && pub.data.recent_payments.length < PAYMENTS_CAP;
  const posted = !!pub.data?.windows.find((w) => w.window.name === "all")?.settlements;
  const tab = !wholeMoney ? "blobs" : tabPick ?? (posted ? "blobs" : "money");
  const offset = (Math.min(page, MAX_PAGE) - 1) * SIZE;
  const live = page === 1;
  const feed = useLedger(`/v1/blobs?limit=${SIZE}&offset=${offset}&publisher=${addr}${ns ? `&namespace=${encodeURIComponent(ns)}` : ""}`, live && tab === "blobs", tip.data?.height, skew);

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
  // Tensile's reading of each of its blobs, in the Blobs list's own words (lane())
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
  // the period's activity: the figures of the publisher row, which the API gives for the period asked
  const active = p.settlements > 0 || p.timeouts > 0;
  const actN = p.timeouts > 0 ? 4 : 3;
  // a queued withdrawal or a timeout adds a cell: the escrow then sits over the activity, at every width
  const stack = !!queued || p.timeouts > 0;
  const moneyN = data.recent_payments.filter((x) => x.kind !== "settlement").length;

  const nsChoices: Choice[] | null = nss
    ? nss.map((x) => ({ value: x.ns, label: <NsName ns={x.ns} />, count: x.n, find: `${nsDisplay(x.ns)} ${nsHex(x.ns)}`.toLowerCase() }))
    : seen ? seen.map((x) => ({ value: x, label: <NsName ns={x} />, find: `${nsDisplay(x)} ${nsHex(x)}`.toLowerCase() })) : null;
  const liveWord = !live || feed.refused ? null : feed.error ? "Not answering" : feed.loaded ? "Live" : "Connecting";
  const i = addr.indexOf("1");

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
            ? <><h1 className="pb-h1 lab" title={p.label_source ? `label source: ${p.label_source}` : undefined}>{p.label}</h1><span className="pb-chip" title={addr}><span className="hd">{addr.slice(0, i)} •••</span><span className="tl">{addr.slice(-4)}</span></span></>
            : <h1 className="pb-h1" title={addr} aria-label={addr}><span className="hd">{addr.slice(0, i)}</span><span className="dots" aria-hidden="true">•••</span><span className="tl">{addr.slice(-4)}</span></h1>}
        </div>
        <div className="pb-addr"><span className="mono">{addr}</span><Copy text={addr} label="the address" /></div>

        {/* when it last and first posted, where, and what Tensile found: all-time, in a light frame of their own; a row
            still being read holds its place under a placeholder */}
        {posted && (newest || reading) && (
          <dl className="pb-meta">
            <dt>Last blob</dt>
            <dd>{newest
              ? <><b>{age(now - Date.parse(newest))} ago</b><em title={utcWord(newest)}>{monthDayTime(newest)} UTC</em></>
              : <span className="wait">2 d 21 h ago Sep 28 20:48:38 UTC</span>}</dd>
            {(first || firstWait) && <><dt>First blob</dt><dd>{first
              ? <><b title={firstBlob ? `${utcWord(first)} · height ${int(firstBlob.settlement_height)}` : utcWord(first)}>{monthDayTime(first)}</b><em>UTC</em></>
              : <span className="wait">Sep 28 12:47:32 UTC</span>}</dd></>}
            {nss && nss.length > 0 && <>
              <dt>Namespace{nss.length + nsMore === 1 ? "" : "s"}</dt>
              <dd className="pb-nss">
                {nss.map((x) => (
                  <button key={x.ns} type="button" className="nsb" title={`${x.ns} · ${int(x.n)} settlement${x.n === 1 ? "" : "s"} · show only these`}
                    onClick={() => { setNs(x.ns); setTab("blobs"); document.getElementById("list")?.scrollIntoView({ block: "start" }); }}>{nsDisplay(x.ns)}</button>
                ))}
                {nsMore > 0 && <span className="more">+{int(nsMore)} more</span>}
              </dd>
            </>}
            {!nss && nsWait && <><dt>Namespaces</dt><dd><span className="wait">sov-niko-a</span></dd></>}
            {read && <>
              <dt>Tensile&rsquo;s reading</dt>
              <dd className="pb-tally" title="Tensile reads each blob once, near the end of its retention window.">
                {[
                  read.unavailable ? <span className="hold"><b>{int(read.unavailable)}</b> unavailable</span> : null,
                  read.available ? <span><b>{int(read.available)}</b> <span className="ok">available</span></span> : null,
                  read["retention window"] ? <span><b>{int(read["retention window"])}</b> in retention window</span> : null,
                  read["not read"] ? <span><b>{int(read["not read"])}</b> not read</span> : null,
                ].filter(Boolean).map((x, k) => <span key={k} className="it">{k > 0 && <span className="sep">·</span>}{x}</span>)}
              </dd>
            </>}
            {readWait && <><dt>Tensile&rsquo;s reading</dt><dd><span className="wait">5 available</span></dd></>}
          </dl>
        )}
      </section>

      {/* the escrow as it stands now, then the period's activity; the period switch sits in the activity's head and
          drives its cells only */}
      <section className={`pan pp${stack ? " stack" : ""}`} aria-label="Escrow and activity">
        <div className="pp-g pp-esc" style={{ "--n": queued ? 2 : 1 } as CSSProperties}>
          <div className="pp-h"><h2 className="pp-t">Escrow <span className="per">(now)</span></h2></div>
          <dl className="pp-cells">
            <PanelFig label="Available" title={e ? `Read from the chain at #${int(e.height)}, ${utcWord(e.updated_at)}` : p.escrow ? "No escrow account on the chain" : "Not read yet"}
              value={e ? <>{unit(tia(e.available_utia))}{short && <Warn text={`Not enough for one more ${bytes(Math.round(avg!))} blob (${tia(need)})`} />}</> : "—"} />
            {queued && e && (
              <PanelFig label="Queued to withdraw" value={unit(tia(queued.utia))} title={`Balance ${tia(e.balance_utia)}: available plus queued`}>
                {queued.next_available_at && <dd className="pan-s" title={utcWord(queued.next_available_at)}>Payable from <b>{monthDayMin(queued.next_available_at)}</b></dd>}
                {queued.reduced_utia > 0 && <dd className="pan-s hold" title="Settlements took this from the queued amount; it will not be paid out.">{tia(queued.reduced_utia)} used by settlements</dd>}
              </PanelFig>
            )}
          </dl>
        </div>
        <div className="pp-g pp-act" style={{ "--n": active ? actN : 3 } as CSSProperties}>
          <div className="pp-h">
            <h2 className="pp-t">Activity</h2>
            {posted && <WindowSwitch value={win} onChange={setWin} />}
          </div>
          {active ? (
            <dl className="pp-cells">
              <PanelFig label="Settlements" value={int(p.settlements)} className={`c-set${p.timeouts > 0 ? "" : " wide"}`} />
              <PanelFig label="Blob size" value={unit(bytes(p.bytes))} className="c-size"
                title={p.avg_blob_bytes != null ? (p.avg_blob_bytes === p.largest_blob_bytes ? `${bytes(p.largest_blob_bytes)} each` : `${bytes(Math.round(p.avg_blob_bytes))} average · ${bytes(p.largest_blob_bytes)} largest`) : undefined} />
              <PanelFig label="Fees paid" value={unit(tia(p.fees_utia))} className="c-fee" title={p.paid_per_mib_utia != null ? `${tia(p.paid_per_mib_utia)} per MiB` : undefined} />
              {p.timeouts > 0 && (
                <PanelFig label="Timed out" value={int(p.timeouts)} className="c-to fault" title="Payment promises not settled in time in the period; each is charged as a blob">
                  <dd className="pan-s"><b>{tia(p.timed_out_utia)}</b> charged</dd>
                </PanelFig>
              )}
            </dl>
          ) : (
            // a period with nothing in it, or an account that never posted: one quiet line across the group
            <div className="pp-cells"><p className="pan-c pp-none">{posted ? `No blobs in ${windowLabel(win)}` : "No blobs"}</p></div>
          )}
        </div>
      </section>

      <section id="list" className="listing lg-list pb-list">
        <div className="list-head">
          <div className="tabs" role="group" aria-label="list">
            <button type="button" aria-pressed={tab === "blobs"} onClick={() => setTab("blobs")}>Blobs</button>
            {wholeMoney && <button type="button" aria-pressed={tab === "money"} onClick={() => setTab("money")}>Deposits and withdrawals{moneyN > 0 && <span className="n">{int(moneyN)}</span>}</button>}
          </div>
          {tab === "blobs" && posted && (
            <div className="lg-tools">
              <Picker name="Namespace" icon={NS_ICON} value={ns} text={ns ? <NsName ns={ns} /> : null} choices={nsChoices}
                accept={(s) => (/^[0-9a-f]{58}$/.test(s) ? s : null)} placeholder="Name or hex" onPick={setNs} />
              {liveWord && <span className={`lg-live${feed.error ? " down" : ""}`} title={feed.error ? `The observer API did not answer (${feed.error}); the list shows the last read.` : "New blobs come in as the chain moves, while this page is open."}><i aria-hidden="true" />{liveWord}</span>}
            </div>
          )}
        </div>

        {tab === "blobs"
          ? (
            <Ledger feed={feed} size={SIZE} live={live} skew={skew} onePublisher onNs={setNs}>
              {(n) => (n === 0 && page === 1 ? null : <Pager total={n} page={page} size={SIZE} maxPages={MAX_PAGE} onPage={setPage}
                noun={ns ? (n === 1 ? "settlement with this filter" : "settlements with this filter") : n === 1 ? "settlement" : "settlements"} />)}
            </Ledger>
          )
          : <Movements d={data} now={now} />}
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
