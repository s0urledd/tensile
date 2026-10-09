"use client";
import { memo, useCallback, useEffect, useLayoutEffect, useRef, useState } from "react";
import Link from "next/link";
import { useRouter } from "next/navigation";
import { type Fetch, type TxPage, type TxRow, int, span, utcWord } from "@/lib/api";
import { CopyMark, Signed, decimals } from "@/components/Ledger";
import Pager from "@/components/Pager";
import { age, monthDayTime } from "@/components/BlobsDeck";
import { reducedMotion } from "@/components/RollNumber";
import { openRow } from "@/lib/row";

/**
 * One publisher's transactions as a transaction table (All, and Escrow): every row a transaction, in five columns that
 * each hold one kind of fact all the way down: its hash (the link to its page, and the copy mark), its type, when and in
 * which block, whether it took effect, and what it moved. A failure is "Failed" in Status and "—" in Amount: it moved
 * nothing, and what it asked for and why it failed are on its page. A payout has no transaction of its own: no hash, and
 * the row opens nothing. The whole row opens the transaction's page; the hash and the copy mark keep their own clicks.
 *
 * Page 1 is live: read again every 15 s, new rows sliding in from the top, or, while the reader is in the list, gathered
 * in the pill on the table's edge until a click brings them in.
 */

const SLIDE_MS = 420;

/** a row's type, the same word whether it took effect or failed */
export const KIND_WORD: Record<TxRow["kind"], string> = {
  settlement: "Blob payment", deposit: "Deposit", withdrawal_request: "Withdrawal request",
  withdrawal_executed: "Withdrawal payout", timeout: "Promise timeout",
};
/** what each kind does to the escrow: in (+), out (−); a withdrawal request only moves money into its queue, inside it */
const SIGN: Record<TxRow["kind"], "+" | "−" | ""> = { settlement: "−", deposit: "+", withdrawal_request: "", withdrawal_executed: "−", timeout: "−" };

const keyOf = (t: TxRow) => `${t.height}:${t.tx_index}:${t.msg_index}:${t.status}`;

/** a failed row's Status hover: why, when Tensile has a reason, and that nothing of it took effect */
export const failedTitle = (reason?: string) => reason ? `Failed: ${reason}. None of its messages took effect.` : "It failed in this block: none of its messages took effect.";

/** a withdrawal request's place in the queue, in words, for the hover on its amount */
function queueWords(t: TxRow): string {
  if (t.outcome === "consumed") return "Used by settlements, not paid out.";
  if (t.outcome === "paid" && t.paid_height != null) return `Paid out in block #${int(t.paid_height)}${t.payout_delay_s != null ? `, ${span(t.payout_delay_s)} after the request` : ""}.`;
  if (t.available_at) return `Payable from ${monthDayTime(t.available_at).slice(0, -3)} UTC.`;
  return "";
}

/** one transaction as a row */
const Row = memo(function Row({ t, dec, ag, fresh, onOpen }: { t: TxRow; dec: number; ag: string | null; fresh: boolean; onOpen: (e: React.MouseEvent, href: string) => void }) {
  const failed = t.status === "failed";
  const word = KIND_WORD[t.kind] ?? t.kind;
  const hash = t.tx_hash ? t.tx_hash.toLowerCase() : "";
  const H = hash.toUpperCase();
  const href = hash ? `/tx/?hash=${hash}` : "";
  const sign = SIGN[t.kind] ?? "";
  const amount = failed || t.amount_utia == null ? null : t.amount_utia;
  const reqNote = t.kind === "withdrawal_request" && !failed
    ? ["Moved from available to the withdrawal queue; the balance changes when it is paid out.", queueWords(t)].filter(Boolean).join(" ")
    : undefined;
  return (
    <tr className={`row${failed ? " xf" : ""}${hash ? "" : " np"}${t.kind === "withdrawal_request" && !failed ? " req" : ""}${fresh ? " fresh" : ""}`} data-k={keyOf(t)}
      onClick={href ? (e) => openRow(e, href, onOpen) : undefined} onAuxClick={href ? (e) => openRow(e, href, onOpen) : undefined}>
      <td className="c-x">{hash
        ? <><Link href={href} aria-label={`Transaction ${H.slice(0, 6)}…${H.slice(-4)}, ${failed ? "failed " : ""}${word.toLowerCase()}, block ${int(t.height)}`}>{H.slice(0, 6)}<span className="el">…</span>{H.slice(-4)}</Link>
          <CopyMark text={H} label="the transaction hash" /></>
        : <span className="na" title={`Paid out by the chain at the start of block #${int(t.height)}: no transaction of its own.`}>—</span>}</td>
      <td className="c-k">{word}</td>
      <td className="c-tb"><span className="tb" title={`${utcWord(t.time)} · block #${int(t.height)}`}><span className="tm">{monthDayTime(t.time)}</span><span className="ag">{ag ?? ""}</span><span className="bk">#{int(t.height)}</span></span></td>
      <td className="c-st">{failed
        ? <span className="st f" title={failedTitle(t.reason)}><i className="dot fault" />Failed</span>
        : <span className="st" title="It took effect in this block."><i className="dot ok" />Success</span>}</td>
      <td className="c-am num">{amount == null
        ? <span className="xd" title={failed ? "Nothing moved: the escrow is as it was." : undefined}>—</span>
        : <span title={reqNote}><Signed sign={sign} utia={amount} dec={dec} /></span>}</td>
    </tr>
  );
});

/** the escrow's statement under its rows: what went in, what the blobs and timed-out promises cost, what went out, and what it leaves */
export type Close = { balance_utia: number; fees_utia: number; settlements: number };

export default function TxTable({ q, view, page, size, maxPages, onPage, live, skew, close }: {
  /** the page read (/v1/publishers/{addr}/txs), as useApi gives it */
  q: Fetch<TxPage>;
  view: "all" | "escrow";
  page: number;
  size: number;
  maxPages?: number;
  onPage: (p: number) => void;
  /** the first page: read again, new rows come in */
  live: boolean;
  /** the observer's clock minus the reader's, for the ages */
  skew: number;
  /** Escrow: the balance and the fees the foot closes on; no foot without them */
  close?: Close | null;
}) {
  const offset = (page - 1) * size;
  const at = `${view}:${offset}`;
  const d = q.data;
  const mine = !!d && d.view === view && d.offset === offset;

  // ---- holding: while the reader points into the list, or has scrolled past its first row, what is on screen stays ----
  const wrapRef = useRef<HTMLDivElement>(null);
  const bodyRef = useRef<HTMLTableSectionElement>(null);
  const [pointerIn, setPointerIn] = useState(false);
  const [away, setAway] = useState(false);
  useEffect(() => {
    let raf = 0;
    const check = () => { raf = 0; const first = bodyRef.current?.firstElementChild; setAway(!!first && first.getBoundingClientRect().top < 0); };
    const on = () => { if (!raf) raf = requestAnimationFrame(check); };
    check();
    window.addEventListener("scroll", on, { passive: true });
    return () => { cancelAnimationFrame(raf); window.removeEventListener("scroll", on); };
  }, []);
  const hold = live && (pointerIn || away);

  type Shown = { at: string; rows: TxRow[]; fresh: Set<string>; id: number };
  const [shown, setShown] = useState<Shown | null>(null);
  const take = useCallback((v: Shown | null, rows: TxRow[]): Shown => {
    if (!v || v.at !== at) return { at, rows, fresh: new Set(), id: 0 };
    if (v.rows === rows) return v;
    const known = new Set(v.rows.map(keyOf));
    const fresh = new Set(rows.map(keyOf).filter((k) => !known.has(k)));
    return { at, rows, fresh, id: fresh.size ? v.id + 1 : v.id };
  }, [at]);
  useEffect(() => {
    if (!d || !mine) return;
    setShown((v) => (!v || v.at !== at || !hold ? take(v, d.txs) : v));
  }, [d, mine, hold, at, take]);
  const rows = shown?.at === at ? shown.rows : mine ? d!.txs : shown?.rows ?? [];
  const waiting = !!shown && shown.at !== at;
  // the rows that came while the reader held the list: the pill counts them
  const onScreen = new Set((shown?.at === at ? shown.rows : []).map(keyOf));
  const pending = hold && mine && shown?.at === at ? d!.txs.filter((t) => !onScreen.has(keyOf(t))).length : 0;

  // the rows that were there slide down by what came in, once
  const [motion, setMotion] = useState(true);
  useEffect(() => { setMotion(!reducedMotion()); }, []);
  useLayoutEffect(() => {
    const tb = bodyRef.current;
    if (!tb || !shown || shown.id === 0 || !motion) return;
    const trs = [...tb.children] as HTMLElement[];
    const firstOld = trs.find((tr) => tr.dataset.k && !shown.fresh.has(tr.dataset.k));
    if (!firstOld) return;
    const shift = firstOld.offsetTop - trs[0].offsetTop;
    if (shift <= 0) return;
    const anims = trs.map((tr) => tr.animate([{ transform: `translateY(${-shift}px)` }, { transform: "none" }], { duration: SLIDE_MS, easing: "cubic-bezier(.2, .8, .2, 1)" }));
    return () => anims.forEach((a) => a.cancel());
  }, [shown?.id]); // eslint-disable-line react-hooks/exhaustive-deps

  // the ages, on the observer's clock
  const [now, setNow] = useState(0);
  useEffect(() => {
    const tick = () => setNow(Date.now() + skew);
    tick();
    const t = window.setInterval(tick, 15000);
    return () => window.clearInterval(t);
  }, [skew]);

  const router = useRouter();
  const onOpen = useCallback((e: React.MouseEvent, href: string) => {
    if (e.defaultPrevented || e.button !== 0 || e.metaKey || e.ctrlKey || e.shiftKey || e.altKey) return;
    router.push(href);
  }, [router]);

  const total = d?.total ?? 0;
  const failed = d?.failed ?? 0;
  const pages = Math.min(maxPages ?? Infinity, Math.max(1, Math.ceil(total / size)));

  // the statement foot: the successful sums alone (a failed row is in no line), shown only when it closes exactly on the
  // balance, a line only when something moved, under the last page
  const foot: { label: string; utia: number; sign: string; tot?: boolean }[] = [];
  if (view === "escrow" && close && d?.sums) {
    const { deposited_utia: dep, withdrawn_utia: out, charged_utia: charged } = d.sums;
    if (dep - close.fees_utia - charged - out === close.balance_utia) {
      foot.push({ label: "Deposited", utia: dep, sign: "+" });
      if (close.fees_utia) foot.push({ label: `Fees paid for ${int(close.settlements)} settlement${close.settlements === 1 ? "" : "s"}`, utia: close.fees_utia, sign: "−" });
      if (charged) foot.push({ label: "Charged for timed-out promises", utia: charged, sign: "−" });
      if (out) foot.push({ label: "Withdrawn", utia: out, sign: "−" });
      foot.push({ label: "Escrow now", utia: close.balance_utia, sign: "", tot: true });
    }
  }
  const footShown = foot.length > 0 && page >= pages;
  // the column's one precision, over the page and its foot
  const dec = decimals([...rows.filter((t) => t.status === "success").map((t) => t.amount_utia ?? 0), ...(footShown ? foot.map((f) => f.utia) : [])]);
  const fresh = motion ? shown?.fresh : undefined;
  const bringIn = () => { if (d && mine) setShown((v) => take(v, d.txs)); const top = wrapRef.current?.getBoundingClientRect().top ?? 0; if (top < 0) window.scrollBy({ top: top - 12, behavior: motion ? "smooth" : "auto" }); };

  const loaded = rows.length > 0 || mine;
  return (
    <>
      <div className="lg-wrap" ref={wrapRef}
        onPointerEnter={(e) => { if (e.pointerType === "mouse") setPointerIn(true); }}
        onPointerLeave={() => setPointerIn(false)}>
        <div className="lg-newbar">
          {pending > 0 && (
            <button type="button" className="lg-new" onClick={bringIn}>
              <i className="d" aria-hidden="true" /><b>{int(pending)}</b> new transaction{pending === 1 ? "" : "s"}
              <svg width="12" height="12" viewBox="0 0 12 12" aria-hidden="true"><path d="M6 10V2.5M2.8 5.5 6 2.3l3.2 3.2" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" /></svg>
            </button>
          )}
        </div>
        <div className={`lg-tw${waiting ? " is-waiting" : ""}`} aria-busy={waiting || !loaded}>
          <table className={`lg-t tx-t${view === "escrow" ? " pb-st" : ""}`}>
            <thead>
              <tr>
                <th className="c-x" title="The transaction's hash. Its page has the gas, the fee, the messages and, if it failed, why.">TX hash</th>
                <th className="c-k">Type</th>
                <th className="c-tb">Time <span className="per">(UTC)</span> · Block</th>
                <th className="c-st" title="Whether the transaction took effect. A failed one changed nothing.">Status</th>
                <th className="c-am num" title={view === "escrow"
                  ? "What each movement put into the escrow (+) or took out of it (−). A withdrawal request moves its amount into the queue, inside the escrow. A failed one moved nothing."
                  : "What each transaction moved into the escrow (+) or out of it (−): a blob's fee, a deposit, a withdrawal paid out. A withdrawal request moves its amount into the queue, inside the escrow. A failed one moved nothing."}>Amount</th>
              </tr>
            </thead>
            <tbody ref={bodyRef}>
              {!loaded && (q.error
                ? <tr className="lg-empty"><td colSpan={5}>The observer API is not answering ({q.error}).</td></tr>
                : Array.from({ length: Math.min(size, 8) }, (_, i) => (
                  <tr key={i} className="row sk" aria-hidden="true">
                    <td className="c-x"><span className="wait">5DA67B…754E</span></td>
                    <td className="c-k"><span className="wait">Blob payment</span></td>
                    <td className="c-tb"><span className="wait">Oct 8 13:25:59 11 h 55 min #1,497,140</span></td>
                    <td className="c-st"><span className="wait">Success</span></td>
                    <td className="c-am num"><span className="wait">−3.575 TIA</span></td>
                  </tr>
                )))}
              {loaded && rows.length === 0 && <tr className="lg-empty"><td colSpan={5}>{view === "escrow" ? "No escrow movement on record." : "No transaction on record."}</td></tr>}
              {rows.map((t) => <Row key={keyOf(t)} t={t} dec={dec} ag={now ? age(now - Date.parse(t.time)) : null} fresh={!!fresh?.has(keyOf(t))} onOpen={onOpen} />)}
            </tbody>
            {footShown && (
              <tfoot>
                {foot.map((f) => (
                  <tr key={f.label} className={f.tot ? "tot" : undefined}>
                    <td colSpan={4} className="fl">{f.label}</td>
                    <td className="c-am num"><Signed sign={f.sign} utia={f.utia} dec={dec} /></td>
                  </tr>
                ))}
              </tfoot>
            )}
          </table>
        </div>
      </div>
      {loaded && total > 0 && <Pager total={total} page={page} size={size} maxPages={maxPages} onPage={onPage}
        noun={<span title={failed > 0 ? `${int(failed)} of them failed` : undefined}>{total === 1 ? "transaction" : "transactions"}</span>} />}
    </>
  );
}
