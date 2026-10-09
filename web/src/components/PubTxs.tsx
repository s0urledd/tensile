"use client";
import { memo, useCallback, useLayoutEffect, useRef } from "react";
import Link from "next/link";
import { useRouter } from "next/navigation";
import { int, span, tia, utcWord, failedTitle } from "@/lib/api";
import { KIND_WORD } from "@/lib/txkind";
import { openRow } from "@/lib/row";
import { CopyMark, Signed, decimals } from "@/components/Ledger";
import { age, monthDayTime } from "@/components/BlobsDeck";
import Pager from "@/components/Pager";

/*
 * One publisher's transactions, successful and failed, as its page lists them under All (every Fibre transaction of the
 * account) and Escrow (its deposits and withdrawal requests, and under the last page the escrow's statement), a page at
 * a time from /v1/publishers/{addr}/txs. A row is the block, when (over how long ago), the hash, what it was, what it
 * moved and how it came out on chain, in even tracks as a validator's endpoint history draws its transactions: the
 * height at the inset as every table's first column, the rest centred under their heads. A withdrawal request's payout
 * is a quiet line under its type: pending until a payout is on record for it, then when it was paid out. A failed
 * transaction says Failed in red, why on hover, and moved nothing. Every row opens its transaction's page.
 */

/** what became of a successful withdrawal request, as the queue's history says: paid only once a payout is on record */
export type Payout = {
  state: "pending" | "paid" | "consumed" | "unattributed";
  /** when it became payable: its request's block time and the withdrawal delay in force then */
  available_at: string;
  paid_height?: number;
  paid_at?: string;
  paid_utia?: number;
  /** from the request to the payout, whole seconds */
  payout_delay_s?: number;
};
/** one Fibre transaction of the account: a message of a success, or a failed transaction */
export type PubTx = {
  kind: "settlement" | "deposit" | "withdrawal_request" | "timeout";
  status: "success" | "failed";
  height: number; tx_index: number; msg_index: number; time: string; tx_hash: string;
  /** what a success moved: a blob's fee, a timed-out promise's charge, a deposit, the amount a withdrawal asked for; none on a failure */
  amount_utia?: number;
  promise_hash?: string;
  /** a failure's: the chain's code, the reason in plain words where Tensile has one, and whether it can never land */
  code?: number; codespace?: string; reason?: string; final?: boolean;
  /** a successful withdrawal request's, when its withdrawal is on record in the queue's history */
  payout?: Payout;
};
/** the escrow's statement over the account's whole record: its successful movements alone */
export type PubTxSums = { deposited_utia: number; settlements: number; fees_utia: number; timeouts: number; charged_utia: number; withdrawn_utia: number };
export type PubTxs = {
  publisher: string; view: "all" | "escrow"; limit: number; offset: number;
  /** every transaction the view holds, successful and failed, and how many of them failed */
  total: number; failed_total: number;
  txs: PubTx[];
  /** Escrow only */
  sums?: PubTxSums;
};
/** the statement's lines under Escrow's last page: the labels up to the Amount column, the figures in it */
export type Foot = { label: string; utia: number; sign: string; tot?: boolean }[];

type Col = "bh" | "tm" | "x" | "k" | "am" | "st";
const COLS: Col[] = ["bh", "tm", "x", "k", "am", "st"];

const NOTHING = "Nothing moved: the escrow is as it was.";
const QUEUED = "Moved from available to the withdrawal queue; the balance changes when it is paid out.";

/** "Oct 7 16:02": the minute is enough in a line; the second and UTC are on hover */
const monthDayMin = (s: string) => monthDayTime(s).slice(0, -3);

/** a request's payout as the quiet line under its type, and that line's hover; none where the history cannot say */
function payoutLine(p: Payout, asked: number | undefined): { line: string; title: string; tone: string } | null {
  switch (p.state) {
    case "paid": {
      const paid = p.paid_utia != null && asked != null && p.paid_utia !== asked ? ` ${tia(p.paid_utia)}` : "";
      return {
        line: p.paid_at ? `paid out ${monthDayMin(p.paid_at)}` : "paid out", tone: "",
        title: [`Paid out${paid}${p.paid_height ? ` in block #${int(p.paid_height)}` : ""}${p.paid_at ? `, ${utcWord(p.paid_at)}` : ""}`,
          p.payout_delay_s != null ? `${span(p.payout_delay_s)} after the request` : ""].filter(Boolean).join(", "),
      };
    }
    case "consumed":
      return { line: "used by settlements", tone: "hold", title: "Used by the account's own settlements before it could be paid out" };
    case "pending":
      return { line: "payout pending", tone: "", title: `Payable from ${utcWord(p.available_at)}; paid out in the first block after` };
    default:
      return null;
  }
}

function Head({ c, esc }: { c: Col; esc: boolean }) {
  switch (c) {
    case "bh": return <th className="c-bh">Height</th>;
    case "tm": return <th className="c-tm">Time <span className="per">(UTC)</span></th>;
    case "x": return <th className="c-x">TX hash</th>;
    case "k": return <th className="c-k">Type</th>;
    case "am": return <th className="c-am" title={`What each transaction moved into the escrow (+) or out of it (−)${esc ? "" : ": a blob's fee, a deposit"}; a withdrawal request's amount, unsigned, moves only when it is paid out. A failed one moved nothing.`}>Amount</th>;
    case "st": return <th className="c-st" title="The transaction's outcome on chain.">Status</th>;
  }
}

const Row = memo(function Row({ t, dec, now, onOpen }: { t: PubTx; dec: number; now: number; onOpen: (e: React.MouseEvent, href: string) => void }) {
  const failed = t.status === "failed";
  const H = t.tx_hash.toUpperCase();
  const href = `/tx/?hash=${t.tx_hash.toLowerCase()}`;
  const word = KIND_WORD[t.kind] ?? t.kind;
  const wr = !failed && t.kind === "withdrawal_request";
  const po = wr && t.payout ? payoutLine(t.payout, t.amount_utia) : null;
  const ag = now ? age(now - Date.parse(t.time)) : "";
  const nil = failed || t.amount_utia == null;
  const amount = nil
    ? <span className="na" title={NOTHING}>—</span>
    : <Signed sign={t.kind === "deposit" ? "+" : wr ? "" : "−"} utia={t.amount_utia!} dec={dec} />;
  const tone = failed ? " xf" : wr ? " wr" : t.kind === "timeout" ? " to" : "";
  return (
    // The whole row opens the transaction; the hash's link and copy mark keep their own.
    <tr className={`row${tone}`} onClick={(e) => openRow(e, href, onOpen)} onAuxClick={(e) => openRow(e, href, onOpen)}>
      <td className="c-bh">{int(t.height)}</td>
      <td className="c-tm"><span className="tw" title={utcWord(t.time)}><span className="tm">{monthDayTime(t.time)}</span><span className="ag">{ag}</span></span></td>
      <td className="c-x">
        <Link href={href} title={`Transaction ${H}`} aria-label={`Transaction ${H.slice(0, 6)}…${H.slice(-4)}, ${failed ? "failed " : ""}${word.toLowerCase()}, height ${int(t.height)}`}>{H.slice(0, 6)}<span className="el">…</span>{H.slice(-4)}</Link>
        <CopyMark text={H} label="the transaction hash" />
      </td>
      <td className="c-k"><span className="kw"><span>{word}</span>{po && <span className={`k2${po.tone ? ` ${po.tone}` : ""}`} title={po.title}>{po.line}</span>}</span></td>
      <td className="c-am" title={wr ? QUEUED : undefined}><span className={`amb${nil ? " nil" : ""}`}>{amount}</span></td>
      <td className="c-st">{failed
        ? <span className="st f" title={failedTitle(t.reason)}><i className="dot fault" />Failed</span>
        : <span className="st"><i className="dot ok" />Success</span>}</td>
    </tr>
  );
});

/** the first page's places while it loads: a page of rows, each cell's shape at its size */
function Placeholders({ rows }: { rows: number }) {
  return (
    <>
      {Array.from({ length: rows }, (_, i) => (
        <tr key={i} className="row sk" aria-hidden="true">
          <td className="c-bh"><span className="wait">1,204,085</span></td>
          <td className="c-tm"><span className="tw"><span className="tm"><span className="wait">Sep 28 20:48:38</span></span><span className="ag"><span className="wait">2 d 21 h</span></span></span></td>
          <td className="c-x"><span className="wait">6A2F9D…B5A8</span></td>
          <td className="c-k"><span className="kw"><span><span className="wait">Blob payment</span></span></span></td>
          <td className="c-am"><span className="amb"><span className="wait">−3.575 TIA</span></span></td>
          <td className="c-st"><span className="wait">Success</span></td>
        </tr>
      ))}
    </>
  );
}

/**
 * All (view "all") or Escrow (view "escrow") at offset: data is the route's last answer, which while the next page or
 * the other view loads is the one before it (its rows stay, quieted, for the same view, until the next answer or its
 * error comes; the other view's are never shown); foot is the statement's lines, which close Escrow's last page.
 */
export default function TxList({ view, offset, data, error, page, size, onPage, now, foot }: {
  view: "all" | "escrow"; offset: number; data: PubTxs | null; error: string | null;
  page: number; size: number; onPage: (p: number) => void; now: number; foot: Foot;
}) {
  const router = useRouter();
  const onOpen = useCallback((e: React.MouseEvent, href: string) => {
    if (e.defaultPrevented || e.button !== 0 || e.metaKey || e.ctrlKey || e.shiftKey || e.altKey) return;
    router.push(href);
  }, [router]);
  const esc = view === "escrow";
  const stale = !!data && data.view === view && data.offset !== offset;
  const mine = data && data.view === view && !(stale && error) ? data : null;
  const waiting = !!mine && stale;
  const rows = mine?.txs ?? [];
  const last = !!mine && !waiting && page >= Math.ceil(mine.total / size);
  const lines = esc && last ? foot : [];
  const dec = decimals([...rows.map((t) => (t.status === "failed" ? 0 : t.amount_utia ?? 0)), ...lines.map((f) => f.utia)]);
  // the amounts right-aligned in one box as wide as the widest on the page, the box centred under its head, so the
  // digits stack and the column stays centred
  const ref = useRef<HTMLTableElement>(null);
  useLayoutEffect(() => {
    const t = ref.current;
    if (!t) return;
    const fit = () => {
      const w = Math.max(0, ...[...t.querySelectorAll<HTMLElement>(".amb > span")].map((s) => s.getBoundingClientRect().width));
      if (w) t.style.setProperty("--am-w", `${Math.ceil(w)}px`);
    };
    fit();
    document.fonts?.ready.then(fit);
  }, [rows, lines.length]);
  const amAt = COLS.indexOf("am");
  const noun = esc ? `escrow transaction${mine?.total === 1 ? "" : "s"}` : `transaction${mine?.total === 1 ? "" : "s"}`;
  return (
    <>
      <div className={`lg-tw${waiting ? " is-waiting" : ""}`} aria-busy={!mine || waiting}>
        {!mine && !error && <span className="sr-only">Loading…</span>}
        <table ref={ref} className={`lg-t ptx-t${esc ? " pb-st" : ""}`}>
          <thead><tr>{COLS.map((c) => <Head key={c} c={c} esc={esc} />)}</tr></thead>
          <tbody>
            {!mine && (error
              ? <tr className="lg-empty"><td colSpan={COLS.length}>The observer API is not answering ({error}).</td></tr>
              : <Placeholders rows={size} />)}
            {mine && rows.length === 0 && <tr className="lg-empty"><td colSpan={COLS.length}>{esc ? "No escrow transaction on record." : "No transaction on record."}</td></tr>}
            {rows.map((t) => <Row key={`${t.status}-${t.height}-${t.tx_index}-${t.msg_index}`} t={t} dec={dec} now={now} onOpen={onOpen} />)}
          </tbody>
          {lines.length > 0 && (
            <tfoot>
              {lines.map((f) => (
                <tr key={f.label} className={f.tot ? "tot" : undefined}>
                  <td colSpan={amAt} className="fl">{f.label}</td>
                  <td className="c-am"><span className="amb"><Signed sign={f.sign} utia={f.utia} dec={dec} /></span></td>
                  {COLS.slice(amAt + 1).map((c) => <td key={c} className={`c-${c}`} />)}
                </tr>
              ))}
            </tfoot>
          )}
        </table>
      </div>
      {mine
        ? mine.total > 0 && (
          <Pager total={mine.total} page={page} size={size} onPage={onPage}
            noun={<span title={mine.failed_total ? `${int(mine.failed_total)} of them failed` : undefined}>{noun}</span>} />
        )
        : !error && (
          <div className="pager" aria-hidden="true">
            <span className="count"><span className="wait">Showing 1–25 of 000 transactions</span></span>
          </div>
        )}
    </>
  );
}
