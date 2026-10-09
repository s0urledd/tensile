"use client";
import { memo, useCallback, useLayoutEffect, useRef } from "react";
import Link from "next/link";
import { useRouter } from "next/navigation";
import { int, span, utcWord } from "@/lib/api";
import { KIND_WORD } from "@/lib/txkind";
import { openRow } from "@/lib/row";
import { CopyMark, Signed, decimals } from "@/components/Ledger";
import { failedTitle } from "@/components/EndpointHistory";
import { age, monthDayTime } from "@/components/BlobsDeck";
import Pager from "@/components/Pager";

/*
 * ---- STUB (design/publisher-txs, a throwaway design branch that is never merged) ----
 * A publisher's transactions, successful and failed, under its page's All | Blobs | Escrow tabs, in three alternatives
 * switched by ?pv= (1: the brief as written; 2: the validator's Endpoint history order, one "Status" head, evenly
 * spread; 3: as 2, a withdrawal's payout a quiet second line under its type, so Escrow has All's columns exactly).
 * All and Escrow read a route the API does not have yet, /v1/publishers/{addr}/txs?view=all|escrow, answered by the
 * design-shots script's mock; Blobs is the Blobs list's own table (Ledger), its Status column as the list's variant A.
 */

/** the alternative, from ?pv=; 0 is today's page */
export type Pv = 0 | 1 | 2 | 3;
export const pvOf = (s: string | null): Pv => (s === "1" ? 1 : s === "2" ? 2 : s === "3" ? 3 : 0);
/** the Status column's head: the brief's "TX status" (1), or the word the Blobs list and the validator page use (2, 3) */
export const stWord = (pv: Pv) => (pv === 1 ? "TX status" : "Status");

/**
 * what became of a successful withdrawal request: queued until available_at, then paid out in the first block after it
 * (paid: a payout on record, never inferred from the time), or used by the account's own settlements before it could be
 */
export type Payout = {
  state: "pending" | "paid" | "consumed";
  available_at: string;
  paid_height?: number; paid_at?: string; paid_utia?: number; payout_delay_s?: number;
};
/** one Fibre transaction of the account, as the proposed route would serve it */
export type PubTx = {
  kind: "settlement" | "deposit" | "withdrawal_request" | "timeout";
  status: "success" | "failed";
  height: number; tx_index: number; msg_index: number; time: string; tx_hash: string;
  /** what it moved: a blob's fee, a deposit, the amount requested; absent on a failed one, which moved nothing */
  amount_utia?: number;
  /** a failed one's reason, when Tensile has one */
  reason?: string;
  promise_hash?: string;
  /** a successful withdrawal request only */
  payout?: Payout;
};
export type PubTxs = {
  publisher: string; view: "all" | "escrow"; limit: number; offset: number;
  /** every transaction the view holds, successful and failed, and how many of them failed */
  total: number; failed: number;
  txs: PubTx[];
  /** escrow only: what the successful ones put in and took out over the whole record */
  sums?: { deposited_utia: number; withdrawn_utia: number; charged_utia: number };
};
export type Foot = { label: string; utia: number; sign: string; tot?: boolean }[];

type Col = "bh" | "tm" | "x" | "k" | "am" | "st" | "po";
const COLS: Record<1 | 2 | 3, { all: Col[]; escrow: Col[] }> = {
  1: { all: ["x", "k", "tm", "bh", "am", "st"], escrow: ["x", "k", "tm", "am", "st", "po"] },
  2: { all: ["bh", "tm", "x", "k", "am", "st"], escrow: ["bh", "tm", "x", "k", "am", "st", "po"] },
  3: { all: ["bh", "tm", "x", "k", "am", "st"], escrow: ["bh", "tm", "x", "k", "am", "st"] },
};

const NOTHING = "Nothing moved: the escrow is as it was.";
const QUEUED = "Moved from available to the withdrawal queue; the balance changes when it is paid out.";

/** the payout of a request in words: its own column's word (1, 2), or the quiet line under the type (3), and its hover */
function payoutOf(p: Payout): { word: string; line: string; title: string; tone: string } {
  if (p.state === "paid") {
    const at = p.paid_at ? monthDayTime(p.paid_at).slice(0, -3) : "";
    return {
      word: "Paid", line: `paid out ${at}`.trim(), tone: "paid",
      title: [`Paid out${p.paid_height ? ` in block #${int(p.paid_height)}` : ""}${p.paid_at ? `, ${utcWord(p.paid_at)}` : ""}`, p.payout_delay_s != null ? `${span(p.payout_delay_s)} after the request` : ""].filter(Boolean).join(", "),
    };
  }
  if (p.state === "consumed") return { word: "Not paid", line: "used by settlements", tone: "hold", title: "Used by the account's own settlements before it could be paid out" };
  return { word: "Pending", line: "payout pending", tone: "", title: `Payable from ${utcWord(p.available_at)}; paid out in the first block after` };
}

function Head({ c, pv, esc }: { c: Col; pv: Pv; esc: boolean }) {
  switch (c) {
    case "bh": return <th className={`c-bh${pv === 1 ? " num" : ""}`}>Height</th>;
    case "tm": return <th className="c-tm">Time <span className="per">(UTC)</span></th>;
    case "x": return <th className="c-x">TX hash</th>;
    case "k": return <th className="c-k">Type</th>;
    case "am": return <th className={`c-am${pv === 1 ? " num" : ""}`} title={`What each transaction moved into the escrow (+) or out of it (−)${esc ? "" : ": a blob's fee, a deposit"}. A failed one moved nothing.`}>Amount</th>;
    case "st": return <th className="c-st" title="The transaction's outcome on chain.">{stWord(pv)}</th>;
    case "po": return <th className="c-po" title="A withdrawal is paid out in the first block after its waiting time; Paid once that payout is on record.">Payout</th>;
  }
}

const Row = memo(function Row({ t, cols, pv, dec, now, onOpen }: { t: PubTx; cols: Col[]; pv: Pv; dec: number; now: number; onOpen: (e: React.MouseEvent, href: string) => void }) {
  const failed = t.status === "failed";
  const H = t.tx_hash.toUpperCase();
  const href = `/tx/?hash=${t.tx_hash.toLowerCase()}`;
  const word = KIND_WORD[t.kind];
  const po = !failed && t.kind === "withdrawal_request" && t.payout ? payoutOf(t.payout) : null;
  const ag = now ? age(now - Date.parse(t.time)) : "";
  const amount = (() => {
    if (failed || t.amount_utia == null) return <span className="na" title={NOTHING}>—</span>;
    const sign = t.kind === "deposit" ? "+" : t.kind === "withdrawal_request" ? "" : "−";
    return <Signed sign={sign} utia={t.amount_utia} dec={dec} />;
  })();
  const cell = (c: Col) => {
    switch (c) {
      case "bh": return <td key={c} className={`c-bh${pv === 1 ? " num" : ""}`}>{int(t.height)}</td>;
      case "tm": return (
        <td key={c} className="c-tm">{pv === 1
          ? <span title={utcWord(t.time)}><span className="tm">{monthDayTime(t.time)}</span>{ag && <span className="ag">{ag}</span>}</span>
          : <span className="tw" title={utcWord(t.time)}><span className="tm">{monthDayTime(t.time)}</span><span className="ag">{ag}</span></span>}</td>
      );
      case "x": return (
        <td key={c} className="c-x">
          <Link href={href} title={`Transaction ${H}`} aria-label={`Transaction ${H.slice(0, 6)}…${H.slice(-4)}, ${failed ? "failed " : ""}${word.toLowerCase()}, height ${int(t.height)}`}>{H.slice(0, 6)}<span className="el">…</span>{H.slice(-4)}</Link>
          <CopyMark text={H} label="the transaction hash" />
        </td>
      );
      case "k": return (
        <td key={c} className="c-k">{pv === 3 && po
          ? <span className="kw"><span>{word}</span><span className={`k2${po.tone ? ` ${po.tone}` : ""}`} title={po.title}>{po.line}</span></span>
          : pv === 3 ? <span className="kw"><span>{word}</span></span> : word}</td>
      );
      case "am": return (
        <td key={c} className={`c-am${pv === 1 ? " num" : ""}`} title={!failed && t.kind === "withdrawal_request" ? QUEUED : undefined}>{pv === 1 ? amount : <span className={`amb${failed || t.amount_utia == null ? " nil" : ""}`}>{amount}</span>}</td>
      );
      case "st": return (
        <td key={c} className="c-st">{failed
          ? <span className="st f" title={failedTitle(t.reason)}><i className="dot fault" />Failed</span>
          : <span className="st"><i className="dot ok" />Success</span>}</td>
      );
      case "po": return <td key={c} className="c-po">{po && <span className={`po ${po.tone}`} title={po.title}>{po.word}</span>}</td>;
    }
  };
  const tone = failed ? " xf" : t.kind === "withdrawal_request" ? " wr" : t.kind === "timeout" ? " to" : "";
  return (
    <tr className={`row${tone}`} onClick={(e) => openRow(e, href, onOpen)} onAuxClick={(e) => openRow(e, href, onOpen)}>
      {cols.map(cell)}
    </tr>
  );
});

/**
 * All (view "all"): every transaction of the account, successful and failed, newest first, one stream; Escrow (view
 * "escrow"): its deposits and withdrawal requests, each request's payout beside it (1, 2) or under its type (3), and the
 * statement's lines under the last page. Every row opens its transaction's page.
 */
export default function TxList({ pv, view, data, error, page, size, onPage, now, foot }: {
  pv: 1 | 2 | 3; view: "all" | "escrow"; data: PubTxs | null; error: string | null;
  page: number; size: number; onPage: (p: number) => void; now: number; foot: Foot;
}) {
  const router = useRouter();
  const onOpen = useCallback((e: React.MouseEvent, href: string) => {
    if (e.defaultPrevented || e.button !== 0 || e.metaKey || e.ctrlKey || e.shiftKey || e.altKey) return;
    router.push(href);
  }, [router]);
  const esc = view === "escrow";
  const cols = COLS[pv][view];
  const rows = data?.txs ?? [];
  const last = !!data && page >= Math.ceil(data.total / size);
  const lines = esc && last ? foot : [];
  const dec = decimals([...rows.map((t) => (t.status === "failed" ? 0 : t.amount_utia ?? 0)), ...lines.map((f) => f.utia)]);
  // 2, 3: the amounts right-aligned in one box as wide as the widest on the page, the box centred under its head, so
  // the digits stack and the column stays centred
  const ref = useRef<HTMLTableElement>(null);
  useLayoutEffect(() => {
    const t = ref.current;
    if (!t || pv === 1) return;
    const fit = () => {
      const w = Math.max(0, ...[...t.querySelectorAll<HTMLElement>(".amb > span")].map((s) => s.getBoundingClientRect().width));
      if (w) t.style.setProperty("--am-w", `${Math.ceil(w)}px`);
    };
    fit();
    document.fonts?.ready.then(fit);
  }, [pv, rows, lines.length]);
  const amAt = cols.indexOf("am");
  return (
    <>
      <div className="lg-tw">
        <table ref={ref} className={`lg-t ptx-t ptx-${pv}${esc ? " ptx-esc pb-st" : ""}`}>
          <thead><tr>{cols.map((c) => <Head key={c} c={c} pv={pv} esc={esc} />)}</tr></thead>
          <tbody>
            {!data && <tr className="lg-empty"><td colSpan={cols.length}>{error ? `The observer API is not answering (${error}).` : "Loading…"}</td></tr>}
            {data && rows.length === 0 && <tr className="lg-empty"><td colSpan={cols.length}>{esc ? "No escrow transaction on record." : "No transaction on record."}</td></tr>}
            {rows.map((t) => <Row key={`${t.tx_hash}-${t.msg_index}`} t={t} cols={cols} pv={pv} dec={dec} now={now} onOpen={onOpen} />)}
          </tbody>
          {lines.length > 0 && (
            <tfoot>
              {lines.map((f) => (
                <tr key={f.label} className={f.tot ? "tot" : undefined}>
                  <td colSpan={amAt} className="fl">{f.label}</td>
                  <td className={`c-am${pv === 1 ? " num" : ""}`}>{pv === 1 ? <Signed sign={f.sign} utia={f.utia} dec={dec} /> : <span className="amb"><Signed sign={f.sign} utia={f.utia} dec={dec} /></span>}</td>
                  {cols.slice(amAt + 1).map((c) => <td key={c} className={`c-${c}`} />)}
                </tr>
              ))}
            </tfoot>
          )}
        </table>
      </div>
      {data && data.total > 0 && (
        <Pager total={data.total} page={page} size={size} onPage={onPage}
          noun={<span title={data.failed ? `${int(data.failed)} of them failed` : undefined}>{esc ? `escrow transaction${data.total === 1 ? "" : "s"}` : `transaction${data.total === 1 ? "" : "s"}`}</span>} />
      )}
    </>
  );
}
