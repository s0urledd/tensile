"use client";
import type { ReactNode } from "react";
import Link from "next/link";
import { useApi, notFound, type FailedTx, type Meta, type TxAnswer, type TxKind, type TxMsg, bytes, coins, exactCoin, feeTia, int, nsDisplay, span, tia, utcWord, shortMid } from "@/lib/api";
import { bech32Hex, validatorHref } from "@/lib/addr";
import { blobIdOf } from "@/lib/blobkey";
import StatusLine from "@/components/StatusLine";
import Copy from "@/components/Copy";
import Warn from "@/components/Warn";
import Avatar from "@/components/Avatar";
import Info from "@/components/Info";
import { Eye } from "@/components/Metrics";
import { lane } from "@/lib/status";
import { CopyMark, Who } from "@/components/Ledger";
import { unit } from "@/components/Unit";
import { monthDayTime } from "@/components/BlobsDeck";

/**
 * One Fibre transaction by its hash (/tx/?hash=, and /blob/?tx= for a failure), the same page whether it took effect or
 * failed: the blob page's mast (its title, its hash with the copy button, its status, type and block as chips), its two
 * light frames (on the left what it touches, the pages it links to, and its gas; on the right what it asked for, moved
 * and cost, ending with its fee), then, for a failure, the error as the node returned it, and its messages.
 *
 * Two results never mix: the mast's status chip is the chain's outcome, with no eye; a blob payment's blob carries
 * Tensile's own result for it beside its link, the Blobs list's Tensile chip with Tensile's eye.
 */

/** past this many messages, only the one that failed and the first this many that carry Fibre are listed */
const LISTED = 12;

/** the transaction's own kind in words, as the lists name it */
const KIND_WORD: Record<Exclude<TxKind, "several">, string> = {
  settlement: "Blob payment", deposit: "Deposit", withdrawal_request: "Withdrawal request", withdrawal_executed: "Withdrawal payout",
  timeout: "Promise timeout", set_host: "Endpoint registration",
};
/** the kind a Fibre message's type URL is, for a record from an API before /v1/txs */
const KIND_OF: Record<string, Exclude<TxKind, "several">> = {
  "/celestia.fibre.v1.MsgPayForFibre": "settlement", "/celestia.fibre.v1.MsgDepositToEscrow": "deposit",
  "/celestia.fibre.v1.MsgRequestWithdrawal": "withdrawal_request", "/celestia.fibre.v1.MsgPaymentPromiseTimeout": "timeout",
  "/celestia.valaddr.v1.MsgSetFibreProviderInfo": "set_host",
};

/** the two cost rows' hovers, the same wherever they stand */
export const GAS_TITLE = "The transaction's own gas: what it used of the limit it set.";
export const FEE_TITLE = "Paid for the transaction itself from the fee payer's bank balance, never from the escrow.";

/** a message's name: its type URL after the last dot */
const nameOf = (url: string) => url.slice(url.lastIndexOf(".") + 1);
/** a message, or a MsgExec with a message inside it, that carries Fibre */
const carries = (m: TxMsg) => m.fibre || !!m.inner?.some((i) => i.fibre);
/** two spellings of one account (a validator's operator address and its account address) are the same account */
const sameAcct = (a?: string, b?: string) => !!a && !!b && (bech32Hex(a) ?? a) === (bech32Hex(b) ?? b);
/** an account, short: its prefix, then its last four ("celestia1…abcd") */
const shortAcct = (a: string) => `${a.slice(0, a.indexOf("1") + 1)}…${a.slice(-4)}`;

/** Gas, in one format everywhere: what it used, of the limit it set */
export function GasValue({ c }: { c?: { gas_used: number; gas_wanted: number } | null }) {
  return c ? <><b>{int(c.gas_used)}</b><em>used of {int(c.gas_wanted)}</em></> : <em>not recorded</em>;
}

/**
 * The transaction fee, the same on a success and a failure: in TIA down to the utia, the figure bold and its unit light,
 * the chain's own figure on hover; then where it came from: the bank balance, or the account that paid it when that is
 * not the page's own (owner). One fee covers every message of the transaction, which it says when there was more than one.
 */
export function FeeValue({ fee, payer, owner, messages = 1 }: { fee?: string; payer?: string; owner?: string; messages?: number }) {
  if (!fee) return <b>None</b>;
  const other = !!payer && !!owner && !sameAcct(payer, owner);
  const text = feeTia(fee);
  return (
    <>
      <b title={coins(fee)}>{text.includes(",") ? text : unit(text)}</b>
      <em>{other ? <span title={payer}>paid by {shortAcct(payer!)}</span> : "from the bank balance"}{messages > 1 && " · whole transaction"}</em>
    </>
  );
}

/** a validator as a chip: its picture or initials, its name; it opens its page */
function ValChip({ v }: { v: { operator_address: string; moniker?: string; avatar_url?: string } }) {
  return (
    <Link className="tx-val" href={validatorHref(v.operator_address, v.operator_address)} title={v.operator_address}>
      <Avatar v={{ avatar_url: v.avatar_url, moniker: v.moniker, address: v.operator_address }} />
      <span>{v.moniker || shortMid(v.operator_address, 18, 4)}</span>
    </Link>
  );
}

/** a record from an API before /v1/txs (the failed_tx /blob/?tx= already has), as the page reads it */
function fromFailed(hex: string, f: FailedTx): TxAnswer {
  const fibre = f.messages.filter(carries);
  const kinds = [...new Set(fibre.map((m) => KIND_OF[m.fibre ? m.type_url : m.inner?.find((i) => i.fibre)?.type_url ?? ""])
    .filter((k): k is Exclude<TxKind, "several"> => !!k))];
  return {
    tx_hash: hex.toLowerCase(), status: "failed", final: f.ante_passed, height: f.height, tx_index: -1, time: f.time,
    kind: kinds.length === 1 ? kinds[0] : "several",
    messages: f.messages,
    cost: { gas_wanted: f.gas_wanted, gas_used: f.gas_used, fee: f.fee },
    failure: { code: f.code, codespace: f.codespace, reason: f.reason, failed_msg_index: f.failed_msg_index, log: f.log, log_cut: f.log_cut },
    effect: {}, related: {},
  };
}

export default function TxDetail({ hex, fallback }: { hex: string; fallback?: FailedTx | null }) {
  const { data: meta, error: metaErr } = useApi<Meta>("/v1/meta");
  const q = useApi<TxAnswer>(`/v1/txs/${hex.toLowerCase()}`, 0);
  const HEX = hex.toUpperCase();
  // an API before /v1/txs answers 404 for every hash: a failure /blob/?tx= found already stands in
  const t = q.data ?? (notFound(q) && fallback ? fromFailed(hex, fallback) : null);
  if (!t) {
    return (
      <>
        <section className="pb-mast bd-mast">
          <h1 className="bd-title">{notFound(q) ? "Transaction not on record" : q.error ? "Transaction" : "Loading…"}</h1>
          <div className="pb-addr"><span className="bd-idl">Hash</span><span className="mono">{HEX}</span><Copy text={HEX} label="transaction hash" /></div>
        </section>
        <StatusLine meta={meta} metaError={metaErr} snap={null} client={{ error: notFound(q) ? null : q.error, fetchedAt: q.fetchedAt, status: q.status }} />
        {notFound(q) && <p className="notice">No Fibre transaction with this hash on record. Tensile keeps Fibre transactions: blob payments, escrow deposits and withdrawals, promise timeouts and endpoint registrations.</p>}
      </>
    );
  }
  return <Page t={t} HEX={HEX} meta={meta} metaErr={metaErr} at={q.fetchedAt} />;
}

function Page({ t, HEX, meta, metaErr, at }: { t: TxAnswer; HEX: string; meta: Meta | null; metaErr: string | null; at: string | null }) {
  const failed = t.status === "failed";
  const f = t.failure;
  const x = t.effect;
  const r = t.related;
  const fibre = t.messages.filter(carries);
  // the type in words; a transaction of Fibre messages of different kinds says how many it carries
  const word = t.kind === "several" ? `${int(fibre.length)} messages` : KIND_WORD[t.kind];
  const typeUrl = fibre[0] ? (fibre[0].fibre ? fibre[0].type_url : fibre[0].inner?.find((i) => i.fibre)?.type_url) : undefined;
  // the account the page is about: the publisher, else the validator's operator address
  const owner = r.publisher ?? r.validator?.operator_address;
  const signer = fibre.map((m) => m.signer ?? m.inner?.find((i) => i.signer)?.signer).find(Boolean);
  // the fee: not taken from a failure the chain stopped before running it; taken and final otherwise
  const notTaken = failed && t.final === false;
  const feeTitle = failed && t.final ? `${FEE_TITLE} It was taken, so this transaction cannot run again.` : FEE_TITLE;

  // ---- the left frame: what it touches, its pages first, then its gas ----
  const left: ReactNode[] = [];
  const row = (k: string, dt: ReactNode, dd: ReactNode, dtTitle?: string, ddTitle?: string) => {
    left.push(<dt key={`${k}t`} title={dtTitle}>{dt}</dt>, <dd key={`${k}d`} title={ddTitle}>{dd}</dd>);
  };
  if (r.publisher) row("pub", "Publisher", <Who addr={r.publisher} />);
  else if (t.kind === "timeout" && failed) row("pub", "Publisher", <em>as the message names it</em>);
  if (r.validator) row("val", "Validator", <ValChip v={r.validator} />);
  else if (t.kind === "set_host" && failed) row("val", "Validator", <em>none: the signer is not a validator</em>);
  if (t.kind === "settlement" && !failed && r.blob) {
    // the blob it settled, a link that looks like one; beside it Tensile's own result for the blob, the Blobs list's
    // Tensile chip with Tensile's eye, so it is never read as the chain's result in the mast
    const id = blobIdOf(r.blob.commitment, r.blob.blob_version);
    const b = r.blob;
    const ln = b.must_serve_until ? lane({ must_serve_until: b.must_serve_until, reconstructable: b.reconstructable ?? null }) : null;
    row("blob", "Blob", <>
      <Link className="tx-go mono" href={`/blob/?hash=${b.promise_hash}`} title={`Blob ID ${id} · its blob page`}>{id.slice(0, 6)}…{id.slice(-6)}<span className="ar" aria-hidden="true">→</span></Link>
      {ln && <Info label="Tensile" trigger={<><Eye />{ln.word}</>} title={`Tensile's own reading of this blob · ${ln.title}`}
        className={`tx-tn${ln.tier === "hold" ? " hold" : ln.tier === "kept" ? " ok" : ""}`}><p>{ln.title}</p></Info>}
    </>);
  }
  if ((t.kind === "settlement" && failed) || t.kind === "timeout") {
    if (x.promise_hash) row("ph", "Promise hash", <><span className="mono" title={x.promise_hash}>{shortMid(x.promise_hash, 10, 6)}</span><Copy text={x.promise_hash} label="the promise hash" /></>);
  }
  if (x.namespace) row("ns", "Namespace", <Link className="bd-ns" href={`/blobs/?namespace=${x.namespace}`} title={`${x.namespace} · every blob in it`}>{nsDisplay(x.namespace)}</Link>);
  if (signer && !sameAcct(signer, owner)) row("sig", "Signer", <><span className="mono" title={signer}>{shortAcct(signer)}</span><CopyMark text={signer} label="the signer" /></>);
  row("gas", "Gas", <GasValue c={t.cost} />, GAS_TITLE);

  // ---- the right frame: what it asked for, moved and cost, ending with its fee ----
  const right: ReactNode[] = [];
  const add = (k: string, dt: ReactNode, dd: ReactNode, ddTitle?: string, dtTitle?: string) => {
    right.push(<dt key={`${k}t`} title={dtTitle}>{dt}</dt>, <dd key={`${k}d`} title={ddTitle}>{dd}</dd>);
  };
  const unchanged = () => add("esc", "Escrow", <><b>Unchanged</b><em>nothing moved</em></>);
  const exact = (u?: number) => (u != null ? `${(u / 1e6).toLocaleString("en-US", { minimumFractionDigits: 6 })} TIA` : undefined);
  switch (t.kind) {
    case "settlement":
      if (x.blob_size != null) add("sz", "Blob size", <b>{unit(bytes(x.blob_size))}</b>, "The size the blob paid for: Celestia's upload size, with header and padding, without parity.");
      if (!failed) {
        add("fee", "Fee paid", x.fee_paid_utia != null ? <><b>{unit(tia(x.fee_paid_utia))}</b><em>from escrow · {x.timed_out ? "timed out" : x.settled ? "settled" : "not settled yet"}</em></> : <em>not recorded</em>,
          "Charged to the publisher's escrow; not the settlement transaction's own fee.");
      } else {
        add("fee", "Fee paid", <><b>None</b><em>nothing was charged to the escrow</em></>);
        add("blob", "Blob", r.blob?.settlement_height != null
          ? <><Link className="tx-go mono" href={`/blob/?hash=${r.blob.promise_hash}`}>{blobIdOf(r.blob.commitment, r.blob.blob_version).slice(0, 6)}…<span className="ar" aria-hidden="true">→</span></Link><em>settled later, block #{int(r.blob.settlement_height)}</em></>
          : <em>not settled</em>);
      }
      break;
    case "deposit":
      if (!failed) add("amt", "Amount", <><b>{unit(`+${tia(x.amount_utia ?? 0)}`)}</b><em>into the escrow</em></>, exact(x.amount_utia));
      else {
        if (x.requested) add("rq", "Requested", <><span className="rq">{exactCoin(x.requested)}</span><em>not deposited</em></>);
        unchanged();
      }
      break;
    case "withdrawal_request":
      if (!failed) {
        const w = x.withdrawal;
        add("amt", "Amount", <><b>{unit(tia(x.amount_utia ?? 0))}</b><em>moved to the withdrawal queue</em></>, exact(x.amount_utia));
        if (w) {
          const used = w.reduced_utia ? ` · ${tia(w.reduced_utia)} of it used by settlements` : "";
          add("out", "Payout", w.outcome === "consumed" ? <><b>Used by settlements</b><em>not paid out</em></>
            : w.outcome === "paid" ? <><b>Paid out</b><em>block #{int(w.paid_height ?? 0)}{w.payout_delay_s != null && ` · ${span(w.payout_delay_s)} after the request`}{used}</em></>
            : <><b>Queued</b><em>{w.available_at ? `payable from ${monthDayTime(w.available_at).slice(0, -3)} UTC` : "in the withdrawal queue"}{used}</em></>);
        }
      } else {
        if (x.requested) add("rq", "Requested", <><span className="rq">{exactCoin(x.requested)}</span><em>not withdrawn</em></>);
        unchanged();
      }
      break;
    case "timeout":
      if (!failed) add("amt", "Amount", <><b>{unit(`−${tia(x.amount_utia ?? 0)}`)}</b><em>charged to the escrow</em></>, exact(x.amount_utia));
      else unchanged();
      break;
    case "set_host":
      if (!failed) {
        // the first registration reads Registered; every later one Changed
        add("act", "Action", <b>{x.action === "registered" ? "Registered" : "Changed"}</b>);
        add("ep", "Endpoint", x.action === "changed" && x.previous_host
          ? <><span className="mono eh-o">{x.previous_host}</span><span className="eh-ar" aria-label="to">→</span><span className="mono eh-n">{x.host}</span></>
          : <><span className="mono eh-n">{x.host}</span>{x.action === "same" && <em>same address</em>}</>);
      } else {
        // the row's own word in Endpoint history, what it tried; never bold, as nothing happened
        const tried = x.attempted ?? (x.host_at_block ? "change" : "registration");
        add("act", "Action", tried === "change" ? "Change requested" : "Registration requested");
        if (x.requested_host) add("rq", "Requested", <span className="mono rq">{x.requested_host}</span>, "Requested; the endpoint did not change.");
        add("ep", "Endpoint", <><b>Unchanged</b>{x.host_at_block && <em><span className="mono">{x.host_at_block}</span> stayed registered</em>}</>);
      }
      break;
  }
  add("tf", "Transaction fee", notTaken
    ? <><b>Not taken</b><em>the chain stopped it before running it, so the same transaction could still be included in a later block</em></>
    : t.cost ? <FeeValue fee={t.cost.fee} payer={t.cost.fee_payer} owner={owner} messages={t.messages.length} /> : <em>not recorded</em>,
    undefined, feeTitle);

  // ---- the messages: the failed one and the first LISTED carrying Fibre past LISTED ----
  const failedAt = f?.failed_msg_index;
  const carrying = new Set(fibre.slice(0, LISTED).map((m) => m.index));
  const listed = t.messages.length > LISTED ? t.messages.filter((m) => m.index === failedAt || carrying.has(m.index)) : t.messages;

  return (
    <>
      <section className="pb-mast bd-mast">
        <h1 className="bd-title">Transaction</h1>
        <div className="pb-addr"><span className="bd-idl">Hash</span><span className="mono">{HEX}</span><Copy text={HEX} label="transaction hash" /></div>
        <div className="chips bd-chips">
          {failed
            ? <span className="state" title="This transaction failed in this block: none of its messages took effect."><i className="dot fault" />Failed</span>
            : <span className="state" title="It took effect in this block."><i className="dot ok" />Success</span>}
          <span title={typeUrl}><b className="word">{word}</b></span>
          <span title={utcWord(t.time)}>Block <b className="word">#{int(t.height)}</b> · {monthDayTime(t.time)} UTC</span>
        </div>
      </section>
      <StatusLine meta={meta} metaError={metaErr} snap={null} client={{ error: null, fetchedAt: at }} />

      {/* what it touches beside what it asked for, moved and cost: the blob page's two light frames, as tall as each other */}
      <div className="bd-top tx-top">
        <dl className="pb-meta bd-meta">{left}</dl>
        <dl className="pb-meta bd-meta bd-figs">{right}</dl>
      </div>

      {/* a failure: why, in plain words with the chain's code, and the error as the node returned it */}
      {failed && f && (
        <div className="bd-under tx-err">
          <section className="bd-sig bd-err">
            <div className="bd-sh">
              <h2>Error</h2>
              <span className="bd-err-a">
                {f.log_cut && <Warn text="Shortened by Tensile: the node's stack trace after a panic, and anything past 8 KiB, are not kept." />}
                <Copy text={f.log} label="the error" title="Copy the error" />
              </span>
            </div>
            <p className="bd-why">{f.reason && <b>{f.reason}</b>}<span className="mono" title="The chain's error code: its codespace and number">{f.codespace ? `${f.codespace} ${f.code}` : f.code}</span></p>
            <pre className="bd-log">{f.log}</pre>
          </section>
        </div>
      )}

      {/* its messages, in the chain's order and with its 0-based index, as the error names them */}
      <div className="bd-under tx-ms">
        <section className="bd-sig">
          <div className="bd-sh"><h2>Messages <span className="n">{int(t.messages.length)}</span></h2></div>
          <table className="bd-t tx-m">
            <thead><tr><th className="c-i">Index</th><th className="c-n">Message</th><th className="c-s">Signer</th></tr></thead>
            <tbody>
              {listed.map((m) => <MsgRows key={m.index} m={m} failed={failedAt != null && m.index === failedAt} r={r} />)}
            </tbody>
          </table>
          {listed.length < t.messages.length && <p className="bd-none">{int(listed.length)} of {int(t.messages.length)} messages: the one that failed and the first {int(LISTED)} that carry Fibre.</p>}
        </section>
      </div>
    </>
  );
}

/** a message's signer: the publisher's chip, the validator's, or a short address with its copy mark */
function Signer({ s, r }: { s?: string; r: TxAnswer["related"] }) {
  if (!s) return <span className="q">—</span>;
  if (r.publisher && sameAcct(s, r.publisher)) return <Who addr={r.publisher} />;
  if (r.validator && sameAcct(s, r.validator.operator_address)) return <ValChip v={r.validator} />;
  return <><span className="mono" title={s}>{shortAcct(s)}</span><CopyMark text={s} label="the signer" /></>;
}

/** one message, and a MsgExec's own messages under it on indented rows */
function MsgRows({ m, failed, r }: { m: TxMsg; failed: boolean; r: TxAnswer["related"] }) {
  return (
    <>
      <tr className={failed ? "bad" : carries(m) ? undefined : "u"}>
        <td className="c-i"><span className="cl">{m.index}</span></td>
        <td className="c-n"><span className="cl"><span className="nm" title={m.type_url}>{nameOf(m.type_url)}</span>{failed && <em>failed</em>}</span></td>
        <td className="c-s"><span className="cl"><Signer s={m.signer} r={r} /></span></td>
      </tr>
      {m.inner?.slice(0, LISTED).map((i) => (
        <tr key={`${m.index}.${i.index}`} className={i.fibre ? "in" : "in u"}>
          <td className="c-i" />
          <td className="c-n"><span className="cl"><span className="ar" aria-hidden="true">↳</span><span className="nm" title={i.type_url}>{nameOf(i.type_url)}</span></span></td>
          <td className="c-s"><span className="cl"><Signer s={i.signer} r={r} /></span></td>
        </tr>
      ))}
    </>
  );
}
