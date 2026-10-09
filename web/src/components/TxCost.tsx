"use client";
import { type TxCost as Cost, coins, int } from "@/lib/api";
import { bech32Hex } from "@/lib/addr";
import Info from "@/components/Info";
import { CopyMark } from "@/components/Ledger";
import { unit } from "@/components/Unit";

/** a transaction's hash as the lists print it: upper case, its first six and last four */
export const shortTx = (h: string) => `${h.slice(0, 6).toUpperCase()}…${h.slice(-4).toUpperCase()}`;

/** an account's address, short: its prefix, then its last four ("celestia1…abcd") */
const shortAcct = (a: string) => `${a.slice(0, a.indexOf("1") + 1)}…${a.slice(-4)}`;

/** Gas, in the failed page's format: what it used, of the limit it set */
export function GasValue({ c }: { c: Pick<Cost, "gas_used" | "gas_wanted"> }) {
  return <><b>{int(c.gas_used)}</b><em>used of {int(c.gas_wanted)}</em></>;
}

/**
 * The transaction fee, the same on a success and a failure: the amount in the site's style (the figure bold, its unit
 * light: "8,000 utia"), then where it came from: the bank balance, or the account that paid it when that is not the
 * page's own (owner: a validator's operator address is its account under another prefix), whole on hover. One fee
 * covers every message of the transaction, which it says when there was more than one. None: "None".
 */
export function FeeValue({ c, owner }: { c: Pick<Cost, "fee" | "fee_payer" | "messages">; owner?: string }) {
  const other = !!c.fee_payer && !!owner && (bech32Hex(c.fee_payer) ?? c.fee_payer) !== (bech32Hex(owner) ?? owner);
  if (!c.fee) return <b>None</b>;
  const parts = coins(c.fee).split(", ");
  return (
    <>
      <b>{parts.map((p, i) => <span key={i}>{i > 0 && ", "}{unit(p)}</span>)}</b>
      <em>{other ? <span title={c.fee_payer}>by {shortAcct(c.fee_payer!)}</span> : "from the bank balance"}{c.messages > 1 && " · whole transaction"}</em>
    </>
  );
}

/** the two rows' hovers, the same wherever they stand */
export const GAS_TITLE = "The transaction's own gas: what it used of the limit it set.";
export const FEE_TITLE = "Paid for the transaction itself from the fee payer's bank balance, never from the escrow.";

/**
 * A successful row's transaction, one click away on its word (a deposit's kind, an endpoint change's verb), with the
 * dotted line every word that opens details carries: a card headed "Transaction", label and value, its hash where the
 * row has no hash column, then its gas and its fee, in the failed page's order. A failed row's word opens the failed
 * page instead, which says them.
 */
export default function TxCost({ word, cost, hash, owner, className }: {
  word: React.ReactNode; cost: Cost; hash?: string; owner?: string; className?: string;
}) {
  return (
    <Info label="Transaction" trigger={word} className={`txw${className ? ` ${className}` : ""}`} title="Gas and transaction fee">
      <dl className="txc">
        {hash && <><dt>Hash</dt><dd><span className="mono">{shortTx(hash)}</span><CopyMark text={hash.toUpperCase()} label="the transaction hash" /></dd></>}
        <dt title={GAS_TITLE}>Gas</dt><dd><GasValue c={cost} /></dd>
        <dt title={FEE_TITLE}>Transaction fee</dt><dd><FeeValue c={cost} owner={owner} /></dd>
      </dl>
    </Info>
  );
}
