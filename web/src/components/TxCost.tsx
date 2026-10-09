"use client";
import { type TxCost as Cost, coins, int } from "@/lib/api";
import { bech32Hex } from "@/lib/addr";
import Info from "@/components/Info";
import { CopyMark } from "@/components/Ledger";

/** a transaction's hash as the lists print it: upper case, its first six and last four */
export const shortTx = (h: string) => `${h.slice(0, 6).toUpperCase()}…${h.slice(-4).toUpperCase()}`;

/** an account's address, short: its prefix, then its last four ("celestia1…abcd") */
const shortAcct = (a: string) => `${a.slice(0, a.indexOf("1") + 1)}…${a.slice(-4)}`;

/** Gas, in the failed page's format: what it used, of the limit it set */
export function GasValue({ c }: { c: Cost }) {
  return <><b>{int(c.gas_used)}</b><em>used of {int(c.gas_wanted)}</em></>;
}

/**
 * The transaction fee, in the failed page's format: what its fee payer paid from its bank balance, never from the
 * escrow. owner: the account of the page (a validator's operator address is its account under another prefix); a fee
 * another account paid names it, whole on hover. One fee covers every message of the transaction, which it says when
 * there was more than one.
 */
export function FeeValue({ c, owner }: { c: Cost; owner?: string }) {
  const other = !!c.fee_payer && !!owner && (bech32Hex(c.fee_payer) ?? c.fee_payer) !== (bech32Hex(owner) ?? owner);
  return (
    <>
      <b>{c.fee ? `Paid ${coins(c.fee)}` : "None"}</b>
      {c.fee && <em>{other ? <span title={c.fee_payer}>paid by {shortAcct(c.fee_payer!)}</span> : "from the bank balance"}{c.messages > 1 && " · whole transaction"}</em>}
    </>
  );
}

/** the two rows' hovers, the same wherever they stand */
export const GAS_TITLE = "The transaction's own gas: what it used of the limit it set.";
export const FEE_TITLE = "Paid for the transaction itself from the fee payer's bank balance, never from the escrow.";

/**
 * A successful row's transaction, one click away on its word (a deposit's kind, an endpoint change's verb), as the
 * Tensile word opens its own: label and value, its hash where the row has no hash column, then its gas and its fee.
 * A failed row never opens it: its row opens the failed page, which already says them.
 */
export default function TxCost({ word, label, cost, hash, owner, className }: {
  word: React.ReactNode; label: string; cost: Cost; hash?: string; owner?: string; className?: string;
}) {
  return (
    <Info label={label} trigger={word} className={`txw${className ? ` ${className}` : ""}`} title="Gas and transaction fee">
      <dl className="txc">
        {hash && <><dt>Transaction</dt><dd><span className="mono">{shortTx(hash)}</span><CopyMark text={hash.toUpperCase()} label="the transaction hash" /></dd></>}
        <dt title={GAS_TITLE}>Gas</dt><dd><GasValue c={cost} /></dd>
        <dt title={FEE_TITLE}>Transaction fee</dt><dd><FeeValue c={cost} owner={owner} /></dd>
      </dl>
    </Info>
  );
}
