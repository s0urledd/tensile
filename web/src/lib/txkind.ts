import type { TxKind } from "@/lib/api";

/**
 * A Fibre transaction's kind in words, and the kind of each Fibre message's type URL. Kept apart from the transaction
 * page (components/TxDetail) so the header search, on every page, names a transaction without loading that page.
 */

/** the transaction's own kind in words, as the lists name it (the header search's too) */
export const KIND_WORD: Record<Exclude<TxKind, "several">, string> = {
  settlement: "Blob payment", deposit: "Deposit", withdrawal_request: "Withdrawal request", withdrawal_executed: "Withdrawal payout",
  timeout: "Promise timeout", set_host: "Endpoint registration",
};

/** the kind a Fibre message's type URL is, for a record from an API before /v1/txs */
export const KIND_OF: Record<string, Exclude<TxKind, "several">> = {
  "/celestia.fibre.v1.MsgPayForFibre": "settlement", "/celestia.fibre.v1.MsgDepositToEscrow": "deposit",
  "/celestia.fibre.v1.MsgRequestWithdrawal": "withdrawal_request", "/celestia.fibre.v1.MsgPaymentPromiseTimeout": "timeout",
  "/celestia.valaddr.v1.MsgSetFibreProviderInfo": "set_host",
};
