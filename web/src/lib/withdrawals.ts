/**
 * Types for the escrow withdrawal queue and the x/fibre params, kept out of
 * api.ts so they can move without touching every page. The shapes mirror
 * observer/api/withdrawals.go and observer/api/params.go.
 *
 * The queue is read from chain state (x/fibre's Withdrawals query), not
 * rebuilt from events: a settlement that finds a publisher's available
 * balance short shrinks or deletes queued withdrawals and announces nothing.
 */
import type { Market, Publisher, PriceFormula } from "./api";

/** one account's queue (or the whole market's) as last read from state */
export type PendingQueue = {
  count: number;
  utia: number;
  /** earliest available_at in the queue: when the chain may first pay one out */
  next_available_at: string | null;
  /** what settlement shortfalls took from still-queued withdrawals while watched (a floor) */
  reduced_utia: number;
};

/** PendingQueue with the read it rests on: the market's, and one publisher's on its own page */
export type PendingSummary = PendingQueue & {
  read_height: number | null;
  read_at: string | null;
};

export type WithdrawalOutcome = "executed" | "consumed" | "unattributed";

export type WithdrawalRow = {
  requested_at: string;
  available_at: string;
  denom: string;
  amount_utia: number;
  first_amount_utia: number;
  reduced_utia: number;
  first_seen_height: number;
  first_seen_at: string;
  last_seen_height: number;
  last_seen_at: string;
  gone_height?: number;
  gone_at?: string;
  /** absent while queued, and while a gone one waits for the scanner */
  outcome?: WithdrawalOutcome;
  outcome_reason?: string;
  paid_height?: number;
  paid_at?: string;
  paid_utia?: number;
  /** paid_at − requested_at, executed ones only */
  payout_delay_s?: number;
  /** paid_at − available_at: the wait for the first block past available_at */
  payout_lag_s?: number;
};

export type QueueCheck = {
  height: number;
  balance_minus_available_utia: number;
  pending_utia: number;
  consistent: boolean;
};

/** the "withdrawals" object of /v1/publishers/{addr}; null until the queue was read. The read it rests on is publisher.pending_withdrawals's. */
export type PublisherWithdrawals = PendingQueue & {
  pending: WithdrawalRow[];
  left_queue: WithdrawalRow[];
  check: QueueCheck | null;
};

export type OutcomeSum = { count: number; utia: number };

/** the "withdrawal_queue" object of /v1/market; absent on an as_of request */
export type WithdrawalQueue = {
  pending: PendingSummary;
  publishers: number;
  executed: OutcomeSum;
  consumed: OutcomeSum;
  unattributed: OutcomeSum;
  unresolved: OutcomeSum;
  payout_delay: { count: number; median_s: number | null; min_s: number | null; max_s: number | null; median_lag_s: number | null };
};

export type MarketWithQueue = Market & { withdrawal_queue?: WithdrawalQueue };
/** a /v1/publishers row; on the publisher's own page the queue carries the read it rests on (PendingSummary) */
export type PublisherWithQueue = Publisher & { pending_withdrawals?: PendingQueue | PendingSummary | null };

/** one x/fibre params value and where it took effect */
export type ParamEntry = {
  effective_from_height: number;
  effective_from_tx_index: number;
  effective_from_time: string | null;
  source: "seed" | "event" | "finalize" | string;
  withdrawal_delay_s: number;
  payment_promise_timeout_s: number;
  payment_promise_height_window: number;
  shard_retention_s: number;
  full_stake_storage_budget_bytes: number;
  changed: string[];
};

/** /v1/params */
export type Params = {
  current: ParamEntry | null;
  derived?: { must_serve_window_s: number; promise_settleable_s: number; processed_payment_retention_s: number };
  history: ParamEntry[];
  changes: number;
  protocol: {
    pinned_celestia_app_commit: string;
    pinned_celestia_app_version: string;
    original_rows: number;
    parity_rows: number;
    total_rows: number;
    encoding_ratio: number;
    min_row_size_bytes: number;
    max_blob_size_bytes: number;
    max_row_size_bytes: number;
    min_rows_per_validator: number;
    max_rows_per_validator: number;
    max_validator_count: number;
    liveness_threshold: string;
    safety_threshold: string;
    unique_decoding_security_bits: number;
    max_shard_size_bytes: number;
    max_message_size_bytes: number;
    assignment_fingerprint: string;
    scanner_fingerprint?: string;
    fingerprint_matches: boolean | null;
    max_promise_clock_skew_s: number;
    min_timeout_settlement_window_s: number;
    param_bounds: Record<string, { min_s?: number; max_s: number }>;
  };
  /** the module's charge for a blob, from which every fee on the site is recomputed */
  price_formula: PriceFormula;
};

/** the words the site uses for an outcome */
export const OUTCOME: Record<WithdrawalOutcome, string> = {
  executed: "paid out",
  consumed: "consumed by settlements",
  unattributed: "left the queue (unattributed)",
};
