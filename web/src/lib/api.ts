"use client";
import { useEffect, useState } from "react";
import type { Signing } from "./signing";

// Base URL of observer-api. Same-origin "/api" is what deploy/Caddyfile
// proxies; override with NEXT_PUBLIC_API_BASE for local development.
export const API_BASE = (process.env.NEXT_PUBLIC_API_BASE ?? "/api").replace(/\/$/, "");

export type Rate = { num: number; den: number; value: number | null };
/**
 * Per (validator, blob) the validator was assigned rows of: whether the
 * settled promise carries its verified endorsement (attested), does not
 * (unattested), or was recorded before signatures were verified (unknown).
 */
export type Attestation = {
  attested_blobs: number;
  unattested_blobs: number;
  unknown_blobs: number;
  blob_coverage: Rate;
};
export type Window = { name: string; start: string; end: string };
export type ClassCounts = Record<string, number>;

export type Meta = {
  api_version: string;
  /** the rules every figure was computed under (a date); see the methodology page */
  methodology_version?: string;
  vantage: string;
  vantage_info: VantageInfo;
  vantage_count: number;
  observed_from_one_location: boolean;
  /**
   * Heartbeat vantages whose rows reached the store in the last hour, newest
   * row of each; primary is this observer's own, the one every figure counts.
   */
  vantages?: VantageSeen[];
  chain_id: string;
  /**
   * The chain's application version and whether it is high enough for x/fibre
   * and x/valaddr to exist. Below fibre_app_version the modules are not there
   * at all, so an empty registry says nothing about any validator: a page that
   * cannot tell "absent" from "empty" will imply the second while the first is
   * true.
   */
  app_version?: string;
  fibre_app_version?: string;
  fibre_active: boolean;
  /** the chain's tip as the collector last saw it; not how far the scanner has read */
  chain_height?: string;
  last_scanned_height: string;
  endpoints_height: string;
  protocol_params_fingerprint: string;
  pinned_celestia_app_commit: string;
  counts: { Publications: number; Assignments: number; Probes: number; OpenEndpoints: number; Runs: number };
  collector: RunStatus | null;
  prober: RunStatus | null;
  /** newest measurement's start time; the prober's only live signal (it writes JSONL, never this database) */
  last_probe_at: string | null;
  server_time: string;
  /** every observer process with its liveness; health is the /v1/health verdict */
  components: Component[];
  health: "ok" | "degraded" | "down";
  /** the /v1/health rows behind that verdict, so a page can say which check failed when no process did */
  checks?: { name: string; ok: boolean; detail: string }[];
  scan_gaps?: ScanGap[];
  /** matches | chain_ahead | chain_behind | unknown */
  pin_status: string;
  unassignable_publications: number;
  /** per headline figure, which of evidence_kinds it rests on */
  evidence?: Record<string, string>;
  evidence_kinds?: Record<string, string>;
  /**
   * x/signal's tally for the app version that brings Fibre, published only
   * while the chain is below it: a chain record, nothing measured here.
   */
  upgrade_signal?: {
    version: number;
    voting_power: number;
    threshold_power: number;
    total_voting_power: number;
    share: number;
    threshold_share: number;
    upgrade_height?: number;
    /** upgrade_height minus the chain tip, while the upgrade is scheduled and ahead */
    blocks_remaining?: number;
    /** the chain's average seconds per block, measured over pace_window_s; absent under half an hour of measurement */
    block_time_s?: number;
    pace_window_s?: number;
    /** blocks_remaining at that pace: an estimate, not a promise */
    eta_seconds?: number;
    /** monikers, as x/signal reports them */
    missing_validators: string[] | null;
    polled_at: string;
  };
};

/** "through #987,616" for a header line, with the block time in the title */
export function through(rt: RecordThrough | null | undefined): { text: string; title: string } | null {
  if (!rt || !rt.height) return null;
  const lag = rt.chain_height && rt.chain_height > rt.height ? ` (${(rt.chain_height - rt.height).toLocaleString("en-US")} behind the tip)` : "";
  return {
    text: `through #${rt.height.toLocaleString("en-US")}`,
    title: `Figures rest on the record through block ${rt.height.toLocaleString("en-US")}${rt.block_time ? `, ${utc(rt.block_time)}` : ""}${lag}.`,
  };
}
/** a heartbeat vantage with rows in the last hour (Meta.vantages) */
export type VantageSeen = { name: string; newest_at: string; primary: boolean };
/** where this observer watches from; both fields are operator-declared */
export type VantageInfo = {
  name: string;
  location?: string;
  provider?: string;
  /** per-field: what a reader can actually check, and how */
  verifiability: Record<string, string>;
  complete: boolean;
};

export type RunStatus = { run_id: number; started_at: string; last_heartbeat_at: string; stopped_at: string | null; alive: boolean };

/** one observer process, from the status file it keeps in the data directory */
export type Component = {
  component: string;
  present: boolean;
  alive: boolean;
  ok: boolean;
  age_s: number;
  started_at?: string;
  updated_at?: string;
  stopped_at?: string;
  stop_reason?: string;
  last_ok_at?: string;
  last_error?: string;
  last_error_at?: string;
  height?: number;
  detail?: Record<string, unknown>;
  disk?: { free_bytes: number; total_bytes: number; free_share: number };
};

/** a height range the scanner could not read from its node */
export type ScanGap = { from: number; to: number; reason: string; last_error?: string; at: string; from_time?: string; to_time?: string };

export type Health = {
  status: "ok" | "degraded" | "down";
  checks: { name: string; ok: boolean; detail: string }[];
  components: Component[];
  scan_gaps?: ScanGap[];
  pin_status: string;
  server_time: string;
};

export type RolledUp = { raw_from: string; days: number; note: string };

/**
 * The point of the chain a set of figures rests on: the scanner's checkpoint
 * (and its block time) when the snapshot was computed, beside the tip the
 * collector had last seen. computed_at says when; this says through which
 * block, which is what a reader checking a figure against the chain needs.
 */
export type RecordThrough = {
  height: number;
  block_time?: string;
  chain_height?: number;
  chain_tip_time?: string;
};
/** the three kinds of evidence a figure can rest on; /v1/meta defines them */
export type Evidence = "chain" | "verified" | "observed";

export type Network = {
  /**
   * When this summary was computed and how long it took. It is a snapshot
   * refreshed on a schedule, not a live query — the figures are aggregates over
   * the whole window and recomputing them per reader is not something a public
   * site can afford — so the page shows its age rather than implying it is now.
   */
  computed_at?: string;
  compute_ms?: number;
  window: Window;
  record_through?: RecordThrough;
  registered_endpoints: number;
  /** a census of the endpoints as of their newest evidence */
  reachability: Rate;
  /** every heartbeat in the window that completed TLS, over every one sent */
  reachability_window: Rate;
  /** one per (validator, blob) endorsed, judged by the blob's reading; the headline */
  obligations: Obligations;
  /** the part of obligations.broken whose faults are all still settling; absent when none (see ProvisionalFaults) */
  provisional_faults?: ProvisionalFaults;
  reconstructable: Reconstructable;
  /** set when the window rests partly on the daily rollup: past the raw retention, "all" is the rollup for days before raw_from plus the raw rows */
  rolled_up?: RolledUp;
  /** a pinned window (?as_of=): what is not rewound */
  as_of_note?: string;
  /** a window recomputed without named validators (?exclude=) */
  excluded?: string[];
  exclude_note?: string;
};

/**
 * The Available tile: over the blobs whose reading decides them (recoverable
 * = yes over yes plus no), the newest sample_limit of them examined. The rest
 * say why a blob has no verdict: pending (its window is open), not_read (its
 * window closed without a reading: Tensile missed it, or not a single
 * request reached a validator), not_yet_read (its reading is still to come),
 * unknown (no assignment).
 */
export type Reconstructable = {
  recoverable: Rate;
  yes: number;
  no: number;
  /** no, split by the Fibre client's error */
  no_by_error?: Record<string, number>;
  pending: number;
  not_read: number;
  not_yet_read: number;
  unknown: number;
  publications_in_window: number;
  publications_examined: number;
  sample_limit: number;
};

/**
 * One per (validator, blob) the settled promise proves the validator owes,
 * judged by the blob's reading. Only served and broken enter the rate.
 */
export type Obligations = {
  total: number;
  /** its rows came back and verified */
  served: number;
  /** not served: the blob could not be reconstructed, and its rows did not come back */
  broken: number;
  /** counted neither way: not asked because the rows were already enough, a failure on a blob that was available, or a blob not read by Tensile */
  not_counted: number;
  /** read, and the retention window has not ended (an endorsed shard not read yet has no obligation row) */
  pending: number;
  /** the deadline rests on a parameter range the observer has not read; no verdict either way */
  held_param_unverified?: number;
  /** served / (served + broken) */
  rate: Rate;
};

/** "12 not counted", or "" when every closed obligation counts one way or the other */
export function notCountedText(o: Obligations | null | undefined): string {
  const n = o ? o.not_counted + (o.held_param_unverified ?? 0) : 0;
  return n > 0 ? `${int(n)} not counted` : "";
}

/** one validator as /v1/validators lists it */
export type Validator = {
  address: string;
  cons_address: string;
  /** the name the operator set in the staking module, read from the chain */
  moniker?: string;
  operator_address?: string;
  /** the API path of the operator's Keybase picture, once the collector fetched it */
  avatar_url?: string;
  /** the chain's own words about the validator, unlike everything we measure */
  jailed: boolean;
  bond_status?: string;
  /** signalled for the app version that brings Fibre; only while the chain is below it, and unset when the moniker cannot be attributed */
  signaled_upgrade?: boolean;
  host: string;
  /** for a validator with no open endpoint: what was registered, and when it left the bonded list */
  last_host?: string;
  endpoint_closed_at?: string;
  voting_power: number;
  last_seen_at: string | null;
  reachable: boolean | null;
  /** reachable (one failed check after a good one still counts) | unreachable (two in a row) */
  endpoint_state?: "reachable" | "unreachable";
  identity_status: string;
  identity_reason?: string;
  /**
   * Another vantage whose check of the same host, in the last fifteen minutes,
   * completed the handshake while this observer's own checks failed; the
   * state is then "reachable" on its word, and last_endpoint_check is still
   * what this observer saw. also_failed_from: the vantage whose recent check
   * failed too. Absent when no other vantage checked the host recently.
   */
  confirmed_from?: string;
  also_failed_from?: string;
  /**
   * How often this observer completed a TLS conversation with the endpoint
   * over the window, from the five-minute handshake. The one stability figure
   * here whose coverage does not depend on being assigned or attested
   * anything: a validator the publisher never collected a signature from
   * still gets 288 samples a day.
   */
  reachability_window: Rate;
  last_reachable_at: string | null;
  /** one per (validator, blob) endorsed, judged by the blob's reading; the headline */
  obligations: Obligations;
  /** the part of obligations.broken whose faults are all still settling; absent when none (see ProvisionalFaults) */
  provisional_faults?: ProvisionalFaults;
  /** signing participation over the period: see lib/signing.ts. Descriptive, never a fault. */
  signing?: Signing;
  /** the shard data it stored and endorsed in the period and what it holds now (from the chain) */
  load?: Load;
  /** network and country the open endpoint resolved into, from this vantage; absent when the lookup is off */
  hosting?: import("./hosting").Hosting;
};

/** the validator object of /v1/validators/{addr}: the list's fields and what only its page shows */
export type ValidatorDetail = Validator & {
  website?: string;
  endpoint_since: string | null;
  /** when it first appeared in x/valaddr's bonded Fibre provider list, whatever host it had then */
  provider_since?: string;
  last_unreachable_at: string | null;
  /** the Endorsements figure of a record from before signing was counted per settlement */
  attestation: Attestation;
  /**
   * The median transfer rate over the download step alone, over served
   * shards of 2 MiB or more: comparable between validators whatever their
   * row count. Null under three such shards.
   */
  serve_bytes_per_second: number | null;
  /** healthy readings of such a shard that carried a byte count */
  serve_throughput_sample: number;
  /**
   * MsgPaymentPromiseTimeout submitted by this validator's operator account
   * in the window. The chain pays nothing for it; above zero says the
   * operator runs the enforcement path at all.
   */
  timeouts_enforced?: number;
};

/** one of a validator's newest readings, as its page lists them */
export type ValidatorReading = {
  vantage: string;
  promise_hash: string;
  /** true proven obliged, false unproven, null recorded before verification existed */
  attested: boolean | null;
  schedule_label: string;
  scheduled_at: string;
  started_at: string;
  phase: string;
  outcome: string;
  classification: string;
  classification_reason: string;
  rows_returned: number;
  rows_expected: number;
  total_duration_ms: number;
  raw_error?: string;
  retry_first_outcome?: string;
  rpc_code?: string;
  shadowed_by?: string;
  /** where the upload went; host_changed when the host read differs (the validator re-registered during the window) */
  host_at_settlement?: string;
  host_changed?: boolean;
  /** what the reading counts as for the validator: served, not_served (the blob was unavailable), or absent when it counts neither way */
  service?: "served" | "not_served";
  /** a not-served reading younger than the settling period: counted, and an x/fibre params change can still withdraw it */
  provisional?: boolean;
};

/** one reading row of /v1/probes */
export type Probe = {
  promise_hash: string;
  validator_address: string;
  validator_host: string;
  assigned: boolean;
  /** true proven obliged, false unproven, null recorded before verification existed */
  attested: boolean | null;
  assigned_row_count: number;
  schedule_label: string;
  scheduled_at: string;
  started_at: string;
  phase: string;
  outcome: string;
  classification: string;
  classification_reason: string;
  rows_returned: number;
  rows_expected: number;
  total_duration_ms: number;
  raw_error?: string;
  retry_first_outcome?: string;
  /** the evidence behind the verdict, on rows that carry it (schema 9 and later), with ?rows=1 */
  row_indices?: number[];
  rows_sha256?: string;
  rpc_code?: string;
  shadowed_by?: string;
  /** the verdict the row was stamped with, when the collector's late shadow judgement replaced it */
  classification_at_probe?: string;
  amended_at?: string;
  shadow_gap?: string;
  /** where the upload went; host_changed when the host probed differs (the validator re-registered during the window) */
  host_at_settlement?: string;
  host_changed?: boolean;
  /** the evidence probe of the settlement host, run when the current host did not serve; never the verdict */
  settlement_host_outcome?: string;
  settlement_host_served?: boolean;
  /** what the reading counts as for the validator: served, not_served (the blob was unavailable), or absent when it counts neither way */
  service?: "served" | "not_served";
  /** a not-served reading younger than the settling period: counted, and an x/fibre params change can still withdraw it */
  provisional?: boolean;
};

// Below this many rated probes a percentage is noise dressed as a
// measurement, so the tables print the counts instead and the ranking leaves
// the validator out. One unlucky probe used to render "0.0%" next to a named
// validator and sort it above one with a hundred real faults.
export const MIN_RATED = 20;

/**
 * The colour of a service rate: green from 98% (one isolated loss stays
 * green), amber from 90%, red below, whatever the sample; the counts behind a
 * small one are in its tooltip.
 */
export type RateTone = "r-good" | "r-warn" | "r-bad";
export function rateTone(served: number, assessed: number): RateTone | undefined {
  if (assessed <= 0) return undefined;
  const r = served / assessed;
  return r >= 0.98 ? "r-good" : r >= 0.9 ? "r-warn" : "r-bad";
}

/**
 * A blob's reading, the Fibre client's result: yes (Available: enough rows
 * came back to reconstruct it), no (Unavailable, with the client's error),
 * pending (its window is open and the reading is not in), not_read (its
 * window closed without a reading: Tensile missed it, or not a single request
 * reached a validator), unknown (no assignment to judge it by).
 */
export type Reconstruct = {
  status: "yes" | "no" | "pending" | "not_read" | "unknown";
  /** on an Unavailable blob, the client's error: "no shards retrieved" or "not enough shards to reconstruct blob" */
  error?: string;
  /** when the reading was scheduled */
  point_at: string;
  served_distinct_rows: number;
  needed_rows: number;
  /** the blob's encoded row count (16384 for blob v0) */
  total_rows: number;
  /** validators whose rows came back verified */
  served_by_validators: number;
  /** validators the reading asked, endorsing or not; it stops once the rows are enough */
  probed_validators: number;
};

export type Blob = {
  charge?: Charge | null;
  /** voting power whose signature over the promise verified, over the set's total at the promise height */
  attested_voting_power?: number;
  total_voting_power?: number;
  /** how many of validators_with_rows endorsed the promise; absent before signatures were verified */
  attested_with_rows?: number;
  promise_hash: string;
  commitment: string;
  namespace: string;
  blob_size: number;
  /** MsgPayForFibre.signer: the account that submitted the settlement, not necessarily who paid */
  signer: string;
  /** who paid: the escrow owner, whose key signed the promise */
  publisher: string;
  settlement_height: number;
  /** the other half of the list's cursor, with settlement_height */
  settlement_tx_index: number;
  settlement_time: string;
  creation_timestamp: string;
  must_serve_until: string;
  validators_with_rows: number;
  assignment_error?: string;
  reconstructable: Reconstruct | null;
};

/** one validator's reading of a blob, as the blob page lists them */
export type BlobReading = {
  validator_address: string;
  schedule_label: string;
  started_at: string;
  phase: string;
  outcome: string;
  classification: string;
  rows_returned: number;
  rows_expected: number;
  total_duration_ms: number;
  raw_error?: string;
  /** with ?rows=1 */
  row_indices?: number[];
  rows_sha256?: string;
  rpc_code?: string;
  service?: "served" | "not_served";
};

/**
 * The fee side of one promise, from the payments table. Null when the
 * publication was ingested before payments were recorded.
 */
export type Charge = {
  fee_utia: number;
  gas_units: number;
  settled: boolean;
  timed_out: boolean;
  processor?: string;
};

/** a count and a total in utia, the shape every money figure takes */
export type Sum = { count: number; utia: number };

export type PriceFormula = { base_gas: number; gas_per_chunk: number; chunk_bytes: number; utia_per_gas: number; note: string };

export type PublisherShare = {
  publisher: string;
  label?: string;
  fees_utia: number;
  fees_share: number | null;
  bytes: number;
  bytes_share: number | null;
  settlements: number;
  publishers?: number;
};

export type DayBucket = { day: string; fees_utia: number; bytes: number; settlements: number; timeouts: number; timed_out_utia: number };
/** one publisher's share of one day; publisher is empty for the folded "other" */
export type DayPublisher = { day: string; publisher: string; label?: string; fees_utia: number; bytes: number; settlements: number };
/** one publisher's share of one UTC hour, split as DayPublisher splits a day */
export type HourPublisher = { hour: string; publisher: string; label?: string; fees_utia: number; bytes: number; settlements: number };

/**
 * The publisher side of Fibre over a window. Every figure is something the
 * chain recorded; none was measured here. `timeouts` is a floor: a promise
 * nobody reports leaves no trace.
 */
export type Market = {
  window: Window;
  vantage: string;
  computed_at?: string;
  compute_ms?: number;
  record_through?: RecordThrough;
  source: string;
  settlements: number;
  /** distinct blobs (BlobID: blob_version || commitment) those settlements paid for, same window */
  blobs: number;
  fees_settled_utia: number;
  bytes: number;
  publishers_active: number;
  paid_per_mib_utia: number | null;
  timeouts: number;
  timed_out_utia: number;
  settlement_rate: Rate;
  timeout_processors: number;
  deposits: Sum;
  withdrawals_requested: Sum;
  withdrawals_executed: Sum;
  escrow_held_utia: number;
  escrow_accounts: number;
  /** x/fibre's module account: every escrow on the chain, read at escrow_total_at */
  escrow_total_utia?: number;
  escrow_total_at?: string;
  daily: DayBucket[];
  /** UTC hours, for a window of a day or less */
  hourly?: { hour: string; bytes: number; settlements: number; fees_utia: number }[];
  /** hourly split by publisher as daily_by_publisher splits daily, set with it */
  hourly_by_publisher?: HourPublisher[];
  daily_by_publisher: DayPublisher[];
  top_publishers: PublisherShare[];
  other_publishers: PublisherShare | null;
  largest_poster: PublisherShare | null;
  price_formula: PriceFormula;
  notes: string[];
  /** namespaces the window's settlements used, and any settlement on record */
  namespaces?: number;
  namespaces_total?: number;
};

export type Escrow = { found: boolean; balance_utia: number; available_utia: number; height: number; updated_at: string };

export type Publisher = {
  publisher: string;
  label?: string;
  label_source?: string;
  settlements: number;
  bytes: number;
  bytes_share: number | null;
  fees_utia: number;
  fees_share: number | null;
  paid_per_mib_utia: number | null;
  avg_blob_bytes: number | null;
  largest_blob_bytes: number;
  timeouts: number;
  timed_out_utia: number;
  first_seen_at: string;
  last_seen_at: string;
  escrow: Escrow | null;
};

export type Payment = {
  kind: "settlement" | "timeout" | "deposit" | "withdrawal_request" | "withdrawal_executed";
  height: number;
  time: string;
  tx_hash?: string;
  publisher: string;
  processor?: string;
  promise_hash?: string;
  namespace?: string;
  blob_size?: number;
  gas_units?: number;
  amount_utia: number;
  available_at?: string;
};

/**
 * fetchedAt is when the payload on screen last arrived from the API. A refresh
 * that fails keeps the payload and the old fetchedAt beside the new error, so
 * a page can say "showing data as of 08:13" instead of pretending it is now.
 */
/**
 * status is the HTTP status of the last failed request: 0 when the API did
 * not answer at all (network error, proxy down, timeout), otherwise the code
 * it answered with. Only 404/410 say the thing asked for is not on record and
 * only 400 says the request itself was wrong; every other failure, a 429 or a
 * 5xx included, says nothing about the record and reads as the API not
 * answering.
 */
/**
 * computing is set while the API says the figure asked for is being computed
 * (a 503 with `computing: true`, after a restart or a change of rules): not
 * an error and not an outage. The stream keeps what is on screen, stays
 * loading when there is nothing yet, and asks again after the few seconds the
 * API names rather than at its next interval.
 */
export type Fetch<T> = { data: T | null; error: string | null; loading: boolean; fetchedAt: string | null; status?: number; computing?: boolean };

/** the API answered that the thing asked for is not on record (404, 410) */
export function notFound(f: { error: string | null; status?: number }): boolean {
  return !!f.error && (f.status === 404 || f.status === 410);
}
/** the API refused the request as malformed (400): a wrong address, not a missing one */
export function badRequest(f: { error: string | null; status?: number }): boolean {
  return !!f.error && f.status === 400;
}
/**
 * the API did not answer usefully: no answer, a 5xx, or a refusal that says
 * nothing about the record (429 rate-limited, 401/403, 408). It used to be
 * "anything but a 4xx", so a 429 printed "no validator with this address is
 * on record" about a validator that was.
 */
export function apiFailing(f: { error: string | null; status?: number }): boolean {
  return !!f.error && !notFound(f) && !badRequest(f);
}
/** the API is up but asked us to slow down */
export function throttled(f: { error: string | null; status?: number }): boolean {
  return !!f.error && f.status === 429;
}

// One in-flight request and one timer per (path, interval), however many
// components ask for it: the header, the banner, the footer and the page all
// want /v1/meta, which was four requests per interval per viewer.
// retry is the one early re-ask a stream has pending while its figure is
// being computed.
type Sub = { subs: Set<(f: Fetch<unknown>) => void>; timer: ReturnType<typeof setInterval> | null; retry: ReturnType<typeof setTimeout> | null; last: Fetch<unknown> };
const streams = new Map<string, Sub>();

/** how soon to ask again for a figure being computed: what the API says, kept between 2 and 30 s */
function computingRetryMs(seconds: unknown): number {
  const s = typeof seconds === "number" && isFinite(seconds) ? seconds : 5;
  return Math.min(Math.max(s, 2), 30) * 1000;
}

async function fetchOnce(path: string): Promise<Fetch<unknown> & { retryMs?: number }> {
  try {
    // A hung API must read as unreachable rather than as a page that never
    // updates: every request is abandoned after 20 s.
    const ctl = new AbortController();
    const timer = setTimeout(() => ctl.abort(), 20000);
    try {
      const r = await fetch(API_BASE + path, { cache: "no-store", signal: ctl.signal });
      if (!r.ok) {
        let msg = `${r.status}`;
        let body: { error?: string; computing?: boolean; retry_after_s?: number } | null = null;
        try { body = await r.json(); msg = body?.error ?? msg; } catch { /* keep status */ }
        if (r.status === 503 && body?.computing) {
          // The figure is being computed, not failing: the Retry-After header
          // is not readable across origins, so the body says how long.
          return { data: null, error: null, loading: true, fetchedAt: null, status: r.status, computing: true, retryMs: computingRetryMs(body.retry_after_s) };
        }
        return { data: null, error: msg, loading: false, fetchedAt: null, status: r.status };
      }
      return { data: await r.json(), error: null, loading: false, fetchedAt: new Date().toISOString() };
    } finally {
      clearTimeout(timer);
    }
  } catch (e) {
    const aborted = e instanceof DOMException && e.name === "AbortError";
    return { data: null, error: aborted ? "no answer within 20 s" : e instanceof Error ? e.message : String(e), loading: false, fetchedAt: null, status: 0 };
  }
}

function subscribe(key: string, path: string, refreshMs: number, fn: (f: Fetch<unknown>) => void): () => void {
  let st = streams.get(key);
  if (!st) {
    st = { subs: new Set(), timer: null, retry: null, last: { data: null, error: null, loading: true, fetchedAt: null } };
    streams.set(key, st);
    const load = async () => {
      const { retryMs, ...next } = await fetchOnce(path);
      const cur = streams.get(key);
      if (!cur) return;
      if (next.computing) {
        // Being computed: what is on screen stays, without an error, and the
        // stream asks again in a few seconds, once, whatever its interval
        // (a stream with no interval asks again too).
        cur.last = cur.last.data !== null ? { ...cur.last, error: null, loading: false, status: next.status, computing: true } : next;
        if (!cur.retry) cur.retry = setTimeout(() => { cur.retry = null; load(); }, retryMs ?? 5000);
      } else {
        // A refresh that fails does not erase the answer already on screen. It
        // used to: fetchOnce returns {data: null, error} on any failure, so one
        // 500 or one proxy hiccup replaced a rendered table with a loading
        // message that never resolved, and the page said nothing about why.
        // The error is carried beside the last good payload instead, for the
        // page to show, and the figures keep their own computed_at so nobody
        // reads stale numbers as fresh ones.
        cur.last = next.error && cur.last.data !== null
          ? { data: cur.last.data, error: next.error, loading: false, fetchedAt: cur.last.fetchedAt, status: next.status }
          : next;
      }
      const out = cur.last;
      cur.subs.forEach((s) => s(out));
    };
    load();
    if (refreshMs > 0) st.timer = setInterval(load, refreshMs);
  } else if (!st.last.loading || st.last.computing) {
    // A later subscriber gets the current value at once. So does one that
    // arrives while the figure is being computed with nothing to show yet:
    // that state is still "loading", and the subscriber would otherwise hear
    // nothing of it until the next retry, seconds later.
    fn(st.last);
  }
  st.subs.add(fn);
  return () => {
    const cur = streams.get(key);
    if (!cur) return;
    cur.subs.delete(fn);
    if (cur.subs.size === 0) {
      if (cur.timer) clearInterval(cur.timer);
      if (cur.retry) clearTimeout(cur.retry);
      streams.delete(key);
    }
  };
}

export function useApi<T>(path: string | null, refreshMs = 30000): Fetch<T> {
  const [state, setState] = useState<Fetch<T>>({ data: null, error: null, loading: !!path, fetchedAt: null });
  useEffect(() => {
    if (!path) return;
    return subscribe(`${refreshMs}|${path}`, path, refreshMs, (f) => setState(f as Fetch<T>));
  }, [path, refreshMs]);
  return state;
}

// ---- formatting ----

/** a share as a percentage to two decimals: "99.95%". Only all of it reads
 *  "100%" and only none of it "0%"; a share that two decimals would round to
 *  either end prints as a bound instead. */
function pct2(v: number, all: boolean, none: boolean): string {
  if (all) return "100%";
  if (none) return "0%";
  const s = (v * 100).toFixed(2);
  if (s === "100.00") return ">99.99%";
  if (s === "0.00") return "<0.01%";
  return s + "%";
}

/** the percentage whenever there is anything to divide; the floor only decides emphasis */
export function fmtPct(r: Rate | undefined | null): string {
  if (!r || r.den === 0 || r.value === null) return "—";
  return pct2(r.value, r.num === r.den, r.num === 0);
}

/** whether a rate has enough observations behind it to rank or compare. */
export function enoughToRank(r: Rate | undefined | null): boolean {
  return !!r && r.den >= MIN_RATED;
}
/** the same, from two counts */
export function pctOf(num: number, den: number): string {
  return fmtPct({ num, den, value: den > 0 ? num / den : null });
}
export function int(n: number | null | undefined): string {
  return n == null ? "—" : n.toLocaleString("en-US");
}
export function fmtCount(r: Rate | undefined | null): string {
  if (!r) return "";
  return `${r.num.toLocaleString("en-US")} / ${r.den.toLocaleString("en-US")}`;
}
export function utc(s: string | null | undefined): string {
  if (!s) return "—";
  const d = new Date(s);
  if (isNaN(d.getTime())) return s;
  return d.toISOString().replace("T", " ").replace(/\.\d+Z$/, "Z");
}
/** how long since: "4 h", "12 min", "3 d"; for a state that has held since s */
export function held(s: string | null | undefined): string {
  if (!s) return "";
  const ms = Date.now() - new Date(s).getTime();
  if (isNaN(ms) || ms < 0) return "";
  const m = Math.round(ms / 60000);
  if (m < 60) return `${Math.max(m, 1)} min`;
  const h = Math.round(m / 60);
  if (h < 48) return `${h} h`;
  return `${Math.round(h / 24)} d`;
}
/** "just now", "12 min", "3 h 12 min", "2 d": how long since s, without the word.
 *  Whole minutes, never rounded up: anything under a minute is "just now", and
 *  59 minutes is "59 min", not "1 h". */
export function since(s: string | null | undefined): string {
  if (!s) return "";
  const ms = Date.now() - new Date(s).getTime();
  if (isNaN(ms)) return "";
  const m = Math.floor(Math.abs(ms) / 60000);
  if (m < 1) return "just now";
  if (m < 60) return `${m} min`;
  const h = Math.floor(m / 60), rm = m % 60;
  if (h < 24) return rm ? `${h} h ${rm} min` : `${h} h`;
  return `${Math.floor(h / 24)} d`;
}
/** how far ahead of the reader's clock a past event may appear and still read
 *  "just now": clock skew between the reader and the chain */
const SKEW_MS = 5000;
/** "12 min ago" for the past, "in 12 min" for the future; under a minute,
 *  "just now" for the past (and a few seconds of clock skew ahead), "in under a
 *  minute" for the future */
export function ago(s: string | null | undefined): string {
  const w = since(s);
  if (w === "") return w;
  const ahead = new Date(s!).getTime() - Date.now();
  if (w === "just now") return ahead > SKEW_MS ? "in under a minute" : w;
  return ahead > 0 ? `in ${w}` : `${w} ago`;
}
/** "09:09 UTC" today, "Sep 21 09:09 UTC" on any other UTC day: a time that says which day it is */
export function whenUTC(s: string | null | undefined): string {
  if (!s) return "—";
  const d = new Date(s);
  if (isNaN(d.getTime())) return s;
  const today = new Date().toISOString().slice(0, 10) === d.toISOString().slice(0, 10);
  const t = d.toISOString().slice(11, 16) + " UTC";
  return today ? t : `${d.toLocaleDateString("en-US", { month: "short", day: "numeric", timeZone: "UTC" })} ${t}`;
}
/** "2026-09-22 08:59:09 UTC" */
export function utcWord(s: string | null | undefined): string {
  const u = utc(s);
  return u.endsWith("Z") ? u.slice(0, -1) + " UTC" : u;
}
/** "08:59 UTC" */
export function hhmm(s: string | null | undefined): string {
  const u = utc(s);
  return u.length >= 16 ? u.slice(11, 16) + " UTC" : u;
}
/** "08:59:09 UTC" */
export function hhmmss(s: string | null | undefined): string {
  const u = utc(s);
  return u.length >= 19 ? u.slice(11, 19) + " UTC" : u;
}
/** "22 Sep 2026" */
export function dateUTC(s: string | null | undefined): string {
  if (!s) return "—";
  const d = new Date(s);
  if (isNaN(d.getTime())) return s;
  return d.toLocaleDateString("en-US", { day: "numeric", month: "short", year: "numeric", timeZone: "UTC" });
}
/** "2 d 4 h", "4 h 36 min", "36 min": a span in seconds, two units at most */
export function span(seconds: number): string {
  const m = Math.round(seconds / 60);
  if (m < 60) return `${Math.max(m, 1)} min`;
  const h = Math.floor(m / 60), rm = m % 60;
  if (h < 24) return rm ? `${h} h ${rm} min` : `${h} h`;
  const d = Math.floor(h / 24), rh = h % 24;
  return rh ? `${d} d ${rh} h` : `${d} d`;
}
/** "4 h 0 min" between two instants */
export function dur(a: string, b: string): string {
  const m = Math.round((new Date(b).getTime() - new Date(a).getTime()) / 60000);
  if (isNaN(m)) return "—";
  const h = Math.floor(Math.abs(m) / 60), rm = Math.abs(m) % 60;
  return h ? `${h} h ${rm} min` : `${rm} min`;
}
/** head…tail of a long identifier */
export function shortMid(s: string | null | undefined, head = 16, tail = 4): string {
  if (!s) return "";
  if (s.length <= head + tail + 1) return s;
  return `${s.slice(0, head)}…${s.slice(-tail)}`;
}
export function shortHex(s: string, n = 8): string {
  if (!s || s.length <= 2 * n + 3) return s;
  return `${s.slice(0, n)} ••• ${s.slice(-n)}`;
}
export function shortBech(s: string): string {
  if (!s) return "";
  const i = s.indexOf("1");
  if (i < 0 || s.length < i + 5) return s;
  return `${s.slice(0, i)} ••• ${s.slice(-4)}`;
}
/**
 * A validator's shard data: the rows it stored of every settled blob it
 * endorsed, kept for the retention window. From the chain, nothing measured. Mocha's own figures: the mainnet
 * sizing belongs to its own page.
 */
export type Load = {
  promises: number;
  bytes: number;
  stored_bytes: number;
  rows_per_blob: number;
};

/** a size in the largest binary unit it reaches: "12.3 MiB", "1.35 GiB".
 *  A value just under a unit's end rounds to 1024 of it ("1024.0 KiB"),
 *  which is the next unit's one, so it is printed there ("1.0 MiB", "1.00
 *  GiB"). Ties round as toFixed rounds them. */
export function bytes(n: number): string {
  if (n < 1024) return `${n} B`;
  const kib = (n / 1024).toFixed(1);
  if (Number(kib) < 1024) return `${kib} KiB`;
  const mib = (n / 1024 / 1024).toFixed(1);
  if (Number(mib) < 1024) return `${mib} MiB`;
  return `${(n / 1024 / 1024 / 1024).toFixed(2)} GiB`;
}
/** a transfer rate, in the unit the size fits */
export function bytesPerSecond(n: number): string {
  return `${bytes(n)}/s`;
}
/**
 * A namespace as a reader can take it in: its bytes as text when every
 * significant byte is printable ASCII ("mochafibre"), else the hex with the
 * leading zero padding dropped. The full hex belongs in a tooltip beside it.
 */
export function nsDisplay(ns: string): string {
  const stripped = ns.replace(/^(00)+/, "");
  if (stripped.length >= 4 && stripped.length % 2 === 0) {
    const bytes = stripped.match(/../g)!.map((h) => parseInt(h, 16));
    if (bytes.every((b) => b >= 0x20 && b < 0x7f)) return String.fromCharCode(...bytes);
  }
  return shortHex(stripped || ns, 6);
}

/** one row of /v1/namespaces */
export type NamespaceRow = {
  namespace: string; blobs: number; bytes: number; blobs_24h: number; bytes_24h: number;
  accounts: number; first_seen: string; last_blob: string;
};
// Wilson 95% interval for a proportion. Both ends matter and they answer
// different questions: the lower bound on the serve rate is the charitable
// reading, the upper bound on the fault rate is the accusatory one. A table
// that publishes faults should show the bound on the claim it is making.
function wilson(num: number, den: number): [number, number] | null {
  if (den === 0) return null;
  const z = 1.96, p = num / den, n = den;
  const denom = 1 + (z * z) / n;
  const centre = p + (z * z) / (2 * n);
  const margin = z * Math.sqrt((p * (1 - p)) / n + (z * z) / (4 * n * n));
  return [Math.max(0, (centre - margin) / denom), Math.min(1, (centre + margin) / denom)];
}

export function wilsonLower(num: number, den: number): number | null {
  const w = wilson(num, den);
  return w && w[0];
}

/**
 * Upper bound on the fault rate: "at most this share of the obligations we
 * could judge went unserved, with 95% confidence". This is the direction an
 * accusation has to be stated in.
 *
 * Pass an obligation-level rate: one per (validator, blob).
 */
export function faultRateUpper(r: Rate | undefined | null): number | null {
  if (!r || r.den === 0) return null;
  const w = wilson(r.den - r.num, r.den);
  return w && w[1];
}

// ---- money ----

/** one TIA in utia */
export const UTIA = 1_000_000;

/**
 * utia as TIA with the precision the size of the figure deserves: a fee is
 * 0.695 TIA, a day is 12.4 TIA, an escrow is 6,000 TIA. Never more than
 * three decimals, never a bare "0" for a non-zero amount.
 */
export function tia(utia: number | null | undefined, opts: { unit?: boolean } = {}): string {
  if (utia == null) return "—";
  const v = utia / UTIA;
  let s: string;
  if (v === 0) s = "0";
  else if (Math.abs(v) >= 1000) s = Math.round(v).toLocaleString("en-US");
  else if (Math.abs(v) >= 100) s = v.toFixed(1);
  else if (Math.abs(v) >= 10) s = v.toFixed(2);
  else if (Math.abs(v) >= 0.001) s = v.toFixed(3);
  else s = `${utia} utia`;
  return opts.unit === false ? s : `${s} TIA`;
}

/** the publisher a row belongs to: its registry label if one exists, else the short address */
export function publisherName(p: { publisher: string; label?: string }): string {
  return p.label || shortBech(p.publisher);
}

export function fmtShare(v: number | null | undefined): string {
  if (v == null) return "—";
  return pct2(v, v >= 1, v <= 0);
}

/** /v1/tip: the newest block the observer has read, for the header ticker */
export type Tip = {
  height: number;
  block_time?: string;
  observed_at?: string;
  source: "scanner" | "collector";
  fibre_active: boolean;
  server_time: string;
};

/** the newest heartbeat against one validator's Fibre host, stage by stage */
export type EndpointCheck = {
  at: string;
  host: string;
  outcome: string;
  dns_ok: boolean;
  tcp_ok: boolean;
  tcp_ms: number;
  tls_ok: boolean;
  tls_ms: number;
  identity_ok: boolean;
  identity_reason?: string;
  raw_error?: string;
  vantage: string;
};

/**
 * Provisional faults: not-served obligations whose every failed reading is
 * younger than the observer's settling period (30 minutes). They are counted
 * in broken and in the rate; the flag says evidence still on its way (an
 * x/fibre params change not yet reconciled) can withdraw them. `until` is
 * when the youngest settles, so a cached answer
 * still tells the page when to drop the badge.
 */
export type ProvisionalFaults = { obligations: number; until: string; settling_seconds: number; note: string };

/** how many of these faults are still provisional right now; 0 once `until` has passed */
export function provisionalNow(p: ProvisionalFaults | null | undefined, now = Date.now()): number {
  if (!p || !p.obligations) return 0;
  const until = Date.parse(p.until);
  return Number.isFinite(until) && until > now ? p.obligations : 0;
}

/** the network's service rate over the same window from the same vantage, beside a validator's own */
export type NetworkReference = {
  /** median of validators' own rates, over those with at least min_rated decided; null when none */
  median_rate: number | null;
  validators: number;
  min_rated: number;
  /** every obligation together */
  pooled_rate: Rate;
};

/**
 * City placement from DB-IP's IP to City Lite file (optional on the
 * observer; every field is absent without it): /v1/hosting's summary may
 * carry `by_city` and sources `city_db`. lat/lon are the city's
 * approximate point, not the machine's.
 */

/** One /v1/hosting summary.by_city entry. key "" = hosts with no city (listed last, no name or point). */
export type HostingCityBucket = {
  key: string;
  city?: string;
  region?: string;
  country?: string;
  lat?: number;
  lon?: number;
  hosts: number;
  host_share: number;
  stake: number;
  stake_share: number;
};

export type HostingCityExtras = {
  by_city?: HostingCityBucket[];
  city_db?: import("./hosting").DBSource;
};
