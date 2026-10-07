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

/** /v1/meta: the chain and observer state the header, banners and footer read */
export type Meta = {
  api_version: string;
  /** the rules every figure was computed under (a date); see the methodology page */
  methodology_version?: string;
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
  counts: { Publications: number; Assignments: number; Probes: number; OpenEndpoints: number; Runs: number };
  /** newest measurement's start time; the prober's only live signal (it writes JSONL, never this database) */
  last_probe_at: string | null;
  server_time: string;
  /** the /v1/health verdict */
  health: "ok" | "degraded" | "down";
  /** the /v1/health rows behind that verdict, so a page can say which check failed when no process did */
  checks?: { name: string; ok: boolean; detail: string }[];
  scan_gaps?: ScanGap[];
  /** matches | chain_ahead | chain_behind | unknown */
  pin_status: string;
  /**
   * The upgrade that brings Fibre, from x/signal, published only while the
   * chain is below it: a chain record, nothing measured here.
   */
  upgrade_signal?: {
    upgrade_height?: number;
    /** the blocks left at the chain's recent pace: an estimate, not a promise */
    eta_seconds?: number;
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
/** the three kinds of evidence a figure can rest on */
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
  /** one per (validator, blob) endorsed, judged on the validator's own answers at the blob's reading; the headline */
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
 * judged on the validator's own answers at the blob's reading (readings
 * before FULL_READ_SINCE keep the rule of their time). Only served and
 * broken enter the rate.
 */
export type Obligations = {
  total: number;
  /** its own rows came back and verified, at the reading or when asked again */
  served: number;
  /**
   * not served: none of its answers served and none was Tensile's own gap, its last giving the reason (no shard, rows
   * that do not verify or fewer of its own than it holds, a wrong or expired certificate, no registered endpoint, an
   * endpoint that could not be reached, a timeout, a rate limit or a server error); at a reading before
   * FULL_READ_SINCE, its rows did not come back and the blob could not be reconstructed
   */
  broken: number;
  /**
   * counted neither way: one of its answers was Tensile's own gap (a request it could not make in time, or an error on
   * its own side: its network, its resolver or its clock) or rows of the blob that are not its own, a request Tensile
   * still owed it is not on record, no request of the reading reached a server, or the blob was not read by Tensile;
   * at a reading before FULL_READ_SINCE also a failure on a blob that was available (a validator such a reading did
   * not ask has no obligation at all)
   */
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
  /** the start of its newest reading in the period whose rows came back verified; null when none did */
  last_served_at: string | null;
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
  /** one per (validator, blob) endorsed, judged on the validator's own answers at the blob's reading; the headline */
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
  /** "full" (every endorser asked for its own rows; so is "end" started at or after FULL_READ_SINCE), "enough" (a reading that stopped at enough rows, judged by the earlier rule), "end" (the one reading near the end of the window, before it), or the earlier schedule's w1…wN, grace, post */
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
  /** when the blob settled, which is when the validator endorsed it; absent from an API before it was named */
  settled_at?: string;
  host_changed?: boolean;
  /**
   * what this request counts as for the validator: served; not_served (at a full reading, its last answer when none
   * served and none was Tensile's own gap or rows of the blob not its own; at an earlier one, rows that did not come
   * back from a blob that was unavailable); absent when it counts neither way, among them an answer a later one
   * replaced, or one whose next attempt is still owed
   */
  service?: "served" | "not_served";
  /** a not-served reading younger than the settling period: counted, and an x/fibre params change can still withdraw it */
  provisional?: boolean;
  /**
   * which of the validator's requests at a full reading this is: absent (0) the reading's own, 1 and 2 asked again
   * after an answer that did not serve; a later one can carry the answer of another request to the same endpoint, which
   * failed before any blob was asked for (its raw_error names that request: sharedAnswer)
   */
  attempt?: number;
  /**
   * at a full reading, on an answer that did not serve: when its validator is to be asked again. The answer is then
   * not its last, and until that request is on record (made, or recorded as not made) the validator counts neither
   * way. Absent when none is owed.
   */
  next_attempt_due?: string;
  /**
   * on a short answer (outcome PARTIAL) only: true when every row that came back is one this promise assigns the
   * validator (at a full reading, not served), false when some are not (rows of the blob that are not its own,
   * counted neither way)
   */
  rows_subset_of_assignment?: boolean;
};

/** one reading row of /v1/probes */
export type Probe = {
  promise_hash: string;
  validator_address: string;
  /** celestiavaloper1… from the staking set, when the collector has read one */
  operator_address?: string;
  validator_host: string;
  assigned: boolean;
  /** true proven obliged, false unproven, null recorded before verification existed */
  attested: boolean | null;
  assigned_row_count: number;
  /** "full" (every endorser asked for its own rows; so is "end" started at or after FULL_READ_SINCE), "enough" (a reading that stopped at enough rows, judged by the earlier rule), "end" (the one reading near the end of the window, before it), or the earlier schedule's w1…wN, grace, post */
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
  /** what this request counts as for the validator; see ValidatorReading.service */
  service?: "served" | "not_served";
  /** a not-served reading younger than the settling period: counted, and an x/fibre params change can still withdraw it */
  provisional?: boolean;
  /** see ValidatorReading.attempt */
  attempt?: number;
  /** see ValidatorReading.next_attempt_due */
  next_attempt_due?: string;
  /** see ValidatorReading.rows_subset_of_assignment */
  rows_subset_of_assignment?: boolean;
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
  /**
   * validators the reading asked (a request of Tensile's own that failed or could not be made asks no one): at a full
   * reading the endorsing validators, and only those; at a reading before FULL_READ_SINCE, endorsing or not, most of
   * them in the client's order until the rows were enough, as at a reading labelled enough
   */
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
  /** the transaction that carried the MsgPayForFibre, in lower-case hex; absent from an API before it was published */
  settlement_tx_hash?: string;
  /** the promise's blob version, the first byte of the client's blob ID; absent from an API before it was published */
  blob_version?: number;
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
  /** celestiavaloper1… from the staking set, when the collector has read one */
  operator_address?: string;
  /** "full" (every endorser asked for its own rows; so is "end" started at or after FULL_READ_SINCE), "enough" (a reading that stopped at enough rows, judged by the earlier rule), "end" (the one reading near the end of the window, before it), or the earlier schedule's w1…wN, grace, post */
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
  /** what this request counts as for the validator; see ValidatorReading.service */
  service?: "served" | "not_served";
  /** see ValidatorReading.attempt */
  attempt?: number;
  /** see ValidatorReading.next_attempt_due */
  next_attempt_due?: string;
  /** see ValidatorReading.rows_subset_of_assignment */
  rows_subset_of_assignment?: boolean;
};

/**
 * When Tensile began reading every blob in full (probe.FullReadSince on the observer): every validator that endorsed
 * the blob is asked for its own rows 10 minutes before the retention window ends, one that did not serve is asked
 * again, up to two more times, about 90 s apart, while the window is open, and each is judged on its own answers,
 * whatever the blob's reconstruction; the first such readings, labelled "end", asked each validator once. Readings
 * started before it (most of them asking validators in the client's order until the blob could be rebuilt), and a
 * reading made after it that stops at enough rows (labelled "enough"), keep the earlier rule: not served only when
 * the blob was unavailable.
 */
export const FULL_READ_SINCE = "2026-10-02T16:09:49Z";
const FULL_READ_SINCE_MS = Date.parse(FULL_READ_SINCE);
/** the same moment as the site writes it in a dated note */
export const FULL_READ_SINCE_WORDS = "2 October 2026, 16:09 UTC";

/** the one reading of a blob near the end of its retention window: "end", "full", or "enough" (one that stopped at enough rows) */
export function endOfWindow(label: string): boolean {
  return label === "end" || label === "full" || label === "enough";
}

/** whether a reading row belongs to a full reading: label "full", or "end" started at or after FULL_READ_SINCE */
export function fullReading(label: string, startedAt: string): boolean {
  return label === "full" || (label === "end" && Date.parse(startedAt) >= FULL_READ_SINCE_MS);
}

/**
 * a request that was Tensile's own gap: not made in time, or failed on its own side (among them its network, its
 * resolver or its clock: ownSide); never counted against a validator
 */
export function ownGap(classification: string): boolean {
  return classification === "NOT_PROBED" || classification === "PROBE_ERROR";
}

/**
 * Which part of Tensile's own side a failure of a full reading rests on, from the raw error the observer wrote when it
 * filed the answer as its own gap: its network (a connect that timed out while it reached no server), its resolver, or
 * its clock (a certificate read at the edge of its validity); "" for any other.
 */
export function ownSide(raw?: string): "network" | "resolver" | "clock" | "" {
  const m = /^this observer's own (network|resolver|clock):/.exec(raw ?? "");
  return m ? (m[1] as "network" | "resolver" | "clock") : "";
}

/**
 * A later attempt that carries the answer of another request to the same endpoint: the endpoint failed before any blob
 * was asked for, while this attempt was due and waiting, so that answer is this attempt's too. blob is the other
 * request's blob, wire what came back to it. Null for a request of its own.
 */
export function sharedAnswer(raw?: string): { blob: string; wire: string } | null {
  const m = /^the validator's endpoint failed before any blob was asked for, on request (\S+) for blob ([0-9a-f]+), made while this attempt was due and waiting for it: ?([\s\S]*)$/.exec(raw ?? "");
  if (!m) return null;
  const key = m[1].split("|");
  return { blob: key.length >= 4 && /^[0-9a-f]{64}$/.test(key[1]) ? key[1] : m[2], wire: m[3] };
}

/** a request's raw error as a page shows it: a shared answer names, in words, the request it repeats */
export function rawErrorWords(p: { raw_error?: string; started_at: string }): string {
  const sh = sharedAnswer(p.raw_error);
  return sh
    ? `the same answer as its request for blob ${sh.blob.slice(0, 6)}…${sh.blob.slice(-4)} at ${hhmm(p.started_at)}: its endpoint failed before any blob was asked for${sh.wire ? `; on the wire: ${sh.wire}` : ""}`
    : p.raw_error ?? "";
}

/**
 * Rows of the blob that verified and are not the validator's own, with no settled promise to explain them: under
 * hash-order serving they show neither that it holds its rows nor that it does not, so at a full reading Tensile counts
 * them neither way. Other rows (WRONG_ROWS), or a short answer (PARTIAL) whose rows the record says are not all its
 * own; a short answer that is a part of its own rows is not served.
 */
export function foreignRows(p: { classification: string; outcome: string; rows_subset_of_assignment?: boolean }): boolean {
  return p.classification === "UNMATCHED_GENUINE"
    && (p.outcome === "WRONG_ROWS" || (p.outcome === "PARTIAL" && p.rows_subset_of_assignment === false));
}

/**
 * Whether a validator's last answer at a full reading still owes it another request (the record's next_attempt_due),
 * and that request can still be made: due before a minute ahead of the window's end. One due later was owed only
 * because of Tensile's own delays, and is recorded as not made, Tensile's own gap.
 */
export function asksAgain(p: { next_attempt_due?: string }, until: number): boolean {
  return !!p.next_attempt_due && Date.parse(p.next_attempt_due) < until - 60_000;
}

/**
 * One validator's requests at one reading, in the order they were made (attempt, then start), and the last of them,
 * which carries the result and its reason: the attempts stop once one serves.
 */
export function attemptsOf<T extends { attempt?: number; started_at: string }>(rows: T[]): { last: T; tries: T[] } {
  const tries = [...rows].sort((a, b) => (a.attempt ?? 0) - (b.attempt ?? 0) || a.started_at.localeCompare(b.started_at));
  return { last: tries[tries.length - 1], tries };
}

/**
 * What a validator's requests at one reading count as, read off the service word the observer put on each: served when
 * one served, not served when one is the not-served answer, otherwise neither ("").
 */
export function judged(tries: { service?: "served" | "not_served" }[]): "served" | "not_served" | "" {
  if (tries.some((t) => t.service === "served")) return "served";
  if (tries.some((t) => t.service === "not_served")) return "not_served";
  return "";
}

/** "asked 3 times", or "" for a single request */
export function askedTimes(n: number): string {
  return n > 1 ? `asked ${n} times` : "";
}

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

/** x/fibre's charge for a blob, on /v1/params: fee = (base_gas + gas_per_chunk × ⌈blob_size / chunk_bytes⌉) × utia_per_gas */
export type PriceFormula = { base_gas: number; gas_per_chunk: number; chunk_bytes: number; utia_per_gas: number; note: string };

/** the fee of one blob of this size, in utia, by the module's price formula */
export function blobFee(f: PriceFormula, size: number): number {
  return (f.base_gas + f.gas_per_chunk * Math.ceil(size / f.chunk_bytes)) * f.utia_per_gas;
}

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
  computed_at?: string;
  record_through?: RecordThrough;
  settlements: number;
  /** distinct blobs (BlobID: blob_version || commitment) those settlements paid for, same window */
  blobs: number;
  fees_settled_utia: number;
  bytes: number;
  publishers_active: number;
  paid_per_mib_utia: number | null;
  timeouts: number;
  timed_out_utia: number;
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
  /** its first and last escrow movement of any kind, over the whole record */
  first_seen_at: string;
  last_seen_at: string;
  /** the namespaces its settlements in the period used, the most used first, at most 10; namespaces_total counts them all */
  namespaces?: PublisherNamespace[];
  namespaces_total?: number;
  /** its first and last settlement over the whole record, whatever the period; null when it never settled */
  first_settlement_at?: string | null;
  last_settlement_at?: string | null;
  /** every blob it paid for, over the whole record, by Tensile's reading; null until the API has counted them after a start */
  readings?: PublisherReadings | null;
  escrow: Escrow | null;
};

/** one namespace a publisher's settlements used */
export type PublisherNamespace = { namespace: string; settlements: number; bytes: number };

/** a publisher's blobs by the Blobs list's Tensile lane (lib/status.ts lane()) */
export type PublisherReadings = { available: number; unavailable: number; in_retention_window: number; not_read: number };

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
  amount_utia: number;
  available_at?: string;
};

/** one of a publisher's newest blobs, as its page lists them; the whole row is /v1/blobs/{promise_hash} */
export type RecentBlob = {
  promise_hash: string;
  commitment: string;
  namespace: string;
  blob_size: number;
  settlement_height: number;
  settlement_time: string;
  validators_with_rows: number;
  charge: { fee_utia: number } | null;
  reconstructable: { status: Reconstruct["status"] } | null;
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
/** a failed request in the words the site's notices use: "The observer API is busy (too many requests)" on a 429, else "… is not answering (…)" */
export function failedWords(f: { error: string | null; status?: number }): string {
  return `${throttled(f) ? "The observer API is busy" : "The observer API is not answering"} (${f.error ?? "no answer"})`;
}

// One in-flight request and one timer per (path, interval), however many
// components ask for it: the header, the banner, the footer and the page all
// want /v1/meta, which was four requests per interval per viewer.
// timer is the stream's one next ask: its interval after an answer, sooner
// while its figure is being computed, after a failure the back-off
// (failRetryMs). It asks nothing while the page is hidden: a stream that
// falls due then is due, and asks as soon as the page is shown again. load
// asks now; busy counts the requests out, and again asks once more when the
// last of them is back (askAgain). fails counts the failures in a row.
type Sub = {
  subs: Set<(f: Fetch<unknown>) => void>; timer: ReturnType<typeof setTimeout> | null; last: Fetch<unknown>;
  load: () => void; busy: number; again: boolean; fails: number; due: boolean;
};
const streams = new Map<string, Sub>();

/** how soon to ask again for a figure being computed: what the API says, kept between 2 and 30 s */
function computingRetryMs(seconds: unknown): number {
  const s = typeof seconds === "number" && isFinite(seconds) ? seconds : 5;
  return Math.min(Math.max(s, 2), 30) * 1000;
}

/**
 * How soon a stream asks again after its n-th failure in a row (n from 1): 2 s, doubling. A restart of the API
 * is then over on the page seconds after it is over, not at the stream's next interval, up to 5 minutes later.
 * It doubles up to the stream's interval; one asked more often than every 30 s goes on doubling up to 8 of its
 * intervals or 30 s, whichever is less, so a refusal (429) or an outage is not asked at the pace of the 1 s tip.
 * A Retry-After the answer gave is waited out, up to 5 minutes. A stream with no interval goes on asking, at
 * most every 5 minutes, until it is answered.
 */
export function failRetryMs(n: number, refreshMs: number, retryAfterMs = 0): number {
  const cap = refreshMs > 0 ? Math.max(refreshMs, Math.min(8 * refreshMs, 30000)) : 300000;
  const ms = Math.min(2000 * 2 ** Math.min(Math.max(n, 1) - 1, 20), cap);
  return Math.min(Math.max(ms, retryAfterMs), 300000);
}

/** a Retry-After header in ms, seconds or an HTTP date; 0 when there is none (across origins it is not readable) */
function retryAfterMs(h: string | null): number {
  if (!h) return 0;
  const s = Number(h);
  if (Number.isFinite(s)) return Math.max(0, s * 1000);
  const t = Date.parse(h);
  return Number.isFinite(t) ? Math.max(0, t - Date.now()) : 0;
}

/**
 * The page's own clock for the streams: none asks at its interval while the page is hidden (a background tab
 * asking every second for a header nobody sees was most of what tripped the proxy's limit), and every stream
 * that fell due meanwhile asks at once when it is shown again.
 */
let watching = false;
function watchVisibility(): void {
  if (watching || typeof document === "undefined") return;
  watching = true;
  document.addEventListener("visibilitychange", () => {
    if (document.hidden) return;
    streams.forEach((st) => { if (st.due) st.load(); });
  });
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
        // a refusal or an outage that says how long to wait (the site's proxy does, on a 429)
        return { data: null, error: msg, loading: false, fetchedAt: null, status: r.status, retryMs: retryAfterMs(r.headers.get("retry-after")) };
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

/** a stream of one path, asked at one interval (0: once), with every answer to fn; exported for its test */
export function subscribe(key: string, path: string, refreshMs: number, fn: (f: Fetch<unknown>) => void): () => void {
  let st = streams.get(key);
  if (!st) {
    const own: Sub = { subs: new Set(), timer: null, last: { data: null, error: null, loading: true, fetchedAt: null }, load: () => {}, busy: 0, again: false, fails: 0, due: false };
    st = own;
    streams.set(key, st);
    watchVisibility();
    // the next ask, ms from now: on a hidden page the stream is only due, and asks when the page is shown again
    const later = (ms: number) => {
      own.timer = setTimeout(() => {
        own.timer = null;
        if (typeof document !== "undefined" && document.hidden) own.due = true;
        else load();
      }, ms);
    };
    const load = async () => {
      if (own.timer) { clearTimeout(own.timer); own.timer = null; }
      own.due = false;
      own.busy++;
      const { retryMs, ...next } = await fetchOnce(path);
      own.busy--;
      if (streams.get(key) !== own) return;
      let wait = refreshMs;
      if (next.computing) {
        // Being computed: what is on screen stays, without an error, and the
        // stream asks again in a few seconds, whatever its interval (a stream
        // with no interval asks again too).
        own.last = own.last.data !== null ? { ...own.last, error: null, loading: false, status: next.status, computing: true } : next;
        own.fails = 0;
        wait = refreshMs > 0 ? Math.min(retryMs ?? 5000, refreshMs) : retryMs ?? 5000;
      } else {
        // A refresh that fails does not erase the answer already on screen. It
        // used to: fetchOnce returns {data: null, error} on any failure, so one
        // 500 or one proxy hiccup replaced a rendered table with a loading
        // message that never resolved, and the page said nothing about why.
        // The error is carried beside the last good payload instead, for the
        // page to show, and the figures keep their own computed_at so nobody
        // reads stale numbers as fresh ones.
        own.last = next.error && own.last.data !== null
          ? { data: own.last.data, error: next.error, loading: false, fetchedAt: own.last.fetchedAt, status: next.status }
          : next;
        // A failure of the API is asked again with a back-off of its own, not
        // at the interval: a stream asked every 5 minutes kept "not answering"
        // on screen for minutes after a restart of a few seconds, and one asked
        // every second went on asking a proxy that refused it. A 404 or a 400
        // says something about the record, not the API, and waits its interval.
        if (apiFailing(next)) wait = failRetryMs(++own.fails, refreshMs, retryMs);
        else own.fails = 0;
      }
      const out = own.last;
      own.subs.forEach((s) => s(out));
      if (own.again && own.busy === 0) {
        own.again = false;
        load();
        return;
      }
      if (own.busy === 0 && wait > 0) later(wait);
    };
    own.load = load;
    load();
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
      if (cur.timer) clearTimeout(cur.timer);
      streams.delete(key);
    }
  };
}

/**
 * How often the site asks for the newest block (/v1/tip): every second, so the header's block moves with the chain's
 * own pace (a block about every 3 s). Every reader of the tip asks with this one interval, so a page shares one stream;
 * the API keeps one answer for a quarter of a second, so a reader costs it a small cached reply, and however many
 * readers there are, the node and the database are asked at most four times a second.
 */
export const TIP_MS = 1000;

/**
 * Asks a stream again now, ahead of its interval: the one useApi(path, refreshMs) reads. A request already out is
 * followed by one more, so an answer from before the reason to ask again never has the last word. Nothing when no
 * page reads the stream.
 */
export function askAgain(path: string, refreshMs = 30000): void {
  const st = streams.get(`${refreshMs}|${path}`);
  if (!st) return;
  if (st.busy > 0) st.again = true;
  else st.load();
}

export function useApi<T>(path: string | null, refreshMs = 30000): Fetch<T> {
  const [state, setState] = useState<Fetch<T>>({ data: null, error: null, loading: !!path, fetchedAt: null });
  useEffect(() => {
    if (!path) return;
    return subscribe(`${refreshMs}|${path}`, path, refreshMs, (f) => setState(f as Fetch<T>));
  }, [path, refreshMs]);
  return state;
}

/**
 * The promise hash of the newest blob on record (/v1/tip's latest_blob), null until the tip has named one. It reads
 * the header's tip stream, so it asks nothing of its own, and it renders its page only when the blob changes, not at
 * every answer: what a page that waits for a blob to be recorded asks again on.
 */
export function useNewestBlob(): string | null {
  const [newest, setNewest] = useState<string | null>(null);
  useEffect(() => subscribe(`${TIP_MS}|/v1/tip`, "/v1/tip", TIP_MS, (f) => setNewest((f.data as Tip | null)?.latest_blob?.promise_hash ?? null)), []);
  return newest;
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
 * A namespace as a reader can take it in, from its significant bytes (the leading zero padding dropped):
 * - every byte printable ASCII: the text ("mochafibre");
 * - a name of three characters or more, then up to four bytes that are not text, as a client numbers the
 *   namespaces of one name: the name and those bytes' hex after a dot ("tensile·04");
 * - else the hex, whole up to six bytes, past that its first and last four characters ("8e5f…116c").
 * `hex` says the name is hex, set in the mono face. The full hex belongs in a tooltip beside it.
 */
export function nsName(ns: string): { text: string; hex: boolean } {
  const s = ns.replace(/^(00)+/, "");
  if (s.length >= 4 && s.length % 2 === 0) {
    const b = s.match(/../g)!.map((h) => parseInt(h, 16));
    const printable = (x: number) => x >= 0x20 && x < 0x7f;
    if (b.every(printable)) return { text: String.fromCharCode(...b), hex: false };
    const n = b.findIndex((x) => !printable(x));
    if (n >= 3 && b.length - n <= 4 && b.slice(n).every((x) => !printable(x))) {
      return { text: `${String.fromCharCode(...b.slice(0, n))}·${s.slice(2 * n)}`, hex: false };
    }
  }
  const h = s || ns;
  return { text: h.length <= 12 ? h : `${h.slice(0, 4)}…${h.slice(-4)}`, hex: true };
}
/** a namespace's name as text: nsName's */
export function nsDisplay(ns: string): string {
  return nsName(ns).text;
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

/**
 * /v1/tip: the newest block the observer has read, for the header ticker, and the newest blob it has stored
 * (latest_blob, in /v1/blobs' order; absent while there is none, and from an API older than it), which the
 * overview's recent blobs compare with the blobs they hold to know when to read the list
 */
export type Tip = {
  height: number;
  block_time?: string;
  fibre_active: boolean;
  latest_blob?: { promise_hash: string; settlement_height: number };
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

