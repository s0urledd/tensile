import type { Rate, Window } from "./api";

/**
 * Signing participation, per validator (validator.signing on /v1/validators
 * and the validator page). Assigned is every settled promise in the period
 * that gave the validator rows and whose signatures this observer verified;
 * signed is how many of those carry its verified signature. Promises from
 * before verification are `unknown`, on neither side.
 *
 * Descriptive only: the publisher stops collecting at two thirds of voting
 * power, so an unsigned promise is unproven, never a fault, and this figure
 * accuses nobody.
 */
/** no_host: assigned promises that settled while the validator had no Fibre host, outside both sides of the rate */
/**
 * last_endorsed_at and recent read the whole record, whatever the window: the
 * newest promise carrying the validator's endorsement, and how many of its
 * newest assigned promises (up to 20) do.
 */
export type Signing = {
  assigned: number; signed: number; unknown: number; no_host?: number;
  last_endorsed_at?: string | null;
  /** on the validator page only */
  recent?: { assigned: number; endorsed: number };
};

/** One bar of /v1/signing: promises whose verified signatures cover [from, to) of total voting power. */
export type SigningBucket = { key: string; label: string; from: number; to: number; above_threshold: boolean; count: number };

/** /v1/signing: how much voting power each settled promise in the period collected. */
export type SigningDistribution = {
  window: Window;
  threshold: { num: number; den: number; rule: string };
  /** settled promises with verified signatures: the histogram's population */
  promises: number;
  /** settled promises recorded before signatures were verified, outside it */
  unknown: number;
  /** promises at or above the quorum as this observer verified them, over promises */
  meets_threshold: Rate;
  buckets: SigningBucket[];
  /** median assigned signers per promise: how many validators it took */
  signers_median: number | null;
  computed_at: string;
  note: string;
};

