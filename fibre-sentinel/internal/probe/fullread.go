package probe

import (
	"strconv"
	"strings"
	"time"
)

// A full reading asks every validator that endorsed the promise for its own
// rows (Config.AskEveryEndorser), whatever the rows already held, and asks a
// validator whose answer did not serve again later in the window (the
// retries, retry.go). Each endorser is then judged on its own answers, not
// on the blob's reconstruction (observer/verdict Row.CountedClass, rollup's
// SQL twin):
//
//   - served: an attempt's rows came back and verified (FullServed);
//   - this observer's gap, counted neither way: an attempt that is NOT_PROBED
//     or PROBE_ERROR (FullGap), or a reading in which not a single request
//     reached a server;
//   - otherwise not served, by the last of its attempts.
//
// Readings that were not full (every label before FullReadSince: w1..wN,
// grace, post, end) keep the rule of their time.

// FullReadLabel is the schedule label of a full reading: the one reading of
// a blob, EndReadOffset before its must_serve_until, that asked every
// endorser. Its rows of a later attempt carry the same label and point,
// with Measurement.Attempt above 0.
const FullReadLabel = "full"

// FullReadSince is the moment the prober began asking every endorser:
// the deploy of -end-read-all (PR #177), 2026-10-02T16:09:49Z. Those
// readings were labelled EndReadLabel until FullReadLabel existed, so an
// "end" row started at or after it belongs to a full reading too
// (FullReading). An "end" row started before it is of a reading that stopped
// once the rows reconstructed the blob. It is a constant of the record:
// moving it would rewrite how readings already made are judged.
var FullReadSince = time.Date(2026, time.October, 2, 16, 9, 49, 0, time.UTC)

// FullReadRetries is how many more times a full reading asks a validator
// whose answer did not serve: up to two, while a retry can still start
// before must_serve_until less Config.RequestStartMargin.
const FullReadRetries = 2

// FullReading reports whether a row with this label, started at startedAt,
// belongs to a full reading.
func FullReading(label string, startedAt time.Time) bool {
	return label == FullReadLabel || (label == EndReadLabel && !startedAt.Before(FullReadSince))
}

// EndOfWindowLabel reports whether a label is the one reading of a blob near
// the end of its window, full or not: a served answer there speaks for the
// end of the promise.
func EndOfWindowLabel(label string) bool {
	return label == EndReadLabel || label == FullReadLabel
}

// FullServed reports whether one answer of a full reading served: its rows
// came back and verified against the commitment, and they were no fewer
// than the validator holds, unless they are exactly another settled
// promise's assignment over the same commitment (SHADOWED_SHARD), which a
// validator cannot tell apart from this one's.
func FullServed(verified bool, o Outcome, c Classification) bool {
	return verified && (o != OutcomePartial || c == ClassShadowedShard)
}

// FullGap reports whether an answer of a full reading is this observer's
// gap: the request could not be made in time (NOT_PROBED), or it failed on
// this observer's side (PROBE_ERROR: its resolver, a request abandoned by a
// restart, a verdict deferred on its own blindness).
func FullGap(c Classification) bool {
	return c == ClassNotProbed || c == ClassProbeError
}

// AttemptOfKey is the attempt a dedupe key names: 0 for the reading's own
// request (a key of four fields), the trailing number otherwise.
func AttemptOfKey(key string) int {
	parts := strings.Split(key, "|")
	if len(parts) < 5 {
		return 0
	}
	n, err := strconv.Atoi(parts[len(parts)-1])
	if err != nil || n < 0 {
		return 0
	}
	return n
}
