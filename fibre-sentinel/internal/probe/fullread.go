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
//   - served: an answer handed over the validator's own rows, verified
//     (FullServed);
//   - this observer's gap, counted neither way: an answer that is NOT_PROBED
//     or PROBE_ERROR, or genuine rows of the blob that are not the
//     validator's and that no settled promise explains (FullGap); a reading
//     in which not a single request reached a server; and an answer after
//     which the validator was still owed an attempt that is not on record
//     (Measurement.NextAttemptDue);
//   - otherwise not served, by the last of its answers.
//
// Readings that were not full (every label before FullReadSince: w1..wN,
// grace, post, end; and EnoughReadLabel after it) keep the rule of their
// time.

// FullReadLabel is the schedule label of a full reading: the one reading of
// a blob, EndReadOffset before its must_serve_until, that asked every
// endorser. Its rows of a later attempt carry the same label and point,
// with Measurement.Attempt above 0.
const FullReadLabel = "full"

// EnoughReadLabel is the schedule label of a reading made after full
// readings began that stopped once the rows reconstructed the blob
// (sentinel-probe -end-read-all=false). It is the end of the window's one
// reading, judged by the rule of a reading that stopped at enough, never as
// a full reading: without its own label it would read as EndReadLabel from
// FullReadSince on, which is a full reading.
const EnoughReadLabel = "enough"

// FullReadSince is the moment the prober began asking every endorser:
// the deploy of -end-read-all (PR #177), 2026-10-02T16:09:49Z. Those
// readings were labelled EndReadLabel until FullReadLabel existed, so an
// "end" row started at or after it belongs to a full reading too
// (FullReading). An "end" row started before it is of a reading that stopped
// once the rows reconstructed the blob. It is a constant of the record:
// moving it would rewrite how readings already made are judged. No build
// writes EndReadLabel any more: a full reading is FullReadLabel, and one
// that stops at enough is EnoughReadLabel.
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
	return label == EndReadLabel || label == FullReadLabel || label == EnoughReadLabel
}

// FullServed reports whether one answer of a full reading served: the rows
// that came back verified against the commitment and are exactly the
// validator's own assignment (SERVED_OK), or exactly another settled
// promise's assignment over the same commitment (SHADOWED_SHARD), which a
// validator cannot tell apart from this one's.
func FullServed(verified bool, o Outcome, c Classification) bool {
	return verified && (o == OutcomeServedOK || c == ClassShadowedShard)
}

// FullForeign reports a verified answer of rows that are not the validator's
// own and that no settled promise explains (UNMATCHED_GENUINE, or a verdict
// deferred on a scan gap): more of them than it holds, or other ones
// (WRONG_ROWS), or fewer that are not a part of its own (PARTIAL, not
// subsetOfOwn). The store serves the first shard of a commitment by
// promise-hash order, so such rows say neither that the validator holds its
// own rows nor that it does not. A short answer that is a part of its own
// rows is not foreign: it handed over fewer of its rows than it holds.
func FullForeign(verified bool, o Outcome, c Classification, subsetOfOwn bool) bool {
	return verified && c != ClassShadowedShard && (o == OutcomeWrongRows || (o == OutcomePartial && !subsetOfOwn))
}

// FullGap reports whether an answer of a full reading is this observer's
// gap, counted neither way: the request could not be made in time
// (NOT_PROBED), it failed on this observer's side (PROBE_ERROR: its
// resolver, its network, a request abandoned by a restart, a verdict
// deferred on its own blindness), or the rows that came back are foreign
// (FullForeign).
func FullGap(verified bool, o Outcome, c Classification, subsetOfOwn bool) bool {
	return c == ClassNotProbed || c == ClassProbeError || FullForeign(verified, o, c, subsetOfOwn)
}

// fullServed and fullGap are FullServed and FullGap of a row.
func (m Measurement) fullServed() bool {
	return FullServed(m.Download.CommitmentVerified, m.Outcome, m.Classification)
}

func (m Measurement) fullGap() bool {
	return FullGap(m.Download.CommitmentVerified, m.Outcome, m.Classification, m.Download.RowsSubsetOfAssignment)
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
