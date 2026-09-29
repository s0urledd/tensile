package verdict

import (
	"sort"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/probe"
)

// Not-served readings confirmed from a second vantage.
//
// A not-served reading counts against a validator only once a second
// location has confirmed it (Row.Confirmed); until then, and for good if it
// never is, it counts neither way. The prober asks for every row that can
// count (probe.ConfirmationDue) and the other location reads the same rows
// from the same validator once more (internal/probe/confirm.go). The rule
// applied to its answer is this file, and it is the only implementation:
// the collector draws it over the stored rows and sentinel-recompute over
// the JSONL record.
//
//   - Confirmed: the other location read the shard under the Fibre client's
//     rules (probe.Measurement.ClientRules), started after the reading and
//     before must_serve_until, within ConfirmWindow of the reading, all on
//     the chain's clock (each row's started_at less its own clock offset),
//     inside the window by its own clock as well, and got no rows for a
//     reason that is the validator's: a class that leaves the reader
//     without rows (probe.Confirmable), not a gap on its own side.
//   - Served: under the same conditions it got rows that verified against
//     the commitment. The row does not count, and says who fetched them
//     (cleared_by); nothing is withdrawn or rewritten, so the reading and
//     the correlated-failure guard are this observer's own.
//   - Anything else (no answer, one past its deadline, a gap on the other
//     side, a build that read by other rules): no confirmation, and the row
//     does not count.

// ConfirmWindow is probe.ConfirmWindow, named here beside the rule.
const ConfirmWindow = probe.ConfirmWindow

// NotServed is what the rule needs from this observer's own row.
type NotServed struct {
	StartedAt      time.Time
	ClockOffsetMS  int64
	MustServeUntil time.Time
}

// Confirmation is what the rule needs from another vantage's answer.
type Confirmation struct {
	Vantage            string
	StartedAt          time.Time
	ClockOffsetMS      int64
	Phase              probe.Phase
	Classification     probe.Classification
	CommitmentVerified bool
	// ClientRules: the answer was read under the Fibre client's rules.
	ClientRules bool
}

// ConfirmResult is the rule's answer for one confirming row.
type ConfirmResult int

const (
	// ConfirmNone: no answer the rule can use; the row does not count.
	ConfirmNone ConfirmResult = iota
	// ConfirmServed: the other vantage got verified rows; the row does not
	// count.
	ConfirmServed
	// ConfirmConfirmed: the other vantage did not get the rows either, for
	// a reason that is the validator's; the row counts.
	ConfirmConfirmed
)

// ChainTime places a row's local start on the chain's clock: started_at
// less the clock offset the row was measured with (the local clock minus
// the chain's latest block time).
func ChainTime(started time.Time, clockOffsetMS int64) time.Time {
	return started.Add(-time.Duration(clockOffsetMS) * time.Millisecond)
}

// ConfirmNotServed applies the rule to one confirming row of a not-served
// reading.
func ConfirmNotServed(r NotServed, c Confirmation) ConfirmResult {
	rs := ChainTime(r.StartedAt, r.ClockOffsetMS)
	cs := ChainTime(c.StartedAt, c.ClockOffsetMS)
	if !c.ClientRules || c.Phase != probe.PhaseInWindow || cs.Before(rs) || cs.After(rs.Add(ConfirmWindow)) || !cs.Before(r.MustServeUntil) {
		return ConfirmNone
	}
	switch {
	case c.CommitmentVerified || c.Classification == probe.ClassHealthy:
		return ConfirmServed
	case probe.Confirmable(probe.EndReadLabel, c.Classification):
		return ConfirmConfirmed
	}
	return ConfirmNone
}

// ConfirmNotServedBy folds every confirming row of one reading: servedBy is
// the first vantage (in name order) that got the rows, confirmedBy the
// first that confirmed, and a reading any vantage got the rows of is not
// confirmed. Both are empty when no answer counts.
func ConfirmNotServedBy(r NotServed, cs []Confirmation) (servedBy, confirmedBy string) {
	sorted := append([]Confirmation(nil), cs...)
	sort.SliceStable(sorted, func(i, j int) bool { return sorted[i].Vantage < sorted[j].Vantage })
	for _, c := range sorted {
		switch ConfirmNotServed(r, c) {
		case ConfirmServed:
			if servedBy == "" {
				servedBy = c.Vantage
			}
		case ConfirmConfirmed:
			if confirmedBy == "" {
				confirmedBy = c.Vantage
			}
		}
	}
	if servedBy != "" {
		confirmedBy = ""
	}
	return servedBy, confirmedBy
}

// ClearedClass is the class a row withdrawn by the earlier rule was filed
// under (a probe_amendments line with cleared_by). The rule no longer
// withdraws anything; sentinel-recompute applies such a line as recorded.
const ClearedClass = probe.ClassProbeError
