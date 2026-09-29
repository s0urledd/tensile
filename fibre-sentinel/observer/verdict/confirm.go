package verdict

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
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
//   - Served: under the same conditions it got the validator's rows back,
//     verified: its own assignment, or at least as many rows as the
//     validator holds, verified against the commitment. The row does not
//     count, and says who fetched them (cleared_by); nothing is withdrawn
//     or rewritten, so the reading and the correlated-failure guard are
//     this observer's own.
//   - Anything else (no answer, one past its deadline, a gap on the other
//     side, a build that read by other rules, or verified rows fewer than
//     the validator holds, which neither confirms the reading nor fetched
//     the validator's rows): no confirmation, and the row does not count.

// ConfirmWindow is probe.ConfirmWindow, named here beside the rule.
const ConfirmWindow = probe.ConfirmWindow

// NotServed is what the rule needs from this observer's own row.
type NotServed struct {
	StartedAt      time.Time
	ClockOffsetMS  int64
	MustServeUntil time.Time
	// Held is how many rows the validator holds (the row's
	// assigned_row_count): an answer with fewer did not fetch its rows.
	Held int
}

// Confirmation is what the rule needs from another vantage's answer.
type Confirmation struct {
	Vantage            string
	StartedAt          time.Time
	ClockOffsetMS      int64
	Phase              probe.Phase
	Classification     probe.Classification
	CommitmentVerified bool
	// AssignmentVerified: the rows that came back are the validator's own
	// assignment, every one of them.
	AssignmentVerified bool
	// RowsReturned is how many rows came back.
	RowsReturned int
	// ClientRules: the answer was read under the Fibre client's rules.
	ClientRules bool
}

// ConfirmResult is the rule's answer for one confirming row.
type ConfirmResult int

const (
	// ConfirmNone: no answer the rule can use; the row does not count.
	ConfirmNone ConfirmResult = iota
	// ConfirmServed: the other vantage got the validator's rows back,
	// verified; the row does not count.
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
	case c.Classification == probe.ClassHealthy || c.AssignmentVerified ||
		(c.CommitmentVerified && r.Held > 0 && c.RowsReturned >= r.Held):
		return ConfirmServed
	case c.CommitmentVerified:
		// Verified rows, fewer than the validator holds: the other vantage
		// did not fetch its rows, and it got some, so the answer neither
		// confirms the reading nor clears it.
		return ConfirmNone
	case probe.Confirmable(probe.EndReadLabel, c.Classification):
		return ConfirmConfirmed
	}
	return ConfirmNone
}

// ConfirmationOf is the rule's input from another vantage's row.
func ConfirmationOf(m probe.Measurement) Confirmation {
	return Confirmation{Vantage: m.Vantage, StartedAt: m.StartedAt, ClockOffsetMS: m.ClockOffsetMS, Phase: m.Phase,
		Classification: m.Classification, CommitmentVerified: m.Download.CommitmentVerified,
		AssignmentVerified: m.Download.AssignmentVerified, RowsReturned: m.Download.RowsReturned, ClientRules: m.ClientRules}
}

// NotServedOf is the rule's input from this observer's own row.
func NotServedOf(m probe.Measurement) NotServed {
	return NotServed{StartedAt: m.StartedAt, ClockOffsetMS: m.ClockOffsetMS, MustServeUntil: m.MustServeUntil, Held: m.AssignedRowCount}
}

// ConfirmationKey is the slot an answer is about, without a vantage: the
// promise, the validator and the reading's scheduled time (the key of
// probe.ConfirmRequest).
func ConfirmationKey(promiseHash, validator string, scheduledAt time.Time) string {
	return promiseHash + "|" + validator + "|" + scheduledAt.UTC().Format(time.RFC3339Nano)
}

// LoadConfirmations reads every other vantage's answers under dir, by
// ConfirmationKey: <dir>/<name>/measurements.jsonl, which is the live
// record's vantages directory and the same path in an untarred daily
// export. Line by line and forgiving, as the collector's tail is: a copy
// that ends in half a line (a pull in progress) loses that line only. A
// missing dir is no answers.
func LoadConfirmations(dir string) map[string][]Confirmation {
	out := map[string][]Confirmation{}
	files, _ := filepath.Glob(filepath.Join(dir, "*", "measurements.jsonl"))
	sort.Strings(files)
	for _, path := range files {
		f, err := os.Open(path)
		if err != nil {
			continue
		}
		r := bufio.NewReaderSize(f, 1<<20)
		for {
			line, err := r.ReadBytes('\n')
			if err != nil {
				break
			}
			var m probe.Measurement
			if json.Unmarshal(line, &m) != nil || m.Vantage == "" || m.PromiseHash == "" {
				continue
			}
			k := ConfirmationKey(m.PromiseHash, m.ValidatorAddress, m.ScheduledAt)
			out[k] = append(out[k], ConfirmationOf(m))
		}
		f.Close()
	}
	return out
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
