package verdict

import (
	"sort"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/probe"
)

// Not-served readings re-checked from a second vantage.
//
// Every row the observer records as not served (probe.Confirmable: a FAULT,
// or at the end reading any answer that left the reader without rows) is
// asked once more from another location (internal/probe/confirm.go): the
// same rows, from the same validator, with the same verification. The rule applied to the answer is this file, and it
// is the only implementation: the collector draws it over the stored rows
// and sentinel-recompute over the JSONL record.
//
//   - Cleared: the other vantage started within probe.ConfirmWindow of the
//     fault and got the exact rows back, verified against the commitment and
//     the assignment (HEALTHY). The shard was there; what failed was this
//     observer's reading of it. The row is withdrawn and filed
//     PROBE_ERROR, the class of an observer-side failure: outside the rate,
//     neither for nor against the validator. It is never credited as served,
//     because the rates are this observer's own readings and a second
//     vantage adds none.
//   - Confirmed: the other vantage started in time, reached a verdict, and
//     it was not HEALTHY. The fault stands, and says so.
//   - No answer: nothing started in time, or what did could not be carried
//     out (PROBE_ERROR, NOT_PROBED). The fault stands as recorded, exactly as
//     it did before there was a second vantage.
//
// The window is measured between the two probes' own start times, both on
// the record, so a third party redraws the same line from the rows; when the
// answer reached the collector does not enter into it.

// ConfirmWindow is probe.ConfirmWindow, named here beside the rule.
const ConfirmWindow = probe.ConfirmWindow

// Confirmation is what the rule needs from another vantage's answer.
type Confirmation struct {
	Vantage        string
	StartedAt      time.Time
	Classification probe.Classification
}

// ConfirmResult is the rule's answer for one confirming row.
type ConfirmResult int

const (
	// ConfirmNone: no answer the rule can use; the fault stands as recorded.
	ConfirmNone ConfirmResult = iota
	// ConfirmCleared: the other vantage got the verified rows; the fault is
	// withdrawn.
	ConfirmCleared
	// ConfirmConfirmed: the other vantage did not get them either.
	ConfirmConfirmed
)

// ConfirmFault applies the rule to one confirming row of a FAULT that
// started at faultStarted.
func ConfirmFault(faultStarted time.Time, c Confirmation) ConfirmResult {
	if c.StartedAt.Before(faultStarted) || c.StartedAt.After(faultStarted.Add(ConfirmWindow)) {
		return ConfirmNone
	}
	switch c.Classification {
	case probe.ClassHealthy:
		return ConfirmCleared
	case probe.ClassProbeError, probe.ClassNotProbed, "":
		return ConfirmNone
	}
	return ConfirmConfirmed
}

// ConfirmFaultBy folds every confirming row of one FAULT: cleared by the
// first vantage (in name order) whose answer clears it, otherwise confirmed
// by the first whose answer confirms it; both empty when no answer counts.
func ConfirmFaultBy(faultStarted time.Time, cs []Confirmation) (clearedBy, confirmedBy string) {
	sorted := append([]Confirmation(nil), cs...)
	sort.SliceStable(sorted, func(i, j int) bool { return sorted[i].Vantage < sorted[j].Vantage })
	for _, c := range sorted {
		switch ConfirmFault(faultStarted, c) {
		case ConfirmCleared:
			if clearedBy == "" {
				clearedBy = c.Vantage
			}
		case ConfirmConfirmed:
			if confirmedBy == "" {
				confirmedBy = c.Vantage
			}
		}
	}
	return clearedBy, confirmedBy
}

// ClearedClass is the class a cleared fault is filed under.
const ClearedClass = probe.ClassProbeError

// ClearedReason is the reason a cleared fault carries.
func ClearedReason(vantage string, confirmStarted time.Time, was string) string {
	return "cleared from a second location: " + vantage + " fetched the same rows at " + confirmStarted.UTC().Format(time.RFC3339) +
		" and they verified against the commitment and the assignment, so the failure (" + was +
		") was this observer's reading, not the validator's; held out of the rate as an observer-side failure, never credited as served"
}
