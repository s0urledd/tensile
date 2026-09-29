package probe

import (
	"testing"
	"time"
)

// A reading the guard sets aside only until its deferred verdicts are
// drawn is not set aside for good: a drawn verdict (genuine rows, served)
// joins the denominator and can lift the guard, so the prober still sends
// the reading's requests. Three unreachable, three deferred and one served
// is 3 of 4 as written and 3 of 7 once drawn.
func TestAGuardADeferredVerdictCanLiftIsNotForGood(t *testing.T) {
	at := time.Date(2026, 9, 25, 12, 0, 0, 0, time.UTC)
	row := func(addr string, out Outcome, cls Classification) Measurement {
		return Measurement{SchemaVersion: MeasurementSchemaVersion, PromiseHash: "p", ValidatorAddress: addr, ScheduleLabel: EndReadLabel,
			ScheduledAt: at, StartedAt: at, Assigned: true, Attested: true, Phase: PhaseInWindow, Outcome: out, Classification: cls,
			AssignedRowCount: 4}
	}
	var ms []Measurement
	for _, a := range []string{"u1", "u2", "u3"} {
		ms = append(ms, row(a, OutcomeRPCTimeout, ClassUnreachable))
	}
	for _, a := range []string{"d1", "d2", "d3"} {
		m := row(a, OutcomePartial, ClassProbeError)
		m.Download.ShadowGap, m.Download.CommitmentVerified, m.Download.RowsReturned = "pending promise scan", true, 2
		ms = append(ms, m)
	}
	ms = append(ms, row("s", OutcomeServedOK, ClassHealthy))
	if !VerdictDeferred(ms[3]) {
		t.Fatal("a genuine short answer awaiting its shadow verdict is not deferred")
	}
	if !GuardSetsAside(ms) {
		t.Fatal("three of four unreachable is not set aside as written")
	}
	if GuardSetsAsideForGood(ms) {
		t.Fatal("set aside for good, though the deferred verdicts can lift the guard")
	}
	// Without the deferred rows the guard holds whatever is drawn later.
	if !GuardSetsAsideForGood(append(append([]Measurement(nil), ms[:3]...), ms[6])) {
		t.Fatal("three of four unreachable, nothing deferred, is not set aside for good")
	}
}
