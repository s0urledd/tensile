package verdict

import (
	"testing"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/probe"
)

// The rule, cell by cell: only a no-rows answer read under the client's
// rules, started after the reading and before must_serve_until, confirms;
// verified rows there say served; anything else is no answer, and the row
// does not count.
func TestConfirmNotServed(t *testing.T) {
	at := time.Date(2026, 9, 25, 12, 0, 0, 0, time.UTC)
	r := NotServed{StartedAt: at, MustServeUntil: at.Add(10 * time.Minute)}
	for _, c := range []struct {
		name   string
		after  time.Duration
		cls    probe.Classification
		phase  probe.Phase
		rules  bool
		offset int64 // the confirming row's clock offset, ms
		want   ConfirmResult
	}{
		{"not found there too", 3 * time.Minute, probe.ClassFault, probe.PhaseInWindow, true, 0, ConfirmConfirmed},
		{"unreachable from there", 3 * time.Minute, probe.ClassUnreachable, probe.PhaseInWindow, true, 0, ConfirmConfirmed},
		{"throttled there", 3 * time.Minute, probe.ClassThrottled, probe.PhaseInWindow, true, 0, ConfirmConfirmed},
		{"server error there", 3 * time.Minute, probe.ClassServerError, probe.PhaseInWindow, true, 0, ConfirmConfirmed},
		{"certificate rejected there", 3 * time.Minute, probe.ClassIdentityMismatch, probe.PhaseInWindow, true, 0, ConfirmConfirmed},
		{"verified rows there", 3 * time.Minute, probe.ClassHealthy, probe.PhaseInWindow, true, 0, ConfirmServed},
		{"an older build's answer", 3 * time.Minute, probe.ClassFault, probe.PhaseInWindow, false, 0, ConfirmNone},
		{"its own gap", 3 * time.Minute, probe.ClassProbeError, probe.PhaseInWindow, true, 0, ConfirmNone},
		{"not probed", 3 * time.Minute, probe.ClassNotProbed, probe.PhaseInWindow, true, 0, ConfirmNone},
		{"no host", 3 * time.Minute, probe.ClassNotRegistered, probe.PhaseInWindow, true, 0, ConfirmNone},
		{"at must_serve_until", 10 * time.Minute, probe.ClassFault, probe.PhaseInWindow, true, 0, ConfirmNone},
		{"after must_serve_until, a prune", 11 * time.Minute, probe.ClassTolerated, probe.PhaseGrace, true, 0, ConfirmNone},
		{"grace by its own clock", 9 * time.Minute, probe.ClassFault, probe.PhaseGrace, true, 0, ConfirmNone},
		{"before the reading", -time.Second, probe.ClassFault, probe.PhaseInWindow, true, 0, ConfirmNone},
		// its clock two minutes behind the chain's: started 90 s after the
		// reading on the chain's clock, though its own says 30 s before
		{"a clock behind, corrected", -30 * time.Second, probe.ClassFault, probe.PhaseInWindow, true, -2 * 60 * 1000, ConfirmConfirmed},
		// its clock two minutes behind again: a minute before
		// must_serve_until by its own clock, a minute past it on the chain's
		{"a clock behind, past the deadline", 9 * time.Minute, probe.ClassFault, probe.PhaseInWindow, true, -2 * 60 * 1000, ConfirmNone},
	} {
		got := ConfirmNotServed(r, Confirmation{Vantage: "de-1", StartedAt: at.Add(c.after), ClockOffsetMS: c.offset,
			Phase: c.phase, Classification: c.cls, CommitmentVerified: c.cls == probe.ClassHealthy, ClientRules: c.rules})
		if got != c.want {
			t.Errorf("%s: %d, want %d", c.name, got, c.want)
		}
	}
	// The reading's own offset counts the same way: this observer's clock a
	// minute ahead puts the reading a minute earlier on the chain's clock.
	ahead := NotServed{StartedAt: at, ClockOffsetMS: 60 * 1000, MustServeUntil: at.Add(10 * time.Minute)}
	if got := ConfirmNotServed(ahead, Confirmation{StartedAt: at.Add(-30 * time.Second), Phase: probe.PhaseInWindow,
		Classification: probe.ClassFault, ClientRules: true}); got != ConfirmConfirmed {
		t.Errorf("a confirmation 30 s before the reading by the clocks, 30 s after it on the chain's: %d", got)
	}
	// Only a class that leaves the reader without rows confirms.
	for _, cls := range probe.AllClassifications {
		got := ConfirmNotServed(r, Confirmation{StartedAt: at.Add(time.Minute), Phase: probe.PhaseInWindow, Classification: cls, ClientRules: true})
		want := probe.Confirmable(probe.EndReadLabel, cls)
		if (got == ConfirmConfirmed) != want {
			t.Errorf("%s: %d, confirms %v", cls, got, want)
		}
	}
}

// The other vantage fetched the validator's rows only when it got all of
// them back verified. Verified rows fewer than the validator holds are
// neither: they do not say the rows were fetched, and the validator
// answered with rows, so they do not confirm a not-served reading either.
func TestAShortVerifiedAnswerThereIsNoAnswer(t *testing.T) {
	at := time.Date(2026, 9, 25, 12, 0, 0, 0, time.UTC)
	r := NotServed{StartedAt: at, MustServeUntil: at.Add(10 * time.Minute), Held: 8}
	for _, c := range []struct {
		name               string
		cls                probe.Classification
		verified, assigned bool
		rows               int
		want               ConfirmResult
	}{
		{"its assignment, verified", probe.ClassHealthy, true, true, 8, ConfirmServed},
		{"as many genuine rows as it holds", probe.ClassUnmatchedGenuine, true, false, 8, ConfirmServed},
		{"the same short shard", probe.ClassUnmatchedGenuine, true, false, 4, ConfirmNone},
		{"one of eight", probe.ClassShadowedShard, true, false, 1, ConfirmNone},
		{"a short shard deferred there", probe.ClassProbeError, true, false, 3, ConfirmNone},
		{"no rows there", probe.ClassFault, false, false, 0, ConfirmConfirmed},
	} {
		got := ConfirmNotServed(r, Confirmation{Vantage: "de-1", StartedAt: at.Add(3 * time.Minute), Phase: probe.PhaseInWindow,
			Classification: c.cls, CommitmentVerified: c.verified, AssignmentVerified: c.assigned, RowsReturned: c.rows, ClientRules: true})
		if got != c.want {
			t.Errorf("%s: %d, want %d", c.name, got, c.want)
		}
	}
	// A row that does not say how many rows the validator holds: only its
	// own assignment, verified, is the rows fetched.
	unknown := NotServed{StartedAt: at, MustServeUntil: at.Add(10 * time.Minute)}
	if got := ConfirmNotServed(unknown, Confirmation{StartedAt: at.Add(time.Minute), Phase: probe.PhaseInWindow,
		Classification: probe.ClassUnmatchedGenuine, CommitmentVerified: true, RowsReturned: 8, ClientRules: true}); got != ConfirmNone {
		t.Errorf("genuine rows against a row that holds none on record: %d, want none", got)
	}
}

// Several vantages: one that got the rows means the reading is not
// confirmed, whatever another says.
func TestConfirmNotServedByFoldsVantages(t *testing.T) {
	at := time.Date(2026, 9, 25, 12, 0, 0, 0, time.UTC)
	r := NotServed{StartedAt: at, MustServeUntil: at.Add(10 * time.Minute)}
	c := func(v string, cls probe.Classification) Confirmation {
		return Confirmation{Vantage: v, StartedAt: at.Add(time.Minute), Phase: probe.PhaseInWindow, Classification: cls,
			CommitmentVerified: cls == probe.ClassHealthy, ClientRules: true}
	}
	served, confirmed := ConfirmNotServedBy(r, []Confirmation{c("us-2", probe.ClassFault), c("de-1", probe.ClassHealthy), c("sg-1", probe.ClassUnreachable)})
	if served != "de-1" || confirmed != "" {
		t.Errorf("served %q confirmed %q, want de-1 and none", served, confirmed)
	}
	served, confirmed = ConfirmNotServedBy(r, []Confirmation{c("us-2", probe.ClassFault), c("sg-1", probe.ClassUnreachable)})
	if served != "" || confirmed != "sg-1" {
		t.Errorf("served %q confirmed %q, want none and sg-1", served, confirmed)
	}
	if served, confirmed := ConfirmNotServedBy(r, nil); served != "" || confirmed != "" {
		t.Errorf("no answers: %q %q", served, confirmed)
	}
}

// A not-served row counts only once confirmed: before that, and for good if
// it never is, it counts neither way, not even provisionally.
func TestANotServedRowCountsOnlyOnceConfirmed(t *testing.T) {
	r := Row{ScheduleLabel: probe.EndReadLabel, Classification: probe.ClassUnreachable}
	if got := r.CountedClass(true); got != NotCounted {
		t.Errorf("unconfirmed on an Unavailable blob: %s, want %s", got, NotCounted)
	}
	r.Confirmed = true
	if got := r.CountedClass(true); got != probe.ClassFault {
		t.Errorf("confirmed on an Unavailable blob: %s, want FAULT", got)
	}
	if got := r.CountedClass(false); got != NotCounted {
		t.Errorf("confirmed on an Available blob: %s, want %s", got, NotCounted)
	}
	short := Row{ScheduleLabel: probe.EndReadLabel, Classification: probe.ClassUnmatchedGenuine, RowsReturned: 2, AssignedRowCount: 4}
	if got := short.CountedClass(true); got != NotCounted {
		t.Errorf("a short answer, unconfirmed: %s, want %s", got, NotCounted)
	}
	short.Confirmed = true
	if got := short.CountedClass(true); got != probe.ClassFault {
		t.Errorf("a short answer, confirmed: %s, want FAULT", got)
	}
	if got := short.CountedClass(false); got != probe.ClassHealthy {
		t.Errorf("a short answer on an Available blob: %s, want HEALTHY", got)
	}
}

// The prober's guard at the end of a reading is the verdict's over the same
// rows, so a reading it sends no request for is one the figures set aside.
func TestTheProbersGuardIsTheVerdicts(t *testing.T) {
	at := time.Date(2026, 9, 25, 12, 0, 0, 0, time.UTC)
	classes := []probe.Classification{probe.ClassUnreachable, probe.ClassFault, probe.ClassThrottled, probe.ClassHealthy,
		probe.ClassProbeError, probe.ClassUnattested, probe.ClassServerError, probe.ClassNotRegistered}
	blobs := Blobs{"p": {Needed: 1000, Endorsed: map[string]int{}, Assigned: map[string]int{}}}
	for n := 0; n < 4096; n++ {
		var ms []probe.Measurement
		var rows []Row
		x := n
		for v := 0; v < 6; v++ {
			cls := classes[x%len(classes)]
			x /= len(classes)
			if v >= 4 {
				cls = classes[(n+v)%len(classes)]
			}
			addr := string(rune('a' + v))
			m := probe.Measurement{PromiseHash: "p", ValidatorAddress: addr, ScheduleLabel: probe.EndReadLabel, ScheduledAt: at,
				StartedAt: at, Assigned: true, Attested: true, Phase: probe.PhaseInWindow, Classification: cls, AssignedRowCount: 10}
			ms = append(ms, m)
			rows = append(rows, FromMeasurement(m))
			blobs["p"].Endorsed[addr], blobs["p"].Assigned[addr] = 10, 10
		}
		got := probe.GuardSetsAside(ms)
		want := len(SuspectPoints(rows, Window{All: true, End: at.Add(time.Hour)}, blobs)) > 0
		if got != want {
			t.Fatalf("case %d: prober %v, verdict %v (%v)", n, got, want, rows)
		}
	}
	if UnreachableThreshold != probe.GuardShare || FaultThreshold != probe.GuardShare || MinValidators != probe.GuardMinValidators {
		t.Fatal("the verdict's guard constants are not the prober's")
	}
}
