package verdict

import (
	"testing"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/probe"
)

// A candidate in range whose assignment rows were never recorded cannot be
// matched or ruled out, so the late verdict is PROBE_ERROR for good, as the
// store's SQL draws it; recorded candidates are still tried first.
func TestLateShadow_UnrecordedCandidateIsAGap(t *testing.T) {
	probeAt := time.Date(2026, 9, 18, 12, 0, 0, 0, time.UTC)
	frontier := probeAt.Add(2 * time.Hour)
	got := []uint32{4, 5}
	recorded := Candidate{PromiseHash: "0bbb", Commitment: "c", SettlementTime: probeAt.Add(20 * time.Minute), MustServeUntil: probeAt.Add(time.Hour), Rows: []int{5, 4}}
	blank := Candidate{PromiseHash: "0aaa", Commitment: "c", SettlementTime: probeAt.Add(-time.Minute), MustServeUntil: probeAt.Add(time.Hour)}
	if cls, by, ok := LateShadow(got, probeAt, frontier, time.Hour, 5*time.Minute, []Candidate{blank, recorded}); !ok || cls != probe.ClassShadowedShard || by != "0bbb" {
		t.Fatalf("a recorded match beside an unrecorded candidate: %s %q ok=%v", cls, by, ok)
	}
	if cls, _, ok := LateShadow(got, probeAt, frontier, time.Hour, 5*time.Minute, []Candidate{blank}); !ok || cls != probe.ClassProbeError {
		t.Fatalf("only an unrecorded candidate: %s ok=%v, want PROBE_ERROR", cls, ok)
	}
	// an unrecorded candidate outside the lifetime is no candidate
	stale := Candidate{PromiseHash: "0aaa", Commitment: "c", SettlementTime: probeAt.Add(-3 * time.Hour), MustServeUntil: probeAt.Add(-2 * time.Hour)}
	if cls, _, ok := LateShadow(got, probeAt, frontier, time.Hour, 5*time.Minute, []Candidate{stale}); !ok || cls != probe.ClassUnmatchedGenuine {
		t.Fatalf("unrecorded candidate outside the lifetime: %s ok=%v, want UNMATCHED_GENUINE", cls, ok)
	}
}

// An observer outage inside a retention window produces one shape in the
// record: a HEALTHY reading early, then gaps — or, past the prober's backfill
// horizon, no rows at all after the healthy one. Reading either as a kept
// promise credits an operator for hours nobody watched, and raises the serve
// rate exactly while this observer is blind. Both count neither way: the
// shard was there when we looked, and we did not look at the end.
func TestObligationServedNeedsAReadingAtTheEndOfTheWindow(t *testing.T) {
	settled := time.Date(2026, 9, 18, 12, 0, 0, 0, time.UTC)
	msu := settled.Add(4 * time.Hour)
	w := Window{All: true, End: msu.Add(time.Hour)}
	set := map[string]time.Time{}
	for _, v := range []string{"whole", "gaps", "silent", "lastgap"} {
		set["p-"+v] = settled
	}

	// the prober's in-window fractions
	at := func(f float64) time.Time {
		return settled.Add(time.Duration(float64(msu.Sub(settled)) * f))
	}
	// each validator's obligation is its own blob, read at the same
	// fractions of its window: a missed point of one says nothing of another
	obl := func(v string, f float64, cls probe.Classification) Row {
		return Row{PromiseHash: "p-" + v, Validator: v, ScheduleLabel: "w", ScheduledAt: at(f), StartedAt: at(f),
			MustServeUntil: msu, Assigned: true, Attested: true, Phase: probe.PhaseInWindow,
			Classification: cls, TLSOK: cls != probe.ClassNotProbed, CommitmentVerified: cls == probe.ClassHealthy}
	}
	H, G := probe.ClassHealthy, probe.ClassNotProbed
	rows := []Row{
		// answered at every point, last one included: vouched for
		obl("whole", 0.12, H), obl("whole", 0.45, H), obl("whole", 0.72, H), obl("whole", 0.92, H),
		// healthy early, then the observer went down and backfilled gaps
		obl("gaps", 0.12, H), obl("gaps", 0.45, G), obl("gaps", 0.72, G), obl("gaps", 0.92, G),
		// healthy early and nothing after: the outage outran the backfill
		// horizon, so the later slots were never written at all
		obl("silent", 0.12, H),
		// only the last point was missed
		obl("lastgap", 0.12, H), obl("lastgap", 0.45, H), obl("lastgap", 0.72, H), obl("lastgap", 0.92, G),
	}
	_, by := ComputeObligations(rows, set, w, nil)
	for _, c := range []struct {
		validator string
		want      Obligations
	}{
		{"whole", Obligations{Total: 1, Served: 1}},
		{"gaps", Obligations{Total: 1, NotCounted: 1}},
		{"silent", Obligations{Total: 1, NotCounted: 1}},
		{"lastgap", Obligations{Total: 1, NotCounted: 1}},
	} {
		if got := by[c.validator]; got != c.want {
			t.Errorf("%s: %+v, want %+v", c.validator, got, c.want)
		}
	}
	// The rate speaks for one obligation, not four. Nothing here is a fault:
	// this observer's blindness can withhold credit, never accuse.
	net, _ := ComputeObligations(rows, set, w, nil)
	if net.Broken != 0 {
		t.Errorf("broken = %d, want 0: a missing reading is not a fault", net.Broken)
	}
	if n, d := net.Served, net.Served+net.Broken; n != 1 || d != 1 {
		t.Errorf("rate = %d/%d, want 1/1 over four obligations", n, d)
	}
}

// EndSegment cuts the window at the same place the SQL does, and only ever
// inside it.
func TestEndSegmentIsTheTailOfTheWindow(t *testing.T) {
	settled := time.Date(2026, 9, 18, 12, 0, 0, 0, time.UTC)
	msu := settled.Add(4 * time.Hour)
	got := EndSegment(settled, msu)
	if want := settled.Add(3 * time.Hour); !got.Equal(want) {
		t.Errorf("EndSegment = %v, want %v (the last quarter of a four-hour window)", got, want)
	}
	// the published schedule's last in-window point clears it
	last := settled.Add(time.Duration(float64(msu.Sub(settled)) * 0.92))
	if last.Before(got) {
		t.Errorf("the schedule's last in-window point (%v) falls before the end segment (%v), so nothing would ever be served", last, got)
	}
}
