package verdict

import (
	"testing"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/probe"
)

// At the end-of-window reading an obligation is what a reader of the chain's
// own client gets: rows that verify against the commitment are served,
// anything that leaves the reader without them is not served (broken), and a
// reading this observer never took decides nothing. The same classes at the
// earlier schedule's points keep their old meaning: no answer there is not a
// fault.
func TestEndReadingObligations(t *testing.T) {
	settled := time.Date(2026, 9, 27, 17, 0, 0, 0, time.UTC)
	msu := settled.Add(4 * time.Hour)
	end := msu.Add(-10 * time.Minute)
	w := Window{All: true, End: msu.Add(time.Hour)}
	set := map[string]time.Time{"p": settled}
	read := func(v, label string, cls probe.Classification) Row {
		return Row{PromiseHash: "p", Validator: v, ScheduleLabel: label, ScheduledAt: end, StartedAt: end,
			MustServeUntil: msu, Assigned: true, Attested: true, Phase: probe.PhaseInWindow,
			Classification: cls, TLSOK: cls == probe.ClassHealthy || cls == probe.ClassFault}
	}
	rows := []Row{
		read("served", probe.EndReadLabel, probe.ClassHealthy),
		read("notfound", probe.EndReadLabel, probe.ClassFault),
		read("silent", probe.EndReadLabel, probe.ClassUnreachable),
		read("wrongcert", probe.EndReadLabel, probe.ClassIdentityMismatch),
		read("expiredcert", probe.EndReadLabel, probe.ClassIdentityExpired),
		read("error", probe.EndReadLabel, probe.ClassServerError),
		read("throttled", probe.EndReadLabel, probe.ClassThrottled),
		read("deregistered", probe.EndReadLabel, probe.ClassNotRegistered),
		read("shadowed", probe.EndReadLabel, probe.ClassShadowedShard),
		read("unmatched", probe.EndReadLabel, probe.ClassUnmatchedGenuine),
		read("ourgap", probe.EndReadLabel, probe.ClassNotProbed),
		read("oldsilent", "w4", probe.ClassUnreachable),
	}
	_, by := ComputeObligations(rows, set, w, nil)
	served := Obligations{Total: 1, Served: 1}
	broken := Obligations{Total: 1, Broken: 1}
	for v, want := range map[string]Obligations{
		"served": served, "shadowed": served, "unmatched": served,
		"notfound": broken, "silent": broken, "wrongcert": broken, "expiredcert": broken,
		"error": broken, "throttled": broken, "deregistered": broken,
		"ourgap":    {Total: 1, Unobserved: 1, UnobservedNotProbed: 1},
		"oldsilent": {Total: 1, Unobserved: 1, UnobservedUnreachable: 1},
	} {
		if got := by[v]; got != want {
			t.Errorf("%s: %+v, want %+v", v, got, want)
		}
	}
}

// A held deadline withholds an end reading's verdict in both directions, as
// it does every other reading's: the no-rows FAULT and the genuine-rows
// HEALTHY alike.
func TestEndReadingHeldDeadline(t *testing.T) {
	for _, c := range []probe.Classification{probe.ClassUnreachable, probe.ClassShadowedShard, probe.ClassFault, probe.ClassHealthy} {
		r := Row{ScheduleLabel: probe.EndReadLabel, Classification: c, Outcome: probe.OutcomeTCPTimeout, RetentionUnverified: true}
		if got := r.ObligationClass(); got != probe.ClassRetentionUnverified {
			t.Errorf("held %s: %s, want RETENTION_UNVERIFIED", c, got)
		}
	}
}
