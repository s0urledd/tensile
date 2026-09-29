package store_test

import (
	"context"
	"testing"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/probe"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/verdict"
)

// Every row that counts not served is re-checked from the second location,
// not only a FAULT: at the end reading a timeout from this observer's one
// path is the likeliest to be its own, and it is withdrawn when the rows
// verify there. A class that is not a not-served row (NOT_REGISTERED names
// no host; an earlier schedule's UNREACHABLE never counted) is judged with
// no effect.
func TestEveryNotServedRowCanBeClearedFromTheSecondLocation(t *testing.T) {
	st := open(t)
	now := time.Now().UTC().Truncate(time.Second)
	at := now.Add(-15 * time.Minute)
	msu := at.Add(10 * time.Minute)
	for _, c := range []struct {
		addr, label string
		cls         probe.Classification
		out         probe.Outcome
		cleared     bool
	}{
		{"timeout", probe.EndReadLabel, probe.ClassUnreachable, probe.OutcomeRPCTimeout, true},
		{"throttled", probe.EndReadLabel, probe.ClassThrottled, probe.OutcomeThrottled, true},
		{"notfound", probe.EndReadLabel, probe.ClassFault, probe.OutcomeNotFound, true},
		{"nohost", probe.EndReadLabel, probe.ClassNotRegistered, probe.OutcomeNoHost, false},
		{"earlier", "w4", probe.ClassUnreachable, probe.OutcomeTCPTimeout, false},
	} {
		m := probe.Measurement{SchemaVersion: probe.MeasurementSchemaVersion, Vantage: "ut-1", PromiseHash: "p-" + c.addr,
			ValidatorAddress: c.addr, ValidatorHost: c.addr + ":7980", Assigned: true, Attested: true, MustServeUntil: msu,
			ScheduleLabel: c.label, ScheduledAt: at, StartedAt: at, FinishedAt: at.Add(15 * time.Second),
			Phase: probe.PhaseInWindow, Outcome: c.out, Classification: c.cls}
		if _, err := st.InsertProbe(m, []byte(`{}`)); err != nil {
			t.Fatal(err)
		}
		conf := m
		conf.Vantage, conf.StartedAt, conf.Outcome, conf.Classification = "de-1", at.Add(3*time.Minute), probe.OutcomeServedOK, probe.ClassHealthy
		if _, err := st.InsertConfirmation(conf, []byte(`{}`), "ut-1"); err != nil {
			t.Fatal(err)
		}
	}
	ds, err := st.JudgeConfirmations(context.Background(), now)
	if err != nil {
		t.Fatal(err)
	}
	if len(ds) != 5 {
		t.Fatalf("%d decisions, want 5", len(ds))
	}
	cleared := map[string]bool{}
	for _, d := range ds {
		if d.Result == verdict.ConfirmCleared {
			if d.Amendment == nil || d.Amendment.To != string(verdict.ClearedClass) {
				t.Fatalf("a cleared row without its amendment: %+v", d)
			}
			cleared[d.Amendment.ValidatorAddress] = true
		}
	}
	for addr, want := range map[string]bool{"timeout": true, "throttled": true, "notfound": true, "nohost": false, "earlier": false} {
		if cleared[addr] != want {
			t.Errorf("%s: cleared %v, want %v", addr, cleared[addr], want)
		}
	}
}
