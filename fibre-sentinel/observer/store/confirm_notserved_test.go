package store_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/probe"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/verdict"
)

// A not-served row counts only once the second location confirms it: read
// under the client's rules, started after the reading and before
// must_serve_until on the chain's clock, and no rows for a reason that is
// the validator's. An answer that got the rows only names who fetched them.
// Nothing is withdrawn or rewritten: the row keeps the class it was read
// with.
func TestANotServedRowIsConfirmedOnlyByAnAnswerUnderTheClientRules(t *testing.T) {
	st := open(t)
	now := time.Now().UTC().Truncate(time.Second)
	at := now.Add(-15 * time.Minute)
	msu := at.Add(10 * time.Minute)
	for _, c := range []struct {
		addr     string
		cls      probe.Classification
		out      probe.Outcome
		answer   probe.Classification
		after    time.Duration
		offsetMS int64 // the answer's clock offset
		rules    bool
		want     verdict.ConfirmResult
	}{
		{"confirmed", probe.ClassUnreachable, probe.OutcomeRPCTimeout, probe.ClassUnreachable, 3 * time.Minute, 0, true, verdict.ConfirmConfirmed},
		{"throttled", probe.ClassThrottled, probe.OutcomeThrottled, probe.ClassFault, 3 * time.Minute, 0, true, verdict.ConfirmConfirmed},
		{"fetched", probe.ClassUnreachable, probe.OutcomeRPCTimeout, probe.ClassHealthy, 3 * time.Minute, 0, true, verdict.ConfirmServed},
		{"oldbuild", probe.ClassFault, probe.OutcomeNotFound, probe.ClassFault, 3 * time.Minute, 0, false, verdict.ConfirmNone},
		{"afterdeadline", probe.ClassFault, probe.OutcomeNotFound, probe.ClassFault, 11 * time.Minute, 0, true, verdict.ConfirmNone},
		// its clock two minutes behind: a minute before the reading by the
		// clocks, a minute after it on the chain's
		{"clockbehind", probe.ClassFault, probe.OutcomeNotFound, probe.ClassFault, -time.Minute, -2 * 60 * 1000, true, verdict.ConfirmConfirmed},
		{"itsgap", probe.ClassFault, probe.OutcomeNotFound, probe.ClassProbeError, 3 * time.Minute, 0, true, verdict.ConfirmNone},
	} {
		m := probe.Measurement{SchemaVersion: probe.MeasurementSchemaVersion, Vantage: "ut-1", PromiseHash: "p-" + c.addr,
			ValidatorAddress: c.addr, ValidatorHost: c.addr + ":7980", Assigned: true, Attested: true, MustServeUntil: msu,
			ScheduleLabel: probe.EndReadLabel, ScheduledAt: at, StartedAt: at, FinishedAt: at.Add(15 * time.Second),
			Phase: probe.PhaseInWindow, Outcome: c.out, Classification: c.cls, ClientRules: true}
		raw, _ := json.Marshal(m)
		if _, err := st.InsertProbe(m, raw); err != nil {
			t.Fatal(err)
		}
		conf := m
		conf.Vantage, conf.StartedAt, conf.Classification, conf.ClockOffsetMS, conf.ClientRules = "de-1", at.Add(c.after), c.answer, c.offsetMS, c.rules
		conf.Outcome = probe.OutcomeNotFound
		if c.answer == probe.ClassHealthy {
			conf.Outcome, conf.Download.CommitmentVerified = probe.OutcomeServedOK, true
		}
		raw, _ = json.Marshal(conf)
		if _, err := st.InsertConfirmation(conf, raw, "ut-1"); err != nil {
			t.Fatal(err)
		}
		ds, err := st.JudgeConfirmations(context.Background(), now)
		if err != nil {
			t.Fatal(err)
		}
		if len(ds) != 1 || ds[0].Result != c.want {
			t.Fatalf("%s: decisions %+v, want one %d", c.addr, ds, c.want)
		}
		if err := st.SettleConfirmation(ds[0]); err != nil {
			t.Fatal(err)
		}
		var cls, cleared, confirmed string
		var amended bool
		if err := st.DB().QueryRow(`SELECT classification, COALESCE(cleared_by, ''), COALESCE(confirmed_by, ''), amended_at IS NOT NULL
			FROM probes WHERE promise_hash = ?`, m.PromiseHash).Scan(&cls, &cleared, &confirmed, &amended); err != nil {
			t.Fatal(err)
		}
		wantConfirmed, wantCleared := "", ""
		switch c.want {
		case verdict.ConfirmConfirmed:
			wantConfirmed = "de-1"
		case verdict.ConfirmServed:
			wantCleared = "de-1"
		}
		if cls != string(c.cls) || amended || confirmed != wantConfirmed || cleared != wantCleared {
			t.Errorf("%s: class %s amended %v confirmed_by %q cleared_by %q; want %s, unamended, %q, %q",
				c.addr, cls, amended, confirmed, cleared, c.cls, wantConfirmed, wantCleared)
		}
	}
}
