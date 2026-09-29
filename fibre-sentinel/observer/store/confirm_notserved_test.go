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

// Every stored answer is drawn again under this build's rule at start. An
// older collector confirmed a failure from any answer that was not a
// verified one, with no client-rules check: such a row counts only if this
// rule confirms it too, as sentinel-recompute draws it. A withdrawal of the
// earlier rule keeps its cleared_by; verified rows fewer than the validator
// holds clear nothing; an answer judged since is left as it is.
func TestTheStoredAnswersAreJudgedAgainAtStart(t *testing.T) {
	st := open(t)
	now := time.Now().UTC().Truncate(time.Second)
	at := now.Add(-15 * time.Minute)
	msu := at.Add(10 * time.Minute)
	type answer struct {
		cls      probe.Classification
		rules    bool
		verified bool
		rows     int
	}
	for _, c := range []struct {
		addr               string
		cls                probe.Classification
		out                probe.Outcome
		rows               int // returned at the reading, of 8
		ans                answer
		judged             bool   // judged before this build
		cleared, confirmed string // what the older build left on the row
		withdrawn          bool   // the earlier rule's withdrawal, with its amendment
	}{
		{"oldbuild", probe.ClassFault, probe.OutcomeNotFound, 0, answer{probe.ClassFault, false, false, 0}, true, "", "de-1", false},
		{"confirmed", probe.ClassFault, probe.OutcomeNotFound, 0, answer{probe.ClassFault, true, false, 0}, true, "", "de-1", false},
		{"unjudged", probe.ClassUnreachable, probe.OutcomeRPCTimeout, 0, answer{probe.ClassUnreachable, true, false, 0}, false, "", "", false},
		{"withdrawn", probe.ClassFault, probe.OutcomeNotFound, 0, answer{probe.ClassHealthy, false, true, 8}, true, "de-1", "", true},
		{"shortclear", probe.ClassUnmatchedGenuine, probe.OutcomeWrongRows, 4, answer{probe.ClassUnmatchedGenuine, true, true, 4}, true, "de-1", "", false},
		{"fetched", probe.ClassFault, probe.OutcomeNotFound, 0, answer{probe.ClassHealthy, true, true, 8}, true, "", "", false},
	} {
		m := probe.Measurement{SchemaVersion: probe.MeasurementSchemaVersion, Vantage: "ut-1", PromiseHash: "rj-" + c.addr,
			ValidatorAddress: c.addr, ValidatorHost: c.addr + ":7980", Assigned: true, Attested: true, MustServeUntil: msu,
			AssignedRowCount: 8, ScheduleLabel: probe.EndReadLabel, ScheduledAt: at, StartedAt: at, FinishedAt: at.Add(15 * time.Second),
			Phase: probe.PhaseInWindow, Outcome: c.out, Classification: c.cls, ClientRules: true}
		m.Download.RowsReturned, m.Download.CommitmentVerified = c.rows, c.rows > 0
		raw, _ := json.Marshal(m)
		if _, err := st.InsertProbe(m, raw); err != nil {
			t.Fatal(err)
		}
		conf := m
		conf.Vantage, conf.StartedAt, conf.Classification, conf.ClientRules = "de-1", at.Add(3*time.Minute), c.ans.cls, c.ans.rules
		conf.Outcome = probe.OutcomeNotFound
		conf.Download.RowsReturned, conf.Download.CommitmentVerified = c.ans.rows, c.ans.verified
		conf.Download.AssignmentVerified = c.ans.cls == probe.ClassHealthy
		if c.ans.verified {
			conf.Outcome = probe.OutcomeServedOK
		}
		raw, _ = json.Marshal(conf)
		if _, err := st.InsertConfirmation(conf, raw, "ut-1"); err != nil {
			t.Fatal(err)
		}
		// What an older collector left behind.
		if c.judged {
			if _, err := st.DB().Exec(`UPDATE probe_confirmations SET judged = 1 WHERE promise_hash = ?`, m.PromiseHash); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := st.DB().Exec(`UPDATE probes SET cleared_by = NULLIF(?, ''), confirmed_by = NULLIF(?, '') WHERE promise_hash = ?`,
			c.cleared, c.confirmed, m.PromiseHash); err != nil {
			t.Fatal(err)
		}
		if c.withdrawn {
			if _, err := st.DB().Exec(`UPDATE probes SET classification_at_probe = classification, classification = 'PROBE_ERROR', amended_at = ?
				WHERE promise_hash = ?`, now.Format(time.RFC3339Nano), m.PromiseHash); err != nil {
				t.Fatal(err)
			}
		}
	}
	n, err := st.RejudgeConfirmations(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if n != 4 {
		t.Errorf("%d rows changed, want 4 (oldbuild, unjudged, shortclear, fetched)", n)
	}
	for _, c := range []struct{ addr, cleared, confirmed string }{
		{"oldbuild", "", ""}, {"confirmed", "", "de-1"}, {"unjudged", "", "de-1"},
		{"withdrawn", "de-1", ""}, {"shortclear", "", ""}, {"fetched", "de-1", ""},
	} {
		var cleared, confirmed string
		if err := st.DB().QueryRow(`SELECT COALESCE(cleared_by, ''), COALESCE(confirmed_by, '') FROM probes WHERE promise_hash = ?`,
			"rj-"+c.addr).Scan(&cleared, &confirmed); err != nil {
			t.Fatal(err)
		}
		if cleared != c.cleared || confirmed != c.confirmed {
			t.Errorf("%s: cleared_by %q confirmed_by %q, want %q %q", c.addr, cleared, confirmed, c.cleared, c.confirmed)
		}
	}
	var open int
	st.DB().QueryRow(`SELECT COUNT(*) FROM probe_confirmations WHERE judged = 0`).Scan(&open)
	if open != 0 {
		t.Errorf("%d answers left unjudged", open)
	}
	if ds, err := st.JudgeConfirmations(context.Background(), now); err != nil || len(ds) != 0 {
		t.Errorf("the regular pass judged %d answers again (%v)", len(ds), err)
	}
	if n, err := st.RejudgeConfirmations(context.Background()); err != nil || n != 0 {
		t.Errorf("a second start changed %d rows (%v), want none", n, err)
	}
}
