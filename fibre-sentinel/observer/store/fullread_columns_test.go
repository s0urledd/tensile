package store

import (
	"database/sql"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/probe"
)

// A row of a full reading stores what its rule reads beside the class:
// whether a short answer's rows are all the validator's own, and when its
// validator is owed another attempt. The rows stored before the columns
// existed (the end readings from probe.FullReadSince on, asked once) get
// the first from their raw JSON (migration 25); earlier rows keep 0.
func TestTheFullReadingColumnsAreStoredAndBackfilled(t *testing.T) {
	st, err := Open(filepath.Join(t.TempDir(), "o.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	after := probe.FullReadSince.Add(time.Hour)
	due := after.Add(90 * time.Second)
	row := func(validator string, started time.Time) probe.Measurement {
		m := probe.Measurement{SchemaVersion: probe.MeasurementSchemaVersion, Vantage: "t", PromiseHash: "p1", ValidatorAddress: validator,
			ScheduleLabel: probe.FullReadLabel, ScheduledAt: started, StartedAt: started, FinishedAt: started, MustServeUntil: started.Add(time.Hour),
			Assigned: true, Attested: true, Phase: probe.PhaseInWindow, Outcome: probe.OutcomePartial, Classification: probe.ClassUnmatchedGenuine}
		m.Download.CommitmentVerified, m.Download.RowsSubsetOfAssignment = true, true
		m.NextAttemptDue = &due
		return m
	}
	ms := []probe.Measurement{row("v-after", after), row("v-before", probe.FullReadSince.Add(-time.Hour))}
	for _, m := range ms {
		raw, err := json.Marshal(m)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := st.InsertProbe(m, raw); err != nil {
			t.Fatal(err)
		}
	}
	read := func(v string) (int, sql.NullString) {
		t.Helper()
		var subset int
		var owed sql.NullString
		if err := st.db.QueryRow(`SELECT rows_subset_of_assignment, next_attempt_due FROM probes WHERE validator_address = ?`, v).
			Scan(&subset, &owed); err != nil {
			t.Fatal(err)
		}
		return subset, owed
	}
	for _, v := range []string{"v-after", "v-before"} {
		if subset, owed := read(v); subset != 1 || owed.String != TS(due) {
			t.Fatalf("%s inserted: subset %d, next attempt due %v", v, subset, owed)
		}
	}

	// As a store from before migration 25 holds them: then the migration's
	// backfill.
	if _, err := st.db.Exec(`UPDATE probes SET rows_subset_of_assignment = 0, next_attempt_due = NULL`); err != nil {
		t.Fatal(err)
	}
	var m25 migration
	for _, m := range migrations {
		if m.version == 25 {
			m25 = m
		}
	}
	for _, stmt := range m25.stmts[2:] {
		if _, err := st.db.Exec(stmt); err != nil {
			t.Fatalf("%v\n%s", err, stmt)
		}
	}
	if subset, _ := read("v-after"); subset != 1 {
		t.Fatalf("a full reading's row after the backfill: subset %d", subset)
	}
	if subset, _ := read("v-before"); subset != 0 {
		t.Fatalf("an earlier row after the backfill: subset %d", subset)
	}
}
