package store

import (
	"database/sql"
	"encoding/json"
	"path/filepath"
	"strings"
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
	lines := map[string][]byte{}
	for _, m := range ms {
		raw, err := json.Marshal(m)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := st.InsertProbe(m, raw); err != nil {
			t.Fatal(err)
		}
		lines[m.ValidatorAddress] = raw
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

	// As a store from before migration 25 holds them (the record as its line, the columns not yet there): then the
	// migration's backfill.
	if _, err := st.db.Exec(`UPDATE probes SET rows_subset_of_assignment = 0, next_attempt_due = NULL`); err != nil {
		t.Fatal(err)
	}
	for v, l := range lines {
		if _, err := st.db.Exec(`UPDATE probes SET raw_json = ? WHERE validator_address = ?`, string(l), v); err != nil {
			t.Fatal(err)
		}
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

// A database rolled back by deleting its schema_migrations row 25 keeps the
// two columns the migration added. The migration runs again over them when a
// build at version 25 opens it: an ADD COLUMN whose column is there is
// skipped, the backfill runs again, and the version is recorded.
func TestMigration25RunsAgainAfterItsVersionRowIsDeleted(t *testing.T) {
	path := filepath.Join(t.TempDir(), "o.db")
	st, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.db.Exec(`DELETE FROM schema_migrations WHERE version = 25`); err != nil {
		t.Fatal(err)
	}
	st.Close()
	st, err = Open(path)
	if err != nil {
		t.Fatalf("reopen after the version row was deleted: %v", err)
	}
	defer st.Close()
	var v int
	if err := st.db.QueryRow(`SELECT MAX(version) FROM schema_migrations`).Scan(&v); err != nil {
		t.Fatal(err)
	}
	if v != SchemaVersion {
		t.Fatalf("schema version %d after the migration ran again, want %d", v, SchemaVersion)
	}
	if _, err := st.db.Exec(`SELECT rows_subset_of_assignment, next_attempt_due FROM probes`); err != nil {
		t.Fatalf("the columns after the migration ran again: %v", err)
	}
}

// The backfill reads the rows whose flag the rule can read through the
// class index, not every row since FullReadSince.
func TestMigration25BackfillUsesTheClassIndex(t *testing.T) {
	st, err := Open(filepath.Join(t.TempDir(), "o.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	var m25 migration
	for _, m := range migrations {
		if m.version == 25 {
			m25 = m
		}
	}
	rows, err := st.db.Query(`EXPLAIN QUERY PLAN ` + m25.stmts[2])
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var plan []string
	for rows.Next() {
		var id, parent, notused int
		var detail string
		if err := rows.Scan(&id, &parent, &notused, &detail); err != nil {
			t.Fatal(err)
		}
		plan = append(plan, detail)
	}
	if len(plan) == 0 || !strings.Contains(strings.Join(plan, "; "), "probes_class_time") {
		t.Fatalf("the backfill's plan: %q, want a search of probes_class_time", plan)
	}
}
