package store

import (
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/probe"
	"github.com/plsgiveup/fibre/fibre-sentinel/internal/scan"
)

// TestMigrationsAreCompleteAndOrdered guards the two mistakes the numbered
// scheme makes easy: a migration added without bumping SchemaVersion, and a
// version number reused or skipped.
func TestMigrationsAreCompleteAndOrdered(t *testing.T) {
	prev := 1
	for _, m := range migrations {
		if m.version != prev+1 {
			t.Fatalf("migration %d follows %d: versions must be consecutive and ascending", m.version, prev)
		}
		if len(m.stmts) == 0 {
			t.Fatalf("migration %d has no statements", m.version)
		}
		if m.note == "" {
			t.Fatalf("migration %d has no note", m.version)
		}
		prev = m.version
	}
	if prev != SchemaVersion {
		t.Fatalf("migrations reach version %d but SchemaVersion is %d", prev, SchemaVersion)
	}
}

// TestFreshDatabaseRecordsEveryVersion: a new database must travel the same
// path as an upgraded one, so every version from 1 up is recorded.
func TestFreshDatabaseRecordsEveryVersion(t *testing.T) {
	st, err := Open(filepath.Join(t.TempDir(), "o.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	for v := 1; v <= SchemaVersion; v++ {
		var n int
		if err := st.db.QueryRow(`SELECT COUNT(*) FROM schema_migrations WHERE version = ?`, v).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n != 1 {
			t.Fatalf("version %d recorded %d times, want 1", v, n)
		}
	}
}

// TestUpgradeFromBaseline builds a version-1 database by hand, writes a row
// into it, then opens it with the current code. The migration must run, and
// the pre-existing row must come back with attestation NULL: that row was
// written without attestation evidence, and "unknown" is the only honest
// value. A 0 there would publish "this validator did not attest".
func TestUpgradeFromBaseline(t *testing.T) {
	path := filepath.Join(t.TempDir(), "old.db")
	dsn := "file:" + path + "?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)"

	old, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatal(err)
	}
	for _, stmt := range splitSQL(schemaSQL) {
		if _, err := old.Exec(stmt); err != nil {
			t.Fatalf("baseline: %v\n%s", err, stmt)
		}
	}
	if _, err := old.Exec(`INSERT INTO schema_migrations (version, applied_at) VALUES (1, ?)`, TS(time.Now())); err != nil {
		t.Fatal(err)
	}
	if _, err := old.Exec(`INSERT INTO assignments (promise_hash, validator_address, voting_power, row_count, rows_json)
		VALUES ('aa', 'bb', 10, 5, '[1,2,3,4,5]')`); err != nil {
		t.Fatal(err)
	}
	old.Close()

	st, err := Open(path)
	if err != nil {
		t.Fatalf("open after baseline: %v", err)
	}
	defer st.Close()

	var highest int
	if err := st.db.QueryRow(`SELECT MAX(version) FROM schema_migrations`).Scan(&highest); err != nil {
		t.Fatal(err)
	}
	if highest != SchemaVersion {
		t.Fatalf("schema version %d after migrate, want %d", highest, SchemaVersion)
	}
	var att sql.NullInt64
	if err := st.db.QueryRow(`SELECT attested FROM assignments WHERE promise_hash = 'aa'`).Scan(&att); err != nil {
		t.Fatal(err)
	}
	if att.Valid {
		t.Fatalf("pre-migration row got attested=%d; a row written without attestation evidence must stay NULL", att.Int64)
	}

	// Idempotent: a second open must not re-apply anything.
	st.Close()
	st2, err := Open(path)
	if err != nil {
		t.Fatalf("second open: %v", err)
	}
	defer st2.Close()
	var rows int
	if err := st2.db.QueryRow(`SELECT COUNT(*) FROM schema_migrations`).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != SchemaVersion {
		t.Fatalf("%d migration rows after two opens, want %d", rows, SchemaVersion)
	}
}

// TestAttestationColumnsFollowRecordVersion: a record that predates
// AttestationSchemaVersion stores NULL, a current record stores the value it
// carries. The difference between "not proven" and "not recorded" has to
// survive into the database, because the API reports them differently.
func TestAttestationColumnsFollowRecordVersion(t *testing.T) {
	st, err := Open(filepath.Join(t.TempDir(), "o.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	mk := func(hash string, version int, attested bool) scan.Publication {
		p := scan.Publication{
			SchemaVersion: version,
			PromiseHash:   hash,
			Assignment: scan.AssignmentTable{
				Validators:         []scan.ValidatorAssignment{{Address: "v1", VotingPower: 7, RowCount: 3, Attested: attested}},
				AttestedWithRows:   1,
				SignatureEntries:   4,
				SignaturesVerified: 3,
			},
		}
		if !attested {
			p.Assignment.AttestedWithRows = 0
		}
		return p
	}

	if _, err := st.UpsertPublication(mk("old", 1, false), []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	if _, err := st.UpsertPublication(mk("new", scan.AttestationSchemaVersion, true), []byte(`{}`)); err != nil {
		t.Fatal(err)
	}

	var att sql.NullInt64
	if err := st.db.QueryRow(`SELECT attested FROM assignments WHERE promise_hash='old'`).Scan(&att); err != nil {
		t.Fatal(err)
	}
	if att.Valid {
		t.Fatalf("v1 publication stored attested=%d, want NULL", att.Int64)
	}
	if err := st.db.QueryRow(`SELECT attested FROM assignments WHERE promise_hash='new'`).Scan(&att); err != nil {
		t.Fatal(err)
	}
	if !att.Valid || att.Int64 != 1 {
		t.Fatalf("v2 publication stored attested=%v, want 1", att)
	}
	var withRows, verified sql.NullInt64
	if err := st.db.QueryRow(`SELECT attested_with_rows, signatures_verified FROM publications WHERE promise_hash='old'`).
		Scan(&withRows, &verified); err != nil {
		t.Fatal(err)
	}
	if withRows.Valid || verified.Valid {
		t.Fatalf("v1 publication stored attestation counters %v/%v, want NULL", withRows, verified)
	}
	if err := st.db.QueryRow(`SELECT attested_with_rows, signatures_verified FROM publications WHERE promise_hash='new'`).
		Scan(&withRows, &verified); err != nil {
		t.Fatal(err)
	}
	if withRows.Int64 != 1 || verified.Int64 != 3 {
		t.Fatalf("v2 counters %d/%d, want 1/3", withRows.Int64, verified.Int64)
	}

	// same for probes
	base := probe.Measurement{
		Vantage: "t", PromiseHash: "new", ValidatorAddress: "v1",
		ScheduledAt: time.Unix(0, 0).UTC(), StartedAt: time.Unix(0, 0).UTC(),
		FinishedAt: time.Unix(0, 0).UTC(), MustServeUntil: time.Unix(0, 0).UTC(),
	}
	// The dedupe key is (vantage, promise_hash, validator_address,
	// scheduled_at), so the two rows need different scheduled_at or the
	// second insert is a no-op.
	oldM, newM := base, base
	oldM.SchemaVersion, oldM.ScheduleLabel = 1, "old"
	oldM.ScheduledAt = time.Unix(100, 0).UTC()
	newM.SchemaVersion, newM.ScheduleLabel, newM.Attested = probe.AttestationSchemaVersion, "new", true
	newM.ScheduledAt = time.Unix(200, 0).UTC()
	for _, m := range []probe.Measurement{oldM, newM} {
		if _, err := st.InsertProbe(m, []byte(`{}`)); err != nil {
			t.Fatal(err)
		}
	}
	if err := st.db.QueryRow(`SELECT attested FROM probes WHERE schedule_label='old'`).Scan(&att); err != nil {
		t.Fatal(err)
	}
	if att.Valid {
		t.Fatalf("v1 measurement stored attested=%d, want NULL", att.Int64)
	}
	if err := st.db.QueryRow(`SELECT attested FROM probes WHERE schedule_label='new'`).Scan(&att); err != nil {
		t.Fatal(err)
	}
	if !att.Valid || att.Int64 != 1 {
		t.Fatalf("v2 measurement stored attested=%v, want 1", att)
	}
}

// A database migrated by a newer collector must be refused by an older
// read-only opener, not served with columns it does not know about.
func TestReadOnlyRefusesNewerSchema(t *testing.T) {
	path := filepath.Join(t.TempDir(), "new.db")
	st, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.db.Exec(`INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)`, SchemaVersion+1, "2030-01-01T00:00:00.000000000Z"); err != nil {
		t.Fatal(err)
	}
	st.Close()
	if _, err := OpenReadOnly(path); err == nil {
		t.Fatal("read-only open accepted a database newer than the binary")
	}
}

// TestMigrationsThatRewriteRowsAreCounted: what is derived from the store and
// kept across restarts (the API's day partials) begins again when a
// migration rewrote rows it was computed from, and stays when one only
// added tables, columns or indexes. So a migration whose statements wrote a
// row moves MetaMigrationRewrites, in its own transaction, and no other
// does: not one that only adds, nor a backfill with nothing to rewrite, as
// over a new store, nor one that writes a note in meta (migration 28 did, and
// the count it moved made every API rebuild its partials for nothing).
func TestMigrationsThatRewriteRowsAreCounted(t *testing.T) {
	st, err := Open(filepath.Join(t.TempDir(), "o.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	count := func() string {
		t.Helper()
		v, err := st.Meta(MetaMigrationRewrites)
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
	if v := count(); v != "" {
		t.Fatalf("a new store, whose backfills had nothing to rewrite, counts %q", v)
	}
	for _, c := range []struct {
		m    migration
		want string
	}{
		{migration{version: 901, note: "adds", stmts: []string{`CREATE TABLE t_added (x INTEGER)`, `CREATE INDEX t_added_x ON t_added (x)`}}, ""},
		{migration{version: 902, note: "a backfill over nothing", stmts: []string{`UPDATE t_added SET x = 1`}}, ""},
		{migration{version: 903, note: "rows", stmts: []string{`INSERT INTO t_added (x) VALUES (1), (2)`}}, "1"},
		{migration{version: 904, note: "a backfill", stmts: []string{`ALTER TABLE t_added ADD COLUMN y INTEGER`, `UPDATE t_added SET y = x`}}, "2"},
		{migration{version: 905, note: "a column", stmts: []string{`ALTER TABLE t_added ADD COLUMN z INTEGER`}}, "2"},
		{migration{version: 906, note: "a meta note", stmts: []string{
			`INSERT INTO meta (key, value, updated_at) SELECT 'note_906', MAX(x), '2026-10-07T00:00:00.000000000Z' FROM t_added WHERE 1
			 ON CONFLICT(key) DO NOTHING`,
			`INSERT OR REPLACE INTO meta (key, value, updated_at) VALUES ('note_906b', '1', '2026-10-07T00:00:00.000000000Z')`,
			`UPDATE meta SET value = '2' WHERE key = 'note_906b'`,
		}}, "2"},
		{migration{version: 907, note: "a meta note beside a backfill", stmts: []string{
			`INSERT INTO meta (key, value, updated_at) VALUES ('note_907', '1', '2026-10-07T00:00:00.000000000Z')`,
			`UPDATE t_added SET z = x`,
		}}, "3"},
	} {
		if err := st.applyMigration(c.m); err != nil {
			t.Fatal(err)
		}
		if v := count(); v != c.want {
			t.Errorf("after migration %d (%s): %q, want %q", c.m.version, c.m.note, v, c.want)
		}
	}
}
