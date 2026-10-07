package store

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/probe"
	"github.com/plsgiveup/fibre/fibre-sentinel/internal/scan"
)

// openAt opens the store at path the way Open does, but migrates it only up to
// version: a store as an older build left it.
func openAt(t *testing.T, path string, version int) *Store {
	t.Helper()
	db, err := sql.Open("sqlite", "file:"+path+"?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=foreign_keys(ON)")
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	s := &Store{db: db}
	for _, stmt := range splitSQL(schemaSQL) {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.Exec(`INSERT OR IGNORE INTO schema_migrations (version, applied_at) VALUES (1, ?)`, ts(time.Now())); err != nil {
		t.Fatal(err)
	}
	for _, m := range migrations {
		if m.version > version {
			break
		}
		if err := s.applyMigration(m); err != nil {
			t.Fatal(err)
		}
	}
	return s
}

// ambiguousPublication is a publication record whose deadline the scanner
// marked ambiguous, or not.
func ambiguousPublication(hashByte byte, ambiguous bool) scan.Publication {
	created := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	p := scan.Publication{SchemaVersion: 3, PromiseHash: hex.EncodeToString(bytes.Repeat([]byte{hashByte}, 32)),
		SettlementHeight: 1000 + int64(hashByte), SettlementTime: created.Add(9 * time.Second),
		Promise:        scan.PromiseFields{ChainID: "test-1", Height: 999, Commitment: hex.EncodeToString(bytes.Repeat([]byte{hashByte ^ 0xff}, 32)), CreationTimestamp: created},
		MustServeUntil: created.Add(4 * time.Hour), MustServeUntilBasis: "creation_timestamp + max(payment_promise_timeout=1h0m0s, shard_retention=4h0m0s) = creation + 4h0m0s",
		RecordedAt: created.Add(10 * time.Second)}
	if ambiguous {
		p.MustServeUntilAmbiguous = true
		p.MustServeUntilBasis += "; AMBIGUOUS: fibre params changed between promise height 999 and settlement height 1010; the server uses the params at upload time"
	}
	return p
}

// Migration 29 on a store at 28 with rows: the sampling index, the empty
// confirmations table and the index on cleared_by go, the columns stay; a
// publication whose record is ambiguous reads so, whichever form its row
// keeps the record in, and whether or not a correction moved its basis; the
// other rows are as they were.
func TestMigration29OnAStoreWithRows(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "observer.db")
	old := openAt(t, path, 28)
	pubs := []scan.Publication{
		ambiguousPublication(0x01, true),  // slim
		ambiguousPublication(0x02, true),  // its line, as an earlier build kept it
		ambiguousPublication(0x03, true),  // corrected: its basis moved
		ambiguousPublication(0x04, false), // not ambiguous
		ambiguousPublication(0x05, true),  // its record stripped by an earlier build's retention: the mark stands
	}
	for _, p := range pubs {
		line, _ := json.Marshal(p)
		if _, err := old.UpsertPublication(p, line); err != nil {
			t.Fatal(err)
		}
		m := probe.Measurement{SchemaVersion: 2, Vantage: "v1", PromiseHash: p.PromiseHash, ValidatorAddress: "aa", ScheduleLabel: "full",
			ScheduledAt: p.MustServeUntil.Add(-10 * time.Minute), StartedAt: p.MustServeUntil.Add(-10 * time.Minute), MustServeUntil: p.MustServeUntil}
		if p.PromiseHash == pubs[3].PromiseHash { // a row a sampled-out publication left, for the collapse to look at
			m.Classification, m.ClassificationReason = probe.ClassNotProbed, "budget:p=0.5"
		}
		ml, _ := json.Marshal(m)
		if _, err := old.InsertProbe(m, ml); err != nil {
			t.Fatal(err)
		}
	}
	// as a build before 29 stored them: the column never written on insert
	if _, err := old.db.Exec(`UPDATE publications SET must_serve_until_ambiguous = 0`); err != nil {
		t.Fatal(err)
	}
	line2, _ := json.Marshal(pubs[1])
	if _, err := old.db.Exec(`UPDATE publications SET raw_json = ? WHERE promise_hash = ?`, string(line2), pubs[1].PromiseHash); err != nil {
		t.Fatal(err)
	}
	if _, err := old.db.Exec(`UPDATE publications SET must_serve_until_basis_at_scan = must_serve_until_basis,
		must_serve_until_basis = 'creation + 1h; CORRECTED: recomputed', corrected_at = '2026-10-02T00:00:00.000000000Z' WHERE promise_hash = ?`, pubs[2].PromiseHash); err != nil {
		t.Fatal(err)
	}
	if _, err := old.db.Exec(`UPDATE publications SET raw_json = '' WHERE promise_hash = ?`, pubs[4].PromiseHash); err != nil {
		t.Fatal(err)
	}
	var slimRows int
	if err := old.db.QueryRow(`SELECT COUNT(*) FROM publications WHERE substr(raw_json, 1, 1) <> '{' AND raw_json <> ''`).Scan(&slimRows); err != nil || slimRows != 3 {
		t.Fatalf("%d publications kept slim before the migration, want 3: %v", slimRows, err)
	}
	rewritesBefore, _ := old.Meta(MetaMigrationRewrites)
	var probesBefore int
	if err := old.db.QueryRow(`SELECT COUNT(*) FROM probes`).Scan(&probesBefore); err != nil {
		t.Fatal(err)
	}
	old.Close()

	st, err := Open(path)
	if err != nil {
		t.Fatalf("migrating a store at 28: %v", err)
	}
	defer st.Close()
	objects := func(names ...string) []string {
		var have []string
		for _, n := range names {
			var k int
			if err := st.db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE name = ?`, n).Scan(&k); err != nil {
				t.Fatal(err)
			}
			if k > 0 {
				have = append(have, n)
			}
		}
		return have
	}
	if left := objects("probes_sampling_started", "probes_cleared", "probe_confirmations", "probe_confirmations_open", "probe_confirmations_started"); len(left) > 0 {
		t.Errorf("still in the store: %v", left)
	}
	if kept := objects("sampling_decisions", "sampling_secrets", "probes_window", "probes_started"); len(kept) != 4 {
		t.Errorf("only %v of what stays is there", kept)
	}
	for _, col := range []string{"cleared_by", "confirmed_by", "sampling_commitment"} {
		if _, err := st.db.Exec(`SELECT ` + col + ` FROM probes LIMIT 1`); err != nil {
			t.Errorf("probes.%s: %v", col, err)
		}
	}
	for i, p := range pubs {
		var got int
		if err := st.db.QueryRow(`SELECT must_serve_until_ambiguous FROM publications WHERE promise_hash = ?`, p.PromiseHash).Scan(&got); err != nil {
			t.Fatal(err)
		}
		if want := b2i(p.MustServeUntilAmbiguous); got != want {
			t.Errorf("publication %d: must_serve_until_ambiguous %d, its record says %d", i, got, want)
		}
	}
	var probesAfter int
	if err := st.db.QueryRow(`SELECT COUNT(*) FROM probes`).Scan(&probesAfter); err != nil || probesAfter != probesBefore {
		t.Errorf("%d probe rows, %d before: %v", probesAfter, probesBefore, err)
	}
	// the backfill wrote rows of a table derived data reads: counted, once
	if after, _ := st.Meta(MetaMigrationRewrites); after != "1" || rewritesBefore != "" {
		t.Errorf("rewriting migrations %q before, %q after; want one more", rewritesBefore, after)
	}
	// a record kept slim reads back as before
	var raw []byte
	if err := st.db.QueryRow(`SELECT raw_json FROM publications WHERE promise_hash = ?`, pubs[0].PromiseHash).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	line0, _ := json.Marshal(pubs[0])
	if got, err := st.Record(ctx, st.db, raw); err != nil || !bytes.Equal(got, line0) {
		t.Errorf("the slim publication back: %v", err)
	}
	// and the collapse of sampled-out rows runs without the table it no longer reads (the row is no whole set: kept)
	if d, n, err := st.CollapseSampledOut(ctx); err != nil || d != 0 || n != 0 {
		t.Errorf("collapse after the migration: %d decision(s), %d row(s), %v", d, n, err)
	}
}

// Migration 29 drops probe_confirmations only empty: with a row in it the
// store stays at 28, the table and the row where they were, and the error
// says why.
func TestMigration29RefusesAConfirmationsTableWithRows(t *testing.T) {
	path := filepath.Join(t.TempDir(), "observer.db")
	old := openAt(t, path, 28)
	if _, err := old.db.Exec(`INSERT INTO probe_confirmations (dedupe_key, probe_key, vantage, promise_hash, validator_address, validator_host,
		scheduled_at, started_at, phase, outcome, classification, classification_reason, rows_returned, rows_expected,
		commitment_verified, assignment_verified, raw_error, raw_json)
		VALUES ('k', 'pk', 'v2', 'p', 'a', 'h', 's', 's', 'IN_WINDOW', 'SERVED_OK', 'HEALTHY', '', 1, 1, 1, 1, '', '{}')`); err != nil {
		t.Fatal(err)
	}
	old.Close()
	st, err := Open(path)
	if err == nil {
		st.Close()
		t.Fatal("migration 29 dropped a confirmations table that holds a row")
	}
	if !strings.Contains(err.Error(), "probe_confirmations holds 1 row") {
		t.Errorf("the refusal says %v", err)
	}
	db, err := sql.Open("sqlite", "file:"+path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var version, rows int
	if err := db.QueryRow(`SELECT MAX(version) FROM schema_migrations`).Scan(&version); err != nil || version != 28 {
		t.Errorf("schema %d after the refusal: %v", version, err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM probe_confirmations`).Scan(&rows); err != nil || rows != 1 {
		t.Errorf("%d confirmation rows after the refusal: %v", rows, err)
	}
	var idx int
	if err := db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE name = 'probes_sampling_started'`).Scan(&idx); err != nil || idx != 1 {
		t.Errorf("the sampling index after the refusal: %d, %v", idx, err)
	}
}

// From 29 on a publication's ambiguity is written as it is stored.
func TestAmbiguityIsWrittenOnInsert(t *testing.T) {
	st, err := Open(filepath.Join(t.TempDir(), "observer.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	for _, p := range []scan.Publication{ambiguousPublication(0x11, true), ambiguousPublication(0x12, false)} {
		line, _ := json.Marshal(p)
		if _, err := st.UpsertPublication(p, line); err != nil {
			t.Fatal(err)
		}
		var got int
		if err := st.db.QueryRow(`SELECT must_serve_until_ambiguous FROM publications WHERE promise_hash = ?`, p.PromiseHash).Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got != b2i(p.MustServeUntilAmbiguous) {
			t.Errorf("ambiguous %v stored as %d", p.MustServeUntilAmbiguous, got)
		}
	}
}

// The backfill's candidates are read from where each row begins, ahead of
// its record: a scan of publications that reads must_serve_until_basis, and
// the corrected ones through their partial index.
func TestTheAmbiguityBackfillReadsNoRecordToFindItsCandidates(t *testing.T) {
	st, err := Open(filepath.Join(t.TempDir(), "observer.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	plan, err := st.QueryPlan(context.Background(), ambiguousCandidatesSQL)
	if err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(plan, "\n") + "\n"
	for _, want := range []string{
		"SEARCH publications USING INDEX sqlite_autoindex_publications_1 (promise_hash=?)\n", // each candidate's row, by its key
		"SCAN publications\n",                               // the marks, in table order
		"publications USING INDEX publications_corrected\n", // the corrected ones' marks, from their partial index
	} {
		if !strings.Contains(joined, want) {
			t.Errorf("the plan has no %q:\n%s", want, joined)
		}
	}
	var cols []string
	rows, err := st.db.Query(`SELECT name FROM pragma_table_info('publications') ORDER BY cid`)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var c string
		if err := rows.Scan(&c); err != nil {
			t.Fatal(err)
		}
		cols = append(cols, c)
	}
	rows.Close()
	at := func(name string) int {
		for i, c := range cols {
			if c == name {
				return i
			}
		}
		t.Fatalf("no column %s", name)
		return -1
	}
	if at("must_serve_until_basis") > at("raw_json") {
		t.Errorf("must_serve_until_basis is stored after raw_json: reading it reads every record")
	}
}
