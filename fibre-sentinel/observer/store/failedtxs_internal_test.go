package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/failedtx"
	"github.com/plsgiveup/fibre/fibre-sentinel/internal/probe"
	"github.com/plsgiveup/fibre/fibre-sentinel/internal/scan"
)

// failedRecord is a failed [MsgSend, MsgDepositToEscrow] whose deposit
// failed, as sentinel-scan writes it.
func failedRecord(height int64, txIndex int, hash string) failedtx.Record {
	at := time.Date(2026, 10, 8, 9, 0, 0, 123456789, time.UTC)
	return failedtx.Record{SchemaVersion: failedtx.SchemaVersion, DedupeKey: failedtx.Key(height, txIndex), Height: height, Time: at,
		AppVersion: 10, TxHash: hash, TxIndex: txIndex, Code: 5, Codespace: "sdk",
		Log:       "failed to execute message; message index: 1: failed to transfer funds to escrow: spendable balance 10utia is smaller than 1000000utia: insufficient funds",
		GasWanted: 200000, GasUsed: 91234, AntePassed: true, Fee: "2000utia",
		Messages: []failedtx.Msg{{Index: 0, TypeURL: "/cosmos.bank.v1beta1.MsgSend"},
			{Index: 1, TypeURL: "/celestia.fibre.v1.MsgDepositToEscrow", Signer: "celestia1pub", Detail: &failedtx.MsgDetail{Publisher: "celestia1pub", Amount: "1000000utia"}}},
		RecordedAt: at.Add(2 * time.Second)}
}

func failedLine(t *testing.T, r failedtx.Record) []byte {
	t.Helper()
	b, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// rowsOf is the store's rows of the tables a failed transaction must never
// reach.
func rowsOf(t *testing.T, st *Store) string {
	t.Helper()
	var parts []string
	for _, table := range []string{"publications", "assignments", "probes", "payments", "host_events"} {
		var n int64
		if err := st.db.QueryRow(`SELECT COUNT(*) FROM ` + table).Scan(&n); err != nil {
			t.Fatal(err)
		}
		parts = append(parts, table+"="+strconv.FormatInt(n, 10))
	}
	return strings.Join(parts, " ")
}

// Migration 30 is the failed transactions' table, wherever it stands in the
// list: the consecutive and upgrade tests hold its place like every other.
func TestMigration30IsTheFailedTxsTable(t *testing.T) {
	for _, m := range migrations {
		if m.version != 30 {
			continue
		}
		if m.note != failedTxsMigration.note || len(m.stmts) != len(failedTxsMigration.stmts) {
			t.Fatalf("migration 30 is %s", m.note)
		}
		return
	}
	t.Fatal("no migration 30")
}

// Migration 30 only adds a table and its index: no statement writes a row,
// and there is no check or backfill, so it can never count as a rewrite
// (MetaMigrationRewrites) and the API's day partials are kept across it.
func TestMigration30IsPureDDL(t *testing.T) {
	writes := regexp.MustCompile(`(?i)\b(INSERT|UPDATE|DELETE|REPLACE|WITH)\b`)
	for _, stmt := range failedTxsMigration.stmts {
		if writes.MatchString(stmt) || reRewrite.MatchString(stmt) {
			t.Errorf("migration 30 writes rows:\n%s", stmt)
		}
		if !regexp.MustCompile(`(?i)^\s*CREATE\s+(TABLE|INDEX)\s+IF\s+NOT\s+EXISTS\b`).MatchString(stmt) {
			t.Errorf("migration 30 has a statement that is not a CREATE ... IF NOT EXISTS:\n%s", stmt)
		}
	}
	if failedTxsMigration.check != nil || failedTxsMigration.backfill != nil {
		t.Error("migration 30 has a check or a backfill")
	}
}

// storeWithRows is a store at version 29, as the build before failed_txs
// left it, holding a publication with its assignment, a reading, a payment
// and a host event, with migration_rewrites at 2 as a store that took two
// backfills holds it.
func storeWithRows(t *testing.T, path string) *Store {
	t.Helper()
	return storeWithRowsAt(t, path, 29)
}

// storeWithRowsAt is storeWithRows at version.
func storeWithRowsAt(t *testing.T, path string, version int) *Store {
	t.Helper()
	old := openAt(t, path, version)
	p := ambiguousPublication(0x31, false)
	p.Assignment.Validators = []scan.ValidatorAssignment{{Address: "v1", VotingPower: 7, RowCount: 2, Rows: []int{0, 1}}}
	line, _ := json.Marshal(p)
	if _, err := old.UpsertPublication(p, line); err != nil {
		t.Fatal(err)
	}
	m := probe.Measurement{SchemaVersion: 2, Vantage: "v1", PromiseHash: p.PromiseHash, ValidatorAddress: "v1", ScheduleLabel: "full",
		ScheduledAt: p.MustServeUntil.Add(-10 * time.Minute), StartedAt: p.MustServeUntil.Add(-10 * time.Minute), MustServeUntil: p.MustServeUntil}
	ml, _ := json.Marshal(m)
	if _, err := old.InsertProbe(m, ml); err != nil {
		t.Fatal(err)
	}
	pay := scan.Payment{SchemaVersion: 1, DedupeKey: "aa:0", Kind: scan.PaymentDeposit, Height: 900, Time: p.SettlementTime, TxHash: "aa",
		Publisher: "celestia1pub", Denom: "utia", AmountUtia: 1000000, RecordedAt: p.RecordedAt}
	pl, _ := json.Marshal(pay)
	if _, err := old.UpsertPayment(pay, pl); err != nil {
		t.Fatal(err)
	}
	if _, err := old.ReplayHostEvent(scan.HostEvent{HostEntry: scan.HostEntry{FromHeight: 901, FromTxIndex: 2, ConsAddress: "ab", Host: "h:7980", Source: "event"},
		Time: p.SettlementTime}); err != nil {
		t.Fatal(err)
	}
	if err := old.SetMeta(MetaMigrationRewrites, "2", p.RecordedAt); err != nil {
		t.Fatal(err)
	}
	return old
}

// On a store with rows, migration 30 adds the table and its index, moves
// the schema version and nothing else: migration_rewrites and every row are
// as they were. (Open takes the store on to SchemaVersion; the migrations
// after 30 hold the same.)
func TestMigration30OnAStoreWithRows(t *testing.T) {
	path := filepath.Join(t.TempDir(), "observer.db")
	old := storeWithRows(t, path)
	before := rowsOf(t, old)
	rewritesBefore, _ := old.Meta(MetaMigrationRewrites)
	old.Close()

	st, err := Open(path)
	if err != nil {
		t.Fatalf("migrating a store at 29: %v", err)
	}
	defer st.Close()
	var version int
	if err := st.db.QueryRow(`SELECT MAX(version) FROM schema_migrations`).Scan(&version); err != nil || version != SchemaVersion {
		t.Fatalf("schema %d after the migration: %v", version, err)
	}
	if after, _ := st.Meta(MetaMigrationRewrites); after != rewritesBefore || after != "2" {
		t.Errorf("migration_rewrites %q before migration 30, %q after", rewritesBefore, after)
	}
	if after := rowsOf(t, st); after != before {
		t.Errorf("rows %s before migration 30, %s after", before, after)
	}
	failedTxsTableExists(t, st.db)
}

func failedTxsTableExists(t *testing.T, db *sql.DB) {
	t.Helper()
	for _, c := range []struct{ kind, name string }{{"table", "failed_txs"}, {"index", "failed_txs_tx"}} {
		var n int
		if err := db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type = ? AND name = ?`, c.kind, c.name).Scan(&n); err != nil || n != 1 {
			t.Errorf("%s %s: %d, %v", c.kind, c.name, n, err)
		}
	}
}

// The way back is the one-row delete: a store whose version-30 row is
// deleted is at 29 for every opener (the read-only one refuses it for a
// binary at 30), keeps its table, index and rows, and migration 30 runs
// again over them without error (and every migration after it, up to
// SchemaVersion).
func TestMigration30RunsAgainAfterTheOneRowDelete(t *testing.T) {
	path := filepath.Join(t.TempDir(), "observer.db")
	st, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	r := failedRecord(1300123, 3, strings.Repeat("ab", 32))
	if ok, err := st.InsertFailedTx(r, failedLine(t, r)); err != nil || !ok {
		t.Fatalf("insert: %v %v", ok, err)
	}
	if _, err := st.db.Exec(`DELETE FROM schema_migrations WHERE version > 29`); err != nil {
		t.Fatal(err)
	}
	st.Close()
	if ro, err := OpenReadOnly(path); err == nil {
		ro.Close()
		t.Fatal("the read-only open accepted a store at 29")
	}

	st, err = Open(path)
	if err != nil {
		t.Fatalf("migration 30 again over its own table: %v", err)
	}
	defer st.Close()
	var version, rows int
	if err := st.db.QueryRow(`SELECT MAX(version) FROM schema_migrations`).Scan(&version); err != nil || version != SchemaVersion {
		t.Fatalf("schema %d after the second migration: %v", version, err)
	}
	if err := st.db.QueryRow(`SELECT COUNT(*) FROM failed_txs`).Scan(&rows); err != nil || rows != 1 {
		t.Fatalf("%d failed tx rows after the second migration: %v", rows, err)
	}
	failedTxsTableExists(t, st.db)
	ro, err := OpenReadOnly(path)
	if err != nil {
		t.Fatalf("the read-only open after the second migration: %v", err)
	}
	ro.Close()
}

// A line is stored verbatim under its key, once: the same key again (a
// re-scan) is left alone, whatever its line says. The hash is kept in lower
// case, as the lookup asks for it, and the row's other columns are the
// record's.
func TestInsertFailedTxKeepsTheFirstLineVerbatim(t *testing.T) {
	st, err := Open(filepath.Join(t.TempDir(), "observer.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	hash := strings.Repeat("5F0C", 16)
	r := failedRecord(1300123, 3, hash)
	// a leading space, which encoding/json never writes: kept as it is
	raw := append([]byte(" "), failedLine(t, r)...)
	if ok, err := st.InsertFailedTx(r, raw); err != nil || !ok {
		t.Fatalf("first insert: %v %v", ok, err)
	}
	again := r
	again.Log, again.RecordedAt = "another log", r.RecordedAt.Add(time.Hour)
	if ok, err := st.InsertFailedTx(again, failedLine(t, again)); err != nil || ok {
		t.Fatalf("the same key again: inserted %v, %v", ok, err)
	}
	var gotHash, gotTime, gotSpace, gotRaw string
	var height, txIndex, code int64
	if err := st.db.QueryRow(`SELECT tx_hash, height, tx_index, time, code, codespace, raw_json FROM failed_txs WHERE dedupe_key = ?`, r.DedupeKey).
		Scan(&gotHash, &height, &txIndex, &gotTime, &code, &gotSpace, &gotRaw); err != nil {
		t.Fatal(err)
	}
	if gotHash != strings.ToLower(hash) || height != r.Height || txIndex != int64(r.TxIndex) || gotTime != TS(r.Time) || code != 5 || gotSpace != "sdk" {
		t.Errorf("row: %s %d %d %s %d %s", gotHash, height, txIndex, gotTime, code, gotSpace)
	}
	if gotRaw != string(raw) {
		t.Errorf("raw_json is not the first line verbatim:\n%s\n%s", gotRaw, raw)
	}
	var n int
	if err := st.db.QueryRow(`SELECT COUNT(*) FROM failed_txs`).Scan(&n); err != nil || n != 1 {
		t.Errorf("%d rows, want 1: %v", n, err)
	}
	// another inclusion of the same transaction is a row of its own
	later := failedRecord(1300200, 0, hash)
	if ok, err := st.InsertFailedTx(later, failedLine(t, later)); err != nil || !ok {
		t.Fatalf("another inclusion: %v %v", ok, err)
	}
	for _, bad := range []failedtx.Record{{TxHash: hash}, {DedupeKey: "h1:0"}} {
		if ok, err := st.InsertFailedTx(bad, []byte(`{}`)); err == nil || ok {
			t.Errorf("a record without key or hash stored: %+v", bad)
		}
	}
}

// A failed transaction is in no count: Count and its running totals are the
// same before and after rows are stored, and so is a full recount.
func TestFailedTxsAreInNoCount(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "observer.db")
	old := storeWithRows(t, path)
	old.Close()
	st, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	c0, err := st.Count(ctx)
	if err != nil {
		t.Fatal(err)
	}
	g0, err := st.growingCounts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if c0.Publications == 0 || c0.Assignments == 0 || c0.Probes == 0 {
		t.Fatalf("the fixture counts nothing: %+v", c0)
	}
	rows0 := rowsOf(t, st)
	for i := 0; i < 5; i++ {
		r := failedRecord(1300123+int64(i), i, strings.Repeat("cd", 32))
		if _, err := st.InsertFailedTx(r, failedLine(t, r)); err != nil {
			t.Fatal(err)
		}
	}
	c1, err := st.Count(ctx)
	if err != nil {
		t.Fatal(err)
	}
	g1, err := st.growingCounts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if c1 != c0 || g1 != g0 {
		t.Errorf("counts moved with failed transactions: %+v %v, then %+v %v", c0, g0, c1, g1)
	}
	st.counts.mu.Lock()
	st.counts.ok = false
	st.counts.mu.Unlock()
	if c2, err := st.Count(ctx); err != nil || c2 != c0 {
		t.Errorf("a full recount: %+v (%v), want %+v", c2, err, c0)
	}
	if rows1 := rowsOf(t, st); rows1 != rows0 {
		t.Errorf("rows %s, then %s", rows0, rows1)
	}
}
