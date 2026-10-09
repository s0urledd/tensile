package store

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/cosmos/cosmos-sdk/types/bech32"
	"github.com/plsgiveup/fibre/fibre-sentinel/internal/failedtx"
	"github.com/plsgiveup/fibre/fibre-sentinel/internal/txcost"
)

const (
	tcURLSend    = "/cosmos.bank.v1beta1.MsgSend"
	tcURLPFF     = "/celestia.fibre.v1.MsgPayForFibre"
	tcURLDeposit = "/celestia.fibre.v1.MsgDepositToEscrow"
	tcURLTimeout = "/celestia.fibre.v1.MsgPaymentPromiseTimeout"
	tcURLSetHost = "/celestia.valaddr.v1.MsgSetFibreProviderInfo"
)

// tcValoper is an operator address of seed's 20 bytes.
func tcValoper(t *testing.T, seed byte) string {
	t.Helper()
	s, err := bech32.ConvertAndEncode("celestiavaloper", bytes.Repeat([]byte{seed}, 20))
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// tcMixed is a final failure of [MsgSend, MsgDepositToEscrow,
// MsgExec[MsgSetFibreProviderInfo], MsgSetFibreProviderInfo,
// MsgPaymentPromiseTimeout], the set-hosts signed in the operator form, the
// only form the chain makes final.
func tcMixed(t *testing.T, height int64, txIndex int, hash string) failedtx.Record {
	t.Helper()
	r := failedRecord(height, txIndex, hash)
	r.Messages = []failedtx.Msg{
		{Index: 0, TypeURL: tcURLSend},
		{Index: 1, TypeURL: tcURLDeposit, Signer: "celestia1pub", Detail: &failedtx.MsgDetail{Publisher: "celestia1pub", Amount: "1000000utia"}},
		{Index: 2, TypeURL: failedtx.ExecTypeURL, Inner: []failedtx.Msg{
			{Index: 0, TypeURL: tcURLSetHost, Signer: tcValoper(t, 'b'), Detail: &failedtx.MsgDetail{Host: "b.example:7980"}}}},
		{Index: 3, TypeURL: tcURLSetHost, Signer: tcValoper(t, 'a'), Detail: &failedtx.MsgDetail{Host: "a.example:7980"}},
		{Index: 4, TypeURL: tcURLTimeout, Signer: "celestia1anyone", Detail: &failedtx.MsgDetail{Publisher: "celestia1owner", PromiseHash: strings.Repeat("ee", 32)}},
	}
	return r
}

// tcPFF is a final failed MsgPayForFibre.
func tcPFF(height int64, txIndex int, hash string) failedtx.Record {
	r := failedRecord(height, txIndex, hash)
	r.Code, r.Codespace, r.Log = 11, "sdk", "out of gas"
	r.Messages = []failedtx.Msg{{Index: 0, TypeURL: tcURLPFF, Signer: "celestia1submitter",
		Detail: &failedtx.MsgDetail{Publisher: "celestia1pub", PromiseHash: strings.Repeat("dd", 32), Namespace: "00", BlobSize: 4096}}}
	return r
}

// tcMsgRows is every failed_tx_msgs row, in key order, one per line.
func tcMsgRows(t *testing.T, db *sql.DB) string {
	t.Helper()
	rows, err := db.Query(`SELECT account, height, tx_index, msg_index, type_url, dedupe_key FROM failed_tx_msgs
		ORDER BY account, height, tx_index, msg_index`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var account, url, key string
		var height, txIndex, msgIndex int64
		if err := rows.Scan(&account, &height, &txIndex, &msgIndex, &url, &key); err != nil {
			t.Fatal(err)
		}
		out = append(out, strings.Join([]string{account, tcItoa(height), tcItoa(txIndex), tcItoa(msgIndex), url, key}, " "))
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return strings.Join(out, "\n")
}

func tcItoa(n int64) string { b, _ := json.Marshal(n); return string(b) }

// tcWantRows are the failed_tx_msgs rows of tcMixed at (mh, mi) and tcPFF at
// (ph, pi), as tcMsgRows prints them.
func tcWantRows(t *testing.T, mh int64, mi int, ph int64, pi int) string {
	t.Helper()
	mk, pk := failedtx.Key(mh, mi), failedtx.Key(ph, pi)
	lines := []string{
		strings.Join([]string{"celestia1owner", tcItoa(mh), tcItoa(int64(mi)), "4", tcURLTimeout, mk}, " "),
		strings.Join([]string{"celestia1pub", tcItoa(mh), tcItoa(int64(mi)), "1", tcURLDeposit, mk}, " "),
		strings.Join([]string{"celestia1pub", tcItoa(ph), tcItoa(int64(pi)), "0", tcURLPFF, pk}, " "),
		strings.Join([]string{tcValoper(t, 'a'), tcItoa(mh), tcItoa(int64(mi)), "3", tcURLSetHost, mk}, " "),
	}
	return strings.Join(lines, "\n")
}

// tcStoreAt30 is a store at version 30, as the build before this one left
// it: storeWithRows' publication, reading, payment and host event, and
// three failed transactions written by plain SQL (InsertFailedTx needs
// migration 31's table): tcMixed, tcPFF and a row whose raw_json does not
// decode.
func tcStoreAt30(t *testing.T, path string) *Store {
	t.Helper()
	old := storeWithRowsAt(t, path, 30)
	for _, r := range []failedtx.Record{tcMixed(t, 1300100, 1, strings.Repeat("a1", 32)), tcPFF(1300101, 0, strings.Repeat("a2", 32))} {
		tcPlainFailedTx(t, old.db, r, string(failedLine(t, r)))
	}
	tcPlainFailedTx(t, old.db, failedRecord(1300102, 0, strings.Repeat("a3", 32)), `{"schema_version":1,"messages":[`)
	return old
}

func tcPlainFailedTx(t *testing.T, db *sql.DB, r failedtx.Record, raw string) {
	t.Helper()
	if _, err := db.Exec(`INSERT INTO failed_txs (dedupe_key, tx_hash, height, tx_index, time, code, codespace, raw_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		r.DedupeKey, r.TxHash, r.Height, r.TxIndex, ts(r.Time), int64(r.Code), r.Codespace, raw); err != nil {
		t.Fatal(err)
	}
}

// tcObjects fails unless migration 31's tables and indexes exist.
func tcObjects(t *testing.T, db *sql.DB) {
	t.Helper()
	for _, c := range []struct{ kind, name string }{
		{"table", "tx_costs"}, {"index", "tx_costs_tx"}, {"table", "failed_tx_msgs"}, {"index", "payments_tx"}, {"index", "host_events_at"},
	} {
		var n int
		if err := db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type = ? AND name = ?`, c.kind, c.name).Scan(&n); err != nil || n != 1 {
			t.Errorf("%s %s: %d, %v", c.kind, c.name, n, err)
		}
	}
}

func tcApplied31(t *testing.T, db *sql.DB) string {
	t.Helper()
	var at string
	if err := db.QueryRow(`SELECT applied_at FROM schema_migrations WHERE version = 31`).Scan(&at); err != nil {
		t.Fatal(err)
	}
	return at
}

func tcCount(t *testing.T, db *sql.DB, table string) int {
	t.Helper()
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM ` + table).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// Migration 31 is the last one, SchemaVersion is its version, and it only
// adds tables and indexes: no statement writes a row, and there is no check
// or backfill, so it never counts as a rewrite and the API's day partials
// are kept across it.
func TestMigration31IsTheLastAndPureDDL(t *testing.T) {
	if SchemaVersion != 31 {
		t.Fatalf("SchemaVersion %d, want 31", SchemaVersion)
	}
	last := migrations[len(migrations)-1]
	if last.version != 31 || last.note != txCostsMigration.note || len(last.stmts) != len(txCostsMigration.stmts) {
		t.Fatalf("the last migration is %d (%s)", last.version, last.note)
	}
	writes := regexp.MustCompile(`(?i)\b(INSERT|UPDATE|DELETE|REPLACE|WITH)\b`)
	create := regexp.MustCompile(`(?i)^\s*CREATE\s+(TABLE|INDEX)\s+IF\s+NOT\s+EXISTS\b`)
	for _, stmt := range txCostsMigration.stmts {
		if writes.MatchString(stmt) || reRewrite.MatchString(stmt) {
			t.Errorf("migration 31 writes rows:\n%s", stmt)
		}
		if !create.MatchString(stmt) {
			t.Errorf("migration 31 has a statement that is not a CREATE ... IF NOT EXISTS:\n%s", stmt)
		}
	}
	if txCostsMigration.check != nil || txCostsMigration.backfill != nil {
		t.Error("migration 31 has a check or a backfill")
	}
}

// On a store at 30 with rows, migration 31 adds its tables and indexes,
// moves the schema version and nothing else: migration_rewrites and every
// row are as they were, and failed_tx_msgs stays empty until the
// collector's fill.
func TestMigration31OnAStoreAt30WithRows(t *testing.T) {
	path := filepath.Join(t.TempDir(), "observer.db")
	old := tcStoreAt30(t, path)
	before := rowsOf(t, old)
	failedBefore := tcTableRows(t, old.db, `SELECT dedupe_key, raw_json FROM failed_txs ORDER BY dedupe_key`)
	rewritesBefore, _ := old.Meta(MetaMigrationRewrites)
	old.Close()

	st, err := Open(path)
	if err != nil {
		t.Fatalf("migrating a store at 30: %v", err)
	}
	defer st.Close()
	var version int
	if err := st.db.QueryRow(`SELECT MAX(version) FROM schema_migrations`).Scan(&version); err != nil || version != 31 {
		t.Fatalf("schema %d after the migration: %v", version, err)
	}
	if after, _ := st.Meta(MetaMigrationRewrites); after != rewritesBefore || after != "2" {
		t.Errorf("migration_rewrites %q before migration 31, %q after", rewritesBefore, after)
	}
	if after := rowsOf(t, st); after != before {
		t.Errorf("rows %s before migration 31, %s after", before, after)
	}
	if after := tcTableRows(t, st.db, `SELECT dedupe_key, raw_json FROM failed_txs ORDER BY dedupe_key`); after != failedBefore || tcCount(t, st.db, "failed_txs") != 3 {
		t.Errorf("failed_txs changed:\n%s\n%s", failedBefore, after)
	}
	tcObjects(t, st.db)
	if n := tcCount(t, st.db, "failed_tx_msgs"); n != 0 {
		t.Errorf("the migration wrote %d message rows", n)
	}
	if v, _ := st.Meta(MetaFailedTxMsgsFrom); v != "" {
		t.Errorf("the migration set %s: %q", MetaFailedTxMsgsFrom, v)
	}
}

// The way back is the one-row delete: a store whose version-31 row is
// deleted is at 30 for every opener (the read-only one refuses it), keeps
// its tables, indexes and rows, and migration 31 runs again over them
// without error, with a new applied_at.
func TestMigration31RunsAgainAfterTheOneRowDelete(t *testing.T) {
	path := filepath.Join(t.TempDir(), "observer.db")
	st, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	c := tcCost(1300200, 2, strings.Repeat("c1", 32))
	if ok, err := st.InsertTxCost(c, tcCostLine(t, c)); err != nil || !ok {
		t.Fatalf("insert tx cost: %v %v", ok, err)
	}
	r := tcMixed(t, 1300201, 0, strings.Repeat("c2", 32))
	if ok, err := st.InsertFailedTx(r, failedLine(t, r)); err != nil || !ok {
		t.Fatalf("insert failed tx: %v %v", ok, err)
	}
	first := tcApplied31(t, st.db)
	msgs := tcMsgRows(t, st.db)
	if _, err := st.db.Exec(`DELETE FROM schema_migrations WHERE version > 30`); err != nil {
		t.Fatal(err)
	}
	st.Close()
	if ro, err := OpenReadOnly(path); err == nil {
		ro.Close()
		t.Fatal("the read-only open accepted a store at 30")
	}

	st, err = Open(path)
	if err != nil {
		t.Fatalf("migration 31 again over its own tables: %v", err)
	}
	defer st.Close()
	var version int
	if err := st.db.QueryRow(`SELECT MAX(version) FROM schema_migrations`).Scan(&version); err != nil || version != 31 {
		t.Fatalf("schema %d after the second migration: %v", version, err)
	}
	if again := tcApplied31(t, st.db); again == first {
		t.Fatalf("the second migration kept applied_at %s", first)
	}
	if n := tcCount(t, st.db, "tx_costs"); n != 1 {
		t.Fatalf("%d tx cost rows after the second migration", n)
	}
	if got := tcMsgRows(t, st.db); got != msgs || got == "" {
		t.Fatalf("message rows after the second migration:\n%s\nwant\n%s", got, msgs)
	}
	tcObjects(t, st.db)
	ro, err := OpenReadOnly(path)
	if err != nil {
		t.Fatalf("the read-only open after the second migration: %v", err)
	}
	ro.Close()
}

func tcCost(height int64, txIndex int, hash string) txcost.Record {
	at := time.Date(2026, 10, 8, 13, 25, 59, 888800317, time.UTC)
	return txcost.Record{SchemaVersion: txcost.SchemaVersion, DedupeKey: txcost.Key(height, txIndex), Height: height, TxIndex: txIndex, Time: at,
		TxHash: hash, GasWanted: 400000, GasUsed: 219118, Fee: "8000utia", FeePayer: "celestia1pub",
		Messages: []failedtx.Msg{{Index: 0, TypeURL: tcURLPFF, Signer: "celestia1pub"}}, RecordedAt: at.Add(2 * time.Second)}
}

func tcCostLine(t *testing.T, r txcost.Record) []byte {
	t.Helper()
	b, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// A cost line is stored verbatim under its (height, tx index), once: the
// same position again is left alone, whatever its line says. The hash is
// kept in lower case and the row's other columns are the record's; a line
// without a key or a hash is refused.
func TestInsertTxCostKeepsTheFirstLineVerbatim(t *testing.T) {
	st, err := Open(filepath.Join(t.TempDir(), "observer.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	hash := strings.Repeat("5DA6", 16)
	r := tcCost(1497140, 2, hash)
	raw := append([]byte(" "), tcCostLine(t, r)...)
	if ok, err := st.InsertTxCost(r, raw); err != nil || !ok {
		t.Fatalf("first insert: %v %v", ok, err)
	}
	again := r
	again.GasUsed, again.RecordedAt = 1, r.RecordedAt.Add(time.Hour)
	if ok, err := st.InsertTxCost(again, tcCostLine(t, again)); err != nil || ok {
		t.Fatalf("the same position again: inserted %v, %v", ok, err)
	}
	var key, gotHash, gotTime, gotRaw string
	if err := st.db.QueryRow(`SELECT dedupe_key, tx_hash, time, raw_json FROM tx_costs WHERE height = ? AND tx_index = ?`, r.Height, r.TxIndex).
		Scan(&key, &gotHash, &gotTime, &gotRaw); err != nil {
		t.Fatal(err)
	}
	if key != r.DedupeKey || gotHash != strings.ToLower(hash) || gotTime != TS(r.Time) {
		t.Errorf("row: %s %s %s", key, gotHash, gotTime)
	}
	if gotRaw != string(raw) {
		t.Errorf("raw_json is not the first line verbatim:\n%s\n%s", gotRaw, raw)
	}
	if n := tcCount(t, st.db, "tx_costs"); n != 1 {
		t.Errorf("%d rows, want 1", n)
	}
	for _, bad := range []txcost.Record{{TxHash: hash, Height: 1}, {DedupeKey: "h1:0", Height: 1}} {
		if ok, err := st.InsertTxCost(bad, []byte(`{}`)); err == nil || ok {
			t.Errorf("a cost line without key or hash stored: %+v", bad)
		}
	}
}

// The message rows of a failure are the top-level Fibre messages of a final
// one that name an account, from the record the store keeps for its key:
// written with the row, again when the same key is read again, and never
// from a later line with that key.
func TestInsertFailedTxWritesTheMessageRowsOfTheKeptRecord(t *testing.T) {
	st, err := Open(filepath.Join(t.TempDir(), "observer.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	mixed := tcMixed(t, 1300100, 1, strings.Repeat("a1", 32))
	pff := tcPFF(1300101, 0, strings.Repeat("a2", 32))
	for _, r := range []failedtx.Record{mixed, pff} {
		if ok, err := st.InsertFailedTx(r, failedLine(t, r)); err != nil || !ok {
			t.Fatalf("insert %s: %v %v", r.DedupeKey, ok, err)
		}
	}
	want := tcWantRows(t, 1300100, 1, 1300101, 0)
	if got := tcMsgRows(t, st.db); got != want {
		t.Fatalf("message rows:\n%s\nwant\n%s", got, want)
	}

	// A non-final failure, and a final one whose only Fibre message was cut:
	// rows of their own, no message row.
	nonFinal := tcPFF(1300102, 0, strings.Repeat("a3", 32))
	nonFinal.AntePassed, nonFinal.Fee = false, ""
	cut := tcPFF(1300103, 0, strings.Repeat("a4", 32))
	cut.Messages[0].Cut = true
	for _, r := range []failedtx.Record{nonFinal, cut} {
		if ok, err := st.InsertFailedTx(r, failedLine(t, r)); err != nil || !ok {
			t.Fatalf("insert %s: %v %v", r.DedupeKey, ok, err)
		}
	}
	if got := tcMsgRows(t, st.db); got != want {
		t.Fatalf("a non-final or cut failure gave message rows:\n%s", got)
	}

	// The same line again: nothing new, and inserted false.
	if ok, err := st.InsertFailedTx(mixed, failedLine(t, mixed)); err != nil || ok {
		t.Fatalf("the same line again: inserted %v, %v", ok, err)
	}
	// Another line under the same key, naming other accounts: the rows stay
	// the first line's.
	other := mixed
	other.Messages = []failedtx.Msg{{Index: 0, TypeURL: tcURLDeposit, Signer: "celestia1other", Detail: &failedtx.MsgDetail{Publisher: "celestia1other", Amount: "5utia"}},
		{Index: 1, TypeURL: tcURLSetHost, Signer: tcValoper(t, 'z'), Detail: &failedtx.MsgDetail{Host: "z.example:7980"}}}
	if ok, err := st.InsertFailedTx(other, failedLine(t, other)); err != nil || ok {
		t.Fatalf("another line under the key: inserted %v, %v", ok, err)
	}
	if got := tcMsgRows(t, st.db); got != want {
		t.Fatalf("a later line with the same key changed the message rows:\n%s\nwant\n%s", got, want)
	}

	// The rows deleted by hand: a second read of any line under the key
	// writes them again from the stored row.
	if _, err := st.db.Exec(`DELETE FROM failed_tx_msgs`); err != nil {
		t.Fatal(err)
	}
	for _, r := range []failedtx.Record{other, pff} {
		if ok, err := st.InsertFailedTx(r, failedLine(t, r)); err != nil || ok {
			t.Fatalf("a second read of %s: inserted %v, %v", r.DedupeKey, ok, err)
		}
	}
	if got := tcMsgRows(t, st.db); got != want {
		t.Fatalf("the rows written again:\n%s\nwant\n%s", got, want)
	}
	if n := tcCount(t, st.db, "failed_txs"); n != 4 {
		t.Fatalf("%d failed tx rows, want 4", n)
	}
}

// The fill gives a store migrated from 30 the message rows of the failures
// it held: once per application of migration 31, under its applied_at, the
// same rows a store that read the lines after the migration has, a line
// that does not decode skipped, and migration_rewrites never moved.
func TestFillFailedTxMsgs(t *testing.T) {
	path := filepath.Join(t.TempDir(), "observer.db")
	tcStoreAt30(t, path).Close()
	st, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 10, 9, 7, 0, 0, 0, time.UTC)
	filled, rows, err := st.FillFailedTxMsgs(now)
	if err != nil || !filled || rows != 4 {
		t.Fatalf("the first fill: %v %d %v", filled, rows, err)
	}
	want := tcWantRows(t, 1300100, 1, 1300101, 0)
	if got := tcMsgRows(t, st.db); got != want {
		t.Fatalf("message rows:\n%s\nwant\n%s", got, want)
	}
	if v, _ := st.Meta(MetaFailedTxMsgsFrom); v != tcApplied31(t, st.db) {
		t.Fatalf("%s = %q, the version-31 row %q", MetaFailedTxMsgsFrom, v, tcApplied31(t, st.db))
	}
	var updated string
	if err := st.db.QueryRow(`SELECT updated_at FROM meta WHERE key = ?`, MetaFailedTxMsgsFrom).Scan(&updated); err != nil || updated != TS(now) {
		t.Fatalf("updated_at %q %v", updated, err)
	}

	// Again: nothing to do, nothing written.
	filled, rows, err = st.FillFailedTxMsgs(now.Add(time.Hour))
	if err != nil || filled || rows != 0 {
		t.Fatalf("the second fill: %v %d %v", filled, rows, err)
	}
	if err := st.db.QueryRow(`SELECT updated_at FROM meta WHERE key = ?`, MetaFailedTxMsgsFrom).Scan(&updated); err != nil || updated != TS(now) {
		t.Fatalf("the second fill wrote the key: %q %v", updated, err)
	}

	// The way back and a second upgrade apply 31 again: the fill runs again,
	// over rows that are all there.
	if _, err := st.db.Exec(`DELETE FROM schema_migrations WHERE version > 30`); err != nil {
		t.Fatal(err)
	}
	st.Close()
	st, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	filled, rows, err = st.FillFailedTxMsgs(now.Add(2 * time.Hour))
	if err != nil || !filled || rows != 0 {
		t.Fatalf("the fill after a second upgrade: %v %d %v", filled, rows, err)
	}
	if got := tcMsgRows(t, st.db); got != want {
		t.Fatalf("message rows after a second upgrade:\n%s\nwant\n%s", got, want)
	}
	if v, _ := st.Meta(MetaFailedTxMsgsFrom); v != tcApplied31(t, st.db) {
		t.Fatalf("%s = %q after a second upgrade, the version-31 row %q", MetaFailedTxMsgsFrom, v, tcApplied31(t, st.db))
	}
	if v, _ := st.Meta(MetaMigrationRewrites); v != "2" {
		t.Fatalf("migration_rewrites %q", v)
	}

	// A store that read the same lines after the migration holds the same
	// rows; its fill finds nothing to add and only sets the key.
	fresh, err := Open(filepath.Join(t.TempDir(), "fresh.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer fresh.Close()
	for _, r := range []failedtx.Record{tcMixed(t, 1300100, 1, strings.Repeat("a1", 32)), tcPFF(1300101, 0, strings.Repeat("a2", 32))} {
		if _, err := fresh.InsertFailedTx(r, failedLine(t, r)); err != nil {
			t.Fatal(err)
		}
	}
	if filled, rows, err := fresh.FillFailedTxMsgs(now); err != nil || !filled || rows != 0 {
		t.Fatalf("a fresh store's fill: %v %d %v", filled, rows, err)
	}
	if got := tcMsgRows(t, fresh.db); got != want {
		t.Fatalf("a fresh store's message rows:\n%s\nwant\n%s", got, want)
	}
}

// Cost lines and message rows are in no count: Count and its running totals
// are the same before and after rows are stored in both tables, and so is a
// full recount.
func TestTxCostsAndFailedTxMsgsAreInNoCount(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "observer.db")
	storeWithRows(t, path).Close()
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
		c := tcCost(1300300+int64(i), i, strings.Repeat("f1", 32))
		if _, err := st.InsertTxCost(c, tcCostLine(t, c)); err != nil {
			t.Fatal(err)
		}
		r := tcMixed(t, 1300400+int64(i), i, strings.Repeat("f2", 32))
		if _, err := st.InsertFailedTx(r, failedLine(t, r)); err != nil {
			t.Fatal(err)
		}
	}
	if tcCount(t, st.db, "tx_costs") != 5 || tcCount(t, st.db, "failed_tx_msgs") != 15 {
		t.Fatalf("the rows were not stored: %d cost, %d message", tcCount(t, st.db, "tx_costs"), tcCount(t, st.db, "failed_tx_msgs"))
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
		t.Errorf("counts moved: %+v %v, then %+v %v", c0, g0, c1, g1)
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

// tcTableRows is every row query reads, in its order, one per line.
func tcTableRows(t *testing.T, db *sql.DB, query string) string {
	t.Helper()
	rows, err := db.Query(query)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	cols, _ := rows.Columns()
	var out []string
	for rows.Next() {
		v := make([]any, len(cols))
		p := make([]any, len(cols))
		for i := range v {
			p[i] = &v[i]
		}
		if err := rows.Scan(p...); err != nil {
			t.Fatal(err)
		}
		b, _ := json.Marshal(v)
		out = append(out, string(b))
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return strings.Join(out, "\n")
}
