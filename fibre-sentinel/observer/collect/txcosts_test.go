package collect_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/failedtx"
	"github.com/plsgiveup/fibre/fibre-sentinel/internal/txcost"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
)

// costLines are the cost lines of the record's own successes: the first
// publication's settlement and the deposit, at their heights, indexes and
// hashes.
func costLines() []any {
	settled := time.Date(2026, 9, 6, 17, 0, 27, 494519078, time.UTC)
	return []any{
		txcost.Record{SchemaVersion: txcost.SchemaVersion, DedupeKey: txcost.Key(20, 1), Height: 20, TxIndex: 1, Time: testdataAt.Add(-time.Minute),
			TxHash: depositTx, GasWanted: 120000, GasUsed: 70000, Fee: "150utia", FeePayer: publisher,
			Messages: []failedtx.Msg{{Index: 0, TypeURL: "/celestia.fibre.v1.MsgDepositToEscrow", Signer: publisher}}, RecordedAt: testdataAt},
		txcost.Record{SchemaVersion: txcost.SchemaVersion, DedupeKey: txcost.Key(32, 0), Height: 32, TxIndex: 0, Time: settled,
			TxHash: settledTx, GasWanted: 400000, GasUsed: 219118, Fee: "8000utia", FeePayer: publisher,
			Messages: []failedtx.Msg{{Index: 0, TypeURL: "/celestia.fibre.v1.MsgPayForFibre", Signer: publisher}}, RecordedAt: settled.Add(time.Second)},
	}
}

const txCostsSQL = `SELECT height, tx_index, dedupe_key, tx_hash, time, raw_json FROM tx_costs ORDER BY height, tx_index`

// msgRowsSQL is every failed_tx_msgs row, one string each.
const msgRowsSQL = `SELECT account || ' ' || height || ' ' || tx_index || ' ' || msg_index || ' ' || type_url || ' ' || dedupe_key
	FROM failed_tx_msgs ORDER BY account, height, tx_index, msg_index`

// column is the one text column query reads, row by row.
func column(t *testing.T, st *store.Store, query string, args ...any) []string {
	t.Helper()
	rows, err := st.DB().Query(query, args...)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			t.Fatal(err)
		}
		out = append(out, s)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

// The pass reads tx_costs.jsonl into its own table, line for line and with
// no ingest error, whether the file is there or not; a store rebuilt from the
// same files holds the same rows; and every other table holds the same rows
// with the file and without it, though the lines carry the record's own
// hashes, heights and indexes.
func TestThePassReadsTxCostsIntoTheirTableAlone(t *testing.T) {
	now := time.Date(2026, 9, 7, 0, 0, 0, 0, time.UTC)
	with, without := testdataDir(t, true), testdataDir(t, true)
	writeLines(t, filepath.Join(with, txcost.FileName), costLines()...)
	a := passTwice(t, with, now)
	b := passTwice(t, without, now)

	lines, err := os.ReadFile(filepath.Join(with, txcost.FileName))
	if err != nil {
		t.Fatal(err)
	}
	want := strings.Split(strings.TrimSuffix(string(lines), "\n"), "\n")
	if got := column(t, a.St, `SELECT raw_json FROM tx_costs ORDER BY rowid`); strings.Join(got, "\n") != strings.Join(want, "\n") || len(got) != 2 {
		t.Fatalf("the tx cost rows are not the lines:\n%s\n---\n%s", strings.Join(got, "\n"), lines)
	}
	if got := column(t, a.St, `SELECT tx_hash FROM tx_costs ORDER BY height`); strings.Join(got, ",") != depositTx+","+settledTx {
		t.Fatalf("hashes %v", got)
	}
	if n := countRows(t, b.St, "tx_costs"); n != 0 {
		t.Fatalf("%d tx cost rows without the file", n)
	}
	var cursors int64
	if err := b.St.DB().QueryRow(`SELECT COUNT(*) FROM ingest_cursors WHERE file = ?`, filepath.Join(without, txcost.FileName)).Scan(&cursors); err != nil || cursors != 0 {
		t.Fatalf("a cursor for a file that is not there: %d %v", cursors, err)
	}

	for _, table := range []string{"publications", "assignments", "payments", "host_events", "probes", "failed_txs", "failed_tx_msgs"} {
		na, nb := countRows(t, a.St, table), countRows(t, b.St, table)
		if na != nb || na == 0 {
			t.Errorf("%s: %d rows with tx costs, %d without", table, na, nb)
		}
	}
	ca, err := a.St.Count(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	cb, err := b.St.Count(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if ca != cb {
		t.Errorf("Count: %+v with tx costs, %+v without", ca, cb)
	}

	// A store rebuilt from zero over the same files: the same rows.
	rebuilt := passTwice(t, with, now.Add(time.Hour))
	if got, want := tableRows(t, rebuilt.St, txCostsSQL), tableRows(t, a.St, txCostsSQL); got != want || got == "" {
		t.Fatalf("the rebuilt store's tx costs differ:\n%s\n---\n%s", got, want)
	}
}

// The pass lists each final failure's top-level Fibre messages under their
// accounts (the record's failed settlements and deposit; not its non-final
// settlement, its delegated registration, nor one signed by a string that is
// no address), the same rows on a store rebuilt from zero. And the fill
// (store.FillFailedTxMsgs) is wired: a store that lost the rows and its mark,
// as one upgraded from 30 holds them, gets the same rows back at its next
// pass, from the stored failures, with the file's cursor where it was.
func TestThePassListsTheFinalFailuresMessages(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 9, 7, 0, 0, 0, 0, time.UTC)
	dir := testdataDir(t, true)
	a := passTwice(t, dir, now)
	want := []string{
		publisher + " 20 1 1 /celestia.fibre.v1.MsgDepositToEscrow h20:1",
		publisher + " 31 2 0 /celestia.fibre.v1.MsgPayForFibre h31:2",
		publisher + " 41 0 0 /celestia.fibre.v1.MsgPayForFibre h41:0",
	}
	if got := column(t, a.St, msgRowsSQL); strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("message rows:\n%s\nwant\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
	rebuilt := passTwice(t, dir, now.Add(time.Hour))
	if got := column(t, rebuilt.St, msgRowsSQL); strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("the rebuilt store's message rows:\n%s", strings.Join(got, "\n"))
	}

	failedPath := filepath.Join(dir, failedtx.FileName)
	cursor := func() string {
		t.Helper()
		var off, line int64
		if err := a.St.DB().QueryRow(`SELECT byte_offset, line_no FROM ingest_cursors WHERE file = ?`, failedPath).Scan(&off, &line); err != nil {
			t.Fatal(err)
		}
		return fmt.Sprintf("offset %d line %d", off, line)
	}
	before := cursor()
	if _, err := a.St.DB().Exec(`DELETE FROM failed_tx_msgs`); err != nil {
		t.Fatal(err)
	}
	if _, err := a.St.DB().Exec(`DELETE FROM meta WHERE key = ?`, store.MetaFailedTxMsgsFrom); err != nil {
		t.Fatal(err)
	}
	if errs := a.Pass(ctx, now.Add(20*time.Second)); len(errs) != 0 {
		t.Fatalf("the pass after the rows were lost: %v", errs)
	}
	if got := column(t, a.St, msgRowsSQL); strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("the rows the fill gave back:\n%s\nwant\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
	if after := cursor(); after != before {
		t.Fatalf("the failed_txs.jsonl cursor moved: %s, then %s", before, after)
	}
	mark, err := a.St.Meta(store.MetaFailedTxMsgsFrom)
	if err != nil {
		t.Fatal(err)
	}
	if applied := column(t, a.St, `SELECT applied_at FROM schema_migrations WHERE version = 31`); len(applied) != 1 || mark != applied[0] {
		t.Fatalf("%s = %q, the version-31 row %v", store.MetaFailedTxMsgsFrom, mark, applied)
	}
}
