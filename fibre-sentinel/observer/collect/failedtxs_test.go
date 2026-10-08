package collect_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/failedtx"
	"github.com/plsgiveup/fibre/fibre-sentinel/internal/scan"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/collect"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
)

// The record of observer/testdata, which these identifiers are from: a
// publication's settlement, its publisher and promise, and a validator.
const (
	settledTx  = "09215b92e74f21c2b177bd2a8d8f3535522f41fe5765b82306f0b438bb2c0af2"
	settledPub = "e1bc7a4c4d4f857dd805f20d6fd48b50f409e180aa8e001e1f9f869fc8431d61"
	publisher  = "celestia1d3mmg652pxj776dyqwlsrc93y64088g6ux8deq"
	namespace  = "00000000000000000000000000000000000000000000000000534e5400"
	validator  = "577b1362af2946edb3a1c3b4c7bc3e2e9889ffe0"
	depositTx  = "d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0"
)

var testdataAt = time.Date(2026, 9, 6, 17, 0, 0, 0, time.UTC)

// writeLines writes one JSON line per value to path.
func writeLines(t *testing.T, path string, vs ...any) {
	t.Helper()
	var b []byte
	for _, v := range vs {
		j, err := json.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		b = append(append(b, j...), '\n')
	}
	if err := os.WriteFile(path, b, 0o644); err != nil {
		t.Fatal(err)
	}
}

// testdataDir is a data dir with observer/testdata's record, the escrow
// movements and host registrations of its publisher and validators, and,
// with failed, failed transactions that carry the record's own identifiers.
func testdataDir(t *testing.T, failed bool) string {
	t.Helper()
	dir := t.TempDir()
	for _, name := range []string{"publications.jsonl", "measurements.jsonl", "state.json"} {
		b, err := os.ReadFile(filepath.Join("..", "testdata", name))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, name), b, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	writeLines(t, filepath.Join(dir, "payments.jsonl"),
		scan.Payment{SchemaVersion: 1, DedupeKey: depositTx + ":0", Kind: scan.PaymentDeposit, Height: 20, Time: testdataAt.Add(-time.Minute),
			TxHash: depositTx, TxIndex: 1, Publisher: publisher, Denom: "utia", AmountUtia: 50_000_000, RecordedAt: testdataAt},
		scan.Payment{SchemaVersion: 1, DedupeKey: settledTx + ":0", Kind: scan.PaymentSettlement, Height: 32, Time: testdataAt.Add(27 * time.Second),
			TxHash: settledTx, Publisher: publisher, PromiseHash: settledPub, Namespace: namespace, BlobSize: 262144, Denom: "utia", AmountUtia: 4000,
			RecordedAt: testdataAt.Add(28 * time.Second)})
	writeLines(t, filepath.Join(dir, "host_history.jsonl"),
		scan.HostEvent{HostEntry: scan.HostEntry{FromHeight: 1, FromTxIndex: -1, ConsAddress: validator, Host: "127.0.0.1:7980", Source: "seed"}, Time: testdataAt.Add(-time.Hour)},
		scan.HostEvent{HostEntry: scan.HostEntry{FromHeight: 25, FromTxIndex: 0, ConsAddress: validator, Host: "127.0.0.1:7981", Source: "event"}, Time: testdataAt.Add(-30 * time.Second)})
	if failed {
		writeLines(t, filepath.Join(dir, failedtx.FileName), failedRecords()...)
	}
	return dir
}

// failedRecords are failed transactions that carry the record's own
// identifiers: the settled transaction's hash at a lower height, a failed
// settlement of the same promise, a failed deposit under the successful
// deposit's hash and height, a failed host registration at a host event's
// height and index, a delegated one, and a panic.
func failedRecords() []any {
	at := testdataAt.Add(10 * time.Second)
	rec := func(height int64, txIndex int, hash string, code uint32, space string, ante bool, msgs ...failedtx.Msg) failedtx.Record {
		r := failedtx.Record{SchemaVersion: failedtx.SchemaVersion, DedupeKey: failedtx.Key(height, txIndex), Height: height,
			Time: at.Add(time.Duration(height) * time.Second), AppVersion: 10, TxHash: hash, TxIndex: txIndex, Code: code, Codespace: space,
			Log: "failed to execute message; message index: 0: test", GasWanted: 200000, GasUsed: 50000, AntePassed: ante, Messages: msgs, RecordedAt: at}
		if ante {
			r.Fee = "2000utia"
		}
		return r
	}
	pff := failedtx.Msg{Index: 0, TypeURL: "/celestia.fibre.v1.MsgPayForFibre", Signer: publisher,
		Detail: &failedtx.MsgDetail{Publisher: publisher, PromiseHash: settledPub, Namespace: namespace, BlobSize: 262144}}
	host := failedtx.Msg{Index: 0, TypeURL: "/celestia.valaddr.v1.MsgSetFibreProviderInfo", Signer: "celestiavaloper1v",
		Detail: &failedtx.MsgDetail{Host: "127.0.0.1:7982", Validator: "celestiavaloper1v"}}
	panicked := rec(41, 0, strings.Repeat("ef", 32), 111222, "undefined", true, pff)
	panicked.Log, panicked.LogCut = "recovered: invalid coin denominations", true
	return []any{
		rec(30, 0, settledTx, 18, "sdk", false, pff),
		rec(31, 2, strings.Repeat("ab", 32), 18, "sdk", true, pff),
		rec(20, 1, depositTx, 5, "sdk", true, failedtx.Msg{Index: 0, TypeURL: "/cosmos.bank.v1beta1.MsgSend"},
			failedtx.Msg{Index: 1, TypeURL: "/celestia.fibre.v1.MsgDepositToEscrow", Signer: publisher, Detail: &failedtx.MsgDetail{Publisher: publisher, Amount: "50000000utia"}}),
		rec(25, 0, strings.Repeat("cd", 32), 2, "valaddr", true, host),
		rec(26, 0, strings.Repeat("ce", 32), 2, "authz", true, failedtx.Msg{Index: 0, TypeURL: failedtx.ExecTypeURL, Inner: []failedtx.Msg{host}}),
		panicked,
	}
}

// tableRows is every row query reads, in its order, as one string.
func tableRows(t *testing.T, st *store.Store, query string) string {
	t.Helper()
	rows, err := st.DB().Query(query)
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
		out = append(out, fmt.Sprintf("%q", v))
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return strings.Join(out, "\n")
}

func countRows(t *testing.T, st *store.Store, table string) int64 {
	t.Helper()
	var n int64
	if err := st.DB().QueryRow(`SELECT COUNT(*) FROM ` + table).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// passTwice runs two collector passes over dir, as the collector runs them
// every interval, on a new store; it fails on any ingest error.
func passTwice(t *testing.T, dir string, now time.Time) *collect.Collector {
	t.Helper()
	c := newCollector(t, dir, &fakeStatus{})
	for i := 0; i < 2; i++ {
		if errs := c.Pass(context.Background(), now.Add(time.Duration(i)*10*time.Second)); len(errs) != 0 {
			t.Fatalf("pass %d: %v", i+1, errs)
		}
	}
	return c
}

const failedTxsSQL = `SELECT dedupe_key, tx_hash, height, tx_index, time, code, codespace, raw_json FROM failed_txs ORDER BY dedupe_key`

// The pass reads failed_txs.jsonl into its own table, line for line and
// with no ingest error, whether the file is there or not; a store rebuilt
// from the same files holds the same rows; and every table the figures are
// counted from holds the same rows with the file and without it, though the
// failures carry the record's own hashes, heights and promise.
func TestThePassReadsFailedTxsIntoTheirTableAlone(t *testing.T) {
	now := time.Date(2026, 9, 7, 0, 0, 0, 0, time.UTC)
	with, without := testdataDir(t, true), testdataDir(t, false)
	a := passTwice(t, with, now)
	b := passTwice(t, without, now)

	lines, err := os.ReadFile(filepath.Join(with, failedtx.FileName))
	if err != nil {
		t.Fatal(err)
	}
	want := strings.Split(strings.TrimSuffix(string(lines), "\n"), "\n")
	rows, err := a.St.DB().Query(`SELECT raw_json FROM failed_txs ORDER BY rowid`)
	if err != nil {
		t.Fatal(err)
	}
	var stored []string
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			t.Fatal(err)
		}
		stored = append(stored, raw)
	}
	rows.Close()
	if len(stored) != len(failedRecords()) || strings.Join(stored, "\n") != strings.Join(want, "\n") {
		t.Fatalf("the failed tx rows are not the lines:\n%s\n---\n%s", strings.Join(stored, "\n"), lines)
	}
	if n := countRows(t, b.St, "failed_txs"); n != 0 {
		t.Fatalf("%d failed tx rows without the file", n)
	}
	var cursors int64
	if err := b.St.DB().QueryRow(`SELECT COUNT(*) FROM ingest_cursors WHERE file = ?`, filepath.Join(without, failedtx.FileName)).Scan(&cursors); err != nil || cursors != 0 {
		t.Fatalf("a cursor for a file that is not there: %d %v", cursors, err)
	}

	for _, table := range []string{"publications", "assignments", "payments", "host_events", "probes"} {
		na, nb := countRows(t, a.St, table), countRows(t, b.St, table)
		if na != nb || na == 0 {
			t.Errorf("%s: %d rows with failed transactions, %d without", table, na, nb)
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
		t.Errorf("Count: %+v with failed transactions, %+v without", ca, cb)
	}

	// A store rebuilt from zero over the same files: the same rows.
	rebuilt := passTwice(t, with, now.Add(time.Hour))
	if got, want := tableRows(t, rebuilt.St, failedTxsSQL), tableRows(t, a.St, failedTxsSQL); got != want || got == "" {
		t.Fatalf("the rebuilt store's failed transactions differ:\n%s\n---\n%s", got, want)
	}
}
