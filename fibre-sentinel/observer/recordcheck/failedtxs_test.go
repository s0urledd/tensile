package recordcheck

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/failedtx"
)

// failedTxLines are two failed transactions of the fixture's day
// (2026-10-05), as sentinel-scan writes them.
func failedTxLines(t *testing.T) [][]byte {
	t.Helper()
	var out [][]byte
	for i, code := range []uint32{5, 18} {
		at := time.Date(2026, 10, 5, 13, i, 0, 0, time.UTC)
		r := failedtx.Record{SchemaVersion: failedtx.SchemaVersion, DedupeKey: failedtx.Key(1100+int64(i), 0), Height: 1100 + int64(i), Time: at,
			AppVersion: 10, TxHash: strings.Repeat(fmt.Sprintf("%02x", i+1), 32), Code: code, Codespace: "sdk",
			Log: "failed to execute message; message index: 0: test", GasWanted: 200000, GasUsed: 90000, AntePassed: true, Fee: "2000utia",
			Messages: []failedtx.Msg{{Index: 0, TypeURL: "/celestia.fibre.v1.MsgPayForFibre"}}, RecordedAt: at.Add(2 * time.Second)}
		l, err := json.Marshal(r)
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, l)
	}
	return out
}

// A day whose export carries failed transactions is checked as the same day
// without them: the export intact and reproducible, failed_txs.jsonl held to
// its manifest digest alone (no kind the store gives back line by line),
// every checked member's report and ledger entry the same, and no ledger
// entry for failed_txs.jsonl.
func TestADayWithFailedTxsIsCheckedAsTheDayWithout(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t)
	// as the collector stores them: they are in the store, and still no check reads them
	for _, l := range failedTxLines(t) {
		var r failedtx.Record
		if err := json.Unmarshal(l, &r); err != nil {
			t.Fatal(err)
		}
		if ok, err := f.st.InsertFailedTx(r, l); err != nil || !ok {
			t.Fatalf("failed tx: %v %v", ok, err)
		}
	}
	check := func(failed [][]byte) (DayReport, LedgerDay) {
		t.Helper()
		data := t.TempDir()
		files := map[string][]byte{
			"publications.jsonl": file(f.pubLine),
			"measurements.jsonl": file(f.readings...),
			"reachability.jsonl": file(f.reachLines...),
		}
		if failed != nil {
			files[failedtx.FileName] = file(failed...)
		}
		writeFiles(t, data, files)
		dir, e := exportDay(t, data, time.Date(2026, 10, 6, 4, 0, 0, 0, time.UTC))
		r, err := CheckDay(ctx, f.st, dir, e)
		if err != nil {
			t.Fatal(err)
		}
		d, ok := r.LedgerDay("t", time.Date(2026, 10, 6, 5, 0, 0, 0, time.UTC))
		if !ok {
			t.Fatalf("nothing to record: %+v", r)
		}
		return r, d
	}
	with, ledgerWith := check(failedTxLines(t))
	without, ledgerWithout := check(nil)

	if !with.ExportIntact || !with.Reproducible || !slices.Contains(with.NotChecked, failedtx.FileName) {
		t.Fatalf("with failed transactions: %+v", with)
	}
	for _, fr := range with.Files {
		if fr.Name == failedtx.FileName {
			t.Fatalf("failed_txs.jsonl was checked line by line: %+v", fr)
		}
	}
	if !reflect.DeepEqual(with.Files, without.Files) {
		t.Fatalf("the checked members differ:\n%+v\n%+v", with.Files, without.Files)
	}
	if !slices.Equal(with.NotChecked, without.NotChecked) {
		t.Fatalf("not checked: %v with failed transactions, %v without", with.NotChecked, without.NotChecked)
	}
	if _, ok := ledgerWith.Files[failedtx.FileName]; ok || !ledgerWith.Reproducible() {
		t.Fatalf("the ledger entry: %+v", ledgerWith.Files)
	}
	if !reflect.DeepEqual(ledgerWith.Files, ledgerWithout.Files) {
		t.Fatalf("the ledger entries differ:\n%+v\n%+v", ledgerWith.Files, ledgerWithout.Files)
	}
	if Checked(failedtx.FileName) {
		t.Fatal("failed_txs.jsonl is a kind the store gives back line by line")
	}
}
