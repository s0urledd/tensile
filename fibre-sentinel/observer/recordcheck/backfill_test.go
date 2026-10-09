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
	"github.com/plsgiveup/fibre/fibre-sentinel/internal/txcost"
)

// backfilledLines are a failed transaction and a cost line of 25 September
// 2026, ten days before the fixture's day, as a backfill's merge
// (cmd/sentinel-txbackfill) appends them after the day's own lines.
func backfilledLines(t *testing.T) (failed, costs [][]byte) {
	t.Helper()
	for i := 0; i < 2; i++ {
		at := time.Date(2026, 9, 25, 20, 30, i, 0, time.UTC)
		h := 10 + int64(i)
		f := failedtx.Record{SchemaVersion: failedtx.SchemaVersion, DedupeKey: failedtx.Key(h, 0), Height: h, Time: at, AppVersion: 10,
			TxHash: strings.Repeat(fmt.Sprintf("%02x", 0x50+i), 32), Code: 5, Codespace: "sdk", Log: "failed to execute message; message index: 0: test",
			GasWanted: 200000, GasUsed: 90000, AntePassed: true, Fee: "2000utia",
			Messages: []failedtx.Msg{{Index: 0, TypeURL: "/celestia.fibre.v1.MsgDepositToEscrow"}}, RecordedAt: time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)}
		c := txcost.Record{SchemaVersion: txcost.SchemaVersion, DedupeKey: txcost.Key(h, 1), Height: h, TxIndex: 1, Time: at,
			TxHash: strings.Repeat(fmt.Sprintf("%02x", 0x60+i), 32), GasWanted: 400000, GasUsed: 219118, Fee: "8000utia", FeePayer: "celestia1pub",
			Messages: []failedtx.Msg{{Index: 0, TypeURL: "/celestia.fibre.v1.MsgPayForFibre", Signer: "celestia1pub"}}, RecordedAt: time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)}
		fl, err := json.Marshal(f)
		if err != nil {
			t.Fatal(err)
		}
		cl, err := json.Marshal(c)
		if err != nil {
			t.Fatal(err)
		}
		failed, costs = append(failed, fl), append(costs, cl)
	}
	return failed, costs
}

// A day whose export carries backfilled failed_txs.jsonl and tx_costs.jsonl
// lines, late, after its own, is checked as the same day without either
// file: the export intact and reproducible, both members held to their
// manifest digests alone and counted late in the manifest, and every
// checked member's report and ledger entry the same.
func TestADayWithBackfilledLinesIsCheckedAsTheDayWithout(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t)
	lateFailed, lateCosts := backfilledLines(t)
	failed, costs := append(failedTxLines(t), lateFailed...), append(txCostLines(t), lateCosts...)
	// as the collector stores them
	for _, l := range failed {
		var r failedtx.Record
		if err := json.Unmarshal(l, &r); err != nil {
			t.Fatal(err)
		}
		if ok, err := f.st.InsertFailedTx(r, l); err != nil || !ok {
			t.Fatalf("failed tx: %v %v", ok, err)
		}
	}
	for _, l := range costs {
		var r txcost.Record
		if err := json.Unmarshal(l, &r); err != nil {
			t.Fatal(err)
		}
		if ok, err := f.st.InsertTxCost(r, l); err != nil || !ok {
			t.Fatalf("tx cost: %v %v", ok, err)
		}
	}
	check := func(with bool) (DayReport, LedgerDay, map[string][2]int64) {
		t.Helper()
		data := t.TempDir()
		files := map[string][]byte{
			"publications.jsonl": file(f.pubLine),
			"measurements.jsonl": file(f.readings...),
			"reachability.jsonl": file(f.reachLines...),
		}
		if with {
			files[failedtx.FileName], files[txcost.FileName] = file(failed...), file(costs...)
		}
		writeFiles(t, data, files)
		dir, e := exportDay(t, data, time.Date(2026, 10, 6, 4, 0, 0, 0, time.UTC))
		counts := map[string][2]int64{}
		for _, m := range e.Manifest.Files {
			counts[m.Name] = [2]int64{m.Lines, m.LateLines}
		}
		r, err := CheckDay(ctx, f.st, dir, e)
		if err != nil {
			t.Fatal(err)
		}
		d, ok := r.LedgerDay("t", time.Date(2026, 10, 6, 5, 0, 0, 0, time.UTC))
		if !ok {
			t.Fatalf("nothing to record: %+v", r)
		}
		return r, d, counts
	}
	with, ledgerWith, counts := check(true)
	without, ledgerWithout, _ := check(false)

	for _, name := range []string{failedtx.FileName, txcost.FileName} {
		if counts[name] != [2]int64{4, 2} {
			t.Errorf("%s: lines and late lines %v, want 4 and 2", name, counts[name])
		}
		if !slices.Contains(with.NotChecked, name) {
			t.Errorf("%s is not among the members held to their digest alone: %v", name, with.NotChecked)
		}
	}
	if !with.ExportIntact || !with.Reproducible || len(with.ExportErrors) != 0 {
		t.Fatalf("with backfilled lines: %+v", with)
	}
	if !reflect.DeepEqual(with.Files, without.Files) {
		t.Fatalf("the checked members differ:\n%+v\n%+v", with.Files, without.Files)
	}
	if !reflect.DeepEqual(ledgerWith.Files, ledgerWithout.Files) || !ledgerWith.Reproducible() {
		t.Fatalf("the ledger entries differ:\n%+v\n%+v", ledgerWith.Files, ledgerWithout.Files)
	}
}
