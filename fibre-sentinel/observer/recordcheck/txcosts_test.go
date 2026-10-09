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

// txCostLines are two successful Fibre transactions of the fixture's day
// (2026-10-05), as sentinel-scan writes their cost lines.
func txCostLines(t *testing.T) [][]byte {
	t.Helper()
	var out [][]byte
	for i := 0; i < 2; i++ {
		at := time.Date(2026, 10, 5, 14, i, 0, 0, time.UTC)
		r := txcost.Record{SchemaVersion: txcost.SchemaVersion, DedupeKey: txcost.Key(1200+int64(i), 1), Height: 1200 + int64(i), TxIndex: 1, Time: at,
			TxHash: strings.Repeat(fmt.Sprintf("%02x", 0xc0+i), 32), GasWanted: 400000, GasUsed: 219118, Fee: "8000utia", FeePayer: "celestia1pub",
			Messages: []failedtx.Msg{{Index: 0, TypeURL: "/celestia.fibre.v1.MsgPayForFibre", Signer: "celestia1pub"}}, RecordedAt: at.Add(2 * time.Second)}
		l, err := json.Marshal(r)
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, l)
	}
	return out
}

// A day whose export carries cost lines is checked as the same day without
// them: the export intact and reproducible, tx_costs.jsonl held to its
// manifest digest alone (no kind the store gives back line by line), every
// checked member's report and ledger entry the same, and no ledger entry for
// tx_costs.jsonl.
func TestADayWithTxCostsIsCheckedAsTheDayWithout(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t)
	// as the collector stores them: they are in the store, and still no check reads them
	for _, l := range txCostLines(t) {
		var r txcost.Record
		if err := json.Unmarshal(l, &r); err != nil {
			t.Fatal(err)
		}
		if ok, err := f.st.InsertTxCost(r, l); err != nil || !ok {
			t.Fatalf("tx cost: %v %v", ok, err)
		}
	}
	check := func(costs [][]byte) (DayReport, LedgerDay) {
		t.Helper()
		data := t.TempDir()
		files := map[string][]byte{
			"publications.jsonl": file(f.pubLine),
			"measurements.jsonl": file(f.readings...),
			"reachability.jsonl": file(f.reachLines...),
		}
		if costs != nil {
			files[txcost.FileName] = file(costs...)
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
	with, ledgerWith := check(txCostLines(t))
	without, ledgerWithout := check(nil)

	if !with.ExportIntact || !with.Reproducible || !slices.Contains(with.NotChecked, txcost.FileName) {
		t.Fatalf("with tx costs: %+v", with)
	}
	for _, fr := range with.Files {
		if fr.Name == txcost.FileName {
			t.Fatalf("tx_costs.jsonl was checked line by line: %+v", fr)
		}
	}
	if !reflect.DeepEqual(with.Files, without.Files) {
		t.Fatalf("the checked members differ:\n%+v\n%+v", with.Files, without.Files)
	}
	if !slices.Equal(with.NotChecked, without.NotChecked) {
		t.Fatalf("not checked: %v with tx costs, %v without", with.NotChecked, without.NotChecked)
	}
	if _, ok := ledgerWith.Files[txcost.FileName]; ok || !ledgerWith.Reproducible() {
		t.Fatalf("the ledger entry: %+v", ledgerWith.Files)
	}
	if !reflect.DeepEqual(ledgerWith.Files, ledgerWithout.Files) {
		t.Fatalf("the ledger entries differ:\n%+v\n%+v", ledgerWith.Files, ledgerWithout.Files)
	}
	if Checked(txcost.FileName) {
		t.Fatal("tx_costs.jsonl is a kind the store gives back line by line")
	}
}
