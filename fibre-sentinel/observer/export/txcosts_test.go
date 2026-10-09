package export

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

const txCostsName = "tx_costs.jsonl"

// txCostLine is a tx_costs.jsonl line as sentinel-scan writes it, dated ts
// (internal/txcost.Record; only its time dates it here).
func txCostLine(ts string, height int64) string {
	return fmt.Sprintf(`{"schema_version":1,"dedupe_key":"h%d:0","height":%d,"tx_index":0,"time":%q,"tx_hash":"%064x","gas_wanted":400000,"gas_used":219118,`+
		`"fee":"8000utia","fee_payer":"celestia1pub","messages":[{"index":0,"type_url":"/celestia.fibre.v1.MsgPayForFibre","signer":"celestia1pub"}],`+
		`"recorded_at":%q}`+"\n", height, height, ts, height, ts)
}

// txCostsData is failedTxsData's data dir, with failed transactions, and
// with costs (the lines of tx_costs.jsonl) when it is not empty.
func txCostsData(t *testing.T, costs string) string {
	t.Helper()
	data := failedTxsData(t, failedTxLine("2026-09-10T08:00:00Z", 90))
	if costs != "" {
		if err := os.WriteFile(filepath.Join(data, txCostsName), []byte(costs), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return data
}

// tx_costs.jsonl is in every export as the observer's last file, before the
// other vantages' heartbeats, dated by its records' time like any other
// record file: a line goes with its block's day, a line that reached the
// file after its day's export goes, late, with the next, and the members of
// consecutive exports tile the file. Each export reads back whole against
// its manifest, its sidecar and its signature.
func TestBuilder_ExportsTxCostsLastAndByTime(t *testing.T) {
	d1, d2 := "2026-09-10", "2026-09-11"
	if last := Files[len(Files)-1]; last.Name != txCostsName || last.TimeField != "time" {
		t.Fatalf("the last of Files is %+v", last)
	}
	data := txCostsData(t, txCostLine(d1+"T08:00:00.123456789Z", 100)+txCostLine(d1+"T23:59:59Z", 200)+txCostLine(d2+"T00:00:01Z", 300))
	path := filepath.Join(data, txCostsName)
	dir := filepath.Join(data, "exports")
	signer := newTestSigner(t)
	b := &Builder{DataDir: data, Dir: dir, Vantage: "v", Build: "x", Hour: 3, Signer: signer}

	built, err := b.Run(time.Date(2026, 9, 11, 4, 0, 0, 0, time.UTC))
	if err != nil || len(built) != 1 {
		t.Fatalf("d1: built %v err %v", built, err)
	}
	a := readBuilt(t, dir, built[0], signer)
	i, m := memberOf(t, a, txCostsName)
	if i != len(Files)-1 || a.Manifest.Files[i-1].Name != failedTxsName || a.Manifest.Files[i+1].Name != "vantages/de-1/reachability.jsonl" ||
		len(a.Manifest.Files) != len(Files)+1 {
		t.Fatalf("members %+v: want %s last of the observer's files, the vantage after it", a.Manifest.Files, txCostsName)
	}
	want := txCostLine(d1+"T08:00:00.123456789Z", 100) + txCostLine(d1+"T23:59:59Z", 200)
	if m.TimeField != "time" || m.Lines != 2 || m.LateLines != 0 || m.From != 0 || string(a.Members[txCostsName]) != want {
		t.Fatalf("d1: member %+v holds %q", m, a.Members[txCostsName])
	}
	tiles := append([]byte(nil), a.Members[txCostsName]...)

	// one for d1 the scanner wrote after d1's export (a re-scan), and one for d2
	appendTo(t, path, txCostLine(d1+"T12:00:00Z", 150)+txCostLine(d2+"T06:00:00Z", 400))
	built, err = b.Run(time.Date(2026, 9, 12, 4, 0, 0, 0, time.UTC))
	if err != nil || len(built) != 1 {
		t.Fatalf("d2: built %v err %v", built, err)
	}
	a = readBuilt(t, dir, built[0], signer)
	_, m2 := memberOf(t, a, txCostsName)
	want = txCostLine(d2+"T00:00:01Z", 300) + txCostLine(d1+"T12:00:00Z", 150) + txCostLine(d2+"T06:00:00Z", 400)
	if m2.From != m.To || m2.Lines != 3 || m2.LateLines != 1 || string(a.Members[txCostsName]) != want {
		t.Fatalf("d2: member %+v holds %q, want from %d", m2, a.Members[txCostsName], m.To)
	}
	tiles = append(tiles, a.Members[txCostsName]...)
	whole, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(tiles, whole) || m2.To != int64(len(whole)) {
		t.Fatalf("the members do not tile the file: %q (to %d) vs %q", tiles, m2.To, whole)
	}
}

// The member changes nothing else: every other member of an export built
// with cost lines has the bytes and the manifest entry of the same export
// built from the same data dir without them, failed_txs.jsonl included.
// Without the file the member is there, empty.
func TestBuilder_TxCostsChangeNoOtherMember(t *testing.T) {
	d1 := "2026-09-10"
	with := txCostsData(t, txCostLine(d1+"T08:00:00Z", 100)+txCostLine(d1+"T09:00:00Z", 101))
	without := txCostsData(t, "")
	now := time.Date(2026, 9, 11, 4, 0, 0, 0, time.UTC)
	signer := newTestSigner(t)
	build := func(data string) *Archive {
		t.Helper()
		dir := filepath.Join(data, "exports")
		b := &Builder{DataDir: data, Dir: dir, Vantage: "v", Build: "x", Hour: 3, Signer: signer}
		built, err := b.Run(now)
		if err != nil || len(built) != 1 {
			t.Fatalf("built %v err %v", built, err)
		}
		return readBuilt(t, dir, built[0], signer)
	}
	a, b := build(with), build(without)
	if len(a.Manifest.Files) != len(b.Manifest.Files) {
		t.Fatalf("%d members with tx costs, %d without", len(a.Manifest.Files), len(b.Manifest.Files))
	}
	checked := 0
	for i, m := range b.Manifest.Files {
		if m.Name == txCostsName {
			if m.Lines != 0 || m.Bytes != 0 || m.SHA256 != emptySHA || m.From != 0 || m.To != 0 || len(b.Members[m.Name]) != 0 {
				t.Errorf("no file: the member is %+v, want an empty one", m)
			}
			if got := a.Manifest.Files[i]; got.Name != txCostsName || got.Lines != 2 || got.SHA256 == emptySHA {
				t.Errorf("with the file: the member at its place is %+v", got)
			}
			continue
		}
		if a.Manifest.Files[i] != m || !bytes.Equal(a.Members[m.Name], b.Members[m.Name]) {
			t.Errorf("%s: %+v with tx costs, %+v without", m.Name, a.Manifest.Files[i], m)
		}
		if m.Name == failedTxsName && m.Lines != 1 {
			t.Errorf("the failed transactions member is %+v", m)
		}
		checked++
	}
	if checked != len(Files) {
		t.Fatalf("%d other members compared, want %d", checked, len(Files))
	}
	if *a.Manifest.State != *b.Manifest.State || !bytes.Equal(a.Members[StateFile], b.Members[StateFile]) {
		t.Errorf("state: %+v, %+v", a.Manifest.State, b.Manifest.State)
	}
}
