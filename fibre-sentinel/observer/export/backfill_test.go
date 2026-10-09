package export

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// A backfill (cmd/sentinel-txbackfill) appends failed_txs.jsonl and
// tx_costs.jsonl lines dated weeks before the first export, of heights
// below every line in the file, after the newer lines the scanner wrote.
// They go into the next day's export, all of them counted late, between
// that day's own lines; the exports built before them stay byte for byte
// what they were (tarball, sidecar, signature and index entry), and the
// members of all the exports still tile each file.
func TestBuilder_BackfilledLinesGoLateIntoTheNextExport(t *testing.T) {
	d1, d2, d3, d4 := "2026-09-10", "2026-09-11", "2026-09-12", "2026-09-13"
	data := txCostsData(t, txCostLine(d1+"T08:00:00Z", 100))
	failedPath, costsPath := filepath.Join(data, failedTxsName), filepath.Join(data, txCostsName)
	dir := filepath.Join(data, "exports")
	signer := newTestSigner(t)
	b := &Builder{DataDir: data, Dir: dir, Vantage: "v", Build: "x", Hour: 3, Signer: signer}
	members := map[string][]byte{}
	build := func(day string, now time.Time) *Archive {
		t.Helper()
		built, err := b.Run(now)
		if err != nil || len(built) != 1 || built[0] != b.name(day) {
			t.Fatalf("%s: built %v err %v", day, built, err)
		}
		a := readBuilt(t, dir, built[0], signer)
		for _, n := range []string{failedTxsName, txCostsName} {
			members[n] = append(members[n], a.Members[n]...)
		}
		return a
	}
	build(d1, time.Date(2026, 9, 11, 4, 0, 0, 0, time.UTC))
	appendTo(t, failedPath, failedTxLine(d2+"T10:00:00Z", 190))
	appendTo(t, costsPath, txCostLine(d2+"T10:00:00Z", 191))
	build(d2, time.Date(2026, 9, 12, 4, 0, 0, 0, time.UTC))

	// The sealed exports, as they stand before the backfill.
	sealed := map[string][]byte{}
	entries := map[string]string{}
	for _, day := range []string{d1, d2} {
		name := b.name(day)
		for _, f := range []string{name, name + ".sha256", name + ".sig"} {
			raw, err := os.ReadFile(filepath.Join(dir, f))
			if err != nil {
				t.Fatal(err)
			}
			sealed[f] = raw
		}
	}
	readEntries := func() map[string]string {
		t.Helper()
		idx, err := ReadIndex(dir)
		if err != nil {
			t.Fatal(err)
		}
		out := map[string]string{}
		for _, e := range idx {
			j, _ := json.Marshal(e)
			out[e.Name] = string(j)
		}
		return out
	}
	for name, j := range readEntries() {
		entries[name] = j
	}

	// d3: the scanner's line, the merge's (activation's days, low heights),
	// the scanner's lines again once started, one of them of d4.
	backfill := func(line func(string, int64) string) string {
		return line("2026-08-20T09:00:00.5Z", 10) + line("2026-08-27T12:00:00Z", 20) + line("2026-09-05T23:59:59Z", 30)
	}
	appendTo(t, failedPath, failedTxLine(d3+"T01:00:00Z", 300)+backfill(failedTxLine)+failedTxLine(d3+"T02:00:00Z", 301)+failedTxLine(d4+"T00:00:01Z", 400))
	appendTo(t, costsPath, txCostLine(d3+"T01:00:00Z", 302)+backfill(txCostLine)+txCostLine(d3+"T02:00:00Z", 303)+txCostLine(d4+"T00:00:01Z", 401))
	a := build(d3, time.Date(2026, 9, 13, 4, 0, 0, 0, time.UTC))
	for name, want := range map[string]string{
		failedTxsName: failedTxLine(d3+"T01:00:00Z", 300) + backfill(failedTxLine) + failedTxLine(d3+"T02:00:00Z", 301),
		txCostsName:   txCostLine(d3+"T01:00:00Z", 302) + backfill(txCostLine) + txCostLine(d3+"T02:00:00Z", 303),
	} {
		_, m := memberOf(t, a, name)
		if m.Lines != 5 || m.LateLines != 3 || m.SkewedLines != 0 || string(a.Members[name]) != want {
			t.Errorf("d3's %s: %+v holds\n%s\nwant\n%s", name, m, a.Members[name], want)
		}
	}
	for f, raw := range sealed {
		got, err := os.ReadFile(filepath.Join(dir, f))
		if err != nil || !bytes.Equal(got, raw) {
			t.Errorf("%s changed after the backfill (%v)", f, err)
		}
	}
	after := readEntries()
	for name, j := range entries {
		if after[name] != j {
			t.Errorf("the index entry of %s changed:\n%s\n%s", name, j, after[name])
		}
	}

	a = build(d4, time.Date(2026, 9, 14, 4, 0, 0, 0, time.UTC))
	for name, path := range map[string]string{failedTxsName: failedPath, txCostsName: costsPath} {
		if _, m := memberOf(t, a, name); m.Lines != 1 || m.LateLines != 0 {
			t.Errorf("d4's %s: %+v", name, m)
		}
		whole, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(members[name], whole) {
			t.Errorf("the members of %s do not tile it:\n%s\nvs\n%s", name, members[name], whole)
		}
	}
	// built again at the same time: nothing is due, nothing changes
	if built, err := b.Run(time.Date(2026, 9, 14, 4, 0, 0, 0, time.UTC)); err != nil || len(built) != 0 {
		t.Fatalf("again: %v %v", built, err)
	}
}
