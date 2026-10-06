package recordcheck

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/probe"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/export"
)

// writeFiles writes each record file under data, by its path relative to it.
func writeFiles(t *testing.T, data string, files map[string][]byte) {
	t.Helper()
	for name, b := range files {
		p := filepath.Join(data, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, b, 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

// exportDay builds the fixture's day (2026-10-05) from data with the export builder, as the collector does, and
// returns the exports dir and the day's index entry.
func exportDay(t *testing.T, data string, now time.Time) (string, export.Entry) {
	t.Helper()
	dir := filepath.Join(data, "exports")
	b := &export.Builder{DataDir: data, Dir: dir, Vantage: "ut-1", Build: "t", Hour: 3}
	built, err := b.Run(now)
	if err != nil || len(built) != 1 {
		t.Fatalf("built %v err %v", built, err)
	}
	idx, err := export.ReadIndex(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range idx {
		if e.Name == built[0] {
			return dir, e
		}
	}
	t.Fatalf("%s is not in the index", built[0])
	return "", export.Entry{}
}

// The ledger records each checked member's answer under the tarball's digest, keeps the days it is not given, and
// stops answering for a tarball once its bytes change: a rebuilt day is checked again and its entry replaced.
func TestLedgerMergesAndRechecksAChangedTarball(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	// One heartbeat the store never received: the file is not reproducible, and the ledger says why.
	var m probe.Measurement
	if err := json.Unmarshal(f.reachLines[0], &m); err != nil {
		t.Fatal(err)
	}
	m.ScheduledAt = m.ScheduledAt.Add(time.Second)
	lost, _ := json.Marshal(m)
	data := t.TempDir()
	writeFiles(t, data, map[string][]byte{
		"publications.jsonl": file(f.pubLine),
		"measurements.jsonl": file(f.readings...),
		"reachability.jsonl": file(append(append([][]byte(nil), f.reachLines...), lost)...),
	})
	dir, e := exportDay(t, data, time.Date(2026, 10, 6, 4, 0, 0, 0, time.UTC))
	path := filepath.Join(dir, LedgerFile)
	other := LedgerDay{SHA256: "ab", CheckedAt: time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC), Build: "old",
		Files: map[string]LedgerMember{"publications.jsonl": {Reproducible: true}}}
	if err := MergeLedger(path, map[string]LedgerDay{"tensile-ut-1-2026-09-30.tar.gz": other}); err != nil {
		t.Fatal(err)
	}

	check := func(at time.Time) LedgerDay {
		t.Helper()
		r, err := CheckDay(ctx, f.st, dir, e)
		if err != nil {
			t.Fatal(err)
		}
		d, ok := r.LedgerDay("b1", at)
		if !ok {
			t.Fatalf("nothing to record: %+v", r)
		}
		if err := MergeLedger(path, map[string]LedgerDay{e.Name: d}); err != nil {
			t.Fatal(err)
		}
		return d
	}
	first := check(time.Date(2026, 10, 6, 5, 0, 0, 0, time.UTC))
	l, err := ReadLedger(path)
	if err != nil {
		t.Fatal(err)
	}
	got, ok, err := l.Current(dir, e.Name)
	if err != nil || !ok || got.SHA256 != e.SHA256 || got.Build != "b1" || !got.CheckedAt.Equal(first.CheckedAt) {
		t.Fatalf("the ledger does not hold the day under its tarball's digest: %+v ok %v err %v", got, ok, err)
	}
	if !got.Files["measurements.jsonl"].Reproducible || got.Files["measurements.jsonl"].Lines != int64(len(f.readings)) || got.Files["measurements.jsonl"].Why != "" {
		t.Fatalf("measurements: %+v", got.Files["measurements.jsonl"])
	}
	reach := got.Files["reachability.jsonl"]
	if reach.Reproducible || reach.Identical != int64(len(f.reachLines)) || !strings.HasPrefix(reach.Why, "1 lines missing (first missing: ") || got.Reproducible() {
		t.Fatalf("reachability: %+v", reach)
	}
	if _, ok := got.Files["registry.jsonl"]; ok {
		t.Fatalf("a member the store does not keep line by line is in the ledger: %+v", got.Files)
	}
	if l["tensile-ut-1-2026-09-30.tar.gz"].Build != "old" {
		t.Fatalf("the merge dropped another day: %+v", l)
	}

	// The day is rebuilt (the export's state lost, the same lines exported again at another hour): another tarball,
	// which the ledger's answer is not about.
	if err := os.Remove(filepath.Join(dir, "state.json")); err != nil {
		t.Fatal(err)
	}
	_, e2 := exportDay(t, data, time.Date(2026, 10, 6, 7, 0, 0, 0, time.UTC))
	if e2.Name != e.Name || e2.SHA256 == e.SHA256 {
		t.Fatalf("the rebuilt export is %s %s, want %s under another digest", e2.Name, e2.SHA256, e.Name)
	}
	if _, ok, err := l.Current(dir, e.Name); err != nil || ok {
		t.Fatalf("the ledger still answers for a changed tarball: ok %v err %v", ok, err)
	}
	e = e2
	check(time.Date(2026, 10, 6, 8, 0, 0, 0, time.UTC))
	if l, err = ReadLedger(path); err != nil {
		t.Fatal(err)
	}
	if got, ok, err := l.Current(dir, e.Name); err != nil || !ok || got.SHA256 != e2.SHA256 || got.CheckedAt.Equal(first.CheckedAt) {
		t.Fatalf("the re-check did not replace the entry: %+v ok %v err %v", got, ok, err)
	}
	if len(l) != 2 {
		t.Fatalf("ledger days %d, want 2", len(l))
	}
}

// A member of an export that is not intact is never recorded as reproducible, whatever its lines did: the export is
// what a removed segment would be read back from.
func TestLedgerRecordsNothingReproducibleFromABrokenExport(t *testing.T) {
	f := newFixture(t)
	data := t.TempDir()
	writeFiles(t, data, map[string][]byte{"publications.jsonl": file(f.pubLine)})
	dir, e := exportDay(t, data, time.Date(2026, 10, 6, 4, 0, 0, 0, time.UTC))
	if err := os.WriteFile(filepath.Join(dir, e.Name+".sha256"), []byte(digest(nil)+"  "+e.Name+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	r, err := CheckDay(context.Background(), f.st, dir, e)
	if err != nil || r.ExportIntact {
		t.Fatalf("%+v %v", r, err)
	}
	d, ok := r.LedgerDay("b1", time.Now())
	if !ok || d.Reproducible() || len(d.Files) == 0 {
		t.Fatalf("%+v", d)
	}
	for name, m := range d.Files {
		if m.Reproducible || !strings.HasPrefix(m.Why, "the export is not intact (sidecar: ") {
			t.Fatalf("%s: %+v", name, m)
		}
	}
}
