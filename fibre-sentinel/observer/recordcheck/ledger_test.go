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
	// The tarball is the one its index entry and sidecar name, but a member is not what the entry's manifest says: a
	// fault of the export as built, there for as long as these bytes are.
	e.Manifest.Files = append([]export.Member(nil), e.Manifest.Files...)
	for i := range e.Manifest.Files {
		if e.Manifest.Files[i].Name == "publications.jsonl" {
			e.Manifest.Files[i].SHA256 = digest(nil)
		}
	}
	r, err := CheckDay(context.Background(), f.st, dir, e)
	if err != nil || r.ExportIntact {
		t.Fatalf("%+v %v", r, err)
	}
	d, ok := r.LedgerDay("b1", time.Now())
	if !ok || d.SHA256 != e.SHA256 || d.Reproducible() || len(d.Files) == 0 {
		t.Fatalf("%+v ok %v", d, ok)
	}
	for name, m := range d.Files {
		if m.Reproducible || !strings.HasPrefix(m.Why, "the export is not intact (publications.jsonl: ") {
			t.Fatalf("%s: %+v", name, m)
		}
	}
}

// A check whose answer is not about the tarball's bytes alone, made against an index entry read before the day was
// rebuilt or with a sidecar that disagrees, records nothing: the day is checked again on the next run, and once the
// export agrees with itself the ledger holds it as reproducible.
func TestLedgerRecordsNoAnswerNotAboutTheTarball(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	data := t.TempDir()
	writeFiles(t, data, map[string][]byte{
		"publications.jsonl": file(f.pubLine),
		"measurements.jsonl": file(f.readings...),
		"reachability.jsonl": file(f.reachLines...),
	})
	dir, stale := exportDay(t, data, time.Date(2026, 10, 6, 4, 0, 0, 0, time.UTC))
	path := filepath.Join(dir, LedgerFile)
	// record-verify read the index (stale); the builder then rebuilt the day (its state lost in a crash).
	if err := os.Remove(filepath.Join(dir, "state.json")); err != nil {
		t.Fatal(err)
	}
	_, e := exportDay(t, data, time.Date(2026, 10, 6, 7, 0, 0, 0, time.UTC))
	if e.Name != stale.Name || e.SHA256 == stale.SHA256 {
		t.Fatalf("the rebuilt export is %s %s, want %s under another digest", e.Name, e.SHA256, stale.Name)
	}
	checkAndRecord := func(e export.Entry) (DayReport, bool) {
		t.Helper()
		r, err := CheckDay(ctx, f.st, dir, e)
		if err != nil {
			t.Fatal(err)
		}
		d, ok := r.LedgerDay("b1", time.Now())
		if ok {
			if err := MergeLedger(path, map[string]LedgerDay{e.Name: d}); err != nil {
				t.Fatal(err)
			}
		}
		return r, ok
	}
	if r, ok := checkAndRecord(stale); r.ExportIntact || ok {
		t.Fatalf("the check against the stale entry: intact %v, recorded %v: %v", r.ExportIntact, ok, r.ExportErrors)
	}
	sidecar := filepath.Join(dir, e.Name+".sha256")
	good, err := os.ReadFile(sidecar)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(sidecar, []byte(digest(nil)+"  "+e.Name+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if r, ok := checkAndRecord(e); r.ExportIntact || ok {
		t.Fatalf("the check with a wrong sidecar: intact %v, recorded %v: %v", r.ExportIntact, ok, r.ExportErrors)
	}
	l, err := ReadLedger(path)
	if err != nil {
		t.Fatal(err)
	}
	if d, ok, err := l.Current(dir, e.Name); err != nil || ok {
		t.Fatalf("the ledger answers for the day from a check that was not about its bytes: %+v ok %v err %v", d, ok, err)
	}

	// The sidecar back as the builder wrote it, the next run checks the day and records it as it is.
	if err := os.WriteFile(sidecar, good, 0o644); err != nil {
		t.Fatal(err)
	}
	if r, ok := checkAndRecord(e); !r.Reproducible || !ok {
		t.Fatalf("the check of the rebuilt day: %+v recorded %v", r, ok)
	}
	if l, err = ReadLedger(path); err != nil {
		t.Fatal(err)
	}
	if d, ok, err := l.Current(dir, e.Name); err != nil || !ok || !d.Reproducible() {
		t.Fatalf("the ledger does not hold the rebuilt day as reproducible: %+v ok %v err %v", d, ok, err)
	}
}

// A merge waits for another writer's lock on the ledger, so it reads the ledger only once the other has put its own in
// place, and the days the other merged are kept.
func TestMergeLedgerWaitsForAnotherWriter(t *testing.T) {
	if !ledgerLocking {
		t.Skip("the ledger's writers are kept apart only where flock is")
	}
	path := filepath.Join(t.TempDir(), LedgerFile)
	day := func(build string) LedgerDay {
		return LedgerDay{SHA256: "ab", Build: build, Files: map[string]LedgerMember{"publications.jsonl": {Reproducible: true}}}
	}
	unlock, err := lockLedger(path)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		done <- MergeLedger(path, map[string]LedgerDay{"tensile-ut-1-2026-10-05.tar.gz": day("second")})
	}()
	select {
	case err := <-done:
		t.Fatalf("merged while another writer held the ledger: %v", err)
	case <-time.After(300 * time.Millisecond):
	}
	// The first writer puts its ledger in place, as its own merge would, and lets go.
	raw, _ := json.Marshal(Ledger{"tensile-ut-1-2026-10-04.tar.gz": day("first")})
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatal(err)
	}
	unlock()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	l, err := ReadLedger(path)
	if err != nil {
		t.Fatal(err)
	}
	if l["tensile-ut-1-2026-10-04.tar.gz"].Build != "first" || l["tensile-ut-1-2026-10-05.tar.gz"].Build != "second" {
		t.Fatalf("a writer's day was dropped: %+v", l)
	}
}
