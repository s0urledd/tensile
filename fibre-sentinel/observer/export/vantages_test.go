package export

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// Another vantage's answers are what a not-served count rests on, so every
// export carries them, dated by their own start, at the path an untarred
// export holds them for sentinel-recompute: vantages/<name>/measurements.jsonl.
// A directory the pull would not have written, and this observer's own
// name, are not exported; a vantage once exported stays in the manifest.
func TestBuilder_CarriesTheOtherVantagesAnswers(t *testing.T) {
	data := t.TempDir()
	dir := filepath.Join(data, "exports")
	d1, d2 := "2026-09-10", "2026-09-11"
	write := func(name, body string) {
		p := filepath.Join(data, VantagesMemberDir, name, "measurements.jsonl")
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write("de-1", line("started_at", d1+"T23:58:00Z", "a")+line("started_at", d2+"T00:04:00Z", "b"))
	write("Bad_Name", line("started_at", d1+"T10:00:00Z", "x"))
	write("ut-1", line("started_at", d1+"T10:00:00Z", "own"))
	b := &Builder{DataDir: data, Dir: dir, Vantage: "ut-1", Build: "abc", Hour: 3}
	built, err := b.Run(time.Date(2026, 9, 11, 4, 0, 0, 0, time.UTC))
	if err != nil || len(built) != 1 {
		t.Fatalf("built %v err %v", built, err)
	}
	raw, err := os.ReadFile(filepath.Join(dir, built[0]))
	if err != nil {
		t.Fatal(err)
	}
	a, err := ReadArchive(raw)
	if err != nil {
		t.Fatal(err)
	}
	if p := a.CheckMembers(); len(p) != 0 {
		t.Fatalf("the export does not verify: %v", p)
	}
	member := VantageMember("de-1")
	if member != "vantages/de-1/measurements.jsonl" {
		t.Fatalf("member %q", member)
	}
	if got := string(a.Members[member]); got != line("started_at", d1+"T23:58:00Z", "a") {
		t.Fatalf("de-1's answers in the d1 export: %q", got)
	}
	if len(a.Manifest.Files) != len(Files)+1 || a.Manifest.Files[len(Files)].Name != member ||
		a.Manifest.Files[len(Files)].TimeField != "started_at" || a.Manifest.Files[len(Files)].Lines != 1 {
		t.Fatalf("manifest members after the record files: %+v", a.Manifest.Files[len(Files):])
	}
	for name := range a.Members {
		if name == VantageMember("Bad_Name") || name == VantageMember("ut-1") {
			t.Errorf("%s exported", name)
		}
	}

	// The next day carries the rest; with the file gone the day after, the
	// member stays, empty.
	built, err = b.Run(time.Date(2026, 9, 12, 4, 0, 0, 0, time.UTC))
	if err != nil || len(built) != 1 {
		t.Fatalf("d2: built %v err %v", built, err)
	}
	members := readTar(t, filepath.Join(dir, built[0]))
	if got := string(members[member]); got != line("started_at", d2+"T00:04:00Z", "b") {
		t.Fatalf("de-1's answers in the d2 export: %q", got)
	}
	if err := os.RemoveAll(filepath.Join(data, VantagesMemberDir, "de-1")); err != nil {
		t.Fatal(err)
	}
	built, err = b.Run(time.Date(2026, 9, 13, 4, 0, 0, 0, time.UTC))
	if err != nil || len(built) != 1 {
		t.Fatalf("d3: built %v err %v", built, err)
	}
	members = readTar(t, filepath.Join(dir, built[0]))
	var man Manifest
	if err := json.Unmarshal(members["manifest.json"], &man); err != nil {
		t.Fatal(err)
	}
	found := false
	for _, m := range man.Files {
		if m.Name == member {
			found = m.Lines == 0 && m.SHA256 == emptySHA
		}
	}
	if !found {
		t.Fatalf("d3: de-1's member is not in the manifest as an empty member: %+v", man.Files)
	}
}
