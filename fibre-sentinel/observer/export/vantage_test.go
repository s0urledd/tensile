package export

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// The other vantages' heartbeats are in the daily exports like the
// observer's own files: one member per vantages/<name>/reachability.jsonl,
// named by that path, after the observer's files and sorted by name. The
// first export that sees a vantage starts its file at offset 0 and counts
// the older lines late, the next continues where it stopped, and the
// members of successive exports tile the file. The tarball reads back and
// checks against its manifest with the nested names.
func TestBuilder_ExportsOtherVantages(t *testing.T) {
	data := t.TempDir()
	dir := filepath.Join(data, "exports")
	d1, d2, d3 := "2026-09-10", "2026-09-11", "2026-09-12"
	if err := os.WriteFile(filepath.Join(data, "measurements.jsonl"), []byte(line("started_at", d1+"T10:00:00Z", "m1")), 0o644); err != nil {
		t.Fatal(err)
	}
	b := &Builder{DataDir: data, Dir: dir, Vantage: "v", Build: "x", Hour: 3}
	// d1 is exported before any other vantage's file is there.
	if built, err := b.Run(time.Date(2026, 9, 11, 4, 0, 0, 0, time.UTC)); err != nil || len(built) != 1 {
		t.Fatalf("d1: built %v err %v", built, err)
	}

	de1 := filepath.Join(data, VantagesDir, "de-1", "reachability.jsonl")
	ab2 := filepath.Join(data, VantagesDir, "ab-2", "reachability.jsonl")
	for _, p := range []string{de1, ab2} {
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	first := line("started_at", d1+"T10:00:00Z", "v1") + line("started_at", d2+"T10:00:00Z", "v2") + line("started_at", d3+"T01:00:00Z", "v3")
	if err := os.WriteFile(de1, []byte(first), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(ab2, []byte(line("started_at", d2+"T05:00:00Z", "a1")), 0o644); err != nil {
		t.Fatal(err)
	}
	const de1Name, ab2Name = "vantages/de-1/reachability.jsonl", "vantages/ab-2/reachability.jsonl"

	var tiles []byte
	check := func(now time.Time, wantFrom int64, wantBody string, wantLate int64) Member {
		t.Helper()
		built, err := b.Run(now)
		if err != nil || len(built) != 1 {
			t.Fatalf("built %v err %v", built, err)
		}
		raw, err := os.ReadFile(filepath.Join(dir, built[0]))
		if err != nil {
			t.Fatal(err)
		}
		a, err := ReadArchive(raw)
		if err != nil {
			t.Fatalf("%s: %v", built[0], err)
		}
		if p := a.CheckMembers(); len(p) != 0 {
			t.Fatalf("%s: %v", built[0], p)
		}
		n := len(a.Manifest.Files)
		if n != len(Files)+2 || a.Manifest.Files[n-2].Name != ab2Name || a.Manifest.Files[n-1].Name != de1Name {
			t.Fatalf("%s: members %+v, want the observer's files then %s and %s", built[0], a.Manifest.Files, ab2Name, de1Name)
		}
		m := a.Manifest.Files[n-1]
		if m.TimeField != "started_at" || m.From != wantFrom || m.LateLines != wantLate || string(a.Members[de1Name]) != wantBody {
			t.Fatalf("%s: %s member %+v holds %q, want from %d, %d late, %q", built[0], de1Name, m, a.Members[de1Name], wantFrom, wantLate, wantBody)
		}
		tiles = append(tiles, a.Members[de1Name]...)
		return m
	}
	// d2: the vantage's file from its first byte, its d1 line late; the d3
	// line waits for d3's export.
	m := check(time.Date(2026, 9, 12, 4, 0, 0, 0, time.UTC), 0,
		line("started_at", d1+"T10:00:00Z", "v1")+line("started_at", d2+"T10:00:00Z", "v2"), 1)

	f, err := os.OpenFile(de1, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	f.WriteString(line("started_at", d3+"T05:00:00Z", "v4"))
	f.Close()
	// d3: from where d2's member stopped.
	m = check(time.Date(2026, 9, 13, 4, 0, 0, 0, time.UTC), m.To,
		line("started_at", d3+"T01:00:00Z", "v3")+line("started_at", d3+"T05:00:00Z", "v4"), 0)

	whole, err := os.ReadFile(de1)
	if err != nil {
		t.Fatal(err)
	}
	if string(tiles) != string(whole) || m.To != int64(len(whole)) {
		t.Fatalf("the members do not tile the file: %q (to %d) vs %q", tiles, m.To, whole)
	}
	st, err := b.loadState()
	if err != nil {
		t.Fatal(err)
	}
	if st.Offsets[de1Name] != int64(len(whole)) || st.Offsets[ab2Name] == 0 {
		t.Fatalf("state offsets %v, want %s at %d keyed by its member name", st.Offsets, de1Name, len(whole))
	}
}
