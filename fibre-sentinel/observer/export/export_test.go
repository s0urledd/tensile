package export

import (
	"archive/tar"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func line(field, ts, extra string) string {
	return `{"` + field + `":"` + ts + `","x":"` + extra + `"}` + "\n"
}

func readTar(t *testing.T, path string) map[string][]byte {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		t.Fatal(err)
	}
	tr := tar.NewReader(gz)
	out := map[string][]byte{}
	for {
		h, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(tr)
		out[h.Name] = b
	}
	return out
}

// Every line of every source file lands in exactly one export, the one
// of its own day or, when it arrived after that export was built, the
// next one, marked late; the manifest's digests match the members.
func TestBuilder_EveryLineInExactlyOneExport(t *testing.T) {
	data := t.TempDir()
	dir := filepath.Join(data, "exports")
	meas := filepath.Join(data, "measurements.jsonl")
	d1, d2, d3 := "2026-09-10", "2026-09-11", "2026-09-12"
	first := line("started_at", d1+"T10:00:00Z", "a") + line("started_at", d1+"T23:59:59Z", "b") +
		line("started_at", d2+"T00:00:01Z", "c") + line("started_at", d2+"T05:00:00Z", "d")
	if err := os.WriteFile(meas, []byte(first), 0o644); err != nil {
		t.Fatal(err)
	}
	// publications: one on d1, and a torn tail that must wait
	pubs := filepath.Join(data, "publications.jsonl")
	if err := os.WriteFile(pubs, []byte(line("settlement_time", d1+"T12:00:00Z", "p1")+`{"settlement_time":"`+d2), 0o644); err != nil {
		t.Fatal(err)
	}
	b := &Builder{DataDir: data, Dir: dir, Vantage: "eu/west 1", Build: "abc", Hour: 3}

	// d2 at 02:00: d1's grace has passed, d2's has not
	built, err := b.Run(time.Date(2026, 9, 12, 2, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if len(built) != 1 || built[0] != "tensile-eu-west-1-2026-09-10.tar.gz" {
		t.Fatalf("built %v", built)
	}
	members := readTar(t, filepath.Join(dir, built[0]))
	var man Manifest
	if err := json.Unmarshal(members["manifest.json"], &man); err != nil {
		t.Fatal(err)
	}
	if man.Day != d1 || man.Vantage != "eu/west 1" || man.Build != "abc" || man.Methodology == "" {
		t.Errorf("manifest = %+v", man)
	}
	byName := map[string]Member{}
	for _, m := range man.Files {
		byName[m.Name] = m
	}
	if got := string(members["measurements.jsonl"]); got != line("started_at", d1+"T10:00:00Z", "a")+line("started_at", d1+"T23:59:59Z", "b") {
		t.Errorf("d1 measurements member:\n%s", got)
	}
	if m := byName["measurements.jsonl"]; m.Lines != 2 || m.LateLines != 0 || m.From != 0 || m.To != int64(len(members["measurements.jsonl"])) {
		t.Errorf("measurements member = %+v", m)
	}
	if m := byName["publications.jsonl"]; m.Lines != 1 || string(members["publications.jsonl"]) != line("settlement_time", d1+"T12:00:00Z", "p1") {
		t.Errorf("publications member = %+v (torn tail must wait)", m)
	}
	if m := byName["payments.jsonl"]; m.Lines != 0 || m.SHA256 != emptySHA {
		t.Errorf("missing file must be an empty member: %+v", m)
	}
	for _, m := range man.Files {
		sum := sha256.Sum256(members[m.Name])
		if hex.EncodeToString(sum[:]) != m.SHA256 {
			t.Errorf("%s: manifest digest does not match the member", m.Name)
		}
	}
	// the sidecar digest is the tarball's
	raw, _ := os.ReadFile(filepath.Join(dir, built[0]))
	side, _ := os.ReadFile(filepath.Join(dir, built[0]+".sha256"))
	sum := sha256.Sum256(raw)
	if !strings.HasPrefix(string(side), hex.EncodeToString(sum[:])+"  "+built[0]) {
		t.Errorf("sidecar = %q", side)
	}

	// a late d1 row arrives after d1's export; d2 rows continue; then the
	// publications tail completes
	late := line("started_at", d1+"T22:00:00Z", "late") + line("started_at", d2+"T23:00:00Z", "e") + line("started_at", d3+"T00:30:00Z", "f")
	f, _ := os.OpenFile(meas, os.O_APPEND|os.O_WRONLY, 0o644)
	f.WriteString(late)
	f.Close()
	f, _ = os.OpenFile(pubs, os.O_APPEND|os.O_WRONLY, 0o644)
	f.WriteString(`T09:00:00Z","x":"p2"}` + "\n")
	f.Close()

	// still d2 at 02:00: nothing new is due
	if built, err := b.Run(time.Date(2026, 9, 12, 2, 30, 0, 0, time.UTC)); err != nil || len(built) != 0 {
		t.Fatalf("built %v err %v before the grace hour", built, err)
	}
	built, err = b.Run(time.Date(2026, 9, 12, 3, 0, 0, 0, time.UTC))
	if err != nil || len(built) != 1 {
		t.Fatalf("built %v err %v", built, err)
	}
	members = readTar(t, filepath.Join(dir, built[0]))
	if err := json.Unmarshal(members["manifest.json"], &man); err != nil {
		t.Fatal(err)
	}
	for _, m := range man.Files {
		byName[m.Name] = m
	}
	want := line("started_at", d2+"T00:00:01Z", "c") + line("started_at", d2+"T05:00:00Z", "d") +
		line("started_at", d1+"T22:00:00Z", "late") + line("started_at", d2+"T23:00:00Z", "e")
	if got := string(members["measurements.jsonl"]); got != want {
		t.Errorf("d2 measurements member:\n%s\nwant:\n%s", got, want)
	}
	if m := byName["measurements.jsonl"]; m.Lines != 4 || m.LateLines != 1 {
		t.Errorf("d2 measurements member = %+v, want 4 lines with 1 late", m)
	}
	if got := string(members["publications.jsonl"]); got != `{"settlement_time":"`+d2+`T09:00:00Z","x":"p2"}`+"\n" {
		t.Errorf("d2 publications member: %q", got)
	}
	// the d3 row waits for d3's export, from the offset the state recorded
	idx, err := ReadIndex(dir)
	if err != nil || len(idx) != 2 || idx[0].Day != d2 || idx[1].Day != d1 {
		t.Fatalf("index = %+v err %v", idx, err)
	}
	built, err = b.Run(time.Date(2026, 9, 13, 3, 0, 0, 0, time.UTC))
	if err != nil || len(built) != 1 {
		t.Fatalf("d3: built %v err %v", built, err)
	}
	members = readTar(t, filepath.Join(dir, built[0]))
	if got := string(members["measurements.jsonl"]); got != line("started_at", d3+"T00:30:00Z", "f") {
		t.Errorf("d3 measurements member: %q", got)
	}
}

func TestNamePattern(t *testing.T) {
	for _, ok := range []string{"tensile-local-2026-09-10.tar.gz", "fibrescope-local-2026-09-10.tar.gz", "fibrescope-eu-west.1-2026-09-10.tar.gz.sha256"} {
		if !NamePattern.MatchString(ok) {
			t.Errorf("%q must match", ok)
		}
	}
	for _, bad := range []string{"../x", "fibrescope-a-2026-09-10.tar", "tensile-a-2026-09-10.tar", "scope-a-2026-09-10.tar.gz", "index.json", "state.json", "fibrescope-a/b-2026-09-10.tar.gz"} {
		if NamePattern.MatchString(bad) {
			t.Errorf("%q must not match", bad)
		}
	}
}

// Builds advance one calendar day at a time up to yesterday, so a line dated
// years ahead — a forward clock step on the vantage — was a stop the export
// could never pass. That file's offset froze and every later export shipped an
// empty member for it: no error, no log line, nothing in the manifest but
// lines: 0, while the record went on growing behind it. For a product whose
// claim is that the export is the record, a file that silently stops
// exporting is the worst shape the failure could take.
func TestBuilder_ForwardClockStepDoesNotFreezeAFile(t *testing.T) {
	data := t.TempDir()
	dir := filepath.Join(data, "exports")
	meas := filepath.Join(data, "measurements.jsonl")
	body := line("started_at", "2026-09-10T10:00:00Z", "a") +
		line("started_at", "2099-01-01T00:00:00Z", "clock-jump") +
		line("started_at", "2026-09-10T10:00:01Z", "b") +
		line("started_at", "2026-09-11T10:00:00Z", "c")
	if err := os.WriteFile(meas, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	b := &Builder{DataDir: data, Dir: dir, Vantage: "v", Build: "x", Hour: 3}
	seen := map[string]bool{}
	skewed := int64(0)
	for _, now := range []time.Time{
		time.Date(2026, 9, 11, 3, 0, 0, 0, time.UTC),
		time.Date(2026, 9, 12, 3, 0, 0, 0, time.UTC),
		time.Date(2026, 9, 13, 3, 0, 0, 0, time.UTC),
	} {
		built, err := b.Run(now)
		if err != nil {
			t.Fatal(err)
		}
		for _, name := range built {
			m := readTar(t, filepath.Join(dir, name))
			for _, id := range []string{"a", "b", "c", "clock-jump"} {
				if strings.Contains(string(m["measurements.jsonl"]), `"`+id+`"`) {
					if seen[id] {
						t.Errorf("%s appears in more than one export", id)
					}
					seen[id] = true
				}
			}
			var man Manifest
			if err := json.Unmarshal(m["manifest.json"], &man); err != nil {
				t.Fatal(err)
			}
			for _, f := range man.Files {
				if f.Name == "measurements.jsonl" {
					skewed += f.SkewedLines
				}
			}
		}
	}
	for _, id := range []string{"a", "b", "c", "clock-jump"} {
		if !seen[id] {
			t.Errorf("%q is in the record but in no export: the file stopped exporting", id)
		}
	}
	if skewed != 1 {
		t.Errorf("manifests report %d skewed lines, want 1: the clock step must be named, not hidden", skewed)
	}
	// And the offset reached the end of the file.
	st, err := b.loadState()
	if err != nil {
		t.Fatal(err)
	}
	if st.Offsets["measurements.jsonl"] != int64(len(body)) {
		t.Errorf("offset stopped at %d of %d bytes", st.Offsets["measurements.jsonl"], len(body))
	}
}

// A catch-up build of several days in one run (the collector down, or the
// export held, past a midnight) judges a line's clock against the day it runs
// on. A line dated two days after the day being built is not a clock step
// then: it waits for its own day's export, and no manifest names a skew that
// never happened. A line dated the day the run is on waits for tomorrow's.
func TestBuilder_CatchUpLeavesLaterDaysToTheirOwnExports(t *testing.T) {
	data := t.TempDir()
	dir := filepath.Join(data, "exports")
	amends := filepath.Join(data, "amendments.jsonl")
	d0, d1, d3, d4 := "2026-10-04", "2026-10-05", "2026-10-07", "2026-10-08"
	if err := os.WriteFile(amends, []byte(line("judged_at", d0+"T10:00:00Z", "d0")), 0o644); err != nil {
		t.Fatal(err)
	}
	b := &Builder{DataDir: data, Dir: dir, Vantage: "v", Build: "x", Hour: 3}
	if built, err := b.Run(time.Date(2026, 10, 5, 3, 0, 0, 0, time.UTC)); err != nil || len(built) != 1 {
		t.Fatalf("d0: built %v, %v", built, err)
	}
	// Sparse: a line on d1, none on the 6th, two on d3, one on d4, the day
	// the catch-up runs on.
	f, err := os.OpenFile(amends, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	f.WriteString(line("judged_at", d1+"T10:00:00Z", "d1") + line("judged_at", d3+"T01:00:00Z", "d3a") +
		line("judged_at", d3+"T02:00:00Z", "d3b") + line("judged_at", d4+"T01:00:00Z", "d4"))
	f.Close()
	built, err := b.Run(time.Date(2026, 10, 8, 3, 5, 0, 0, time.UTC))
	if err != nil || len(built) != 3 {
		t.Fatalf("catch-up: built %v, %v", built, err)
	}
	want := map[string]string{
		b.name(d1):           line("judged_at", d1+"T10:00:00Z", "d1"),
		b.name("2026-10-06"): "",
		b.name(d3):           line("judged_at", d3+"T01:00:00Z", "d3a") + line("judged_at", d3+"T02:00:00Z", "d3b"),
	}
	for _, name := range built {
		m := readTar(t, filepath.Join(dir, name))
		if got := string(m["amendments.jsonl"]); got != want[name] {
			t.Errorf("%s amendments member:\n%q\nwant:\n%q", name, got, want[name])
		}
		var man Manifest
		if err := json.Unmarshal(m["manifest.json"], &man); err != nil {
			t.Fatal(err)
		}
		for _, f := range man.Files {
			if f.SkewedLines != 0 {
				t.Errorf("%s: %s reports %d skewed lines; nothing was dated after the build", name, f.Name, f.SkewedLines)
			}
		}
	}
	// The d4 line waits for d4's export.
	built, err = b.Run(time.Date(2026, 10, 9, 3, 0, 0, 0, time.UTC))
	if err != nil || len(built) != 1 {
		t.Fatalf("d4: built %v, %v", built, err)
	}
	m := readTar(t, filepath.Join(dir, built[0]))
	if got := string(m["amendments.jsonl"]); got != line("judged_at", d4+"T01:00:00Z", "d4") {
		t.Errorf("d4 amendments member: %q", got)
	}
}

// Every export carries the scanner state, because sentinel-recompute redraws
// verdicts with the scan gaps, the param history and the host seed that live
// in it. Without it a late shadow verdict the record defers on a gap is
// redrawn as a decided one, and the tool reports a difference where the
// observer was honestly blind.
func TestBuilder_CarriesTheScannerState(t *testing.T) {
	data := t.TempDir()
	dir := filepath.Join(data, "exports")
	if err := os.WriteFile(filepath.Join(data, "measurements.jsonl"),
		[]byte(line("started_at", "2026-09-10T10:00:00Z", "a")), 0o644); err != nil {
		t.Fatal(err)
	}
	state := `{"chain_id":"mocha-4","last_scanned_height":12,"gaps":[{"from":5,"to":6}]}`
	if err := os.WriteFile(filepath.Join(data, StateFile), []byte(state), 0o644); err != nil {
		t.Fatal(err)
	}
	b := &Builder{DataDir: data, Dir: dir, Vantage: "v", Build: "x", Hour: 3}
	built, err := b.Run(time.Date(2026, 9, 11, 3, 0, 0, 0, time.UTC))
	if err != nil || len(built) == 0 {
		t.Fatalf("built %v, err %v", built, err)
	}
	m := readTar(t, filepath.Join(dir, built[0]))
	if got := string(m[StateFile]); got != state {
		t.Fatalf("state member = %q, want the file as it stands", got)
	}
	var man Manifest
	if err := json.Unmarshal(m["manifest.json"], &man); err != nil {
		t.Fatal(err)
	}
	if man.State == nil || man.State.SHA256 == "" || man.State.Bytes != int64(len(state)) {
		t.Fatalf("the manifest does not attest the state: %+v", man.State)
	}
}

// An exports directory whose index.json is gone while older tarballs are
// there is not given a new index that lists only the next day: the build
// stops, says why and writes nothing, and goes on once the index is back.
// A first build that stopped before its index leaves only its own tarball,
// which the next build of that day replaces.
func TestBuilder_RefusesANewIndexOverOlderExports(t *testing.T) {
	data := t.TempDir()
	dir := filepath.Join(data, "exports")
	meas := filepath.Join(data, "measurements.jsonl")
	body := line("started_at", "2026-09-10T10:00:00Z", "a") + line("started_at", "2026-09-11T10:00:00Z", "b") +
		line("started_at", "2026-09-12T10:00:00Z", "c")
	if err := os.WriteFile(meas, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	b := &Builder{DataDir: data, Dir: dir, Vantage: "v", Build: "x", Hour: 3}
	for _, now := range []time.Time{time.Date(2026, 9, 11, 4, 0, 0, 0, time.UTC), time.Date(2026, 9, 12, 4, 0, 0, 0, time.UTC)} {
		if built, err := b.Run(now); err != nil || len(built) != 1 {
			t.Fatalf("%s: built %v, %v", now, built, err)
		}
	}
	index := filepath.Join(dir, "index.json")
	saved, err := os.ReadFile(index)
	if err != nil {
		t.Fatal(err)
	}
	state, _ := os.ReadFile(filepath.Join(dir, "state.json"))
	if err := os.Remove(index); err != nil {
		t.Fatal(err)
	}
	day3 := time.Date(2026, 9, 13, 4, 0, 0, 0, time.UTC)
	built, err := b.Run(day3)
	if err == nil || len(built) != 0 || !strings.Contains(err.Error(), "no index.json but holds 2 export tarball(s)") {
		t.Fatalf("a build over a lost index: built %v, %v", built, err)
	}
	if _, err := os.Stat(index); !os.IsNotExist(err) {
		t.Fatalf("a new index.json was written: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, b.name("2026-09-12"))); !os.IsNotExist(err) {
		t.Fatalf("the day's tarball was written: %v", err)
	}
	if now, _ := os.ReadFile(filepath.Join(dir, "state.json")); string(now) != string(state) {
		t.Fatal("state.json moved on")
	}
	if err := os.WriteFile(index, saved, 0o644); err != nil {
		t.Fatal(err)
	}
	if built, err := b.Run(day3); err != nil || len(built) != 1 {
		t.Fatalf("with the index back: built %v, %v", built, err)
	}
	entries, err := ReadIndex(dir)
	if err != nil || len(entries) != 3 {
		t.Fatalf("index lists %d export(s), %v", len(entries), err)
	}

	// A first build that stopped before its index: only its own tarball.
	data2 := t.TempDir()
	dir2 := filepath.Join(data2, "exports")
	if err := os.WriteFile(filepath.Join(data2, "measurements.jsonl"), []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	b2 := &Builder{DataDir: data2, Dir: dir2, Vantage: "v", Build: "x", Hour: 3}
	if err := os.MkdirAll(dir2, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir2, b2.name("2026-09-10")), []byte("partial"), 0o644); err != nil {
		t.Fatal(err)
	}
	if built, err := b2.Run(time.Date(2026, 9, 11, 4, 0, 0, 0, time.UTC)); err != nil || len(built) != 1 {
		t.Fatalf("a first build over its own leftover: built %v, %v", built, err)
	}
}
