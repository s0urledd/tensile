package main

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/record"
)

var now = time.Date(2026, 9, 25, 4, 0, 0, 0, time.UTC)

func dataDir(t *testing.T, days int) string {
	t.Helper()
	dir := t.TempDir()
	var m, r bytes.Buffer
	for d := days; d > 0; d-- {
		at := now.Add(-time.Duration(d) * 24 * time.Hour).Format(time.RFC3339)
		fmt.Fprintf(&m, `{"vantage":"t","promise_hash":"p%d","validator_address":"v","scheduled_at":%q}`+"\n", d, at)
		fmt.Fprintf(&r, `{"vantage":"t","validator_address":"v%d","scheduled_at":%q}`+"\n", d, at)
	}
	os.WriteFile(filepath.Join(dir, "measurements.jsonl"), m.Bytes(), 0o644)
	os.WriteFile(filepath.Join(dir, "reachability.jsonl"), r.Bytes(), 0o644)
	os.WriteFile(filepath.Join(dir, "state.json"), []byte(`{"param_history":[{"params":{"payment_promise_timeout_seconds":3600,"shard_retention_seconds":14400}}]}`), 0o644)
	scannerStarted(t, dir, true)
	return dir
}

func runArgs(t *testing.T, args ...string) (int, string, string) {
	t.Helper()
	var out, errb bytes.Buffer
	code := run(args, &out, &errb, now)
	return code, out.String(), errb.String()
}

func TestMinKeepAndCutoff(t *testing.T) {
	dir := dataDir(t, 1)
	k, err := MinKeep(filepath.Join(dir, "state.json"))
	if err != nil || k != 28*time.Hour {
		t.Fatalf("min keep %s %v, want 28h (4h retention + 24h)", k, err)
	}
	if k, _ := MinKeep(filepath.Join(dir, "none.json")); k != minMargin {
		t.Fatalf("no state: %s", k)
	}
	if c := Cutoff(now, 7*24*time.Hour); !c.Equal(time.Date(2026, 9, 18, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("cutoff %s", c)
	}
}

func TestRunArchivesVerifiesAndRefuses(t *testing.T) {
	if !rotates(t) {
		t.Skip("record.Archive needs flock, which this platform does not have; this runs on Linux (CI)")
	}
	dir := dataDir(t, 12)
	if code, _, errs := runArgs(t, "-data-dir", dir, "-keep", "20h"); code != 2 || !strings.Contains(errs, "shorter than") {
		t.Fatalf("a keep under the retention window was accepted: %d %s", code, errs)
	}
	before, _ := os.ReadFile(filepath.Join(dir, "measurements.jsonl"))
	code, out, errs := runArgs(t, "-data-dir", dir, "-dry-run")
	after, _ := os.ReadFile(filepath.Join(dir, "measurements.jsonl"))
	if code != 0 || !strings.Contains(out, "would archive 5 line(s)") || !bytes.Equal(before, after) {
		t.Fatalf("dry run: %d\n%s%s", code, out, errs)
	}
	// the export has read only four lines of measurements and nothing of
	// reachability: that is all that may move
	os.MkdirAll(filepath.Join(dir, "exports"), 0o755)
	lines := strings.SplitAfter(string(before), "\n")
	four := len(lines[0]) + len(lines[1]) + len(lines[2]) + len(lines[3])
	os.WriteFile(filepath.Join(dir, "exports", "state.json"), []byte(fmt.Sprintf(`{"last_day":"2026-09-24","offsets":{"measurements.jsonl":%d}}`, four)), 0o644)
	code, out, errs = runArgs(t, "-data-dir", dir)
	if code != 0 || !strings.Contains(out, "measurements.jsonl: archived 4 line(s)") || !strings.Contains(out, "reachability.jsonl: nothing to archive") {
		t.Fatalf("capped run: %d\n%s%s", code, out, errs)
	}
	code, out, errs = runArgs(t, "-data-dir", dir, "-ignore-exports")
	if code != 0 || !strings.Contains(out, "measurements.jsonl: archived 1 line(s)") || !strings.Contains(out, "reachability.jsonl: archived 5 line(s)") {
		t.Fatalf("uncapped run: %d\n%s%s", code, out, errs)
	}
	code, out, _ = runArgs(t, "-data-dir", dir, "-ignore-exports")
	if code != 0 || strings.Contains(out, "archived") {
		t.Fatalf("a second run the same day moved something:\n%s", out)
	}
	code, out, errs = runArgs(t, "-data-dir", dir, "-verify")
	if code != 0 || !strings.Contains(out, "measurements.jsonl: 2 segment(s) verified") {
		t.Fatalf("verify: %d\n%s%s", code, out, errs)
	}
	code, out, _ = runArgs(t, "-data-dir", dir, "-status")
	if code != 0 || !strings.Contains(out, "live since 2026-09-18") {
		t.Fatalf("status: %d\n%s", code, out)
	}
	all, err := record.OpenAll(filepath.Join(dir, "measurements.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	defer all.Close()
	var got bytes.Buffer
	got.ReadFrom(all)
	if !bytes.Equal(got.Bytes(), before) {
		t.Fatal("the record read through the archive is not the file as it was")
	}
	if code, _, errs := runArgs(t, "-data-dir", dir, "-files", "registry.jsonl"); code != 2 || !strings.Contains(errs, "not an archived file") {
		t.Fatalf("an unknown file was accepted: %d %s", code, errs)
	}
}

// The scanner's two files are rotated only once runs.jsonl shows that the
// scanner running now follows a rotation: with no scanner start, or with a
// newest start from a build that held its files open with plain appends
// (one not restarted since an upgrade), they are left as they are and the
// run says why, while the other files are archived; a start of this
// build's scanner lets them through.
func TestScannerFilesWaitForAScannerThatFollows(t *testing.T) {
	dir := t.TempDir()
	var archived []string
	archiveFile = func(path string, o record.Options) (record.Result, error) {
		archived = append(archived, filepath.Base(path))
		return record.Result{File: filepath.Base(path), Skipped: "stand-in"}, nil
	}
	t.Cleanup(func() { archiveFile = record.Archive })
	runOnce := func() string {
		t.Helper()
		archived = nil
		code, out, errs := runArgs(t, "-data-dir", dir)
		if code != 0 {
			t.Fatalf("%d\n%s%s", code, out, errs)
		}
		return out
	}

	out := runOnce()
	for _, name := range []string{"publications.jsonl", "payments.jsonl"} {
		if !strings.Contains(out, name+": left as it is: no scanner start in runs.jsonl") {
			t.Fatalf("no scanner start, %s:\n%s", name, out)
		}
	}
	if strings.Join(archived, ",") != "measurements.jsonl,sampling_decisions.jsonl,reachability.jsonl" {
		t.Fatalf("archived %v", archived)
	}
	scannerStarted(t, dir, true)
	scannerStarted(t, dir, false) // the newest start counts: an older build's
	out = runOnce()
	if !strings.Contains(out, "publications.jsonl: left as it is: the scanner started at 2026-10-01T00:00:00Z (build t) writes it without following a rotation; restart fibre-scan on this build first") ||
		len(archived) != 3 {
		t.Fatalf("an older scanner: archived %v\n%s", archived, out)
	}
	scannerStarted(t, dir, true)
	if out = runOnce(); strings.Contains(out, "left as it is") || len(archived) != len(Files) {
		t.Fatalf("this build's scanner: archived %v\n%s", archived, out)
	}
}
