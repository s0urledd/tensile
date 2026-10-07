package ingest_test

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/scan"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/ingest"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
)

// beatLine is one heartbeat record as observer-heartbeat writes it.
func beatLine(vantage, addr string, minute int) string {
	at := time.Date(2026, 9, 25, 10, minute, 0, 0, time.UTC).Format(time.RFC3339)
	return fmt.Sprintf(`{"vantage":%q,"validator_address":%q,"validator_host":"h:7980","scheduled_at":%q,"started_at":%q,"tcp":{"ok":true},"tls":{"ok":true},"outcome":"REACHABLE"}`,
		vantage, addr, at, at)
}

func countRows(t *testing.T, st *store.Store, vantage string) int {
	t.Helper()
	var n int
	if err := st.DB().QueryRow(`SELECT COUNT(*) FROM reachability WHERE vantage = ?`, vantage).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// ingestVantages is the collector's pass over the copied files.
func ingestVantages(t *testing.T, st *store.Store, dir string) (inserted, skipped int64) {
	t.Helper()
	files, err := ingest.VantageFiles(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range files {
		r, err := ingest.VantageReachability(st, f, "ut-1", time.Now())
		if err != nil {
			t.Fatalf("%s: %v", f, err)
		}
		inserted += r.Inserted
		skipped += r.Skipped
	}
	return inserted, skipped
}

// Other vantages' heartbeat files are tailed beside this observer's own, each
// on its own cursor: a copy replaced by a longer one reads only what is new, a
// half-copied last line waits, a shrunken copy re-reads without duplicating,
// a vantage directory that appears later is picked up, and the observer's own
// file keeps the cursor it had.
func TestVantageFilesAreTailedBesideTheOwnFile(t *testing.T) {
	st := openStore(t)
	data := t.TempDir()
	own := filepath.Join(data, "reachability.jsonl")
	vdir := filepath.Join(data, ingest.VantagesDir)

	// The observer's own file, ingested before any vantage exists: this is
	// the cursor an upgraded collector already holds.
	os.WriteFile(own, []byte(beatLine("ut-1", "aa", 0)+"\n"+beatLine("ut-1", "aa", 5)+"\n"), 0o644)
	if r, err := ingest.Reachability(st, own, time.Now()); err != nil || r.Inserted != 2 {
		t.Fatalf("own file: %+v %v", r, err)
	}
	ownOff, ownLine, _ := st.Cursor(own)

	// No vantages directory yet: nothing to do, and no error.
	if files, err := ingest.VantageFiles(vdir); err != nil || len(files) != 0 {
		t.Fatalf("missing dir: %v %v", files, err)
	}

	// de-1 arrives, its copy ending half-way through a line.
	de := filepath.Join(vdir, "de-1", "reachability.jsonl")
	os.MkdirAll(filepath.Dir(de), 0o755)
	third := beatLine("de-1", "aa", 10)
	os.WriteFile(de, []byte(beatLine("de-1", "aa", 0)+"\n"+beatLine("de-1", "aa", 5)+"\n"+third[:40]), 0o644)
	if ins, _ := ingestVantages(t, st, vdir); ins != 2 {
		t.Fatalf("first copy inserted %d, want 2 (the partial line waits)", ins)
	}
	if ins, _ := ingestVantages(t, st, vdir); ins != 0 {
		t.Fatalf("unchanged copy inserted %d", ins)
	}

	// rsync replaces the whole file with a longer copy: the line completes
	// and one more follows.
	grown := beatLine("de-1", "aa", 0) + "\n" + beatLine("de-1", "aa", 5) + "\n" + third + "\n" + beatLine("de-1", "aa", 15) + "\n"
	os.WriteFile(de+".tmp", []byte(grown), 0o644)
	os.Rename(de+".tmp", de)
	if ins, _ := ingestVantages(t, st, vdir); ins != 2 {
		t.Fatalf("grown copy inserted %d, want 2", ins)
	}

	// A copy shorter than the cursor (the sender's file was replaced) is
	// read again from the top, and the keys keep it from duplicating.
	os.WriteFile(de, []byte(beatLine("de-1", "aa", 0)+"\n"), 0o644)
	if ins, _ := ingestVantages(t, st, vdir); ins != 0 {
		t.Fatalf("shrunken copy inserted %d", ins)
	}
	os.WriteFile(de, []byte(grown), 0o644)
	if ins, _ := ingestVantages(t, st, vdir); ins != 0 {
		t.Fatalf("re-grown copy inserted %d", ins)
	}

	// A second vantage directory appears between passes. A row in it that
	// claims this observer's own vantage is stepped over, not counted as ours.
	us := filepath.Join(vdir, "us-2", "reachability.jsonl")
	os.MkdirAll(filepath.Dir(us), 0o755)
	os.WriteFile(us, []byte(beatLine("us-2", "aa", 0)+"\n"+beatLine("ut-1", "aa", 20)+"\n"), 0o644)
	ins, skipped := ingestVantages(t, st, vdir)
	if ins != 1 || skipped != 1 {
		t.Fatalf("new vantage: inserted %d skipped %d, want 1 and 1", ins, skipped)
	}

	if n := countRows(t, st, "de-1"); n != 4 {
		t.Errorf("de-1 rows = %d, want 4", n)
	}
	if n := countRows(t, st, "us-2"); n != 1 {
		t.Errorf("us-2 rows = %d, want 1", n)
	}
	if n := countRows(t, st, "ut-1"); n != 2 {
		t.Errorf("own rows = %d, want 2: another file's line was counted as this observer's", n)
	}

	// Each file has its own cursor, and the own file's did not move.
	if off, line, _ := st.Cursor(own); off != ownOff || line != ownLine {
		t.Errorf("own cursor moved: %d/%d, was %d/%d", off, line, ownOff, ownLine)
	}
	if off, _, _ := st.Cursor(de); off != int64(len(grown)) {
		t.Errorf("de-1 cursor = %d, want %d", off, len(grown))
	}
	if r, err := ingest.Reachability(st, own, time.Now()); err != nil || r.Read != 0 {
		t.Errorf("own file re-read after the vantage passes: %+v %v", r, err)
	}
}

// beatAt is a heartbeat record with the tip height its round began at.
func beatAt(vantage, addr string, minute int, height int64) string {
	at := time.Date(2026, 9, 25, 10, minute, 0, 0, time.UTC).Format(time.RFC3339)
	return fmt.Sprintf(`{"vantage":%q,"validator_address":%q,"validator_host":"h:7980","validator_set_height":%d,"scheduled_at":%q,"started_at":%q,"tcp":{"ok":true},"tls":{"ok":true},"outcome":"REACHABLE"}`,
		vantage, addr, height, at, at)
}

// A file copied in from another network's heartbeat stops at its first row,
// whose validator this chain does not know or whose height this chain was
// nowhere near; nothing of it is stored, the pass keeps saying so, and the
// file goes on once the row is one of this chain's (a validator that had
// just joined is in the staking set by the next poll).
func TestAVantageFileFromAnotherChainStopsBeforeItsFirstRow(t *testing.T) {
	st := openStore(t)
	data := t.TempDir()
	own := filepath.Join(data, "reachability.jsonl")
	vdir := filepath.Join(data, ingest.VantagesDir)
	now := time.Date(2026, 9, 25, 11, 0, 0, 0, time.UTC)

	// This observer's chain: its own heartbeat reached "aa", and the tip
	// the collector last polled.
	os.WriteFile(own, []byte(beatAt("ut-1", "aa", 0, 1_400_000)+"\n"), 0o644)
	if r, err := ingest.Reachability(st, own, now); err != nil || r.Inserted != 1 {
		t.Fatalf("own file: %+v %v", r, err)
	}
	st.SetMeta("chain_height", "1400600", now)
	st.SetMeta("chain_tip_time", store.TS(now), now)

	de := filepath.Join(vdir, "de-1", "reachability.jsonl")
	os.MkdirAll(filepath.Dir(de), 0o755)
	lines := beatAt("de-1", "aa", 5, 1_400_050) + "\n" + // this chain
		beatAt("de-1", "bb", 5, 1_400_050) + "\n" + // a validator this chain does not know (yet)
		beatAt("de-1", "aa", 10, 1_400_100) + "\n"
	os.WriteFile(de, []byte(lines), 0o644)
	for pass := 0; pass < 2; pass++ {
		r, err := ingest.VantageReachability(st, de, "ut-1", now)
		if err == nil || !strings.Contains(err.Error(), "another network") {
			t.Fatalf("pass %d: an unknown validator was not refused: %+v %v", pass, r, err)
		}
		if n := countRows(t, st, "de-1"); n != 1 {
			t.Fatalf("pass %d: de-1 rows = %d, want only the one before the refused row", pass, n)
		}
	}
	// "bb" is in the staking set by the next poll: the file goes on.
	if _, err := st.UpsertValidatorIdentities([]scan.ValidatorIdentity{{ConsAddressHex: "bb"}}, now); err != nil {
		t.Fatal(err)
	}
	if r, err := ingest.VantageReachability(st, de, "ut-1", now); err != nil || r.Inserted != 2 {
		t.Fatalf("after the validator joined: %+v %v", r, err)
	}

	// A row of a validator this chain knows, at a height another network
	// was at: refused, and nothing after it is read.
	mocha := filepath.Join(vdir, "mo-1", "reachability.jsonl")
	os.MkdirAll(filepath.Dir(mocha), 0o755)
	os.WriteFile(mocha, []byte(beatAt("mo-1", "aa", 5, 9_200_000)+"\n"+beatAt("mo-1", "aa", 10, 1_400_100)+"\n"), 0o644)
	if r, err := ingest.VantageReachability(st, mocha, "ut-1", now); err == nil || !strings.Contains(err.Error(), "another network") || r.Inserted != 0 {
		t.Fatalf("a height millions of blocks away was not refused: %+v %v", r, err)
	}
	if n := countRows(t, st, "mo-1"); n != 0 {
		t.Fatalf("mo-1 rows = %d, want none", n)
	}
	if off, _, _ := st.Cursor(mocha); off != 0 {
		t.Fatalf("the cursor moved past the refused row: %d", off)
	}
}
