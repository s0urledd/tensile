package main

import (
	"encoding/json"
	"errors"
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

// appendLine appends raw to path, creating it.
func appendLine(t *testing.T, path, raw string) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.WriteString(raw); err != nil {
		t.Fatal(err)
	}
}

func jsonLine(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return string(b) + "\n"
}

// count is one COUNT(*) over the store.
func count(t *testing.T, st *store.Store, q string, args ...any) int {
	t.Helper()
	var n int
	if err := st.DB().QueryRow(q, args...).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// writes is every row the tick could have written, with the moment it was
// written: the cursors and the checkpoint's meta keys.
func writes(t *testing.T, st *store.Store) string {
	t.Helper()
	var s string
	if err := st.DB().QueryRow(`SELECT COALESCE((SELECT GROUP_CONCAT(file || '@' || byte_offset || '@' || updated_at, ' ') FROM ingest_cursors), '') || ' | ' ||
		COALESCE((SELECT GROUP_CONCAT(key || '@' || updated_at, ' ') FROM meta), '')`).Scan(&s); err != nil {
		t.Fatal(err)
	}
	return s
}

// The fast tick stores a new blob's row, its payment and the checkpoint read
// before them; opens nothing and writes nothing while the files stand still;
// reads half a line as nothing; and reports a failure on the status file at
// every tick, as a pass does, while logging it once.
func TestFastTickReadsANewBlobAndNothingElse(t *testing.T) {
	st := openStore(t)
	dir := t.TempDir()
	statePath := filepath.Join(dir, "state.json")
	pubsPath := filepath.Join(dir, "publications.jsonl")
	payPath := filepath.Join(dir, "payments.jsonl")

	var logs, failed []string
	logf := func(f string, a ...any) { logs = append(logs, fmt.Sprintf(f, a...)) }
	ft := newFastTick(st, statePath, pubsPath, payPath, logf, func(m string) { failed = append(failed, m) })
	tails := map[string]int{}
	for i := range ft.files {
		what, tail := ft.files[i].what, ft.files[i].tail
		ft.files[i].tail = func(st *store.Store, path string, now time.Time) (ingest.Result, error) {
			tails[what]++
			return tail(st, path, now)
		}
	}
	at := time.Date(2026, 10, 4, 12, 0, 0, 0, time.UTC)
	tick := func() {
		at = at.Add(time.Second)
		ft.run(at)
	}

	// nothing on disk yet: nothing to read, nothing written
	tick()
	if len(tails) != 0 || writes(t, st) != " | " {
		t.Fatalf("an empty data dir was read: tails %v, writes %q", tails, writes(t, st))
	}

	// a block with a blob in it: the checkpoint, the row and its charge
	hash := strings.Repeat("ab", 32)
	if err := os.WriteFile(statePath, []byte(`{"schema_version":1,"chain_id":"t","start_height":1,"last_scanned_height":100}`), 0o644); err != nil {
		t.Fatal(err)
	}
	appendLine(t, pubsPath, jsonLine(t, scan.Publication{SchemaVersion: 1, PromiseHash: hash, SettlementHeight: 100, SettlementTime: at}))
	appendLine(t, payPath, jsonLine(t, scan.Payment{SchemaVersion: 1, DedupeKey: "settle-1", Kind: "settlement", Height: 100, Time: at,
		Publisher: "celestia1payer", PromiseHash: hash, Denom: "utia", AmountUtia: 1234}))
	tick()
	if n := count(t, st, `SELECT COUNT(*) FROM publications WHERE promise_hash = ?`, hash); n != 1 {
		t.Fatalf("the new blob's row is not stored (%d)", n)
	}
	if n := count(t, st, `SELECT COUNT(*) FROM payments WHERE promise_hash = ? AND kind = 'settlement'`, hash); n != 1 {
		t.Fatalf("the new blob's charge is not stored (%d)", n)
	}
	if v, _ := st.Meta("last_scanned_height"); v != "100" {
		t.Fatalf("the checkpoint was not read: last_scanned_height %q", v)
	}
	if tails["publications"] != 1 || tails["payments"] != 1 {
		t.Fatalf("tails %v", tails)
	}
	if !strings.Contains(strings.Join(logs, "\n"), "publications: +1 (read 1, line 1)") {
		t.Fatalf("the read is not logged as the pass logs it: %q", logs)
	}

	// standing still: no file opened, no row written
	before := writes(t, st)
	tick()
	tick()
	if tails["publications"] != 1 || tails["payments"] != 1 {
		t.Fatalf("files that did not change were read again: %v", tails)
	}
	if after := writes(t, st); after != before {
		t.Fatalf("a tick with nothing new wrote to the store:\nbefore %s\nafter  %s", before, after)
	}

	// half a line is read as nothing, and the cursor is not written for it
	second := strings.Repeat("cd", 32)
	line := jsonLine(t, scan.Publication{SchemaVersion: 1, PromiseHash: second, SettlementHeight: 101, SettlementTime: at})
	appendLine(t, pubsPath, line[:len(line)/2])
	tick()
	if tails["publications"] != 2 || count(t, st, `SELECT COUNT(*) FROM publications`) != 1 {
		t.Fatalf("half a line: tails %v, rows %d", tails, count(t, st, `SELECT COUNT(*) FROM publications`))
	}
	if after := writes(t, st); after != before {
		t.Fatalf("half a line wrote to the store:\nbefore %s\nafter  %s", before, after)
	}
	appendLine(t, pubsPath, line[len(line)/2:])
	tick()
	if n := count(t, st, `SELECT COUNT(*) FROM publications WHERE promise_hash = ?`, second); n != 1 {
		t.Fatalf("the completed line is not stored (%d)", n)
	}

	// a failure: told to the status file at every tick, logged once, and
	// its end logged too
	tail := ft.files[1].tail
	ft.files[1].tail = func(*store.Store, string, time.Time) (ingest.Result, error) {
		return ingest.Result{}, errors.New("disk on fire")
	}
	appendLine(t, payPath, jsonLine(t, scan.Payment{SchemaVersion: 1, DedupeKey: "settle-2", Kind: "settlement", Height: 101, Time: at,
		Publisher: "celestia1payer", PromiseHash: second, Denom: "utia", AmountUtia: 1234}))
	logs = nil
	tick()
	tick()
	if len(failed) != 2 || failed[0] != "ingest: payments: disk on fire" {
		t.Fatalf("the status file was told %q", failed)
	}
	if n := strings.Count(strings.Join(logs, "\n"), "payments: disk on fire"); n != 1 {
		t.Fatalf("a failure that persists is logged %d times: %q", n, logs)
	}
	ft.files[1].tail = tail
	tick()
	if len(failed) != 2 {
		t.Fatalf("a tick that read cleanly told the status file %q", failed)
	}
	if !strings.Contains(strings.Join(logs, "\n"), "fast tick: reading without errors again") {
		t.Fatalf("the end of the failure is not logged: %q", logs)
	}
	if n := count(t, st, `SELECT COUNT(*) FROM payments WHERE promise_hash = ?`, second); n != 1 {
		t.Fatalf("the payment the failure held back is not stored once it clears (%d)", n)
	}
}
