package store_test

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/probe"
	"github.com/plsgiveup/fibre/fibre-sentinel/internal/scan"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
)

// A row whose record does not decode is that row's problem, not its stage's: the corrector's reads hand it back with
// no record (the corrector counts it unreached, so it stays withheld) and every other row as before, and the late
// verdicts leave it deferred and draw the others. What is not a record (the database itself failing) is still the
// caller's error.

// undecodable is a body the slim decoder refuses: not a line, and no slim record either.
var undecodable = []byte{0x05}

// seedReadings stores a publication and a reading of each of its first n validators, the reading's rows given by rows
// (the validator's own assignment when nil).
func seedReadings(t *testing.T, st *store.Store, hashByte byte, n int, rows func(i int, v scan.ValidatorAssignment) []uint32) (scan.Publication, []probe.Measurement) {
	t.Helper()
	pub := slimPublication(t, hashByte, 5)
	line, _ := json.Marshal(pub)
	if _, err := st.UpsertPublication(pub, line); err != nil {
		t.Fatal(err)
	}
	var ms []probe.Measurement
	for i, v := range pub.Assignment.Validators[:n] {
		r := u32(v.Rows)
		if rows != nil {
			r = rows(i, v)
		}
		m := slimReading(pub, v, r)
		l, _ := json.Marshal(m)
		if _, err := st.InsertProbe(m, l); err != nil {
			t.Fatal(err)
		}
		ms = append(ms, m)
	}
	return pub, ms
}

func TestARowThatDoesNotDecodeIsUnreachedNotTheCorrectorsError(t *testing.T) {
	ctx := context.Background()
	st := open(t)
	pub, ms := seedReadings(t, st, 0x41, 3, nil)
	bad := ms[1].DedupeKey()
	if _, err := st.DB().Exec(`UPDATE probes SET raw_json = ? WHERE dedupe_key = ?`, undecodable, bad); err != nil {
		t.Fatal(err)
	}
	if _, err := st.ProbeRecord(ctx, st.DB(), pub.PromiseHash, undecodable); !errors.Is(err, store.ErrUndecodable) {
		t.Fatalf("the body's own error is %v, not one ErrUndecodable names", err)
	}

	rows, err := st.ProbeRowsOf(ctx, pub.PromiseHash, "u-1")
	if err != nil {
		t.Fatalf("one undecodable row failed the publication's rows: %v", err)
	}
	if len(rows) != len(ms) {
		t.Fatalf("%d rows, want %d", len(rows), len(ms))
	}
	for _, r := range rows {
		if r.DedupeKey == bad {
			if r.RawJSON != "" || !errors.Is(r.RecordErr, store.ErrUndecodable) {
				t.Errorf("the undecodable row: record %.40q, error %v", r.RawJSON, r.RecordErr)
			}
			continue
		}
		var m probe.Measurement
		if r.RecordErr != nil || json.Unmarshal([]byte(r.RawJSON), &m) != nil || m.DedupeKey() != r.DedupeKey {
			t.Errorf("row %s: record %.60q, error %v", r.DedupeKey, r.RawJSON, r.RecordErr)
		}
	}

	// The same rows through the standing sweep: the publication's deadline moved, so every row disagrees with it.
	moved := pub.MustServeUntil.Add(-time.Hour)
	if _, err := st.DB().Exec(`UPDATE publications SET must_serve_until = ?, corrected_at = ? WHERE promise_hash = ?`,
		store.TS(moved), store.TS(moved), pub.PromiseHash); err != nil {
		t.Fatal(err)
	}
	stale, err := st.StaleDeadlineRows(ctx, 100)
	if err != nil {
		t.Fatalf("one undecodable row failed the sweep: %v", err)
	}
	if len(stale) != len(ms) {
		t.Fatalf("%d stale rows, want %d", len(stale), len(ms))
	}
	for _, r := range stale {
		if (r.DedupeKey == bad) != (r.RawJSON == "") || (r.DedupeKey == bad) != errors.Is(r.RecordErr, store.ErrUndecodable) {
			t.Errorf("stale row %s: record %.40q, error %v", r.DedupeKey, r.RawJSON, r.RecordErr)
		}
	}

	// A database that fails is not a row that does not decode.
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := st.ProbeRowsOf(cancelled, pub.PromiseHash, "u-1"); err == nil {
		t.Error("a read the database refused came back without its error")
	}
}

func TestADeferredRowThatDoesNotDecodeWaitsAndTheOthersAreJudged(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "observer.db")
	st, err := store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	// Two deferred rows of two publications: the first returned its own assignment (marked, so its rows come from its
	// publication), the second rows no promise assigns (kept as a list).
	pubA, msA := seedReadings(t, st, 0x51, 1, nil)
	pubB, msB := seedReadings(t, st, 0x52, 1, func(int, scan.ValidatorAssignment) []uint32 { return []uint32{16383, 5, 9001} })
	for _, m := range []probe.Measurement{msA[0], msB[0]} {
		if _, err := st.DB().Exec(`UPDATE probes SET classification = 'PROBE_ERROR', shadow_gap = 'awaiting candidates', outcome = 'PARTIAL'
			WHERE dedupe_key = ?`, m.DedupeKey()); err != nil {
			t.Fatal(err)
		}
	}
	var marked string
	if err := st.DB().QueryRow(`SELECT row_indices FROM probes WHERE dedupe_key = ?`, msA[0].DedupeKey()).Scan(&marked); err != nil || marked != store.RowsAssigned {
		t.Fatalf("the first row's rows are %q, not marked: %v", marked, err)
	}
	// the first publication stops decoding (in a process that has not read it yet)
	if _, err := st.DB().Exec(`UPDATE publications SET raw_json = ? WHERE promise_hash = ?`, undecodable, pubA.PromiseHash); err != nil {
		t.Fatal(err)
	}
	st.Close()
	st, err = store.Open(path)
	if err != nil {
		t.Fatal(err)
	}

	frontier := msB[0].StartedAt.Add(2 * time.Hour)
	ams, err := st.LateShadowVerdicts(ctx, frontier, frontier, time.Minute)
	if err != nil {
		t.Fatalf("one undecodable row failed every late verdict: %v", err)
	}
	if len(ams) != 1 || ams[0].DedupeKey != msB[0].DedupeKey() || ams[0].To != string(probe.ClassUnmatchedGenuine) {
		t.Fatalf("verdicts %+v, want the second row's alone", ams)
	}
	if ams[0].PromiseHash != pubB.PromiseHash {
		t.Fatalf("verdict on %s", ams[0].PromiseHash)
	}
	// the first row is still deferred, for a pass that can read it
	var amended *string
	if err := st.DB().QueryRow(`SELECT amended_at FROM probes WHERE dedupe_key = ?`, msA[0].DedupeKey()).Scan(&amended); err != nil || amended != nil {
		t.Fatalf("the undecodable row: amended_at %v, %v", amended, err)
	}
}
