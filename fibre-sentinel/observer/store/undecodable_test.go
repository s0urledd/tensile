package store_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/probe"
	"github.com/plsgiveup/fibre/fibre-sentinel/internal/scan"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/ingest"
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

// A candidate promise whose assignment does not decode may be the one that owns a deferred row's rows: the row is left
// deferred rather than judged without it, and the other rows are judged. Once the candidate reads again the row is
// judged against it, and it was the owner: passing over it would have drawn a wrong verdict, which is final.
func TestADeferredRowWaitsForACandidateThatDoesNotDecode(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "observer.db")
	st, err := store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	// The candidate: another promise over the same blob, to another validator set, so it assigns the validator other
	// rows than the first promise does. Its assignment row is marked, so its rows come from its publication.
	cand := slimPublication(t, 0x55, 6)
	cand.PromiseHash = hex.EncodeToString(bytes.Repeat([]byte{0x5a}, 32))
	cand.SettlementTxHash = hex.EncodeToString(bytes.Repeat([]byte{0x5a, 0xcd}, 16))
	candLine, _ := json.Marshal(cand)
	if ok, err := st.UpsertPublication(cand, candLine); err != nil || !ok {
		t.Fatalf("candidate: %v %v", ok, err)
	}
	v := cand.Assignment.Validators[0]
	var marked string
	if err := st.DB().QueryRow(`SELECT rows_json FROM assignments WHERE promise_hash = ? AND validator_address = ?`,
		cand.PromiseHash, v.Address).Scan(&marked); err != nil || marked != store.RowsAssigned {
		t.Fatalf("the candidate's assignment of the validator is %q, not marked: %v", marked, err)
	}
	// Two deferred rows: the first promise's reading that returned the candidate's rows for the validator (kept as a
	// list), and another blob's, rows no promise assigns.
	pubA, msA := seedReadings(t, st, 0x55, 1, func(int, scan.ValidatorAssignment) []uint32 { return u32(v.Rows) })
	_, msB := seedReadings(t, st, 0x56, 1, func(int, scan.ValidatorAssignment) []uint32 { return []uint32{16383, 5, 9001} })
	if pubA.Promise.Commitment != cand.Promise.Commitment || msA[0].ValidatorAddress != v.Address {
		t.Fatal("the candidate is not over the first promise's blob, to its validator")
	}
	var own string
	if err := st.DB().QueryRow(`SELECT row_indices FROM probes WHERE dedupe_key = ?`, msA[0].DedupeKey()).Scan(&own); err != nil || own == store.RowsAssigned {
		t.Fatalf("the first row's rows are %q, its own assignment: %v", own, err)
	}
	for _, m := range []probe.Measurement{msA[0], msB[0]} {
		if _, err := st.DB().Exec(`UPDATE probes SET classification = 'PROBE_ERROR', shadow_gap = 'awaiting candidates', outcome = 'WRONG_ROWS'
			WHERE dedupe_key = ?`, m.DedupeKey()); err != nil {
			t.Fatal(err)
		}
	}
	var candRaw []byte
	if err := st.DB().QueryRow(`SELECT raw_json FROM publications WHERE promise_hash = ?`, cand.PromiseHash).Scan(&candRaw); err != nil {
		t.Fatal(err)
	}
	// the candidate's record becomes raw, read by a process that has not read it yet
	reopenWith := func(raw []byte) {
		t.Helper()
		if _, err := st.DB().Exec(`UPDATE publications SET raw_json = ? WHERE promise_hash = ?`, raw, cand.PromiseHash); err != nil {
			t.Fatal(err)
		}
		st.Close()
		if st, err = store.Open(path); err != nil {
			t.Fatal(err)
		}
	}
	reopenWith(undecodable)

	frontier := msB[0].StartedAt.Add(2 * time.Hour)
	ams, err := st.LateShadowVerdicts(ctx, frontier, frontier, time.Minute)
	if err != nil {
		t.Fatalf("a candidate that does not decode failed every late verdict: %v", err)
	}
	if len(ams) != 1 || ams[0].DedupeKey != msB[0].DedupeKey() || ams[0].To != string(probe.ClassUnmatchedGenuine) {
		t.Fatalf("verdicts %+v, want the second row's alone", ams)
	}

	// the candidate reads again: the first row is its shard
	reopenWith(candRaw)
	ams, err = st.LateShadowVerdicts(ctx, frontier, frontier, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(ams) != 2 {
		t.Fatalf("verdicts %+v, want both rows'", ams)
	}
	for _, a := range ams {
		if a.DedupeKey == msA[0].DedupeKey() && (a.To != string(probe.ClassShadowedShard) || a.ShadowedBy != cand.PromiseHash) {
			t.Fatalf("the first row once its candidate reads: %+v", a)
		}
	}
}

// A reading whose publication is stored but does not decode is stored as its line, and the readings after it in its
// file are ingested: the store refusing it would hold the file's ingest at that line for good. Its rows are kept as a
// list, and the reading's count of distinct rows, which a row marked as its assignment no longer gives, is raised by
// what the rows read now give and never lowered.
func TestAReadingOfAPublicationThatDoesNotDecodeIsStoredAsItsLine(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	path := filepath.Join(dir, "observer.db")
	st, err := store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	pub, ms := seedReadings(t, st, 0x45, 1, nil) // the first validator's reading, its rows marked as its assignment
	exact := func() int {
		t.Helper()
		var n int
		if err := st.DB().QueryRow(`SELECT exact FROM reading_rows WHERE promise_hash = ? AND scheduled_at = ?`,
			pub.PromiseHash, store.TS(ms[0].ScheduledAt)).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	first := len(pub.Assignment.Validators[0].Rows)
	if n := exact(); n != first {
		t.Fatalf("the reading counts %d rows, want %d", n, first)
	}
	// the publication stops decoding (in a process that has not read it yet)
	if _, err := st.DB().Exec(`UPDATE publications SET raw_json = ? WHERE promise_hash = ?`, undecodable, pub.PromiseHash); err != nil {
		t.Fatal(err)
	}
	st.Close()
	if st, err = store.Open(path); err != nil {
		t.Fatal(err)
	}

	// the next validators' readings of the same point, from the file
	var lines [][]byte
	var keys []string
	next := map[int]bool{}
	for _, v := range pub.Assignment.Validators[1:3] {
		m := slimReading(pub, v, u32(v.Rows))
		l, _ := json.Marshal(m)
		lines, keys = append(lines, l), append(keys, m.DedupeKey())
		for _, r := range v.Rows {
			next[r] = true
		}
	}
	file := filepath.Join(dir, "measurements.jsonl")
	if err := os.WriteFile(file, append(bytes.Join(lines, []byte("\n")), '\n'), 0o644); err != nil {
		t.Fatal(err)
	}
	res, err := ingest.Measurements(st, file, time.Now())
	if err != nil || res.Inserted != int64(len(lines)) {
		t.Fatalf("ingest past a publication that does not decode: %+v %v", res, err)
	}
	for i, k := range keys {
		var raw []byte
		var idx sql.NullString
		if err := st.DB().QueryRow(`SELECT raw_json, row_indices FROM probes WHERE dedupe_key = ?`, k).Scan(&raw, &idx); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(raw, lines[i]) {
			t.Errorf("reading %d is kept as %.60q, not its line", i, raw)
		}
		if !idx.Valid || idx.String == store.RowsAssigned {
			t.Errorf("reading %d keeps its rows as %q, not their list", i, idx.String)
		}
		if got, err := st.ProbeRecord(ctx, st.DB(), pub.PromiseHash, raw); err != nil || !bytes.Equal(got, lines[i]) {
			t.Errorf("reading %d back: %v", i, err)
		}
	}
	if n, want := exact(), max(first, len(next)); n != want {
		t.Fatalf("the reading counts %d rows, want %d: the first validator's %d, counted when they arrived, or the next two's %d",
			n, want, first, len(next))
	}
}

// failingFor reads through db, but a read that names hash fails as one does when the store is busy or the run's
// context has ended.
type failingFor struct {
	*sql.DB
	hash string
}

func (q failingFor) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	for _, a := range args {
		if a == q.hash {
			ended, cancel := context.WithCancel(ctx)
			cancel()
			return q.DB.QueryRowContext(ended, query, args...)
		}
	}
	return q.DB.QueryRowContext(ctx, query, args...)
}

// A reading whose rows are the assignment of the promise that answered in its place is kept naming that promise, and
// its rows are read back from that promise's publication. A read of the publication that fails (the store busy, the
// run's context ended) says nothing about the reading: it is the read's error, not ErrUndecodable, which record-verify
// counted as a different line, so the day went into the ledger as not reproducible for good. A shadowing publication
// that does not decode is still the reading's own.
func TestAShadowingPublicationThatCouldNotBeReadIsTheReadsError(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "observer.db")
	st, err := store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	pub, other := slimPublication(t, 0x51, 5), slimPublication(t, 0x52, 5)
	for _, p := range []scan.Publication{pub, other} {
		l, _ := json.Marshal(p)
		if ok, err := st.UpsertPublication(p, l); err != nil || !ok {
			t.Fatalf("publication %s: %v %v", p.PromiseHash, ok, err)
		}
	}
	// the validator's rows in the other promise
	m := slimReading(pub, pub.Assignment.Validators[1], u32(other.Assignment.Validators[1].Rows))
	m.Download.ShadowedBy = other.PromiseHash
	line, _ := json.Marshal(m)
	if ok, err := st.InsertProbe(m, line); err != nil || !ok {
		t.Fatalf("probe: %v %v", ok, err)
	}
	var raw []byte
	if err := st.DB().QueryRow(`SELECT raw_json FROM probes WHERE dedupe_key = ?`, m.DedupeKey()).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	if raw[0] == '{' {
		t.Fatal("the reading is kept as its line")
	}
	// each read in a handle of its own, as record-verify opens the store: no publication is read in it yet
	read := func(through func(*sql.DB) store.Querier) ([]byte, error) {
		t.Helper()
		ro, err := store.OpenReadOnly(path)
		if err != nil {
			t.Fatal(err)
		}
		defer ro.Close()
		return ro.ProbeRecord(ctx, through(ro.DB()), pub.PromiseHash, raw)
	}
	plain := func(db *sql.DB) store.Querier { return db }
	if back, err := read(plain); err != nil || !bytes.Equal(back, line) {
		t.Fatalf("the reading back: %v\n got %.300s\nwant %.300s", err, back, line)
	}
	_, err = read(func(db *sql.DB) store.Querier { return failingFor{db, other.PromiseHash} })
	if err == nil {
		t.Fatal("the reading came back without the other promise's publication: its rows were not kept naming it")
	}
	if errors.Is(err, store.ErrUndecodable) || !errors.Is(err, context.Canceled) {
		t.Fatalf("a shadowing publication the store could not read: %v", err)
	}
	// the shadowing publication stops decoding: that is the reading's own
	if _, err := st.DB().Exec(`UPDATE publications SET raw_json = ? WHERE promise_hash = ?`, undecodable, other.PromiseHash); err != nil {
		t.Fatal(err)
	}
	if _, err := read(plain); !errors.Is(err, store.ErrUndecodable) {
		t.Fatalf("a shadowing publication that does not decode: %v", err)
	}
}
