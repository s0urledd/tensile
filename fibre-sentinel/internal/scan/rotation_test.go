package scan

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/record"
)

var rotT0 = time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)

func rotDay(d int) time.Time { return rotT0.Add(time.Duration(d) * 24 * time.Hour) }

// rotationSupported reports whether record.Archive runs here: it needs
// flock, and refuses before touching anything without it.
func rotationSupported(t *testing.T) bool {
	t.Helper()
	path := filepath.Join(t.TempDir(), "probe.jsonl")
	if err := os.WriteFile(path, []byte(`{"time":"2026-09-01T00:00:00Z"}`+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := record.Archive(path, record.Options{Cutoff: rotT0, TimeField: "time", Limit: -1})
	return !errors.Is(err, record.ErrUnsupported)
}

// appendRecord writes record i, a publication and a payment dated at, the
// way the scanner writes one block's.
func appendRecord(st *Store, i int, at time.Time) error {
	if err := st.AppendPublication(Publication{PromiseHash: fmt.Sprintf("p%04d", i), SettlementTxHash: fmt.Sprintf("t%04d", i),
		SettlementHeight: int64(i), SettlementTime: at}); err != nil {
		return err
	}
	return st.AppendPayment(Payment{SchemaVersion: 1, DedupeKey: fmt.Sprintf("k%04d", i), Kind: PaymentDeposit,
		Height: int64(i), Time: at, Publisher: "a", AmountUtia: 1})
}

// recordKeys reads the whole record of path, segments then live file, and
// returns the key field of every line in order.
func recordKeys(t *testing.T, path, key string) []string {
	t.Helper()
	r, err := record.OpenAll(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	var out []string
	sc := bufio.NewScanner(r)
	for sc.Scan() {
		var m map[string]any
		if err := json.Unmarshal(sc.Bytes(), &m); err != nil {
			t.Fatalf("%s: line %d: %v", path, len(out)+1, err)
		}
		k, _ := m[key].(string)
		out = append(out, k)
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

// wantKeys fails unless got is prefix<from>..prefix<to-1>, each once and in
// order.
func wantKeys(t *testing.T, what string, got []string, prefix string, from, to int) {
	t.Helper()
	if len(got) != to-from {
		t.Fatalf("%s: %d lines in the record, want %d", what, len(got), to-from)
	}
	for i, k := range got {
		if want := fmt.Sprintf("%s%04d", prefix, from+i); k != want {
			t.Fatalf("%s: line %d is %s, want %s (a line lost, doubled or out of order)", what, i+1, k, want)
		}
	}
}

// tailFrom is how many bytes of path start at its first line dated at or
// after cutoff: what a rotation at cutoff leaves live.
func tailFrom(t *testing.T, path, field string, cutoff time.Time) int64 {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	off := 0
	for _, l := range bytes.SplitAfter(raw, []byte{'\n'}) {
		var m map[string]json.RawMessage
		var at time.Time
		if json.Unmarshal(l, &m) != nil || json.Unmarshal(m[field], &at) != nil || !at.Before(cutoff) {
			break
		}
		off += len(l)
	}
	return int64(len(raw) - off)
}

// rotateLive rotates path at cutoff with record.Archive where it runs, and
// reports true. Elsewhere it leaves the live file a rotation would (its
// tail, renamed over the path) and reports false: the lines cut are then
// gone rather than in a segment.
func rotateLive(t *testing.T, path, field string, cutoff time.Time) bool {
	t.Helper()
	res, err := record.Archive(path, record.Options{Cutoff: cutoff, TimeField: field, Limit: -1, Now: cutoff})
	if err == nil {
		if res.Skipped != "" {
			t.Fatalf("%s: nothing archived: %s", path, res.Skipped)
		}
		return true
	}
	if !errors.Is(err, record.ErrUnsupported) {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	tmp := path + ".rotate.tmp"
	if err := os.WriteFile(tmp, raw[int64(len(raw))-tailFrom(t, path, field, cutoff):], 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(tmp, path); err != nil {
		t.Fatal(err)
	}
	return false
}

// waitForSize waits until the file at path is size bytes long, and fails
// when the archive run reports on done first, or it takes too long.
func waitForSize(t *testing.T, path string, size int64, done <-chan error) {
	t.Helper()
	for deadline := time.Now().Add(10 * time.Second); ; {
		if info, err := os.Stat(path); err == nil && info.Size() == size {
			return
		}
		select {
		case err := <-done:
			t.Fatalf("the archive run ended before it waited for the writers' lock: %v", err)
		case <-time.After(time.Millisecond):
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s never reached %d bytes", path, size)
		}
	}
}

// The scanner writes publications and payments while observer-archive
// rotates each file. Lines written before the run, lines written once the
// archiver has copied the tail and waits for the exclusive lock (another
// writer holds the shared one), lines racing the swap, and lines written
// after it are each in the record exactly once, in the order written: the
// store follows the rotation instead of writing into the file it replaced.
func TestStoreWritesFollowRotation(t *testing.T) {
	if !rotationSupported(t) {
		t.Skip("record.Archive needs flock, which this platform does not have, so these files are never rotated here")
	}
	st, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	n := 0
	add := func(at time.Time) error {
		err := appendRecord(st, n, at)
		n++
		return err
	}
	for d := 0; d < 10; d++ {
		if err := add(rotDay(d)); err != nil {
			t.Fatal(err)
		}
	}
	if err := st.Sync(); err != nil {
		t.Fatal(err)
	}
	cutoff, live := rotDay(5), rotDay(10) // lines dated live are never cut
	for _, f := range []struct{ path, field string }{{st.PublicationsPath(), "settlement_time"}, {st.PaymentsPath(), "time"}} {
		tail := tailFrom(t, f.path, f.field, cutoff)
		release := holdShared(t, f.path)
		defer release()
		done := make(chan error, 1)
		go func() {
			res, err := record.Archive(f.path, record.Options{Cutoff: cutoff, TimeField: f.field, Limit: -1, Now: cutoff})
			if err == nil && res.Lines != 5 {
				err = fmt.Errorf("%s: archived %d lines, want 5 (%+v)", f.path, res.Lines, res)
			}
			done <- err
		}()
		// The archiver copies the tail without the lock and then waits for
		// the exclusive one: these land in the old file after the copy, and
		// it has to carry them over under the lock.
		waitForSize(t, f.path+".rotate.tmp", tail, done)
		for i := 0; i < 3; i++ {
			if err := add(live); err != nil {
				t.Fatal(err)
			}
		}
		// The archiver swaps while a writer keeps appending: some lines land
		// before the swap, some wait out the exclusive lock and find the
		// path moved on.
		stop, wrote := make(chan struct{}), make(chan error, 1)
		go func() {
			for k := 0; k < 5000; k++ {
				select {
				case <-stop:
					wrote <- nil
					return
				default:
				}
				if err := add(live); err != nil {
					wrote <- err
					return
				}
			}
			wrote <- nil
		}()
		release()
		err := <-done
		close(stop)
		if werr := <-wrote; werr != nil {
			t.Fatal(werr)
		}
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 3; i++ {
			if err := add(live); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := st.Sync(); err != nil {
		t.Fatal(err)
	}
	wantKeys(t, "publications", recordKeys(t, st.PublicationsPath(), "promise_hash"), "p", 0, n)
	wantKeys(t, "payments", recordKeys(t, st.PaymentsPath(), "dedupe_key"), "k", 0, n)
	for _, p := range []string{st.PublicationsPath(), st.PaymentsPath()} {
		if segs, err := record.Verify(p); err != nil || segs != 1 {
			t.Fatalf("%s: verify: %d segments, %v", p, segs, err)
		}
	}
}

// A scanner restarted after its files were rotated loads its dedupe sets
// from the live files: the blocks it scans again after a crash (the newest,
// so in the live window) append nothing twice, new records follow the live
// lines, and a torn tail left in a rotated file is repaired as before. The
// lines only in the segments are not in the sets, by design (loadSeen), and
// the whole record still reads each line once.
func TestStoreRestartsAfterRotation(t *testing.T) {
	dir := t.TempDir()
	st, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		if err := appendRecord(st, i, rotDay(i)); err != nil {
			t.Fatal(err)
		}
	}
	if err := st.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := st.SaveState(PersistState{ChainID: "c1", LastScannedHeight: 9}); err != nil {
		t.Fatal(err)
	}
	st.Close()

	archived := rotateLive(t, st.PublicationsPath(), "settlement_time", rotDay(5))
	rotateLive(t, st.PaymentsPath(), "time", rotDay(5))
	// a crash mid-write after the rotation
	f, err := os.OpenFile(st.PublicationsPath(), os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	f.WriteString(`{"promise_hash":"torn","settlement_ti`)
	f.Close()

	st2, err := OpenStore(dir)
	if err != nil {
		t.Fatalf("open after the rotation: %v", err)
	}
	defer st2.Close()
	if b, _ := os.ReadFile(st2.PublicationsPath()); strings.Contains(string(b), "torn") {
		t.Fatal("the torn tail of the rotated file was not repaired")
	}
	for i := 0; i < 10; i++ {
		tx, key := fmt.Sprintf("t%04d", i), fmt.Sprintf("k%04d", i)
		if live := i >= 5; st2.Seen(tx) != live || st2.PaymentSeen(key) != live {
			t.Fatalf("record %d: seen %v, payment seen %v; want %v (the live window only)", i, st2.Seen(tx), st2.PaymentSeen(key), live)
		}
	}
	// the last blocks scanned again, then the next one
	for i := 8; i < 11; i++ {
		if err := appendRecord(st2, i, rotDay(i)); err != nil {
			t.Fatal(err)
		}
	}
	if err := st2.Sync(); err != nil {
		t.Fatal(err)
	}
	first := 0
	if !archived {
		first = 5 // no segments without flock: the record is the live file
	}
	wantKeys(t, "publications", recordKeys(t, st2.PublicationsPath(), "promise_hash"), "p", first, 11)
	wantKeys(t, "payments", recordKeys(t, st2.PaymentsPath(), "dedupe_key"), "k", first, 11)
}
