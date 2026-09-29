package api

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
)

// derivedFixture is a store with publications and assignments, and a
// directory for the files.
type derivedFixture struct {
	t   *testing.T
	st  *store.Store
	dir string
	now time.Time
	r   *rand.Rand
	n   int
}

func newDerivedFixture(t *testing.T, seed int64) *derivedFixture {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "observer.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	if err := st.SetMeta("chain_id", "t", time.Now()); err != nil {
		t.Fatal(err)
	}
	f := &derivedFixture{t: t, st: st, dir: t.TempDir(), now: time.Date(2026, 9, 28, 16, 0, 0, 0, time.UTC), r: rand.New(rand.NewSource(seed))}
	f.grow(80)
	return f
}

// grow writes n more publications with their assignments.
func (f *derivedFixture) grow(n int) {
	f.t.Helper()
	pubs, asgs := fxLoad(f.r, f.now, n, 1000*f.n+10000)
	f.n++
	fxInsert(f.t, f.st.DB(), pubs, asgs)
}

// server is a server over the fixture's store keeping its files in dir
// ("" keeps none).
func (f *derivedFixture) server(dir string) *Server {
	s := &Server{st: f.st}
	if dir != "" {
		s.origRows.file = filepath.Join(dir, originalRowsFile)
		s.recent.file = filepath.Join(dir, endorsementLedgerFile)
		// No write of the memo's outlives the test and its directory.
		f.t.Cleanup(s.origRows.wait)
	}
	return s
}

// figures is everything the memo and the ledger feed, for s, as JSON:
// every window's load figures and the recent endorsements.
func (f *derivedFixture) figures(s *Server) string {
	f.t.Helper()
	ctx := context.Background()
	var b strings.Builder
	for _, win := range fxWindows(f.now) {
		l, err := s.loadByValidatorAt(ctx, win, "", f.now)
		if err != nil {
			f.t.Fatal(err)
		}
		fmt.Fprintf(&b, "%s %s\n", win.Name, jsonOf(f.t, l))
	}
	out := map[string]signingStats{}
	if err := s.recentSigning(ctx, "", out); err != nil {
		f.t.Fatal(err)
	}
	fmt.Fprintf(&b, "recent %s\n", recentString(out))
	return b.String()
}

// The memo and the ledger written by one process are read back by the next,
// caught up with what the store gained meanwhile, and give every figure a
// process with no files gives.
func TestTheMemoAndTheLedgerAreKeptAcrossARestart(t *testing.T) {
	f := newDerivedFixture(t, 3)
	ctx := context.Background()
	first := f.server(f.dir)
	want := f.figures(f.server(""))
	if got := f.figures(first); got != want {
		t.Fatalf("with files kept:\n got %s\nwant %s", got, want)
	}
	if err := first.keepDerived(ctx); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{originalRowsFile, endorsementLedgerFile} {
		if _, err := os.Stat(filepath.Join(f.dir, name)); err != nil {
			t.Fatalf("%s not written: %v", name, err)
		}
	}
	if !strings.HasPrefix(first.origRows.origin, "built from the store: no ") || !strings.HasPrefix(first.recent.origin, "built from the store: no ") {
		t.Errorf("a first start: memo %q, ledger %q", first.origRows.origin, first.recent.origin)
	}

	// A restart with nothing new: both are read back, and the memo looks
	// nothing up.
	again := f.server(f.dir)
	if got := f.figures(again); got != want {
		t.Fatalf("after a restart:\n got %s\nwant %s", got, want)
	}
	if !strings.HasPrefix(again.origRows.origin, "loaded ") || !strings.HasPrefix(again.recent.origin, "loaded ") {
		t.Fatalf("after a restart: memo %q, ledger %q", again.origRows.origin, again.recent.origin)
	}
	if again.origRows.size() != first.origRows.size() || again.recent.upTo != first.recent.upTo {
		t.Errorf("restart: memo %d entries (was %d), ledger through %d (was %d)",
			again.origRows.size(), first.origRows.size(), again.recent.upTo, first.recent.upTo)
	}

	// The store grows while nothing runs; the next start catches up.
	f.grow(30)
	want = f.figures(f.server(""))
	later := f.server(f.dir)
	if got := f.figures(later); got != want {
		t.Fatalf("after the store grew:\n got %s\nwant %s", got, want)
	}
	if !strings.HasPrefix(later.origRows.origin, "loaded ") || !strings.HasPrefix(later.recent.origin, "loaded ") {
		t.Fatalf("after the store grew: memo %q, ledger %q", later.origRows.origin, later.recent.origin)
	}
	if later.recent.upTo <= first.recent.upTo {
		t.Errorf("the ledger did not catch up: through %d, was %d", later.recent.upTo, first.recent.upTo)
	}
	// And what it wrote at the end of that is the caught-up state.
	if err := later.keepDerived(ctx); err != nil {
		t.Fatal(err)
	}
	var lf ledgerFile
	if ok, why := readDerived(filepath.Join(f.dir, endorsementLedgerFile), &lf); !ok || lf.UpTo != later.recent.upTo {
		t.Errorf("ledger file through %d (%v %s), want %d", lf.UpTo, ok, why, later.recent.upTo)
	}
}

// A file is used only for the store it was computed from while that store
// holds everything it was computed from. Every other file is refused and
// removed, and the memo and the ledger are built from the store, so every
// figure is what a process with no files computes.
func TestADerivedFileFromAnotherStoreOrAnEarlierStateIsRefused(t *testing.T) {
	ctx := context.Background()
	type edit struct {
		name string
		// break the files in dir, or the store, before the next start
		apply func(t *testing.T, f *derivedFixture, dir string)
		// what each file must be refused for, as its origin says it; ""
		// is a file that must be loaded
		memo, ledger string
	}
	// rewrite changes a file and seals it again, as a writer that got it
	// wrong would leave it: the digest is right, the content is not, and the
	// checks against the store are what must refuse it.
	rewrite := func(t *testing.T, path string, change func(m map[string]any)) {
		t.Helper()
		b, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		var m map[string]any
		if err := json.Unmarshal(b, &m); err != nil {
			t.Fatal(err)
		}
		change(m)
		delete(m, "digest")
		if b, err = json.Marshal(m); err != nil {
			t.Fatal(err)
		}
		if b, err = sealDerived(append([]byte(digestEmpty+","), b[1:]...)); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, b, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	// tamper changes a file as a hand or a damaged disk would: it still
	// parses, keeps its layout and its digest, and the digest is what must
	// refuse it.
	tamper := func(t *testing.T, path string, v any, change func()) {
		t.Helper()
		b, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(b, v); err != nil {
			t.Fatal(err)
		}
		change()
		if b, err = json.Marshal(v); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, b, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	both := func(t *testing.T, dir string, change func(m map[string]any)) {
		t.Helper()
		rewrite(t, filepath.Join(dir, originalRowsFile), change)
		rewrite(t, filepath.Join(dir, endorsementLedgerFile), change)
	}
	cases := []edit{
		{"another store's files", func(t *testing.T, f *derivedFixture, dir string) {
			// A store of its own, the same chain, the same rows even: its
			// files are its own.
			other := newDerivedFixture(t, 3)
			s := other.server(dir)
			other.figures(s)
			if err := s.keepDerived(ctx); err != nil {
				t.Fatal(err)
			}
		}, "computed from another store", "computed from another store"},
		{"truncated", func(t *testing.T, f *derivedFixture, dir string) {
			for _, name := range []string{originalRowsFile, endorsementLedgerFile} {
				p := filepath.Join(dir, name)
				b, err := os.ReadFile(p)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(p, b[:len(b)/2], 0o644); err != nil {
					t.Fatal(err)
				}
			}
		}, "its digest is not its body's", "its digest is not its body's"},
		{"empty", func(t *testing.T, f *derivedFixture, dir string) {
			for _, name := range []string{originalRowsFile, endorsementLedgerFile} {
				if err := os.WriteFile(filepath.Join(dir, name), nil, 0o644); err != nil {
					t.Fatal(err)
				}
			}
		}, "no digest where the file opens", "no digest where the file opens"},
		{"another definition", func(t *testing.T, f *derivedFixture, dir string) {
			both(t, dir, func(m map[string]any) { m["definition"] = "0" })
		}, "computed with another definition", "computed with another definition"},
		{"another format", func(t *testing.T, f *derivedFixture, dir string) {
			both(t, dir, func(m map[string]any) { m["format"] = 99 })
		}, "another format", "another format"},
		{"the store restored from before the files", func(t *testing.T, f *derivedFixture, dir string) {
			// What a restore from an older backup leaves: the newest rows
			// gone.
			db := f.st.DB()
			if _, err := db.Exec(`DELETE FROM assignments WHERE rowid > (SELECT MAX(rowid) - 40 FROM assignments)`); err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec(`DELETE FROM publications WHERE rowid > (SELECT MAX(rowid) - 5 FROM publications)`); err != nil {
				t.Fatal(err)
			}
		}, "older than the file", "older than the file"},
		{"the newest rows replaced under the same rowids", func(t *testing.T, f *derivedFixture, dir string) {
			db := f.st.DB()
			if _, err := db.Exec(`DELETE FROM assignments WHERE rowid > (SELECT MAX(rowid) - 40 FROM assignments)`); err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec(`DELETE FROM publications WHERE rowid > (SELECT MAX(rowid) - 5 FROM publications)`); err != nil {
				t.Fatal(err)
			}
			f.grow(10) // takes the freed rowids again, with other rows
		}, "is another one now", "is another one now"},
		{"a memo entry the record contradicts", func(t *testing.T, f *derivedFixture, dir string) {
			// The newest publication the memo holds a number for gets
			// another number.
			rows, err := f.st.DB().Query(`SELECT promise_hash FROM publications ORDER BY rowid DESC LIMIT ?`, memoChecked)
			if err != nil {
				t.Fatal(err)
			}
			var newest []any
			for rows.Next() {
				var h string
				if err := rows.Scan(&h); err != nil {
					t.Fatal(err)
				}
				newest = append(newest, h)
			}
			rows.Close()
			rewrite(t, filepath.Join(dir, originalRowsFile), func(m map[string]any) {
				vals := m["values"].(map[string]any)
				for _, want := range newest {
					for k, hs := range vals {
						list := hs.([]any)
						for i, h := range list {
							if h == want {
								vals[k] = append(list[:i:i], list[i+1:]...)
								vals["123456789"] = []any{h}
								return
							}
						}
					}
				}
				t.Fatal("fixture: none of the newest publications has a number in the memo")
			})
		}, "not the file's", ""},
		{"a ledger row the store contradicts", func(t *testing.T, f *derivedFixture, dir string) {
			rewrite(t, filepath.Join(dir, endorsementLedgerFile), func(m map[string]any) {
				for _, e := range m["validators"].(map[string]any) {
					top := e.(map[string]any)["top"].([]any)
					row := top[0].([]any)
					row[3] = 1 - row[3].(float64) // the endorsement flipped
					return
				}
			})
		}, "", "is not what the file says"},
		{"a ledger mark the store contradicts", func(t *testing.T, f *derivedFixture, dir string) {
			rewrite(t, filepath.Join(dir, endorsementLedgerFile), func(m map[string]any) {
				m["up_to_row"].(map[string]any)["validator_address"] = "nobody"
			})
		}, "", "is another one now"},
		{"a migration since the files", func(t *testing.T, f *derivedFixture, dir string) {
			// One may rewrite, for rows below the marks, a column the files
			// were computed from; the load would never see it.
			if _, err := f.st.DB().Exec(`INSERT INTO schema_migrations (version, applied_at)
				SELECT MAX(version) + 1, ? FROM schema_migrations`, store.TS(f.now)); err != nil {
				t.Fatal(err)
			}
		}, "computed under schema version", "computed under schema version"},
		{"a memo entry below the ones read again, edited", func(t *testing.T, f *derivedFixture, dir string) {
			// Past the newest memoChecked publications, which the load reads
			// again: only the digest can tell.
			var older []string
			rows, err := f.st.DB().Query(`SELECT promise_hash FROM publications ORDER BY rowid DESC LIMIT -1 OFFSET ?`, memoChecked)
			if err != nil {
				t.Fatal(err)
			}
			for rows.Next() {
				var h string
				if err := rows.Scan(&h); err != nil {
					t.Fatal(err)
				}
				older = append(older, h)
			}
			rows.Close()
			var mf memoFile
			tamper(t, filepath.Join(dir, originalRowsFile), &mf, func() {
				for _, want := range older {
					for k, hs := range mf.Values {
						for i, h := range hs {
							if h == want {
								mf.Values[k] = append(hs[:i:i], hs[i+1:]...)
								mf.Values["24"] = append(mf.Values["24"], h)
								return
							}
						}
					}
				}
				t.Fatal("fixture: none of the older publications has a number in the memo")
			})
		}, "its digest is not its body's", ""},
		{"a validator dropped from the ledger", func(t *testing.T, f *derivedFixture, dir string) {
			// Nothing of it is left to read again, and only the assignments
			// past up_to would be folded in later.
			var lf ledgerFile
			tamper(t, filepath.Join(dir, endorsementLedgerFile), &lf, func() {
				for addr := range lf.Validators {
					delete(lf.Validators, addr)
					return
				}
			})
		}, "", "its digest is not its body's"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := newDerivedFixture(t, 5)
			dir := t.TempDir()
			s := f.server(dir)
			f.figures(s)
			if err := s.keepDerived(ctx); err != nil {
				t.Fatal(err)
			}
			c.apply(t, f, dir)
			want := f.figures(f.server(""))
			next := f.server(dir)
			if got := f.figures(next); got != want {
				t.Fatalf("after %s:\n got %s\nwant %s", c.name, got, want)
			}
			check := func(what, origin, file, why string) {
				t.Helper()
				if why != "" {
					if !strings.HasPrefix(origin, "built from the store: "+filepath.Join(dir, file)+" refused: ") || !strings.Contains(origin, why) {
						t.Errorf("%s after %s: %q, want it refused: ...%s", what, c.name, origin, why)
					}
					return
				}
				if !strings.HasPrefix(origin, "loaded ") {
					t.Errorf("%s was refused after %s: %q", what, c.name, origin)
				}
			}
			check("the memo", next.origRows.origin, originalRowsFile, c.memo)
			check("the ledger", next.recent.origin, endorsementLedgerFile, c.ledger)
			t.Logf("memo: %s; ledger: %s", next.origRows.origin, next.recent.origin)
		})
	}
}

// A file as written is read back, and one with any single byte of it
// changed, its digest's own included, is refused.
func TestADerivedFileWithAnyByteChangedIsRefused(t *testing.T) {
	f := newDerivedFixture(t, 7)
	dir := t.TempDir()
	s := f.server(dir)
	f.figures(s)
	if err := s.keepDerived(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{originalRowsFile, endorsementLedgerFile} {
		p := filepath.Join(dir, name)
		var v map[string]any
		if ok, why := readDerived(p, &v); !ok {
			t.Fatalf("%s as written: refused: %s", name, why)
		}
		b, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		for i := range b {
			c := append([]byte(nil), b...)
			c[i] ^= 1
			if why := unsealDerived(c); why == "" {
				t.Errorf("%s with byte %d changed from %q to %q: not refused", name, i, b[i], c[i])
			}
		}
	}
}

// The ledger's fold is the one ledgerVersion names. A file keeps what the
// fold made of the rows below its mark, and the load reads again only the
// rows it publishes, so a fold changed under the same version would carry
// on from a file folded the old way. This digests what add and newer make
// of a fixed run of rows, in rowid order as refresh hands them, with ties
// on height, transaction index and settlement time, and rows handed in
// again. When it fails the fold has changed: bump ledgerVersion and record
// the new digest under it.
func TestTheLedgerFoldIsTheOneItsVersionNames(t *testing.T) {
	folds := map[int]string{
		1: "906f637593e789932422d479fd36836b2e5198ca3f90aa9262174d710903af66",
	}
	r := rand.New(rand.NewSource(1))
	type in struct {
		row ledgerRow
		at  string
	}
	var seen []in
	e := &ledgerEntry{}
	var b strings.Builder
	for id := int64(1); id <= 3000; id++ {
		x := in{ledgerRow{height: 1000 + r.Int63n(300), txIndex: r.Int63n(3), rowid: id, attested: r.Int63n(2)},
			fmt.Sprintf("2026-09-%02dT%02d:00:00Z", 1+r.Intn(28), r.Intn(4))}
		if len(seen) > 0 && r.Intn(10) == 0 {
			x = seen[r.Intn(len(seen))] // a refresh that failed half way, run again
		} else {
			seen = append(seen, x)
		}
		e.add(x.row, x.at)
		fmt.Fprintf(&b, "%v %q %v %d\n", e.top, e.last, e.set, e.lastRow)
	}
	sum := sha256.Sum256([]byte(b.String()))
	got := hex.EncodeToString(sum[:])
	if want := folds[ledgerVersion]; got != want {
		t.Fatalf("the fold under ledgerVersion %d digests to %s, recorded %q: it has changed, so bump ledgerVersion and record this under it",
			ledgerVersion, got, want)
	}
}

// No computation waits for the memo's file. With a write holding it for as
// long as it likes, a computation still reads the file's state, learns what
// the memo lacks and answers, and the write it asks for is made once the
// file is free, in the background.
func TestAComputationDoesNotWaitForTheMemosFile(t *testing.T) {
	f := newDerivedFixture(t, 9)
	ctx := context.Background()
	s := f.server(f.dir)
	f.figures(s)
	s.origRows.wait()

	f.grow(40)
	want := f.figures(f.server(""))
	s.origRows.mu.Lock()
	s.origRows.savedAt = time.Time{} // as if memoSaveEvery had passed
	s.origRows.mu.Unlock()
	s.origRows.fileMu.Lock() // a write that takes its time
	held := true
	release := func() {
		if held {
			held = false
			s.origRows.fileMu.Unlock()
		}
	}
	defer release()
	done := make(chan error, 1)
	go func() {
		for _, win := range fxWindows(f.now) {
			if _, err := s.loadByValidatorAt(ctx, win, "", f.now); err != nil {
				done <- err
				return
			}
		}
		done <- nil
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("a computation waited for a write of the memo's file")
	}
	release()
	if got := f.figures(s); got != want {
		t.Fatalf("with the file held:\n got %s\nwant %s", got, want)
	}

	s.origRows.wait()
	var mf memoFile
	if ok, why := readDerived(filepath.Join(f.dir, originalRowsFile), &mf); !ok {
		t.Fatalf("the memo's file: %s", why)
	}
	n := len(mf.Nulls)
	for _, hs := range mf.Values {
		n += len(hs)
	}
	if n != s.origRows.size() {
		t.Errorf("the file holds %d entries, the memo %d: the write asked for while the file was held was not made", n, s.origRows.size())
	}
}
