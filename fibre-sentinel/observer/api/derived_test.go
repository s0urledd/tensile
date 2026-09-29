package api

import (
	"context"
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
		// which file must be refused ("" for both)
		memo, ledger bool
	}
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
		b, err = json.Marshal(m)
		if err != nil {
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
		}, true, true},
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
		}, true, true},
		{"empty", func(t *testing.T, f *derivedFixture, dir string) {
			for _, name := range []string{originalRowsFile, endorsementLedgerFile} {
				if err := os.WriteFile(filepath.Join(dir, name), nil, 0o644); err != nil {
					t.Fatal(err)
				}
			}
		}, true, true},
		{"another definition", func(t *testing.T, f *derivedFixture, dir string) {
			both(t, dir, func(m map[string]any) { m["definition"] = "0" })
		}, true, true},
		{"another format", func(t *testing.T, f *derivedFixture, dir string) {
			both(t, dir, func(m map[string]any) { m["format"] = 99 })
		}, true, true},
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
		}, true, true},
		{"the newest rows replaced under the same rowids", func(t *testing.T, f *derivedFixture, dir string) {
			db := f.st.DB()
			if _, err := db.Exec(`DELETE FROM assignments WHERE rowid > (SELECT MAX(rowid) - 40 FROM assignments)`); err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec(`DELETE FROM publications WHERE rowid > (SELECT MAX(rowid) - 5 FROM publications)`); err != nil {
				t.Fatal(err)
			}
			f.grow(10) // takes the freed rowids again, with other rows
		}, true, true},
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
		}, true, false},
		{"a ledger row the store contradicts", func(t *testing.T, f *derivedFixture, dir string) {
			rewrite(t, filepath.Join(dir, endorsementLedgerFile), func(m map[string]any) {
				for _, e := range m["validators"].(map[string]any) {
					top := e.(map[string]any)["top"].([]any)
					row := top[0].([]any)
					row[3] = 1 - row[3].(float64) // the endorsement flipped
					return
				}
			})
		}, false, true},
		{"a ledger mark the store contradicts", func(t *testing.T, f *derivedFixture, dir string) {
			rewrite(t, filepath.Join(dir, endorsementLedgerFile), func(m map[string]any) {
				m["up_to_row"].(map[string]any)["validator_address"] = "nobody"
			})
		}, false, true},
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
			check := func(what, origin, file string, refused bool) {
				t.Helper()
				if refused {
					if !strings.HasPrefix(origin, "built from the store: "+filepath.Join(dir, file)+" refused: ") {
						t.Errorf("%s was used after %s: %q", what, c.name, origin)
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
