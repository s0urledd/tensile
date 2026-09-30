package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
)

// TestSnapshotHarness computes every snapshot of a store copy at one fixed
// moment and writes each as the JSON it is published and persisted as, so
// that two builds can be compared byte for byte on real data: a change that
// must not move a published figure is run once from the build before it
// (TENSILE_SNAPSHOT_OUT alone) and once from the change, pointed at the
// first run's files (TENSILE_SNAPSHOT_REF). Every case is compared whole,
// computed_at and compute_ms aside, and a single difference fails the run.
//
// It is skipped unless TENSILE_SNAPSHOT_DB names a store. The store is
// opened the way the API opens it (store.OpenReadOnly) and only read, so a
// copy taken with SQLite's backup API is enough; never point it at a store
// a collector is writing, whose figures move between the two runs.
//
// The clock is fixed (TENSILE_SNAPSHOT_NOW, Server.clock): a provisional
// fault, what a validator holds now and whether a live window's readings
// have closed all depend on it, and two runs minutes apart would otherwise
// differ for that reason alone.
//
//	TENSILE_SNAPSHOT_DB          the store copy
//	TENSILE_SNAPSHOT_NOW         the clock, RFC 3339 (nanoseconds allowed)
//	TENSILE_SNAPSHOT_OUT         where each case's JSON is written
//	TENSILE_SNAPSHOT_REF         optional: another run's OUT to compare with
//	TENSILE_SNAPSHOT_VANTAGE     the unit's -vantage (default "local")
//	TENSILE_SNAPSHOT_PUBLISHERS  optional: the unit's -publishers file
//	TENSILE_SNAPSHOT_PINS        optional: as_of pins, comma-separated, RFC 3339;
//	                             "now" is the clock
//	TENSILE_SNAPSHOT_EXCLUDE     optional: exclusion sizes, e.g. "1,8": the
//	                             network is also computed without that many
//	                             bonded validators (the lowest addresses)
//	TENSILE_SNAPSHOT_CASES       optional: a regexp the case names must match
//	TENSILE_SNAPSHOT_STATE       optional: the snapshot directory the memo and
//	                             the ledger are kept in (derived.go); they are
//	                             brought up to date for the whole record first,
//	                             timed, and written there at the end
//	TENSILE_SNAPSHOT_EXPECT      optional, with STATE: how both must have begun,
//	                             "loaded" or "built"
//
// Run with -timeout 0: a busy store takes minutes per build.
func TestSnapshotHarness(t *testing.T) {
	dbPath := os.Getenv("TENSILE_SNAPSHOT_DB")
	if dbPath == "" {
		t.Skip("TENSILE_SNAPSHOT_DB is not set; the harness runs against a store copy")
	}
	now, err := time.Parse(time.RFC3339Nano, os.Getenv("TENSILE_SNAPSHOT_NOW"))
	if err != nil {
		t.Fatalf("TENSILE_SNAPSHOT_NOW: %v", err)
	}
	out := os.Getenv("TENSILE_SNAPSHOT_OUT")
	if out == "" {
		t.Fatal("TENSILE_SNAPSHOT_OUT is not set")
	}
	if err := os.MkdirAll(out, 0o755); err != nil {
		t.Fatal(err)
	}
	ref := os.Getenv("TENSILE_SNAPSHOT_REF")
	var only *regexp.Regexp
	if v := os.Getenv("TENSILE_SNAPSHOT_CASES"); v != "" {
		if only, err = regexp.Compile(v); err != nil {
			t.Fatalf("TENSILE_SNAPSHOT_CASES: %v", err)
		}
	}
	vantage := os.Getenv("TENSILE_SNAPSHOT_VANTAGE")
	if vantage == "" {
		vantage = "local"
	}
	st, err := store.OpenReadOnly(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	var opts []Option
	if p := os.Getenv("TENSILE_SNAPSHOT_PUBLISHERS"); p != "" {
		labels, err := LoadPublisherLabels(p)
		if err != nil {
			t.Fatal(err)
		}
		opts = append(opts, WithPublisherLabels(labels))
	}
	state := os.Getenv("TENSILE_SNAPSHOT_STATE")
	if state != "" {
		opts = append(opts, WithSnapshotDir(state))
	}
	s := newServer(st, VantageInfo{Name: vantage}, nil, opts...)
	s.clock = func() time.Time { return now }
	ctx := context.Background()
	if state != "" {
		// What a start costs before the first figure: the memo for every
		// publication "all" reads, and the ledger for every assignment.
		t0 := time.Now()
		all := windowFor("all", now)
		if _, err := s.origRows.doc(ctx, st.DB(), all.startArg(), all.endArg(), store.TS(now)); err != nil {
			t.Fatal(err)
		}
		if err := s.recent.fill(ctx, st.DB(), "", map[string]signingStats{}); err != nil {
			t.Fatal(err)
		}
		t.Logf("memo and ledger up to date in %.3fs", time.Since(t0).Seconds())
		t.Logf("memo: %s", s.origRows.origin)
		t.Logf("ledger: %s", s.recent.origin)
		if want := os.Getenv("TENSILE_SNAPSHOT_EXPECT"); want != "" &&
			(!strings.HasPrefix(s.origRows.origin, want) || !strings.HasPrefix(s.recent.origin, want)) {
			t.Errorf("the memo and the ledger were to be %s", want)
		}
		defer func() {
			if err := s.keepDerived(ctx); err != nil {
				t.Errorf("keeping the memo and the ledger: %v", err)
			}
		}()
	}

	cases := harnessCases(t, s, now)
	var failed, compared int
	for _, c := range cases {
		if only != nil && !only.MatchString(c.name) {
			continue
		}
		t0 := time.Now()
		b, err := c.run(ctx)
		took := time.Since(t0)
		if err != nil {
			t.Errorf("%s: %v", c.name, err)
			failed++
			continue
		}
		if err := os.WriteFile(filepath.Join(out, c.name+".json"), b, 0o644); err != nil {
			t.Fatal(err)
		}
		verdict := "written"
		if ref != "" {
			want, err := os.ReadFile(filepath.Join(ref, c.name+".json"))
			switch {
			case err != nil:
				verdict = "NO REFERENCE"
				failed++
			case !bytes.Equal(scrubComputed(want), scrubComputed(b)):
				verdict = "DIFFERENT " + firstDifference(scrubComputed(want), scrubComputed(b))
				failed++
			default:
				verdict = "identical"
			}
			compared++
		}
		t.Logf("%-44s %9.3fs %8d bytes  %s", c.name, took.Seconds(), len(b), verdict)
	}
	if ref != "" {
		t.Logf("%d cases compared with %s, %d differ or failed", compared, ref, failed)
	}
	if failed > 0 {
		t.Fail()
	}
}

// harnessCase is one snapshot: its name (the file it is written to) and how
// it is computed, as the cache or the handler computes it.
type harnessCase struct {
	name string
	run  func(context.Context) ([]byte, error)
}

// harnessCases is every snapshot the harness computes: each window of each
// cache as its keeper computes it at the clock, the same windows pinned
// (?as_of=) at every pin, as the handlers compute them, and the network with
// validators excluded (?exclude=).
func harnessCases(t *testing.T, s *Server, now time.Time) []harnessCase {
	t.Helper()
	var cases []harnessCase
	for _, name := range warmWindows {
		win := windowFor(name, now)
		cases = append(cases,
			harnessCase{"network-" + name, func(ctx context.Context) ([]byte, error) {
				resp, err := s.net.compute(ctx, win)
				if err != nil {
					return nil, err
				}
				return harnessJSON(resp, networkOutOf(resp))
			}},
			harnessCase{"validators-" + name, func(ctx context.Context) ([]byte, error) {
				snap, err := s.vals.compute(ctx, win)
				if err != nil {
					return nil, err
				}
				pub := map[string]any{"window": snap.Window, "validators": listOfRows(snap.Rows), "record_through": snap.RecordThrough}
				if _, label, err := s.rolledFor(ctx, win, ""); err == nil && label != nil {
					pub["rolled_up"] = label
				}
				return harnessJSON(snap, pub)
			}},
			harnessCase{"market-" + name, func(ctx context.Context) ([]byte, error) {
				resp, err := s.market.compute(ctx, win)
				if err != nil {
					return nil, err
				}
				return harnessJSON(resp, nil)
			}})
	}
	var pins []string
	if v := os.Getenv("TENSILE_SNAPSHOT_PINS"); v != "" {
		pins = strings.Split(v, ",")
	}
	for _, pin := range pins {
		at := strings.TrimSpace(pin)
		if at == "now" {
			at = now.Format(time.RFC3339Nano)
		}
		label := strings.NewReplacer(":", "", "-", "", ".", "_").Replace(at)
		for _, name := range warmWindows {
			win := harnessWindow(t, now, url.Values{"window": {name}, "as_of": {at}})
			cases = append(cases,
				harnessCase{"network-" + name + "-asof-" + label, func(ctx context.Context) ([]byte, error) {
					resp, err := s.computeNetwork(ctx, win, excludeSet{}, nil)
					if err != nil {
						return nil, err
					}
					resp.RecordThrough = s.recordThrough(ctx)
					return harnessJSON(resp, networkOutOf(resp))
				}},
				harnessCase{"validators-" + name + "-asof-" + label, func(ctx context.Context) ([]byte, error) {
					rows, err := s.validatorRows(ctx, win, "")
					if err != nil {
						return nil, err
					}
					return harnessJSON(rows, map[string]any{"window": win, "validators": listOfRows(rows), "as_of_note": AsOfNote,
						"record_through": s.recordThrough(ctx)})
				}},
				harnessCase{"market-" + name + "-asof-" + label, func(ctx context.Context) ([]byte, error) {
					resp, err := s.computeMarket(ctx, win)
					if err != nil {
						return nil, err
					}
					resp.AsOfNote = marketAsOfNote
					return harnessJSON(resp, nil)
				}})
		}
	}
	if v := os.Getenv("TENSILE_SNAPSHOT_EXCLUDE"); v != "" {
		for _, f := range strings.Split(v, ",") {
			n, err := strconv.Atoi(strings.TrimSpace(f))
			if err != nil || n < 0 {
				t.Fatalf("TENSILE_SNAPSHOT_EXCLUDE: %q is not a count", f)
			}
			addrs := harnessValidators(t, s, n)
			q := url.Values{"exclude": {strings.Join(addrs, ",")}}
			ex, excluded, err := s.parseExclude(httptest.NewRequest("GET", "/v1/network?"+q.Encode(), nil))
			if err != nil {
				t.Fatalf("exclude %d: %v", n, err)
			}
			for _, name := range warmWindows {
				win := windowFor(name, now)
				cases = append(cases, harnessCase{fmt.Sprintf("network-%s-exclude-%d", name, n), func(ctx context.Context) ([]byte, error) {
					resp, err := s.computeNetwork(ctx, win, ex, excluded)
					if err != nil {
						return nil, err
					}
					resp.RecordThrough = s.recordThrough(ctx)
					return harnessJSON(resp, networkOutOf(resp))
				}})
			}
		}
	}
	return cases
}

// harnessWindow is the window a request with q would get at now.
func harnessWindow(t *testing.T, now time.Time, q url.Values) Window {
	t.Helper()
	win, err := parseWindow(httptest.NewRequest("GET", "/v1/network?"+q.Encode(), nil), now)
	if err != nil {
		t.Fatalf("%s: %v", q.Encode(), err)
	}
	return win
}

// harnessValidators is n bonded validators, the lowest addresses first, so
// every build excludes the same ones.
func harnessValidators(t *testing.T, s *Server, n int) []string {
	t.Helper()
	rows, err := s.st.DB().Query(`SELECT LOWER(cons_address) FROM validator_identities
		WHERE status = 'BOND_STATUS_BONDED' ORDER BY 1 LIMIT ?`, n)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var a string
		if err := rows.Scan(&a); err != nil {
			t.Fatal(err)
		}
		out = append(out, a)
	}
	if len(out) < n {
		t.Fatalf("exclude %d: the store has %d bonded validators", n, len(out))
	}
	return out
}

// harnessJSON is a case's file: the value as the snapshot file holds it and,
// where the handler reshapes it, the body a reader is sent.
func harnessJSON(snapshot, published any) ([]byte, error) {
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	if err := enc.Encode(map[string]any{"snapshot": snapshot, "published": published}); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

// computedFields are the two fields that say when and how fast, not what.
var computedFields = regexp.MustCompile(`"(computed_at|compute_ms)":("[^"]*"|[0-9]+),?`)

func scrubComputed(b []byte) []byte { return computedFields.ReplaceAll(b, nil) }

// firstDifference locates the first byte where a and b part, with some of
// each around it.
func firstDifference(a, b []byte) string {
	i := 0
	for i < len(a) && i < len(b) && a[i] == b[i] {
		i++
	}
	from := max(i-80, 0)
	return fmt.Sprintf("at byte %d:\n  ref: %s\n  got: %s", i, a[from:min(i+80, len(a))], b[from:min(i+80, len(b))])
}
