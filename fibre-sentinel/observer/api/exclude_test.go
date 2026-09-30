package api_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/probe"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/api"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
)

// selfAddrs are the four validators the exclusion fixture writes: one clean
// verdict of each kind the buckets separate. Hex, because that is what the
// store holds and what ?exclude= parses.
var selfAddrs = map[string]string{
	"kept":      strings.Repeat("a1", 20),
	"broke":     strings.Repeat("b2", 20),
	"earlyonly": strings.Repeat("c3", 20),
	"silent":    strings.Repeat("d4", 20),
}

func excludeFixture(t *testing.T) *httptest.Server {
	t.Helper()
	ts, _ := excludeFixtureStore(t)
	return ts
}

func excludeFixtureStore(t *testing.T) (*httptest.Server, *store.Store) {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "observer.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	now := time.Now().UTC().Truncate(time.Second)
	created, msu := now.Add(-2*time.Hour), now.Add(-30*time.Minute)
	at := msu.Add(-10 * time.Minute)
	// One blob Available on kept's rows, beside two validators that failed
	// and count neither way; one Unavailable, whose only validator did not
	// serve.
	insertReading(t, st, "ex1", created, msu, 4, probe.EndReadLabel, at, []endVal{
		{addr: selfAddrs["kept"], rows: 4, w: ok},
		{addr: selfAddrs["earlyonly"], rows: 2, w: err500},
		{addr: selfAddrs["silent"], rows: 2, w: refused},
	})
	insertReading(t, st, "ex2", created, msu, 2, probe.EndReadLabel, at, []endVal{
		{addr: selfAddrs["broke"], rows: 2, w: gone},
	})
	return httptestServer(t, st), st
}

type netFigures struct {
	Obligations obligationsJSON `json:"obligations"`
	Excluded    []string        `json:"excluded"`
	ExcludeNote string          `json:"exclude_note"`
}

// tallyFigures are the reading tallies the summary keeps and does not
// publish, recomputed with or without an exclusion.
type tallyFigures struct {
	Classes    map[string]int64 `json:"classes"`
	ProbeCount int64            `json:"probe_count"`
}

func talliesExcluding(t *testing.T, st *store.Store, exclude ...string) tallyFigures {
	t.Helper()
	raw, err := api.NetworkExcludingJSON(st, "test", "all", exclude)
	if err != nil {
		t.Fatal(err)
	}
	var out tallyFigures
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatal(err)
	}
	return out
}

// This observer's own operator runs a validator on the network
// it measures. The answer is not to hide that row but to let a reader
// recompute the headline figures without it, so `?exclude=` is a filter on
// the request and never a setting on the deployment.
func TestExcludeRecomputesTheHeadlineWithoutAValidator(t *testing.T) {
	ts, st := excludeFixtureStore(t)

	var all netFigures
	if code := get(t, ts, "/v1/network?window=all", &all); code != 200 {
		t.Fatalf("network: %d", code)
	}
	o := all.Obligations
	if o.Total != 4 || o.Served != 1 || o.Broken != 1 || o.NotCounted != 2 {
		t.Fatalf("fixture: obligations = %+v, want one served, one not served, two counted neither way", o)
	}
	allTallies := talliesExcluding(t, st)
	if allTallies.Classes["FAULT"] != 1 {
		t.Fatalf("fixture: FAULT readings = %d, want 1", allTallies.Classes["FAULT"])
	}
	if all.Excluded != nil || all.ExcludeNote != "" {
		t.Errorf("an unfiltered answer must not claim an exclusion: %v %q", all.Excluded, all.ExcludeNote)
	}

	// Take the one broken obligation out. Every figure drawn from a
	// per-validator population moves with it, and nothing else does.
	var less netFigures
	if code := get(t, ts, "/v1/network?window=all&exclude="+selfAddrs["broke"], &less); code != 200 {
		t.Fatalf("exclude: %d", code)
	}
	l := less.Obligations
	if l.Total != 3 || l.Served != 1 || l.Broken != 0 || l.NotCounted != 2 {
		t.Errorf("excluded obligations = %+v, want the same without the not-served one", l)
	}
	if l.Rate.Num != 1 || l.Rate.Den != 1 {
		t.Errorf("excluded rate = %d/%d, want 1/1", l.Rate.Num, l.Rate.Den)
	}
	lessTallies := talliesExcluding(t, st, selfAddrs["broke"])
	if lessTallies.Classes["FAULT"] != 0 {
		t.Errorf("excluded FAULT readings = %d, want 0: the only one was that validator's", lessTallies.Classes["FAULT"])
	}
	if lessTallies.ProbeCount != allTallies.ProbeCount-1 {
		t.Errorf("excluded probe_count = %d, want %d: its one reading left with it", lessTallies.ProbeCount, allTallies.ProbeCount-1)
	}
	if len(less.Excluded) != 1 || less.Excluded[0] != selfAddrs["broke"] {
		t.Errorf("excluded = %v, want the address that was asked for", less.Excluded)
	}
	if less.ExcludeNote == "" {
		t.Error("a filtered answer must say it is filtered, so the figure cannot be quoted as the headline")
	}

	// And the shared snapshot is untouched: one reader's filter is not
	// everyone's headline. This is the bug class a pinned window had before
	// it was given its own path.
	var again netFigures
	if code := get(t, ts, "/v1/network?window=all", &again); code != 200 {
		t.Fatalf("network after exclude: %d", code)
	}
	if again.Obligations != all.Obligations {
		t.Errorf("the cached summary was poisoned by a filtered request: %+v, want %+v", again.Obligations, all.Obligations)
	}
	if again.Excluded != nil {
		t.Errorf("the unfiltered answer came back claiming an exclusion: %v", again.Excluded)
	}
	partsAfter(t, st, "test", time.Time{})
}

// A filtered answer is computed per request, so it must not be stored by a
// cache in front of this API either.
func TestExcludeIsNotCacheable(t *testing.T) {
	ts := excludeFixture(t)
	resp, err := http.Get(ts.URL + "/v1/network?window=all&exclude=" + selfAddrs["broke"])
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if got := resp.Header.Get("Cache-Control"); got != "no-store" {
		t.Errorf("Cache-Control = %q, want no-store", got)
	}
}

// The parameter takes what the rest of the API takes, refuses what it cannot
// resolve to a validator, and is bounded: it answers "recompute without the
// people who run this site", not "carve any subset out and quote the result".
func TestExcludeRejectsWhatItCannotResolve(t *testing.T) {
	ts := excludeFixture(t)
	for _, q := range []string{
		"&exclude=not-an-address",
		"&exclude=" + strings.Repeat("zz", 20),
		"&exclude=" + strings.Join([]string{
			strings.Repeat("01", 20), strings.Repeat("02", 20), strings.Repeat("03", 20),
			strings.Repeat("04", 20), strings.Repeat("05", 20), strings.Repeat("06", 20),
			strings.Repeat("07", 20), strings.Repeat("08", 20), strings.Repeat("09", 20),
		}, ","),
	} {
		if code := get(t, ts, "/v1/network?window=all"+q, nil); code != 400 {
			t.Errorf("%s: %d, want 400", q, code)
		}
	}
	// Repeated and comma-separated forms both work, and a duplicate is not
	// counted twice.
	var f netFigures
	q := "/v1/network?window=all&exclude=" + selfAddrs["broke"] + "," + selfAddrs["broke"] + "&exclude=" + selfAddrs["silent"]
	if code := get(t, ts, q, &f); code != 200 {
		t.Fatalf("%s: %d", q, code)
	}
	if len(f.Excluded) != 2 {
		t.Errorf("excluded = %v, want the two distinct addresses", f.Excluded)
	}
	if f.Obligations.Total != 2 {
		t.Errorf("obligations total = %d, want 2", f.Obligations.Total)
	}
}
