package api

import (
	"context"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
)

// A snapshot nobody reads must still be refreshed as its TTL runs out:
// otherwise the reader who finally triggers the refresh is handed the old
// figure whole, and on a quiet site that figure can be a day old.
func TestKeeperRefreshesAStaleWindowWithoutARead(t *testing.T) {
	var n atomic.Int64
	c := newSnapshotCache("t", func(ctx context.Context, win Window) (int64, error) {
		return n.Add(1), nil
	})
	now := time.Now()
	c.refreshDue(nil, now)
	if got := n.Load(); got != int64(len(warmWindows)) {
		t.Fatalf("an empty cache: %d computations, want one per window (%d)", got, len(warmWindows))
	}
	// nothing is due inside the TTL
	c.refreshDue(nil, now)
	if got := n.Load(); got != int64(len(warmWindows)) {
		t.Fatalf("a fresh cache was recomputed: %d computations", got)
	}
	// age the 24h window past its TTL and only that one is recomputed
	c.mu.Lock()
	c.entries["24h"].at = now.Add(-2 * time.Hour)
	c.mu.Unlock()
	c.refreshDue(nil, time.Now())
	if got := n.Load(); got != int64(len(warmWindows))+1 {
		t.Fatalf("after aging 24h: %d computations, want %d", got, len(warmWindows)+1)
	}
	c.mu.Lock()
	at := c.entries["24h"].at
	c.mu.Unlock()
	if time.Since(at) > time.Minute {
		t.Errorf("the stale 24h snapshot was not replaced (taken %s)", at)
	}
}

// A window being refreshed by a reader is left alone: the keeper must not
// stack a second computation of the same aggregate behind it.
func TestKeeperSkipsAWindowAlreadyRefreshing(t *testing.T) {
	var n atomic.Int64
	c := newSnapshotCache("t", func(ctx context.Context, win Window) (int64, error) {
		return n.Add(1), nil
	})
	c.mu.Lock()
	for _, w := range warmWindows {
		c.refreshing[w] = true
	}
	c.mu.Unlock()
	c.refreshDue(nil, time.Now())
	if got := n.Load(); got != 0 {
		t.Fatalf("%d computations started behind ones already running", got)
	}
}

// Every figure computed before Fibre went live describes a chain without
// Fibre. At activation those snapshots are dropped, not served for the rest
// of their TTL as "nothing happened".
func TestActivationDropsPreActivationSnapshots(t *testing.T) {
	s := newSnapshotServer(t)
	if err := s.st.SetMeta("fibre_active", "no", time.Now()); err != nil {
		t.Fatal(err)
	}
	var n atomic.Int64
	c := newSnapshotCache("market", func(ctx context.Context, win Window) (int64, error) {
		return n.Add(1), nil
	})
	c.revision = s.activationRevision
	ctx := context.Background()
	win := testWindow("30d")
	if v, _, _, err := c.get(ctx, nil, win); err != nil || v != 1 {
		t.Fatalf("first read: %d, %v", v, err)
	}
	if v, _, _, _ := c.get(ctx, nil, win); v != 1 {
		t.Fatalf("a read inside the TTL recomputed: %d", v)
	}
	if err := s.st.SetMeta("fibre_active", "yes", time.Now()); err != nil {
		t.Fatal(err)
	}
	if v, _, _, err := c.get(ctx, nil, win); err != nil || v != 2 {
		t.Fatalf("after activation the pre-activation snapshot was served (%d, %v)", v, err)
	}
}

// The verdict snapshots carry both invalidations: a hold and activation.
func TestSnapshotRevisionCarriesHoldsAndActivation(t *testing.T) {
	s := newSnapshotServer(t)
	before := s.snapshotRevision()
	if err := s.st.SetMeta("fibre_active", "yes", time.Now()); err != nil {
		t.Fatal(err)
	}
	if s.snapshotRevision() == before {
		t.Error("activation did not change the revision the network and validator snapshots are served under")
	}
	PartsAfter(t, s.st, "test", time.Time{})
}

// refreshDue with windows named refreshes those and nothing else.
func TestRefreshDueOnlyTheNamedWindows(t *testing.T) {
	var mu sync.Mutex
	seen := map[string]int{}
	c := newSnapshotCache("t", func(ctx context.Context, win Window) (int64, error) {
		mu.Lock()
		defer mu.Unlock()
		seen[win.Name]++
		return 1, nil
	})
	c.refreshDue(nil, time.Now(), "24h")
	mu.Lock()
	defer mu.Unlock()
	if len(seen) != 1 || seen["24h"] != 1 {
		t.Fatalf("refreshDue(24h) computed %v, want only 24h once", seen)
	}
}

// A keeper's windows are never held back by another keeper's: with the
// validator list's "all" window stuck computing on the longer windows'
// keeper, the 24h list's keeper still refreshes it, over and over.
func TestLiveLaneIsNotHeldBackBySlowWindows(t *testing.T) {
	release := make(chan struct{})
	var live atomic.Int64
	c := newSnapshotCache("validators", func(ctx context.Context, win Window) (int64, error) {
		switch win.Name {
		case "all":
			<-release
		case "24h":
			live.Add(1)
		}
		return 1, nil
	})
	c.ttls = map[string]time.Duration{"24h": time.Millisecond}
	done := make(chan struct{})
	go func() {
		defer close(done)
		c.refreshDue(nil, time.Now(), "7d", "30d", "all") // the longer windows' keeper
	}()
	deadline := time.Now().Add(5 * time.Second)
	for live.Load() < 3 && time.Now().Before(deadline) {
		c.refreshDue(nil, time.Now(), "24h") // the 24h list's keeper
		time.Sleep(2 * time.Millisecond)
	}
	if got := live.Load(); got < 3 {
		t.Errorf("24h refreshed %d times while all was computing, want it to keep refreshing", got)
	}
	close(release)
	<-done
}

// NewWithVantage wires the schedule and runs it:
//
//   - the TTLs: the 24h validator list and every market window at the live
//     lane's; the network's 24h window at a minute; 7d at five; 30d and "all"
//     at fifteen, except the network's "all" (the overview's Available
//     figure) at five;
//   - the keepers: every window of the three caches on exactly one keeper,
//     and the 24h validator list, the market and the network's 24h window
//     each on a keeper of its own, the first two at the live lane's pace, so
//     no other computation holds them back;
//   - and every keeper running: with nobody reading, a window of each is
//     computed again once it is past its TTL.
func TestServerWiresTheLanes(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "observer.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if defaultLanes.liveTTL != 10*time.Second {
		t.Errorf("the live lane's TTL is %s, want ten seconds", defaultLanes.liveTTL)
	}
	// liveEvery and slowEvery differ, so a keeper's pace says which it is.
	l := lanes{liveTTL: 50 * time.Millisecond, liveEvery: 5 * time.Millisecond, slowEvery: 8 * time.Millisecond}
	s := NewWithVantage(st, VantageInfo{Name: "test"}, nil, withLanes(l))
	defer s.Close()

	schedule := map[string]struct{ vals, market, net time.Duration }{
		"24h": {l.liveTTL, l.liveTTL, time.Minute},
		"7d":  {5 * time.Minute, l.liveTTL, 5 * time.Minute},
		"30d": {15 * time.Minute, l.liveTTL, 15 * time.Minute},
		"all": {15 * time.Minute, l.liveTTL, 5 * time.Minute},
	}
	for _, w := range warmWindows {
		want, ok := schedule[w]
		if !ok {
			t.Fatalf("window %s has no place in the schedule", w)
		}
		if got := s.vals.ttl(w); got != want.vals {
			t.Errorf("validators %s: ttl %s, want %s", w, got, want.vals)
		}
		if got := s.market.ttl(w); got != want.market {
			t.Errorf("market %s: ttl %s, want %s", w, got, want.market)
		}
		if got := s.net.ttl(w); got != want.net {
			t.Errorf("network %s: ttl %s, want %s", w, got, want.net)
		}
	}

	// The keepers the server started, as the windows each one holds.
	caches := map[refresher]string{s.vals: "validators", s.market: "market", s.net: "network"}
	holds := make([][]string, len(s.keepers))
	on := map[string][]int{}
	for i, k := range s.keepers {
		for _, j := range k.jobs {
			label, ok := caches[j.cache]
			if !ok {
				t.Fatalf("keeper %q refreshes a cache the server does not serve", k.name)
			}
			for _, w := range j.windows {
				holds[i] = append(holds[i], label+" "+w)
				on[label+" "+w] = append(on[label+" "+w], i)
			}
		}
	}
	for _, label := range []string{"validators", "market", "network"} {
		for _, w := range warmWindows {
			if n := len(on[label+" "+w]); n != 1 {
				t.Errorf("%s %s is on %d keepers, want exactly one", label, w, n)
			}
		}
	}
	own := []struct {
		windows []string
		every   time.Duration
	}{
		{[]string{"validators 24h"}, l.liveEvery},
		{[]string{"market 24h", "market 7d", "market 30d", "market all"}, l.liveEvery},
		{[]string{"network 24h"}, l.slowEvery},
	}
	mine := map[int]bool{}
	for _, o := range own {
		ks := on[o.windows[0]]
		for _, i := range ks {
			mine[i] = true
		}
		if len(ks) != 1 {
			continue // reported above
		}
		k := s.keepers[ks[0]]
		got := slices.Sorted(slices.Values(holds[ks[0]]))
		if want := slices.Sorted(slices.Values(o.windows)); !slices.Equal(got, want) {
			t.Errorf("keeper %q holds %v, want %v and nothing else", k.name, got, want)
		}
		if k.every != o.every {
			t.Errorf("keeper %q looks every %s, want %s", k.name, k.every, o.every)
		}
	}
	for i, k := range s.keepers {
		if !mine[i] && k.every != l.slowEvery {
			t.Errorf("keeper %q (%v) looks every %s, want %s", k.name, holds[i], k.every, l.slowEvery)
		}
	}

	// Every keeper runs. Each window below is backdated past its TTL once it
	// has been computed, and must be computed again with nobody reading it:
	// one on each keeper, and both caches on the longer windows' keeper.
	again := map[string]func() bool{
		"validators 24h": backdate(t, s.vals, "24h"),
		"market 7d":      backdate(t, s.market, "7d"),
		"network 24h":    backdate(t, s.net, "24h"),
		"network 7d":     backdate(t, s.net, "7d"),
		"validators 7d":  backdate(t, s.vals, "7d"),
	}
	deadline := time.Now().Add(20 * time.Second)
	for name, done := range again {
		for !done() && time.Now().Before(deadline) {
			time.Sleep(5 * time.Millisecond)
		}
		if !done() {
			t.Errorf("%s, backdated past its TTL, was not computed again with nobody reading it", name)
		}
	}
}

// backdate waits for c's window to be computed and not being refreshed, then
// moves its snapshot two hours into the past, beyond any TTL. It returns a
// check of whether the window has been computed again since. The snapshot is
// replaced, not edited: a computation that persists it reads it outside the
// lock.
func backdate[T any](t *testing.T, c *snapshotCache[T], name string) func() bool {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		c.mu.Lock()
		if e := c.entries[name]; e != nil && !c.refreshing[name] {
			old := *e
			old.at = time.Now().Add(-2 * time.Hour)
			c.entries[name] = &old
			at := time.Now()
			c.mu.Unlock()
			return func() bool {
				c.mu.Lock()
				defer c.mu.Unlock()
				e := c.entries[name]
				return e != nil && e.at.After(at)
			}
		}
		c.mu.Unlock()
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("%s %s was never computed", c.label, name)
	return nil
}
