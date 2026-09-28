package api

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
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

// The live keeper's window is never held back by the slow keeper: with the
// "all" window stuck computing, 24h still refreshes, over and over.
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
		c.refreshDue(nil, time.Now(), slowVals...) // what keepSnapshotsFresh runs
	}()
	deadline := time.Now().Add(5 * time.Second)
	for live.Load() < 3 && time.Now().Before(deadline) {
		c.refreshDue(nil, time.Now(), liveVals...) // what keepLiveFresh runs
		time.Sleep(2 * time.Millisecond)
	}
	if got := live.Load(); got < 3 {
		t.Errorf("24h refreshed %d times while all was computing, want it to keep refreshing", got)
	}
	close(release)
	<-done
}
