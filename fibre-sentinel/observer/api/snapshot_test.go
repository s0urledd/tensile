package api

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
)

// The snapshot's whole purpose is that a reader is not made to wait for a
// window aggregate, so what has to be true is: the second reader does not
// recompute, the age is published rather than implied, and a window's own
// snapshot is its own.

func testWindow(name string) Window { return windowFor(name, time.Now()) }

func newSnapshotServer(t *testing.T) *Server {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "observer.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	s := &Server{st: st, vantage: "test"}
	s.net = newSnapshotCache("network", func(ctx context.Context, win Window) (*networkResponse, error) {
		return s.computeNetwork(ctx, win, excludeSet{}, nil)
	})
	return s
}

func TestSnapshotServesWithoutRecomputing(t *testing.T) {
	s := newSnapshotServer(t)
	ctx := context.Background()
	win := testWindow("24h")

	first, firstAt, ms, err := s.net.get(ctx, nil, win)
	if err != nil {
		t.Fatal(err)
	}
	if firstAt.IsZero() {
		t.Error("the snapshot does not say when it was computed, so its age cannot be published")
	}
	_ = ms

	// Take the snapshot's identity before the second call and after it: a
	// second reader inside the TTL must be handed the same computation.
	s.net.mu.Lock()
	before := s.net.entries[win.Name]
	s.net.mu.Unlock()

	second, secondAt, _, err := s.net.get(ctx, nil, win)
	if err != nil {
		t.Fatal(err)
	}
	s.net.mu.Lock()
	after := s.net.entries[win.Name]
	s.net.mu.Unlock()

	if before != after {
		t.Error("a second request inside the TTL recomputed the snapshot")
	}
	if !secondAt.Equal(firstAt) {
		t.Errorf("the second read reports a different computation time: %v then %v", firstAt, secondAt)
	}
	if first != second {
		t.Error("a second read inside the TTL returned a different value")
	}
}

func TestSnapshotIsPerWindow(t *testing.T) {
	s := newSnapshotServer(t)
	ctx := context.Background()
	for _, name := range []string{"24h", "7d"} {
		win := testWindow(name)
		resp, _, _, err := s.net.get(ctx, nil, win)
		if err != nil {
			t.Fatal(err)
		}
		if resp.Window.Name != name {
			t.Errorf("asked for window %q and got %q", name, resp.Window.Name)
		}
	}
	s.net.mu.Lock()
	defer s.net.mu.Unlock()
	if len(s.net.entries) != 2 {
		t.Errorf("two windows produced %d snapshots", len(s.net.entries))
	}
}

// A stale snapshot is served as it stands while its replacement is computed, so
// the reader of a stale window waits no longer than the reader of a fresh one.
func TestStaleSnapshotIsServedWhileRefreshing(t *testing.T) {
	s := newSnapshotServer(t)
	ctx := context.Background()
	win := testWindow("24h")
	if _, _, _, err := s.net.get(ctx, nil, win); err != nil {
		t.Fatal(err)
	}

	// Age the snapshot past its TTL.
	s.net.mu.Lock()
	snap := s.net.entries[win.Name]
	stale := snap.at.Add(-2 * ttlFor(win.Name))
	snap.at = stale
	s.net.mu.Unlock()

	_, at, _, err := s.net.get(ctx, nil, win)
	if err != nil {
		t.Fatal(err)
	}
	if !at.Equal(stale) {
		t.Errorf("a stale snapshot was not served as it stands: taken %v, wanted %v", at, stale)
	}

	// The refresh it triggered should replace the snapshot shortly. On an empty
	// store that is fast; the deadline is generous because this asserts that a
	// refresh happens, not how quickly.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		s.net.mu.Lock()
		got := s.net.entries[win.Name]
		s.net.mu.Unlock()
		if got != nil && got.at.After(stale) {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Error("a stale snapshot was served but no refresh replaced it")
}

// Only the current figures need to be fresh: 24h refreshes every minute, 7d
// every five, 30d and "all" every fifteen, and a window added later takes the
// slowest pace rather than the most expensive one.
func TestLongWindowsRefreshSlowly(t *testing.T) {
	for name, want := range map[string]time.Duration{
		"24h": time.Minute, "7d": 5 * time.Minute, "30d": 15 * time.Minute, "all": 15 * time.Minute,
		"something-new": 15 * time.Minute,
	} {
		if got := ttlFor(name); got != want {
			t.Errorf("ttlFor(%s) = %s, want %s", name, got, want)
		}
	}
}

// A cache's own TTL overrides ttlFor for the windows it names, from the first
// moment: the live lane's windows must not wait a minute on a new cache.
func TestSnapshotTTLOverride(t *testing.T) {
	c := newSnapshotCache("t", func(context.Context, Window) (int, error) { return 0, nil })
	c.ttls = map[string]time.Duration{"24h": liveTTL}
	if got := c.ttl("24h"); got != liveTTL {
		t.Fatalf("ttl(24h) = %s, want the override %s", got, liveTTL)
	}
	if got := c.ttl("7d"); got != ttlFor("7d") {
		t.Fatalf("ttl(7d) = %s, want ttlFor's %s", got, ttlFor("7d"))
	}
	now := time.Now()
	s := &snap[int]{at: now.Add(-liveTTL - time.Millisecond), ms: 5}
	if !c.stale(s, "24h", now) {
		t.Error("a 24h snapshot past the override is not stale")
	}
	if c.stale(s, "7d", now) {
		t.Error("a 7d snapshot ten seconds old is stale")
	}
}

// A computation that takes longer than its TTL is not rerun back to back: its
// window waits twice the computation.
func TestSlowComputationWaitsTwiceItsCost(t *testing.T) {
	c := newSnapshotCache("t", func(context.Context, Window) (int, error) { return 0, nil })
	c.ttls = map[string]time.Duration{"24h": liveTTL}
	now := time.Now()
	s := &snap[int]{at: now.Add(-15 * time.Second), ms: 12_000}
	if c.stale(s, "24h", now) {
		t.Error("a 12s computation was due again 15s after it started")
	}
	s.at = now.Add(-25 * time.Second)
	if !c.stale(s, "24h", now) {
		t.Error("a 12s computation was not due 25s after it started")
	}
}

func TestSnapshotPersistsAcrossProcesses(t *testing.T) {
	s := newSnapshotServer(t)
	dir := t.TempDir()
	s.net.persistTo(dir, nil)
	win := testWindow("24h")
	if _, at, _, err := s.net.get(context.Background(), nil, win); err != nil || at.IsZero() {
		t.Fatal(err)
	}
	// A second cache, as a restarted process would build, serves the file
	// without computing anything.
	calls := 0
	c2 := newSnapshotCache("network", func(ctx context.Context, w Window) (*networkResponse, error) {
		calls++
		return s.computeNetwork(ctx, w, excludeSet{}, nil)
	})
	c2.persistTo(dir, nil)
	v, at, _, err := c2.get(context.Background(), nil, win)
	if err != nil || v == nil {
		t.Fatal(err)
	}
	if calls != 0 {
		t.Fatalf("a persisted snapshot must be served without recomputing; compute ran %d time(s)", calls)
	}
	if at.IsZero() || time.Since(at) > time.Minute {
		t.Fatalf("persisted snapshot lost its computation time: %v", at)
	}
	// A different label does not pick up the file.
	c3 := newSnapshotCache("validators-not-network", func(ctx context.Context, w Window) (*networkResponse, error) { return nil, nil })
	c3.persistTo(dir, nil)
	c3.mu.Lock()
	n := len(c3.entries)
	c3.mu.Unlock()
	if n != 0 {
		t.Fatal("a snapshot file of another label was loaded")
	}
}

// A hold that lands while a window is being computed must not be stamped onto
// the figure computed before it. The revision is read before the queries, so
// the next reader sees a snapshot from the old revision and recomputes; read
// after, the stale figure passed for current until its TTL ran out.
func TestASnapshotComputedAcrossARevisionChangeIsNotServedAsCurrent(t *testing.T) {
	rev := "r1"
	computed := 0
	c := newSnapshotCache("test", func(ctx context.Context, win Window) (int, error) {
		computed++
		if computed == 1 {
			rev = "r2" // a hold lands while the first computation runs
		}
		return computed, nil
	})
	c.revision = func() string { return rev }
	ctx := context.Background()
	win := testWindow("24h")

	v, _, _, err := c.get(ctx, nil, win)
	if err != nil || v != 1 {
		t.Fatalf("first read: %d %v", v, err)
	}
	v, _, _, err = c.get(ctx, nil, win)
	if err != nil || v != 2 {
		t.Fatalf("second read served %d (computations %d); the figure from before the hold must be recomputed", v, computed)
	}
	v, _, _, err = c.get(ctx, nil, win)
	if err != nil || v != 2 || computed != 2 {
		t.Fatalf("third read: %d after %d computations; nothing changed since the second", v, computed)
	}
}

// A reader who arrives after a hold, while a refresh started before it is
// still running, must not be handed that refresh's figure when it lands: it
// was computed from the rows the hold withdrew.
func TestAReaderAfterAHoldDoesNotTakeTheRefreshStartedBeforeIt(t *testing.T) {
	var rev atomic.Value
	rev.Store("r1")
	var computed atomic.Int32
	refreshing, release := make(chan struct{}), make(chan struct{})
	c := newSnapshotCache("test", func(ctx context.Context, win Window) (int, error) {
		n := int(computed.Add(1))
		if n == 2 {
			close(refreshing)
			<-release
		}
		return n, nil
	})
	c.revision = func() string { return rev.Load().(string) }
	ctx := context.Background()
	win := testWindow("24h")

	if v, _, _, err := c.get(ctx, nil, win); err != nil || v != 1 {
		t.Fatalf("first read: %d %v", v, err)
	}
	c.mu.Lock()
	c.entries[win.Name].at = time.Now().Add(-24 * time.Hour) // past its TTL
	c.mu.Unlock()
	if v, _, _, err := c.get(ctx, nil, win); err != nil || v != 1 {
		t.Fatalf("stale read: %d %v", v, err)
	}
	<-refreshing // the refresh has read its rows
	rev.Store("r2")

	type result struct {
		v   int
		err error
	}
	after := make(chan result, 1)
	go func() {
		v, _, _, err := c.get(ctx, nil, win)
		after <- result{v, err}
	}()
	time.Sleep(200 * time.Millisecond) // let it find the refresh running
	close(release)
	r := <-after
	c.bg.Wait()
	if r.err != nil || r.v != 3 {
		t.Fatalf("the reader after the hold got %d (%v) after %d computations, want the figure computed after the hold", r.v, r.err, computed.Load())
	}
}

// A market snapshot of a window in which no publisher did anything is read
// back after a restart like any other: its empty publisher list is left out
// of the file, and the file still says it carries one. A file from before the
// list was kept in the snapshot is still refused.
func TestMarketSnapshotWithNoPublisherIsReloaded(t *testing.T) {
	s := newSnapshotServer(t)
	ctx := context.Background()
	dir := t.TempDir()
	s.market = newSnapshotCache("market", s.computePublishing)
	s.market.accept = marketSnapshotCurrent
	s.market.persistTo(dir, nil)
	win := testWindow("24h")
	v, _, _, err := s.market.get(ctx, nil, win)
	if err != nil {
		t.Fatal(err)
	}
	if len(v.Publishers) != 0 || !v.PublishersListed {
		t.Fatalf("an empty store gave %d publishers, listed %v", len(v.Publishers), v.PublishersListed)
	}
	b, err := os.ReadFile(s.market.file("24h"))
	if err != nil {
		t.Fatal(err)
	}
	var file struct {
		Value map[string]json.RawMessage `json:"value"`
	}
	if err := json.Unmarshal(b, &file); err != nil {
		t.Fatal(err)
	}
	if _, ok := file.Value["publishers"]; ok {
		t.Fatal("the empty list was written to the file; this no longer tests what omitempty leaves out")
	}
	if string(file.Value["publishers_listed"]) != "true" {
		t.Fatalf("the file does not say it carries the list: %s", file.Value["publishers_listed"])
	}
	calls := 0
	c2 := newSnapshotCache("market", func(ctx context.Context, w Window) (*marketResponse, error) {
		calls++
		return s.computePublishing(ctx, w)
	})
	c2.accept = marketSnapshotCurrent
	c2.persistTo(dir, nil)
	if _, _, _, err := c2.get(ctx, nil, win); err != nil {
		t.Fatal(err)
	}
	if calls != 0 {
		t.Fatal("the snapshot of a window with no publisher was not read back; it was computed again")
	}

	old := *v
	old.Publishers, old.PublishersListed = nil, false
	s.market.persist("24h", &snap[*marketResponse]{v: &old, at: time.Now(), ms: 1})
	c3 := newSnapshotCache[*marketResponse]("market", nil)
	c3.accept = marketSnapshotCurrent
	c3.persistTo(dir, nil)
	c3.mu.Lock()
	n := len(c3.entries)
	c3.mu.Unlock()
	if n != 0 {
		t.Fatal("an older build's market snapshot, without the publisher list, was loaded")
	}
}
