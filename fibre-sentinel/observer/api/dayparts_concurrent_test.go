package api

import (
	"context"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
)

// TestDayPartsUnderConcurrency runs the API as it runs in production (the
// keepers refreshing every window, the sealer, pinned and filtered requests
// arriving at once) while the collector ingests pass after pass, with the
// race detector watching when it is on. The epochs are immutable once
// published and every change goes through the partials' lock, so nothing
// races; at the end every figure is still what the shipped statements
// compute.
func TestDayPartsUnderConcurrency(t *testing.T) {
	cfg := defaultSimConfig(77)
	cfg.perDay, cfg.days = 10, 6
	if raceOn {
		cfg.perDay, cfg.days = 6, 3
	}
	s := newSim(t, cfg)
	// the record ends at the real clock, which the keepers' windows use
	wall := time.Now().UTC()
	s.t0 = wall.Truncate(24 * time.Hour).Add(-time.Duration(cfg.days) * 24 * time.Hour)
	s.now = s.t0
	s.plan()
	var clock atomic.Int64
	clock.Store(s.t0.UnixNano())
	s.pass() // the schema, for the read-only store
	ro, err := store.OpenReadOnly(s.dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer ro.Close()
	s.ro = ro
	srv := NewWithVantage(ro, VantageInfo{Name: simVantage}, nil, WithSnapshotDir(s.snaps),
		withLanes(lanes{liveTTL: 50 * time.Millisecond, liveEvery: 20 * time.Millisecond, slowEvery: 20 * time.Millisecond}),
		func(srv *Server) {
			srv.clock = func() time.Time { return time.Unix(0, clock.Load()).UTC() }
			srv.sealPace = [2]time.Duration{5 * time.Millisecond, 20 * time.Millisecond}
		})
	ctx := context.Background()
	stop := make(chan struct{})
	var wg sync.WaitGroup
	var reads atomic.Int64
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(uint64(i), 3))
			for {
				select {
				case <-stop:
					return
				default:
				}
				at := time.Unix(0, clock.Load()).UTC()
				win := pinned([]string{"7d", "30d", "all"}[rng.IntN(3)], at)
				ex, names := excludeSet{}, []string(nil)
				if rng.IntN(2) == 0 {
					addr := s.vals[rng.IntN(len(s.vals))].addr
					ex, names = excludeSet{addrs: []any{addr}}, []string{addr}
				}
				if _, err := srv.networkSnapshot(ctx, win, ex, names); err != nil {
					t.Error(err)
					return
				}
				if _, err := srv.validatorsSnapshot(ctx, win); err != nil {
					t.Error(err)
					return
				}
				if _, _, err := srv.validatorDetail(ctx, s.vals[rng.IntN(len(s.vals))].addr, win, at); err != nil {
					t.Error(err)
					return
				}
				reads.Add(1)
			}
		}(i)
	}
	for at := s.t0.Add(time.Hour); at.Before(wall); at = at.Add(4 * time.Hour) {
		s.advance(at)
		clock.Store(at.UnixNano())
		s.pass()
		time.Sleep(20 * time.Millisecond)
	}
	close(stop)
	wg.Wait()
	srv.Close()
	t.Logf("%d rounds of reads beside the passes, the keepers and the sealer; %d row days and %d settlement days sealed, %d dropped",
		reads.Load(), len(srv.parts.cur.rows), sealedSettle(srv.parts.cur), srv.parts.dropped)
	if reads.Load() == 0 || len(srv.parts.cur.rows) == 0 {
		t.Errorf("nothing ran concurrently")
	}
	// Every figure, from the partials the concurrent run left, against the
	// shipped statements.
	check := s.openAPI()
	check.clock = func() time.Time { return s.now }
	tally := s.compare(check, rand.New(rand.NewPCG(77, 1)), 0, "after the concurrent run")
	t.Logf("after the concurrent run:\n%s", tally)
}
