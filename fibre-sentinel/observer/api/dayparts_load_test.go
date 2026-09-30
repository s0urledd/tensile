package api

import (
	"context"
	"errors"
	"math"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// TestDayPartsLoadInTheBackground holds the load of the kept partials to
// what a start needs of it. It runs beside the computations: one of the
// day's window, which sums nothing, is answered while the load is still
// going. A load that fails (the store busy, the first caller's deadline) is
// tried again by the next computation that sums the partials, and loads the
// same file, rather than building the partials from nothing over it; the
// computation that met the failure reads the whole window meanwhile.
func TestDayPartsLoadInTheBackground(t *testing.T) {
	skipUnderRace(t)
	t.Parallel()
	cfg := defaultSimConfig(66)
	cfg.perDay, cfg.days = 6, 3
	s := newSim(t, cfg)
	s.plan()
	srv := s.openAPI()
	ctx := context.Background()
	for at := s.t0.Add(time.Hour); at.Before(s.t0.Add(3*24*time.Hour + 6*time.Hour)); at = at.Add(12 * time.Hour) {
		s.advance(at)
		s.pass()
	}
	if _, err := srv.sealDue(ctx, math.MaxInt); err != nil {
		t.Fatal(err)
	}
	if err := srv.keepDerived(ctx); err != nil {
		t.Fatal(err)
	}
	sealed := len(srv.parts.cur.rows)
	if sealed == 0 {
		t.Fatal("nothing sealed to keep")
	}

	// The load held up: the day's window does not wait for it.
	next := s.openAPI()
	release := make(chan struct{})
	var fails atomic.Int32
	fails.Store(1)
	next.parts.failLoad = func() error {
		<-release
		if fails.Add(-1) >= 0 {
			return errors.New("injected: the store was busy")
		}
		return nil
	}
	day, cancel := context.WithTimeout(ctx, 30*time.Second)
	if _, err := next.networkSnapshot(day, windowFor("24h", s.now), excludeSet{}, nil); err != nil {
		t.Fatalf("the day's window waited for the load: %v", err)
	}
	cancel()
	// A window that sums the partials waits, and gives up with its context.
	short, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	_, err := next.networkSnapshot(short, windowFor("7d", s.now), excludeSet{}, nil)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("a window that sums the partials did not wait for the load: %v", err)
	}
	// The load fails: a computation waiting for it reads the whole window,
	// and nothing is installed.
	got := make(chan *epoch, 1)
	go func() {
		var e *epoch
		if err := next.readTx(ctx, func(ctx context.Context) error { e = epochOf(ctx); return nil }); err != nil {
			t.Error(err)
		}
		got <- e
	}()
	time.Sleep(100 * time.Millisecond)
	close(release)
	if e := <-got; e != nil {
		t.Fatalf("a computation that met a failed load was handed an epoch")
	}
	for {
		next.parts.mu.Lock()
		loading, loaded, cur, rebuilds := next.parts.loading, next.parts.loaded, next.parts.cur, next.parts.rebuilds
		next.parts.mu.Unlock()
		if loading != nil {
			time.Sleep(10 * time.Millisecond)
			continue
		}
		if loaded || cur != nil || rebuilds != 0 {
			t.Fatalf("a failed load left loaded %v, an epoch %v, %d rebuild(s)", loaded, cur != nil, rebuilds)
		}
		break
	}
	// The next computation loads the file.
	tally := s.compare(next, rand.New(rand.NewPCG(66, 1)), 4, "after a load tried again")
	if !strings.HasPrefix(next.parts.origin, "loaded") || next.parts.rebuilds != 0 || len(next.parts.cur.rows) < sealed {
		t.Errorf("the load tried again: %s, %d rebuild(s), %d of %d row days", next.parts.origin, next.parts.rebuilds, len(next.parts.cur.rows), sealed)
	}
	t.Logf("%s\n%s", next.parts.origin, tally)
}

// TestDayPartsSavedWhenTheSealerRests runs the sealer as the API does and
// lets it seal everything due: once it rests, the file on disk holds every
// day it sealed, with no write asked of it. It used to write at most once
// a minute, and only after a unit of work, so the seals of a burst's last
// minute waited for the sealer's next work, and a start in between began
// from none of them.
func TestDayPartsSavedWhenTheSealerRests(t *testing.T) {
	skipUnderRace(t)
	t.Parallel()
	cfg := defaultSimConfig(67)
	cfg.perDay, cfg.days = 6, 3
	s := newSim(t, cfg)
	s.plan()
	srv := s.openAPI()
	for at := s.t0.Add(time.Hour); at.Before(s.t0.Add(3*24*time.Hour + 6*time.Hour)); at = at.Add(12 * time.Hour) {
		s.advance(at)
		s.pass()
	}
	srv.sealPace = [2]time.Duration{time.Millisecond, 20 * time.Millisecond}
	srv.stop = make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		srv.sealer()
	}()
	// Resting: nothing new sealed for a second, and no write running.
	sealed, still := -1, 0
	for deadline := time.Now().Add(3 * time.Minute); still < 20; {
		time.Sleep(50 * time.Millisecond)
		srv.parts.mu.Lock()
		n, writing := 0, srv.parts.writing
		if srv.parts.cur != nil {
			n = len(srv.parts.cur.rows)
		}
		srv.parts.mu.Unlock()
		if n > 0 && n == sealed && writing == nil {
			still++
		} else {
			sealed, still = n, 0
		}
		if time.Now().After(deadline) {
			t.Fatalf("the sealer did not come to rest: %d row days sealed", n)
		}
	}
	close(srv.stop)
	<-done
	next := s.openAPI()
	tally := s.compare(next, rand.New(rand.NewPCG(67, 1)), 4, "from the file the sealer wrote")
	if !strings.HasPrefix(next.parts.origin, "loaded") || len(next.parts.cur.rows) < sealed {
		t.Errorf("a start from what the sealer wrote: %s, %d of %d row days", next.parts.origin, len(next.parts.cur.rows), sealed)
	}
	t.Logf("%d row days sealed and written\n%s", sealed, tally)
}

// TestDayPartsSealTempsSwept: a seal write killed before its rename leaves
// a temporary file the size of the seal beside the seals; the next write
// of the partials removes it once it is old enough that no write can still
// be at it, and leaves a young one alone.
func TestDayPartsSealTempsSwept(t *testing.T) {
	skipUnderRace(t)
	t.Parallel()
	cfg := defaultSimConfig(68)
	cfg.perDay, cfg.days = 5, 2
	s := newSim(t, cfg)
	s.plan()
	srv := s.openAPI()
	s.advance(s.t0.Add(30 * time.Hour))
	s.pass()
	ctx := context.Background()
	if _, err := srv.sealDue(ctx, math.MaxInt); err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(s.snaps, dayPartsDir)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	old, young := filepath.Join(dir, "row-2026-03-02.g1.json.123.tmp"), filepath.Join(dir, "row-2026-03-02.g2.json.456.tmp")
	for _, p := range []string{old, young} {
		if err := os.WriteFile(p, []byte("{"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	then := time.Now().Add(-staleDerivedTemp - time.Hour)
	if err := os.Chtimes(old, then, then); err != nil {
		t.Fatal(err)
	}
	if err := srv.keepDerived(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(old); !os.IsNotExist(err) {
		t.Errorf("an abandoned seal write's temporary file is still there: %v", err)
	}
	if _, err := os.Stat(young); err != nil {
		t.Errorf("a young temporary file was removed: %v", err)
	}
}

// TestDayPartsIndexWithoutItsSeals copies the kept partials the way the
// warm-up's copy step once did, the *.json files and not day-partials/, and
// starts from the copy: the index is loaded, marks, ledger and all, and
// only the days whose seal files are missing are left unsealed, rather than
// the whole file refused and everything built again. A seal file that is
// another's of the same name leaves its day unsealed the same way.
func TestDayPartsIndexWithoutItsSeals(t *testing.T) {
	skipUnderRace(t)
	t.Parallel()
	cfg := defaultSimConfig(69)
	cfg.perDay, cfg.days = 6, 3
	s := newSim(t, cfg)
	s.plan()
	srv := s.openAPI()
	for at := s.t0.Add(time.Hour); at.Before(s.t0.Add(3*24*time.Hour + 6*time.Hour)); at = at.Add(12 * time.Hour) {
		s.advance(at)
		s.pass()
	}
	ctx := context.Background()
	if _, err := srv.sealDue(ctx, math.MaxInt); err != nil {
		t.Fatal(err)
	}
	if err := srv.keepDerived(ctx); err != nil {
		t.Fatal(err)
	}
	sealed := len(srv.parts.cur.rows)
	names, err := filepath.Glob(filepath.Join(s.snaps, "*.json"))
	if err != nil {
		t.Fatal(err)
	}
	copied := filepath.Join(t.TempDir(), "snapshots")
	if err := os.MkdirAll(copied, 0o755); err != nil {
		t.Fatal(err)
	}
	for _, n := range names {
		b, err := os.ReadFile(n)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(copied, filepath.Base(n)), b, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	next := s.openAPI(WithSnapshotDir(copied))
	rng := rand.New(rand.NewPCG(69, 1))
	tally := s.compare(next, rng, 4, "from an index without its seals")
	if !strings.HasPrefix(next.parts.origin, "loaded") || next.parts.rebuilds != 0 || len(next.parts.cur.rows) != 0 || !next.parts.cur.ledgerBuilt {
		t.Errorf("an index without its seals: %s, %d rebuild(s), %d row days, ledger built %v", next.parts.origin, next.parts.rebuilds, len(next.parts.cur.rows), next.parts.cur.ledgerBuilt)
	}

	// One seal file swapped for another of the same kind: that day alone
	// is left unsealed.
	dir := filepath.Join(s.snaps, dayPartsDir)
	files, err := filepath.Glob(filepath.Join(dir, "row-*.json"))
	if err != nil || len(files) < 2 {
		t.Fatalf("row seal files: %v %v", files, err)
	}
	b, err := os.ReadFile(files[1])
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(files[0], b, 0o644); err != nil {
		t.Fatal(err)
	}
	again := s.openAPI()
	tally.add(s.compare(again, rng, 4, "with a seal file swapped"))
	if !strings.HasPrefix(again.parts.origin, "loaded") || len(again.parts.cur.rows) != sealed-1 {
		t.Errorf("a seal file swapped: %s, %d of %d row days", again.parts.origin, len(again.parts.cur.rows), sealed)
	}
	t.Logf("%s\n%s", again.parts.origin, tally)
}
