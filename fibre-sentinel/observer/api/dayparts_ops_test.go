package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// The partials as a process on a shared disk runs them: at a pace, backing
// off what fails, writing only what moved, and saying what it does.

// opsSim is a record of four days, passed through to its end, nothing
// sealed yet.
func opsSim(t *testing.T, seed uint64) (*sim, *Server) {
	t.Helper()
	cfg := defaultSimConfig(seed)
	cfg.perDay, cfg.days = 8, 4
	s := newSim(t, cfg)
	s.plan()
	srv := s.openAPI()
	for at := s.t0.Add(time.Hour); at.Before(s.t0.Add(4*24*time.Hour + 6*time.Hour)); at = at.Add(12 * time.Hour) {
		s.advance(at)
		s.pass()
	}
	return s, srv
}

// logLines collects what the partials log.
type logLines struct {
	mu    sync.Mutex
	lines []string
}

func (l *logLines) logf(format string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.lines = append(l.lines, fmt.Sprintf(format, args...))
}

func (l *logLines) with(sub string) []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []string
	for _, x := range l.lines {
		if strings.Contains(x, sub) {
			out = append(out, x)
		}
	}
	return out
}

// TestThePaceRestsAfterEachUnit: with a pace of k the sealer rests k times
// as long as each unit took before the next, the warm-up's run of every
// unit due (sealDue) as much as the live sealer's, so its reads take the
// disk a share 1/(k+1) of the time. The same record sealed with a pace of 3
// takes about four times as long as without one, and at least twice.
func TestThePaceRestsAfterEachUnit(t *testing.T) {
	skipUnderRace(t)
	ctx := context.Background()
	run := func(k float64) (time.Duration, time.Duration, int) {
		_, srv := opsSim(t, 71)
		WithSealPace(k)(srv)
		if err := srv.readTx(ctx, func(context.Context) error { return nil }); err != nil {
			t.Fatal(err)
		}
		t0 := time.Now()
		n, err := srv.sealDue(ctx, math.MaxInt)
		if err != nil {
			t.Fatal(err)
		}
		return time.Since(t0), time.Duration(srv.parts.rested.Load()), n
	}
	free, freeRest, n0 := run(0)
	paced, rest, n2 := run(3)
	if n0 != n2 || n0 < 5 {
		t.Fatalf("the two runs did %d and %d unit(s)", n0, n2)
	}
	if freeRest != 0 {
		t.Errorf("rested %s without a pace", freeRest)
	}
	// Each unit but the last is followed by a rest three times as long as
	// it took, so the rests come to nearly three times the work. Held to the
	// run's own work rather than to another run's time, which the machine's
	// load moves (on a busy CI runner the unpaced run took as long as the
	// paced one's work and rests together).
	work := paced - rest
	if rest < 2*work || rest > 4*work {
		t.Errorf("%d unit(s): %s of work and %s of rest with a pace of 3; want about three times the work", n0, work, rest)
	}
	t.Logf("%d unit(s): %s without a pace; with a pace of 3, %s of work and %s of rest", n0, free.Round(time.Millisecond),
		work.Round(time.Millisecond), rest.Round(time.Millisecond))
}

// TestAFailedUnitBacksOff: a day whose seal fails is not tried again on the
// sealer's next unit, as it was every thirty seconds, but after a minute,
// then two, then four; the sealer seals the other days meanwhile; the
// failure is logged once when it begins and once when it ends.
func TestAFailedUnitBacksOff(t *testing.T) {
	skipUnderRace(t)
	t.Parallel()
	s, srv := opsSim(t, 72)
	var logs logLines
	srv.parts.log = logs.logf
	ctx := context.Background()
	bad, tries := "", 0
	srv.parts.failUnit = func(kind, day string) error {
		if bad == "" && kind == "row" {
			bad = day
		}
		if kind == "row" && day == bad {
			tries++
			return errors.New("injected: the disk is busy")
		}
		return nil
	}
	// At one moment: the day fails once, and every other unit due is done.
	for i := 0; i < 200; i++ {
		u, err := srv.sealOne(ctx)
		if err == nil && u.out == outNone {
			break
		}
	}
	if bad == "" || tries != 1 {
		t.Fatalf("row day %q was tried %d time(s) at one moment", bad, tries)
	}
	if sealedRow(srv, bad) || len(srv.parts.cur.rows) == 0 {
		t.Fatalf("the other days were not sealed meanwhile: %d row days", len(srv.parts.cur.rows))
	}
	step := func(d time.Duration) {
		s.now = s.now.Add(d)
		for i := 0; i < 20; i++ {
			if u, err := srv.sealOne(ctx); err == nil && u.out == outNone {
				return
			}
		}
	}
	step(sealFailBase) // tried again: fails
	step(sealFailBase) // within the second delay: not tried
	if tries != 2 {
		t.Errorf("after a minute and two: tried %d time(s), want 2", tries)
	}
	step(sealFailBase) // the second delay waited out: tried, fails
	if tries != 3 {
		t.Errorf("after four minutes: tried %d time(s), want 3", tries)
	}
	srv.parts.failUnit = nil
	step(4 * sealFailBase)
	if !sealedRow(srv, bad) {
		t.Errorf("row day %s was not sealed once it stopped failing", bad)
	}
	if f, r := logs.with("failed: injected"), logs.with("succeeded after 3 failure(s)"); len(f) != 1 || len(r) != 1 {
		t.Errorf("the failure was logged %d time(s) and the recovery %d, want once each:\n%s", len(f), len(r), strings.Join(logs.lines, "\n"))
	}
}

// TestTheIndexIsWrittenWhenWhatItKeepsMoves: computations catch up all the
// time, and their marks move; the index used to be written whenever they
// had, once a minute while the sealer worked and every unit of it counted
// as work, a day it only found not ready as much as one it sealed. Now a
// unit that seals nothing reports nothing done, and the index is written
// when a day is sealed or dropped or the ledger grows, not when only the
// marks moved.
func TestTheIndexIsWrittenWhenWhatItKeepsMoves(t *testing.T) {
	skipUnderRace(t)
	t.Parallel()
	s, srv := opsSim(t, 73)
	ctx := context.Background()
	if _, err := srv.sealDue(ctx, math.MaxInt); err != nil {
		t.Fatal(err)
	}
	if err := srv.keepDerived(ctx); err != nil {
		t.Fatal(err)
	}
	index := filepath.Join(s.snaps, dayPartsFile)
	stamp := func() string {
		fi, err := os.Stat(index)
		if err != nil {
			t.Fatal(err)
		}
		b, _ := os.ReadFile(index)
		return fmt.Sprintf("%s %d %x", fi.ModTime(), fi.Size(), b[:80])
	}
	before := stamp()
	// Catch-ups over a store that grows, and the sealer with nothing to do:
	// nothing written.
	for i := 0; i < 5; i++ {
		s.advance(s.now.Add(10 * time.Minute))
		s.fastTick()
		catchUpNow(t, srv)
		if n, err := srv.sealDue(ctx, 1); err != nil || n != 0 {
			t.Fatalf("a unit with nothing due: %d, %v", n, err)
		}
		srv.parts.saveLater(srv, true)
		srv.parts.wait()
	}
	if got := stamp(); got != before {
		t.Errorf("the index was written though nothing it keeps of substance moved")
	}
	// A sealed day dropped: written.
	var day string
	srv.parts.mu.Lock()
	for d := range srv.parts.cur.rows {
		day = max(day, d)
	}
	srv.parts.mu.Unlock()
	if !s.lateRow(day, "") {
		t.Fatalf("no row to write on %s", day)
	}
	catchUpNow(t, srv)
	srv.parts.saveLater(srv, true)
	srv.parts.wait()
	if got := stamp(); got == before {
		t.Errorf("a sealed day dropped was not written")
	}
	var f partsFile
	if ok, why := readDerived(index, &f); !ok {
		t.Fatal(why)
	}
	if _, ok := f.Seals["row:"+day]; ok {
		t.Errorf("the index written still names row day %s", day)
	}
}

// TestThePartialsInHealth: /v1/health carries the partials' state, and
// fails on a day due and not sealed for two days, and on raw reads after an
// audit found a difference.
func TestThePartialsInHealth(t *testing.T) {
	skipUnderRace(t)
	t.Parallel()
	s, srv := opsSim(t, 74)
	ctx := context.Background()
	if _, err := srv.sealDue(ctx, math.MaxInt); err != nil {
		t.Fatal(err)
	}
	h := srv.health(ctx, s.now)
	b, err := json.Marshal(h)
	if err != nil {
		t.Fatal(err)
	}
	var shape struct {
		Parts map[string]any `json:"day_partials"`
	}
	if err := json.Unmarshal(b, &shape); err != nil {
		t.Fatal(err)
	}
	if shape.Parts["state"] != "on" || shape.Parts["origin"] != "built" || shape.Parts["sealed_row_days"].(float64) == 0 {
		t.Errorf("day_partials in /v1/health: %v", shape.Parts)
	}
	check := func(want bool) healthCheck {
		t.Helper()
		for _, c := range srv.health(ctx, s.now).Checks {
			if c.Name == "day_partials" {
				if c.OK != want {
					t.Errorf("the day_partials check: %+v, want ok %v", c, want)
				}
				return c
			}
		}
		t.Fatal("no day_partials check")
		return healthCheck{}
	}
	check(true)
	// A day due and not sealed for two days and more.
	srv.parts.mu.Lock()
	srv.parts.dueSince = map[string]time.Time{"row:2026-03-01": s.now.Add(-unsealedFor - time.Hour)}
	srv.parts.mu.Unlock()
	if c := check(false); !strings.Contains(c.Detail, "row 2026-03-01") {
		t.Errorf("detail: %s", c.Detail)
	}
	srv.parts.mu.Lock()
	srv.parts.dueSince = nil
	srv.parts.mu.Unlock()
	check(true)
	// Raw reads.
	srv.parts.fallBack("test: a window differs")
	if c := check(false); !strings.Contains(c.Detail, "test: a window differs") {
		t.Errorf("detail: %s", c.Detail)
	}
	// Off.
	off := s.openAPI(WithDayParts(false))
	if h := off.health(ctx, s.now); h.DayPartials == nil || h.DayPartials.State != "off" {
		t.Errorf("/v1/health with the partials off: %+v", h.DayPartials)
	}
	for _, c := range off.health(ctx, s.now).Checks {
		if c.Name == "day_partials" {
			t.Errorf("a day_partials check with the partials off: %+v", c)
		}
	}
}
