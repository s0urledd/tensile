package api

import (
	"context"
	"math"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
)

// TestDayPartsSealOnlyDaysWithRows holds the sealer to the days that hold
// a row. A reading stamped a year before the record, and one whose start
// was never set (the year 1, which ingest stores as it is), used to make
// the sealer seal, and write a file for, every empty day between them and
// the record, oldest first, before any day that held something: 365 days
// for the one, about 740,000 for the other. Now each of them is one day
// sealed, the days between are read raw with a seek of each index, and
// every figure is still the shipped statements'.
func TestDayPartsSealOnlyDaysWithRows(t *testing.T) {
	skipUnderRace(t)
	t.Parallel()
	cfg := defaultSimConfig(55)
	cfg.perDay, cfg.days = 10, 4
	s := newSim(t, cfg)
	s.plan()
	srv := s.openAPI()
	ctx := context.Background()
	for at := s.t0.Add(time.Hour); at.Before(s.t0.Add(4*24*time.Hour + 6*time.Hour)); at = at.Add(6 * time.Hour) {
		s.advance(at)
		s.pass()
	}
	for _, at := range []time.Time{s.t0.AddDate(-1, 0, 0), {}} {
		s.copyRow(s.t0.Add(24*time.Hour), store.TS(at), "stray"+at.Format("2006")+"|")
	}
	n, err := srv.sealDue(ctx, math.MaxInt)
	if err != nil {
		t.Fatal(err)
	}
	srv.parts.mu.Lock()
	e := srv.parts.cur
	rows := len(e.rows)
	year, zero := e.rows[dayOfTime(s.t0.AddDate(-1, 0, 0))] != nil, e.rows["0001-01-01"] != nil
	srv.parts.mu.Unlock()
	// the four days of the record that are over, and the two strays' days
	if rows > 6 || !year || !zero {
		t.Errorf("%d row days sealed (a year back: %v, the year 1: %v); want the days that hold a row, at most 6", rows, year, zero)
	}
	if n > 100 {
		t.Errorf("the sealer did %d units of work for four days of record", n)
	}
	tally := s.compare(srv, rand.New(rand.NewPCG(55, 1)), 0, "stray rows")
	t.Logf("%d units, %d row days sealed:\n%s", n, rows, tally)
}
