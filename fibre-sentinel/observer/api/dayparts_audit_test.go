package api

import (
	"context"
	"math"
	"math/rand/v2"
	"strings"
	"testing"
	"time"
)

// TestTheAuditDropsASealTheStoreDoesNotHold holds the two checks of the
// partials against the store to what they are for. On partials that are
// what the store holds, the hourly audit drops nothing. With every sealed
// day changed behind the store's back, the audit of one day drops that
// day, the comparison of a window drops every sealed day, and a file
// holding the changed days is refused when it is loaded; the partials
// then built from the store are exact again.
func TestTheAuditDropsASealTheStoreDoesNotHold(t *testing.T) {
	skipUnderRace(t)
	t.Parallel()
	cfg := defaultSimConfig(33)
	cfg.perDay, cfg.days = 10, 5
	s := newSim(t, cfg)
	s.plan()
	srv := s.openAPI()
	ctx := context.Background()
	for at := s.t0.Add(time.Hour); at.Before(s.t0.Add(4*24*time.Hour + 6*time.Hour)); at = at.Add(6 * time.Hour) {
		s.advance(at)
		s.pass()
	}
	seal := func(srv *Server) {
		if _, err := srv.sealDue(ctx, math.MaxInt); err != nil {
			t.Fatal(err)
		}
	}
	seal(srv)
	count := func(srv *Server) (int, int) {
		srv.parts.mu.Lock()
		defer srv.parts.mu.Unlock()
		return len(srv.parts.cur.rows), sealedSettle(srv.parts.cur)
	}
	rows, settles := count(srv)
	if rows < 2 || settles < 2 {
		t.Fatalf("too little sealed to audit: %d row days, %d settlement days", rows, settles)
	}
	// The days alone, eight times over, then with a window.
	for i := 0; i < 8; i++ {
		if err := srv.auditOnce(ctx); err != nil {
			t.Fatal(err)
		}
	}
	for _, list := range []bool{false, true} {
		if _, err := srv.auditWindow(ctx, list); err != nil {
			t.Fatal(err)
		}
	}
	if r, st := count(srv); r != rows || st != settles {
		t.Fatalf("the audit dropped days the store holds: %d row days and %d settlement days left of %d and %d", r, st, rows, settles)
	}

	// Every sealed day one more than the store holds, as a new generation.
	corrupt := func(srv *Server) {
		srv.parts.publish(func(e *epoch, _ []journalEntry) bool {
			for d, rd := range e.rows {
				c := *rd
				c.Vals = map[string]*rowPart{}
				for a, p := range rd.Vals {
					q := *p
					q.Probes++
					c.Vals[a] = &q
				}
				c.Gen = srv.parts.nextGen("row:" + d)
				e.rows[d] = &c
			}
			for d, sd := range e.settle {
				if sd.Seal == nil {
					continue
				}
				c := *sd.Seal
				c.Readable++
				c.Gen = srv.parts.nextGen("settle:" + d)
				e.settleMut(d).Seal = &c
			}
			return true
		}, 0)
	}
	corrupt(srv)
	if err := srv.auditOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if r, st := count(srv); r != rows-1 || st != settles-1 {
		t.Errorf("the audit of one day of each kind left %d row days and %d settlement days of %d and %d", r, st, rows, settles)
	}
	if _, err := srv.auditWindow(ctx, rand.IntN(2) == 0); err != nil {
		t.Fatal(err)
	}
	if r, st := count(srv); r != 0 || st != 0 {
		t.Errorf("a window that differed left %d row days and %d settlement days sealed", r, st)
	}
	rng := rand.New(rand.NewPCG(33, 1))
	s.compare(srv, rng, 4, "after the audit")

	// Sealed again, changed again, and written out: the next start refuses
	// the file.
	seal(srv)
	corrupt(srv)
	if err := srv.keepDerived(ctx); err != nil {
		t.Fatal(err)
	}
	next := s.openAPI()
	tally := s.compare(next, rng, 4, "after the refused file")
	if !strings.Contains(next.parts.origin, "refused") || !strings.Contains(next.parts.origin, "is not what the store holds") {
		t.Errorf("a file of changed days was not refused: %s", next.parts.origin)
	}
	t.Logf("%s\n%s", next.parts.origin, tally)
}

// sealedSettle counts the settlement days with a seal.
func sealedSettle(e *epoch) int {
	n := 0
	for _, sd := range e.settle {
		if sd.Seal != nil {
			n++
		}
	}
	return n
}
