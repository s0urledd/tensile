package api

import (
	"context"
	"math"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
)

// moveHolds lifts the hold of some probe rows and raises it on others in
// one transaction, counted as the store's own paths count it
// (store.HeldFlagsMoved): what SyncParamHolds does when one range is lifted
// and another covers rows stored since, in one pass.
func moveHolds(t *testing.T, s *sim, lift, raise []int64) {
	t.Helper()
	tx, err := s.st.DB().Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	for _, x := range []struct {
		ids  []int64
		held int
	}{{lift, 0}, {raise, 1}} {
		for _, id := range x.ids {
			if _, err := tx.Exec(`UPDATE probes SET retention_unverified = ? WHERE rowid = ?`, x.held, id); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := store.HeldFlagsMoved(tx); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

// TestAHoldMovedBetweenRowsOfEqualSumsIsFound: a hold lifted from two rows
// of sealed days and raised on two others whose rowids add up alike, of
// another validator, in one transaction between two catch-ups. The holds
// were followed by an aggregate of the held rows (their count, the sum of
// their rowids and of a mix of them), which such a move leaves as it was:
// the mix was linear in the rowid over runs of tens of thousands of rowids,
// so its sum was fixed by the other two. The sealed days kept the rows'
// old classes and both validators' figures were wrong until something else
// dropped the days. They are found now by the holds' counter, moved in the
// same transaction, and an exact digest of each promise's held rows.
func TestAHoldMovedBetweenRowsOfEqualSumsIsFound(t *testing.T) {
	skipUnderRace(t)
	t.Parallel()
	cfg := defaultSimConfig(701)
	cfg.perDay, cfg.days, cfg.vals = 12, 6, 8
	s := newSim(t, cfg)
	s.plan()
	srv := s.openAPI()
	ctx := context.Background()
	for at := s.t0.Add(time.Hour); at.Before(s.t0.Add(5*24*time.Hour + 6*time.Hour)); at = at.Add(3 * time.Hour) {
		s.advance(at)
		s.pass()
		if _, err := srv.sealDue(ctx, math.MaxInt); err != nil {
			t.Fatal(err)
		}
	}
	rng := rand.New(rand.NewPCG(701, 1))
	s.compare(srv, rng, 0, "before")
	// Rows a < b < c < d of sealed row days, a + d = b + c, whose class a
	// hold changes: {a, d} of one validator, {b, c} of another.
	srv.parts.mu.Lock()
	sealed := map[string]bool{}
	for d := range srv.parts.cur.rows {
		sealed[d] = true
	}
	srv.parts.mu.Unlock()
	type row struct {
		id        int64
		addr, day string
	}
	rs, err := s.st.DB().Query(`SELECT rowid, validator_address, substr(started_at, 1, 10) FROM probes
		WHERE classification IN ('HEALTHY', 'FAULT') AND outcome <> 'INVALID_ROWS'
		  AND retention_unverified = 0 AND phase = 'in_window' AND assigned = 1 ORDER BY rowid`)
	if err != nil {
		t.Fatal(err)
	}
	var cand []row
	byID := map[int64]row{}
	for rs.Next() {
		var r row
		if err := rs.Scan(&r.id, &r.addr, &r.day); err != nil {
			t.Fatal(err)
		}
		if sealed[r.day] {
			cand = append(cand, r)
			byID[r.id] = r
		}
	}
	rs.Close()
	var quad []int64
	for i := 0; i < len(cand) && quad == nil; i++ {
		a := cand[i]
		for j := i + 1; j < len(cand) && j < i+60 && quad == nil; j++ {
			b := cand[j]
			if b.addr == a.addr {
				continue
			}
			for k := j + 1; k < len(cand) && k < i+60 && quad == nil; k++ {
				c := cand[k]
				if d, ok := byID[b.id+c.id-a.id]; ok && c.addr == b.addr && d.id > c.id && d.addr == a.addr {
					quad = []int64{a.id, b.id, c.id, d.id}
				}
			}
		}
	}
	if quad == nil {
		t.Fatal("no four rows to move a hold between")
	}
	day := byID[quad[0]].day
	// Held on a and d: found, the days sealed again with the hold.
	moveHolds(t, s, nil, []int64{quad[0], quad[3]})
	s.compare(srv, rng, 0, "held on two rows")
	if _, err := srv.sealDue(ctx, math.MaxInt); err != nil {
		t.Fatal(err)
	}
	srv.parts.mu.Lock()
	again := srv.parts.cur.rows[day] != nil
	srv.parts.mu.Unlock()
	if !again {
		t.Fatalf("row day %s was not sealed again with the hold", day)
	}
	// Moved to b and c: the count and both sums as they were.
	dropped := srv.parts.dropped
	moveHolds(t, s, []int64{quad[0], quad[3]}, []int64{quad[1], quad[2]})
	tally := s.compare(srv, rng, 0, "the hold moved between rows of equal sums")
	if srv.parts.dropped == dropped {
		t.Errorf("moving the hold dropped no sealed day")
	}
	t.Logf("rows %v on sealed day %s:\n%s", quad, day, tally)
}
