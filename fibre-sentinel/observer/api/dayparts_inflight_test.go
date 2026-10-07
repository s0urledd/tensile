package api

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/probe"
	"github.com/plsgiveup/fibre/fibre-sentinel/internal/scan"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
)

// A seal is read in a transaction of its own and published into the newest
// epoch afterwards, so whatever a catch-up finds between the two must keep
// the seal out when it reaches the day: the sealer reads optimistically and
// the journal of what the catch-ups found since decides (sealRow,
// sealSettle). These tests put a write and a computation's catch-up in that
// gap (dayParts.beforePublish), one for every way the partials find a write
// that reaches a day, and hold the seal out and every figure to the shipped
// statements'.

// TestARowDaySealedAcrossACollapseIsNotPublished: a row day holding a
// promise's sampled-out rows is being sealed when the collector's pass
// collapses them into a decision and deletes them, and a computation
// catches up before the seal is published. The catch-up drops the sealed
// days whose Collapsible lists name the promise; the day being sealed was
// not one of them, so its seal, which counts the deleted rows, used to be
// published, and nothing dropped it after: every window over the day
// counted the rows twice, once as rows and once as the decision. The
// collapsed promises are journalled now, and the seal is not published.
func TestARowDaySealedAcrossACollapseIsNotPublished(t *testing.T) {
	skipUnderRace(t)
	t.Parallel()
	cfg := defaultSimConfig(703)
	cfg.perDay, cfg.days, cfg.vals = 12, 9, 8
	s := newSim(t, cfg)
	s.plan()
	if s.scen.collapse < 0 {
		t.Fatal("the record holds no publication written the old way")
	}
	sp := s.pubs[s.scen.collapse]
	srv := s.openAPI()
	ctx := context.Background()
	for at := s.t0.Add(time.Hour); at.Before(sp.emitAt.Add(-10 * time.Minute)); at = at.Add(3 * time.Hour) {
		s.advance(at)
		s.pass()
		if _, err := srv.sealDue(ctx, math.MaxInt); err != nil {
			t.Fatal(err)
		}
	}
	var y string
	if err := s.st.DB().QueryRow(`SELECT MAX(substr(started_at, 1, 10)) FROM probes WHERE promise_hash = ?`, sp.pub.PromiseHash).Scan(&y); err != nil {
		t.Fatal(err)
	}
	rng := rand.New(rand.NewPCG(703, 1))
	s.compare(srv, rng, 0, "before")
	// The fast tick stores the late record (the collapse waits for the next
	// pass), and a restarted prober's row of another publication drops day y,
	// so the sealer takes it up again.
	s.advance(sp.emitAt.Add(time.Minute))
	s.fastTick()
	if !s.lateRow(y, "") {
		t.Fatalf("no publication to write a row of on %s", y)
	}
	collapsed := false
	srv.parts.beforePublish = func(kind, day string) {
		if kind != "row" || day != y || collapsed {
			return
		}
		decisions, rows, err := s.st.CollapseSampledOut(context.Background())
		if err != nil || decisions == 0 || rows == 0 {
			t.Errorf("the collapse: %d decision(s), %d row(s): %v", decisions, rows, err)
		}
		collapsed = true
		catchUpNow(t, srv)
	}
	if _, err := srv.sealDue(ctx, math.MaxInt); err != nil {
		t.Fatal(err)
	}
	if !collapsed {
		t.Fatalf("row day %s was not sealed again", y)
	}
	if sealedRow(srv, y) {
		t.Errorf("the seal of row day %s, read before the collapse, was published", y)
	}
	s.compare(srv, rng, 0, "after the collapse")
	// And sealed again, and still exact a pass later.
	s.advance(s.now.Add(time.Hour))
	s.pass()
	if _, err := srv.sealDue(ctx, math.MaxInt); err != nil {
		t.Fatal(err)
	}
	if !sealedRow(srv, y) {
		t.Errorf("row day %s was not sealed again after the collapse", y)
	}
	t.Logf("%s", s.compare(srv, rng, 0, "a pass later"))
}

// catchUpNow is a computation's catch-up: a read transaction of the API's.
func catchUpNow(t *testing.T, srv *Server) {
	t.Helper()
	if err := srv.readTx(context.Background(), func(ctx context.Context) error {
		if epochOf(ctx) == nil {
			return fmt.Errorf("no epoch: the catch-up failed")
		}
		return nil
	}); err != nil {
		t.Error(err)
	}
}

func sealedRow(srv *Server, d string) bool {
	srv.parts.mu.Lock()
	defer srv.parts.mu.Unlock()
	return srv.parts.cur.rows[d] != nil
}

func sealedSettleDay(srv *Server, d string) bool {
	srv.parts.mu.Lock()
	defer srv.parts.mu.Unlock()
	sd := srv.parts.cur.settle[d]
	return sd != nil && sd.Seal != nil
}

// rowOn is a reading of day d (or, with promise set, of that promise), of
// a class a hold or a correction moves.
func (s *sim) rowOn(d, promise string) (key, h, addr, sched, phase, cls, msu string, ok bool) {
	q := `SELECT dedupe_key, promise_hash, validator_address, scheduled_at, phase, classification, must_serve_until FROM probes
		WHERE started_at >= ? AND started_at <= ? AND classification IN ('HEALTHY', 'FAULT') AND outcome <> 'INVALID_ROWS'
		  AND amended_at IS NULL AND corrected_at IS NULL AND retention_unverified = 0`
	args := []any{dayLo(d), dayHi(d)}
	if promise != "" {
		q = `SELECT dedupe_key, promise_hash, validator_address, scheduled_at, phase, classification, must_serve_until FROM probes
			WHERE promise_hash = ? AND classification IN ('HEALTHY', 'FAULT') AND outcome <> 'INVALID_ROWS'
			  AND amended_at IS NULL AND corrected_at IS NULL AND retention_unverified = 0`
		args = []any{promise}
	}
	err := s.st.DB().QueryRow(q+` ORDER BY rowid LIMIT 1`, args...).Scan(&key, &h, &addr, &sched, &phase, &cls, &msu)
	return key, h, addr, sched, phase, cls, msu, err == nil
}

// settledOn is a publication settled on day d with readings, not sampled
// out.
func (s *sim) settledOn(d string) *simPub {
	for _, sp := range s.pubs {
		if dayOfTime(sp.pub.SettlementTime) == d && !sp.sampled && sp.pub.Assignment.Error == "" && sp.pub.SettlementTxCode == 0 {
			var n int
			if err := s.st.DB().QueryRow(`SELECT COUNT(*) FROM probes WHERE promise_hash = ?`, sp.pub.PromiseHash).Scan(&n); err == nil && n > 0 {
				return sp
			}
		}
	}
	return nil
}

// lateRow writes a restarted prober's NOT_PROBED row started on day d, of
// a publication with a reading on it (or of the promise named).
func (s *sim) lateRow(d, promise string) bool {
	s.t.Helper()
	if promise == "" {
		var ok bool
		if _, promise, _, _, _, _, _, ok = s.rowOn(d, ""); !ok {
			return false
		}
	}
	sp := s.byHash[promise]
	if sp == nil {
		return false
	}
	at, _ := time.Parse(dayLayout, d)
	pt := probe.SchedulePoint{At: at.Add(23 * time.Hour), Phase: probe.PhaseInWindow, Label: "w9"}
	m := s.missed(sp, s.vals[0], pt, at.Add(23*time.Hour+30*time.Minute))
	raw, _ := json.Marshal(m)
	ok, err := s.st.InsertProbe(m, raw)
	if err != nil {
		s.t.Fatal(err)
	}
	return ok
}

// holdOver raises an open range over the heights of a publication: the
// publication and its rows are held in the range's own transaction.
func (s *sim) holdOver(promise string) bool {
	s.t.Helper()
	sp := s.byHash[promise]
	if sp == nil {
		return false
	}
	h := sp.pub.SettlementHeight
	u := scan.ParamUncertainty{SchemaVersion: scan.ParamUncertaintySchemaVersion, ID: fmt.Sprintf("sim-1:silent_change:%d-%d", h, h),
		ChainID: "sim-1", Kind: scan.UncertaintySilentChange, FromHeight: h, ToHeight: h, EffectiveFromHeight: h + 1,
		PublicationsAffected: 1, DetectedAt: s.now}
	raw, _ := json.Marshal(u)
	if _, err := s.st.UpsertParamUncertainty(u, raw, s.now); err != nil {
		s.t.Fatal(err)
	}
	return true
}

// pruneThrough deletes every row started up to the end of day d, as a
// build before 2026-10-04 pruned (pruneLikeBefore), and moves raw_from
// past it.
func (s *sim) pruneThrough(d string) {
	s.t.Helper()
	db := s.st.DB()
	for _, q := range []string{
		`DELETE FROM probes WHERE started_at <= ?`, `DELETE FROM reachability WHERE started_at <= ?`,
		`DELETE FROM sampling_decisions WHERE decided_at <= ?`,
	} {
		if _, err := db.Exec(q, dayHi(d)); err != nil {
			s.t.Fatal(err)
		}
	}
	if err := s.st.SetMeta("raw_from", dayAdd(d, 1), s.now); err != nil {
		s.t.Fatal(err)
	}
}

// TestASealReadBeforeADropIsNotPublished puts, between a seal's read and
// its publish, each write the partials find that reaches the day being
// sealed, and a computation's catch-up after it: the seal is not
// published, the day is read raw, and every figure is the shipped
// statements'. The collapse has a test of its own
// (TestARowDaySealedAcrossACollapseIsNotPublished), as has a hold moved
// between rows of equal sums (TestAHoldMovedBetweenRowsOfEqualSumsIsFound).
func TestASealReadBeforeADropIsNotPublished(t *testing.T) {
	skipUnderRace(t)
	t.Parallel()
	type write func(s *sim, d string) bool
	cases := []struct {
		name, kind string
		write      write
	}{
		{"a row started on the day", "row", func(s *sim, d string) bool { return s.lateRow(d, "") }},
		{"an amendment of a row of the day", "row", func(s *sim, d string) bool {
			key, _, _, _, _, cls, _, ok := s.rowOn(d, "")
			if !ok {
				return false
			}
			changed, err := s.st.ApplyAmendment(store.Amendment{DedupeKey: key, From: cls, To: string(probe.ClassUnmatchedGenuine),
				Reason: "test: a late verdict", JudgedAt: s.now, ScannerFrontier: s.now})
			return err == nil && changed
		}},
		{"a correction of a row of the day", "row", func(s *sim, d string) bool {
			key, h, addr, sched, phase, cls, msu, ok := s.rowOn(d, "")
			if !ok {
				return false
			}
			at, _ := time.Parse(store.TimeLayout, sched)
			from, _ := time.Parse(store.TimeLayout, msu)
			changed, err := s.st.ApplyProbeCorrection(store.Correction{SchemaVersion: store.CorrectionSchemaVersion, Kind: store.CorrectionProbeVerdict,
				UncertaintyID: "test", PromiseHash: h, DedupeKey: key, ValidatorAddress: addr, ScheduledAt: at,
				FromPhase: phase, ToPhase: string(probe.PhasePost), FromClassification: cls, ToClassification: cls,
				FromMustServeUntil: from, ToMustServeUntil: from.Add(-6 * time.Hour), PruneToleranceS: 300, Reason: "test", JudgedAt: s.now})
			return err == nil && changed
		}},
		{"a hold over a promise of the day", "row", func(s *sim, d string) bool {
			_, h, _, _, _, _, _, ok := s.rowOn(d, "")
			return ok && s.holdOver(h)
		}},
		{"the prune of the day", "row", func(s *sim, d string) bool { s.pruneThrough(d); return true }},
		{"a row of a promise of the day", "settle", func(s *sim, d string) bool {
			sp := s.settledOn(d)
			return sp != nil && s.lateRow(dayOfTime(sp.pub.SettlementTime.Add(2*time.Hour)), sp.pub.PromiseHash)
		}},
		{"a deadline of the day corrected", "settle", func(s *sim, d string) bool {
			sp := s.settledOn(d)
			if sp == nil {
				return false
			}
			msu := sp.pub.MustServeUntil
			changed, err := s.st.ApplyPublicationCorrection(store.Correction{SchemaVersion: store.CorrectionSchemaVersion, Kind: store.CorrectionPublicationDeadline,
				UncertaintyID: "test", PromiseHash: sp.pub.PromiseHash, FromMustServeUntil: msu, ToMustServeUntil: msu.Add(-30 * time.Minute),
				FromBasis: "sim", ToBasis: "sim; CORRECTED", Reason: "test", JudgedAt: s.now})
			return err == nil && changed
		}},
		{"a hold over a publication of the day", "settle", func(s *sim, d string) bool {
			sp := s.settledOn(d)
			return sp != nil && s.holdOver(sp.pub.PromiseHash)
		}},
		{"the prune of the day's rows", "settle", func(s *sim, d string) bool { s.pruneThrough(d); return true }},
	}
	for i, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			cfg := defaultSimConfig(uint64(810 + i))
			cfg.perDay, cfg.days, cfg.vals = 10, 6, 8
			s := newSim(t, cfg)
			s.plan()
			srv := s.openAPI()
			ctx := context.Background()
			// Nothing sealed on the way: every day over is due at the end.
			for at := s.t0.Add(time.Hour); at.Before(s.t0.Add(4 * 24 * time.Hour)); at = at.Add(4 * time.Hour) {
				s.advance(at)
				s.pass()
			}
			hit := ""
			srv.parts.beforePublish = func(kind, day string) {
				if hit != "" || kind != c.kind || !c.write(s, day) {
					return
				}
				hit = day
				catchUpNow(t, srv)
			}
			if _, err := srv.sealDue(ctx, math.MaxInt); err != nil {
				t.Fatal(err)
			}
			if hit == "" {
				t.Fatalf("no %s day to write to while it was sealed", c.kind)
			}
			if (c.kind == "row" && sealedRow(srv, hit)) || (c.kind == "settle" && sealedSettleDay(srv, hit)) {
				t.Errorf("the seal of %s day %s, read before the write, was published", c.kind, hit)
			}
			t.Logf("%s day %s:\n%s", c.kind, hit, s.compare(srv, rand.New(rand.NewPCG(uint64(810+i), 1)), 0, c.name))
		})
	}
}
