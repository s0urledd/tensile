package api

import (
	"context"
	"database/sql"
	"errors"
	"sort"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/observer/rollup"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
)

// Sealing: when a day stops being read raw.
//
// Correctness never depends on it. Every change to a sealed day is found
// and drops it (dayparts_catchup.go), so a day could be sealed at any
// moment and still be exact; sealing decides only when a day that is still
// changing stops being recomputed on every refresh, which is when it can no
// longer change in the ordinary course of things:
//
//   - a settlement day once every promise of it is final by the rollup's
//     own tests (rollup.WindowsClosed, rollup.ShadowVerdictsSettled), an
//     hour has passed since the latest row of its promises started (a
//     restarted prober stamps the readings it missed at its restart), no
//     row of them has a deadline after now, and nothing of it is pending.
//     A held day is sealed like any other: the held rows are diffed on
//     every catch-up, so a hold raised or lifted drops it either way, and
//     a range that cannot be closed does not keep the day raw for good;
//   - a row day an hour after it ends, once no row started on it awaits the
//     late shadow verdict and nothing that is not a store timestamp sorts
//     against its bounds (the boundary guard).
//
// Neither is sealed before raw_from: the rollup holds those days. A
// settlement day before raw_from is sealed without obligations, for the
// publications it keeps readable.
//
// The sealer works one day at a time, oldest first, each in a read
// transaction of its own, after the ledger is built and every settlement
// day's row span is known. It seals optimistically: a day it read under
// one epoch is published into the newest only if no catch-up since dropped
// it (the journal).

// sealMargin is how long after the newest row of a day, or of a day's
// promises, the sealer waits before it seals.
const sealMargin = time.Hour

// sealRetry is how long a day that could not be sealed is left before it
// is tried again.
const sealRetry = 10 * time.Minute

// ledgerChunk is how many publications one step of the ledger build folds.
const ledgerChunk = 2000

// partsTx runs fn in a read transaction under an epoch caught up to it, as
// a computation is (readTx), and hands it the epoch.
func (s *Server) partsTx(ctx context.Context, fn func(ctx context.Context, e *epoch) error) error {
	return s.readTx(ctx, func(ctx context.Context) error {
		e := epochOf(ctx)
		if e == nil {
			return errNoParts
		}
		return fn(ctx, e)
	})
}

// publish folds a change into the newest epoch under the lock, if it still
// applies: apply gets a copy and reports whether it changed it.
func (dp *dayParts) publish(apply func(e *epoch, since []journalEntry) bool, base uint64) bool {
	dp.mu.Lock()
	defer dp.mu.Unlock()
	if dp.cur == nil {
		return false
	}
	var since []journalEntry
	if base > 0 {
		if len(dp.journal) == 0 || dp.journal[0].seq > base+1 {
			return false // the journal no longer reaches back to the base
		}
		for _, j := range dp.journal {
			if j.seq > base {
				since = append(since, j)
			}
		}
	}
	e := dp.cur.clone()
	if !apply(e, since) {
		return false
	}
	e.seq = dp.cur.seq + 1
	dp.journal = append(dp.journal, journalEntry{seq: e.seq})
	if len(dp.journal) > journalKeep {
		dp.journal = dp.journal[len(dp.journal)-journalKeep:]
	}
	dp.cur = e
	return true
}

// touched reports whether a journal entry since the base dropped the row
// day (row) or the settlement day d.
func touched(since []journalEntry, d string, row bool) bool {
	for _, j := range since {
		if j.all {
			return true
		}
		if row && j.rows[d] {
			return true
		}
		if !row && j.settle[d] {
			return true
		}
	}
	return false
}

// SealDue does up to n units of the sealer's work at the server's clock and
// reports how many it did: a step of the ledger build, a day's span, or a
// day sealed.
func (s *Server) sealDue(ctx context.Context, n int) (int, error) {
	if s.parts == nil {
		return 0, nil
	}
	s.parts.sealing.Lock()
	defer s.parts.sealing.Unlock()
	done := 0
	for done < n {
		did, err := s.sealOnce(ctx)
		if err != nil {
			return done, err
		}
		if !did {
			break
		}
		done++
	}
	return done, nil
}

// sealOnce does one unit of work, if there is one.
func (s *Server) sealOnce(ctx context.Context) (bool, error) {
	did := false
	err := s.partsTx(ctx, func(ctx context.Context, e *epoch) error {
		now := s.now().UTC()
		switch {
		case e.pubsOdd:
		case !e.ledgerBuilt:
			did = true
			return s.buildLedger(ctx, e)
		default:
			if d := e.unknownSpan(); d != "" {
				did = true
				return s.knowSpan(ctx, e, d)
			}
		}
		kind, d := s.parts.nextSeal(e, now)
		switch kind {
		case "row":
			did = true
			return s.sealRow(ctx, e, d, now)
		case "settle":
			did = true
			return s.sealSettle(ctx, e, d, now)
		}
		return nil
	})
	if errors.Is(err, errNoParts) {
		return false, nil
	}
	return did, err
}

// buildLedger folds the next chunk of the publications the epoch began
// with into the ledger.
func (s *Server) buildLedger(ctx context.Context, e *epoch) error {
	from := e.ledgerTo
	hi := min(from+ledgerChunk, e.ledgerHi)
	dl, err := s.readLedger(ctx, s.q(ctx), from, hi, false)
	if err != nil {
		return err
	}
	s.parts.publish(func(cur *epoch, _ []journalEntry) bool {
		if cur.ledgerBuilt || cur.ledgerTo != from || cur.ledgerHi != e.ledgerHi || cur.store != e.store {
			return false
		}
		cur.applyLedger(dl)
		cur.ledgerTo = hi
		cur.ledgerBuilt = hi >= cur.ledgerHi
		c := &catchUp{s: s, q: s.q(ctx), ctx: ctx, e: cur}
		_ = c.coverNew(dl.pubs)
		return true
	}, 0)
	return nil
}

// unknownSpan is the newest settlement day whose span is not known yet.
func (e *epoch) unknownSpan() string {
	best := ""
	for d, sd := range e.settle {
		if sd.Pubs > 0 && !sd.SpanKnown && d > best {
			best = d
		}
	}
	return best
}

// knowSpan reads day d's span. Every row inserted since the epoch began has
// been widened into the day as it arrived, known or not, so the span read
// now joined with what the day holds covers every row of its promises.
func (s *Server) knowSpan(ctx context.Context, e *epoch, d string) error {
	sd := e.settle[d]
	lo, hi, msu, err := s.daySpan(ctx, d, sd)
	if err != nil {
		return err
	}
	s.parts.publish(func(cur *epoch, _ []journalEntry) bool {
		c, ok := cur.settle[d]
		if !ok || c.SpanKnown || cur.store != e.store {
			return false
		}
		m := cur.settleMut(d)
		if lo != "" {
			m.widen(lo, msu)
			m.widen(hi, msu)
		} else if msu != "" {
			m.widen("", msu)
		}
		m.SpanKnown = true
		return true
	}, 0)
	return nil
}

// daySpan reads the exact row span of the publications settled on d.
func (s *Server) daySpan(ctx context.Context, d string, sd *settleDay) (lo, hi, msu string, err error) {
	var a, b, m sql.NullString
	err = s.q(ctx).QueryRowContext(ctx, daySpanSQL, dayLo(d), dayHi(d), sd.HLo, sd.HHi).Scan(&a, &b, &m)
	return a.String, b.String, m.String, err
}

// nextSeal picks the oldest day due to be sealed: a row day or a
// settlement day, whichever is older, among those not waiting out a retry.
func (dp *dayParts) nextSeal(e *epoch, now time.Time) (kind, day string) {
	dp.mu.Lock()
	defer dp.mu.Unlock()
	if dp.retry == nil {
		dp.retry = map[string]time.Time{}
	}
	waiting := func(k string) bool { t, ok := dp.retry[k]; return ok && now.Before(t) }
	var settle []string
	for d, sd := range e.settle {
		if sd.Pubs == 0 || sd.Seal != nil || !sd.SpanKnown || waiting("settle:"+d) {
			continue
		}
		if t, err := time.Parse(store.TimeLayout, sd.MaxPubMSU); err == nil && now.Before(t.Add(rollup.FinalMargin)) {
			continue
		}
		settle = append(settle, d)
	}
	sort.Strings(settle)
	row := ""
	first := e.firstRow
	if e.rawFrom != "" && e.rawFrom > first {
		first = e.rawFrom
	}
	if first != "" {
		for d := first; ; d = dayAdd(d, 1) {
			t, err := time.Parse(dayLayout, d)
			if err != nil || now.Before(t.Add(24*time.Hour+sealMargin)) {
				break
			}
			if e.rows[d] == nil && !waiting("row:"+d) {
				row = d
				break
			}
		}
	}
	switch {
	case row != "" && (len(settle) == 0 || row <= settle[0]):
		return "row", row
	case len(settle) > 0:
		return "settle", settle[0]
	}
	return "", ""
}

// later leaves a day for sealRetry.
func (dp *dayParts) later(key string, now time.Time) {
	dp.mu.Lock()
	defer dp.mu.Unlock()
	if dp.retry == nil {
		dp.retry = map[string]time.Time{}
	}
	dp.retry[key] = now.Add(sealRetry)
}

// sealRow seals row day d: every validator's rows started on it, the
// promises a collapse would move, and its anchors.
func (s *Server) sealRow(ctx context.Context, e *epoch, d string, now time.Time) error {
	q := s.q(ctx)
	lo, hi := dayLo(d), dayHi(d)
	var deferred int64
	if err := q.QueryRowContext(ctx, dayDeferredSQL, lo, hi).Scan(&deferred); err != nil {
		return err
	}
	if deferred > 0 {
		s.parts.later("row:"+d, now)
		return nil
	}
	// The boundary guard: nothing sorts between this day's bounds and its
	// neighbours'.
	var weird []string
	for _, t := range []string{"probes", "sampling_decision_points", "reachability"} {
		for _, g := range [][2]string{{dayHi(dayAdd(d, -1)), lo}, {hi, dayLo(dayAdd(d, 1))}} {
			var v string
			err := q.QueryRowContext(ctx, boundaryGapSQL(t), g[0], g[1]).Scan(&v)
			if err == nil {
				weird = append(weird, v)
				continue
			}
			if !errors.Is(err, sql.ErrNoRows) {
				return err
			}
		}
	}
	if len(weird) > 0 {
		s.parts.publish(func(cur *epoch, _ []journalEntry) bool {
			for _, w := range weird {
				cur.addWeird(w)
			}
			return true
		}, 0)
		s.parts.later("row:"+d, now)
		return nil
	}
	vals, err := s.rowSpanParts(ctx, q, lo, hi, "")
	if err != nil {
		return err
	}
	rd := &rowDay{Day: d, Vals: vals, SealedAt: store.TS(now)}
	rows, err := q.QueryContext(ctx, dayCollapsibleSQL, lo, hi)
	if err != nil {
		return err
	}
	for rows.Next() {
		var h string
		if err := rows.Scan(&h); err != nil {
			rows.Close()
			return err
		}
		rd.Collapsible = append(rd.Collapsible, h)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	anchors, err := s.anchorsFor(ctx, e, []string{d})
	if err != nil {
		return err
	}
	if !s.parts.publish(func(cur *epoch, since []journalEntry) bool {
		if cur.store != e.store || touched(since, d, true) || cur.rows[d] != nil {
			return false
		}
		rd.Gen = s.parts.nextGen("row:" + d)
		cur.rows[d] = rd
		cur.addAnchors(anchors)
		return true
	}, e.seq) {
		s.parts.later("row:"+d, now.Add(-sealRetry+time.Second))
	}
	return nil
}

// sealSettle seals settlement day d: its exact span, how many of its
// publications are readable, and, from raw_from on, its obligations read
// with that span.
func (s *Server) sealSettle(ctx context.Context, e *epoch, d string, now time.Time) error {
	q := s.q(ctx)
	sd := e.settle[d]
	day, _ := time.Parse(dayLayout, d)
	h := &rollup.Heights{Lo: sd.HLo, Hi: sd.HHi}
	for _, check := range []func() (bool, string, error){
		func() (bool, string, error) { return rollup.WindowsClosed(ctx, q, day, now, h) },
		func() (bool, string, error) { return rollup.ShadowVerdictsSettled(ctx, q, day, h) },
	} {
		final, _, err := check()
		if err != nil {
			return err
		}
		if !final {
			s.parts.later("settle:"+d, now)
			return nil
		}
	}
	lo, hi, msu, err := s.daySpan(ctx, d, sd)
	if err != nil {
		return err
	}
	if lo != "" {
		t, err := time.Parse(store.TimeLayout, hi)
		if err != nil || now.Before(t.Add(sealMargin)) {
			s.parts.later("settle:"+d, now)
			return nil
		}
	}
	if msu > store.TS(now) {
		s.parts.later("settle:"+d, now)
		return nil
	}
	seal := &settleSeal{SealedAt: store.TS(now), MinStart: lo, MaxStart: hi, MaxRowMSU: msu}
	if err := q.QueryRowContext(ctx, dayReadableSQL, dayLo(d), dayHi(d), sd.HLo, sd.HHi).Scan(&seal.Readable); err != nil {
		return err
	}
	if e.rawFrom == "" || d >= e.rawFrom {
		seal.Obl = map[string]rollup.Obligations{}
		if lo != "" {
			rows, err := q.QueryContext(ctx, obligationPassSQL(""), "", "", store.TS(now), dayLo(d), dayHi(d), hi, lo)
			if err != nil {
				return err
			}
			var pending int64
			for rows.Next() {
				var addr string
				var r rollup.Obligations
				var n int64
				var young sql.NullString
				if err := rows.Scan(append(append([]any{&addr}, scanObligations(&r)...), &n, &young)...); err != nil {
					rows.Close()
					return err
				}
				seal.Obl[addr] = r
				pending += r.Pending
				seal.MaxFirstFault = maxString(seal.MaxFirstFault, young.String)
			}
			if err := rows.Close(); err != nil {
				return err
			}
			if pending > 0 {
				s.parts.later("settle:"+d, now)
				return nil
			}
			if err := q.QueryRowContext(ctx, dayTiesSQL, dayLo(d), dayHi(d), hi, lo).Scan(&seal.Ties); err != nil {
				return err
			}
			if seal.Ties > 0 && s.parts.log != nil {
				s.parts.log("day partials: settlement day %s has %d obligation row(s) tied on their newest reading; the class they count by depends on the statement's plan", d, seal.Ties)
			}
		}
	}
	var days []string
	if lo != "" {
		for x := lo[:10]; x <= hi[:10]; x = dayAdd(x, 1) {
			days = append(days, x)
		}
	}
	anchors, err := s.anchorsFor(ctx, e, days)
	if err != nil {
		return err
	}
	if !s.parts.publish(func(cur *epoch, since []journalEntry) bool {
		c, ok := cur.settle[d]
		if cur.store != e.store || !ok || c.Seal != nil || touched(since, d, false) {
			return false
		}
		seal.Gen = s.parts.nextGen("settle:" + d)
		m := cur.settleMut(d)
		m.Seal = seal
		m.MinStart, m.MaxStart, m.MaxRowMSU, m.SpanKnown = lo, hi, msu, true
		cur.addAnchors(anchors)
		return true
	}, e.seq) {
		s.parts.later("settle:"+d, now.Add(-sealRetry+time.Second))
	}
	return nil
}

// anchorsFor picks an anchor row of each table on every day in days that
// lacks one: a day not anchored yet, or one anchored while a table had no
// row on it. That happens when the collector is down over a whole day: the
// sealer seals the day empty, and when the backlog arrives the day is
// dropped and sealed again with rows, which the prune then deletes. An
// anchor still missing then would leave the prune of those rows unseen
// until raw_from moved, after its last statement.
func (s *Server) anchorsFor(ctx context.Context, e *epoch, days []string) (map[string]*dayAnchors, error) {
	q := s.q(ctx)
	out := map[string]*dayAnchors{}
	pick := func(query string, args ...any) (*anchor, error) {
		var a anchor
		err := q.QueryRowContext(ctx, query, args...).Scan(&a.Rowid, &a.Key)
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		if err != nil {
			return nil, err
		}
		return &a, nil
	}
	for _, d := range days {
		if len(d) != 10 {
			continue
		}
		var a dayAnchors
		if cur := e.anchors[d]; cur != nil {
			if cur.Probe != nil && cur.Point != nil && cur.Beat != nil {
				continue
			}
			a = *cur
		}
		lo, hi := dayLo(d), dayHi(d)
		var err error
		if a.Probe == nil {
			if a.Probe, err = pick(anchorProbeSQL, lo, hi); err != nil {
				return nil, err
			}
		}
		if a.Probe == nil {
			if a.Probe, err = pick(anchorProbeAnySQL, lo, hi); err != nil {
				return nil, err
			}
		}
		if a.Point == nil {
			if a.Point, err = pick(anchorPointSQL, lo, hi); err != nil {
				return nil, err
			}
		}
		if a.Beat == nil {
			if a.Beat, err = pick(anchorBeatSQL, lo, hi, s.vantage); err != nil {
				return nil, err
			}
		}
		out[d] = &a
	}
	return out, nil
}

// addAnchors installs anchors for days that have none, and on a day that
// has some fills in those it lacks; an anchor already there stays. An
// anchor picked in an older snapshot than the epoch's can only be gone
// already, which the next catch-up takes for the prune and drops the day
// for: too much dropped, never too little.
func (e *epoch) addAnchors(m map[string]*dayAnchors) {
	for d, a := range m {
		cur := e.anchors[d]
		if cur == nil {
			e.anchors[d] = a
			continue
		}
		n := *cur
		if n.Probe == nil {
			n.Probe = a.Probe
		}
		if n.Point == nil {
			n.Point = a.Point
		}
		if n.Beat == nil {
			n.Beat = a.Beat
		}
		if n != *cur {
			e.anchors[d] = &n
		}
	}
}

// nextGen is the next seal generation of a day, for its file's name.
func (dp *dayParts) nextGen(key string) int {
	if dp.gens == nil {
		dp.gens = map[string]int{}
	}
	dp.gens[key]++
	return dp.gens[key]
}

// sealEvery is how often the sealer looks for work, and sealIdle how long
// it rests once there is none (Server.sealPace sets others, for tests).
const (
	sealEvery = 2 * time.Second
	sealIdle  = 30 * time.Second
)

// sealer runs until Close: the ledger build, the spans and the seals, one
// unit at a time.
func (s *Server) sealer() {
	every, idle := sealEvery, sealIdle
	if s.sealPace != [2]time.Duration{} {
		every, idle = s.sealPace[0], s.sealPace[1]
	}
	wait := every
	started, audited := s.now(), s.now()
	for {
		select {
		case <-s.stop:
			return
		case <-time.After(wait):
		}
		ctx, cancel := context.WithTimeout(context.Background(), snapshotTimeout)
		n, err := s.sealDue(ctx, 1)
		cancel()
		switch {
		case err != nil:
			if s.log != nil {
				s.log.Printf("day partials: sealer: %v", err)
			}
			wait = idle
		case n == 0:
			wait = idle
		default:
			wait = every
			s.parts.saveLater(s)
		}
		if s.now().Sub(audited) >= auditEvery {
			audited = s.now()
			ctx, cancel := context.WithTimeout(context.Background(), snapshotTimeoutAll)
			if err := s.auditOnce(ctx, started); err != nil && s.log != nil {
				s.log.Printf("day partials: audit: %v", err)
			}
			cancel()
		}
	}
}
