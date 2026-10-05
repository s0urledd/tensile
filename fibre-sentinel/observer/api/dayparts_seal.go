package api

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync/atomic"
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
//     row of them has a deadline after now, nothing of it is pending, and
//     no two of its obligation rows tie on their newest reading (the class
//     such an obligation counts as is the plan's to choose, so a day's
//     statement and a window's cannot be shown to agree on it).
//     A held day is sealed like any other: every move of a hold is found
//     (the holds' counter, then a digest of each promise's held rows), so
//     a hold raised or lifted drops it either way, and a range that cannot
//     be closed does not keep the day raw for good;
//   - a row day that holds a row, an hour after it ends, once no row
//     started on it awaits the late shadow verdict and nothing that is not
//     a store timestamp sorts against its bounds (the boundary guard). A
//     day with no row is never sealed: the raw span over it is a seek of
//     each index.
//
// Neither is sealed before raw_from: the rollup holds those days. A
// settlement day before raw_from is sealed without obligations, for the
// publications it keeps readable.
//
// The sealer works one day at a time, oldest first, each in a read
// transaction of its own, after the ledger is built and every settlement
// day's row span is known. It seals optimistically: a day it read under
// one epoch is published into the newest only if nothing a catch-up found
// since reaches it (the journal): a drop of the day, a collapse of a
// promise its rows hold, the prune of rows in its span.
//
// The disk it reads may be shared, and its reads are the partials' only
// burst of I/O: it works at a pace (WithSealPace, -day-partials-pace),
// resting k times as long as each unit took, the warm-up's run as much as
// the live sealer. A unit that fails is tried again after a delay that
// doubles each time (fail), not every rest, and the sealer goes on with
// the others; a day not ready is tried again after sealRetry, and is not
// work: the sealer rests after it as after none.

// sealMargin is how long after the newest row of a day, or of a day's
// promises, the sealer waits before it seals.
const sealMargin = time.Hour

// sealRetry is how long a day that could not be sealed is left before it
// is tried again.
const sealRetry = 10 * time.Minute

// ledgerChunk is how many publications one step of the ledger build folds:
// seconds of work at the busiest traffic seen, so a build of a long record
// is hundreds of steps, not thousands.
const ledgerChunk = 10000

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
// applies: apply gets a copy and reports whether it changed it. dropped
// names the days the change drops, journalled so that a seal of them being
// read meanwhile is not published.
func (dp *dayParts) publish(apply func(e *epoch, since []journalEntry) bool, base uint64, dropped ...journalEntry) bool {
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
	j := journalEntry{seq: e.seq}
	for _, d := range dropped {
		j.rows, j.settle = d.rows, d.settle
	}
	dp.journal = append(dp.journal, j)
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

// collapsedAny reports whether a journal entry since the base found a
// collapse of any of the promises (a row day's Collapsible).
func collapsedAny(since []journalEntry, promises []string) bool {
	for _, j := range since {
		for _, h := range promises {
			if j.collapsed[h] {
				return true
			}
		}
	}
	return false
}

// reached reports whether a journal entry since the base found the rows of
// a span in [lo, hi] taken by the prune.
func reached(since []journalEntry, lo, hi string) bool {
	for _, j := range since {
		for _, r := range j.reach {
			if r[0] <= hi && r[1] >= lo {
				return true
			}
		}
	}
	return false
}

// msuMoved reports whether a journal entry since the base read day d's
// latest deadline again while the ledger did not hold the day yet.
func msuMoved(since []journalEntry, d string) bool {
	for _, j := range since {
		if j.all || j.msu[d] {
			return true
		}
	}
	return false
}

// outcome is what one unit of the sealer's work came to.
type outcome int

const (
	// outNone: no unit was due.
	outNone outcome = iota
	// outDone: a day sealed, a step of the ledger or a day's span: the
	// partials moved.
	outDone
	// outDeferred: the day is not final yet, or kept raw (tied obligation
	// rows, a start beside it that is not a store timestamp); it is tried
	// again later. Nothing moved.
	outDeferred
	// outRefused: the day was read, and a catch-up found something that
	// reaches it before its seal was published; it is tried again at once.
	outRefused
)

// unit is one unit of the sealer's work: what it was and what it came to.
type unit struct {
	kind string // "ledger", "span", "row", "settle"; "" for none
	day  string
	out  outcome
}

// key names the unit for its backoff and its retry.
func (u unit) key() string {
	if u.day == "" {
		return u.kind
	}
	return u.kind + ":" + u.day
}

// sealDue does up to n units of the sealer's work at the server's clock and
// reports how many moved the partials: a step of the ledger build, a day's
// span, or a day sealed. A unit that only finds its day not ready counts
// toward n and not in what it reports. With a pace (WithSealPace), it rests
// pace times as long as each unit took before the next.
func (s *Server) sealDue(ctx context.Context, n int) (int, error) {
	if s.parts == nil {
		return 0, nil
	}
	s.parts.sealing.Lock()
	defer s.parts.sealing.Unlock()
	moved := 0
	for i := 0; i < n; i++ {
		t0 := time.Now()
		u, err := s.sealOnce(ctx)
		if err != nil {
			return moved, err
		}
		if u.out == outNone {
			break
		}
		if u.out == outDone {
			moved++
		}
		if i+1 < n && !s.pause(ctx, time.Since(t0)) {
			return moved, ctx.Err()
		}
	}
	return moved, nil
}

// pause rests pace times took (WithSealPace), and reports false when ctx
// ended or the server stopped first.
func (s *Server) pause(ctx context.Context, took time.Duration) bool {
	if s.sealDuty <= 0 {
		return true
	}
	t := time.NewTimer(time.Duration(s.sealDuty * float64(took)))
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	case <-s.stop:
		return false
	}
}

// sealOnce does one unit of work, if there is one. A unit that fails is
// tried again after a delay that doubles each time it fails (backoff), and
// the sealer takes the next meanwhile.
func (s *Server) sealOnce(ctx context.Context) (unit, error) {
	var u unit
	dp := s.parts
	err := s.partsTx(ctx, func(ctx context.Context, e *epoch) error {
		now := s.now().UTC()
		building := !e.ledgerBuilt && !e.pubsOdd
		var err error
		switch {
		case e.pubsOdd:
		case building:
			// A row day does not read the ledger, so the ledger's steps take
			// turns with the row days rather than all coming first: after a
			// rebuild the validators' rows are summed long before every
			// publication ever stored is in the ledger.
			if !dp.backingOff("ledger", now) && dp.ledgerTurn() {
				u.kind = "ledger"
				u.out, err = s.buildLedger(ctx, e)
				return err
			}
		default:
			if d := e.unknownSpan(func(d string) bool { return dp.backingOff("span:"+d, now) }); d != "" {
				u.kind, u.day = "span", d
				u.out, err = s.knowSpan(ctx, e, d)
				return err
			}
		}
		kind, d, err := s.nextSeal(ctx, e, now)
		if err != nil {
			u.kind = "next"
			return err
		}
		switch {
		case kind == "row":
			u.kind, u.day = kind, d
			u.out, err = s.sealRow(ctx, e, d, now)
			return err
		case kind == "settle" && !building:
			u.kind, u.day = kind, d
			u.out, err = s.sealSettle(ctx, e, d, now)
			return err
		}
		if building && !dp.backingOff("ledger", now) {
			u.kind = "ledger"
			u.out, err = s.buildLedger(ctx, e)
			return err
		}
		return nil
	})
	if errors.Is(err, errNoParts) {
		return unit{}, nil
	}
	if err != nil {
		if u.kind != "" {
			dp.fail(u.key(), s.now(), sealFailBase, sealFailCeiling, err)
		}
		return u, err
	}
	if u.kind != "" {
		dp.recover(u.key())
	}
	return u, nil
}

// sealFailBase and sealFailCeiling bound the delay before a failed unit is
// tried again: a minute after its first failure, twice as long after each
// one after it, at most six hours.
const (
	sealFailBase    = time.Minute
	sealFailCeiling = 6 * time.Hour
)

// buildLedger folds the next chunk of the publications the epoch began
// with into the ledger.
func (s *Server) buildLedger(ctx context.Context, e *epoch) (outcome, error) {
	from := e.ledgerTo
	hi := min(from+ledgerChunk, e.ledgerHi)
	dl, err := s.readLedger(ctx, s.q(ctx), from, hi, false)
	if err != nil {
		return outNone, err
	}
	ok := s.parts.publish(func(cur *epoch, since []journalEntry) bool {
		if cur.ledgerBuilt || cur.ledgerTo != from || cur.ledgerHi != e.ledgerHi || cur.born != e.born {
			return false
		}
		// A correction that moved a deadline of the chunk's publications
		// since it was read found no day of theirs in the ledger to read
		// again (readMaxPubMSU): the next catch-up does.
		for d := range dl.days {
			if msuMoved(since, d) {
				due := copySet(cur.msuDue)
				due[d] = true
				cur.msuDue = due
			}
		}
		cur.applyLedger(dl)
		cur.ledgerTo = hi
		cur.ledgerBuilt = hi >= cur.ledgerHi
		c := &catchUp{s: s, q: s.q(ctx), ctx: ctx, e: cur}
		_ = c.coverNew(dl.pubs)
		return true
	}, e.seq)
	return published(ok), nil
}

// published is the outcome of a unit whose publish succeeded or not.
func published(ok bool) outcome {
	if ok {
		return outDone
	}
	return outRefused
}

// unknownSpan is the newest settlement day whose span is not known yet,
// of those skip leaves.
func (e *epoch) unknownSpan(skip func(d string) bool) string {
	best := ""
	for d, sd := range e.settle {
		if sd.Pubs > 0 && !sd.SpanKnown && d > best && !skip(d) {
			best = d
		}
	}
	return best
}

// knowSpan reads day d's span. Every row inserted since the epoch began has
// been widened into the day as it arrived, known or not, so the span read
// now joined with what the day holds covers every row of its promises.
func (s *Server) knowSpan(ctx context.Context, e *epoch, d string) (outcome, error) {
	sd := e.settle[d]
	lo, hi, msu, err := s.daySpan(ctx, d, sd)
	if err != nil {
		return outNone, err
	}
	ok := s.parts.publish(func(cur *epoch, _ []journalEntry) bool {
		c, ok := cur.settle[d]
		if !ok || c.SpanKnown || cur.born != e.born {
			return false
		}
		cur.content++
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
	return published(ok), nil
}

// daySpan reads the exact row span of the publications settled on d.
func (s *Server) daySpan(ctx context.Context, d string, sd *settleDay) (lo, hi, msu string, err error) {
	var a, b, m sql.NullString
	err = s.q(ctx).QueryRowContext(ctx, daySpanSQL, dayLo(d), dayHi(d), sd.HLo, sd.HHi).Scan(&a, &b, &m)
	return a.String, b.String, m.String, err
}

// nextSeal picks the oldest day due to be sealed: a row day or a
// settlement day, whichever is older, among those not waiting out a retry
// or a failure's backoff. It notes every day it finds due and not sealed,
// waiting or not, for /v1/health (noteDue).
//
// A row day is sealed only when it has a row of a table the row days sum
// (nextRowDay); a day with none is left to the raw spans, where it costs a
// seek of each index and nothing more. Sealing every day from the first
// row on sealed, and kept a file for, every empty day between a row
// stamped long before the record (a clock gone wrong; a start the prober
// never set is the year 1) and the record itself, oldest first, before any
// day that holds something.
func (s *Server) nextSeal(ctx context.Context, e *epoch, now time.Time) (kind, day string, err error) {
	dp := s.parts
	waiting := dp.waitingAt(now)
	due := map[string]bool{}
	var settle []string
	for d, sd := range e.settle {
		if sd.Pubs == 0 || sd.Seal != nil || !sd.SpanKnown {
			continue
		}
		if t, err := time.Parse(store.TimeLayout, sd.MaxPubMSU); err == nil && now.Before(t.Add(rollup.FinalMargin)) {
			continue
		}
		due["settle:"+d] = true
		if !waiting("settle:" + d) {
			settle = append(settle, d)
		}
	}
	sort.Strings(settle)
	first := e.firstRow
	if e.rawFrom != "" && e.rawFrom > first {
		first = e.rawFrom
	}
	row := ""
	if first != "" {
		isDue := func(d string) bool {
			t, err := time.Parse(dayLayout, d)
			return err == nil && !now.Before(t.Add(24*time.Hour+sealMargin))
		}
		for d := first; isDue(d); {
			if e.rows[d] != nil {
				d = dayAdd(d, 1)
				continue
			}
			if waiting("row:" + d) {
				due["row:"+d] = true
				d = dayAdd(d, 1)
				continue
			}
			next, ok, err := s.nextRowDay(ctx, s.q(ctx), d)
			if err != nil {
				return "", "", err
			}
			if !ok {
				break
			}
			if next == d {
				row = d
				due["row:"+d] = true
				break
			}
			d = next
		}
	}
	dp.noteDue(due, now)
	switch {
	case row != "" && (len(settle) == 0 || row <= settle[0]):
		return "row", row, nil
	case len(settle) > 0:
		return "settle", settle[0], nil
	}
	return "", "", nil
}

// nextRowDay is the first day at or after d that a row the row days sum
// started on.
func (s *Server) nextRowDay(ctx context.Context, q store.Querier, d string) (string, bool, error) {
	return s.rowDayFrom(ctx, q, dayLo(d))
}

// rowDayFrom is the day of the first start at or after from of a row the
// row days sum: a probe row, a decision point, or one of this observer's
// heartbeats, sought through the tables' started_at indexes. A start whose
// first ten characters are not a day is stepped over.
func (s *Server) rowDayFrom(ctx context.Context, q store.Querier, from string) (string, bool, error) {
	for {
		var v sql.NullString
		if err := q.QueryRowContext(ctx, nextRowSQL, from, s.vantage).Scan(&v); err != nil {
			return "", false, err
		}
		if !v.Valid {
			return "", false, nil
		}
		if len(v.String) >= 10 {
			if _, err := time.Parse(dayLayout, v.String[:10]); err == nil {
				return v.String[:10], true, nil
			}
		}
		from = v.String + "\x00"
	}
}

// later leaves a day that is not ready for sealRetry.
func (dp *dayParts) later(key string, now time.Time) {
	dp.retryAt(key, now.Add(sealRetry))
}

// keepRaw leaves a day kept raw on purpose (why), tried again after a
// delay that doubles each time it is found so, from sealRetry to six hours:
// tied obligation rows, a start beside the day that is not a store
// timestamp, do not go away on their own. /v1/health counts these days
// apart from the ones that are due.
func (dp *dayParts) keepRaw(key, why string, now time.Time) {
	dp.mu.Lock()
	if dp.raw == nil {
		dp.raw = map[string]*keptRaw{}
	}
	k := dp.raw[key]
	if k == nil {
		k = &keptRaw{}
		dp.raw[key] = k
	}
	k.why = why
	delay := min(sealRetry<<min(k.times, 6), sealFailCeiling)
	k.times++
	dp.mu.Unlock()
	dp.retryAt(key, now.Add(delay))
}

// keptRaw is a day kept raw on purpose: why, and how many times in a row.
type keptRaw struct {
	why   string
	times int
}

func (dp *dayParts) retryAt(key string, at time.Time) {
	dp.mu.Lock()
	defer dp.mu.Unlock()
	if dp.retry == nil {
		dp.retry = map[string]time.Time{}
	}
	dp.retry[key] = at
}

// sealedNow forgets what was waited for of a day just sealed.
func (dp *dayParts) sealedNow(key string) {
	dp.mu.Lock()
	defer dp.mu.Unlock()
	delete(dp.raw, key)
	delete(dp.retry, key)
	delete(dp.dueSince, key)
}

// waitingAt is whether a unit waits, at now, for its retry or for its
// failure's backoff: a retry waited out is forgotten.
func (dp *dayParts) waitingAt(now time.Time) func(key string) bool {
	dp.mu.Lock()
	wait := make(map[string]bool, len(dp.retry)+len(dp.failures))
	for k, t := range dp.retry {
		if now.Before(t) {
			wait[k] = true
		} else {
			delete(dp.retry, k)
		}
	}
	for k, f := range dp.failures {
		if now.Before(f.until) {
			wait[k] = true
		}
	}
	dp.mu.Unlock()
	return func(key string) bool { return wait[key] }
}

// noteDue keeps, for each day due and not sealed, when the sealer first
// found it so; a day it no longer finds due (sealed, or no longer due) is
// forgotten. /v1/health leaves out the days kept raw on purpose.
func (dp *dayParts) noteDue(due map[string]bool, now time.Time) {
	dp.mu.Lock()
	defer dp.mu.Unlock()
	if dp.dueSince == nil {
		dp.dueSince = map[string]time.Time{}
	}
	for k := range dp.dueSince {
		if !due[k] {
			delete(dp.dueSince, k)
		}
	}
	for k := range due {
		if _, ok := dp.dueSince[k]; !ok {
			dp.dueSince[k] = now
		}
	}
}

// failure is a unit that failed: how many times in a row, the last error,
// and when it may be tried again.
type failure struct {
	n     int
	err   string
	until time.Time
}

// fail notes that a unit failed at now: it is tried again after base, twice
// as long after each failure after that, at most ceiling. The first failure
// of a run is logged, and the recovery (recover); the ones between are not.
func (dp *dayParts) fail(key string, now time.Time, base, ceiling time.Duration, err error) {
	dp.mu.Lock()
	if dp.failures == nil {
		dp.failures = map[string]*failure{}
	}
	f := dp.failures[key]
	if f == nil {
		f = &failure{}
		dp.failures[key] = f
	}
	delay := min(base<<min(f.n, 16), ceiling)
	f.n++
	f.err, f.until = err.Error(), now.Add(delay)
	first := f.n == 1
	dp.mu.Unlock()
	if first && dp.log != nil {
		dp.log("day partials: %s failed: %v; tried again in %s, then twice as late after each failure, at most %s", key, err, delay, ceiling)
	}
}

// recover forgets a unit's failures once it succeeded, and says so.
func (dp *dayParts) recover(key string) {
	dp.mu.Lock()
	f := dp.failures[key]
	delete(dp.failures, key)
	dp.mu.Unlock()
	if f != nil && dp.log != nil {
		dp.log("day partials: %s succeeded after %d failure(s)", key, f.n)
	}
}

// backingOff is whether a unit that failed is still waiting out its delay.
func (dp *dayParts) backingOff(key string, now time.Time) bool {
	dp.mu.Lock()
	defer dp.mu.Unlock()
	f := dp.failures[key]
	return f != nil && now.Before(f.until)
}

// logOnce logs a line once per key for the process: a day tried again
// every sealRetry says why it stays raw the first time only.
func (dp *dayParts) logOnce(key, format string, args ...any) {
	dp.mu.Lock()
	if dp.logged == nil {
		dp.logged = map[string]bool{}
	}
	seen := dp.logged[key]
	dp.logged[key] = true
	dp.mu.Unlock()
	if !seen && dp.log != nil {
		dp.log(format, args...)
	}
}

// sealRow seals row day d: every validator's rows started on it, the
// promises a collapse would move, and its anchors.
func (s *Server) sealRow(ctx context.Context, e *epoch, d string, now time.Time) (outcome, error) {
	if f := s.parts.failUnit; f != nil {
		if err := f("row", d); err != nil {
			return outNone, err
		}
	}
	q := s.q(ctx)
	lo, hi := dayLo(d), dayHi(d)
	var deferred int64
	if err := q.QueryRowContext(ctx, dayDeferredSQL, lo, hi).Scan(&deferred); err != nil {
		return outNone, err
	}
	if deferred > 0 {
		s.parts.later("row:"+d, now)
		return outDeferred, nil
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
				return outNone, err
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
		s.parts.keepRaw("row:"+d, "a row start beside it is not a store timestamp", now)
		return outDeferred, nil
	}
	vals, err := s.rowSpanParts(ctx, q, lo, hi, "")
	if err != nil {
		return outNone, err
	}
	rd := &rowDay{Day: d, Vals: vals, SealedAt: store.TS(now)}
	rows, err := q.QueryContext(ctx, dayCollapsibleSQL, lo, hi)
	if err != nil {
		return outNone, err
	}
	for rows.Next() {
		var h string
		if err := rows.Scan(&h); err != nil {
			rows.Close()
			return outNone, err
		}
		rd.Collapsible = append(rd.Collapsible, h)
	}
	if err := rows.Close(); err != nil {
		return outNone, err
	}
	anchors, err := s.anchorsFor(ctx, e, []string{d})
	if err != nil {
		return outNone, err
	}
	// The day whole goes to its seal file now, and the epoch keeps it
	// without its histograms (dayparts_hist.go); with the partials kept
	// nowhere it keeps it whole.
	dp := s.parts
	kept, name, digest := rd, "", ""
	if dp.file != "" {
		if name, digest, err = dp.writeRowSeal(e, rd); err != nil {
			return outNone, err
		}
		kept = rd.stripped(name, digest)
	}
	if dp.beforePublish != nil {
		dp.beforePublish("row", d)
	}
	// Published only if nothing a catch-up found since the read reaches the
	// day: a drop of it (a row, a hold, a correction), a collapse of a
	// promise its rows held, or the prune of its rows.
	if !dp.publish(func(cur *epoch, since []journalEntry) bool {
		if cur.born != e.born || touched(since, d, true) || collapsedAny(since, rd.Collapsible) ||
			reached(since, lo, hi) || cur.rows[d] != nil {
			return false
		}
		if name == "" {
			rd.Gen = dp.nextGen("row:" + d)
		} else {
			// Under the lock the publish holds: a write of the partials
			// from here on names the file with the day.
			if dp.sealFiles == nil {
				dp.sealFiles = map[string]string{}
			}
			dp.sealFiles[name] = digest
			delete(dp.pending, name)
		}
		cur.rows[d] = kept
		cur.addAnchors(anchors)
		cur.content++
		return true
	}, e.seq) {
		if name != "" {
			dp.unpend(name, true)
		}
		dp.retryAt("row:"+d, now.Add(time.Second))
		return outRefused, nil
	}
	if name != "" {
		dp.hists.put(d, digest, histsOf(rd))
	}
	dp.sealedNow("row:" + d)
	return outDone, nil
}

// sealSettle seals settlement day d: its exact span, how many of its
// publications are readable, and, from raw_from on, its obligations read
// with that span.
func (s *Server) sealSettle(ctx context.Context, e *epoch, d string, now time.Time) (outcome, error) {
	if f := s.parts.failUnit; f != nil {
		if err := f("settle", d); err != nil {
			return outNone, err
		}
	}
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
			return outNone, err
		}
		if !final {
			s.parts.later("settle:"+d, now)
			return outDeferred, nil
		}
	}
	lo, hi, msu, err := s.daySpan(ctx, d, sd)
	if err != nil {
		return outNone, err
	}
	if lo != "" {
		t, err := time.Parse(store.TimeLayout, hi)
		if err != nil || now.Before(t.Add(sealMargin)) {
			s.parts.later("settle:"+d, now)
			return outDeferred, nil
		}
	}
	if msu > store.TS(now) {
		s.parts.later("settle:"+d, now)
		return outDeferred, nil
	}
	seal := &settleSeal{SealedAt: store.TS(now), MinStart: lo, MaxStart: hi, MaxRowMSU: msu}
	if err := q.QueryRowContext(ctx, dayReadableSQL, dayLo(d), dayHi(d), sd.HLo, sd.HHi).Scan(&seal.Readable); err != nil {
		return outNone, err
	}
	if e.rawFrom == "" || d >= e.rawFrom {
		seal.Obl = map[string]rollup.Obligations{}
		if lo != "" {
			rows, err := q.QueryContext(ctx, obligationPassSQL(""), "", "", store.TS(now), dayLo(d), dayHi(d), hi, lo)
			if err != nil {
				return outNone, err
			}
			var pending int64
			for rows.Next() {
				var addr string
				var r rollup.Obligations
				var n int64
				var young sql.NullString
				if err := rows.Scan(append(append([]any{&addr}, scanObligations(&r)...), &n, &young)...); err != nil {
					rows.Close()
					return outNone, err
				}
				seal.Obl[addr] = r
				pending += r.Pending
				seal.MaxFirstFault = maxString(seal.MaxFirstFault, young.String)
			}
			if err := rows.Close(); err != nil {
				return outNone, err
			}
			if pending > 0 {
				s.parts.later("settle:"+d, now)
				return outDeferred, nil
			}
			// Rows that tie on everything ObligationBuckets orders by leave
			// the class an obligation counts as to the plan and the sort, so
			// the day's statement and a window's cannot be shown to agree on
			// it: the day is read raw, and tried again as any other.
			var ties int64
			if err := q.QueryRowContext(ctx, dayTiesSQL, dayLo(d), dayHi(d), hi, lo).Scan(&ties); err != nil {
				return outNone, err
			}
			if ties > 0 {
				s.parts.logOnce("ties:"+d, "day partials: settlement day %s has %d obligation row(s) tied on their newest reading; it is read raw", d, ties)
				s.parts.keepRaw("settle:"+d, "obligation rows tied on their newest reading", now)
				return outDeferred, nil
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
		return outNone, err
	}
	if s.parts.beforePublish != nil {
		s.parts.beforePublish("settle", d)
	}
	// Published only if nothing a catch-up found since the read reaches the
	// day: a drop of it (a row of its promises, a hold, a correction, a
	// collapse, a deadline moved), or the prune of rows in its span.
	if !s.parts.publish(func(cur *epoch, since []journalEntry) bool {
		c, ok := cur.settle[d]
		if cur.born != e.born || !ok || c.Seal != nil || touched(since, d, false) || (lo != "" && reached(since, lo, hi)) {
			return false
		}
		cur.content++
		seal.Gen = s.parts.nextGen("settle:" + d)
		m := cur.settleMut(d)
		m.Seal = seal
		m.MinStart, m.MaxStart, m.MaxRowMSU, m.SpanKnown = lo, hi, msu, true
		cur.addAnchors(anchors)
		return true
	}, e.seq) {
		s.parts.retryAt("settle:"+d, now.Add(time.Second))
		return outRefused, nil
	}
	s.parts.sealedNow("settle:" + d)
	return outDone, nil
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

// sealBusy is the sealer's least pause between two units of work while
// there is more; with a pace (WithSealPace) it rests pace times as long as
// the unit took, so that its reads leave the disk to whatever shares it.
// sealIdle is how long it rests once there is none, or after a unit that
// only found its day not ready (Server.sealPace sets others, for tests).
const (
	sealBusy = time.Duration(0)
	sealIdle = 30 * time.Second
)

// DefaultSealPace is observer-api's -day-partials-pace: after each unit of
// the sealer's work it rests three times as long as the unit took, so that
// sealing takes at most a quarter of the time the disk has.
const DefaultSealPace = 3.0

// WithSealPace sets the sealer's pace (observer-api -day-partials-pace):
// after each unit of its work, the live sealer's and the warm-up's alike,
// it rests k times as long as the unit took. 0 does not rest.
func WithSealPace(k float64) Option {
	return func(s *Server) {
		if k > 0 {
			s.sealDuty = k
		}
	}
}

// burst is what the sealer did since it last rested: written to the log in
// one line when it rests (sealer).
type burst struct {
	units, deferred, refused int
	done                     map[string]int
	work                     time.Duration
	start                    time.Time
}

func (b *burst) add(u unit, took time.Duration) {
	if b.units == 0 {
		b.start = time.Now()
	}
	b.units++
	b.work += took
	switch u.out {
	case outDone:
		if b.done == nil {
			b.done = map[string]int{}
		}
		b.done[u.kind]++
	case outDeferred:
		b.deferred++
	case outRefused:
		b.refused++
	}
}

func (b *burst) String() string {
	var parts []string
	for _, k := range []struct{ kind, what string }{{"row", "row day(s)"}, {"settle", "settlement day(s)"}, {"ledger", "ledger step(s)"}, {"span", "span(s)"}} {
		if n := b.done[k.kind]; n > 0 {
			parts = append(parts, fmt.Sprintf("%d %s", n, k.what))
		}
	}
	line := "sealed nothing"
	if len(parts) > 0 {
		line = "sealed " + strings.Join(parts, ", ")
	}
	if b.deferred > 0 {
		line += fmt.Sprintf("; %d not ready", b.deferred)
	}
	if b.refused > 0 {
		line += fmt.Sprintf("; %d read again (changed while read)", b.refused)
	}
	return fmt.Sprintf("%s, in %d unit(s), %s working over %s", line, b.units, b.work.Round(time.Millisecond), time.Since(b.start).Round(10*time.Millisecond))
}

// sealer runs until Close: the ledger build, the spans and the seals, one
// unit at a time at its pace, and the hourly audit. Each burst of work ends
// with one line in the log and the partials written (when they moved).
func (s *Server) sealer() {
	every, rest := sealBusy, sealIdle
	if s.sealPace != [2]time.Duration{} {
		every, rest = s.sealPace[0], s.sealPace[1]
	}
	dp := s.parts
	wait := every
	auditAt := s.now().Add(auditEvery)
	windows := auditWindowRuns
	var comparing, over atomic.Bool
	var b burst
	for {
		select {
		case <-s.stop:
			return
		case <-time.After(wait):
		}
		if why := dp.rawFallback(); why != "" {
			// Every window is read raw: nothing is sealed, audited or
			// written, and the files stay as they were for whoever looks.
			wait = rest
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), snapshotTimeout)
		t0 := time.Now()
		u, err := s.sealOne(ctx)
		took := time.Since(t0)
		cancel()
		switch {
		case err != nil:
			// The unit backs off (sealOnce); the sealer goes on with the
			// others after its rest.
			wait = max(rest, time.Duration(s.sealDuty*float64(took)))
		case u.out == outNone || u.out == outDeferred:
			if u.out == outDeferred {
				b.add(u, took)
			}
			wait = max(rest, time.Duration(s.sealDuty*float64(took)))
			if b.units > 0 && u.out == outNone {
				// The burst is over: one line for it, and what it sealed
				// written now, not when the sealer next has work.
				if len(b.done) > 0 || b.refused > 0 {
					dp.logf("day partials: sealer: %s", &b)
				}
				b = burst{}
				dp.wait()
				dp.saveLater(s, true)
			}
		default:
			b.add(u, took)
			wait = max(every, time.Duration(s.sealDuty*float64(took)))
			dp.saveLater(s, false)
		}
		if now := s.now(); !now.Before(auditAt) && !dp.backingOff("audit", now) {
			auditAt = now.Add(auditEvery)
			ctx, cancel := context.WithTimeout(context.Background(), snapshotTimeout)
			res, err := s.auditOnce(ctx)
			cancel()
			dp.noteAudit(res, err, now)
			if err == nil {
				// The window beside the sealer, one at a time, the first few
				// hours only, and none after one that ran over its budget.
				if windows > 0 && !over.Load() && comparing.CompareAndSwap(false, true) {
					windows--
					list := windows%2 == 0
					s.bg.Add(1)
					go func() {
						defer s.bg.Done()
						defer comparing.Store(false)
						ctx, cancel := context.WithTimeout(context.Background(), snapshotTimeout)
						defer cancel()
						took, err := s.auditWindow(ctx, list)
						switch {
						case err != nil:
							dp.logf("day partials: audit of a window: %v", err)
						case dp.rawFallback() == "":
							dp.logf("day partials: audit: a window computed both ways alike (%s)", took.Round(time.Millisecond))
						}
						if took > auditWindowBudget {
							dp.logf("day partials: audit: a window took %s both ways; no more are compared", took.Round(time.Second))
							over.Store(true)
						}
					}()
				}
			}
		}
	}
}

// sealOne is one unit of the live sealer's work, under the sealing lock.
func (s *Server) sealOne(ctx context.Context) (unit, error) {
	s.parts.sealing.Lock()
	defer s.parts.sealing.Unlock()
	return s.sealOnce(ctx)
}

// noteAudit logs an hour's audit in one line, keeps it for /v1/health, and
// backs the audit off when it failed: an hour after its first failure, twice
// as long after each one after it, at most a day.
func (dp *dayParts) noteAudit(res auditResult, err error, now time.Time) {
	switch {
	case err != nil:
		dp.fail("audit", now, auditEvery, 24*time.Hour, err)
	case len(res.days) == 0:
		dp.recover("audit")
		return // nothing sealed to read
	case len(res.diffs) > 0:
		dp.recover("audit")
		dp.logf("day partials: audit: DIFFERS FROM THE STORE (%s): %s; %s", res.took.Round(time.Millisecond), strings.Join(res.diffs, "; "),
			map[bool]string{true: "every window is read raw from now on", false: "the day is dropped and read raw until it is sealed again"}[res.fallback])
	default:
		dp.recover("audit")
		dp.logf("day partials: audit: %s as the store holds them (%s)", strings.Join(res.days, ", "), res.took.Round(time.Millisecond))
	}
	dp.mu.Lock()
	defer dp.mu.Unlock()
	dp.audited = now
	switch {
	case err != nil:
		dp.auditNote = "failed: " + err.Error()
	case len(res.diffs) > 0:
		dp.auditNote = "differs: " + strings.Join(res.diffs, "; ")
	default:
		dp.auditNote = "as the store holds them: " + strings.Join(res.days, ", ")
	}
}

// logf logs through the partials' logger, if there is one.
func (dp *dayParts) logf(format string, args ...any) {
	if dp.log != nil {
		dp.log(format, args...)
	}
}

// ledgerTurn reports whether the ledger's build takes this unit of the
// sealer's work: every other one while the ledger is being built.
func (dp *dayParts) ledgerTurn() bool {
	dp.mu.Lock()
	defer dp.mu.Unlock()
	dp.turn = !dp.turn
	return dp.turn
}
