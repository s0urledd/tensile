package api

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"sort"
	"strings"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/observer/rollup"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
)

// Catching up: every write that can move a partial, found and applied.
//
// The collector writes the store and the API only reads it, so nothing
// tells the API what changed; each computation asks, in its own read
// transaction, before it reads anything else, and drops what the answer
// touches. Every write path, how it is found and what it drops:
//
//	InsertProbe (late readings, backlogs,      probes past the mark      its row day; its promise's settlement
//	  a restarted prober's NOT_PROBED rows,                              day (span widened, seal dropped)
//	  a full reading's later attempt, which
//	  moves how the answers before it count:
//	  the same promise)
//	InsertSampledOut, the collapse's           sampling_decision_points  the points' row days; the settlement
//	  decisions and points                     and _decisions past the   day; the sealed row days whose
//	                                           marks                     collapsible promises it names, and a
//	                                                                     row day being sealed whose do (the
//	                                                                     journal)
//	the collapse's DELETE                      its new decision          the same
//	UpsertPublication with its assignments     publications past the     the ledger adds it; its settlement day
//	  (in the pass, or between passes by the   mark                      (span from its rows, seal dropped); the
//	  collector's fast tick)                                             row days of its decision points
//	ApplyAmendment                             probe_amendments past     the row's row day and settlement day
//	                                           the mark
//	ApplyProbeCorrection                       probe_corrections past    the same, the deadline widened
//	                                           the mark
//	ApplyPublicationCorrection                 publication_corrections   the day's latest deadline read again;
//	                                           past the mark             its seal dropped
//	either of them again under the same        a fingerprint of the      the same; the rows' row days and the
//	  range (the log keeps its first line)     corrected publications    settlement day
//	                                           and of every row a
//	                                           correction wrote
//	ApplySampledOutCorrection (no log)         a fingerprint of every    the points' row days and the
//	                                           decision a correction     settlement day
//	                                           can reach
//	SyncParamHolds, a range's own raise        the holds' counter, moved the rows' row days and settlement day
//	                                           in their transaction; a   of a promise whose held rows moved;
//	                                           digest of every promise's the publication's settlement day
//	                                           held rows, and the held
//	                                           publications, once it
//	                                           moves
//	a row or a publication born held           past the mark             its days, as any; its promise's held
//	                                                                     rows read again
//	the prune of a build before 2026-10-04     raw_from moving; one      the row days before raw_from; the
//	  (probes, heartbeats, decisions; not one  anchor row per table      settlement days whose span reaches a
//	  transaction), on a database it pruned    per row day               pruned day or an anchor that went,
//	                                                                     and a day being sealed that does
//	                                                                     (the journal)
//	InsertReachability (own vantage)           reachability past the     its row day
//	                                           mark
//	a migration that changes a table or view   the definition the        everything: the partials begin again
//	  the partials read, or rewrites rows; a   partials hold; the count
//	  restore                                  of migrations that
//	                                           rewrote rows; the store's
//	                                           creation; the rows at the
//	                                           marks
//
// A table's mark is the newest row the catch-up read, by rowid and by its
// key, and the last sixteen are kept (the ladder). SQLite gives a new row
// the highest rowid plus one, so while a mark's row is there every row
// written since has a greater rowid. A mark whose row has gone is
// explained by the prune (its day is before raw_from), or by the collapse
// (it was a sampled-out row and its decision is there), and the ladder
// steps back to the newest mark still there and folds in everything past
// it again: folding drops and widens, so doing it twice is harmless. A
// mark gone for any other reason means the store is not the one the
// partials were read from (restored, rebuilt, vacuumed), and they begin
// again.

// ladderKeep is how many marks of each table are kept.
const ladderKeep = 16

// ladderTable is a table the catch-up follows by rowid.
type ladderTable struct {
	name string
	// top reads the table's newest row: rowid, key, the day it started
	// ("" for a table the prune never deletes from), and whether a
	// collapse could delete it (its vantage|promise when so).
	top string
	// keyAt reads a row's key by rowid.
	keyAt string
}

// weirdSQL is 1 when a timestamp column holds something other than a
// store timestamp: it then sorts between days. A value of the right shape
// that is no time at all (a minute or a second of 60, a thirteenth month)
// makes strftime NULL, and the test with it; that is weird too, not the 0
// MAX would read a NULL as or the false a WHERE would.
func weirdSQL(col string) string {
	return `COALESCE(NOT (` + col + ` GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]T[0-9][0-9]:[0-9][0-9]:[0-9][0-9].[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]Z'
		AND strftime('%Y-%m-%dT%H:%M:%S', substr(` + col + `, 1, 19)) = substr(` + col + `, 1, 19)), 1)`
}

var ladderTables = []ladderTable{
	{"probes", `SELECT rowid, dedupe_key, substr(started_at, 1, 10),
			CASE WHEN ` + store.SampledOutReasonSQL + ` THEN vantage || '|' || promise_hash ELSE '' END
		FROM probes ORDER BY rowid DESC LIMIT 1`,
		`SELECT dedupe_key FROM probes WHERE rowid = ?`},
	{"sampling_decision_points", `SELECT rowid, vantage || '|' || promise_hash || '|' || scheduled_at, substr(started_at, 1, 10), ''
		FROM sampling_decision_points ORDER BY rowid DESC LIMIT 1`,
		`SELECT vantage || '|' || promise_hash || '|' || scheduled_at FROM sampling_decision_points WHERE rowid = ?`},
	{"sampling_decisions", `SELECT rowid, vantage || '|' || promise_hash, substr(decided_at, 1, 10), ''
		FROM sampling_decisions ORDER BY rowid DESC LIMIT 1`,
		`SELECT vantage || '|' || promise_hash FROM sampling_decisions WHERE rowid = ?`},
	{"publications", `SELECT rowid, promise_hash, '', '' FROM publications ORDER BY rowid DESC LIMIT 1`,
		`SELECT promise_hash FROM publications WHERE rowid = ?`},
	{"assignments", `SELECT rowid, promise_hash || '|' || validator_address, '', '' FROM assignments ORDER BY rowid DESC LIMIT 1`,
		`SELECT promise_hash || '|' || validator_address FROM assignments WHERE rowid = ?`},
	{"reachability", `SELECT rowid, dedupe_key, substr(started_at, 1, 10), '' FROM reachability ORDER BY rowid DESC LIMIT 1`,
		`SELECT dedupe_key FROM reachability WHERE rowid = ?`},
	// An amendment goes when the prune deletes its row (ON DELETE
	// CASCADE): its day is its row's.
	{"probe_amendments", `SELECT a.rowid, a.dedupe_key, COALESCE((SELECT substr(p.started_at, 1, 10) FROM probes p WHERE p.dedupe_key = a.dedupe_key), ''), ''
		FROM probe_amendments a ORDER BY a.rowid DESC LIMIT 1`,
		`SELECT dedupe_key FROM probe_amendments WHERE rowid = ?`},
	{"probe_corrections", `SELECT rowid, dedupe_key || '|' || uncertainty_id, '', '' FROM probe_corrections ORDER BY rowid DESC LIMIT 1`,
		`SELECT dedupe_key || '|' || uncertainty_id FROM probe_corrections WHERE rowid = ?`},
	{"publication_corrections", `SELECT rowid, promise_hash || '|' || uncertainty_id, '', '' FROM publication_corrections ORDER BY rowid DESC LIMIT 1`,
		`SELECT promise_hash || '|' || uncertainty_id FROM publication_corrections WHERE rowid = ?`},
}

// markCol is the collapse key a mark keeps beside its day: "" for a row
// no collapse deletes.
type markTop struct {
	m   mark
	col string
}

// errRegressed is a store that is not the one the partials were read from.
var errRegressed = errors.New("the store is not the one the partials were read from")

// catchUp is one catch-up's work on a copy of the newest epoch.
type catchUp struct {
	s   *Server
	q   store.Querier
	ctx context.Context
	e   *epoch
	now time.Time
	j   journalEntry
	// dropped counts the sealed days this catch-up dropped.
	dropped int
	// heldDirty are the promises whose held rows moved by a write the
	// holds' counter does not count (a row born held, a held row the
	// collapse deleted), and pubsHeld the publications born held: their
	// part of the holds is read again whatever the counter says
	// (readHolds). fullHolds reads every hold again and diffs it: after a
	// load, when the counter may have stood still under a build that did
	// not keep it.
	heldDirty map[string]bool
	pubsHeld  map[string]bool
	fullHolds bool
}

func (c *catchUp) touchRow(d string) {
	if c.e.rows[d] != nil {
		c.dropped++
		c.e.content++
	}
	if _, err := time.Parse(dayLayout, d); err == nil && (c.e.firstRow == "" || d < c.e.firstRow) {
		c.e.firstRow = d
	}
	if c.j.rows == nil {
		c.j.rows = map[string]bool{}
	}
	c.j.rows[d] = true
	delete(c.e.rows, d)
}

// touchSettle drops day d's seal and widens its span by a row's start and
// deadline ("" for none).
func (c *catchUp) touchSettle(d, lo, hi, msu string) {
	if d == "" {
		return
	}
	if c.j.settle == nil {
		c.j.settle = map[string]bool{}
	}
	c.j.settle[d] = true
	sd := c.e.settleMut(d)
	if sd.Seal != nil {
		c.dropped++
		c.e.content++
	}
	sd.Seal = nil
	if lo != "" {
		sd.widen(lo, msu)
		sd.widen(hi, msu)
	} else if msu != "" {
		sd.widen("", msu)
	}
}

// dropSealsReaching drops the seal of every settlement day whose rows
// reach into [lo, hi], and journals the span for a seal being read.
func (c *catchUp) dropSealsReaching(lo, hi string) {
	c.j.reach = append(c.j.reach, [2]string{lo, hi})
	for d, sd := range c.e.settle {
		if sd.Seal != nil && sd.Seal.MinStart != "" && sd.Seal.MinStart <= hi && sd.Seal.MaxStart >= lo {
			c.touchSettle(d, "", "", "")
		}
	}
}

// advance catches the newest epoch up to the store as q sees it and
// publishes it. The caller holds dp.mu, and q's snapshot was taken under
// it.
func (dp *dayParts) advance(ctx context.Context, s *Server, q store.Querier, now time.Time) (*epoch, error) {
	var (
		e   *epoch
		j   journalEntry
		err error
	)
	built := false
	if dp.cur == nil {
		dp.rebuilds++
		e, err = build(ctx, s, q)
		j.all, built = true, true
	} else {
		var dropped int
		e, j, dropped, err = catchUpFrom(ctx, s, q, dp.cur, now, dp.fullHolds)
		dp.dropped += dropped
		if errors.Is(err, errRegressed) {
			if dp.log != nil {
				dp.log("day partials: %v; building them again", err)
			}
			dp.rebuilds++
			e, err = build(ctx, s, q)
			j, built = journalEntry{all: true}, true
		}
	}
	if err != nil {
		return nil, err
	}
	dp.fullHolds = false
	if dp.cur != nil {
		e.seq = dp.cur.seq + 1
	} else {
		e.seq = 1
	}
	if built {
		e.born = e.seq
	}
	j.seq = e.seq
	dp.journal = append(dp.journal, j)
	if len(dp.journal) > journalKeep {
		dp.journal = dp.journal[len(dp.journal)-journalKeep:]
	}
	dp.cur = e
	return e, nil
}

// build begins the partials again from the store as q sees it: the marks
// where the tables are now, the holds and fingerprints as they are, and an
// empty ledger the sealer fills (dayparts_seal.go). Nothing is summed yet,
// so every figure is read raw until the days are sealed.
func build(ctx context.Context, s *Server, q store.Querier) (*epoch, error) {
	e := newEpoch()
	var err error
	if e.store, e.schema, err = readPartsIdentity(ctx, q); err != nil {
		return nil, err
	}
	if e.def, err = partsDefinition(ctx, q); err != nil {
		return nil, err
	}
	if from, ok := rollup.RawFromIn(ctx, q); ok {
		e.rawFrom = from.Format(dayLayout)
	}
	for _, t := range ladderTables {
		top, ok, err := readTop(ctx, q, t)
		if err != nil {
			return nil, err
		}
		if ok {
			e.marks[t.name] = []mark{top.m}
			if top.col != "" {
				e.marks[t.name][0].Key += "\x00" + top.col
			}
		}
		if t.name == "publications" && ok {
			e.ledgerHi = top.m.Rowid
		}
	}
	e.ledgerBuilt = e.ledgerHi == 0
	if first, ok, err := s.rowDayFrom(ctx, q, ""); err != nil {
		return nil, err
	} else if ok {
		e.firstRow = first
	}
	c := &catchUp{s: s, q: q, ctx: ctx, e: e}
	if err := c.readHolds(false); err != nil {
		return nil, err
	}
	if err := c.readCorrections(false); err != nil {
		return nil, err
	}
	if err := c.readFingerprints(false); err != nil {
		return nil, err
	}
	return e, nil
}

// readTop reads a table's newest row as a mark.
func readTop(ctx context.Context, q store.Querier, t ladderTable) (markTop, bool, error) {
	var top markTop
	err := q.QueryRowContext(ctx, t.top).Scan(&top.m.Rowid, &top.m.Key, &top.m.Day, &top.col)
	if errors.Is(err, sql.ErrNoRows) {
		return top, false, nil
	}
	return top, err == nil, err
}

// markKey splits a mark's stored key into the row's key and its collapse
// key.
func markKey(k string) (key, col string) {
	if i := strings.IndexByte(k, 0); i >= 0 {
		return k[:i], k[i+1:]
	}
	return k, ""
}

// catchUpFrom is one catch-up: base, copied, brought to the store as q
// sees it. It returns the new epoch, what it dropped and how many sealed
// days that was. fullHolds reads every hold again (readHolds).
//
// A migration is judged by what it did, not by its number: one that
// changed a table or view the partials read changes the definition read
// from the store (partsDefinition), and one that rewrote rows moves the
// store's count of such migrations (partsIdentity); either begins the
// partials again. One that only added what they do not read (a table, an
// index, a column of a table they do not read) leaves them as they are, so
// an upgrade does not cost a full sealing again in the API still serving
// and in the warm-up beside it at once.
func catchUpFrom(ctx context.Context, s *Server, q store.Querier, base *epoch, now time.Time, fullHolds bool) (*epoch, journalEntry, int, error) {
	c := &catchUp{s: s, q: q, ctx: ctx, e: base.clone(), now: now, fullHolds: fullHolds}
	id, schema, err := readPartsIdentity(ctx, q)
	if err != nil {
		return nil, c.j, 0, err
	}
	if id != c.e.store {
		return nil, c.j, 0, fmt.Errorf("%w: %s", errRegressed, storeChange(c.e.store, id))
	}
	if schema != c.e.schema {
		def, err := partsDefinition(ctx, q)
		if err != nil {
			return nil, c.j, 0, err
		}
		if def != c.e.def {
			return nil, c.j, 0, fmt.Errorf("%w: the store moved from schema version %d to %d, which changed a table the partials read", errRegressed, c.e.schema, schema)
		}
		c.e.schema = schema
	}
	steps := []func() error{c.rawFrom, c.checkAnchors, c.readMSUDue}
	// Publications first: a row folded below finds its publication's day,
	// which must already be in the ledger to be widened.
	for _, t := range ladderTables {
		steps = append(steps, func() error { return c.follow(t) })
	}
	steps = append(steps, func() error { return c.readHolds(true) }, func() error { return c.readCorrections(true) },
		func() error { return c.readFingerprints(true) })
	for _, step := range steps {
		if err := step(); err != nil {
			return nil, c.j, 0, err
		}
	}
	return c.e, c.j, c.dropped, nil
}

// readMSUDue reads again the latest deadline of the days a step of the
// ledger build brought in after a correction moved one (buildLedger).
func (c *catchUp) readMSUDue() error {
	days := c.e.msuDue
	c.e.msuDue = nil
	for d := range days {
		if err := c.readMaxPubMSU(d); err != nil {
			return err
		}
	}
	return nil
}

// rawFrom applies a prune: the row days before raw_from go, and every
// settlement day whose rows reached them loses its seal (its readable
// count and its obligations are read again once it is sealed again).
func (c *catchUp) rawFrom() error {
	var now string
	if from, ok := rollup.RawFromIn(c.ctx, c.q); ok {
		now = from.Format(dayLayout)
	}
	was := c.e.rawFrom
	switch {
	case now == was:
		return nil
	case now < was:
		return fmt.Errorf("%w: raw_from moved back from %s to %s", errRegressed, was, now)
	}
	c.e.rawFrom = now
	for d := range c.e.rows {
		if d < now {
			c.touchRow(d)
		}
	}
	for d := range c.e.anchors {
		if d < now {
			delete(c.e.anchors, d)
		}
	}
	// A start that is not a store timestamp sorts between days, where the
	// prune never deletes it, but no day before raw_from is summed, so only
	// those from the gap before raw_from on are still wanted (gapClean).
	i := sort.SearchStrings(c.e.weird, dayHi(dayAdd(now, -1)))
	c.e.weird = append([]string(nil), c.e.weird[i:]...)
	c.dropSealsReaching("0000", dayHi(dayAdd(now, -1)))
	return nil
}

// checkAnchors looks for every anchor row. One that is gone means its
// table's rows of that day were deleted (the prune deletes a table's day in
// one statement), whatever raw_from says yet.
func (c *catchUp) checkAnchors() error {
	for d, a := range c.e.anchors {
		gone := false
		for _, x := range []struct {
			a     *anchor
			keyAt string
		}{
			{a.Probe, `SELECT dedupe_key FROM probes WHERE rowid = ?`},
			{a.Point, `SELECT vantage || '|' || promise_hash || '|' || scheduled_at FROM sampling_decision_points WHERE rowid = ?`},
			{a.Beat, `SELECT dedupe_key FROM reachability WHERE rowid = ?`},
		} {
			if x.a == nil {
				continue
			}
			var k string
			err := c.q.QueryRowContext(c.ctx, x.keyAt, x.a.Rowid).Scan(&k)
			if errors.Is(err, sql.ErrNoRows) || (err == nil && k != x.a.Key) {
				gone = true
				break
			}
			if err != nil {
				return err
			}
		}
		if !gone {
			continue
		}
		delete(c.e.anchors, d)
		c.touchRow(d)
		c.dropSealsReaching(dayLo(d), dayHi(d))
	}
	return nil
}

// follow folds in a table's rows past its newest intact mark.
func (c *catchUp) follow(t ladderTable) error {
	ladder := c.e.marks[t.name]
	from := int64(0)
	for i := len(ladder) - 1; i >= 0; i-- {
		m := ladder[i]
		key, col := markKey(m.Key)
		var k string
		err := c.q.QueryRowContext(c.ctx, t.keyAt, m.Rowid).Scan(&k)
		if err == nil && k == key {
			from = m.Rowid
			break
		}
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		explained := m.Day != "" && c.e.rawFrom != "" && m.Day <= c.e.rawFrom
		if !explained && col != "" {
			// a sampled-out row the collapse made a decision of
			var n int
			vp := strings.SplitN(col, "|", 2)
			if len(vp) == 2 {
				if err := c.q.QueryRowContext(c.ctx, `SELECT COUNT(*) FROM sampling_decisions WHERE vantage = ? AND promise_hash = ?`, vp[0], vp[1]).Scan(&n); err != nil {
					return err
				}
			}
			explained = n > 0
		}
		if !explained {
			return fmt.Errorf("%w: %s row %d (%s) is gone", errRegressed, t.name, m.Rowid, key)
		}
		if i == 0 {
			from = 0
		}
	}
	top, ok, err := readTop(c.ctx, c.q, t)
	if err != nil || !ok {
		return err
	}
	hi := top.m.Rowid
	if hi > from {
		if err := c.fold(t.name, from, hi); err != nil {
			return fmt.Errorf("fold %s: %w", t.name, err)
		}
	}
	nm := top.m
	if top.col != "" {
		nm.Key += "\x00" + top.col
	}
	if len(ladder) == 0 || ladder[len(ladder)-1] != nm {
		ladder = append(ladder, nm)
		if len(ladder) > ladderKeep {
			ladder = ladder[len(ladder)-ladderKeep:]
		}
	}
	c.e.marks[t.name] = ladder
	return nil
}

// fold applies a table's rows with a rowid in (from, hi].
func (c *catchUp) fold(table string, from, hi int64) error {
	switch table {
	case "publications":
		return c.foldPublications(from, hi)
	case "probes":
		if err := c.foldRows(`SELECT substr(r.started_at, 1, 10), COALESCE(substr(p.settlement_time, 1, 10), ''),
				MIN(r.started_at), MAX(r.started_at), MAX(r.must_serve_until), MAX(`+weirdSQL("r.started_at")+`)
			FROM probes r LEFT JOIN publications p ON p.promise_hash = r.promise_hash
			WHERE r.rowid > ? AND r.rowid <= ? GROUP BY 1, 2`,
			`SELECT DISTINCT started_at FROM probes WHERE rowid > ? AND rowid <= ? AND `+weirdSQL("started_at"), from, hi); err != nil {
			return err
		}
		// A row born held (store.ProbeHeldAtInsert) moves its promise's held
		// rows with no count of the holds' counter.
		return c.readNames(newHeldRowsSQL, &c.heldDirty, from, hi)
	case "sampling_decision_points":
		return c.foldRows(`SELECT substr(pt.started_at, 1, 10), COALESCE(substr(p.settlement_time, 1, 10), ''),
				MIN(pt.started_at), MAX(pt.started_at), COALESCE(MAX(d.must_serve_until), ''), MAX(`+weirdSQL("pt.started_at")+`)
			FROM sampling_decision_points pt
			LEFT JOIN sampling_decisions d ON d.vantage = pt.vantage AND d.promise_hash = pt.promise_hash
			LEFT JOIN publications p ON p.promise_hash = pt.promise_hash
			WHERE pt.rowid > ? AND pt.rowid <= ? GROUP BY 1, 2`,
			`SELECT DISTINCT started_at FROM sampling_decision_points WHERE rowid > ? AND rowid <= ? AND `+weirdSQL("started_at"), from, hi)
	case "sampling_decisions":
		return c.foldDecisions(from, hi)
	case "reachability":
		return c.foldRows(`SELECT substr(started_at, 1, 10), '', '', '', '', MAX(`+weirdSQL("started_at")+`)
			FROM reachability WHERE rowid > ? AND rowid <= ? AND +vantage = ?`+" GROUP BY 1",
			`SELECT DISTINCT started_at FROM reachability WHERE rowid > ? AND rowid <= ? AND +vantage = ? AND `+weirdSQL("started_at"), from, hi, c.s.vantage)
	case "probe_amendments":
		return c.foldRows(`SELECT substr(r.started_at, 1, 10), COALESCE(substr(p.settlement_time, 1, 10), ''), '', '', '', 0
			FROM probe_amendments a JOIN probes r ON r.dedupe_key = a.dedupe_key
			LEFT JOIN publications p ON p.promise_hash = r.promise_hash
			WHERE a.rowid > ? AND a.rowid <= ? GROUP BY 1, 2`, "", from, hi)
	case "probe_corrections":
		return c.foldProbeCorrections(from, hi)
	case "publication_corrections":
		return c.foldPublicationCorrections(from, hi)
	}
	return nil // assignments: they arrive with their publication
}

// foldRows applies rows grouped by (row day, settlement day), each group
// with its start extent, its latest deadline and whether any start is not
// a store timestamp (weird lists those).
func (c *catchUp) foldRows(groups, weird string, args ...any) error {
	rows, err := c.q.QueryContext(c.ctx, groups, args...)
	if err != nil {
		return err
	}
	type g struct {
		rowDay, setDay, lo, hi, msu string
		weird                       bool
	}
	var gs []g
	anyWeird := false
	for rows.Next() {
		var x g
		var w sql.NullInt64
		if err := rows.Scan(&x.rowDay, &x.setDay, &x.lo, &x.hi, &x.msu, &w); err != nil {
			rows.Close()
			return err
		}
		x.weird = w.Int64 == 1
		anyWeird = anyWeird || x.weird
		gs = append(gs, x)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if err := rows.Err(); err != nil {
		return err
	}
	for _, x := range gs {
		c.touchRow(x.rowDay)
		if x.setDay != "" {
			c.touchSettle(x.setDay, x.lo, x.hi, x.msu)
		}
	}
	if anyWeird && weird != "" {
		wr, err := c.q.QueryContext(c.ctx, weird, args...)
		if err != nil {
			return err
		}
		defer wr.Close()
		for wr.Next() {
			var v string
			if err := wr.Scan(&v); err != nil {
				return err
			}
			c.e.addWeird(v)
		}
		return wr.Err()
	}
	return nil
}

// foldDecisions applies new decisions: a collapse moves the rows of its
// promise, so the sealed row days that held them and its settlement day go.
func (c *catchUp) foldDecisions(from, hi int64) error {
	rows, err := c.q.QueryContext(c.ctx, `SELECT d.promise_hash, COALESCE(substr(p.settlement_time, 1, 10), '')
		FROM sampling_decisions d LEFT JOIN publications p ON p.promise_hash = d.promise_hash
		WHERE d.rowid > ? AND d.rowid <= ?`, from, hi)
	if err != nil {
		return err
	}
	promises := map[string]bool{}
	var days []string
	for rows.Next() {
		var h, d string
		if err := rows.Scan(&h, &d); err != nil {
			rows.Close()
			return err
		}
		promises[h] = true
		days = append(days, d)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if err := rows.Err(); err != nil {
		return err
	}
	for d, rd := range c.e.rows {
		for _, h := range rd.Collapsible {
			if promises[h] {
				c.touchRow(d)
				break
			}
		}
	}
	// A day being sealed is not among them: its seal is matched against the
	// promises when it is published (sealRow). And the rows the collapse
	// deleted may have been held.
	if len(promises) > 0 {
		if c.j.collapsed == nil {
			c.j.collapsed = map[string]bool{}
		}
		if c.heldDirty == nil {
			c.heldDirty = map[string]bool{}
		}
		for h := range promises {
			c.j.collapsed[h] = true
			c.heldDirty[h] = true
		}
	}
	for _, d := range days {
		c.touchSettle(d, "", "", "")
	}
	// A new decision of a publication a correction can reach is
	// fingerprinted from now on (readFingerprints), whether or not the
	// publication had one before.
	if len(promises) == 0 {
		return nil
	}
	list := make([]string, 0, len(promises))
	for h := range promises {
		list = append(list, h)
	}
	sort.Strings(list)
	b, _ := json.Marshal(list)
	rr, err := c.q.QueryContext(c.ctx, reachableSQL, string(b))
	if err != nil {
		return err
	}
	defer rr.Close()
	for rr.Next() {
		var h string
		if err := rr.Scan(&h); err != nil {
			return err
		}
		c.e.reach[h] = true
	}
	return rr.Err()
}

// foldPublicationCorrections reads again the latest deadline of every day
// a corrected publication settled on.
func (c *catchUp) foldPublicationCorrections(from, hi int64) error {
	rows, err := c.q.QueryContext(c.ctx, `SELECT DISTINCT substr(p.settlement_time, 1, 10)
		FROM publication_corrections k JOIN publications p ON p.promise_hash = k.promise_hash
		WHERE k.rowid > ? AND k.rowid <= ?`, from, hi)
	if err != nil {
		return err
	}
	var days []string
	for rows.Next() {
		var d string
		if err := rows.Scan(&d); err != nil {
			rows.Close()
			return err
		}
		days = append(days, d)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	for _, d := range days {
		if err := c.readMaxPubMSU(d); err != nil {
			return err
		}
	}
	return nil
}

// readMaxPubMSU reads day d's latest publication deadline again, as a
// correction may have moved one earlier.
func (c *catchUp) readMaxPubMSU(d string) error {
	c.touchSettle(d, "", "", "")
	sd := c.e.settle[d]
	if sd == nil || sd.Pubs == 0 {
		// A step of the ledger build that read the day before this may
		// bring it in after: it has the next catch-up read it then.
		if !c.e.ledgerBuilt {
			if c.j.msu == nil {
				c.j.msu = map[string]bool{}
			}
			c.j.msu[d] = true
		}
		return nil
	}
	var msu sql.NullString
	if err := c.q.QueryRowContext(c.ctx, dayMaxPubMSUSQL, dayLo(d), dayHi(d), sd.HLo, sd.HHi).Scan(&msu); err != nil {
		return err
	}
	sd.MaxPubMSU = msu.String
	return nil
}

// ---- the ledger ----

// ledgerDelta is what a run of publications adds to the ledger.
type ledgerDelta struct {
	days  map[string]*settleDay
	spans map[string][3]string
	// pointDays are the start days of their decision points.
	pointDays map[string]bool
	pubs      []pubHeights
	odd       bool
}

type pubHeights struct {
	promise                     string
	promiseHeight, settleHeight int64
}

// readLedger reads what the publications with a rowid in (from, hi] add to
// the ledger; withRows also reads their rows' span, for publications that
// arrive after the ledger began (the build's publications have their
// spans computed whole).
func (s *Server) readLedger(ctx context.Context, q store.Querier, from, hi int64, withRows bool) (*ledgerDelta, error) {
	dl := &ledgerDelta{days: map[string]*settleDay{}, spans: map[string][3]string{}, pointDays: map[string]bool{}}
	day := func(d string) *settleDay {
		sd, ok := dl.days[d]
		if !ok {
			sd = &settleDay{Signing: map[string]sigPart{}, Load: map[string]*loadPart{}}
			dl.days[d] = sd
		}
		return sd
	}
	rows, err := q.QueryContext(ctx, ledgerPubsSQL, from, hi)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var h, st, msu string
		var height, promiseHeight, size int64
		if err := rows.Scan(&h, &st, &height, &promiseHeight, &size, &msu); err != nil {
			rows.Close()
			return nil, err
		}
		if !isTS(st) {
			dl.odd = true
			continue
		}
		sd := day(st[:10])
		if sd.Pubs == 0 || height < sd.HLo {
			sd.HLo = height
		}
		if height > sd.HHi {
			sd.HHi = height
		}
		sd.Pubs++
		sd.Bytes += size
		sd.MaxPubMSU = maxString(sd.MaxPubMSU, msu)
		dl.pubs = append(dl.pubs, pubHeights{h, promiseHeight, height})
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	rows, err = q.QueryContext(ctx, ledgerSigningSQL, from, hi)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var d, addr string
		var t sigPart
		if err := rows.Scan(&d, &addr, &t[0], &t[1], &t[2], &t[3]); err != nil {
			rows.Close()
			return nil, err
		}
		sd := day(d)
		c := sd.Signing[addr]
		for i := range c {
			c[i] += t[i]
		}
		sd.Signing[addr] = c
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	rows, err = q.QueryContext(ctx, ledgerLoadSQL, from, hi)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var d, addr string
		var orig any
		var t loadTerms
		if err := rows.Scan(&d, &addr, &orig, &t.n, &t.rows, &t.hi, &t.lo, &t.maxTerm, &t.maxSize); err != nil {
			rows.Close()
			return nil, err
		}
		sd := day(d)
		l, ok := sd.Load[addr]
		if !ok {
			l = &loadPart{Bytes: map[int]*big.Int{}}
			sd.Load[addr] = l
		}
		l.add(t.part(orig))
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	if withRows {
		rows, err = q.QueryContext(ctx, ledgerRowsSpanSQL, from, hi)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var d string
			var lo, hi, msu sql.NullString
			if err := rows.Scan(&d, &lo, &hi, &msu); err != nil {
				rows.Close()
				return nil, err
			}
			dl.spans[d] = [3]string{lo.String, hi.String, msu.String}
		}
		if err := rows.Close(); err != nil {
			return nil, err
		}
		rows, err = q.QueryContext(ctx, ledgerPointDaysSQL, from, hi)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var st string
			if err := rows.Scan(&st); err != nil {
				rows.Close()
				return nil, err
			}
			if len(st) >= 10 {
				dl.pointDays[st[:10]] = true
			}
		}
		if err := rows.Close(); err != nil {
			return nil, err
		}
	}
	return dl, nil
}

// applyLedger adds a delta to the epoch's ledger.
func (e *epoch) applyLedger(dl *ledgerDelta) {
	if dl.odd {
		e.pubsOdd = true
	}
	if dl.odd || len(dl.days) > 0 {
		e.content++
	}
	for d, add := range dl.days {
		sd := e.settleMut(d)
		if add.Pubs > 0 {
			if sd.Pubs == 0 || add.HLo < sd.HLo {
				sd.HLo = add.HLo
			}
			if add.HHi > sd.HHi {
				sd.HHi = add.HHi
			}
		}
		sd.Pubs += add.Pubs
		sd.Bytes += add.Bytes
		sd.MaxPubMSU = maxString(sd.MaxPubMSU, add.MaxPubMSU)
		for addr, t := range add.Signing {
			c := sd.Signing[addr]
			for i := range c {
				c[i] += t[i]
			}
			sd.Signing[addr] = c
		}
		for addr, l := range add.Load {
			if cur, ok := sd.Load[addr]; ok {
				cur = cur.clone()
				cur.add(l)
				sd.Load[addr] = cur
			} else {
				sd.Load[addr] = l
			}
		}
	}
}

// foldPublications folds new publications into the ledger: their days'
// seals go, their rows widen their days' spans, and the rows their
// decisions stand for appear on the days of their points.
func (c *catchUp) foldPublications(from, hi int64) error {
	dl, err := c.s.readLedger(c.ctx, c.q, from, hi, true)
	if err != nil {
		return err
	}
	c.e.applyLedger(dl)
	for d := range dl.days {
		sp := dl.spans[d]
		c.touchSettle(d, sp[0], sp[1], sp[2])
	}
	for d := range dl.pointDays {
		c.touchRow(d)
	}
	// A publication stored held: the held publications have it from now on
	// (readHolds), with no count of the holds' counter.
	if err := c.readNames(newHeldPubsSQL, &c.pubsHeld, from, hi); err != nil {
		return err
	}
	return c.coverNew(dl.pubs)
}

// readNames adds the first column of every row of query to *set.
func (c *catchUp) readNames(query string, set *map[string]bool, args ...any) error {
	rows, err := c.q.QueryContext(c.ctx, query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var h string
		if err := rows.Scan(&h); err != nil {
			return err
		}
		if *set == nil {
			*set = map[string]bool{}
		}
		(*set)[h] = true
	}
	return rows.Err()
}

// ---- holds and fingerprints ----

// The holds. A row held or released changes the class it counts as, and a
// publication held or released its settlement day's figures, so every
// catch-up asks whether either moved. What SyncParamHolds and a range's own
// raise change they change in place, with no log, but in the transaction
// that changes the flags they bump a counter of their own
// (store.MetaHeldFlagsRev), so a catch-up that finds it where the last one
// left it knows that no flag of a stored row moved since, at the cost of one
// row read. When it moved, every held row is read again, a digest of each
// promise's held rowids, and every held publication (heldByPromiseSQL,
// heldPubsSQL, a covering walk of probes_held and of publications_held), and
// the promises whose digest or hold moved drop their rows' days and their
// settlement days: exactly, whatever moved (an aggregate of the rowids,
// read before, left a hold moved from some rows to as many others with the
// same sums unseen).
//
// What moves the held rows with no count of the counter is a write the
// catch-up meets anyway: a row or a publication born held (it is past its
// table's mark), a held row the collapse deleted (its decision is). Their
// promises' part of the holds is read again then (heldDirty, pubsHeld), so
// what the epoch keeps is always what the store holds and a later move is
// never measured from a stale picture. After a load every hold is read
// again and diffed, whatever the counter says: a build that does not keep
// the counter may have moved the flags meanwhile (a rollback's collector).

// readHolds reads the holds, and with diff drops what moved.
func (c *catchUp) readHolds(diff bool) error {
	var rev string
	if err := c.q.QueryRowContext(c.ctx, metaValueSQL, store.MetaHeldFlagsRev).Scan(&rev); err != nil {
		return err
	}
	if diff && !c.fullHolds && rev == c.e.heldRev {
		return c.readDirtyHolds()
	}
	per := map[string]string{}
	rows, err := c.q.QueryContext(c.ctx, heldByPromiseSQL)
	if err != nil {
		return err
	}
	for rows.Next() {
		var h, ids string
		if err := rows.Scan(&h, &ids); err != nil {
			rows.Close()
			return err
		}
		per[h] = heldDigest(ids)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	var moved []string
	for h, d := range per {
		if c.e.heldProm[h] != d {
			moved = append(moved, h)
		}
	}
	for h := range c.e.heldProm {
		if _, ok := per[h]; !ok {
			moved = append(moved, h)
		}
	}
	pubs := map[string]bool{}
	if err := c.readNames(heldPubsSQL, &pubs); err != nil {
		return err
	}
	promises := map[string]bool{}
	for h := range pubs {
		if !c.e.heldPubs[h] {
			promises[h] = true
		}
	}
	for h := range c.e.heldPubs {
		if !pubs[h] {
			promises[h] = true
		}
	}
	c.e.heldRev, c.e.heldProm, c.e.heldPubs = rev, per, pubs
	if !diff {
		return nil
	}
	if len(moved) > 0 {
		sort.Strings(moved)
		days, err := c.promiseRowDays(moved)
		if err != nil {
			return err
		}
		for _, d := range days {
			c.touchRow(d)
		}
		for _, h := range moved {
			promises[h] = true
		}
	}
	return c.touchPromises(promises, nil)
}

// readDirtyHolds reads again the held rows of the promises a write the
// counter does not count reached (heldDirty), and adds the publications
// stored held (pubsHeld), with nothing dropped: those writes dropped what
// they touched as they were folded.
func (c *catchUp) readDirtyHolds() error {
	if len(c.pubsHeld) > 0 {
		pubs := copySet(c.e.heldPubs)
		for h := range c.pubsHeld {
			pubs[h] = true
		}
		c.e.heldPubs = pubs
	}
	if len(c.heldDirty) == 0 {
		return nil
	}
	list := make([]string, 0, len(c.heldDirty))
	for h := range c.heldDirty {
		list = append(list, h)
	}
	sort.Strings(list)
	b, _ := json.Marshal(list)
	rows, err := c.q.QueryContext(c.ctx, heldOfPromisesSQL, string(b))
	if err != nil {
		return err
	}
	defer rows.Close()
	per := make(map[string]string, len(c.e.heldProm)+len(list))
	for h, d := range c.e.heldProm {
		if !c.heldDirty[h] {
			per[h] = d
		}
	}
	for rows.Next() {
		var h, ids string
		if err := rows.Scan(&h, &ids); err != nil {
			return err
		}
		per[h] = heldDigest(ids)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	c.e.heldProm = per
	return nil
}

// heldDigest is the digest of a promise's held rowids as heldByPromiseSQL
// lists them.
func heldDigest(ids string) string {
	sum := sha256.Sum256([]byte(ids))
	return hex.EncodeToString(sum[:])
}

// promiseRowDays is the days the probe rows of the promises started on.
func (c *catchUp) promiseRowDays(promises []string) ([]string, error) {
	b, _ := json.Marshal(promises)
	dr, err := c.q.QueryContext(c.ctx, promiseRowDaysSQL, string(b))
	if err != nil {
		return nil, err
	}
	defer dr.Close()
	var days []string
	for dr.Next() {
		var d string
		if err := dr.Scan(&d); err != nil {
			return nil, err
		}
		days = append(days, d)
	}
	return days, dr.Err()
}

// touchPromises drops the settlement day of every promise named.
func (c *catchUp) touchPromises(promises map[string]bool, msu map[string]string) error {
	if len(promises) == 0 {
		return nil
	}
	list := make([]string, 0, len(promises))
	for h := range promises {
		list = append(list, h)
	}
	b, _ := json.Marshal(list)
	rows, err := c.q.QueryContext(c.ctx, `SELECT p.promise_hash, substr(p.settlement_time, 1, 10) FROM json_each(?) j
		CROSS JOIN publications p ON p.promise_hash = j.value`, string(b))
	if err != nil {
		return err
	}
	type hd struct{ h, d string }
	var hds []hd
	for rows.Next() {
		var x hd
		if err := rows.Scan(&x.h, &x.d); err != nil {
			rows.Close()
			return err
		}
		hds = append(hds, x)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	for _, x := range hds {
		c.touchSettle(x.d, "", "", msu[x.h])
	}
	return nil
}

// A correction applied again under the range it was first applied under
// leaves no trace in either log. ApplyPublicationCorrection and
// ApplyProbeCorrection always rewrite their row, but the log keeps the first
// line of a (target, range) pair (ON CONFLICT DO NOTHING), so nothing new
// lands past the logs' marks. The corrector does it: its range pass runs
// again on every pass while a verified range stays open, and applies a
// publication's correction again whenever the deadline it recomputes has
// moved (params_history grew); its sweep then grades the publication's rows
// again under that publication's latest range, which is the same one. What
// every apply does rewrite is corrected_at, with the deadline, the phase and
// the class it sets, so each catch-up reads those of what a correction has
// written: the corrected publications (publications_corrected, the few a
// params range ever moved), and the corrected rows of every promise with
// one still in the store (a probes_promise seek each). A row is corrected
// a first time only with a line in probe_corrections, so the promises are
// taken from the log as its lines arrive, and dropped once the prune has
// taken their rows. A publication that moved has its day's latest deadline
// read again; rows that moved drop their row days and their settlement day.

// readCorrections reads the corrected publications and rows, and with diff
// drops what moved. Without it (a build), the promises are read from the
// whole log first.
func (c *catchUp) readCorrections(diff bool) error {
	rows, err := c.q.QueryContext(c.ctx, correctedPubsSQL)
	if err != nil {
		return err
	}
	pubs, days := map[string]string{}, map[string]string{}
	for rows.Next() {
		var h, msu, at, day string
		if err := rows.Scan(&h, &msu, &at, &day); err != nil {
			rows.Close()
			return err
		}
		pubs[h], days[h] = msu+"|"+at, day
	}
	if err := rows.Close(); err != nil {
		return err
	}
	for h, fp := range pubs {
		old, had := c.e.corrPub[h]
		if had && old == fp {
			continue
		}
		if !had {
			c.e.reach[h] = true // its decisions, if it has any, are fingerprinted too
		}
		if diff && days[h] != "" {
			if err := c.readMaxPubMSU(days[h]); err != nil {
				return err
			}
		}
	}
	c.e.corrPub = pubs

	if !diff {
		lr, err := c.q.QueryContext(c.ctx, correctedPromisesSQL)
		if err != nil {
			return err
		}
		for lr.Next() {
			var h string
			if err := lr.Scan(&h); err != nil {
				lr.Close()
				return err
			}
			c.e.corrRows[h] = ""
		}
		if err := lr.Close(); err != nil {
			return err
		}
	}
	if len(c.e.corrRows) == 0 {
		return nil
	}
	list := make([]string, 0, len(c.e.corrRows))
	for h := range c.e.corrRows {
		list = append(list, h)
	}
	sort.Strings(list)
	b, _ := json.Marshal(list)
	rr, err := c.q.QueryContext(c.ctx, correctedRowsSQL, string(b))
	if err != nil {
		return err
	}
	// A promise with no corrected row left (the prune took them) is dropped:
	// a line of the log arriving for it takes it back.
	corrRows := make(map[string]string, len(list))
	var moved []string
	type settleOf struct{ day, msu string }
	settles := map[string]settleOf{}
	for rr.Next() {
		var h, text, msu, settle string
		if err := rr.Scan(&h, &text, &msu, &settle); err != nil {
			rr.Close()
			return err
		}
		sum := sha256.Sum256([]byte(text))
		d := hex.EncodeToString(sum[:])
		corrRows[h] = d
		if diff && c.e.corrRows[h] != d {
			moved = append(moved, h)
			settles[h] = settleOf{settle, msu}
		}
	}
	if err := rr.Close(); err != nil {
		return err
	}
	c.e.corrRows = corrRows
	if len(moved) == 0 {
		return nil
	}
	// The rows' days, of the promises whose corrected rows moved.
	sort.Strings(moved)
	mb, _ := json.Marshal(moved)
	dr, err := c.q.QueryContext(c.ctx, promiseRowDaysSQL, string(mb))
	if err != nil {
		return err
	}
	var rowDays []string
	for dr.Next() {
		var d string
		if err := dr.Scan(&d); err != nil {
			dr.Close()
			return err
		}
		rowDays = append(rowDays, d)
	}
	if err := dr.Close(); err != nil {
		return err
	}
	for _, d := range rowDays {
		c.touchRow(d)
	}
	for _, h := range moved {
		c.touchSettle(settles[h].day, "", "", settles[h].msu)
	}
	return nil
}

// foldProbeCorrections applies new lines of the row corrections' log: the
// rows' days and settlement days, and their promises are followed from now
// on (readCorrections).
func (c *catchUp) foldProbeCorrections(from, hi int64) error {
	if err := c.foldRows(`SELECT substr(r.started_at, 1, 10), COALESCE(substr(p.settlement_time, 1, 10), ''), '', '', MAX(r.must_serve_until), 0
			FROM probe_corrections k JOIN probes r ON r.dedupe_key = k.dedupe_key
			LEFT JOIN publications p ON p.promise_hash = r.promise_hash
			WHERE k.rowid > ? AND k.rowid <= ? GROUP BY 1, 2`, "", from, hi); err != nil {
		return err
	}
	rows, err := c.q.QueryContext(c.ctx, newCorrectedPromisesSQL, from, hi)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var h string
		if err := rows.Scan(&h); err != nil {
			return err
		}
		if _, ok := c.e.corrRows[h]; !ok {
			c.e.corrRows[h] = ""
		}
	}
	return rows.Err()
}

// A sampled-out correction (store.ApplySampledOutCorrection) moves a
// decision's points and deadline and writes no log: the corrector's line
// is in corrections.jsonl, which the API does not read, and the snapshot
// revision is bumped only when a pass applied some and failed none. The
// decisions it can reach are few and known: those of the publications a
// correction moved (publications_corrected) and of the publications a
// verified params range covers (the corrector's own range pass). Each
// catch-up fingerprints those decisions, and one whose fingerprint moved
// drops the days of its points and its settlement day.
//
// Publications are never deleted, but their decisions are, with the prune,
// so the publications fingerprinted (reach) are only those that had a
// decision at the last catch-up: one left with none is dropped from them,
// and taken back if a decision of it arrives (foldDecisions), as a
// publication newly covered or corrected is added. What a catch-up reads
// for them is what the store still holds of their decisions, not every
// publication a correction ever reached.

// coverNew adds to reach the new publications a verified range covers.
func (c *catchUp) coverNew(pubs []pubHeights) error {
	if len(pubs) == 0 {
		return nil
	}
	ranges, err := c.verifiedRanges()
	if err != nil {
		return err
	}
	for _, p := range pubs {
		for _, r := range ranges {
			if p.promiseHeight-1 <= r[1] && p.settleHeight >= r[0] {
				c.e.reach[p.promise] = true
			}
		}
	}
	return nil
}

func (c *catchUp) verifiedRanges() ([][2]int64, error) {
	rows, err := c.q.QueryContext(c.ctx, `SELECT from_height, to_height FROM param_uncertainty WHERE resolution = 'verified'`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out [][2]int64
	for rows.Next() {
		var r [2]int64
		if err := rows.Scan(&r[0], &r[1]); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// readFingerprints adds to reach what the verified ranges new since cover,
// reads the fingerprint of every decision of reach, drops from reach what
// has none, and with diff drops what moved.
func (c *catchUp) readFingerprints(diff bool) error {
	rows, err := c.q.QueryContext(c.ctx, `SELECT id, from_height, to_height FROM param_uncertainty WHERE resolution = 'verified'`)
	if err != nil {
		return err
	}
	type rng struct {
		id       string
		from, to int64
	}
	var fresh []rng
	for rows.Next() {
		var r rng
		if err := rows.Scan(&r.id, &r.from, &r.to); err != nil {
			rows.Close()
			return err
		}
		if !c.e.verified[r.id] {
			fresh = append(fresh, r)
		}
	}
	if err := rows.Close(); err != nil {
		return err
	}
	for _, r := range fresh {
		pr, err := c.q.QueryContext(c.ctx, `SELECT promise_hash FROM publications WHERE settlement_height >= ? AND promise_height - 1 <= ?`, r.from, r.to)
		if err != nil {
			return err
		}
		for pr.Next() {
			var h string
			if err := pr.Scan(&h); err != nil {
				pr.Close()
				return err
			}
			c.e.reach[h] = true
		}
		if err := pr.Close(); err != nil {
			return err
		}
		c.e.verified[r.id] = true
	}
	universe := c.e.reach
	fps := map[string]string{}
	msu := map[string]string{}
	pointDays := map[string]map[string]bool{}
	if len(universe) > 0 {
		list := make([]string, 0, len(universe))
		for h := range universe {
			list = append(list, h)
		}
		sort.Strings(list)
		b, _ := json.Marshal(list)
		fr, err := c.q.QueryContext(c.ctx, `SELECT d.promise_hash, d.vantage, d.must_serve_until, pt.scheduled_at, pt.phase, pt.started_at
			FROM json_each(?) j CROSS JOIN sampling_decisions d ON d.promise_hash = j.value
			JOIN sampling_decision_points pt ON pt.vantage = d.vantage AND pt.promise_hash = d.promise_hash
			ORDER BY d.promise_hash, d.vantage, pt.scheduled_at`, string(b))
		if err != nil {
			return err
		}
		hs := map[string][]string{}
		for fr.Next() {
			var h, v, m, at, ph, st string
			if err := fr.Scan(&h, &v, &m, &at, &ph, &st); err != nil {
				fr.Close()
				return err
			}
			hs[h] = append(hs[h], v, m, at, ph)
			msu[h] = maxString(msu[h], m)
			if pointDays[h] == nil {
				pointDays[h] = map[string]bool{}
			}
			if len(st) >= 10 {
				pointDays[h][st[:10]] = true
			}
		}
		if err := fr.Close(); err != nil {
			return err
		}
		for h, parts := range hs {
			sum := sha256.Sum256([]byte(strings.Join(parts, "\x00")))
			fps[h] = hex.EncodeToString(sum[:])
		}
	}
	// A publication with no decision left has nothing a correction could
	// move unseen; a decision of it arriving takes it back (foldDecisions).
	for h := range c.e.reach {
		if _, ok := fps[h]; !ok {
			delete(c.e.reach, h)
		}
	}
	moved := map[string]bool{}
	for h, f := range fps {
		if c.e.fps[h] != f {
			moved[h] = true
		}
	}
	for h := range c.e.fps {
		if _, ok := fps[h]; !ok {
			moved[h] = true
		}
	}
	c.e.fps = fps
	if !diff || len(moved) == 0 {
		return nil
	}
	for h := range moved {
		for d := range pointDays[h] {
			c.touchRow(d)
		}
	}
	return c.touchPromises(moved, msu)
}
