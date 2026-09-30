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
	"strconv"
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
//	InsertProbe (late readings, backlogs,     probes past the mark      its row day; its promise's settlement
//	  a restarted prober's NOT_PROBED rows)                             day (span widened, seal dropped)
//	InsertSampledOut, the collapse's           sampling_decision_points  the points' row days; the settlement
//	  decisions and points                     and _decisions past the   day; the sealed row days whose
//	                                           marks                     collapsible promises it names
//	the collapse's DELETE                      its new decision          the same
//	UpsertPublication with its assignments     publications past the     the ledger adds it; its settlement day
//	                                           mark                      (span from its rows, seal dropped); the
//	                                                                     row days of its decision points
//	ApplyAmendment                             probe_amendments past     the row's row day and settlement day
//	                                           the mark
//	ApplyProbeCorrection                       probe_corrections past    the same, the deadline widened
//	                                           the mark
//	ApplyPublicationCorrection                 publication_corrections   the day's latest deadline read again;
//	                                           past the mark             its seal dropped
//	either of them again under the same        a fingerprint of the      the same; the rows' row days and the
//	  range (the log keeps its first line)     corrected publications    settlement day
//	                                           and their rows
//	ApplySampledOutCorrection (no log)         a fingerprint of every    the points' row days and the
//	                                           decision a correction     settlement day
//	                                           can reach
//	SyncParamHolds, a range's own raise,       the held rows (rowid,     the row's row day and settlement day;
//	  a row born held                          promise) and held         the publication's settlement day
//	                                           publications, diffed
//	the prune (probes, heartbeats,             raw_from moving; one      the row days before raw_from; the
//	  decisions; not one transaction)          anchor row per table      settlement days whose span reaches a
//	                                           per row day               pruned day or an anchor that went
//	InsertReachability (own vantage)           reachability past the     its row day
//	                                           mark
//	a migration, a restore                     the store's identity and  everything: the partials begin again
//	                                           the rows at the marks
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
// store timestamp: it then sorts between days.
func weirdSQL(col string) string {
	return `(NOT (` + col + ` GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]T[0-9][0-9]:[0-9][0-9]:[0-9][0-9].[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]Z'
		AND strftime('%Y-%m-%dT%H:%M:%S', substr(` + col + `, 1, 19)) = substr(` + col + `, 1, 19)))`
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
}

func (c *catchUp) touchRow(d string) {
	if c.e.rows[d] != nil {
		c.dropped++
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
// reach into [lo, hi].
func (c *catchUp) dropSealsReaching(lo, hi string) {
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
	if dp.cur == nil {
		e, err = dp.build(ctx, s, q)
		j.all = true
	} else {
		var dropped int
		e, j, dropped, err = dp.catchUp(ctx, s, q, now)
		dp.dropped += dropped
		if errors.Is(err, errRegressed) {
			if dp.log != nil {
				dp.log("day partials: %v; building them again", err)
			}
			e, err = dp.build(ctx, s, q)
			j = journalEntry{all: true}
		}
	}
	if err != nil {
		return nil, err
	}
	if dp.cur != nil {
		e.seq = dp.cur.seq + 1
	} else {
		e.seq = 1
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
func (dp *dayParts) build(ctx context.Context, s *Server, q store.Querier) (*epoch, error) {
	dp.rebuilds++
	e := newEpoch()
	var err error
	if e.store, err = readStoreIdentity(ctx, q); err != nil {
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
	var first sql.NullString
	if err := q.QueryRowContext(ctx, `SELECT MIN(t) FROM (SELECT MIN(started_at) AS t FROM probes
		UNION ALL SELECT MIN(started_at) FROM sampling_decision_points UNION ALL SELECT MIN(started_at) FROM reachability)`).Scan(&first); err != nil {
		return nil, err
	}
	if len(first.String) >= 10 {
		if _, err := time.Parse(dayLayout, first.String[:10]); err == nil {
			e.firstRow = first.String[:10]
		}
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

// catchUp is one catch-up: dp.cur, copied, brought to the store as q sees
// it.
func (dp *dayParts) catchUp(ctx context.Context, s *Server, q store.Querier, now time.Time) (*epoch, journalEntry, int, error) {
	c := &catchUp{s: s, q: q, ctx: ctx, e: dp.cur.clone(), now: now}
	id, err := readStoreIdentity(ctx, q)
	if err != nil {
		return nil, c.j, 0, err
	}
	if id != c.e.store {
		return nil, c.j, 0, fmt.Errorf("%w: %s", errRegressed, storeChange(c.e.store, id))
	}
	steps := []func() error{c.rawFrom, c.checkAnchors}
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
		return c.foldRows(`SELECT substr(r.started_at, 1, 10), COALESCE(substr(p.settlement_time, 1, 10), ''),
				MIN(r.started_at), MAX(r.started_at), MAX(r.must_serve_until), MAX(`+weirdSQL("r.started_at")+`)
			FROM probes r LEFT JOIN publications p ON p.promise_hash = r.promise_hash
			WHERE r.rowid > ? AND r.rowid <= ? GROUP BY 1, 2`,
			`SELECT DISTINCT started_at FROM probes WHERE rowid > ? AND rowid <= ? AND `+weirdSQL("started_at"), from, hi)
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
		return c.foldRows(`SELECT substr(r.started_at, 1, 10), COALESCE(substr(p.settlement_time, 1, 10), ''), '', '', MAX(r.must_serve_until), 0
			FROM probe_corrections k JOIN probes r ON r.dedupe_key = k.dedupe_key
			LEFT JOIN publications p ON p.promise_hash = r.promise_hash
			WHERE k.rowid > ? AND k.rowid <= ? GROUP BY 1, 2`, "", from, hi)
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
	for _, d := range days {
		c.touchSettle(d, "", "", "")
	}
	return nil
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
	doc, err := s.origRows.docWhere(ctx, s.st.DB(), `p.rowid > ? AND p.rowid <= ? AND p.settlement_tx_code = 0 AND p.assignment_error = ''`, from, hi)
	if err != nil {
		return nil, err
	}
	rows, err = q.QueryContext(ctx, ledgerLoadSQL, from, hi, doc)
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
	return c.coverNew(dl.pubs)
}

// ---- holds and fingerprints ----

// readHolds reads the held rows and held publications, and with diff drops
// what moved since the last catch-up: a row held or released changes the
// class it counts as.
func (c *catchUp) readHolds(diff bool) error {
	held := map[int64]string{}
	rows, err := c.q.QueryContext(c.ctx, `SELECT rowid, promise_hash FROM probes WHERE retention_unverified = 1`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var id int64
		var h string
		if err := rows.Scan(&id, &h); err != nil {
			rows.Close()
			return err
		}
		held[id] = h
	}
	if err := rows.Close(); err != nil {
		return err
	}
	var added []int64
	for id, h := range held {
		if old, ok := c.e.held[id]; !ok || old.Promise != h {
			added = append(added, id)
		}
	}
	promises := map[string]bool{}
	for id, old := range c.e.held {
		if h, ok := held[id]; !ok || h != old.Promise {
			if diff {
				c.touchRow(old.Day)
			}
			promises[old.Promise] = true
			delete(c.e.held, id)
		}
	}
	if len(added) > 0 {
		list, _ := json.Marshal(added)
		ar, err := c.q.QueryContext(c.ctx, `SELECT r.rowid, r.started_at, r.promise_hash FROM json_each(?) j CROSS JOIN probes r ON r.rowid = j.value`, string(list))
		if err != nil {
			return err
		}
		for ar.Next() {
			var id int64
			var st, h string
			if err := ar.Scan(&id, &st, &h); err != nil {
				ar.Close()
				return err
			}
			d := st
			if len(d) >= 10 {
				d = d[:10]
			}
			c.e.held[id] = heldRow{Promise: h, Day: d}
			if diff {
				c.touchRow(d)
			}
			promises[h] = true
		}
		if err := ar.Close(); err != nil {
			return err
		}
	}
	pubs := map[string]bool{}
	pr, err := c.q.QueryContext(c.ctx, `SELECT promise_hash FROM publications WHERE retention_unverified = 1`)
	if err != nil {
		return err
	}
	for pr.Next() {
		var h string
		if err := pr.Scan(&h); err != nil {
			pr.Close()
			return err
		}
		pubs[h] = true
	}
	if err := pr.Close(); err != nil {
		return err
	}
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
	c.e.heldPubs = pubs
	if !diff {
		return nil
	}
	return c.touchPromises(promises, nil)
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
// the class it sets. So each catch-up reads the corrected publications
// (publications_corrected: the few a params range ever moved) and, for
// those that moved and those with a row the sweep has still to grade,
// their corrected and stale rows. A row is graded again only while its
// deadline disagrees with its publication's, which only a correction of the
// publication makes it do (store.StaleDeadline), so the rows of a
// publication that neither moved nor has such a row cannot move unseen.

// readCorrections reads the corrected publications and the rows it has to,
// and with diff drops what moved: a publication's settlement day has its
// latest deadline read again, and rows that moved drop their row days and
// their settlement day.
func (c *catchUp) readCorrections(diff bool) error {
	rows, err := c.q.QueryContext(c.ctx, correctedPubsSQL)
	if err != nil {
		return err
	}
	type pub struct{ fp, day string }
	pubs := map[string]pub{}
	for rows.Next() {
		var h, msu, at, day string
		if err := rows.Scan(&h, &msu, &at, &day); err != nil {
			rows.Close()
			return err
		}
		pubs[h] = pub{msu + "|" + at, day}
	}
	if err := rows.Close(); err != nil {
		return err
	}
	var check []string
	for h, p := range pubs {
		if old, ok := c.e.corr[h]; !diff || !ok || old.Pub != p.fp || c.e.corrStale[h] {
			check = append(check, h)
		}
	}
	gone := false
	for h := range c.e.corr {
		if _, ok := pubs[h]; !ok {
			gone = true
		}
	}
	if len(check) == 0 && !gone {
		return nil
	}
	type rowsOf struct {
		parts []string
		days  map[string]bool
		msu   string
		stale bool
	}
	got := map[string]*rowsOf{}
	if len(check) > 0 {
		sort.Strings(check)
		b, _ := json.Marshal(check)
		rr, err := c.q.QueryContext(c.ctx, correctedRowsSQL, string(b))
		if err != nil {
			return err
		}
		for rr.Next() {
			var h, phase, cls, msu, at, started string
			var id, stale int64
			if err := rr.Scan(&h, &id, &phase, &cls, &msu, &at, &started, &stale); err != nil {
				rr.Close()
				return err
			}
			x := got[h]
			if x == nil {
				x = &rowsOf{days: map[string]bool{}}
				got[h] = x
			}
			x.parts = append(x.parts, strconv.FormatInt(id, 10), phase, cls, msu, at)
			if len(started) >= 10 {
				x.days[started[:10]] = true
			}
			x.msu = maxString(x.msu, msu)
			x.stale = x.stale || stale != 0
		}
		if err := rr.Close(); err != nil {
			return err
		}
	}
	// The publications not read again are as they were, and none of them
	// had a row to grade.
	corr := make(map[string]corrFP, len(pubs))
	for h := range pubs {
		if old, ok := c.e.corr[h]; ok {
			corr[h] = old
		}
	}
	stale := map[string]bool{}
	for _, h := range check {
		p, x := pubs[h], got[h]
		if x == nil {
			x = &rowsOf{}
		}
		sum := sha256.Sum256([]byte(strings.Join(x.parts, "\x00")))
		fp := corrFP{Pub: p.fp, Rows: hex.EncodeToString(sum[:])}
		old, had := c.e.corr[h]
		corr[h] = fp
		if x.stale {
			stale[h] = true
		}
		if !diff {
			continue
		}
		if (!had || old.Pub != fp.Pub) && p.day != "" {
			if err := c.readMaxPubMSU(p.day); err != nil {
				return err
			}
		}
		if !had || old.Rows != fp.Rows {
			for d := range x.days {
				c.touchRow(d)
			}
			c.touchSettle(p.day, "", "", x.msu)
		}
	}
	c.e.corr, c.e.corrStale = corr, stale
	return nil
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

// coverNew adds to the covered set the new publications a verified range
// covers.
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
				c.e.covered[p.promise] = true
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

// readFingerprints brings the covered set up to the verified ranges, reads
// the fingerprint of every decision a correction can reach, and with diff
// drops what moved.
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
			c.e.covered[h] = true
		}
		if err := pr.Close(); err != nil {
			return err
		}
		c.e.verified[r.id] = true
	}
	universe := map[string]bool{}
	for h := range c.e.covered {
		universe[h] = true
	}
	cr, err := c.q.QueryContext(c.ctx, `SELECT promise_hash FROM publications WHERE corrected_at IS NOT NULL`)
	if err != nil {
		return err
	}
	for cr.Next() {
		var h string
		if err := cr.Scan(&h); err != nil {
			cr.Close()
			return err
		}
		universe[h] = true
	}
	if err := cr.Close(); err != nil {
		return err
	}
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
