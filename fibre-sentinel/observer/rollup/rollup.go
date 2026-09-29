// Package rollup is the retention policy: raw probe rows are kept for a
// bounded time, their raw JSON for a shorter one, and beyond that the
// "all" window rests on per-day rollups computed while the rows were still
// there, by the same SQL the API runs live. The obligation SQL lives here
// so the live figures and the rolled ones are one implementation; the API
// imports it.
//
// Three meta keys carry the state: rollup_through (the last day rolled,
// inclusive), raw_from (the first day whose raw rows are all still
// present; unset until the first prune) and nothing else. A day is pruned
// only after it is rolled, oldest first, whole days at a time, so the
// record is never thinner than the rollup behind it.
package rollup

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/probe"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/verdict"
)

// ObligationBuckets reduces probe rows to one row per (validator, promise)
// obligation. Arguments, in order: the as-of moment (pending cut), the
// settlement window start and end (inclusive), the row upper bound (as_of),
// the row lower bound, then any caller filter appended to the WHERE.
//
// Each row counts as its blob's reading leaves it (CountedClass): served
// when its rows came back verified, not served when they did not and the
// blob was Unavailable.
//
// The rows are obligation_rows (with commitment_verified beside them,
// store.ObligationRowsVerified): the stored probes plus the NOT_PROBED rows a
// sampled-out publication's one decision stands for (store/sampledout.go),
// so an assigned, attested validator of a publication drawn out of the
// sample is an obligation this observer did not read, as it was when the
// prober wrote those rows out one by one.
//
// late_healthy is the count of HEALTHY readings that speak for the end of
// the promise: the one reading 10 minutes before must_serve_until
// (probe.EndReadLabel), or, for a blob read on the earlier schedule, a
// reading in the tail of its own retention window
// (verdict.EndSegmentDivisor). An early HEALTHY probe says the shard was
// there minutes after settlement, not that it survived the hours the
// promise covers. The cut is drawn from pb.settlement_time and
// pr.must_serve_until, so it is reproducible from the exported rows.
//
// The row lower bound is not a filter on the answer; it is what keeps the
// cost proportional to the window. With only an upper bound on pr.started_at
// the planner drives the join from probes and walks every in-window assigned
// row the store holds, doing a publications lookup per row to discard almost
// all of them, so a 24h figure costs exactly what "all" costs and the
// snapshot refresh grows with -retain-raw rather than with the window. It
// cannot narrow the result: a row with phase = 'in_window' was scheduled
// inside its promise's retention window, which opens at settlement_time, so
// a probe of a promise settled at or after the window start cannot have
// started materially before it. Callers pass the settlement start less an
// hour, which covers prober and chain clock skew many times over.
//
// first_fault is the start of the obligation's earliest FAULT row. No count
// reads it; the API uses it to mark a broken obligation provisional while
// even its oldest fault is younger than verdict.FaultSettling (see
// observer/api/provisional.go). Sums ignore it, so the rollup is unchanged.
//
// It is a query over many readings, so a served row counts by
// CountedClassBulk.
var ObligationBuckets = obligationBuckets(CountedClassBulk("pr"))

// BlobObligationBuckets is ObligationBuckets for the rows of one blob (the
// caller filters on pr.promise_hash): a served row counts by CountedClass,
// which looks at its own reading only.
var BlobObligationBuckets = obligationBuckets(CountedClass("pr"))

func obligationBuckets(cls string) string {
	return `SELECT validator_address, promise_hash,
			SUM(cls = 'FAULT')                AS faults,
			SUM(cls = 'HEALTHY')              AS healthy,
			SUM(cls = 'RETENTION_UNVERIFIED') AS held,
			SUM(cls = 'HEALTHY' AND (schedule_label = '` + probe.EndReadLabel + `' OR julianday(scheduled_at) >=
			    julianday(must_serve_until) - (julianday(must_serve_until) - julianday(settlement_time)) / 4.0)) AS late_healthy,
			SUM(cls NOT IN ('NOT_PROBED','PROBE_ERROR'))                AS attempted,
			SUM(cls NOT IN ('NOT_PROBED','PROBE_ERROR') AND tls_ok = 1) AS reached,
			COALESCE(MAX(CASE WHEN rn = 1 THEN cls END), '')            AS last_cls,
			MAX(must_serve_until > ?)                                   AS pending,
			MIN(CASE WHEN cls = 'FAULT' THEN started_at END)            AS first_fault
		FROM (
			SELECT pr.validator_address, pr.promise_hash,
			       ` + cls + ` AS cls,
			       pr.tls_ok, pr.must_serve_until, pr.started_at,
			       pr.scheduled_at, pr.schedule_label, pb.settlement_time,
			       ROW_NUMBER() OVER (PARTITION BY pr.validator_address, pr.promise_hash
			                          ORDER BY (pr.classification IN ('NOT_PROBED','PROBE_ERROR')), pr.scheduled_at DESC, pr.started_at DESC) AS rn
			FROM ` + store.ObligationRowsVerified + ` pr JOIN publications pb ON pb.promise_hash = pr.promise_hash
			WHERE pb.settlement_time >= ? AND pb.settlement_time <= ? AND pr.started_at <= ? AND pr.started_at >= ?
			  AND pr.assigned = 1 AND pr.phase = 'in_window' AND pr.attested = 1`
}

// cls is CountedClass: served, not served on an Unavailable blob, and the
// retention hold over both (a row whose deadline this observer cannot vouch
// for publishes neither). The Go twin is verdict.Row.CountedClass.
//
// The 4.0 in that SUM is verdict.EndSegmentDivisor, spelled out because a
// query fragment is a constant; a test in this package holds the two to the
// same number.
//
// The window ORDER BY deliberately keeps pr.classification: it asks whether
// a row is a gap, and a counted row is served or not, never a gap. Asking
// the same question of cls would give the same answer more slowly.
// RowLowerBound is the probe-row lower bound that goes with a settlement
// window start: the start less an hour of clock-skew margin, or the zero
// string when the window is unbounded. Written once so the API and the
// rollup cannot disagree about it.
func RowLowerBound(settlementStart string) string {
	t, err := time.Parse(store.TimeLayout, settlementStart)
	if err != nil {
		return "0000" // an unbounded window: admit every row
	}
	return store.TS(t.Add(-time.Hour))
}

// ObligationSums turns bucketed obligations into the nine counts, in the
// order Obligations' fields are scanned and obligation_daily holds them:
// total, broken, served, end_unobserved, held_param_unverified,
// unobserved_reachable, unobserved_unreachable, unobserved_not_probed,
// pending.
//
// Four of them are published as one, not_counted (Obligations.NotCounted):
// an obligation decided with no count either way. end_unobserved is a HEALTHY
// reading that does not speak for the end of the window (the earlier
// schedule); the three unobserved arms split the rest by what the rows saw:
// an answer that did not count (TLS came up, or not), or no reading at all.
// The split stays in the table because the table's columns are its schema.
//
// held_param_unverified excludes them all: an obligation whose only readings
// were withheld is one this observer observed and cannot speak for.
const ObligationSums = `COUNT(*),
			COALESCE(SUM(NOT pending AND faults > 0), 0),
			COALESCE(SUM(NOT pending AND faults = 0 AND last_cls = 'HEALTHY' AND late_healthy > 0), 0),
			COALESCE(SUM(NOT pending AND faults = 0 AND healthy > 0 AND NOT (last_cls = 'HEALTHY' AND late_healthy > 0)), 0),
			COALESCE(SUM(NOT pending AND faults = 0 AND healthy = 0 AND held > 0), 0),
			COALESCE(SUM(NOT pending AND faults = 0 AND healthy = 0 AND held = 0 AND reached > 0), 0),
			COALESCE(SUM(NOT pending AND faults = 0 AND healthy = 0 AND held = 0 AND reached = 0 AND attempted > 0), 0),
			COALESCE(SUM(NOT pending AND faults = 0 AND healthy = 0 AND held = 0 AND attempted = 0), 0),
			COALESCE(SUM(pending), 0)`

// Obligations are the nine counts, as the rollup table holds them.
type Obligations struct {
	Total, Broken, Served, EndUnobserved, HeldParamUnverified                int64
	UnobservedReachable, UnobservedUnreachable, UnobservedNotProbed, Pending int64
}

// NotCounted is the four counts published as one: obligations decided with
// no count either way.
func (o Obligations) NotCounted() int64 {
	return o.EndUnobserved + o.UnobservedReachable + o.UnobservedUnreachable + o.UnobservedNotProbed
}

// Add sums another set in.
func (o *Obligations) Add(x Obligations) {
	o.Total += x.Total
	o.Broken += x.Broken
	o.Served += x.Served
	o.EndUnobserved += x.EndUnobserved
	o.HeldParamUnverified += x.HeldParamUnverified
	o.UnobservedReachable += x.UnobservedReachable
	o.UnobservedUnreachable += x.UnobservedUnreachable
	o.UnobservedNotProbed += x.UnobservedNotProbed
	o.Pending += x.Pending
}

// EffectiveClass is the classification every published class tally is
// built from: the row's own, except that a row of a publication whose
// retention deadline this observer cannot vouch for publishes no serve
// verdict. The Go twin is verdict.Row.EffectiveClass, and
// TestTheSQLAndTheGoTwinHoldTheSameRows runs both over every cell of
// (classification, outcome, held).
//
// alias is the probes alias, or "" for a bare FROM probes. The class list
// is probe.DeadlineDerivedClasses and the carve-out is
// probe.DeadlineDerived's; TestTheSQLAndTheGoTwinHoldTheSameRows fails if
// this text and that slice disagree, which is the only reason it is spelled
// here rather than generated: ObligationBuckets is a const, and the API
// takes it as one.
func EffectiveClass(alias string) string {
	p := ""
	if alias != "" {
		p = alias + "."
	}
	return `(CASE WHEN ` + p + `retention_unverified = 1 AND ` + p + `classification IN ` + DeadlineDerivedSQL +
		` AND ` + p + `outcome <> 'INVALID_ROWS' THEN 'RETENTION_UNVERIFIED' ELSE ` + p + `classification END)`
}

// DeadlineDerivedSQL is probe.DeadlineDerivedClasses as a SQL IN list.
const DeadlineDerivedSQL = `('HEALTHY','FAULT')`

// CountedClass is what an endorsing validator's row counts as once its
// blob's reading is known, in the window: HEALTHY (served) when its rows
// came back verified; FAULT (not served) when they did not and the reading
// left the blob Unavailable; NOT_PROBED and PROBE_ERROR, this observer's
// gap, and 'NOT_COUNTED' for any other answer, when it was not, and for
// rows that came back on a blob not read by Tensile (the prober missed
// part of the reading and the rows are short). Over served and not served,
// the retention hold of EffectiveClass. A row outside the window, or of a
// validator that did not endorse, keeps its own class. The Go twin is
// verdict.Row.CountedClass.
//
// The blob's reading is looked at only where it can change the count: a
// row whose rows did not come back, or a served row of a reading the prober
// missed part of (missedSQL, a list drawn once per query). That keeps a
// tally over many readings to the few of them that need it. A served row of
// a missed reading asks whether its own reading's rows reconstruct the
// blob, which is right for the rows of one blob or a page of rows; a query
// over many readings uses CountedClassBulk.
//
// alias must name the row's table or view (the correlated subqueries read
// probes under an alias of their own).
func CountedClass(alias string) string {
	return countedClass(alias, false)
}

// CountedClassBulk is CountedClass for a query over many readings: a served
// row of a missed reading is looked up in one list of the missed readings
// that are short (notReadSQL), drawn once for the whole query, instead of
// asking of each served row.
func CountedClassBulk(alias string) string {
	return countedClass(alias, true)
}

// NotServedSQL is true of a row that counts as not served (CountedClass
// FAULT). A served row of an endorsing validator in the window is never not
// served, so its reading is not looked at: a filter over many rows stays as
// cheap as the rows that failed.
func NotServedSQL(alias string) string {
	p := alias + "."
	return `(NOT (COALESCE(` + p + `commitment_verified, 0) = 1 AND ` + p + `phase = 'in_window' AND COALESCE(` + p + `assigned, 0) = 1 AND COALESCE(` +
		p + `attested, 0) = 1) AND ` + CountedClass(alias) + ` = 'FAULT')`
}

func countedClass(alias string, bulk bool) string {
	if alias == "" {
		panic("rollup.CountedClass needs the row's alias")
	}
	p := alias + "."
	h, t := p+"promise_hash", p+"scheduled_at"
	held := func(c string) string {
		return `(CASE WHEN ` + p + `retention_unverified = 1 AND ` + p + `outcome <> 'INVALID_ROWS' THEN 'RETENTION_UNVERIFIED' ELSE '` + c + `' END)`
	}
	nc := `'` + string(verdict.NotCounted) + `'`
	// served, unless the prober missed part of the reading and the rows
	// that came back do not reconstruct the blob (verdict.Reading.Available):
	// not read by Tensile
	served := `(CASE WHEN NOT ` + missedSQL(h, t) + ` THEN ` + held("HEALTHY") +
		` WHEN ` + notAvailableSQL(h, t) + ` = 1 THEN ` + nc +
		` ELSE ` + held("HEALTHY") + ` END)`
	if bulk {
		served = `(CASE WHEN NOT ` + missedSQL(h, t) + ` THEN ` + held("HEALTHY") +
			` WHEN ` + notReadSQL(h, t) + ` THEN ` + nc +
			` ELSE ` + held("HEALTHY") + ` END)`
	}
	return `(CASE WHEN ` + p + `phase <> 'in_window' OR COALESCE(` + p + `assigned, 0) <> 1 OR COALESCE(` + p + `attested, 0) <> 1 THEN ` + p + `classification` +
		` WHEN ` + p + `commitment_verified = 1 THEN ` + served +
		` WHEN ` + p + `classification = 'NOT_PROBED' THEN 'NOT_PROBED'` +
		` WHEN ` + unavailableSQL(h, t) + ` = 1 THEN ` + held("FAULT") +
		` WHEN ` + p + `classification = 'PROBE_ERROR' THEN 'PROBE_ERROR'` +
		` ELSE ` + nc + ` END)`
}

// The pieces of one reading (promise h at scheduled time t), as
// verdict.ReadingOf draws them from all of the reading's rows, whatever
// phase each carries: rows needed, the two bounds on the distinct verified
// rows, the exact count, whether a request reached a server, and whether
// the prober missed one.
func neededSQL(h string) string {
	return `(SELECT json_extract(pk.raw_json, '$.assignment.protocol_params.original_rows') FROM publications pk WHERE pk.promise_hash = ` + h + `)`
}

func lowerSQL(h, t string) string {
	return `((SELECT COALESCE(SUM(r), 0) FROM (SELECT MAX(ql.rows_returned) AS r FROM probes ql
			WHERE ql.promise_hash = ` + h + ` AND ql.scheduled_at = ` + t + ` AND ql.outcome = 'SERVED_OK'
			GROUP BY ql.validator_address))
		- (SELECT MAX(pe.sigma_rows - pe.distinct_rows, 0) FROM publications pe WHERE pe.promise_hash = ` + h + `))`
}

func upperSQL(h, t string) string {
	return `(SELECT COALESCE(SUM(qu.rows_returned), 0) FROM probes qu
			WHERE qu.promise_hash = ` + h + ` AND qu.scheduled_at = ` + t + ` AND qu.commitment_verified = 1)`
}

func exactSQL(h, t string) string {
	return `(SELECT COUNT(DISTINCT j.value) FROM probes qx, json_each(qx.row_indices) j
			WHERE qx.promise_hash = ` + h + ` AND qx.scheduled_at = ` + t + ` AND qx.commitment_verified = 1)`
}

func ranSQL(h, t string) string {
	return `EXISTS (SELECT 1 FROM probes q WHERE q.promise_hash = ` + h + ` AND q.scheduled_at = ` + t + ` AND ` + Reached("q") + `)`
}

// readingKey is one reading, promise h at scheduled time t, as a single
// text value. SQLite probes a list of these far faster than a row value
// (h, t) IN (SELECT ...): over 452,000 stored rows, half a second against
// ten. A promise hash is hex, so the separator cannot occur in it.
func readingKey(h, t string) string {
	return `(` + h + ` || '|' || ` + t + `)`
}

// missedSQL is true when the prober missed a request of the reading of
// promise h at t: a NOT_PROBED row of an assigned validator in the window.
// The list is not correlated, so it is drawn once for a whole query, from
// the covering index over assigned in-window rows.
func missedSQL(h, t string) string {
	return `(` + readingKey(h, t) + ` IN (SELECT ` + readingKey("qm.promise_hash", "qm.scheduled_at") + ` FROM probes qm
			WHERE qm.assigned = 1 AND qm.phase = 'in_window' AND qm.classification = 'NOT_PROBED'))`
}

// notAvailableSQL is 1 when the rows of the reading of promise h at t do not
// reconstruct the blob (not verdict.Reading.Available), 0 when they do; k
// is the rows needed. The cheap bounds settle nearly every reading.
func notAvailableSQL(h, t string) string {
	return notAvailableK(h, t, neededSQL(h))
}

func notAvailableK(h, t, k string) string {
	return `(CASE WHEN COALESCE(` + k + `, 0) <= 0 THEN 1
		WHEN ` + lowerSQL(h, t) + ` >= ` + k + ` THEN 0
		WHEN ` + upperSQL(h, t) + ` < ` + k + ` THEN 1
		WHEN ` + exactSQL(h, t) + ` >= ` + k + ` THEN 0
		ELSE 1 END)`
}

// notReadSQL is true when the reading of promise h at t is one the prober
// missed part of (missedSQL) and whose rows do not reconstruct the blob:
// not read by Tensile, so the rows that came back count neither way. It is
// a list rather than a question per row: the availability of every missed
// reading is drawn once for a whole query, where a question per served row
// took minutes over a day's obligations on a copy of the live store.
// CountedClassBulk asks it only of a served row whose reading is missed, so
// a query that meets none never draws it.
//
// The rows needed are read from one publication per protocol params
// fingerprint (the fingerprint hashes original_rows, so it names one
// value), since parsing each record's raw_json for it was most of the
// list's cost; a record without a fingerprint is read on its own.
func notReadSQL(h, t string) string {
	return `(` + readingKey(h, t) + ` IN (WITH
		mr AS MATERIALIZED (SELECT qm.promise_hash AS h, qm.scheduled_at AS t FROM probes qm
			WHERE qm.assigned = 1 AND qm.phase = 'in_window' AND qm.classification = 'NOT_PROBED'
			GROUP BY qm.promise_hash, qm.scheduled_at),
		mp AS MATERIALIZED (SELECT mr.h AS h, mr.t AS t, pb.protocol_params_fingerprint AS fp FROM mr LEFT JOIN publications pb ON pb.promise_hash = mr.h),
		fk AS MATERIALIZED (SELECT f.fp AS fp, ` + neededSQL("f.h") + ` AS k
			FROM (SELECT mp.fp AS fp, MIN(mp.h) AS h FROM mp WHERE COALESCE(mp.fp, '') <> '' GROUP BY mp.fp) f)
		SELECT ` + readingKey("mp.h", "mp.t") + ` FROM mp LEFT JOIN fk ON fk.fp = mp.fp
		WHERE ` + notAvailableK("mp.h", "mp.t", "COALESCE(fk.k, "+neededSQL("mp.h")+")") + ` = 1))`
}

// Reached is verdict.Reached (probe.Reached) over a probes row under the
// alias given: the request's connection was opened, its host refused it, or
// rows came back verified. A reading with none did not happen.
func Reached(alias string) string {
	p := alias + "."
	return `(` + p + `commitment_verified = 1 OR ` + p + `tcp_ok = 1 OR ` + p + `outcome = 'TCP_REFUSED')`
}

// unavailableSQL is 1 when the reading of promise h at t happened and left
// the blob Unavailable (verdict.Reading.Unavailable), 0 otherwise. The
// cheap bounds settle nearly every reading; the row lists are read only
// when they cannot.
func unavailableSQL(h, t string) string {
	k := neededSQL(h)
	return `(CASE WHEN COALESCE(` + k + `, 0) <= 0 THEN 0
		WHEN ` + lowerSQL(h, t) + ` >= ` + k + ` THEN 0
		WHEN ` + missedSQL(h, t) + ` THEN 0
		WHEN NOT ` + ranSQL(h, t) + ` THEN 0
		WHEN ` + upperSQL(h, t) + ` < ` + k + ` THEN 1
		WHEN ` + exactSQL(h, t) + ` >= ` + k + ` THEN 0
		ELSE 1 END)`
}

// Config is the retention policy.
type Config struct {
	// RetainRaw is how long probe and heartbeat rows are kept. 0 keeps
	// them forever (and prunes nothing).
	RetainRaw time.Duration
	// RetainRawJSON is how long a row keeps its raw_json, the bulk of it.
	// 0 keeps it forever.
	RetainRawJSON time.Duration
	// RollupAfter is how long after a UTC day ends its rollup is computed;
	// it must clear every retention window a promise settled that day can
	// have, so that nothing is pending when the day is rolled.
	RollupAfter time.Duration
	// Batch bounds the rows one pass strips raw_json from, and the days one
	// pass prunes.
	Batch int
	// Vantage, when set, is the observer's own: the heartbeat counts rolled
	// into probe_daily are its rows only, as every live reachability figure
	// is. Other vantages' copied heartbeats are pruned with the rest but
	// never counted. Empty counts every row.
	Vantage string
}

// Default is the retention decision of 2026-09-18: rows 90 days, raw JSON
// 30 days, rollup 14 days after the day.
func Default() Config {
	return Config{RetainRaw: 90 * 24 * time.Hour, RetainRawJSON: 30 * 24 * time.Hour, RollupAfter: 14 * 24 * time.Hour, Batch: 5000}
}

// Report is what one pass did.
type Report struct {
	RolledDays []string
	// Waiting is the first due day that is not final yet, and why: a
	// promise settled that day is still under obligation, or rows await
	// the late shadow verdict. RollupAfter is a floor; this is the ceiling.
	Waiting, WaitingWhy string
	PendingAtRoll       int64 // obligations still pending when their day was rolled, which dayFinal should make impossible
	RawJSONDropped      int64
	PrunedDays          []string
	PrunedRows          int64
}

const (
	metaRollupThrough = "rollup_through"
	metaRawFrom       = "raw_from"
)

const dayLayout = "2006-01-02"

func dayOf(t time.Time) time.Time {
	t = t.UTC()
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
}

// dayRange is the inclusive TS bounds of one UTC day.
func dayRange(d time.Time) (string, string) {
	return store.TS(d), store.TS(d.Add(24*time.Hour - time.Nanosecond))
}

// RawFrom is the first day whose raw rows are all still present, and false
// until a prune has happened (every row is present then).
func RawFrom(st *store.Store) (time.Time, bool) {
	v, err := st.Meta(metaRawFrom)
	if err != nil || v == "" {
		return time.Time{}, false
	}
	d, err := time.Parse(dayLayout, v)
	if err != nil {
		return time.Time{}, false
	}
	return d, true
}

// Run does one retention pass: roll every day that is due, strip raw_json
// past its retention, prune rolled days past theirs.
func Run(ctx context.Context, st *store.Store, now time.Time, cfg Config) (Report, error) {
	var rep Report
	if cfg.Batch <= 0 {
		cfg.Batch = 5000
	}
	now = now.UTC()
	db := st.DB()

	// ---- rollups ----
	if cfg.RollupAfter > 0 {
		lastDue := dayOf(now.Add(-cfg.RollupAfter)).Add(-24 * time.Hour)
		start, ok, err := nextRollupDay(st)
		if err != nil {
			return rep, err
		}
		for d := start; ok && !d.After(lastDue); d = d.Add(24 * time.Hour) {
			if ctx.Err() != nil {
				return rep, ctx.Err()
			}
			final, why, err := dayFinal(ctx, db, d, now)
			if err != nil {
				return rep, err
			}
			if !final {
				// Days roll in order; a day that is not final holds every
				// later one, so the rollup never silently carries a
				// pending obligation.
				rep.Waiting, rep.WaitingWhy = d.Format(dayLayout), why
				break
			}
			pending, err := rollDay(ctx, db, d, now, cfg.Vantage)
			if err != nil {
				return rep, fmt.Errorf("roll %s: %w", d.Format(dayLayout), err)
			}
			rep.PendingAtRoll += pending
			if err := st.SetMeta(metaRollupThrough, d.Format(dayLayout), now); err != nil {
				return rep, err
			}
			rep.RolledDays = append(rep.RolledDays, d.Format(dayLayout))
		}
	}

	// ---- raw_json ----
	if cfg.RetainRawJSON > 0 {
		cut := store.TS(now.Add(-cfg.RetainRawJSON))
		for _, table := range []string{"probes", "reachability"} {
			res, err := db.ExecContext(ctx, `UPDATE `+table+` SET raw_json = '' WHERE rowid IN
				(SELECT rowid FROM `+table+` WHERE started_at < ? AND raw_json <> '' LIMIT ?)`, cut, cfg.Batch)
			if err != nil {
				return rep, fmt.Errorf("strip raw_json from %s: %w", table, err)
			}
			n, _ := res.RowsAffected()
			rep.RawJSONDropped += n
		}
	}

	// ---- prune ----
	if cfg.RetainRaw > 0 {
		through, err := st.Meta(metaRollupThrough)
		if err != nil || through == "" {
			return rep, err // nothing rolled: nothing may be pruned
		}
		rolled, err := time.Parse(dayLayout, through)
		if err != nil {
			return rep, fmt.Errorf("rollup_through %q: %w", through, err)
		}
		from, ok := RawFrom(st)
		if !ok {
			from, ok, err = firstRowDay(ctx, db)
			if err != nil || !ok {
				return rep, err
			}
		}
		cut := dayOf(now.Add(-cfg.RetainRaw))
		for days := 0; days < 3 && !from.After(rolled) && from.Before(cut); days++ {
			lo, hi := dayRange(from)
			var n int64
			for _, table := range []string{"probes", "reachability", "probe_confirmations"} {
				res, err := db.ExecContext(ctx, `DELETE FROM `+table+` WHERE started_at >= ? AND started_at <= ?`, lo, hi)
				if err != nil {
					return rep, fmt.Errorf("prune %s %s: %w", table, from.Format(dayLayout), err)
				}
				k, _ := res.RowsAffected()
				n += k
			}
			// A sampled-out publication's rows are its decision, started at
			// decided_at: pruned with the day its rows would have been,
			// points and all (ON DELETE CASCADE).
			res, err := db.ExecContext(ctx, `DELETE FROM sampling_decisions WHERE decided_at >= ? AND decided_at <= ?`, lo, hi)
			if err != nil {
				return rep, fmt.Errorf("prune sampling_decisions %s: %w", from.Format(dayLayout), err)
			}
			k, _ := res.RowsAffected()
			n += k
			rep.PrunedRows += n
			rep.PrunedDays = append(rep.PrunedDays, from.Format(dayLayout))
			from = from.Add(24 * time.Hour)
			if err := st.SetMeta(metaRawFrom, from.Format(dayLayout), now); err != nil {
				return rep, err
			}
		}
	}
	return rep, nil
}

// finalMargin is added to the last must_serve_until of a day's promises
// before the day counts as final: the last in-window probe may start late
// (the prober's lateness allowance) and the prune tolerance sits past the
// deadline.
const finalMargin = time.Hour

// dayFinal reports whether every obligation of the promises settled on d
// is decided: their windows have closed, and no probe row of theirs still
// awaits the late shadow verdict.
func dayFinal(ctx context.Context, db *sql.DB, d, now time.Time) (bool, string, error) {
	lo, hi := dayRange(d)
	var last sql.NullString
	if err := db.QueryRowContext(ctx, `SELECT MAX(must_serve_until) FROM publications WHERE settlement_time >= ? AND settlement_time <= ?`, lo, hi).Scan(&last); err != nil {
		return false, "", err
	}
	if last.Valid && last.String != "" {
		t, err := time.Parse(store.TimeLayout, last.String)
		if err != nil {
			return false, "", fmt.Errorf("must_serve_until %q: %w", last.String, err)
		}
		if now.Before(t.Add(finalMargin)) {
			return false, "a promise settled that day is under obligation until " + t.UTC().Format(time.RFC3339), nil
		}
	}
	var deferred int64
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM probes pr JOIN publications pb ON pb.promise_hash = pr.promise_hash
		WHERE pb.settlement_time >= ? AND pb.settlement_time <= ?
		  AND pr.classification = 'PROBE_ERROR' AND pr.shadow_gap IS NOT NULL AND pr.amended_at IS NULL
		  AND pr.outcome IN ('WRONG_ROWS','PARTIAL') AND pr.commitment_verified = 1`, lo, hi).Scan(&deferred); err != nil {
		return false, "", err
	}
	if deferred > 0 {
		return false, fmt.Sprintf("%d probe row(s) of its promises await the late shadow verdict", deferred), nil
	}
	// A promise whose params range is still open must keep its raw rows:
	// rolling the day freezes the buckets and the prune then deletes the
	// rows a correction would re-grade, so a fault withheld today would
	// come back as a frozen fault tomorrow with nothing left to correct.
	// rollup.Run walks days in order, so one held day holds every later
	// one — which is why the scanner closes a range in the pass that opens
	// it, and why an unreadable range is recorded unresolvable rather than
	// left open forever.
	var held int64
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM publications
		WHERE retention_unverified = 1 AND settlement_time >= ? AND settlement_time <= ?`, lo, hi).Scan(&held); err != nil {
		return false, "", err
	}
	if held > 0 {
		return false, fmt.Sprintf("%d promise(s) settled that day sit in an x/fibre params range this observer has not read every height of", held), nil
	}
	return true, "", nil
}

// nextRollupDay is the day after the last rolled one, or the first day with
// any row when nothing is rolled yet; false when the store is empty.
func nextRollupDay(st *store.Store) (time.Time, bool, error) {
	v, err := st.Meta(metaRollupThrough)
	if err == nil && v != "" {
		d, err := time.Parse(dayLayout, v)
		if err != nil {
			return time.Time{}, false, fmt.Errorf("rollup_through %q: %w", v, err)
		}
		return d.Add(24 * time.Hour), true, nil
	}
	return firstRowDay(context.Background(), st.DB())
}

func firstRowDay(ctx context.Context, db *sql.DB) (time.Time, bool, error) {
	var first sql.NullString
	if err := db.QueryRowContext(ctx, `SELECT MIN(t) FROM (
			SELECT MIN(started_at) AS t FROM probes
			UNION ALL SELECT MIN(decided_at) FROM sampling_decisions
			UNION ALL SELECT MIN(started_at) FROM reachability
			UNION ALL SELECT MIN(settlement_time) FROM publications)`).Scan(&first); err != nil {
		return time.Time{}, false, err
	}
	if !first.Valid || first.String == "" {
		return time.Time{}, false, nil
	}
	t, err := time.Parse(store.TimeLayout, first.String)
	if err != nil {
		return time.Time{}, false, fmt.Errorf("first row time %q: %w", first.String, err)
	}
	return dayOf(t), true, nil
}

// rollDay writes obligation_daily for the promises settled on d and
// probe_daily for the rows started on d, replacing any earlier rollup of
// the day. It returns how many obligations were still pending.
func rollDay(ctx context.Context, db *sql.DB, d, now time.Time, vantage string) (int64, error) {
	lo, hi := dayRange(d)
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	// obligations of the day's promises. lo is the day's first moment; rows
	// of a promise settled that day cannot have started before it, less the
	// skew margin.
	args := []any{store.TS(now), lo, hi, store.TS(now), RowLowerBound(lo)}
	rows, err := tx.QueryContext(ctx, `SELECT validator_address, `+ObligationSums+` FROM (`+ObligationBuckets+`)
			GROUP BY validator_address, promise_hash) GROUP BY validator_address`, args...)
	if err != nil {
		return 0, err
	}
	type vo struct {
		addr string
		o    Obligations
	}
	var obls []vo
	var pending int64
	for rows.Next() {
		var v vo
		if err := rows.Scan(&v.addr, &v.o.Total, &v.o.Broken, &v.o.Served, &v.o.EndUnobserved, &v.o.HeldParamUnverified,
			&v.o.UnobservedReachable, &v.o.UnobservedUnreachable, &v.o.UnobservedNotProbed, &v.o.Pending); err != nil {
			rows.Close()
			return 0, err
		}
		pending += v.o.Pending
		obls = append(obls, v)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM obligation_daily WHERE day = ?`, d.Format(dayLayout)); err != nil {
		return 0, err
	}
	for _, v := range obls {
		if _, err := tx.ExecContext(ctx, `INSERT INTO obligation_daily (day, validator_address, total, served, broken, end_unobserved,
				held_param_unverified, unobserved_reachable, unobserved_unreachable, unobserved_not_probed, pending, computed_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, d.Format(dayLayout), v.addr, v.o.Total, v.o.Served, v.o.Broken, v.o.EndUnobserved,
			v.o.HeldParamUnverified, v.o.UnobservedReachable, v.o.UnobservedUnreachable, v.o.UnobservedNotProbed, v.o.Pending, store.TS(now)); err != nil {
			return 0, err
		}
	}

	// rows started on the day
	// probe_daily keeps four columns nothing reads any more: faults (a raw
	// FAULT count, which counts failures the rule does not) and the
	// per-reading attestation split (attested, unattested, unknown_att).
	// They are the table's schema, so they are written as 0.
	type pd struct {
		probes, gaps   int64
		classes        map[string]int64
		beats, beatsUp int64
		identityUp     int64
	}
	byVal := map[string]*pd{}
	get := func(a string) *pd {
		v, ok := byVal[a]
		if !ok {
			v = &pd{classes: map[string]int64{}}
			byVal[a] = v
		}
		return v
	}
	rows, err = tx.QueryContext(ctx, `SELECT validator_address, COUNT(*),
			COALESCE(SUM(classification IN ('NOT_PROBED','PROBE_ERROR')), 0)
		FROM probe_rows WHERE started_at >= ? AND started_at <= ? GROUP BY validator_address`, lo, hi)
	if err != nil {
		return 0, err
	}
	for rows.Next() {
		var a string
		var n, g int64
		if err := rows.Scan(&a, &n, &g); err != nil {
			rows.Close()
			return 0, err
		}
		v := get(a)
		v.probes, v.gaps = n, g
	}
	rows.Close()
	rows, err = tx.QueryContext(ctx, `SELECT validator_address, `+EffectiveClass("")+`, COUNT(*) FROM probe_rows
		WHERE started_at >= ? AND started_at <= ? AND assigned = 1 AND phase = 'in_window' GROUP BY validator_address, `+EffectiveClass(""),
		lo, hi)
	if err != nil {
		return 0, err
	}
	for rows.Next() {
		var a, c string
		var n int64
		if err := rows.Scan(&a, &c, &n); err != nil {
			rows.Close()
			return 0, err
		}
		get(a).classes[c] = n
	}
	rows.Close()
	vq, vargs := "", []any{lo, hi}
	if vantage != "" {
		vq, vargs = " AND +vantage = ?", append(vargs, vantage)
	}
	rows, err = tx.QueryContext(ctx, `SELECT validator_address, COUNT(*),
			COALESCE(SUM(CASE WHEN tcp_ok = 1 AND tls_ok = 1 THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN tcp_ok = 1 AND tls_ok = 1 AND identity_ok = 1 THEN 1 ELSE 0 END), 0)
		FROM reachability WHERE started_at >= ? AND started_at <= ? AND outcome <> 'PROBE_ERROR'`+vq+` GROUP BY validator_address`, vargs...)
	if err != nil {
		return 0, err
	}
	for rows.Next() {
		var a string
		var n, up, ident int64
		if err := rows.Scan(&a, &n, &up, &ident); err != nil {
			rows.Close()
			return 0, err
		}
		v := get(a)
		v.beats, v.beatsUp, v.identityUp = n, up, ident
	}
	rows.Close()
	if _, err := tx.ExecContext(ctx, `DELETE FROM probe_daily WHERE day = ?`, d.Format(dayLayout)); err != nil {
		return 0, err
	}
	for a, v := range byVal {
		cj, err := json.Marshal(v.classes)
		if err != nil {
			return 0, err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO probe_daily (day, validator_address, probes, gaps, faults, classes_json, beats, beats_up, identity_up, attested, unattested, unknown_att, computed_at)
			VALUES (?, ?, ?, ?, 0, ?, ?, ?, ?, 0, 0, 0, ?)`, d.Format(dayLayout), a, v.probes, v.gaps, string(cj), v.beats, v.beatsUp, v.identityUp,
			store.TS(now)); err != nil {
			return 0, err
		}
	}
	return pending, tx.Commit()
}

// Rolled is what the rollup tables hold for days before `before`, per
// validator and in total: the figures the "all" window adds to the raw
// record once rows have been pruned.
type Rolled struct {
	Days             int64
	Obligations      Obligations
	ObligationsByVal map[string]Obligations
	Probes, Gaps     int64
	Classes          map[string]int64
	ProbesByVal      map[string]*RolledProbes
	Beats, BeatsUp   int64
	IdentityUp       int64
}

// RolledProbes is one validator's rolled row counts.
type RolledProbes struct {
	Probes, Gaps   int64
	Classes        map[string]int64
	Beats, BeatsUp int64
	// IdentityUp is how many of BeatsUp presented a certificate endorsed by
	// the validator's consensus key: the numerator of identity_rate_window,
	// whose denominator is BeatsUp itself.
	IdentityUp int64
}

// Without returns a copy with the named validators taken out of every total,
// recomputed from the per-validator rows this already carries rather than
// from a second query. The API's `?exclude=` has to reach the rolled days as
// well as the raw ones, or the "all" window would answer half the question:
// excluded from the last thirty days, counted for every day before them.
//
// Days is left as it was. It counts the days rolled up, not anyone's rows.
func (r *Rolled) Without(addrs []string) *Rolled {
	if r == nil || len(addrs) == 0 {
		return r
	}
	drop := make(map[string]bool, len(addrs))
	for _, a := range addrs {
		drop[a] = true
	}
	out := &Rolled{Days: r.Days, ObligationsByVal: map[string]Obligations{},
		ProbesByVal: map[string]*RolledProbes{}, Classes: map[string]int64{}}
	for a, o := range r.ObligationsByVal {
		if drop[a] {
			continue
		}
		out.ObligationsByVal[a] = o
		out.Obligations.Add(o)
	}
	for a, p := range r.ProbesByVal {
		if drop[a] {
			continue
		}
		out.ProbesByVal[a] = p
		out.Probes += p.Probes
		out.Gaps += p.Gaps
		out.Beats += p.Beats
		out.BeatsUp += p.BeatsUp
		out.IdentityUp += p.IdentityUp
		for c, n := range p.Classes {
			out.Classes[c] += n
		}
	}
	return out
}

// Load reads the rollups for days before `before` (a UTC day). only, when
// set, restricts to one validator.
func Load(ctx context.Context, db *sql.DB, before time.Time, only string) (*Rolled, error) {
	out := &Rolled{ObligationsByVal: map[string]Obligations{}, ProbesByVal: map[string]*RolledProbes{}, Classes: map[string]int64{}}
	b := before.UTC().Format(dayLayout)
	filter, args := "", []any{b}
	if only != "" {
		filter, args = " AND validator_address = ?", []any{b, only}
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(DISTINCT day) FROM probe_daily WHERE day < ?`, b).Scan(&out.Days); err != nil {
		return nil, err
	}
	rows, err := db.QueryContext(ctx, `SELECT validator_address, SUM(total), SUM(broken), SUM(served), SUM(end_unobserved),
			SUM(held_param_unverified),
			SUM(unobserved_reachable), SUM(unobserved_unreachable), SUM(unobserved_not_probed), SUM(pending)
		FROM obligation_daily WHERE day < ?`+filter+` GROUP BY validator_address`, args...)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var a string
		var o Obligations
		if err := rows.Scan(&a, &o.Total, &o.Broken, &o.Served, &o.EndUnobserved, &o.HeldParamUnverified, &o.UnobservedReachable,
			&o.UnobservedUnreachable, &o.UnobservedNotProbed, &o.Pending); err != nil {
			rows.Close()
			return nil, err
		}
		out.ObligationsByVal[a] = o
		out.Obligations.Add(o)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	// One row per rolled day, not one per distinct class map. Grouping by
	// classes_json would sum the numeric columns across every day in the
	// group while taking the JSON of one of them, so two days on which a
	// validator produced the same class distribution — the ordinary shape
	// of a steady validator, {"HEALTHY":8} day after day — would contribute
	// both days' readings and one day's classes. The maps are added in Go
	// instead; the row count is days x validators, which is small.
	rows, err = db.QueryContext(ctx, `SELECT validator_address, probes, gaps, beats, beats_up, identity_up, classes_json
		FROM probe_daily WHERE day < ?`+filter, args...)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var a, cj string
		var p RolledProbes
		if err := rows.Scan(&a, &p.Probes, &p.Gaps, &p.Beats, &p.BeatsUp, &p.IdentityUp, &cj); err != nil {
			rows.Close()
			return nil, err
		}
		var classes map[string]int64
		if err := json.Unmarshal([]byte(cj), &classes); err != nil {
			rows.Close()
			return nil, fmt.Errorf("probe_daily classes for %s: %w", a, err)
		}
		v, ok := out.ProbesByVal[a]
		if !ok {
			v = &RolledProbes{Classes: map[string]int64{}}
			out.ProbesByVal[a] = v
		}
		v.Probes += p.Probes
		v.Gaps += p.Gaps
		v.Beats += p.Beats
		v.BeatsUp += p.BeatsUp
		v.IdentityUp += p.IdentityUp
		for c, n := range classes {
			v.Classes[c] += n
			out.Classes[c] += n
		}
		out.Probes += p.Probes
		out.Gaps += p.Gaps
		out.Beats += p.Beats
		out.BeatsUp += p.BeatsUp
		out.IdentityUp += p.IdentityUp
	}
	rows.Close()
	return out, rows.Err()
}
