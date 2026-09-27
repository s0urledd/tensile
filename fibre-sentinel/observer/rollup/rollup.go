// Package rollup is the retention policy: raw probe rows are kept for a
// bounded time, their raw JSON for a shorter one, and beyond that the
// "all" window rests on per-day rollups computed while the rows were still
// there, by the same SQL the API runs live. The obligation and suspect-point
// SQL lives here so the live figures and the rolled ones are one
// implementation; the API imports it.
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
	"strings"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/probe"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/verdict"
)

// ObligationBuckets reduces probe rows to one row per (validator, promise)
// obligation. Arguments, in order: the as-of moment (pending cut), the
// settlement window start and end (inclusive), the row upper bound (as_of),
// the row lower bound, then any suspect-point exclusion and caller filter
// appended to the WHERE.
//
// The rows are obligation_rows: the stored probes plus the NOT_PROBED rows a
// sampled-out publication's one decision stands for (store/sampledout.go),
// so an assigned, attested validator of a publication drawn out of the
// sample is an obligation unobserved_not_probed, as it was when the prober
// wrote those rows out one by one.
//
// late_healthy is the count of HEALTHY readings in the tail of the promise's
// own retention window (verdict.EndSegmentDivisor). Served needs one: an
// early HEALTHY probe says the shard was there minutes after settlement, not
// that it survived the hours the promise covers, and treating it as a kept
// promise is what let this observer's own downtime raise a validator's serve
// rate. The cut is drawn from pb.settlement_time and pr.must_serve_until, so
// it is reproducible from the exported rows and does not move when the
// prober's schedule changes.
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
var ObligationBuckets = `SELECT validator_address, promise_hash,
			SUM(cls = 'FAULT')                AS faults,
			SUM(cls = 'HEALTHY')              AS healthy,
			SUM(cls = 'RETENTION_UNVERIFIED') AS held,
			SUM(cls = 'HEALTHY' AND julianday(scheduled_at) >=
			    julianday(must_serve_until) - (julianday(must_serve_until) - julianday(settlement_time)) / 4.0) AS late_healthy,
			SUM(cls NOT IN ('NOT_PROBED','PROBE_ERROR'))                AS attempted,
			SUM(cls NOT IN ('NOT_PROBED','PROBE_ERROR') AND tls_ok = 1) AS reached,
			COALESCE(MAX(CASE WHEN rn = 1 THEN cls END), '')            AS last_cls,
			MAX(must_serve_until > ?)                                   AS pending,
			MIN(CASE WHEN cls = 'FAULT' THEN started_at END)            AS first_fault
		FROM (
			SELECT pr.validator_address, pr.promise_hash,
			       ` + ObligationClass("pr") + ` AS cls,
			       pr.tls_ok, pr.must_serve_until, pr.started_at,
			       pr.scheduled_at, pb.settlement_time,
			       ROW_NUMBER() OVER (PARTITION BY pr.validator_address, pr.promise_hash
			                          ORDER BY (pr.classification IN ('NOT_PROBED','PROBE_ERROR')), pr.scheduled_at DESC, pr.started_at DESC) AS rn
			FROM obligation_rows pr JOIN publications pb ON pb.promise_hash = pr.promise_hash
			WHERE pb.settlement_time >= ? AND pb.settlement_time <= ? AND pr.started_at <= ? AND pr.started_at >= ?
			  AND pr.assigned = 1 AND pr.phase = 'in_window' AND pr.attested = 1`

// cls is ObligationClass: the effective classification, with an
// end-of-window reading counted as a reader of the chain's client meets it
// (probe.EndReadClass).
//
// The 4.0 in that SUM is verdict.EndSegmentDivisor, spelled out because a
// query fragment is a constant; a test in this package holds the two to the
// same number.
//
// cls is the effective classification: the row's own, except that a row of
// a publication whose deadline this observer cannot vouch for publishes no
// serve verdict. The class list in that CASE is probe.DeadlineDerivedClasses
// and the carve-out is probe.DeadlineDerived's; the Go twin is
// verdict.Row.EffectiveClass, and TestTheSQLAndTheGoTwinHoldTheSameRows runs
// both over every cell of (classification, outcome, held). It is a class
// override on an unchanged population rather than a WHERE exclusion, which
// is what keeps the response reconciling against itself: coverage() sums
// every class for its denominator and the attestation split partitions the
// same rows by a column no override touches, so both identities hold with
// no edit at all.
//
// The window ORDER BY deliberately keeps pr.classification: it asks whether
// a row is a gap, and a held row is HEALTHY or FAULT, never a gap. Asking
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
// order Obligations' fields are scanned: total, broken, served,
// end_unobserved, held_param_unverified, unobserved_reachable,
// unobserved_unreachable, unobserved_not_probed, pending.
//
// served and end_unobserved partition the obligations that have a HEALTHY
// reading and no fault: served when the newest verdict is HEALTHY and one of
// those readings falls in the tail of the window, end_unobserved otherwise —
// the shard was there when this observer looked, and this observer did not
// look at the end.
//
// held_param_unverified sits between them and the unobserved arms, and each
// unobserved arm excludes it: an obligation whose only readings were
// withheld is not one this observer failed to observe, it is one it observed
// and cannot speak for. Folding it into unobserved would file the
// observer's own uncertainty about the deadline as a gap in coverage.
const ObligationSums = `COUNT(*),
			COALESCE(SUM(NOT pending AND faults > 0), 0),
			COALESCE(SUM(NOT pending AND faults = 0 AND last_cls = 'HEALTHY' AND late_healthy > 0), 0),
			COALESCE(SUM(NOT pending AND faults = 0 AND healthy > 0 AND NOT (last_cls = 'HEALTHY' AND late_healthy > 0)), 0),
			COALESCE(SUM(NOT pending AND faults = 0 AND healthy = 0 AND held > 0), 0),
			COALESCE(SUM(NOT pending AND faults = 0 AND healthy = 0 AND held = 0 AND reached > 0), 0),
			COALESCE(SUM(NOT pending AND faults = 0 AND healthy = 0 AND held = 0 AND reached = 0 AND attempted > 0), 0),
			COALESCE(SUM(NOT pending AND faults = 0 AND healthy = 0 AND held = 0 AND attempted = 0), 0),
			COALESCE(SUM(pending), 0)`

// Obligations are the nine counts, as the API and the rollup table hold
// them.
type Obligations struct {
	Total, Broken, Served, EndUnobserved, HeldParamUnverified                int64
	UnobservedReachable, UnobservedUnreachable, UnobservedNotProbed, Pending int64
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

// Point is one schedule point's tally, as SuspectPoints returns it.
type Point struct {
	At          string
	Label       string
	Unreachable int64
	Faulted     int64
	Validators  int64
	Rows        int64
}

// Reason applies the correlated-failure guard to one point: "unreachable",
// "fault", both, or "" when the point is not suspect.
func (p Point) Reason() string {
	if p.Validators == 0 {
		return ""
	}
	reason := ""
	if float64(p.Unreachable)/float64(p.Validators) >= verdict.UnreachableThreshold && p.Unreachable >= verdict.MinValidators {
		reason = "unreachable"
	}
	if float64(p.Faulted)/float64(p.Validators) >= verdict.FaultThreshold && p.Faulted >= verdict.MinValidators {
		if reason != "" {
			reason += ","
		}
		reason += "fault"
	}
	return reason
}

// Querier is what SuspectPoints reads through: a *sql.DB or, inside a
// transaction on a single-connection store, the *sql.Tx itself.
type Querier interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

// GuardSilentSQL is the SQL spelling of verdict.GuardSilentClasses: the
// classifications that leave the observer without a reachability verdict
// for the endpoint, so the row could not have landed in either numerator
// whatever happened at the point.
// TestTheSQLAndTheGoTwinExcludeTheSameClassesFromTheGuard holds the two
// lists to each other.
const GuardSilentSQL = `('NOT_PROBED','PROBE_ERROR','NOT_REGISTERED','UNATTESTED','RETENTION_UNVERIFIED')`

// EffectiveClass is the classification every published figure must be
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

// EndNoRowsSQL and EndGenuineRowsSQL are probe.EndNoRowsClasses and
// probe.EndGenuineRowsClasses as SQL IN lists; a test holds them to the Go
// lists.
const (
	EndNoRowsSQL      = `('UNREACHABLE','IDENTITY_MISMATCH','IDENTITY_EXPIRED','SERVER_ERROR','THROTTLED','NOT_REGISTERED')`
	EndGenuineRowsSQL = `('SHADOWED_SHARD','UNMATCHED_GENUINE')`
)

// ObligationClass is the class a row counts as for its obligation: at the
// end-of-window reading (schedule label probe.EndReadLabel) no rows is FAULT
// and genuine rows is HEALTHY (probe.EndReadClass), and over that, the same
// retention hold as EffectiveClass. It is used by the obligation buckets
// only; the class tallies and the correlated-failure guard keep
// EffectiveClass, so they still see what happened on the wire. The Go twin
// is verdict.Row.ObligationClass.
func ObligationClass(alias string) string {
	p := ""
	if alias != "" {
		p = alias + "."
	}
	mapped := `(CASE WHEN ` + p + `schedule_label = '` + probe.EndReadLabel + `' AND ` + p + `classification IN ` + EndNoRowsSQL + ` THEN 'FAULT'` +
		` WHEN ` + p + `schedule_label = '` + probe.EndReadLabel + `' AND ` + p + `classification IN ` + EndGenuineRowsSQL + ` THEN 'HEALTHY'` +
		` ELSE ` + p + `classification END)`
	return `(CASE WHEN ` + p + `retention_unverified = 1 AND ` + mapped + ` IN ` + DeadlineDerivedSQL +
		` AND ` + p + `outcome <> 'INVALID_ROWS' THEN 'RETENTION_UNVERIFIED' ELSE ` + mapped + ` END)`
}

// SuspectPoints tallies every schedule point at which more than one
// validator was probed, over the assigned in-window rows that `where`
// selects, in schedule order. The caller applies Reason. Validators counts
// the validators that gave a reachability verdict at the point; a row that
// could not be in the numerator whatever happened must not dilute the
// share (see GuardSilentSQL). Rows counts every row at the point, because the
// exclusion removes them all. The Go twin is verdict.SuspectPoints.
//
// The rows are probe_rows, aliased probes for the callers that bound it
// by alias: a sampled-out publication's rows are NOT_PROBED, silent by
// GuardSilentSQL, so they never make a point suspect or keep one from
// being; they are among the rows the exclusion removes, and Rows counts
// them as it did when they were stored.
func SuspectPoints(ctx context.Context, db Querier, where string, args ...any) ([]Point, error) {
	cls := EffectiveClass("")
	rows, err := db.QueryContext(ctx, `SELECT scheduled_at, schedule_label,
			COUNT(DISTINCT CASE WHEN `+cls+` = 'UNREACHABLE' THEN validator_address END),
			COUNT(DISTINCT CASE WHEN `+cls+` = 'FAULT' THEN validator_address END),
			COUNT(DISTINCT CASE WHEN `+cls+` NOT IN `+GuardSilentSQL+` THEN validator_address END), COUNT(*)
		FROM probe_rows probes
		WHERE `+where+` AND assigned = 1 AND phase = 'in_window'
		GROUP BY scheduled_at HAVING COUNT(DISTINCT CASE WHEN `+cls+` NOT IN `+GuardSilentSQL+` THEN validator_address END) > 1
		ORDER BY scheduled_at`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Point
	for rows.Next() {
		var p Point
		if err := rows.Scan(&p.At, &p.Label, &p.Unreachable, &p.Faulted, &p.Validators, &p.Rows); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// Exclusion is " AND <col> NOT IN (?, ...)" for the suspect points among
// pts, with their arguments; empty when none is suspect.
func Exclusion(col string, pts []Point) (string, []any) {
	var args []any
	for _, p := range pts {
		if p.Reason() != "" {
			args = append(args, p.At)
		}
	}
	if len(args) == 0 {
		return "", nil
	}
	return " AND " + col + " NOT IN (?" + strings.Repeat(", ?", len(args)-1) + ")", args
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

	// obligations of the day's promises, suspect points over their rows
	pts, err := SuspectPoints(ctx, tx, `promise_hash IN (SELECT promise_hash FROM publications WHERE settlement_time >= ? AND settlement_time <= ?)`, lo, hi)
	if err != nil {
		return 0, err
	}
	excl, exclArgs := Exclusion("pr.scheduled_at", pts)
	// lo is the day's first moment; rows of a promise settled that day cannot
	// have started before it, less the skew margin.
	args := append([]any{store.TS(now), lo, hi, store.TS(now), RowLowerBound(lo)}, exclArgs...)
	rows, err := tx.QueryContext(ctx, `SELECT validator_address, `+ObligationSums+` FROM (`+ObligationBuckets+excl+`)
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

	// rows started on the day, suspect points over them
	pts, err = SuspectPoints(ctx, tx, `started_at >= ? AND started_at <= ?`, lo, hi)
	if err != nil {
		return 0, err
	}
	excl, exclArgs = Exclusion("scheduled_at", pts)
	type pd struct {
		probes, gaps, faults int64
		classes              map[string]int64
		beats, beatsUp       int64
		identityUp           int64
		attested             int64
		unattested           int64
		unknownAtt           int64
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
	rows, err = tx.QueryContext(ctx, `SELECT validator_address, COUNT(*) FROM probes
		WHERE started_at >= ? AND started_at <= ? AND assigned = 1 AND `+EffectiveClass("")+` = 'FAULT'`+excl+` GROUP BY validator_address`,
		append([]any{lo, hi}, exclArgs...)...)
	if err != nil {
		return 0, err
	}
	for rows.Next() {
		var a string
		var n int64
		if err := rows.Scan(&a, &n); err != nil {
			rows.Close()
			return 0, err
		}
		get(a).faults = n
	}
	rows.Close()
	rows, err = tx.QueryContext(ctx, `SELECT validator_address, `+EffectiveClass("")+`, COUNT(*) FROM probe_rows
		WHERE started_at >= ? AND started_at <= ? AND assigned = 1 AND phase = 'in_window'`+excl+` GROUP BY validator_address, `+EffectiveClass(""),
		append([]any{lo, hi}, exclArgs...)...)
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
	// The attestation split over exactly the population the classes above
	// were counted on, suspect exclusion included, so the identity the
	// response publishes survives the fold-in.
	rows, err = tx.QueryContext(ctx, `SELECT validator_address,
			COALESCE(SUM(CASE WHEN attested = 1 THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN attested = 0 THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN attested IS NULL THEN 1 ELSE 0 END), 0)
		FROM probe_rows WHERE started_at >= ? AND started_at <= ? AND assigned = 1 AND phase = 'in_window'`+excl+` GROUP BY validator_address`,
		append([]any{lo, hi}, exclArgs...)...)
	if err != nil {
		return 0, err
	}
	for rows.Next() {
		var a string
		var at, un, unk int64
		if err := rows.Scan(&a, &at, &un, &unk); err != nil {
			rows.Close()
			return 0, err
		}
		v := get(a)
		v.attested, v.unattested, v.unknownAtt = at, un, unk
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
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, d.Format(dayLayout), a, v.probes, v.gaps, v.faults, string(cj), v.beats, v.beatsUp, v.identityUp,
			v.attested, v.unattested, v.unknownAtt, store.TS(now)); err != nil {
			return 0, err
		}
	}
	return pending, tx.Commit()
}

// Rolled is what the rollup tables hold for days before `before`, per
// validator and in total: the figures the "all" window adds to the raw
// record once rows have been pruned.
type Rolled struct {
	Days                 int64
	Obligations          Obligations
	ObligationsByVal     map[string]Obligations
	Probes, Gaps, Faults int64
	Classes              map[string]int64
	ProbesByVal          map[string]*RolledProbes
	Beats, BeatsUp       int64
	IdentityUp           int64
	Attested             int64
	Unattested           int64
	UnknownAtt           int64
}

// RolledProbes is one validator's rolled row counts.
type RolledProbes struct {
	Probes, Gaps, Faults int64
	Classes              map[string]int64
	Beats, BeatsUp       int64
	// IdentityUp is how many of BeatsUp presented a certificate endorsed by
	// the validator's consensus key: the numerator of identity_rate_window,
	// whose denominator is BeatsUp itself.
	IdentityUp int64
	// The attestation split over the same rows the classes were counted on,
	// so that attested + unattested + unknown still equals the coverage
	// denominator once a rolled day is folded into the "all" window.
	Attested   int64
	Unattested int64
	UnknownAtt int64
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
		out.Faults += p.Faults
		out.Beats += p.Beats
		out.BeatsUp += p.BeatsUp
		out.IdentityUp += p.IdentityUp
		out.Attested += p.Attested
		out.Unattested += p.Unattested
		out.UnknownAtt += p.UnknownAtt
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
	// both days' probes and one day's classes. The class tally is what the
	// serve rate and its coverage are drawn from, so that loss moves a
	// published figure about a named validator in the accusing direction.
	// The maps are added in Go instead; the row count is days x validators,
	// which is small.
	rows, err = db.QueryContext(ctx, `SELECT validator_address, probes, gaps, faults, beats, beats_up, identity_up,
			attested, unattested, unknown_att, classes_json
		FROM probe_daily WHERE day < ?`+filter, args...)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var a, cj string
		var p RolledProbes
		if err := rows.Scan(&a, &p.Probes, &p.Gaps, &p.Faults, &p.Beats, &p.BeatsUp, &p.IdentityUp,
			&p.Attested, &p.Unattested, &p.UnknownAtt, &cj); err != nil {
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
		v.Faults += p.Faults
		v.Beats += p.Beats
		v.BeatsUp += p.BeatsUp
		v.IdentityUp += p.IdentityUp
		v.Attested += p.Attested
		v.Unattested += p.Unattested
		v.UnknownAtt += p.UnknownAtt
		for c, n := range classes {
			v.Classes[c] += n
			out.Classes[c] += n
		}
		out.Probes += p.Probes
		out.Gaps += p.Gaps
		out.Faults += p.Faults
		out.Beats += p.Beats
		out.BeatsUp += p.BeatsUp
		out.IdentityUp += p.IdentityUp
		out.Attested += p.Attested
		out.Unattested += p.Unattested
		out.UnknownAtt += p.UnknownAtt
	}
	rows.Close()
	return out, rows.Err()
}
