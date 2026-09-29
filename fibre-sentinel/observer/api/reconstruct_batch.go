package api

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/probe"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/rollup"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/verdict"
)

// Batched blob status.
//
// reconstructable answers for one publication from its rows, which is right
// for the blob detail page and wrong for the network summary, which asks it
// of up to reconstructSample publications per refresh. This computes the
// same status for a whole selection in five queries plus a rare per-blob
// fallback. It is deliberately a second implementation rather than a
// replacement: reconstructable stays as the reference, and
// TestReconstructBatchMatchesReference asserts the two agree on every
// publication of a fixture that exercises every status.
//
// The saving that matters is not the query count but the row lists. The
// distinct verified rows are what the status turns on, and counting them
// means parsing a JSON array per validator per blob. It is almost always
// avoidable, because two sums bound the count (verdict.Reading):
//
//	served shards (SERVED_OK, one per validator) - excess  <=  distinct  <=  every verified row
//
// where excess, sigma_rows - distinct_rows, is how many assigned row indices
// the assignment hands to more than one validator. Available needs the
// distinct rows at or above needed_rows, and Unavailable needs them, plus
// the rows of the validators without an answer of their own (every
// validator the assignment gives rows, endorsing or not), below it; whenever
// the bounds land on one side, and on nearly every blob they do, no row list
// is read. Only the narrow ambiguous band falls back to the exact count, one
// blob at a time.

// blobSel is the selection every batch query joins against: the same ordering
// and limit blobRows applies, expressed once as a CTE so the database does the
// join instead of an IN list with two thousand parameters.
func blobSel(where string, limit int) string { return blobSelAt(where, limit, 0) }

// blobSelAt is blobSel from the offset-th row of the same order.
func blobSelAt(where string, limit, offset int) string {
	q := `WITH sel AS (SELECT promise_hash FROM publications`
	if where != "" {
		q += " WHERE " + where
	}
	q += ` ORDER BY settlement_height DESC, settlement_tx_index DESC LIMIT ` + strconv.Itoa(limit)
	if offset > 0 {
		q += ` OFFSET ` + strconv.Itoa(offset)
	}
	return q + `) `
}

// asOfPin bounds a reconstructability pass in time. The zero value is live.
//
// Everything else on a pinned /v1/network answer is bounded by
// started_at <= as_of, and AsOfNote tells the reader so. A verdict about a
// moment must be drawn from what was known at that moment: a blob whose
// reading had not happened yet at the pin is pending there, whatever it
// became.
type asOfPin struct {
	// at is store.TimeLayout of the pinned end, or "" when live.
	at string
	// now is the moment "has the retention window closed?" is asked at: the
	// pinned end, or the real clock when live.
	now time.Time
}

func pinFor(win Window) asOfPin {
	if !win.AsOf {
		return asOfPin{now: time.Now()}
	}
	return asOfPin{at: win.endArg(), now: win.End}
}

// bound appends the row bound this pin implies to a WHERE clause on the alias
// given, and returns the argument list to use with it.
func (p asOfPin) bound(alias string, args []any) (string, []any) {
	if p.at == "" {
		return "", args
	}
	return " AND " + alias + ".started_at <= ?", append(append([]any{}, args...), p.at)
}

// over reports whether the retention deadline had passed as of this pin.
func (p asOfPin) over(msu string) bool {
	at := p.now
	if at.IsZero() {
		at = time.Now()
	}
	if t, err := time.Parse(store.TimeLayout, msu); err == nil {
		return at.After(t)
	}
	if t, err := time.Parse(time.RFC3339Nano, msu); err == nil {
		return at.After(t)
	}
	return false
}

// readingAgg is one reading (a scheduled time) of one publication, already
// aggregated.
type readingAgg struct {
	at       string
	end      bool
	asked    int // distinct validators asked: a row other than NOT_PROBED or PROBE_ERROR
	answered int // of those, with an answer of their own (verdict.Answered)
	endorsed int // of those, endorsing validators that answered
	served   int // whose rows verified
	upper    int // every verified row
	guard    rollup.Point
}

// reconstructBatch returns the status of every publication in the selection,
// keyed by promise hash. Only the status and the counts beside it are
// computed; served_distinct_rows is left at zero, which is what lets the
// bounds replace the row lists. A list or detail page, which does publish
// it, uses the reference.
func (s *Server) reconstructBatch(ctx context.Context, where string, limit int, pin asOfPin, args ...any) (map[string]*reconstruct, error) {
	db := s.st.DB()
	sel := blobSel(where, limit)

	// 1. the publications themselves.
	type blobFacts struct {
		needed, total   int64
		excess          int
		msu, assignment string
		known           bool
	}
	facts := map[string]blobFacts{}
	rows, err := db.QueryContext(ctx, sel+`
		SELECT p.promise_hash, p.sigma_rows, p.distinct_rows, p.must_serve_until, p.assignment_error,
		       json_extract(p.raw_json,'$.assignment.protocol_params.original_rows'),
		       json_extract(p.raw_json,'$.assignment.protocol_params.total_rows')
		FROM publications p JOIN sel ON sel.promise_hash = p.promise_hash`, args...)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var hash string
		var f blobFacts
		var sigma, distinct int
		var needed, total sql.NullInt64
		if err := rows.Scan(&hash, &sigma, &distinct, &f.msu, &f.assignment, &needed, &total); err != nil {
			rows.Close()
			return nil, err
		}
		f.excess = max(sigma-distinct, 0)
		f.needed, f.total, f.known = needed.Int64, total.Int64, needed.Valid && needed.Int64 > 0
		facts[hash] = f
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(facts) == 0 {
		return map[string]*reconstruct{}, nil
	}

	// 2. the rows every validator the assignment gives rows holds (the set
	// the client asks), and how many of them endorse, per publication.
	held, endorsers := map[string]int{}, map[string]int{}
	rows, err = db.QueryContext(ctx, sel+`
		SELECT a.promise_hash, COALESCE(SUM(a.row_count), 0), COALESCE(SUM(a.attested = 1 OR a.attested IS NULL), 0)
		FROM assignments a JOIN sel ON sel.promise_hash = a.promise_hash
		WHERE a.row_count > 0 GROUP BY a.promise_hash`, args...)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var hash string
		var n, v int
		if err := rows.Scan(&hash, &n, &v); err != nil {
			rows.Close()
			return nil, err
		}
		held[hash], endorsers[hash] = n, v
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// 3. every reading: who was asked, who answered, the verified rows, and
	// the correlated-failure guard's tally, as rollup.SuspectPoints makes
	// it over this blob's rows.
	pb, pargs := pin.bound("p", args)
	cls := rollup.EffectiveClass("p")
	passed := rollup.PassedOverSQL("p")
	failed := `((` + rollup.ObligationClass("p") + ` = 'FAULT' AND p.classification <> 'NOT_REGISTERED') OR ` + passed + `)`
	answered := rollup.Answered("p")
	points := map[string][]readingAgg{}
	rows, err = db.QueryContext(ctx, sel+`
		SELECT p.promise_hash, p.scheduled_at, MAX(p.schedule_label = '`+probe.EndReadLabel+`'),
		       COUNT(DISTINCT CASE WHEN p.classification NOT IN ('NOT_PROBED','PROBE_ERROR') THEN p.validator_address END),
		       COUNT(DISTINCT CASE WHEN `+answered+` THEN p.validator_address END),
		       COUNT(DISTINCT CASE WHEN p.commitment_verified = 1 THEN p.validator_address END),
		       COALESCE(SUM(CASE WHEN p.commitment_verified = 1 THEN p.rows_returned END), 0),
		       COUNT(DISTINCT CASE WHEN p.assigned = 1 AND `+cls+` = 'UNREACHABLE' THEN p.validator_address END),
		       COUNT(DISTINCT CASE WHEN p.assigned = 1 AND `+failed+` THEN p.validator_address END),
		       COUNT(DISTINCT CASE WHEN p.assigned = 1 AND (`+cls+` NOT IN `+rollup.GuardSilentSQL+` OR `+passed+`) THEN p.validator_address END)
		FROM probes p JOIN sel ON sel.promise_hash = p.promise_hash
		WHERE p.phase = 'in_window'`+pb+`
		GROUP BY p.promise_hash, p.scheduled_at`, pargs...)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var hash string
		var ra readingAgg
		if err := rows.Scan(&hash, &ra.at, &ra.end, &ra.asked, &ra.answered, &ra.served, &ra.upper,
			&ra.guard.Unreachable, &ra.guard.Faulted, &ra.guard.Validators); err != nil {
			rows.Close()
			return nil, err
		}
		points[hash] = append(points[hash], ra)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// 4. the served shards at each reading, one per validator: the lower
	// bound's sum.
	type pointKey struct{ hash, at string }
	lower := map[pointKey]int{}
	rows, err = db.QueryContext(ctx, sel+`
		SELECT promise_hash, scheduled_at, SUM(r) FROM (
		  SELECT p.promise_hash AS promise_hash, p.scheduled_at AS scheduled_at, MAX(p.rows_returned) AS r
		  FROM probes p JOIN sel ON sel.promise_hash = p.promise_hash
		  WHERE p.phase = 'in_window' AND p.outcome = 'SERVED_OK'`+pb+`
		  GROUP BY p.promise_hash, p.scheduled_at, p.validator_address)
		GROUP BY promise_hash, scheduled_at`, pargs...)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var k pointKey
		var n int
		if err := rows.Scan(&k.hash, &k.at, &n); err != nil {
			rows.Close()
			return nil, err
		}
		lower[k] = n
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// 5. the validators that answered at each reading and the rows they
	// hold: what the rest, who might still have served, hold is the
	// difference to step 2. And how many of them endorse, for readingOf.
	answeredRows, answeredBy := map[pointKey]int{}, map[pointKey]int{}
	rows, err = db.QueryContext(ctx, sel+`
		SELECT promise_hash, scheduled_at, SUM(rc), SUM(e) FROM (
		  SELECT DISTINCT p.promise_hash AS promise_hash, p.scheduled_at AS scheduled_at, p.validator_address AS v, a.row_count AS rc,
		         (a.attested = 1 OR a.attested IS NULL) AS e
		  FROM probes p
		  JOIN assignments a ON a.promise_hash = p.promise_hash AND a.validator_address = p.validator_address
		  JOIN sel ON sel.promise_hash = p.promise_hash
		  WHERE `+answered+` AND a.row_count > 0`+pb+`)
		GROUP BY promise_hash, scheduled_at`, pargs...)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var k pointKey
		var n, v int
		if err := rows.Scan(&k.hash, &k.at, &n, &v); err != nil {
			rows.Close()
			return nil, err
		}
		answeredRows[k], answeredBy[k] = n, v
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	out := make(map[string]*reconstruct, len(facts))
	for hash, f := range facts {
		rc := &reconstruct{NeededRows: int(f.needed), TotalRows: int(f.total), WindowOver: pin.over(f.msu)}
		out[hash] = rc
		if f.assignment != "" || !f.known {
			rc.Status = "unknown"
			continue
		}
		pts := points[hash]
		for i := range pts {
			pts[i].endorsed = answeredBy[pointKey{hash, pts[i].at}]
		}
		pa, ok := readingOf(pts, endorsers[hash])
		idle := verdict.BlobNotRead
		if !rc.WindowOver {
			idle = verdict.BlobPending
		}
		if !ok {
			rc.Status = idle
			continue
		}
		rc.PointAt, rc.ProbedValidators, rc.ServedBy = pa.at, pa.asked, pa.served
		k := pointKey{hash, pa.at}
		needed := int(f.needed)
		potential := held[hash] - answeredRows[k]
		floor := lower[k] - f.excess // at most the distinct verified rows
		switch {
		case floor >= needed:
			rc.Status = verdict.BlobAvailable
		case pa.upper < needed && pa.guard.Reason() != "":
			// not Available, and set aside by the guard
			rc.Status = idle
		case pa.upper+potential < needed:
			rc.Status = verdict.BlobUnavailable
		case pa.upper < needed && floor+potential >= needed:
			// not Available, and the rows that might still have come back
			// could have made it so, even counted at the floor
			rc.Status = idle
		default:
			// Within the overlaps of the threshold (duplicated verified rows
			// put the distinct count anywhere between the bounds): only the
			// exact count can answer. Rare enough to pay for one blob at a
			// time.
			ref, err := s.reconstructable(ctx, hash, pin)
			if err != nil {
				return nil, fmt.Errorf("reconstructable %s: %w", hash, err)
			}
			out[hash] = ref
		}
	}
	return out, nil
}

// readingOf picks the reading a blob is judged at from its aggregated
// readings, as verdict.ReadingPoint does from rows: the end-of-window
// reading; or the newest point at which every one of the endorsers
// answered; or the newest point at which any validator answered.
func readingOf(pts []readingAgg, endorsers int) (readingAgg, bool) {
	for _, pa := range pts {
		if pa.end {
			return pa, true
		}
	}
	var complete, newest readingAgg
	whole, some := false, false
	for _, pa := range pts {
		if pa.answered == 0 {
			continue
		}
		if !some || pa.at > newest.at {
			newest, some = pa, true
		}
		if endorsers > 0 && pa.endorsed >= endorsers && (!whole || pa.at > complete.at) {
			complete, whole = pa, true
		}
	}
	switch {
	case whole:
		return complete, true
	case some:
		return newest, true
	}
	return readingAgg{}, false
}
