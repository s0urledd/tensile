package api

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/observer/rollup"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
)

// Batched reconstructability.
//
// reconstructable answers for one publication in five queries, which is right
// for the blob detail page and wrong for everything else: the network summary
// asked it for up to reconstructSample publications and the blob list for up to
// 500, so a single /v1/network was ten thousand queries and two thousand JSON
// parses. Measured on a store with 260 publications and 84,000 probes it took
// 2.9s, uncached, and the dashboard polls it every thirty seconds per viewer.
//
// This computes the same verdict for a whole selection in four queries plus a
// rare per-blob fallback. It is deliberately a second implementation rather
// than a replacement: reconstructable stays as the reference, and
// TestReconstructBatchMatchesReference asserts the two agree on every
// publication of a fixture that exercises every status.
//
// The saving that matters is not the query count but the row lists. The union
// of served row indices is what the verdict turns on, and unioning it means
// parsing a JSON array per validator per blob. It is almost always avoidable:
// the publication already records sigma_rows (the sum of every validator's row
// count) and distinct_rows (how many indices that covers), so
//
//	excess := sigma - distinct
//
// is the total number of duplicate index occurrences across the whole
// assignment, and for any subset S of validators
//
//	sum(row_count for S) - excess  <=  |union(S)|  <=  sum(row_count for S)
//
// because the duplicates within S cannot exceed the duplicates overall. The
// verdict only needs to know which side of needed_rows the union falls, so
// whenever both bounds land on the same side — which is every blob that is not
// within `excess` rows of the threshold, so nearly all of them — the arithmetic
// settles it and no row list is read. Only the narrow ambiguous band falls back
// to the exact union, one blob at a time.

// blobSel is the selection every batch query joins against: the same ordering
// and limit blobRows applies, expressed once as a CTE so the database does the
// join instead of an IN list with two thousand parameters.
func blobSel(where string, limit int) string {
	q := `WITH sel AS (SELECT promise_hash FROM publications`
	if where != "" {
		q += " WHERE " + where
	}
	return q + ` ORDER BY settlement_height DESC, settlement_tx_index DESC LIMIT ` + strconv.Itoa(limit) + `) `
}

// blobFacts is what the publications row contributes to the verdict.
type blobFacts struct {
	assigned int
	needed   int64
	total    int64
	// excess is sigma_rows - distinct_rows: how many row indices the
	// assignment hands to more than one validator, counted with multiplicity.
	excess int
	known  bool // needed/total were recorded
}

// pointAgg is one schedule point of one publication, already aggregated.
type pointAgg struct {
	at, label, msu string
	probed         int // distinct validators with a real result
	probedAtt      int // of those, how many the promise proves were obliged
	servedBy       int // distinct validators that served correctly
	servedAtt      int // of those, how many the promise proves were obliged
	sumRows        int // sum of their assigned row counts
	nullRows       int // how many of them have no recorded row list
}

// asOfPin bounds a reconstructability pass in time. The zero value is live.
//
// Everything else on a pinned /v1/network answer is bounded by
// started_at <= as_of, and AsOfNote tells the reader so. The reconstructability
// block was not: its publication selection was pinned but its two probe
// queries had no time bound at all, so a blob that was still in flight at the
// pinned moment — which, with a four-hour retention window, is every blob for
// four hours after it settles — was judged with evidence that did not exist
// yet. The honest answer at that moment is "pending"; the answer given was
// today's, and it could say a blob was not fully served. A verdict about a
// moment must be drawn from what was known at that moment.
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

// pinArgs is the pin's argument list for a clause built with bound(_, nil).
func pinArgs(p asOfPin) []any {
	if p.at == "" {
		return nil
	}
	return []any{p.at}
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

// reconstructBatch returns the verdict for every publication in the selection,
// keyed by promise hash. A publication with no in-window probe at all is absent
// from the map, matching reconstructable's "unknown" for the same case.
//
// Only the status is computed. The network summary is the one caller, it reads
// nothing else, and leaving ServedRows at zero is what lets the bounds replace
// the row lists. A list or detail page, which does publish served_distinct_rows,
// uses the reference.
func (s *Server) reconstructBatch(ctx context.Context, where string, limit int, pin asOfPin, args ...any) (map[string]*reconstruct, error) {
	db := s.st.DB()
	sel := blobSel(where, limit)

	// 1. the publications themselves.
	facts := map[string]blobFacts{}
	rows, err := db.QueryContext(ctx, sel+`
		SELECT p.promise_hash, p.validators_with_rows, p.sigma_rows, p.distinct_rows,
		       json_extract(p.raw_json,'$.assignment.protocol_params.original_rows'),
		       json_extract(p.raw_json,'$.assignment.protocol_params.total_rows')
		FROM publications p JOIN sel ON sel.promise_hash = p.promise_hash`, args...)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var hash string
		var assigned, sigma, distinct int
		var needed, total sql.NullInt64
		if err := rows.Scan(&hash, &assigned, &sigma, &distinct, &needed, &total); err != nil {
			rows.Close()
			return nil, err
		}
		excess := sigma - distinct
		if excess < 0 {
			excess = 0
		}
		facts[hash] = blobFacts{assigned: assigned, needed: needed.Int64, total: total.Int64,
			excess: excess, known: needed.Valid && needed.Int64 > 0}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(facts) == 0 {
		return map[string]*reconstruct{}, nil
	}

	// 2. how many assigned validators the promise proves stored the blob.
	// COUNT(attested) skips NULLs, so a zero count means the publication
	// predates signature verification and attestation says nothing.
	type att struct{ known, attested int }
	atts := map[string]att{}
	rows, err = db.QueryContext(ctx, sel+`
		SELECT a.promise_hash, COUNT(a.attested), COALESCE(SUM(a.attested), 0)
		FROM assignments a JOIN sel ON sel.promise_hash = a.promise_hash
		WHERE a.row_count > 0 GROUP BY a.promise_hash`, args...)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var hash string
		var known, attested int
		if err := rows.Scan(&hash, &known, &attested); err != nil {
			rows.Close()
			return nil, err
		}
		atts[hash] = att{known: known, attested: attested}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// 3. every in-window point with a real result, newest first per blob,
	// less the points the correlated-failure guard calls suspect: the same
	// tally rollup.SuspectPoints makes, over this blob's rows, which is what
	// reconstructable excludes. The rows the WHERE drops are guard-silent
	// classes and count toward none of the three.
	pb, pargs := pin.bound("p", args)
	cls := rollup.EffectiveClass("p")
	points := map[string][]pointAgg{}
	rows, err = db.QueryContext(ctx, sel+`
		SELECT p.promise_hash, p.scheduled_at, p.schedule_label, p.must_serve_until,
		       COUNT(DISTINCT p.validator_address),
		       COUNT(DISTINCT CASE WHEN p.attested = 1 THEN p.validator_address END),
		       COUNT(DISTINCT CASE WHEN `+cls+` = 'UNREACHABLE' THEN p.validator_address END),
		       COUNT(DISTINCT CASE WHEN `+cls+` = 'FAULT' THEN p.validator_address END),
		       COUNT(DISTINCT CASE WHEN `+cls+` NOT IN `+rollup.GuardSilentSQL+` THEN p.validator_address END)
		FROM probes p JOIN sel ON sel.promise_hash = p.promise_hash
		WHERE p.phase = 'in_window' AND p.assigned = 1
		  AND p.classification NOT IN ('NOT_PROBED','PROBE_ERROR')`+pb+`
		GROUP BY p.promise_hash, p.scheduled_at
		ORDER BY p.promise_hash, p.scheduled_at DESC`, pargs...)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var hash string
		var pa pointAgg
		var guard rollup.Point
		if err := rows.Scan(&hash, &pa.at, &pa.label, &pa.msu, &pa.probed, &pa.probedAtt, &guard.Unreachable, &guard.Faulted, &guard.Validators); err != nil {
			rows.Close()
			return nil, err
		}
		if guard.Reason() != "" {
			continue
		}
		points[hash] = append(points[hash], pa)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// 4. what was actually served at each point. The inner DISTINCT is what
	// makes the sums right: a validator probed twice at the same scheduled_at
	// must contribute its rows once, which is the same reason the reference
	// selects DISTINCT validator_address.
	type servedKey struct{ hash, at string }
	served := map[servedKey]pointAgg{}
	rows, err = db.QueryContext(ctx, sel+`
		SELECT promise_hash, scheduled_at, COUNT(*), COALESCE(SUM(row_count),0),
		       COALESCE(SUM(att),0), COALESCE(SUM(no_rows),0)
		FROM (
		  SELECT DISTINCT p.promise_hash AS promise_hash, p.scheduled_at AS scheduled_at,
		         p.validator_address AS validator_address,
		         a.row_count AS row_count,
		         CASE WHEN a.attested = 1 THEN 1 ELSE 0 END AS att,
		         CASE WHEN a.rows_json IS NULL THEN 1 ELSE 0 END AS no_rows
		  FROM probes p
		  JOIN assignments a ON a.promise_hash = p.promise_hash AND a.validator_address = p.validator_address
		  JOIN sel ON sel.promise_hash = p.promise_hash
		  WHERE p.phase = 'in_window' AND p.assigned = 1 AND p.outcome = 'SERVED_OK'`+pb+`
		)
		GROUP BY promise_hash, scheduled_at`, pargs...)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var k servedKey
		var pa pointAgg
		if err := rows.Scan(&k.hash, &k.at, &pa.servedBy, &pa.sumRows, &pa.servedAtt, &pa.nullRows); err != nil {
			rows.Close()
			return nil, err
		}
		served[k] = pa
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	out := make(map[string]*reconstruct, len(facts))
	for hash, f := range facts {
		pts := points[hash]
		if len(pts) == 0 {
			// No in-window point with a real result: nothing to say yet.
			out[hash] = &reconstruct{Status: "unknown"}
			continue
		}
		// The newest point is the default; the newest COMPLETE point wins.
		// Scanning newest first is what makes "complete" mean "the most recent
		// moment at which every assigned validator had been heard from".
		a := atts[hash]
		chosen, complete := pts[0], false
		for _, pa := range pts {
			if pointComplete(pa.probed, pa.probedAtt, f.assigned, a.known > 0, a.attested) {
				chosen, complete = pa, true
				break
			}
		}
		sv := served[servedKey{hash, chosen.at}]

		windowOver := pin.over(chosen.msu)

		rc := &reconstruct{
			Point: chosen.label, PointAt: chosen.at, WindowOver: windowOver,
			NeededRows: int(f.needed), TotalRows: int(f.total),
			ServedBy: sv.servedBy, AssignedTotal: f.assigned,
			ProbedValidators: chosen.probed, AttestedValidators: a.attested,
			AttestationKnown: a.known > 0, ServedByAttested: sv.servedAtt,
		}

		// "yes" means nobody the promise proves owed this blob failed to serve
		// it. Without proof of storage a quiet validator is not a fault, so it
		// must not demote a blob whose rows all came back.
		whole := sv.servedBy == f.assigned
		if rc.AttestationKnown {
			whole = sv.servedAtt == a.attested
		}

		// A served validator with no usable row list, or a publication with no
		// recorded protocol params: the reference calls this unknown rather
		// than guessing, and so does this.
		if sv.nullRows > 0 || !f.known {
			rc.Status = "unknown"
			out[hash] = rc
			continue
		}
		if !complete {
			rc.Status = "pending"
			out[hash] = rc
			continue
		}

		enough := false
		lo, hi := sv.sumRows-f.excess, sv.sumRows
		switch {
		case hi < int(f.needed):
			enough = false
		case lo >= int(f.needed):
			enough = true
		default:
			// Within `excess` rows of the threshold: the bounds straddle it, so
			// only the exact union can answer. Rare enough to pay for one blob
			// at a time.
			ref, err := s.reconstructable(ctx, hash, pin)
			if err != nil {
				return nil, fmt.Errorf("reconstructable %s: %w", hash, err)
			}
			out[hash] = ref
			continue
		}

		switch {
		case enough && whole:
			rc.Status = "yes"
		case enough:
			rc.Status = "degraded"
		default:
			rc.Status = "no"
		}
		out[hash] = rc
	}
	return out, nil
}
