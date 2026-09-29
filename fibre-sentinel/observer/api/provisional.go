package api

// Provisional not-served readings, and the network reference beside a
// validator's rate.
//
// A reading that counts as not served (rollup.CountedClass FAULT: the blob
// was Unavailable and the validator's rows did not come back) and is
// younger than verdict.FaultSettling can still be withdrawn by evidence
// already on its way (a params range the scanner has not noticed yet). This
// file labels it: a reading carries provisional: true, and a not-served
// obligation whose every such reading is that young is counted in
// provisional_faults beside the figure it is already part of.
//
// Why the headline keeps them. Three options were weighed against the
// methodology's own rules:
//
//   - Hold provisional faults out of the rate. Rejected: it withholds the
//     accusation and not the credit, and docs/verdicts.md refuses exactly
//     that everywhere else (RETENTION_UNVERIFIED, UNATTESTED) because it
//     raises every rate it touches. A validator failing now would read
//     cleaner for half an hour than one that failed yesterday.
//   - Hold every obligation decided in the last half hour, serves and
//     faults alike. Symmetric, but it is only a longer "pending": the
//     headline lags by the settling period for everyone, and the thing a
//     reader wanted to know — this fault is fresh, it can still move — is
//     no longer shown at all.
//   - Count it, flag it. Chosen. A not-served count rests on the blob's
//     whole reading (it could not be reconstructed at that minute), and the
//     automatic withdrawal paths already act on the store — a params range
//     withholds the rows in the transaction that records it and moves the
//     snapshot revision — so a reading that is withdrawn leaves the
//     headline by itself. The flag says which part of the figure can still
//     move and until when.
//
// The label decays with the clock, not with a recomputation: every
// provisional count carries `until`, the moment its youngest fault settles,
// so a snapshot served from cache still tells the page when to drop the
// badge.

import (
	"context"
	"sort"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/verdict"
)

// provisionalFaults is the provisional part of a broken-obligation count.
type provisionalFaults struct {
	// Obligations is how many of obligations.broken rest only on faults
	// younger than the settling period. They are already in broken.
	Obligations int64 `json:"obligations"`
	// Until is when the youngest of them settles, after which the count is
	// final whatever the snapshot says.
	Until string `json:"until"`
	// SettlingSeconds is verdict.FaultSettling, published so it is not a
	// hidden constant.
	SettlingSeconds int64  `json:"settling_seconds"`
	Note            string `json:"note"`
}

const provisionalNote = "Counted in broken and in the rate, and final at `until` unless an x/fibre params change not reconciled yet withdraws it."

// provisionalCutoff is the started_at bound above which a FAULT is
// provisional at now.
func provisionalCutoff(now time.Time) string { return store.TS(now.Add(-verdict.FaultSettling)) }

// isProvisional says whether a reading that counts as not served
// (rollup.CountedClass FAULT) is still settling.
func isProvisional(counted, startedAt string, now time.Time) bool {
	return counted == "FAULT" && startedAt > provisionalCutoff(now)
}

// provisionalByValidator counts, per validator, the broken obligations of the
// window whose earliest FAULT is still settling at now. It reads the same
// buckets the obligation counts do, with the same filter, so the count is a
// subset of obligations.broken by construction.
func (s *Server) provisionalByValidator(ctx context.Context, win Window, now time.Time, extra string, extraArgs ...any) (map[string]*provisionalFaults, error) {
	out := map[string]*provisionalFaults{}
	if win.End.Before(now.Add(-verdict.FaultSettling)) {
		return out, nil // no row this window admits can still be settling
	}
	args := append(s.obligationArgs(win, extraArgs...), provisionalCutoff(now))
	// obligationBuckets leaves its inner FROM ( open for the caller's
	// filters, hence the ")" before its GROUP BY, as in obligationsWhere.
	rows, err := s.st.DB().QueryContext(ctx, `SELECT validator_address, COUNT(*), MAX(first_fault) FROM (`+obligationBuckets+extra+`)
			GROUP BY validator_address, promise_hash)
		WHERE NOT pending AND faults > 0 AND first_fault > ?
		GROUP BY validator_address`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var addr, youngest string
		var n int64
		if err := rows.Scan(&addr, &n, &youngest); err != nil {
			return nil, err
		}
		out[addr] = newProvisional(n, youngest)
	}
	return out, rows.Err()
}

func newProvisional(n int64, youngest string) *provisionalFaults {
	until := youngest
	if t, err := time.Parse(store.TimeLayout, youngest); err == nil {
		until = t.Add(verdict.FaultSettling).UTC().Format(time.RFC3339)
	}
	return &provisionalFaults{Obligations: n, Until: until, SettlingSeconds: int64(verdict.FaultSettling / time.Second), Note: provisionalNote}
}

// provisionalTotal folds the per-validator counts into one for the network
// row: the sum, settling when the youngest does.
func provisionalTotal(m map[string]*provisionalFaults) *provisionalFaults {
	var total *provisionalFaults
	for _, p := range m {
		if total == nil {
			c := *p
			total = &c
			continue
		}
		total.Obligations += p.Obligations
		if p.Until > total.Until {
			total.Until = p.Until
		}
	}
	return total
}

// networkReference is the network's figure over the same window from the
// same vantage, for the validator page's service rate: the median of the
// validators' own rates and the pooled rate of every obligation.
type networkReference struct {
	Window Window `json:"window"`
	// Median is the median service rate over validators with at least
	// MinRated decided obligations; nil when there are none. Every
	// validator's rate counts once, so one large validator cannot move it.
	Median *float64 `json:"median_rate"`
	// Validators is how many rates the median is drawn from.
	Validators int `json:"validators"`
	// MinRated is the floor below which a rate is not ranked anywhere on
	// the site (web MIN_RATED); a rate from a handful of obligations is not
	// a reference point.
	MinRated int64 `json:"min_rated"`
	// Pooled is served / (served + broken) over every validator's
	// obligations together: the network's own rate.
	Pooled     Rate   `json:"pooled_rate"`
	ComputedAt string `json:"computed_at"`
	Note       string `json:"note"`
}

// networkReferenceMinRated mirrors the site's MIN_RATED: below 20 decided
// obligations a rate is printed but never ranked.
const networkReferenceMinRated = 20

const networkReferenceNote = "The same window, the same vantage and the same obligation rule as this validator's rate. " +
	"The median is over validators with at least min_rated decided obligations; the pooled rate is every obligation together."

// networkReferenceFrom computes the reference from validator rows.
func networkReferenceFrom(win Window, rows []validatorRow, at time.Time) *networkReference {
	ref := &networkReference{Window: win, MinRated: networkReferenceMinRated, ComputedAt: at.UTC().Format(time.RFC3339), Note: networkReferenceNote}
	var rates []float64
	var served, decided int64
	for _, v := range rows {
		o := v.Obligations
		d := o.Served + o.Broken
		served += o.Served
		decided += d
		if d >= networkReferenceMinRated {
			rates = append(rates, float64(o.Served)/float64(d))
		}
	}
	ref.Pooled = rate(served, decided)
	ref.Validators = len(rates)
	if len(rates) > 0 {
		sort.Float64s(rates)
		m := rates[len(rates)/2]
		if len(rates)%2 == 0 {
			m = (rates[len(rates)/2-1] + rates[len(rates)/2]) / 2
		}
		ref.Median = &m
	}
	return ref
}

// networkReference answers from the validators snapshot for a live window
// (the same rows /v1/validators serves, so the page and the table agree).
// A pinned window has no snapshot and computing every validator's row per
// page view is what the as_of limiter exists to prevent, so it is omitted
// there rather than computed. So is a live window whose snapshot is still
// being computed: the validator page does not wait for one.
func (s *Server) networkReference(win Window) *networkReference {
	if win.AsOf {
		return nil
	}
	snap, at, _, ok := s.vals.peek(s.logf(), win)
	if !ok {
		return nil
	}
	return networkReferenceFrom(snap.Window, snap.Rows, at)
}
