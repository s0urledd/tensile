// Package verdict derives the published figures from rows alone, with no
// database: the correlated-failure guard (suspect points) and the
// obligation buckets, over probe rows and publications as they sit in the
// JSONL record or a daily export. It is the second implementation of the
// rules the API evaluates in SQL, kept deliberately apart from it, so a
// third party holding the raw rows can reproduce every figure the site
// prints and the two can be checked against each other
// (sentinel-recompute; the API's tests run both over the same rows).
package verdict

import (
	"sort"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/probe"
	"github.com/plsgiveup/fibre/fibre-sentinel/internal/scan"
)

// The correlated-failure guard. At or above UnreachableThreshold of the
// validators probed at one schedule point being unreachable, or
// FaultThreshold of them faulting, the likeliest explanation is the
// observer's own side (its network, a stale pin, a broken coder) rather
// than that many independent operators at the same minute; such a point
// is suspect and every rate leaves its rows out. MinValidators is the
// floor under which a share is not a signal.
const (
	UnreachableThreshold = 0.5
	FaultThreshold       = 0.5
	MinValidators        = 3
)

// EndSegmentDivisor cuts the tail off a retention window: the final
// 1/EndSegmentDivisor of it, which is where a reading has to fall before an
// obligation counts as served.
//
// An obligation is a promise to hold a shard until must_serve_until, so the
// only reading that speaks to the whole promise is one taken near its end. A
// HEALTHY probe at the first schedule point says the shard was there minutes
// after settlement; it says nothing about the hours that follow, which are
// the part an early prune would take. Crediting it as served is what made the
// serve rate rise while this observer was down: the obligation kept its early
// verdict and every later slot was a gap, or — past the prober's backfill
// horizon — never written at all.
//
// So served needs a HEALTHY reading in the last quarter of the window. The
// published schedule puts its last in-window point at 92% of the window
// (internal/probe/schedule.go), so a validator that answers the last probe
// clears this with room; one this observer could not reach at the end does
// not, and lands in end_unobserved, which is published beside the rate and
// outside it.
//
// The cut is deliberately expressed against the promise's own settlement
// time and must_serve_until — both on the record, both in every export —
// rather than against the prober's schedule config, so a third party can
// redraw the same line from the rows alone and a later change to the
// fractions cannot move a verdict already published.
//
// It can only move an obligation out of served, never into broken: the worst
// this observer's blindness can now do to an operator is decline to vouch
// for them, and the figure that says how often that happened is printed
// beside the rate.
const EndSegmentDivisor = probe.EndSegmentDivisor

// EndSegment is the first moment of that tail for one promise.
func EndSegment(settled, mustServeUntil time.Time) time.Time {
	return mustServeUntil.Add(-time.Duration(float64(mustServeUntil.Sub(settled)) / EndSegmentDivisor))
}

// Row is what the rules need from one probe row.
type Row struct {
	PromiseHash    string
	Validator      string
	ScheduleLabel  string
	ScheduledAt    time.Time
	StartedAt      time.Time
	MustServeUntil time.Time
	Assigned       bool
	// Attested is the row's own attestation flag; false when unknown, as
	// the store's NULL is not 1.
	Attested       bool
	Phase          probe.Phase
	Classification probe.Classification
	Outcome        probe.Outcome
	// RetentionUnverified is set when this row's publication sits inside
	// an x/fibre params range the observer has not read every height of,
	// so the deadline the phase and the verdict were drawn against may not
	// be the one the server used. Derived from the records by
	// MarkRetentionUnverified, never from the row itself.
	RetentionUnverified bool
	TLSOK               bool
}

// EffectiveClass is the classification every rule below is built from: the
// row's own, except that a row whose deadline the observer cannot vouch for
// publishes no serve verdict. The SQL twin is rollup.EffectiveClass, and a
// test runs both over every cell of (classification, outcome, held).
//
// It is an override on the same row rather than a filter that removes it,
// which is what keeps the published response reconciling against itself:
// the row stays in every population it was in, under a different name.
func (r Row) EffectiveClass() probe.Classification {
	if r.RetentionUnverified && probe.DeadlineDerived(r.Classification, r.Outcome) {
		return probe.ClassRetentionUnverified
	}
	return r.Classification
}

// ObligationClass is the class a row counts as for its obligation: an
// end-of-window reading as a reader of the chain's own client meets it
// (probe.EndReadClass), then the same retention hold as EffectiveClass. The
// SQL twin is rollup.ObligationClass; the class tallies and the
// correlated-failure guard keep EffectiveClass.
func (r Row) ObligationClass() probe.Classification {
	c := r.Classification
	if r.ScheduleLabel == probe.EndReadLabel {
		c = probe.EndReadClass(c)
	}
	if r.RetentionUnverified && probe.DeadlineDerived(c, r.Outcome) {
		return probe.ClassRetentionUnverified
	}
	return c
}

// PromiseHeights is the pair a hold is derived from: the interval a
// publication's upload could have fallen in.
type PromiseHeights struct{ PromiseHeight, SettlementHeight int64 }

// MarkRetentionUnverified sets the hold on every row whose publication's
// upload interval overlaps a range that still withholds. This is the whole
// derivation, and it is the same one the collector applies in SQL, so a
// third party running this package over the export reaches the same rows.
//
// corrected is the set of range ids whose corrections have landed, from the
// range_corrected lines in corrections.jsonl. Verifying a range is not what
// releases it — applying what the verification proved is — so a verified
// range with no completion line still withholds here, exactly as it does in
// the store.
func MarkRetentionUnverified(rows []Row, pubs map[string]PromiseHeights, us []scan.ParamUncertainty, corrected map[string]bool) {
	held := map[string]bool{}
	for _, u := range us {
		if !u.Holds() || corrected[u.ID] {
			continue
		}
		for hash, p := range pubs {
			if u.Covers(p.PromiseHeight, p.SettlementHeight) {
				held[hash] = true
			}
		}
	}
	for i := range rows {
		rows[i].RetentionUnverified = held[rows[i].PromiseHash]
	}
}

// FromMeasurement reduces a measurement to a Row.
func FromMeasurement(m probe.Measurement) Row {
	return Row{
		PromiseHash: m.PromiseHash, Validator: m.ValidatorAddress, ScheduleLabel: m.ScheduleLabel,
		ScheduledAt: m.ScheduledAt, StartedAt: m.StartedAt, MustServeUntil: m.MustServeUntil,
		Assigned: m.Assigned, Attested: m.Attested && m.HasAttestation(),
		Phase: m.Phase, Classification: m.Classification, Outcome: m.Outcome, TLSOK: m.TLS.OK,
	}
}

// Window bounds a computation: rows started in [Start, End] and
// publications settled in it; End is also the as-of moment that decides
// pending. Start is ignored when All is set.
type Window struct {
	Start time.Time
	End   time.Time
	All   bool
}

func (w Window) holds(t time.Time) bool {
	if t.After(w.End) {
		return false
	}
	return w.All || !t.Before(w.Start)
}

// SuspectPoint is one schedule point the rates leave out.
type SuspectPoint struct {
	At          time.Time
	Label       string
	Validators  int
	Unreachable int
	Faulted     int
	Rows        int
	Reason      string
}

// SuspectPoints applies the correlated-failure guard: over assigned
// in-window rows started in the window, grouped by scheduled time, with
// more than one validator probed at the point. "Probed" is a row that
// carries a reachability verdict for the endpoint, which is the only kind
// of row that could land in either numerator — see noReachVerdict. A row
// that could not be in the numerator whatever happened at the point must
// not sit in the denominator either, or it drags the share down by its
// mere presence. Rows counts every row at the point, excluded ones
// included, because the exclusion removes them all.
func SuspectPoints(rows []Row, w Window) []SuspectPoint {
	type acc struct {
		label                  string
		vals, unreach, faulted map[string]bool
		n                      int
	}
	groups := map[time.Time]*acc{}
	for _, r := range rows {
		if !r.Assigned || r.Phase != probe.PhaseInWindow || !w.holds(r.StartedAt) {
			continue
		}
		k := r.ScheduledAt.UTC()
		g, ok := groups[k]
		if !ok {
			g = &acc{label: r.ScheduleLabel, vals: map[string]bool{}, unreach: map[string]bool{}, faulted: map[string]bool{}}
			groups[k] = g
		}
		g.n++
		cls := r.EffectiveClass()
		if noReachVerdict(cls) {
			continue
		}
		g.vals[r.Validator] = true
		switch cls {
		case probe.ClassUnreachable:
			g.unreach[r.Validator] = true
		case probe.ClassFault:
			g.faulted[r.Validator] = true
		}
	}
	var out []SuspectPoint
	for at, g := range groups {
		all := len(g.vals)
		if all <= 1 {
			continue
		}
		bad, faulted := len(g.unreach), len(g.faulted)
		reason := ""
		if float64(bad)/float64(all) >= UnreachableThreshold && bad >= MinValidators {
			reason = "unreachable"
		}
		if float64(faulted)/float64(all) >= FaultThreshold && faulted >= MinValidators {
			if reason != "" {
				reason += ","
			}
			reason += "fault"
		}
		if reason == "" {
			continue
		}
		out = append(out, SuspectPoint{At: at, Label: g.label, Validators: all, Unreachable: bad, Faulted: faulted, Rows: g.n, Reason: reason})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].At.Before(out[j].At) })
	return out
}

// Obligations are the buckets one validator's (or the network's) proven
// obligations in a window fall into. See docs/verdicts.md, "Obligations".
type Obligations struct {
	Total                 int64 `json:"total"`
	Served                int64 `json:"served"`
	Broken                int64 `json:"broken"`
	EndUnobserved         int64 `json:"end_unobserved"`
	HeldParamUnverified   int64 `json:"held_param_unverified"`
	Unobserved            int64 `json:"unobserved"`
	UnobservedReachable   int64 `json:"unobserved_reachable"`
	UnobservedUnreachable int64 `json:"unobserved_unreachable"`
	UnobservedNotProbed   int64 `json:"unobserved_not_probed"`
	Pending               int64 `json:"pending"`
}

func (o *Obligations) add(x Obligations) {
	o.Total += x.Total
	o.Served += x.Served
	o.Broken += x.Broken
	o.EndUnobserved += x.EndUnobserved
	o.HeldParamUnverified += x.HeldParamUnverified
	o.Unobserved += x.Unobserved
	o.UnobservedReachable += x.UnobservedReachable
	o.UnobservedUnreachable += x.UnobservedUnreachable
	o.UnobservedNotProbed += x.UnobservedNotProbed
	o.Pending += x.Pending
}

// Rate is served over served plus broken; ok is false with nothing rated.
func (o Obligations) Rate() (v float64, ok bool) {
	if o.Served+o.Broken == 0 {
		return 0, false
	}
	return float64(o.Served) / float64(o.Served+o.Broken), true
}

func isGap(c probe.Classification) bool {
	return c == probe.ClassNotProbed || c == probe.ClassProbeError
}

// GuardSilentClasses is every classification that leaves the observer
// without a reachability verdict for the endpoint at that point, so that
// the rollup's SQL twin can spell the same list into its query and a test
// can hold the two to it.
//
// The guard asks whether many validators failed at once. A row can answer
// that only if it could itself have come back UNREACHABLE or FAULT:
//
//   - NOT_PROBED, PROBE_ERROR: the observer never asked, or could not carry
//     the probe out.
//   - NOT_REGISTERED: no reachable Fibre host was registered, so no
//     connection was attempted.
//   - UNATTESTED: Classify returns it before it looks at reachability at
//     all, so the row reads UNATTESTED whether the endpoint answered or
//     refused. On mocha a publisher stops collecting at two thirds of
//     stake, which leaves roughly a third of assigned rows unattested at
//     every point — enough, left in the denominator, to hold the guard
//     below its threshold through a real outage.
//   - RETENTION_UNVERIFIED: the override replaces exactly HEALTHY and
//     FAULT, so a held row can never be the FAULT in the numerator, and it
//     was never going to be the UNREACHABLE either.
//
// Everything else kept in the denominator means a connection was attempted
// and the endpoint answered or refused: an identity failure, a throttle or
// a server error is positive evidence that the network was up, so it
// belongs there.
var GuardSilentClasses = []probe.Classification{
	probe.ClassNotProbed, probe.ClassProbeError, probe.ClassNotRegistered, probe.ClassUnattested,
	probe.ClassRetentionUnverified,
}

func noReachVerdict(c probe.Classification) bool {
	for _, s := range GuardSilentClasses {
		if c == s {
			return true
		}
	}
	return false
}

// ComputeObligations buckets every proven obligation: an assigned,
// attested (validator, promise) pair whose promise settled in the window,
// judged over its in-window rows started by the window's end, at schedule
// points that are not suspect. settled maps promise hash to settlement
// time; a row whose promise is not in it is left out, as the SQL join
// leaves it out. The network total is the sum over validators.
func ComputeObligations(rows []Row, settled map[string]time.Time, w Window, suspect []SuspectPoint) (Obligations, map[string]Obligations) {
	sus := map[time.Time]bool{}
	for _, p := range suspect {
		sus[p.At.UTC()] = true
	}
	type key struct{ validator, promise string }
	type obl struct {
		faults, healthy, lateHealthy, held, attempted, reached int64
		pending                                                bool
		last                                                   *Row
	}
	obls := map[key]*obl{}
	for i := range rows {
		r := &rows[i]
		st, ok := settled[r.PromiseHash]
		if !ok || !w.holds(st) || r.StartedAt.After(w.End) {
			continue
		}
		if !r.Assigned || !r.Attested || r.Phase != probe.PhaseInWindow || sus[r.ScheduledAt.UTC()] {
			continue
		}
		k := key{r.Validator, r.PromiseHash}
		o, ok := obls[k]
		if !ok {
			o = &obl{}
			obls[k] = o
		}
		cls := r.ObligationClass()
		switch cls {
		case probe.ClassFault:
			o.faults++
		case probe.ClassHealthy:
			o.healthy++
			if !r.ScheduledAt.Before(EndSegment(st, r.MustServeUntil)) {
				o.lateHealthy++
			}
		case probe.ClassRetentionUnverified:
			o.held++
		}
		if !isGap(cls) {
			o.attempted++
			if r.TLSOK {
				o.reached++
			}
		}
		if r.MustServeUntil.After(w.End) {
			o.pending = true
		}
		// the newest row: a verdict row before a gap row, then the latest
		// schedule point, then the latest start
		if o.last == nil || newer(r, o.last) {
			o.last = r
		}
	}
	byVal := map[string]Obligations{}
	var net Obligations
	for k, o := range obls {
		var b Obligations
		b.Total = 1
		last := o.last.ObligationClass()
		switch {
		case o.pending:
			b.Pending = 1
		case o.faults > 0:
			b.Broken = 1
		case last == probe.ClassHealthy && o.lateHealthy > 0:
			b.Served = 1
		case o.healthy > 0:
			b.EndUnobserved = 1
		// An obligation whose only serve evidence was withheld is not one
		// this observer failed to observe: it looked, and cannot speak for
		// what it saw. Filing it under unobserved would report the
		// observer's own uncertainty about the deadline as a gap in
		// coverage.
		case o.held > 0:
			b.HeldParamUnverified = 1
		case o.reached > 0:
			b.UnobservedReachable = 1
		case o.attempted > 0:
			b.UnobservedUnreachable = 1
		default:
			b.UnobservedNotProbed = 1
		}
		b.Unobserved = b.UnobservedReachable + b.UnobservedUnreachable + b.UnobservedNotProbed
		v := byVal[k.validator]
		v.add(b)
		byVal[k.validator] = v
		net.add(b)
	}
	return net, byVal
}

// newer orders rows as the API's ROW_NUMBER does: a gap row (NOT_PROBED,
// PROBE_ERROR) never outranks a verdict row; then the later schedule
// point; then the later start.
func newer(a, b *Row) bool {
	ga, gb := isGap(a.Classification), isGap(b.Classification)
	if ga != gb {
		return !ga
	}
	if !a.ScheduledAt.Equal(b.ScheduledAt) {
		return a.ScheduledAt.After(b.ScheduledAt)
	}
	return a.StartedAt.After(b.StartedAt)
}

// Candidate is a settled promise over a commitment with the rows it
// assigns one validator, for LateShadow.
type Candidate struct {
	PromiseHash    string
	Commitment     string
	SettlementTime time.Time
	MustServeUntil time.Time
	Rows           []int
}

// LateShadow draws the deferred verdict on a row that returned genuine
// rows no promise assigned at the probe, the way the collector does once
// the scanner has read past probe time + timeout: SHADOWED_SHARD with the
// owning promise when a candidate settled by then, alive at the probe
// (must_serve_until + tolerance after it), assigns exactly the returned
// rows; UNMATCHED_GENUINE otherwise, held out of the rate, because an
// upload for a promise that never settled can answer under hash-order
// serving and no fault is supported. ok is false when the frontier has not
// reached the bound, or the row carries no indices, or timeout is unknown.
func LateShadow(got []uint32, probeAt, frontier time.Time, timeout, tolerance time.Duration, cands []Candidate) (cls probe.Classification, shadowedBy string, ok bool) {
	if timeout <= 0 || len(got) == 0 {
		return "", "", false
	}
	deadline := probeAt.Add(timeout)
	if frontier.Before(deadline) {
		return "", "", false
	}
	want := map[int]int{}
	for _, g := range got {
		want[int(g)]++
	}
	sort.Slice(cands, func(i, j int) bool { return cands[i].PromiseHash < cands[j].PromiseHash })
	unrecorded := false
	for _, c := range cands {
		if c.SettlementTime.After(deadline) || c.MustServeUntil.Add(tolerance).Before(probeAt) {
			continue
		}
		if len(c.Rows) == 0 {
			// A candidate in range whose assignment rows were never
			// recorded (a scan without -rows) cannot be matched or ruled
			// out; the SQL twin draws PROBE_ERROR for good, and so does
			// this one, after the recorded candidates have had their turn.
			unrecorded = true
			continue
		}
		if len(c.Rows) != len(got) {
			continue
		}
		seen := map[int]int{}
		for _, r := range c.Rows {
			seen[r]++
		}
		same := true
		for r, n := range want {
			if seen[r] != n {
				same = false
				break
			}
		}
		if same {
			return probe.ClassShadowedShard, c.PromiseHash, true
		}
	}
	if unrecorded {
		return probe.ClassProbeError, "", true
	}
	return probe.ClassUnmatchedGenuine, "", true
}
