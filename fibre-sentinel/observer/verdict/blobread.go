package verdict

import (
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/probe"
	"github.com/plsgiveup/fibre/fibre-sentinel/internal/scan"
)

// A blob is read the way celestia-app's Fibre client downloads one: the
// endorsing validators are asked for their shards until enough distinct rows,
// each verified against the commitment, are in hand to reconstruct it
// (Needed: original_rows, 4096 for blob version 0). What that reading came
// to decides what counts:
//
//   - Available: at least Needed distinct verified rows came back, from any
//     validator, whatever else happened. Verified rows cannot come from this
//     observer's own trouble, so nothing sets this aside.
//   - Unavailable: fewer came back, and even every row of the endorsing
//     validators that gave no answer of their own (none asked, none reached
//     by this observer, a rate limit) could not have made up the difference:
//     in the client's words, "some rows were retrieved, but not enough to
//     reconstruct", or "no rows were retrieved".
//   - otherwise the reading says nothing yet (pending, while the window is
//     open) or nothing at all (not read by Tensile).
//
// A validator's failure to hand over its rows counts against it only on an
// Unavailable blob: when the blob could be rebuilt without it, a reader was
// not left without the data, and a failure then is as likely this
// observer's own path as the validator. A validator whose rows came back is
// served either way. The SQL twin is rollup.CountedClass.

// NotCounted is the class a row counts as when it left the reader without
// rows on a blob that was Available all the same: counted neither for nor
// against the validator.
const NotCounted probe.Classification = "NOT_COUNTED"

// The blob statuses, as /v1/blobs publishes them.
const (
	BlobAvailable   = "yes"
	BlobUnavailable = "no"
	BlobPending     = "pending"
	BlobNotRead     = "not_read"
)

// BlobFacts is what the rule needs from a publication.
type BlobFacts struct {
	// Needed is how many distinct rows reconstruct the blob
	// (protocol_params.original_rows); 0 when the record does not say.
	Needed int
	// Excess is sigma_rows - distinct_rows: how many assigned row indices
	// are held by more than one validator, counted with multiplicity. It
	// turns the sum of served shards into a lower bound on the rows they
	// cover.
	Excess int
	// Endorsed maps every endorsing validator (rows assigned, and a verified
	// signature on the promise, or a record from before signatures were
	// verified) to the rows it holds.
	Endorsed map[string]int
}

// Blobs is BlobFacts by promise hash.
type Blobs map[string]BlobFacts

// FactsOf reads the facts from a publication record.
func FactsOf(p scan.Publication) BlobFacts {
	f := BlobFacts{Needed: p.Assignment.ProtocolParams.OriginalRows, Excess: p.Assignment.Sigma - p.Assignment.Distinct, Endorsed: map[string]int{}}
	if f.Excess < 0 {
		f.Excess = 0
	}
	for _, v := range p.Assignment.Validators {
		if v.RowCount > 0 && (v.Attested || !p.HasAttestation()) {
			f.Endorsed[v.Address] = v.RowCount
		}
	}
	return f
}

// BlobsOf is FactsOf for every publication.
func BlobsOf(pubs []scan.Publication) Blobs {
	out := make(Blobs, len(pubs))
	for _, p := range pubs {
		out[p.PromiseHash] = FactsOf(p)
	}
	return out
}

// Answered reports whether a row is the validator's own answer at a
// reading: in the window, and neither this observer's gap (NOT_PROBED,
// PROBE_ERROR) nor a rate limit, which may be this observer's own request
// rate. A validator without such a row could still have served its rows,
// so it is counted among those that might have made the blob readable.
func Answered(r Row) bool {
	if r.Phase != probe.PhaseInWindow {
		return false
	}
	switch r.Classification {
	case probe.ClassNotProbed, probe.ClassProbeError, probe.ClassThrottled:
		return false
	}
	return true
}

// Reading is what the rows of one reading of a blob come to, before the
// correlated-failure guard and the clock.
type Reading struct {
	// Have is the distinct row indices that came back verified.
	Have int
	// Lower and Upper bound Have from the counts alone: the served shards
	// less the assignment's overlaps, and every verified row counted.
	Lower, Upper int
	// Potential is the rows the endorsing validators without an answer of
	// their own hold.
	Potential int
	// IndicesMissing: a verified row carries no index list, so Have is a
	// floor rather than a count.
	IndicesMissing bool
	// Asked is the validators the reading asked (a row other than this
	// observer's own gap, NOT_PROBED or PROBE_ERROR); Served, those whose
	// rows verified.
	Asked, Served int
	Needed        int
}

// ReadingOf reduces the rows of one reading (one promise, one scheduled
// time; every vantage) with the publication's facts.
func ReadingOf(point []Row, f BlobFacts) Reading {
	rd := Reading{Needed: f.Needed}
	seen := map[uint32]struct{}{}
	asked := map[string]bool{}
	served := map[string]bool{}
	answered := map[string]bool{}
	servedOK := map[string]int{}
	for _, r := range point {
		if r.Classification != probe.ClassNotProbed && r.Classification != probe.ClassProbeError {
			asked[r.Validator] = true
		}
		if Answered(r) {
			answered[r.Validator] = true
		}
		if r.Phase != probe.PhaseInWindow {
			continue
		}
		if r.Outcome == probe.OutcomeServedOK && r.RowsReturned > servedOK[r.Validator] {
			servedOK[r.Validator] = r.RowsReturned
		}
		if !r.CommitmentVerified {
			continue
		}
		served[r.Validator] = true
		rd.Upper += r.RowsReturned
		if len(r.RowIndices) == 0 && r.RowsReturned > 0 {
			rd.IndicesMissing = true
		}
		for _, i := range r.RowIndices {
			seen[i] = struct{}{}
		}
	}
	rd.Have = len(seen)
	for _, n := range servedOK {
		rd.Lower += n
	}
	rd.Lower -= f.Excess
	for v, n := range f.Endorsed {
		if !answered[v] {
			rd.Potential += n
		}
	}
	rd.Asked, rd.Served = len(asked), len(served)
	return rd
}

// Available reports whether the rows that came back reconstruct the blob.
func (rd Reading) Available() bool {
	if rd.Needed <= 0 {
		return false
	}
	if rd.Lower >= rd.Needed {
		return true
	}
	return !rd.IndicesMissing && rd.Have >= rd.Needed
}

// Unavailable reports whether the blob could not be reconstructed from this
// reading, and could not have been even had every endorsing validator
// without an answer of its own served. The order of the tests is the SQL
// twin's (rollup.CountedClass), so the two agree on every row.
func (rd Reading) Unavailable() bool {
	switch {
	case rd.Needed <= 0:
		return false
	case rd.Lower >= rd.Needed:
		return false
	case rd.Upper+rd.Potential < rd.Needed:
		return true
	case rd.IndicesMissing:
		return false
	}
	return rd.Have+rd.Potential < rd.Needed
}

// BlobResult is one blob's status as the reading leaves it.
type BlobResult struct {
	Status string
	Reading
}

// BlobReading judges one reading of a blob. suspect is whether the
// correlated-failure guard sets the reading aside; windowOpen whether the
// retention window was still open at the moment asked about. Available
// stands whatever the guard says: verified rows are not this observer's
// trouble.
func BlobReading(point []Row, f BlobFacts, suspect, windowOpen bool) BlobResult {
	rd := ReadingOf(point, f)
	res := BlobResult{Reading: rd}
	switch {
	case rd.Available():
		res.Status = BlobAvailable
	case !suspect && len(point) > 0 && rd.Unavailable():
		res.Status = BlobUnavailable
	case windowOpen:
		res.Status = BlobPending
	default:
		res.Status = BlobNotRead
	}
	return res
}

// ReadingPoint picks the reading a blob is judged at from its rows: the
// end-of-window reading when there is one, otherwise (a blob read on the
// earlier schedule) the newest in-window point with an answer from a
// validator. ok is false when there is none.
func ReadingPoint(rows []Row) (at time.Time, ok bool) {
	for _, r := range rows {
		if r.ScheduleLabel == probe.EndReadLabel {
			return r.ScheduledAt.UTC(), true
		}
	}
	for _, r := range rows {
		if r.Phase == probe.PhaseInWindow && Answered(r) && (!ok || r.ScheduledAt.After(at)) {
			at, ok = r.ScheduledAt.UTC(), true
		}
	}
	return at, ok
}

// pointKey names one reading: a promise at a scheduled time.
type pointKey struct {
	promise string
	at      time.Time
}

// readings indexes rows by reading and judges each once, on demand.
type readings struct {
	rows  map[pointKey][]Row
	blobs Blobs
	memo  map[pointKey]Reading
}

func newReadings(rows []Row, blobs Blobs) *readings {
	r := &readings{rows: map[pointKey][]Row{}, blobs: blobs, memo: map[pointKey]Reading{}}
	for _, row := range rows {
		k := pointKey{row.PromiseHash, row.ScheduledAt.UTC()}
		r.rows[k] = append(r.rows[k], row)
	}
	return r
}

func (r *readings) at(promise string, at time.Time) Reading {
	k := pointKey{promise, at.UTC()}
	if rd, ok := r.memo[k]; ok {
		return rd
	}
	rd := ReadingOf(r.rows[k], r.blobs[promise])
	r.memo[k] = rd
	return rd
}
