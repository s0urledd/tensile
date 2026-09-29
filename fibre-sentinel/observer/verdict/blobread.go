package verdict

import (
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/probe"
	"github.com/plsgiveup/fibre/fibre-sentinel/internal/scan"
)

// A blob is read the way celestia-app's Fibre client downloads one, and its
// result is the client's (Reading):
//
//   - Available: at least Needed distinct rows came back verified (Needed is
//     original_rows, 4096 for blob version 0), from any validator.
//   - Unavailable, with the client's error: "no shards retrieved" when no
//     verified row came back, "not enough shards to reconstruct blob" when
//     some did, fewer than Needed.
//
// Nothing else for a reading that happened. A reading did not happen when
// not a single request reached a server (this observer's own network was
// down: Reached holds for none) or when the prober missed its requests (a
// NOT_PROBED row: it was down, restarting, or late) and the rows are short;
// then the blob was not read by Tensile, or is still in its retention
// window, and no one is not served on it (rows that came back verified
// still count as served).
//
// A reading is judged from all of its rows (one promise, one scheduled
// time), whatever phase each row carries: the rows that came back are the
// client's result.
//
// What counts for a validator follows from it (Row.CountedClass): served
// when its rows came back verified; not served when it endorsed the
// promise, its rows did not come back, and the blob was Unavailable. On an
// Available blob a validator that failed or was not asked counts neither
// way. The SQL twin is rollup.CountedClass.

// NotCounted is the class a row counts as when it left the reader without
// rows on a blob that was not Unavailable: counted neither for nor against
// the validator.
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
	f := BlobFacts{Needed: p.Assignment.ProtocolParams.OriginalRows, Excess: p.Assignment.Sigma - p.Assignment.Distinct,
		Endorsed: map[string]int{}}
	if f.Excess < 0 {
		f.Excess = 0
	}
	for _, v := range p.Assignment.Validators {
		if v.RowCount <= 0 {
			continue
		}
		if v.Attested || !p.HasAttestation() {
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

// Reached reports whether a row's request got through to a server
// (probe.Reached): its connection was opened, its host refused it, or rows
// came back verified. A reading with none did not happen.
func Reached(r Row) bool {
	return probe.Reached(r.TCPOK, r.Outcome, r.CommitmentVerified)
}

// Reading is what the rows of one reading of a blob come to.
type Reading struct {
	// Have is the distinct row indices that came back verified.
	Have int
	// Lower and Upper bound Have from the counts alone: the served shards
	// less the assignment's overlaps, and every verified row counted.
	Lower, Upper int
	// IndicesMissing: a verified row carries no index list (a record from
	// before they were kept), so Have is a floor rather than a count; the
	// served shards' bound (Lower) still decides Available.
	IndicesMissing bool
	// Ran: a request reached a server (Reached).
	Ran bool
	// Missed: the prober missed a request of this reading (a NOT_PROBED row
	// of an assigned validator in the window).
	Missed bool
	// Asked is the validators the reading asked (a row other than this
	// observer's own gap, NOT_PROBED or PROBE_ERROR); Served, those whose
	// rows verified.
	Asked, Served int
	Needed        int
}

// ReadingOf reduces the rows of one reading (one promise, one scheduled
// time) with the publication's facts, whatever phase each row carries.
func ReadingOf(point []Row, f BlobFacts) Reading {
	rd := Reading{Needed: f.Needed}
	seen := map[uint32]struct{}{}
	asked := map[string]bool{}
	served := map[string]bool{}
	servedOK := map[string]int{}
	for _, r := range point {
		if r.Classification == probe.ClassNotProbed && r.Assigned && r.Phase == probe.PhaseInWindow {
			rd.Missed = true
		}
		if r.Classification != probe.ClassNotProbed && r.Classification != probe.ClassProbeError {
			asked[r.Validator] = true
		}
		if Reached(r) {
			rd.Ran = true
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
	rd.Asked, rd.Served = len(asked), len(served)
	return rd
}

// Available reports whether the rows that came back reconstruct the blob.
func (rd Reading) Available() bool {
	return rd.Needed > 0 && (rd.Lower >= rd.Needed || rd.Have >= rd.Needed)
}

// Unavailable reports whether the reading happened and the rows that came
// back could not reconstruct the blob. The SQL twin is rollup's
// unavailableSQL.
func (rd Reading) Unavailable() bool {
	return rd.Needed > 0 && !rd.Available() && rd.Ran && !rd.Missed
}

// Error is the client's error for an Unavailable reading, "" otherwise.
func (rd Reading) Error() string {
	if !rd.Unavailable() {
		return ""
	}
	return probe.ClientError(rd.Upper)
}

// BlobResult is one blob's status as the reading leaves it.
type BlobResult struct {
	Status string
	// Error is the client's error on an Unavailable blob.
	Error string
	Reading
}

// BlobReading judges one reading of a blob. windowOpen is whether the
// retention window was still open at the moment asked about: a blob not
// read yet is in its retention window, one whose window closed without a
// reading was not read by Tensile.
func BlobReading(point []Row, f BlobFacts, windowOpen bool) BlobResult {
	rd := ReadingOf(point, f)
	res := BlobResult{Reading: rd}
	switch {
	case rd.Available():
		res.Status = BlobAvailable
	case rd.Unavailable():
		res.Status, res.Error = BlobUnavailable, rd.Error()
	case windowOpen:
		res.Status = BlobPending
	default:
		res.Status = BlobNotRead
	}
	return res
}

// BlobOf judges one blob from all its rows: at the reading ReadingPoint
// picks, over all of that reading's rows, with the retention window open
// when asOf is not after must_serve_until. It is what the API's reference
// (reconstructable) draws from the store.
func BlobOf(rows []Row, f BlobFacts, mustServeUntil, asOf time.Time) BlobResult {
	var point []Row
	if at, ok := ReadingPoint(rows, f); ok {
		for _, r := range rows {
			if r.ScheduledAt.UTC().Equal(at) {
				point = append(point, r)
			}
		}
	}
	return BlobReading(point, f, !asOf.After(mustServeUntil))
}

// ReadingPoint picks the reading a blob is judged at from its rows: the
// end-of-window reading when there is one. A blob read on the earlier
// schedule was read at several points, each written a validator at a time,
// some of them after its window: it is judged at the newest point in the
// window every endorsing validator was reached at, or failing that the
// newest point in the window any validator was reached at. ok is false when
// there is none.
func ReadingPoint(rows []Row, f BlobFacts) (at time.Time, ok bool) {
	for _, r := range rows {
		if r.ScheduleLabel == probe.EndReadLabel {
			return r.ScheduledAt.UTC(), true
		}
	}
	answered := map[time.Time]map[string]bool{}
	for _, r := range rows {
		if r.Phase != probe.PhaseInWindow || !Reached(r) {
			continue
		}
		k := r.ScheduledAt.UTC()
		if answered[k] == nil {
			answered[k] = map[string]bool{}
		}
		answered[k][r.Validator] = true
	}
	var complete, newest time.Time
	for k, vals := range answered {
		if k.After(newest) {
			newest = k
		}
		whole := len(f.Endorsed) > 0
		for v := range f.Endorsed {
			if !vals[v] {
				whole = false
				break
			}
		}
		if whole && k.After(complete) {
			complete = k
		}
	}
	switch {
	case !complete.IsZero():
		return complete, true
	case !newest.IsZero():
		return newest, true
	}
	return time.Time{}, false
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
