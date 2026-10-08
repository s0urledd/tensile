package api

import (
	"container/list"
	"context"
	"strconv"
	"sync"
)

// Per-publication verdicts, kept because they cannot change.
//
// /v1/blobs answers each row with a class tally and a reconstructability
// verdict, and both are per publication: the tally is one query, the verdict is
// five. A page of 200 was therefore 1,200 queries, and measured on a store with
// 260 publications and 85,000 probes it took 1.4s — for a page whose rows, past
// the first screen, describe obligations that ended hours ago and can never be
// rated differently again.
//
// The store is append-only in its inserts: UpsertPublication and InsertProbe
// both end in ON CONFLICT DO NOTHING, so no row is ever replaced. It is not
// append-only in its verdicts. ApplyAmendment UPDATEs a probe row's
// classification in place once the scanner's frontier passes the promise
// timeout, which is how a deferred shadow verdict is settled: a row filed
// PROBE_ERROR with shadow_gap becomes SHADOWED_SHARD or UNMATCHED_GENUINE
// without any new row arriving.
//
// So two things can move a publication's verdict: a probe row that was not
// there before, and an amendment to one that was. The fingerprint carries
// both —
//
//	(how many probes, the highest rowid, how many are amended, the newest amended_at)
//
// — and it cannot stay the same across either change. An earlier version
// counted only the first two and argued the store was immutable; it was not,
// and a cached page kept publishing a pre-amendment class tally beside the
// amended rows on the same page. One query fetches it for a whole page.
//
// The one thing that is not immutable is the clock. A verdict carries
// window_over, which flips once when must_serve_until passes, so nothing is
// cached until it has flipped. That costs nothing: it is exactly the newest
// publications, the ones still being probed, whose verdict is not settled
// anyway.
//
// This is deliberately not the snapshot cache in snapshot.go. That one holds
// one value per window and refreshes it on a timer, which is right for an
// aggregate that changes a little with every probe. A per-publication verdict
// changes never or completely, so it wants exactness, not freshness.

// blobVerdict is everything /v1/blobs computes per publication.
type blobVerdict struct {
	fp      string // probe count and highest probe rowid, at the time this was computed
	classes classCounts
	total   int64
	rc      *reconstruct
}

// blobCacheMax bounds the map. At roughly 400 bytes a verdict this is a few
// megabytes, and it holds every publication of a busy week.
const blobCacheMax = 20000

// blobWorkSlots bounds the verdicts /v1/blobs computes at once, across every
// request: a cold page is up to 501 of them, each the tally and the status's
// reads of the reference (reconstructable), and deep pages walked one after
// another (offset up to maxBlobOffset, limit up to 500) were each that, on
// the request, with nothing bounding how many ran together. Taken per
// publication, so a page of one blob waits for at most a verdict or two of
// a crawl's, not for its page.
const blobWorkSlots = 2

// blobCache keeps the verdicts by their last use: once full, the one used
// longest ago gives way. It used to start again from nothing once full, so
// past blobCacheMax publications a walk through the pages emptied it over and
// over, and dropped the first pages' verdicts, the ones every reader asks
// for, with the rest.
type blobCache struct {
	mu  sync.Mutex
	max int
	m   map[string]*list.Element
	// lru holds the cached verdicts (*blobEntry), the one used last at the
	// front.
	lru *list.List
}

type blobEntry struct {
	hash string
	v    blobVerdict
}

func newBlobCache() *blobCache { return newBlobCacheOf(blobCacheMax) }

// newBlobCacheOf is a cache of at most size verdicts.
func newBlobCacheOf(size int) *blobCache {
	return &blobCache{max: size, m: map[string]*list.Element{}, lru: list.New()}
}

// get returns the cached verdict if it was computed against exactly the probes
// the publication has now.
func (c *blobCache) get(hash, fp string) (blobVerdict, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.m[hash]
	if !ok {
		return blobVerdict{}, false
	}
	v := e.Value.(*blobEntry).v
	if v.fp != fp {
		return blobVerdict{}, false
	}
	c.lru.MoveToFront(e)
	return v, true
}

func (c *blobCache) put(hash string, v blobVerdict) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if e, ok := c.m[hash]; ok {
		e.Value.(*blobEntry).v = v
		c.lru.MoveToFront(e)
		return
	}
	for c.lru.Len() > 0 && c.lru.Len() >= c.max {
		old := c.lru.Back()
		c.lru.Remove(old)
		delete(c.m, old.Value.(*blobEntry).hash)
	}
	c.m[hash] = c.lru.PushFront(&blobEntry{hash: hash, v: v})
}

// probeFingerprints returns, for every publication in the selection (limit
// rows from the offset-th of blobRows' order), a string
// that changes if and only if its probe rows have changed. A publication with
// no probes at all is absent from the map and gets the zero fingerprint, which
// is still a fingerprint: it stops being the zero one the moment a probe lands.
func (s *Server) probeFingerprints(ctx context.Context, where string, limit, offset int, args ...any) (map[string]string, error) {
	// The corrected_at and retention_unverified terms are here for the same
	// reason the amended_at ones are: neither a hold nor a correction adds
	// a row or moves MAX(rowid), so without them a cached verdict outlives
	// the moment this observer stopped standing behind it.
	rows, err := s.st.DB().QueryContext(ctx, blobSelAt(where, limit, offset)+`
		SELECT p.promise_hash, COUNT(*), COALESCE(MAX(p.rowid), 0),
		       COUNT(p.amended_at), COALESCE(MAX(p.amended_at), ''),
		       COUNT(p.corrected_at), COALESCE(MAX(p.corrected_at), ''),
		       COALESCE(MAX(p.retention_unverified), 0)
		FROM probes p JOIN sel ON sel.promise_hash = p.promise_hash
		GROUP BY p.promise_hash`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]string{}
	for rows.Next() {
		var hash, lastAmended, lastCorrected string
		var n, maxRowID, amended, corrected, held int64
		if err := rows.Scan(&hash, &n, &maxRowID, &amended, &lastAmended, &corrected, &lastCorrected, &held); err != nil {
			return nil, err
		}
		out[hash] = strconv.FormatInt(n, 10) + ":" + strconv.FormatInt(maxRowID, 10) +
			":" + strconv.FormatInt(amended, 10) + ":" + lastAmended +
			":" + strconv.FormatInt(corrected, 10) + ":" + lastCorrected +
			":" + strconv.FormatInt(held, 10)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	rows.Close()
	// A sampled-out publication's rows are its decision (store/sampledout.go),
	// which adds no probe row: the decision is part of the fingerprint, or
	// a verdict cached before it landed would outlive it.
	drows, err := s.st.DB().QueryContext(ctx, blobSelAt(where, limit, offset)+`
		SELECT d.promise_hash, COUNT(*), MAX(d.decided_at)
		FROM sampling_decisions d JOIN sel ON sel.promise_hash = d.promise_hash
		GROUP BY d.promise_hash`, args...)
	if err != nil {
		return nil, err
	}
	defer drows.Close()
	for drows.Next() {
		var hash, last string
		var n int64
		if err := drows.Scan(&hash, &n, &last); err != nil {
			return nil, err
		}
		out[hash] += ":sampled_out:" + strconv.FormatInt(n, 10) + ":" + last
	}
	return out, drows.Err()
}
