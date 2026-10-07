package api

import (
	"context"
	"encoding/json"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
)

// The routes publish a projection of the rows they are built from
// (shapes.go). The rows keep every figure, and a test of one the
// projection leaves out (a reading tally, latency, the row count behind a
// validator's shard data) reads the rows here, computed as the snapshot
// computes them, in the JSON the rows are stored in.

// windowAt is the window the snapshot would compute for name at now, or
// the pinned one when asOf is set.
func windowAt(name string, asOf time.Time) Window {
	if asOf.IsZero() {
		return windowFor(name, time.Now())
	}
	w := windowFor(name, asOf)
	w.AsOf = true
	return w
}

// ValidatorRowsJSON is {"validators": [...]} with every validator row of
// the window, whole, as the snapshot stores them.
func ValidatorRowsJSON(st *store.Store, vantage, window string, asOf time.Time) ([]byte, error) {
	s := newServer(st, VantageInfo{Name: vantage}, nil)
	rows, err := s.validatorRows(context.Background(), windowAt(window, asOf), "")
	if err != nil {
		return nil, err
	}
	return json.Marshal(map[string]any{"validators": rows})
}

// BlobTally is what the server computes for one publication beside what
// /v1/blobs publishes: the reading tally, and whether its retention window
// is over.
type BlobTally struct {
	ProbeCount int64
	Classes    map[string]int64
	WindowOver bool
}

// BlobTallies is BlobTally for every row of the /v1/blobs page at limit and
// offset, by promise hash, through the server's own verdict cache.
func (s *Server) BlobTallies(limit, offset int) (map[string]BlobTally, error) {
	rows, err := s.blobRowsAt(context.Background(), "", limit, offset)
	if err != nil {
		return nil, err
	}
	rows, _ = trim(rows, limit)
	out := map[string]BlobTally{}
	for _, b := range rows {
		out[b.PromiseHash] = BlobTally{ProbeCount: b.ProbeCount, Classes: b.Classes, WindowOver: b.Reconstructable != nil && b.Reconstructable.WindowOver}
	}
	return out, nil
}

// UpgradeSignalJSON is x/signal's tally as the store holds it at now, whole:
// /v1/meta publishes only its scheduled height and ETA.
func UpgradeSignalJSON(st *store.Store, now time.Time) ([]byte, error) {
	rows, err := st.DB().Query(`SELECT key, value FROM meta`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	meta := map[string]string{}
	for rows.Next() {
		var k, v string
		if err := rows.Scan(&k, &v); err != nil {
			return nil, err
		}
		meta[k] = v
	}
	return json.Marshal(upgradeSignalOf(meta, now))
}

// NetworkExcludingJSON is NetworkJSON over a window ending now, recomputed
// without the validators named (hex), as ?exclude= recomputes it.
func NetworkExcludingJSON(st *store.Store, vantage, window string, exclude []string) ([]byte, error) {
	s := newServer(st, VantageInfo{Name: vantage}, nil)
	var ex excludeSet
	for _, a := range exclude {
		ex.addrs = append(ex.addrs, a)
	}
	resp, err := s.computeNetwork(context.Background(), windowAt(window, time.Time{}), ex, exclude)
	if err != nil {
		return nil, err
	}
	return json.Marshal(resp)
}

// NetworkJSON is the network summary of the window, whole, as the snapshot
// stores it.
func NetworkJSON(st *store.Store, vantage, window string, asOf time.Time) ([]byte, error) {
	s := newServer(st, VantageInfo{Name: vantage}, nil)
	resp, err := s.computeNetwork(context.Background(), windowAt(window, asOf), excludeSet{}, nil)
	if err != nil {
		return nil, err
	}
	return json.Marshal(resp)
}

// WithClock fixes the server's clock (withClock), for a test that moves it
// between two calls of a check that remembers what the first one saw.
func WithClock(f func() time.Time) Option { return withClock(f) }

// FailDayPartsSave records err as the day partials' last write, as a write
// of the files that failed records it (dayParts.save).
func (s *Server) FailDayPartsSave(err error) {
	s.parts.mu.Lock()
	s.parts.saveErr, s.parts.saveErrAt = err.Error(), time.Now()
	s.parts.mu.Unlock()
}
