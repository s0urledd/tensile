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
