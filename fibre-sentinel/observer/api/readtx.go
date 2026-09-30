package api

import (
	"context"

	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
)

// A window's figures are read through q, never through the database handle
// directly, so that a computation can hand every read it makes one snapshot
// of the store: the read transaction its window is computed in. Without
// one, every statement is its own autocommit read and a window is not one
// moment of the store but as many as it has statements, which a collector
// pass landing in between makes visible (a publication counted in one
// figure and not yet in the next).
//
// The memo of original_rows and the endorsement ledger are the exception:
// they are caches of the whole record kept across computations
// (origrows.go, signing.go), and read the database as they always did.

// querierKey carries the querier a computation reads through.
type querierKey struct{}

// q is what a computation reads the store through: the transaction its
// window is being computed in, or the database when it has none.
func (s *Server) q(ctx context.Context) store.Querier {
	if q, ok := ctx.Value(querierKey{}).(store.Querier); ok && q != nil {
		return q
	}
	return s.st.DB()
}

// withQuerier makes q what every read under ctx goes through.
func withQuerier(ctx context.Context, q store.Querier) context.Context {
	return context.WithValue(ctx, querierKey{}, q)
}

// readTx runs fn in one read transaction: every read fn makes through
// s.q sees the store as it stood at its first read.
//
// A transaction holds one of the store's connections for as long as fn
// runs, and fn still reads the memo and the ledger through the database
// (origrows.go, signing.go), which takes a second one for a moment. With
// every connection held by a transaction whose holder waits for another,
// nothing would move, so the transactions open at once are bounded one
// below the pool (txSlots) and a computation beyond that waits for a
// slot. A store with a single connection (store.Open, the collector's and
// the tests') has no slot to spare and is read without a transaction, as
// it always was: nothing writes it while a computation of its own runs.
func (s *Server) readTx(ctx context.Context, fn func(context.Context) error) error {
	if _, in := ctx.Value(querierKey{}).(store.Querier); in || s.txSlots == nil {
		// Already inside one, or no connection to spare for one.
		return fn(ctx)
	}
	select {
	case s.txSlots <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	defer func() { <-s.txSlots }()
	// BEGIN, deferred: the snapshot is taken at fn's first read. The
	// connection is query_only (store.OpenReadOnly), so nothing here can
	// write.
	tx, err := s.st.DB().BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	return fn(withQuerier(ctx, tx))
}

// maxReadTx bounds the transactions open at once over a pool with no limit
// of its own.
const maxReadTx = 64

// readSlots is the txSlots a pool of n connections allows: nil, reading
// without transactions, when there is none to spare.
func readSlots(n int) chan struct{} {
	switch {
	case n <= 0:
		return make(chan struct{}, maxReadTx)
	case n == 1:
		return nil
	}
	return make(chan struct{}, n-1)
}
