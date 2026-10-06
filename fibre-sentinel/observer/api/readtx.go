package api

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

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
// (origrows.go, signing.go), and read the database as they always did. A
// comparison of the partials with the shipped statements (comparePaths)
// reads the ledger once for both ways (ledgerOnce): read per way, a
// publication stored in between moved one way's newest endorsements.

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
// s.q sees the store as it stood at its first read. The computation sums
// the day partials where it can (readTxFor says when it cannot).
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
// With the partials off there is no transaction at all (readTxWith).
func (s *Server) readTx(ctx context.Context, fn func(context.Context) error) error {
	return s.readTxWith(ctx, true, fn)
}

// readTxFor is readTx for a computation of win: one the partials do not
// serve (the day's window, partsFor) reads no epoch, so it neither waits
// for the partials to load nor catches them up.
func (s *Server) readTxFor(ctx context.Context, win Window, fn func(context.Context) error) error {
	return s.readTxWith(ctx, partsFor(win), fn)
}

func (s *Server) readTxWith(ctx context.Context, parts bool, fn func(context.Context) error) error {
	if _, in := ctx.Value(querierKey{}).(store.Querier); in {
		return fn(ctx) // already inside one
	}
	if s.parts == nil {
		// The partials off (-day-partials=false): every statement is its
		// own read, as in the build before them, with no connection held
		// for a transaction and no slot to wait for. It is the way back
		// without a rollback of the binary, so it goes all the way back.
		return fn(ctx)
	}
	if s.parts.rawFallback() != "" {
		// An audit found the partials not what the store holds: every window
		// is read as with the partials off, for the rest of the process
		// (fallBack).
		return fn(ctx)
	}
	dp := s.parts
	if !parts {
		dp = nil
	}
	if dp != nil {
		// The partials kept on disk are loaded before the first catch-up,
		// in the background: waiting for them here holds no connection and
		// no lock, and ends with ctx.
		if err := dp.ready(ctx, s); err != nil {
			return err
		}
	}
	if s.txSlots == nil {
		// No connection to spare for one. The partials are still caught up
		// first, under their lock, so the computation reads one epoch.
		if dp != nil {
			dp.mu.Lock()
			ctx = dp.enterLocked(ctx, s, s.st.DB())
			dp.mu.Unlock()
		}
		return fn(ctx)
	}
	select {
	case s.txSlots <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	defer func() { <-s.txSlots }()
	// BEGIN, deferred: the snapshot is taken at the first read. The
	// connection is query_only (store.OpenReadOnly), so nothing here can
	// write. With the partials on, the transaction is begun and its first
	// read (the catch-up) made under their lock, so the epoch a catch-up
	// starts from is never newer than the snapshot it reads.
	var tx *sql.Tx
	var err error
	if dp != nil {
		dp.mu.Lock()
		tx, err = s.st.DB().BeginTx(ctx, nil)
		if err == nil {
			ctx = dp.enterLocked(ctx, s, tx)
		}
		dp.mu.Unlock()
	} else {
		tx, err = s.st.DB().BeginTx(ctx, nil)
	}
	if err != nil {
		return err
	}
	defer tx.Rollback()
	return fn(withQuerier(ctx, tx))
}

// readSnapshot runs fn over one read transaction of its own, on a slot
// like any computation's, with no partials: the load's.
func (s *Server) readSnapshot(ctx context.Context, fn func(q store.Querier) error) error {
	if s.txSlots == nil {
		return fn(s.st.DB())
	}
	select {
	case s.txSlots <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	defer func() { <-s.txSlots }()
	tx, err := s.st.DB().BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	return fn(tx)
}

// enterLocked catches the partials up to the store as q sees it and hands
// the epoch to the computation under ctx. A catch-up that fails is logged
// and the computation reads the shipped statements over its whole window,
// as it did before there were partials; the next computation tries again.
// So does one that finds the kept partials not loaded, because their load
// failed. The caller holds the lock.
func (dp *dayParts) enterLocked(ctx context.Context, s *Server, q store.Querier) context.Context {
	if dp.cur == nil && !dp.loaded {
		return ctx
	}
	e, err := dp.advance(ctx, s, q, s.now())
	if err != nil {
		if dp.log != nil {
			dp.log("day partials: catching up: %v; reading the whole window", err)
		}
		return ctx
	}
	return withEpoch(ctx, e)
}

// The first catch-up after a start begins from the partials kept on disk
// (dayparts_file.go) when they may be used: the file is read, caught up to
// the store and the newest sealed days of it recomputed (verifyLoaded).
// That is seconds, and minutes on a busy record, so it is not done under
// the partials' lock, which every computation that sums them takes: it
// runs in the background, in a read transaction of its own, and the epoch
// it ends with is installed under the lock at the end. The computations
// that sum the partials wait for it (ready), holding nothing; the rest do
// not (readTxFor), nor does a validator's page served from the snapshots.
// A load that fails, as opposed to one that refuses the file, installs
// nothing: a computation after it starts it again, rather than building the
// partials from nothing over a file that may well be good, once the load
// has waited out a delay that doubles each time it fails (loadRetryBase to
// loadRetryCeiling), and the computations meanwhile read the whole window.

// loadTimeout bounds the background load.
const loadTimeout = snapshotTimeoutAll

// loadRetryBase and loadRetryCeiling bound the delay before a failed load
// is started again: half a minute after its first failure, twice as long
// after each one after it, at most half an hour.
const (
	loadRetryBase    = 30 * time.Second
	loadRetryCeiling = 30 * time.Minute
)

// ready returns once the kept partials have been loaded, refused or found
// missing, starting the load if nothing has: or with ctx's error, if ctx
// ends first. The load is not the caller's: it goes on for the next.
func (dp *dayParts) ready(ctx context.Context, s *Server) error {
	dp.mu.Lock()
	if dp.cur != nil || dp.loaded {
		dp.mu.Unlock()
		return nil
	}
	done := dp.loading
	if f := dp.failures["load"]; done == nil && f != nil && s.now().Before(f.until) {
		dp.mu.Unlock()
		return nil // the last load failed: the whole window is read meanwhile
	}
	if done == nil {
		done = make(chan struct{})
		dp.loading = done
		go dp.loadKept(s, done)
	}
	dp.mu.Unlock()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// loadKept is the background load: it reads, catches up and checks the
// kept partials, or builds them from the store when there are none or they
// are refused, and installs the epoch. On an error it installs nothing.
func (dp *dayParts) loadKept(s *Server, done chan struct{}) {
	defer close(done)
	ctx, cancel := context.WithTimeout(context.Background(), loadTimeout)
	defer cancel()
	go func() {
		select {
		case <-s.stop:
			cancel()
		case <-ctx.Done():
		}
	}()
	t0 := time.Now()
	var (
		e       *epoch
		kept    keptSeals
		origin  string
		loaded  bool
		dropped int
		rebuilt bool
	)
	err := s.readSnapshot(ctx, func(q store.Querier) error {
		var why refusal
		var err error
		var f *epoch
		if f, kept, why, err = dp.load(ctx, s, q); err != nil {
			return err
		}
		if f != nil {
			var caught *epoch
			caught, _, dropped, err = catchUpFrom(ctx, s, q, f, s.now(), true)
			switch {
			case errors.Is(err, errRegressed):
				why = refusal(err.Error())
			case err != nil:
				return err
			default:
				if dp.failLoad != nil {
					if err := dp.failLoad(); err != nil {
						return err
					}
				}
				if why, err = s.verifyLoaded(ctx, q, caught); err != nil {
					return err
				}
				if why == "" {
					e, loaded = caught, true
					origin = "loaded from " + dp.file
					return nil
				}
			}
		}
		if why == "" {
			origin = "built from the store: no " + dp.file
		} else {
			origin = "built from the store: " + dp.file + " refused: " + string(why)
		}
		kept = keptSeals{}
		rebuilt = true
		e, err = build(ctx, s, q)
		return err
	})
	if err != nil {
		dp.fail("load", s.now(), loadRetryBase, loadRetryCeiling, fmt.Errorf("loading %s: %w (every window is read whole meanwhile)", dp.file, err))
	} else {
		dp.recover("load")
	}
	dp.mu.Lock()
	defer dp.mu.Unlock()
	dp.loading = nil
	if err != nil {
		return
	}
	dp.loaded, dp.origin = true, origin
	if dp.cur != nil {
		return // cannot happen: nothing catches up before the load ends
	}
	e.seq, e.born = 1, 1
	dp.cur, dp.journal = e, []journalEntry{{seq: 1, all: true}}
	dp.dropped += dropped
	if rebuilt {
		dp.rebuilds++
	}
	if loaded {
		// What the file holds is content 0: the drops of the catch-up after
		// the load are a reason to write it again.
		dp.sealFiles, dp.savedBorn, dp.saved, dp.auditAt = kept.files, e.born, 0, kept.audit
		if dp.gens == nil {
			dp.gens = map[string]int{}
		}
		for k, g := range kept.gens {
			dp.gens[k] = max(dp.gens[k], g)
		}
	}
	if dp.log != nil && (loaded || dp.file != "") {
		dp.log("day partials: %s (%s)", origin, time.Since(t0).Round(time.Millisecond))
	}
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
