package api

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"modernc.org/sqlite"
)

// A row that does not decode is that row's fault, not the answer's.
//
// Most of what a list publishes is columns. A few parts are read out of a
// row's record: the row indices of a reading whose list marks its own
// assignment, and the assignment of a served shard that kept no list. A
// record this build cannot decode (a format a newer build wrote, a table
// entry it does not know, a decoder bug like #253's) used to fail the whole
// answer: one such publication answered /v1/blobs with a 500 for everyone,
// /v1/blobs/{hash} and /v1/probes?rows=1 with it, and the network and
// market snapshots, which read it in their batches, stopped refreshing.
//
// Such a row is now published with its undecodable part marked, and the rest
// of the answer stands: a blob's reconstructable status is "unknown" (the
// word for a blob nothing can be judged by), a reading's row_indices are
// left out. Each row is logged once, the first time it fails, and counted
// (rowFaults), and /v1/health's records check fails while one is met
// (recordsCheck), so the failure is seen rather than masked. A status
// computed from such a row is never cached or kept by a memo, nor counted by
// the reading totals: once the row decodes again (a newer build, the table
// entry loaded), its status is computed again.
//
// Only a fault of the row itself is contained. The request's context ending
// and the database failing still fail the answer, as they did: a timeout must
// not turn a page of statuses into "unknown".

// errRowFault marks an error as one row's: the record of hash, read for
// what, did not decode.
type errRowFault struct {
	hash, what string
	err        error
}

func (e *errRowFault) Error() string { return e.what + " of " + e.hash + ": " + e.err.Error() }
func (e *errRowFault) Unwrap() error { return e.err }

// rowFault classifies an error met reading one row's record: an
// *errRowFault when it is the row's own, err as it is when it is the
// request's (its context ended) or the database's.
func rowFault(ctx context.Context, hash, what string, err error) error {
	if err == nil || ctx.Err() != nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	var se *sqlite.Error
	if errors.As(err, &se) || errors.Is(err, sql.ErrConnDone) || errors.Is(err, sql.ErrTxDone) || errors.Is(err, driver.ErrBadConn) ||
		// database/sql's own, which it does not export
		strings.Contains(err.Error(), "sql: database is closed") {
		return err
	}
	return &errRowFault{hash: hash, what: what, err: err}
}

// isRowFault reports whether err is one row's (rowFault), and which.
func isRowFault(err error) (*errRowFault, bool) {
	var f *errRowFault
	if errors.As(err, &f) {
		return f, true
	}
	return nil, false
}

// rowFaultsKept bounds how many distinct rows rowFaults remembers having
// logged: past it a row is counted and logged again, which is noise, never
// silence.
const rowFaultsKept = 100000

// rowFaults is every row this process found undecodable: logged once each,
// and counted.
type rowFaults struct {
	mu     sync.Mutex
	logged map[string]bool
	// rows is how many distinct rows failed since the start; last is the
	// newest failure, at.
	rows int64
	last string
	at   time.Time
}

// note records a row's fault and logs it the first time.
func (s *Server) noteRowFault(f *errRowFault) {
	key := f.what + " " + f.hash
	s.faults.mu.Lock()
	first := !s.faults.logged[key]
	if first {
		if s.faults.logged == nil || len(s.faults.logged) >= rowFaultsKept {
			s.faults.logged = map[string]bool{}
		}
		s.faults.logged[key] = true
		s.faults.rows++
	}
	s.faults.last, s.faults.at = f.Error(), s.now()
	s.faults.mu.Unlock()
	if first {
		if log := s.logf(); log != nil {
			log("a row that does not decode is published without it: %v", f)
		}
	}
}

// UndecodableRows is how many distinct rows this process found undecodable
// since it started, the newest failure and when: zero and "" while every
// row decodes.
func (s *Server) UndecodableRows() (rows int64, last string, at time.Time) {
	s.faults.mu.Lock()
	defer s.faults.mu.Unlock()
	return s.faults.rows, s.faults.last, s.faults.at
}

// rowFaultFresh is how long the records check fails after the newest row
// found undecodable. A row that stays undecodable is met again at every
// readings update, seconds apart, so the check fails for as long as one
// does and clears this long after the last.
const rowFaultFresh = 15 * time.Minute

// recordsCheck is /v1/health's check of the rows this process found
// undecodable: failing while one was met in the last rowFaultFresh. The
// detail counts them and says when the newest was met; which rows, and why,
// is in the log, a line each (noteRowFault), as a check's detail carries no
// raw error.
func (s *Server) recordsCheck(now time.Time) healthCheck {
	rows, _, at := s.UndecodableRows()
	switch {
	case rows == 0:
		return healthCheck{"records", true, "every row read since the API started decoded"}
	case now.Sub(at) < rowFaultFresh:
		return healthCheck{"records", false, fmt.Sprintf("rows that did not decode since the API started: %d, published without the part that needed them; the newest met %s ago (the API's log names each)",
			rows, now.Sub(at).Round(time.Second))}
	default:
		return healthCheck{"records", true, fmt.Sprintf("rows that did not decode since the API started: %d, none met in the last %s (the API's log names each)", rows, rowFaultFresh)}
	}
}

// faulted is what reconstructable returns for err: the blob as far as rc
// knows it, status "unknown", beside a row fault; any other error alone.
func faulted(rc *reconstruct, err error) (*reconstruct, error) {
	if _, ok := isRowFault(err); !ok {
		return nil, err
	}
	return &reconstruct{Status: "unknown", NeededRows: rc.NeededRows, TotalRows: rc.TotalRows, WindowOver: rc.WindowOver, faulted: true}, err
}

// readStatus is reconstructable with a row fault contained: the blob is
// published "unknown" and the fault is noted; any other error is the
// caller's.
func (s *Server) readStatus(ctx context.Context, hash string, pin asOfPin) (*reconstruct, error) {
	rc, err := s.reconstructable(ctx, hash, pin)
	if f, ok := isRowFault(err); ok {
		s.noteRowFault(f)
		return rc, nil
	}
	return rc, err
}
