package api

import (
	"context"
	"fmt"
	"net/http"
	"runtime/debug"
	"sync"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
)

// namespaceRow is one namespace's settled Fibre publications: every figure is
// a count or a sum over the chain's own records (MsgPayForFibre), nothing
// measured.
type namespaceRow struct {
	Namespace string `json:"namespace"` // hex, as the chain carries it
	Blobs     int64  `json:"blobs"`
	Bytes     int64  `json:"bytes"` // padded blob size, as charged
	Blobs24h  int64  `json:"blobs_24h"`
	Bytes24h  int64  `json:"bytes_24h"`
	Accounts  int64  `json:"accounts"` // distinct publishers: the escrow owner that paid, not the tx submitter
	FirstSeen string `json:"first_seen"`
	LastBlob  string `json:"last_blob"`
}

// The bounds of /v1/namespaces: the page asked for when none is, and the
// most a page holds.
const (
	namespacesPageDefault = 100
	namespacesPageMax     = 500
)

// The answer is summed over every publication on record, which is never
// pruned, so its cost grows with the record for good (69 ms at 8,900
// publications, seconds at a million), and the Blobs page asks for it every
// thirty seconds. It is computed in the background and served from memory.
// Past namespacesTTL the first reader sets a new computation going and is
// served the answer it will replace, as is everyone until it lands: only the
// first reader after a start waits for one. A computation that fails leaves
// the last answer served for at most namespacesStale; a reader after that
// waits for a computation and is answered its error, so a failure that lasts
// is an error a reader and /v1/health see rather than an answer that
// silently stops moving.
const (
	namespacesTTL   = 30 * time.Second
	namespacesStale = 15 * time.Minute
)

// namespaceCache is the answer as last computed: every namespace up to one
// past namespacesPageMax, in the order the route lists them, so any page
// is its first rows.
type namespaceCache struct {
	mu   sync.Mutex
	rows []namespaceRow
	// at is the clock the rows were computed at, and flight the computation
	// under way, nil when none is.
	at     time.Time
	flight *namespaceFlight
}

// namespaceFlight is one computation of the answer: its rows or its error
// once done is closed.
type namespaceFlight struct {
	done chan struct{}
	rows []namespaceRow
	err  error
}

// namespacesSQL is the whole answer: per namespace, newest publication
// first, the 24 hours before ?1 counted apart, at most ?2 namespaces.
const namespacesSQL = `SELECT pub.namespace, COUNT(*), COALESCE(SUM(pub.blob_size), 0),
		COALESCE(SUM(CASE WHEN pub.settlement_time >= ?1 THEN 1 ELSE 0 END), 0),
		COALESCE(SUM(CASE WHEN pub.settlement_time >= ?1 THEN pub.blob_size ELSE 0 END), 0),
		COUNT(DISTINCT COALESCE(pay.publisher, pub.signer)), MIN(pub.settlement_time), MAX(pub.settlement_time)
	FROM publications pub
	LEFT JOIN payments pay ON pay.promise_hash = pub.promise_hash AND pay.kind = 'settlement'
	GROUP BY pub.namespace
	ORDER BY MAX(pub.settlement_height) DESC, pub.namespace LIMIT ?2`

// computeNamespaces is the answer at now.
func (s *Server) computeNamespaces(ctx context.Context, now time.Time) ([]namespaceRow, error) {
	since := store.TS(now.UTC().Add(-24 * time.Hour))
	rows, err := s.st.DB().QueryContext(ctx, namespacesSQL, since, namespacesPageMax+1)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []namespaceRow{}
	for rows.Next() {
		var n namespaceRow
		if err := rows.Scan(&n.Namespace, &n.Blobs, &n.Bytes, &n.Blobs24h, &n.Bytes24h, &n.Accounts, &n.FirstSeen, &n.LastBlob); err != nil {
			return nil, err
		}
		out = append(out, n)
	}
	return out, rows.Err()
}

// namespaceRows is the answer as namespaceCache serves it. The rows are
// shared: the caller does not change them.
func (s *Server) namespaceRows(ctx context.Context) ([]namespaceRow, error) {
	c := &s.namespaces
	now := s.now()
	c.mu.Lock()
	age := now.Sub(c.at)
	held := c.rows != nil && age >= 0
	if held && age < namespacesTTL {
		rows := c.rows
		c.mu.Unlock()
		return rows, nil
	}
	f := c.flight
	if f == nil {
		f = &namespaceFlight{done: make(chan struct{})}
		c.flight = f
		s.bg.Add(1) // Close waits for it like any other background work
		go s.computeNamespacesFlight(f, now)
	}
	if held && age < namespacesStale {
		rows := c.rows
		c.mu.Unlock()
		return rows, nil
	}
	c.mu.Unlock()
	select {
	case <-f.done:
		return f.rows, f.err
	case <-ctx.Done():
		return nil, ctx.Err() // the reader left; the answer still lands for the next one
	}
}

// computeNamespacesFlight runs f, a computation of the answer at now, and
// keeps what it computed. A panic is a failed computation, as on a goroutine
// of its own it would end the whole API.
func (s *Server) computeNamespacesFlight(f *namespaceFlight, now time.Time) {
	defer s.bg.Done()
	c := &s.namespaces
	defer func() {
		if p := recover(); p != nil {
			f.rows, f.err = nil, fmt.Errorf("panic: %v\n%s", p, debug.Stack())
		}
		c.mu.Lock()
		c.flight = nil
		if f.err == nil {
			c.rows, c.at = f.rows, now
		}
		c.mu.Unlock()
		close(f.done)
		if f.err != nil {
			if log := s.logf(); log != nil {
				log("namespaces: %v", f.err)
			}
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), snapshotTimeout)
	defer cancel()
	go func() {
		select {
		case <-s.stop:
			cancel()
		case <-ctx.Done():
		}
	}()
	f.rows, f.err = s.computeNamespaces(ctx, now)
}

// handleNamespaces lists namespaces by their newest settled publication.
func (s *Server) handleNamespaces(w http.ResponseWriter, r *http.Request) {
	limit, err := parseLimit(r, namespacesPageDefault, namespacesPageMax)
	if err != nil {
		writeErr(w, 400, err.Error())
		return
	}
	all, err := s.namespaceRows(r.Context())
	if err != nil {
		s.writeInternal(w, r.URL.Path, err)
		return
	}
	out, truncated := trim(all, limit)
	writeJSON(w, 200, map[string]any{"namespaces": out, "limit": limit, "truncated": truncated})
}
