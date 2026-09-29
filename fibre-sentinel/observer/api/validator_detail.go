package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/observer/rollup"
)

// The validator page used to be the one live route with no snapshot behind
// it: every request built the row and four spans of aggregates over the
// validator's whole probe history, 4–11 s for an active validator, and only
// then found out whether the address was on record at all. The answer is now
//
//   - refused with 404 before any aggregate when nothing about the address is
//     on record (validatorKnown);
//   - read from the validator snapshots for a live window: the row and the
//     four spans are the validator's rows in them (detailFromSnapshots), and
//     only the newest readings are read per request;
//   - kept for detailTTL per (address, window) and dropped as soon as a
//     parameter hold lands, like the network and validator snapshots;
//   - computed once for concurrent readers of the same key under the same
//     hold revision;
//   - computed at most detailConcurrency at a time.

const (
	detailTTL         = 30 * time.Second
	detailConcurrency = 4
	detailMaxEntries  = 1024
	// detailCompute bounds one computation. It runs detached from the
	// request that started it, so a reader who leaves does not fail the
	// others waiting on the same answer.
	detailCompute = 60 * time.Second
)

type detailEntry struct {
	status int
	body   []byte
	at     time.Time
	rev    string
}

type detailCall struct {
	done  chan struct{}
	entry detailEntry
	err   error
}

// detailCache is usable as its zero value.
type detailCache struct {
	mu      sync.Mutex
	entries map[string]detailEntry
	flight  map[string]*detailCall
	once    sync.Once
	sem     chan struct{}
	// compute is validatorDetail unless a test puts something in its place.
	compute func(ctx context.Context, addr string, win Window, now time.Time) (int, any, error)
}

func (c *detailCache) init() {
	c.once.Do(func() {
		c.entries = map[string]detailEntry{}
		c.flight = map[string]*detailCall{}
		c.sem = make(chan struct{}, detailConcurrency)
	})
}

// validatorKnown is a cheap superset of "validatorDetail finds a row": one
// indexed lookup in every table a row can be built from. Once raw rows have
// been pruned a validator can live on in the daily rollup alone, so from then
// on it answers true and the full path decides.
func (s *Server) validatorKnown(ctx context.Context, addr string) (bool, error) {
	if _, pruned := rollup.RawFrom(s.st); pruned {
		return true, nil
	}
	bech, _ := consBech(addr)
	var known bool
	err := s.st.DB().QueryRowContext(ctx, `SELECT
		   EXISTS(SELECT 1 FROM assignments  WHERE validator_address = ?)
		OR EXISTS(SELECT 1 FROM probes       WHERE validator_address = ?)
		OR EXISTS(SELECT 1 FROM reachability WHERE validator_address = ?)
		OR EXISTS(SELECT 1 FROM endpoints    WHERE validator_cons_address = ?)
		OR EXISTS(SELECT 1 FROM validator_identities WHERE lower(cons_address) = ?)`,
		addr, addr, addr, bech, addr).Scan(&known)
	return known, err
}

// detailFromSnapshots answers a live window from the snapshots /v1/validators
// serves: the row is the list's own row for addr and each span is addr's row
// in that span's snapshot, so the page and the table agree to the figure.
//
// ok is false when a snapshot is still being computed, or does not list addr
// yet because the validator appeared after it was taken; the caller then
// computes the answer. It never waits for a snapshot.
func (s *Server) detailFromSnapshots(ctx context.Context, addr string, win Window, now time.Time) (map[string]any, bool, error) {
	main, at, _, ok := s.vals.peek(s.logf(), win)
	if !ok {
		return nil, false, nil
	}
	row, ok := snapshotRow(main.Rows, addr)
	if !ok {
		return nil, false, nil
	}
	spans := make([]detailSpan, 0, len(detailSpans))
	for _, name := range detailSpans {
		snap := main
		if name != win.Name {
			if snap, _, _, ok = s.vals.peek(s.logf(), windowFor(name, now)); !ok {
				return nil, false, nil
			}
		}
		r, ok := snapshotRow(snap.Rows, addr)
		if !ok {
			return nil, false, nil
		}
		_, label, err := s.rolledFor(ctx, snap.Window, addr)
		if err != nil {
			return nil, false, err
		}
		spans = append(spans, detailSpan{Window: snap.Window, Obligations: r.Obligations, RolledUp: label, Provisional: r.ProvisionalFaults})
	}
	out := map[string]any{
		"window":         main.Window,
		"record_through": main.RecordThrough,
		"validator":      detailOf(row),
		"windows":        spans,
		// when the row was computed, as /v1/validators says it; each span's
		// window ends at its own snapshot's moment
		"computed_at": at.UTC().Format(time.RFC3339Nano),
	}
	if err := s.detailReadings(ctx, addr, win, now, out); err != nil {
		return nil, false, err
	}
	return out, true, nil
}

// snapshotRow is addr's row in a snapshot's rows.
func snapshotRow(rows []validatorRow, addr string) (validatorRow, bool) {
	for _, r := range rows {
		if r.Address == addr {
			return r, true
		}
	}
	return validatorRow{}, false
}

func (s *Server) serveValidatorDetail(w http.ResponseWriter, r *http.Request, addr string, win Window, now time.Time) {
	ctx := r.Context()
	known, err := s.validatorKnown(ctx, addr)
	if err != nil {
		s.writeInternal(w, r.URL.Path, err)
		return
	}
	if !known {
		writeErr(w, 404, validatorNotSeen)
		return
	}
	c := &s.details
	c.init()
	key := addr + "|" + win.Name
	rev := s.paramHoldsRevision()

	c.mu.Lock()
	if e, ok := c.entries[key]; ok && e.rev == rev && time.Since(e.at) < detailTTL {
		c.mu.Unlock()
		writeDetail(w, e)
		return
	}
	// A computation is shared only by readers who asked under one revision.
	// One started before a hold landed has read the rows the hold withdrew,
	// and a reader who arrives after the hold must not be handed its fault.
	fkey := key + "|" + rev
	call, leader := c.flight[fkey], false
	if call == nil {
		call, leader = &detailCall{done: make(chan struct{})}, true
		c.flight[fkey] = call
	}
	c.mu.Unlock()

	if leader {
		s.bg.Add(1) // Close waits for it like any other background work
		go func() {
			defer s.bg.Done()
			s.computeDetail(c, key, fkey, call, addr, win, now, rev)
		}()
	}
	select {
	case <-call.done:
	case <-ctx.Done():
		return // the reader left; the answer still lands for the next one
	}
	if call.err != nil {
		s.writeInternal(w, r.URL.Path, call.err)
		return
	}
	writeDetail(w, call.entry)
}

// computeDetail runs one computation for key and hands it to everyone
// waiting on call.
func (s *Server) computeDetail(c *detailCache, key, fkey string, call *detailCall, addr string, win Window, now time.Time, rev string) {
	defer func() {
		// Kept only if no hold landed while it ran. Its readers asked before
		// the hold and get it; the next one did not, and a late answer from
		// before the hold would also replace the one computed after it.
		keep := call.err == nil && rev != "unreadable" && s.paramHoldsRevision() == rev
		c.mu.Lock()
		delete(c.flight, fkey)
		if keep {
			if len(c.entries) >= detailMaxEntries {
				c.entries = map[string]detailEntry{} // crude, and bounded
			}
			c.entries[key] = call.entry
		}
		c.mu.Unlock()
		close(call.done)
	}()
	ctx, cancel := context.WithTimeout(context.Background(), detailCompute)
	defer cancel()
	select {
	case c.sem <- struct{}{}:
		defer func() { <-c.sem }()
	case <-ctx.Done():
		call.err = ctx.Err()
		return
	}
	compute := c.compute
	if compute == nil {
		compute = s.validatorDetail
	}
	status, out, err := compute(ctx, addr, win, now)
	if err != nil {
		call.err = err
		return
	}
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(out); err != nil {
		call.err = err
		return
	}
	call.entry = detailEntry{status: status, body: buf.Bytes(), at: time.Now(), rev: rev}
}

func writeDetail(w http.ResponseWriter, e detailEntry) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(e.status)
	_, _ = w.Write(e.body)
}
