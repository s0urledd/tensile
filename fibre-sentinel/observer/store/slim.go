package store

import (
	"bytes"
	"container/list"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/slim"
)

// The record in its slim form (internal/slim, migrations 27 and 28).
//
// publications.raw_json and probes.raw_json hold the slim record of each row this build writes: every field of the
// JSONL line but those computed again from the others, from which the line is written back byte for byte (Record,
// ProbeRecord). reachability.raw_json holds an endpoint check the same way with nothing derived (ReachRecord). Each
// is kept slim only where it reads back to its line byte for byte, and as its line otherwise (publicationBody). A row an
// earlier build wrote keeps its line, which always starts with '{' and a slim record never does, so both are read the
// same way and nothing has to be rewritten.
//
// The copies the slim record replaces are marked, not stored: probes.row_indices and assignments.rows_json hold
// RowsAssigned ("=") where the list is the validator's own assignment in its order, which the publication's
// validator set and commitment give again (fibre-assign); any other list is kept as it was. Rows and lists are
// resolved by RowIndices and AssignedRows. The per-reading count of distinct verified rows the rollup used to take
// from the lists in SQL is kept in reading_rows (exact), recomputed as each verified row arrives.
//
// What every slim record shares (the dictionary, object shapes, validator sets, host vectors) is in slim_entries,
// written in the same transaction as the record that first needed it, and read by every process that decodes.

// RowsAssigned marks a row list that is the validator's own assignment.
const RowsAssigned = "="

func isLine(b []byte) bool { return len(b) > 0 && b[0] == '{' }

// asIs says whether raw is read as it is: a line, or nothing (a record an earlier build's retention stripped).
func asIs(raw []byte) bool { return len(raw) == 0 || isLine(raw) }

// slimState is a store's slim tables and its cache of publications as their readings use them.
type slimState struct {
	mu     sync.Mutex
	t      *slim.Tables
	loaded bool
	cache  pubCache
	// enc is the one encoding: held from the tables' Stored() before a record is encoded through the commit or
	// rollback of the transaction that keeps what it added. Pending hands out every entry past the count,
	// whoever added it, and Rollback forgets every one, so two encodings interleaved would have one transaction keep
	// or forget the other's entries. The tables themselves are safe to decode from any number of goroutines.
	enc sync.Mutex
}

// ErrUndecodable is what a stored record that does not read back to its line is (errors.Is): a slim body the tables
// do not decode, or a row list marked as an assignment its publication does not give. It belongs to that row: a read
// over many rows leaves the row aside, saying so, and goes on with the others. Any other error (the database's own)
// is not one, and stays the read's.
var ErrUndecodable = errors.New("record does not decode")

// undecodable marks err as ErrUndecodable, its text unchanged.
type undecodable struct{ err error }

func (e undecodable) Error() string        { return e.err.Error() }
func (e undecodable) Unwrap() error        { return e.err }
func (e undecodable) Is(target error) bool { return target == ErrUndecodable }

// leftAside says, once per process for each row, that a read went on past a row whose record does not decode: the
// row keeps what it is (withheld, or deferred) until a build that reads it, and the log names it and why.
func (s *Store) leftAside(row string, err error) {
	if _, said := s.said.LoadOrStore(row, true); !said {
		log.Printf("store: %s left aside, its record does not decode: %v", row, err)
	}
}

// pubCache keeps the publications most recently used, by promise hash.
type pubCache struct {
	max   int
	order *list.List
	byKey map[string]*list.Element
}

type pubEntry struct {
	key string
	pub *slim.Pub
}

const pubCacheSize = 512

func (c *pubCache) get(k string) *slim.Pub {
	if c.byKey == nil {
		return nil
	}
	if e, ok := c.byKey[k]; ok {
		c.order.MoveToFront(e)
		return e.Value.(*pubEntry).pub
	}
	return nil
}

func (c *pubCache) put(k string, p *slim.Pub) {
	if c.byKey == nil {
		c.max, c.order, c.byKey = pubCacheSize, list.New(), map[string]*list.Element{}
	}
	if e, ok := c.byKey[k]; ok {
		e.Value.(*pubEntry).pub = p
		c.order.MoveToFront(e)
		return
	}
	c.byKey[k] = c.order.PushFront(&pubEntry{k, p})
	for c.order.Len() > c.max {
		last := c.order.Back()
		c.order.Remove(last)
		delete(c.byKey, last.Value.(*pubEntry).key)
	}
}

// tables returns the slim tables, loading what the store holds the first time and, with refresh, whatever was added
// since (by another process: the collector writes, the API reads).
func (s *Store) tables(ctx context.Context, q Querier, refresh bool) (*slim.Tables, error) {
	s.slim.mu.Lock()
	if s.slim.t == nil {
		s.slim.t = slim.NewTables()
	}
	t, loaded := s.slim.t, s.slim.loaded
	s.slim.mu.Unlock()
	if loaded && !refresh {
		return t, nil
	}
	// read without the lock held: a reader waiting on the database must not hold up the others
	have := t.Stored()
	rows, err := q.QueryContext(ctx, `SELECT kind, id, body FROM slim_entries
		WHERE (kind = 0 AND id >= ?) OR (kind = 1 AND id >= ?) OR (kind = 2 AND id >= ?) OR (kind = 3 AND id >= ?)
		ORDER BY kind, id`, have[0], have[1], have[2], have[3])
	if err != nil {
		return nil, err
	}
	var es []slim.Entry
	for rows.Next() {
		var e slim.Entry
		if err := rows.Scan(&e.Kind, &e.ID, &e.Body); err != nil {
			rows.Close()
			return nil, err
		}
		es = append(es, e)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	s.slim.mu.Lock()
	defer s.slim.mu.Unlock()
	for _, e := range es {
		if e.ID < t.Stored()[e.Kind] {
			continue // another reader loaded it meanwhile
		}
		if err := t.Add(e); err != nil {
			return nil, fmt.Errorf("slim entry %d/%d: %w", e.Kind, e.ID, err)
		}
	}
	s.slim.loaded = true
	return t, nil
}

// decodeRetry runs f with the tables, and once more after loading the entries added since when f meets one it does
// not know yet. What f still refuses is ErrUndecodable; what loading the tables refuses is not.
func (s *Store) decodeRetry(ctx context.Context, q Querier, f func(*slim.Tables) error) error {
	t, err := s.tables(ctx, q, false)
	if err != nil {
		return err
	}
	err = guarded(func() error { return f(t) })
	if errors.Is(err, slim.ErrUnknownEntry) {
		if t, err = s.tables(ctx, q, true); err != nil {
			return err
		}
		err = guarded(func() error { return f(t) })
	}
	if err != nil {
		return undecodable{err}
	}
	return nil
}

// guarded runs a decoding. A decoder that panics has met a record of a shape it does not expect (a slim publication
// without an assignment, today), which is that record failing to decode: the row's error, not the process's end.
// The tables are as they were: a decoding only reads them, and releases their lock as it unwinds.
func guarded(f func() error) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("slim: the decoder failed on this record: %v", r)
		}
	}()
	return f()
}

// pub is the publication of a promise as its readings use it: nil when the store does not hold it.
func (s *Store) pub(ctx context.Context, q Querier, hash string) (*slim.Pub, error) {
	s.slim.mu.Lock()
	p := s.slim.cache.get(hash)
	s.slim.mu.Unlock()
	if p != nil {
		return p, nil
	}
	var raw []byte
	err := q.QueryRowContext(ctx, `SELECT raw_json FROM publications WHERE promise_hash = ?`, hash).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if isLine(raw) {
		if p, err = slim.PubFromLine(raw); err != nil {
			err = undecodable{err}
		}
	} else {
		err = s.decodeRetry(ctx, q, func(t *slim.Tables) error {
			var e error
			_, p, e = t.DecodePublication(raw)
			return e
		})
	}
	if err != nil {
		return nil, fmt.Errorf("publication %s: %w", hash, err)
	}
	s.slim.mu.Lock()
	s.slim.cache.put(hash, p)
	s.slim.mu.Unlock()
	return p, nil
}

func (s *Store) lookup(ctx context.Context, q Querier) slim.Lookup {
	return func(hash string) *slim.Pub {
		p, _ := s.pub(ctx, q, hash)
		return p
	}
}

// Record is a publication's record line, whatever form its row keeps it in. q is what the caller reads through
// (its read transaction, if any): nothing is read beside it.
func (s *Store) Record(ctx context.Context, q Querier, raw []byte) ([]byte, error) {
	if asIs(raw) {
		return raw, nil
	}
	var line []byte
	err := s.decodeRetry(ctx, q, func(t *slim.Tables) error {
		var e error
		line, _, e = t.DecodePublication(raw)
		return e
	})
	return line, err
}

// ProbeRecord is a reading's record line, whatever form its row keeps it in; promiseHash is the row's. q as Record.
func (s *Store) ProbeRecord(ctx context.Context, q Querier, promiseHash string, raw []byte) ([]byte, error) {
	if asIs(raw) {
		return raw, nil
	}
	p, err := s.pub(ctx, q, promiseHash)
	if err != nil {
		return nil, err
	}
	var line []byte
	err = s.decodeRetry(ctx, q, func(t *slim.Tables) error {
		var e error
		line, e = t.DecodeMeasurement(raw, p, s.lookup(ctx, q))
		return e
	})
	return line, err
}

// ReachRecord is an endpoint check's record line, whatever form its reachability row keeps it in. q as Record.
func (s *Store) ReachRecord(ctx context.Context, q Querier, raw []byte) ([]byte, error) {
	if asIs(raw) {
		return raw, nil
	}
	var line []byte
	err := s.decodeRetry(ctx, q, func(t *slim.Tables) error {
		var e error
		line, e = t.DecodeReachability(raw)
		return e
	})
	return line, err
}

// RowIndices is a probe row's row_indices as a JSON list: the column as it is, or, where it marks the validator's own
// assignment, that assignment. "" where the row has none. q as Record; not while q has rows open.
func (s *Store) RowIndices(ctx context.Context, q Querier, promiseHash, validator, col string) (string, error) {
	if col != RowsAssigned {
		return col, nil
	}
	return s.assignedJSON(ctx, q, promiseHash, validator)
}

// AssignedRows is an assignments row's rows_json: the column as it is, or, where it marks the assignment, the rows
// the publication's validator set and commitment give the validator.
func (s *Store) AssignedRows(ctx context.Context, q Querier, promiseHash, validator string, col sql.NullString) (sql.NullString, error) {
	if !col.Valid || col.String != RowsAssigned {
		return col, nil
	}
	j, err := s.assignedJSON(ctx, q, promiseHash, validator)
	return sql.NullString{String: j, Valid: err == nil}, err
}

func (s *Store) assignedJSON(ctx context.Context, q Querier, promiseHash, validator string) (string, error) {
	p, err := s.pub(ctx, q, promiseHash)
	if err != nil {
		return "", err
	}
	rows, ok := p.Rows(validator)
	if !ok {
		return "", undecodable{fmt.Errorf("no assignment of %s on record for %s", validator, promiseHash)}
	}
	b, err := json.Marshal(rows)
	return string(b), err
}

// sameRows reports whether got is exactly the validator's assigned rows, in their order.
func sameRows(p *slim.Pub, validator string, got []uint32) bool {
	rows, ok := p.Rows(validator)
	if !ok || len(rows) != len(got) || len(rows) == 0 {
		return false
	}
	for i := range rows {
		if uint32(rows[i]) != got[i] {
			return false
		}
	}
	return true
}

// A record is kept in its slim form only where that reads back to its line byte for byte, and as its line otherwise,
// as an endpoint check is (slim.EncodeReachability). The case is a string carrying a byte that is not UTF-8 (a remote
// server's error text, a host the chain never checked): encoding/json writes it as the escape of U+FFFD, the slim form
// keeps the character and writes it back unescaped, so the line it gives is the same JSON value in other bytes, and
// the export of its day would not reproduce. kept is the tables' count before the encoding: a line needs none of
// what the attempt added, which is forgotten.

// publicationBody is what publications.raw_json keeps of the line raw, and what its readings take from it.
func publicationBody(t *slim.Tables, raw []byte, kept [4]int) (any, *slim.Pub, error) {
	b, pub, err := t.EncodePublication(raw)
	if err != nil {
		return nil, nil, err
	}
	var back []byte
	if guarded(func() (err error) { back, _, err = t.DecodePublication(b); return err }) == nil && bytes.Equal(back, raw) {
		return b, pub, nil
	}
	t.Rollback(kept)
	return string(raw), pub, nil
}

// measurementBody is what probes.raw_json keeps of the line raw, a reading of pub.
func measurementBody(t *slim.Tables, raw []byte, pub *slim.Pub, lookup slim.Lookup, kept [4]int) (any, error) {
	b, err := t.EncodeMeasurement(raw, pub, lookup)
	if err != nil {
		return nil, err
	}
	var back []byte
	if guarded(func() (err error) { back, err = t.DecodeMeasurement(b, pub, lookup); return err }) == nil && bytes.Equal(back, raw) {
		return b, nil
	}
	t.Rollback(kept)
	return string(raw), nil
}

// keepEntries writes the table entries the last encoding added, in tx.
func keepEntries(tx *sql.Tx, es []slim.Entry) error {
	for _, e := range es {
		if _, err := tx.Exec(`INSERT INTO slim_entries (kind, id, body) VALUES (?, ?, ?)`, e.Kind, e.ID, e.Body); err != nil {
			return fmt.Errorf("slim entry %d/%d: %w", e.Kind, e.ID, err)
		}
	}
	return nil
}

// updateReadingRows recomputes the count of distinct verified rows of the reading of promiseHash at scheduledAt,
// in tx: what exactSQL in the rollup counts.
func (s *Store) updateReadingRows(ctx context.Context, tx *sql.Tx, promiseHash, scheduledAt string) error {
	rows, err := tx.QueryContext(ctx, `SELECT validator_address, row_indices FROM probes
		WHERE promise_hash = ? AND scheduled_at = ? AND commitment_verified = 1 AND row_indices IS NOT NULL`, promiseHash, scheduledAt)
	if err != nil {
		return err
	}
	type pr struct{ v, idx string }
	var all []pr
	for rows.Next() {
		var x pr
		if err := rows.Scan(&x.v, &x.idx); err != nil {
			rows.Close()
			return err
		}
		all = append(all, x)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	seen := map[int64]struct{}{}
	for _, x := range all {
		j := x.idx
		if j == RowsAssigned {
			if j, err = s.assignedJSON(ctx, tx, promiseHash, x.v); err != nil {
				return err
			}
		}
		var idx []json.Number
		d := json.NewDecoder(strings.NewReader(j))
		d.UseNumber()
		if err := d.Decode(&idx); err != nil {
			continue // json_each would have refused the row; it counts nothing
		}
		for _, n := range idx {
			if v, err := n.Int64(); err == nil {
				seen[v] = struct{}{}
			}
		}
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO reading_rows (promise_hash, scheduled_at, exact) VALUES (?, ?, ?)
		ON CONFLICT(promise_hash, scheduled_at) DO UPDATE SET exact = excluded.exact`, promiseHash, scheduledAt, len(seen))
	return err
}
