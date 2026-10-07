package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/scan"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/verdict"
)

// Each publisher's blobs by Tensile's reading of them, over the whole record.
//
// A publisher row counts every blob the account paid for (paidBy: by the
// settlement on record, else by the key that signed the promise) by the
// status /v1/blobs gives that blob (reconstructable), in the words of the
// Blobs list's Tensile lane (web/src/lib/status.ts, lane()): available
// (yes), unavailable (no), and for a blob with neither, in_retention_window
// while its must_serve_until is still ahead and not_read once it has
// passed.
//
// The counts are taken with the market snapshot, every window of it on the
// live lane, and a blob's status is the reading of its probe rows: a
// handful of queries per blob, and every blob ever settled. So each
// publication's status is computed once and kept (readingMemo), and computed
// again only when something it is computed from has changed. Two things
// make that exact rather than hopeful:
//
//   - Available and Unavailable do not depend on the clock
//     (verdict.BlobReading decides them from the rows alone), and the clock
//     only splits the rest between in_retention_window and not_read, which
//     is done from must_serve_until when the rows are counted. A status
//     kept is never stale because time passed.
//   - Everything a status is computed from changes in one of a few ways,
//     each of which leaves a mark the memo watches (readingMarks): a
//     publication or a probe row added (their rowids pass the mark); a probe
//     row's verdict amended (ApplyAmendment logs it in probe_amendments in
//     the same transaction); a deadline corrected (probe_corrections and
//     publication_corrections, likewise); a settlement recorded, which can
//     move who a blob is counted for (payments); probe rows deleted by the
//     retention prune (raw_from moves) or by a migration, which may also
//     rewrite columns (the schema version moves). The first kinds name the
//     publications to compute again, and so does the prune: it deletes
//     whole days of rows older than raw_from, so only a publication with a
//     row scheduled before it (oldest) can have lost one. A migration, and
//     anything that makes a mark doubtful (its row gone, or another row
//     under its rowid, as a VACUUM or the reuse of a deleted newest rowid
//     would leave), computes every publication again.
//
// Not watched, because no status reads it: the retention hold
// (retention_unverified), raw_json stripped from old probes (row_indices is
// a column of its own), the validator identities, and the collapse of a
// sampled-out publication's rows into its decision (store/sampledout.go),
// which only takes a publication whose every row is a NOT_PROBED row, read
// or not the same.
//
// Computing every publication is the one cost that grows with the record:
// on the Mocha store of 2 October 2026 about 16 s for 8,627 publications, 6
// of them reading each record's original_rows out of its raw_json, which
// puts a year of mainnet volume at tens of minutes. So it never holds the
// market snapshot up for long or fails it on a timeout: each market
// computation gives it at most a slice of time (readingSlice), and it goes
// on from where it stopped at the next. Until the first lands the rows
// carry no reading (null) and the site reads the blobs itself; while a
// later one runs, the counts are the memo's as it stood before. An update
// with nothing new costs well under a millisecond.
//
// The memo lives in memory only: a new process computes every publication
// once. Kept across restarts as the endorsement ledger is (derived.go), a
// restart would skip that; it is the next step before mainnet volumes.
type readingMemo struct {
	// upd orders updates: one at a time, and a reader that finds one
	// running waits for it rather than doing the same work again. It also
	// guards full and computed.
	upd sync.Mutex
	mu  sync.Mutex
	// built is false until the first full computation lands.
	built bool
	marks readingMarks
	blobs map[string]readingEntry
	// names interns the publisher addresses, one string per account.
	names map[string]string
	// full is a full computation under way, swapped in whole when it
	// lands.
	full *readingBuild
	// slice is the time one market computation gives a full computation
	// (readingSlice when zero), and chunk how many publications one batch
	// computes (readingChunk when zero).
	slice time.Duration
	chunk int
	// computed is how many publications the last update computed.
	computed int
}

// readingBuild is a full computation under way: the marks it started from,
// the publications it has yet to compute and the ones it has.
type readingBuild struct {
	marks readingMarks
	todo  []string
	blobs map[string]readingEntry
}

// readingEntry is what the counts need of one publication.
type readingEntry struct {
	// publisher is the account paidBy counts the blob for; "" when neither
	// a settlement nor a readable signer key names one.
	publisher string
	// msu is must_serve_until; zero when it does not parse.
	msu time.Time
	// status is verdict.BlobAvailable, verdict.BlobUnavailable, or "" for
	// any other status.
	status string
	// oldest is the earliest scheduled_at of its probe rows (Unix seconds),
	// 0 when it has none: what the prune is held against.
	oldest int64
}

// readingCounts is a publisher's blobs by what Tensile's reading left each.
type readingCounts struct {
	Available         int64 `json:"available"`
	Unavailable       int64 `json:"unavailable"`
	InRetentionWindow int64 `json:"in_retention_window"`
	NotRead           int64 `json:"not_read"`
}

// readingSlice is how long one market computation lets a full computation
// of the memo run; four windows refresh about every ten seconds, so one
// runs at a few thousand publications a second without any of them waiting
// more than a few seconds. readingSliceWarm is the bound in a warm-up run
// (WarmSnapshots), which serves nothing, so that the files it leaves carry
// the readings.
const (
	readingSlice     = 3 * time.Second
	readingSliceWarm = 2 * time.Minute
)

// tableMark is the newest row of a table as last seen: its rowid and its
// key, so a mark whose rowid now holds another row (or none) is caught.
type tableMark struct {
	rowid int64
	key   string
}

// readingMarks is the state of the store a memo was brought up to.
type readingMarks struct {
	publications, probes, payments, amendments, probeFixes, publicationFixes tableMark
	// schema is the highest migration: when it moves, every status is
	// computed again. rawFrom is the retention prune's raw_from: when it
	// moves on, the publications with rows before it are.
	schema  int64
	rawFrom string
}

// readingTable is one table the memo watches: its key, and the publications
// a row added above the mark names (a statement over rowid > ?1 AND rowid
// <= ?2).
type readingTable struct {
	name, key, changed string
	mark               func(*readingMarks) *tableMark
}

var readingTables = []readingTable{
	{"publications", "promise_hash",
		`SELECT promise_hash FROM publications WHERE rowid > ?1 AND rowid <= ?2`,
		func(m *readingMarks) *tableMark { return &m.publications }},
	{"probes", "dedupe_key",
		`SELECT DISTINCT promise_hash FROM probes WHERE rowid > ?1 AND rowid <= ?2`,
		func(m *readingMarks) *tableMark { return &m.probes }},
	{"payments", "dedupe_key",
		// the unary + keeps kind off the index choice: payments_kind_time
		// would walk every settlement for the few rows above the mark
		`SELECT promise_hash FROM payments WHERE rowid > ?1 AND rowid <= ?2 AND +kind = 'settlement' AND promise_hash <> ''`,
		func(m *readingMarks) *tableMark { return &m.payments }},
	{"probe_amendments", "dedupe_key",
		`SELECT p.promise_hash FROM probe_amendments a JOIN probes p ON p.dedupe_key = a.dedupe_key WHERE a.rowid > ?1 AND a.rowid <= ?2`,
		func(m *readingMarks) *tableMark { return &m.amendments }},
	{"probe_corrections", "dedupe_key || ':' || uncertainty_id",
		`SELECT promise_hash FROM probe_corrections WHERE rowid > ?1 AND rowid <= ?2`,
		func(m *readingMarks) *tableMark { return &m.probeFixes }},
	{"publication_corrections", "promise_hash || ':' || uncertainty_id",
		`SELECT promise_hash FROM publication_corrections WHERE rowid > ?1 AND rowid <= ?2`,
		func(m *readingMarks) *tableMark { return &m.publicationFixes }},
}

// newestSQL reads the table's newest row: one step from the end of its
// rowid order, whatever the table holds.
func (t readingTable) newestSQL() string {
	return `SELECT rowid, ` + t.key + ` FROM ` + t.name + ` ORDER BY rowid DESC LIMIT 1`
}

// atSQL reads the key of the row at a rowid.
func (t readingTable) atSQL() string {
	return `SELECT ` + t.key + ` FROM ` + t.name + ` WHERE rowid = ?`
}

// readingChunk is how many publications one batch computes.
const readingChunk = 2000

// readingSel selects the publications of a batch, passed as a JSON list.
const readingSel = `promise_hash IN (SELECT value FROM json_each(?))`

// readingFactsSQL is what a batch reads of each publication besides its
// status: its deadline, and who paid (the settlement on record, else the
// key that signed the promise, which only Go reads).
const readingFactsSQL = `SELECT pub.promise_hash, pub.must_serve_until, pub.signer_public_key,
			COALESCE((SELECT pay.publisher FROM payments pay WHERE pay.promise_hash = pub.promise_hash AND pay.kind = 'settlement'
				ORDER BY pay.rowid LIMIT 1), '')
		FROM publications pub WHERE pub.` + readingSel

// readingOldestSQL is each publication's earliest probe row of a batch,
// read off probes_promise alone.
const readingOldestSQL = `SELECT promise_hash, MIN(scheduled_at) FROM probes WHERE ` + readingSel + ` GROUP BY promise_hash`

// rawFromLayout is how raw_from names a day (rollup).
const rawFromLayout = "2006-01-02"

// pruneMargin is added to raw_from before a publication's oldest row is
// held against it: the prune deletes by started_at and oldest is
// scheduled_at, which a start can precede by moments, never by a day.
const pruneMargin = 24 * time.Hour

// readMarks reads the store's marks now.
func readMarks(ctx context.Context, db *sql.DB, st *store.Store) (readingMarks, error) {
	var m readingMarks
	for _, t := range readingTables {
		mk := t.mark(&m)
		err := db.QueryRowContext(ctx, t.newestSQL()).Scan(&mk.rowid, &mk.key)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return m, fmt.Errorf("%s mark: %w", t.name, err)
		}
	}
	if err := db.QueryRowContext(ctx, `SELECT COALESCE(MAX(version), 0) FROM schema_migrations`).Scan(&m.schema); err != nil {
		return m, fmt.Errorf("schema version: %w", err)
	}
	v, err := st.Meta("raw_from")
	if err != nil {
		return m, fmt.Errorf("raw_from: %w", err)
	}
	m.rawFrom = v
	return m, nil
}

// pruned reports whether raw_from moved on from was to is, as the prune
// moves it (a day, later than the last), and the day it moved to.
func pruned(was, is string) (time.Time, bool) {
	day, err := time.Parse(rawFromLayout, is)
	if err != nil {
		return time.Time{}, false
	}
	if was == "" {
		return day, true
	}
	prev, err := time.Parse(rawFromLayout, was)
	return day, err == nil && day.After(prev)
}

// marksHold reports whether every mark of old still holds its row (no rowid
// at or below it was renumbered or reused), the schema is the same, and
// raw_from is where it was or moved on as the prune moves it.
func marksHold(ctx context.Context, db *sql.DB, old, now readingMarks) (bool, error) {
	if old.schema != now.schema {
		return false, nil
	}
	if old.rawFrom != now.rawFrom {
		if _, ok := pruned(old.rawFrom, now.rawFrom); !ok {
			return false, nil
		}
	}
	for _, t := range readingTables {
		was, is := t.mark(&old), t.mark(&now)
		if is.rowid < was.rowid {
			return false, nil
		}
		if was.rowid == 0 {
			continue
		}
		var key string
		err := db.QueryRowContext(ctx, t.atSQL(), was.rowid).Scan(&key)
		if errors.Is(err, sql.ErrNoRows) || (err == nil && key != was.key) {
			return false, nil
		}
		if err != nil {
			return false, fmt.Errorf("%s mark: %w", t.name, err)
		}
	}
	return true, nil
}

// sliceFor is the time one market computation gives a full computation.
func (m *readingMemo) sliceFor() time.Duration {
	if m.slice > 0 {
		return m.slice
	}
	return readingSlice
}

func (m *readingMemo) chunkSize() int {
	if m.chunk > 0 {
		return m.chunk
	}
	return readingChunk
}

// update brings the memo up to the store as it is now. A full computation
// (the first, or one a doubtful mark calls for) runs for at most slice (0:
// to the end, and at least one batch whatever the slice) and goes on at
// the next update; the memo is brought up to now only once it has landed.
func (m *readingMemo) update(ctx context.Context, s *Server, slice time.Duration) error {
	m.upd.Lock()
	defer m.upd.Unlock()
	m.computed = 0
	// The work is the memo's, not the caller's: a request that goes away
	// does not throw away a computation the next caller would start again.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), snapshotTimeout)
	defer cancel()
	db := s.st.DB()
	// The marks are read before anything they cover: a row committed after
	// this lies above them and is taken by the next update.
	now, err := readMarks(ctx, db, s.st)
	if err != nil {
		return err
	}
	// A full computation under way goes on from where it stopped while its
	// marks hold, and starts again when they do not.
	if m.full != nil {
		ok, err := marksHold(ctx, db, m.full.marks, now)
		if err != nil {
			return err
		}
		if !ok {
			m.full = nil
		}
	}
	m.mu.Lock()
	built, old := m.built, m.marks
	m.mu.Unlock()
	if m.full == nil {
		start := !built
		if built {
			ok, err := marksHold(ctx, db, old, now)
			if err != nil {
				return err
			}
			start = !ok
		}
		if start {
			hashes, err := queryStrings(ctx, db, `SELECT promise_hash FROM publications`)
			if err != nil {
				return fmt.Errorf("publications: %w", err)
			}
			m.full = &readingBuild{marks: now, todo: hashes, blobs: make(map[string]readingEntry, len(hashes))}
		}
	}
	if f := m.full; f != nil {
		began := time.Now()
		for len(f.todo) > 0 {
			if slice > 0 && m.computed > 0 && time.Since(began) >= slice {
				return nil
			}
			n := min(m.chunkSize(), len(f.todo))
			if err := m.compute(ctx, s, f.todo[:n], f.blobs); err != nil {
				return err
			}
			f.todo, m.computed = f.todo[n:], m.computed+n
		}
		names := map[string]string{}
		for h, e := range f.blobs {
			e.publisher = intern(names, e.publisher)
			f.blobs[h] = e
		}
		m.mu.Lock()
		m.blobs, m.names, m.marks, m.built = f.blobs, names, f.marks, true
		m.mu.Unlock()
		m.full, old = nil, f.marks
	}

	// What changed since old: the publications rows above its marks name,
	// and those the prune may have taken rows of. Only this goroutine
	// writes blobs (upd), so it is read here without mu.
	var hashes []string
	seen := map[string]bool{}
	add := func(h string) {
		if !seen[h] {
			seen[h] = true
			hashes = append(hashes, h)
		}
	}
	for _, t := range readingTables {
		was, is := t.mark(&old), t.mark(&now)
		if is.rowid <= was.rowid {
			continue
		}
		hs, err := queryStrings(ctx, db, t.changed, was.rowid, is.rowid)
		if err != nil {
			return fmt.Errorf("%s changed: %w", t.name, err)
		}
		for _, h := range hs {
			add(h)
		}
	}
	if day, ok := pruned(old.rawFrom, now.rawFrom); ok && old.rawFrom != now.rawFrom {
		cut := day.Add(pruneMargin).Unix()
		for h, e := range m.blobs {
			if e.oldest != 0 && e.oldest < cut {
				add(h)
			}
		}
	}
	fresh := make(map[string]readingEntry, len(hashes))
	for i := 0; i < len(hashes); i += m.chunkSize() {
		chunk := hashes[i:min(i+m.chunkSize(), len(hashes))]
		if err := m.compute(ctx, s, chunk, fresh); err != nil {
			return err
		}
		m.computed += len(chunk)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for h, e := range fresh {
		e.publisher = intern(m.names, e.publisher)
		m.blobs[h] = e
	}
	m.marks = now
	return nil
}

// intern is v as names already holds it, one string per account.
func intern(names map[string]string, v string) string {
	if n, ok := names[v]; ok {
		return n
	}
	names[v] = v
	return v
}

// compute reads the status, the publisher and the oldest probe row of each
// of hashes into out. A hash that is no publication (a probe or a payment
// ahead of its publication) is left out; it is taken when its publication
// lands.
func (m *readingMemo) compute(ctx context.Context, s *Server, hashes []string, out map[string]readingEntry) error {
	list, err := json.Marshal(hashes)
	if err != nil {
		return err
	}
	db := s.st.DB()
	rows, err := db.QueryContext(ctx, readingFactsSQL, string(list))
	if err != nil {
		return fmt.Errorf("publications: %w", err)
	}
	got := make(map[string]readingEntry, len(hashes))
	for rows.Next() {
		var h, msu, payer string
		var key sql.NullString
		if err := rows.Scan(&h, &msu, &key, &payer); err != nil {
			rows.Close()
			return err
		}
		e := readingEntry{publisher: payer, msu: parseStoreTime(msu)}
		if e.publisher == "" {
			// as paidBy: a publication with no settlement on record is the
			// account whose key signed its promise
			if p, err := scan.PublisherOf(key.String); err == nil {
				e.publisher = p
			}
		}
		got[h] = e
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	rows, err = db.QueryContext(ctx, readingOldestSQL, string(list))
	if err != nil {
		return fmt.Errorf("oldest rows: %w", err)
	}
	for rows.Next() {
		var h, at string
		if err := rows.Scan(&h, &at); err != nil {
			rows.Close()
			return err
		}
		if e, ok := got[h]; ok {
			if t := parseStoreTime(at); !t.IsZero() {
				e.oldest = t.Unix()
			} else {
				// a time that does not parse is held against every prune
				e.oldest = 1
			}
			got[h] = e
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	// The status as /v1/blobs gives it: the batch is held to the reference
	// (TestReconstructBatchMatchesReference). Live: Available and
	// Unavailable do not depend on the moment asked at.
	verdicts, err := s.reconstructBatch(ctx, readingSel, len(hashes), asOfPin{now: s.now()}, string(list))
	if err != nil {
		return fmt.Errorf("statuses: %w", err)
	}
	for h, e := range got {
		if rc := verdicts[h]; rc != nil && (rc.Status == verdict.BlobAvailable || rc.Status == verdict.BlobUnavailable) {
			e.status = rc.Status
		}
		out[h] = e
	}
	return nil
}

// counts is every publisher's blobs by status at now: what the Blobs list's
// Tensile lane would say of each of them. Nil until the first full
// computation has landed.
func (m *readingMemo) counts(now time.Time) map[string]readingCounts {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.built {
		return nil
	}
	out := map[string]readingCounts{}
	for _, e := range m.blobs {
		if e.publisher == "" {
			continue
		}
		c := out[e.publisher]
		switch {
		case e.status == verdict.BlobAvailable:
			c.Available++
		case e.status == verdict.BlobUnavailable:
			c.Unavailable++
		case e.msu.After(now):
			c.InRetentionWindow++
		default:
			c.NotRead++
		}
		out[e.publisher] = c
	}
	return out
}

// parseStoreTime reads a time as the store writes it, or RFC 3339; zero
// when it is neither.
func parseStoreTime(v string) time.Time {
	if t, err := time.Parse(store.TimeLayout, v); err == nil {
		return t
	}
	if t, err := time.Parse(time.RFC3339Nano, v); err == nil {
		return t
	}
	return time.Time{}
}

// queryStrings is a statement's first column.
func queryStrings(ctx context.Context, db *sql.DB, q string, args ...any) ([]string, error) {
	rows, err := db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}
