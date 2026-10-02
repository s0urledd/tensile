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
//     publications to compute again; the last two, and anything that makes
//     a mark doubtful (its row gone, or another row under its rowid, as a
//     VACUUM or the reuse of a deleted newest rowid would leave), compute
//     every publication again.
//
// Not watched, because no status reads it: the retention hold
// (retention_unverified), raw_json stripped from old probes (row_indices is
// a column of its own) and the validator identities.
//
// The memo lives in memory only: a new process computes every publication
// once, in readingChunk batches, in its first market computation (the
// requests that come before it leave readings null rather than wait). On
// the Mocha store of 2 October 2026 that took about 16 s for 8,627
// publications, 6 of them reading each record's original_rows out of its
// raw_json; an update with nothing new costs well under a millisecond. The
// first computation grows with the record, so before mainnet volumes the
// memo wants keeping across restarts as the original-rows memo is
// (derived.go).
type readingMemo struct {
	// upd orders updates: one at a time, and a reader that finds one
	// running waits for it rather than doing the same work again.
	upd sync.Mutex
	mu  sync.Mutex
	// built is false until the first full computation lands.
	built bool
	marks readingMarks
	blobs map[string]readingEntry
	// names interns the publisher addresses, one string per account.
	names map[string]string
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
}

// readingCounts is a publisher's blobs by what Tensile's reading left each.
type readingCounts struct {
	Available         int64 `json:"available"`
	Unavailable       int64 `json:"unavailable"`
	InRetentionWindow int64 `json:"in_retention_window"`
	NotRead           int64 `json:"not_read"`
}

// tableMark is the newest row of a table as last seen: its rowid and its
// key, so a mark whose rowid now holds another row (or none) is caught.
type tableMark struct {
	rowid int64
	key   string
}

// readingMarks is the state of the store a memo was brought up to.
type readingMarks struct {
	publications, probes, payments, amendments, probeFixes, publicationFixes tableMark
	// schema is the highest migration and rawFrom the retention prune's
	// raw_from: when either moves, every status is computed again.
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

// marksHold reports whether every mark of old still holds its row: no rowid
// at or below it was renumbered or reused.
func marksHold(ctx context.Context, db *sql.DB, old, now readingMarks) (bool, error) {
	if old.schema != now.schema || old.rawFrom != now.rawFrom {
		return false, nil
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

// ready reports whether the memo's first full computation has landed.
func (m *readingMemo) ready() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.built
}

// update brings the memo up to the store as it is now.
func (m *readingMemo) update(ctx context.Context, s *Server) error {
	m.upd.Lock()
	defer m.upd.Unlock()
	// The work is the memo's, not the caller's: a request that goes away
	// does not throw away a computation the next caller would start again.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), snapshotTimeoutAll)
	defer cancel()
	db := s.st.DB()
	// The marks are read before anything they cover: a row committed after
	// this lies above them and is taken by the next update.
	now, err := readMarks(ctx, db, s.st)
	if err != nil {
		return err
	}
	m.mu.Lock()
	built, old := m.built, m.marks
	m.mu.Unlock()
	full := !built
	if built {
		ok, err := marksHold(ctx, db, old, now)
		if err != nil {
			return err
		}
		full = !ok
	}
	var hashes []string
	if full {
		if hashes, err = queryStrings(ctx, db, `SELECT promise_hash FROM publications`); err != nil {
			return fmt.Errorf("publications: %w", err)
		}
	} else {
		seen := map[string]bool{}
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
				if !seen[h] {
					seen[h] = true
					hashes = append(hashes, h)
				}
			}
		}
	}
	fresh := make(map[string]readingEntry, len(hashes))
	for i := 0; i < len(hashes); i += readingChunk {
		if err := m.compute(ctx, s, hashes[i:min(i+readingChunk, len(hashes))], fresh); err != nil {
			return err
		}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if full || m.blobs == nil {
		m.blobs = make(map[string]readingEntry, len(fresh))
		m.names = map[string]string{}
	}
	for h, e := range fresh {
		if n, ok := m.names[e.publisher]; ok {
			e.publisher = n
		} else {
			m.names[e.publisher] = e.publisher
		}
		m.blobs[h] = e
	}
	m.built, m.marks = true, now
	return nil
}

// compute reads the status and the publisher of each of hashes into out.
// A hash that is no publication (a probe or a payment ahead of its
// publication) is left out; it is taken when its publication lands.
func (m *readingMemo) compute(ctx context.Context, s *Server, hashes []string, out map[string]readingEntry) error {
	list, err := json.Marshal(hashes)
	if err != nil {
		return err
	}
	rows, err := s.st.DB().QueryContext(ctx, readingFactsSQL, string(list))
	if err != nil {
		return fmt.Errorf("publications: %w", err)
	}
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
		out[h] = e
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
	for h, e := range out {
		rc := verdicts[h]
		if rc == nil {
			continue
		}
		e.status = ""
		if rc.Status == verdict.BlobAvailable || rc.Status == verdict.BlobUnavailable {
			e.status = rc.Status
		}
		out[h] = e
	}
	return nil
}

// counts is every publisher's blobs by status at now: what the Blobs list's
// Tensile lane would say of each of them.
func (m *readingMemo) counts(now time.Time) map[string]readingCounts {
	m.mu.Lock()
	defer m.mu.Unlock()
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
