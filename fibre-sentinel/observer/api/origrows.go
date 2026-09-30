package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// originalRowsMemo remembers each publication's original_rows, the one value
// the load figures need from its raw_json, so a snapshot refresh does not
// parse a 100 KB record per publication to read one integer.
//
// Remembering it is safe because the record never changes: UpsertPublication
// inserts with ON CONFLICT DO NOTHING, no UPDATE touches raw_json and nothing
// deletes a publication. The memo is keyed by promise hash, not rowid, so a
// VACUUM that renumbered rows would cost it nothing. It never supplies a value
// the record would not: an entry holds exactly what json_extract returned (an
// integer, or NULL when the record has none), and anything else, a real or a
// string, is not kept, so the statement reads the record for it as it always
// did.
//
// Nothing waits on it. A computation looks up the publications it is about
// to read that the memo does not know yet, without holding the lock while it
// does, and passes the statement only the entries for its own publications.
// Two computations that start cold may both look up the same records, which
// costs what the statement would have cost without a memo, once.
//
// With file set it is kept across restarts (derived.go): read back before
// the first computation uses it, and written again when it has grown, at
// most every memoSaveEvery unless it has doubled since. What a restart
// finds missing is only what was learned since the last write, and it is
// looked up as any new publication is. The file's mark is a publications
// rowid, so a VACUUM that renumbered rows costs one rebuild at the next
// start, not a wrong value.
//
// The file is written in the background (saveLater), never by the
// computation that learned what it adds, and no computation waits for it:
// the file is rewritten whole, and at a year's volume that is hundreds of
// megabytes, seconds of work that would otherwise come out of a snapshot's
// timeout.
//
// The file names the store the entries were read from (store), not the store
// as it is when the file is written: a migration between the two may have
// rewritten records the memo will not read again. A write that finds the
// store at another identity writes nothing and drops the memo, which learns
// its entries again from the store as it is now.
//
// The zero value is ready to use.
type originalRowsMemo struct {
	mu sync.Mutex
	// vals holds, per promise hash, original_rows (null false) or the
	// record's lack of one (null true).
	vals map[string]memoRows
	// odd are the hashes whose value is neither an integer nor NULL: looked
	// up once, then left to the statement every time.
	odd map[string]bool
	// saved is how many entries the file holds as last written and savedAt
	// when; writing, while a write saveLater started runs, is closed when it
	// ends.
	saved   int
	savedAt time.Time
	writing chan struct{}
	// gen counts the times save dropped the memo (drop), so that a lookup
	// that read its records before a drop keeps none of them after it.
	gen uint64

	// file is where the memo is kept, or "" for nowhere. fileMu orders
	// reading the file and writing it, and is taken by a computation only
	// until the file has been read: opened says it has been (or found
	// missing, or refused) by this process. origin says how this process's
	// memo began, which log (when set) is told.
	file   string
	log    logf
	fileMu sync.Mutex
	opened atomic.Bool
	origin string
	// store is the identity of the store the entries were read from, and
	// the one the file is written under: the file's, when open kept it, or
	// the store's, read before any entry was, when open found nothing to
	// keep. fileMu guards it.
	store storeIdentity
}

// memoSaveEvery is how often a grown memo is written out.
const memoSaveEvery = 10 * time.Minute

// memoDefinition is what the memo's entries are computed with: the value it
// reads and how learn keeps it.
var memoDefinition = definitionOf(originalRowsSQL, "memo "+strconv.Itoa(memoVersion))

// memoFile is the memo on disk: the hashes grouped by their original_rows,
// which is the same number for nearly every publication.
type memoFile struct {
	derivedHeader
	// Mark is the newest publication when the file was written: every
	// entry is of a publication at or below it.
	Mark struct {
		Rowid       int64  `json:"rowid"`
		PromiseHash string `json:"promise_hash"`
	} `json:"mark"`
	Values map[string][]string `json:"values"`
	Nulls  []string            `json:"nulls"`
}

// memoCheckSQL reads original_rows again from the records of the newest
// publications at or below a rowid (?), newest first, as many as the
// second argument says.
const memoCheckSQL = `SELECT p.promise_hash, ` + originalRowsSQL + ` FROM publications p
		WHERE p.rowid <= ? ORDER BY p.rowid DESC LIMIT ?`

// memoChecked is how many of the newest publications at or below the mark
// are read again when a file is loaded and compared with its entries.
const memoChecked = 32

type memoRows struct {
	n    int64
	null bool
}

// originalRowsSQL is the value the memo remembers, as rowBytesSQL reads it.
const originalRowsSQL = `json_extract(p.raw_json, '$.assignment.protocol_params.original_rows')`

// memoKey reports whether a hash can go into the JSON object doc builds
// without escaping. Promise hashes are hex; anything else is left to the
// statement.
func memoKey(h string) bool {
	if h == "" {
		return false
	}
	for i := 0; i < len(h); i++ {
		c := h[i]
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z') {
			return false
		}
	}
	return true
}

// doc returns the JSON object loadSQL reads as ?4 (promise hash to original
// rows) for the publications loadPopulationSQL selects with the same
// arguments, after looking up the ones the memo does not know yet. A
// publication committed between this and the statement is simply not in the
// object, and the statement reads its record.
func (m *originalRowsMemo) doc(ctx context.Context, db *sql.DB, start, end, now string) (string, error) {
	if err := m.open(ctx, db); err != nil {
		return "", err
	}
	rows, err := db.QueryContext(ctx, `SELECT p.promise_hash FROM publications p WHERE `+loadPopulationSQL, start, end, now)
	if err != nil {
		return "", err
	}
	var hashes []string
	for rows.Next() {
		var h string
		if err := rows.Scan(&h); err != nil {
			rows.Close()
			return "", err
		}
		if memoKey(h) {
			hashes = append(hashes, h)
		}
	}
	if err := rows.Close(); err != nil {
		return "", err
	}
	if err := rows.Err(); err != nil {
		return "", err
	}

	m.mu.Lock()
	var missing []string
	for _, h := range hashes {
		if _, ok := m.vals[h]; !ok && !m.odd[h] {
			missing = append(missing, h)
		}
	}
	m.mu.Unlock()
	if len(missing) > 0 {
		if err := m.learn(ctx, db, missing); err != nil {
			return "", err
		}
		m.saveLater(db)
	}

	var b strings.Builder
	b.WriteByte('{')
	first := true
	m.mu.Lock()
	for _, h := range hashes {
		v, ok := m.vals[h]
		if !ok {
			continue
		}
		if !first {
			b.WriteByte(',')
		}
		first = false
		b.WriteByte('"')
		b.WriteString(h)
		b.WriteString(`":`)
		if v.null {
			b.WriteString("null")
		} else {
			b.WriteString(strconv.FormatInt(v.n, 10))
		}
	}
	m.mu.Unlock()
	b.WriteByte('}')
	return b.String(), nil
}

// memoVersion names how learn turns what originalRowsSQL returns into an
// entry: an integer kept as it is, NULL kept as none, anything else never
// kept. It must be bumped whenever that changes, so that a file learned the
// old way is rebuilt rather than trusted (memoDefinition).
const memoVersion = 1

// learn reads original_rows for hashes from their records and remembers what
// it can. The lock is not held while the records are parsed.
func (m *originalRowsMemo) learn(ctx context.Context, db *sql.DB, hashes []string) error {
	m.mu.Lock()
	gen := m.gen
	m.mu.Unlock()
	list, err := json.Marshal(hashes)
	if err != nil {
		return err
	}
	rows, err := db.QueryContext(ctx, `SELECT p.promise_hash, `+originalRowsSQL+`
		FROM json_each(?) j JOIN publications p ON p.promise_hash = j.value`, string(list))
	if err != nil {
		return err
	}
	got := map[string]memoRows{}
	odd := map[string]bool{}
	for rows.Next() {
		var h string
		var v any
		if err := rows.Scan(&h, &v); err != nil {
			rows.Close()
			return err
		}
		switch x := v.(type) {
		case nil:
			got[h] = memoRows{null: true}
		case int64:
			got[h] = memoRows{n: x}
		default:
			// A real or a string: json_each would hand the statement back
			// something that is not provably the same value, so the
			// statement keeps reading the record for it.
			odd[h] = true
		}
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if err := rows.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.gen != gen {
		// The memo was dropped while these were read, which may have been
		// before the store changed: none is kept, and the next lookup reads
		// them again.
		return nil
	}
	if m.vals == nil {
		m.vals = map[string]memoRows{}
	}
	if m.odd == nil {
		m.odd = map[string]bool{}
	}
	for h, v := range got {
		m.vals[h] = v
	}
	for h := range odd {
		m.odd[h] = true
	}
	return nil
}

// open reads the memo's file the first time the memo is used, and keeps its
// entries if the store is still the one they were computed from (derived.go);
// otherwise the memo starts empty, as it did before there were files, and
// the file is left for its first write to replace. An error is a query that
// failed, and the next computation tries again. Once the file has been read,
// open takes no lock, so a computation never waits for a write.
func (m *originalRowsMemo) open(ctx context.Context, db *sql.DB) error {
	if m.opened.Load() {
		return nil
	}
	m.fileMu.Lock()
	defer m.fileMu.Unlock()
	if m.opened.Load() {
		return nil
	}
	if m.file == "" {
		m.origin = "not kept on disk"
		m.opened.Store(true)
		return nil
	}
	t0 := time.Now()
	sweepDerivedTemps(m.file, t0)
	f, vals, why, err := m.load(ctx, db)
	if err != nil {
		return err
	}
	if vals == nil {
		// Learned from the store from here on, under the identity it has
		// before the first record is read.
		if m.store, err = readStoreIdentity(ctx, db); err != nil {
			return err
		}
	}
	switch {
	case why != "":
		m.origin = "built from the store: " + m.file + " refused: " + string(why)
	case vals == nil:
		m.origin = "built from the store: no " + m.file
	default:
		m.mu.Lock()
		if m.vals == nil {
			m.vals = map[string]memoRows{}
		}
		for h, v := range vals {
			if _, ok := m.vals[h]; !ok {
				m.vals[h] = v
			}
		}
		m.saved, m.savedAt = len(vals), time.Now()
		m.mu.Unlock()
		m.store = f.Store
		m.origin = "loaded " + strconv.Itoa(len(vals)) + " entries from " + m.file + " (publications through rowid " + strconv.FormatInt(f.Mark.Rowid, 10) + ")"
	}
	m.opened.Store(true)
	if m.log != nil {
		m.log("original_rows memo: %s (%s)", m.origin, time.Since(t0).Round(time.Millisecond))
	}
	return nil
}

// load reads and checks the file: the file and its entries when it may be
// used, a refusal when it may not, nothing when there is none.
func (m *originalRowsMemo) load(ctx context.Context, db *sql.DB) (memoFile, map[string]memoRows, refusal, error) {
	var f memoFile
	ok, why := readDerived(m.file, &f)
	if !ok {
		return f, nil, why, nil
	}
	if why, err := checkHeader(ctx, db, f.derivedHeader, "original-rows", memoDefinition); why != "" || err != nil {
		return f, nil, why, err
	}
	vals := map[string]memoRows{}
	add := func(h string, v memoRows) refusal {
		if !memoKey(h) {
			return refusal("an entry keyed " + strconv.Quote(h))
		}
		if _, dup := vals[h]; dup {
			return refusal("two entries for " + h)
		}
		vals[h] = v
		return ""
	}
	for k, hs := range f.Values {
		n, err := strconv.ParseInt(k, 10, 64)
		if err != nil {
			return f, nil, refusal("a value " + strconv.Quote(k)), nil
		}
		for _, h := range hs {
			if why := add(h, memoRows{n: n}); why != "" {
				return f, nil, why, nil
			}
		}
	}
	for _, h := range f.Nulls {
		if why := add(h, memoRows{null: true}); why != "" {
			return f, nil, why, nil
		}
	}
	// The mark: the newest publication then must be the same one now, under
	// the same rowid.
	if f.Mark.Rowid <= 0 {
		if len(vals) > 0 {
			return f, nil, "entries with no publication behind them", nil
		}
		return f, vals, "", nil
	}
	var h string
	switch err := db.QueryRowContext(ctx, `SELECT promise_hash FROM publications WHERE rowid = ?`, f.Mark.Rowid).Scan(&h); {
	case errors.Is(err, sql.ErrNoRows):
		return f, nil, refusal("the store has no publication " + strconv.FormatInt(f.Mark.Rowid, 10) + ": it is older than the file"), nil
	case err != nil:
		return f, nil, "", err
	case h != f.Mark.PromiseHash:
		return f, nil, refusal("publication " + strconv.FormatInt(f.Mark.Rowid, 10) + " is another one now"), nil
	}
	// And the newest entries are what their records say.
	rows, err := db.QueryContext(ctx, memoCheckSQL, f.Mark.Rowid, memoChecked)
	if err != nil {
		return f, nil, "", err
	}
	defer rows.Close()
	for rows.Next() {
		var h string
		var v any
		if err := rows.Scan(&h, &v); err != nil {
			return f, nil, "", err
		}
		e, kept := vals[h]
		if !kept {
			continue
		}
		switch x := v.(type) {
		case nil:
			if !e.null {
				return f, nil, refusal(h + " has no original_rows, the file says " + strconv.FormatInt(e.n, 10)), nil
			}
		case int64:
			if e.null || e.n != x {
				return f, nil, refusal(h + " has original_rows " + strconv.FormatInt(x, 10) + ", not the file's"), nil
			}
		default:
			return f, nil, refusal(h + " has an original_rows the memo never keeps"), nil
		}
	}
	if err := rows.Err(); err != nil {
		return f, nil, "", err
	}
	return f, vals, "", nil
}

// due reports whether a memo of n entries is to be written: it has grown
// since the last write, and memoSaveEvery has passed or it has doubled;
// force asks only that it has grown. The caller holds mu.
func (m *originalRowsMemo) due(n int, force bool) bool {
	return n > m.saved && (force || n >= 2*m.saved || time.Since(m.savedAt) >= memoSaveEvery)
}

// saveLater starts save in the background when the memo is due to be
// written and no write saveLater started is running. One that is running
// already, or one that fails, leaves the entries to the next lookup that
// finds the memo due; not being able to write costs the next start a
// lookup, never a computation its answer.
func (m *originalRowsMemo) saveLater(db *sql.DB) {
	if m.file == "" {
		return
	}
	m.mu.Lock()
	if m.writing != nil || !m.due(len(m.vals), false) {
		m.mu.Unlock()
		return
	}
	done := make(chan struct{})
	m.writing = done
	m.mu.Unlock()
	go func() {
		defer func() {
			m.mu.Lock()
			m.writing = nil
			m.mu.Unlock()
			close(done)
		}()
		_ = m.save(context.Background(), db, false)
	}()
}

// wait returns once no write saveLater started is running.
func (m *originalRowsMemo) wait() {
	m.mu.Lock()
	done := m.writing
	m.mu.Unlock()
	if done != nil {
		<-done
	}
}

// save writes the memo out when it is due; force writes whatever has grown.
// Only the copy of the entries is made under mu, which is all a computation
// can wait for; the file is made and written under fileMu, which only
// another write and the first read take. The store's identity and the mark
// are read after the entries are taken, so every entry was read before the
// one and is of a publication at or below the other. For a store no longer
// at the identity the entries were read from nothing is written, and the
// memo is dropped (drop).
func (m *originalRowsMemo) save(ctx context.Context, db *sql.DB, force bool) error {
	if m.file == "" || !m.opened.Load() {
		return nil // not kept, or nothing learned yet that the file does not have
	}
	m.fileMu.Lock()
	defer m.fileMu.Unlock()
	type entry struct {
		h string
		v memoRows
	}
	m.mu.Lock()
	n := len(m.vals)
	if !m.due(n, force) {
		m.mu.Unlock()
		return nil
	}
	entries := make([]entry, 0, n)
	for h, v := range m.vals {
		entries = append(entries, entry{h, v})
	}
	m.mu.Unlock()
	id, err := readStoreIdentity(ctx, db)
	if err != nil {
		return err
	}
	if id != m.store {
		m.drop(id)
		return nil
	}
	f := memoFile{Values: map[string][]string{}, Nulls: []string{}}
	for _, e := range entries {
		if e.v.null {
			f.Nulls = append(f.Nulls, e.h)
			continue
		}
		k := strconv.FormatInt(e.v.n, 10)
		f.Values[k] = append(f.Values[k], e.h)
	}
	if err := db.QueryRowContext(ctx, `SELECT rowid, promise_hash FROM publications ORDER BY rowid DESC LIMIT 1`).
		Scan(&f.Mark.Rowid, &f.Mark.PromiseHash); err != nil {
		return err
	}
	f.derivedHeader = derivedHeader{Kind: "original-rows", Format: derivedFormat, Definition: memoDefinition, Store: m.store}
	if err := writeDerived(m.file, f); err != nil {
		return err
	}
	m.mu.Lock()
	m.saved, m.savedAt = n, time.Now()
	m.mu.Unlock()
	return nil
}

// drop forgets every entry, for a store no longer at the identity they were
// read from (save): a migration since may have rewritten records the memo
// will not read again, and a file of them would be believed under the
// identity the store has now. The memo learns them again from the store as
// it is now, id, which was read before this; a lookup that read records
// before it keeps nothing (learn). The caller holds fileMu.
func (m *originalRowsMemo) drop(id storeIdentity) {
	m.mu.Lock()
	m.vals, m.odd = nil, nil
	m.saved, m.savedAt = 0, time.Time{}
	m.gen++
	m.mu.Unlock()
	m.origin = "built from the store again: " + storeChange(m.store, id)
	m.store = id
	if m.log != nil {
		m.log("original_rows memo: %s", m.origin)
	}
}

// size is how many publications the memo knows, for tests.
func (m *originalRowsMemo) size() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.vals)
}
