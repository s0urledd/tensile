package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"strconv"
	"strings"
	"sync"
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
// The zero value is ready to use.
type originalRowsMemo struct {
	mu sync.Mutex
	// vals holds, per promise hash, original_rows (null false) or the
	// record's lack of one (null true).
	vals map[string]memoRows
	// odd are the hashes whose value is neither an integer nor NULL: looked
	// up once, then left to the statement every time.
	odd map[string]bool

	// file is where the memo is kept, or "" for nowhere. fileMu orders
	// reading and writing it; opened says it has been read (or found
	// missing, or refused) by this process, saved is how many entries the
	// file holds as last written and savedAt when, and origin says how
	// this process's memo began, which log (when set) is told.
	file    string
	log     logf
	fileMu  sync.Mutex
	opened  bool
	saved   int
	savedAt time.Time
	origin  string
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
		// Not being able to write the file costs the next start a lookup,
		// not this computation its answer.
		_ = m.save(ctx, db, false)
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
// otherwise the file is removed and the memo starts empty, as it did before
// there were files. An error is a query that failed, and the next
// computation tries again.
func (m *originalRowsMemo) open(ctx context.Context, db *sql.DB) error {
	m.fileMu.Lock()
	defer m.fileMu.Unlock()
	if m.opened {
		return nil
	}
	if m.file == "" {
		m.opened, m.origin = true, "not kept on disk"
		return nil
	}
	t0 := time.Now()
	vals, n, why, err := m.load(ctx, db)
	if err != nil {
		return err
	}
	m.opened = true
	defer func() {
		if m.log != nil {
			m.log("original_rows memo: %s (%s)", m.origin, time.Since(t0).Round(time.Millisecond))
		}
	}()
	switch {
	case why != "":
		_ = os.Remove(m.file)
		m.origin = "built from the store: " + m.file + " refused: " + string(why)
		return nil
	case vals == nil:
		m.origin = "built from the store: no " + m.file
		return nil
	}
	m.mu.Lock()
	if m.vals == nil {
		m.vals = map[string]memoRows{}
	}
	for h, v := range vals {
		if _, ok := m.vals[h]; !ok {
			m.vals[h] = v
		}
	}
	m.mu.Unlock()
	m.saved, m.savedAt = len(vals), time.Now()
	m.origin = "loaded " + strconv.Itoa(len(vals)) + " entries from " + m.file + " (publications through rowid " + strconv.FormatInt(n, 10) + ")"
	return nil
}

// load reads and checks the file: its entries and mark when it may be used,
// a refusal when it may not, nothing when there is none.
func (m *originalRowsMemo) load(ctx context.Context, db *sql.DB) (map[string]memoRows, int64, refusal, error) {
	var f memoFile
	ok, why := readDerived(m.file, &f)
	if !ok {
		return nil, 0, why, nil
	}
	if why, err := checkHeader(ctx, db, f.derivedHeader, "original-rows", memoDefinition); why != "" || err != nil {
		return nil, 0, why, err
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
			return nil, 0, refusal("a value " + strconv.Quote(k)), nil
		}
		for _, h := range hs {
			if why := add(h, memoRows{n: n}); why != "" {
				return nil, 0, why, nil
			}
		}
	}
	for _, h := range f.Nulls {
		if why := add(h, memoRows{null: true}); why != "" {
			return nil, 0, why, nil
		}
	}
	// The mark: the newest publication then must be the same one now, under
	// the same rowid.
	if f.Mark.Rowid <= 0 {
		if len(vals) > 0 {
			return nil, 0, "entries with no publication behind them", nil
		}
		return vals, 0, "", nil
	}
	var h string
	switch err := db.QueryRowContext(ctx, `SELECT promise_hash FROM publications WHERE rowid = ?`, f.Mark.Rowid).Scan(&h); {
	case errors.Is(err, sql.ErrNoRows):
		return nil, 0, refusal("the store has no publication " + strconv.FormatInt(f.Mark.Rowid, 10) + ": it is older than the file"), nil
	case err != nil:
		return nil, 0, "", err
	case h != f.Mark.PromiseHash:
		return nil, 0, refusal("publication " + strconv.FormatInt(f.Mark.Rowid, 10) + " is another one now"), nil
	}
	// And the newest entries are what their records say.
	rows, err := db.QueryContext(ctx, memoCheckSQL, f.Mark.Rowid, memoChecked)
	if err != nil {
		return nil, 0, "", err
	}
	defer rows.Close()
	for rows.Next() {
		var h string
		var v any
		if err := rows.Scan(&h, &v); err != nil {
			return nil, 0, "", err
		}
		e, kept := vals[h]
		if !kept {
			continue
		}
		switch x := v.(type) {
		case nil:
			if !e.null {
				return nil, 0, refusal(h + " has no original_rows, the file says " + strconv.FormatInt(e.n, 10)), nil
			}
		case int64:
			if e.null || e.n != x {
				return nil, 0, refusal(h + " has original_rows " + strconv.FormatInt(x, 10) + ", not the file's"), nil
			}
		default:
			return nil, 0, refusal(h + " has an original_rows the memo never keeps"), nil
		}
	}
	if err := rows.Err(); err != nil {
		return nil, 0, "", err
	}
	return vals, f.Mark.Rowid, "", nil
}

// save writes the memo out when it has grown since the last write and
// memoSaveEvery has passed or it has doubled; force writes whatever has
// grown. The mark is read after the entries are taken, so every entry is of
// a publication at or below it.
func (m *originalRowsMemo) save(ctx context.Context, db *sql.DB, force bool) error {
	if m.file == "" {
		return nil
	}
	m.fileMu.Lock()
	defer m.fileMu.Unlock()
	if !m.opened {
		return nil // nothing learned yet that the file does not have
	}
	f := memoFile{Values: map[string][]string{}, Nulls: []string{}}
	m.mu.Lock()
	n := len(m.vals)
	if n <= m.saved || !(force || n >= 2*m.saved || time.Since(m.savedAt) >= memoSaveEvery) {
		m.mu.Unlock()
		return nil
	}
	for h, v := range m.vals {
		if v.null {
			f.Nulls = append(f.Nulls, h)
			continue
		}
		k := strconv.FormatInt(v.n, 10)
		f.Values[k] = append(f.Values[k], h)
	}
	m.mu.Unlock()
	id, err := readStoreIdentity(ctx, db)
	if err != nil {
		return err
	}
	if err := db.QueryRowContext(ctx, `SELECT rowid, promise_hash FROM publications ORDER BY rowid DESC LIMIT 1`).
		Scan(&f.Mark.Rowid, &f.Mark.PromiseHash); err != nil {
		return err
	}
	f.derivedHeader = derivedHeader{Kind: "original-rows", Format: derivedFormat, Definition: memoDefinition, Store: id}
	if err := writeDerived(m.file, f); err != nil {
		return err
	}
	m.saved, m.savedAt = n, time.Now()
	return nil
}

// size is how many publications the memo knows, for tests.
func (m *originalRowsMemo) size() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.vals)
}
