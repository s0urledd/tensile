package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"strconv"
	"strings"
	"sync"
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
// The zero value is ready to use.
type originalRowsMemo struct {
	mu sync.Mutex
	// vals holds, per promise hash, original_rows (null false) or the
	// record's lack of one (null true).
	vals map[string]memoRows
	// odd are the hashes whose value is neither an integer nor NULL: looked
	// up once, then left to the statement every time.
	odd map[string]bool
}

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

// size is how many publications the memo knows, for tests.
func (m *originalRowsMemo) size() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.vals)
}
