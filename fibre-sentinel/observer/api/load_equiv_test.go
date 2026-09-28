package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math/rand"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
)

// The load, signing and recent-endorsement figures were rewritten for speed:
// one pass over the selected publications instead of two walks of every
// assignment, a memo of original_rows, a ledger for the newest endorsements.
// None of that may move a figure. The oracles below are the statements as
// they shipped, kept verbatim, and every case compares them with the new code
// on the same store at the same moment.

// oldRowBytesSQL is rowBytesSQL as it shipped.
const oldRowBytesSQL = `(p.blob_size * 1.0 / NULLIF(json_extract(p.raw_json, '$.assignment.protocol_params.original_rows'), 0))`

// oldLoad is loadByValidator's two statements as they shipped, with "now"
// passed in.
func oldLoad(ctx context.Context, db *sql.DB, win Window, only string, now time.Time) (map[string]loadStats, error) {
	filter, args := "", []any{win.startArg(), win.endArg()}
	if only != "" {
		filter = ` AND a.validator_address = ?`
		args = append(args, only)
	}
	out := map[string]loadStats{}
	rows, err := db.QueryContext(ctx, `SELECT a.validator_address, COUNT(*), COALESCE(SUM(a.row_count), 0),
			COALESCE(CAST(SUM(a.row_count * `+oldRowBytesSQL+`) AS INTEGER), 0)
		FROM assignments a JOIN publications p ON p.promise_hash = a.promise_hash
		WHERE `+signingPopulation+` AND a.row_count > 0 AND a.attested = 1`+filter+`
		GROUP BY a.validator_address`, args...)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var addr string
		var l loadStats
		if err := rows.Scan(&addr, &l.Promises, &l.Rows, &l.Bytes); err != nil {
			rows.Close()
			return nil, err
		}
		out[addr] = l
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	nowArgs := []any{store.TS(now.UTC())}
	if only != "" {
		nowArgs = append(nowArgs, only)
	}
	rows, err = db.QueryContext(ctx, `SELECT a.validator_address,
			COALESCE(CAST(SUM(a.row_count * `+oldRowBytesSQL+`) AS INTEGER), 0)
		FROM assignments a JOIN publications p ON p.promise_hash = a.promise_hash
		WHERE p.settlement_tx_code = 0 AND p.assignment_error = '' AND p.must_serve_until > ?
		  AND a.row_count > 0 AND a.attested = 1`+filter+`
		GROUP BY a.validator_address`, nowArgs...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var addr string
		var b int64
		if err := rows.Scan(&addr, &b); err != nil {
			return nil, err
		}
		l := out[addr]
		l.StoredBytes = b
		out[addr] = l
	}
	return out, rows.Err()
}

// newLoadDoc runs loadSQL with the memo document given, bypassing the memo.
func newLoadDoc(ctx context.Context, db *sql.DB, win Window, only string, now time.Time, doc string) (map[string]loadStats, error) {
	filter, args := "", []any{win.startArg(), win.endArg(), store.TS(now.UTC()), doc}
	if only != "" {
		filter = ` AND a.validator_address = ?5`
		args = append(args, only)
	}
	rows, err := db.QueryContext(ctx, loadSQL(filter), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]loadStats{}
	for rows.Next() {
		var addr string
		var l loadStats
		if err := rows.Scan(&addr, &l.Promises, &l.Rows, &l.Bytes, &l.StoredBytes); err != nil {
			return nil, err
		}
		out[addr] = l
	}
	return out, rows.Err()
}

// ---- fixture ----

type fxPub struct {
	hash            string
	height, txIndex int64
	at, msu         time.Time
	code            int
	aerr            string
	size            int64
	orig            any // original_rows as written into raw_json; fxMissing leaves it out
}

type fxAsg struct {
	hash, val string
	rows      int64
	attested  any // 1, 0 or nil
	host      any // a host, "" (none registered) or nil (unknown)
}

type fxMissingT struct{}

var fxMissing = fxMissingT{}

func fxInsert(t *testing.T, db *sql.DB, pubs []fxPub, asgs []fxAsg) {
	t.Helper()
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	heights := map[string]int64{}
	for _, p := range pubs {
		pp := map[string]any{"total_rows": 16384}
		if _, missing := p.orig.(fxMissingT); !missing {
			pp["original_rows"] = p.orig
		}
		raw, err := json.Marshal(map[string]any{"assignment": map[string]any{"protocol_params": pp},
			"padding": strings.Repeat("x", 300)})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(`INSERT INTO publications (
			promise_hash, commitment, blob_version, blob_size, namespace, chain_id,
			promise_height, creation_timestamp, signer, signer_public_key,
			validator_signature_count, settlement_height, settlement_time,
			settlement_tx_hash, settlement_tx_index, settlement_tx_code,
			must_serve_until, must_serve_until_basis, shard_retention_s,
			payment_promise_timeout_s, assignment_error, protocol_params_fingerprint,
			pinned_celestia_app, validator_set_height, total_voting_power, sigma_rows,
			distinct_rows, wrap_overlaps, validators_with_rows, recorded_at, raw_json
		) VALUES (?,?,0,?,'ns','test',?,?,'signer','pk',0,?,?,'tx',?,?,?,'shard_retention',14400,3600,?,'','',?,0,0,0,0,0,?,?)`,
			p.hash, "c"+p.hash, p.size, p.height-1, store.TS(p.at), p.height, store.TS(p.at),
			p.txIndex, p.code, store.TS(p.msu), p.aerr, p.height-1, store.TS(p.at), string(raw)); err != nil {
			t.Fatal(err)
		}
		heights[p.hash] = p.height
	}
	for _, a := range asgs {
		if _, err := tx.Exec(`INSERT INTO assignments
			(promise_hash, validator_address, voting_power, row_count, rows_json, attested, host_at_settlement, settlement_height)
			VALUES (?,?,10,?,NULL,?,?,?)`, a.hash, a.val, a.rows, a.attested, a.host, heights[a.hash]); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

var fxVals = []string{"v1", "v2", "v3", "v4", "v5", "v6"}

// fxLoad builds n publications around now, settled over twelve days and
// written out of settlement order, with every original_rows shape a record
// can carry and every assignment shape the figures filter on.
func fxLoad(r *rand.Rand, now time.Time, n, first int) ([]fxPub, []fxAsg) {
	origs := []any{4096, 4096, 3000, 4097, 0, fxMissing, 4096.5, "4096", 1, 7}
	var pubs []fxPub
	var asgs []fxAsg
	for i := 0; i < n; i++ {
		k := first + i
		at := now.Add(-time.Duration(r.Int63n(int64(12 * 24 * time.Hour))))
		p := fxPub{
			hash:   fmt.Sprintf("%064x", k+1),
			height: 1000 + int64(r.Intn(100000)), txIndex: int64(k),
			at: at, msu: at.Add(time.Duration(1+r.Intn(8*24)) * time.Hour),
			size: 1 + r.Int63n(8<<20),
			orig: origs[r.Intn(len(origs))],
		}
		switch r.Intn(12) {
		case 0:
			p.code = 7
		case 1:
			p.aerr = "no protocol params for blob version 9"
		}
		pubs = append(pubs, p)
		for _, v := range fxVals {
			if r.Intn(8) == 0 {
				continue
			}
			a := fxAsg{hash: p.hash, val: v, rows: int64(r.Intn(2000)), attested: []any{1, 1, 1, 0, nil}[r.Intn(5)],
				host: []any{"h:1", "h:1", "", nil}[r.Intn(4)]}
			asgs = append(asgs, a)
		}
	}
	return pubs, asgs
}

func fxWindows(now time.Time) []Window {
	pinned := Window{Name: "7d", Span: 7 * 24 * time.Hour, Start: now.Add(-9 * 24 * time.Hour), End: now.Add(-2 * 24 * time.Hour), AsOf: true}
	return []Window{windowFor("24h", now), windowFor("7d", now), windowFor("30d", now), windowFor("all", now), pinned}
}

func indexNames(t *testing.T, db *sql.DB) string {
	t.Helper()
	rows, err := db.Query(`SELECT name FROM sqlite_master WHERE type = 'index' AND tbl_name = 'assignments' ORDER BY name`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			t.Fatal(err)
		}
		out = append(out, n)
	}
	return strings.Join(out, ",")
}

// The one-pass load statement gives every validator the figures the two
// shipped statements gave, in every window, for one validator and for all,
// with the memo empty, partly filled and full, on a store in both index
// states a deployment can be in: fresh, where migration 22 has dropped
// assignments_validator, and reopened, where the baseline has put it back.
func TestLoadOnePassMatchesTheShippedStatements(t *testing.T) {
	path := filepath.Join(t.TempDir(), "observer.db")
	st, err := store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 9, 28, 16, 0, 0, 0, time.UTC)
	r := rand.New(rand.NewSource(7))
	pubs, asgs := fxLoad(r, now, 160, 0)
	fxInsert(t, st.DB(), pubs, asgs)
	fresh := indexNames(t, st.DB())
	check := func(st *store.Store, state string) {
		ctx := context.Background()
		db := st.DB()
		s := &Server{st: st}
		full := map[string]any{}
		part := map[string]any{}
		for i, p := range pubs {
			var v any
			if err := db.QueryRow(`SELECT json_extract(raw_json, '$.assignment.protocol_params.original_rows') FROM publications WHERE promise_hash = ?`, p.hash).Scan(&v); err != nil {
				t.Fatal(err)
			}
			switch v.(type) {
			case nil, int64:
				full[p.hash] = v
				if i%3 == 0 {
					part[p.hash] = v
				}
			}
		}
		docOf := func(m map[string]any) string {
			b, err := json.Marshal(m)
			if err != nil {
				t.Fatal(err)
			}
			return string(b)
		}
		docs := map[string]string{"empty": "{}", "partial": docOf(part), "full": docOf(full)}
		compared := 0
		for _, win := range fxWindows(now) {
			for _, only := range append([]string{"", "nobody"}, fxVals...) {
				want, err := oldLoad(ctx, db, win, only, now)
				if err != nil {
					t.Fatal(err)
				}
				for name, doc := range docs {
					got, err := newLoadDoc(ctx, db, win, only, now, doc)
					if err != nil {
						t.Fatal(err)
					}
					if fmt.Sprint(got) != fmt.Sprint(want) {
						t.Errorf("%s, %s window, only=%q, memo %s:\n got %v\nwant %v", state, win.Name, only, name, got, want)
					}
					compared++
				}
				// and through the memo, as the API runs it (RowsPerBlob aside)
				got, err := s.loadByValidatorAt(ctx, win, only, now)
				if err != nil {
					t.Fatal(err)
				}
				for a, l := range got {
					l.RowsPerBlob = 0
					if _, ok := want[a]; !ok && l == (loadStats{}) {
						delete(got, a) // listed only for its rows on the newest promise
						continue
					}
					got[a] = l
				}
				if fmt.Sprint(got) != fmt.Sprint(want) {
					t.Errorf("%s, %s window, only=%q, through the memo:\n got %v\nwant %v", state, win.Name, only, got, want)
				}
			}
		}
		if compared == 0 {
			t.Fatal("nothing compared")
		}
		// The comparison is not vacuous: a memo entry that disagreed with
		// the record would move a figure.
		wrong := map[string]any{}
		for h, v := range full {
			if v != nil && v.(int64) > 1 {
				wrong[h] = v.(int64) - 1
			}
		}
		win := windowFor("all", now)
		want, _ := oldLoad(ctx, db, win, "", now)
		got, err := newLoadDoc(ctx, db, win, "", now, docOf(wrong))
		if err != nil {
			t.Fatal(err)
		}
		if fmt.Sprint(got) == fmt.Sprint(want) {
			t.Errorf("%s: a wrong memo left every figure unchanged; the comparison cannot see the memo", state)
		}
	}
	check(st, "fresh store ("+fresh+")")
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	st, err = store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	reopened := indexNames(t, st.DB())
	if reopened == fresh {
		t.Logf("index set unchanged on reopen: %s", reopened)
	}
	check(st, "reopened store ("+reopened+")")
}

// The memo keeps integers and NULLs, nothing else, learns only what it did
// not know, and hands a computation only the entries for its own
// publications.
func TestOriginalRowsMemo(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "observer.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	db := st.DB()
	ctx := context.Background()
	now := time.Date(2026, 9, 28, 16, 0, 0, 0, time.UTC)
	mk := func(i int, ago time.Duration, orig any) fxPub {
		at := now.Add(-ago)
		return fxPub{hash: fmt.Sprintf("%064x", i), height: int64(100 + i), txIndex: 0, at: at, msu: at.Add(time.Hour), size: 1024, orig: orig}
	}
	fxInsert(t, db, []fxPub{
		mk(1, time.Hour, 4096), mk(2, 2*time.Hour, fxMissing), mk(3, 3*time.Hour, 4096.5),
		mk(4, 4*time.Hour, "4096"), mk(5, 3*24*time.Hour, 3000),
	}, nil)
	var m originalRowsMemo
	all := windowFor("all", now)
	doc, err := m.doc(ctx, db, all.startArg(), all.endArg(), store.TS(now))
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err := json.Unmarshal([]byte(doc), &got); err != nil {
		t.Fatalf("doc %q: %v", doc, err)
	}
	h := func(i int) string { return fmt.Sprintf("%064x", i) }
	if len(got) != 3 || got[h(1)] != float64(4096) || got[h(5)] != float64(3000) {
		t.Fatalf("doc = %v, want 1 → 4096, 2 → null, 5 → 3000", got)
	}
	if v, ok := got[h(2)]; !ok || v != nil {
		t.Fatalf("a record without original_rows is not remembered as null: %v", got)
	}
	if _, ok := got[h(3)]; ok {
		t.Fatal("a real original_rows was remembered")
	}
	if _, ok := got[h(4)]; ok {
		t.Fatal("a string original_rows was remembered")
	}
	if m.size() != 3 {
		t.Fatalf("memo holds %d, want 3", m.size())
	}

	// A new record is learned, and only it: the others are not looked up
	// again, so an entry altered behind the memo's back stays as it was.
	fxInsert(t, db, []fxPub{mk(6, 30*time.Minute, 4097)}, nil)
	m.mu.Lock()
	m.vals[h(1)] = memoRows{n: 1}
	m.mu.Unlock()
	if _, err := m.doc(ctx, db, all.startArg(), all.endArg(), store.TS(now)); err != nil {
		t.Fatal(err)
	}
	m.mu.Lock()
	six, one := m.vals[h(6)], m.vals[h(1)]
	m.mu.Unlock()
	if six.n != 4097 || six.null || one.n != 1 {
		t.Fatalf("after a new record: 6 → %+v, 1 → %+v; want 4097 learned and 1 left alone", six, one)
	}

	// A day's computation is handed the day's publications and those still
	// held, not the whole memo.
	day := windowFor("24h", now)
	doc, err = m.doc(ctx, db, day.startArg(), day.endArg(), store.TS(now))
	if err != nil {
		t.Fatal(err)
	}
	got = nil
	if err := json.Unmarshal([]byte(doc), &got); err != nil {
		t.Fatal(err)
	}
	if _, ok := got[h(5)]; ok || len(got) != 3 {
		t.Fatalf("24h doc = %v, want the three remembered publications of the day", got)
	}
}

// oldRecent is recentSigning's statement as it shipped.
func oldRecent(ctx context.Context, db *sql.DB, only string) (map[string]signingStats, error) {
	filter, args := "", []any{recentEndorsements}
	if only != "" {
		filter = ` AND a.validator_address = ?`
		args = []any{only, recentEndorsements}
	}
	rows, err := db.QueryContext(ctx, `SELECT validator_address, COUNT(*), COALESCE(SUM(attested), 0), MAX(last_endorsed)
		FROM (SELECT a.validator_address AS validator_address, a.attested AS attested,
				MAX(CASE WHEN a.attested = 1 THEN p.settlement_time END) OVER (PARTITION BY a.validator_address) AS last_endorsed,
				ROW_NUMBER() OVER (PARTITION BY a.validator_address ORDER BY p.settlement_height DESC, p.settlement_tx_index DESC) AS rn
			FROM assignments a JOIN publications p ON p.promise_hash = a.promise_hash
			WHERE p.settlement_tx_code = 0 AND p.assignment_error = '' AND a.row_count > 0
			  AND a.host_at_settlement IS NOT '' AND a.attested IS NOT NULL`+filter+`)
		WHERE rn <= ? GROUP BY validator_address`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]signingStats{}
	for rows.Next() {
		var addr string
		var r recentEndorsement
		var last sql.NullString
		if err := rows.Scan(&addr, &r.Assigned, &r.Endorsed, &last); err != nil {
			return nil, err
		}
		st := out[addr]
		st.Recent = r
		if last.Valid {
			v := last.String
			st.LastEndorsedAt = &v
		}
		out[addr] = st
	}
	return out, rows.Err()
}

func recentString(m map[string]signingStats) string {
	var b strings.Builder
	for _, v := range fxVals {
		st, ok := m[v]
		if !ok {
			continue
		}
		last := "-"
		if st.LastEndorsedAt != nil {
			last = *st.LastEndorsedAt
		}
		fmt.Fprintf(&b, "%s:%d/%d@%s ", v, st.Recent.Endorsed, st.Recent.Assigned, last)
	}
	return b.String()
}

// The endorsement ledger publishes what the shipped window-function statement
// published: for every validator, and for one; built from nothing, and then
// brought up to date with records that arrive later, some of them older than
// what it already holds. Heights are written out of order and settlement
// times do not follow heights, so nothing here leans on either.
func TestEndorsementLedgerMatchesTheShippedStatement(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "observer.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	db := st.DB()
	ctx := context.Background()
	now := time.Date(2026, 9, 28, 16, 0, 0, 0, time.UTC)
	r := rand.New(rand.NewSource(11))
	var l endorsementLedger
	compare := func(stage string) {
		for _, only := range append([]string{"", "nobody"}, fxVals...) {
			want, err := oldRecent(ctx, db, only)
			if err != nil {
				t.Fatal(err)
			}
			got := map[string]signingStats{}
			if err := l.fill(ctx, db, only, got); err != nil {
				t.Fatal(err)
			}
			if recentString(got) != recentString(want) || len(got) != len(want) {
				t.Errorf("%s, only=%q:\n got %s\nwant %s", stage, only, recentString(got), recentString(want))
			}
		}
	}
	pubs, asgs := fxLoad(r, now, 120, 0)
	fxInsert(t, db, pubs, asgs)
	compare("built from scratch")
	for round := 1; round <= 3; round++ {
		pubs, asgs := fxLoad(r, now, 25, 1000*round)
		fxInsert(t, db, pubs, asgs)
		compare(fmt.Sprintf("after %d later batches", round))
	}
	if l.upTo == 0 {
		t.Fatal("the ledger never advanced")
	}
}
