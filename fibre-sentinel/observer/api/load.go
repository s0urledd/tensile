package api

import (
	"context"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
)

// Load is what a validator committed to store: the rows of every settled blob
// it endorsed. The protocol assigns rows to every validator in the set, but a
// publisher stops collecting at two thirds of stake, and only an endorsement
// says the validator received its shard and undertook to keep it until the
// retention window ends; rows it was assigned and did not endorse may or may
// not have reached it, and are no duty. The service rate counts the same
// population. Every figure is computed from the chain's own records
// (MsgPayForFibre, its verified signatures, and the assignment fibre-assign
// derives from the validator set at the promise height), nothing measured, so
// anyone can recompute it. Bytes are row data: a row is blob_size /
// original_rows, without the proofs and framing that travel with it.
type loadStats struct {
	// Promises and Rows are the settled promises in the window that carry
	// this validator's verified endorsement, and its rows on them.
	Promises int64 `json:"promises"`
	Rows     int64 `json:"rows"`
	// Bytes is the row data it committed to store for those promises.
	Bytes int64 `json:"bytes"`
	// StoredBytes is the row data it has to hold right now: endorsed promises
	// whose retention window has not ended, whatever the selected period. On
	// a pinned window "now" is the pin (heldAt).
	StoredBytes int64 `json:"stored_bytes"`
	// RowsPerBlob is its assignment on the newest settled promise, the
	// newest by the pin on a pinned window; rows follow stake, not blob size,
	// so it is the same for every blob of that set.
	RowsPerBlob int64 `json:"rows_per_blob"`
}

// rowBytesSQL is the row data one assigned row carries: blob_size over the
// promise's original rows, as recorded with its assignment. The original rows
// come from originalRowsMemo when the memo passed them (m, which loadSQL
// joins) and from the record itself otherwise. The memo only ever holds what
// json_extract returned for the same record, so the two cannot differ.
const rowBytesSQL = `(p.blob_size * 1.0 / NULLIF(CASE WHEN m.promise_hash IS NOT NULL THEN m.original_rows
			ELSE json_extract(p.raw_json, '$.assignment.protocol_params.original_rows') END, 0))`

// loadPopulationSQL selects the publications loadSQL reads: settled, with an
// assignment, and either in the window (?1, ?2) or held at ?3 (heldSQL).
// originalRowsMemo.doc selects the same set, to know which entries to pass.
const loadPopulationSQL = `p.settlement_tx_code = 0 AND p.assignment_error = ''
			AND ((p.settlement_time >= ?1 AND p.settlement_time <= ?2) OR ` + heldSQL + `)`

// heldSQL is a publication a validator holds at ?3: settled by then, and its
// retention not over. The settlement bound only matters on a pinned window,
// where ?3 is the pin: without it, a promise settled after the pin and
// still under retention was counted as held at the pin, and a pinned
// answer was not the answer the observer would have given then.
const heldSQL = `(p.must_serve_until > ?3 AND p.settlement_time <= ?3)`

// loadSQL is every figure loadByValidator reads from assignments, in one
// statement; filter narrows it to one validator (?5).
//
// It used to be two statements, the window's figures and what is held now.
// Each walked every assignment ever stored and parsed its publication's
// raw_json, about 100 KB, once per assignment row: the same record some 80
// times per statement, and a minute per validator snapshot at 900
// publications. Now pb reads each selected publication once, with its row
// size and which of the two sums it belongs to, and the assignments are
// sought from it by primary key, so the cost follows the publications
// selected rather than the whole assignment history.
//
// The float sums are pinned to assignment rowid order (ORDER BY a.rowid
// inside SUM). That is the order the two statements summed in on a deployed
// store, whose plan walked assignments_validator (validator_address, rowid),
// and pinning it keeps every figure the same whatever access path SQLite
// picks here. Each term is the same IEEE operation on the same double as
// before, and a CASE without ELSE adds NULL, which SUM skips, so a validator
// with rows on only one side gets the zeros it had when the other statement
// did not list it.
func loadSQL(filter string) string {
	return `WITH m AS MATERIALIZED (SELECT key AS promise_hash, value AS original_rows FROM json_each(?4)),
		pb AS MATERIALIZED (
			SELECT p.promise_hash, ` + rowBytesSQL + ` AS rb,
				(p.settlement_time >= ?1 AND p.settlement_time <= ?2) AS in_win,
				` + heldSQL + ` AS held
			FROM publications p LEFT JOIN m ON m.promise_hash = p.promise_hash
			WHERE ` + loadPopulationSQL + `)
		SELECT a.validator_address,
			COUNT(CASE WHEN pb.in_win THEN 1 END),
			COALESCE(SUM(CASE WHEN pb.in_win THEN a.row_count END), 0),
			COALESCE(CAST(SUM(CASE WHEN pb.in_win THEN a.row_count * pb.rb END ORDER BY a.rowid) AS INTEGER), 0),
			COALESCE(CAST(SUM(CASE WHEN pb.held THEN a.row_count * pb.rb END ORDER BY a.rowid) AS INTEGER), 0)
		FROM pb CROSS JOIN assignments a ON a.promise_hash = pb.promise_hash
		WHERE a.row_count > 0 AND a.attested = 1` + filter + `
		GROUP BY a.validator_address`
}

// loadByValidator computes loadStats per validator over win, for one
// validator when only is set.
func (s *Server) loadByValidator(ctx context.Context, win Window, only string) (map[string]loadStats, error) {
	return s.loadByValidatorAt(ctx, win, only, heldAt(win, time.Now()))
}

// heldAt is the moment "held now" is asked at: now, or the pin on a pinned
// window. It was the wall clock on both, so a pinned answer counted what
// validators hold today, promises settled after the pin included, beside
// figures that leave out every row after it (AsOfNote).
func heldAt(win Window, now time.Time) time.Time {
	if win.AsOf {
		return win.End
	}
	return now
}

// loadByValidatorAt is loadByValidator with "held now" asked at now.
func (s *Server) loadByValidatorAt(ctx context.Context, win Window, only string, now time.Time) (map[string]loadStats, error) {
	db := s.st.DB()
	// held now: settled by now and retention not over at now, whatever the
	// window
	nowArg := store.TS(now.UTC())
	doc, err := s.origRows.doc(ctx, db, win.startArg(), win.endArg(), nowArg)
	if err != nil {
		return nil, err
	}
	filter, args := "", []any{win.startArg(), win.endArg(), nowArg, doc}
	if only != "" {
		filter = ` AND a.validator_address = ?5`
		args = append(args, only)
	}
	out := map[string]loadStats{}
	rows, err := db.QueryContext(ctx, loadSQL(filter), args...)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var addr string
		var l loadStats
		if err := rows.Scan(&addr, &l.Promises, &l.Rows, &l.Bytes, &l.StoredBytes); err != nil {
			rows.Close()
			return nil, err
		}
		out[addr] = l
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// rows on the newest promise settled by now
	var newest string
	if err := db.QueryRowContext(ctx, `SELECT promise_hash FROM publications
		WHERE settlement_tx_code = 0 AND assignment_error = '' AND settlement_time <= ?
		ORDER BY settlement_height DESC, settlement_tx_index DESC LIMIT 1`, nowArg).Scan(&newest); err != nil {
		return out, nil // nothing settled yet
	}
	lastArgs := []any{newest}
	lastFilter := ""
	if only != "" {
		lastFilter = ` AND a.validator_address = ?`
		lastArgs = append(lastArgs, only)
	}
	rows, err = db.QueryContext(ctx, `SELECT a.validator_address, a.row_count FROM assignments a
		WHERE a.promise_hash = ? AND a.row_count > 0`+lastFilter, lastArgs...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var addr string
		var n int64
		if err := rows.Scan(&addr, &n); err != nil {
			return nil, err
		}
		l := out[addr]
		l.RowsPerBlob = n
		out[addr] = l
	}
	return out, rows.Err()
}

// fillLoad sets Load on every row validatorRows built.
func (s *Server) fillLoad(ctx context.Context, win Window, only string, byAddr map[string]*validatorRow) error {
	load, err := s.loadByValidator(ctx, win, only)
	if err != nil {
		return err
	}
	for addr, v := range byAddr {
		v.Load = load[addr]
	}
	return nil
}
