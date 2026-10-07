package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
)

// Migration 29 takes away what the sampling and the second vantage's
// confirmations left in the store, and writes must_serve_until_ambiguous.
//
// What goes is what nothing reads and nothing is lost with:
//
//   - probes_sampling_started (migration 18) served /v1/sampling alone, and
//     the route is gone; every probe insert still wrote an entry to it.
//     sampling_decisions, sampling_secrets and the sampling columns stay: the
//     record and its recomputation read them.
//   - probe_confirmations (migration 23) and its two indexes: the table is
//     empty, and the migration refuses (and changes nothing) if it is not.
//     probes_cleared indexed the rows cleared_by names, which is none; the
//     columns cleared_by and confirmed_by stay, since dropping a column
//     rewrites the table, and nothing reads them any more.
//
// Each is a drop of an index or of an empty table, which frees their pages
// and rewrites no row: the store it runs on shares its disk with a
// validator.
//
// publications.must_serve_until_ambiguous was filled once by migration 19
// and never written again, so every publication stored since reads 0 whatever
// its record says. UpsertPublication now writes it, and the backfill below
// gives the rows stored meanwhile what their records say.
var leftoversMigration = migration{
	version: 29,
	note:    "what the sampling and the second vantage's confirmations left goes (the index /v1/sampling read, the empty probe_confirmations and its indexes, the index on cleared_by); must_serve_until_ambiguous is written on insert and filled from the records",
	check:   confirmationsEmpty,
	stmts: []string{
		`DROP INDEX IF EXISTS probes_sampling_started`,
		`DROP INDEX IF EXISTS probes_cleared`,
		`DROP INDEX IF EXISTS probe_confirmations_open`,
		`DROP INDEX IF EXISTS probe_confirmations_started`,
		`DROP TABLE IF EXISTS probe_confirmations`,
	},
	backfill: backfillAmbiguous,
}

// confirmationsEmpty refuses migration 29 on a store whose probe_confirmations
// holds a row: the table is dropped only when that loses nothing.
func confirmationsEmpty(tx *sql.Tx) error {
	var tables int
	if err := tx.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'probe_confirmations'`).Scan(&tables); err != nil {
		return err
	}
	if tables == 0 {
		return nil
	}
	var n int64
	if err := tx.QueryRow(`SELECT COUNT(*) FROM probe_confirmations`).Scan(&n); err != nil {
		return err
	}
	if n > 0 {
		return fmt.Errorf("probe_confirmations holds %d row(s), and it is dropped only empty: nothing was changed", n)
	}
	return nil
}

// ambiguousMarkedSQL is the publications whose deadline the scanner marked
// ambiguous. The basis it wrote carries "; AMBIGUOUS: " exactly when it set
// must_serve_until_ambiguous (scan.ParamHistory.MustServeUntilForPromise, in
// every build since the field exists), and a correction keeps the basis it
// moved in must_serve_until_basis_at_scan. must_serve_until_basis is read
// where the row begins, ahead of raw_json, so the scan reads a page or two of
// each publication, in table order, rather than its record; the corrected
// ones are a handful, found by their index. (A publication in both halves is
// named twice, which an IN list does not mind.)
const ambiguousMarkedSQL = `SELECT promise_hash FROM publications WHERE must_serve_until_basis LIKE '%; AMBIGUOUS: %'
	UNION ALL
	SELECT promise_hash FROM publications WHERE corrected_at IS NOT NULL AND must_serve_until_basis_at_scan LIKE '%; AMBIGUOUS: %'`

// ambiguousCandidatesSQL is the marked publications whose row does not say
// so yet, with their records: each found by its key, so the flag and the
// record are read of these rows alone.
const ambiguousCandidatesSQL = `SELECT promise_hash, raw_json FROM publications
	WHERE promise_hash IN (` + ambiguousMarkedSQL + `) AND must_serve_until_ambiguous = 0`

// backfillAmbiguous sets must_serve_until_ambiguous on every publication
// whose record says so and whose row does not. The candidates are the marked
// ones; each one's value is read from its record, in whichever form the row
// keeps it. Where the record cannot be read (stripped by an earlier build's
// retention, or not decoding), the mark stands for it: the scanner writes the
// two together.
func backfillAmbiguous(s *Store, tx *sql.Tx) (int64, error) {
	ctx := context.Background()
	rows, err := tx.QueryContext(ctx, ambiguousCandidatesSQL)
	if err != nil {
		return 0, err
	}
	type cand struct {
		hash string
		raw  []byte
	}
	var cands []cand
	for rows.Next() {
		var c cand
		if err := rows.Scan(&c.hash, &c.raw); err != nil {
			rows.Close()
			return 0, err
		}
		cands = append(cands, c)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}
	var n int64
	for _, c := range cands {
		ambiguous := true
		line, err := s.Record(ctx, tx, c.raw)
		switch {
		case errors.Is(err, ErrUndecodable):
		case err != nil:
			return n, err
		case len(line) > 0:
			var r struct {
				Ambiguous bool `json:"must_serve_until_ambiguous"`
			}
			if json.Unmarshal(line, &r) == nil {
				ambiguous = r.Ambiguous
			}
		}
		if !ambiguous {
			continue
		}
		res, err := tx.ExecContext(ctx, `UPDATE publications SET must_serve_until_ambiguous = 1 WHERE promise_hash = ?`, c.hash)
		if err != nil {
			return n, err
		}
		k, _ := res.RowsAffected()
		n += k
	}
	return n, nil
}
