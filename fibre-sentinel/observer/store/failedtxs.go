package store

import (
	"fmt"
	"strings"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/failedtx"
)

// Migration 30 keeps the scanner's record of the transactions that failed in
// a block while carrying a Fibre message (failed_txs.jsonl): one row per
// failed inclusion, its line kept verbatim, for the transaction lookup
// alone.
//
// It only adds a table and its index, which nothing derived reads: the
// table is in no count (Count), no rollup, no slim record and no aggregate,
// so migration_rewrites does not move and the API's day partials are kept.
// The schema version does move, which is what keeps an API older than the
// table off a store that has it, and a newer one off a store that does not.
// A store taken back to 29 (its schema_migrations row deleted) keeps the
// table and the index, unread, and the migration runs again over them.
var failedTxsMigration = migration{
	version: 30,
	note:    "failed Fibre transactions (failed_txs.jsonl): one row per failed inclusion, read only by the transaction lookup",
	stmts: []string{
		`CREATE TABLE IF NOT EXISTS failed_txs (
			dedupe_key TEXT PRIMARY KEY,
			tx_hash    TEXT NOT NULL,
			height     INTEGER NOT NULL,
			tx_index   INTEGER NOT NULL,
			time       TEXT NOT NULL,
			code       INTEGER NOT NULL,
			codespace  TEXT NOT NULL,
			raw_json   TEXT NOT NULL
		)`,
		// A transaction's inclusions by its hash, newest last: the lookup
		// seeks the hash and reads the order from the index.
		`CREATE INDEX IF NOT EXISTS failed_txs_tx ON failed_txs (tx_hash, height, tx_index)`,
	},
}

// InsertFailedTx stores one line of failed_txs.jsonl verbatim; a key already
// present is left alone (the scanner's key is one per inclusion, and a
// re-scan writes the same line again). The hash is stored in lower case,
// as the lookup asks for it.
func (s *Store) InsertFailedTx(r failedtx.Record, raw []byte) (inserted bool, err error) {
	if r.DedupeKey == "" || r.TxHash == "" {
		return false, fmt.Errorf("failed tx without dedupe_key or tx_hash")
	}
	res, err := s.db.Exec(`INSERT INTO failed_txs (dedupe_key, tx_hash, height, tx_index, time, code, codespace, raw_json)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(dedupe_key) DO NOTHING`,
		r.DedupeKey, strings.ToLower(r.TxHash), r.Height, r.TxIndex, ts(r.Time), int64(r.Code), r.Codespace, string(raw))
	if err != nil {
		return false, fmt.Errorf("failed tx %s: %w", r.DedupeKey, err)
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}
