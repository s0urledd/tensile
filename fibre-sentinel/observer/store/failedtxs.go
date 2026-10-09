package store

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

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

// InsertFailedTx stores one line of failed_txs.jsonl verbatim (a key already
// present is left alone: the scanner's key is one per inclusion, and a
// re-scan writes the same line again) and, in the same transaction, the
// failed_tx_msgs rows of the record failed_txs keeps for that key
// (failedtx.MsgRows): the line just stored, or, when the key was already
// there, its stored raw_json, never a later line with the same key. The
// message rows are written on every call (ON CONFLICT DO NOTHING), so they
// always describe the stored row the API joins them to. The hash is stored
// in lower case, as the lookup asks for it. inserted reports the failed_txs
// row only.
func (s *Store) InsertFailedTx(r failedtx.Record, raw []byte) (inserted bool, err error) {
	if r.DedupeKey == "" || r.TxHash == "" {
		return false, fmt.Errorf("failed tx without dedupe_key or tx_hash")
	}
	tx, err := s.db.Begin()
	if err != nil {
		return false, fmt.Errorf("failed tx %s: %w", r.DedupeKey, err)
	}
	defer tx.Rollback()
	res, err := tx.Exec(`INSERT INTO failed_txs (dedupe_key, tx_hash, height, tx_index, time, code, codespace, raw_json)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(dedupe_key) DO NOTHING`,
		r.DedupeKey, strings.ToLower(r.TxHash), r.Height, r.TxIndex, ts(r.Time), int64(r.Code), r.Codespace, string(raw))
	if err != nil {
		return false, fmt.Errorf("failed tx %s: %w", r.DedupeKey, err)
	}
	n, _ := res.RowsAffected()
	kept, height, txIndex := r, r.Height, r.TxIndex
	if n == 0 {
		var stored string
		if err := tx.QueryRow(`SELECT height, tx_index, raw_json FROM failed_txs WHERE dedupe_key = ?`, r.DedupeKey).
			Scan(&height, &txIndex, &stored); err != nil {
			return false, fmt.Errorf("failed tx %s: %w", r.DedupeKey, err)
		}
		// A stored line that does not decode lists nothing: the API counts
		// the same row as a row fault.
		kept = failedtx.Record{}
		if json.Unmarshal([]byte(stored), &kept) != nil {
			kept = failedtx.Record{}
		}
	}
	if _, err := insertFailedTxMsgs(tx, r.DedupeKey, height, txIndex, failedtx.MsgRows(kept)); err != nil {
		return false, err
	}
	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("failed tx %s: %w", r.DedupeKey, err)
	}
	return n > 0, nil
}

// insertFailedTxMsgSQL stores one failed_tx_msgs row (failedtx.MsgRow).
const insertFailedTxMsgSQL = `INSERT INTO failed_tx_msgs (account, height, tx_index, msg_index, type_url, dedupe_key)
	VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING`

// insertFailedTxMsgs stores rows, the failed_tx_msgs rows of the failure
// failed_txs keeps under key at (height, txIndex), and returns how many
// were new.
func insertFailedTxMsgs(tx *sql.Tx, key string, height int64, txIndex int, rows []failedtx.MsgRow) (int64, error) {
	var n int64
	for _, m := range rows {
		res, err := tx.Exec(insertFailedTxMsgSQL, m.Account, height, txIndex, m.MsgIndex, m.TypeURL, key)
		if err != nil {
			return n, fmt.Errorf("failed tx %s message %d: %w", key, m.MsgIndex, err)
		}
		k, _ := res.RowsAffected()
		n += k
	}
	return n, nil
}

// MetaFailedTxMsgsFrom is the meta key holding the applied_at of the
// version-31 schema_migrations row that failed_tx_msgs was last filled under.
const MetaFailedTxMsgsFrom = "failed_tx_msgs_from"

// FillFailedTxMsgs makes failed_tx_msgs whole for the failures stored before
// migration 31 was applied to this store. When meta's failed_tx_msgs_from is
// not the version-31 row's applied_at (absent on a store migrated from 30;
// different after a way back and a second upgrade, which applies 31 again),
// it writes the message rows of every failed_txs row (insertFailedTxMsgs over
// its stored raw_json; one that does not decode is skipped) and sets the key,
// in one transaction. Otherwise it reads the two values and returns. filled
// reports whether it ran; rows is the number of message rows it inserted.
//
// It reads the store, never the record file, and touches no ingest cursor
// and no count: failed_tx_msgs is read by no partial. The collector calls it
// at the start of every pass, so the live store holds what a rebuild from
// the data dir gives, whatever ways back it went through.
func (s *Store) FillFailedTxMsgs(now time.Time) (filled bool, rows int64, err error) {
	var applied string
	if err := s.db.QueryRow(`SELECT applied_at FROM schema_migrations WHERE version = ?`, txCostsMigration.version).Scan(&applied); err != nil {
		return false, 0, fmt.Errorf("the version-%d migration row: %w", txCostsMigration.version, err)
	}
	from, err := s.Meta(MetaFailedTxMsgsFrom)
	if err != nil {
		return false, 0, err
	}
	if from == applied {
		return false, 0, nil
	}
	tx, err := s.db.Begin()
	if err != nil {
		return false, 0, err
	}
	defer tx.Rollback()
	// The rows to write, read through one cursor that is closed before
	// anything is written: only the derived rows are held, never a line.
	type failure struct {
		key     string
		height  int64
		txIndex int
		rows    []failedtx.MsgRow
	}
	var todo []failure
	q, err := tx.Query(`SELECT dedupe_key, height, tx_index, raw_json FROM failed_txs ORDER BY rowid`)
	if err != nil {
		return false, 0, err
	}
	for q.Next() {
		var f failure
		var raw string
		if err := q.Scan(&f.key, &f.height, &f.txIndex, &raw); err != nil {
			q.Close()
			return false, 0, err
		}
		var r failedtx.Record
		if json.Unmarshal([]byte(raw), &r) != nil {
			continue
		}
		if f.rows = failedtx.MsgRows(r); len(f.rows) > 0 {
			todo = append(todo, f)
		}
	}
	q.Close()
	if err := q.Err(); err != nil {
		return false, 0, err
	}
	for _, f := range todo {
		n, err := insertFailedTxMsgs(tx, f.key, f.height, f.txIndex, f.rows)
		if err != nil {
			return false, rows, err
		}
		rows += n
	}
	if _, err := tx.Exec(upsertMetaSQL, MetaFailedTxMsgsFrom, applied, ts(now)); err != nil {
		return false, rows, fmt.Errorf("meta %s: %w", MetaFailedTxMsgsFrom, err)
	}
	if err := tx.Commit(); err != nil {
		return false, rows, err
	}
	return true, rows, nil
}
