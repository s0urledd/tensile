package store

import (
	"fmt"
	"strings"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/txcost"
)

// Migration 31 serves the transaction page and the endpoint history: the
// scanner's record of what each successful Fibre transaction cost
// (tx_costs.jsonl), the accounts a final failure's top-level Fibre messages
// are listed under (failed_tx_msgs, written with failed_txs), and the two
// indexes /v1/txs/{hash} seeks a movement and a registration by. Pure DDL:
// nothing derived reads these, so migration_rewrites does not move and the
// API's day partials are kept; the schema version moves, which keeps every
// older binary off the store. A store taken back to 30 keeps the tables and
// indexes, unread, and the migration runs again over them.
//
// The message rows of the failures stored before the migration are not
// written here: a statement that wrote them would count as a rewrite. The
// collector's pass writes them from the store (FillFailedTxMsgs).
var txCostsMigration = migration{
	version: 31,
	note:    "the transaction page: tx_costs (tx_costs.jsonl), failed_tx_msgs, and the indexes /v1/txs/{hash} seeks",
	stmts: []string{
		// one row per successful Fibre transaction, its line verbatim; rows can
		// be long (a many-message tx), so a rowid table with the key as its
		// unique index rather than WITHOUT ROWID
		`CREATE TABLE IF NOT EXISTS tx_costs (
			height     INTEGER NOT NULL,
			tx_index   INTEGER NOT NULL,
			dedupe_key TEXT NOT NULL,
			tx_hash    TEXT NOT NULL,
			time       TEXT NOT NULL,
			raw_json   TEXT NOT NULL,
			PRIMARY KEY (height, tx_index)
		)`,
		`CREATE INDEX IF NOT EXISTS tx_costs_tx ON tx_costs (tx_hash, height, tx_index)`,
		// the top-level Fibre messages of final failures, by the account a
		// list shows them under (failedtx.ListAccount)
		`CREATE TABLE IF NOT EXISTS failed_tx_msgs (
			account    TEXT NOT NULL,
			height     INTEGER NOT NULL,
			tx_index   INTEGER NOT NULL,
			msg_index  INTEGER NOT NULL,
			type_url   TEXT NOT NULL,
			dedupe_key TEXT NOT NULL,
			PRIMARY KEY (account, height, tx_index, msg_index)
		) WITHOUT ROWID`,
		// a movement by its transaction, in message order
		`CREATE INDEX IF NOT EXISTS payments_tx ON payments (tx_hash, msg_index)`,
		// a registration by the transaction that made it (from_tx_index = tx index + 1)
		`CREATE INDEX IF NOT EXISTS host_events_at ON host_events (from_height, from_tx_index, cons_address)`,
	},
}

// insertTxCostSQL stores one line of tx_costs.jsonl (InsertTxCost).
const insertTxCostSQL = `INSERT INTO tx_costs (height, tx_index, dedupe_key, tx_hash, time, raw_json)
	VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT(height, tx_index) DO NOTHING`

// InsertTxCost stores one line of tx_costs.jsonl verbatim; a (height, tx
// index) already present is left alone (a re-scan writes the same line
// again). The hash is stored in lower case, as the lookup asks for it. An
// empty key or hash is an error.
func (s *Store) InsertTxCost(r txcost.Record, raw []byte) (inserted bool, err error) {
	if r.DedupeKey == "" || r.TxHash == "" {
		return false, fmt.Errorf("tx cost without dedupe_key or tx_hash")
	}
	res, err := s.db.Exec(insertTxCostSQL, r.Height, r.TxIndex, r.DedupeKey, strings.ToLower(r.TxHash), ts(r.Time), string(raw))
	if err != nil {
		return false, fmt.Errorf("tx cost %s: %w", r.DedupeKey, err)
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}
