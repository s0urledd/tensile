package store

// Migration 23 added a second vantage's answers to this observer's failed
// readings (probe_confirmations) and cleared_by / confirmed_by on the row
// they answered. Nothing reads or writes them any more: a blob's reading is
// this observer's own, and what counts follows from it alone
// (observer/verdict). The migration stays, so a new store travels the same
// path as an old one; migration 29 (leftovers.go) drops the table, empty,
// and the indexes, and leaves the two columns, all NULL, where they are.

var confirmMigration = migration{
	version: 23,
	note:    "second-vantage confirmation of faults: the other vantage's answers, and cleared_by / confirmed_by on the fault's row",
	stmts: []string{
		// NULL on every row no other vantage answered.
		`ALTER TABLE probes ADD COLUMN cleared_by TEXT`,
		`ALTER TABLE probes ADD COLUMN confirmed_by TEXT`,
		// probe_key is the row the answer is about: this observer's own
		// dedupe key for the same promise, validator and schedule point.
		// No FOREIGN KEY: an answer can arrive before the fault's row is
		// ingested, and the retention prune removes both by started_at.
		// judged is set once the rule has been applied to the answer.
		`CREATE TABLE IF NOT EXISTS probe_confirmations (
			dedupe_key            TEXT PRIMARY KEY,
			probe_key             TEXT NOT NULL,
			vantage               TEXT NOT NULL,
			promise_hash          TEXT NOT NULL,
			validator_address     TEXT NOT NULL,
			validator_host        TEXT NOT NULL,
			scheduled_at          TEXT NOT NULL,
			started_at            TEXT NOT NULL,
			phase                 TEXT NOT NULL,
			outcome               TEXT NOT NULL,
			classification        TEXT NOT NULL,
			classification_reason TEXT NOT NULL,
			rows_returned         INTEGER NOT NULL,
			rows_expected         INTEGER NOT NULL,
			commitment_verified   INTEGER NOT NULL,
			assignment_verified   INTEGER NOT NULL,
			rows_sha256           TEXT,
			raw_error             TEXT NOT NULL,
			raw_json              TEXT NOT NULL,
			judged                INTEGER NOT NULL DEFAULT 0
		)`,
		`CREATE INDEX IF NOT EXISTS probe_confirmations_open ON probe_confirmations (probe_key) WHERE judged = 0`,
		`CREATE INDEX IF NOT EXISTS probe_confirmations_started ON probe_confirmations (started_at)`,
		// The validator page counts its cleared faults; partial, so the
		// rows no one cleared (all but a handful) cost nothing.
		`CREATE INDEX IF NOT EXISTS probes_cleared ON probes (validator_address, started_at) WHERE cleared_by IS NOT NULL`,
	},
}
