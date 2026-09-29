package store

import (
	"context"
	"fmt"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/probe"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/verdict"
)

// Other vantages' answers to this observer's not-served readings.
//
// A second vantage fetches the rows of every row that can count not served
// once more (probe.ConfirmationDue; internal/probe/confirm.go) and writes
// the result to its own measurements.jsonl, which is copied in beside this
// observer's record and ingested here, into probe_confirmations: never into
// probes. Every published figure is counted over probes, so a confirming
// row cannot add an obligation, a reading or a served shard anywhere, by
// construction rather than by a filter each query has to remember.
//
// What an answer does is decided by verdict.ConfirmNotServed and lands on
// the row it answers: confirmed_by when the other vantage did not get the
// rows either, which is what lets a not-served row count
// (rollup.ConfirmedSQL); cleared_by when it got them, which only names who
// did. Neither rewrites the row: its class, the blob's reading and the
// correlated-failure guard stay this observer's own.

var confirmMigration = migration{
	version: 23,
	note:    "second-vantage confirmation of not-served readings: the other vantage's answers, and cleared_by / confirmed_by on the row",
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

// InsertConfirmation stores one row another vantage wrote in answer to a
// confirmation request. own is this observer's vantage: the row answers
// own's probe of the same slot. Idempotent on the row's own key.
func (s *Store) InsertConfirmation(m probe.Measurement, raw []byte, own string) (bool, error) {
	pk := m
	pk.Vantage = own
	res, err := s.db.Exec(`INSERT INTO probe_confirmations
		(dedupe_key, probe_key, vantage, promise_hash, validator_address, validator_host, scheduled_at, started_at,
		 phase, outcome, classification, classification_reason, rows_returned, rows_expected,
		 commitment_verified, assignment_verified, rows_sha256, raw_error, raw_json)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(dedupe_key) DO NOTHING`,
		m.DedupeKey(), pk.DedupeKey(), m.Vantage, m.PromiseHash, m.ValidatorAddress, m.ValidatorHost,
		ts(m.ScheduledAt), ts(m.StartedAt), string(m.Phase), string(m.Outcome), string(m.Classification), m.ClassificationReason,
		m.Download.RowsReturned, m.Download.RowsExpected, b2i(m.Download.CommitmentVerified), b2i(m.Download.AssignmentVerified),
		nullIfEmpty(m.Download.RowsSHA256), m.RawError, string(raw))
	if err != nil {
		return false, fmt.Errorf("confirmation %s: %w", m.DedupeKey(), err)
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}

// ConfirmDecision is the rule applied to one answer.
type ConfirmDecision struct {
	ConfirmKey string // the other vantage's row
	ProbeKey   string // this observer's row
	Vantage    string
	Result     verdict.ConfirmResult
}

// JudgeConfirmations applies verdict.ConfirmNotServed to every answer not
// yet judged whose row is in the store. Nothing is written: the caller
// settles each decision (SettleConfirmation). Nothing is ever withdrawn or
// rewritten: a confirmation is what lets a not-served row count, and an
// answer that got the rows only names who fetched them. An answer about a
// row that cannot count not served (probe.ConfirmationDue: its rows came
// back, or it names no host) is judged with no effect.
func (s *Store) JudgeConfirmations(ctx context.Context, now time.Time) ([]ConfirmDecision, error) {
	rows, err := s.db.QueryContext(ctx, judgeConfirmationsSQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ConfirmDecision
	for rows.Next() {
		var d ConfirmDecision
		var cStarted, cPhase, cCls, pStarted, pMSU, pLabel, pCls, pAtProbe string
		var cVerified, cRules, pVerified bool
		var cOffset, pOffset int64
		var pRows, pHeld int
		if err := rows.Scan(&d.ConfirmKey, &d.ProbeKey, &d.Vantage, &cStarted, &cPhase, &cCls, &cVerified, &cRules, &cOffset,
			&pStarted, &pOffset, &pMSU, &pLabel, &pCls, &pAtProbe, &pVerified, &pRows, &pHeld); err != nil {
			return nil, err
		}
		// The row as it was read: a deferred verdict drawn since, or a
		// deadline correction, does not change whether it was a failure.
		due := probe.Confirmable(pLabel, probe.Classification(pCls)) || probe.Confirmable(pLabel, probe.Classification(pAtProbe)) ||
			(pVerified && pRows < pHeld)
		ps, err1 := time.Parse(TimeLayout, pStarted)
		cs, err2 := time.Parse(TimeLayout, cStarted)
		msu, err3 := time.Parse(TimeLayout, pMSU)
		if due && err1 == nil && err2 == nil && err3 == nil {
			d.Result = verdict.ConfirmNotServed(verdict.NotServed{StartedAt: ps, ClockOffsetMS: pOffset, MustServeUntil: msu},
				verdict.Confirmation{Vantage: d.Vantage, StartedAt: cs, ClockOffsetMS: cOffset, Phase: probe.Phase(cPhase),
					Classification: probe.Classification(cCls), CommitmentVerified: cVerified, ClientRules: cRules})
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

// SettleConfirmation records a decision: confirmed_by on the row when the
// other vantage confirmed it, cleared_by when it got the rows (which also
// takes back a confirmation another vantage gave: a row any vantage got
// the rows of does not count, verdict.ConfirmNotServedBy), and the answer
// marked judged either way.
func (s *Store) SettleConfirmation(d ConfirmDecision) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	switch d.Result {
	case verdict.ConfirmConfirmed:
		if _, err := tx.Exec(`UPDATE probes SET confirmed_by = ? WHERE dedupe_key = ? AND confirmed_by IS NULL AND cleared_by IS NULL`, d.Vantage, d.ProbeKey); err != nil {
			return err
		}
	case verdict.ConfirmServed:
		if _, err := tx.Exec(`UPDATE probes SET cleared_by = COALESCE(cleared_by, ?), confirmed_by = NULL WHERE dedupe_key = ?`, d.Vantage, d.ProbeKey); err != nil {
			return err
		}
	}
	if _, err := tx.Exec(`UPDATE probe_confirmations SET judged = 1 WHERE dedupe_key = ?`, d.ConfirmKey); err != nil {
		return err
	}
	return tx.Commit()
}

// judgeConfirmationsSQL runs every collector pass. It starts from the
// answers not yet judged (a partial index: nearly none) and reaches each
// row by its primary key, so it costs what is new, not what is stored. The
// answer's client_rules flag and clock offset are read from its own record
// (the table has no column for them).
const judgeConfirmationsSQL = `SELECT c.dedupe_key, c.probe_key, c.vantage, c.started_at, c.phase, c.classification, c.commitment_verified,
		COALESCE(json_extract(c.raw_json, '$.client_rules'), 0) = 1, COALESCE(json_extract(c.raw_json, '$.clock_offset_ms'), 0),
		pr.started_at, COALESCE(pr.clock_offset_ms, 0), pr.must_serve_until,
		pr.schedule_label, pr.classification, COALESCE(pr.classification_at_probe, ''), pr.commitment_verified, pr.rows_returned, pr.assigned_row_count
	FROM probe_confirmations c JOIN probes pr ON pr.dedupe_key = c.probe_key
	WHERE c.judged = 0
	ORDER BY c.probe_key, c.vantage`
