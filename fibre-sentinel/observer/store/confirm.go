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
// A second vantage fetches the rows of every row that counts not served
// once more (probe.Confirmable: a FAULT, or at the end reading any answer
// that left the reader without rows; internal/probe/confirm.go) and writes the result to its own
// measurements.jsonl, which is copied in beside this observer's record and
// ingested here, into probe_confirmations: never into probes. Every
// published figure is counted over probes, so a confirming row cannot add
// an obligation, a reading or a served shard anywhere, by construction
// rather than by a filter each query has to remember.
//
// What a confirmation can do is decided by verdict.ConfirmFault and lands on
// the fault's own row: cleared_by when another vantage got the verified rows
// (the fault is withdrawn through an amendment, PROBE_ERROR, and the
// amendment is logged like every other late verdict), confirmed_by when it
// did not get them either.

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
	ProbeKey   string // the fault's row
	Vantage    string
	Result     verdict.ConfirmResult
	// Amendment withdraws the fault; set when Result is ConfirmCleared.
	// The caller logs it and applies it (ApplyAmendment) before settling
	// the decision, in that order, as for every late verdict.
	Amendment *Amendment
}

// JudgeConfirmations applies verdict.ConfirmFault to every answer not yet
// judged whose fault row is in the store. Nothing is written: the caller
// logs and applies each amendment, then SettleConfirmation. An answer to a
// row that does not count not served (probe.Confirmable), or whose verdict
// was already amended, is judged with no effect.
func (s *Store) JudgeConfirmations(ctx context.Context, now time.Time) ([]ConfirmDecision, error) {
	rows, err := s.db.QueryContext(ctx, judgeConfirmationsSQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ConfirmDecision
	for rows.Next() {
		var d ConfirmDecision
		var cStarted, cCls, pStarted, pCls, hash, addr, sched, outcome, label string
		var amended bool
		if err := rows.Scan(&d.ConfirmKey, &d.ProbeKey, &d.Vantage, &cStarted, &cCls, &pStarted, &pCls, &amended, &hash, &addr, &sched, &outcome, &label); err != nil {
			return nil, err
		}
		if probe.Confirmable(label, probe.Classification(pCls)) && !amended {
			ps, err1 := time.Parse(TimeLayout, pStarted)
			cs, err2 := time.Parse(TimeLayout, cStarted)
			if err1 == nil && err2 == nil {
				d.Result = verdict.ConfirmFault(ps, verdict.Confirmation{Vantage: d.Vantage, StartedAt: cs, Classification: probe.Classification(cCls)})
			}
			if d.Result == verdict.ConfirmCleared {
				scheduled, _ := time.Parse(TimeLayout, sched)
				started := cs.UTC()
				d.Amendment = &Amendment{
					DedupeKey: d.ProbeKey, PromiseHash: hash, ValidatorAddress: addr, ScheduledAt: scheduled,
					From: pCls, To: string(verdict.ClearedClass), Reason: verdict.ClearedReason(d.Vantage, cs, outcome),
					JudgedAt: now.UTC(), ClearedBy: d.Vantage, ConfirmKey: d.ConfirmKey, ConfirmStartedAt: &started,
				}
			}
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

// SettleConfirmation records a decision: confirmed_by on the fault's row
// when the other vantage failed too, and the answer marked judged either
// way. A cleared fault's amendment has been applied by then.
func (s *Store) SettleConfirmation(d ConfirmDecision) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if d.Result == verdict.ConfirmConfirmed {
		if _, err := tx.Exec(`UPDATE probes SET confirmed_by = ? WHERE dedupe_key = ? AND confirmed_by IS NULL`, d.Vantage, d.ProbeKey); err != nil {
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
// fault by its primary key, so it costs what is new, not what is stored.
const judgeConfirmationsSQL = `SELECT c.dedupe_key, c.probe_key, c.vantage, c.started_at, c.classification,
		pr.started_at, pr.classification, pr.amended_at IS NOT NULL, pr.promise_hash, pr.validator_address, pr.scheduled_at, pr.outcome,
		pr.schedule_label
	FROM probe_confirmations c JOIN probes pr ON pr.dedupe_key = c.probe_key
	WHERE c.judged = 0
	ORDER BY c.probe_key, c.vantage`
