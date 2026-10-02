package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/probe"
)

// A publication the load policy samples out is recorded once
// (probe.SampledOut, sampling_decisions.jsonl) rather than as a NOT_PROBED
// row per assigned validator per schedule point. It is stored the same way:
// one sampling_decisions row, plus its points, and the rows it stands for
// are derived on read by sampled_out_rows: the decision times its points
// times the publication's assigned validators, from assignments. probe_rows
// (and obligation_rows, the same with two columns more) is probes with
// those rows beside it, and every figure that counted the NOT_PROBED rows
// (obligations, class tallies, gap and probe counts, the attestation split,
// the heatmap, the daily rollup) reads them, so it comes out as it did when
// the rows were stored.
//
// Figures that read only real results (faults, latency, reconstructability)
// stay on probes: a sampled-out row is never one of them.

// SampledOutReasonSQL is the test for a row written for a publication the
// sampler drew out whole, spelled so the partial index probes_sampled_out
// and every statement that seeks by it carry the same text.
const SampledOutReasonSQL = `classification = 'NOT_PROBED' AND substr(classification_reason, 1, 9) = 'budget:p='`

// sampledOutRowsView is the rows a decision stands for, column for column
// with what the prober wrote before (probe.SampledOut.Expand is the Go
// twin): assigned, the validator's attestation as the publication record
// has it, outcome MISSED, class NOT_PROBED, started at the decision.
//
// retention_unverified is 0: the hold overrides HEALTHY and FAULT only
// (rollup.EffectiveClass), so it changes nothing a NOT_PROBED row counts
// toward, and HeldCounts adds the held decisions itself.
const sampledOutRowsView = `CREATE VIEW IF NOT EXISTS sampled_out_rows AS
	SELECT pt.vantage AS vantage, pt.promise_hash AS promise_hash, a.validator_address AS validator_address,
	       '' AS validator_host, 1 AS assigned, a.row_count AS assigned_row_count, a.attested AS attested,
	       pt.schedule_label AS schedule_label, pt.scheduled_at AS scheduled_at, pt.started_at AS started_at,
	       pt.phase AS phase, 'MISSED' AS outcome, 'NOT_PROBED' AS classification, d.reason AS classification_reason,
	       0 AS tls_ok, 0 AS total_duration_ms, 0 AS retention_unverified, d.must_serve_until AS must_serve_until,
	       d.sampling_p AS sampling_p, d.sampling_binding AS sampling_binding, d.sampling_commitment AS sampling_commitment
	FROM sampling_decision_points pt
	CROSS JOIN sampling_decisions d ON d.vantage = pt.vantage AND d.promise_hash = pt.promise_hash
	CROSS JOIN assignments a ON a.promise_hash = pt.promise_hash AND a.row_count > 0`

// The point drives, and the joins are CROSS so it stays that way: every
// column a caller bounds the rows by (started_at, scheduled_at,
// promise_hash) is the point's own and indexed, and the decision and the
// assignments are then primary-key seeks. Left to itself the planner starts
// a one-validator query from that validator's assignments, which is every
// publication it was ever assigned rather than the window's.

// probeRowsCols are the columns of probe_rows: exactly the ones the covering
// indexes probes_window and probes_validator_window hold (migration 22), so
// a tally over probe_rows reads the stored rows from the index alone, as it
// did over probes. SQLite materialises every column of a UNION ALL view for
// each row, so one column more here puts every tally back on a table
// lookup per row.
const probeRowsCols = `assigned, phase, started_at, classification, schedule_label, attested,
	       validator_address, promise_hash, scheduled_at, retention_unverified, outcome`

// probeRowsView is every probe row, stored or stood for by a decision, as
// the figures read it: the class tallies, the probe, gap and attestation
// counts, the heatmap, the daily rollup.
const probeRowsView = `CREATE VIEW IF NOT EXISTS probe_rows AS
	SELECT ` + probeRowsCols + ` FROM probes
	UNION ALL
	SELECT ` + probeRowsCols + ` FROM sampled_out_rows`

// obligationRowsView is probe_rows with the two columns the obligation
// buckets read besides (rollup.ObligationBuckets): whether TLS came up and
// the deadline the row was drawn against. They were never in the covering
// indexes, so the obligation query reads them from the table as it did.
const obligationRowsView = `CREATE VIEW IF NOT EXISTS obligation_rows AS
	SELECT ` + probeRowsCols + `, tls_ok, must_serve_until FROM probes
	UNION ALL
	SELECT ` + probeRowsCols + `, tls_ok, must_serve_until FROM sampled_out_rows`

// ObligationRowsVerified is obligation_rows with commitment_verified beside
// it (0 for a sampled-out row, whose rows never came back), which the
// obligation buckets count served from (rollup.CountedClass), and the two
// columns a full reading's rule reads besides: rows_subset_of_assignment and
// next_attempt_due (0 and NULL for a sampled-out row). It is written inline
// rather than as a view so the schema does not move; the columns sit in the
// table row the view already reads tls_ok and must_serve_until from.
const ObligationRowsVerified = `(SELECT ` + probeRowsCols + `, tls_ok, must_serve_until, commitment_verified,
	       rows_subset_of_assignment, next_attempt_due FROM probes
	UNION ALL
	SELECT ` + probeRowsCols + `, tls_ok, must_serve_until, 0, 0, NULL FROM sampled_out_rows)`

// collapseSampledOut turns the NOT_PROBED rows of a publication sampled out
// whole, written before decisions had a record of their own, into the one
// decision they stand for, and deletes them. The migration runs it over the
// store as it is; the collector runs it after every ingest pass, which is
// what reaches a store rebuilt from a measurements.jsonl older than the
// decisions file.
//
// A publication is collapsed only when the decision reproduces its rows
// exactly, so no figure moves:
//
//   - every row of the promise is a sampled-out row (no real probe, no other
//     NOT_PROBED reason, no second vantage) of an assigned validator;
//   - they agree on the draw (p, binding, commitment, reason) and on the
//     deadline, which is the publication's own, uncorrected and unheld;
//   - there is exactly one row per assigned validator (assignments, row
//     count > 0, the same row count and attestation) per point, each point
//     with one label and one phase: which is what sampled_out_rows derives;
//   - nothing amended, corrected, cleared or confirmed any of them, and no
//     decision for the promise exists yet.
//
// A set short of that (a prober stopped half-way through writing one, a
// vantage that also probed unassigned validators) keeps its rows.
var collapseSampledOut = []string{
	`DROP TABLE IF EXISTS temp.collapse_sampled_out`,
	`CREATE TEMP TABLE collapse_sampled_out AS
	WITH cand AS (
		SELECT promise_hash, MIN(vantage) AS vantage, COUNT(*) AS n, COUNT(DISTINCT vantage) AS nv,
		       COUNT(DISTINCT scheduled_at) AS npts,
		       COUNT(DISTINCT scheduled_at || '|' || schedule_label || '|' || phase) AS nlabels,
		       MIN(assigned) AS all_assigned, MAX(retention_unverified) AS held,
		       COUNT(corrected_at) + COUNT(amended_at) + COUNT(cleared_by) + COUNT(confirmed_by) AS touched,
		       COUNT(DISTINCT classification_reason) AS nreason,
		       COUNT(DISTINCT COALESCE(sampling_p, -1)) AS np, MIN(sampling_p) AS p,
		       COUNT(DISTINCT COALESCE(sampling_binding, '')) AS nbinding,
		       COUNT(DISTINCT COALESCE(sampling_commitment, '')) AS ncommitment,
		       COUNT(DISTINCT must_serve_until) AS nmsu, MIN(must_serve_until) AS msu
		FROM probes WHERE ` + SampledOutReasonSQL + `
		GROUP BY promise_hash)
	SELECT c.promise_hash AS promise_hash, c.vantage AS vantage
	FROM cand c JOIN publications pb ON pb.promise_hash = c.promise_hash
	WHERE c.nv = 1 AND c.npts = c.nlabels AND c.all_assigned = 1 AND c.held = 0 AND c.touched = 0
	  AND c.nreason = 1 AND c.np = 1 AND c.p IS NOT NULL AND c.nbinding = 1 AND c.ncommitment = 1
	  AND c.nmsu = 1 AND pb.must_serve_until = c.msu AND pb.corrected_at IS NULL AND pb.retention_unverified = 0
	  AND c.n = (SELECT COUNT(*) FROM probes x WHERE x.promise_hash = c.promise_hash)
	  AND c.n = c.npts * (SELECT COUNT(*) FROM assignments a WHERE a.promise_hash = c.promise_hash AND a.row_count > 0)
	  AND c.n = (SELECT COUNT(*) FROM probes x JOIN assignments a
	                 ON a.promise_hash = x.promise_hash AND a.validator_address = x.validator_address
	             WHERE x.promise_hash = c.promise_hash AND a.row_count > 0
	               AND a.row_count = x.assigned_row_count AND a.attested IS x.attested)
	  AND NOT EXISTS (SELECT 1 FROM probe_corrections k WHERE k.promise_hash = c.promise_hash)
	  AND NOT EXISTS (SELECT 1 FROM probe_confirmations k WHERE k.promise_hash = c.promise_hash)
	  AND NOT EXISTS (SELECT 1 FROM sampling_decisions d WHERE d.promise_hash = c.promise_hash)`,
	// The decision, in the shape the prober now writes it (probe.SampledOut),
	// so an export or a reader of raw_json sees one kind of record whichever
	// way it was made; collapsed_from_rows says it was made from the rows.
	`INSERT INTO sampling_decisions (vantage, promise_hash, decided_at, sampling_p, sampling_binding, sampling_commitment,
		reason, must_serve_until, commitment, settlement_time, validators, points, source, raw_json)
	SELECT pr.vantage, pr.promise_hash, MIN(pr.started_at), MIN(pr.sampling_p), MIN(pr.sampling_binding), MIN(pr.sampling_commitment),
	       MIN(pr.classification_reason), MIN(pr.must_serve_until), MIN(pr.commitment), MIN(pb.settlement_time),
	       COUNT(DISTINCT pr.validator_address), COUNT(DISTINCT pr.scheduled_at), 'rows',
	       json_object('schema_version', 1, 'kind', 'sampled_out', 'vantage', pr.vantage, 'promise_hash', pr.promise_hash,
	           'commitment', MIN(pr.commitment), 'blob_version', MIN(pr.blob_version),
	           'settlement_time', MIN(pb.settlement_time), 'must_serve_until', MIN(pr.must_serve_until),
	           'validator_set_height', MIN(pr.validator_set_height), 'decided_at', MIN(pr.started_at),
	           'sampling', json_object('p', MIN(pr.sampling_p), 'binding', MIN(pr.sampling_binding), 'day_commitment', MIN(pr.sampling_commitment)),
	           'reason', MIN(pr.classification_reason),
	           'points', (SELECT json_group_array(json_object('schedule_label', q.schedule_label, 'scheduled_at', q.scheduled_at, 'phase', q.phase))
	                      FROM (SELECT DISTINCT y.schedule_label, y.scheduled_at, y.phase FROM probes y
	                            WHERE y.promise_hash = pr.promise_hash ORDER BY y.scheduled_at) q),
	           'validators', COUNT(DISTINCT pr.validator_address),
	           'collapsed_from_rows', COUNT(*))
	FROM probes pr
	JOIN temp.collapse_sampled_out c ON c.promise_hash = pr.promise_hash AND c.vantage = pr.vantage
	JOIN publications pb ON pb.promise_hash = pr.promise_hash
	GROUP BY pr.vantage, pr.promise_hash
	ON CONFLICT(vantage, promise_hash) DO NOTHING`,
	`INSERT INTO sampling_decision_points (vantage, promise_hash, schedule_label, scheduled_at, phase, started_at)
	SELECT DISTINCT pr.vantage, pr.promise_hash, pr.schedule_label, pr.scheduled_at, pr.phase, d.decided_at
	FROM probes pr JOIN temp.collapse_sampled_out c ON c.promise_hash = pr.promise_hash AND c.vantage = pr.vantage
	JOIN sampling_decisions d ON d.vantage = pr.vantage AND d.promise_hash = pr.promise_hash
	ON CONFLICT(vantage, promise_hash, scheduled_at) DO NOTHING`,
	`DELETE FROM probes WHERE promise_hash IN (SELECT promise_hash FROM temp.collapse_sampled_out) AND ` + SampledOutReasonSQL,
	`DROP TABLE temp.collapse_sampled_out`,
}

// sampledOutMigration is version 24: the tables a sampled-out publication is
// stored in, the views that derive its rows, and the collapse of the rows the
// store already holds.
var sampledOutMigration = migration{
	version: 24,
	note:    "a sampled-out publication is one sampling_decisions row, not a NOT_PROBED row per validator per point; probe_rows derives the rows for every figure, and the rows already stored are collapsed",
	stmts: append([]string{
		// source: 'prober' for a decision read from sampling_decisions.jsonl,
		// 'rows' for one collapsed from the NOT_PROBED rows the prober wrote
		// before the file existed.
		`CREATE TABLE IF NOT EXISTS sampling_decisions (
			vantage             TEXT NOT NULL,
			promise_hash        TEXT NOT NULL,
			decided_at          TEXT NOT NULL,
			sampling_p          REAL NOT NULL,
			sampling_binding    TEXT,
			sampling_commitment TEXT,
			reason              TEXT NOT NULL,
			must_serve_until    TEXT NOT NULL,
			commitment          TEXT NOT NULL DEFAULT '',
			settlement_time     TEXT NOT NULL DEFAULT '',
			validators          INTEGER NOT NULL DEFAULT 0,
			points              INTEGER NOT NULL DEFAULT 0,
			source              TEXT NOT NULL,
			raw_json            TEXT NOT NULL,
			PRIMARY KEY (vantage, promise_hash)
		)`,
		`CREATE INDEX IF NOT EXISTS sampling_decisions_decided ON sampling_decisions (decided_at)`,
		`CREATE INDEX IF NOT EXISTS sampling_decisions_promise ON sampling_decisions (promise_hash)`,
		`CREATE TABLE IF NOT EXISTS sampling_decision_points (
			vantage        TEXT NOT NULL,
			promise_hash   TEXT NOT NULL,
			schedule_label TEXT NOT NULL,
			scheduled_at   TEXT NOT NULL,
			phase          TEXT NOT NULL,
			started_at     TEXT NOT NULL,
			PRIMARY KEY (vantage, promise_hash, scheduled_at),
			FOREIGN KEY (vantage, promise_hash) REFERENCES sampling_decisions (vantage, promise_hash) ON DELETE CASCADE
		)`,
		// The points drive sampled_out_rows (see there): started_at is the
		// decision's decided_at, the started_at of every row it stands for,
		// copied here so a window seeks the points directly.
		`CREATE INDEX IF NOT EXISTS sampling_decision_points_started ON sampling_decision_points (started_at)`,
		`CREATE INDEX IF NOT EXISTS sampling_decision_points_scheduled ON sampling_decision_points (scheduled_at)`,
		`CREATE INDEX IF NOT EXISTS sampling_decision_points_promise ON sampling_decision_points (promise_hash)`,
		// The rows a collapse looks for: partial, so it holds only what is
		// still to collapse (nothing, once the store is migrated) and costs
		// nothing on every other insert.
		`CREATE INDEX IF NOT EXISTS probes_sampled_out ON probes (promise_hash) WHERE ` + SampledOutReasonSQL,
		sampledOutRowsView,
		probeRowsView,
		obligationRowsView,
	}, collapseSampledOut...),
}

// InsertSampledOut stores one decision from sampling_decisions.jsonl. raw is
// the line as read. Idempotent on (vantage, promise_hash). A decision for a
// promise whose rows this vantage already stored (the prober wrote them
// before the file existed) is not stored twice: the rows stand, and a
// decision beside them would count every one of them again.
func (s *Store) InsertSampledOut(d probe.SampledOut, raw []byte) (bool, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	var rows int
	if err := tx.QueryRow(`SELECT COUNT(*) FROM probes WHERE promise_hash = ? AND vantage = ?`, d.PromiseHash, d.Vantage).Scan(&rows); err != nil {
		return false, err
	}
	if rows > 0 {
		return false, tx.Commit()
	}
	res, err := tx.Exec(`INSERT INTO sampling_decisions (vantage, promise_hash, decided_at, sampling_p, sampling_binding, sampling_commitment,
			reason, must_serve_until, commitment, settlement_time, validators, points, source, raw_json)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'prober', ?)
		ON CONFLICT(vantage, promise_hash) DO NOTHING`,
		d.Vantage, d.PromiseHash, ts(d.DecidedAt), d.Sampling.P, nullIfEmpty(d.Sampling.Binding), nullIfEmpty(d.Sampling.DayCommitment),
		d.Reason, ts(d.MustServeUntil), d.Commitment, ts(d.SettlementTime), d.Validators, len(d.Points), string(raw))
	if err != nil {
		return false, fmt.Errorf("sampling decision %s: %w", d.Key(), err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return false, tx.Commit()
	}
	for _, pt := range d.Points {
		if _, err := tx.Exec(`INSERT INTO sampling_decision_points (vantage, promise_hash, schedule_label, scheduled_at, phase, started_at)
			VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT(vantage, promise_hash, scheduled_at) DO NOTHING`,
			d.Vantage, d.PromiseHash, pt.Label, ts(pt.At), string(pt.Phase), ts(d.DecidedAt)); err != nil {
			return false, fmt.Errorf("sampling decision %s point %s: %w", d.Key(), pt.Label, err)
		}
	}
	return true, tx.Commit()
}

// sampledOutDecided reports whether a decision stands for this sampled-out
// row, in which case the row is not stored: re-reading the measurements.jsonl
// lines a collapse deleted must not bring them back.
func (s *Store) sampledOutDecided(m probe.Measurement) (bool, error) {
	if !probe.IsSampledOutRow(m) {
		return false, nil
	}
	var n int
	err := s.db.QueryRow(`SELECT COUNT(*) FROM sampling_decisions WHERE vantage = ? AND promise_hash = ?`, m.Vantage, m.PromiseHash).Scan(&n)
	return n > 0, err
}

// CollapseSampledOut runs the collapse (see collapseSampledOut) and reports
// how many decisions it made and how many rows it deleted. Cheap when there
// is nothing to do: the candidates are read from probes_sampled_out, which
// is empty then.
func (s *Store) CollapseSampledOut(ctx context.Context) (decisions, rows int64, err error) {
	var pending int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM probes WHERE `+SampledOutReasonSQL).Scan(&pending); err != nil {
		return 0, 0, err
	}
	if pending == 0 {
		return 0, 0, nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, 0, err
	}
	defer tx.Rollback()
	var before int64
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM sampling_decisions`).Scan(&before); err != nil {
		return 0, 0, err
	}
	for i, stmt := range collapseSampledOut {
		res, err := tx.ExecContext(ctx, stmt)
		if err != nil {
			return 0, 0, fmt.Errorf("collapse sampled-out rows: %w", err)
		}
		if i == len(collapseSampledOut)-2 { // the DELETE
			rows, _ = res.RowsAffected()
		}
	}
	var after int64
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM sampling_decisions`).Scan(&after); err != nil {
		return 0, 0, err
	}
	return after - before, rows, tx.Commit()
}

// SampledOutPoint is one point of a sampled-out decision as the corrector
// re-grades it: the phase the rows it stands for carry, and the deadline the
// decision was drawn against.
type SampledOutPoint struct {
	Vantage        string
	PromiseHash    string
	Label          string
	ScheduledAt    time.Time
	Phase          string
	MustServeUntil time.Time
	// Deadline and UncertaintyID are set by StaleSampledOutPoints: the
	// deadline the publication carries now, and the range that moved it.
	Deadline      time.Time
	UncertaintyID string
}

// Key is the point's key in corrections.jsonl (Correction.DedupeKey of a
// sampled_out_point line): the rows' dedupe key without the validator, which
// every validator's row at the point shares.
func (p SampledOutPoint) Key() string {
	return p.Vantage + "|" + p.PromiseHash + "|*|" + p.ScheduledAt.UTC().Format(time.RFC3339Nano)
}

const sampledOutPointsSQL = `SELECT pt.vantage, pt.promise_hash, pt.schedule_label, pt.scheduled_at, pt.phase, d.must_serve_until`

func scanSampledOutPoints(rows *sql.Rows, stale bool) ([]SampledOutPoint, error) {
	defer rows.Close()
	var out []SampledOutPoint
	for rows.Next() {
		var p SampledOutPoint
		var sched, msu, deadline string
		dest := []any{&p.Vantage, &p.PromiseHash, &p.Label, &sched, &p.Phase, &msu}
		if stale {
			dest = append(dest, &deadline, &p.UncertaintyID)
		}
		if err := rows.Scan(dest...); err != nil {
			return nil, err
		}
		p.ScheduledAt, _ = time.Parse(TimeLayout, sched)
		p.MustServeUntil, _ = time.Parse(TimeLayout, msu)
		if stale {
			var err error
			if p.Deadline, err = time.Parse(TimeLayout, deadline); err != nil {
				continue
			}
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// SampledOutPointsOf returns the points of every sampled-out decision on one
// publication, for the corrector.
func (s *Store) SampledOutPointsOf(ctx context.Context, promiseHash string) ([]SampledOutPoint, error) {
	rows, err := s.db.QueryContext(ctx, sampledOutPointsSQL+`
		FROM sampling_decisions d
		JOIN sampling_decision_points pt ON pt.vantage = d.vantage AND pt.promise_hash = d.promise_hash
		WHERE d.promise_hash = ? ORDER BY pt.vantage, pt.scheduled_at`, promiseHash)
	if err != nil {
		return nil, err
	}
	return scanSampledOutPoints(rows, false)
}

// StaleSampledOutPoints is StaleDeadlineRows for decisions: the points of
// every decision drawn against a deadline its publication has since moved
// away from (a decision recorded after the correction, from the deadline on
// the append-only record).
func (s *Store) StaleSampledOutPoints(ctx context.Context, limit int) ([]SampledOutPoint, error) {
	rows, err := s.db.QueryContext(ctx, sampledOutPointsSQL+`, pb.must_serve_until,
			COALESCE((SELECT c.uncertainty_id FROM publication_corrections c
			          WHERE c.promise_hash = pb.promise_hash ORDER BY c.judged_at DESC LIMIT 1), '')
		FROM publications pb
		CROSS JOIN sampling_decisions d ON d.promise_hash = pb.promise_hash
		JOIN sampling_decision_points pt ON pt.vantage = d.vantage AND pt.promise_hash = d.promise_hash
		WHERE pb.corrected_at IS NOT NULL AND d.must_serve_until <> pb.must_serve_until
		ORDER BY d.decided_at, pt.scheduled_at
		LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	return scanSampledOutPoints(rows, true)
}

// ApplySampledOutCorrection moves one point of a sampled-out decision to the
// phase a verified range supports, and the decision to the corrected
// deadline: what ApplyProbeCorrection does to each row the decision stands
// for. Idempotent; ErrNoSuchRow when the decision is not stored (yet).
func (s *Store) ApplySampledOutCorrection(c Correction) (bool, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	res, err := tx.Exec(`UPDATE sampling_decision_points SET phase = ?
		WHERE vantage = ? AND promise_hash = ? AND scheduled_at = ?`, c.ToPhase, c.Vantage, c.PromiseHash, ts(c.ScheduledAt))
	if err != nil {
		return false, err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return false, ErrNoSuchRow
	}
	if _, err := tx.Exec(`UPDATE sampling_decisions SET must_serve_until = ? WHERE vantage = ? AND promise_hash = ?`,
		ts(c.ToMustServeUntil), c.Vantage, c.PromiseHash); err != nil {
		return false, err
	}
	return true, tx.Commit()
}
