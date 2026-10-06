package api

import (
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/rollup"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
)

// The statements the day partials read besides the shipped ones. Each is
// either the shipped text over other bounds (the row statements in
// rowsql.go, obligationPassSQL, signingByValidatorSQL with a height range)
// or a read of what the ledger and the invalidation need (the new rows of a
// table, a day's row span, the anchors).

// heightsSQL narrows publications (alias p, with its dot) to a settlement
// height range. It is a planner aid: the range is the lowest and highest
// height the ledger has counted on the days the statement covers, which
// holds every publication settled on them, so the answer is the same as
// without it, and publications_settlement is sought instead of every
// publication walked. It assumes nothing about time and height moving
// together.
func heightsSQL(p string) string {
	return ` AND ` + p + `settlement_height >= ? AND ` + p + `settlement_height <= ?`
}

// pubCountSQL is the network's publication count and bytes over a span of
// whole or partial days, narrowed by their height range: the shipped
// statement with heightsSQL.
const pubCountSQL = `SELECT COUNT(*), COALESCE(SUM(blob_size),0) FROM publications
	WHERE settlement_time >= ? AND settlement_time <= ? AND settlement_height >= ? AND settlement_height <= ?`

// readableCountSQL is reconstructableCount's count statement over a span,
// narrowed by its height range. Its arguments are the pin's bound (none,
// or the pinned end), the moment the windows are asked at, the pin's bound
// again, that moment again, the span's bounds and its height range.
func readableCountSQL(bound string) string {
	readable := readableSQL(bound)
	return `SELECT COUNT(*),
			COALESCE(SUM(NOT ` + readable + ` AND must_serve_until >= ?), 0),
			COALESCE(SUM(NOT ` + readable + ` AND must_serve_until < ?), 0)
		FROM publications WHERE settlement_time >= ? AND settlement_time <= ?` + heightsSQL("")
}

// dayReadableSQL is how many publications of a span have a reading on
// record: the part of readableCountSQL a sealed day keeps.
var dayReadableSQL = `SELECT COUNT(*) FROM publications
	WHERE settlement_time >= ? AND settlement_time <= ?` + heightsSQL("") + ` AND ` + readableSQL("")

// loadSpanSQL is the load of a span of publications the ledger does not
// hold whole, per validator and original_rows: the in-window half of
// loadSQL's population over the span, narrowed by its height range, read as
// the exact integers loadBytes forms the sum from. ?1 and ?2 are the span's
// bounds, ?3 and ?4 its height range, ?5 the memo's JSON (loadSQL's ?4);
// filter narrows it to one validator (?6).
//
// The load bytes are split into row_count times the high and low twenty
// bits of blob_size, so the integer sums cannot overflow however many
// assignments a group holds.
func loadSpanSQL(filter string) string {
	return `WITH m AS MATERIALIZED (SELECT key AS promise_hash, value AS original_rows FROM json_each(?5)),
		pb AS MATERIALIZED (
			SELECT p.promise_hash, p.blob_size,
				` + origSQL + ` AS orig
			FROM publications p LEFT JOIN m ON m.promise_hash = p.promise_hash
			WHERE p.settlement_time >= ?1 AND p.settlement_time <= ?2 AND p.settlement_height >= ?3 AND p.settlement_height <= ?4
			  AND p.settlement_tx_code = 0 AND p.assignment_error = '')
		SELECT a.validator_address, pb.orig, COUNT(*), COALESCE(SUM(a.row_count), 0),
			COALESCE(SUM(a.row_count * (pb.blob_size >> 20)), 0), COALESCE(SUM(a.row_count * (pb.blob_size & 1048575)), 0),
			COALESCE(MAX(a.row_count * pb.blob_size), 0), COALESCE(MAX(pb.blob_size), 0)
		FROM pb CROSS JOIN assignments a ON a.promise_hash = pb.promise_hash
		WHERE a.row_count > 0 AND a.attested = 1` + filter + `
		GROUP BY a.validator_address, pb.orig`
}

// origSQL is original_rows as rowBytesSQL reads it: the memo's entry (m)
// when it has one, the record's own otherwise.
const origSQL = `CASE WHEN m.promise_hash IS NOT NULL THEN m.original_rows
					ELSE p.original_rows END`

// loadHeldSQL is loadSQL's other half, what each validator holds at ?3, over
// its own population: the held publications only (heldSQL), read through
// publications_msu. The float sum is loadSQL's, over the same terms in the
// same order (ORDER BY a.rowid; a term of a publication not held was NULL
// there, and SUM skips a NULL), so the result is the same bits. ?4 is the
// memo's JSON; filter narrows it to one validator (?5).
func loadHeldSQL(filter string) string {
	return `WITH m AS MATERIALIZED (SELECT key AS promise_hash, value AS original_rows FROM json_each(?4)),
		pb AS MATERIALIZED (
			SELECT p.promise_hash, ` + rowBytesSQL + ` AS rb
			FROM publications p LEFT JOIN m ON m.promise_hash = p.promise_hash
			WHERE p.settlement_tx_code = 0 AND p.assignment_error = '' AND ` + heldSQL + `)
		SELECT a.validator_address,
			COALESCE(CAST(SUM(a.row_count * pb.rb ORDER BY a.rowid) AS INTEGER), 0)
		FROM pb CROSS JOIN assignments a ON a.promise_hash = pb.promise_hash
		WHERE a.row_count > 0 AND a.attested = 1` + filter + `
		GROUP BY a.validator_address`
}

// loadHeldPopulationSQL selects the publications loadHeldSQL reads, for the
// memo (originalRowsMemo.docWhere). Its argument is ?3 of heldSQL.
const loadHeldPopulationSQL = `p.settlement_tx_code = 0 AND p.assignment_error = '' AND ` + heldSQL

// ---- the ledger: publications folded in by rowid ----

// ledgerPubsSQL is the publications with a rowid in (?1, ?2]: what the
// ledger keeps of each one besides its assignments.
const ledgerPubsSQL = `SELECT promise_hash, settlement_time, settlement_height, promise_height, blob_size, must_serve_until
	FROM publications WHERE rowid > ?1 AND rowid <= ?2`

// ledgerSigningSQL is signingByValidatorSQL's counts over the publications
// with a rowid in (?1, ?2], per settlement day and validator.
const ledgerSigningSQL = `SELECT substr(p.settlement_time, 1, 10), a.validator_address,
		COALESCE(SUM(CASE WHEN a.attested IS NOT NULL AND a.host_at_settlement IS NOT '' THEN 1 ELSE 0 END), 0),
		COALESCE(SUM(CASE WHEN a.attested = 1 AND a.host_at_settlement IS NOT '' THEN 1 ELSE 0 END), 0),
		COALESCE(SUM(CASE WHEN a.attested IS NULL AND a.host_at_settlement IS NOT '' THEN 1 ELSE 0 END), 0),
		COALESCE(SUM(CASE WHEN a.host_at_settlement = '' THEN 1 ELSE 0 END), 0)
	FROM publications p CROSS JOIN assignments a ON a.promise_hash = p.promise_hash
	WHERE p.rowid > ?1 AND p.rowid <= ?2 AND p.settlement_tx_code = 0 AND p.assignment_error = '' AND a.row_count > 0
	GROUP BY 1, 2`

// ledgerLoadSQL is loadSpanSQL over the publications with a rowid in
// (?1, ?2], per settlement day, validator and original_rows. ?3 is the
// memo's JSON.
const ledgerLoadSQL = `WITH m AS MATERIALIZED (SELECT key AS promise_hash, value AS original_rows FROM json_each(?3)),
	pb AS MATERIALIZED (
		SELECT p.promise_hash, p.blob_size, p.settlement_time,
			` + origSQL + ` AS orig
		FROM publications p LEFT JOIN m ON m.promise_hash = p.promise_hash
		WHERE p.rowid > ?1 AND p.rowid <= ?2 AND p.settlement_tx_code = 0 AND p.assignment_error = '')
	SELECT substr(pb.settlement_time, 1, 10), a.validator_address, pb.orig, COUNT(*), COALESCE(SUM(a.row_count), 0),
		COALESCE(SUM(a.row_count * (pb.blob_size >> 20)), 0), COALESCE(SUM(a.row_count * (pb.blob_size & 1048575)), 0),
		COALESCE(MAX(a.row_count * pb.blob_size), 0), COALESCE(MAX(pb.blob_size), 0)
	FROM pb CROSS JOIN assignments a ON a.promise_hash = pb.promise_hash
	WHERE a.row_count > 0 AND a.attested = 1
	GROUP BY 1, 2, 3`

// ledgerRowsSpanSQL is the start and deadline extent of the rows (both arms
// of store.ObligationRowsVerified) of the publications with a rowid in
// (?1, ?2], per settlement day: what a publication that arrives after its
// rows adds to its day's span.
const ledgerRowsSpanSQL = `WITH pb AS MATERIALIZED (SELECT promise_hash, settlement_time FROM publications WHERE rowid > ?1 AND rowid <= ?2)
	SELECT day, MIN(lo), MAX(hi), MAX(msu) FROM (
		SELECT substr(pb.settlement_time, 1, 10) AS day, MIN(r.started_at) AS lo, MAX(r.started_at) AS hi, MAX(r.must_serve_until) AS msu
		FROM pb CROSS JOIN probes r ON r.promise_hash = pb.promise_hash GROUP BY 1
		UNION ALL
		SELECT substr(pb.settlement_time, 1, 10), MIN(pt.started_at), MAX(pt.started_at), MAX(d.must_serve_until)
		FROM pb CROSS JOIN sampling_decision_points pt ON pt.promise_hash = pb.promise_hash
		JOIN sampling_decisions d ON d.vantage = pt.vantage AND d.promise_hash = pt.promise_hash GROUP BY 1)
	GROUP BY day`

// ledgerPointDaysSQL is the days the decision points of the publications
// with a rowid in (?1, ?2] started on: the rows a decision stands for
// appear there only once its publication's assignments do.
const ledgerPointDaysSQL = `SELECT DISTINCT pt.started_at FROM publications p
	CROSS JOIN sampling_decision_points pt ON pt.promise_hash = p.promise_hash
	WHERE p.rowid > ?1 AND p.rowid <= ?2`

// ---- a day's span, its seal, its anchors ----

// daySpanSQL is the exact row span of the publications settled in a span
// (?1, ?2, heights ?3, ?4): the start of every row of theirs in both arms of
// store.ObligationRowsVerified, and the latest deadline among them.
const daySpanSQL = `WITH pb AS MATERIALIZED (SELECT promise_hash FROM publications
		WHERE settlement_time >= ?1 AND settlement_time <= ?2 AND settlement_height >= ?3 AND settlement_height <= ?4)
	SELECT MIN(lo), MAX(hi), MAX(msu) FROM (
		SELECT MIN(r.started_at) AS lo, MAX(r.started_at) AS hi, MAX(r.must_serve_until) AS msu
		FROM pb CROSS JOIN probes r ON r.promise_hash = pb.promise_hash
		UNION ALL
		SELECT MIN(pt.started_at), MAX(pt.started_at), MAX(d.must_serve_until)
		FROM pb CROSS JOIN sampling_decision_points pt ON pt.promise_hash = pb.promise_hash
		JOIN sampling_decisions d ON d.vantage = pt.vantage AND d.promise_hash = pt.promise_hash)`

// dayMaxPubMSUSQL is the latest deadline of the publications settled in a
// span, as a correction may have moved it.
const dayMaxPubMSUSQL = `SELECT MAX(must_serve_until) FROM publications
	WHERE settlement_time >= ? AND settlement_time <= ?` + ` AND settlement_height >= ? AND settlement_height <= ?`

// nextRowSQL is the first start at or after ?1 of a row the row days sum:
// a probe row, a decision point, or one of this observer's heartbeats (?2),
// each the first entry of its table's started_at index from there on.
const nextRowSQL = `SELECT MIN(t) FROM (
	SELECT * FROM (SELECT started_at AS t FROM probes WHERE started_at >= ?1 ORDER BY started_at LIMIT 1)
	UNION ALL SELECT * FROM (SELECT started_at FROM sampling_decision_points WHERE started_at >= ?1 ORDER BY started_at LIMIT 1)
	UNION ALL SELECT * FROM (SELECT started_at FROM reachability WHERE started_at >= ?1 AND +vantage = ?2 ORDER BY started_at LIMIT 1))`

// ---- what the catch-up reads that no log records ----

// reachableSQL is, of the publications named in ?1 (a JSON array of promise
// hashes), those a correction can reach: moved by one, or covered by a
// verified params range (the corrector's range pass).
const reachableSQL = `SELECT p.promise_hash FROM json_each(?) j CROSS JOIN publications p ON p.promise_hash = j.value
	WHERE p.corrected_at IS NOT NULL OR EXISTS (SELECT 1 FROM param_uncertainty u
		WHERE u.resolution = 'verified' AND p.settlement_height >= u.from_height AND p.promise_height - 1 <= u.to_height)`

// metaValueSQL reads a key of meta (?1), "" when it is absent: the holds'
// counter (store.MetaHeldFlagsRev), the migrations that rewrote rows.
const metaValueSQL = `SELECT COALESCE((SELECT value FROM meta WHERE key = ?), '')`

// heldByPromiseSQL is every promise's held probe rows, their rowids listed
// in order (heldDigest), a walk of probes_held alone; heldOfPromisesSQL the
// same of the promises named in ?1 (a JSON array), a seek of it each.
const (
	heldByPromiseSQL = `SELECT promise_hash, group_concat(rowid, ',' ORDER BY rowid)
		FROM probes WHERE retention_unverified = 1 GROUP BY promise_hash`
	heldOfPromisesSQL = `SELECT r.promise_hash, group_concat(r.rowid, ',' ORDER BY r.rowid)
		FROM json_each(?) j CROSS JOIN probes r ON r.promise_hash = j.value AND r.retention_unverified = 1
		GROUP BY r.promise_hash`
)

// heldPubsSQL is the held publications, a walk of publications_held alone.
const heldPubsSQL = `SELECT promise_hash FROM publications WHERE retention_unverified = 1`

// newHeldRowsSQL and newHeldPubsSQL are the promises of the rows and the
// publications with a rowid in (?1, ?2] stored held.
const (
	newHeldRowsSQL = `SELECT DISTINCT promise_hash FROM probes WHERE rowid > ?1 AND rowid <= ?2 AND retention_unverified = 1`
	newHeldPubsSQL = `SELECT promise_hash FROM publications WHERE rowid > ?1 AND rowid <= ?2 AND retention_unverified = 1`
)

// promiseRowDaysSQL is the days the probe rows of the promises in ?1 (a
// JSON array) started on.
const promiseRowDaysSQL = `SELECT DISTINCT substr(r.started_at, 1, 10) FROM json_each(?) j CROSS JOIN probes r ON r.promise_hash = j.value`

// correctedPubsSQL is every corrected publication (publications_corrected):
// its deadline, when a correction last wrote it, and its settlement day.
const correctedPubsSQL = `SELECT promise_hash, must_serve_until, corrected_at, COALESCE(substr(settlement_time, 1, 10), '')
	FROM publications WHERE corrected_at IS NOT NULL`

// correctedRowsSQL is, per promise named in ?1 (a JSON array of promise
// hashes), what a correction wrote on its rows, one line of text in rowid
// order that the catch-up digests, their latest deadline, and the
// publication's settlement day. The text is made in the statement, so a
// catch-up is handed a row per promise, not one per corrected row.
const correctedRowsSQL = `SELECT r.promise_hash,
		group_concat(r.rowid || ' ' || r.phase || ' ' || r.classification || ' ' || r.must_serve_until || ' ' || r.corrected_at, ',' ORDER BY r.rowid),
		MAX(r.must_serve_until),
		COALESCE((SELECT substr(p.settlement_time, 1, 10) FROM publications p WHERE p.promise_hash = r.promise_hash), '')
	FROM json_each(?) j CROSS JOIN probes r ON r.promise_hash = j.value
	WHERE r.corrected_at IS NOT NULL
	GROUP BY r.promise_hash`

// correctedPromisesSQL is every promise the row corrections' log names
// (a walk of probe_corrections_promise, at a build), and
// newCorrectedPromisesSQL those of its lines with a rowid in (?1, ?2].
const (
	correctedPromisesSQL    = `SELECT DISTINCT promise_hash FROM probe_corrections`
	newCorrectedPromisesSQL = `SELECT DISTINCT promise_hash FROM probe_corrections WHERE rowid > ?1 AND rowid <= ?2`
)

// dayTiesSQL counts, among the obligation rows of the publications settled
// in a span, the rows that tie on everything ObligationBuckets orders an
// obligation's newest row by: where two exist, the class it reads depends
// on the plan and the sort. A day that has any is not sealed.
var dayTiesSQL = `SELECT COUNT(*) FROM (
		SELECT pr.validator_address, pr.promise_hash, pr.classification IN ('NOT_PROBED','PROBE_ERROR') AS g, pr.scheduled_at, pr.started_at
		FROM ` + store.ObligationRowsVerified + ` pr JOIN publications pb ON pb.promise_hash = pr.promise_hash
		WHERE pb.settlement_time >= ? AND pb.settlement_time <= ? AND pr.started_at <= ? AND pr.started_at >= ?
		  AND pr.assigned = 1 AND pr.phase = 'in_window' AND pr.attested = 1
		GROUP BY 1, 2, 3, 4, 5 HAVING COUNT(*) > 1)`

// dayCollapsibleSQL is the promises with a row on a day that a collapse
// would make a decision of, read from probes_sampled_out, which holds only
// the rows still to collapse (none, once a store is migrated): left to
// itself the planner walks every NOT_PROBED row instead.
const dayCollapsibleSQL = `SELECT DISTINCT promise_hash FROM probes INDEXED BY probes_sampled_out
	WHERE ` + store.SampledOutReasonSQL + ` AND +started_at >= ? AND +started_at <= ?`

// The anchors of a row day: one row of each table started on it, and the
// check that it is still there.
const (
	anchorProbeSQL = `SELECT rowid, dedupe_key FROM probes WHERE started_at >= ? AND started_at <= ?
		AND NOT (` + store.SampledOutReasonSQL + `) LIMIT 1`
	anchorProbeAnySQL = `SELECT rowid, dedupe_key FROM probes WHERE started_at >= ? AND started_at <= ? LIMIT 1`
	anchorPointSQL    = `SELECT rowid, vantage || '|' || promise_hash || '|' || scheduled_at FROM sampling_decision_points
		WHERE started_at >= ? AND started_at <= ? LIMIT 1`
	anchorBeatSQL = `SELECT rowid, dedupe_key FROM reachability WHERE started_at >= ? AND started_at <= ? AND +vantage = ? LIMIT 1`
)

// boundaryGapSQL finds a row start of a table strictly between two bounds:
// between one day's last timestamp and the next day's first, where only a
// start that is not a store timestamp can sort.
func boundaryGapSQL(table string) string {
	return `SELECT started_at FROM ` + table + ` WHERE started_at > ? AND started_at < ? LIMIT 1`
}

// dayDeferredSQL counts the rows started on a day that still await the late
// shadow verdict (rollup.ShadowVerdictsSettled's rows, by start), sought
// through probes_deferred.
const dayDeferredSQL = `SELECT COUNT(*) FROM probes
	WHERE classification = 'PROBE_ERROR' AND shadow_gap IS NOT NULL AND amended_at IS NULL
	  AND outcome IN ('WRONG_ROWS','PARTIAL') AND commitment_verified = 1 AND +started_at >= ? AND +started_at <= ?`

// obligationPassSQL is readObligations' statement: every validator's nine
// counts over the buckets, and how many of its broken obligations still
// settle and the youngest first fault among them. Its arguments are the
// provisional cutoff twice, then ObligationBuckets' own, then extra's.
func obligationPassSQL(extra string) string {
	return `SELECT validator_address, ` + obligationSums + `,
			COALESCE(SUM(NOT pending AND faults > 0 AND first_fault > ?), 0),
			MAX(CASE WHEN NOT pending AND faults > 0 AND first_fault > ? THEN first_fault END)
		FROM (` + rollup.ObligationBuckets + extra + `)
			GROUP BY validator_address, promise_hash) GROUP BY validator_address`
}
