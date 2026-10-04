package api

import "github.com/plsgiveup/fibre/fibre-sentinel/observer/store"

// dayPartsPlan is one statement the day partials run per refresh, per
// catch-up or per seal, and the index its plan must use: each costs what
// its span holds, never the whole table (TestHotQueriesUseIndexes).
type dayPartsPlan struct {
	name  string
	q     string
	args  []any
	want  []string
	scans []string // subquery results and CTEs it may walk whole
}

func dayPartsPlans() []dayPartsPlan {
	const lo, hi, now = "2026-09-01T00:00:00.000000000Z", "2026-09-01T23:59:59.999999999Z", "2026-09-08T00:00:00.000000000Z"
	settle := "publications_settlement (settlement_height>? AND settlement_height<?)"
	rowid := "INTEGER PRIMARY KEY (rowid>? AND rowid<?)"
	byRowid := "INTEGER PRIMARY KEY (rowid=?)"
	return []dayPartsPlan{
		// the raw spans of a window, bounded by the ledger's height range
		{"parts: publications of a span", pubCountSQL, []any{lo, hi, 1, 100}, []string{settle}, nil},
		{"parts: readable counts of a span", readableCountSQL(""), []any{now, now, lo, hi, 1, 100}, []string{settle, "probes_promise (promise_hash=?)"}, nil},
		{"parts: readable counts of a span, pinned", readableCountSQL(" AND r.started_at <= ?"), []any{hi, now, hi, now, lo, hi, 1, 100},
			[]string{settle, "probes_promise (promise_hash=?)"}, nil},
		{"parts: signing of a span", signingByValidatorSQL(heightsSQL("p.")), []any{lo, hi, 1, 100}, []string{settle, "SEARCH a USING INDEX"}, nil},
		{"parts: load of a span", loadSpanSQL(""), []any{lo, hi, 1, 100, "{}"}, []string{settle, "SEARCH a USING INDEX"}, []string{"pb", "json_each"}},
		{"parts: what is held now", loadHeldSQL(""), []any{nil, nil, now, "{}"}, []string{"publications_msu (must_serve_until>?)", "SEARCH a USING INDEX"}, []string{"pb", "json_each"}},
		{"parts: gaps of a span", valGapsSQL(""), []any{lo, hi}, []string{"probes_class_time", "sampling_decision_points_started"}, []string{"probe_rows"}},
		{"parts: service times of a span", valLatencyHistSQL(""), []any{lo, hi}, []string{"(classification=? AND started_at>? AND started_at<?)"}, nil},
		{"parts: transfer rates of a span", valThroughputHistSQL(""), []any{lo, hi}, []string{"probes_"}, nil},
		// the ledger, folded by rowid
		{"parts: ledger publications", ledgerPubsSQL, []any{0, 10}, []string{rowid}, nil},
		{"parts: ledger signing", ledgerSigningSQL, []any{0, 10}, []string{rowid, "SEARCH a USING INDEX"}, nil},
		{"parts: ledger load", ledgerLoadSQL, []any{0, 10, "{}"}, []string{rowid, "SEARCH a USING INDEX"}, []string{"pb", "json_each"}},
		{"parts: ledger row span", ledgerRowsSpanSQL, []any{0, 10}, []string{rowid, "probes_promise (promise_hash=?)", "sampling_decision_points_promise (promise_hash=?)"}, []string{"pb"}},
		{"parts: ledger point days", ledgerPointDaysSQL, []any{0, 10}, []string{rowid, "sampling_decision_points_promise (promise_hash=?)"}, nil},
		// a day's span, seal and anchors
		{"parts: a day's span", daySpanSQL, []any{lo, hi, 1, 100}, []string{settle, "probes_promise (promise_hash=?)", "sampling_decision_points_promise (promise_hash=?)"}, []string{"pb"}},
		{"parts: a day's latest deadline", dayMaxPubMSUSQL, []any{lo, hi, 1, 100}, []string{settle}, nil},
		{"parts: a day's readable count", dayReadableSQL, []any{lo, hi, 1, 100}, []string{settle, "probes_promise (promise_hash=?)"}, nil},
		{"parts: a day's deferred verdicts", dayDeferredSQL, []any{lo, hi}, []string{"probes_deferred"}, nil},
		{"parts: a day's collapsible promises", dayCollapsibleSQL, []any{lo, hi}, []string{"probes_sampled_out"}, nil},
		{"parts: probe anchor", anchorProbeSQL, []any{lo, hi}, []string{"probes_started (started_at>? AND started_at<?)"}, nil},
		{"parts: point anchor", anchorPointSQL, []any{lo, hi}, []string{"sampling_decision_points_started (started_at>? AND started_at<?)"}, nil},
		{"parts: heartbeat anchor", anchorBeatSQL, []any{lo, hi, "v1"}, []string{"reachability_started (started_at>? AND started_at<?)"}, nil},
		{"parts: the next day with a row", nextRowSQL, []any{lo, "v1"}, []string{"probes_started (started_at>?)",
			"sampling_decision_points_started (started_at>?)", "reachability_started (started_at>?)"}, nil},
		{"parts: probe anchor still there", `SELECT dedupe_key FROM probes WHERE rowid = ?`, []any{1}, []string{byRowid}, nil},
		{"parts: boundary guard, probes", boundaryGapSQL("probes"), []any{hi, now}, []string{"probes_started (started_at>? AND started_at<?)"}, nil},
		{"parts: boundary guard, points", boundaryGapSQL("sampling_decision_points"), []any{hi, now}, []string{"sampling_decision_points_started (started_at>? AND started_at<?)"}, nil},
		{"parts: boundary guard, heartbeats", boundaryGapSQL("reachability"), []any{hi, now}, []string{"reachability_started (started_at>? AND started_at<?)"}, nil},
		// the catch-up
		{"parts: a key of meta", metaValueSQL, []any{"held_flags_rev"}, []string{"sqlite_autoindex_meta_1 (key=?)"}, nil},
		{"parts: held rows by promise", heldByPromiseSQL, nil, []string{"COVERING INDEX probes_held"}, nil},
		{"parts: held rows of some promises", heldOfPromisesSQL, []any{"[]"}, []string{"probes_held (promise_hash=?)"}, []string{"j"}},
		{"parts: held publications", heldPubsSQL, nil, []string{"publications_held"}, nil},
		{"parts: new rows stored held", newHeldRowsSQL, []any{0, 10}, []string{rowid}, nil},
		{"parts: new publications stored held", newHeldPubsSQL, []any{0, 10}, []string{rowid}, nil},
		{"parts: the days of some promises' rows", promiseRowDaysSQL, []any{"[]"}, []string{"probes_promise (promise_hash=?)"}, []string{"j"}},
		{"parts: corrected publications", correctedPubsSQL, nil, []string{"publications_corrected"}, nil},
		{"parts: publications a correction can reach", reachableSQL, []any{"[]"},
			[]string{"sqlite_autoindex_publications_1 (promise_hash=?)"}, []string{"j", "u"}},
		{"parts: corrected rows of some promises", correctedRowsSQL, []any{"[]"},
			[]string{"probes_promise (promise_hash=?)", "sqlite_autoindex_publications_1 (promise_hash=?)"}, []string{"j"}},
		{"parts: promises of the row corrections' log", correctedPromisesSQL, nil, []string{"COVERING INDEX probe_corrections_promise"}, []string{"probe_corrections"}},
		{"parts: promises of new lines of that log", newCorrectedPromisesSQL, []any{0, 10}, []string{rowid}, nil},
		{"parts: new rows", `SELECT substr(r.started_at, 1, 10), COALESCE(substr(p.settlement_time, 1, 10), ''), MIN(r.started_at), MAX(r.started_at), MAX(r.must_serve_until), MAX(` + weirdSQL("r.started_at") + `)
			FROM probes r LEFT JOIN publications p ON p.promise_hash = r.promise_hash WHERE r.rowid > ? AND r.rowid <= ? GROUP BY 1, 2`, []any{0, 10},
			[]string{rowid, "sqlite_autoindex_publications_1 (promise_hash=?)"}, nil},
		{"parts: newest non-collapsible probe", ladderTables[0].top, nil, []string{"SCAN probes"}, []string{"probes"}},
		{"parts: points of the decisions a correction can reach", `SELECT d.promise_hash, d.vantage, d.must_serve_until, pt.scheduled_at, pt.phase, pt.started_at
			FROM json_each(?) j CROSS JOIN sampling_decisions d ON d.promise_hash = j.value
			JOIN sampling_decision_points pt ON pt.vantage = d.vantage AND pt.promise_hash = d.promise_hash
			ORDER BY d.promise_hash, d.vantage, pt.scheduled_at`, []any{"[]"}, []string{"sampling_decision_points_promise (promise_hash=?)", "sqlite_autoindex_sampling_decisions_1"}, []string{"j"}},
	}
}

// dayPartsPartial are the partial indexes the partials' statements may walk
// whole: each holds only what it is asked for.
var dayPartsPartial = []string{"probes_held", "publications_held", "publications_corrected", "probes_sampled_out", "probes_deferred"}

var _ = store.FullScans
