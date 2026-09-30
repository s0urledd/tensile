package api

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/plsgiveup/fibre/fibre-sentinel/observer/rollup"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
)

// apiPartial are partial indexes a plan may walk whole: each holds only the
// rows its query wants.
var apiPartial = []string{"publications_unassignable", "probes_cleared"}

// TestHotQueriesUseIndexes pins the plans of the queries the API runs per
// request or per snapshot refresh that used to walk a whole table: the
// latest reachability answer per validator, the newest assignment per
// validator, the vantage count, the unassignable count, the publisher list,
// and the effective-class tallies, which migration 19 had pushed off their
// covering indexes. A plan regression here is invisible on a test store and
// costs seconds per request on a real one, so the plan is the assertion.
func TestHotQueriesUseIndexes(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "o.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()

	cls := rollup.EffectiveClass("")
	const lo, hi = "2026-09-01T00:00:00.000Z", "2026-09-08T00:00:00.000Z"
	type c struct {
		name string
		q    string
		args []any
		want []string
	}
	var cases []c
	// scans names, per case, tables it may walk whole; the reason is on the
	// cases that fill it.
	scans := map[string][]string{}
	for _, tb := range []struct{ table, ok, vantage, idx string }{
		{"reachability", `outcome <> 'PROBE_ERROR'`, "ut-1", "reachability_latest_answer"},
		{"probes", probeAnswerSQL, "", "probes_latest_answer"},
	} {
		for _, v := range []struct{ only, asOf string }{{"", ""}, {"ab", ""}, {"", hi}, {"ab", hi}} {
			q, args := latestAnswerSQL(tb.table, tb.ok, "x", tb.vantage, v.only, v.asOf)
			cases = append(cases, c{"latest answer " + tb.table + " only=" + v.only + " asOf=" + v.asOf, q, args,
				[]string{"USING INDEX " + tb.idx + " (validator_address=?)"}})
		}
	}
	for _, only := range []string{"", "ab"} {
		q, args := latestAssignmentSQL(only)
		cases = append(cases, c{"latest assignment only=" + only, q, args,
			[]string{"assignments_validator_height (validator_address=?)"}})
	}
	cases = append(cases,
		c{"vantage count", vantageCountSQL, nil, []string{"COVERING INDEX probes_vantage", "COVERING INDEX reachability_vantage"}},
		c{"recent vantages", recentVantagesSQL, nil, []string{"COVERING INDEX reachability_vantage", "reachability_vantage (vantage=?)"}},
		c{"other vantage's check", otherVantageSQL, []any{"ab", lo, hi, "ut-1", "h:7980"},
			[]string{"reachability_validator_time (validator_address=? AND started_at>? AND started_at<?)"}},
		c{"own heartbeats over a window", `SELECT COUNT(*) FROM reachability WHERE started_at >= ? AND started_at <= ? AND outcome <> 'PROBE_ERROR' AND +vantage = ?`,
			[]any{lo, hi, "ut-1"}, []string{"reachability_started (started_at>? AND started_at<?)"}},
		c{"unassignable", `SELECT COUNT(*) FROM publications WHERE assignment_error != ''`, nil,
			[]string{"publications_unassignable"}},
		c{"unassignable recent", `SELECT COUNT(*) FROM publications WHERE settlement_height >= ? AND settlement_time >= ? AND assignment_error != ''`,
			[]any{1, lo}, []string{"publications_unassignable (settlement_height>?)"}},
		c{"publishers", publisherRowsSQL(""), []any{lo, hi}, []string{"payments_time (time>? AND time<?)", "payments_publisher_time (publisher=?)"}},
		c{"publisher", publisherRowsSQL(" AND p.publisher = ?"), []any{lo, hi, "celestia1x"}, []string{"payments_publisher_time (publisher=?"}},
		c{"class tally", `SELECT ` + cls + `, COUNT(*) FROM probes WHERE started_at >= ? AND started_at <= ? AND assigned = 1 AND phase = 'in_window' GROUP BY 1`,
			[]any{lo, hi}, []string{"COVERING INDEX probes_"}},
		c{"per-validator class tally", `SELECT validator_address, ` + cls + `, COUNT(*) FROM probes WHERE started_at >= ? AND started_at <= ? AND assigned = 1 AND phase = 'in_window' GROUP BY 1, 2`,
			[]any{lo, hi}, []string{"COVERING INDEX probes_"}},
		c{"faults per validator", `SELECT validator_address, COUNT(*) FROM probes WHERE started_at >= ? AND started_at <= ? AND assigned = 1 AND ` + cls + ` = 'FAULT' GROUP BY validator_address`,
			[]any{lo, hi}, []string{"COVERING INDEX probes_"}},
		c{"one validator's tally", `SELECT ` + cls + `, COUNT(*) FROM probes WHERE validator_address = ? AND assigned = 1 AND phase = 'in_window' AND started_at >= ? AND started_at <= ? GROUP BY 1`,
			[]any{"ab", lo, hi}, []string{"COVERING INDEX probes_validator_window"}},
		// The same tallies over probe_rows, which is what the figures read: the
		// stored rows keep their covering indexes, and the rows a sampled-out
		// decision stands for are sought from its points by the same bound,
		// never by walking every decision or every assignment of a validator.
		c{"class tally with sampled-out rows", `SELECT ` + cls + `, COUNT(*) FROM probe_rows WHERE started_at >= ? AND started_at <= ? AND assigned = 1 AND phase = 'in_window' GROUP BY 1`,
			[]any{lo, hi}, []string{"COVERING INDEX probes_", "sampling_decision_points_started (started_at>? AND started_at<?)"}},
		c{"one validator's tally with sampled-out rows", `SELECT ` + cls + `, COUNT(*) FROM probe_rows WHERE validator_address = ? AND assigned = 1 AND phase = 'in_window' AND started_at >= ? AND started_at <= ? GROUP BY 1`,
			[]any{"ab", lo, hi}, []string{"COVERING INDEX probes_validator_window", "sampling_decision_points_started (started_at>? AND started_at<?)"}},
		c{"one blob's rows with sampled-out rows", `SELECT ` + cls + `, COUNT(*) FROM probe_rows WHERE promise_hash = ? GROUP BY 1`,
			[]any{"ab"}, []string{"probes_promise (promise_hash=?)", "sampling_decision_points_promise (promise_hash=?)"}},
		c{"one point's rows with sampled-out rows", `SELECT COUNT(*) FROM probe_rows WHERE scheduled_at = ?`,
			[]any{lo}, []string{"probes_scheduled (scheduled_at=?)", "sampling_decision_points_scheduled (scheduled_at=?)"}},
	)
	// The per-snapshot statements the validator list and the network summary
	// read chain records with. Each may walk publications whole: nothing
	// indexes settlement_time, and a walk that reads each row's local payload
	// and never its raw_json costs about a millisecond per thousand
	// publications. What they must not do is walk assignments, which grow by
	// one row per validator per blob and are never pruned: the assignments
	// are sought from the publications selected, by primary key.
	const now = "2026-09-08T00:00:00.000Z"
	for _, only := range []string{"", " AND a.validator_address = ?5"} {
		args := []any{lo, hi, now, "{}"}
		if only != "" {
			args = append(args, "ab")
		}
		cases = append(cases, c{"load one pass" + only, loadSQL(only), args,
			[]string{"MATERIALIZE pb", "SCAN pb", "SEARCH a USING INDEX", "(promise_hash=?"}})
		scans["load one pass"+only] = []string{"p", "json_each", "pb"}
	}
	for _, only := range []string{"", " AND a.validator_address = ?"} {
		args := []any{lo, hi}
		if only != "" {
			args = append(args, "ab")
		}
		cases = append(cases, c{"signing by validator" + only, signingByValidatorSQL(only), args,
			[]string{"SCAN p", "SEARCH a USING INDEX", "(promise_hash=?"}})
		scans["signing by validator"+only] = []string{"p"}
	}
	cases = append(cases,
		c{"load memo candidates", `SELECT p.promise_hash FROM publications p WHERE ` + loadPopulationSQL, []any{lo, hi, now},
			[]string{"SCAN p"}},
		c{"load memo lookup", `SELECT p.promise_hash, ` + originalRowsSQL + ` FROM json_each(?) j JOIN publications p ON p.promise_hash = j.value`, []any{"[]"},
			[]string{"SEARCH p USING INDEX sqlite_autoindex_publications_1 (promise_hash=?)"}},
		c{"endorsement ledger", ledgerRowsSQL + ` WHERE a.rowid > ? AND a.rowid <= ? AND ` + recentPopulationSQL,
			[]any{0, 10}, []string{"SEARCH a USING INTEGER PRIMARY KEY (rowid>? AND rowid<?)", "SEARCH p USING INDEX sqlite_autoindex_publications_1 (promise_hash=?)"}},
		// The checks a kept memo or ledger passes at a start (derived.go):
		// the rows the file names, sought one by one, never a walk.
		c{"endorsement ledger file", ledgerCheckSQL, []any{"[1,2]"},
			[]string{"SEARCH a USING INTEGER PRIMARY KEY (rowid=?)", "SEARCH p USING INDEX sqlite_autoindex_publications_1 (promise_hash=?)"}},
		c{"memo file", memoCheckSQL, []any{10, memoChecked}, []string{"SEARCH p USING INTEGER PRIMARY KEY (rowid<?)"}},
	)
	scans["endorsement ledger file"] = []string{"j"} // the list of rowids passed in
	scans["load memo candidates"] = []string{"p"}
	scans["load memo lookup"] = []string{"j"} // the list of hashes passed in
	// The window's publications still waiting for a reading
	// (reconstructableCount). Publications are walked whole, as nothing
	// indexes settlement_time; the readings of each are sought by its hash.
	cases = append(cases,
		c{"not yet read", `SELECT COUNT(*) FROM publications WHERE settlement_time >= ? AND settlement_time <= ? AND NOT ` + readableSQL(""), []any{lo, hi},
			[]string{"probes_promise (promise_hash=?)"}},
		c{"not yet read, pinned", `SELECT COUNT(*) FROM publications WHERE settlement_time >= ? AND settlement_time <= ? AND NOT ` + readableSQL(" AND r.started_at <= ?"),
			[]any{lo, hi, hi}, []string{"probes_promise (promise_hash=?)"}},
	)
	scans["not yet read"] = []string{"publications"}
	scans["not yet read, pinned"] = []string{"publications"}
	for _, tc := range cases {
		plan, err := st.QueryPlan(ctx, tc.q, tc.args...)
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		joined := strings.Join(plan, "\n")
		// probe_rows is the co-routine a UNION ALL view runs as: its arms are
		// the plan steps above it, and those are what must not walk a table.
		if bad := store.FullScans(plan, append([]string{"v", "m", "w", "vp", "vr", "probe_rows"}, scans[tc.name]...), apiPartial); len(bad) > 0 {
			t.Errorf("%s walks a whole table or index: %v\nplan:\n%s", tc.name, bad, joined)
		}
		for _, w := range tc.want {
			if !strings.Contains(joined, w) {
				t.Errorf("%s: plan does not use %q\nplan:\n%s", tc.name, w, joined)
			}
		}
		if strings.Contains(joined, "TEMP B-TREE FOR ORDER BY") && strings.HasPrefix(tc.name, "latest answer") {
			t.Errorf("%s sorts instead of reading the index in rowid order\nplan:\n%s", tc.name, joined)
		}
	}
}
