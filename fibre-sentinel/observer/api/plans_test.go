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
		// the namespaces of the rows: the window's settlements, or one
		// publisher's in the window
		c{"publisher namespaces", publisherNamespacesSQL(""), []any{lo, hi}, []string{"payments_kind_time (kind=? AND time>? AND time<?)"}},
		c{"one publisher's namespaces", publisherNamespacesSQL(" AND publisher = ?"), []any{lo, hi, "celestia1x"},
			[]string{"payments_publisher_time (publisher=? AND time>? AND time<?)"}},
		c{"a publisher's namespaces in each span", spanNamespacesSQL, []any{"celestia1x", hi, lo, lo},
			[]string{"payments_publisher_time (publisher=?)"}},
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
	for _, only := range []string{"", " AND a.validator_address = ?4"} {
		args := []any{lo, hi, now}
		if only != "" {
			args = append(args, "ab")
		}
		cases = append(cases, c{"load one pass" + only, loadSQL(only), args,
			[]string{"MATERIALIZE pb", "SCAN pb", "SEARCH a USING INDEX", "(promise_hash=?"}})
		scans["load one pass"+only] = []string{"p", "pb"}
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
		c{"endorsement ledger", ledgerRowsSQL + ` WHERE a.rowid > ? AND a.rowid <= ? AND ` + recentPopulationSQL,
			[]any{0, 10}, []string{"SEARCH a USING INTEGER PRIMARY KEY (rowid>? AND rowid<?)", "SEARCH p USING INDEX sqlite_autoindex_publications_1 (promise_hash=?)"}},
		// The checks a kept ledger passes at a start (derived.go): the rows
		// the file names, sought one by one, never a walk.
		c{"endorsement ledger file", ledgerCheckSQL, []any{"[1,2]"},
			[]string{"SEARCH a USING INTEGER PRIMARY KEY (rowid=?)", "SEARCH p USING INDEX sqlite_autoindex_publications_1 (promise_hash=?)"}},
	)
	scans["endorsement ledger file"] = []string{"j"} // the list of rowids passed in
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
	// The day partials' statements (dayparts_plans_test.go).
	for _, p := range dayPartsPlans() {
		cases = append(cases, c{p.name, p.q, p.args, p.want})
		scans[p.name] = p.scans
	}
	// A blob found by a hash a developer holds (/v1/blobs?commitment=,
	// ?tx=, and the blob ID, which the site turns into its commitment): the
	// page, the selection its verdict cache fingerprints, and the count
	// each seek migration 26's index, never walk publications.
	for _, f := range []struct {
		name, cond, idx string
		args            []any
	}{
		{"blobs by commitment", blobByCommitmentSQL, "publications_commitment (commitment=?)", []any{"ab"}},
		{"blobs by tx", blobByTxSQL, "publications_tx (settlement_tx_hash=?)", []any{"ab", "AB"}},
		// with a namespace beside them, as the route writes it: still the
		// hash's seek, not every blob of the namespace
		{"blobs by commitment in a namespace", blobByCommitmentSQL + " AND " + blobInNamespaceBesideSQL, "publications_commitment (commitment=?)", []any{"ab", "ns"}},
		{"blobs by tx in a namespace", blobByTxSQL + " AND " + blobInNamespaceBesideSQL, "publications_tx (settlement_tx_hash=?)", []any{"ab", "AB", "ns"}},
	} {
		cases = append(cases,
			c{f.name + " page", blobRowsSQL(f.cond, blobPageDefault, 0), f.args, []string{f.idx}},
			c{f.name + " selection", blobSelAt(f.cond, blobPageDefault+1, 0) + `SELECT promise_hash FROM sel`, f.args, []string{f.idx}},
			c{f.name + " count", `SELECT COUNT(*) FROM publications WHERE ` + f.cond, f.args, []string{f.idx}},
		)
		scans[f.name+" selection"] = []string{"sel"}
	}
	// A transaction's newest failed inclusion, asked on every lookup by a
	// hash no publication carries: the hash sought in migration 30's index,
	// which gives the order too (no sort, checked below).
	cases = append(cases, c{"failed tx by hash", failedTxByHashSQL, []any{"ab"}, []string{"failed_txs_tx (tx_hash=?)"}})
	// The transaction page, the blob page's tx_cost and a validator's
	// endpoint history (txs.go, endpoint_history.go): each a seek of
	// migration 31's indexes or a primary key, the order read from the
	// index, never a walk and never a sort (checked below).
	noSort := map[string]bool{"failed tx by hash": true}
	for _, tc := range []c{
		{"tx cost by hash", txCostByHashSQL, []any{"ab"}, []string{"USING INDEX tx_costs_tx (tx_hash=?)"}},
		{"tx cost at a settlement", txCostAtSQL, []any{1, 0}, []string{"USING INDEX sqlite_autoindex_tx_costs_1 (height=? AND tx_index=?)"}},
		{"payments by tx", paymentsByTxSQL, []any{"ab"}, []string{"USING INDEX payments_tx (tx_hash=?)"}},
		{"host events at a tx", hostEventsAtSQL, []any{1, 1}, []string{"USING INDEX host_events_at (from_height=? AND from_tx_index=?)"}},
		{"host walk", hostWalkSQL, []any{"ab"},
			[]string{"USING INDEX sqlite_autoindex_host_events_1 (cons_address=?)", "USING INDEX sqlite_autoindex_tx_costs_1 (height=? AND tx_index=?)"}},
		{"failed set-hosts of an operator", failedSetHostsSQL, []any{"celestiavaloper1x"},
			[]string{"USING PRIMARY KEY (account=?)", "USING INDEX sqlite_autoindex_failed_txs_1 (dedupe_key=?)"}},
	} {
		cases, noSort[tc.name] = append(cases, tc), true
	}
	// The Blobs list's failed blob payments (failedblobs.go): failed_txs
	// read on from a rowid, sought by the rowid in its order, its top from
	// the rowid b-tree's last entry; and the blobs before a failure, a
	// bounded range of publications_settlement, never a walk.
	cases = append(cases,
		c{"failed payments' top", failedTxsTopSQL, nil, []string{"SEARCH failed_txs"}},
		c{"failed payments since", failedTxsSinceSQL, []any{0, 10}, []string{"SEARCH failed_txs USING INTEGER PRIMARY KEY (rowid>? AND rowid<?)"}},
		c{"blobs before a failed payment", blobsBeforeSQL(""), []any{1, 1, 0, 25}, []string{"SEARCH publications USING INDEX publications_settlement (settlement_height>?)"}},
		c{"blobs before a failed payment in a namespace", blobsBeforeSQL(blobInNamespaceSQL), []any{"ns", 1, 1, 0, 25}, []string{"SEARCH publications USING INDEX"}},
	)
	noSort["failed payments since"] = true
	// A publisher's transactions (pubtxs.go): its counts and sums, a page of
	// its successes and the successes before a failure, each a range of
	// payments_publisher_time whose order gives the page's (only one
	// block's rows are sorted among themselves, never the whole range); its
	// failed deposits and withdrawal requests from failed_tx_msgs' primary
	// key in its order; a request's queue row by its primary key.
	cases = append(cases,
		c{"publisher txs sums", pubTxSumsSQL, []any{"celestia1x"}, []string{"SEARCH payments USING INDEX payments_publisher_time (publisher=?)"}},
		c{"publisher failed escrow txs", pubTxFailedMsgsSQL, []any{"celestia1x", "a", "b"},
			[]string{"USING PRIMARY KEY (account=?)", "USING INDEX sqlite_autoindex_failed_txs_1 (dedupe_key=?)"}},
		c{"a withdrawal request's queue row", pubTxQueueSQL, []any{"celestia1x", lo},
			[]string{"USING INDEX sqlite_autoindex_withdrawal_queue_1 (publisher=? AND requested_at=?)"}},
	)
	noSort["publisher failed escrow txs"] = true
	for _, v := range []struct{ name, kinds string }{{"all", pubTxKindsAll}, {"escrow", pubTxKindsEscrow}} {
		cases = append(cases,
			c{"publisher txs page " + v.name, pubTxPageSQL(v.kinds), []any{"celestia1x", 25, 0},
				[]string{"SEARCH payments USING INDEX payments_publisher_time (publisher=?)"}},
			c{"publisher txs before a failure " + v.name, pubTxBeforeSQL(v.kinds), []any{"celestia1x", lo, 1, 1, 0, 25},
				[]string{"SEARCH payments USING INDEX payments_publisher_time (publisher=? AND time>?)"}},
		)
		noSort["publisher txs page "+v.name] = true
	}
	// The tip's newest blob, asked up to four times a second whoever is
	// reading: the highest height from the index's last entry, then that
	// block's rows, never a walk of publications.
	cases = append(cases, c{"tip's newest blob", latestBlobSQL, nil,
		[]string{"SEARCH publications USING INDEX publications_settlement (settlement_height=?)", "COVERING INDEX publications_settlement"}})
	// /v1/probes by a class alone: the stored class beside the published
	// one, sought newest first through probes_class_time, never the table
	// walked for a class few rows carry (probeClassConds); served=no alone
	// over its span (probeScanSpan), never the whole table.
	byClass, classArgs := probeClassConds("IDENTITY_MISMATCH", false)
	cases = append(cases,
		c{"probes by class", probeRowsSQL(strings.Join(byClass, " AND "), 100, false), classArgs,
			[]string{"probes_class_time (classification=?)"}},
		c{"probes by class since", probeRowsSQL(strings.Join(append([]string{`started_at >= ?`}, byClass...), " AND "), 100, false),
			append([]any{lo}, classArgs...), []string{"probes_class_time (classification=? AND started_at>?)"}},
		c{"probes not served", probeRowsSQL(rollup.NotServedSQL("probes")+` AND started_at >= ?`, 100, false), []any{lo},
			[]string{"probes_started (started_at>?)"}},
	)
	for _, tc := range cases {
		plan, err := st.QueryPlan(ctx, tc.q, tc.args...)
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		joined := strings.Join(plan, "\n")
		// probe_rows is the co-routine a UNION ALL view runs as: its arms are
		// the plan steps above it, and those are what must not walk a table.
		if bad := store.FullScans(plan, append([]string{"v", "m", "w", "vp", "vr", "probe_rows"}, scans[tc.name]...), append(append([]string{}, apiPartial...), dayPartsPartial...)); len(bad) > 0 {
			t.Errorf("%s walks a whole table or index: %v\nplan:\n%s", tc.name, bad, joined)
		}
		for _, w := range tc.want {
			if !strings.Contains(joined, w) {
				t.Errorf("%s: plan does not use %q\nplan:\n%s", tc.name, w, joined)
			}
		}
		if strings.Contains(joined, "TEMP B-TREE FOR ORDER BY") && (strings.HasPrefix(tc.name, "latest answer") || noSort[tc.name]) {
			t.Errorf("%s sorts instead of reading the index in rowid order\nplan:\n%s", tc.name, joined)
		}
	}
}
