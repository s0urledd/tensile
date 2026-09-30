package rollup_test

import (
	"context"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/probe"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/rollup"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/verdict"
)

func openStore(t *testing.T) *store.Store {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "rollup.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st
}

// A steady validator's day repeats: the same eight HEALTHY probes today as
// yesterday. Those rolled days must both be counted. They were not, while
// the read grouped by the class JSON: the numeric columns were summed over
// the group and the classes of one day in it were taken once, so the serve
// rate published for that validator on the "all" window fell every time a
// day repeated. Nothing about the rollup may move a figure printed against
// an operator's name.
func TestLoad_RepeatedDailyClassMapIsNotCollapsed(t *testing.T) {
	st := openStore(t)
	db := st.DB()
	ctx := context.Background()
	ins := func(day, addr string, probes, gaps, faults, beats, beatsUp, identityUp int64, classes string) {
		t.Helper()
		if _, err := db.ExecContext(ctx, `INSERT INTO probe_daily
			(day, validator_address, probes, gaps, faults, classes_json, beats, beats_up, identity_up, computed_at)
			VALUES (?,?,?,?,?,?,?,?,?,?)`, day, addr, probes, gaps, faults, classes, beats, beatsUp, identityUp, "2026-01-03T00:00:00Z"); err != nil {
			t.Fatalf("insert %s %s: %v", day, addr, err)
		}
	}
	// v1 repeats itself; v2 does not. v1's certificate lapsed on the second
	// day, which is the case identity_up exists to carry past the prune.
	ins("2026-01-01", "v1", 4, 0, 0, 4, 4, 4, `{"HEALTHY":4}`)
	ins("2026-01-02", "v1", 4, 0, 0, 4, 4, 1, `{"HEALTHY":4}`)
	ins("2026-01-01", "v2", 1, 0, 1, 1, 0, 0, `{"FAULT":1}`)
	ins("2026-01-02", "v2", 2, 0, 2, 2, 0, 0, `{"FAULT":2}`)

	got, err := rollup.Load(ctx, db, time.Date(2026, 1, 3, 0, 0, 0, 0, time.UTC), "")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if got.Probes != 11 {
		t.Fatalf("probes=%d, want 11", got.Probes)
	}
	if got.Classes["HEALTHY"] != 8 || got.Classes["FAULT"] != 3 {
		t.Fatalf("classes=%v, want HEALTHY:8 FAULT:3", got.Classes)
	}
	// The invariant that makes the serve rate readable: the class tally and
	// the probe count are two counts of the same rows.
	var summed int64
	for _, n := range got.Classes {
		summed += n
	}
	if summed != got.Probes {
		t.Fatalf("classes sum to %d but probes is %d", summed, got.Probes)
	}
	if v := got.ProbesByVal["v1"]; v == nil || v.Classes["HEALTHY"] != 8 || v.Probes != 8 {
		t.Fatalf("v1 = %+v, want 8 probes all HEALTHY", v)
	}
	// Endorsement is carried past the prune beside reachability, over the
	// same denominator (BeatsUp), so the two figures on one row cannot end
	// up covering different spans.
	if v := got.ProbesByVal["v1"]; v.IdentityUp != 5 || v.BeatsUp != 8 {
		t.Fatalf("v1 identity_up=%d beats_up=%d, want 5/8", v.IdentityUp, v.BeatsUp)
	}
	if got.IdentityUp != 5 {
		t.Fatalf("network identity_up = %d, want 5", got.IdentityUp)
	}
	if v := got.ProbesByVal["v2"]; v == nil || v.Classes["FAULT"] != 3 || v.Probes != 3 {
		t.Fatalf("v2 = %+v, want 3 probes all FAULT", v)
	}
	if got.Days != 2 {
		t.Fatalf("days = %d, want 2", got.Days)
	}
	// Restricted to one validator, the other's rows are not in the totals.
	one, err := rollup.Load(ctx, db, time.Date(2026, 1, 3, 0, 0, 0, 0, time.UTC), "v1")
	if err != nil {
		t.Fatalf("load one: %v", err)
	}
	if one.Probes != 8 || one.Classes["FAULT"] != 0 || len(one.ProbesByVal) != 1 {
		t.Fatalf("only=v1 gave probes=%d classes=%v vals=%d", one.Probes, one.Classes, len(one.ProbesByVal))
	}
}

// The obligation SQL is a constant, so verdict.EndSegmentDivisor is spelled
// into it by hand. If the Go constant moves and the query does not, the SQL
// and its Go twin would cut the retention window at two different places and
// publish two different serve rates from the same rows — the one divergence
// sentinel-recompute exists to catch, arriving as a silent disagreement
// between two implementations that are supposed to be one.
func TestTheSQLAndTheGoTwinCutTheWindowAtTheSamePoint(t *testing.T) {
	want := "/ " + strconv.FormatFloat(verdict.EndSegmentDivisor, 'f', 1, 64)
	if !strings.Contains(rollup.ObligationBuckets, want) {
		t.Fatalf("verdict.EndSegmentDivisor is %v, so the obligation SQL must divide by %q; it does not:\n%s",
			verdict.EndSegmentDivisor, want, rollup.ObligationBuckets)
	}
}

// Whether the prober missed a request of a reading is asked of that
// reading's rows, sought by (promise_hash, scheduled_at). As a list drawn
// once per statement it walked every assigned in-window row the store
// holds, so every obligation figure paid for the whole retained history;
// and left to the planner, the correlated form seeks probes_window by
// (assigned, phase), which is the same walk once per reading asked. The plan
// is the assertion: a store in a test is too small for either to show.
func TestTheMissedTestReadsOnlyItsReadingsRows(t *testing.T) {
	st := openStore(t)
	const lo, hi = "2026-09-01T00:00:00.000000000Z", "2026-09-02T00:00:00.000000000Z"
	plan, err := st.QueryPlan(context.Background(), `SELECT `+rollup.ObligationSums+` FROM (`+rollup.ObligationBuckets+`)
		GROUP BY validator_address, promise_hash)`, hi, lo, hi, hi, rollup.RowLowerBound(lo))
	if err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(plan, "\n")
	if !strings.Contains(joined, "SEARCH qm USING INDEX probes_promise (promise_hash=? AND scheduled_at=?)") {
		t.Errorf("the missed test does not seek the reading's rows by probes_promise:\n%s", joined)
	}
	for _, step := range plan {
		if strings.Contains(step, " qm ") && strings.Contains(step, "probes_window") {
			t.Errorf("the missed test walks probes_window: %s\nplan:\n%s", step, joined)
		}
	}
	if strings.Contains(joined, "LIST SUBQUERY") {
		t.Errorf("the obligation statement draws a list over the whole store:\n%s", joined)
	}
}

// The hold is applied twice, once in SQL and once in Go, and the two must
// land on the same answer for every row. A disagreement here is worse than
// a wrong answer: sentinel-recompute would report a divergence on every
// held row, and one of the two implementations would be publishing a fault
// the other says it cannot support.
//
// This sweeps the whole cell space rather than a fixture, so a new
// classification or a new outcome is covered without anyone remembering to
// add it.
func TestTheSQLAndTheGoTwinHoldTheSameRows(t *testing.T) {
	st := openStore(t)
	db := st.DB()
	ctx := context.Background()

	type cell struct {
		key  string
		cls  probe.Classification
		out  probe.Outcome
		held bool
	}
	var cells []cell
	i := 0
	for _, c := range probe.AllClassifications {
		for _, o := range probe.AllOutcomes {
			for _, held := range []bool{false, true} {
				i++
				cells = append(cells, cell{key: "k" + strconv.Itoa(i), cls: c, out: o, held: held})
			}
		}
	}
	for _, c := range cells {
		h := 0
		if c.held {
			h = 1
		}
		if _, err := db.ExecContext(ctx, `INSERT INTO probes
			(dedupe_key, vantage, promise_hash, commitment, blob_version, must_serve_until, validator_set_height,
			 validator_address, validator_host, assigned, assigned_row_count, schedule_label, scheduled_at, started_at,
			 finished_at, lateness_ms, dns_ok, dns_ms, tcp_ok, tcp_ms, tls_ok, tls_ms, identity_ok, download_ok,
			 download_ms, rows_returned, rows_expected, commitment_verified, assignment_verified, phase, outcome,
			 classification, total_duration_ms, raw_json, retention_unverified)
			VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
			c.key, "t", "p", "c", 0, "2026-01-01T00:00:00.000000000Z", 1,
			"v", "h", 1, 2, "w1", "2026-01-01T00:00:00.000000000Z", "2026-01-01T00:00:00.000000000Z",
			"2026-01-01T00:00:00.000000000Z", 0, 1, 0, 1, 0, 1, 0, 1, 1,
			0, 2, 2, 1, 1, "in_window", string(c.out),
			string(c.cls), 10, "{}", h); err != nil {
			t.Fatalf("insert %s: %v", c.key, err)
		}
	}

	rows, err := db.QueryContext(ctx, `SELECT dedupe_key, `+rollup.EffectiveClass("")+` FROM probes`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	got := map[string]string{}
	for rows.Next() {
		var k, c string
		if err := rows.Scan(&k, &c); err != nil {
			t.Fatal(err)
		}
		got[k] = c
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(got) != len(cells) {
		t.Fatalf("read %d rows back, inserted %d", len(got), len(cells))
	}
	for _, c := range cells {
		want := verdict.Row{Classification: c.cls, Outcome: c.out, RetentionUnverified: c.held}.EffectiveClass()
		if got[c.key] != string(want) {
			t.Errorf("(%s, %s, held=%v): SQL says %q, the Go twin says %q", c.cls, c.out, c.held, got[c.key], want)
		}
	}
}
