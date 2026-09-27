package rollup_test

import (
	"context"
	"strconv"
	"strings"
	"testing"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/probe"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/rollup"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/verdict"
)

// The SQL IN lists are the Go lists, spelled out because a query fragment is
// a constant.
func TestEndReadListsMatchTheGoLists(t *testing.T) {
	sqlList := func(cs []probe.Classification) string {
		q := make([]string, len(cs))
		for i, c := range cs {
			q[i] = "'" + string(c) + "'"
		}
		return "(" + strings.Join(q, ",") + ")"
	}
	if got := sqlList(probe.EndNoRowsClasses); got != rollup.EndNoRowsSQL {
		t.Errorf("EndNoRowsSQL = %s, the Go list is %s", rollup.EndNoRowsSQL, got)
	}
	if got := sqlList(probe.EndGenuineRowsClasses); got != rollup.EndGenuineRowsSQL {
		t.Errorf("EndGenuineRowsSQL = %s, the Go list is %s", rollup.EndGenuineRowsSQL, got)
	}
}

// The obligation class of every (classification, outcome, held, label) cell
// is the same in SQL and in the Go twin, at the end-of-window reading and at
// an earlier schedule's point alike.
func TestTheSQLAndTheGoTwinCountEndReadingsTheSame(t *testing.T) {
	st := openStore(t)
	db := st.DB()
	ctx := context.Background()

	type cell struct {
		key   string
		cls   probe.Classification
		out   probe.Outcome
		held  bool
		label string
	}
	var cells []cell
	i := 0
	for _, label := range []string{"w4", probe.EndReadLabel} {
		for _, c := range probe.AllClassifications {
			for _, o := range probe.AllOutcomes {
				for _, held := range []bool{false, true} {
					i++
					cells = append(cells, cell{key: "k" + strconv.Itoa(i), cls: c, out: o, held: held, label: label})
				}
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
			"v", "h", 1, 2, c.label, "2026-01-01T00:00:00.000000000Z", "2026-01-01T00:00:00.000000000Z",
			"2026-01-01T00:00:00.000000000Z", 0, 1, 0, 1, 0, 1, 0, 1, 1,
			0, 2, 2, 1, 1, "in_window", string(c.out),
			string(c.cls), 10, "{}", h); err != nil {
			t.Fatalf("insert %s: %v", c.key, err)
		}
	}

	rows, err := db.QueryContext(ctx, `SELECT dedupe_key, `+rollup.ObligationClass("")+` FROM probes`)
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
		want := verdict.Row{Classification: c.cls, Outcome: c.out, RetentionUnverified: c.held, ScheduleLabel: c.label}.ObligationClass()
		if got[c.key] != string(want) {
			t.Errorf("(%s, %s, held=%v, %s): SQL says %q, the Go twin says %q", c.cls, c.out, c.held, c.label, got[c.key], want)
		}
	}
}
