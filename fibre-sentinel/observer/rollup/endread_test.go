package rollup_test

import (
	"context"
	"strconv"
	"testing"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/probe"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/rollup"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/verdict"
)

// The counted class of every cell of (classification, outcome, held,
// verified, endorsed, phase) is the same in SQL and in the Go twin. Each
// row is a reading of its own, of a blob whose record is not stored, so
// the reading is neither Available nor Unavailable: the fixture in
// counted_test.go covers the readings that are.
func TestTheSQLAndTheGoTwinCountEveryCellTheSame(t *testing.T) {
	st := openStore(t)
	db := st.DB()
	ctx := context.Background()

	type cell struct {
		key                string
		cls                probe.Classification
		out                probe.Outcome
		held, verified     bool
		assigned, attested bool
		phase              probe.Phase
	}
	var cells []cell
	i := 0
	for _, phase := range []probe.Phase{probe.PhaseInWindow, probe.PhaseGrace} {
		for _, who := range [][2]bool{{true, true}, {true, false}, {false, false}} {
			for _, c := range probe.AllClassifications {
				for _, o := range probe.AllOutcomes {
					for _, held := range []bool{false, true} {
						for _, verified := range []bool{false, true} {
							i++
							cells = append(cells, cell{key: "k" + strconv.Itoa(i), cls: c, out: o, held: held, verified: verified,
								assigned: who[0], attested: who[1], phase: phase})
						}
					}
				}
			}
		}
	}
	b := func(v bool) int {
		if v {
			return 1
		}
		return 0
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range cells {
		if _, err := tx.ExecContext(ctx, `INSERT INTO probes
			(dedupe_key, vantage, promise_hash, commitment, blob_version, must_serve_until, validator_set_height,
			 validator_address, validator_host, assigned, attested, assigned_row_count, schedule_label, scheduled_at, started_at,
			 finished_at, lateness_ms, dns_ok, dns_ms, tcp_ok, tcp_ms, tls_ok, tls_ms, identity_ok, download_ok,
			 download_ms, rows_returned, rows_expected, commitment_verified, assignment_verified, phase, outcome,
			 classification, total_duration_ms, raw_json, retention_unverified)
			VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
			c.key, "t", c.key, "c", 0, "2026-01-01T00:00:00.000000000Z", 1,
			"v", "h", b(c.assigned), b(c.attested), 2, probe.EndReadLabel, "2026-01-01T00:00:00.000000000Z", "2026-01-01T00:00:00.000000000Z",
			"2026-01-01T00:00:00.000000000Z", 0, 1, 0, 1, 0, 1, 0, 1, 1,
			0, 2, 2, b(c.verified), b(c.verified), string(c.phase), string(c.out),
			string(c.cls), 10, "{}", b(c.held)); err != nil {
			t.Fatalf("insert %s: %v", c.key, err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	rows, err := db.QueryContext(ctx, `SELECT dedupe_key, `+rollup.CountedClass("probes")+` FROM probes`)
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
		r := verdict.Row{PromiseHash: c.key, Validator: "v", ScheduleLabel: probe.EndReadLabel, Classification: c.cls, Outcome: c.out,
			RetentionUnverified: c.held, CommitmentVerified: c.verified, Assigned: c.assigned, Attested: c.attested, Phase: c.phase,
			RowsReturned: 2, AssignedRowCount: 2}
		want := r.CountedClass(verdict.ReadingOf([]verdict.Row{r}, verdict.BlobFacts{}))
		if got[c.key] != string(want) {
			t.Errorf("(%s, %s, held=%v, verified=%v, assigned=%v, attested=%v, %s): SQL says %q, the Go twin says %q",
				c.cls, c.out, c.held, c.verified, c.assigned, c.attested, c.phase, got[c.key], want)
		}
	}
}
