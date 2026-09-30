package api

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/observer/rollup"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
)

// PartsAfter holds a parity test's store to the day partials as the clock
// moves on past its fixtures: two hours after base (the days not over
// yet), fifteen days (after the rollup, every day sealed) and ninety-five
// (after the prune, the "all" window resting on the rollup). At each the
// longer windows of the network and the validator list are computed from
// the partials and with the shipped statements (CompareDayParts), and
// must not differ. A zero base is the newest moment the store holds.
func PartsAfter(t testing.TB, st *store.Store, vantage string, base time.Time) {
	t.Helper()
	ctx := context.Background()
	if base.IsZero() {
		var last sql.NullString
		if err := st.DB().QueryRow(`SELECT MAX(t) FROM (SELECT MAX(started_at) AS t FROM probes UNION ALL SELECT MAX(settlement_time) FROM publications)`).Scan(&last); err != nil {
			t.Fatal(err)
		}
		if b, err := time.Parse(store.TimeLayout, last.String); err == nil {
			base = b
		} else {
			base = time.Now()
		}
	}
	for _, step := range []struct {
		name string
		off  time.Duration
	}{{"+2h", 2 * time.Hour}, {"+15d", 15 * 24 * time.Hour}, {"+95d", 95 * 24 * time.Hour}} {
		now := base.Add(step.off)
		if step.off >= 15*24*time.Hour {
			if _, err := rollup.Run(ctx, st, now, rollup.Default()); err != nil {
				t.Fatalf("%s: rollup: %v", step.name, err)
			}
		}
		n, diffs, err := CompareDayParts(ctx, st, vantage, now)
		if err != nil {
			t.Fatalf("%s: %v", step.name, err)
		}
		for _, d := range diffs {
			t.Errorf("%s: the partials differ from the shipped statements: %s", step.name, d)
		}
		if n == 0 {
			t.Errorf("%s: nothing compared", step.name)
		}
	}
}
