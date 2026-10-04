package rollup_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/observer/api"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/rollup"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
)

// partsAfter holds a test's store to the API's day partials as the clock
// moves on past its fixtures, two hours, fifteen days (after the rollup)
// and ninety-five (long after it): the longer windows computed from the
// partials must be what the shipped statements compute
// (api.CompareDayParts).
func partsAfter(t *testing.T, st *store.Store) {
	t.Helper()
	ctx := context.Background()
	var last sql.NullString
	if err := st.DB().QueryRow(`SELECT MAX(t) FROM (SELECT MAX(started_at) AS t FROM probes UNION ALL SELECT MAX(settlement_time) FROM publications)`).Scan(&last); err != nil {
		t.Fatal(err)
	}
	base, err := time.Parse(store.TimeLayout, last.String)
	if err != nil {
		t.Fatalf("the store's newest moment %q: %v", last.String, err)
	}
	for _, off := range []time.Duration{2 * time.Hour, 15 * 24 * time.Hour, 95 * 24 * time.Hour} {
		now := base.Add(off)
		if off >= 15*24*time.Hour {
			if _, err := rollup.Run(ctx, st, now, rollup.Default()); err != nil {
				t.Fatal(err)
			}
		}
		n, diffs, err := api.CompareDayParts(ctx, st, "test", now)
		if err != nil {
			t.Fatal(err)
		}
		for _, d := range diffs {
			t.Errorf("+%s: the partials differ from the shipped statements: %s", off, d)
		}
		if n == 0 {
			t.Errorf("+%s: nothing compared", off)
		}
	}
}
