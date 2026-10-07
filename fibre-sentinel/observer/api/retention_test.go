package api_test

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/observer/rollup"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
)

type allSnapshot struct {
	Obligations obligationsJSON          `json:"obligations"`
	ServeRate   struct{ Num, Den int64 } `json:"serve_rate"`
	Coverage    struct{ Num, Den int64 } `json:"serve_rate_coverage"`
	HeldOut     map[string]int64         `json:"serve_rate_held_out"`
	Attestation struct {
		Attested   int64 `json:"attested_probes"`
		Unattested int64 `json:"unattested_probes"`
		Unknown    int64 `json:"unknown_probes"`
	} `json:"attestation"`
	ReachWindow struct{ Num, Den int64 } `json:"reachability_window"`
	RolledUp    *struct {
		RawFrom string `json:"raw_from"`
		Days    int64  `json:"days"`
		Note    string `json:"note"`
	} `json:"rolled_up"`
}

// tallies is what the "all" window keeps and does not publish: the
// reading tallies of the summary and of each validator's row.
type tallies struct {
	Classes    map[string]int64 `json:"classes"`
	ProbeCount int64            `json:"probe_count"`
	Gaps       int64            `json:"probe_gaps"`
	Validators int64            `json:"validators_probed"`
	Rows       []struct {
		Address    string           `json:"address"`
		Classes    map[string]int64 `json:"classes"`
		ProbeCount int64            `json:"probe_count"`
	} `json:"validators"`
}

func talliesOf(t *testing.T, st *store.Store) tallies {
	t.Helper()
	var out tallies
	networkOf(t, st, "test", "all", time.Time{}, &out)
	rowsOf(t, st, "test", "all", time.Time{}, &out)
	return out
}

type valSnapshot struct {
	Address     string                   `json:"address"`
	Obligations obligationsJSON          `json:"obligations"`
	Reach       struct{ Num, Den int64 } `json:"reachability_window"`
}

// A database pruned before 2026-10-04, when the prune was retired, answers
// the "all" window from the daily rollup for the pruned days plus the raw
// rows: rolling a day up and pruning it must leave every "all" figure
// exactly as it was, now labelled, and stripping raw_json must leave every
// typed figure in place. The prune is pruneLikeBefore here; the observer no
// longer runs one.
func TestRollupAndPruneKeepTheAllWindow(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "observer.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	now := time.Now().UTC().Truncate(time.Second)
	// an old day, 40 days back: two publications
	old := now.Add(-40 * 24 * time.Hour).Truncate(24 * time.Hour).Add(10 * time.Hour)
	hexAddr := strings.Repeat("ab", 20) // the detail endpoint takes a real address
	insertProbeSet(t, st, "old1", old, old.Add(30*time.Minute), map[string][]wire{
		"served": {ok, ok, ok, ok}, "endun": {ok, err500, err500, err500}, "broken": {ok, gone, ok, ok},
		"unreach": {refused, refused, refused, refused}, "backoff": {skipped, skipped, skipped, skipped},
		hexAddr: {ok, ok, gone, ok},
	})
	insertProbeSet(t, st, "old2", old.Add(2*time.Hour), old.Add(3*time.Hour), map[string][]wire{
		"served": {refused, ok}, "endun": {refused, ok}, "broken": {refused, ok}, "unreach": {ok, ok},
	})
	// a publication settled eight minutes before the end of the last day the
	// prune will take, probed into the first retained day: its obligations
	// belong to the rolled day (settlement day) while three of its rows
	// start on a raw day. The rollup counts it once; the raw part must not
	// count it again. Eight minutes, because the first in-window probe of a
	// thirty-minute window falls 3.6 minutes in and has to land on the day
	// being pruned for the seam to be a seam.
	seam := now.Add(-31 * 24 * time.Hour).Truncate(24 * time.Hour).Add(23*time.Hour + 52*time.Minute)
	insertProbeSet(t, st, "seam", seam, seam.Add(30*time.Minute), map[string][]wire{
		"served": {ok, ok, ok, ok}, "broken": {ok, gone, ok, ok}, hexAddr: {ok, ok, ok, ok},
	})
	// a recent publication, inside every retention
	recent := now.Add(-2 * time.Hour)
	insertProbeSet(t, st, "new1", recent, now.Add(-30*time.Minute), map[string][]wire{
		"served": {ok, ok, ok, ok}, "broken": {ok, gone, ok, ok}, hexAddr: {ok, ok, ok, ok},
	})
	ts := httptestServer(t, st)

	var before allSnapshot
	get(t, ts, "/v1/network?window=all", &before)
	if before.RolledUp != nil {
		t.Fatalf("nothing pruned yet, but labelled: %+v", before.RolledUp)
	}
	if before.Obligations.Total != 16 || before.Obligations.Broken == 0 || before.Obligations.Served == 0 {
		t.Fatalf("fixture: %+v", before)
	}
	var beforeVals struct {
		Validators []valSnapshot `json:"validators"`
	}
	get(t, ts, "/v1/validators?window=all", &beforeVals)
	beforeTallies := talliesOf(t, st)

	// roll up (14 days after the day), then prune as the observer did before
	// 2026-10-04: rows older than 30 days, raw_json older than 7
	rep, err := rollup.Run(context.Background(), st, now, rollup.Config{RollupAfter: 14 * 24 * time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	if len(rep.RolledDays) == 0 || rep.PendingAtRoll != 0 {
		t.Fatalf("report = %+v", rep)
	}
	prunedDays, prunedRows, stripped := pruneLikeBefore(t, st, now, 30*24*time.Hour, 7*24*time.Hour)
	if prunedRows == 0 || prunedDays[0] != old.Format("2006-01-02") {
		t.Fatalf("pruned %d rows over %v", prunedRows, prunedDays)
	}
	var left int64
	_ = st.DB().QueryRow(`SELECT COUNT(*) FROM probes WHERE promise_hash IN ('old1','old2')`).Scan(&left)
	if left != 0 {
		t.Fatalf("%d old rows survived the prune", left)
	}
	// the seam publication's rows that started on the first retained day
	// survive (rows are pruned by the day they started), its earlier row
	// is gone
	_ = st.DB().QueryRow(`SELECT COUNT(*) FROM probes WHERE promise_hash = 'seam'`).Scan(&left)
	if left != 9 {
		t.Fatalf("%d seam rows survived the prune, want the 9 that started on the retained day", left)
	}
	if stripped == 0 {
		t.Errorf("no raw_json was dropped (rows older than the 7-day retention existed)")
	}
	from, ok := rollup.RawFrom(st)
	if !ok || !from.After(old) {
		t.Fatalf("raw_from = %v %v", from, ok)
	}

	// the API restarts its snapshots: use a fresh server
	ts2 := httptestServer(t, st)
	var after allSnapshot
	get(t, ts2, "/v1/network?window=all", &after)
	if after.RolledUp == nil || after.RolledUp.RawFrom != from.Format("2006-01-02") || after.RolledUp.Days == 0 || after.RolledUp.Note == "" {
		t.Fatalf("not labelled: %+v", after.RolledUp)
	}
	if after.Obligations != before.Obligations {
		t.Errorf("obligations changed:\nbefore %+v\nafter  %+v", before.Obligations, after.Obligations)
	}
	if after.ServeRate != before.ServeRate || after.ReachWindow != before.ReachWindow {
		t.Errorf("rates changed: serve %v -> %v, reach %v -> %v", before.ServeRate, after.ServeRate, before.ReachWindow, after.ReachWindow)
	}
	// the tallies, which the summary and the rows keep, fold the rollup in
	// the same way
	afterTallies := talliesOf(t, st)
	if bt, at := beforeTallies, afterTallies; bt.ProbeCount != at.ProbeCount || bt.Gaps != at.Gaps || bt.Validators != at.Validators || bt.ProbeCount == 0 {
		t.Errorf("counts changed: before probes=%d gaps=%d validators=%d; after %d %d %d",
			bt.ProbeCount, bt.Gaps, bt.Validators, at.ProbeCount, at.Gaps, at.Validators)
	}
	for c, n := range beforeTallies.Classes {
		if afterTallies.Classes[c] != n {
			t.Errorf("class %s: %d -> %d", c, n, afterTallies.Classes[c])
		}
	}
	rowAfter := map[string]int{}
	for i, r := range afterTallies.Rows {
		rowAfter[r.Address] = i
	}
	for _, b := range beforeTallies.Rows {
		i, ok := rowAfter[b.Address]
		if !ok {
			t.Errorf("%s has no row in the all window after the prune", b.Address)
			continue
		}
		a := afterTallies.Rows[i]
		if a.ProbeCount != b.ProbeCount {
			t.Errorf("%s probe_count: %d -> %d", b.Address, b.ProbeCount, a.ProbeCount)
		}
		for c, n := range b.Classes {
			if a.Classes[c] != n {
				t.Errorf("%s class %s: %d -> %d", b.Address, c, n, a.Classes[c])
			}
		}
	}
	// docs/verdicts.md tells a reader to check the answer against itself.
	// The classes are folded in from the rollup on this window; the
	// attestation counts have to be folded in with them or the identity
	// fails on exactly the window a reader would check it on.
	for _, c := range []struct {
		name string
		snap allSnapshot
	}{{"before the prune", before}, {"after the prune", after}} {
		att := c.snap.Attestation
		if got, want := att.Attested+att.Unattested+att.Unknown, c.snap.Coverage.Den; got != want {
			t.Errorf("%s: attested+unattested+unknown = %d, serve_rate_coverage.den = %d", c.name, got, want)
		}
		if got, want := c.snap.HeldOut["UNATTESTED"], att.Unattested; got != want {
			t.Errorf("%s: serve_rate_held_out.UNATTESTED = %d, attestation.unattested_probes = %d", c.name, got, want)
		}
	}
	if after.Attestation != before.Attestation {
		t.Errorf("attestation changed across the prune: before %+v, after %+v", before.Attestation, after.Attestation)
	}
	var afterVals struct {
		Validators []valSnapshot `json:"validators"`
		RolledUp   *struct {
			RawFrom string `json:"raw_from"`
		} `json:"rolled_up"`
	}
	get(t, ts2, "/v1/validators?window=all", &afterVals)
	if afterVals.RolledUp == nil {
		t.Errorf("validators list not labelled")
	}
	byAddr := map[string]valSnapshot{}
	for _, v := range afterVals.Validators {
		byAddr[v.Address] = v
	}
	for _, b := range beforeVals.Validators {
		a, ok := byAddr[b.Address]
		if !ok {
			t.Errorf("%s vanished from the all window", b.Address)
			continue
		}
		if a.Obligations != b.Obligations || a.Reach != b.Reach {
			t.Errorf("%s changed:\nbefore %+v\nafter  %+v", b.Address, b, a)
		}
	}
	// the validator detail's all span carries the label and the same obligations
	var detail struct {
		Windows []struct {
			Window      struct{ Name string } `json:"window"`
			Obligations obligationsJSON       `json:"obligations"`
			RolledUp    *struct{ Days int64 } `json:"rolled_up"`
		} `json:"windows"`
	}
	if code := get(t, ts2, "/v1/validators/"+hexAddr+"?window=all", &detail); code != 200 {
		t.Fatalf("detail: %d", code)
	}
	found := false
	for _, w := range detail.Windows {
		if w.Window.Name == "all" {
			found = true
			if w.RolledUp == nil || w.Obligations != byAddr[hexAddr].Obligations || w.Obligations.Total != 3 || w.Obligations.Served != 3 {
				t.Errorf("detail all span: %+v vs %+v", w, byAddr[hexAddr].Obligations)
			}
		}
	}
	if !found {
		t.Error("no all span in the detail")
	}
	// 30d and 7d are untouched by the rollup: raw rows only, no label
	var thirty allSnapshot
	get(t, ts2, "/v1/network?window=30d", &thirty)
	if thirty.RolledUp != nil || thirty.Obligations.Total != 3 {
		t.Errorf("30d: %+v", thirty)
	}
	// stripped raw_json leaves the probe list's typed fields in place
	var probes struct {
		Probes []json.RawMessage `json:"probes"`
	}
	if code := get(t, ts2, "/v1/probes?limit=5", &probes); code != 200 || len(probes.Probes) == 0 {
		t.Errorf("probes: %d, %d rows", code, len(probes.Probes))
	}
}

// pruneLikeBefore does to st what the collector's retention pass did until
// 2026-10-04, when it was retired: strip raw_json from probe and heartbeat
// rows older than strip (0: none), and delete whole rolled days of rows
// older than keep, moving raw_from past them. Nothing in the observer prunes
// any more; the API's rolled-up path stays until it is removed, and this
// keeps it under test for a database pruned before then.
func pruneLikeBefore(t *testing.T, st *store.Store, now time.Time, keep, strip time.Duration) (days []string, rows, stripped int64) {
	t.Helper()
	db := st.DB()
	if strip > 0 {
		for _, table := range []string{"probes", "reachability"} {
			res, err := db.Exec(`UPDATE `+table+` SET raw_json = '' WHERE started_at < ? AND raw_json <> ''`, store.TS(now.Add(-strip)))
			if err != nil {
				t.Fatal(err)
			}
			n, _ := res.RowsAffected()
			stripped += n
		}
	}
	through, err := st.Meta("rollup_through")
	if err != nil || through == "" {
		return
	}
	rolled, err := time.Parse("2006-01-02", through)
	if err != nil {
		t.Fatal(err)
	}
	var first sql.NullString
	if err := db.QueryRow(`SELECT MIN(t) FROM (
			SELECT MIN(started_at) AS t FROM probes
			UNION ALL SELECT MIN(decided_at) FROM sampling_decisions
			UNION ALL SELECT MIN(started_at) FROM reachability
			UNION ALL SELECT MIN(settlement_time) FROM publications)`).Scan(&first); err != nil {
		t.Fatal(err)
	}
	if !first.Valid || first.String == "" {
		return
	}
	ft, err := time.Parse(store.TimeLayout, first.String)
	if err != nil {
		t.Fatal(err)
	}
	day := func(x time.Time) time.Time {
		x = x.UTC()
		return time.Date(x.Year(), x.Month(), x.Day(), 0, 0, 0, 0, time.UTC)
	}
	cut := day(now.Add(-keep))
	for from := day(ft); !from.After(rolled) && from.Before(cut); from = from.Add(24 * time.Hour) {
		lo, hi := store.TS(from), store.TS(from.Add(24*time.Hour-time.Nanosecond))
		for _, q := range []string{
			`DELETE FROM probes WHERE started_at >= ? AND started_at <= ?`,
			`DELETE FROM reachability WHERE started_at >= ? AND started_at <= ?`,
			`DELETE FROM sampling_decisions WHERE decided_at >= ? AND decided_at <= ?`,
		} {
			res, err := db.Exec(q, lo, hi)
			if err != nil {
				t.Fatal(err)
			}
			n, _ := res.RowsAffected()
			rows += n
		}
		days = append(days, from.Format("2006-01-02"))
		if err := st.SetMeta("raw_from", from.Add(24*time.Hour).Format("2006-01-02"), now); err != nil {
			t.Fatal(err)
		}
	}
	return
}

// tableDigests is every table's rows, as a count and a digest of the sorted
// rows, so two states of a store compare table by table.
func tableDigests(t *testing.T, st *store.Store) map[string]string {
	t.Helper()
	names, err := st.DB().Query(`SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'`)
	if err != nil {
		t.Fatal(err)
	}
	var tables []string
	for names.Next() {
		var n string
		if err := names.Scan(&n); err != nil {
			t.Fatal(err)
		}
		tables = append(tables, n)
	}
	names.Close()
	out := map[string]string{}
	for _, table := range tables {
		r, err := st.DB().Query(`SELECT * FROM "` + table + `"`)
		if err != nil {
			t.Fatal(err)
		}
		cols, _ := r.Columns()
		var lines []string
		for r.Next() {
			v := make([]any, len(cols))
			p := make([]any, len(cols))
			for i := range v {
				p[i] = &v[i]
			}
			if err := r.Scan(p...); err != nil {
				t.Fatal(err)
			}
			lines = append(lines, fmt.Sprintf("%q", v))
		}
		r.Close()
		sort.Strings(lines)
		h := sha256.Sum256([]byte(strings.Join(lines, "\n")))
		out[table] = fmt.Sprintf("%d rows %s", len(lines), hex.EncodeToString(h[:8]))
	}
	return out
}

// The rollup pass deletes nothing and rewrites no row: run as the collector
// runs it, today, a year on and ten years on, every table but the rollups'
// own holds exactly the rows it held, raw_json included, raw_from is never
// set, and the "all" window reads the same, unlabelled. An old blob reads in
// ten years as it reads today (the retention decision of 2026-10-04).
func TestRollupDeletesNothing(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "observer.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	now := time.Now().UTC().Truncate(time.Second)
	old := now.Add(-120 * 24 * time.Hour).Truncate(24 * time.Hour).Add(10 * time.Hour)
	insertProbeSet(t, st, "old1", old, old.Add(30*time.Minute), map[string][]wire{
		"served": {ok, ok, ok, ok}, "broken": {ok, gone, ok, ok}, "unreach": {refused, refused, refused, refused},
	})
	mid := now.Add(-40 * 24 * time.Hour)
	insertProbeSet(t, st, "mid1", mid, mid.Add(30*time.Minute), map[string][]wire{
		"served": {ok, ok, ok, ok}, "endun": {ok, err500, err500, err500},
	})
	recent := now.Add(-2 * time.Hour)
	insertProbeSet(t, st, "new1", recent, now.Add(-30*time.Minute), map[string][]wire{
		"served": {ok, ok, ok, ok}, "broken": {ok, gone, ok, ok},
	})
	ts := httptestServer(t, st)
	var before allSnapshot
	get(t, ts, "/v1/network?window=all", &before)
	beforeTables := tableDigests(t, st)

	ctx := context.Background()
	for _, cfg := range []rollup.Config{rollup.Default(), {RollupAfter: time.Nanosecond, Vantage: "test"}} {
		for _, at := range []time.Time{now, now.Add(400 * 24 * time.Hour), now.Add(10 * 365 * 24 * time.Hour)} {
			for i := 0; i < 3; i++ {
				if _, err := rollup.Run(ctx, st, at, cfg); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	if through, _ := st.Meta("rollup_through"); through == "" {
		t.Fatal("the fixture was never rolled up, so the pass was not exercised")
	}
	after := tableDigests(t, st)
	for table, b := range beforeTables {
		switch table {
		case "obligation_daily", "probe_daily", "meta":
			continue // the rollups and their mark are what the pass writes
		}
		if after[table] != b {
			t.Errorf("table %s changed: before %s, after %s", table, b, after[table])
		}
	}
	if v, _ := st.Meta("raw_from"); v != "" {
		t.Errorf("raw_from was set to %q; nothing may be pruned", v)
	}
	ts2 := httptestServer(t, st)
	var later allSnapshot
	get(t, ts2, "/v1/network?window=all", &later)
	if later.RolledUp != nil {
		t.Errorf("the all window is labelled rolled up: %+v", later.RolledUp)
	}
	if later.Obligations != before.Obligations || later.ServeRate != before.ServeRate || later.ReachWindow != before.ReachWindow {
		t.Errorf("the all window changed: before %+v, after %+v", before, later)
	}
}
