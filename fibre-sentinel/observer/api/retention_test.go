package api_test

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/observer/rollup"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
)

type allSnapshot struct {
	Obligations obligationsJSON          `json:"obligations"`
	Classes     map[string]int64         `json:"classes"`
	Faults      int64                    `json:"faults"`
	ProbeCount  int64                    `json:"probe_count"`
	Gaps        int64                    `json:"probe_gaps"`
	Validators  int64                    `json:"validators_probed"`
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

type valSnapshot struct {
	Address     string                   `json:"address"`
	Obligations obligationsJSON          `json:"obligations"`
	Classes     map[string]int64         `json:"classes"`
	Faults      int64                    `json:"faults"`
	ProbeCount  int64                    `json:"probe_count"`
	Reach       struct{ Num, Den int64 } `json:"reachability_window"`
}

// Past the raw retention the "all" window is the daily rollup for the
// pruned days plus the raw rows: rolling a day up and pruning it must
// leave every "all" figure exactly as it was, now labelled, and stripping
// raw_json must leave every typed figure in place.
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

	// roll up (14 days after the day) and prune (rows older than 30 days)
	cfg := rollup.Config{RetainRaw: 30 * 24 * time.Hour, RetainRawJSON: 7 * 24 * time.Hour, RollupAfter: 14 * 24 * time.Hour}
	rep, err := rollup.Run(context.Background(), st, now, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if len(rep.RolledDays) == 0 || rep.PendingAtRoll != 0 {
		t.Fatalf("report = %+v", rep)
	}
	// rolled through yesterday minus 14 days; the old day is pruned, the
	// days after it up to the retention cut are pruned three at a time
	for i := 0; i < 20 && len(rep.PrunedDays) < 11; i++ {
		more, err := rollup.Run(context.Background(), st, now, cfg)
		if err != nil {
			t.Fatal(err)
		}
		rep.PrunedDays = append(rep.PrunedDays, more.PrunedDays...)
		rep.PrunedRows += more.PrunedRows
		rep.RawJSONDropped += more.RawJSONDropped
	}
	if rep.PrunedRows == 0 || rep.PrunedDays[0] != old.Format("2006-01-02") {
		t.Fatalf("pruned %d rows over %v", rep.PrunedRows, rep.PrunedDays)
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
	var withJSON int64
	_ = st.DB().QueryRow(`SELECT COUNT(*) FROM probes WHERE raw_json = ''`).Scan(&withJSON)
	if rep.RawJSONDropped == 0 {
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
	if after.Faults != before.Faults || after.ProbeCount != before.ProbeCount || after.Gaps != before.Gaps || after.Validators != before.Validators {
		t.Errorf("counts changed: before faults=%d probes=%d gaps=%d validators=%d; after %d %d %d %d",
			before.Faults, before.ProbeCount, before.Gaps, before.Validators, after.Faults, after.ProbeCount, after.Gaps, after.Validators)
	}
	if after.ServeRate != before.ServeRate || after.ReachWindow != before.ReachWindow {
		t.Errorf("rates changed: serve %v -> %v, reach %v -> %v", before.ServeRate, after.ServeRate, before.ReachWindow, after.ReachWindow)
	}
	for c, n := range before.Classes {
		if after.Classes[c] != n {
			t.Errorf("class %s: %d -> %d", c, n, after.Classes[c])
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
		if a.Obligations != b.Obligations || a.Faults != b.Faults || a.ProbeCount != b.ProbeCount || a.Reach != b.Reach {
			t.Errorf("%s changed:\nbefore %+v\nafter  %+v", b.Address, b, a)
		}
		for c, n := range b.Classes {
			if a.Classes[c] != n {
				t.Errorf("%s class %s: %d -> %d", b.Address, c, n, a.Classes[c])
			}
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
