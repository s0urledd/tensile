package api_test

import (
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/verdict"
)

// A broken obligation whose only fault is younger than the settling period
// is counted — in broken and in the rate — and flagged provisional, with
// the moment it settles. One that faulted hours ago is broken and final. The
// flag reaches the probe row, the validator row, the network row and the
// validator page, and the page carries the network's rate over the same
// window beside the validator's own.
func TestProvisionalFaultsAreCountedAndFlagged(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "observer.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	now := time.Now().UTC().Truncate(time.Second)
	a, b, c := strings.Repeat("a", 40), strings.Repeat("b", 40), strings.Repeat("c", 40)

	// Hours ago: a faulted at the first point. Final.
	oldCreated, oldMSU := now.Add(-6*time.Hour), now.Add(-4*time.Hour)
	insertProbeSet(t, st, "oldp", oldCreated, oldMSU, map[string][]wire{
		a: {gone, ok, ok, ok}, b: {ok, ok, ok, ok}, c: {ok, ok, ok, ok},
	})
	// Just now: the window ended a minute ago and a's last reading, at 92%
	// of it (eight minutes ago), was a fault. Provisional.
	freshCreated, freshMSU := now.Add(-100*time.Minute), now.Add(-time.Minute)
	insertProbeSet(t, st, "newp", freshCreated, freshMSU, map[string][]wire{
		a: {ok, ok, ok, gone}, b: {ok, ok, ok, ok}, c: {ok, ok, ok, ok},
	})
	freshFault := inWindowPoint(freshCreated, freshMSU, 3)
	if now.Sub(freshFault) >= verdict.FaultSettling {
		t.Fatalf("fixture: the fresh fault is %s old, not younger than the settling period", now.Sub(freshFault))
	}
	if _, err := st.StartRun("collector", "test", "t", now); err != nil {
		t.Fatal(err)
	}
	ts := httptestServerWith(t, st)

	type prov struct {
		Obligations     int64  `json:"obligations"`
		Until           string `json:"until"`
		SettlingSeconds int64  `json:"settling_seconds"`
	}
	var vals struct {
		Validators []struct {
			Address     string          `json:"address"`
			Obligations obligationsJSON `json:"obligations"`
			Provisional *prov           `json:"provisional_faults"`
		} `json:"validators"`
	}
	if code := get(t, ts, "/v1/validators?window=24h", &vals); code != 200 {
		t.Fatalf("validators: %d", code)
	}
	seen := 0
	for _, v := range vals.Validators {
		switch v.Address {
		case a:
			seen++
			// Both faults count: provisional is a label, not a hold.
			if v.Obligations.Broken != 2 || v.Obligations.Served != 0 {
				t.Errorf("a: broken/served = %d/%d, want 2/0: a provisional fault still counts", v.Obligations.Broken, v.Obligations.Served)
			}
			if v.Provisional == nil || v.Provisional.Obligations != 1 {
				t.Fatalf("a: provisional = %+v, want 1 (the old fault is final)", v.Provisional)
			}
			want := freshFault.Add(verdict.FaultSettling).Format(time.RFC3339)
			if v.Provisional.Until != want || v.Provisional.SettlingSeconds != int64(verdict.FaultSettling/time.Second) {
				t.Errorf("a: until %s settling %d, want %s", v.Provisional.Until, v.Provisional.SettlingSeconds, want)
			}
		case b, c:
			seen++
			if v.Provisional != nil {
				t.Errorf("%s: provisional %+v on a validator with no fault", v.Address[:4], v.Provisional)
			}
		}
	}
	if seen != 3 {
		t.Fatalf("saw %d of the three validators", seen)
	}

	var net struct {
		Provisional *prov `json:"provisional_faults"`
	}
	if code := get(t, ts, "/v1/network?window=24h", &net); code != 200 || net.Provisional == nil || net.Provisional.Obligations != 1 {
		t.Fatalf("network provisional = %+v (%d)", net.Provisional, code)
	}

	var detail struct {
		Recent []struct {
			PromiseHash    string `json:"promise_hash"`
			Classification string `json:"classification"`
			Provisional    bool   `json:"provisional"`
			SettledAt      string `json:"settled_at"`
		} `json:"recent_probes"`
		Windows []struct {
			Provisional *prov `json:"provisional_faults"`
		} `json:"windows"`
		Ref *struct {
			Median     *float64                 `json:"median_rate"`
			Validators int                      `json:"validators"`
			MinRated   int64                    `json:"min_rated"`
			Pooled     struct{ Num, Den int64 } `json:"pooled_rate"`
		} `json:"network_reference"`
	}
	if code := get(t, ts, "/v1/validators/"+a+"?window=24h", &detail); code != 200 {
		t.Fatalf("detail: %d", code)
	}
	// each check says when its blob settled, the time its endorsement was given
	for _, p := range detail.Recent {
		want := map[string]time.Time{"newp": freshCreated, "oldp": oldCreated}[p.PromiseHash]
		if got, err := time.Parse(time.RFC3339Nano, p.SettledAt); err != nil || !got.Equal(want) {
			t.Errorf("%s settled_at = %q, want %s", p.PromiseHash, p.SettledAt, want.Format(time.RFC3339))
		}
	}
	var fresh, old int
	for _, p := range detail.Recent {
		if p.Classification != "FAULT" {
			if p.Provisional {
				t.Errorf("a %s row is marked provisional", p.Classification)
			}
			continue
		}
		switch {
		case p.PromiseHash == "newp" && p.Provisional:
			fresh++
		case p.PromiseHash == "oldp" && !p.Provisional:
			old++
		default:
			t.Errorf("fault on %s: provisional=%v", p.PromiseHash, p.Provisional)
		}
	}
	if fresh != 1 || old != 1 {
		t.Errorf("fresh/old faults = %d/%d, want 1/1", fresh, old)
	}
	if len(detail.Windows) == 0 || detail.Windows[0].Provisional == nil || detail.Windows[0].Provisional.Obligations != 1 {
		t.Errorf("24h span provisional = %+v", detail.Windows)
	}
	// Six obligations decided, four served: the pooled rate is the whole
	// network's. No validator has 20 decided, so there is no median to
	// stand beside the rate, and the answer says so rather than inventing one.
	if detail.Ref == nil || detail.Ref.Median != nil || detail.Ref.Validators != 0 || detail.Ref.MinRated != 20 || detail.Ref.Pooled.Num != 4 || detail.Ref.Pooled.Den != 6 {
		t.Fatalf("network_reference = %+v", detail.Ref)
	}
}
