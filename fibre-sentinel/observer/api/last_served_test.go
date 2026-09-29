package api_test

import (
	"testing"
)

// The overview's map names the validators that served most recently. It
// used to find them in the newest readings from /v1/probes; the list now
// says it per validator, as the newest reading whose rows came back
// verified, and it is that reading's start.
func TestTheListSaysWhenAValidatorLastServed(t *testing.T) {
	ts := excludeFixture(t)
	var list struct {
		Validators []struct {
			Address      string  `json:"address"`
			LastServedAt *string `json:"last_served_at"`
		} `json:"validators"`
	}
	if code := get(t, ts, "/v1/validators?window=all", &list); code != 200 {
		t.Fatalf("validators: %d", code)
	}
	seen := 0
	for _, v := range list.Validators {
		var newest struct {
			Probes []struct {
				StartedAt string `json:"started_at"`
			} `json:"probes"`
		}
		if code := get(t, ts, "/v1/probes?validator="+v.Address+"&class=HEALTHY&limit=1", &newest); code != 200 {
			t.Fatalf("probes of %s: %d", v.Address, code)
		}
		switch {
		case len(newest.Probes) == 0 && v.LastServedAt != nil:
			t.Errorf("%s never served, last_served_at %s", v.Address, *v.LastServedAt)
		case len(newest.Probes) > 0 && (v.LastServedAt == nil || *v.LastServedAt != newest.Probes[0].StartedAt):
			t.Errorf("%s last_served_at %v, its newest served reading started %s", v.Address, v.LastServedAt, newest.Probes[0].StartedAt)
		}
		if v.LastServedAt != nil {
			seen++
		}
	}
	// kept served; broke, earlyonly and silent did not
	if seen != 1 {
		t.Errorf("%d validators served, want 1", seen)
	}
}
