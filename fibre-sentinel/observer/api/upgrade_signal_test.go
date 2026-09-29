package api_test

import (
	"encoding/json"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/scan"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/api"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
)

// Before Fibre exists on a chain, the one Fibre-relevant fact the chain
// carries is who has signalled for the version that brings it. The
// operator's own validator signalled days before this test existed, and the
// site did not know, because the observer read x/fibre, x/valaddr and
// staking and never x/signal. Now the tally, the scheduled height and the
// missing validators reach /v1/meta, each bonded validator says whether it
// signalled, and a moniker two validators share is left unattributed rather
// than guessed. Once Fibre is live the block is gone.
func ptr(b bool) *bool { return &b }

func TestTheUpgradeSignalIsPublishedUntilFibreIsLive(t *testing.T) {
	dir := t.TempDir()
	st, err := store.Open(filepath.Join(dir, "observer.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	now := time.Now()
	for k, v := range map[string]string{
		"app_version": "9", "fibre_active": "no", "fibre_app_version": "10",
		"signal_version": "10", "signal_voting_power": "50500001", "signal_threshold_power": "268982696",
		"signal_total_voting_power": "322779235", "signal_upgrade_height": "0",
		"signal_missing": `["beta","twin"]`, "signal_polled_at": store.TS(now),
	} {
		if err := st.SetMeta(k, v, now); err != nil {
			t.Fatal(err)
		}
	}
	ids := []scan.ValidatorIdentity{
		{ConsAddressHex: "aa" + "00000000000000000000000000000000000000", OperatorAddress: "celestiavaloper1alpha", Moniker: "alpha", Tokens: "1000000", Status: "BOND_STATUS_BONDED"},
		{ConsAddressHex: "bb" + "00000000000000000000000000000000000000", OperatorAddress: "celestiavaloper1beta", Moniker: "beta", Tokens: "1000000", Status: "BOND_STATUS_BONDED"},
		{ConsAddressHex: "cc" + "00000000000000000000000000000000000000", OperatorAddress: "celestiavaloper1twin1", Moniker: "twin", Tokens: "1000000", Status: "BOND_STATUS_BONDED"},
		{ConsAddressHex: "dd" + "00000000000000000000000000000000000000", OperatorAddress: "celestiavaloper1twin2", Moniker: "twin", Tokens: "1000000", Status: "BOND_STATUS_BONDED"},
	}
	if _, err := st.UpsertValidatorIdentities(ids, now); err != nil {
		t.Fatal(err)
	}
	srv := api.NewWithVantage(st, api.VantageInfo{Name: "test"}, nil, api.WithDataDir(dir))
	ts := httptest.NewServer(srv)
	defer func() { ts.Close(); srv.Close() }()

	var meta struct {
		UpgradeSignal *struct {
			UpgradeHeight int64 `json:"upgrade_height"`
		} `json:"upgrade_signal"`
	}
	if code := get(t, ts, "/v1/meta", &meta); code != 200 {
		t.Fatalf("meta: %d", code)
	}
	if meta.UpgradeSignal == nil || meta.UpgradeSignal.UpgradeHeight != 0 {
		t.Fatalf("upgrade_signal: %+v", meta.UpgradeSignal)
	}
	// the tally behind it, which the validators' signaled_upgrade comes from
	raw, err := api.UpgradeSignalJSON(st, now)
	if err != nil {
		t.Fatal(err)
	}
	var u *struct {
		Version        int64    `json:"version"`
		Share          float64  `json:"share"`
		ThresholdShare float64  `json:"threshold_share"`
		Missing        []string `json:"missing_validators"`
	}
	if err := json.Unmarshal(raw, &u); err != nil {
		t.Fatal(err)
	}
	if u == nil || u.Version != 10 || len(u.Missing) != 2 {
		t.Fatalf("upgrade signal: %+v", u)
	}
	if u.Share < 0.156 || u.Share > 0.157 || u.ThresholdShare < 0.833 || u.ThresholdShare > 0.834 {
		t.Fatalf("shares: %v / %v", u.Share, u.ThresholdShare)
	}

	var vals struct {
		Validators []struct {
			Moniker  string `json:"moniker"`
			Signaled *bool  `json:"signaled_upgrade"`
		} `json:"validators"`
	}
	if code := get(t, ts, "/v1/validators?window=24h", &vals); code != 200 {
		t.Fatalf("validators: %d", code)
	}
	got := map[string][]*bool{}
	for _, v := range vals.Validators {
		got[v.Moniker] = append(got[v.Moniker], v.Signaled)
	}
	if s := got["alpha"]; len(s) != 1 || s[0] == nil || !*s[0] {
		t.Fatalf("alpha signalled and must say so: %v", s)
	}
	if s := got["beta"]; len(s) != 1 || s[0] == nil || *s[0] {
		t.Fatalf("beta is missing and must say so: %v", s)
	}
	if s := got["twin"]; len(s) != 2 || s[0] != nil || s[1] != nil {
		t.Fatalf("two validators named twin cannot be attributed; both must be unset: %v", s)
	}

	// The single-validator route builds one row and must reach the same
	// answer: it used to count monikers over the rows it was building, so a
	// twin asked for alone looked unique and got its sibling's signal.
	for _, c := range []struct {
		addr string
		want *bool
	}{
		{ids[0].ConsAddressHex, ptr(true)}, {ids[1].ConsAddressHex, ptr(false)},
		{ids[2].ConsAddressHex, nil}, {ids[3].ConsAddressHex, nil},
	} {
		var one struct {
			Validator struct {
				Moniker  string `json:"moniker"`
				Signaled *bool  `json:"signaled_upgrade"`
			} `json:"validator"`
		}
		if code := get(t, ts, "/v1/validators/"+c.addr+"?window=24h", &one); code != 200 {
			t.Fatalf("validator %s: %d", c.addr, code)
		}
		switch {
		case c.want == nil && one.Validator.Signaled != nil:
			t.Fatalf("%s (%s): ambiguous moniker attributed on the detail route: %v", c.addr[:2], one.Validator.Moniker, *one.Validator.Signaled)
		case c.want != nil && (one.Validator.Signaled == nil || *one.Validator.Signaled != *c.want):
			t.Fatalf("%s (%s): want %v, got %v", c.addr[:2], one.Validator.Moniker, *c.want, one.Validator.Signaled)
		}
	}

	// Fibre live: the block is gone from /v1/meta.
	if err := st.SetMeta("fibre_active", "yes", now); err != nil {
		t.Fatal(err)
	}
	meta.UpgradeSignal = nil
	get(t, ts, "/v1/meta", &meta)
	if meta.UpgradeSignal != nil {
		t.Fatalf("upgrade_signal still published with Fibre live: %+v", meta.UpgradeSignal)
	}
}
