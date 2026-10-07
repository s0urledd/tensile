package api_test

import (
	"encoding/json"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/observer/api"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
)

// Once x/signal has scheduled the upgrade, the site says how many blocks
// are left and, at the chain's recently measured pace, roughly when that
// is. The pace is a measurement over the collector's older anchor, never a
// nominal block time, and it is withheld until the measurement spans half
// an hour.
func TestUpgradeSignalCarriesBlocksRemainingAndAnETAAtTheMeasuredPace(t *testing.T) {
	dir := t.TempDir()
	st, err := store.Open(filepath.Join(dir, "observer.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	// the API measures the tip's age against its own clock
	now := time.Now().UTC().Truncate(time.Second)
	set := func(kv map[string]string) {
		t.Helper()
		for k, v := range kv {
			if err := st.SetMeta(k, v, now); err != nil {
				t.Fatal(err)
			}
		}
	}
	set(map[string]string{
		"app_version": "9", "fibre_active": "no", "fibre_app_version": "10",
		"signal_version": "10", "signal_voting_power": "300000000", "signal_threshold_power": "268982696",
		"signal_total_voting_power": "322779235", "signal_upgrade_height": "1082619", "signal_upgrade_app_version": "10", "signal_missing": `null`,
		"signal_polled_at": store.TS(now),
		"chain_height":     "1016732", "chain_tip_time": store.TS(now),
		// six hours ago the tip was 7,606 blocks lower: 2.84 s a block
		"chain_pace_from_height": "1009126", "chain_pace_from_time": store.TS(now.Add(-6 * time.Hour)),
	})
	srv := api.NewWithVantage(st, api.VantageInfo{Name: "test"}, nil, api.WithDataDir(dir))
	ts := httptest.NewServer(srv)
	defer func() { ts.Close(); srv.Close() }()

	type signal struct {
		UpgradeHeight     int64     `json:"upgrade_height"`
		BlocksRemaining   int64     `json:"blocks_remaining"`
		BlockTimeS        float64   `json:"block_time_s"`
		PaceWindowS       int64     `json:"pace_window_s"`
		ETASeconds        int64     `json:"eta_seconds"`
		MissingValidators *[]string `json:"missing_validators"`
	}
	// /v1/meta publishes the height and the ETA; the rest of the tally is
	// what the API computes them from
	read := func() *signal {
		t.Helper()
		var meta struct {
			UpgradeSignal *struct {
				UpgradeHeight int64 `json:"upgrade_height"`
				ETASeconds    int64 `json:"eta_seconds"`
			} `json:"upgrade_signal"`
		}
		if code := get(t, ts, "/v1/meta", &meta); code != 200 {
			t.Fatalf("meta: %d", code)
		}
		raw, err := api.UpgradeSignalJSON(st, time.Now())
		if err != nil {
			t.Fatal(err)
		}
		var u *signal
		if err := json.Unmarshal(raw, &u); err != nil {
			t.Fatal(err)
		}
		if p := meta.UpgradeSignal; (p == nil) != (u == nil) || (p != nil && (p.UpgradeHeight != u.UpgradeHeight || p.ETASeconds != u.ETASeconds)) {
			t.Fatalf("meta publishes %+v of %+v", p, u)
		}
		return u
	}
	u := read()
	// "null" on record, from an older collector, is published as a list
	if u == nil || u.MissingValidators == nil || len(*u.MissingValidators) != 0 {
		t.Fatalf("missing_validators must be a list: %+v", u)
	}
	if u == nil || u.BlocksRemaining != 65887 {
		t.Fatalf("blocks_remaining: %+v", u)
	}
	if u.BlockTimeS < 2.83 || u.BlockTimeS > 2.85 || u.PaceWindowS != 6*3600 {
		t.Fatalf("pace: %v s over %d s", u.BlockTimeS, u.PaceWindowS)
	}
	// 65,887 blocks at 2.84 s: about 2 d 4 h
	if u.ETASeconds < 186000 || u.ETASeconds > 188000 {
		t.Fatalf("eta_seconds: %d", u.ETASeconds)
	}

	// a measurement shorter than half an hour is not stated (a fresh
	// decode target: omitted fields would otherwise keep the old values)
	set(map[string]string{"chain_pace_from_height": "1016500", "chain_pace_from_time": store.TS(now.Add(-10 * time.Minute))})
	if u = read(); u == nil || u.BlocksRemaining != 65887 || u.ETASeconds != 0 || u.BlockTimeS != 0 {
		t.Fatalf("short window should carry no pace: %+v", u)
	}

	// a chain that stopped: the blocks left still stand, the ETA does not,
	// because a pace measured up to a halt says nothing about the restart
	set(map[string]string{
		"chain_tip_time":         store.TS(now.Add(-20 * time.Minute)),
		"chain_pace_from_height": "1009126", "chain_pace_from_time": store.TS(now.Add(-6 * time.Hour)),
	})
	if u = read(); u == nil || u.BlocksRemaining != 65887 || u.ETASeconds != 0 {
		t.Fatalf("a stopped chain should carry no ETA: %+v", u)
	}
}

// The height x/signal scheduled is the upgrade that reached quorum, which
// need not be the one that brings Fibre: a chain two versions below it
// schedules the one in between first. Only an upgrade to the Fibre version
// is published as the height Fibre activates at, with its countdown; one to
// another version, or one a collector recorded no version for, is not.
func TestOnlyTheFibreUpgradeIsCountedDownTo(t *testing.T) {
	dir := t.TempDir()
	st, err := store.Open(filepath.Join(dir, "observer.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	now := time.Now().UTC().Truncate(time.Second)
	set := func(kv map[string]string) {
		t.Helper()
		for k, v := range kv {
			if err := st.SetMeta(k, v, now); err != nil {
				t.Fatal(err)
			}
		}
	}
	set(map[string]string{
		"app_version": "8", "fibre_active": "no", "fibre_app_version": "10",
		"signal_version": "9", "signal_voting_power": "300000000", "signal_threshold_power": "268982696",
		"signal_total_voting_power": "322779235", "signal_upgrade_height": "1082619", "signal_missing": `[]`,
		"signal_polled_at": store.TS(now),
		"chain_height":     "1016732", "chain_tip_time": store.TS(now),
		"chain_pace_from_height": "1009126", "chain_pace_from_time": store.TS(now.Add(-6 * time.Hour)),
	})
	srv := api.NewWithVantage(st, api.VantageInfo{Name: "test"}, nil, api.WithDataDir(dir))
	ts := httptest.NewServer(srv)
	defer func() { ts.Close(); srv.Close() }()
	heightOf := func() (int64, int64) {
		t.Helper()
		var meta struct {
			UpgradeSignal *struct {
				UpgradeHeight int64 `json:"upgrade_height"`
				ETASeconds    int64 `json:"eta_seconds"`
			} `json:"upgrade_signal"`
		}
		if code := get(t, ts, "/v1/meta", &meta); code != 200 {
			t.Fatalf("meta: %d", code)
		}
		if meta.UpgradeSignal == nil {
			t.Fatal("no upgrade_signal while the chain is below Fibre")
		}
		return meta.UpgradeSignal.UpgradeHeight, meta.UpgradeSignal.ETASeconds
	}
	// a collector that recorded no version beside the height
	if h, eta := heightOf(); h != 0 || eta != 0 {
		t.Errorf("no version on record: height %d, eta %d; want neither", h, eta)
	}
	// the upgrade to v9 scheduled while the chain is on v8
	set(map[string]string{"signal_upgrade_app_version": "9"})
	if h, eta := heightOf(); h != 0 || eta != 0 {
		t.Errorf("the v9 upgrade: height %d, eta %d; want neither", h, eta)
	}
	// and the one to v10, once it is the one scheduled
	set(map[string]string{"signal_version": "10", "signal_upgrade_app_version": "10"})
	if h, eta := heightOf(); h != 1082619 || eta == 0 {
		t.Errorf("the Fibre upgrade: height %d, eta %d; want 1082619 and an estimate", h, eta)
	}
}
