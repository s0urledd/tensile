package api_test

import (
	"encoding/hex"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/cosmos/cosmos-sdk/types/bech32"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/probe"
	"github.com/plsgiveup/fibre/fibre-sentinel/internal/scan"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
)

// A validator with no open Fibre endpoint has no endpoint state: neither a
// bonded validator that never registered a host, whose readings say only
// that it has none (NO_REGISTERED_HOST, a registry state, not a refusal to
// serve), nor one whose endpoint closed after a check that succeeded. Both
// used to be published from their newest check, "unreachable" and
// "reachable", to every tool polling the status route. A validator with an
// open endpoint keeps the word of its newest check of that host.
func TestAValidatorWithNoOpenEndpointHasNoEndpointState(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "observer.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	now := time.Now().UTC().Truncate(time.Second)
	addr := func(b byte) (hexAddr, cons string) {
		raw := make([]byte, 20)
		raw[19] = b
		cons, err := bech32.ConvertAndEncode("celestiavalcons", raw)
		if err != nil {
			t.Fatal(err)
		}
		return hex.EncodeToString(raw), cons
	}
	hostless, _ := addr(0x0a)
	open, openCons := addr(0x0b)
	closed, closedCons := addr(0x0c)
	var ids []scan.ValidatorIdentity
	for i, a := range []string{hostless, open, closed} {
		ids = append(ids, scan.ValidatorIdentity{ConsAddressHex: a, OperatorAddress: "celestiavaloper1v" + string(rune('a'+i)), Moniker: "v" + string(rune('a'+i)),
			Tokens: "1000000", Status: "BOND_STATUS_BONDED"})
	}
	if _, err := st.UpsertValidatorIdentities(ids, now); err != nil {
		t.Fatal(err)
	}
	for _, e := range []struct {
		cons, host string
		closed     any
	}{{openCons, "b:7980", nil}, {closedCons, "c:7980", store.TS(now.Add(-time.Minute))}} {
		if _, err := st.DB().Exec(`INSERT INTO endpoints
			(validator_cons_address, host, first_seen_at, first_seen_height, last_seen_at, last_seen_height, closed_at)
			VALUES (?, ?, ?, ?, ?, ?, ?)`, e.cons, e.host, store.TS(now.Add(-2*time.Hour)), 99, store.TS(now), 99, e.closed); err != nil {
			t.Fatal(err)
		}
	}
	// the open endpoint and the closed one each answered their newest check
	heartbeat(t, st, open, "b:7980", 5*time.Minute, true)
	heartbeat(t, st, closed, "c:7980", 5*time.Minute, true)
	// the hostless validator's readings: nothing to connect to
	at := now.Add(-3 * time.Minute)
	m := probe.Measurement{
		SchemaVersion: probe.AttestationSchemaVersion, Vantage: "test", PromiseHash: "nohost", ValidatorAddress: hostless, ValidatorHost: "",
		Assigned: true, Attested: true, ScheduleLabel: "full", ScheduledAt: at, StartedAt: at, FinishedAt: at, Phase: probe.PhaseInWindow,
		Outcome: probe.OutcomeNoHost, Classification: probe.ClassNotRegistered,
	}
	raw, _ := json.Marshal(m)
	if _, err := st.InsertProbe(m, raw); err != nil {
		t.Fatal(err)
	}

	ts := httptestServer(t, st)
	want := map[string]struct {
		state string
		up    *bool
	}{hostless: {"", nil}, closed: {"", nil}, open: {"reachable", ptr(true)}}
	for a, w := range want {
		var s statusBody
		if code := get(t, ts, "/v1/validators/"+a+"/status?window=24h", &s); code != 200 {
			t.Fatalf("%s: %d", a, code)
		}
		if s.EndpointState != w.state || (s.Reachable == nil) != (w.up == nil) || (s.Reachable != nil && *s.Reachable != *w.up) {
			t.Errorf("%s: endpoint_state %q reachable %v, want %q %v", a, s.EndpointState, s.Reachable, w.state, w.up)
		}
		if w.up == nil && s.IdentityStatus != "unknown" {
			t.Errorf("%s: identity_status %q from a check of no open endpoint", a, s.IdentityStatus)
		}
	}
	// and the list says the same
	var list struct {
		Validators []struct {
			Address       string `json:"address"`
			EndpointState string `json:"endpoint_state"`
			Reachable     *bool  `json:"reachable"`
		} `json:"validators"`
	}
	if code := get(t, ts, "/v1/validators?window=24h", &list); code != 200 {
		t.Fatalf("validators: %d", code)
	}
	seen := 0
	for _, v := range list.Validators {
		if w, ok := want[v.Address]; ok {
			seen++
			if v.EndpointState != w.state || (v.Reachable == nil) != (w.up == nil) {
				t.Errorf("list, %s: endpoint_state %q reachable %v, want %q %v", v.Address, v.EndpointState, v.Reachable, w.state, w.up)
			}
		}
	}
	if seen != 3 {
		t.Errorf("the list has %d of the three validators", seen)
	}
}
