package api_test

import (
	"encoding/hex"
	"encoding/json"
	"net/http"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/cosmos/cosmos-sdk/types/bech32"

	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
)

// heartbeat stores one of this observer's handshakes with addr's host, ago
// before now.
func heartbeat(t *testing.T, st *store.Store, addr, host string, ago time.Duration, up bool) {
	t.Helper()
	at := store.TS(time.Now().UTC().Add(-ago))
	ok, outcome := 0, "TCP_REFUSED"
	if up {
		ok, outcome = 1, "REACHABLE"
	}
	if _, err := st.DB().Exec(`INSERT INTO reachability (dedupe_key, vantage, validator_address, validator_host, height, scheduled_at, started_at,
		dns_ok, tcp_ok, tcp_ms, tls_ok, tls_ms, identity_ok, identity_reason, outcome, raw_error, total_duration_ms, raw_json)
		VALUES (?, 'test', ?, ?, 1, ?, ?, 1, ?, 10, ?, 20, ?, '', ?, '', 30, '{}')`,
		addr+"|"+at, addr, host, at, at, ok, ok, ok, outcome); err != nil {
		t.Fatal(err)
	}
}

type statusBody struct {
	Address         string  `json:"address"`
	ConsAddress     string  `json:"cons_address"`
	Host            string  `json:"host"`
	Jailed          bool    `json:"jailed"`
	EndpointState   string  `json:"endpoint_state"`
	Reachable       *bool   `json:"reachable"`
	IdentityStatus  string  `json:"identity_status"`
	LastReachableAt *string `json:"last_reachable_at"`
	Window          struct {
		Name string `json:"name"`
	} `json:"window"`
	ComputedAt  string `json:"computed_at"`
	Obligations struct {
		Served int64                    `json:"served"`
		Broken int64                    `json:"broken"`
		Rate   struct{ Num, Den int64 } `json:"rate"`
	} `json:"obligations"`
	Signing struct {
		Assigned       int64   `json:"assigned"`
		Signed         int64   `json:"signed"`
		LastEndorsedAt *string `json:"last_endorsed_at"`
	} `json:"signing"`
	LastEndpointCheck *struct {
		At      string `json:"at"`
		Outcome string `json:"outcome"`
	} `json:"last_endpoint_check"`
}

// The status route is the validator page's own figures, few of them, for
// a tool that polls one validator and alerts: the same endpoint state and
// obligations as the page, the newest endpoint check, whichever address
// spelling the tool holds.
func TestValidatorStatusIsTheValidatorsOwnFigures(t *testing.T) {
	_, st := excludeFixtureStore(t)
	broke := selfAddrs["broke"]
	heartbeat(t, st, broke, "b:7980", 10*time.Minute, true)
	heartbeat(t, st, broke, "b:7980", 5*time.Minute, false)
	ts := httptestServer(t, st)

	var s statusBody
	if code := get(t, ts, "/v1/validators/"+broke+"/status?window=all", &s); code != 200 {
		t.Fatalf("status: %d", code)
	}
	if s.Address != broke || s.Window.Name != "all" || s.ComputedAt == "" {
		t.Fatalf("status: %+v", s)
	}
	// its one endorsed blob could not be reconstructed and its rows did not
	// come back
	if o := s.Obligations; o.Served != 0 || o.Broken != 1 || o.Rate.Num != 0 || o.Rate.Den != 1 {
		t.Fatalf("obligations: %+v", o)
	}
	if c := s.LastEndpointCheck; c == nil || c.Outcome != "TCP_REFUSED" || c.At == "" {
		t.Fatalf("last endpoint check: %+v", c)
	}

	// the same figures the validator page shows
	var page struct {
		Validator statusBody `json:"validator"`
		Check     *struct {
			At      string `json:"at"`
			Outcome string `json:"outcome"`
		} `json:"last_endpoint_check"`
	}
	if code := get(t, ts, "/v1/validators/"+broke+"?window=all", &page); code != 200 {
		t.Fatalf("detail: %d", code)
	}
	v := page.Validator
	if s.EndpointState != v.EndpointState || !reflect.DeepEqual(s.Reachable, v.Reachable) || s.IdentityStatus != v.IdentityStatus ||
		!reflect.DeepEqual(s.LastReachableAt, v.LastReachableAt) || s.Jailed != v.Jailed || s.ConsAddress != v.ConsAddress ||
		s.Obligations != v.Obligations || s.Signing.Assigned != v.Signing.Assigned || s.Signing.Signed != v.Signing.Signed ||
		!reflect.DeepEqual(s.Signing.LastEndorsedAt, v.Signing.LastEndorsedAt) || page.Check == nil || *page.Check != *s.LastEndpointCheck {
		t.Fatalf("status and page disagree:\nstatus %+v\npage   %+v", s, page)
	}

	// compact: nothing but the status
	var raw map[string]json.RawMessage
	get(t, ts, "/v1/validators/"+broke+"/status?window=all", &raw)
	var keys []string
	for k := range raw {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	want := []string{"address", "computed_at", "cons_address", "endpoint_state", "host", "identity_status", "jailed", "last_endpoint_check",
		"last_reachable_at", "obligations", "reachable", "signing", "window"}
	if !reflect.DeepEqual(keys, want) {
		t.Errorf("keys = %v, want %v", keys, want)
	}

	// the consensus address in bech32 is the same validator
	b, err := hex.DecodeString(broke)
	if err != nil {
		t.Fatal(err)
	}
	valcons, err := bech32.ConvertAndEncode("celestiavalcons", b)
	if err != nil {
		t.Fatal(err)
	}
	var again statusBody
	if code := get(t, ts, "/v1/validators/"+valcons+"/status?window=all", &again); code != 200 || again.Address != broke {
		t.Fatalf("by valcons: %d %+v", code, again)
	}
}

// What the route refuses, and what it cannot answer yet.
func TestValidatorStatusRefusals(t *testing.T) {
	_, st := excludeFixtureStore(t)
	ts := httptestServer(t, st)
	kept := selfAddrs["kept"]
	for path, want := range map[string]int{
		"/v1/validators/" + strings.Repeat("0f", 20) + "/status":        404, // on record nowhere
		"/v1/validators/zzz/status":                                     400,
		"/v1/validators/" + kept + "/status?window=1y":                  400,
		"/v1/validators/" + kept + "/status?as_of=2026-09-01T00:00:00Z": 400,
		"/v1/validators/" + kept + "/status":                            200,
	} {
		if code := get(t, ts, path, nil); code != want {
			t.Errorf("%s: %d, want %d", path, code, want)
		}
	}
	// A validator that appeared after the snapshot was taken is on record
	// and not in it yet: the answer says it is being computed.
	if code := get(t, ts, "/v1/validators/"+kept+"/status?window=all", nil); code != 200 {
		t.Fatalf("all: %d", code)
	}
	late := strings.Repeat("e5", 20)
	heartbeat(t, st, late, "e:7980", time.Minute, true)
	resp, err := http.Get(ts.URL + "/v1/validators/" + late + "/status?window=all")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var body struct {
		Computing bool `json:"computing"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 503 || !body.Computing || resp.Header.Get("Retry-After") == "" {
		t.Fatalf("a validator the snapshot does not list yet: %d %+v", resp.StatusCode, body)
	}
}
