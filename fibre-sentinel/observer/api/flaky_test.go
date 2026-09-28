package api_test

import (
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/observer/api"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
)

// One failed check after a good one is still reachable; two in a row is
// unreachable; a good newest check is reachable.
func TestEndpointStateDebouncesASingleFailure(t *testing.T) {
	dir := t.TempDir()
	st, err := store.Open(filepath.Join(dir, "observer.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ins := `INSERT INTO reachability (dedupe_key, vantage, validator_address, validator_host, height, scheduled_at, started_at,
		dns_ok, tcp_ok, tcp_ms, tls_ok, tls_ms, identity_ok, identity_reason, outcome, raw_error, total_duration_ms, raw_json)
		VALUES (?, 'test', ?, 'h:7980', 1, ?, ?, 1, 1, 10, ?, 20, ?, '', ?, '', 30, '{}')`
	now := time.Now()
	add := func(key, addr string, ago time.Duration, ok bool) {
		at := store.TS(now.Add(-ago))
		tls, outcome := 1, "REACHABLE"
		if !ok {
			tls, outcome = 0, "TLS_HANDSHAKE_FAIL"
		}
		if _, err := st.DB().Exec(ins, key, addr, at, at, tls, tls, outcome); err != nil {
			t.Fatal(err)
		}
	}
	flaky := "0000000000000000000000000000000000000001"
	down := "0000000000000000000000000000000000000002"
	up := "0000000000000000000000000000000000000003"
	add("f1", flaky, 10*time.Minute, true)
	add("f2", flaky, 5*time.Minute, false)
	add("d1", down, 10*time.Minute, false)
	add("d2", down, 5*time.Minute, false)
	add("u1", up, 10*time.Minute, false)
	add("u2", up, 5*time.Minute, true)

	srv := api.NewWithVantage(st, api.VantageInfo{Name: "test"}, nil, api.WithDataDir(dir))
	ts := httptest.NewServer(srv)
	defer func() { ts.Close(); srv.Close() }()
	want := map[string]struct {
		state string
		up    bool
	}{flaky: {"reachable", true}, down: {"unreachable", false}, up: {"reachable", true}}
	for addr, w := range want {
		var body struct {
			V struct {
				Reachable     *bool  `json:"reachable"`
				EndpointState string `json:"endpoint_state"`
				Identity      string `json:"identity_status"`
			} `json:"validator"`
		}
		if code := getAny(t, ts, "/v1/validators/"+addr, &body); code != 200 {
			t.Fatalf("%s: %d", addr, code)
		}
		if body.V.EndpointState != w.state || body.V.Reachable == nil || *body.V.Reachable != w.up {
			t.Errorf("%s: state %q reachable %v, want %q %v", addr, body.V.EndpointState, body.V.Reachable, w.state, w.up)
		}
		if addr == flaky && body.V.Identity != "verified" {
			t.Errorf("one failed check keeps the last good check's identity, got %q", body.V.Identity)
		}
	}
}
