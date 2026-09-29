package api_test

import (
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/status"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/api"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
)

// A check that is not a process — here the chain having upgraded past the
// assignment pin — has to reach /v1/meta by name, or the site announces
// "Observer degraded" with nothing after the heading: the banner used to be
// drawn from the components alone, and every component can be alive while
// the chain has stopped producing blocks, a scan gap is on record or the pin
// is stale. Fails on the code before this test with "meta carries no checks".
func TestMetaCarriesTheHealthChecksThatAreNotProcesses(t *testing.T) {
	dir := t.TempDir()
	st, err := store.Open(filepath.Join(dir, "observer.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if err := st.SetMeta("app_version", "11", time.Now()); err != nil {
		t.Fatal(err)
	}
	// Every process alive and well: the only thing wrong is the chain.
	for _, c := range []string{"scanner", "prober", "heartbeat", "collector"} {
		w := status.New(dir, c, "test", "t")
		w.Start()
		w.OK()
		w.Progress(1000)
		defer w.Stop("test")
	}
	time.Sleep(1200 * time.Millisecond) // the delayed status write

	srv := api.NewWithVantage(st, api.VantageInfo{Name: "test"}, nil, api.WithDataDir(dir))
	ts := httptest.NewServer(srv)
	defer func() { ts.Close(); srv.Close() }()

	var meta struct {
		Health    string `json:"health"`
		PinStatus string `json:"pin_status"`
		Checks    []struct {
			Name   string `json:"name"`
			OK     bool   `json:"ok"`
			Detail string `json:"detail"`
		} `json:"checks"`
	}
	if code := getAny(t, ts, "/v1/meta", &meta); code != 200 {
		t.Fatalf("meta: %d", code)
	}
	if meta.Health != "degraded" || meta.PinStatus != "chain_ahead" {
		t.Fatalf("health=%s pin=%s, want degraded/chain_ahead", meta.Health, meta.PinStatus)
	}
	// the processes, which /v1/health lists
	var h healthBody
	getAny(t, ts, "/v1/health", &h)
	for _, c := range meta.Checks {
		for _, k := range h.Components {
			if c.Name == k.Component && !c.OK {
				t.Fatalf("%s failed its check; this test needs every process healthy so the banner has only a non-process reason: %s", c.Name, c.Detail)
			}
		}
	}
	if len(meta.Checks) == 0 {
		t.Fatal("meta carries no checks")
	}
	var pin *struct {
		Name   string `json:"name"`
		OK     bool   `json:"ok"`
		Detail string `json:"detail"`
	}
	for i := range meta.Checks {
		if meta.Checks[i].Name == "pin" {
			pin = &meta.Checks[i]
		}
	}
	if pin == nil || pin.OK || pin.Detail == "" {
		t.Fatalf("the failing pin check is not in meta.checks: %+v", meta.Checks)
	}
	for _, c := range h.Components {
		if c.Component == pin.Name {
			t.Fatal("pin is a component; this test no longer exercises a non-process check")
		}
	}
	// The same rows /v1/health serves, not a second opinion.
	if ok, detail, found := check(h, "pin"); !found || ok || detail != pin.Detail {
		t.Fatalf("meta and health disagree on the pin check: health found=%v ok=%v %q vs meta %q", found, ok, detail, pin.Detail)
	}
}
