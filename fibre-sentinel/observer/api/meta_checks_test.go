package api_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/status"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/api"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
)

// The observer's health is /v1/health's alone. /v1/meta, which every page
// reads, carried a copy of the verdict and every check (the disk's size
// among them), ran them all on each call, and nothing on the site read it:
// the site shows the observer's state only when the API does not answer.
// A check that is not a process (here the chain past the assignment pin)
// is on /v1/health by name, and /v1/meta keeps the pin status itself.
func TestMetaCarriesNoCopyOfTheHealthChecks(t *testing.T) {
	dir := t.TempDir()
	st, err := store.Open(filepath.Join(dir, "observer.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if err := st.SetMeta("app_version", "11", time.Now()); err != nil {
		t.Fatal(err)
	}
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

	resp, err := http.Get(ts.URL + "/v1/meta")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var meta map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&meta); err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"health", "checks"} {
		if _, ok := meta[k]; ok {
			t.Errorf("/v1/meta carries %q", k)
		}
	}
	if meta["pin_status"] != "chain_ahead" {
		t.Fatalf("pin_status: %v", meta["pin_status"])
	}
	var h healthBody
	getAny(t, ts, "/v1/health", &h)
	if ok, detail, found := check(h, "pin"); !found || ok || detail == "" {
		t.Fatalf("the pin check on /v1/health: found=%v ok=%v %q", found, ok, detail)
	}
}
