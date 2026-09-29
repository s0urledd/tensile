package api_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/status"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/api"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
	"net/http/httptest"
)

type healthBody struct {
	Status string `json:"status"`
	Checks []struct {
		Name   string `json:"name"`
		OK     bool   `json:"ok"`
		Detail string `json:"detail"`
	} `json:"checks"`
	Components []struct {
		Component string `json:"component"`
		Present   bool   `json:"present"`
		Alive     bool   `json:"alive"`
		OK        bool   `json:"ok"`
	} `json:"components"`
	PinStatus string `json:"pin_status"`
}

// getAny decodes whatever status code comes back; /v1/health answers 503
// with a body on purpose.
func getAny(t *testing.T, ts *httptest.Server, path string, into any) int {
	t.Helper()
	resp, err := http.Get(ts.URL + path)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if err := json.NewDecoder(resp.Body).Decode(into); err != nil {
		t.Fatalf("%s: decode: %v", path, err)
	}
	return resp.StatusCode
}

func check(b healthBody, name string) (bool, string, bool) {
	for _, c := range b.Checks {
		if c.Name == name {
			return c.OK, c.Detail, true
		}
	}
	return false, "", false
}

func TestHealthReadsStatusFiles(t *testing.T) {
	dir := t.TempDir()
	st, err := store.Open(filepath.Join(dir, "observer.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if err := st.SetMeta("app_version", "10", time.Now()); err != nil {
		t.Fatal(err)
	}

	// Three processes alive, the prober never started.
	for _, c := range []string{"scanner", "heartbeat", "collector"} {
		w := status.New(dir, c, "test", "t")
		w.Start()
		w.OK()
		w.Progress(1000)
		w.Stop("") // writes the file; a stopped process is "not alive" below
	}
	// The scanner is alive (fresh file, no stopped_at) and behind the chain.
	sw := status.New(dir, "scanner", "test", "t")
	sw.Start()
	sw.Progress(100)
	defer sw.Stop("test")
	cw := status.New(dir, "collector", "test", "t")
	cw.Start()
	cw.Progress(1000)
	defer cw.Stop("test")

	time.Sleep(1200 * time.Millisecond) // the delayed status write

	srv := api.NewWithVantage(st, api.VantageInfo{Name: "test"}, nil, api.WithDataDir(dir))
	ts := httptest.NewServer(srv)
	defer func() { ts.Close(); srv.Close() }() // let the snapshot writers land before the temp dir goes

	var h healthBody
	if code := getAny(t, ts, "/v1/health", &h); code != 503 {
		t.Fatalf("health with a missing prober must be 503, got %d", code)
	}
	if h.Status != "degraded" {
		t.Fatalf("status: %s", h.Status)
	}
	if ok, detail, found := check(h, "prober"); !found || ok || detail == "" {
		t.Fatalf("prober check: found=%v ok=%v detail=%q", found, ok, detail)
	}
	if ok, _, _ := check(h, "heartbeat"); ok {
		t.Fatal("a stopped heartbeat must fail its check")
	}
	if ok, _, _ := check(h, "scanner"); !ok {
		t.Fatal("a live scanner must pass its check")
	}
	if ok, detail, found := check(h, "scanner_lag"); !found || ok {
		t.Fatalf("scanner 900 blocks behind must fail scanner_lag: found=%v ok=%v %s", found, ok, detail)
	}
	if _, detail, found := check(h, "disk"); !found || detail == "" {
		t.Fatal("disk check missing") // its verdict depends on the machine
	}
	if h.PinStatus != "matches" {
		t.Fatalf("pin status with app_version 10: %s", h.PinStatus)
	}
	present := 0
	for _, c := range h.Components {
		if c.Present {
			present++
		}
	}
	if present != 3 || len(h.Components) != 4 {
		t.Fatalf("components: %+v", h.Components)
	}

	// /v1/meta carries the same verdict.
	var meta struct {
		Health    string `json:"health"`
		PinStatus string `json:"pin_status"`
	}
	if code := get(t, ts, "/v1/meta", &meta); code != 200 {
		t.Fatalf("meta: %d", code)
	}
	if meta.Health != "degraded" || meta.PinStatus != "matches" {
		t.Fatalf("meta: %+v", meta)
	}

	// Chain ahead of the pin is a health failure with a named check.
	if err := st.SetMeta("app_version", "11", time.Now()); err != nil {
		t.Fatal(err)
	}
	getAny(t, ts, "/v1/health", &h)
	if ok, _, found := check(h, "pin"); !found || ok || h.PinStatus != "chain_ahead" {
		t.Fatalf("pin check: found=%v ok=%v status=%s", found, ok, h.PinStatus)
	}
}

func TestHealthWithoutDataDirIsDown(t *testing.T) {
	ts := serverWithSample(t)
	var h healthBody
	if code := getAny(t, ts, "/v1/health", &h); code != 503 || h.Status != "down" {
		t.Fatalf("no status files: code=%d status=%s", code, h.Status)
	}
}

// insertUnassignable writes one publication the scanner could not assign,
// settled at height h and time at.
func insertUnassignable(t *testing.T, st *store.Store, idx int, h int64, at time.Time) {
	t.Helper()
	hash := fmt.Sprintf("%064x", 0xbad000+idx)
	if _, err := st.DB().Exec(`INSERT INTO publications (
		promise_hash, commitment, blob_version, blob_size, namespace, chain_id,
		promise_height, creation_timestamp, signer, signer_public_key,
		validator_signature_count, settlement_height, settlement_time,
		settlement_tx_hash, settlement_tx_index, settlement_tx_code,
		must_serve_until, must_serve_until_basis, shard_retention_s,
		payment_promise_timeout_s, assignment_error, validator_set_height,
		total_voting_power, sigma_rows, distinct_rows, wrap_overlaps,
		validators_with_rows, recorded_at, raw_json
	) VALUES (?,?,99,1024,'ns','test',?,?,'signer','pk',0,?,?,'tx',0,0,?,'shard_retention',7200,3600,
		'unknown blob version 99',?,0,0,0,0,0,?,'{}')`,
		hash, hash, h-1, store.TS(at), h, store.TS(at), store.TS(at.Add(2*time.Hour)), h, store.TS(at)); err != nil {
		t.Fatal(err)
	}
}

// One publication nobody could assign, long ago, must not hold health at
// 503 forever: nothing ever clears assignment_error, so an all-time count
// latches "degraded" and masks every later failure. An old one is listed,
// passing, with its count; a recent one fails.
func TestHealthUnassignableAgesOut(t *testing.T) {
	dir := t.TempDir()
	st, err := store.Open(filepath.Join(dir, "observer.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	// A live collector at a tip far above the old publication, so the
	// height prefilter is in play: the recent one must still fall inside it.
	const tip = 1_000_000
	cw := status.New(dir, "collector", "test", "t")
	cw.Start()
	cw.OK()
	cw.Progress(tip)
	defer cw.Stop("test")
	time.Sleep(1200 * time.Millisecond) // the delayed status write

	srv := api.NewWithVantage(st, api.VantageInfo{Name: "test"}, nil, api.WithDataDir(dir))
	ts := httptest.NewServer(srv)
	defer func() { ts.Close(); srv.Close() }()

	var h healthBody
	getAny(t, ts, "/v1/health", &h)
	if _, detail, found := check(h, "unassignable_publications"); found {
		t.Fatalf("no unassignable publications, but the check is listed: %s", detail)
	}

	// Settled two days and 30,000 blocks ago: history, not a failure.
	insertUnassignable(t, st, 1, tip-30_000, time.Now().Add(-48*time.Hour))
	getAny(t, ts, "/v1/health", &h)
	ok, detail, found := check(h, "unassignable_publications")
	if !found || !ok {
		t.Fatalf("an old unassignable publication must pass: found=%v ok=%v %s", found, ok, detail)
	}
	if !strings.Contains(detail, "1 older") {
		t.Fatalf("the all-time count must stay visible: %q", detail)
	}

	// Settled an hour ago: still happening, so health fails, and says both.
	insertUnassignable(t, st, 2, tip-600, time.Now().Add(-time.Hour))
	getAny(t, ts, "/v1/health", &h)
	ok, detail, found = check(h, "unassignable_publications")
	if !found || ok {
		t.Fatalf("a recent unassignable publication must fail: found=%v ok=%v %s", found, ok, detail)
	}
	if !strings.HasPrefix(detail, "1 publication(s) settled in the last 24h") || !strings.Contains(detail, "2 all time") {
		t.Fatalf("detail: %q", detail)
	}
	if h.Status == "ok" {
		t.Fatal("a failing check must not leave health ok")
	}
}
