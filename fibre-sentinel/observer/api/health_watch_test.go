package api_test

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/status"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/api"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/hosting"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
)

// healthFixture is a store and a data directory whose status files a test
// writes whole, so a component's last OK can be any age while its file is
// fresh: the shape of a loop that is stuck while its status goroutine goes
// on rewriting the file.
type healthFixture struct {
	t   *testing.T
	dir string
	st  *store.Store
	now time.Time
	// srv is the server server() started last.
	srv *api.Server
}

func newHealthFixture(t *testing.T) *healthFixture {
	t.Helper()
	dir := t.TempDir()
	st, err := store.Open(filepath.Join(dir, "observer.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	return &healthFixture{t: t, dir: dir, st: st, now: time.Now().UTC()}
}

// report writes one component's status file, refreshed now.
func (f *healthFixture) report(r status.Report) {
	f.t.Helper()
	if r.StartedAt.IsZero() {
		r.StartedAt = f.now.Add(-time.Hour)
	}
	r.UpdatedAt = f.now
	if r.Detail == nil {
		r.Detail = map[string]any{}
	}
	b, err := json.Marshal(r)
	if err != nil {
		f.t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(f.dir, "status"), 0o755); err != nil {
		f.t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(f.dir, "status", r.Component+".json"), b, 0o644); err != nil {
		f.t.Fatal(err)
	}
}

// healthy writes every expected component alive with a cycle just done,
// and the collector's chain polls just made.
func (f *healthFixture) healthy() {
	ok := f.now.Add(-10 * time.Second)
	for _, c := range []string{"scanner", "prober", "heartbeat", "collector"} {
		f.report(status.Report{Component: c, OK: true, LastOKAt: &ok, Height: 1000, Detail: map[string]any{status.CadenceKey: 60}})
	}
	for _, k := range []string{"chain_status_polled_at", "endpoints_polled_at", "escrow_polled_at", "identities_polled_at"} {
		f.meta(k, store.TS(ok))
	}
}

func (f *healthFixture) meta(k, v string) {
	f.t.Helper()
	if err := f.st.SetMeta(k, v, f.now); err != nil {
		f.t.Fatal(err)
	}
}

func (f *healthFixture) server(opts ...api.Option) *httptest.Server {
	srv := api.NewWithVantage(f.st, api.VantageInfo{Name: "ut-1"}, nil, append([]api.Option{api.WithDataDir(f.dir)}, opts...)...)
	ts := httptest.NewServer(srv)
	f.t.Cleanup(func() { ts.Close(); srv.Close() })
	f.srv = srv
	return ts
}

// testClock is a clock a test moves while the server reads it.
type testClock struct{ ns atomic.Int64 }

func newTestClock(at time.Time) *testClock {
	c := &testClock{}
	c.ns.Store(at.UnixNano())
	return c
}

func (c *testClock) now() time.Time      { return time.Unix(0, c.ns.Load()).UTC() }
func (c *testClock) add(d time.Duration) { c.ns.Add(int64(d)) }

func (f *healthFixture) health(ts *httptest.Server) healthBody {
	f.t.Helper()
	var h healthBody
	getAny(f.t, ts, "/v1/health", &h)
	return h
}

func ago(d time.Duration) *time.Time {
	t := time.Now().UTC().Add(-d)
	return &t
}

// Incident 2: the collector's loop deadlocked on its store connection while
// its status goroutine kept the file fresh. Every check read "alive" for as
// long as it was stuck. A component that states its cadence now fails once
// its last completed cycle is three cadences old (at least three minutes),
// with the file fresh and no error written; one that states none (an older
// build) is judged as before.
func TestHealthFailsAStuckLoopWhoseFileIsFresh(t *testing.T) {
	f := newHealthFixture(t)
	f.healthy()
	f.report(status.Report{Component: "collector", OK: true, LastOKAt: ago(25 * time.Minute), Height: 1000,
		Detail: map[string]any{status.CadenceKey: 60}})
	// stated no cadence, an hour without a cycle: today's rule, passing
	f.report(status.Report{Component: "heartbeat", OK: true, LastOKAt: ago(time.Hour)})
	ts := f.server()
	h := f.health(ts)
	ok, detail, found := check(h, "collector")
	if !found || ok || !strings.HasPrefix(detail, "no completed cycle for 25m") {
		t.Fatalf("collector: found=%v ok=%v %q", found, ok, detail)
	}
	if ok, detail, _ := check(h, "heartbeat"); !ok {
		t.Fatalf("a component that states no cadence keeps today's rule: %q", detail)
	}
	if h.Status != "degraded" {
		t.Fatalf("status %s", h.Status)
	}
	// A cycle within the bound passes.
	f.report(status.Report{Component: "collector", OK: true, LastOKAt: ago(2 * time.Minute), Height: 1000,
		Detail: map[string]any{status.CadenceKey: 60}})
	if ok, detail, _ := check(f.health(ts), "collector"); !ok {
		t.Fatalf("a collector two minutes after its last cycle: %q", detail)
	}
	// Never a cycle since its start, twenty minutes ago.
	f.report(status.Report{Component: "collector", OK: false, StartedAt: f.now.Add(-20 * time.Minute),
		Detail: map[string]any{status.CadenceKey: 60}})
	if ok, detail, _ := check(f.health(ts), "collector"); ok || !strings.HasPrefix(detail, "no completed cycle for 20m") {
		t.Fatalf("a collector with no cycle since its start: ok=%v %q", ok, detail)
	}
	// Its loop runs and every cycle fails: the failing stage is named,
	// never the error itself.
	f.report(status.Report{Component: "collector", OK: false, LastOKAt: ago(10 * time.Minute),
		LastError: "chain status: dial tcp 127.0.0.1:26657: connection refused", LastErrorAt: ago(30 * time.Second),
		Detail: map[string]any{status.CadenceKey: 60}})
	if ok, detail, _ := check(f.health(ts), "collector"); ok || !strings.HasPrefix(detail, "no completed cycle for 10m") ||
		!strings.HasSuffix(detail, "; failing: chain status") {
		t.Fatalf("a collector failing every cycle: ok=%v %q", ok, detail)
	}
	// One error written, then the loop hung: stuck, not failing.
	f.report(status.Report{Component: "collector", OK: false, LastOKAt: ago(10 * time.Minute),
		LastError: "chain status: timeout", LastErrorAt: ago(9 * time.Minute),
		Detail: map[string]any{status.CadenceKey: 60}})
	if ok, detail, _ := check(f.health(ts), "collector"); ok || !strings.HasPrefix(detail, "no completed cycle for 10m") || strings.Contains(detail, "failing") {
		t.Fatalf("a collector hung after one error: ok=%v %q", ok, detail)
	}
}

// The scanner completes a cycle per block. With the chain halted it has
// none to read: that is chain_liveness failing, not the scanner stuck.
// With newer blocks than its last cycle, it is stuck.
func TestHealthTellsAScannerWaitingAtTheTipFromAStuckOne(t *testing.T) {
	f := newHealthFixture(t)
	f.healthy()
	lastOK := ago(12 * time.Minute)
	f.report(status.Report{Component: "scanner", OK: true, LastOKAt: lastOK, Height: 1000,
		Detail: map[string]any{status.CadenceKey: 60, "chain_tip": 1000, "tip_block_time": lastOK.Add(-3 * time.Second)}})
	f.meta("chain_tip_time", store.TS(lastOK.Add(-3*time.Second)))
	ts := f.server()
	h := f.health(ts)
	if ok, detail, _ := check(h, "scanner"); !ok {
		t.Fatalf("a scanner waiting for a block of a halted chain: %q", detail)
	}
	if ok, _, found := check(h, "chain_liveness"); !found || ok {
		t.Fatal("a halted chain must fail chain_liveness")
	}
	// The collector saw a block a minute ago: the scanner is the one stuck.
	f.meta("chain_tip_time", store.TS(f.now.Add(-time.Minute)))
	h = f.health(ts)
	if ok, detail, _ := check(h, "scanner"); ok || !strings.HasPrefix(detail, "no completed cycle") {
		t.Fatalf("a scanner stuck under a live chain: ok=%v %q", ok, detail)
	}
	if ok, detail, _ := check(h, "chain_liveness"); !ok {
		t.Fatalf("the chain is live: %q", detail)
	}
}

// A stuck collector froze chain_tip_time, and after ten minutes the only
// failing check blamed the chain while the scanner, in the same answer,
// went on reading blocks. The chain's liveness now takes the newer of the
// collector's view and the scanner's own.
func TestHealthDoesNotBlameTheChainForAStuckCollector(t *testing.T) {
	f := newHealthFixture(t)
	f.healthy()
	f.meta("chain_tip_time", store.TS(f.now.Add(-30*time.Minute)))
	f.report(status.Report{Component: "scanner", OK: true, LastOKAt: ago(5 * time.Second), Height: 1300,
		Detail: map[string]any{status.CadenceKey: 60, "chain_tip": 1300, "tip_block_time": f.now.Add(-6 * time.Second)}})
	f.report(status.Report{Component: "collector", OK: true, LastOKAt: ago(30 * time.Minute), Height: 1000,
		Detail: map[string]any{status.CadenceKey: 60}})
	h := f.health(f.server())
	if ok, detail, _ := check(h, "chain_liveness"); !ok {
		t.Fatalf("chain_liveness blames the chain for a stuck collector: %q", detail)
	}
	if ok, _, _ := check(h, "collector"); ok {
		t.Fatal("the stuck collector must fail its own check")
	}
	if ok, detail, _ := check(h, "scanner_lag"); !ok || !strings.Contains(detail, "(0 blocks behind)") {
		t.Fatalf("scanner_lag with the scanner at the tip: ok=%v %q", ok, detail)
	}
}

// Every reading failing (a coder or client-order error after a params
// change) wrote each blob as this observer's own gap while the prober's
// check stayed green: the planning loop's next OK overwrote each error.
func TestHealthFailsAProberWhoseReadingsAllFail(t *testing.T) {
	f := newHealthFixture(t)
	f.healthy()
	f.report(status.Report{Component: "prober", OK: true, LastOKAt: ago(5 * time.Second),
		Detail: map[string]any{status.CadenceKey: 90, "reading_errors_15m": 7, "readings_15m": 0}})
	ts := f.server()
	ok, detail, _ := check(f.health(ts), "prober")
	if ok || !strings.HasPrefix(detail, "alive but failing: 7 reading(s) failed in the last 15m") {
		t.Fatalf("prober: ok=%v %q", ok, detail)
	}
	f.report(status.Report{Component: "prober", OK: true, LastOKAt: ago(5 * time.Second),
		Detail: map[string]any{status.CadenceKey: 90, "reading_errors_15m": 7, "readings_15m": 2}})
	if ok, detail, _ := check(f.health(ts), "prober"); !ok {
		t.Fatalf("some readings made beside the failures: %q", detail)
	}
}

// The collector's work beside its chain poll (the export, the registry,
// the holds) fails health once a stage has failed for ten minutes; the
// poll's OK every minute no longer hides it.
func TestHealthFailsCollectorWorkThatKeepsFailing(t *testing.T) {
	f := newHealthFixture(t)
	f.healthy()
	f.report(status.Report{Component: "collector", OK: true, LastOKAt: ago(5 * time.Second), Height: 1000,
		Detail: map[string]any{status.CadenceKey: 60, "work_errors": map[string]any{
			"export": map[string]any{"error": "export: /var/lib/x/exports has no index.json", "since": f.now.Add(-52 * time.Minute).Format(time.RFC3339)},
			"holds":  map[string]any{"error": "holds: database is locked", "since": f.now.Add(-2 * time.Minute).Format(time.RFC3339)},
		}}})
	ts := f.server()
	ok, detail, found := check(f.health(ts), "work")
	if !found || ok || detail != "export failing for 52m0s" {
		t.Fatalf("work: found=%v ok=%v %q", found, ok, detail)
	}
	f.report(status.Report{Component: "collector", OK: true, LastOKAt: ago(5 * time.Second), Height: 1000,
		Detail: map[string]any{status.CadenceKey: 60, "work_errors": map[string]any{
			"holds": map[string]any{"error": "holds: database is locked", "since": f.now.Add(-2 * time.Minute).Format(time.RFC3339)},
		}}})
	if ok, detail, _ := check(f.health(ts), "work"); !ok || !strings.Contains(detail, "holds") {
		t.Fatalf("a stage failing for two minutes: ok=%v %q", ok, detail)
	}
}

// /v1/health and /v1/meta are public: they say which stage failed and how
// long ago, and keep the host's internals (the raw error, with its paths,
// RPC addresses and SQL; the disk's size; the build) to the host.
// healthwatch reads only status and the checks' names and details.
func TestHealthPublishesNoHostInternals(t *testing.T) {
	f := newHealthFixture(t)
	f.healthy()
	f.report(status.Report{Component: "collector", Version: "1caeda46cf87", OK: false,
		LastOKAt: ago(20 * time.Minute), LastErrorAt: ago(10 * time.Second),
		LastError: `chain status: Post "http://127.0.0.1:26657": dial tcp 127.0.0.1:26657: connection refused`,
		Disk:      &status.Disk{FreeBytes: 493777162240, TotalBytes: 948334632960, FreeShare: 0.52},
		Detail:    map[string]any{"clock_offset_ms": 3307}})
	f.meta("scan_gaps", `[{"from":10,"to":12,"reason":"block_results unavailable","last_error":"Post \"http://127.0.0.1:26657\": EOF","at":"2026-10-01T00:00:00Z"}]`)
	ts := f.server()
	// A write of the day partials that failed: its error names the files
	// under the data directory.
	f.srv.FailDayPartsSave(&os.PathError{Op: "open", Path: filepath.Join(f.dir, "snapshots", "day-partials.json.tmp"), Err: errors.New("no space left on device")})
	get := func(route string) (map[string]any, []byte) {
		t.Helper()
		resp, err := http.Get(ts.URL + route)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var raw map[string]any
		if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
			t.Fatal(err)
		}
		b, _ := json.Marshal(raw)
		for _, leak := range []string{"127.0.0.1", "26657", "1caeda46cf87", "493777162240", "948334632960", "GiB", "clock_offset", "last_error", "\"disk\":{", "hostname", "day-partials.json", "no space left"} {
			if strings.Contains(string(b), leak) {
				t.Errorf("%s carries %q: %s", route, leak, b)
			}
		}
		if gaps, _ := raw["scan_gaps"].([]any); len(gaps) != 1 || gaps[0].(map[string]any)["from"] != float64(10) {
			t.Errorf("%s: the scan gap itself must stay: %v", route, raw["scan_gaps"])
		}
		return raw, b
	}
	get("/v1/meta")
	raw, b := get("/v1/health")
	parts, _ := raw["day_partials"].(map[string]any)
	if parts["last_save_error"] == nil || parts["last_save_error_at"] == nil {
		t.Errorf("the failed write and its time must stay: %v", parts)
	}
	for _, c := range raw["components"].([]any) {
		for k := range c.(map[string]any) {
			switch k {
			case "component", "present", "alive", "ok", "age_s", "started_at":
			default:
				t.Errorf("component field %q is published", k)
			}
		}
	}
	var h healthBody
	_ = json.Unmarshal(b, &h)
	ok, detail, _ := check(h, "collector")
	if ok || detail != "alive but failing: chain status" {
		t.Fatalf("collector: ok=%v %q", ok, detail)
	}
	if _, detail, found := check(h, "disk"); found && !strings.HasSuffix(detail, "% free") {
		t.Fatalf("disk detail: %q", detail)
	}
}

// Lines that a call saw waiting in a record file, still there at the same
// offset ten minutes later: the collector has stopped ingesting, whatever
// its status file says. The wait is the lines' own, not the cursor's: a
// file the collector ingests rarely has a cursor that last moved long ago,
// and a line appended to it waits a pass like any other. A file with
// nothing waiting says nothing.
func TestHealthFailsWhenTheRecordStopsBeingIngested(t *testing.T) {
	f := newHealthFixture(t)
	f.healthy()
	file := filepath.Join(f.dir, "publications.jsonl")
	line := `{"x":1}` + "\n"
	if err := os.WriteFile(file, []byte(strings.Repeat(line, 4)), 0o644); err != nil {
		t.Fatal(err)
	}
	// A quiet file: its cursor last moved 25 minutes ago, and lines came
	// since, which the collector's next pass takes.
	if err := f.st.SetCursor(file, 16, 2, f.now.Add(-25*time.Minute)); err != nil {
		t.Fatal(err)
	}
	clock := newTestClock(f.now)
	ts := f.server(api.WithClock(clock.now))
	if ok, detail, found := check(f.health(ts), "ingest"); !found || !ok {
		t.Fatalf("a quiet cursor and fresh lines: found=%v ok=%v %q", found, ok, detail)
	}
	// The same lines, the cursor unmoved, eleven minutes on.
	clock.add(11 * time.Minute)
	ok, detail, _ := check(f.health(ts), "ingest")
	if ok || detail != "collector has ingested nothing for 36m0s; lines waiting in publications.jsonl" {
		t.Fatalf("ingest: ok=%v %q", ok, detail)
	}
	if strings.Contains(detail, f.dir) {
		t.Fatalf("the detail carries a path: %q", detail)
	}
	// A pass over a long backlog keeps its start time on the cursor: an
	// offset that moved between two calls is progress, lines still waiting
	// past it or not.
	if err := f.st.SetCursor(file, 24, 3, f.now.Add(-25*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if ok, detail, _ := check(f.health(ts), "ingest"); !ok {
		t.Fatalf("a cursor that moved: %q", detail)
	}
	// Everything ingested: nothing waits, however long the cursor stays.
	if err := f.st.SetCursor(file, 32, 4, f.now.Add(-25*time.Minute)); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if ok, detail, _ := check(f.health(ts), "ingest"); !ok {
			t.Fatalf("nothing waiting: %q", detail)
		}
		clock.add(11 * time.Minute)
	}
	// A line the collector never takes, and an API restarted meanwhile,
	// which has seen nothing wait: it says so once its own first call is
	// ten minutes old.
	if err := os.WriteFile(file, []byte(strings.Repeat(line, 5)), 0o644); err != nil {
		t.Fatal(err)
	}
	ts = f.server(api.WithClock(clock.now))
	if ok, detail, _ := check(f.health(ts), "ingest"); !ok {
		t.Fatalf("a restarted API's first call: %q", detail)
	}
	clock.add(9 * time.Minute)
	if ok, detail, _ := check(f.health(ts), "ingest"); !ok {
		t.Fatalf("nine minutes on: %q", detail)
	}
	clock.add(2 * time.Minute)
	if ok, detail, _ := check(f.health(ts), "ingest"); ok || !strings.Contains(detail, "lines waiting in publications.jsonl") {
		t.Fatalf("eleven minutes on: ok=%v %q", ok, detail)
	}
}

// One file stuck while another moves (a line the collector cannot get
// past) names the stuck one; with no record file readable from here, the
// cursors alone say how long nothing was ingested.
func TestHealthIngestNamesTheStuckFileAndFallsBackOnTheCursors(t *testing.T) {
	f := newHealthFixture(t)
	f.healthy()
	stuck := filepath.Join(f.dir, "measurements.jsonl")
	moving := filepath.Join(f.dir, "reachability.jsonl")
	for _, p := range []string{stuck, moving} {
		if err := os.WriteFile(p, []byte(strings.Repeat(`{"x":1}`+"\n", 3)), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if err := f.st.SetCursor(stuck, 8, 1, f.now.Add(-25*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := f.st.SetCursor(moving, 16, 2, f.now.Add(-time.Minute)); err != nil {
		t.Fatal(err)
	}
	clock := newTestClock(f.now)
	ts := f.server(api.WithClock(clock.now))
	if ok, detail, _ := check(f.health(ts), "ingest"); !ok {
		t.Fatalf("lines first seen waiting: %q", detail)
	}
	clock.add(11 * time.Minute)
	if err := f.st.SetCursor(moving, 24, 3, clock.now().Add(-time.Minute)); err != nil {
		t.Fatal(err)
	}
	ok, detail, _ := check(f.health(ts), "ingest")
	if ok || detail != "collector has ingested nothing for 11m0s from measurements.jsonl while lines wait there; other files are ingested" {
		t.Fatalf("ingest: ok=%v %q", ok, detail)
	}

	g := newHealthFixture(t)
	g.healthy()
	if err := g.st.SetCursor(filepath.Join(g.dir, "elsewhere", "measurements.jsonl"), 8, 1, g.now.Add(-40*time.Minute)); err != nil {
		t.Fatal(err)
	}
	ts = g.server()
	if ok, detail, _ := check(g.health(ts), "ingest"); ok || !strings.HasPrefix(detail, "collector has ingested nothing for 40m") {
		t.Fatalf("ingest blind, cursors 40 minutes old: ok=%v %q", ok, detail)
	}
	if err := g.st.SetCursor(filepath.Join(g.dir, "elsewhere", "measurements.jsonl"), 16, 2, g.now.Add(-5*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if ok, detail, _ := check(g.health(ts), "ingest"); !ok {
		t.Fatalf("ingest blind, a cursor five minutes old: %q", detail)
	}
}

// The chain-side polls keep their last value when they fail; health fails
// once one is older than its bound. The endpoint and escrow polls are not
// judged before Fibre is active.
func TestHealthFailsStaleChainPolls(t *testing.T) {
	f := newHealthFixture(t)
	f.healthy()
	f.meta("chain_status_polled_at", store.TS(f.now.Add(-time.Minute)))
	f.meta("endpoints_polled_at", store.TS(f.now.Add(-time.Minute)))
	f.meta("escrow_polled_at", store.TS(f.now.Add(-47*time.Minute)))
	f.meta("identities_polled_at", store.TS(f.now.Add(-3*time.Hour)))
	ts := f.server()
	if ok, detail, _ := check(f.health(ts), "chain_polls"); !ok {
		t.Fatalf("before Fibre is active escrow is not judged: %q", detail)
	}
	f.meta("fibre_active", "yes")
	ok, detail, found := check(f.health(ts), "chain_polls")
	if !found || ok || detail != "stale: escrow last polled 47m0s ago (bound 15m0s)" {
		t.Fatalf("chain_polls: found=%v ok=%v %q", found, ok, detail)
	}
	f.meta("escrow_polled_at", store.TS(f.now.Add(-4*time.Minute)))
	f.meta("identities_polled_at", store.TS(f.now.Add(-27*time.Hour)))
	if ok, detail, _ := check(f.health(ts), "chain_polls"); ok || !strings.Contains(detail, "identities last polled 27h0m0s ago") {
		t.Fatalf("stale identities: ok=%v %q", ok, detail)
	}
}

// A second vantage whose rows stop arriving (a pull failing every minute,
// a dead remote heartbeat) fails health; the observer's own vantage is the
// heartbeat check's, and one not seen for a week is no longer expected.
func TestHealthFailsASecondVantageThatStoppedArriving(t *testing.T) {
	f := newHealthFixture(t)
	f.healthy()
	v := &vantageFixture{t: t, st: f.st, now: f.now}
	v.beat("ut-1", "aa", "a.example:443", 2*time.Hour, true) // own: not this check's
	v.beat("old-1", "aa", "a.example:443", 8*24*time.Hour, true)
	v.beat("de-1", "aa", "a.example:443", 45*time.Minute, true)
	ts := f.server()
	ok, detail, found := check(f.health(ts), "vantages")
	if !found || ok || detail != "de-1: newest row 45m0s old (bound 20m0s)" {
		t.Fatalf("vantages: found=%v ok=%v %q", found, ok, detail)
	}
	v.beat("de-1", "aa", "a.example:443", 4*time.Minute, true)
	if ok, detail, _ := check(f.health(ts), "vantages"); !ok || detail != "de-1: newest row 4m0s old" {
		t.Fatalf("a vantage arriving: ok=%v %q", ok, detail)
	}
}

// Incident 1: one undecodable row made /v1/blobs answer 500 for the whole
// page, every request, with /v1/health at 200. A route's 5xx answers now
// fail api_errors for ten minutes, by route, without the error's text.
func TestHealthFailsWhileARouteAnswers500(t *testing.T) {
	f := newHealthFixture(t)
	f.healthy()
	ts := f.server()
	if ok, detail, found := check(f.health(ts), "api_errors"); !found || !ok {
		t.Fatalf("api_errors before any error: found=%v %q", found, detail)
	}
	if _, err := f.st.DB().Exec(`ALTER TABLE validator_avatars RENAME TO validator_avatars_gone`); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		resp, err := http.Get(ts.URL + "/v1/avatars/0123456789ABCDEF")
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != 500 {
			t.Fatalf("the avatar route answered %d, want 500", resp.StatusCode)
		}
	}
	ok, detail, _ := check(f.health(ts), "api_errors")
	if ok || !strings.HasPrefix(detail, "/v1/avatars/{identity}: 2 answer(s) 500 in the last 15m") {
		t.Fatalf("api_errors: ok=%v %q", ok, detail)
	}
	if strings.Contains(detail, "validator_avatars") || strings.Contains(detail, "no such table") {
		t.Fatalf("the detail carries the SQL error: %q", detail)
	}
}

// The hosting lookup's IP-to-ASN file is refreshed monthly; one past 45
// days fails, and its age is said either way.
func TestHealthFailsAStaleHostingDatabase(t *testing.T) {
	f := newHealthFixture(t)
	f.healthy()
	f.meta(hosting.MetaEnabled, "yes")
	f.meta(hosting.MetaASNDBModified, store.TS(f.now.Add(-60*24*time.Hour)))
	ts := f.server()
	ok, detail, found := check(f.health(ts), "hosting_db")
	if !found || ok || !strings.HasPrefix(detail, "the IP-to-ASN file is 60 days old") {
		t.Fatalf("hosting_db: found=%v ok=%v %q", found, ok, detail)
	}
	f.meta(hosting.MetaASNDBModified, store.TS(f.now.Add(-3*24*time.Hour)))
	if ok, detail, _ := check(f.health(ts), "hosting_db"); !ok {
		t.Fatalf("a three-day-old file: %q", detail)
	}
}
