package collect_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/observer/collect"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/correct"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/ingest"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
)

// fakeStatus is the status file, as far as the collector writes to it.
type fakeStatus struct {
	mu     sync.Mutex
	errs   []string
	detail map[string]any
	sets   int
}

func (f *fakeStatus) Error(msg string) { f.mu.Lock(); f.errs = append(f.errs, msg); f.mu.Unlock() }
func (f *fakeStatus) Set(key string, v any) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.detail == nil {
		f.detail = map[string]any{}
	}
	f.detail[key] = v
	f.sets++
}

func (f *fakeStatus) work() map[string]any {
	f.mu.Lock()
	defer f.mu.Unlock()
	w, _ := f.detail[collect.WorkErrorsKey].(map[string]any)
	return w
}

// A failing stage is listed with its error and the time it started failing;
// a later failure keeps that time, a success removes it, and the status
// file is written only when the list changes.
func TestTheWorkListKeepsWhenAStageStartedFailing(t *testing.T) {
	live := &fakeStatus{}
	w := collect.NewWorkErrors(live)
	if wl := live.work(); wl == nil || len(wl) != 0 {
		t.Fatalf("the empty list is not published at start: %v", live.detail)
	}
	t0 := time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC)
	w.Report("export", errors.New("index.json missing"), t0)
	w.Report("export", errors.New("index.json missing"), t0.Add(time.Minute))
	sets := live.sets
	w.Report("export", errors.New("index.json missing"), t0.Add(2*time.Minute))
	if live.sets != sets {
		t.Fatalf("an unchanged list was written again")
	}
	w.Report("export", errors.New("disk full"), t0.Add(3*time.Minute))
	w.Report("holds", nil, t0.Add(3*time.Minute))
	e, _ := live.work()["export"].(map[string]any)
	if e == nil || e["error"] != "disk full" || e["since"] != t0.Format(time.RFC3339) || len(live.work()) != 1 {
		t.Fatalf("work_errors = %v", live.work())
	}
	w.Report("export", nil, t0.Add(4*time.Minute))
	if len(live.work()) != 0 || !w.Since("export").IsZero() {
		t.Fatalf("a stage that succeeded is still listed: %v", live.work())
	}
	var nilList *collect.WorkErrors
	nilList.Report("export", errors.New("x"), t0) // a collector without a list records nothing
}

func newCollector(t *testing.T, dir string, live collect.Status) *collect.Collector {
	t.Helper()
	st, err := store.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	amend, err := os.OpenFile(filepath.Join(dir, "amendments.jsonl"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { amend.Close() })
	corr, err := os.OpenFile(filepath.Join(dir, "corrections.jsonl"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { corr.Close() })
	return collect.New(collect.Collector{St: st, Paths: collect.DefaultPaths(dir), Vantage: "ut-1", Live: live,
		AmendFile: amend, PruneTolerance: 5 * time.Minute, Corrector: correct.New(st, corr, 5*time.Minute),
		Work: collect.NewWorkErrors(live)})
}

// The endpoint poll waits on the registry replay alone: a registry line the
// store refuses says the replay is not done; a stuck line in another file
// does not.
func TestThePassSaysWhetherTheRegistryWasReplayed(t *testing.T) {
	dir := t.TempDir()
	c := newCollector(t, dir, &fakeStatus{})
	ctx := context.Background()
	now := time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC)
	reg := filepath.Join(dir, "registry.jsonl")
	if err := os.WriteFile(reg, []byte(`{"kind":"endpoint_moved","validator_cons_address":"celestiavalcons1aa","host":"a:7980","height":5,"at":"2026-10-07T11:00:00Z"}`+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if errs := c.Pass(ctx, now); len(errs) == 0 || c.RegistryReplayed() {
		t.Fatalf("a refused registry line: errs=%v replayed=%v", errs, c.RegistryReplayed())
	}
	if err := os.WriteFile(reg, []byte(`{"kind":"endpoint_opened","validator_cons_address":"celestiavalcons1aa","host":"a:7980","height":5,"at":"2026-10-07T11:00:00Z"}`+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	// A payment line the store refuses, in a file the endpoints do not
	// come from: the pass fails, the registry is replayed all the same.
	if err := os.WriteFile(filepath.Join(dir, "payments.jsonl"), []byte(`{"dedupe_key":"x","publisher":"celestia1aa"}`+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if errs := c.Pass(ctx, now.Add(10*time.Second)); len(errs) == 0 || !c.RegistryReplayed() {
		t.Fatalf("a refused payment: errs=%v replayed=%v", errs, c.RegistryReplayed())
	}
}

// A vantage file stopped at a row from another chain is named after the
// pass, so the collector can hold the exports, which copy the vantage files
// whole; once the file is gone, nothing is named.
func TestThePassNamesAVantageFileFromAnotherChain(t *testing.T) {
	dir := t.TempDir()
	c := newCollector(t, dir, &fakeStatus{})
	ctx := context.Background()
	now := time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC)
	vf := filepath.Join(dir, ingest.VantagesDir, "mo-1", "reachability.jsonl")
	if err := os.MkdirAll(filepath.Dir(vf), 0o755); err != nil {
		t.Fatal(err)
	}
	// A validator this chain has no record of.
	line := `{"vantage":"mo-1","validator_address":"aa","validator_host":"h:7980","scheduled_at":"2026-10-07T11:00:00Z","started_at":"2026-10-07T11:00:00Z","outcome":"REACHABLE"}`
	if err := os.WriteFile(vf, []byte(line+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if errs := c.Pass(ctx, now); len(errs) == 0 || len(c.VantagesFromOtherChain()) != 1 || c.VantagesFromOtherChain()[0] != "mo-1" {
		t.Fatalf("errs=%v other chain=%v", errs, c.VantagesFromOtherChain())
	}
	if err := os.RemoveAll(filepath.Dir(vf)); err != nil {
		t.Fatal(err)
	}
	if errs := c.Pass(ctx, now.Add(10*time.Second)); len(errs) != 0 || len(c.VantagesFromOtherChain()) != 0 {
		t.Fatalf("after the file went: errs=%v other chain=%v", errs, c.VantagesFromOtherChain())
	}
}

// Work that fails after the ingest goes on the work list, while the pass
// itself reports a clean ingest, and leaves it when it next succeeds.
func TestAFailureAfterTheIngestIsOnTheWorkList(t *testing.T) {
	dir := t.TempDir()
	live := &fakeStatus{}
	c := newCollector(t, dir, live)
	ctx := context.Background()
	now := time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC)
	// A frontier the late verdicts cannot read, and a state.json that does
	// not decode.
	if err := c.St.SetMeta("last_scanned_time", "yesterday", now); err != nil {
		t.Fatal(err)
	}
	state := filepath.Join(dir, "state.json")
	if err := os.WriteFile(state, []byte("{not json"), 0o644); err != nil {
		t.Fatal(err)
	}
	if errs := c.Pass(ctx, now); len(errs) != 0 {
		t.Fatalf("ingest errors: %v", errs)
	}
	w := live.work()
	if w["late verdicts"] == nil || w["state"] == nil {
		t.Fatalf("work_errors = %v, want late verdicts and state", w)
	}
	// The scanner writes a good state.json, frontier included: both clear.
	if err := os.WriteFile(state, []byte(`{"chain_id":"test-1","last_scanned_height":10,"last_scanned_time":"2026-10-07T11:59:00Z"}`), 0o644); err != nil {
		t.Fatal(err)
	}
	c.Pass(ctx, now.Add(10*time.Second))
	if w := live.work(); len(w) != 0 {
		t.Fatalf("work_errors after recovery = %v", w)
	}
}
