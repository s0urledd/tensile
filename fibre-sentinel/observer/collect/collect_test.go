package collect_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/probe"
	"github.com/plsgiveup/fibre/fibre-sentinel/internal/scan"
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

// deferredRow stores a publication and one probe row of it the prober
// deferred on a scan gap, which a late verdict judges at once, and returns
// the row's dedupe key.
func deferredRow(t *testing.T, st *store.Store, at time.Time) string {
	t.Helper()
	pub := scan.Publication{
		SchemaVersion: scan.AttestationSchemaVersion, PromiseHash: "aaaa",
		SettlementHeight: 300, SettlementTime: at, MustServeUntil: at.Add(30 * time.Minute), RecordedAt: at,
		SettlementTxHash: "txaaaa", Signer: "celestia1pub",
		Promise:                 scan.PromiseFields{ChainID: "t", Height: 299, Commitment: "cc1", CreationTimestamp: at.Add(-time.Minute), BlobSize: 4096},
		ValidatorSignatureCount: 1,
		Assignment: scan.AssignmentTable{
			ProtocolParams:     scan.ProtocolParamsSnapshot{OriginalRows: 4, TotalRows: 16},
			ValidatorSetHeight: 299, TotalVotingPower: 10, Sigma: 4, Distinct: 4,
			ValidatorsWithRows: 1, AttestedWithRows: 1, SignatureEntries: 1, SignaturesVerified: 1, AttestedVotingPower: 10,
			Validators: []scan.ValidatorAssignment{{Address: "v1", VotingPower: 10, RowCount: 2, Rows: []int{0, 1}, Attested: true}},
		},
	}
	pub.ParamsAtPublication.PaymentPromiseTimeoutSeconds = 3600
	pub.ParamsAtPublication.ShardRetentionSeconds = 1800
	raw, err := json.Marshal(pub)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.UpsertPublication(pub, raw); err != nil {
		t.Fatal(err)
	}
	probeAt := at.Add(10 * time.Minute)
	m := probe.Measurement{
		SchemaVersion: probe.AttestationSchemaVersion, Vantage: "ut-1",
		PromiseHash: pub.PromiseHash, Commitment: pub.Promise.Commitment, MustServeUntil: pub.MustServeUntil, ValidatorSetHeight: 299,
		ValidatorAddress: "v1", ValidatorHost: "v1:443", Assigned: true, Attested: true, AssignedRowCount: 2,
		ScheduleLabel: "w2", ScheduledAt: probeAt, StartedAt: probeAt, FinishedAt: probeAt.Add(time.Second),
		Phase: probe.PhaseInWindow, Outcome: probe.OutcomeWrongRows, TotalDurationMS: 1000,
	}
	m.TCP.OK, m.TLS.OK, m.Identity.OK = true, true, true
	m.Download.Attempted, m.Download.RowsReturned, m.Download.RowsExpected = true, 1, 2
	m.Download.CommitmentVerified, m.Download.RowIndices = true, []uint32{9}
	m.Download.ShadowGap = probe.ShadowGapScanPrefix + " #100-#110 overlaps the shard lifetime"
	m.Classification, m.ClassificationReason = probe.Classify(probe.Evidence{Assigned: true, Attested: true, Phase: probe.PhaseInWindow,
		Outcome: probe.OutcomeWrongRows, CommitmentVerified: true, ShadowUncertain: true})
	if raw, err = json.Marshal(m); err != nil {
		t.Fatal(err)
	}
	if _, err := st.InsertProbe(m, raw); err != nil {
		t.Fatal(err)
	}
	return m.DedupeKey()
}

func amendedAt(t *testing.T, st *store.Store, key string) string {
	t.Helper()
	var at sql.NullString
	if err := st.DB().QueryRow(`SELECT amended_at FROM probes WHERE dedupe_key = ?`, key).Scan(&at); err != nil {
		t.Fatal(err)
	}
	return at.String
}

// A late verdict on record that the store does not have yet (its apply
// failed, or the replay stopped before it) is the one the row gets: no
// second verdict is drawn for the row while amendments.jsonl has not been
// replayed to its end. ApplyAmendment takes a row's first amendment, so a
// second line drawn live would be the row's verdict in the live store and
// the first line its verdict in every store rebuilt from the file.
func TestNoLateVerdictIsDrawnWhileOneOnRecordWaits(t *testing.T) {
	dir := t.TempDir()
	c := newCollector(t, dir, &fakeStatus{})
	ctx := context.Background()
	t0 := time.Date(2026, 10, 7, 10, 0, 0, 0, time.UTC)
	key := deferredRow(t, c.St, t0)
	now := t0.Add(2 * time.Hour)
	if err := c.St.SetMeta("last_scanned_time", store.TS(now), now); err != nil {
		t.Fatal(err)
	}
	// On record: a verdict for a row not ingested yet, which the replay
	// waits on for a few passes, and after it the row's own verdict, drawn
	// an hour ago and never applied.
	recorded := t0.Add(time.Hour)
	waiting, _ := json.Marshal(store.Amendment{DedupeKey: "not-ingested-yet", To: "PROBE_ERROR", JudgedAt: recorded})
	mine, _ := json.Marshal(store.Amendment{DedupeKey: key, PromiseHash: "aaaa", ValidatorAddress: "v1", From: "PROBE_ERROR", To: "PROBE_ERROR",
		Reason: "no verdict: a scan gap covers the interval", JudgedAt: recorded, ScannerFrontier: recorded})
	path := filepath.Join(dir, "amendments.jsonl")
	body := string(waiting) + "\n" + string(mine) + "\n"
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 4; i++ {
		c.Pass(ctx, now.Add(time.Duration(i)*10*time.Second))
		if got, _ := os.ReadFile(path); string(got) != body {
			t.Fatalf("pass %d drew a second verdict for a row whose verdict is on record:\n%s", i+1, got)
		}
	}
	if got := amendedAt(t, c.St, key); got != store.TS(recorded) {
		t.Fatalf("the row's verdict was drawn at %q, want the one on record (%s)", got, store.TS(recorded))
	}
}

// The corrector stops at the first line it cannot write, which a full disk
// leaves written in part; that part is cut before the corrector runs again,
// so its next line is not glued onto it.
func TestAPartCorrectionLineIsCutBeforeTheNextRun(t *testing.T) {
	dir := t.TempDir()
	c := newCollector(t, dir, &fakeStatus{})
	path := filepath.Join(dir, "corrections.jsonl")
	if err := os.WriteFile(path, []byte(`{"kind":"probe_verdict","dedupe_key":"ab`), 0o644); err != nil {
		t.Fatal(err)
	}
	c.Pass(context.Background(), time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC))
	if got, _ := os.ReadFile(path); len(got) != 0 {
		t.Fatalf("corrections.jsonl after the pass: %q", got)
	}
}
