package api_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/probe"
	"github.com/plsgiveup/fibre/fibre-sentinel/internal/scan"
	"github.com/plsgiveup/fibre/fibre-sentinel/internal/status"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/api"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/ingest"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
)

// A pinned window (?as_of=) answers what the observer would have published
// at that moment from the rows it had by then: rows started later are
// left out, an obligation whose deadline is after as_of is pending, and
// the answer is uncached and rationed.
func TestAsOfPinsTheWindow(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "observer.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	now := time.Now().UTC().Truncate(time.Second)
	created, msu := now.Add(-2*time.Hour), now.Add(-30*time.Minute)
	// four validators, each served at w1 and w2; at w3 and w4 three of them
	// did not, and the blob, which needs every row, was Unavailable there
	profile := map[string][]wire{
		"v1": {ok, ok, err500, err500}, "v2": {ok, ok, err500, err500},
		"v3": {ok, ok, gone, gone}, "v4": {ok, ok, ok, ok},
	}
	insertProbeSet(t, st, "asof1", created, msu, profile)
	ts := httptestServer(t, st)

	type resp struct {
		Window struct {
			End  time.Time `json:"end"`
			AsOf bool      `json:"as_of"`
		} `json:"window"`
		AsOfNote    string          `json:"as_of_note"`
		ProbeCount  int64           `json:"probe_count"`
		Obligations obligationsJSON `json:"obligations"`
	}
	// pinned between w2 and w3: two rows per validator, every obligation
	// still pending
	var pinned resp
	at := afterPoint(created, msu, 1).Format(time.RFC3339)
	if code := get(t, ts, "/v1/network?window=24h&as_of="+at, &pinned); code != 200 {
		t.Fatalf("as_of: %d", code)
	}
	if !pinned.Window.AsOf || pinned.AsOfNote == "" {
		t.Errorf("pinned window not marked: %+v", pinned.Window)
	}
	if pinned.ProbeCount != 8 {
		t.Errorf("probe_count at as_of = %d, want 8 (two points of four validators)", pinned.ProbeCount)
	}
	if pinned.Obligations.Pending != 4 || pinned.Obligations.Served != 0 || pinned.Obligations.Broken != 0 {
		t.Errorf("obligations at as_of = %+v, want 4 pending", pinned.Obligations)
	}
	// pinned at now: the same answer as the live window
	var live, pinnedNow resp
	get(t, ts, "/v1/network?window=24h", &live)
	if code := get(t, ts, "/v1/network?window=24h&as_of="+now.Format(time.RFC3339), &pinnedNow); code != 200 {
		t.Fatalf("as_of now: %d", code)
	}
	if pinnedNow.ProbeCount != live.ProbeCount || pinnedNow.Obligations != live.Obligations {
		t.Errorf("pinned at now differs from live:\n%+v\n%+v", pinnedNow, live)
	}
	if live.ProbeCount != 16 || live.Obligations.Served != 1 || live.Obligations.Broken != 3 || live.Obligations.NotCounted != 0 {
		t.Errorf("live = %+v", live)
	}
	// validators too
	var vals struct {
		AsOfNote   string `json:"as_of_note"`
		Validators []struct {
			Address     string           `json:"address"`
			Obligations obligationsJSON  `json:"obligations"`
			ProbeCount  int64            `json:"probe_count"`
			Faults      int64            `json:"faults"`
			Classes     map[string]int64 `json:"classes"`
		} `json:"validators"`
	}
	if code := get(t, ts, "/v1/validators?window=24h&as_of="+at, &vals); code != 200 {
		t.Fatalf("validators as_of: %d", code)
	}
	if vals.AsOfNote == "" || len(vals.Validators) != 4 {
		t.Fatalf("validators as_of = %+v", vals)
	}
	for _, v := range vals.Validators {
		if v.Obligations.Pending != 1 {
			t.Errorf("%s at as_of: %+v, want pending", v.Address, v.Obligations)
		}
		// two served points by then; the later failures are not yet there
		if v.ProbeCount != 2 || v.Faults != 0 || v.Classes["HEALTHY"] != 2 || len(v.Classes) != 1 {
			t.Errorf("%s at as_of: probes=%d faults=%d classes=%v, want 2 HEALTHY rows only", v.Address, v.ProbeCount, v.Faults, v.Classes)
		}
	}
	// uncached, and rationed: three pinned requests are spent, the burst
	// is four, so the second of these must be refused
	var limited bool
	for i := 0; i < 2 && !limited; i++ {
		r, err := http.Get(ts.URL + "/v1/network?window=24h&as_of=" + at)
		if err != nil {
			t.Fatal(err)
		}
		r.Body.Close()
		if i == 0 && r.Header.Get("Cache-Control") != "no-store" {
			t.Errorf("pinned answer Cache-Control = %q, want no-store", r.Header.Get("Cache-Control"))
		}
		if r.StatusCode == 429 {
			limited = r.Header.Get("Retry-After") != ""
		}
	}
	if !limited {
		t.Error("pinned requests past the burst were not refused with 429 and Retry-After")
	}
	// a future as_of is refused
	if code := get(t, ts, "/v1/network?as_of="+now.Add(time.Hour).Format(time.RFC3339), nil); code != 400 {
		t.Errorf("future as_of: %d, want 400", code)
	}
}

// Every component's starts and stops, with the configuration it ran under,
// come from runs.jsonl and are served at /v1/runs.
func TestRunsCarryTheirConfiguration(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "observer.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	dir := t.TempDir()
	// a prober writing through the status package
	w := status.New(dir, "prober", "eu", "abc123")
	w.RecordRuns(map[string]any{"prune-tolerance": "2m30s", "in-window-probes": "4"})
	w.Start()
	w.Stop("signal")
	// and a second start of the same component, still running
	w2 := status.New(dir, "prober", "eu", "abc124")
	w2.RecordRuns(map[string]any{"prune-tolerance": "3m0s"})
	w2.Start()
	t.Cleanup(func() { w2.Stop("test") })

	now := time.Now()
	res, err := ingest.Runs(st, filepath.Join(dir, status.RunsFile), now)
	if err != nil {
		t.Fatal(err)
	}
	if res.Inserted != 3 { // two starts, one stop applied
		t.Fatalf("replayed %d events, want 3 (%+v)", res.Inserted, res)
	}
	// replaying again changes nothing
	if res, err := ingest.Runs(st, filepath.Join(dir, status.RunsFile), now); err != nil || res.Inserted != 0 {
		t.Fatalf("second replay: %+v %v", res, err)
	}
	ts := httptestServer(t, st)
	var out struct {
		Runs []struct {
			Component  string          `json:"component"`
			Version    string          `json:"version"`
			StoppedAt  *string         `json:"stopped_at"`
			StopReason *string         `json:"stop_reason"`
			PID        *int64          `json:"pid"`
			Config     json.RawMessage `json:"config"`
		} `json:"runs"`
	}
	if code := get(t, ts, "/v1/runs?window=all", &out); code != 200 {
		t.Fatalf("runs: %d", code)
	}
	var stopped, open int
	for _, r := range out.Runs {
		if r.Component != "prober" {
			continue
		}
		var cfg map[string]string
		if err := json.Unmarshal(r.Config, &cfg); err != nil || cfg["prune-tolerance"] == "" {
			t.Errorf("run %s config = %s", r.Version, r.Config)
		}
		if r.PID == nil {
			t.Errorf("run %s has no pid", r.Version)
		}
		switch {
		case r.Version == "abc123" && r.StoppedAt != nil && r.StopReason != nil && *r.StopReason == "signal":
			stopped++
		case r.Version == "abc124" && r.StoppedAt == nil:
			open++
		default:
			t.Errorf("unexpected run row: %+v", r)
		}
	}
	if stopped != 1 || open != 1 {
		t.Errorf("stopped=%d open=%d, want 1 and 1: %+v", stopped, open, out.Runs)
	}
}

// A revealed day secret is served beside the day's commitment, and the
// endpoint says how many days are revealed.
func TestSamplingServesRevealedSecrets(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "observer.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	now := time.Now().UTC().Truncate(time.Second)
	created, msu := now.Add(-2*time.Hour), now.Add(-30*time.Minute)
	insertProbeSet(t, st, "smp1", created, msu, map[string][]wire{"v1": {ok}, "v2": {ok}, "v3": {ok}})
	// stamp the rows with a sampling decision under two day commitments
	for i, c := range []string{"commitA", "commitB"} {
		m := probe.Measurement{
			SchemaVersion: probe.AttestationSchemaVersion, Vantage: "test", PromiseHash: "smp" + string(rune('2'+i)),
			Commitment: "cc", MustServeUntil: msu, ValidatorAddress: "v1", ValidatorHost: "v1:443", Assigned: true, Attested: true,
			ScheduleLabel: "w1", ScheduledAt: created, StartedAt: created, FinishedAt: created, Phase: probe.PhaseInWindow,
			Outcome: probe.OutcomeMissed, Classification: probe.ClassNotProbed, ClassificationReason: "budget:p=0.500:global_bytes_per_hour:day_commitment=" + c,
			Sampling: &probe.SamplingDecision{P: 0.5, Binding: "global_bytes_per_hour", DayCommitment: c},
		}
		raw, _ := json.Marshal(m)
		if _, err := st.InsertProbe(m, raw); err != nil {
			t.Fatal(err)
		}
	}
	dir := t.TempDir()
	line := `{"day":"2026-09-01","commitment":"commitA","secret":"00ff","revealed_at":"2026-09-08T00:00:00Z"}` + "\n"
	if err := os.WriteFile(filepath.Join(dir, "sampling-secrets.jsonl"), []byte(line), 0o644); err != nil {
		t.Fatal(err)
	}
	if res, err := ingest.SamplingSecrets(st, filepath.Join(dir, "sampling-secrets.jsonl"), now); err != nil || res.Inserted != 1 {
		t.Fatalf("ingest: %+v %v", res, err)
	}
	ts := httptestServer(t, st)
	var out struct {
		Decisions []struct {
			DayCommitment string  `json:"day_commitment"`
			Day           *string `json:"day"`
			Secret        *string `json:"secret"`
		} `json:"decisions"`
		Revealed  int  `json:"secrets_revealed"`
		Published bool `json:"secret_published"`
	}
	if code := get(t, ts, "/v1/sampling?window=all", &out); code != 200 {
		t.Fatalf("sampling: %d", code)
	}
	if out.Revealed != 1 || !out.Published {
		t.Errorf("revealed=%d published=%v", out.Revealed, out.Published)
	}
	seen := map[string]bool{}
	for _, d := range out.Decisions {
		seen[d.DayCommitment] = true
		switch d.DayCommitment {
		case "commitA":
			if d.Secret == nil || *d.Secret != "00ff" || d.Day == nil || *d.Day != "2026-09-01" {
				t.Errorf("commitA not revealed: %+v", d)
			}
		case "commitB":
			if d.Secret != nil {
				t.Errorf("commitB must stay sealed: %+v", d)
			}
		}
	}
	if !seen["commitA"] || !seen["commitB"] {
		t.Errorf("decisions = %+v", out.Decisions)
	}
}

// The exports index is served, and only export files are reachable.
func TestExportsAreListedAndServed(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "observer.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	dataDir := t.TempDir()
	dir := filepath.Join(dataDir, "exports")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	name := "fibrescope-test-2026-09-10.tar.gz"
	if err := os.WriteFile(filepath.Join(dir, name), []byte("not really a tarball"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, name+".sha256"), []byte("abc  "+name+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "state.json"), []byte(`{"last_day":"2026-09-10"}`), 0o644); err != nil {
		t.Fatal(err)
	}
	index := `[{"name":"` + name + `","bytes":20,"sha256":"abc","vantage":"test","day":"2026-09-10","generated_at":"2026-09-11T03:00:00Z","build":"x","files":[],"rule":"r"}]`
	if err := os.WriteFile(filepath.Join(dir, "index.json"), []byte(index), 0o644); err != nil {
		t.Fatal(err)
	}
	ts := httptestServerWith(t, st, api.WithDataDir(dataDir))
	var out struct {
		Exports []struct {
			Name string `json:"name"`
			Day  string `json:"day"`
		} `json:"exports"`
		HowToVerify string `json:"how_to_verify"`
	}
	if code := get(t, ts, "/v1/exports", &out); code != 200 {
		t.Fatalf("exports: %d", code)
	}
	if len(out.Exports) != 1 || out.Exports[0].Name != name || out.Exports[0].Day != "2026-09-10" || out.HowToVerify == "" {
		t.Errorf("exports = %+v", out)
	}
	r, err := http.Get(ts.URL + "/v1/exports/" + name)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Body.Close()
	if r.StatusCode != 200 || r.Header.Get("Content-Type") != "application/gzip" || r.Header.Get("Cache-Control") != "public, max-age=86400, immutable" {
		t.Errorf("export file: %d %q %q", r.StatusCode, r.Header.Get("Content-Type"), r.Header.Get("Cache-Control"))
	}
	if code := get(t, ts, "/v1/exports/"+name+".sha256", nil); code != 200 {
		t.Errorf("sidecar: %d", code)
	}
	for _, bad := range []string{"state.json", "index.json", "..%2Fobserver.db", "fibrescope-test-2026-09-11.tar.gz"} {
		if code := get(t, ts, "/v1/exports/"+bad, nil); code != 404 {
			t.Errorf("%s: %d, want 404", bad, code)
		}
	}
}

func httptestServer(t *testing.T, st *store.Store) *httptest.Server {
	t.Helper()
	return httptestServerWith(t, st)
}

func httptestServerWith(t *testing.T, st *store.Store, opts ...api.Option) *httptest.Server {
	t.Helper()
	srv := api.NewWithVantage(st, api.VantageInfo{Name: "test"}, nil, opts...)
	ts := httptest.NewServer(srv)
	t.Cleanup(func() { ts.Close(); srv.Close() })
	return ts
}

// A row whose validator re-registered during the window carries the host
// the upload went to and what that host answered when the new one did not
// serve; the verdict is the new host's.
func TestProbesCarryTheSettlementHostEvidence(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "observer.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	now := time.Now().UTC().Truncate(time.Second)
	m := probe.Measurement{
		SchemaVersion: probe.AttestationSchemaVersion, Vantage: "test", PromiseHash: "mv1", Commitment: "cc",
		MustServeUntil: now.Add(time.Hour), ValidatorAddress: "v1", ValidatorHost: "new.example:9090", HostSource: "bonded",
		HostAtSettlement: "old.example:9090", Assigned: true, Attested: true, AssignedRowCount: 2,
		ScheduleLabel: "w1", ScheduledAt: now, StartedAt: now, FinishedAt: now, Phase: probe.PhaseInWindow,
		Outcome: probe.OutcomeNotFound, Classification: probe.ClassFault, ClassificationReason: "not found; host changed since settlement",
		SettlementHost: &probe.HostProbe{Host: "old.example:9090", Outcome: probe.OutcomeServedOK, RowsReturned: 2, CommitmentVerified: true, AssignmentVerified: true},
	}
	m.TCP.OK, m.TLS.OK, m.Identity.OK = true, true, true
	raw, _ := json.Marshal(m)
	if _, err := st.InsertProbe(m, raw); err != nil {
		t.Fatal(err)
	}
	ts := httptestServer(t, st)
	var out struct {
		Probes []struct {
			Classification        string `json:"classification"`
			HostAtSettlement      string `json:"host_at_settlement"`
			HostChanged           bool   `json:"host_changed"`
			SettlementHostOutcome string `json:"settlement_host_outcome"`
			SettlementHostServed  *bool  `json:"settlement_host_served"`
		} `json:"probes"`
	}
	if code := get(t, ts, "/v1/probes?limit=5", &out); code != 200 || len(out.Probes) != 1 {
		t.Fatalf("probes: %d, %d rows", code, len(out.Probes))
	}
	p := out.Probes[0]
	if p.Classification != "FAULT" || p.HostAtSettlement != "old.example:9090" || !p.HostChanged ||
		p.SettlementHostOutcome != "SERVED_OK" || p.SettlementHostServed == nil || !*p.SettlementHostServed {
		t.Errorf("row = %+v", p)
	}
}

// The scanner derives host_at_settlement from the chain's registration
// events and records them in host_history.jsonl; the collector ingests
// that record, and a blob's assignments carry the host with its source.
func TestHostHistoryIsIngestedAndAssignmentsCarryTheHost(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "observer.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	dir := t.TempDir()
	lines := `{"from_height":100,"from_tx_index":-1,"cons_address":"aa","host":"a-seed:1","source":"seed","time":"2026-09-18T09:00:00Z"}` + "\n" +
		`{"from_height":120,"from_tx_index":4,"cons_address":"aa","host":"a-new:1","source":"event","time":"2026-09-18T09:10:00Z"}` + "\n"
	if err := os.WriteFile(filepath.Join(dir, "host_history.jsonl"), []byte(lines), 0o644); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	res, err := ingest.HostEvents(st, filepath.Join(dir, "host_history.jsonl"), now)
	if err != nil || res.Inserted != 2 {
		t.Fatalf("ingest: %+v %v", res, err)
	}
	if res, err := ingest.HostEvents(st, filepath.Join(dir, "host_history.jsonl"), now); err != nil || res.Inserted != 0 {
		t.Fatalf("replay: %+v %v", res, err)
	}
	var n int
	if err := st.DB().QueryRow(`SELECT COUNT(*) FROM host_events WHERE cons_address = 'aa'`).Scan(&n); err != nil || n != 2 {
		t.Fatalf("host_events rows = %d (%v)", n, err)
	}
	// a publication whose assignments carry the event-derived host, one
	// unknown behind a scan gap, one never named
	pub := scan.Publication{
		SchemaVersion: scan.AttestationSchemaVersion, PromiseHash: "hh1", SettlementHeight: 130, SettlementTime: now,
		MustServeUntil: now.Add(time.Hour), RecordedAt: now, SettlementTxHash: "txhh1", Signer: "celestia1pub",
		Promise: scan.PromiseFields{ChainID: "t", Height: 129, Commitment: "cch", CreationTimestamp: now, BlobSize: 4096},
		Assignment: scan.AssignmentTable{
			ProtocolParams: scan.ProtocolParamsSnapshot{OriginalRows: 4, TotalRows: 16}, ValidatorSetHeight: 129,
			Validators: []scan.ValidatorAssignment{
				{Address: "aa", VotingPower: 10, RowCount: 2, Rows: []int{0, 1}, Attested: true, Host: "a-new:1", HostSource: scan.HostFromEvent},
				{Address: "bb", VotingPower: 10, RowCount: 2, Rows: []int{2, 3}, Attested: true, HostSource: scan.HostUnknownGap},
				{Address: "cc", VotingPower: 10, RowCount: 2, Rows: []int{4, 5}, Attested: true, HostSource: scan.HostNone},
			},
		},
	}
	raw, _ := json.Marshal(pub)
	if _, err := st.UpsertPublication(pub, raw); err != nil {
		t.Fatal(err)
	}
	ts := httptestServer(t, st)
	var out struct {
		Assignments []struct {
			Address string  `json:"validator_address"`
			Host    *string `json:"host_at_settlement"`
		} `json:"assignments"`
	}
	if code := get(t, ts, "/v1/blobs/hh1", &out); code != 200 {
		t.Fatalf("blob: %d", code)
	}
	got := map[string]*string{}
	for _, a := range out.Assignments {
		got[a.Address] = a.Host
	}
	if got["aa"] == nil || *got["aa"] != "a-new:1" {
		t.Errorf("aa host = %v, want the event host", got["aa"])
	}
	if got["bb"] != nil {
		t.Errorf("bb (scan gap) host = %q, want null", *got["bb"])
	}
	if got["cc"] == nil || *got["cc"] != "" {
		t.Errorf("cc (never named) host = %v, want empty, not null", got["cc"])
	}
}
