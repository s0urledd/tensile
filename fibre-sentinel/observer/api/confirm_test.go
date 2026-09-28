package api_test

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/probe"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/api"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/ingest"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/verdict"
)

type confirmNet struct {
	Obligations obligationsJSON          `json:"obligations"`
	ServeRate   struct{ Num, Den int64 } `json:"serve_rate"`
	Faults      int64                    `json:"faults"`
	Classes     map[string]int64         `json:"classes"`
}

func confirmServer(t *testing.T, st *store.Store) *httptest.Server {
	t.Helper()
	srv := api.NewWithVantage(st, api.VantageInfo{Name: "test"}, nil, api.WithDataDir(t.TempDir()))
	ts := httptest.NewServer(srv)
	t.Cleanup(func() { ts.Close(); srv.Close() })
	return ts
}

// judge is the collector's confirmation step (cmd/observer-collector).
func judge(t *testing.T, st *store.Store, now time.Time) []store.ConfirmDecision {
	t.Helper()
	ds, err := st.JudgeConfirmations(context.Background(), now)
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range ds {
		if d.Amendment != nil {
			if _, err := st.ApplyAmendment(*d.Amendment); err != nil {
				t.Fatal(err)
			}
		}
		if err := st.SettleConfirmation(d); err != nil {
			t.Fatal(err)
		}
	}
	return ds
}

// A retention fault is re-checked from a second location before it counts.
//
//   - cleared: the second vantage got the verified rows five minutes later.
//     The fault is withdrawn and filed PROBE_ERROR (observer-side, outside
//     the rate) with cleared_by; the obligation is not credited as served:
//     it counts neither way, because the rates are this observer's own
//     readings and the second vantage adds none.
//   - confirmed: the second vantage failed too; the fault stands, with
//     confirmed_by.
//   - late: the second vantage got the rows, but started after the
//     confirmation window; no answer, the fault stands.
//   - noanswer: nothing came back; the fault stands.
//   - served: a second vantage's row about a probe that did not fault
//     changes nothing.
//
// Every figure except the cleared fault's own is the same before and after:
// the second vantage's rows add no obligation, no reading and no credit.
func TestAFaultIsClearedOrConfirmedFromASecondVantage(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "observer.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	now := time.Now().UTC().Truncate(time.Second)
	broke := []wire{ok, ok, ok, gone}
	cases := []struct {
		addr    string
		profile []wire
		answer  probe.Classification // "" = no row from the second vantage
		after   time.Duration        // its start, after the primary's
	}{
		{"cleared", broke, probe.ClassHealthy, 5 * time.Minute},
		{"confirmed", broke, probe.ClassFault, 4 * time.Minute},
		{"late", broke, probe.ClassHealthy, verdict.ConfirmWindow + 5*time.Minute},
		{"noanswer", broke, "", 0},
		{"served", []wire{ok, ok, ok, ok}, probe.ClassHealthy, 2 * time.Minute},
	}
	type slot struct {
		hash    string
		at      time.Time
		answer  probe.Classification
		after   time.Duration
		address string
	}
	var slots []slot
	for i, c := range cases {
		// One blob per validator, a minute apart, so no schedule point holds
		// more than one validator and the correlated-failure guard stays out.
		created := now.Add(-3*time.Hour + time.Duration(i)*time.Minute)
		msu := created.Add(2 * time.Hour)
		hash := "cf-" + c.addr
		insertProbeSet(t, st, hash, created, msu, map[string][]wire{c.addr: c.profile}, false)
		slots = append(slots, slot{hash, inWindowPoint(created, msu, 3), c.answer, c.after, c.addr})
	}

	before := confirmNet{}
	if code := get(t, confirmServer(t, st), "/v1/network?window=all", &before); code != 200 {
		t.Fatalf("network: %d", code)
	}
	if before.Obligations.Broken != 4 || before.Faults != 4 {
		t.Fatalf("before: broken %d, faults %d; want 4 and 4", before.Obligations.Broken, before.Faults)
	}

	// The second vantage's answers, as the pull copies them in.
	vfile := filepath.Join(t.TempDir(), ingest.VantagesDir, "de-1", "measurements.jsonl")
	os.MkdirAll(filepath.Dir(vfile), 0o755)
	var lines []byte
	for _, s := range slots {
		if s.answer == "" {
			continue
		}
		outcome := probe.OutcomeServedOK
		if s.answer != probe.ClassHealthy {
			outcome = probe.OutcomeNotFound
		}
		m := probe.Measurement{SchemaVersion: probe.MeasurementSchemaVersion, Vantage: "de-1", PromiseHash: s.hash,
			ValidatorAddress: s.address, ValidatorHost: s.address + ":443", Assigned: true, Attested: true,
			ScheduleLabel: "w4", ScheduledAt: s.at, StartedAt: s.at.Add(s.after), Phase: probe.PhaseInWindow,
			Outcome: outcome, Classification: s.answer}
		b, _ := json.Marshal(m)
		lines = append(append(lines, b...), '\n')
	}
	os.WriteFile(vfile, lines, 0o644)
	if r, err := ingest.VantageMeasurements(st, vfile, "test", now); err != nil || r.Inserted != 4 {
		t.Fatalf("ingest: %+v %v", r, err)
	}
	var inProbes int
	st.DB().QueryRow(`SELECT COUNT(*) FROM probes WHERE vantage = 'de-1'`).Scan(&inProbes)
	if inProbes != 0 {
		t.Fatalf("%d of the second vantage's rows reached the probes table", inProbes)
	}

	ds := judge(t, st, now)
	if len(ds) != 4 {
		t.Fatalf("%d decisions, want 4", len(ds))
	}
	if again := judge(t, st, now); len(again) != 0 {
		t.Errorf("answers judged twice: %+v", again)
	}

	after := confirmNet{}
	if code := get(t, confirmServer(t, st), "/v1/network?window=all", &after); code != 200 {
		t.Fatalf("network: %d", code)
	}
	b, a := before.Obligations, after.Obligations
	if a.Total != b.Total || a.Served != b.Served || a.Pending != b.Pending {
		t.Errorf("total/served/pending moved: before %+v after %+v", b, a)
	}
	if a.Broken != b.Broken-1 || a.NotCounted != b.NotCounted+1 {
		t.Errorf("broken %d -> %d, not_counted %d -> %d; want one fault withdrawn to not counted, never to served",
			b.Broken, a.Broken, b.NotCounted, a.NotCounted)
	}
	if after.Faults != 3 {
		t.Errorf("faults = %d, want 3", after.Faults)
	}
	if after.ServeRate.Num != before.ServeRate.Num || after.ServeRate.Den != before.ServeRate.Den-1 {
		t.Errorf("serve_rate %d/%d -> %d/%d; want the cleared fault out of the denominator and nothing added",
			before.ServeRate.Num, before.ServeRate.Den, after.ServeRate.Num, after.ServeRate.Den)
	}

	ts := confirmServer(t, st)
	for _, c := range []struct {
		addr, cls, clearedBy, confirmedBy, atProbe string
	}{
		{"cleared", "PROBE_ERROR", "de-1", "", "FAULT"},
		{"confirmed", "FAULT", "", "de-1", ""},
		{"late", "FAULT", "", "", ""},
		{"noanswer", "FAULT", "", "", ""},
		{"served", "HEALTHY", "", "", ""},
	} {
		var resp struct {
			Probes []struct {
				Label       string `json:"schedule_label"`
				Class       string `json:"classification"`
				AtProbe     string `json:"classification_at_probe"`
				ClearedBy   string `json:"cleared_by"`
				ConfirmedBy string `json:"confirmed_by"`
				Vantage     string `json:"vantage"`
			} `json:"probes"`
		}
		if code := get(t, ts, "/v1/probes?blob=cf-"+c.addr, &resp); code != 200 {
			t.Fatalf("%s probes: %d", c.addr, code)
		}
		if len(resp.Probes) != 4 {
			t.Fatalf("%s: %d probe rows, want this observer's 4 only", c.addr, len(resp.Probes))
		}
		for _, p := range resp.Probes {
			if p.Vantage != "test" {
				t.Errorf("%s: a row from %s in the probe list", c.addr, p.Vantage)
			}
			if p.Label != "w4" {
				continue
			}
			if p.Class != c.cls || p.ClearedBy != c.clearedBy || p.ConfirmedBy != c.confirmedBy || p.AtProbe != c.atProbe {
				t.Errorf("%s w4: class %s cleared_by %q confirmed_by %q at_probe %q; want %s %q %q %q",
					c.addr, p.Class, p.ClearedBy, p.ConfirmedBy, p.AtProbe, c.cls, c.clearedBy, c.confirmedBy, c.atProbe)
			}
		}
	}

	var vals struct {
		Validators []struct {
			Address       string `json:"address"`
			Faults        int64  `json:"faults"`
			FaultsCleared int64  `json:"faults_cleared"`
		} `json:"validators"`
	}
	if code := get(t, ts, "/v1/validators?window=all", &vals); code != 200 {
		t.Fatalf("validators: %d", code)
	}
	for _, v := range vals.Validators {
		wantCleared, wantFaults := int64(0), int64(1)
		switch v.Address {
		case "cleared":
			wantCleared, wantFaults = 1, 0
		case "served":
			wantFaults = 0
		}
		if v.FaultsCleared != wantCleared || v.Faults != wantFaults {
			t.Errorf("%s: faults %d, faults_cleared %d; want %d, %d", v.Address, v.Faults, v.FaultsCleared, wantFaults, wantCleared)
		}
	}
}
