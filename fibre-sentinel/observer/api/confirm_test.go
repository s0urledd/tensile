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
	Obligations obligationsJSON  `json:"obligations"`
	Classes     map[string]int64 `json:"classes"`
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
		if err := st.SettleConfirmation(d); err != nil {
			t.Fatal(err)
		}
	}
	return ds
}

// A not-served reading counts only once a second location confirms it.
//
//   - confirmed: the second vantage read the rows under the client's rules
//     four minutes later and got none either; the reading counts, with
//     confirmed_by.
//   - fetched: the second vantage got the verified rows; the reading does
//     not count, and says who fetched them (cleared_by). Nothing is
//     rewritten: it keeps its class.
//   - oldbuild: an answer that does not say it was read under the client's
//     rules (a second location on an older build) confirms nothing.
//   - late: the answer started after the confirmation window.
//   - noanswer: nothing came back.
//   - served: a second vantage's row about a reading that did not fail
//     changes nothing.
//
// Before any answer nothing counts not served; after, only the confirmed
// reading does, and no row, reading or credit is added.
func TestANotServedReadingCountsOnlyOnceConfirmedFromASecondVantage(t *testing.T) {
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
		rules   bool
	}{
		{"confirmed", broke, probe.ClassFault, 4 * time.Minute, true},
		{"fetched", broke, probe.ClassHealthy, 5 * time.Minute, true},
		{"oldbuild", broke, probe.ClassFault, 4 * time.Minute, false},
		{"late", broke, probe.ClassFault, verdict.ConfirmWindow + 5*time.Minute, true},
		{"noanswer", broke, "", 0, true},
		{"served", []wire{ok, ok, ok, ok}, probe.ClassHealthy, 2 * time.Minute, true},
	}
	type slot struct {
		hash    string
		at      time.Time
		answer  probe.Classification
		after   time.Duration
		rules   bool
		address string
	}
	var slots []slot
	for i, c := range cases {
		// One blob per validator, a minute apart, so no reading holds more
		// than one validator and the correlated-failure guard stays out.
		created := now.Add(-3*time.Hour + time.Duration(i)*time.Minute)
		msu := created.Add(2 * time.Hour)
		hash := "cf-" + c.addr
		insertProbeSetUnconfirmed(t, st, hash, created, msu, map[string][]wire{c.addr: c.profile})
		slots = append(slots, slot{hash, inWindowPoint(created, msu, 3), c.answer, c.after, c.rules, c.addr})
	}

	before := confirmNet{}
	if code := get(t, confirmServer(t, st), "/v1/network?window=all", &before); code != 200 {
		t.Fatalf("network: %d", code)
	}
	if before.Obligations.Broken != 0 || before.Classes["FAULT"] != 5 {
		t.Fatalf("before: broken %d, FAULT readings %d; want 0 (nothing confirmed yet) and 5", before.Obligations.Broken, before.Classes["FAULT"])
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
			Outcome: outcome, Classification: s.answer, ClientRules: s.rules}
		m.Download.CommitmentVerified = s.answer == probe.ClassHealthy
		b, _ := json.Marshal(m)
		lines = append(append(lines, b...), '\n')
	}
	os.WriteFile(vfile, lines, 0o644)
	if r, err := ingest.VantageMeasurements(st, vfile, "test", now); err != nil || r.Inserted != 5 {
		t.Fatalf("ingest: %+v %v", r, err)
	}
	var inProbes int
	st.DB().QueryRow(`SELECT COUNT(*) FROM probes WHERE vantage = 'de-1'`).Scan(&inProbes)
	if inProbes != 0 {
		t.Fatalf("%d of the second vantage's rows reached the probes table", inProbes)
	}

	ds := judge(t, st, now)
	if len(ds) != 5 {
		t.Fatalf("%d decisions, want 5", len(ds))
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
	if a.Broken != 1 || a.NotCounted != b.NotCounted-1 {
		t.Errorf("broken %d -> %d, not_counted %d -> %d; want the one confirmed reading counted, nothing else",
			b.Broken, a.Broken, b.NotCounted, a.NotCounted)
	}
	if after.Classes["FAULT"] != 5 || after.Classes["HEALTHY"] != before.Classes["HEALTHY"] {
		t.Errorf("classes %v -> %v; want nothing rewritten and nothing added", before.Classes, after.Classes)
	}

	ts := confirmServer(t, st)
	for _, c := range []struct {
		addr, service, clearedBy, confirmedBy string
		unconfirmed                           bool
	}{
		{"confirmed", "not_served", "", "de-1", false},
		{"fetched", "", "de-1", "", true},
		{"oldbuild", "", "", "", true},
		{"late", "", "", "", true},
		{"noanswer", "", "", "", true},
		{"served", "served", "", "", false},
	} {
		var resp struct {
			Probes []struct {
				Label       string `json:"schedule_label"`
				Class       string `json:"classification"`
				AtProbe     string `json:"classification_at_probe"`
				Service     string `json:"service"`
				Unconfirmed bool   `json:"unconfirmed"`
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
			if p.AtProbe != "" || p.Service != c.service || p.Unconfirmed != c.unconfirmed || p.ClearedBy != c.clearedBy || p.ConfirmedBy != c.confirmedBy {
				t.Errorf("%s w4: service %q unconfirmed %v cleared_by %q confirmed_by %q at_probe %q; want %q %v %q %q and no amendment",
					c.addr, p.Service, p.Unconfirmed, p.ClearedBy, p.ConfirmedBy, p.AtProbe, c.service, c.unconfirmed, c.clearedBy, c.confirmedBy)
			}
		}
	}
	var notServed struct {
		Probes []struct {
			PromiseHash string `json:"promise_hash"`
		} `json:"probes"`
	}
	if code := get(t, ts, "/v1/probes?served=no", &notServed); code != 200 || len(notServed.Probes) != 1 || notServed.Probes[0].PromiseHash != "cf-confirmed" {
		t.Errorf("served=no: %d %+v, want the confirmed reading alone", code, notServed.Probes)
	}

	var vals struct {
		Validators []struct {
			Address       string           `json:"address"`
			Classes       map[string]int64 `json:"classes"`
			FaultsCleared int64            `json:"faults_cleared"`
		} `json:"validators"`
	}
	if code := get(t, ts, "/v1/validators?window=all", &vals); code != 200 {
		t.Fatalf("validators: %d", code)
	}
	for _, v := range vals.Validators {
		wantCleared, wantFaults := int64(0), int64(1)
		switch v.Address {
		case "fetched":
			wantCleared = 1
		case "served":
			wantFaults = 0
		}
		if v.FaultsCleared != wantCleared || v.Classes["FAULT"] != wantFaults {
			t.Errorf("%s: FAULT readings %d, faults_cleared %d; want %d, %d", v.Address, v.Classes["FAULT"], v.FaultsCleared, wantFaults, wantCleared)
		}
	}
}
