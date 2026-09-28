package api_test

import (
	"encoding/json"
	"net/http/httptest"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/probe"
	"github.com/plsgiveup/fibre/fibre-sentinel/internal/scan"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/api"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
)

// ?served=no lists the rows the obligations count as not served: a FAULT at
// any point, and at the end reading every reading that returned no rows. An
// unreachable row at an earlier schedule's point is not one of them, and
// neither are genuine or served rows.
func TestProbesServedNoFollowsTheObligationRule(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "observer.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })

	now := time.Now().UTC().Truncate(time.Second)
	created, msu := now.Add(-5*time.Hour), now.Add(-time.Hour)
	end := msu.Add(-10 * time.Minute)
	type reading struct {
		label string
		w     wire
	}
	readings := map[string]reading{
		"endok":      {probe.EndReadLabel, ok},
		"endsilent":  {probe.EndReadLabel, refused},
		"endgone":    {probe.EndReadLabel, gone},
		"enderror":   {probe.EndReadLabel, err500},
		"w4silent":   {"w4", refused},
		"w4gone":     {"w4", gone},
		"w4servedok": {"w4", ok},
	}
	var addrs []string
	for a := range readings {
		addrs = append(addrs, a)
	}
	sort.Strings(addrs)
	var vals []scan.ValidatorAssignment
	for i, a := range addrs {
		vals = append(vals, scan.ValidatorAssignment{Address: a, VotingPower: 10, RowCount: 2, Rows: []int{2 * i, 2*i + 1}, Attested: true})
	}
	pub := scan.Publication{
		SchemaVersion: scan.AttestationSchemaVersion, PromiseHash: "sn1",
		SettlementHeight: 100, SettlementTime: created, MustServeUntil: msu, RecordedAt: now,
		SettlementTxHash: "tx", Signer: "celestia1pub",
		Promise:                 scan.PromiseFields{ChainID: "t", Height: 99, Commitment: "cc", CreationTimestamp: created, BlobSize: 4096},
		ValidatorSignatureCount: len(vals),
		Assignment: scan.AssignmentTable{
			ProtocolParams:     scan.ProtocolParamsSnapshot{OriginalRows: 4, TotalRows: 16},
			ValidatorSetHeight: 99, TotalVotingPower: int64(10 * len(vals)), Sigma: 2 * len(vals), Distinct: 2 * len(vals),
			ValidatorsWithRows: len(vals), AttestedWithRows: len(vals), SignatureEntries: len(vals), SignaturesVerified: len(vals),
			AttestedVotingPower: int64(10 * len(vals)), Validators: vals,
		},
	}
	raw, err := json.Marshal(pub)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.UpsertPublication(pub, raw); err != nil {
		t.Fatal(err)
	}
	for addr, r := range readings {
		class, reason := probe.Classify(probe.Evidence{Assigned: true, Attested: true, Phase: probe.PhaseInWindow, Outcome: r.w.outcome})
		m := probe.Measurement{
			SchemaVersion: probe.AttestationSchemaVersion, Vantage: "test",
			PromiseHash: "sn1", Commitment: "cc", MustServeUntil: msu, ValidatorSetHeight: 99,
			ValidatorAddress: addr, ValidatorHost: addr + ":443",
			Assigned: true, Attested: true, AssignedRowCount: 2,
			ScheduleLabel: r.label, ScheduledAt: end, StartedAt: end, FinishedAt: end,
			Phase: probe.PhaseInWindow, Outcome: r.w.outcome,
			Classification: class, ClassificationReason: reason, TotalDurationMS: 10,
		}
		m.TCP.OK, m.TLS.OK, m.Identity.OK = r.w.tls, r.w.tls, r.w.tls
		if r.w.outcome == probe.OutcomeServedOK {
			m.Download.OK, m.Download.RowsReturned, m.Download.RowsExpected = true, 2, 2
			m.Download.CommitmentVerified, m.Download.AssignmentVerified = true, true
		}
		raw, err := json.Marshal(m)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := st.InsertProbe(m, raw); err != nil {
			t.Fatal(err)
		}
	}
	ts := httptest.NewServer(api.New(st, "test"))
	t.Cleanup(ts.Close)

	var probes struct {
		Probes []struct {
			ValidatorAddress string `json:"validator_address"`
		} `json:"probes"`
	}
	if code := get(t, ts, "/v1/probes?served=no&limit=100", &probes); code != 200 {
		t.Fatalf("served=no: %d", code)
	}
	var got []string
	for _, p := range probes.Probes {
		got = append(got, p.ValidatorAddress)
	}
	sort.Strings(got)
	if want := "enderror,endgone,endsilent,w4gone"; strings.Join(got, ",") != want {
		t.Errorf("served=no lists %v, want %s", got, want)
	}

	// The blob page's one word per validator is the same rule.
	var blob struct {
		Assignments []struct {
			ValidatorAddress string `json:"validator_address"`
			Service          string `json:"service"`
		} `json:"assignments"`
	}
	if code := get(t, ts, "/v1/blobs/sn1", &blob); code != 200 {
		t.Fatalf("blob: %d", code)
	}
	want := map[string]string{
		"endok": "served", "endsilent": "not_served", "endgone": "not_served", "enderror": "not_served",
		"w4silent": "no_verdict", "w4gone": "not_served", "w4servedok": "served",
	}
	if len(blob.Assignments) != len(want) {
		t.Fatalf("%d assignments, want %d", len(blob.Assignments), len(want))
	}
	for _, a := range blob.Assignments {
		if a.Service != want[a.ValidatorAddress] {
			t.Errorf("%s: service %q, want %q", a.ValidatorAddress, a.Service, want[a.ValidatorAddress])
		}
	}
}

// On the earlier schedule the word follows the same buckets: a fault is not
// served, a reading near the end served, readings that stop short of the end
// no verdict, an unendorsed validator nothing, and a window still running is
// in its retention window.
func TestBlobServiceWords(t *testing.T) {
	ts := obligationsFixture(t)
	type assignments struct {
		Assignments []struct {
			ValidatorAddress string `json:"validator_address"`
			Service          string `json:"service"`
		} `json:"assignments"`
	}
	var b assignments
	if code := get(t, ts, "/v1/blobs/obl1", &b); code != 200 {
		t.Fatalf("obl1: %d", code)
	}
	want := map[string]string{"served": "served", "broken": "not_served", "gaplast": "no_verdict", "endun": "no_verdict",
		"unreach": "no_verdict", "reach": "no_verdict", "unatt": ""}
	for _, a := range b.Assignments {
		if w, ok := want[a.ValidatorAddress]; ok && a.Service != w {
			t.Errorf("obl1 %s: service %q, want %q", a.ValidatorAddress, a.Service, w)
		}
	}
	var p assignments
	if code := get(t, ts, "/v1/blobs/obl2", &p); code != 200 {
		t.Fatalf("obl2: %d", code)
	}
	if len(p.Assignments) != 1 || p.Assignments[0].Service != "in_retention_window" {
		t.Errorf("obl2: %+v, want one in_retention_window", p.Assignments)
	}
}

// Before its first reading, an endorsed validator of a blob whose window is
// still running is in its retention window; an unendorsed one owes nothing.
func TestBlobServiceBeforeTheFirstReading(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "observer.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	now := time.Now().UTC().Truncate(time.Second)
	created := now.Add(-5 * time.Minute)
	pub := scan.Publication{
		SchemaVersion: scan.AttestationSchemaVersion, PromiseHash: "new1",
		SettlementHeight: 100, SettlementTime: created, MustServeUntil: created.Add(4 * time.Hour), RecordedAt: now,
		SettlementTxHash: "tx", Signer: "celestia1pub",
		Promise:                 scan.PromiseFields{ChainID: "t", Height: 99, Commitment: "cc", CreationTimestamp: created, BlobSize: 4096},
		ValidatorSignatureCount: 1,
		Assignment: scan.AssignmentTable{
			ProtocolParams:     scan.ProtocolParamsSnapshot{OriginalRows: 4, TotalRows: 16},
			ValidatorSetHeight: 99, TotalVotingPower: 20, Sigma: 4, Distinct: 4,
			ValidatorsWithRows: 2, AttestedWithRows: 1, SignatureEntries: 1, SignaturesVerified: 1, AttestedVotingPower: 10,
			Validators: []scan.ValidatorAssignment{
				{Address: "endorsed", VotingPower: 10, RowCount: 2, Rows: []int{0, 1}, Attested: true},
				{Address: "unendorsed", VotingPower: 10, RowCount: 2, Rows: []int{2, 3}, Attested: false},
			},
		},
	}
	raw, err := json.Marshal(pub)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.UpsertPublication(pub, raw); err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(api.New(st, "test"))
	t.Cleanup(ts.Close)
	var b struct {
		Assignments []struct {
			ValidatorAddress string `json:"validator_address"`
			Service          string `json:"service"`
		} `json:"assignments"`
	}
	if code := get(t, ts, "/v1/blobs/new1", &b); code != 200 {
		t.Fatalf("new1: %d", code)
	}
	want := map[string]string{"endorsed": "in_retention_window", "unendorsed": ""}
	if len(b.Assignments) != 2 {
		t.Fatalf("%d assignments", len(b.Assignments))
	}
	for _, a := range b.Assignments {
		if a.Service != want[a.ValidatorAddress] {
			t.Errorf("%s: service %q, want %q", a.ValidatorAddress, a.Service, want[a.ValidatorAddress])
		}
	}
}
