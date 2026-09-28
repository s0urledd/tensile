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

// ?served=no lists the readings the obligations count as not served: no
// rows came back, on a blob that could not be reconstructed from the rows
// that did. At the one reading of a blob every answer that leaves the
// reader without rows is one; at an earlier schedule's point only a FAULT
// is. A failure on a blob that was Available counts neither way and is not
// listed, and neither are served rows.
func TestProbesServedNoFollowsTheObligationRule(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "observer.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })

	now := time.Now().UTC().Truncate(time.Second)
	created, msu := now.Add(-5*time.Hour), now.Add(-time.Hour)
	// Unavailable: the three validators that did not serve hold most of the
	// rows, and fewer than half the validators asked failed, so the
	// correlated-failure guard does not set the reading aside.
	insertReading(t, st, "sn1", created, msu, 12, probe.EndReadLabel, msu.Add(-10*time.Minute), []endVal{
		{addr: "endsilent", rows: 4, w: refused}, {addr: "endgone", rows: 4, w: gone}, {addr: "enderror", rows: 4, w: err500},
		{addr: "endok1", rows: 1, w: ok}, {addr: "endok2", rows: 1, w: ok}, {addr: "endok3", rows: 1, w: ok}, {addr: "endok4", rows: 1, w: ok},
	})
	// The earlier schedule's last point of another unreadable blob: only
	// the FAULT is not served.
	insertReading(t, st, "sn0", created.Add(-time.Minute), msu.Add(-time.Minute), 12, "w4", msu.Add(-30*time.Minute), []endVal{
		{addr: "w4silent", rows: 4, w: refused}, {addr: "w4gone", rows: 4, w: gone},
		{addr: "w4ok1", rows: 1, w: ok}, {addr: "w4ok2", rows: 1, w: ok}, {addr: "w4ok3", rows: 1, w: ok},
	})
	// Available: the same answers count for nothing.
	insertReading(t, st, "sn2", created.Add(time.Minute), msu.Add(time.Minute), 4, probe.EndReadLabel, msu.Add(-9*time.Minute), []endVal{
		{addr: "availok", rows: 4, w: ok}, {addr: "availsilent", rows: 4, w: refused}, {addr: "availgone", rows: 4, w: gone},
		{addr: "availok2", rows: 4, w: ok}, {addr: "never", rows: 4, unasked: true},
	})
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
	for _, a := range blob.Assignments {
		want := "served"
		if !strings.HasPrefix(a.ValidatorAddress, "endok") {
			want = "not_served"
		}
		if a.Service != want {
			t.Errorf("%s: service %q, want %q", a.ValidatorAddress, a.Service, want)
		}
	}
}

// Every validator failing at the same reading is set aside by the
// correlated-failure guard: nothing is listed as not served.
func TestProbesServedNoLeavesOutASuspectReading(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "observer.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	now := time.Now().UTC().Truncate(time.Second)
	created, msu := now.Add(-5*time.Hour), now.Add(-time.Hour)
	insertReading(t, st, "sus", created, msu, 8, probe.EndReadLabel, msu.Add(-10*time.Minute), []endVal{
		{addr: "a", rows: 2, w: refused}, {addr: "b", rows: 2, w: refused}, {addr: "c", rows: 2, w: gone}, {addr: "d", rows: 2, w: err500},
	})
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
	if len(probes.Probes) != 0 {
		t.Errorf("served=no lists %d readings of a reading the guard set aside", len(probes.Probes))
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
