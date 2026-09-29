package api_test

import (
	"encoding/json"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/probe"
	"github.com/plsgiveup/fibre/fibre-sentinel/internal/scan"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/api"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
)

// attestedFixture builds one publication over three validators of equal
// power, each assigned two of the blob's four original rows. v1 and v2 carry
// a verified signature; v3 does not, so nothing proves v3 ever stored its
// shard. All three are probed at the same in-window point: v1 and v2 serve,
// v3 answers "no such shard".
func attestedFixture(t *testing.T) *httptest.Server {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "observer.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })

	now := time.Now().UTC().Truncate(time.Second)
	created := now.Add(-30 * time.Minute)
	msu := now.Add(30 * time.Minute)
	const hash = "aa11"

	pub := scan.Publication{
		SchemaVersion:           scan.AttestationSchemaVersion,
		PromiseHash:             hash,
		SettlementHeight:        100,
		SettlementTime:          created,
		MustServeUntil:          msu,
		RecordedAt:              now,
		SettlementTxHash:        "tx",
		Signer:                  "celestia1pub",
		Promise:                 scan.PromiseFields{ChainID: "t", Height: 99, Commitment: "cc", CreationTimestamp: created, BlobSize: 1024},
		ValidatorSignatureCount: 2,
		Assignment: scan.AssignmentTable{
			ProtocolParams:      scan.ProtocolParamsSnapshot{OriginalRows: 4, TotalRows: 8},
			ValidatorSetHeight:  99,
			TotalVotingPower:    30,
			Sigma:               6,
			Distinct:            6,
			ValidatorsWithRows:  3,
			AttestedWithRows:    2,
			SignatureEntries:    2,
			SignaturesVerified:  2,
			AttestedVotingPower: 20,
			Validators: []scan.ValidatorAssignment{
				{Address: "v1", VotingPower: 10, RowCount: 2, Rows: []int{0, 1}, Attested: true},
				{Address: "v2", VotingPower: 10, RowCount: 2, Rows: []int{2, 3}, Attested: true},
				{Address: "v3", VotingPower: 10, RowCount: 2, Rows: []int{4, 5}, Attested: false},
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

	point := now.Add(-10 * time.Minute)
	mk := func(addr string, attested bool, outcome probe.Outcome) probe.Measurement {
		class, reason := probe.Classify(probe.Evidence{
			Assigned: true, Attested: attested, Phase: probe.PhaseInWindow, Outcome: outcome,
		})
		m := probe.Measurement{
			SchemaVersion: probe.AttestationSchemaVersion, Vantage: "test",
			PromiseHash: hash, Commitment: "cc", MustServeUntil: msu, ValidatorSetHeight: 99,
			ValidatorAddress: addr, ValidatorHost: addr + ":443",
			Assigned: true, Attested: attested, AssignedRowCount: 2,
			ScheduleLabel: "w1", ScheduledAt: point, StartedAt: point, FinishedAt: point,
			Phase: probe.PhaseInWindow, Outcome: outcome,
			Classification: class, ClassificationReason: reason,
		}
		if outcome == probe.OutcomeServedOK {
			m.Download.OK, m.Download.RowsReturned, m.Download.RowsExpected = true, 2, 2
			m.Download.CommitmentVerified, m.Download.AssignmentVerified = true, true
			m.Download.RowIndices = map[string][]uint32{"v1": {0, 1}, "v2": {2, 3}}[addr]
		}
		return m
	}
	for _, m := range []probe.Measurement{
		mk("v1", true, probe.OutcomeServedOK),
		mk("v2", true, probe.OutcomeServedOK),
		mk("v3", false, probe.OutcomeNotFound),
	} {
		raw, err := json.Marshal(m)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := st.InsertProbe(m, raw); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := st.StartRun("collector", "test", "t", now); err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(api.New(st, "test"))
	t.Cleanup(ts.Close)
	return ts
}

// An unproven obligation is no obligation. v3 did not serve, but nothing on
// chain says v3 ever stored the shard, so it owes nothing: its reading is
// UNATTESTED, and it has no obligation to count either way.
func TestAnUnattestedReadingIsNoObligation(t *testing.T) {
	ts := attestedFixture(t)

	var net struct {
		Classes     map[string]int64 `json:"classes"`
		Obligations obligationsJSON  `json:"obligations"`
		Attestation struct {
			Attested   int64                    `json:"attested_blobs"`
			Unattested int64                    `json:"unattested_blobs"`
			Unknown    int64                    `json:"unknown_blobs"`
			Coverage   struct{ Num, Den int64 } `json:"blob_coverage"`
		} `json:"attestation"`
	}
	if code := get(t, ts, "/v1/network?window=all", &net); code != 200 {
		t.Fatalf("network: %d", code)
	}
	if net.Obligations.Total != 2 || net.Obligations.Pending != 2 {
		t.Fatalf("obligations = %+v, want v1's and v2's, pending", net.Obligations)
	}
	if net.Classes["UNATTESTED"] != 1 || net.Classes["FAULT"] != 0 {
		t.Fatalf("classes = %v, want one UNATTESTED and no FAULT", net.Classes)
	}
	if net.Attestation.Attested != 2 || net.Attestation.Unattested != 1 || net.Attestation.Unknown != 0 {
		t.Fatalf("attestation = %+v, want 2 attested / 1 unattested / 0 unknown", net.Attestation)
	}
	if net.Attestation.Coverage.Num != 2 || net.Attestation.Coverage.Den != 3 {
		t.Fatalf("coverage = %d/%d, want 2/3", net.Attestation.Coverage.Num, net.Attestation.Coverage.Den)
	}

	var vals struct {
		Validators []struct {
			Address      string           `json:"address"`
			Obligations  obligationsJSON  `json:"obligations"`
			AttestedLast *bool            `json:"attested_last"`
			Classes      map[string]int64 `json:"classes"`
		} `json:"validators"`
	}
	if code := get(t, ts, "/v1/validators?window=all", &vals); code != 200 {
		t.Fatalf("validators: %d", code)
	}
	seen := 0
	for _, v := range vals.Validators {
		switch v.Address {
		case "v1", "v2":
			seen++
			if v.Obligations.Total != 1 || v.Obligations.Pending != 1 {
				t.Fatalf("%s obligations = %+v, want one, pending", v.Address, v.Obligations)
			}
			if v.AttestedLast == nil || !*v.AttestedLast {
				t.Fatalf("%s attested_last = %v, want true", v.Address, v.AttestedLast)
			}
		case "v3":
			seen++
			if v.Obligations.Total != 0 {
				t.Fatalf("v3 obligations = %+v, want none: nothing proves it stored the shard", v.Obligations)
			}
			if v.Classes["UNATTESTED"] != 1 {
				t.Fatalf("v3 classes = %v, want one UNATTESTED", v.Classes)
			}
			if v.AttestedLast == nil || *v.AttestedLast {
				t.Fatalf("v3 attested_last = %v, want false", v.AttestedLast)
			}
		}
	}
	if seen != 3 {
		t.Fatalf("saw %d of the 3 validators", seen)
	}
}

// v1 and v2 together returned all 4 rows the blob needs: it is Available,
// whatever v3, which never endorsed it, answered.
func TestReconstructabilityIgnoresUnattestedNonServers(t *testing.T) {
	ts := attestedFixture(t)

	var blob struct {
		Blob struct {
			Reconstructable struct {
				Status     string `json:"status"`
				ServedRows int    `json:"served_distinct_rows"`
				NeededRows int    `json:"needed_rows"`
				ServedBy   int    `json:"served_by_validators"`
				Asked      int    `json:"probed_validators"`
			} `json:"reconstructable"`
		} `json:"blob"`
		Assignments []struct {
			ValidatorAddress string `json:"validator_address"`
			Attested         *bool  `json:"attested"`
		} `json:"assignments"`
	}
	if code := get(t, ts, "/v1/blobs/aa11", &blob); code != 200 {
		t.Fatalf("blob: %d", code)
	}
	rc := blob.Blob.Reconstructable
	if rc.ServedRows != 4 || rc.NeededRows != 4 {
		t.Fatalf("rows = %d/%d, want 4/4", rc.ServedRows, rc.NeededRows)
	}
	if rc.ServedBy != 2 || rc.Asked != 3 {
		t.Fatalf("served_by=%d asked=%d, want 2 of 3", rc.ServedBy, rc.Asked)
	}
	if rc.Status != "yes" {
		t.Fatalf("status = %q, want \"yes\": the rows are all there", rc.Status)
	}
	for _, a := range blob.Assignments {
		if a.Attested == nil {
			t.Fatalf("%s has no attestation in the assignment table", a.ValidatorAddress)
		}
		if want := a.ValidatorAddress != "v3"; *a.Attested != want {
			t.Fatalf("%s attested=%v, want %v", a.ValidatorAddress, *a.Attested, want)
		}
	}
}
