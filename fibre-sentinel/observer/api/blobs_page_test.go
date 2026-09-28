package api_test

import (
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/probe"
	"github.com/plsgiveup/fibre/fibre-sentinel/internal/scan"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/api"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
)

// Numbered pages: offset walks the same order the cursor does, and total
// counts every publication the filters select.
func TestBlobsPageByOffset(t *testing.T) {
	ts := serverWithSample(t)
	type page struct {
		Blobs []struct {
			PromiseHash string `json:"promise_hash"`
		} `json:"blobs"`
		Total     int64 `json:"total"`
		Offset    int   `json:"offset"`
		Truncated bool  `json:"truncated"`
	}
	var all, first, second page
	if code := get(t, ts, "/v1/blobs", &all); code != 200 || all.Total != 3 || len(all.Blobs) != 3 {
		t.Fatalf("all: %d, total %d, %d rows", code, all.Total, len(all.Blobs))
	}
	if code := get(t, ts, "/v1/blobs?limit=2", &first); code != 200 || first.Total != 3 || len(first.Blobs) != 2 || !first.Truncated {
		t.Fatalf("first page: %d %+v", code, first)
	}
	if code := get(t, ts, "/v1/blobs?limit=2&offset=2", &second); code != 200 || second.Total != 3 || second.Offset != 2 || len(second.Blobs) != 1 || second.Truncated {
		t.Fatalf("second page: %d %+v", code, second)
	}
	if second.Blobs[0].PromiseHash != all.Blobs[2].PromiseHash {
		t.Errorf("offset 2 is %s, the third row is %s", second.Blobs[0].PromiseHash, all.Blobs[2].PromiseHash)
	}
	// the answer says which namespace it is for, so a page can tell its rows from the last ones
	var filtered struct {
		Namespace string `json:"namespace"`
	}
	if code := get(t, ts, "/v1/blobs?namespace=ABCD", &filtered); code != 200 || filtered.Namespace != "abcd" {
		t.Errorf("namespace filter echo: %d %q, want abcd", code, filtered.Namespace)
	}
	for _, bad := range []string{"-1", "x", "100001"} {
		if code := get(t, ts, "/v1/blobs?offset="+bad, nil); code != 400 {
			t.Errorf("offset=%s: %d, want 400", bad, code)
		}
	}
}

// A numbered page fingerprints its own rows: a probe that lands on a blob
// shown on page 2 after the page was read shows up on the next read of it,
// instead of a verdict cached under another page's fingerprint. Every blob
// was read in its window and the window is over, so its verdict is cached.
// The blob under test is the 53rd, past the first page the server warms at
// start (blobPageDefault rows and one more), so nothing else reads it.
func TestBlobsOffsetPageSeesNewProbes(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "observer.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	now := time.Now().UTC().Truncate(time.Second)
	created, msu := now.Add(-5*time.Hour), now.Add(-time.Hour)
	at := msu.Add(-10 * time.Minute)
	insert := func(hash string, height int64, at time.Time) {
		m := probe.Measurement{
			SchemaVersion: probe.AttestationSchemaVersion, Vantage: "test",
			PromiseHash: hash, Commitment: "c" + hash, MustServeUntil: msu, ValidatorSetHeight: height - 1,
			ValidatorAddress: "v", ValidatorHost: "v:443", Assigned: true, Attested: true, AssignedRowCount: 2,
			ScheduleLabel: probe.EndReadLabel, ScheduledAt: at, StartedAt: at, FinishedAt: at,
			Phase: probe.PhaseInWindow, Outcome: probe.OutcomeServedOK, Classification: probe.ClassHealthy, TotalDurationMS: 10,
		}
		m.TCP.OK, m.TLS.OK, m.Identity.OK = true, true, true
		m.Download.OK, m.Download.RowsReturned, m.Download.RowsExpected = true, 2, 2
		m.Download.CommitmentVerified, m.Download.AssignmentVerified = true, true
		raw, err := json.Marshal(m)
		if err != nil {
			t.Fatal(err)
		}
		if ok, err := st.InsertProbe(m, raw); err != nil || !ok {
			t.Fatalf("insert %s at %s: inserted=%v err=%v", hash, at, ok, err)
		}
	}
	type pubAt struct {
		hash   string
		height int64
	}
	var pubs []pubAt
	for i := 0; i < 53; i++ {
		pubs = append(pubs, pubAt{fmt.Sprintf("b%02d", i), int64(100 + i)})
	}
	for _, p := range pubs {
		pub := scan.Publication{
			SchemaVersion: scan.AttestationSchemaVersion, PromiseHash: p.hash,
			SettlementHeight: p.height, SettlementTime: created, MustServeUntil: msu, RecordedAt: now,
			SettlementTxHash: "tx" + p.hash, Signer: "celestia1pub",
			Promise:                 scan.PromiseFields{ChainID: "t", Height: p.height - 1, Commitment: "c" + p.hash, CreationTimestamp: created, BlobSize: 4096},
			ValidatorSignatureCount: 1,
			Assignment: scan.AssignmentTable{
				ProtocolParams:     scan.ProtocolParamsSnapshot{OriginalRows: 4, TotalRows: 16},
				ValidatorSetHeight: p.height - 1, TotalVotingPower: 10, Sigma: 2, Distinct: 2,
				ValidatorsWithRows: 1, AttestedWithRows: 1, SignatureEntries: 1, SignaturesVerified: 1, AttestedVotingPower: 10,
				Validators: []scan.ValidatorAssignment{{Address: "v", VotingPower: 10, RowCount: 2, Rows: []int{0, 1}, Attested: true}},
			},
		}
		raw, err := json.Marshal(pub)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := st.UpsertPublication(pub, raw); err != nil {
			t.Fatal(err)
		}
		insert(p.hash, p.height, at)
	}
	ts := httptest.NewServer(api.New(st, "test"))
	t.Cleanup(ts.Close)
	type page struct {
		Blobs []struct {
			PromiseHash     string `json:"promise_hash"`
			ProbeCount      int64  `json:"probe_count"`
			Reconstructable *struct {
				WindowOver bool `json:"window_over"`
			} `json:"reconstructable"`
		} `json:"blobs"`
	}
	var before, after page
	if code := get(t, ts, "/v1/blobs?limit=1&offset=52", &before); code != 200 || len(before.Blobs) != 1 || before.Blobs[0].PromiseHash != "b00" {
		t.Fatalf("offset page: %d %+v", code, before)
	}
	if rc := before.Blobs[0].Reconstructable; rc == nil || !rc.WindowOver {
		t.Fatalf("the fixture must give a verdict the cache keeps: %+v", rc)
	}
	// a second reading of the blob, five minutes later, still in its window
	insert("b00", 100, at.Add(5*time.Minute))
	if code := get(t, ts, "/v1/blobs?limit=1&offset=52", &after); code != 200 || len(after.Blobs) != 1 {
		t.Fatalf("offset page again: %d %+v", code, after)
	}
	if after.Blobs[0].ProbeCount != before.Blobs[0].ProbeCount+1 {
		t.Errorf("probe_count %d after one more probe, was %d: a cached verdict outlived the change", after.Blobs[0].ProbeCount, before.Blobs[0].ProbeCount)
	}
}
