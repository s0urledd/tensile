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

// A cached verdict is only worth having if it cannot be wrong, and the way it
// could be wrong is a probe arriving after it was computed — a collector coming
// back with a backlog. The cache is keyed on the publication's probes, so this
// checks the key does its job on a store that is changing underneath it.

// closedBlob builds one publication whose retention window has already ended,
// assigned to two validators, with one probe each: v1 served, v2 was never
// probed at all. The verdict is therefore settled and cacheable.
func closedBlob(t *testing.T) (*httptest.Server, *store.Store, string, time.Time) {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "observer.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })

	now := time.Now().UTC().Truncate(time.Second)
	created := now.Add(-4 * time.Hour)
	msu := now.Add(-2 * time.Hour) // the obligation ended two hours ago
	const hash = "closed1"

	pub := scan.Publication{
		SchemaVersion: scan.AttestationSchemaVersion, PromiseHash: hash,
		SettlementHeight: 100, SettlementTime: created, MustServeUntil: msu, RecordedAt: now,
		SettlementTxHash: "tx", Signer: "celestia1pub",
		Promise:                 scan.PromiseFields{ChainID: "t", Height: 99, Commitment: "cc", CreationTimestamp: created, BlobSize: 1024},
		ValidatorSignatureCount: 2,
		Assignment: scan.AssignmentTable{
			ProtocolParams:     scan.ProtocolParamsSnapshot{OriginalRows: 4, TotalRows: 8},
			ValidatorSetHeight: 99, TotalVotingPower: 20, Sigma: 4, Distinct: 4,
			ValidatorsWithRows: 2, AttestedWithRows: 2, SignatureEntries: 2, SignaturesVerified: 2,
			AttestedVotingPower: 20,
			Validators: []scan.ValidatorAssignment{
				{Address: "v1", VotingPower: 10, RowCount: 2, Rows: []int{0, 1}, Attested: true},
				{Address: "v2", VotingPower: 10, RowCount: 2, Rows: []int{2, 3}, Attested: true},
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
	point := msu.Add(-time.Hour)
	insertProbe(t, st, hash, "v1", point, probe.OutcomeServedOK)

	if _, err := st.StartRun("collector", "test", "t", now); err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(api.New(st, "test"))
	t.Cleanup(ts.Close)
	return ts, st, hash, point
}

func insertProbe(t *testing.T, st *store.Store, hash, addr string, at time.Time, outcome probe.Outcome) {
	t.Helper()
	class, reason := probe.Classify(probe.Evidence{
		Assigned: true, Attested: true, Phase: probe.PhaseInWindow, Outcome: outcome,
	})
	m := probe.Measurement{
		SchemaVersion: probe.AttestationSchemaVersion, Vantage: "test",
		PromiseHash: hash, Commitment: "cc", MustServeUntil: at.Add(time.Hour), ValidatorSetHeight: 99,
		ValidatorAddress: addr, ValidatorHost: addr + ":443",
		Assigned: true, Attested: true, AssignedRowCount: 2,
		ScheduleLabel: "w1", ScheduledAt: at, StartedAt: at, FinishedAt: at,
		Phase: probe.PhaseInWindow, Outcome: outcome,
		Classification: class, ClassificationReason: reason, TotalDurationMS: 40,
	}
	if outcome == probe.OutcomeServedOK {
		m.Download.OK, m.Download.RowsReturned, m.Download.RowsExpected = true, 2, 2
		m.Download.CommitmentVerified, m.Download.AssignmentVerified = true, true
	}
	m.TCP.Attempted, m.TCP.OK = true, opensConnection(outcome)
	raw, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.InsertProbe(m, raw); err != nil {
		t.Fatal(err)
	}
}

// opensConnection reports whether a request with this outcome opened its
// connection: everything but the answers from before one (no host to
// connect to, a lookup or a connect that failed, a failure on this
// observer's side, a request never made).
func opensConnection(o probe.Outcome) bool {
	switch o {
	case probe.OutcomeNoHost, probe.OutcomeBadHost, probe.OutcomeDNSFail, probe.OutcomeTCPTimeout, probe.OutcomeTCPUnreachable,
		probe.OutcomeTCPRefused, probe.OutcomeProbeError, probe.OutcomeMissed:
		return false
	}
	return true
}

type blobListRow struct {
	PromiseHash string           `json:"promise_hash"`
	ProbeCount  int64            `json:"probe_count"`
	Classes     map[string]int64 `json:"classes"`
	Recon       struct {
		Status           string `json:"status"`
		ProbedValidators int    `json:"probed_validators"`
		WindowOver       bool   `json:"window_over"`
	} `json:"reconstructable"`
}

func blobList(t *testing.T, ts *httptest.Server) blobListRow {
	t.Helper()
	var resp struct {
		Blobs []blobListRow `json:"blobs"`
	}
	if code := get(t, ts, "/v1/blobs?limit=50", &resp); code != 200 {
		t.Fatalf("blobs: %d", code)
	}
	if len(resp.Blobs) != 1 {
		t.Fatalf("want one publication, got %d", len(resp.Blobs))
	}
	return resp.Blobs[0]
}

// A probe that lands after the verdict was cached must change the verdict. The
// window has closed, so nothing about the clock reopens it: only the new row
// does, and only because the key notices it.
func TestCachedBlobVerdictFollowsLateProbes(t *testing.T) {
	ts, st, hash, point := closedBlob(t)

	first := blobList(t, ts)
	if !first.Recon.WindowOver {
		t.Fatal("the fixture's window has not closed, so nothing would be cached")
	}
	if first.ProbeCount != 1 || first.Recon.ProbedValidators != 1 {
		t.Fatalf("first read: probe_count %d, probed %d, want 1 and 1", first.ProbeCount, first.Recon.ProbedValidators)
	}
	if first.Recon.Status != "no" {
		t.Fatalf("first read status %q, want \"no\": the rows on record are short of the blob, and the reading happened",
			first.Recon.Status)
	}

	// The second read must be identical, which is the point of caching it.
	second := blobList(t, ts)
	if second.ProbeCount != first.ProbeCount || second.Recon != first.Recon ||
		len(second.Classes) != len(first.Classes) {
		t.Errorf("second read differs from the first:\n %+v\n %+v", first, second)
	}
	for k, n := range first.Classes {
		if second.Classes[k] != n {
			t.Errorf("second read class %s = %d, want %d", k, second.Classes[k], n)
		}
	}

	// Now the collector catches up with v2's probe, taken at the same schedule
	// point as v1's: a complete point, which is what settles the verdict.
	insertProbe(t, st, hash, "v2", point, probe.OutcomeServedOK)

	third := blobList(t, ts)
	if third.ProbeCount != 2 || third.Recon.ProbedValidators != 2 {
		t.Fatalf("after the late probe: probe_count %d, probed %d, want 2 and 2 — the cache did not notice a new row",
			third.ProbeCount, third.Recon.ProbedValidators)
	}
	if third.Recon.Status != "yes" {
		t.Errorf("status %q after both validators' rows came back: a stale verdict about a settled blob", third.Recon.Status)
	}
	if third.Classes["HEALTHY"] != 2 {
		t.Errorf("classes = %v, want two HEALTHY", third.Classes)
	}
}

// The store is not immutable: ApplyAmendment rewrites a probe row's
// classification in place when a deferred shadow verdict settles. That
// changes neither the number of probe rows nor the highest rowid, so a
// fingerprint built from those two alone could not see it, and the cached
// page went on publishing the pre-amendment tally beside the amended rows
// on the same page — the document contradicting itself about a named
// validator. On mocha this is the ordinary path, not an edge case: every
// shadow_pending row is filed PROBE_ERROR and amended later.
func TestCachedBlobVerdictFollowsAnAmendment(t *testing.T) {
	ts, st, hash, point := closedBlob(t)

	first := blobList(t, ts)
	if !first.Recon.WindowOver {
		t.Fatal("the fixture's window has not closed, so nothing would be cached")
	}
	if first.Classes["HEALTHY"] != 1 {
		t.Fatalf("first read classes = %v, want one HEALTHY", first.Classes)
	}

	// The scanner's frontier passes the promise timeout and the deferred
	// verdict settles: the row is rewritten, no row is added.
	ok, err := st.ApplyAmendment(store.Amendment{
		DedupeKey:        probe.Measurement{Vantage: "test", PromiseHash: hash, ValidatorAddress: "v1", ScheduledAt: point}.DedupeKey(),
		PromiseHash:      hash,
		ValidatorAddress: "v1",
		ScheduledAt:      point,
		From:             string(probe.ClassHealthy),
		To:               string(probe.ClassUnmatchedGenuine),
		Reason:           "no candidate in range",
		JudgedAt:         point.Add(2 * time.Hour),
		ScannerFrontier:  point.Add(2 * time.Hour),
	})
	if err != nil || !ok {
		t.Fatalf("amend: ok=%v err=%v", ok, err)
	}

	second := blobList(t, ts)
	if second.Classes["HEALTHY"] != 0 || second.Classes[string(probe.ClassUnmatchedGenuine)] != 1 {
		t.Fatalf("after the amendment classes = %v, want the amended class only — the cache did not notice the rewrite", second.Classes)
	}
}
