package api_test

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/probe"
	"github.com/plsgiveup/fibre/fibre-sentinel/internal/scan"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/export"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/ingest"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/verdict"
)

// writeJSONL writes one JSON line per value.
func writeJSONL(t *testing.T, path string, vs ...any) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	var b []byte
	for _, v := range vs {
		j, err := json.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		b = append(append(b, j...), '\n')
	}
	if err := os.WriteFile(path, b, 0o644); err != nil {
		t.Fatal(err)
	}
}

// The stored readings give the same through the API as through the daily
// export redrawn the way sentinel-recompute redraws it: four blobs read at
// the end of their windows, one for each end the reading can come to.
//
//   - Available: two validators' rows rebuild it; the third timed out, and
//     counts neither way.
//   - Unavailable, not enough shards: one validator served, one answered
//     NOT_FOUND, one timed out; both are not served.
//   - Unavailable, no shards retrieved: every validator answered without
//     rows; every one is not served.
//   - Not read: not a single request reached a server (two failed on this
//     observer's side, the third validator has no host); nothing counts.
func TestTheExportRedrawsTheAPIsCounts(t *testing.T) {
	data := t.TempDir()
	st, err := store.Open(filepath.Join(t.TempDir(), "observer.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	now := time.Now().UTC().Truncate(time.Second)
	settled, msu := now.Add(-3*time.Hour), now.Add(-30*time.Minute)
	at := msu.Add(-10 * time.Minute)
	type val struct {
		addr string
		out  probe.Outcome
	}
	var pubs []any
	var ms []any
	blob := func(n int, result, clientErr string, vals ...val) string {
		hash := fmt.Sprintf("ex%062d", n)
		var as []scan.ValidatorAssignment
		for i, v := range vals {
			as = append(as, scan.ValidatorAssignment{Address: v.addr, VotingPower: 10, RowCount: 2, Rows: []int{2 * i, 2*i + 1}, Attested: true})
		}
		pubs = append(pubs, scan.Publication{
			SchemaVersion: scan.AttestationSchemaVersion, PromiseHash: hash,
			SettlementHeight: int64(200 + n), SettlementTime: settled, MustServeUntil: msu, RecordedAt: settled,
			SettlementTxHash: "tx" + hash[58:], Signer: "celestia1pub",
			Promise:                 scan.PromiseFields{ChainID: "t", Height: int64(199 + n), Commitment: "cc" + hash[60:], CreationTimestamp: settled, BlobSize: 4096},
			ValidatorSignatureCount: len(vals),
			Assignment: scan.AssignmentTable{
				ProtocolParams:     scan.ProtocolParamsSnapshot{OriginalRows: 4, TotalRows: 16},
				ValidatorSetHeight: int64(199 + n), TotalVotingPower: int64(10 * len(vals)), Sigma: 2 * len(vals), Distinct: 2 * len(vals),
				ValidatorsWithRows: len(vals), AttestedWithRows: len(vals), SignatureEntries: len(vals), SignaturesVerified: len(vals),
				AttestedVotingPower: int64(10 * len(vals)), Validators: as,
			},
		})
		for i, v := range vals {
			cls, reason := probe.Classify(probe.Evidence{Assigned: true, Attested: true, Phase: probe.PhaseInWindow, Outcome: v.out})
			m := probe.Measurement{SchemaVersion: probe.MeasurementSchemaVersion, Vantage: "test", PromiseHash: hash, Commitment: "cc" + hash[60:],
				MustServeUntil: msu, ValidatorSetHeight: int64(199 + n), ValidatorAddress: v.addr, ValidatorHost: v.addr + ":7980",
				Assigned: true, Attested: true, AssignedRowCount: 2, ScheduleLabel: probe.EndReadLabel, ScheduledAt: at,
				StartedAt: at.Add(time.Duration(i) * time.Second), FinishedAt: at.Add(time.Duration(i+1) * time.Second), Phase: probe.PhaseInWindow,
				Outcome: v.out, Classification: cls, ClassificationReason: reason, TotalDurationMS: 10, ClientRules: true}
			m.Read = &probe.ReadInfo{Order: i, BlobResult: result, BlobError: clientErr}
			// the connection was opened unless the request never left: no
			// host, or a failure on this observer's side
			m.TCP.Attempted = v.out != probe.OutcomeNoHost
			m.TCP.OK = m.TCP.Attempted && v.out != probe.OutcomeProbeError
			if v.out == probe.OutcomeServedOK {
				m.Download.OK, m.Download.RowsReturned, m.Download.RowsExpected = true, 2, 2
				m.Download.CommitmentVerified, m.Download.AssignmentVerified = true, true
				m.Download.RowIndices = []uint32{uint32(2 * i), uint32(2*i + 1)}
			}
			ms = append(ms, m)
		}
		return hash
	}
	available := blob(1, probe.ReadAvailable, "", val{"a1", probe.OutcomeServedOK}, val{"a2", probe.OutcomeServedOK}, val{"a3", probe.OutcomeRPCTimeout})
	notEnough := blob(2, probe.ReadUnavailable, probe.ClientErrNotEnoughShards,
		val{"okval", probe.OutcomeServedOK}, val{"goneval", probe.OutcomeNotFound}, val{"downval", probe.OutcomeRPCTimeout})
	noShards := blob(3, probe.ReadUnavailable, probe.ClientErrNoShards,
		val{"n1", probe.OutcomeNotFound}, val{"n2", probe.OutcomeThrottled}, val{"n3", probe.OutcomeTLSFail})
	notRead := blob(4, probe.ReadNotRead, "", val{"l1", probe.OutcomeProbeError}, val{"l2", probe.OutcomeProbeError},
		val{"l3", probe.OutcomeNoHost})
	writeJSONL(t, filepath.Join(data, "publications.jsonl"), pubs...)
	writeJSONL(t, filepath.Join(data, "measurements.jsonl"), ms...)

	// The collector's passes.
	if _, err := ingest.Publications(st, filepath.Join(data, "publications.jsonl"), now); err != nil {
		t.Fatal(err)
	}
	if _, err := ingest.Measurements(st, filepath.Join(data, "measurements.jsonl"), now); err != nil {
		t.Fatal(err)
	}
	ts := httptestServer(t, st)
	api := fetchObligations(t, ts, "window=all")
	byAddr := map[string]obligationsJSON{}
	for _, v := range api.Validators {
		byAddr[v.Address] = v.Obligations
	}
	for addr, want := range map[string][2]int64{ // served, not served
		"a1": {1, 0}, "a2": {1, 0}, "a3": {0, 0},
		"okval": {1, 0}, "goneval": {0, 1}, "downval": {0, 1},
		"n1": {0, 1}, "n2": {0, 1}, "n3": {0, 1},
		"l1": {0, 0}, "l2": {0, 0}, "l3": {0, 0},
	} {
		if got := byAddr[addr]; got.Served != want[0] || got.Broken != want[1] {
			t.Errorf("the API, %s: %+v; want served %d, not served %d", addr, got, want[0], want[1])
		}
	}
	apiBlob := map[string][2]string{}
	for _, h := range []string{available, notEnough, noShards, notRead} {
		var b struct {
			Blob struct {
				Reconstructable struct {
					Status string `json:"status"`
					Error  string `json:"error"`
				} `json:"reconstructable"`
			} `json:"blob"`
		}
		get(t, ts, "/v1/blobs/"+h, &b)
		apiBlob[h] = [2]string{b.Blob.Reconstructable.Status, b.Blob.Reconstructable.Error}
	}
	for h, want := range map[string][2]string{
		available: {verdict.BlobAvailable, ""},
		notEnough: {verdict.BlobUnavailable, probe.ClientErrNotEnoughShards},
		noShards:  {verdict.BlobUnavailable, probe.ClientErrNoShards},
		notRead:   {verdict.BlobNotRead, ""},
	} {
		if apiBlob[h] != want {
			t.Errorf("the API, blob %s: %v, want %v", h[60:], apiBlob[h], want)
		}
	}

	// The day's export, built from the same record and untarred: every
	// member appended to its path, which is how the union of several
	// exports is taken too.
	b := &export.Builder{DataDir: data, Dir: filepath.Join(data, "exports"), Vantage: "test", Build: "t", Hour: 0}
	built, err := b.Run(now.Add(26 * time.Hour))
	if err != nil || len(built) == 0 {
		t.Fatalf("export: %v %v", built, err)
	}
	untar := t.TempDir()
	for _, name := range built {
		raw, err := os.ReadFile(filepath.Join(data, "exports", name))
		if err != nil {
			t.Fatal(err)
		}
		a, err := export.ReadArchive(raw)
		if err != nil {
			t.Fatal(err)
		}
		if p := a.CheckMembers(); len(p) != 0 {
			t.Fatalf("%s: %v", name, p)
		}
		for member, body := range a.Members {
			p := filepath.Join(untar, filepath.FromSlash(member))
			if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
				t.Fatal(err)
			}
			f, err := os.OpenFile(p, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := f.Write(body); err != nil {
				t.Fatal(err)
			}
			f.Close()
		}
	}

	// sentinel-recompute's redraw over the untarred export.
	epubs, err := scan.LoadPublications(filepath.Join(untar, "publications.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	ems, err := probe.LoadMeasurements(filepath.Join(untar, "measurements.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	var rows []verdict.Row
	byBlob := map[string][]verdict.Row{}
	for _, m := range ems {
		r := verdict.FromMeasurement(m)
		rows = append(rows, r)
		byBlob[r.PromiseHash] = append(byBlob[r.PromiseHash], r)
	}
	settledAt := map[string]time.Time{}
	for _, p := range epubs {
		settledAt[p.PromiseHash] = p.SettlementTime
	}
	blobs := verdict.BlobsOf(epubs)
	_, byVal := verdict.ComputeObligations(rows, settledAt, verdict.Window{All: true, End: now}, blobs)
	for addr, want := range byAddr {
		if !same(want, byVal[addr]) {
			t.Errorf("%s: the API %+v, the export %+v", addr, want, byVal[addr])
		}
	}
	for _, p := range epubs {
		res := verdict.BlobOf(byBlob[p.PromiseHash], blobs[p.PromiseHash], p.MustServeUntil, now)
		if got := [2]string{res.Status, res.Error}; got != apiBlob[p.PromiseHash] {
			t.Errorf("blob %s: the export %v, the API %v", p.PromiseHash[60:], got, apiBlob[p.PromiseHash])
		}
	}
}
