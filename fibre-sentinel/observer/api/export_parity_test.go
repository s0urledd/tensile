package api_test

import (
	"encoding/json"
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

// The daily export carries what a not-served count rests on. A blob read
// Unavailable at the end reading: one validator served its rows, one
// answered NOT_FOUND and the second location did not get the rows either
// (the reading counts), one timed out and the second location fetched its
// rows (it does not). The collector ingests the record and the second
// location's answers, and the API counts the one confirmed reading. The
// export built from the same record, untarred and redrawn the way
// sentinel-recompute redraws it, gives the API's obligations for every
// validator; without the second location's member it would count nothing.
func TestTheExportRedrawsTheConfirmedNotServedCount(t *testing.T) {
	data := t.TempDir()
	st, err := store.Open(filepath.Join(t.TempDir(), "observer.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	now := time.Now().UTC().Truncate(time.Second)
	settled, msu := now.Add(-3*time.Hour), now.Add(-30*time.Minute)
	at := msu.Add(-10 * time.Minute)
	const hash = "ex0000000000000000000000000000000000000000000000000000000000beef"
	vals := []scan.ValidatorAssignment{
		{Address: "okval", VotingPower: 10, RowCount: 2, Rows: []int{0, 1}, Attested: true},
		{Address: "goneval", VotingPower: 10, RowCount: 2, Rows: []int{2, 3}, Attested: true},
		{Address: "downval", VotingPower: 10, RowCount: 2, Rows: []int{4, 5}, Attested: true},
	}
	pub := scan.Publication{
		SchemaVersion: scan.AttestationSchemaVersion, PromiseHash: hash,
		SettlementHeight: 200, SettlementTime: settled, MustServeUntil: msu, RecordedAt: settled,
		SettlementTxHash: "txex", Signer: "celestia1pub",
		Promise:                 scan.PromiseFields{ChainID: "t", Height: 199, Commitment: "ccex", CreationTimestamp: settled, BlobSize: 4096},
		ValidatorSignatureCount: len(vals),
		Assignment: scan.AssignmentTable{
			ProtocolParams:     scan.ProtocolParamsSnapshot{OriginalRows: 4, TotalRows: 16},
			ValidatorSetHeight: 199, TotalVotingPower: 30, Sigma: 6, Distinct: 6,
			ValidatorsWithRows: 3, AttestedWithRows: 3, SignatureEntries: 3, SignaturesVerified: 3,
			AttestedVotingPower: 30, Validators: vals,
		},
	}
	row := func(addr string, out probe.Outcome) probe.Measurement {
		cls, reason := probe.Classify(probe.Evidence{Assigned: true, Attested: true, Phase: probe.PhaseInWindow, Outcome: out})
		m := probe.Measurement{SchemaVersion: probe.MeasurementSchemaVersion, Vantage: "test", PromiseHash: hash, Commitment: "ccex",
			MustServeUntil: msu, ValidatorSetHeight: 199, ValidatorAddress: addr, ValidatorHost: addr + ":7980",
			Assigned: true, Attested: true, AssignedRowCount: 2, ScheduleLabel: probe.EndReadLabel, ScheduledAt: at,
			StartedAt: at, FinishedAt: at.Add(time.Second), Phase: probe.PhaseInWindow, Outcome: out,
			Classification: cls, ClassificationReason: reason, TotalDurationMS: 10, ClientRules: true}
		m.Read = &probe.ReadInfo{Pass: 2, BlobResult: probe.ReadUnavailable}
		if out == probe.OutcomeServedOK {
			m.Download.OK, m.Download.RowsReturned, m.Download.RowsExpected = true, 2, 2
			m.Download.CommitmentVerified, m.Download.AssignmentVerified = true, true
			m.Download.RowIndices = []uint32{0, 1}
		}
		return m
	}
	answer := func(m probe.Measurement, out probe.Outcome, rows []uint32) probe.Measurement {
		a := m
		a.Vantage, a.StartedAt, a.FinishedAt, a.Read = "de-1", at.Add(3*time.Minute), at.Add(3*time.Minute+time.Second), nil
		a.Outcome = out
		a.Classification, a.ClassificationReason = probe.Classify(probe.Evidence{Assigned: true, Attested: true, Phase: probe.PhaseInWindow, Outcome: out})
		a.Download.OK, a.Download.RowsReturned, a.Download.RowsExpected = out == probe.OutcomeServedOK, len(rows), 2
		a.Download.CommitmentVerified, a.Download.AssignmentVerified = out == probe.OutcomeServedOK, out == probe.OutcomeServedOK
		a.Download.RowIndices = rows
		return a
	}
	ok, gone, down := row("okval", probe.OutcomeServedOK), row("goneval", probe.OutcomeNotFound), row("downval", probe.OutcomeRPCTimeout)
	writeJSONL(t, filepath.Join(data, "publications.jsonl"), pub)
	writeJSONL(t, filepath.Join(data, "measurements.jsonl"), ok, gone, down)
	vfile := filepath.Join(data, ingest.VantagesDir, "de-1", "measurements.jsonl")
	writeJSONL(t, vfile, answer(gone, probe.OutcomeNotFound, nil), answer(down, probe.OutcomeServedOK, []uint32{4, 5}))

	// The collector's passes.
	if _, err := ingest.Publications(st, filepath.Join(data, "publications.jsonl"), now); err != nil {
		t.Fatal(err)
	}
	if _, err := ingest.Measurements(st, filepath.Join(data, "measurements.jsonl"), now); err != nil {
		t.Fatal(err)
	}
	if r, err := ingest.VantageMeasurements(st, vfile, "test", now); err != nil || r.Inserted != 2 {
		t.Fatalf("second location's answers: %+v %v", r, err)
	}
	judge(t, st, now)
	api := fetchObligations(t, httptestServer(t, st), "window=all")
	byAddr := map[string]obligationsJSON{}
	for _, v := range api.Validators {
		byAddr[v.Address] = v.Obligations
	}
	if byAddr["goneval"].Broken != 1 || byAddr["downval"].Broken != 0 || byAddr["okval"].Served != 1 {
		t.Fatalf("the API: %+v; want goneval not served (confirmed), downval not counted (fetched there), okval served", byAddr)
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
	redraw := func(confirms map[string][]verdict.Confirmation) map[string]verdict.Obligations {
		pubs, err := scan.LoadPublications(filepath.Join(untar, "publications.jsonl"))
		if err != nil {
			t.Fatal(err)
		}
		ms, err := probe.LoadMeasurements(filepath.Join(untar, "measurements.jsonl"))
		if err != nil {
			t.Fatal(err)
		}
		var rows []verdict.Row
		for _, m := range ms {
			r := verdict.FromMeasurement(m)
			due := probe.Confirmable(m.ScheduleLabel, m.Classification) ||
				(m.Download.CommitmentVerified && m.Download.RowsReturned < m.AssignedRowCount)
			if cs, have := confirms[verdict.ConfirmationKey(m.PromiseHash, m.ValidatorAddress, m.ScheduledAt)]; have && due {
				_, by := verdict.ConfirmNotServedBy(verdict.NotServedOf(m), cs)
				r.Confirmed = by != ""
			}
			rows = append(rows, r)
		}
		settledAt := map[string]time.Time{}
		for _, p := range pubs {
			settledAt[p.PromiseHash] = p.SettlementTime
		}
		blobs := verdict.BlobsOf(pubs)
		win := verdict.Window{All: true, End: now}
		_, byVal := verdict.ComputeObligations(rows, settledAt, win, verdict.SuspectPoints(rows, win, blobs), blobs)
		return byVal
	}
	confirms := verdict.LoadConfirmations(filepath.Join(untar, export.VantagesMemberDir))
	if len(confirms) != 2 {
		t.Fatalf("the untarred export holds %d answered slots, want 2", len(confirms))
	}
	got := redraw(confirms)
	for addr, want := range byAddr {
		if !same(want, got[addr]) {
			t.Errorf("%s: the API %+v, the export %+v", addr, want, got[addr])
		}
	}
	if without := redraw(nil); without["goneval"].Broken != 0 {
		t.Errorf("without the second location's answers the export still counts goneval: %+v", without["goneval"])
	}
}
