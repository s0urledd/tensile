package correct_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/probe"
	"github.com/plsgiveup/fibre/fibre-sentinel/internal/scan"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/correct"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/ingest"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
)

// sampledPub is a second publication in the fixture's range, drawn out of
// the sample: its six points against the deadline it was recorded with.
func sampledPub(created time.Time) (scan.Publication, probe.SampledOut) {
	msu := created.Add(window)
	var vals []scan.ValidatorAssignment
	for i := 0; i < 3; i++ {
		vals = append(vals, scan.ValidatorAssignment{Address: addr(i), VotingPower: 10, RowCount: 2, Rows: []int{2 * i, 2*i + 1}, Attested: true})
	}
	pub := scan.Publication{
		SchemaVersion: scan.AttestationSchemaVersion, PromiseHash: "p2",
		SettlementHeight: 150, SettlementTxIndex: 1, SettlementTime: created, MustServeUntil: msu, RecordedAt: created,
		SettlementTxHash: "tx2", Signer: "celestia1pub",
		Promise:                 scan.PromiseFields{ChainID: "t", Height: 150, Commitment: "cc2", CreationTimestamp: created, BlobSize: 4096},
		ValidatorSignatureCount: 3,
		Assignment: scan.AssignmentTable{
			ProtocolParams:     scan.ProtocolParamsSnapshot{OriginalRows: 4, TotalRows: 16},
			ValidatorSetHeight: 149, TotalVotingPower: 30, Sigma: 6, Distinct: 6,
			ValidatorsWithRows: 3, AttestedWithRows: 3, SignatureEntries: 3, SignaturesVerified: 3,
			AttestedVotingPower: 30, Validators: vals,
		},
	}
	d := probe.SampledOut{
		SchemaVersion: probe.SampledOutSchemaVersion, Kind: probe.SampledOutKind, Vantage: "test",
		PromiseHash: "p2", Commitment: "cc2", SettlementTime: created, MustServeUntil: msu, ValidatorSetHeight: 149,
		DecidedAt: created.Add(10 * time.Second),
		Sampling:  probe.SamplingDecision{P: 0.3, Binding: "validator_bytes_per_day", DayCommitment: "c0ffee"},
		Reason:    "budget:p=0.300:validator_bytes_per_day:day_commitment=c0ffee", Validators: 3,
	}
	for _, pt := range earlierSchedule(pub) {
		d.Points = append(d.Points, probe.SampledOutPoint{Label: pt.Label, At: pt.At, Phase: probe.PhaseAt(pt.At, pub, probe.ScheduleConfig{})})
	}
	return pub, d
}

func sampledRows(t *testing.T, st *store.Store) string {
	t.Helper()
	r, err := st.DB().Query(`SELECT validator_address, schedule_label, scheduled_at, phase, classification, must_serve_until
		FROM obligation_rows WHERE promise_hash = 'p2' ORDER BY validator_address, scheduled_at`)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	var out []string
	for r.Next() {
		var a, l, s, p, c, m string
		if err := r.Scan(&a, &l, &s, &p, &c, &m); err != nil {
			t.Fatal(err)
		}
		out = append(out, fmt.Sprint(a, l, s, p, c, m))
	}
	return strings.Join(out, "\n")
}

// A verified range that moves a sampled-out publication's deadline re-grades
// the rows its decision stands for exactly as it re-grades stored rows: the
// same phases and deadline come out whether the store holds the rows or the
// decision, the moves are on the record, and a store rebuilt from that
// record reaches the same state.
func TestACorrectionReGradesASampledOutDecisionAsItsRows(t *testing.T) {
	clock := time.Now().UTC() // one clock for both stores, or a second boundary between them fails the comparison
	build := func(asRows bool) (*store.Store, time.Time, probe.SampledOut) {
		st, created := fixtureAt(t, 2, clock)
		pub, d := sampledPub(created)
		raw, _ := json.Marshal(pub)
		if _, err := st.UpsertPublication(pub, raw); err != nil {
			t.Fatal(err)
		}
		if asRows {
			for _, m := range d.Expand(pub) {
				raw, _ := json.Marshal(m)
				if _, err := st.InsertProbe(m, raw); err != nil {
					t.Fatal(err)
				}
			}
		} else {
			raw, _ := json.Marshal(d)
			if ok, err := st.InsertSampledOut(d, raw); err != nil || !ok {
				t.Fatalf("decision: %v %v", ok, err)
			}
		}
		record(t, st, verifiedRange(created))
		return st, created, d
	}
	run := func(st *store.Store, path string) {
		f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		if _, err := correct.New(st, f, 5*time.Minute).Run(context.Background(), time.Now().UTC()); err != nil {
			t.Fatal(err)
		}
	}
	rows, _, _ := build(true)
	dec, created, d := build(false)
	before := sampledRows(t, dec)
	dir := t.TempDir()
	run(rows, filepath.Join(dir, "rows.jsonl"))
	decPath := filepath.Join(dir, "decision.jsonl")
	run(dec, decPath)

	want, got := sampledRows(t, rows), sampledRows(t, dec)
	if want != got {
		t.Fatalf("after correction:\nrows:\n%s\ndecision:\n%s", want, got)
	}
	if got == before {
		t.Fatal("the correction moved nothing: the fixture does not exercise it")
	}
	b, err := os.ReadFile(decPath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), `"kind":"`+store.CorrectionSampledOutPoint+`"`) {
		t.Fatal("the decision's moves are not on the record")
	}

	// A second pass writes nothing more.
	run(dec, decPath)
	if b2, _ := os.ReadFile(decPath); len(b2) != len(b) {
		t.Fatalf("a second pass appended %d bytes", len(b2)-len(b))
	}

	// Replayed into a store rebuilt from the record: the publication, its
	// decision, and the record's lines about it.
	fresh, err := store.Open(filepath.Join(t.TempDir(), "rebuilt.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer fresh.Close()
	pub, _ := sampledPub(created)
	raw, _ := json.Marshal(pub)
	if _, err := fresh.UpsertPublication(pub, raw); err != nil {
		t.Fatal(err)
	}
	raw, _ = json.Marshal(d)
	if _, err := fresh.InsertSampledOut(d, raw); err != nil {
		t.Fatal(err)
	}
	var mine []byte
	for _, line := range strings.SplitAfter(string(b), "\n") {
		if strings.Contains(line, `"promise_hash":"p2"`) {
			mine = append(mine, line...)
		}
	}
	replayPath := filepath.Join(dir, "replay.jsonl")
	if err := os.WriteFile(replayPath, mine, 0o644); err != nil {
		t.Fatal(err)
	}
	if r, err := ingest.Corrections(fresh, replayPath, time.Now()); err != nil || r.Deferred != "" || r.Inserted == 0 {
		t.Fatalf("replay: %+v %v", r, err)
	}
	if replay := sampledRows(t, fresh); replay != got {
		t.Fatalf("replayed:\n%s\nlive:\n%s", replay, got)
	}
}

// earlierSchedule is the schedule the sampled-out decisions on record were
// made under: four in-window points, a grace point and a post point.
func earlierSchedule(pub scan.Publication) []probe.SchedulePoint {
	span := pub.MustServeUntil.Sub(pub.SettlementTime)
	var pts []probe.SchedulePoint
	for i, f := range []float64{0.12, 0.45, 0.72, 0.92} {
		pts = append(pts, probe.SchedulePoint{At: pub.SettlementTime.Add(time.Duration(float64(span) * f)), Phase: probe.PhaseInWindow, Label: fmt.Sprintf("w%d", i+1)})
	}
	return append(pts,
		probe.SchedulePoint{At: pub.MustServeUntil.Add(30 * time.Second), Phase: probe.PhaseGrace, Label: "grace"},
		probe.SchedulePoint{At: pub.MustServeUntil.Add(210 * time.Second), Phase: probe.PhasePost, Label: "post"})
}
