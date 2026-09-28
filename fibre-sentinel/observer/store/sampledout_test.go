package store_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/probe"
	"github.com/plsgiveup/fibre/fibre-sentinel/internal/scan"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/ingest"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
)

// sampledFixture is one publication of three assigned validators (two
// attested) and one listed with no rows, and the decision that sampled it
// out: six points, so eighteen NOT_PROBED rows.
func sampledFixture(hash string, settle time.Time) (scan.Publication, probe.SampledOut) {
	msu := settle.Add(4 * time.Hour)
	pub := scan.Publication{
		SchemaVersion: scan.AttestationSchemaVersion, PromiseHash: hash,
		SettlementHeight: 100, SettlementTime: settle, MustServeUntil: msu, RecordedAt: settle,
		Promise: scan.PromiseFields{ChainID: "t", Height: 99, Commitment: "cc" + hash, CreationTimestamp: settle, BlobSize: 4096},
		Assignment: scan.AssignmentTable{
			ProtocolParams:     scan.ProtocolParamsSnapshot{OriginalRows: 4, TotalRows: 16},
			ValidatorSetHeight: 99, ValidatorsWithRows: 3,
			Validators: []scan.ValidatorAssignment{
				{Address: "va", RowCount: 6, Attested: true},
				{Address: "vb", RowCount: 6, Attested: false},
				{Address: "vc", RowCount: 4, Attested: true},
				{Address: "vd"},
			},
		},
	}
	d := probe.SampledOut{
		SchemaVersion: probe.SampledOutSchemaVersion, Kind: probe.SampledOutKind, Vantage: "ut-1",
		PromiseHash: hash, Commitment: pub.Promise.Commitment, SettlementTime: settle, MustServeUntil: msu,
		ValidatorSetHeight: 99, DecidedAt: settle.Add(9 * time.Second),
		Sampling: probe.SamplingDecision{P: 0.286, Binding: "validator_bytes_per_day", DayCommitment: "c0ffee"},
		Reason:   "budget:p=0.286:validator_bytes_per_day:day_commitment=c0ffee", Validators: 3,
	}
	for _, pt := range earlierSchedule(pub) {
		d.Points = append(d.Points, probe.SampledOutPoint{Label: pt.Label, At: pt.At, Phase: probe.PhaseAt(pt.At, pub, probe.ScheduleConfig{})})
	}
	return pub, d
}

func upsertPub(t *testing.T, st *store.Store, p scan.Publication) {
	t.Helper()
	raw, _ := json.Marshal(p)
	if _, err := st.UpsertPublication(p, raw); err != nil {
		t.Fatal(err)
	}
}

func insertRow(t *testing.T, st *store.Store, m probe.Measurement) bool {
	t.Helper()
	raw, _ := json.Marshal(m)
	ok, err := st.InsertProbe(m, raw)
	if err != nil {
		t.Fatal(err)
	}
	return ok
}

func count(t *testing.T, st *store.Store, q string, args ...any) int64 {
	t.Helper()
	var n int64
	if err := st.DB().QueryRow(q, args...).Scan(&n); err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	return n
}

// The rows the store derives for a decision are the rows the prober used to
// write for it, column for column, so every figure reads the same.
func TestSampledOutRowsViewIsTheRows(t *testing.T) {
	settle := time.Now().UTC().Add(-6 * time.Hour).Truncate(time.Second)
	pub, d := sampledFixture("so1", settle)

	rows := open(t)
	upsertPub(t, rows, pub)
	for _, m := range d.Expand(pub) {
		insertRow(t, rows, m)
	}
	dec := open(t)
	upsertPub(t, dec, pub)
	raw, _ := json.Marshal(d)
	if ok, err := dec.InsertSampledOut(d, raw); err != nil || !ok {
		t.Fatalf("insert decision: %v %v", ok, err)
	}
	if n := count(t, dec, `SELECT COUNT(*) FROM probes`); n != 0 {
		t.Fatalf("a decision stored %d probe rows", n)
	}
	const cols = `vantage, promise_hash, validator_address, assigned, assigned_row_count, attested, schedule_label, scheduled_at,
		started_at, phase, outcome, classification, classification_reason, tls_ok, retention_unverified, must_serve_until,
		sampling_p, sampling_binding, sampling_commitment`
	dump := func(st *store.Store, from string) []string {
		r, err := st.DB().Query(`SELECT ` + cols + ` FROM ` + from + ` ORDER BY validator_address, scheduled_at`)
		if err != nil {
			t.Fatal(err)
		}
		defer r.Close()
		var out []string
		for r.Next() {
			v := make([]any, 19)
			p := make([]any, len(v))
			for i := range v {
				p[i] = &v[i]
			}
			if err := r.Scan(p...); err != nil {
				t.Fatal(err)
			}
			out = append(out, fmt.Sprint(v...))
		}
		return out
	}
	a, b := dump(rows, "probes"), dump(dec, "sampled_out_rows")
	if len(a) != 18 || len(a) != len(b) {
		t.Fatalf("rows %d, derived %d", len(a), len(b))
	}
	for i := range a {
		if a[i] != b[i] {
			t.Fatalf("row %d:\n stored  %s\n derived %s", i, a[i], b[i])
		}
	}
	// And through the views the figures read.
	for _, v := range []string{"probe_rows", "obligation_rows"} {
		for _, c := range []string{"started_at", "phase", "classification", "attested", "outcome"} {
			q := `SELECT ` + c + `, COUNT(*) FROM ` + v + ` GROUP BY 1 ORDER BY 1`
			if x, y := dumpQ(t, rows, q), dumpQ(t, dec, q); x != y {
				t.Fatalf("%s: stored %s, derived %s", q, x, y)
			}
		}
	}
}

func dumpQ(t *testing.T, st *store.Store, q string) string {
	t.Helper()
	r, err := st.DB().Query(q)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	cols, _ := r.Columns()
	var out []string
	for r.Next() {
		v := make([]any, len(cols))
		p := make([]any, len(cols))
		for i := range v {
			p[i] = &v[i]
		}
		if err := r.Scan(p...); err != nil {
			t.Fatal(err)
		}
		out = append(out, fmt.Sprint(v...))
	}
	return fmt.Sprint(out)
}

// The migration turns the rows an older prober wrote for a sampled-out
// publication into its decision and deletes them, and touches nothing else:
// a set it cannot reproduce exactly keeps its rows.
func TestMigrationCollapsesOnlyWholeSampledOutSets(t *testing.T) {
	path := filepath.Join(t.TempDir(), "observer.db")
	st, err := store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	settle := time.Now().UTC().Add(-6 * time.Hour).Truncate(time.Second)

	// whole: collapses.
	whole, dw := sampledFixture("whole", settle)
	upsertPub(t, st, whole)
	for _, m := range dw.Expand(whole) {
		insertRow(t, st, m)
	}
	// short: one row missing (a prober stopped mid-write).
	short, ds := sampledFixture("short", settle)
	upsertPub(t, st, short)
	for i, m := range ds.Expand(short) {
		if i != 3 {
			insertRow(t, st, m)
		}
	}
	// mixed: a real probe of the promise beside the sampled-out rows.
	mixed, dm := sampledFixture("mixed", settle)
	upsertPub(t, st, mixed)
	ms := dm.Expand(mixed)
	for _, m := range ms[1:] {
		insertRow(t, st, m)
	}
	real := ms[0]
	real.Outcome, real.Classification, real.ClassificationReason = probe.OutcomeServedOK, probe.ClassHealthy, "served"
	insertRow(t, st, real)
	// twop: two draws disagree on p.
	twop, d2 := sampledFixture("twop", settle)
	upsertPub(t, st, twop)
	for i, m := range d2.Expand(twop) {
		if i == 0 {
			m.Sampling = &probe.SamplingDecision{P: 0.5, Binding: m.Sampling.Binding, DayCommitment: m.Sampling.DayCommitment}
		}
		insertRow(t, st, m)
	}
	// perval: a per-validator budget denial, not a draw.
	perval, dp := sampledFixture("perval", settle)
	upsertPub(t, st, perval)
	for _, m := range dp.Expand(perval) {
		m.ClassificationReason = "budget:validator_bytes_per_day"
		insertRow(t, st, m)
	}
	before := count(t, st, `SELECT COUNT(*) FROM probes`)
	st.Close()

	// Back to schema 23, as the live store is before the upgrade.
	raw, err := store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, q := range []string{
		`DROP VIEW probe_rows`, `DROP VIEW obligation_rows`, `DROP VIEW sampled_out_rows`, `DROP TABLE sampling_decision_points`,
		`DROP TABLE sampling_decisions`, `DROP INDEX probes_sampled_out`, `DELETE FROM schema_migrations WHERE version = 24`,
	} {
		if _, err := raw.DB().Exec(q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	raw.Close()

	st, err = store.Open(path) // runs migration 24
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if n := count(t, st, `SELECT COUNT(*) FROM sampling_decisions`); n != 1 {
		t.Fatalf("want 1 decision, got %d", n)
	}
	if n := count(t, st, `SELECT COUNT(*) FROM sampling_decisions WHERE promise_hash = 'whole' AND source = 'rows' AND sampling_p = 0.286 AND points = 6 AND validators = 3`); n != 1 {
		t.Fatal("the whole set was not collapsed into its decision")
	}
	if n := count(t, st, `SELECT COUNT(*) FROM probes WHERE promise_hash = 'whole'`); n != 0 {
		t.Fatalf("%d rows of the collapsed set are left", n)
	}
	if after := count(t, st, `SELECT COUNT(*) FROM probes`); after != before-18 {
		t.Fatalf("probes %d -> %d: only the 18 collapsed rows may go", before, after)
	}
	if n := count(t, st, `SELECT COUNT(*) FROM probe_rows`); n != before {
		t.Fatalf("probe_rows %d, want every row the store held before (%d)", n, before)
	}
	// The recorded decision parses as the prober's own record.
	var rj string
	if err := st.DB().QueryRow(`SELECT raw_json FROM sampling_decisions WHERE promise_hash = 'whole'`).Scan(&rj); err != nil {
		t.Fatal(err)
	}
	var got probe.SampledOut
	if err := json.Unmarshal([]byte(rj), &got); err != nil {
		t.Fatal(err)
	}
	if got.Kind != probe.SampledOutKind || got.Sampling.P != 0.286 || len(got.Points) != 6 || got.Reason != dw.Reason ||
		!got.DecidedAt.Equal(dw.DecidedAt) || got.Points[0].Label != dw.Points[0].Label || !got.Points[0].At.Equal(dw.Points[0].At) {
		t.Fatalf("recorded decision: %+v", got)
	}

	// Re-reading the old lines (a cursor reset, a rebuild) does not bring
	// the deleted rows back.
	for _, m := range dw.Expand(whole) {
		if insertRow(t, st, m) {
			t.Fatal("a sampled-out row came back beside its decision")
		}
	}
	if n := count(t, st, `SELECT COUNT(*) FROM probes WHERE promise_hash = 'whole'`); n != 0 {
		t.Fatalf("%d rows came back", n)
	}
	// A second collapse finds nothing more to do.
	if n, rows, err := st.CollapseSampledOut(context.Background()); err != nil || n != 0 || rows != 0 {
		t.Fatalf("second collapse: %d decisions, %d rows, %v", n, rows, err)
	}
}

// A store rebuilt from a measurements.jsonl that still holds the rows gets
// the same decisions the migration made, and re-reading the file from the
// start after that does not bring the rows back.
func TestRebuildFromOldRowsCollapsesAndStaysCollapsed(t *testing.T) {
	dir := t.TempDir()
	settle := time.Now().UTC().Add(-6 * time.Hour).Truncate(time.Second)
	pub, d := sampledFixture("rb1", settle)
	var lines []byte
	for _, m := range d.Expand(pub) {
		b, _ := json.Marshal(m)
		lines = append(append(lines, b...), '\n')
	}
	meas := filepath.Join(dir, "measurements.jsonl")
	if err := os.WriteFile(meas, lines, 0o644); err != nil {
		t.Fatal(err)
	}
	st := open(t)
	upsertPub(t, st, pub)
	now := time.Now()
	if r, err := ingest.Measurements(st, meas, now); err != nil || r.Inserted != 18 {
		t.Fatalf("ingest: %+v %v", r, err)
	}
	n, rows, err := st.CollapseSampledOut(context.Background())
	if err != nil || n != 1 || rows != 18 {
		t.Fatalf("collapse: %d decisions, %d rows, %v", n, rows, err)
	}
	if err := st.SetCursor(meas, 0, 0, now); err != nil {
		t.Fatal(err)
	}
	if r, err := ingest.Measurements(st, meas, now); err != nil || r.Read != 18 || r.Inserted != 0 {
		t.Fatalf("re-ingest: %+v %v", r, err)
	}
	if n := count(t, st, `SELECT COUNT(*) FROM probes`); n != 0 {
		t.Fatalf("%d rows came back", n)
	}
	if n := count(t, st, `SELECT COUNT(*) FROM probe_rows`); n != 18 {
		t.Fatalf("probe_rows %d, want 18", n)
	}
}

// sampling_decisions.jsonl is tailed like every record file: idempotent,
// a half-written last line left for the next pass, a bad line stepped over.
func TestIngestSampledOutFile(t *testing.T) {
	dir := t.TempDir()
	st := open(t)
	settle := time.Now().UTC().Add(-6 * time.Hour).Truncate(time.Second)
	p1, d1 := sampledFixture("f1", settle)
	p2, d2 := sampledFixture("f2", settle)
	upsertPub(t, st, p1)
	upsertPub(t, st, p2)
	b1, _ := json.Marshal(d1)
	b2, _ := json.Marshal(d2)
	path := filepath.Join(dir, probe.SampledOutFile)
	half := len(b2) / 2
	if err := os.WriteFile(path, append(append(append([]byte{}, b1...), '\n'), b2[:half]...), 0o644); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	r, err := ingest.SampledOut(st, path, now)
	if err != nil || r.Inserted != 1 {
		t.Fatalf("first pass: %+v %v", r, err)
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	f.Write(append(append([]byte{}, b2[half:]...), '\n'))
	f.Write([]byte("{not json\n"))
	f.Close()
	r, err = ingest.SampledOut(st, path, now)
	if err != nil || r.Inserted != 1 || r.Skipped != 1 {
		t.Fatalf("second pass: %+v %v", r, err)
	}
	if err := st.SetCursor(path, 0, 0, now); err != nil {
		t.Fatal(err)
	}
	if r, err := ingest.SampledOut(st, path, now); err != nil || r.Inserted != 0 {
		t.Fatalf("re-read: %+v %v", r, err)
	}
	if n := count(t, st, `SELECT COUNT(*) FROM sampling_decisions`); n != 2 {
		t.Fatalf("decisions %d", n)
	}
	if n := count(t, st, `SELECT COUNT(*) FROM sampling_decision_points`); n != 12 {
		t.Fatalf("points %d", n)
	}
	if n := count(t, st, `SELECT COUNT(*) FROM probe_rows`); n != 36 {
		t.Fatalf("probe_rows %d, want 2 x 3 validators x 6 points", n)
	}
}

// A decision is never stored beside rows its vantage already wrote for the
// promise: both would be counted.
func TestSampledOutNotStoredBesideRows(t *testing.T) {
	st := open(t)
	settle := time.Now().UTC().Add(-6 * time.Hour).Truncate(time.Second)
	pub, d := sampledFixture("dup", settle)
	upsertPub(t, st, pub)
	m := d.Expand(pub)[0]
	m.Outcome, m.Classification, m.ClassificationReason = probe.OutcomeServedOK, probe.ClassHealthy, "served"
	insertRow(t, st, m)
	raw, _ := json.Marshal(d)
	if ok, err := st.InsertSampledOut(d, raw); err != nil || ok {
		t.Fatalf("decision stored beside rows: %v %v", ok, err)
	}
	if n := count(t, st, `SELECT COUNT(*) FROM probe_rows`); n != 1 {
		t.Fatalf("probe_rows %d", n)
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
