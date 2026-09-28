package rollup_test

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/probe"
	"github.com/plsgiveup/fibre/fibre-sentinel/internal/scan"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/rollup"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/verdict"
)

// readings is a store holding blobs read the way the prober reads them, and
// the same record as the Go twin sees it.
type readings struct {
	t       *testing.T
	st      *store.Store
	pubs    []scan.Publication
	ms      []probe.Measurement
	settled time.Time
	n       int
}

func newReadings(t *testing.T) *readings {
	return &readings{t: t, st: openStore(t), settled: time.Date(2026, 9, 10, 12, 0, 0, 0, time.UTC)}
}

// validator is one endorsing validator of a blob: the rows it holds, and
// what the reading got from it.
type validator struct {
	name    string
	holds   []int
	cls     probe.Classification
	out     probe.Outcome
	got     []uint32 // rows that came back verified
	unasked bool     // the reading never got to it
	gap     bool     // this observer could not read it (PROBE_ERROR)
}

func served(name string, holds []int) validator {
	got := make([]uint32, len(holds))
	for i, r := range holds {
		got[i] = uint32(r)
	}
	return validator{name: name, holds: holds, cls: probe.ClassHealthy, out: probe.OutcomeServedOK, got: got}
}

func failed(name string, holds []int, cls probe.Classification, out probe.Outcome) validator {
	return validator{name: name, holds: holds, cls: cls, out: out}
}

func rowsFrom(from, n int) []int {
	out := make([]int, n)
	for i := range out {
		out[i] = from + i
	}
	return out
}

// blob records one publication and its end-of-window reading, and returns
// its promise hash. Each blob settles a minute after the one before, so
// every reading has its own scheduled time.
func (r *readings) blob(needed, total int, vals ...validator) string {
	r.t.Helper()
	r.n++
	hash := fmt.Sprintf("%064x", r.n)
	settled := r.settled.Add(time.Duration(r.n) * time.Minute)
	msu := settled.Add(4 * time.Hour)
	at := msu.Add(-10 * time.Minute)
	p := scan.Publication{SchemaVersion: scan.AttestationSchemaVersion, PromiseHash: hash, SettlementHeight: int64(100 + r.n), SettlementTime: settled,
		MustServeUntil: msu, Promise: scan.PromiseFields{Commitment: hash, Height: int64(99 + r.n), CreationTimestamp: settled}}
	p.Assignment.ProtocolParams.OriginalRows, p.Assignment.ProtocolParams.TotalRows = needed, total
	distinct := map[int]bool{}
	for _, v := range vals {
		p.Assignment.Validators = append(p.Assignment.Validators, scan.ValidatorAssignment{Address: v.name, VotingPower: 10, RowCount: len(v.holds), Rows: v.holds, Attested: true})
		p.Assignment.Sigma += len(v.holds)
		for _, x := range v.holds {
			distinct[x] = true
		}
	}
	p.Assignment.Distinct, p.Assignment.ValidatorsWithRows = len(distinct), len(vals)
	raw, _ := json.Marshal(p)
	if _, err := r.st.UpsertPublication(p, raw); err != nil {
		r.t.Fatal(err)
	}
	r.pubs = append(r.pubs, p)
	for i, v := range vals {
		if v.unasked {
			continue
		}
		start := at.Add(time.Duration(i) * time.Second)
		m := probe.Measurement{SchemaVersion: probe.MeasurementSchemaVersion, Vantage: "ut-1", PromiseHash: hash, Commitment: hash,
			MustServeUntil: msu, ValidatorAddress: v.name, ValidatorHost: v.name + ":7980", Assigned: true, Attested: true,
			AssignedRowCount: len(v.holds), ScheduleLabel: probe.EndReadLabel, ScheduledAt: at, StartedAt: start, FinishedAt: start.Add(time.Second),
			Phase: probe.PhaseInWindow, Outcome: v.out, Classification: v.cls}
		if v.gap {
			m.Outcome, m.Classification = probe.OutcomeProbeError, probe.ClassProbeError
		}
		m.TLS.OK = m.Outcome != probe.OutcomeTLSFail && !v.gap
		m.Download.RowsExpected = len(v.holds)
		if len(v.got) > 0 {
			m.Download.RowIndices, m.Download.RowsReturned, m.Download.CommitmentVerified = v.got, len(v.got), true
		}
		raw, _ := json.Marshal(m)
		if _, err := r.st.InsertProbe(m, raw); err != nil {
			r.t.Fatal(err)
		}
		r.ms = append(r.ms, m)
	}
	return hash
}

func (r *readings) twin() ([]verdict.Row, map[string]time.Time, verdict.Blobs) {
	rows := make([]verdict.Row, 0, len(r.ms))
	for _, m := range r.ms {
		rows = append(rows, verdict.FromMeasurement(m))
	}
	settled := map[string]time.Time{}
	for _, p := range r.pubs {
		settled[p.PromiseHash] = p.SettlementTime
	}
	return rows, settled, verdict.BlobsOf(r.pubs)
}

// fixture is every shape of reading the rule tells apart.
func fixture(t *testing.T) (*readings, map[string]string) {
	r := newReadings(t)
	hashes := map[string]string{}
	// Available: two validators' rows rebuild it, the third timed out.
	hashes["available"] = r.blob(8, 32, served("a", rowsFrom(0, 4)), served("b", rowsFrom(4, 4)),
		failed("slow", rowsFrom(8, 4), probe.ClassUnreachable, probe.OutcomeTLSFail),
		validator{name: "never", holds: rowsFrom(12, 4), unasked: true})
	// Unavailable: the big validator did not serve, five small ones did,
	// one this observer could not read, one handed over a token of its rows.
	short := served("short", rowsFrom(20, 4))
	short.cls, short.out, short.got = probe.ClassUnmatchedGenuine, probe.OutcomePartial, []uint32{20}
	hashes["unavailable"] = r.blob(8, 32, failed("big", rowsFrom(0, 8), probe.ClassFault, probe.OutcomeNotFound),
		served("s1", rowsFrom(8, 1)), served("s2", rowsFrom(9, 1)), served("s3", rowsFrom(10, 1)), served("s4", rowsFrom(11, 1)),
		served("s5", rowsFrom(12, 1)), validator{name: "gap", holds: rowsFrom(13, 1), gap: true}, short)
	// Incomplete: the rows of a validator never asked could have filled it.
	hashes["incomplete"] = r.blob(8, 32, served("a", rowsFrom(0, 4)), failed("b", rowsFrom(4, 4), probe.ClassFault, probe.OutcomeNotFound),
		validator{name: "c", holds: rowsFrom(8, 4), unasked: true})
	// Every validator failed at once.
	hashes["allfail"] = r.blob(8, 32, failed("a", rowsFrom(0, 2), probe.ClassUnreachable, probe.OutcomeTLSFail),
		failed("b", rowsFrom(2, 2), probe.ClassServerError, probe.OutcomeServerError),
		failed("c", rowsFrom(4, 2), probe.ClassUnreachable, probe.OutcomeTCPTimeout),
		failed("d", rowsFrom(6, 2), probe.ClassFault, probe.OutcomeNotFound))
	// Available though most of those asked failed: the guard leaves it be.
	hashes["mostfail"] = r.blob(8, 32, served("a", rowsFrom(0, 8)),
		failed("x", rowsFrom(8, 2), probe.ClassUnreachable, probe.OutcomeTLSFail),
		failed("y", rowsFrom(10, 2), probe.ClassUnreachable, probe.OutcomeTLSFail),
		failed("z", rowsFrom(12, 2), probe.ClassUnreachable, probe.OutcomeTLSFail))
	return r, hashes
}

// Every row counts the same in SQL (rollup.CountedClass, which is how the
// stored readings are re-read, with no file rewritten) and in the Go twin.
func TestTheSQLAndTheGoTwinCountTheSameRows(t *testing.T) {
	r, hashes := fixture(t)
	ctx := context.Background()
	rows, _, blobs := r.twin()

	q, err := r.st.DB().QueryContext(ctx, `SELECT promise_hash, validator_address, `+rollup.CountedClass("probes")+` FROM probes`)
	if err != nil {
		t.Fatal(err)
	}
	sqlCls := map[string]string{}
	for q.Next() {
		var h, v, c string
		if err := q.Scan(&h, &v, &c); err != nil {
			t.Fatal(err)
		}
		sqlCls[h+"|"+v] = c
	}
	q.Close()
	byPoint := map[string][]verdict.Row{}
	for _, row := range rows {
		byPoint[row.PromiseHash] = append(byPoint[row.PromiseHash], row)
	}
	for _, row := range rows {
		rd := verdict.ReadingOf(byPoint[row.PromiseHash], blobs[row.PromiseHash])
		want := row.CountedClass(rd.Unavailable())
		if got := sqlCls[row.PromiseHash+"|"+row.Validator]; got != string(want) {
			t.Errorf("%s %s: SQL %q, Go %q", row.PromiseHash[60:], row.Validator, got, want)
		}
	}
	for blob, want := range map[string]map[string]string{
		"available":   {"a": "HEALTHY", "slow": "NOT_COUNTED"},
		"unavailable": {"big": "FAULT", "s1": "HEALTHY", "gap": "PROBE_ERROR", "short": "FAULT"},
		"incomplete":  {"b": "NOT_COUNTED"},
		"mostfail":    {"x": "NOT_COUNTED"},
	} {
		for v, c := range want {
			if got := sqlCls[hashes[blob]+"|"+v]; got != c {
				t.Errorf("%s %s: %q, want %q", blob, v, got, c)
			}
		}
	}
}

// The guard, and the obligations the day's rollup draws, agree between the
// two implementations: the every-validator-failed reading is set aside, the
// Available one the guard would otherwise catch is not.
func TestTheSQLAndTheGoTwinAgreeOnTheGuardAndTheObligations(t *testing.T) {
	r, hashes := fixture(t)
	ctx := context.Background()
	rows, settled, blobs := r.twin()
	now := r.settled.Add(20 * 24 * time.Hour)

	pts, err := rollup.SuspectPoints(ctx, r.st.DB(), `started_at >= ?`, "0000")
	if err != nil {
		t.Fatal(err)
	}
	var sqlSuspect []string
	for _, p := range pts {
		if p.Reason() != "" {
			sqlSuspect = append(sqlSuspect, p.At)
		}
	}
	w := verdict.Window{All: true, End: now}
	goPts := verdict.SuspectPoints(rows, w, blobs)
	var goSuspect []string
	for _, p := range goPts {
		goSuspect = append(goSuspect, store.TS(p.At))
	}
	if fmt.Sprint(sqlSuspect) != fmt.Sprint(goSuspect) {
		t.Fatalf("suspect points: SQL %v, Go %v", sqlSuspect, goSuspect)
	}
	var allfailAt string
	for _, m := range r.ms {
		if m.PromiseHash == hashes["allfail"] {
			allfailAt = store.TS(m.ScheduledAt)
		}
	}
	if len(sqlSuspect) != 1 || sqlSuspect[0] != allfailAt {
		t.Fatalf("suspect %v, want only the reading where every validator failed (%s)", sqlSuspect, allfailAt)
	}

	// The daily rollup, read back, against the Go twin.
	if _, err := rollup.Run(ctx, r.st, now, rollup.Config{RollupAfter: 14 * 24 * time.Hour}); err != nil {
		t.Fatal(err)
	}
	rolled, err := rollup.Load(ctx, r.st.DB(), now, "")
	if err != nil {
		t.Fatal(err)
	}
	_, byVal := verdict.ComputeObligations(rows, settled, w, goPts, blobs)
	names := map[string]bool{}
	for a := range byVal {
		names[a] = true
	}
	for a := range rolled.ObligationsByVal {
		names[a] = true
	}
	var sorted []string
	for a := range names {
		sorted = append(sorted, a)
	}
	sort.Strings(sorted)
	for _, a := range sorted {
		o := rolled.ObligationsByVal[a]
		got := verdict.Obligations{Total: o.Total, Served: o.Served, Broken: o.Broken, HeldParamUnverified: o.HeldParamUnverified,
			NotCounted: o.NotCounted(), Pending: o.Pending}
		if got != byVal[a] {
			t.Errorf("%s: rolled %+v, Go %+v", a, got, byVal[a])
		}
	}
	if b := byVal["big"]; b.Broken != 1 {
		t.Errorf("big: %+v, want one not served", b)
	}
	if b := byVal["slow"]; b.NotCounted != 1 || b.Broken != 0 {
		t.Errorf("slow: %+v, want not counted", b)
	}
}

// The readings already stored, re-read under the rule with no record
// rewritten: on 28 September four TLS handshakes timed out from this
// observer's one location during the prober's overload, each on a blob
// whose other validators' rows came back verified (6,140, 7,843, 7,516 and
// 8,082 distinct rows of the 4,096 needed). They were counted as not served;
// now they count neither way, and the blob is Available.
func TestAStoredTimeoutOnAnAvailableBlobNoLongerCounts(t *testing.T) {
	r := newReadings(t)
	ctx := context.Background()
	// 6,140 distinct rows from twelve validators, the way the record has it.
	var vals []validator
	from := 0
	for i := 0; i < 12; i++ {
		n := 512
		if i == 11 {
			n = 6140 - 11*512
		}
		vals = append(vals, served(fmt.Sprintf("v%02d", i), rowsFrom(from, n)))
		from += n
	}
	vals = append(vals, failed("timedout", rowsFrom(from, 700), probe.ClassUnreachable, probe.OutcomeTLSFail))
	hash := r.blob(4096, 16384, vals...)
	rows, settled, blobs := r.twin()

	var cls string
	if err := r.st.DB().QueryRowContext(ctx, `SELECT `+rollup.CountedClass("probes")+` FROM probes WHERE promise_hash = ? AND validator_address = 'timedout'`, hash).Scan(&cls); err != nil {
		t.Fatal(err)
	}
	if cls != string(verdict.NotCounted) {
		t.Fatalf("the stored TLS timeout counts as %q, want NOT_COUNTED", cls)
	}
	res := verdict.BlobReading(rows, blobs[hash], false, false)
	if res.Status != verdict.BlobAvailable || res.Have != 6140 {
		t.Fatalf("blob %+v, want Available with 6,140 rows", res)
	}
	w := verdict.Window{All: true, End: r.settled.Add(24 * time.Hour)}
	_, by := verdict.ComputeObligations(rows, settled, w, verdict.SuspectPoints(rows, w, blobs), blobs)
	if got := by["timedout"]; got.Broken != 0 || got.NotCounted != 1 {
		t.Fatalf("timedout: %+v, want not counted", got)
	}
}
