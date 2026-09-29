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
	// confirmed is every row a second location confirmed (confirmed_by),
	// by promise|validator.
	confirmed map[string]bool
	// sameAs, when set, is the settlement time the next blob takes, so two
	// blobs can share a must_serve_until (and so a scheduled time).
	sameAs time.Time
}

func newReadings(t *testing.T) *readings {
	return &readings{t: t, st: openStore(t), settled: time.Date(2026, 9, 10, 12, 0, 0, 0, time.UTC), confirmed: map[string]bool{}}
}

// validator is one validator of a blob: the rows it holds, and what the
// reading got from it.
type validator struct {
	name       string
	holds      []int
	cls        probe.Classification
	out        probe.Outcome
	got        []uint32 // rows that came back verified
	unasked    bool     // the reading never got to it
	gap        bool     // this observer could not read it (PROBE_ERROR)
	unendorsed bool     // rows assigned, no endorsement on the promise
	passedOver bool     // the deciding pass did not ask it (PASSED_OVER)
	// unconfirmed: a failure the second location did not confirm; every
	// other failure it did
	unconfirmed bool
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
	if !r.sameAs.IsZero() {
		settled = r.sameAs
	}
	msu := settled.Add(4 * time.Hour)
	at := msu.Add(-10 * time.Minute)
	p := scan.Publication{SchemaVersion: scan.AttestationSchemaVersion, PromiseHash: hash, SettlementHeight: int64(100 + r.n), SettlementTime: settled,
		MustServeUntil: msu, Promise: scan.PromiseFields{Commitment: hash, Height: int64(99 + r.n), CreationTimestamp: settled}}
	p.Assignment.ProtocolParams.OriginalRows, p.Assignment.ProtocolParams.TotalRows = needed, total
	distinct := map[int]bool{}
	for _, v := range vals {
		p.Assignment.Validators = append(p.Assignment.Validators, scan.ValidatorAssignment{Address: v.name, VotingPower: 10, RowCount: len(v.holds), Rows: v.holds, Attested: !v.unendorsed})
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
			MustServeUntil: msu, ValidatorAddress: v.name, ValidatorHost: v.name + ":7980", Assigned: true, Attested: !v.unendorsed,
			AssignedRowCount: len(v.holds), ScheduleLabel: probe.EndReadLabel, ScheduledAt: at, StartedAt: start, FinishedAt: start.Add(time.Second),
			Phase: probe.PhaseInWindow, Outcome: v.out, Classification: v.cls}
		if v.gap {
			m.Outcome, m.Classification = probe.OutcomeProbeError, probe.ClassProbeError
		}
		if v.passedOver {
			m.Outcome, m.Classification = probe.OutcomePassedOver, probe.ClassProbeError
		}
		m.TLS.OK = m.Outcome != probe.OutcomeTLSFail && !v.gap && !v.passedOver
		m.Download.RowsExpected = len(v.holds)
		if len(v.got) > 0 {
			m.Download.RowIndices, m.Download.RowsReturned, m.Download.CommitmentVerified = v.got, len(v.got), true
		}
		raw, _ := json.Marshal(m)
		if _, err := r.st.InsertProbe(m, raw); err != nil {
			r.t.Fatal(err)
		}
		r.ms = append(r.ms, m)
		if !v.unconfirmed && !v.gap && !v.passedOver && len(v.got) < len(v.holds) {
			if _, err := r.st.DB().Exec(`UPDATE probes SET confirmed_by = 'de-1' WHERE promise_hash = ? AND validator_address = ?`, hash, v.name); err != nil {
				r.t.Fatal(err)
			}
			r.confirmed[hash+"|"+v.name] = true
		}
	}
	return hash
}

func (r *readings) twin() ([]verdict.Row, map[string]time.Time, verdict.Blobs) {
	rows := make([]verdict.Row, 0, len(r.ms))
	for _, m := range r.ms {
		row := verdict.FromMeasurement(m)
		row.Confirmed = r.confirmed[m.PromiseHash+"|"+m.ValidatorAddress]
		rows = append(rows, row)
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
	// A rate limit is the validator not serving, and does not shield the
	// validator beside it.
	hashes["throttled"] = r.blob(8, 32, failed("nf", rowsFrom(0, 4), probe.ClassFault, probe.OutcomeNotFound),
		failed("thr", rowsFrom(4, 4), probe.ClassThrottled, probe.OutcomeThrottled),
		served("s1", rowsFrom(8, 1)), served("s2", rowsFrom(9, 1)), served("s3", rowsFrom(10, 1)))
	// Rows already verified, from a row whose verdict waits for the late
	// shadow judgement (PROBE_ERROR): counted once, in the rows.
	deferred := served("deferred", rowsFrom(20, 6))
	deferred.cls, deferred.out, deferred.got = probe.ClassProbeError, probe.OutcomePartial, []uint32{20}
	hashes["deferred"] = r.blob(8, 32, failed("bigd", rowsFrom(0, 8), probe.ClassFault, probe.OutcomeNotFound),
		served("s1", rowsFrom(8, 1)), served("s2", rowsFrom(9, 1)), deferred)
	// The endorsers alone are short, and a validator that did not endorse
	// was not asked: its rows could have made the blob.
	other := validator{name: "other", holds: rowsFrom(6, 8), unasked: true, unendorsed: true}
	hashes["wholeset"] = r.blob(8, 32, failed("e1", rowsFrom(0, 4), probe.ClassFault, probe.OutcomeNotFound),
		served("e2", rowsFrom(4, 2)), other)
	// Asked, it had nothing either: Unavailable over the whole set.
	other = failed("other", rowsFrom(6, 8), probe.ClassUnattested, probe.OutcomeNotFound)
	other.unendorsed = true
	hashes["wholeset-asked"] = r.blob(8, 32, failed("e1", rowsFrom(0, 4), probe.ClassFault, probe.OutcomeNotFound),
		served("e2", rowsFrom(4, 2)), other)
	// Available though most of those asked failed: the guard leaves it be.
	hashes["mostfail"] = r.blob(8, 32, served("a", rowsFrom(0, 8)),
		failed("x", rowsFrom(8, 2), probe.ClassUnreachable, probe.OutcomeTLSFail),
		failed("y", rowsFrom(10, 2), probe.ClassUnreachable, probe.OutcomeTLSFail),
		failed("z", rowsFrom(12, 2), probe.ClassUnreachable, probe.OutcomeTLSFail))
	// Unavailable, and the second location did not confirm the failure: it
	// counts neither way.
	uc := failed("uc", rowsFrom(0, 8), probe.ClassUnreachable, probe.OutcomeRPCTimeout)
	uc.unconfirmed = true
	hashes["unconfirmed"] = r.blob(8, 32, uc, served("s1", rowsFrom(8, 1)), served("s2", rowsFrom(9, 1)))
	// An endorser the deciding pass passed over, busy with this observer's
	// other readings, beside four not found and five served one row each:
	// it counts as failed to the guard, half of the ten, and the reading is
	// set aside as it would have been had the pass asked it.
	passed := []validator{{name: "po", holds: rowsFrom(0, 2), passedOver: true}}
	for i := 1; i <= 4; i++ {
		passed = append(passed, failed(fmt.Sprintf("pn%d", i), rowsFrom(2*i, 2), probe.ClassFault, probe.OutcomeNotFound))
	}
	for i := 0; i < 5; i++ {
		passed = append(passed, served(fmt.Sprintf("ps%d", i), rowsFrom(10+i, 1)))
	}
	hashes["passedover"] = r.blob(8, 32, passed...)
	// Two blobs at one scheduled time (the publisher chose one
	// creation_timestamp for both): every validator of the first failed, two
	// of the second's eight did. The guard is per reading, so the first is
	// set aside and the second's failures count.
	r.sameAs = r.settled.Add(1000 * time.Minute)
	hashes["pair-sacrifice"] = r.blob(8, 32, failed("p1", rowsFrom(0, 2), probe.ClassFault, probe.OutcomeNotFound),
		failed("p2", rowsFrom(2, 2), probe.ClassFault, probe.OutcomeNotFound), failed("p3", rowsFrom(4, 2), probe.ClassFault, probe.OutcomeNotFound),
		failed("p4", rowsFrom(6, 2), probe.ClassFault, probe.OutcomeNotFound))
	var target []validator
	target = append(target, failed("t1", rowsFrom(0, 4), probe.ClassFault, probe.OutcomeNotFound), failed("t2", rowsFrom(4, 4), probe.ClassFault, probe.OutcomeNotFound))
	for i := 0; i < 6; i++ {
		target = append(target, served(fmt.Sprintf("u%d", i), rowsFrom(8+i, 1)))
	}
	hashes["pair-target"] = r.blob(8, 32, target...)
	r.sameAs = time.Time{}
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
		"available":      {"a": "HEALTHY", "slow": "NOT_COUNTED"},
		"unavailable":    {"big": "FAULT", "s1": "HEALTHY", "gap": "PROBE_ERROR", "short": "FAULT"},
		"incomplete":     {"b": "NOT_COUNTED"},
		"mostfail":       {"x": "NOT_COUNTED"},
		"throttled":      {"nf": "FAULT", "thr": "FAULT", "s1": "HEALTHY"},
		"deferred":       {"bigd": "FAULT", "deferred": "PROBE_ERROR"},
		"wholeset":       {"e1": "NOT_COUNTED", "e2": "HEALTHY"},
		"wholeset-asked": {"e1": "FAULT", "e2": "HEALTHY", "other": "UNATTESTED"},
		"unconfirmed":    {"uc": "NOT_COUNTED", "s1": "HEALTHY"},
		"pair-target":    {"t1": "FAULT", "t2": "FAULT", "u0": "HEALTHY"},
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
			sqlSuspect = append(sqlSuspect, p.PromiseHash+"@"+p.At)
		}
	}
	w := verdict.Window{All: true, End: now}
	goPts := verdict.SuspectPoints(rows, w, blobs)
	var goSuspect []string
	for _, p := range goPts {
		goSuspect = append(goSuspect, p.PromiseHash+"@"+store.TS(p.At))
	}
	if fmt.Sprint(sqlSuspect) != fmt.Sprint(goSuspect) {
		t.Fatalf("suspect readings: SQL %v, Go %v", sqlSuspect, goSuspect)
	}
	want := map[string]bool{}
	for _, m := range r.ms {
		if m.PromiseHash == hashes["allfail"] || m.PromiseHash == hashes["pair-sacrifice"] || m.PromiseHash == hashes["passedover"] {
			want[m.PromiseHash+"@"+store.TS(m.ScheduledAt)] = true
		}
	}
	if len(sqlSuspect) != 3 || !want[sqlSuspect[0]] || !want[sqlSuspect[1]] || !want[sqlSuspect[2]] {
		t.Fatalf("suspect %v, want only the readings where every validator failed, and the one where half did with one passed over (%v)", sqlSuspect, want)
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
	if b := byVal["uc"]; b.NotCounted != 1 || b.Broken != 0 {
		t.Errorf("uc: %+v, want not counted: the second location did not confirm it", b)
	}
	// The reading beside a set-aside one at the same scheduled time is its
	// own: its failures count.
	for _, v := range []string{"t1", "t2"} {
		if b := byVal[v]; b.Broken != 1 {
			t.Errorf("%s: %+v, want one not served beside a set-aside reading at the same time", v, b)
		}
	}
	if b := byVal["p1"]; b.Broken != 0 || b.Total != 0 {
		t.Errorf("p1: %+v, want no obligation: its reading is set aside", b)
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

// A short answer (rows that verified, fewer than the validator holds) on an
// Unavailable blob waits for its deferred verdict at the reading, and is
// sent to the second location all the same: once the second location got
// no rows from it and the verdict is drawn, it counts not served; without
// the confirmation it never does.
func TestAShortAnswerCountsOnceConfirmedAndItsVerdictDrawn(t *testing.T) {
	r := newReadings(t)
	ctx := context.Background()
	short := served("short", rowsFrom(20, 4))
	short.cls, short.out, short.got = probe.ClassProbeError, probe.OutcomePartial, []uint32{20}
	short.unconfirmed = true
	other := short
	other.name = "other"
	other.holds, other.got = rowsFrom(24, 4), []uint32{24}
	hash := r.blob(8, 32, failed("big", rowsFrom(0, 8), probe.ClassFault, probe.OutcomeNotFound), served("s1", rowsFrom(8, 1)), short, other)
	count := func(v string) string {
		var cls string
		if err := r.st.DB().QueryRowContext(ctx, `SELECT `+rollup.CountedClass("probes")+` FROM probes WHERE promise_hash = ? AND validator_address = ?`, hash, v).Scan(&cls); err != nil {
			t.Fatal(err)
		}
		return cls
	}
	// the second location confirms the one, not the other
	if _, err := r.st.DB().Exec(`UPDATE probes SET confirmed_by = 'de-1' WHERE promise_hash = ? AND validator_address = 'short'`, hash); err != nil {
		t.Fatal(err)
	}
	if got := count("short"); got != string(probe.ClassProbeError) {
		t.Fatalf("before its verdict: %s, want PROBE_ERROR", got)
	}
	for _, m := range r.ms {
		if m.PromiseHash != hash || (m.ValidatorAddress != "short" && m.ValidatorAddress != "other") {
			continue
		}
		if _, err := r.st.ApplyAmendment(store.Amendment{DedupeKey: m.DedupeKey(), PromiseHash: hash, ValidatorAddress: m.ValidatorAddress,
			ScheduledAt: m.ScheduledAt, From: string(probe.ClassProbeError), To: string(probe.ClassUnmatchedGenuine), Reason: "late",
			JudgedAt: m.StartedAt.Add(time.Hour), ScannerFrontier: m.StartedAt.Add(time.Hour)}); err != nil {
			t.Fatal(err)
		}
	}
	if got := count("short"); got != string(probe.ClassFault) {
		t.Errorf("confirmed, verdict drawn: %s, want FAULT", got)
	}
	if got := count("other"); got != string(verdict.NotCounted) {
		t.Errorf("unconfirmed, verdict drawn: %s, want %s", got, verdict.NotCounted)
	}
}

// A second location that fetched the rows of a failed validator names
// itself (cleared_by) and rewrites nothing: the correlated-failure guard,
// drawn from this observer's own reading, sets the reading aside exactly as
// before.
func TestASecondLocationsAnswerNeverSwitchesTheGuardOff(t *testing.T) {
	r := newReadings(t)
	ctx := context.Background()
	hash := r.blob(64, 128, failed("a", rowsFrom(0, 4), probe.ClassUnreachable, probe.OutcomeRPCTimeout),
		failed("b", rowsFrom(4, 20), probe.ClassUnreachable, probe.OutcomeRPCTimeout),
		failed("c", rowsFrom(24, 20), probe.ClassUnreachable, probe.OutcomeRPCTimeout),
		served("d", rowsFrom(44, 10)), served("e", rowsFrom(54, 10)), served("f", rowsFrom(64, 10)))
	suspect := func() bool {
		pts, err := rollup.SuspectPoints(ctx, r.st.DB(), `promise_hash = ?`, hash)
		if err != nil {
			t.Fatal(err)
		}
		return len(pts) == 1 && pts[0].Reason() != ""
	}
	if !suspect() {
		t.Fatal("three of six unreachable is not set aside")
	}
	if _, err := r.st.DB().Exec(`UPDATE probes SET cleared_by = 'de-1', confirmed_by = NULL WHERE promise_hash = ? AND validator_address = 'a'`, hash); err != nil {
		t.Fatal(err)
	}
	if !suspect() {
		t.Fatal("the second location's answer lifted the guard")
	}
}
