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
	// sameAs, when set, is the settlement time the next blob takes, so two
	// blobs can share a must_serve_until (and so a scheduled time).
	sameAs time.Time
}

func newReadings(t *testing.T) *readings {
	return &readings{t: t, st: openStore(t), settled: time.Date(2026, 9, 10, 12, 0, 0, 0, time.UTC)}
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
	gap        bool     // the request failed on this observer's side (PROBE_ERROR)
	missed     bool     // the prober missed the request (NOT_PROBED)
	unendorsed bool     // rows assigned, no endorsement on the promise
	late       bool     // the request started after must_serve_until (a stored row, phase grace)
	unassigned bool     // the promise gives it no rows: its row is stored, it is not in the assignment
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

// connected reports whether a request with this outcome opened its
// connection: everything but the answers from before one (no host to
// connect to, a lookup or a connect that failed).
func connected(o probe.Outcome) bool {
	switch o {
	case probe.OutcomeNoHost, probe.OutcomeBadHost, probe.OutcomeDNSFail, probe.OutcomeTCPTimeout, probe.OutcomeTCPUnreachable,
		probe.OutcomeTCPRefused, probe.OutcomeProbeError, probe.OutcomeMissed:
		return false
	}
	return true
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
	// the fingerprint names the params, as the scanner's does
	p.Assignment.ProtocolParams.Fingerprint = fmt.Sprintf("fp-%d-%d", needed, total)
	distinct := map[int]bool{}
	for _, v := range vals {
		if v.unassigned {
			continue
		}
		p.Assignment.Validators = append(p.Assignment.Validators, scan.ValidatorAssignment{Address: v.name, VotingPower: 10, RowCount: len(v.holds), Rows: v.holds, Attested: !v.unendorsed})
		p.Assignment.Sigma += len(v.holds)
		for _, x := range v.holds {
			distinct[x] = true
		}
	}
	p.Assignment.Distinct, p.Assignment.ValidatorsWithRows = len(distinct), len(p.Assignment.Validators)
	raw, _ := json.Marshal(p)
	if _, err := r.st.UpsertPublication(p, raw); err != nil {
		r.t.Fatal(err)
	}
	r.pubs = append(r.pubs, p)
	for i, v := range vals {
		if v.unasked {
			continue
		}
		start, phase := at.Add(time.Duration(i)*time.Second), probe.PhaseInWindow
		if v.late {
			start, phase = msu.Add(5*time.Second), probe.PhaseGrace
		}
		m := probe.Measurement{SchemaVersion: probe.MeasurementSchemaVersion, Vantage: "ut-1", PromiseHash: hash, Commitment: hash,
			MustServeUntil: msu, ValidatorAddress: v.name, ValidatorHost: v.name + ":7980", Assigned: !v.unassigned, Attested: !v.unendorsed,
			AssignedRowCount: len(v.holds), ScheduleLabel: probe.EndReadLabel, ScheduledAt: at, StartedAt: start, FinishedAt: start.Add(time.Second),
			Phase: phase, Outcome: v.out, Classification: v.cls}
		switch {
		case v.gap:
			m.Outcome, m.Classification = probe.OutcomeProbeError, probe.ClassProbeError
		case v.missed:
			m.Outcome, m.Classification = probe.OutcomeMissed, probe.ClassNotProbed
		}
		m.TCP.OK = !v.gap && !v.missed && connected(m.Outcome)
		m.TLS.OK = m.TCP.OK && m.Outcome != probe.OutcomeTLSFail
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
	// Available: two validators' rows rebuild it, the third timed out, one
	// request failed here, and the reading never got to the last.
	hashes["available"] = r.blob(8, 32, served("a", rowsFrom(0, 4)), served("b", rowsFrom(4, 4)),
		failed("slow", rowsFrom(8, 4), probe.ClassUnreachable, probe.OutcomeTLSFail),
		validator{name: "here", holds: rowsFrom(12, 2), gap: true},
		validator{name: "never", holds: rowsFrom(14, 4), unasked: true})
	// Unavailable, not enough shards: the big validator did not serve, five
	// small ones did, one request failed here while the others reached
	// their validators, one handed over a token of its rows.
	short := served("short", rowsFrom(20, 4))
	short.cls, short.out, short.got = probe.ClassUnmatchedGenuine, probe.OutcomePartial, []uint32{20}
	// A validator with no host is asked like the rest, and not served when
	// the reading happened.
	hashes["notenough"] = r.blob(8, 32, failed("big", rowsFrom(0, 8), probe.ClassFault, probe.OutcomeNotFound),
		served("s1", rowsFrom(8, 1)), served("s2", rowsFrom(9, 1)), served("s3", rowsFrom(10, 1)), served("s4", rowsFrom(11, 1)),
		served("s5", rowsFrom(12, 1)), validator{name: "gap", holds: rowsFrom(13, 1), gap: true}, short,
		failed("nohost", rowsFrom(14, 1), probe.ClassNotRegistered, probe.OutcomeNoHost))
	// Unavailable, no shards retrieved: every validator failed, each its
	// own way.
	hashes["noshards"] = r.blob(8, 32, failed("a", rowsFrom(0, 2), probe.ClassUnreachable, probe.OutcomeTLSFail),
		failed("b", rowsFrom(2, 2), probe.ClassServerError, probe.OutcomeServerError),
		failed("c", rowsFrom(4, 2), probe.ClassThrottled, probe.OutcomeThrottled),
		failed("d", rowsFrom(6, 2), probe.ClassFault, probe.OutcomeNotFound))
	// Rows already verified, from a row whose verdict waits for the late
	// shadow judgement (PROBE_ERROR): they came back.
	deferred := served("deferred", rowsFrom(20, 6))
	deferred.cls, deferred.out, deferred.got = probe.ClassProbeError, probe.OutcomePartial, []uint32{20}
	hashes["deferred"] = r.blob(8, 32, failed("bigd", rowsFrom(0, 8), probe.ClassFault, probe.OutcomeNotFound),
		served("s1", rowsFrom(8, 1)), served("s2", rowsFrom(9, 1)), deferred)
	// A validator that did not endorse is asked like the rest and never
	// counted, whatever it answers.
	other := failed("other", rowsFrom(6, 8), probe.ClassUnattested, probe.OutcomeNotFound)
	other.unendorsed = true
	hashes["wholeset"] = r.blob(8, 32, failed("e1", rowsFrom(0, 4), probe.ClassFault, probe.OutcomeNotFound),
		served("e2", rowsFrom(4, 2)), other)
	// Not read: every request failed on this observer's side, and the one
	// validator with no host was no one to reach.
	hashes["local"] = r.blob(8, 32, validator{name: "l1", holds: rowsFrom(0, 4), gap: true},
		validator{name: "l2", holds: rowsFrom(4, 4), gap: true},
		failed("l3", rowsFrom(8, 4), probe.ClassNotRegistered, probe.OutcomeNoHost))
	// Not read: not a single connection was opened, the connects timed out
	// (this observer's network down upstream).
	hashes["unconnected"] = r.blob(8, 32, failed("n1", rowsFrom(0, 4), probe.ClassUnreachable, probe.OutcomeTCPTimeout),
		failed("n2", rowsFrom(4, 4), probe.ClassUnreachable, probe.OutcomeTCPTimeout),
		failed("n3", rowsFrom(8, 4), probe.ClassNotRegistered, probe.OutcomeNoHost))
	// Not read: the prober missed part of the reading, as the older build
	// stored readings a validator at a time, and the rows are short. No one
	// is not served; the rows that came back are served all the same.
	hashes["partmissed"] = r.blob(8, 32, served("m1", rowsFrom(0, 4)),
		validator{name: "m2", holds: rowsFrom(4, 4), missed: true}, validator{name: "m3", holds: rowsFrom(8, 4), missed: true})
	// A reading is judged from all of its rows, whatever phase each
	// carries: a stored request that started after must_serve_until handed
	// over the rows that made the blob Available, so the validator that
	// timed out in the window counts neither way.
	lateRows := served("x2", rowsFrom(4, 4))
	lateRows.late = true
	hashes["straddle"] = r.blob(8, 32, served("x1", rowsFrom(0, 4)), lateRows,
		failed("x3", rowsFrom(8, 4), probe.ClassUnreachable, probe.OutcomeTLSFail))
	// The same, short of the rows: Unavailable, and the timed-out validator
	// is not served.
	lateShort := served("y2", rowsFrom(4, 1))
	lateShort.late = true
	hashes["straddle-short"] = r.blob(8, 32, served("y1", rowsFrom(0, 4)), lateShort,
		failed("y3", rowsFrom(8, 4), probe.ClassUnreachable, probe.OutcomeTLSFail))
	// Two blobs at one scheduled time (the publisher chose one
	// creation_timestamp for both): each is its own reading.
	r.sameAs = r.settled.Add(1000 * time.Minute)
	hashes["pair-available"] = r.blob(8, 32, served("q1", rowsFrom(0, 8)))
	hashes["pair-unavailable"] = r.blob(8, 32, failed("t1", rowsFrom(0, 4), probe.ClassFault, probe.OutcomeNotFound),
		served("u0", rowsFrom(4, 1)))
	r.sameAs = time.Time{}
	// A NOT_PROBED row says the prober missed the reading only when it is an
	// assigned validator's, in the window. One that started after
	// must_serve_until, or one of a validator the promise gives no rows,
	// missed nothing: the blob is Unavailable, and the validator that did
	// not serve is not served. The rows that came back are short of the
	// cheap bound, so the missed test is what decides both.
	lateMissed := validator{name: "g3", holds: rowsFrom(8, 4), missed: true}
	lateMissed.late = true
	hashes["missed-late"] = r.blob(8, 32, failed("g1", rowsFrom(0, 4), probe.ClassFault, probe.OutcomeNotFound),
		served("g2", rowsFrom(4, 1)), lateMissed)
	hashes["missed-no-rows"] = r.blob(8, 32, failed("h1", rowsFrom(0, 4), probe.ClassFault, probe.OutcomeNotFound),
		served("h2", rowsFrom(4, 1)), validator{name: "h3", missed: true, unassigned: true})
	return r, hashes
}

// Every row counts the same in SQL (rollup.CountedClass, which is how the
// stored readings are re-read, with no file rewritten, and NotServedSQL)
// and in the Go twin, and as the rule says.
func TestTheSQLAndTheGoTwinCountTheSameRows(t *testing.T) {
	r, hashes := fixture(t)
	ctx := context.Background()
	rows, _, blobs := r.twin()

	q, err := r.st.DB().QueryContext(ctx, `SELECT promise_hash, validator_address, `+rollup.CountedClass("probes")+`, `+
		rollup.NotServedSQL("probes")+` FROM probes`)
	if err != nil {
		t.Fatal(err)
	}
	sqlCls := map[string]string{}
	for q.Next() {
		var h, v, c string
		var notServed bool
		if err := q.Scan(&h, &v, &c, &notServed); err != nil {
			t.Fatal(err)
		}
		if notServed != (c == "FAULT") {
			t.Errorf("%s %s: CountedClass %q, not served %v", h[60:], v, c, notServed)
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
		want := row.CountedClass(rd)
		if got := sqlCls[row.PromiseHash+"|"+row.Validator]; got != string(want) {
			t.Errorf("%s %s: SQL %q, Go %q", row.PromiseHash[60:], row.Validator, got, want)
		}
	}
	for blob, want := range map[string]map[string]string{
		"available":        {"a": "HEALTHY", "slow": "NOT_COUNTED", "here": "PROBE_ERROR"},
		"notenough":        {"big": "FAULT", "s1": "HEALTHY", "gap": "FAULT", "short": "HEALTHY", "nohost": "FAULT"},
		"noshards":         {"a": "FAULT", "b": "FAULT", "c": "FAULT", "d": "FAULT"},
		"deferred":         {"bigd": "FAULT", "deferred": "HEALTHY"},
		"wholeset":         {"e1": "FAULT", "e2": "HEALTHY", "other": "UNATTESTED"},
		"local":            {"l1": "PROBE_ERROR", "l2": "PROBE_ERROR", "l3": "NOT_COUNTED"},
		"unconnected":      {"n1": "NOT_COUNTED", "n2": "NOT_COUNTED", "n3": "NOT_COUNTED"},
		"partmissed":       {"m1": "HEALTHY", "m2": "NOT_PROBED", "m3": "NOT_PROBED"},
		"straddle":         {"x1": "HEALTHY", "x3": "NOT_COUNTED"},
		"straddle-short":   {"y1": "HEALTHY", "y3": "FAULT"},
		"pair-available":   {"q1": "HEALTHY"},
		"pair-unavailable": {"t1": "FAULT", "u0": "HEALTHY"},
		"missed-late":      {"g1": "FAULT", "g2": "HEALTHY", "g3": "NOT_PROBED"},
		"missed-no-rows":   {"h1": "FAULT", "h2": "HEALTHY", "h3": "NOT_PROBED"},
	} {
		for v, c := range want {
			if got := sqlCls[hashes[blob]+"|"+v]; got != c {
				t.Errorf("%s %s: %q, want %q", blob, v, got, c)
			}
		}
	}
	for blob, want := range map[string][2]string{
		"available":        {verdict.BlobAvailable, ""},
		"notenough":        {verdict.BlobUnavailable, probe.ClientErrNotEnoughShards},
		"noshards":         {verdict.BlobUnavailable, probe.ClientErrNoShards},
		"deferred":         {verdict.BlobUnavailable, probe.ClientErrNotEnoughShards},
		"local":            {verdict.BlobNotRead, ""},
		"unconnected":      {verdict.BlobNotRead, ""},
		"partmissed":       {verdict.BlobNotRead, ""},
		"straddle":         {verdict.BlobAvailable, ""},
		"straddle-short":   {verdict.BlobUnavailable, probe.ClientErrNotEnoughShards},
		"pair-available":   {verdict.BlobAvailable, ""},
		"pair-unavailable": {verdict.BlobUnavailable, probe.ClientErrNotEnoughShards},
		"missed-late":      {verdict.BlobUnavailable, probe.ClientErrNotEnoughShards},
		"missed-no-rows":   {verdict.BlobUnavailable, probe.ClientErrNotEnoughShards},
	} {
		h := hashes[blob]
		if got := verdict.BlobReading(byPoint[h], blobs[h], false); got.Status != want[0] || got.Error != want[1] {
			t.Errorf("%s: %s %q, want %s %q", blob, got.Status, got.Error, want[0], want[1])
		}
	}
	partsAfter(t, r.st)
}

// Whether a reading happened is the same fact in SQL and in the Go twin
// (rollup.Reached, verdict.Reached), and it is a fact about the request: a
// request whose connection was opened, or refused, reached a server; one
// with no host, or whose lookup or connect failed, did not. One
// one-validator reading per outcome: the validator is not served exactly
// when its request reached a server.
func TestTheTwinsAgreeOnWhetherAReadingHappened(t *testing.T) {
	r := newReadings(t)
	for _, o := range probe.AllOutcomes {
		r.blob(8, 32, failed("v-"+string(o), rowsFrom(0, 8), probe.ClassUnreachable, o))
	}
	rows, _, blobs := r.twin()
	q, err := r.st.DB().QueryContext(context.Background(), `SELECT validator_address, `+rollup.CountedClass("probes")+` FROM probes`)
	if err != nil {
		t.Fatal(err)
	}
	sqlCls := map[string]string{}
	for q.Next() {
		var v, c string
		if err := q.Scan(&v, &c); err != nil {
			t.Fatal(err)
		}
		sqlCls[v] = c
	}
	q.Close()
	for _, row := range rows {
		want := row.CountedClass(verdict.ReadingOf([]verdict.Row{row}, blobs[row.PromiseHash]))
		if got := sqlCls[row.Validator]; got != string(want) {
			t.Errorf("%s: SQL %q, Go %q", row.Outcome, got, want)
		}
		if reached := probe.Reached(row.TCPOK, row.Outcome, false); (want == probe.ClassFault) != reached {
			t.Errorf("%s: counted %s, reached a server %v", row.Outcome, want, reached)
		}
	}
	for o, want := range map[probe.Outcome]string{probe.OutcomeNoHost: "NOT_COUNTED", probe.OutcomeTCPTimeout: "NOT_COUNTED",
		probe.OutcomeTCPUnreachable: "NOT_COUNTED", probe.OutcomeDNSFail: "NOT_COUNTED", probe.OutcomeTCPRefused: "FAULT",
		probe.OutcomeTLSFail: "FAULT", probe.OutcomeNotFound: "FAULT"} {
		if got := sqlCls["v-"+string(o)]; got != want {
			t.Errorf("%s alone: %q, want %q", o, got, want)
		}
	}
}

// The obligations the day's rollup draws agree with the Go twin's, and say
// what the rule says: not served only on an Unavailable blob, nothing
// counted against anyone on an Available one or where the reading did not
// happen.
func TestTheSQLAndTheGoTwinAgreeOnTheObligations(t *testing.T) {
	r, _ := fixture(t)
	ctx := context.Background()
	rows, settled, blobs := r.twin()
	now := r.settled.Add(20 * 24 * time.Hour)

	if _, err := rollup.Run(ctx, r.st, now, rollup.Config{RollupAfter: 14 * 24 * time.Hour}); err != nil {
		t.Fatal(err)
	}
	rolled, err := rollup.Load(ctx, r.st.DB(), now, "")
	if err != nil {
		t.Fatal(err)
	}
	w := verdict.Window{All: true, End: now}
	_, byVal := verdict.ComputeObligations(rows, settled, w, blobs)
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
	for _, v := range []string{"big", "gap", "nohost", "bigd", "e1", "t1", "b", "c", "d", "y3", "g1", "h1"} {
		if b := byVal[v]; b.Broken != 1 {
			t.Errorf("%s: %+v, want one not served", v, b)
		}
	}
	for _, v := range []string{"slow", "here", "l1", "l2", "l3", "n1", "n2", "n3", "m2", "m3", "x3"} {
		if b := byVal[v]; b.Total != 1 || b.Broken != 0 || b.Served != 0 {
			t.Errorf("%s: %+v, want counted neither way", v, b)
		}
	}
	for _, v := range []string{"short", "deferred", "u0", "q1", "x1", "y1", "m1", "g2", "h2"} {
		if b := byVal[v]; b.Served != 1 {
			t.Errorf("%s: %+v, want served", v, b)
		}
	}
	for _, v := range []string{"never", "x2", "y2", "g3", "h3"} {
		if _, ok := byVal[v]; ok {
			t.Errorf("%s: an obligation for a validator the reading never asked in the window", v)
		}
	}
	if _, ok := byVal["other"]; ok {
		t.Error("a validator that did not endorse has an obligation")
	}
}

// The readings already stored, re-read under the rule with no record
// rewritten: on 28 September four TLS handshakes timed out from this
// observer during the prober's overload, each on a blob whose other
// validators' rows came back verified (6,140, 7,843, 7,516 and 8,082
// distinct rows of the 4,096 needed). The blob is Available, and the
// timeout counts neither way.
func TestAStoredTimeoutOnAnAvailableBlobDoesNotCount(t *testing.T) {
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
	res := verdict.BlobReading(rows, blobs[hash], false)
	if res.Status != verdict.BlobAvailable || res.Have != 6140 {
		t.Fatalf("blob %+v, want Available with 6,140 rows", res)
	}
	w := verdict.Window{All: true, End: r.settled.Add(24 * time.Hour)}
	_, by := verdict.ComputeObligations(rows, settled, w, blobs)
	if got := by["timedout"]; got.Broken != 0 || got.NotCounted != 1 {
		t.Fatalf("timedout: %+v, want not counted", got)
	}
}
