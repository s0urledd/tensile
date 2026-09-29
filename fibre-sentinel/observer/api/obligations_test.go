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

type obligationsJSON struct {
	Total               int64                    `json:"total"`
	Served              int64                    `json:"served"`
	Broken              int64                    `json:"broken"`
	HeldParamUnverified int64                    `json:"held_param_unverified"`
	NotCounted          int64                    `json:"not_counted"`
	Pending             int64                    `json:"pending"`
	Rate                struct{ Num, Den int64 } `json:"rate"`
}

// obligationsFixture: one blob, eight validators, one obligation each, every
// profile the buckets are meant to separate.
func obligationsFixture(t *testing.T) *httptest.Server {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "observer.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })

	now := time.Now().UTC().Truncate(time.Second)
	// The window ended half an hour ago, so every verdict below is final.
	created, msu := now.Add(-2*time.Hour), now.Add(-30*time.Minute)
	const hash = "obl1"
	addrs := []string{"served", "endun", "broken", "unreach", "reach", "backoff", "gaplast", "unatt"}
	var vals []scan.ValidatorAssignment
	for i, a := range addrs {
		vals = append(vals, scan.ValidatorAssignment{Address: a, VotingPower: 10, RowCount: 2, Rows: []int{2 * i, 2*i + 1}, Attested: a != "unatt"})
	}
	pub := scan.Publication{
		SchemaVersion: scan.AttestationSchemaVersion, PromiseHash: hash,
		SettlementHeight: 100, SettlementTime: created, MustServeUntil: msu, RecordedAt: now,
		SettlementTxHash: "tx", Signer: "celestia1pub",
		Promise:                 scan.PromiseFields{ChainID: "t", Height: 99, Commitment: "cc", CreationTimestamp: created, BlobSize: 4096},
		ValidatorSignatureCount: 7,
		Assignment: scan.AssignmentTable{
			// every assigned row is needed, so a validator that does not
			// serve at a point leaves the blob unreadable there
			ProtocolParams:     scan.ProtocolParamsSnapshot{OriginalRows: 16, TotalRows: 64},
			ValidatorSetHeight: 99, TotalVotingPower: 80, Sigma: 16, Distinct: 16,
			ValidatorsWithRows: 8, AttestedWithRows: 7, SignatureEntries: 7, SignaturesVerified: 7,
			AttestedVotingPower: 70, Validators: vals,
		},
	}
	raw, err := json.Marshal(pub)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.UpsertPublication(pub, raw); err != nil {
		t.Fatal(err)
	}

	// what each validator's four in-window probes saw, in schedule order
	profile := map[string][]wire{
		"served":  {ok, ok, ok, ok},
		"endun":   {ok, err500, err500, err500}, // served at 12%, answered 500 after: the early-prune profile
		"broken":  {ok, ok, ok, gone},
		"unreach": {refused, refused, refused, refused},
		"reach":   {err500, err500, err500, err500},
		"backoff": {skipped, skipped, skipped, skipped},
		// Three served probes and then nothing at the last point: the shape
		// this observer's own downtime produces. It used to publish as
		// served, so the serve rate rose while the observer was blind.
		"gaplast": {ok, ok, ok, skipped},
		"unatt":   {gone, gone, gone, gone},
	}
	held := map[string][]uint32{}
	for i, a := range addrs {
		held[a] = []uint32{uint32(2 * i), uint32(2*i + 1)}
	}
	points := []string{"w1", "w2", "w3", "w4"}
	for addr, ws := range profile {
		for i, w := range ws {
			at := inWindowPoint(created, msu, i)
			attested := addr != "unatt"
			class, reason := probe.Classify(probe.Evidence{
				Assigned: true, Attested: attested, Phase: probe.PhaseInWindow, Outcome: w.outcome,
			})
			m := probe.Measurement{
				SchemaVersion: probe.AttestationSchemaVersion, Vantage: "test",
				PromiseHash: hash, Commitment: "cc", MustServeUntil: msu, ValidatorSetHeight: 99,
				ValidatorAddress: addr, ValidatorHost: addr + ":443",
				Assigned: true, Attested: attested, AssignedRowCount: 2,
				ScheduleLabel: points[i], ScheduledAt: at, StartedAt: at, FinishedAt: at,
				Phase: probe.PhaseInWindow, Outcome: w.outcome,
				Classification: class, ClassificationReason: reason, TotalDurationMS: 10,
			}
			m.TCP.OK, m.TLS.OK, m.Identity.OK = w.tls, w.tls, w.tls
			if w.outcome == probe.OutcomeServedOK {
				m.Download.OK, m.Download.RowsReturned, m.Download.RowsExpected = true, 2, 2
				m.Download.CommitmentVerified, m.Download.AssignmentVerified = true, true
				m.Download.RowIndices = held[addr]
			}
			raw, err := json.Marshal(m)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := st.InsertProbe(m, raw); err != nil {
				t.Fatal(err)
			}
		}
	}
	confirmFailures(t, st, `promise_hash = ?`, hash)
	// A second blob whose retention window has not ended: one served probe
	// so far. Its obligation is pending, not served, until must_serve_until
	// passes; a verdict drawn before the last probe is not a verdict.
	insertProbeSet(t, st, "obl2", now.Add(-10*time.Minute), now.Add(time.Hour), map[string][]wire{
		"served": {ok},
	}, false)
	// A third blob at which every validator faulted at the same minute: the
	// share of the set faulting reaches the threshold, so those points are
	// the observer's problem (a stale pin, a broken coder) and no rate
	// counts them. The obligations, faults and per-probe rate must read as
	// if the blob had never been probed.
	insertProbeSet(t, st, "sus1", now.Add(-3*time.Hour), now.Add(-90*time.Minute), map[string][]wire{
		"served": {gone, gone, gone, gone}, "broken": {gone, gone, gone, gone},
		"reach": {gone, gone, gone, gone}, "gaplast": {gone, gone, gone, gone},
	}, true)
	if _, err := st.StartRun("collector", "test", "t", now); err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(api.New(st, "test"))
	t.Cleanup(ts.Close)
	return ts
}

// endVal is one endorsing validator of a reading insertReading writes: the
// rows it holds and what the reading got from it. unasked leaves it
// without a row, as a reading that had enough rows before its turn does.
type endVal struct {
	addr    string
	rows    int
	w       wire
	unasked bool
}

// insertReading writes one publication read once at the given time under
// label (probe.EndReadLabel for the one reading of a blob), needing needed
// distinct rows to reconstruct. Rows are assigned in order, so a validator
// that serves returns rows no other holds.
func insertReading(t *testing.T, st *store.Store, hash string, created, msu time.Time, needed int, label string, at time.Time, vals []endVal) {
	t.Helper()
	now := time.Now().UTC()
	var assigned []scan.ValidatorAssignment
	held := map[string][]uint32{}
	next := 0
	for _, v := range vals {
		var rows []int
		for i := 0; i < v.rows; i++ {
			rows = append(rows, next)
			held[v.addr] = append(held[v.addr], uint32(next))
			next++
		}
		assigned = append(assigned, scan.ValidatorAssignment{Address: v.addr, VotingPower: 10, RowCount: v.rows, Rows: rows, Attested: true})
	}
	pub := scan.Publication{
		SchemaVersion: scan.AttestationSchemaVersion, PromiseHash: hash,
		SettlementHeight: 300, SettlementTime: created, MustServeUntil: msu, RecordedAt: now,
		SettlementTxHash: "tx" + hash, Signer: "celestia1pub",
		Promise:                 scan.PromiseFields{ChainID: "t", Height: 299, Commitment: "cc" + hash, CreationTimestamp: created, BlobSize: 4096},
		ValidatorSignatureCount: len(vals),
		Assignment: scan.AssignmentTable{
			ProtocolParams:     scan.ProtocolParamsSnapshot{OriginalRows: needed, TotalRows: 4 * needed},
			ValidatorSetHeight: 299, TotalVotingPower: int64(10 * len(vals)), Sigma: next, Distinct: next,
			ValidatorsWithRows: len(vals), AttestedWithRows: len(vals), SignatureEntries: len(vals), SignaturesVerified: len(vals),
			AttestedVotingPower: int64(10 * len(vals)), Validators: assigned,
		},
	}
	raw, err := json.Marshal(pub)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.UpsertPublication(pub, raw); err != nil {
		t.Fatal(err)
	}
	for i, v := range vals {
		if v.unasked {
			continue
		}
		start := at.Add(time.Duration(i) * time.Second)
		class, reason := probe.Classify(probe.Evidence{Assigned: true, Attested: true, Phase: probe.PhaseInWindow, Outcome: v.w.outcome})
		m := probe.Measurement{
			SchemaVersion: probe.AttestationSchemaVersion, Vantage: "test",
			PromiseHash: hash, Commitment: "cc" + hash, MustServeUntil: msu, ValidatorSetHeight: 299,
			ValidatorAddress: v.addr, ValidatorHost: v.addr + ":443",
			Assigned: true, Attested: true, AssignedRowCount: v.rows,
			ScheduleLabel: label, ScheduledAt: at, StartedAt: start, FinishedAt: start,
			Phase: probe.PhaseInWindow, Outcome: v.w.outcome,
			Classification: class, ClassificationReason: reason, TotalDurationMS: 10,
		}
		m.TCP.OK, m.TLS.OK, m.Identity.OK = v.w.tls, v.w.tls, v.w.tls
		if v.w.outcome == probe.OutcomeServedOK {
			m.Download.OK, m.Download.RowsReturned, m.Download.RowsExpected = true, v.rows, v.rows
			m.Download.CommitmentVerified, m.Download.AssignmentVerified = true, true
			m.Download.RowIndices = held[v.addr]
		}
		raw, err := json.Marshal(m)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := st.InsertProbe(m, raw); err != nil {
			t.Fatal(err)
		}
	}
	confirmFailures(t, st, `promise_hash = ?`, hash)
}

// inWindowFractions are the prober's in-window schedule fractions
// (internal/probe/schedule.go). A fixture that puts its four probes in the
// first four minutes of a ninety-minute window is not testing the rule that
// decides served, which turns on whether a reading fell near the deadline.
var inWindowFractions = []float64{0.12, 0.45, 0.72, 0.92}

// inWindowPoint is where the i-th in-window probe of a promise falls.
func inWindowPoint(created, msu time.Time, i int) time.Time {
	return created.Add(time.Duration(float64(msu.Sub(created)) * inWindowFractions[i])).Truncate(time.Second)
}

// afterPoint is a moment between the i-th in-window probe and the next, for
// pinning a window mid-schedule.
func afterPoint(created, msu time.Time, i int) time.Time {
	a, b := inWindowPoint(created, msu, i), inWindowPoint(created, msu, i+1)
	return a.Add(b.Sub(a) / 2)
}

// wire is what one probe saw.
type wire struct {
	outcome probe.Outcome
	tls     bool
}

var (
	ok      = wire{probe.OutcomeServedOK, true}
	err500  = wire{probe.OutcomeServerError, true}
	gone    = wire{probe.OutcomeNotFound, true}
	refused = wire{probe.OutcomeTCPRefused, false}
	skipped = wire{probe.OutcomeReachable, true} // backoff: handshake done, download deliberately skipped
)

// insertProbeSet writes one publication and, per validator, its in-window
// probes in schedule order. allSuspect marks the intent only; the API decides.
//
// The blob needs every row it assigns (original_rows is their sum), so a
// validator that does not serve at a point leaves the blob unreadable there:
// its failure counts, as the rule says only an unreadable blob's failures do.
//
// Every failure it writes is confirmed from a second location (confirmed_by),
// so it counts as the rule has it; insertProbeSetUnconfirmed leaves them
// unconfirmed.
func insertProbeSet(t *testing.T, st *store.Store, hash string, created, msu time.Time, profile map[string][]wire, allSuspect bool) {
	t.Helper()
	insertProbeSetUnconfirmed(t, st, hash, created, msu, profile)
	confirmFailures(t, st, `promise_hash = ?`, hash)
}

// confirmFailures marks every failed reading the rows under where hold as
// confirmed from a second location, as the collector would once de-1
// answered: the failures of a fixture count as the rule has them.
func confirmFailures(t *testing.T, st *store.Store, where string, args ...any) {
	t.Helper()
	if _, err := st.DB().Exec(`UPDATE probes SET confirmed_by = 'de-1'
		WHERE commitment_verified = 0 AND classification NOT IN ('NOT_PROBED', 'PROBE_ERROR') AND `+where, args...); err != nil {
		t.Fatal(err)
	}
}

// insertProbeSetUnconfirmed is insertProbeSet with no second location's
// answer.
func insertProbeSetUnconfirmed(t *testing.T, st *store.Store, hash string, created, msu time.Time, profile map[string][]wire) {
	t.Helper()
	now := time.Now().UTC()
	var vals []scan.ValidatorAssignment
	held := map[string][]uint32{}
	i := 0
	for addr := range profile {
		vals = append(vals, scan.ValidatorAssignment{Address: addr, VotingPower: 10, RowCount: 2, Rows: []int{2 * i, 2*i + 1}, Attested: true})
		held[addr] = []uint32{uint32(2 * i), uint32(2*i + 1)}
		i++
	}
	needed, total := 2*len(vals), 16
	if needed > total {
		total = needed
	}
	pub := scan.Publication{
		SchemaVersion: scan.AttestationSchemaVersion, PromiseHash: hash,
		SettlementHeight: 200, SettlementTime: created, MustServeUntil: msu, RecordedAt: now,
		SettlementTxHash: "tx" + hash, Signer: "celestia1pub",
		Promise:                 scan.PromiseFields{ChainID: "t", Height: 199, Commitment: "cc" + hash, CreationTimestamp: created, BlobSize: 4096},
		ValidatorSignatureCount: len(vals),
		Assignment: scan.AssignmentTable{
			ProtocolParams:     scan.ProtocolParamsSnapshot{OriginalRows: needed, TotalRows: total},
			ValidatorSetHeight: 199, TotalVotingPower: int64(10 * len(vals)), Sigma: 2 * len(vals), Distinct: 2 * len(vals),
			ValidatorsWithRows: len(vals), AttestedWithRows: len(vals), SignatureEntries: len(vals), SignaturesVerified: len(vals),
			AttestedVotingPower: int64(10 * len(vals)), Validators: vals,
		},
	}
	raw, err := json.Marshal(pub)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.UpsertPublication(pub, raw); err != nil {
		t.Fatal(err)
	}
	points := []string{"w1", "w2", "w3", "w4"}
	for addr, ws := range profile {
		for i, w := range ws {
			at := inWindowPoint(created, msu, i)
			class, reason := probe.Classify(probe.Evidence{Assigned: true, Attested: true, Phase: probe.PhaseInWindow, Outcome: w.outcome})
			m := probe.Measurement{
				SchemaVersion: probe.AttestationSchemaVersion, Vantage: "test",
				PromiseHash: hash, Commitment: "cc" + hash, MustServeUntil: msu, ValidatorSetHeight: 199,
				ValidatorAddress: addr, ValidatorHost: addr + ":443",
				Assigned: true, Attested: true, AssignedRowCount: 2,
				ScheduleLabel: points[i], ScheduledAt: at, StartedAt: at, FinishedAt: at,
				Phase: probe.PhaseInWindow, Outcome: w.outcome,
				Classification: class, ClassificationReason: reason, TotalDurationMS: 10,
			}
			m.TCP.OK, m.TLS.OK, m.Identity.OK = w.tls, w.tls, w.tls
			if w.outcome == probe.OutcomeServedOK {
				m.Download.OK, m.Download.RowsReturned, m.Download.RowsExpected = true, 2, 2
				m.Download.CommitmentVerified, m.Download.AssignmentVerified = true, true
				m.Download.RowIndices = held[addr]
			}
			raw, err := json.Marshal(m)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := st.InsertProbe(m, raw); err != nil {
				t.Fatal(err)
			}
		}
	}
}

// An obligation is served only when this observer saw the shard near the end
// of the window the promise covers. The fixture's "gaplast" validator is the
// case that matters: three HEALTHY readings and then a gap at the last point,
// which is what an observer outage looks like from inside the record. Reading
// that as a kept promise made the serve rate rise while this observer was
// down, and credited an operator for hours nobody watched.
func TestAnObligationIsServedOnlyWhenTheEndOfItsWindowWasObserved(t *testing.T) {
	ts := obligationsFixture(t)

	var net struct {
		Obligations obligationsJSON          `json:"obligations"`
		ByObl       struct{ Num, Den int64 } `json:"serve_rate_by_obligation"`
		Classes     map[string]int64         `json:"classes"`
		Vantage     struct {
			Suspect     []struct{ Label, Reason string } `json:"suspect"`
			SuspectRows int64                            `json:"suspect_rows"`
		} `json:"vantage_health"`
	}
	if code := get(t, ts, "/v1/network?window=all", &net); code != 200 {
		t.Fatalf("network: %d", code)
	}
	o := net.Obligations
	// The unattested validator is nothing to keep or break, so seven from the
	// first blob, plus the second blob's one pending obligation; the third
	// blob's four are at suspect points and outside every count.
	if o.Total != 8 || o.Pending != 1 {
		t.Errorf("total/pending = %d/%d, want 8/1: seven proven and decided, one still in its window", o.Total, o.Pending)
	}
	if len(net.Vantage.Suspect) != 4 || net.Vantage.SuspectRows != 16 {
		t.Fatalf("suspect points = %+v (%d rows), want the third blob's four points and 16 rows", net.Vantage.Suspect, net.Vantage.SuspectRows)
	}
	for _, sp := range net.Vantage.Suspect {
		if sp.Reason != "fault" {
			t.Errorf("suspect point %s reason = %q, want fault", sp.Label, sp.Reason)
		}
	}
	// Four validators faulting at once at the third blob's points would have
	// been 16 FAULT readings; none is in any figure.
	if net.Classes["FAULT"] != 1 {
		t.Errorf("FAULT readings = %d, want 1: the sixteen at suspect points are the observer's, not the validators'", net.Classes["FAULT"])
	}
	if o.Served != 1 || o.Broken != 1 || o.NotCounted != 5 {
		t.Errorf("served/broken/not_counted = %d/%d/%d, want 1/1/5: only the validator answering at the last point is vouched for", o.Served, o.Broken, o.NotCounted)
	}
	if o.Rate.Num != 1 || o.Rate.Den != 2 {
		t.Errorf("obligation rate = %d/%d, want 1/2: only served and broken enter it", o.Rate.Num, o.Rate.Den)
	}
	if net.ByObl != o.Rate {
		t.Errorf("serve_rate_by_obligation %+v must repeat obligations.rate %+v", net.ByObl, o.Rate)
	}

	var resp struct {
		Validators []struct {
			Address     string          `json:"address"`
			Obligations obligationsJSON `json:"obligations"`
		} `json:"validators"`
	}
	if code := get(t, ts, "/v1/validators?window=all", &resp); code != 200 {
		t.Fatalf("validators: %d", code)
	}
	by := map[string]obligationsJSON{}
	for _, v := range resp.Validators {
		by[v.Address] = v.Obligations
	}
	want := map[string]obligationsJSON{
		"served":  {Total: 2, Served: 1, Pending: 1},
		"endun":   {Total: 1, NotCounted: 1},
		"broken":  {Total: 1, Broken: 1},
		"unreach": {Total: 1, NotCounted: 1},
		"reach":   {Total: 1, NotCounted: 1},
		"backoff": {Total: 1, NotCounted: 1},
		"gaplast": {Total: 1, NotCounted: 1},
		"unatt":   {},
	}
	for addr, w := range want {
		g := by[addr]
		g.Rate = struct{ Num, Den int64 }{}
		if g != w {
			t.Errorf("%s: obligations = %+v, want %+v", addr, g, w)
		}
	}
}

// The obligation SQL gained a lower bound on the probe rows so the planner
// stops walking the whole retained record for a windowed figure. It is a cost
// bound, not a filter, and the claim that lets it exist is narrow: a row with
// phase = 'in_window' was scheduled inside its promise's retention window,
// which opens at settlement_time, so a probe of a promise settled at or after
// the window start cannot have started materially before it. If that claim is
// ever wrong, an obligation silently leaves the count — the one direction
// this observer must not move in without saying so.
func TestObligationRowBoundDoesNotDropAnObligation(t *testing.T) {
	ts := obligationsFixture(t)
	type oblJSON struct {
		Total  int64 `json:"total"`
		Served int64 `json:"served"`
		Broken int64 `json:"broken"`
	}
	read := func(q string) oblJSON {
		t.Helper()
		var resp struct {
			Obligations oblJSON `json:"obligations"`
		}
		if code := get(t, ts, q, &resp); code != 200 {
			t.Fatalf("%s: %d", q, code)
		}
		return resp.Obligations
	}
	// Every window the dashboard offers, against the same record. The "all"
	// window's bound is the zero string, so it is the unbounded reference:
	// a narrower window may hold fewer obligations, but never more, and a
	// window wide enough to cover the record must match "all" exactly.
	all := read("/v1/network?window=all")
	if all.Total == 0 {
		t.Fatal("the sample record has no obligations; this test would prove nothing")
	}
	wide := read("/v1/network?window=30d")
	if wide != all {
		t.Errorf("30d = %+v but all = %+v, over a record that fits inside 30 days", wide, all)
	}
	for _, w := range []string{"24h", "7d", "30d"} {
		got := read("/v1/network?window=" + w)
		if got.Total > all.Total || got.Served > all.Served || got.Broken > all.Broken {
			t.Errorf("%s = %+v exceeds all = %+v", w, got, all)
		}
	}
}
