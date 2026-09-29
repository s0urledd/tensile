package api

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/probe"
	"github.com/plsgiveup/fibre/fibre-sentinel/internal/scan"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/rollup"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/verdict"
)

// The obligation counts and the provisional faults were two statements over
// the same buckets and are now one pass (readObligations). Neither figure may
// move. The oracles are the statements as they shipped, kept verbatim
// (obligationsWhere still ships for the previous window), and every case
// compares them with the pass on the same store at the same clock.

// oldObligationsByValidator is obligationsByValidator as it shipped.
func oldObligationsByValidator(ctx context.Context, s *Server, win Window, extra string, extraArgs ...any) (map[string]obligationStats, error) {
	rows, err := s.st.DB().QueryContext(ctx, `SELECT validator_address, `+obligationSums+` FROM (`+obligationBuckets+extra+`)
			GROUP BY validator_address, promise_hash) GROUP BY validator_address`, s.obligationArgs(win, extraArgs...)...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]obligationStats{}
	for rows.Next() {
		var addr string
		var r rollup.Obligations
		if err := rows.Scan(append([]any{&addr}, scanObligations(&r)...)...); err != nil {
			return nil, err
		}
		out[addr] = obligationsOf(r)
	}
	return out, rows.Err()
}

// oldProvisionalByValidator is provisionalByValidator as it shipped.
func oldProvisionalByValidator(ctx context.Context, s *Server, win Window, now time.Time, extra string, extraArgs ...any) (map[string]*provisionalFaults, error) {
	out := map[string]*provisionalFaults{}
	if win.End.Before(now.Add(-verdict.FaultSettling)) {
		return out, nil
	}
	args := append(s.obligationArgs(win, extraArgs...), provisionalCutoff(now))
	rows, err := s.st.DB().QueryContext(ctx, `SELECT validator_address, COUNT(*), MAX(first_fault) FROM (`+obligationBuckets+extra+`)
			GROUP BY validator_address, promise_hash)
		WHERE NOT pending AND faults > 0 AND first_fault > ?
		GROUP BY validator_address`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var addr, youngest string
		var n int64
		if err := rows.Scan(&addr, &n, &youngest); err != nil {
			return nil, err
		}
		out[addr] = newProvisional(n, youngest)
	}
	return out, rows.Err()
}

// passFixture writes n publications settled every twenty minutes back from
// last, each read at the four in-window points by every validator, with
// the outcomes drawn at random: a validator that does not serve at a point
// leaves the blob unreadable there, so its failure counts as not served,
// and a missed request (NOT_PROBED) leaves the point uncounted.
func passFixture(t *testing.T, st *store.Store, r *rand.Rand, vals []string, last time.Time, n int) {
	t.Helper()
	outcomes := []probe.Outcome{probe.OutcomeServedOK, probe.OutcomeServedOK, probe.OutcomeServedOK, probe.OutcomeServedOK,
		probe.OutcomeNotFound, probe.OutcomeServerError, probe.OutcomeTCPRefused, probe.OutcomeMissed, probe.OutcomeProbeError}
	fractions := []float64{0.12, 0.45, 0.72, 0.92}
	labels := []string{"w1", "w2", "w3", probe.EndReadLabel}
	for k := 0; k < n; k++ {
		hash := fmt.Sprintf("%064x", k+1)
		created := last.Add(-time.Duration(k) * 20 * time.Minute)
		msu := created.Add(90 * time.Minute)
		var asg []scan.ValidatorAssignment
		for i, v := range vals {
			asg = append(asg, scan.ValidatorAssignment{Address: v, VotingPower: 10, RowCount: 2, Rows: []int{2 * i, 2*i + 1}, Attested: true})
		}
		pub := scan.Publication{
			SchemaVersion: scan.AttestationSchemaVersion, PromiseHash: hash,
			SettlementHeight: int64(1000 + k), SettlementTime: created, MustServeUntil: msu, RecordedAt: created,
			SettlementTxHash: "tx" + hash, Signer: "celestia1pub",
			Promise:                 scan.PromiseFields{ChainID: "t", Height: int64(999 + k), Commitment: "cc" + hash, CreationTimestamp: created, BlobSize: 4096},
			ValidatorSignatureCount: len(vals),
			Assignment: scan.AssignmentTable{
				ProtocolParams:     scan.ProtocolParamsSnapshot{OriginalRows: 2 * len(vals), TotalRows: 4 * len(vals)},
				ValidatorSetHeight: int64(999 + k), TotalVotingPower: int64(10 * len(vals)), Sigma: 2 * len(vals), Distinct: 2 * len(vals),
				ValidatorsWithRows: len(vals), AttestedWithRows: len(vals), SignatureEntries: len(vals), SignaturesVerified: len(vals),
				AttestedVotingPower: int64(10 * len(vals)), Validators: asg,
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
			for p, f := range fractions {
				at := created.Add(time.Duration(float64(msu.Sub(created)) * f)).Truncate(time.Second)
				out := outcomes[r.Intn(len(outcomes))]
				class, reason := probe.Classify(probe.Evidence{Assigned: true, Attested: true, Phase: probe.PhaseInWindow, Outcome: out})
				m := probe.Measurement{
					SchemaVersion: probe.AttestationSchemaVersion, Vantage: "test",
					PromiseHash: hash, Commitment: "cc" + hash, MustServeUntil: msu, ValidatorSetHeight: int64(999 + k),
					ValidatorAddress: v, ValidatorHost: v + ":443",
					Assigned: true, Attested: true, AssignedRowCount: 2,
					ScheduleLabel: labels[p], ScheduledAt: at, StartedAt: at, FinishedAt: at,
					Phase: probe.PhaseInWindow, Outcome: out,
					Classification: class, ClassificationReason: reason, TotalDurationMS: 10,
				}
				tls := out != probe.OutcomeTCPRefused && out != probe.OutcomeMissed && out != probe.OutcomeProbeError
				m.TCP.OK, m.TLS.OK, m.Identity.OK = tls, tls, tls
				if out == probe.OutcomeServedOK {
					m.Download.OK, m.Download.RowsReturned, m.Download.RowsExpected = true, 2, 2
					m.Download.CommitmentVerified, m.Download.AssignmentVerified = true, true
					m.Download.RowIndices = []uint32{uint32(2 * i), uint32(2*i + 1)}
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
}

// One pass gives the network and every validator the obligation counts and
// the provisional faults the two shipped statements gave: in every window,
// pinned or live, with a window over before the settling period and one
// cutting through it, for every filter the callers pass (none, validators
// excluded, one validator), at clocks that put the cutoff before, among and
// exactly on the faults' start times.
func TestObligationPassMatchesTheShippedStatements(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "observer.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	var vals []string
	for _, c := range "abcd" {
		vals = append(vals, strings.Repeat(string(c), 40))
	}
	last := time.Date(2026, 9, 28, 18, 0, 0, 0, time.UTC)
	passFixture(t, st, rand.New(rand.NewSource(11)), vals, last, 12)
	s := &Server{st: st}

	// Every fault start is a clock worth trying: with the cutoff exactly on
	// it, the fault is not provisional (the test is strictly after).
	var starts []time.Time
	rows, err := st.DB().Query(`SELECT DISTINCT started_at FROM probes WHERE classification <> 'HEALTHY' ORDER BY 1 DESC LIMIT 3`)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var a string
		if err := rows.Scan(&a); err != nil {
			t.Fatal(err)
		}
		at, err := time.Parse(store.TimeLayout, a)
		if err != nil {
			t.Fatal(err)
		}
		starts = append(starts, at)
	}
	rows.Close()
	clocks := []time.Time{last.Add(90 * time.Minute), last.Add(20 * time.Minute), last.Add(10 * 24 * time.Hour)}
	for _, a := range starts {
		clocks = append(clocks, a.Add(verdict.FaultSettling), a.Add(verdict.FaultSettling-time.Nanosecond), a.Add(verdict.FaultSettling+time.Nanosecond))
	}
	type filter struct {
		name  string
		extra string
		args  []any
	}
	filters := []filter{
		{"none", "", nil},
		{"exclude two", excludeSet{addrs: []any{vals[1], vals[3]}}.clause("pr.validator_address"), []any{vals[1], vals[3]}},
		{"one validator", ` AND pr.validator_address = ?`, []any{vals[2]}},
		{"nobody", ` AND pr.validator_address = ?`, []any{"nobody"}},
	}
	var provisional, broken, pending, compared int
	for _, now := range clocks {
		wins := []Window{windowFor("24h", now), windowFor("all", now),
			{Name: "all", End: last.Add(-3 * time.Hour), AsOf: true},
			{Name: "24h", Span: 24 * time.Hour, Start: now.Add(-24*time.Hour - 20*time.Minute), End: now.Add(-20 * time.Minute), AsOf: true}}
		for _, win := range wins {
			for _, f := range filters {
				pass, err := s.readObligations(ctx, win, now, f.extra, f.args...)
				if err != nil {
					t.Fatal(err)
				}
				where := fmt.Sprintf("clock %s, %s window ending %s, filter %s", store.TS(now), win.Name, store.TS(win.End), f.name)

				want, err := s.obligationsWhere(ctx, win, f.extra, f.args...)
				if err != nil {
					t.Fatal(err)
				}
				if got := pass.total(); !jsonEqual(t, got, want) {
					t.Errorf("%s: total\n got %+v\nwant %+v", where, got, want)
				}
				wantBy, err := oldObligationsByValidator(ctx, s, win, f.extra, f.args...)
				if err != nil {
					t.Fatal(err)
				}
				if got := pass.byValidator(); !jsonEqual(t, got, wantBy) {
					t.Errorf("%s: by validator\n got %+v\nwant %+v", where, got, wantBy)
				}
				wantProv, err := oldProvisionalByValidator(ctx, s, win, now, f.extra, f.args...)
				if err != nil {
					t.Fatal(err)
				}
				if !jsonEqual(t, pass.prov, wantProv) {
					t.Errorf("%s: provisional\n got %s\nwant %s", where, jsonOf(t, pass.prov), jsonOf(t, wantProv))
				}
				if !jsonEqual(t, provisionalTotal(pass.prov), provisionalTotal(wantProv)) {
					t.Errorf("%s: provisional total differs", where)
				}
				if len(wantProv) > 0 {
					provisional++
				}
				if want.Broken > 0 {
					broken++
				}
				if want.Pending > 0 {
					pending++
				}
				compared++
			}
		}
	}
	// The comparison is not vacuous: the fixture has provisional faults at
	// some clocks, final ones at others, and pending obligations.
	if provisional == 0 || broken == 0 || pending == 0 || provisional == compared {
		t.Fatalf("%d cases, %d with provisional faults, %d with broken, %d with pending: the fixture does not exercise the pass", compared, provisional, broken, pending)
	}
	t.Logf("%d cases, %d with provisional faults, %d with broken, %d with pending", compared, provisional, broken, pending)
}

func jsonOf(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func jsonEqual(t *testing.T, a, b any) bool {
	t.Helper()
	return jsonOf(t, a) == jsonOf(t, b)
}
