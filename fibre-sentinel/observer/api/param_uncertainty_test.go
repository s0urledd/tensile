package api_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/probe"
	"github.com/plsgiveup/fibre/fibre-sentinel/internal/scan"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/api"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/correct"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/ingest"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/verdict"
)

// shortWindow is the retention the params really carried from height 150.
// The fixture's window is 90 minutes and its in-window points sit at
// probe.DefaultInWindowFractions, so the last two land at 64.8 and 82.8
// minutes. 55 minutes puts both of them past the deadline and past the
// five-minute prune tolerance, which is what turns their NOT_FOUND from a
// fault into an expected prune.
const shortWindow = 55 * time.Minute

// seedParams puts one params value into the store's history, as ingesting
// the scanner's state.json does.
func seedParams(t *testing.T, st *store.Store, fromHeight int64, created time.Time, window time.Duration) {
	t.Helper()
	p := scan.ParamsSnapshot{
		WithdrawalDelaySeconds:       int64(13 * time.Hour / time.Second),
		PaymentPromiseTimeoutSeconds: 600,
		ShardRetentionSeconds:        int64(window / time.Second),
		PaymentPromiseHeightWindow:   1000,
		EffectiveFromHeight:          fromHeight, EffectiveFromTxIndex: -1, Source: "seed",
	}
	if err := st.UpsertParams([]scan.ParamEntry{{FromHeight: fromHeight, FromTxIndex: -1, Source: "seed", ParamsJSON: p}}); err != nil {
		t.Fatal(err)
	}
}

// verifiedRange is the same range openRange records, closed by reading
// every height: the long window until 150, the short one from there.
func verifiedRange(created, msu time.Time) scan.ParamUncertainty {
	long := scan.ParamsSnapshot{
		WithdrawalDelaySeconds: int64(13 * time.Hour / time.Second), PaymentPromiseTimeoutSeconds: 600,
		ShardRetentionSeconds: int64(msu.Sub(created) / time.Second), PaymentPromiseHeightWindow: 1000,
		EffectiveFromHeight: 120, EffectiveFromTxIndex: -1, Source: "verified",
	}
	short := long
	short.ShardRetentionSeconds = int64(shortWindow / time.Second)
	short.EffectiveFromHeight = 150
	at := time.Now().UTC()
	return scan.ParamUncertainty{
		SchemaVersion: scan.ParamUncertaintySchemaVersion,
		ID:            "t:silent_change:121-180", ChainID: "t", Kind: scan.UncertaintySilentChange,
		FromHeight: 121, ToHeight: 180, EffectiveFromHeight: 181, IntervalStartKnown: true,
		Direction: "shorter", PublicationsAffected: 1, DetectedAt: at,
		Resolution: scan.ResolutionVerified, ResolveMethod: "exhaustive_read", HeightsRead: 61, ResolvedAt: &at,
		Values: []scan.ResolvedValue{{FromHeight: 120, Params: long}, {FromHeight: 150, Params: short}},
	}
}

// heldJSON is the part of /v1/network these tests read.
type heldJSON struct {
	Faults      int64            `json:"faults"`
	Classes     map[string]int64 `json:"classes"`
	Obligations struct {
		Total                 int64 `json:"total"`
		Served                int64 `json:"served"`
		Broken                int64 `json:"broken"`
		EndUnobserved         int64 `json:"end_unobserved"`
		HeldParamUnverified   int64 `json:"held_param_unverified"`
		Unobserved            int64 `json:"unobserved"`
		UnobservedReachable   int64 `json:"unobserved_reachable"`
		UnobservedUnreachable int64 `json:"unobserved_unreachable"`
		UnobservedNotProbed   int64 `json:"unobserved_not_probed"`
		Pending               int64 `json:"pending"`
	} `json:"obligations"`
	ServeRate struct {
		Value *float64 `json:"value"`
		Num   int64    `json:"num"`
		Den   int64    `json:"den"`
	} `json:"serve_rate"`
	Coverage struct {
		Num int64 `json:"num"`
		Den int64 `json:"den"`
	} `json:"serve_rate_coverage"`
	HeldOut     map[string]int64 `json:"serve_rate_held_out"`
	Attestation struct {
		AttestedProbes   int64 `json:"attested_probes"`
		UnattestedProbes int64 `json:"unattested_probes"`
		UnknownProbes    int64 `json:"unknown_probes"`
	} `json:"attestation"`
	RetentionUncertainty *struct {
		OpenRanges       int64  `json:"open_ranges"`
		PublicationsHeld int64  `json:"publications_held"`
		ProbesHeld       int64  `json:"probes_held"`
		Note             string `json:"note"`
	} `json:"retention_uncertainty"`
	VantageHealth struct {
		Suspect []struct {
			At     string `json:"at"`
			Reason string `json:"reason"`
		} `json:"suspect"`
	} `json:"vantage_health"`
}

// heldFixture builds one publication whose promise sits at height 150,
// inside the range 121-180 the tests open, with `outcomes` per validator
// over four in-window schedule points.
func heldFixture(t *testing.T, outcomes map[string][]probe.Outcome) (*store.Store, time.Time, time.Time) {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "observer.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })

	now := time.Now().UTC().Truncate(time.Second)
	created, msu := now.Add(-2*time.Hour), now.Add(-30*time.Minute)

	var addrs []string
	for a := range outcomes {
		addrs = append(addrs, a)
	}
	sortStrings(addrs)
	var vals []scan.ValidatorAssignment
	for i, a := range addrs {
		vals = append(vals, scan.ValidatorAssignment{Address: a, VotingPower: 10, RowCount: 2, Rows: []int{2 * i, 2*i + 1}, Attested: true})
	}
	pub := scan.Publication{
		SchemaVersion: scan.AttestationSchemaVersion, PromiseHash: "held1",
		SettlementHeight: 150, SettlementTime: created, MustServeUntil: msu, RecordedAt: now,
		SettlementTxHash: "txheld", Signer: "celestia1pub",
		Promise:                 scan.PromiseFields{ChainID: "t", Height: 150, Commitment: "ccheld", CreationTimestamp: created, BlobSize: 4096},
		ValidatorSignatureCount: len(vals),
		Assignment: scan.AssignmentTable{
			ProtocolParams:     scan.ProtocolParamsSnapshot{OriginalRows: 4, TotalRows: 16},
			ValidatorSetHeight: 149, TotalVotingPower: int64(10 * len(vals)), Sigma: 2 * len(vals), Distinct: 2 * len(vals),
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
	for addr, os := range outcomes {
		for i, o := range os {
			at := inWindowPoint(created, msu, i)
			class, reason := probe.Classify(probe.Evidence{Assigned: true, Attested: true, Phase: probe.PhaseInWindow, Outcome: o})
			m := probe.Measurement{
				SchemaVersion: probe.AttestationSchemaVersion, Vantage: "test",
				PromiseHash: "held1", Commitment: "ccheld", MustServeUntil: msu, ValidatorSetHeight: 149,
				ValidatorAddress: addr, ValidatorHost: addr + ":443",
				Assigned: true, Attested: true, AssignedRowCount: 2,
				ScheduleLabel: points[i], ScheduledAt: at, StartedAt: at, FinishedAt: at,
				Phase: probe.PhaseInWindow, Outcome: o,
				Classification: class, ClassificationReason: reason, TotalDurationMS: 10,
			}
			m.TCP.OK, m.TLS.OK, m.Identity.OK = true, true, true
			if o == probe.OutcomeServedOK {
				m.Download.OK, m.Download.RowsReturned, m.Download.RowsExpected = true, 2, 2
				m.Download.CommitmentVerified, m.Download.AssignmentVerified = true, true
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
	return st, created, msu
}

func sortStrings(s []string) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j] < s[j-1]; j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
}

// openRange records a silent shortening over heights 121-180 that could not
// be closed, so it holds. The publication's promise height is 150.
func openRange(t *testing.T, st *store.Store) scan.ParamUncertainty {
	t.Helper()
	u := scan.ParamUncertainty{
		SchemaVersion: scan.ParamUncertaintySchemaVersion,
		ID:            "t:silent_change:121-180", ChainID: "t", Kind: scan.UncertaintySilentChange,
		FromHeight: 121, ToHeight: 180, EffectiveFromHeight: 181, IntervalStartKnown: true,
		Direction: "shorter", WindowBeforeS: 14400, WindowAfterS: 3600,
		PublicationsAffected: 1, DetectedAt: time.Now().UTC(),
		Resolution: scan.ResolutionUnresolvable, ResolveError: "the node cannot answer for those heights",
	}
	raw, err := json.Marshal(u)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.UpsertParamUncertainty(u, raw, time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := st.SyncParamHolds(context.Background()); err != nil {
		t.Fatal(err)
	}
	return u
}

func networkHeld(t *testing.T, st *store.Store) heldJSON {
	t.Helper()
	ts := httptest.NewServer(api.New(st, "test"))
	t.Cleanup(ts.Close)
	var out heldJSON
	get(t, ts, "/v1/network?window=24h", &out)
	return out
}

// The open P1, closed. Two validators prune on the retention the server
// really used and answer NOT_FOUND inside the window this observer still
// thinks is running. Two is under the correlated-failure guard's floor of
// three, so the guard does not fire and never would: the guard is a
// mitigation for a correlated outage, not for the observer being wrong
// about the deadline.
//
// The first half asserts the defect as a fact, so that anyone who "fixes"
// this by weakening the guard instead breaks a test that says why that is
// not the fix.
func TestTwoValidatorsPruningOnAShortenedRetentionAreNotPublishedAsFaults(t *testing.T) {
	ok := []probe.Outcome{probe.OutcomeServedOK, probe.OutcomeServedOK, probe.OutcomeServedOK, probe.OutcomeServedOK}
	gone := []probe.Outcome{probe.OutcomeServedOK, probe.OutcomeServedOK, probe.OutcomeNotFound, probe.OutcomeNotFound}
	outcomes := map[string][]probe.Outcome{
		"v1": gone, "v2": gone,
		"v3": ok, "v4": ok, "v5": ok, "v6": ok, "v7": ok, "v8": ok,
	}

	st, _, _ := heldFixture(t, outcomes)

	// Without the range on record: the fault is published, and the guard
	// says nothing about it.
	before := networkHeld(t, st)
	if before.Faults != 4 {
		t.Fatalf("without the range, faults = %d, want 4 (two validators x two points)", before.Faults)
	}
	if before.Obligations.Broken != 2 {
		t.Fatalf("without the range, broken = %d, want 2", before.Obligations.Broken)
	}
	if len(before.VantageHealth.Suspect) != 0 {
		t.Fatalf("two faulting validators is under MinValidators=%d; the guard must not fire: %+v",
			verdict.MinValidators, before.VantageHealth.Suspect)
	}
	if before.RetentionUncertainty != nil {
		t.Fatal("nothing is held yet")
	}

	// With it: neither the fault nor the credit is published.
	openRange(t, st)
	after := networkHeld(t, st)
	if after.Faults != 0 {
		t.Fatalf("faults = %d, want 0: this observer cannot say when the obligation ended", after.Faults)
	}
	if after.Obligations.Broken != 0 {
		t.Fatalf("broken = %d, want 0", after.Obligations.Broken)
	}
	if after.Obligations.HeldParamUnverified != 8 {
		t.Fatalf("held_param_unverified = %d, want 8: every obligation of the covered publication", after.Obligations.HeldParamUnverified)
	}
	if after.Obligations.Served != 0 {
		t.Fatalf("served = %d, want 0: withholding only the accusations would raise the rate", after.Obligations.Served)
	}
	if after.Obligations.Unobserved != 0 {
		t.Fatalf("unobserved = %d, want 0: the observer looked, it just cannot speak for what it saw", after.Obligations.Unobserved)
	}
	if n := after.HeldOut["RETENTION_UNVERIFIED"]; n != 32 {
		t.Fatalf("serve_rate_held_out[RETENTION_UNVERIFIED] = %d, want 32 (8 validators x 4 points)", n)
	}
	if after.ServeRate.Den != 0 || after.ServeRate.Value != nil {
		t.Fatalf("serve_rate = %+v, want no rate at all", after.ServeRate)
	}
	if after.RetentionUncertainty == nil || after.RetentionUncertainty.PublicationsHeld != 1 || after.RetentionUncertainty.OpenRanges != 1 {
		t.Fatalf("retention_uncertainty = %+v, want one range holding one publication", after.RetentionUncertainty)
	}
	// The removal is visible, not silent: the rows never left the
	// population, they changed name.
	if after.Coverage.Den != before.Coverage.Den {
		t.Fatalf("coverage.den moved from %d to %d; the hold is an override, not an exclusion", before.Coverage.Den, after.Coverage.Den)
	}
	assertReconciles(t, after)
}

// A share under the guard's threshold is the other half of the same hole:
// four of ten validators faulting is 40%, under FaultThreshold, so the
// guard does not fire. The hold does not care about the share at all —
// it is keyed on the publication, not on the schedule point.
func TestAShareUnderTheGuardThresholdIsStillHeld(t *testing.T) {
	ok := []probe.Outcome{probe.OutcomeServedOK, probe.OutcomeServedOK, probe.OutcomeServedOK, probe.OutcomeServedOK}
	gone := []probe.Outcome{probe.OutcomeServedOK, probe.OutcomeServedOK, probe.OutcomeNotFound, probe.OutcomeNotFound}
	outcomes := map[string][]probe.Outcome{}
	for _, a := range []string{"f1", "f2", "f3", "f4"} {
		outcomes[a] = gone
	}
	for _, a := range []string{"g1", "g2", "g3", "g4", "g5", "g6"} {
		outcomes[a] = ok
	}
	if share := 4.0 / 10.0; share >= verdict.FaultThreshold {
		t.Fatalf("this test no longer sits under the guard's threshold: %v >= %v", share, verdict.FaultThreshold)
	}

	st, _, _ := heldFixture(t, outcomes)
	before := networkHeld(t, st)
	if before.Faults != 8 || before.Obligations.Broken != 4 {
		t.Fatalf("without the range: faults=%d broken=%d, want 8/4", before.Faults, before.Obligations.Broken)
	}
	if len(before.VantageHealth.Suspect) != 0 {
		t.Fatalf("40%% faulting is under the threshold; the guard must not fire: %+v", before.VantageHealth.Suspect)
	}

	openRange(t, st)
	after := networkHeld(t, st)
	if after.Faults != 0 || after.Obligations.Broken != 0 {
		t.Fatalf("faults=%d broken=%d, want 0/0", after.Faults, after.Obligations.Broken)
	}
	// The symmetry that matters: the six clean validators are withheld too.
	// If only the four accusations were withheld, the published rate would
	// read 6/6 = 100% off the back of a deadline this observer cannot
	// vouch for — the exact inflation the codebase refuses for UNATTESTED
	// and for grace probes.
	if after.Obligations.Served != 0 || after.ServeRate.Den != 0 {
		t.Fatalf("served=%d serve_rate.den=%d: withholding only the failures inflates the rate",
			after.Obligations.Served, after.ServeRate.Den)
	}
	if after.Obligations.HeldParamUnverified != 10 {
		t.Fatalf("held_param_unverified = %d, want 10", after.Obligations.HeldParamUnverified)
	}
	assertReconciles(t, after)
}

// A held row is still a row, so the response must still reconcile against
// itself exactly as docs/verdicts.md promises. This is what the class
// override buys over a WHERE exclusion, and it is asserted rather than
// argued.
func assertReconciles(t *testing.T, r heldJSON) {
	t.Helper()
	if sum := r.Attestation.AttestedProbes + r.Attestation.UnattestedProbes + r.Attestation.UnknownProbes; sum != r.Coverage.Den {
		t.Errorf("serve_rate_coverage.den = %d but attested+unattested+unknown = %d", r.Coverage.Den, sum)
	}
	if r.HeldOut["UNATTESTED"] != r.Attestation.UnattestedProbes {
		t.Errorf("serve_rate_held_out[UNATTESTED] = %d but attestation.unattested_probes = %d",
			r.HeldOut["UNATTESTED"], r.Attestation.UnattestedProbes)
	}
	var out int64
	for _, n := range r.HeldOut {
		out += n
	}
	if r.ServeRate.Den+out != r.Coverage.Den {
		t.Errorf("serve_rate.den (%d) + held out (%d) = %d, want coverage.den %d", r.ServeRate.Den, out, r.ServeRate.Den+out, r.Coverage.Den)
	}
	o := r.Obligations
	if sum := o.Broken + o.Served + o.EndUnobserved + o.HeldParamUnverified +
		o.UnobservedReachable + o.UnobservedUnreachable + o.UnobservedNotProbed + o.Pending; sum != o.Total {
		t.Errorf("the obligation buckets sum to %d, not total %d", sum, o.Total)
	}
	if o.Unobserved != o.UnobservedReachable+o.UnobservedUnreachable+o.UnobservedNotProbed {
		t.Errorf("unobserved (%d) absorbed something it should not have", o.Unobserved)
	}
}

// runCorrector drives the real path: ingest a range line, run the
// correction pass, sync the holds. Nothing here reaches into the store to
// mark a range corrected by hand — the first cut of this mechanism had a
// corrector that could never run, and the test that called
// MarkRangeCorrected itself passed anyway.
func runCorrector(t *testing.T, st *store.Store, u scan.ParamUncertainty) (corrections string, applied int) {
	t.Helper()
	dir := t.TempDir()
	raw, err := json.Marshal(u)
	if err != nil {
		t.Fatal(err)
	}
	line := filepath.Join(dir, "param_uncertainty.jsonl")
	if err := os.WriteFile(line, append(raw, '\n'), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := ingest.ParamUncertainty(st, line, time.Now()); err != nil {
		t.Fatalf("ingest range: %v", err)
	}
	if _, err := st.SyncParamHolds(context.Background()); err != nil {
		t.Fatal(err)
	}
	corrections = filepath.Join(dir, "corrections.jsonl")
	f, err := os.OpenFile(corrections, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	n, err := correct.New(st, f, 5*time.Minute).Run(context.Background(), time.Now().UTC())
	if err != nil {
		t.Fatalf("corrector: %v", err)
	}
	if _, err := st.SyncParamHolds(context.Background()); err != nil {
		t.Fatal(err)
	}
	return corrections, n
}

// The whole point, end to end and through the real path: a range verified
// by the scanner has its deadlines moved and its rows re-graded, and only
// then does the hold lift.
//
// This is the test the first cut did not have. `holds` was written from the
// record alone, so it went false the moment a range was verified;
// HoldingRanges selected holds = 1 and the corrector filtered that set to
// verified ranges, so the two sets never intersected and the corrector was
// dead code. The rows were released with the old deadline and the old
// FAULT still on them, and the API test did not notice because it marked
// the range corrected by hand.
func TestAVerifiedRangeIsCorrectedBeforeItsHoldLifts(t *testing.T) {
	// Two validators answer NOT_FOUND at the last two points, under the
	// deadline the scanner stamped. The params really shortened at height
	// 150, which puts both of those points past the true deadline.
	ok := []probe.Outcome{probe.OutcomeServedOK, probe.OutcomeServedOK, probe.OutcomeServedOK, probe.OutcomeServedOK}
	gone := []probe.Outcome{probe.OutcomeServedOK, probe.OutcomeServedOK, probe.OutcomeNotFound, probe.OutcomeNotFound}
	st, created, msu := heldFixture(t, map[string][]probe.Outcome{"v1": gone, "v2": gone, "v3": ok})

	before := networkHeld(t, st)
	if before.Faults != 4 || before.Obligations.Broken != 2 {
		t.Fatalf("without the range: faults=%d broken=%d, want 4/2", before.Faults, before.Obligations.Broken)
	}

	// The params history the scanner persisted, as the collector ingested
	// it: the long window from the start, the short one only from 181.
	seedParams(t, st, 100, created, msu.Sub(created))
	seedParams(t, st, 181, created, shortWindow)

	u := verifiedRange(created, msu)
	corrections, applied := runCorrector(t, st, u)
	if applied == 0 {
		t.Fatal("the corrector applied nothing; it never saw the verified range")
	}

	after := networkHeld(t, st)
	if after.RetentionUncertainty != nil {
		t.Fatalf("the hold is still up after a completed correction pass: %+v", after.RetentionUncertainty)
	}
	if after.Obligations.HeldParamUnverified != 0 {
		t.Fatalf("held = %d after the corrections landed", after.Obligations.HeldParamUnverified)
	}
	// The deadline moved, so the two NOT_FOUND points fall past it and are
	// expected rather than faults. That is the whole defect, closed.
	if after.Faults != 0 {
		t.Fatalf("faults = %d after the deadline was corrected, want 0", after.Faults)
	}
	if after.Obligations.Broken != 0 {
		t.Fatalf("broken = %d after the deadline was corrected, want 0", after.Obligations.Broken)
	}
	assertReconciles(t, after)

	// The record carries it: the corrections and the line that closes the
	// range, so a rebuild reaches the same holds.
	body, err := os.ReadFile(corrections)
	if err != nil {
		t.Fatal(err)
	}
	var kinds []string
	for _, l := range strings.Split(strings.TrimSpace(string(body)), "\n") {
		if l == "" {
			continue
		}
		var c store.Correction
		if err := json.Unmarshal([]byte(l), &c); err != nil {
			t.Fatalf("decode %q: %v", l, err)
		}
		kinds = append(kinds, c.Kind)
	}
	var complete int
	for _, k := range kinds {
		if k == store.CorrectionRangeComplete {
			complete++
		}
	}
	if complete != 1 {
		t.Fatalf("corrections.jsonl carries %d range_corrected line(s), want 1: %v", complete, kinds)
	}
	if kinds[len(kinds)-1] != store.CorrectionRangeComplete {
		t.Fatalf("the completion line is not last: %v", kinds)
	}
}

// The same range arriving open first and verified second — the transition
// the live scanner produces when its node could not answer at detection and
// an archive node closes the range later.
func TestAnOpenRangeThatLaterVerifiesIsCorrectedToo(t *testing.T) {
	gone := []probe.Outcome{probe.OutcomeServedOK, probe.OutcomeServedOK, probe.OutcomeNotFound, probe.OutcomeNotFound}
	st, created, msu := heldFixture(t, map[string][]probe.Outcome{"v1": gone, "v2": gone})

	openRange(t, st)
	held := networkHeld(t, st)
	if held.Obligations.HeldParamUnverified != 2 || held.Faults != 0 {
		t.Fatalf("while unresolvable: held=%d faults=%d, want 2/0", held.Obligations.HeldParamUnverified, held.Faults)
	}

	seedParams(t, st, 100, created, msu.Sub(created))
	seedParams(t, st, 181, created, shortWindow)
	if _, applied := runCorrector(t, st, verifiedRange(created, msu)); applied == 0 {
		t.Fatal("the corrector applied nothing after the range verified")
	}
	after := networkHeld(t, st)
	if after.RetentionUncertainty != nil || after.Obligations.HeldParamUnverified != 0 {
		t.Fatalf("still held after the range verified and corrected: %+v", after.RetentionUncertainty)
	}
	if after.Faults != 0 || after.Obligations.Broken != 0 {
		t.Fatalf("faults=%d broken=%d after the correction, want 0/0", after.Faults, after.Obligations.Broken)
	}
}

// A row the corrector cannot re-derive keeps its range open, so nothing it
// covers is released under a deadline nothing checked. The comment in the
// corrector said this; before this commit the code marked the range
// corrected anyway.
func TestARowThatCannotBeReDerivedKeepsTheRangeHeld(t *testing.T) {
	gone := []probe.Outcome{probe.OutcomeServedOK, probe.OutcomeServedOK, probe.OutcomeNotFound, probe.OutcomeNotFound}
	st, created, msu := heldFixture(t, map[string][]probe.Outcome{"v1": gone, "v2": gone})
	seedParams(t, st, 100, created, msu.Sub(created))
	seedParams(t, st, 181, created, shortWindow)

	// The retention pass strips raw_json after 30 days; a row without it
	// cannot be graded the way it was graded.
	if _, err := st.DB().Exec(`UPDATE probes SET raw_json = '' WHERE validator_address = 'v2'`); err != nil {
		t.Fatal(err)
	}
	runCorrector(t, st, verifiedRange(created, msu))

	after := networkHeld(t, st)
	if after.RetentionUncertainty == nil {
		t.Fatal("the range closed although a row could not be re-derived")
	}
	if after.Faults != 0 {
		t.Fatalf("faults = %d; a row this observer cannot re-derive must not publish one", after.Faults)
	}
}

// insertLate adds one measurement that arrives after the range was closed,
// carrying the deadline the scanner stamped — which is what the prober
// does, because it schedules from publications.jsonl and that file is
// append-only. `at` is a fraction of the ORIGINAL window, so a point past
// the corrected deadline but inside the old one is where the false fault
// would be.
func insertLate(t *testing.T, st *store.Store, created, staleMSU time.Time, addr string, frac float64, out probe.Outcome) {
	t.Helper()
	at := created.Add(time.Duration(float64(staleMSU.Sub(created)) * frac))
	cls, reason := probe.Classify(probe.Evidence{Assigned: true, Attested: true, Phase: probe.PhaseInWindow, Outcome: out})
	m := probe.Measurement{
		SchemaVersion: probe.AttestationSchemaVersion, Vantage: "test",
		PromiseHash: "held1", Commitment: "ccheld", MustServeUntil: staleMSU, ValidatorSetHeight: 149,
		ValidatorAddress: addr, ValidatorHost: addr + ":443",
		Assigned: true, Attested: true, AssignedRowCount: 2,
		ScheduleLabel: "late", ScheduledAt: at, StartedAt: at, FinishedAt: at,
		Phase: probe.PhaseInWindow, Outcome: out,
		Classification: cls, ClassificationReason: reason, TotalDurationMS: 10,
	}
	m.TCP.OK, m.TLS.OK, m.Identity.OK = true, true, true
	raw, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.InsertProbe(m, raw); err != nil {
		t.Fatal(err)
	}
}

// The case the range-keyed framing could not reach. The prober schedules
// from publications.jsonl, which is append-only and still carries the
// deadline the scanner stamped, so it keeps producing measurements against
// a withdrawn deadline for as long as the OLD window runs — hours after the
// range that corrected it was closed. "This range has been corrected" says
// nothing about those rows.
//
// Old window 90 minutes, verified window 55. A NOT_FOUND at 80 minutes is
// inside the old window and well past the corrected one. It must never be
// published as a fault: not while it waits for the corrector, and not
// after.
func TestAMeasurementArrivingAfterTheRangeClosedIsStillCorrected(t *testing.T) {
	ok := []probe.Outcome{probe.OutcomeServedOK, probe.OutcomeServedOK, probe.OutcomeServedOK, probe.OutcomeServedOK}
	st, created, staleMSU := heldFixture(t, map[string][]probe.Outcome{"v1": ok, "v2": ok})
	seedParams(t, st, 100, created, staleMSU.Sub(created))
	seedParams(t, st, 181, created, shortWindow)

	// The range is corrected and closed.
	if _, applied := runCorrector(t, st, verifiedRange(created, staleMSU)); applied == 0 {
		t.Fatal("the corrector applied nothing")
	}
	if got := networkHeld(t, st); got.RetentionUncertainty != nil {
		t.Fatalf("the range did not close: %+v", got.RetentionUncertainty)
	}

	// Two validators are probed again, after the close, against the old
	// deadline still on the append-only record.
	insertLate(t, st, created, staleMSU, "v1", 0.89, probe.OutcomeNotFound)
	insertLate(t, st, created, staleMSU, "v2", 0.89, probe.OutcomeNotFound)

	// Before the corrector sees them they are already withheld: the hold is
	// stamped in the same statement that writes the row, so there is no
	// moment at which they exist and read FAULT.
	between := networkHeld(t, st)
	if between.Faults != 0 {
		t.Fatalf("faults = %d between the insert and the sweep; a stale row must be born held", between.Faults)
	}

	// The collector's next pass re-grades them against the deadline the
	// publication now carries.
	dir := t.TempDir()
	path := filepath.Join(dir, "corrections.jsonl")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	n, err := correct.New(st, f, 5*time.Minute).Run(context.Background(), time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("the sweep corrected %d row(s), want 2", n)
	}
	if _, err := st.SyncParamHolds(context.Background()); err != nil {
		t.Fatal(err)
	}

	after := networkHeld(t, st)
	if after.Faults != 0 {
		t.Fatalf("faults = %d after the sweep: a measurement arriving against a withdrawn deadline was published as one", after.Faults)
	}
	if after.Obligations.Broken != 0 {
		t.Fatalf("broken = %d after the sweep", after.Obligations.Broken)
	}
	if after.RetentionUncertainty != nil {
		t.Fatalf("the late rows are still withheld after being re-graded: %+v", after.RetentionUncertainty)
	}
	assertReconciles(t, after)

	// The correction is on the record, both of them.
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	late := 0
	for _, l := range strings.Split(strings.TrimSpace(string(body)), "\n") {
		if l == "" {
			continue
		}
		var c store.Correction
		if err := json.Unmarshal([]byte(l), &c); err != nil {
			t.Fatal(err)
		}
		if c.Kind == store.CorrectionProbeVerdict && c.FromMustServeUntil.Equal(staleMSU) &&
			!c.ToMustServeUntil.Equal(staleMSU) && c.FromClassification == string(probe.ClassFault) {
			late++
		}
	}
	if late != 2 {
		t.Fatalf("corrections.jsonl carries %d correction(s) for the late rows, want 2", late)
	}
}

// A deadline that moves is itself a correction, even when the phase and the
// classification come out the same. probes.must_serve_until is what
// ObligationBuckets cuts the end segment at and what decides pending, so a
// row left with a withdrawn deadline puts a HEALTHY reading in the wrong
// bucket.
func TestADeadlineThatMovesIsCorrectedEvenWhenTheVerdictDoesNot(t *testing.T) {
	// Every reading HEALTHY and early, so no phase or class can change.
	ok := []probe.Outcome{probe.OutcomeServedOK, probe.OutcomeServedOK, probe.OutcomeServedOK, probe.OutcomeServedOK}
	st, created, staleMSU := heldFixture(t, map[string][]probe.Outcome{"v1": ok})
	seedParams(t, st, 100, created, staleMSU.Sub(created))
	seedParams(t, st, 181, created, shortWindow)

	var before string
	if err := st.DB().QueryRow(`SELECT must_serve_until FROM probes WHERE schedule_label = 'w1'`).Scan(&before); err != nil {
		t.Fatal(err)
	}
	runCorrector(t, st, verifiedRange(created, staleMSU))

	var stale int
	if err := st.DB().QueryRow(`SELECT COUNT(*) FROM probes prb WHERE ` + store.StaleDeadline).Scan(&stale); err != nil {
		t.Fatal(err)
	}
	if stale != 0 {
		t.Fatalf("%d row(s) kept a withdrawn deadline because their verdict did not change", stale)
	}
	var after string
	if err := st.DB().QueryRow(`SELECT must_serve_until FROM probes WHERE schedule_label = 'w1'`).Scan(&after); err != nil {
		t.Fatal(err)
	}
	if after == before {
		t.Fatal("the early HEALTHY row kept the deadline this observer withdrew")
	}
}

// A measurement arriving while the range is still OPEN carries the same
// deadline the publication carries — nothing has been corrected yet — so
// the deadline-disagreement arm says nothing about it. The publication is
// already withheld, and its rows must be too, from the moment they exist.
//
// This is the common case, not the exotic one: an open range is the normal
// state before verification. Keying the insert on disagreement alone left
// every such row readable as FAULT until the next SyncParamHolds, and for
// as long as the collector stayed down if it stopped in between.
func TestAMeasurementArrivingIntoAnOpenRangeIsHeldOnInsert(t *testing.T) {
	ok := []probe.Outcome{probe.OutcomeServedOK, probe.OutcomeServedOK, probe.OutcomeServedOK, probe.OutcomeServedOK}
	st, created, msu := heldFixture(t, map[string][]probe.Outcome{"v1": ok, "v2": ok})
	openRange(t, st) // unresolvable, so nothing is ever corrected

	// Two more probes land, carrying the very deadline the publication
	// carries. No sync runs afterwards.
	insertLate(t, st, created, msu, "v1", 0.95, probe.OutcomeNotFound)
	insertLate(t, st, created, msu, "v2", 0.95, probe.OutcomeNotFound)

	got := networkHeld(t, st)
	if got.Faults != 0 {
		t.Fatalf("faults = %d without a sync; a row of a withheld publication must be born withheld", got.Faults)
	}
	if got.Obligations.Broken != 0 {
		t.Fatalf("broken = %d", got.Obligations.Broken)
	}
	var unheld int
	if err := st.DB().QueryRow(`SELECT COUNT(*) FROM probes WHERE schedule_label = 'late' AND retention_unverified = 0`).Scan(&unheld); err != nil {
		t.Fatal(err)
	}
	if unheld != 0 {
		t.Fatalf("%d late row(s) went in unheld", unheld)
	}
	assertReconciles(t, got)
}

// A publication settling into a range recorded on an EARLIER pass arrives
// unheld — publications are ingested before the ranges, so nothing has
// raised its flag. It and its rows must still be withheld from the moment
// they exist, which is what the range-coverage arm of ProbeHeldAtInsert
// and the matching arm on the publication insert are for.
func TestAPublicationSettlingIntoARangeAlreadyOnRecordIsHeldOnInsert(t *testing.T) {
	ok := []probe.Outcome{probe.OutcomeServedOK, probe.OutcomeServedOK, probe.OutcomeServedOK, probe.OutcomeServedOK}
	st, created, msu := heldFixture(t, map[string][]probe.Outcome{"v1": ok})
	openRange(t, st)

	// A second publication, settled at height 160, lands now. Its promise
	// height is inside 121-180, so the range covers it.
	pub := scan.Publication{
		SchemaVersion: scan.AttestationSchemaVersion, PromiseHash: "held2",
		SettlementHeight: 160, SettlementTime: created, MustServeUntil: msu, RecordedAt: time.Now().UTC(),
		SettlementTxHash: "txheld2", Signer: "celestia1pub",
		Promise:                 scan.PromiseFields{ChainID: "t", Height: 160, Commitment: "ccheld2", CreationTimestamp: created, BlobSize: 4096},
		ValidatorSignatureCount: 1,
		Assignment: scan.AssignmentTable{
			ProtocolParams:     scan.ProtocolParamsSnapshot{OriginalRows: 4, TotalRows: 16},
			ValidatorSetHeight: 159, TotalVotingPower: 10, Sigma: 2, Distinct: 2,
			ValidatorsWithRows: 1, AttestedWithRows: 1, SignatureEntries: 1, SignaturesVerified: 1,
			AttestedVotingPower: 10,
			Validators:          []scan.ValidatorAssignment{{Address: "v1", VotingPower: 10, RowCount: 2, Rows: []int{0, 1}, Attested: true}},
		},
	}
	raw, err := json.Marshal(pub)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.UpsertPublication(pub, raw); err != nil {
		t.Fatal(err)
	}
	var pubHeld int
	if err := st.DB().QueryRow(`SELECT retention_unverified FROM publications WHERE promise_hash = 'held2'`).Scan(&pubHeld); err != nil {
		t.Fatal(err)
	}
	if pubHeld != 1 {
		t.Fatal("a publication settling into a range that still withholds went in unheld")
	}

	at := created.Add(time.Duration(float64(msu.Sub(created)) * 0.95))
	cls, reason := probe.Classify(probe.Evidence{Assigned: true, Attested: true, Phase: probe.PhaseInWindow, Outcome: probe.OutcomeNotFound})
	m := probe.Measurement{
		SchemaVersion: probe.AttestationSchemaVersion, Vantage: "test",
		PromiseHash: "held2", Commitment: "ccheld2", MustServeUntil: msu, ValidatorSetHeight: 159,
		ValidatorAddress: "v1", ValidatorHost: "v1:443",
		Assigned: true, Attested: true, AssignedRowCount: 2,
		ScheduleLabel: "late", ScheduledAt: at, StartedAt: at, FinishedAt: at,
		Phase: probe.PhaseInWindow, Outcome: probe.OutcomeNotFound,
		Classification: cls, ClassificationReason: reason, TotalDurationMS: 10,
	}
	m.TCP.OK, m.TLS.OK, m.Identity.OK = true, true, true
	raw, err = json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.InsertProbe(m, raw); err != nil {
		t.Fatal(err)
	}

	var rowHeld int
	if err := st.DB().QueryRow(`SELECT retention_unverified FROM probes WHERE promise_hash = 'held2'`).Scan(&rowHeld); err != nil {
		t.Fatal(err)
	}
	if rowHeld != 1 {
		t.Fatal("the row went in unheld although a range that still withholds covers its publication")
	}
	if got := networkHeld(t, st); got.Faults != 0 {
		t.Fatalf("faults = %d without a sync", got.Faults)
	}
}

// The disclosure follows the rows, not the ranges. A row withheld after
// every range has closed is still a row this observer is not speaking for,
// and RETENTION_UNVERIFIED sitting in serve_rate_held_out with no block
// beside it saying why is a held-out bucket with no explanation.
func TestTheDisclosureSurvivesTheRangeThatCausedIt(t *testing.T) {
	ok := []probe.Outcome{probe.OutcomeServedOK, probe.OutcomeServedOK, probe.OutcomeServedOK, probe.OutcomeServedOK}
	st, created, staleMSU := heldFixture(t, map[string][]probe.Outcome{"v1": ok})
	seedParams(t, st, 100, created, staleMSU.Sub(created))
	seedParams(t, st, 181, created, shortWindow)
	runCorrector(t, st, verifiedRange(created, staleMSU))
	if got := networkHeld(t, st); got.RetentionUncertainty != nil {
		t.Fatalf("the range did not close: %+v", got.RetentionUncertainty)
	}

	// A row arrives against the withdrawn deadline. No range is open.
	insertLate(t, st, created, staleMSU, "v1", 0.95, probe.OutcomeNotFound)

	got := networkHeld(t, st)
	if got.HeldOut["RETENTION_UNVERIFIED"] == 0 {
		t.Fatal("this test needs a withheld row to mean anything")
	}
	if got.RetentionUncertainty == nil {
		t.Fatal("the response withholds rows and says nothing about why")
	}
	if got.RetentionUncertainty.OpenRanges != 0 || got.RetentionUncertainty.ProbesHeld == 0 {
		t.Fatalf("retention_uncertainty = %+v, want no open range and a held probe", got.RetentionUncertainty)
	}
}

// Rows already in the store when a range lands are the case the insert-time
// hold cannot reach: they were written before anything knew to distrust
// their deadline. Recording the range must withhold them in the same
// transaction, not at the end of the collector's pass — the API reads the
// store concurrently, and a collector that stops in between leaves the old
// verdicts published for as long as it is down.
//
// The second half is the same scenario with the answer already cached. The
// window snapshots run to a fifteen-minute TTL, so a hold that commits while
// the snapshot holding the fault stays valid publishes the accusation it
// exists to withdraw.
func TestRecordingARangeWithholdsTheRowsAlreadyStored(t *testing.T) {
	gone := []probe.Outcome{probe.OutcomeServedOK, probe.OutcomeServedOK, probe.OutcomeNotFound, probe.OutcomeNotFound}
	st, _, _ := heldFixture(t, map[string][]probe.Outcome{"v1": gone, "v2": gone})

	// One server for the whole test, so the cache is the live one.
	ts := httptest.NewServer(api.New(st, "test"))
	t.Cleanup(ts.Close)
	read := func() heldJSON {
		t.Helper()
		var out heldJSON
		get(t, ts, "/v1/network?window=24h", &out)
		return out
	}

	// The faults are published, and now they are also cached.
	before := read()
	if before.Faults != 4 || before.Obligations.Broken != 2 {
		t.Fatalf("faults=%d broken=%d before the range, want 4/2", before.Faults, before.Obligations.Broken)
	}

	// The range lands. Nothing else runs: no hold sync, no corrector.
	u := scan.ParamUncertainty{
		SchemaVersion: scan.ParamUncertaintySchemaVersion,
		ID:            "t:silent_change:121-180", ChainID: "t", Kind: scan.UncertaintySilentChange,
		FromHeight: 121, ToHeight: 180, EffectiveFromHeight: 181, IntervalStartKnown: true,
		Direction: "shorter", PublicationsAffected: 1, DetectedAt: time.Now().UTC(),
		Resolution: scan.ResolutionUnresolvable, ResolveError: "the node cannot answer for those heights",
	}
	raw, err := json.Marshal(u)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.UpsertParamUncertainty(u, raw, time.Now()); err != nil {
		t.Fatal(err)
	}

	// Every row of the covered publication is withheld already.
	var unheld int
	if err := st.DB().QueryRow(`SELECT COUNT(*) FROM probes WHERE retention_unverified = 0`).Scan(&unheld); err != nil {
		t.Fatal(err)
	}
	if unheld != 0 {
		t.Fatalf("%d row(s) stored before the range are still unheld after it was recorded", unheld)
	}

	after := read()
	if after.Faults != 0 {
		t.Fatalf("faults = %d from the cached answer after the range was recorded; the hold did not invalidate it", after.Faults)
	}
	if after.Obligations.Broken != 0 {
		t.Fatalf("broken = %d", after.Obligations.Broken)
	}
	if after.Obligations.HeldParamUnverified != 2 {
		t.Fatalf("held_param_unverified = %d, want 2", after.Obligations.HeldParamUnverified)
	}
	if after.RetentionUncertainty == nil {
		t.Fatal("rows are withheld and the response says nothing about why")
	}
	assertReconciles(t, after)
}

// addPublication stores one publication and one NOT_FOUND probe per
// validator at the last in-window point, so each validator has a fault.
func addPublication(t *testing.T, st *store.Store, hash string, height int64, created, msu time.Time, addrs ...string) {
	t.Helper()
	var vals []scan.ValidatorAssignment
	for i, a := range addrs {
		vals = append(vals, scan.ValidatorAssignment{Address: a, VotingPower: 10, RowCount: 2, Rows: []int{2 * i, 2*i + 1}, Attested: true})
	}
	pub := scan.Publication{
		SchemaVersion: scan.AttestationSchemaVersion, PromiseHash: hash,
		SettlementHeight: height, SettlementTime: created, MustServeUntil: msu, RecordedAt: time.Now().UTC(),
		SettlementTxHash: "tx" + hash, Signer: "celestia1pub",
		Promise:                 scan.PromiseFields{ChainID: "t", Height: height, Commitment: "cc" + hash, CreationTimestamp: created, BlobSize: 4096},
		ValidatorSignatureCount: len(vals),
		Assignment: scan.AssignmentTable{
			ProtocolParams:     scan.ProtocolParamsSnapshot{OriginalRows: 4, TotalRows: 16},
			ValidatorSetHeight: height - 1, TotalVotingPower: int64(10 * len(vals)), Sigma: 2 * len(vals), Distinct: 2 * len(vals),
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
	at := inWindowPoint(created, msu, 3)
	for _, a := range addrs {
		cls, reason := probe.Classify(probe.Evidence{Assigned: true, Attested: true, Phase: probe.PhaseInWindow, Outcome: probe.OutcomeNotFound})
		m := probe.Measurement{
			SchemaVersion: probe.AttestationSchemaVersion, Vantage: "test",
			PromiseHash: hash, Commitment: "cc" + hash, MustServeUntil: msu, ValidatorSetHeight: height - 1,
			ValidatorAddress: a, ValidatorHost: a + ":443",
			Assigned: true, Attested: true, AssignedRowCount: 2,
			ScheduleLabel: "w4", ScheduledAt: at, StartedAt: at, FinishedAt: at,
			Phase: probe.PhaseInWindow, Outcome: probe.OutcomeNotFound,
			Classification: cls, ClassificationReason: reason, TotalDurationMS: 10,
		}
		m.TCP.OK, m.TLS.OK, m.Identity.OK = true, true, true
		raw, err := json.Marshal(m)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := st.InsertProbe(m, raw); err != nil {
			t.Fatal(err)
		}
	}
}

func rangeOver(id string, from, to int64) scan.ParamUncertainty {
	return scan.ParamUncertainty{
		SchemaVersion: scan.ParamUncertaintySchemaVersion,
		ID:            id, ChainID: "t", Kind: scan.UncertaintySilentChange,
		FromHeight: from, ToHeight: to, EffectiveFromHeight: to + 1, IntervalStartKnown: true,
		Direction: "shorter", PublicationsAffected: 1, DetectedAt: time.Now().UTC(),
		Resolution: scan.ResolutionUnresolvable, ResolveError: "the node cannot answer for those heights",
	}
}

// Two ranges landing in the same collector pass must not write the same
// cache revision. The pass stamps one time.Now() at its top and threads it
// through every record it ingests, so a revision built from that clock was
// identical for both — and a snapshot computed between them carried that
// value, still matched it afterwards, and went on publishing the faults the
// second range had just withheld.
//
// Nothing here clears a cache or runs a hold sync. The only thing that may
// invalidate the cached answer is the revision moving.
func TestTwoRangesInOnePassEachInvalidateTheCachedAnswer(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "observer.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	base := time.Now().UTC().Truncate(time.Second)

	// Two validators each, and the two publications on different windows so
	// their schedule points do not coincide. Two faults at one point is
	// under the correlated-failure guard's floor of three, so the guard
	// never fires here and the faults are the plain published kind — which
	// is what this test needs to watch disappear.
	addPublication(t, st, "pubA", 150, base.Add(-2*time.Hour), base.Add(-30*time.Minute), "a1", "a2")
	addPublication(t, st, "pubB", 400, base.Add(-3*time.Hour), base.Add(-45*time.Minute), "b1", "b2")

	ts := httptest.NewServer(api.New(st, "test"))
	t.Cleanup(ts.Close)
	read := func() heldJSON {
		t.Helper()
		var out heldJSON
		get(t, ts, "/v1/network?window=24h", &out)
		return out
	}
	if got := read().Faults; got != 4 {
		t.Fatalf("faults = %d before any range, want 4", got)
	}

	// The one clock the collector would use for the whole pass.
	now := time.Now()

	// Range A lands, covering pubA only.
	rawA, err := json.Marshal(rangeOver("t:silent_change:121-180", 121, 180))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.UpsertParamUncertainty(rangeOver("t:silent_change:121-180", 121, 180), rawA, now); err != nil {
		t.Fatal(err)
	}

	// The cache is filled here, between the two records. pubB's faults are
	// real at this moment and correctly published.
	mid := read()
	if mid.Faults != 2 {
		t.Fatalf("faults = %d after range A, want pubB's 2", mid.Faults)
	}

	// Range B lands in the same pass, under the same clock, covering pubB.
	rawB, err := json.Marshal(rangeOver("t:silent_change:381-420", 381, 420))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.UpsertParamUncertainty(rangeOver("t:silent_change:381-420", 381, 420), rawB, now); err != nil {
		t.Fatal(err)
	}

	after := read()
	if after.Faults != 0 {
		t.Fatalf("faults = %d from the cached answer after range B; the two ranges wrote the same revision", after.Faults)
	}
	if after.Obligations.Broken != 0 {
		t.Fatalf("broken = %d", after.Obligations.Broken)
	}
	if after.Obligations.HeldParamUnverified != 4 {
		t.Fatalf("held_param_unverified = %d, want 4", after.Obligations.HeldParamUnverified)
	}
	assertReconciles(t, after)
}

// The per-validator figures come from the same held-aware class as the
// network's. They used the stored class, so with a hold on record the
// network withheld a FAULT that the validator's own row still counted: a
// validator accused on its row for what the observer had just said it could
// not judge. Summed over validators they now equal the network's figures,
// and /v1/probes filters on the class it publishes.
func TestPerValidatorFiguresHonourTheHoldAndAddUpToTheNetwork(t *testing.T) {
	ok := []probe.Outcome{probe.OutcomeServedOK, probe.OutcomeServedOK, probe.OutcomeServedOK, probe.OutcomeServedOK}
	gone := []probe.Outcome{probe.OutcomeServedOK, probe.OutcomeServedOK, probe.OutcomeNotFound, probe.OutcomeNotFound}
	st, _, _ := heldFixture(t, map[string][]probe.Outcome{
		"v1": gone, "v2": gone, "v3": ok, "v4": ok, "v5": ok, "v6": ok, "v7": ok, "v8": ok,
	})
	openRange(t, st)
	ts := httptest.NewServer(api.New(st, "test"))
	defer ts.Close()

	var network heldJSON
	get(t, ts, "/v1/network?window=24h", &network)
	var vals struct {
		Validators []struct {
			Address   string           `json:"address"`
			Faults    int64            `json:"faults"`
			Classes   map[string]int64 `json:"classes"`
			ServeRate struct {
				Den int64 `json:"den"`
			} `json:"serve_rate"`
			ByPoint []struct {
				Key  string `json:"key"`
				Rate struct {
					Num int64 `json:"num"`
					Den int64 `json:"den"`
				} `json:"serve_rate"`
			} `json:"serve_rate_by_point"`
		} `json:"validators"`
	}
	if code := get(t, ts, "/v1/validators?window=24h", &vals); code != 200 || len(vals.Validators) != 8 {
		t.Fatalf("validators: %d, %d rows", code, len(vals.Validators))
	}
	var faults, held, faultClass int64
	for _, v := range vals.Validators {
		faults += v.Faults
		held += v.Classes["RETENTION_UNVERIFIED"]
		faultClass += v.Classes["FAULT"]
		if v.ServeRate.Den != 0 {
			t.Errorf("%s: a rate over held rows (den %d)", v.Address, v.ServeRate.Den)
		}
		for _, p := range v.ByPoint {
			if p.Rate.Den != 0 {
				t.Errorf("%s at %s: a per-point rate over held rows (%d/%d)", v.Address, p.Key, p.Rate.Num, p.Rate.Den)
			}
		}
	}
	if network.Faults != 0 || faults != network.Faults || faultClass != 0 {
		t.Fatalf("faults: network %d, per-validator sum %d, FAULT class %d; want all 0 under the hold", network.Faults, faults, faultClass)
	}
	if held != network.HeldOut["RETENTION_UNVERIFIED"] || held != 32 {
		t.Fatalf("held rows: per-validator sum %d, network %d, want 32", held, network.HeldOut["RETENTION_UNVERIFIED"])
	}

	var probes struct {
		Probes []struct {
			Classification string `json:"classification"`
		} `json:"probes"`
	}
	if code := get(t, ts, "/v1/probes?class=fault&limit=100", &probes); code != 200 || len(probes.Probes) != 0 {
		t.Fatalf("?class=fault under the hold: %d, %d rows", code, len(probes.Probes))
	}
	probes.Probes = nil
	if code := get(t, ts, "/v1/probes?class=retention_unverified&limit=100", &probes); code != 200 || len(probes.Probes) != 32 {
		t.Fatalf("?class=retention_unverified: %d, %d rows", code, len(probes.Probes))
	}
	for _, p := range probes.Probes {
		if p.Classification != "RETENTION_UNVERIFIED" {
			t.Fatalf("filtered row published as %s", p.Classification)
		}
	}
}

// The validator page is served from a short cache that a hold still clears
// at once, and an address nothing is on record for is refused before any
// aggregate is built.
func TestTheValidatorPageIsCachedUntilAHoldLandsAndRefusesUnknownAddresses(t *testing.T) {
	a := strings.Repeat("a1", 20)
	ok := []probe.Outcome{probe.OutcomeServedOK, probe.OutcomeServedOK, probe.OutcomeServedOK, probe.OutcomeServedOK}
	gone := []probe.Outcome{probe.OutcomeServedOK, probe.OutcomeServedOK, probe.OutcomeNotFound, probe.OutcomeNotFound}
	outcomes := map[string][]probe.Outcome{a: gone}
	for i := 2; i <= 8; i++ {
		outcomes[strings.Repeat(fmt.Sprintf("b%d", i), 20)] = ok
	}
	st, _, _ := heldFixture(t, outcomes)
	srv := api.New(st, "test")
	ts := httptest.NewServer(srv)
	defer func() { ts.Close(); srv.Close() }()

	if code := get(t, ts, "/v1/validators/"+strings.Repeat("cd", 20), nil); code != 404 {
		t.Fatalf("an address with nothing on record: %d, want 404", code)
	}

	type detail struct {
		Validator struct {
			Faults int64 `json:"faults"`
		} `json:"validator"`
		Recent []any `json:"recent_probes"`
	}
	var d detail
	if code := get(t, ts, "/v1/validators/"+a+"?window=24h", &d); code != 200 || d.Validator.Faults != 2 || len(d.Recent) != 4 {
		t.Fatalf("before: %d %+v", code, d)
	}
	// Within the TTL the answer is the cached one: the rows behind it are
	// gone and it still reads as it did.
	if _, err := st.DB().Exec(`DELETE FROM probes WHERE validator_address = ?`, a); err != nil {
		t.Fatal(err)
	}
	d = detail{}
	if code := get(t, ts, "/v1/validators/"+a+"?window=24h", &d); code != 200 || len(d.Recent) != 4 {
		t.Fatalf("cached: %d %+v", code, d)
	}
	// A hold clears it at once.
	openRange(t, st)
	d = detail{}
	if code := get(t, ts, "/v1/validators/"+a+"?window=24h", &d); code != 200 || d.Validator.Faults != 0 || len(d.Recent) != 0 {
		t.Fatalf("after the hold: %d %+v", code, d)
	}

	// Many readers of one page at once get one answer.
	b := strings.Repeat("b2", 20)
	bodies := make([]string, 8)
	var wg sync.WaitGroup
	for i := range bodies {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			resp, err := http.Get(ts.URL + "/v1/validators/" + b + "?window=7d")
			if err != nil {
				return
			}
			defer resp.Body.Close()
			raw, _ := io.ReadAll(resp.Body)
			if resp.StatusCode == 200 {
				bodies[i] = string(raw)
			}
		}(i)
	}
	wg.Wait()
	for i := range bodies {
		if bodies[i] == "" || bodies[i] != bodies[0] {
			t.Fatalf("reader %d of 8 got a different or failed answer", i)
		}
	}
}
