package correct_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/probe"
	"github.com/plsgiveup/fibre/fibre-sentinel/internal/scan"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/correct"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
)

const (
	window      = 90 * time.Minute
	shortWindow = 55 * time.Minute
)

// fixture: one publication settled at height 150 inside the range 121-180,
// with `n` validators each probed at the four default in-window fractions,
// the last two answering NOT_FOUND.
func fixture(t *testing.T, n int) (*store.Store, time.Time) {
	t.Helper()
	return fixtureAt(t, n, time.Now().UTC())
}

// fixtureAt is fixture with the clock given, so a test that builds two stores
// to compare them builds both on the same second: each reading the clock on
// its own put them a second apart whenever the two calls straddled a second
// boundary, and every timestamp in the comparison with them.
func fixtureAt(t *testing.T, n int, clock time.Time) (*store.Store, time.Time) {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "observer.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })

	now := clock.UTC().Truncate(time.Second)
	created := now.Add(-2 * time.Hour)
	msu := created.Add(window)

	var vals []scan.ValidatorAssignment
	for i := 0; i < n; i++ {
		vals = append(vals, scan.ValidatorAssignment{Address: addr(i), VotingPower: 10, RowCount: 2, Rows: []int{2 * i, 2*i + 1}, Attested: true})
	}
	pub := scan.Publication{
		SchemaVersion: scan.AttestationSchemaVersion, PromiseHash: "p1",
		SettlementHeight: 150, SettlementTime: created, MustServeUntil: msu, RecordedAt: now,
		SettlementTxHash: "tx1", Signer: "celestia1pub",
		Promise:                 scan.PromiseFields{ChainID: "t", Height: 150, Commitment: "cc", CreationTimestamp: created, BlobSize: 4096},
		ValidatorSignatureCount: n,
		Assignment: scan.AssignmentTable{
			ProtocolParams:     scan.ProtocolParamsSnapshot{OriginalRows: 4, TotalRows: 16},
			ValidatorSetHeight: 149, TotalVotingPower: int64(10 * n), Sigma: 2 * n, Distinct: 2 * n,
			ValidatorsWithRows: n, AttestedWithRows: n, SignatureEntries: n, SignaturesVerified: n,
			AttestedVotingPower: int64(10 * n), Validators: vals,
		},
	}
	raw, err := json.Marshal(pub)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.UpsertPublication(pub, raw); err != nil {
		t.Fatal(err)
	}
	labels := []string{"w1", "w2", "w3", "w4"}
	for i := 0; i < n; i++ {
		for j, f := range []float64{0.12, 0.45, 0.72, 0.92} { // the earlier schedule's points
			at := created.Add(time.Duration(float64(window) * f))
			out := probe.OutcomeServedOK
			if j >= 2 {
				out = probe.OutcomeNotFound
			}
			cls, reason := probe.Classify(probe.Evidence{Assigned: true, Attested: true, Phase: probe.PhaseInWindow, Outcome: out})
			m := probe.Measurement{
				SchemaVersion: probe.AttestationSchemaVersion, Vantage: "test",
				PromiseHash: "p1", Commitment: "cc", MustServeUntil: msu, ValidatorSetHeight: 149,
				ValidatorAddress: addr(i), ValidatorHost: addr(i) + ":443",
				Assigned: true, Attested: true, AssignedRowCount: 2,
				ScheduleLabel: labels[j], ScheduledAt: at, StartedAt: at, FinishedAt: at,
				Phase: probe.PhaseInWindow, Outcome: out,
				Classification: cls, ClassificationReason: reason, TotalDurationMS: 10,
			}
			m.TCP.OK, m.TLS.OK, m.Identity.OK = true, true, true
			if out == probe.OutcomeServedOK {
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
	// The params history as the collector ingests it from state.json.
	for _, e := range []struct {
		h int64
		w time.Duration
	}{{100, window}, {181, shortWindow}} {
		p := scan.ParamsSnapshot{
			WithdrawalDelaySeconds: int64(13 * time.Hour / time.Second), PaymentPromiseTimeoutSeconds: 600,
			ShardRetentionSeconds: int64(e.w / time.Second), PaymentPromiseHeightWindow: 1000,
			EffectiveFromHeight: e.h, EffectiveFromTxIndex: -1, Source: "seed",
		}
		if err := st.UpsertParams([]scan.ParamEntry{{FromHeight: e.h, FromTxIndex: -1, Source: "seed", ParamsJSON: p}}); err != nil {
			t.Fatal(err)
		}
	}
	return st, created
}

func addr(i int) string { return string(rune('a' + i)) }

func verifiedRange(created time.Time) scan.ParamUncertainty {
	long := scan.ParamsSnapshot{
		WithdrawalDelaySeconds: int64(13 * time.Hour / time.Second), PaymentPromiseTimeoutSeconds: 600,
		ShardRetentionSeconds: int64(window / time.Second), PaymentPromiseHeightWindow: 1000,
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

func record(t *testing.T, st *store.Store, u scan.ParamUncertainty) {
	t.Helper()
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
}

func faults(t *testing.T, st *store.Store) int {
	t.Helper()
	var n int
	if err := st.DB().QueryRow(`SELECT COUNT(*) FROM probes WHERE classification = 'FAULT'`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func stillHeld(t *testing.T, st *store.Store) bool {
	t.Helper()
	_, _, ranges, err := st.HeldCounts(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return ranges > 0
}

// A publication's deadline is corrected before its rows are re-graded, so
// a crash between the two leaves the deadline moved and the rows carrying
// verdicts drawn against the old one. The next pass must pick those rows
// up.
//
// Before this commit it could not: PublicationsCoveredBy skipped any
// publication that already carried a deadline correction for the range, so
// one interrupted pass made the half-corrected state permanent — and the
// range was marked complete anyway, releasing the rows with the old FAULT
// still on them.
func TestAPassInterruptedAfterTheDeadlineFinishesTheRowsOnTheNextRun(t *testing.T) {
	st, created := fixture(t, 2)
	record(t, st, verifiedRange(created))

	dir := t.TempDir()
	// A file opened read-only makes every correction line fail to append,
	// which is where the corrector gives up — after the deadline
	// correction of the first publication has been applied.
	blocked, err := os.OpenFile(filepath.Join(dir, "blocked.jsonl"), os.O_CREATE|os.O_RDONLY, 0o444)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := correct.New(st, blocked, 5*time.Minute).Run(context.Background(), time.Now().UTC()); err == nil {
		t.Fatal("the corrector reported success although it could not write its record")
	}
	blocked.Close()
	if !stillHeld(t, st) {
		t.Fatal("the range closed although the pass failed")
	}
	if faults(t, st) != 4 {
		t.Fatalf("faults = %d before a successful pass, want 4", faults(t, st))
	}

	// The pass runs again with a writable record. Every row must be
	// re-graded, including those of the publication the failed pass had
	// already touched.
	f, err := os.OpenFile(filepath.Join(dir, "corrections.jsonl"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := correct.New(st, f, 5*time.Minute).Run(context.Background(), time.Now().UTC()); err != nil {
		t.Fatalf("second pass: %v", err)
	}
	if _, err := st.SyncParamHolds(context.Background()); err != nil {
		t.Fatal(err)
	}
	if n := faults(t, st); n != 0 {
		t.Fatalf("faults = %d after the second pass; the rows of an already-corrected publication were skipped", n)
	}
	if stillHeld(t, st) {
		t.Fatal("the range is still holding after every target was corrected")
	}
}

// Running the corrector twice over a range it already closed changes
// nothing and writes nothing new. The clamp is drawn against what the
// scanner stamped, not against the current deadline, so a second pass
// cannot compare a corrected value with itself and conclude there is
// nothing to do before it reaches the rows.
func TestASecondPassIsANoOp(t *testing.T) {
	st, created := fixture(t, 2)
	record(t, st, verifiedRange(created))
	dir := t.TempDir()
	path := filepath.Join(dir, "corrections.jsonl")
	run := func() int {
		f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		n, err := correct.New(st, f, 5*time.Minute).Run(context.Background(), time.Now().UTC())
		if err != nil {
			t.Fatal(err)
		}
		if _, err := st.SyncParamHolds(context.Background()); err != nil {
			t.Fatal(err)
		}
		return n
	}
	first := run()
	if first == 0 {
		t.Fatal("the first pass applied nothing")
	}
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if n := run(); n != 0 {
		t.Fatalf("the second pass applied %d correction(s), want 0", n)
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(after) != len(before) {
		t.Fatalf("the second pass appended %d bytes to the record", len(after)-len(before))
	}
	if faults(t, st) != 0 {
		t.Fatal("a fault came back on the second pass")
	}
}

// An unresolvable range has nothing to correct against, so the corrector
// leaves it alone and its hold stands.
func TestAnUnresolvableRangeIsNotCorrected(t *testing.T) {
	st, created := fixture(t, 2)
	u := verifiedRange(created)
	u.Resolution = scan.ResolutionUnresolvable
	u.Values = nil
	record(t, st, u)

	f, err := os.OpenFile(filepath.Join(t.TempDir(), "corrections.jsonl"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	n, err := correct.New(st, f, 5*time.Minute).Run(context.Background(), time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("the corrector applied %d correction(s) against a range it never read", n)
	}
	if !stillHeld(t, st) {
		t.Fatal("an unresolvable range stopped holding")
	}
}

// TestEveryMoveOfTheHoldFlagsIsCounted: the API's day partials follow the
// holds by store.MetaHeldFlagsRev and read the flags again only when it
// moves, so every transaction that moves a flag of a stored row moves it
// too, and one that moves none leaves it: a range's own raise, the hold
// sync that lifts it once the range is corrected, and a sync with nothing
// to do.
func TestEveryMoveOfTheHoldFlagsIsCounted(t *testing.T) {
	st, created := fixture(t, 2)
	ctx := context.Background()
	state := func() (string, int) {
		t.Helper()
		rev, err := st.Meta(store.MetaHeldFlagsRev)
		if err != nil {
			t.Fatal(err)
		}
		var held int
		if err := st.DB().QueryRow(`SELECT (SELECT COUNT(*) FROM probes WHERE retention_unverified = 1)
			+ (SELECT COUNT(*) FROM publications WHERE retention_unverified = 1)`).Scan(&held); err != nil {
			t.Fatal(err)
		}
		return rev, held
	}
	if rev, held := state(); rev != "" || held != 0 {
		t.Fatalf("before any range: counter %q, %d held", rev, held)
	}
	// Raised open: the range's own transaction holds the publication and
	// its rows, and counts it.
	open := verifiedRange(created)
	open.Resolution, open.ResolveMethod, open.HeightsRead, open.ResolvedAt, open.Values = "", "", 0, nil, nil
	raw, err := json.Marshal(open)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.UpsertParamUncertainty(open, raw, time.Now()); err != nil {
		t.Fatal(err)
	}
	rev1, held := state()
	if rev1 != "1" || held == 0 {
		t.Fatalf("after the raise: counter %q, %d held", rev1, held)
	}
	// A sync with nothing to move counts nothing.
	if n, err := st.SyncParamHolds(ctx); err != nil || n != 0 {
		t.Fatalf("a sync over holds already in place moved %d flag(s): %v", n, err)
	}
	if rev, _ := state(); rev != rev1 {
		t.Fatalf("a sync that moved nothing moved the counter from %q to %q", rev1, rev)
	}
	// Verified, corrected and lifted: the sync that releases the rows counts.
	record(t, st, verifiedRange(created))
	f, err := os.OpenFile(filepath.Join(t.TempDir(), "corrections.jsonl"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := correct.New(st, f, 5*time.Minute).Run(ctx, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if n, err := st.SyncParamHolds(ctx); err != nil || n == 0 {
		t.Fatalf("the sync after the correction moved %d flag(s): %v", n, err)
	}
	rev2, held := state()
	if rev2 == rev1 || held != 0 {
		t.Fatalf("after the lift: counter %q (was %q), %d held", rev2, rev1, held)
	}
}
