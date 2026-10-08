package scan

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	fibretypes "github.com/celestiaorg/celestia-app/v10/x/fibre/types"
)

// A reconcile whose state read failed has narrowed nothing: the interval a
// silent param change could have landed in still starts where it did. If the
// marker moved anyway, the next successful reconcile would report an interval
// beginning after the failed check and the heights in between would drop out
// of the only record that says which publications carry a deadline computed
// from the old params.
func TestAFailedReconcileDoesNotNarrowTheUncertaintyInterval(t *testing.T) {
	p := params(10*time.Minute, 30*time.Minute, 13*time.Hour)
	s := &Scanner{log: NewLogger(10), startHeight: 100, params: NewParamHistory(100, p)}

	s.reconcileParamsWith(120, func() (fibretypes.Params, error) { return p, nil }, nil)
	if s.lastReconcile != 120 {
		t.Fatalf("a reconcile that read state should move the marker: got %d", s.lastReconcile)
	}

	for _, h := range []int64{180, 240, 300} {
		s.reconcileParamsWith(h, func() (fibretypes.Params, error) {
			return fibretypes.Params{}, errors.New("rpc: connection refused")
		}, nil)
		if s.lastReconcile != 120 {
			t.Fatalf("h=%d: a reconcile that could not read state moved the marker to %d", h, s.lastReconcile)
		}
	}

	// The outage ends. The interval the next successful reconcile would
	// report is (120, 360] — every height the failed checks did not cover.
	s.reconcileParamsWith(360, func() (fibretypes.Params, error) { return p, nil }, nil)
	if s.lastReconcile != 360 {
		t.Fatalf("marker after recovery: got %d, want 360", s.lastReconcile)
	}
}

// With no reconcile on record the interval is the whole scan, and a failed
// first check must not turn that into a narrow one.
func TestAFailedFirstReconcileLeavesTheIntervalAtTheWholeScan(t *testing.T) {
	p := params(10*time.Minute, 30*time.Minute, 13*time.Hour)
	s := &Scanner{log: NewLogger(10), startHeight: 100, params: NewParamHistory(100, p)}

	s.reconcileParamsWith(120, func() (fibretypes.Params, error) {
		return fibretypes.Params{}, errors.New("rpc: connection refused")
	}, nil)
	if s.lastReconcile != 0 {
		t.Fatalf("marker after a failed first reconcile: got %d, want 0 (unknown)", s.lastReconcile)
	}
}

// storeFor gives a Scanner a real store so emitUncertainty has somewhere to
// write, and returns the path of the record file.
func storeFor(t *testing.T, s *Scanner) (*Store, string) {
	t.Helper()
	dir := t.TempDir()
	st, err := OpenStore(dir)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	s.store = st
	return st, filepath.Join(dir, "param_uncertainty.jsonl")
}

func readUncertainty(t *testing.T, path string) []ParamUncertainty {
	t.Helper()
	b, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	var out []ParamUncertainty
	for _, line := range strings.Split(strings.TrimSpace(string(b)), "\n") {
		if line == "" {
			continue
		}
		var u ParamUncertainty
		if err := json.Unmarshal([]byte(line), &u); err != nil {
			t.Fatalf("decode %q: %v", line, err)
		}
		out = append(out, u)
	}
	return out
}

// A silent shortening must leave behind a record a machine can act on, not
// only a log line. Before this the whole fact lived in a WARNING that
// nothing read: not in the export, not in the database, not in the API.
func TestASilentShorteningWritesARecordAndClosesItByReadingEveryHeight(t *testing.T) {
	old := params(10*time.Minute, 4*time.Hour, 13*time.Hour)
	shorter := params(10*time.Minute, 1*time.Hour, 13*time.Hour)
	s := &Scanner{log: NewLogger(10), chainID: "mocha-5", startHeight: 100, params: NewParamHistory(100, old)}
	_, path := storeFor(t, s)
	s.store.SetSettledCoverFrom(100)

	s.reconcileParamsWith(120, func() (fibretypes.Params, error) { return old, nil },
		func(int64) (fibretypes.Params, error) { return old, nil })
	if got := readUncertainty(t, path); len(got) != 0 {
		t.Fatalf("a reconcile that found nothing wrote %d record(s)", len(got))
	}

	// The change is in force from height 150; the reconcile at 180 is the
	// first read that can see it.
	readAt := func(at int64) (fibretypes.Params, error) {
		if at >= 150 {
			return shorter, nil
		}
		return old, nil
	}
	s.reconcileParamsWith(180, func() (fibretypes.Params, error) { return shorter, nil }, readAt)

	got := readUncertainty(t, path)
	if len(got) != 1 {
		t.Fatalf("want one record, got %d", len(got))
	}
	u := got[0]
	if u.Kind != UncertaintySilentChange || u.FromHeight != 121 || u.ToHeight != 180 {
		t.Fatalf("range = %s %d-%d, want silent_change 121-180", u.Kind, u.FromHeight, u.ToHeight)
	}
	if u.ID != "mocha-5:silent_change:121-180" {
		t.Fatalf("id = %q", u.ID)
	}
	if u.Direction != "shorter" || u.EffectiveFromHeight != 181 || !u.IntervalStartKnown {
		t.Fatalf("direction=%q effective_from=%d start_known=%v", u.Direction, u.EffectiveFromHeight, u.IntervalStartKnown)
	}
	if u.WindowBeforeS != int64(4*time.Hour/time.Second) || u.WindowAfterS != int64(time.Hour/time.Second) {
		t.Fatalf("windows = %d -> %d", u.WindowBeforeS, u.WindowAfterS)
	}
	if u.Resolution != ResolutionVerified {
		t.Fatalf("resolution = %q (%s)", u.Resolution, u.ResolveError)
	}
	// Every height from FromHeight-1 through ToHeight: 120..180.
	if u.HeightsRead != 61 {
		t.Fatalf("heights_read = %d, want 61", u.HeightsRead)
	}
	if len(u.Values) != 2 || u.Values[0].FromHeight != 120 || u.Values[1].FromHeight != 150 {
		t.Fatalf("values = %+v, want the old value from 120 and the new one from 150", u.Values)
	}
	// Verified, but still withholding: reading every height says what the
	// deadline should have been, the corrections are what move it, and
	// until they land the store still carries the old deadline and the
	// rows the verdicts drawn from it.
	if !u.Holds() {
		t.Fatal("a silent change withholds until its corrections land, verified or not")
	}
	// The proven value is in the history at the height it was really in
	// force from, not at the h+1 the single read could vouch for.
	if e := s.params.at(150, 0); e == nil || e.Source != "verified" || e.Params.ShardRetention != time.Hour {
		t.Fatalf("history at 150 = %+v, want the verified shorter value", e)
	}
}

// A range the node cannot answer for is recorded unresolvable and keeps its
// hold. A partial read is not a partial proof: the heights it skipped are
// exactly where a third value could hide, so nothing read is kept.
func TestARangeTheNodeCannotAnswerForStaysHeld(t *testing.T) {
	old := params(10*time.Minute, 4*time.Hour, 13*time.Hour)
	shorter := params(10*time.Minute, time.Hour, 13*time.Hour)
	s := &Scanner{log: NewLogger(10), chainID: "mocha-5", startHeight: 100, params: NewParamHistory(100, old), lastReconcile: 120}
	_, path := storeFor(t, s)
	s.store.SetSettledCoverFrom(100)

	s.reconcileParamsWith(180, func() (fibretypes.Params, error) { return shorter, nil },
		func(at int64) (fibretypes.Params, error) {
			if at > 140 {
				return fibretypes.Params{}, errors.New("height 141 is not available, lowest height is 160")
			}
			return old, nil
		})

	got := readUncertainty(t, path)
	if len(got) != 1 {
		t.Fatalf("want one record, got %d", len(got))
	}
	u := got[0]
	if u.Resolution != ResolutionUnresolvable {
		t.Fatalf("resolution = %q, want unresolvable", u.Resolution)
	}
	if !u.Holds() {
		t.Fatal("an unresolvable silent change must still hold its verdicts")
	}
	if len(u.Values) != 0 {
		t.Fatalf("a partial read kept %d value(s); it proves nothing about the heights it skipped", len(u.Values))
	}
	if u.HeightsRead != 21 || !strings.Contains(u.ResolveError, "height 141") {
		t.Fatalf("heights_read=%d err=%q", u.HeightsRead, u.ResolveError)
	}
}

// A run of failed checks is one blind stretch, not one per check. Writing a
// record every sixty blocks through an outage would bury the one that
// matters under hundreds that say the same thing.
func TestAFailedReconcileRecordsTheBlindStretchOncePerRun(t *testing.T) {
	p := params(10*time.Minute, 4*time.Hour, 13*time.Hour)
	s := &Scanner{log: NewLogger(10), chainID: "mocha-5", startHeight: 100, params: NewParamHistory(100, p), lastReconcile: 120}
	_, path := storeFor(t, s)
	s.store.SetSettledCoverFrom(100)

	fail := func() (fibretypes.Params, error) { return fibretypes.Params{}, errors.New("rpc: connection refused") }
	for _, h := range []int64{180, 240, 300} {
		s.reconcileParamsWith(h, fail, nil)
	}
	got := readUncertainty(t, path)
	if len(got) != 1 {
		t.Fatalf("three failed checks in one run wrote %d record(s), want 1", len(got))
	}
	if got[0].Kind != UncertaintyCheckSkipped || got[0].FromHeight != 121 || got[0].ToHeight != 180 {
		t.Fatalf("record = %s %d-%d", got[0].Kind, got[0].FromHeight, got[0].ToHeight)
	}
	if got[0].Holds() {
		t.Fatal("a check that did not happen is not evidence that anything changed; it must not hold")
	}
	// The run ends; a later one is its own record.
	s.reconcileParamsWith(360, func() (fibretypes.Params, error) { return p, nil }, nil)
	if s.reconcileFailingSince != 0 {
		t.Fatalf("reconcileFailingSince = %d after a check that read state", s.reconcileFailingSince)
	}
	s.reconcileParamsWith(420, fail, nil)
	if got := readUncertainty(t, path); len(got) != 2 {
		t.Fatalf("a second run wrote %d record(s) in total, want 2", len(got))
	}
}

// A range wider than one pass will read is recorded unresolvable without
// reading anything, rather than stalling the block loop for hours.
func TestARangeTooWideToReadIsRecordedUnresolvable(t *testing.T) {
	old := params(10*time.Minute, 4*time.Hour, 13*time.Hour)
	shorter := params(10*time.Minute, time.Hour, 13*time.Hour)
	s := &Scanner{log: NewLogger(10), chainID: "mocha-5", startHeight: 1, params: NewParamHistory(1, old)}
	_, path := storeFor(t, s)
	reads := 0
	s.reconcileParamsWith(maxVerifyHeights+500, func() (fibretypes.Params, error) { return shorter, nil },
		func(int64) (fibretypes.Params, error) { reads++; return old, nil })
	if reads != 0 {
		t.Fatalf("read %d height(s) for a range it had already decided not to read", reads)
	}
	got := readUncertainty(t, path)
	if len(got) != 1 || got[0].Resolution != ResolutionUnresolvable || !got[0].Holds() {
		t.Fatalf("record = %+v", got)
	}
}

// The record is the only thing that makes a range knowable downstream, so
// nothing may move past it until it is on disk. Before this, a failed
// append was logged and the scanner carried on: the param history had
// already taken the new value and lastReconcile had already moved, so the
// next check found state and history in agreement and had nothing to
// report. The range was gone for good, and every publication inside it
// kept a deadline nothing knew to distrust.
func TestAReconcileThatCannotRecordItsRangeDoesNotAdvancePastIt(t *testing.T) {
	old := params(10*time.Minute, 4*time.Hour, 13*time.Hour)
	shorter := params(10*time.Minute, time.Hour, 13*time.Hour)
	s := &Scanner{log: NewLogger(10), chainID: "mocha-5", startHeight: 100, params: NewParamHistory(100, old), lastReconcile: 120}
	st, path := storeFor(t, s)
	s.store.SetSettledCoverFrom(100)

	// A directory where the record file belongs: every append fails.
	if err := os.RemoveAll(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(path, 0o755); err != nil {
		t.Fatal(err)
	}

	readAt := func(at int64) (fibretypes.Params, error) {
		if at >= 150 {
			return shorter, nil
		}
		return old, nil
	}
	s.reconcileParamsWith(180, func() (fibretypes.Params, error) { return shorter, nil }, readAt)

	if s.lastReconcile != 120 {
		t.Fatalf("lastReconcile = %d after a record that could not be written; want it left at 120 so the next check re-detects", s.lastReconcile)
	}
	if e := s.params.at(180, 0); e == nil || e.Params.ShardRetention != 4*time.Hour {
		t.Fatalf("the params history moved past a range nothing recorded: %+v", e)
	}

	// The file becomes writable. The next check re-detects the same
	// disagreement over a range that has only grown, and this time the
	// record lands.
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	s.reconcileParamsWith(240, func() (fibretypes.Params, error) { return shorter, nil }, readAt)

	got := readUncertainty(t, path)
	if len(got) != 1 {
		t.Fatalf("want one record after the retry, got %d", len(got))
	}
	if got[0].FromHeight != 121 || got[0].ToHeight != 240 {
		t.Fatalf("the retried range is %d-%d, want 121-240: the whole stretch, not just the second check's",
			got[0].FromHeight, got[0].ToHeight)
	}
	if s.lastReconcile != 240 {
		t.Fatalf("lastReconcile = %d after the record landed", s.lastReconcile)
	}
	if e := s.params.at(180, 0); e == nil || e.Params.ShardRetention != time.Hour {
		t.Fatalf("the proven value did not reach the history once the record landed: %+v", e)
	}
	_ = st
}

// The same rule for the blind-stretch record: the run is latched only once
// its line is on disk, so a failed write is retried at the next check
// rather than swallowed by the latch.
func TestAFailedCheckSkippedRecordIsRetriedRatherThanLatched(t *testing.T) {
	p := params(10*time.Minute, 4*time.Hour, 13*time.Hour)
	s := &Scanner{log: NewLogger(10), chainID: "mocha-5", startHeight: 100, params: NewParamHistory(100, p), lastReconcile: 120}
	_, path := storeFor(t, s)
	if err := os.RemoveAll(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(path, 0o755); err != nil {
		t.Fatal(err)
	}
	fail := func() (fibretypes.Params, error) { return fibretypes.Params{}, errors.New("rpc: connection refused") }

	s.reconcileParamsWith(180, fail, nil)
	if s.reconcileFailingSince != 0 {
		t.Fatalf("the run latched at %d although its record could not be written", s.reconcileFailingSince)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	s.reconcileParamsWith(240, fail, nil)
	if s.reconcileFailingSince != 121 {
		t.Fatalf("reconcileFailingSince = %d after the record landed, want 121", s.reconcileFailingSince)
	}
	if got := readUncertainty(t, path); len(got) != 1 || got[0].ToHeight != 240 {
		t.Fatalf("records = %+v", got)
	}
}

// Governance executes a passed proposal in EndBlock, and the handler's
// EventUpdateFibreParams comes out with the block's FinalizeBlock events,
// in force from the next block. When that block was a check height, the
// check compared the state after it (the new values) with the history at
// the end of it (the old ones) and reported the announced change as a
// silent one: a public silent_change record holding every verdict in the
// interval, and a "verified" value placed a block early. A change made in
// a block with no event is still found.
func TestAGovernanceChangeOnACheckHeightIsNotASilentChange(t *testing.T) {
	old := params(10*time.Minute, 4*time.Hour, 13*time.Hour)
	shorter := params(10*time.Minute, time.Hour, 13*time.Hour)
	s := &Scanner{log: NewLogger(10), chainID: "mocha-5", startHeight: 100, params: NewParamHistory(100, old), lastReconcile: 120}
	_, path := storeFor(t, s)
	s.store.SetSettledCoverFrom(100)

	// block 180's FinalizeBlock events, as processBlock reads them
	if !s.params.AddFinalizeEvent(180, shorter) {
		t.Fatal("finalize event not added")
	}
	readAt := func(at int64) (fibretypes.Params, error) {
		if at >= 180 {
			return shorter, nil
		}
		return old, nil
	}
	s.reconcileParamsWith(180, func() (fibretypes.Params, error) { return shorter, nil }, readAt)
	if got := readUncertainty(t, path); len(got) != 0 {
		t.Fatalf("an announced change was recorded as a silent one: %+v", got)
	}
	if s.lastReconcile != 180 {
		t.Fatalf("lastReconcile = %d, want 180", s.lastReconcile)
	}
	if e := s.params.at(180, 1<<30); e == nil || e.Params.ShardRetention != 4*time.Hour {
		t.Fatalf("block 180 itself = %+v, want the old value: the change is in force from 181", e)
	}
	if e := s.params.at(181, -1); e == nil || e.Params.ShardRetention != time.Hour || e.Source != "finalize" {
		t.Fatalf("from 181 = %+v, want the finalize entry", e)
	}

	// Block 240 changes them back with no event: that is a silent change.
	s.reconcileParamsWith(240, func() (fibretypes.Params, error) { return old, nil }, func(at int64) (fibretypes.Params, error) {
		if at >= 240 {
			return old, nil
		}
		return shorter, nil
	})
	got := readUncertainty(t, path)
	if len(got) != 1 || got[0].Kind != UncertaintySilentChange || got[0].FromHeight != 181 || got[0].ToHeight != 240 {
		t.Fatalf("records = %+v, want one silent_change 181-240", got)
	}
}

// A stop in the middle of reading a range closes nothing and proves
// nothing: written then, the range was unresolvable only because of the
// stop, and held as such. Nothing is written and nothing moves, so the
// next process's first check finds the difference and reads it again.
func TestAStopWhileARangeIsReadLeavesItForTheNextCheck(t *testing.T) {
	old := params(10*time.Minute, 4*time.Hour, 13*time.Hour)
	shorter := params(10*time.Minute, time.Hour, 13*time.Hour)
	s := &Scanner{log: NewLogger(10), chainID: "mocha-5", startHeight: 100, params: NewParamHistory(100, old), lastReconcile: 120}
	_, path := storeFor(t, s)
	s.store.SetSettledCoverFrom(100)

	s.reconcileParamsWith(180, func() (fibretypes.Params, error) { return shorter, nil }, func(at int64) (fibretypes.Params, error) {
		if at > 140 {
			return fibretypes.Params{}, fmt.Errorf("%w (abci query: context canceled)", errStopped)
		}
		return old, nil
	})
	if got := readUncertainty(t, path); len(got) != 0 {
		t.Fatalf("a stop mid-read wrote %+v", got)
	}
	if s.lastReconcile != 120 {
		t.Fatalf("lastReconcile = %d, want 120", s.lastReconcile)
	}
	if e := s.params.at(181, -1); e == nil || e.Params.ShardRetention != 4*time.Hour {
		t.Fatalf("the history moved past a range nothing recorded: %+v", e)
	}
}
