package api

import (
	"testing"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/verdict"
)

// The median is over validators' own rates, each once, and only those with
// enough decided obligations to be ranked; the pooled rate is every
// obligation together. One large validator moves the pooled rate and not
// the median, which is why both are published.
func TestNetworkReferenceMedian(t *testing.T) {
	row := func(served, broken int64) validatorRow {
		return validatorRow{Obligations: obligationStats{Served: served, Broken: broken}}
	}
	rows := []validatorRow{
		row(20, 0),    // 1.00
		row(18, 2),    // 0.90
		row(10, 10),   // 0.50
		row(5, 0),     // too few to rank, still pooled
		row(900, 100), // 0.90, and most of the pooled rate
	}
	ref := networkReferenceFrom(rows)
	if ref.Validators != 4 || ref.Median == nil || *ref.Median < 0.8999 || *ref.Median > 0.9001 {
		t.Fatalf("median over %d = %v, want 0.9 over 4", ref.Validators, ref.Median)
	}
	if ref.Pooled.Num != 953 || ref.Pooled.Den != 1065 {
		t.Errorf("pooled = %d/%d", ref.Pooled.Num, ref.Pooled.Den)
	}
	even := networkReferenceFrom(rows[:2])
	if even.Median == nil || *even.Median < 0.9499 || *even.Median > 0.9501 {
		t.Errorf("even median = %v, want 0.95", even.Median)
	}
	if none := networkReferenceFrom(rows[3:4]); none.Median != nil || none.Validators != 0 {
		t.Errorf("a median was drawn from no ranked validator: %+v", none)
	}
}

func TestIsProvisional(t *testing.T) {
	now := time.Now()
	young := store.TS(now.Add(-verdict.FaultSettling + time.Minute))
	old := store.TS(now.Add(-verdict.FaultSettling - time.Minute))
	// The class is the counted one (rollup.CountedClass): a reading with no
	// rows on a blob that could not be reconstructed is FAULT there, one on
	// a blob that could is NOT_COUNTED.
	if !isProvisional("FAULT", young, now) || isProvisional("FAULT", old, now) || isProvisional("HEALTHY", young, now) ||
		isProvisional("NOT_COUNTED", young, now) || isProvisional("UNREACHABLE", young, now) {
		t.Error("provisional is a counted not-served reading younger than the settling period, and nothing else")
	}
	total := provisionalTotal(map[string]*provisionalFaults{
		"a": {Obligations: 1, Until: "2026-09-24T10:00:00Z"},
		"b": {Obligations: 2, Until: "2026-09-24T10:20:00Z"},
	})
	if total == nil || total.Obligations != 3 || total.Until != "2026-09-24T10:20:00Z" {
		t.Errorf("total = %+v", total)
	}
	if provisionalTotal(nil) != nil {
		t.Error("no provisional faults must be absent, not zero")
	}
}
