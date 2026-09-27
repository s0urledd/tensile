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
	ref := networkReferenceFrom(Window{Name: "24h"}, rows, time.Now())
	if ref.Validators != 4 || ref.Median == nil || *ref.Median < 0.8999 || *ref.Median > 0.9001 {
		t.Fatalf("median over %d = %v, want 0.9 over 4", ref.Validators, ref.Median)
	}
	if ref.Pooled.Num != 953 || ref.Pooled.Den != 1065 {
		t.Errorf("pooled = %d/%d", ref.Pooled.Num, ref.Pooled.Den)
	}
	even := networkReferenceFrom(Window{}, rows[:2], time.Now())
	if even.Median == nil || *even.Median < 0.9499 || *even.Median > 0.9501 {
		t.Errorf("even median = %v, want 0.95", even.Median)
	}
	if none := networkReferenceFrom(Window{}, rows[3:4], time.Now()); none.Median != nil || none.Validators != 0 {
		t.Errorf("a median was drawn from no ranked validator: %+v", none)
	}
}

func TestIsProvisional(t *testing.T) {
	now := time.Now()
	young := store.TS(now.Add(-verdict.FaultSettling + time.Minute))
	old := store.TS(now.Add(-verdict.FaultSettling - time.Minute))
	if !isProvisional("FAULT", "w4", young, now) || isProvisional("FAULT", "w4", old, now) || isProvisional("HEALTHY", "w4", young, now) ||
		isProvisional("UNREACHABLE", "w4", young, now) {
		t.Error("provisional is a FAULT younger than the settling period, and nothing else")
	}
	if !isProvisional("UNREACHABLE", "end", young, now) || isProvisional("UNREACHABLE", "end", old, now) || isProvisional("SHADOWED_SHARD", "end", young, now) {
		t.Error("an end reading with no rows settles like a FAULT, and genuine rows are not provisional")
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
