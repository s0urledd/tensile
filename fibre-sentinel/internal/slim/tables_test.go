package slim

import (
	"runtime"
	"testing"
)

// The tables keep every validator set a store has met for the life of the process (the collector's and the API's),
// so a set is held as its addresses and powers and nothing more: no hex copy of each address, and no key that spells
// the set out again.
func TestAValidatorSetIsHeldOnce(t *testing.T) {
	const sets, vals = 200, 100
	var es []Entry
	for i := 0; i < sets; i++ {
		var w writer
		w.u(vals)
		for j := 0; j < vals; j++ {
			var a [20]byte
			a[0], a[1], a[19] = byte(j), byte(j>>8), 0x5a
			w.b = append(w.b, a[:]...)
			w.s(int64(1_000_000 + i*vals + j)) // one power moved: another set
		}
		es = append(es, Entry{KindSet, i, w.b})
	}
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	tb := NewTables()
	for _, e := range es {
		if err := tb.Add(e); err != nil {
			t.Fatal(err)
		}
	}
	runtime.GC()
	runtime.ReadMemStats(&after)
	per := (int64(after.HeapAlloc) - int64(before.HeapAlloc)) / sets
	runtime.KeepAlive(es) // the entries are counted on both sides
	runtime.KeepAlive(tb)
	// addresses and powers are 2,800 bytes of a 100-validator set
	if per > 4500 {
		t.Errorf("a %d-validator set holds %d bytes in the tables", vals, per)
	}
	if len(tb.setID) != sets {
		t.Fatalf("%d sets keyed, want %d", len(tb.setID), sets)
	}
	// the same set again is the same entry
	again := &valSet{addr: tb.sets[7].addr, power: tb.sets[7].power}
	tb.mu.Lock()
	id := tb.setEntry(again)
	tb.mu.Unlock()
	if id != 7 || len(tb.sets) != sets {
		t.Fatalf("the same set is entry %d of %d", id, len(tb.sets))
	}
}
