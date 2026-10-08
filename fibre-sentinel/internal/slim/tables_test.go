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

// A shape, set or host entry whose counts or references its body cannot hold (a torn page, a hand edit, another
// build's layout) is that entry's error, and the tables are left as they were, so the same entry, read right, still
// loads under its number. A count of 2^35 asked make() for 512 GiB, which no recover catches, and one past 2^63 read
// as a negative capacity and panicked.
func TestAddRefusesWhatABodyCannotHold(t *testing.T) {
	huge := []byte{0x80, 0x80, 0x80, 0x80, 0x80, 0x01}                             // 2^35
	negative := []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01} // 2^64-1: -1 as an int
	cat := func(parts ...[]byte) []byte {
		var b []byte
		for _, p := range parts {
			b = append(b, p...)
		}
		return b
	}
	tb := NewTables()
	if err := tb.Add(Entry{KindString, 0, []byte("a:7980")}); err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		kind int
		body []byte
	}{
		{KindShape, huge},
		{KindShape, negative},
		{KindShape, []byte{0x02, 0x00}},                              // two keys, one byte for them
		{KindShape, []byte{0x01, 0x05}},                              // a string not loaded
		{KindShape, cat([]byte{0x01}, negative)},                     // a key id past 2^63
		{KindShape, []byte{0x02, 0x00, 0x80}},                        // the second key cut
		{KindSet, negative},                                          // set counts were bounded already, negative ones too
		{KindSet, cat([]byte{0x01}, make([]byte, 20), []byte{0x80})}, // the power cut
		{KindHosts, huge},
		{KindHosts, negative},
		{KindHosts, []byte{0x02, 0x01, 0x00}}, // two entries, room for one
		{KindHosts, []byte{0x01, 0x02, 0x00}}, // host id + 1 = 2, one string loaded
		{KindHosts, []byte{0x01, 0x00, 0x02}}, // the same for the source
		{KindHosts, cat([]byte{0x01}, negative, []byte{0x00})},
		{KindHosts, []byte{0x02, 0x01, 0x01, 0x01, 0x80}}, // the second entry cut
	} {
		if err := tb.Add(Entry{c.kind, 0, c.body}); err == nil {
			t.Errorf("kind %d, body % x: loaded", c.kind, c.body)
		}
	}
	if got := tb.Stored(); got != [4]int{1, 0, 0, 0} {
		t.Fatalf("stored %v after refused entries", got)
	}
	// What the store writes for these, read right, under the numbers the refused ones did not take.
	for _, e := range []Entry{
		{KindShape, 0, []byte{0x01, 0x00}},
		{KindSet, 0, cat([]byte{0x01}, make([]byte, 20), []byte{0x02})},
		{KindHosts, 0, []byte{0x02, 0x01, 0x00, 0x00, 0x00}},
	} {
		if err := tb.Add(e); err != nil {
			t.Fatalf("kind %d: %v", e.Kind, err)
		}
	}
	if got := tb.Stored(); got != [4]int{1, 1, 1, 1} {
		t.Fatalf("stored %v", got)
	}
}
