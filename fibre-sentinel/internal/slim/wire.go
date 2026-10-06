package slim

import (
	"encoding/binary"
	"encoding/hex"
	"errors"
	"strconv"
	"time"
)

// Tags of an encoded value. Every value carries one, so a decoder needs no schema: the format holds any field a
// record gains later, and a field it does not expect costs its bytes rather than its meaning.
const (
	tNull     = 0
	tFalse    = 1
	tTrue     = 2
	tInt      = 3  // a number written as a plain integer: zig-zag varint
	tNumLit   = 4  // any other number literal, by its dictionary entry
	tStrRef   = 5  // a string, by its dictionary entry
	tStrInl   = 6  // a string written in place: length, bytes
	tHexInl   = 7  // a lowercase hex string written in place as its bytes
	tTime     = 8  // an RFC 3339 UTC time: nanoseconds from the record's previous time, zig-zag varint
	tObj      = 9  // an object: its shape (the key list, by its table entry), then each value
	tArr      = 10 // an array: count, then each item
	tIntArr   = 11 // an array of plain integers: count, then each one's difference from the one before it
	tDerived  = 12 // computed again on decoding
	tAsgPos   = 13 // a row list as positions in the validator's own assignment: count, positions
	tShadow   = 14 // a row list equal to another settled promise's assignment of this validator: that promise's entry
	tValTable = 15 // a publication's validator list: set, hosts, attested bitmap, exceptions (pub.go)
)

var errShort = errors.New("slim: record ends early")

type writer struct{ b []byte }

func (w *writer) u(x uint64)   { w.b = binary.AppendUvarint(w.b, x) }
func (w *writer) s(x int64)    { w.b = binary.AppendVarint(w.b, x) }
func (w *writer) tag(t byte)   { w.b = append(w.b, t) }
func (w *writer) raw(p []byte) { w.u(uint64(len(p))); w.b = append(w.b, p...) }

type reader struct {
	b   []byte
	i   int
	err error
}

func (r *reader) u() uint64 {
	if r.err != nil {
		return 0
	}
	x, n := binary.Uvarint(r.b[r.i:])
	if n <= 0 {
		r.err = errShort
		return 0
	}
	r.i += n
	return x
}

func (r *reader) s() int64 {
	if r.err != nil {
		return 0
	}
	x, n := binary.Varint(r.b[r.i:])
	if n <= 0 {
		r.err = errShort
		return 0
	}
	r.i += n
	return x
}

func (r *reader) tag() byte {
	if r.err != nil || r.i >= len(r.b) {
		r.err = errShort
		return 0
	}
	t := r.b[r.i]
	r.i++
	return t
}

func (r *reader) raw() []byte {
	n := int(r.u())
	if r.err != nil || r.i+n > len(r.b) {
		r.err = errShort
		return nil
	}
	p := r.b[r.i : r.i+n]
	r.i += n
	return p
}

// Tables are what every record of a store shares, kept once: the dictionary of repeated strings, the shapes of
// objects (their key lists), the validator sets, the vectors of registered hosts, and each publication's facts
// that its readings repeat. Their bytes count towards the store's size like any record's.
type Tables struct {
	strs    []string
	strID   map[string]int
	shapes  [][]string
	shapeID map[string]int
	sets    []*valSet
	setID   map[string]int
	hosts   [][]hostEntry
	hostID  map[string]int
	pubs    []*pubInfo
	pubID   map[string]int

	// Repeats counts how often each string occurs across the records (a first pass fills it): a string seen once is
	// written in place, a repeated one goes to the dictionary. The choice changes sizes only, never what decodes.
	Repeats map[string]int
}

func NewTables() *Tables {
	return &Tables{strID: map[string]int{}, shapeID: map[string]int{}, setID: map[string]int{}, hostID: map[string]int{}, pubID: map[string]int{}, Repeats: map[string]int{}}
}

func (t *Tables) str(s string) int {
	if id, ok := t.strID[s]; ok {
		return id
	}
	t.strs = append(t.strs, s)
	t.strID[s] = len(t.strs) - 1
	return len(t.strs) - 1
}

func (t *Tables) shape(keys []string) int {
	k := ""
	for _, x := range keys {
		k += x + "\x00"
	}
	if id, ok := t.shapeID[k]; ok {
		return id
	}
	t.shapes = append(t.shapes, append([]string(nil), keys...))
	t.shapeID[k] = len(t.shapes) - 1
	return len(t.shapes) - 1
}

// Size is the tables' bytes as a store would write them: each string and each shape's key list once (keys by their
// dictionary entry), each validator set as 20-byte addresses and varint voting powers, each host vector as entries.
func (t *Tables) Size() (total int, parts map[string]int) {
	parts = map[string]int{}
	var w writer
	for _, s := range t.strs {
		w.raw([]byte(s))
	}
	parts["dictionary"] = len(w.b)
	w = writer{}
	for _, sh := range t.shapes {
		w.u(uint64(len(sh)))
		for _, k := range sh {
			w.u(uint64(t.strID[k]))
		}
	}
	parts["object shapes"] = len(w.b)
	w = writer{}
	for _, s := range t.sets {
		w.u(uint64(len(s.addr)))
		for i := range s.addr {
			w.b = append(w.b, s.addr[i][:]...)
			w.s(s.power[i])
		}
	}
	parts["validator sets"] = len(w.b)
	w = writer{}
	for _, hv := range t.hosts {
		w.u(uint64(len(hv)))
		for _, h := range hv {
			w.u(uint64(h.host))
			w.u(uint64(h.source))
		}
	}
	parts["host vectors"] = len(w.b)
	for _, v := range parts {
		total += v
	}
	return total, parts
}

// Counts says how big the tables are, in entries.
func (t *Tables) Counts() map[string]int {
	return map[string]int{"strings": len(t.strs), "shapes": len(t.shapes), "validator sets": len(t.sets), "host vectors": len(t.hosts), "publications": len(t.pubs)}
}

// enc writes one record's values; base is the time the next time value is written relative to.
type enc struct {
	t    *Tables
	w    writer
	base int64
	// bytes spent on exceptions: derivable fields the computation did not give back, row lists that are not the
	// validator's own assignment in its order
	exc int
}

func isHex(s string) bool {
	if len(s) < 16 || len(s)%2 == 1 {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}

func asTime(s string) (int64, bool) {
	if len(s) < 20 || s[len(s)-1] != 'Z' {
		return 0, false
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil || t.UTC().Format(time.RFC3339Nano) != s {
		return 0, false
	}
	return t.UnixNano(), true
}

func asInt(s string) (int64, bool) {
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil || strconv.FormatInt(n, 10) != s {
		return 0, false
	}
	return n, true
}

func (e *enc) value(v *Value) {
	switch v.Kind {
	case Null:
		e.w.tag(tNull)
	case Bool:
		if v.B {
			e.w.tag(tTrue)
		} else {
			e.w.tag(tFalse)
		}
	case Num:
		if n, ok := asInt(v.S); ok {
			e.w.tag(tInt)
			e.w.s(n)
		} else {
			e.w.tag(tNumLit)
			e.w.u(uint64(e.t.str(v.S)))
		}
	case Str:
		e.string(v.S)
	case Obj:
		e.w.tag(tObj)
		e.w.u(uint64(e.t.shape(v.Keys)))
		for _, k := range v.Keys {
			e.t.str(k)
		}
		for _, x := range v.Vals {
			e.value(x)
		}
	case Arr:
		ints := len(v.Vals) > 0
		for _, x := range v.Vals {
			if x.Kind != Num {
				ints = false
				break
			}
			if _, ok := asInt(x.S); !ok {
				ints = false
				break
			}
		}
		if ints {
			e.w.tag(tIntArr)
			e.w.u(uint64(len(v.Vals)))
			var prev int64
			for _, x := range v.Vals {
				n, _ := asInt(x.S)
				e.w.s(n - prev)
				prev = n
			}
			return
		}
		e.w.tag(tArr)
		e.w.u(uint64(len(v.Vals)))
		for _, x := range v.Vals {
			e.value(x)
		}
	case derived:
		if v.rowsBy != rowsNone {
			e.rowsValue(v)
			return
		}
		e.w.tag(tDerived)
	}
}

func (e *enc) string(s string) {
	if ns, ok := asTime(s); ok {
		e.w.tag(tTime)
		e.w.s(ns - e.base)
		e.base = ns
		return
	}
	if e.t.Repeats[s] < 2 {
		if isHex(s) {
			b, _ := hex.DecodeString(s)
			e.w.tag(tHexInl)
			e.w.raw(b)
			return
		}
		e.w.tag(tStrInl)
		e.w.raw([]byte(s))
		return
	}
	e.w.tag(tStrRef)
	e.w.u(uint64(e.t.str(s)))
}

// dec reads one record's values back.
type dec struct {
	t    *Tables
	r    reader
	base int64
}

var errTag = errors.New("slim: unknown tag")

func (d *dec) value() *Value {
	switch d.r.tag() {
	case tNull:
		return &Value{Kind: Null}
	case tFalse:
		return boolean(false)
	case tTrue:
		return boolean(true)
	case tInt:
		return num(d.r.s())
	case tNumLit:
		return &Value{Kind: Num, S: d.strAt(d.r.u())}
	case tStrRef:
		return str(d.strAt(d.r.u()))
	case tStrInl:
		return str(string(d.r.raw()))
	case tHexInl:
		return str(hex.EncodeToString(d.r.raw()))
	case tTime:
		d.base += d.r.s()
		return str(time.Unix(0, d.base).UTC().Format(time.RFC3339Nano))
	case tObj:
		id := d.r.u()
		if d.r.err != nil || id >= uint64(len(d.t.shapes)) {
			d.r.err = errTag
			return &Value{}
		}
		v := &Value{Kind: Obj, Keys: append([]string(nil), d.t.shapes[id]...)}
		for range v.Keys {
			v.Vals = append(v.Vals, d.value())
		}
		return v
	case tArr:
		n := int(d.r.u())
		v := &Value{Kind: Arr}
		for i := 0; i < n && d.r.err == nil; i++ {
			v.Vals = append(v.Vals, d.value())
		}
		return v
	case tIntArr:
		n := int(d.r.u())
		v := &Value{Kind: Arr}
		var prev int64
		for i := 0; i < n && d.r.err == nil; i++ {
			prev += d.r.s()
			v.Vals = append(v.Vals, num(prev))
		}
		return v
	case tDerived:
		return &Value{Kind: derived}
	case tAsgPos:
		n := int(d.r.u())
		v := &Value{Kind: derived, pos: make([]int, 0, n), rowsBy: rowsOwnPositions}
		for i := 0; i < n && d.r.err == nil; i++ {
			v.pos = append(v.pos, int(d.r.u()))
		}
		return v
	case tShadow:
		return &Value{Kind: derived, rowsBy: rowsShadow, shadow: int(d.r.u())}
	}
	d.r.err = errTag
	return &Value{}
}

func (d *dec) strAt(id uint64) string {
	if id >= uint64(len(d.t.strs)) {
		d.r.err = errTag
		return ""
	}
	return d.t.strs[id]
}
