package slim

import (
	"encoding/binary"
	"encoding/hex"
	"errors"
	"strconv"
	"sync"
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
	tShadow   = 14 // a row list equal to another settled promise's assignment of this validator: that promise's hash
	tValTable = 15 // a publication's validator list: set, hosts, attested bitmap, exceptions (pub.go)
)

var (
	errShort = errors.New("slim: record ends early")
	errTag   = errors.New("slim: unknown tag")
	// ErrUnknownEntry is a reference to a table entry this Tables does not hold yet: one another process added
	// since it was loaded. Load the new entries and decode again.
	ErrUnknownEntry = errors.New("slim: reference to a table entry not loaded")
)

// inlineHex are the keys whose hex values are a record's own (hashes, signatures): written in place as bytes. Every
// other string goes to the dictionary, which is where repeated ones (hosts, words, errors, certificates) belong.
var inlineHex = map[string]bool{"promise_hash": true, "commitment": true, "settlement_tx_hash": true, "signer_public_key": true,
	"signature": true, "rows_sha256": true, "shadowed_by": true, "day_commitment": true}

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

// tag, like every read, keeps the first error a record meets: an unknown entry met inside an object must stay the
// error the decode returns, not become "ends early" when the reads after it run off the misaligned bytes, since only
// an unknown entry makes a store load the entries added since.
func (r *reader) tag() byte {
	if r.err != nil {
		return 0
	}
	if r.i >= len(r.b) {
		r.err = errShort
		return 0
	}
	t := r.b[r.i]
	r.i++
	return t
}

func (r *reader) raw() []byte {
	n := int(r.u())
	if r.err != nil {
		return nil
	}
	if n < 0 || r.i+n > len(r.b) {
		r.err = errShort
		return nil
	}
	p := r.b[r.i : r.i+n]
	r.i += n
	return p
}

// Tables are what every record of a store shares, kept once: the dictionary of strings, the shapes of objects (their
// key lists), the validator sets and the vectors of registered hosts. Entries are only ever added, each under the
// next number of its kind, so a reader that loads them in order numbers them as the writer did. Safe for concurrent
// use.
type Tables struct {
	mu      sync.Mutex
	strs    []string
	strID   map[string]int
	shapes  [][]string
	shapeID map[string]int
	sets    []*valSet
	setID   map[[32]byte]int
	hosts   [][]hostEntry
	hostID  map[string]int
	// how many of each kind the store holds: entries past these are pending (Pending)
	stored [4]int
}

// Entry kinds.
const (
	KindString = 0
	KindShape  = 1
	KindSet    = 2
	KindHosts  = 3
)

// Entry is one table entry as a store keeps it: its kind, its number within the kind, and its body.
type Entry struct {
	Kind int
	ID   int
	Body []byte
}

func NewTables() *Tables {
	return &Tables{strID: map[string]int{}, shapeID: map[string]int{}, setID: map[[32]byte]int{}, hostID: map[string]int{}}
}

func (t *Tables) str(s string) int {
	if id, ok := t.strID[s]; ok {
		return id
	}
	t.strs = append(t.strs, s)
	t.strID[s] = len(t.strs) - 1
	return len(t.strs) - 1
}

func shapeKey(keys []string) string {
	k := ""
	for _, x := range keys {
		k += x + "\x00"
	}
	return k
}

func (t *Tables) shape(keys []string) int {
	k := shapeKey(keys)
	if id, ok := t.shapeID[k]; ok {
		return id
	}
	for _, x := range keys {
		t.str(x)
	}
	t.shapes = append(t.shapes, append([]string(nil), keys...))
	t.shapeID[k] = len(t.shapes) - 1
	return len(t.shapes) - 1
}

// Pending returns the entries added since the last call, in the order a store must keep them (strings first, since
// shapes and host vectors refer to them), and counts them as stored.
func (t *Tables) Pending() []Entry {
	t.mu.Lock()
	defer t.mu.Unlock()
	var out []Entry
	for i := t.stored[KindString]; i < len(t.strs); i++ {
		out = append(out, Entry{KindString, i, []byte(t.strs[i])})
	}
	for i := t.stored[KindShape]; i < len(t.shapes); i++ {
		var w writer
		w.u(uint64(len(t.shapes[i])))
		for _, k := range t.shapes[i] {
			w.u(uint64(t.strID[k]))
		}
		out = append(out, Entry{KindShape, i, w.b})
	}
	for i := t.stored[KindSet]; i < len(t.sets); i++ {
		var w writer
		s := t.sets[i]
		w.u(uint64(len(s.addr)))
		for j := range s.addr {
			w.b = append(w.b, s.addr[j][:]...)
			w.s(s.power[j])
		}
		out = append(out, Entry{KindSet, i, w.b})
	}
	for i := t.stored[KindHosts]; i < len(t.hosts); i++ {
		var w writer
		w.u(uint64(len(t.hosts[i])))
		for _, h := range t.hosts[i] {
			w.u(uint64(h.host))
			w.u(uint64(h.source))
		}
		out = append(out, Entry{KindHosts, i, w.b})
	}
	t.stored = [4]int{len(t.strs), len(t.shapes), len(t.sets), len(t.hosts)}
	return out
}

// Rollback forgets the entries Pending last handed out, for a store whose transaction did not commit: they were
// never kept, so the next record adds them again.
func (t *Tables) Rollback(kept [4]int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, s := range t.strs[kept[KindString]:] {
		delete(t.strID, s)
	}
	t.strs = t.strs[:kept[KindString]]
	for _, sh := range t.shapes[kept[KindShape]:] {
		delete(t.shapeID, shapeKey(sh))
	}
	t.shapes = t.shapes[:kept[KindShape]]
	for _, s := range t.sets[kept[KindSet]:] {
		delete(t.setID, s.key())
	}
	t.sets = t.sets[:kept[KindSet]]
	for _, hv := range t.hosts[kept[KindHosts]:] {
		delete(t.hostID, hostKey(hv))
	}
	t.hosts = t.hosts[:kept[KindHosts]]
	t.stored = kept
}

// Stored is how many entries of each kind the store holds, as of the last Pending or Add.
func (t *Tables) Stored() [4]int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.stored
}

// Add loads a stored entry. Entries of a kind must come in their order, each the next number.
func (t *Tables) Add(e Entry) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	r := reader{b: e.Body}
	switch e.Kind {
	case KindString:
		if e.ID != len(t.strs) {
			return errors.New("slim: string entries out of order")
		}
		t.strs = append(t.strs, string(e.Body))
		t.strID[string(e.Body)] = e.ID
	case KindShape:
		if e.ID != len(t.shapes) {
			return errors.New("slim: shape entries out of order")
		}
		n := int(r.u())
		keys := make([]string, 0, n)
		for i := 0; i < n && r.err == nil; i++ {
			id := int(r.u())
			if id >= len(t.strs) {
				return ErrUnknownEntry
			}
			keys = append(keys, t.strs[id])
		}
		t.shapes = append(t.shapes, keys)
		t.shapeID[shapeKey(keys)] = e.ID
	case KindSet:
		if e.ID != len(t.sets) {
			return errors.New("slim: set entries out of order")
		}
		// each validator is its address and at least one byte of power
		n := int(r.u())
		if r.err == nil && (n < 0 || n > (len(r.b)-r.i)/21) {
			return errShort
		}
		s := &valSet{addr: make([][20]byte, 0, n), power: make([]int64, 0, n)}
		for i := 0; i < n && r.err == nil; i++ {
			if r.i+20 > len(r.b) {
				return errShort
			}
			var a [20]byte
			copy(a[:], r.b[r.i:r.i+20])
			r.i += 20
			s.addr = append(s.addr, a)
			s.power = append(s.power, r.s())
		}
		t.sets = append(t.sets, s)
		t.setID[s.key()] = e.ID
	case KindHosts:
		if e.ID != len(t.hosts) {
			return errors.New("slim: host entries out of order")
		}
		n := int(r.u())
		hv := make([]hostEntry, 0, n)
		for i := 0; i < n && r.err == nil; i++ {
			hv = append(hv, hostEntry{host: int(r.u()), source: int(r.u())})
		}
		t.hosts = append(t.hosts, hv)
		t.hostID[hostKey(hv)] = e.ID
	default:
		return errors.New("slim: unknown entry kind")
	}
	if r.err != nil {
		return r.err
	}
	t.stored[e.Kind] = e.ID + 1
	return nil
}

// enc writes one record's values; base is the time the next time value is written relative to.
type enc struct {
	t    *Tables
	w    writer
	base int64
	exc  int // bytes spent on exceptions
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
	// nanoseconds since 1970 hold the years 1678 to 2262 only: a zero time (year 1) stays a string
	if y := t.Year(); y < 1700 || y > 2200 {
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

// value writes v, the value of key (the key decides only how a hex string is written).
func (e *enc) value(key string, v *Value) {
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
		e.string(key, v.S)
	case Obj:
		e.w.tag(tObj)
		e.w.u(uint64(e.t.shape(v.Keys)))
		for i, x := range v.Vals {
			e.value(v.Keys[i], x)
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
			e.value(key, x)
		}
	case derived:
		switch {
		case v.vt != nil:
			e.valTable(v.vt)
		case v.rowsBy != rowsNone:
			e.rowsValue(v)
		default:
			e.w.tag(tDerived)
		}
	}
}

func (e *enc) string(key, s string) {
	if ns, ok := asTime(s); ok {
		e.w.tag(tTime)
		e.w.s(ns - e.base)
		e.base = ns
		return
	}
	if inlineHex[key] && isHex(s) {
		b, _ := hex.DecodeString(s)
		e.w.tag(tHexInl)
		e.w.raw(b)
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

func (d *dec) fail(err error) *Value {
	if d.r.err == nil {
		d.r.err = err
	}
	return &Value{}
}

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
		if d.r.err != nil {
			return &Value{}
		}
		if id >= uint64(len(d.t.shapes)) {
			return d.fail(ErrUnknownEntry)
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
		v := &Value{Kind: derived, rowsBy: rowsOwnPositions}
		for i := 0; i < n && d.r.err == nil; i++ {
			v.pos = append(v.pos, int(d.r.u()))
		}
		return v
	case tShadow:
		return &Value{Kind: derived, rowsBy: rowsShadow, shadow: hex.EncodeToString(d.r.raw())}
	case tValTable:
		return d.valTable()
	}
	return d.fail(errTag)
}

func (d *dec) strAt(id uint64) string {
	if id >= uint64(len(d.t.strs)) {
		d.fail(ErrUnknownEntry)
		return ""
	}
	return d.t.strs[id]
}
