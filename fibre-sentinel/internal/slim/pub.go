package slim

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	assign "github.com/plsgiveup/fibre/fibre-assign"
)

// valSet is a validator set as a publication's assignment lists it: addresses and voting powers, voting power
// descending then address ascending (the scanner's canonical order). A store's tables hold every set they have met
// for the life of the process, so a set keeps nothing it can compute: an address's hex is written when it is needed.
type valSet struct {
	addr  [][20]byte
	power []int64
}

func (s *valSet) hex(i int) string { return hex.EncodeToString(s.addr[i][:]) }

// key identifies the set in its table by a hash of its addresses and powers: a key spelling them out would keep a
// second copy of every set the tables hold.
func (s *valSet) key() [32]byte {
	h := sha256.New()
	var b []byte
	for i := range s.addr {
		b = append(b[:0], s.addr[i][:]...)
		b = binary.AppendVarint(b, s.power[i])
		h.Write(b)
	}
	var k [32]byte
	h.Sum(k[:0])
	return k
}

func (s *valSet) validators() []assign.Validator {
	out := make([]assign.Validator, len(s.addr))
	for i := range s.addr {
		out[i] = assign.Validator{Address: assign.Address(s.addr[i]), VotingPower: s.power[i]}
	}
	return out
}

// hostEntry is one validator's registered host at settlement and where the scanner learned it; 0 is absent, else a
// dictionary entry + 1.
type hostEntry struct{ host, source int }

func hostKey(hv []hostEntry) string {
	var b strings.Builder
	for _, h := range hv {
		fmt.Fprintf(&b, "%d,%d;", h.host, h.source)
	}
	return b.String()
}

// Pub is what a publication's readings take from it: its facts and its assignment. A store keeps one per
// publication it has decoded or encoded, so a reading is encoded and decoded against it.
type Pub struct {
	Hash string
	tree *Value
	set  *valSet
	idx  map[[20]byte]int // validator address → its place in set
	rows [][]int          // each validator's assigned rows, as fibre-assign computes them
	// pinErr is ErrAssignmentPin when the record's assignment is from a celestia-app this build does not reproduce:
	// then set is nil, and a reading that takes its validator from the publication cannot be decoded.
	pinErr error
}

// index is the place in the publication's set of the validator at addr (its consensus address in lowercase hex).
func (p *Pub) index(addr string) (int, bool) {
	if len(addr) != 40 || !isHex(addr) {
		return 0, false
	}
	var a [20]byte
	hex.Decode(a[:], []byte(addr))
	i, ok := p.idx[a]
	return i, ok
}

// assigned keeps set and rows as the publication's assignment, and indexes the set.
func (p *Pub) assigned(set *valSet, rows [][]int) {
	p.set, p.rows = set, rows
	p.idx = make(map[[20]byte]int, len(set.addr))
	for i, a := range set.addr {
		p.idx[a] = i
	}
}

// ErrAssignmentPin is a record whose rows were derived under a celestia-app pin this build's fibre-assign is not
// known to reproduce. Decoding it would compute the rows with another algorithm and show a past that is not the
// record's, so it is refused instead; the record's own bytes are untouched.
var ErrAssignmentPin = errors.New("slim: the record's assignment is from a celestia-app pin this build does not reproduce")

// assignPins are the celestia-app commits whose shard assignment the compiled fibre-assign computes exactly: its own
// pin, and each earlier pin whose assignment code did not change when the pin moved on. A record names the pin its
// scanner assigned with (assignment.protocol_params.pinned_celestia_app_commit), and its rows are derived on encoding,
// and computed again on decoding, only when that pin is one of these. When fibre-assign is re-pinned and reftest is
// bit-identical across the two, the pin it leaves is added here; when the assignment changed, it is not, and the
// records derived under it are refused (ErrAssignmentPin) rather than rebuilt with the new algorithm.
var assignPins = map[string]bool{
	assign.PinnedCelestiaAppCommit: true,
	// v10.2.0-mocha, the pin until 2026-10-06: v10.4.0-mocha changes none of fibre/protocol_params.go, fibre/blob.go,
	// fibre/validator or x/fibre, and reftest is bit-identical across the two (fibre-assign/params.go)
	"3b77dc2f5b00e1a646a2e9dd98b5c024a0d9ad8a": true,
}

// pinOf is nil when this build reproduces the assignment of the celestia-app pin a publication names, else
// ErrAssignmentPin naming that pin.
func pinOf(pub *Value) error {
	pin, _ := strField(pub.Path("assignment", "protocol_params"), "pinned_celestia_app_commit")
	if assignPins[pin] {
		return nil
	}
	return fmt.Errorf("%w (record pin %q, built with %s)", ErrAssignmentPin, pin, assign.PinnedCelestiaAppCommit)
}

// pubTree keeps of a publication record only what its readings take from it: the promise's commitment and blob
// version, the deadline, the validator set height, and each validator's attestation and host. A store caches Pubs, and
// the whole record (its row lists above all) would make each one megabytes.
func pubTree(full *Value) *Value {
	keep := func(src *Value, keys ...string) *Value {
		o := &Value{Kind: Obj}
		for _, k := range keys {
			if x := src.Get(k); x != nil {
				o.Keys = append(o.Keys, k)
				o.Vals = append(o.Vals, clone(x))
			}
		}
		return o
	}
	t := keep(full, "promise_hash", "must_serve_until")
	if p := full.Get("promise"); p != nil {
		t.Keys, t.Vals = append(t.Keys, "promise"), append(t.Vals, keep(p, "commitment", "blob_version", "creation_timestamp"))
	}
	if a := full.Get("assignment"); a != nil {
		at := keep(a, "validator_set_height")
		if vs := a.Get("validators"); vs != nil && vs.Kind == Arr {
			list := &Value{Kind: Arr}
			for _, v := range vs.Vals {
				list.Vals = append(list.Vals, keep(v, "attested", "host_at_settlement"))
			}
			at.Keys, at.Vals = append(at.Keys, "validators"), append(at.Vals, list)
		}
		t.Keys, t.Vals = append(t.Keys, "assignment"), append(t.Vals, at)
	}
	return t
}

// PubFromLine is what a publication record in its JSONL form gives its readings (a row an earlier build stored): no
// table is touched. Its rows are computed only under a pin this build reproduces, as EncodePublication derives them.
func PubFromLine(line []byte) (*Pub, error) {
	orig, err := Parse(line)
	if err != nil {
		return nil, err
	}
	info := &Pub{tree: pubTree(orig), pinErr: pinOf(orig)}
	info.Hash, _ = strField(orig, "promise_hash")
	a := orig.Get("assignment")
	set := setFrom(a.Get("validators"))
	c, okc := commitmentOf(orig)
	pp, okp := protocolParams(orig)
	if set != nil && okc && okp && a.Get("error") == nil && info.pinErr == nil {
		if rows, ok := assignRows(set, c, pp); ok {
			info.assigned(set, rows)
		}
	}
	return info, nil
}

// PinErr is ErrAssignmentPin, naming the record's pin, when the publication's assignment is from a celestia-app this
// build does not reproduce (Rows then gives no rows); nil otherwise.
func (p *Pub) PinErr() error {
	if p == nil {
		return nil
	}
	return p.pinErr
}

func (p *Pub) validator(i int) *Value {
	vs := p.tree.Path("assignment", "validators")
	if vs == nil || i >= len(vs.Vals) {
		return nil
	}
	return vs.Vals[i]
}

// Rows is the validator's assigned rows (its consensus address in hex), and whether the publication has an
// assignment that names it.
func (p *Pub) Rows(addr string) ([]int, bool) {
	if p == nil || p.set == nil {
		return nil, false
	}
	i, ok := p.index(addr)
	if !ok {
		return nil, false
	}
	return p.rows[i], true
}

// rowsKind says how a decoder rebuilds a row list.
type rowsKind uint8

const (
	rowsNone         rowsKind = iota
	rowsOwnPositions          // positions in the validator's own assignment
	rowsShadow                // another promise's assignment of the validator
)

// valTable is a publication's validator list in its slim form.
type valTable struct {
	set        int // validator set entry
	hosts      int // host vector entry
	attested   []byte
	rowsStored bool
	exceptions map[int]*Value // validators whose entry is not what the set, rows, bit and host give
}

func protocolParams(pub *Value) (assign.ProtocolParams, bool) {
	p := pub.Path("assignment", "protocol_params")
	get := func(k string) (int64, bool) {
		v := p.Get(k)
		if v == nil || v.Kind != Num {
			return 0, false
		}
		return asInt(v.S)
	}
	o, ok1 := get("original_rows")
	t, ok2 := get("total_rows")
	m, ok3 := get("min_rows_per_validator")
	n, ok4 := get("liveness_threshold_numerator")
	d, ok5 := get("liveness_threshold_denominator")
	if !(ok1 && ok2 && ok3 && ok4 && ok5) {
		return assign.ProtocolParams{}, false
	}
	pp := assign.ProtocolParams{OriginalRows: int(o), TotalRows: int(t), MinRowsPerValidator: int(m), LivenessThreshold: assign.Fraction{Numerator: uint64(n), Denominator: uint64(d)}}
	if pp.Validate() != nil {
		return assign.ProtocolParams{}, false
	}
	return pp, true
}

func commitmentOf(pub *Value) ([32]byte, bool) {
	var c [32]byte
	v := pub.Path("promise", "commitment")
	if v == nil || v.Kind != Str {
		return c, false
	}
	b, err := hex.DecodeString(v.S)
	if err != nil || len(b) != 32 {
		return c, false
	}
	copy(c[:], b)
	return c, true
}

// assignRows computes each validator's rows over set.
func assignRows(set *valSet, c [32]byte, pp assign.ProtocolParams) ([][]int, bool) {
	sm, err := assign.Assign(c, set.validators(), pp)
	if err != nil {
		return nil, false
	}
	out := make([][]int, len(set.addr))
	for i := range set.addr {
		out[i] = sm[assign.Address(set.addr[i])]
	}
	return out, true
}

// expectedValidator is a validator's entry as the scanner writes it from the set, its rows, its attestation and its host.
func (t *Tables) expectedValidator(set *valSet, i int, rows []int, rowsStored, attested bool, h hostEntry) *Value {
	v := &Value{Kind: Obj}
	add := func(k string, x *Value) { v.Keys = append(v.Keys, k); v.Vals = append(v.Vals, x) }
	add("address", str(set.hex(i)))
	add("voting_power", num(set.power[i]))
	add("row_count", num(int64(len(rows))))
	if rowsStored && len(rows) > 0 {
		add("rows", rowList(rows))
	}
	add("attested", boolean(attested))
	if h.host > 0 {
		add("host_at_settlement", str(t.strs[h.host-1]))
	}
	if h.source > 0 {
		add("host_at_settlement_source", str(t.strs[h.source-1]))
	}
	return v
}

// setFrom reads the validator set off a publication's validator list; nil when an entry is not address + power.
func setFrom(vs *Value) *valSet {
	if vs == nil || vs.Kind != Arr {
		return nil
	}
	s := &valSet{}
	for _, v := range vs.Vals {
		a, p := v.Get("address"), v.Get("voting_power")
		if a == nil || p == nil || a.Kind != Str || p.Kind != Num {
			return nil
		}
		b, err := hex.DecodeString(a.S)
		n, ok := asInt(p.S)
		if err != nil || len(b) != 20 || !ok || hex.EncodeToString(b) != a.S {
			return nil
		}
		var ad [20]byte
		copy(ad[:], b)
		s.addr = append(s.addr, ad)
		s.power = append(s.power, n)
	}
	return s
}

func (t *Tables) setEntry(s *valSet) int {
	k := s.key()
	if id, ok := t.setID[k]; ok {
		return id
	}
	t.sets = append(t.sets, s)
	t.setID[k] = len(t.sets) - 1
	return len(t.sets) - 1
}

func (t *Tables) hostEntry(hv []hostEntry) int {
	k := hostKey(hv)
	if id, ok := t.hostID[k]; ok {
		return id
	}
	t.hosts = append(t.hosts, hv)
	t.hostID[k] = len(t.hosts) - 1
	return len(t.hosts) - 1
}

func strField(v *Value, k string) (string, bool) {
	x := v.Get(k)
	if x == nil || x.Kind != Str {
		return "", false
	}
	return x.S, true
}

// assignmentSummary computes the assignment's totals the way the scanner does.
func assignmentSummary(set *valSet, rows [][]int, attested []bool) map[string]int64 {
	var total, attPower int64
	sigma, withRows, attWithRows := 0, 0, 0
	seen := map[int]int{}
	for i := range set.addr {
		total += set.power[i]
		if attested[i] {
			attPower += set.power[i]
		}
		if len(rows[i]) > 0 {
			withRows++
			if attested[i] {
				attWithRows++
			}
		}
		sigma += len(rows[i])
		for _, r := range rows[i] {
			seen[r]++
		}
	}
	overlaps := 0
	for _, c := range seen {
		if c > 1 {
			overlaps++
		}
	}
	return map[string]int64{"total_voting_power": total, "sigma": int64(sigma), "distinct": int64(len(seen)), "wrap_overlaps": int64(overlaps),
		"validators_with_rows": int64(withRows), "attested_with_rows": int64(attWithRows), "attested_voting_power": attPower}
}

// mustServeUntil is creation + max(payment promise timeout, shard retention), from the params the record carries.
func mustServeUntil(pub *Value) (string, bool) {
	c, ok := strField(pub.Get("promise"), "creation_timestamp")
	if !ok {
		return "", false
	}
	ct, err := time.Parse(time.RFC3339Nano, c)
	if err != nil {
		return "", false
	}
	p := pub.Get("params_at_publication")
	ppt, ok1 := strField(p, "payment_promise_timeout")
	ret, ok2 := strField(p, "shard_retention")
	if !ok1 || !ok2 {
		return "", false
	}
	a, err1 := time.ParseDuration(ppt)
	b, err2 := time.ParseDuration(ret)
	if err1 != nil || err2 != nil {
		return "", false
	}
	if b > a {
		a = b
	}
	return ct.Add(a).UTC().Format(time.RFC3339Nano), true
}

// derive replaces v's value at key with the derived marker when want writes out the same; reports whether it did.
func derive(v *Value, key string, want *Value) bool {
	if v == nil || v.Kind != Obj || want == nil {
		return false
	}
	for i, k := range v.Keys {
		if k == key && Same(v.Vals[i], want) {
			v.Vals[i] = &Value{Kind: derived}
			return true
		}
	}
	return false
}

// EncodePublication writes a publication record in slim form, and returns what its readings will take from it.
// The bytes it adds to the tables are in Pending.
func (t *Tables) EncodePublication(line []byte) ([]byte, *Pub, error) {
	orig, err := Parse(line)
	if err != nil {
		return nil, nil, err
	}
	if orig.Kind != Obj {
		return nil, nil, errors.New("slim: a publication record is not an object")
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	v := clone(orig)
	info := &Pub{tree: pubTree(orig), pinErr: pinOf(orig)}
	info.Hash, _ = strField(orig, "promise_hash")

	// the namespace's version and id are its first byte and the rest
	if p := v.Get("promise"); p != nil {
		if ns, ok := strField(p, "namespace"); ok && len(ns) >= 2 {
			if n, err := strconv.ParseUint(ns[:2], 16, 8); err == nil {
				derive(p, "namespace_version", num(int64(n)))
			}
			derive(p, "namespace_id", str(ns[2:]))
		}
	}
	if msu, ok := mustServeUntil(orig); ok {
		derive(v, "must_serve_until", str(msu))
	}

	// the validator list: the set, the hosts, one bit each for attested, and only the entries that differ in full;
	// only under a pin this build reproduces, since decoding computes the rows again (else the list is kept whole)
	a := v.Get("assignment")
	vs := a.Get("validators")
	set := setFrom(vs)
	c, okc := commitmentOf(orig)
	pp, okp := protocolParams(orig)
	if set != nil && okc && okp && a.Get("error") == nil && info.pinErr == nil {
		if rows, ok := assignRows(set, c, pp); ok {
			info.assigned(set, rows)
			vt := &valTable{set: t.setEntry(set), attested: make([]byte, (len(set.addr)+7)/8), exceptions: map[int]*Value{}}
			hv := make([]hostEntry, len(set.addr))
			att := make([]bool, len(set.addr))
			for i, x := range vs.Vals {
				if x.Get("rows") != nil {
					vt.rowsStored = true
				}
				if b := x.Get("attested"); b != nil && b.Kind == Bool && b.B {
					att[i] = true
					vt.attested[i/8] |= 1 << (i % 8)
				}
				if s, ok := strField(x, "host_at_settlement"); ok {
					hv[i].host = t.str(s) + 1
				}
				if s, ok := strField(x, "host_at_settlement_source"); ok {
					hv[i].source = t.str(s) + 1
				}
			}
			vt.hosts = t.hostEntry(hv)
			for i, x := range vs.Vals {
				if !Same(x, t.expectedValidator(set, i, rows[i], vt.rowsStored, att[i], hv[i])) {
					vt.exceptions[i] = x
				}
			}
			for i, k := range a.Keys {
				if k == "validators" {
					a.Vals[i] = &Value{Kind: derived, vt: vt}
				}
			}
			for k, n := range assignmentSummary(set, rows, att) {
				derive(a, k, num(n))
			}
		}
	}
	e := &enc{t: t}
	e.value("", v)
	return e.w.b, info, nil
}

func (e *enc) valTable(vt *valTable) {
	e.w.tag(tValTable)
	e.w.u(uint64(vt.set))
	e.w.u(uint64(vt.hosts))
	if vt.rowsStored {
		e.w.u(1)
	} else {
		e.w.u(0)
	}
	e.w.b = append(e.w.b, vt.attested...)
	e.w.u(uint64(len(vt.exceptions)))
	excStart := len(e.w.b)
	for i := 0; i < len(e.t.sets[vt.set].addr); i++ {
		if x, ok := vt.exceptions[i]; ok {
			e.w.u(uint64(i))
			e.value("", x)
		}
	}
	e.exc += len(e.w.b) - excStart
}

func (d *dec) valTable() *Value {
	vt := &valTable{exceptions: map[int]*Value{}}
	vt.set = int(d.r.u())
	vt.hosts = int(d.r.u())
	vt.rowsStored = d.r.u() == 1
	if d.r.err != nil {
		return &Value{}
	}
	if vt.set >= len(d.t.sets) || vt.hosts >= len(d.t.hosts) {
		return d.fail(ErrUnknownEntry)
	}
	n := len(d.t.sets[vt.set].addr)
	nb := (n + 7) / 8
	if d.r.i+nb > len(d.r.b) {
		return d.fail(errShort)
	}
	vt.attested = d.r.b[d.r.i : d.r.i+nb]
	d.r.i += nb
	ne := int(d.r.u())
	for j := 0; j < ne && d.r.err == nil; j++ {
		i := int(d.r.u())
		vt.exceptions[i] = d.value()
	}
	return &Value{Kind: derived, vt: vt}
}

// DecodePublication reads a slim publication back to its record line, and returns what its readings take from it.
func (t *Tables) DecodePublication(b []byte) ([]byte, *Pub, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	d := &dec{t: t, r: reader{b: b}}
	v := d.value()
	if d.r.err != nil {
		return nil, nil, d.r.err
	}
	if d.r.i != len(b) || v.Kind != Obj {
		return nil, nil, errors.New("slim: not a publication record")
	}
	info := &Pub{}
	if err := t.fillPublication(v, info); err != nil {
		return nil, nil, err
	}
	info.tree = pubTree(v)
	info.Hash, _ = strField(v, "promise_hash")
	return Emit(v), info, nil
}

// fillPublication computes every derived field of a decoded publication.
func (t *Tables) fillPublication(v *Value, info *Pub) error {
	if p := v.Get("promise"); p != nil {
		ns, _ := strField(p, "namespace")
		for i, k := range p.Keys {
			if p.Vals[i].Kind != derived {
				continue
			}
			switch k {
			case "namespace_version":
				n, _ := strconv.ParseUint(ns[:2], 16, 8)
				p.Vals[i] = num(int64(n))
			case "namespace_id":
				p.Vals[i] = str(ns[2:])
			default:
				return fmt.Errorf("slim: no recipe for promise.%s", k)
			}
		}
	}
	a := v.Get("assignment")
	if a == nil || a.Kind != Obj {
		return fillTop(v)
	}
	var set *valSet
	var rows [][]int
	var att []bool
	for i, k := range a.Keys {
		x := a.Vals[i]
		if k != "validators" || x.Kind != derived || x.vt == nil {
			continue
		}
		// the rows were derived by the build that wrote the record: computed again only by one that assigns the same
		if err := pinOf(v); err != nil {
			return err
		}
		vt := x.vt
		set = t.sets[vt.set]
		c, okc := commitmentOf(v)
		pp, okp := protocolParams(v)
		if !okc || !okp {
			return errors.New("slim: a validator table without its commitment or params")
		}
		var ok bool
		if rows, ok = assignRows(set, c, pp); !ok {
			return errors.New("slim: the assignment failed on decoding")
		}
		hv := t.hosts[vt.hosts]
		if len(hv) != len(set.addr) {
			return errors.New("slim: a host vector of another length than its set")
		}
		att = make([]bool, len(set.addr))
		list := &Value{Kind: Arr}
		for j := range set.addr {
			att[j] = vt.attested[j/8]&(1<<(j%8)) != 0
			if e, ok := vt.exceptions[j]; ok {
				list.Vals = append(list.Vals, e)
				continue
			}
			list.Vals = append(list.Vals, t.expectedValidator(set, j, rows[j], vt.rowsStored, att[j], hv[j]))
		}
		a.Vals[i] = list
		info.assigned(set, rows)
	}
	if set != nil {
		sum := assignmentSummary(set, rows, att)
		for i, k := range a.Keys {
			if a.Vals[i].Kind == derived {
				n, ok := sum[k]
				if !ok {
					return fmt.Errorf("slim: no recipe for assignment.%s", k)
				}
				a.Vals[i] = num(n)
			}
		}
	}
	return fillTop(v)
}

// fillTop computes a decoded publication's derived top-level fields.
func fillTop(v *Value) error {
	for i, k := range v.Keys {
		if v.Vals[i].Kind != derived {
			continue
		}
		if k != "must_serve_until" {
			return fmt.Errorf("slim: no recipe for %s", k)
		}
		msu, ok := mustServeUntil(v)
		if !ok {
			return errors.New("slim: must_serve_until not computable")
		}
		v.Vals[i] = str(msu)
	}
	return nil
}
