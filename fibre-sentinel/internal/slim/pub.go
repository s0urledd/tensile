package slim

import (
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"time"

	assign "github.com/plsgiveup/fibre/fibre-assign"
)

// valSet is a validator set as a publication's assignment lists it: addresses and voting powers, voting power
// descending then address ascending (the scanner's canonical order).
type valSet struct {
	addr  [][20]byte
	power []int64
	hexes []string
}

func (s *valSet) key() string {
	var b strings.Builder
	for i := range s.addr {
		b.WriteString(s.hexes[i])
		b.WriteString(strconv.FormatInt(s.power[i], 10))
	}
	return b.String()
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

// pubInfo is what a publication's readings repeat from it, kept as the publication is encoded or decoded.
type pubInfo struct {
	hash string
	tree *Value
	set  *valSet
	idx  map[string]int // validator address (hex) → its place in set
	rows [][]int        // each validator's assigned rows, as fibre-assign computes them
}

func (p *pubInfo) validator(i int) *Value {
	vs := p.tree.Path("assignment", "validators")
	if vs == nil || i >= len(vs.Vals) {
		return nil
	}
	return vs.Vals[i]
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
	add("address", str(set.hexes[i]))
	add("voting_power", num(set.power[i]))
	add("row_count", num(int64(len(rows))))
	if rowsStored && len(rows) > 0 {
		arr := &Value{Kind: Arr}
		for _, r := range rows {
			arr.Vals = append(arr.Vals, num(int64(r)))
		}
		add("rows", arr)
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
		s.hexes = append(s.hexes, a.S)
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
	var b strings.Builder
	for _, h := range hv {
		fmt.Fprintf(&b, "%d,%d;", h.host, h.source)
	}
	k := b.String()
	if id, ok := t.hostID[k]; ok {
		return id
	}
	t.hosts = append(t.hosts, hv)
	t.hostID[k] = len(t.hosts) - 1
	return len(t.hosts) - 1
}

func strField(v *Value, k string) (string, bool) {
	x := v.Get(k)
	if x == nil {
		return "", false
	}
	if x.Kind != Str {
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
func derive(v *Value, key string, want *Value, e *enc) bool {
	if v == nil || v.Kind != Obj {
		return false
	}
	for i, k := range v.Keys {
		if k != key {
			continue
		}
		if want != nil && Same(v.Vals[i], want) {
			v.Vals[i] = &Value{Kind: derived}
			return true
		}
		return false
	}
	return false
}

// EncodePublication writes a publication in slim form and records what its readings will take from it.
func (t *Tables) EncodePublication(line []byte) ([]byte, Stats, error) {
	orig, err := Parse(line)
	if err != nil {
		return nil, Stats{}, err
	}
	v := clone(orig)
	st := Stats{Fields: map[string]int{}}
	info := &pubInfo{tree: orig, idx: map[string]int{}}
	if h, ok := strField(orig, "promise_hash"); ok {
		info.hash = h
	}
	e := &enc{t: t}

	// the namespace's version and id are its first byte and the rest
	if p := v.Get("promise"); p != nil {
		if ns, ok := strField(p, "namespace"); ok && len(ns) >= 2 {
			if n, err := strconv.ParseUint(ns[:2], 16, 8); err == nil {
				derive(p, "namespace_version", num(int64(n)), e)
			}
			derive(p, "namespace_id", str(ns[2:]), e)
		}
	}
	if msu, ok := mustServeUntil(orig); ok {
		derive(v, "must_serve_until", str(msu), e)
	}

	// the validator list: the set, the hosts, one bit each for attested, and only the entries that differ in full
	a := v.Get("assignment")
	vs := a.Get("validators")
	set := setFrom(vs)
	c, okc := commitmentOf(orig)
	pp, okp := protocolParams(orig)
	var rows [][]int
	okr := false
	if set != nil && okc && okp && a.Get("error") == nil {
		rows, okr = assignRows(set, c, pp)
	}
	if okr {
		info.set, info.rows = set, rows
		for i, h := range set.hexes {
			info.idx[h] = i
		}
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
			derive(a, k, num(n), e)
		}
	}

	// write it, counting each top-level field's bytes
	e.w.tag(tObj)
	e.w.u(uint64(t.shape(v.Keys)))
	for i, x := range v.Vals {
		t.str(v.Keys[i])
		before := len(e.w.b)
		if x.Kind == derived && x.vt != nil {
			e.valTable(x.vt)
		} else if v.Keys[i] == "assignment" {
			e.w.tag(tObj)
			e.w.u(uint64(t.shape(x.Keys)))
			for j, y := range x.Vals {
				t.str(x.Keys[j])
				if y.Kind == derived && y.vt != nil {
					e.valTable(y.vt)
				} else {
					e.value(y)
				}
			}
		} else {
			e.value(x)
		}
		st.Fields[v.Keys[i]] += len(e.w.b) - before
	}
	info.hash, _ = strField(orig, "promise_hash")
	t.pubs = append(t.pubs, info)
	t.pubID[info.hash] = len(t.pubs) - 1
	st.Bytes = len(e.w.b)
	st.Exceptions = e.exc
	return e.w.b, st, nil
}

func (e *enc) valTable(vt *valTable) {
	start := len(e.w.b)
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
			e.value(x)
		}
	}
	e.exc += len(e.w.b) - excStart
	_ = start
}

// DecodePublication reads a slim publication back to its record line.
func (t *Tables) DecodePublication(b []byte) ([]byte, error) {
	d := &dec{t: t, r: reader{b: b}}
	if d.r.tag() != tObj {
		return nil, errTag
	}
	sid := d.r.u()
	if d.r.err != nil || sid >= uint64(len(t.shapes)) {
		return nil, errTag
	}
	v := &Value{Kind: Obj, Keys: append([]string(nil), t.shapes[sid]...)}
	for _, k := range v.Keys {
		if k == "assignment" {
			v.Vals = append(v.Vals, d.assignment())
		} else {
			v.Vals = append(v.Vals, d.value())
		}
	}
	if d.r.err != nil {
		return nil, d.r.err
	}
	info := &pubInfo{tree: v, idx: map[string]int{}}
	if err := t.fillPublication(v, info); err != nil {
		return nil, err
	}
	info.hash, _ = strField(v, "promise_hash")
	t.pubs = append(t.pubs, info)
	t.pubID[info.hash] = len(t.pubs) - 1
	return Emit(v), nil
}

func (d *dec) assignment() *Value {
	if d.r.tag() != tObj {
		d.r.err = errTag
		return &Value{}
	}
	sid := d.r.u()
	if d.r.err != nil || sid >= uint64(len(d.t.shapes)) {
		d.r.err = errTag
		return &Value{}
	}
	v := &Value{Kind: Obj, Keys: append([]string(nil), d.t.shapes[sid]...)}
	for range v.Keys {
		// a value table is tagged; read it here, everything else as any value
		if d.r.i < len(d.r.b) && d.r.b[d.r.i] == tValTable {
			d.r.i++
			vt := &valTable{exceptions: map[int]*Value{}}
			vt.set = int(d.r.u())
			vt.hosts = int(d.r.u())
			vt.rowsStored = d.r.u() == 1
			if vt.set >= len(d.t.sets) || vt.hosts >= len(d.t.hosts) {
				d.r.err = errTag
				return v
			}
			n := len(d.t.sets[vt.set].addr)
			nb := (n + 7) / 8
			if d.r.i+nb > len(d.r.b) {
				d.r.err = errShort
				return v
			}
			vt.attested = d.r.b[d.r.i : d.r.i+nb]
			d.r.i += nb
			ne := int(d.r.u())
			for j := 0; j < ne && d.r.err == nil; j++ {
				i := int(d.r.u())
				vt.exceptions[i] = d.value()
			}
			v.Vals = append(v.Vals, &Value{Kind: derived, vt: vt})
			continue
		}
		v.Vals = append(v.Vals, d.value())
	}
	return v
}

// fillPublication computes every derived field of a decoded publication.
func (t *Tables) fillPublication(v *Value, info *pubInfo) error {
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
			}
		}
	}
	a := v.Get("assignment")
	var set *valSet
	var rows [][]int
	var att []bool
	for i, k := range a.Keys {
		x := a.Vals[i]
		if k != "validators" || x.Kind != derived || x.vt == nil {
			continue
		}
		vt := x.vt
		set = t.sets[vt.set]
		c, okc := commitmentOf(v)
		pp, okp := protocolParams(v)
		if !okc || !okp {
			return fmt.Errorf("slim: a validator table without its commitment or params")
		}
		var ok bool
		if rows, ok = assignRows(set, c, pp); !ok {
			return fmt.Errorf("slim: assignment failed on decoding")
		}
		hv := t.hosts[vt.hosts]
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
		info.set, info.rows = set, rows
		for j, h := range set.hexes {
			info.idx[h] = j
		}
	}
	if set != nil {
		sum := assignmentSummary(set, rows, att)
		for i, k := range a.Keys {
			if a.Vals[i].Kind == derived {
				a.Vals[i] = num(sum[k])
			}
		}
	}
	for i, k := range v.Keys {
		if v.Vals[i].Kind == derived && k == "must_serve_until" {
			msu, ok := mustServeUntil(v)
			if !ok {
				return fmt.Errorf("slim: must_serve_until not computable")
			}
			v.Vals[i] = str(msu)
		}
	}
	return nil
}

// Stats is one record's encoded size, by top-level field, and the part of it spent on exceptions.
type Stats struct {
	Bytes      int
	Exceptions int
	Fields     map[string]int
	// NoPublication: a reading whose publication the store does not hold (one settled before its first day), so
	// nothing of it could be derived
	NoPublication bool
	// ExcFields: the exception bytes by field
	ExcFields map[string]int
}
