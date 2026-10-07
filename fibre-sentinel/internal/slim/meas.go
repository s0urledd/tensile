package slim

import (
	"encoding/hex"
	"errors"
	"fmt"
	"time"
)

// What a reading repeats from its publication, by key: the reading's own copy is dropped when the publication gives
// back exactly the same.
var fromPublication = map[string][]string{
	"commitment":           {"promise", "commitment"},
	"blob_version":         {"promise", "blob_version"},
	"must_serve_until":     {"must_serve_until"},
	"validator_set_height": {"assignment", "validator_set_height"},
}

func timeNS(v *Value, k string) (int64, bool) {
	s, ok := strField(v, k)
	if !ok {
		return 0, false
	}
	return asTime(s)
}

func rowList(rows []int) *Value {
	a := &Value{Kind: Arr}
	for _, r := range rows {
		a.Vals = append(a.Vals, num(int64(r)))
	}
	return a
}

func intsOf(v *Value) ([]int, bool) {
	if v == nil || v.Kind != Arr {
		return nil, false
	}
	out := make([]int, len(v.Vals))
	for i, x := range v.Vals {
		if x.Kind != Num {
			return nil, false
		}
		n, ok := asInt(x.S)
		if !ok {
			return nil, false
		}
		out[i] = int(n)
	}
	return out, true
}

// Lookup finds the publication a promise hash names, for a reading whose rows are another promise's (shadowing):
// nil when the store does not hold it.
type Lookup func(promiseHash string) *Pub

// EncodeMeasurement writes one reading record in slim form against its publication (nil when the store does not hold
// it: then nothing is derived). The bytes it adds to the tables are in Pending.
func (t *Tables) EncodeMeasurement(line []byte, pub *Pub, lookup Lookup) ([]byte, error) {
	orig, err := Parse(line)
	if err != nil {
		return nil, err
	}
	if orig.Kind != Obj {
		return nil, errors.New("slim: a reading record is not an object")
	}
	// the publication of a shadowing promise, looked up before the tables are locked (a lookup may decode it)
	var shadowPub *Pub
	if sb, ok := strField(orig.Get("download"), "shadowed_by"); ok && lookup != nil {
		shadowPub = lookup(sb)
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	v := clone(orig)
	e := &enc{t: t}
	vidx := -1
	if pub != nil {
		if h, _ := strField(orig, "promise_hash"); h != pub.Hash {
			pub = nil // another publication's reading: nothing of this one applies
		}
	}
	// the validator's place in the publication's set, and with it everything the set and its rows give, only when the
	// publication's rows were derived (its pin is one this build reproduces)
	if pub != nil && pub.set != nil {
		if a, ok := strField(orig, "validator_address"); ok {
			if i, ok := pub.index(a); ok {
				vidx = i
			}
		}
	}
	if pub != nil {
		e.w.u(1)
	} else {
		e.w.u(0)
	}
	e.w.u(uint64(vidx + 1))
	if pub != nil {
		if ns, ok := timeNS(pub.tree, "must_serve_until"); ok {
			e.base = ns
		}
		derive(v, "promise_hash", str(pub.Hash))
		for k, path := range fromPublication {
			derive(v, k, pub.tree.Path(path...))
		}
		if vidx >= 0 {
			pv := pub.validator(vidx)
			rows := pub.rows[vidx]
			derive(v, "validator_address", str(pub.set.hex(vidx)))
			derive(v, "assigned", boolean(len(rows) > 0))
			derive(v, "assigned_row_count", num(int64(len(rows))))
			derive(v, "attested", pv.Get("attested"))
			derive(v, "host_at_settlement", pv.Get("host_at_settlement"))
			rowsSlim(v.Get("download"), pub, vidx, shadowPub)
		}
	}
	if s, ok1 := timeNS(orig, "started_at"); ok1 {
		if c, ok2 := timeNS(orig, "scheduled_at"); ok2 {
			derive(v, "lateness_ms", num(time.Duration(s-c).Milliseconds()))
		}
	}
	e.value("", v)
	return e.w.b, nil
}

// rowsSlim replaces a reading's returned row list by what rebuilds it: nothing when it is the validator's own
// assignment in its order; its positions in that assignment when it is part of it or in another order; the shadowing
// promise when it is that promise's assignment of the validator; else the list itself.
func rowsSlim(dl *Value, pub *Pub, vidx int, shadowPub *Pub) {
	if dl == nil || dl.Kind != Obj {
		return
	}
	for i, k := range dl.Keys {
		if k != "row_indices" {
			continue
		}
		got, ok := intsOf(dl.Vals[i])
		if !ok {
			return
		}
		own := pub.rows[vidx]
		if Same(dl.Vals[i], rowList(own)) {
			dl.Vals[i] = &Value{Kind: derived}
		} else if sb, ok := strField(dl, "shadowed_by"); ok && shadowPub != nil && shadowPub.Hash == sb && shadowMatches(shadowPub, pub.set.hex(vidx), got) {
			dl.Vals[i] = &Value{Kind: derived, rowsBy: rowsShadow, shadow: sb}
		} else if pos, ok := positions(own, got); ok {
			dl.Vals[i] = &Value{Kind: derived, rowsBy: rowsOwnPositions, pos: pos}
		}
		for j, k2 := range dl.Keys {
			if k2 == "rows_returned" && Same(dl.Vals[j], num(int64(len(got)))) {
				dl.Vals[j] = &Value{Kind: derived}
			}
		}
	}
}

func shadowMatches(o *Pub, addr string, got []int) bool {
	if o == nil || o.set == nil {
		return false
	}
	j, ok := o.index(addr)
	if !ok {
		return false
	}
	return Same(rowList(o.rows[j]), rowList(got))
}

// positions gives each returned row's place in own, when every one of them is there.
func positions(own, got []int) ([]int, bool) {
	at := make(map[int]int, len(own))
	for i, r := range own {
		if _, seen := at[r]; !seen {
			at[r] = i
		}
	}
	out := make([]int, len(got))
	for i, r := range got {
		p, ok := at[r]
		if !ok {
			return nil, false
		}
		out[i] = p
	}
	return out, true
}

func (e *enc) rowsValue(v *Value) {
	start := len(e.w.b)
	switch v.rowsBy {
	case rowsOwnPositions:
		e.w.tag(tAsgPos)
		e.w.u(uint64(len(v.pos)))
		for _, p := range v.pos {
			e.w.u(uint64(p))
		}
	case rowsShadow:
		b, _ := hex.DecodeString(v.shadow)
		e.w.tag(tShadow)
		e.w.raw(b)
	}
	e.exc += len(e.w.b) - start
}

// DecodeMeasurement reads a slim reading back to its record line, against the publication it was encoded against.
func (t *Tables) DecodeMeasurement(b []byte, pub *Pub, lookup Lookup) ([]byte, error) {
	v, pub, vidx, err := t.readMeasurement(b, pub)
	if err != nil {
		return nil, err
	}
	return fillMeasurement(v, pub, vidx, lookup)
}

// readMeasurement reads a slim reading's values, its derived fields still marked; under the tables' lock.
func (t *Tables) readMeasurement(b []byte, pub *Pub) (*Value, *Pub, int, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	d := &dec{t: t, r: reader{b: b}}
	withPub := d.r.u() == 1
	vidx := int(d.r.u()) - 1
	if d.r.err != nil {
		return nil, nil, 0, d.r.err
	}
	if withPub && pub == nil {
		return nil, nil, 0, errors.New("slim: a reading encoded against a publication, decoded without it")
	}
	if !withPub {
		pub = nil
	}
	if vidx >= 0 && pub.PinErr() != nil {
		// encoded against the publication's rows by a build that reproduced its pin; this one does not
		return nil, nil, 0, pub.PinErr()
	}
	if vidx >= 0 && (pub == nil || pub.set == nil || vidx >= len(pub.set.addr)) {
		return nil, nil, 0, errors.New("slim: a reading's validator past its publication's set")
	}
	if pub != nil {
		if ns, ok := timeNS(pub.tree, "must_serve_until"); ok {
			d.base = ns
		}
	}
	v := d.value()
	if d.r.err != nil {
		return nil, nil, 0, d.r.err
	}
	if d.r.i != len(b) || v.Kind != Obj {
		return nil, nil, 0, errors.New("slim: not a reading record")
	}
	return v, pub, vidx, nil
}

// fillMeasurement computes a read reading's derived fields and writes it out.
func fillMeasurement(v *Value, pub *Pub, vidx int, lookup Lookup) ([]byte, error) {
	for i, k := range v.Keys {
		if v.Vals[i].Kind != derived {
			continue
		}
		if pub == nil && k != "lateness_ms" {
			return nil, fmt.Errorf("slim: %s derived without a publication", k)
		}
		needVal := k == "validator_address" || k == "assigned" || k == "assigned_row_count" || k == "attested" || k == "host_at_settlement"
		if needVal && vidx < 0 {
			return nil, fmt.Errorf("slim: %s derived without a validator", k)
		}
		switch k {
		case "promise_hash":
			v.Vals[i] = str(pub.Hash)
		case "commitment", "blob_version", "must_serve_until", "validator_set_height":
			v.Vals[i] = clone(pub.tree.Path(fromPublication[k]...))
		case "validator_address":
			v.Vals[i] = str(pub.set.hex(vidx))
		case "assigned":
			v.Vals[i] = boolean(len(pub.rows[vidx]) > 0)
		case "assigned_row_count":
			v.Vals[i] = num(int64(len(pub.rows[vidx])))
		case "attested":
			v.Vals[i] = clone(pub.validator(vidx).Get("attested"))
		case "host_at_settlement":
			v.Vals[i] = clone(pub.validator(vidx).Get("host_at_settlement"))
		case "lateness_ms":
			s, _ := timeNS(v, "started_at")
			c, _ := timeNS(v, "scheduled_at")
			v.Vals[i] = num(time.Duration(s - c).Milliseconds())
		default:
			return nil, fmt.Errorf("slim: no recipe for %s", k)
		}
	}
	if dl := v.Get("download"); dl != nil && dl.Kind == Obj {
		for i, k := range dl.Keys {
			x := dl.Vals[i]
			if k != "row_indices" || x.Kind != derived {
				continue
			}
			if vidx < 0 {
				return nil, errors.New("slim: rows derived without a validator")
			}
			var rows []int
			switch x.rowsBy {
			case rowsNone:
				rows = pub.rows[vidx]
			case rowsOwnPositions:
				own := pub.rows[vidx]
				for _, p := range x.pos {
					if p >= len(own) {
						return nil, errors.New("slim: a row position past the assignment")
					}
					rows = append(rows, own[p])
				}
			case rowsShadow:
				var o *Pub
				if lookup != nil {
					o = lookup(x.shadow)
				}
				if err := o.PinErr(); err != nil {
					return nil, fmt.Errorf("slim: the shadowing promise %s: %w", x.shadow, err)
				}
				r, ok := o.Rows(pub.set.hex(vidx))
				if !ok {
					return nil, fmt.Errorf("slim: the shadowing promise %s is not on record", x.shadow)
				}
				rows = r
			}
			dl.Vals[i] = rowList(rows)
		}
		for i, k := range dl.Keys {
			if k == "rows_returned" && dl.Vals[i].Kind == derived {
				ri := dl.Get("row_indices")
				if ri == nil || ri.Kind != Arr {
					return nil, errors.New("slim: rows_returned without its rows")
				}
				dl.Vals[i] = num(int64(len(ri.Vals)))
			}
		}
	}
	return Emit(v), nil
}
