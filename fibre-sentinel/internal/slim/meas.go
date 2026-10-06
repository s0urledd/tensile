package slim

import (
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

// derivable lists a reading's top-level fields that are computed again; their bytes count as an exception when the
// computation does not give them back.
var derivable = map[string]bool{"promise_hash": true, "commitment": true, "blob_version": true, "must_serve_until": true,
	"validator_set_height": true, "validator_address": true, "assigned": true, "assigned_row_count": true, "attested": true,
	"host_at_settlement": true, "lateness_ms": true}

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

// EncodeMeasurement writes one reading in slim form. Its publication must have been encoded first.
func (t *Tables) EncodeMeasurement(line []byte) ([]byte, Stats, error) {
	orig, err := Parse(line)
	if err != nil {
		return nil, Stats{}, err
	}
	v := clone(orig)
	st := Stats{Fields: map[string]int{}, ExcFields: map[string]int{}}
	e := &enc{t: t}
	pid, vidx := -1, -1
	var info *pubInfo
	if h, ok := strField(orig, "promise_hash"); ok {
		if id, ok := t.pubID[h]; ok {
			pid, info = id, t.pubs[id]
		}
	}
	if info != nil && info.set != nil {
		if a, ok := strField(orig, "validator_address"); ok {
			if i, ok := info.idx[a]; ok {
				vidx = i
			}
		}
	}
	e.w.u(uint64(pid + 1))
	e.w.u(uint64(vidx + 1))
	st.NoPublication = info == nil
	if info != nil {
		if ns, ok := timeNS(info.tree, "must_serve_until"); ok {
			e.base = ns
		}
		derive(v, "promise_hash", str(info.hash), e)
		for k, path := range fromPublication {
			derive(v, k, info.tree.Path(path...), e)
		}
		if vidx >= 0 {
			pv := info.validator(vidx)
			rows := info.rows[vidx]
			derive(v, "validator_address", str(info.set.hexes[vidx]), e)
			derive(v, "assigned", boolean(len(rows) > 0), e)
			derive(v, "assigned_row_count", num(int64(len(rows))), e)
			derive(v, "attested", pv.Get("attested"), e)
			derive(v, "host_at_settlement", pv.Get("host_at_settlement"), e)
			t.rowsSlim(v.Get("download"), info, vidx, e)
		}
	}
	if s, ok1 := timeNS(orig, "started_at"); ok1 {
		if c, ok2 := timeNS(orig, "scheduled_at"); ok2 {
			derive(v, "lateness_ms", num(time.Duration(s-c).Milliseconds()), e)
		}
	}

	e.w.tag(tObj)
	e.w.u(uint64(t.shape(v.Keys)))
	for i, x := range v.Vals {
		k := v.Keys[i]
		t.str(k)
		before := len(e.w.b)
		e.value(x)
		n := len(e.w.b) - before
		st.Fields[k] += n
		if info != nil && derivable[k] && x.Kind != derived {
			e.exc += n
			st.ExcFields[k] += n
		}
	}
	st.Bytes = len(e.w.b)
	st.ExcFields["row_indices"] = e.exc - sumOf(st.ExcFields)
	st.Exceptions = e.exc
	return e.w.b, st, nil
}

func sumOf(m map[string]int) (n int) {
	for _, v := range m {
		n += v
	}
	return
}

// rowsSlim replaces a reading's returned row list by what rebuilds it: nothing when it is the validator's own
// assignment in its order; its positions in that assignment when it is part of it or in another order; the shadowing
// promise when it is that promise's assignment of the validator; else the list itself. Each but the first is an
// exception.
func (t *Tables) rowsSlim(dl *Value, info *pubInfo, vidx int, e *enc) {
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
		own := info.rows[vidx]
		if Same(dl.Vals[i], rowList(own)) {
			dl.Vals[i] = &Value{Kind: derived}
		} else if sb, ok := strField(dl, "shadowed_by"); ok && t.shadowMatches(sb, info.set.hexes[vidx], got) {
			dl.Vals[i] = &Value{Kind: derived, rowsBy: rowsShadow, shadow: t.pubID[sb]}
		} else if pos, ok := positions(own, got); ok {
			dl.Vals[i] = &Value{Kind: derived, rowsBy: rowsOwnPositions, pos: pos}
		} else {
			// rows of no promise this store knows: the list itself, all of it an exception
			x := &enc{t: t}
			x.value(dl.Vals[i])
			e.exc += len(x.w.b)
		}
		// rows_returned is the list's length
		for j, k2 := range dl.Keys {
			if k2 == "rows_returned" && Same(dl.Vals[j], num(int64(len(got)))) {
				dl.Vals[j] = &Value{Kind: derived}
			}
		}
	}
}

func (t *Tables) shadowMatches(promise, addr string, got []int) bool {
	id, ok := t.pubID[promise]
	if !ok || t.pubs[id].set == nil {
		return false
	}
	o := t.pubs[id]
	j, ok := o.idx[addr]
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

// a derived value's own bytes: positions or the shadowing promise; counted as an exception
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
		e.w.tag(tShadow)
		e.w.u(uint64(v.shadow))
	}
	e.exc += len(e.w.b) - start
}

// DecodeMeasurement reads a slim reading back to its record line.
func (t *Tables) DecodeMeasurement(b []byte) ([]byte, error) {
	d := &dec{t: t, r: reader{b: b}}
	pid := int(d.r.u()) - 1
	vidx := int(d.r.u()) - 1
	var info *pubInfo
	if pid >= 0 {
		if pid >= len(t.pubs) {
			return nil, fmt.Errorf("slim: reading of an unknown publication")
		}
		info = t.pubs[pid]
		if ns, ok := timeNS(info.tree, "must_serve_until"); ok {
			d.base = ns
		}
	}
	v := d.value()
	if d.r.err != nil {
		return nil, d.r.err
	}
	if d.r.i != len(b) {
		return nil, fmt.Errorf("slim: %d bytes left after a reading", len(b)-d.r.i)
	}
	for i, k := range v.Keys {
		if v.Vals[i].Kind != derived {
			continue
		}
		switch k {
		case "promise_hash":
			v.Vals[i] = str(info.hash)
		case "commitment", "blob_version", "must_serve_until", "validator_set_height":
			v.Vals[i] = clone(info.tree.Path(fromPublication[k]...))
		case "validator_address":
			v.Vals[i] = str(info.set.hexes[vidx])
		case "assigned":
			v.Vals[i] = boolean(len(info.rows[vidx]) > 0)
		case "assigned_row_count":
			v.Vals[i] = num(int64(len(info.rows[vidx])))
		case "attested":
			v.Vals[i] = clone(info.validator(vidx).Get("attested"))
		case "host_at_settlement":
			v.Vals[i] = clone(info.validator(vidx).Get("host_at_settlement"))
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
			var rows []int
			switch x.rowsBy {
			case rowsNone:
				rows = info.rows[vidx]
			case rowsOwnPositions:
				own := info.rows[vidx]
				for _, p := range x.pos {
					if p >= len(own) {
						return nil, fmt.Errorf("slim: a row position past the assignment")
					}
					rows = append(rows, own[p])
				}
			case rowsShadow:
				o := t.pubs[x.shadow]
				rows = o.rows[o.idx[info.set.hexes[vidx]]]
			}
			dl.Vals[i] = rowList(rows)
		}
		// rows_returned is the returned list's length, however the list was kept
		for i, k := range dl.Keys {
			if k == "rows_returned" && dl.Vals[i].Kind == derived {
				ri := dl.Get("row_indices")
				if ri == nil || ri.Kind != Arr {
					return nil, fmt.Errorf("slim: rows_returned without its rows")
				}
				dl.Vals[i] = num(int64(len(ri.Vals)))
			}
		}
	}
	return Emit(v), nil
}
