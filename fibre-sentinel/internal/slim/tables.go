package slim

import "fmt"

// MarshalTables writes the shared tables as a store would keep them: the bytes Size counts.
func (t *Tables) MarshalTables() []byte {
	var w writer
	w.u(uint64(len(t.strs)))
	for _, s := range t.strs {
		w.raw([]byte(s))
	}
	w.u(uint64(len(t.shapes)))
	for _, sh := range t.shapes {
		w.u(uint64(len(sh)))
		for _, k := range sh {
			w.u(uint64(t.strID[k]))
		}
	}
	w.u(uint64(len(t.sets)))
	for _, s := range t.sets {
		w.u(uint64(len(s.addr)))
		for i := range s.addr {
			w.b = append(w.b, s.addr[i][:]...)
			w.s(s.power[i])
		}
	}
	w.u(uint64(len(t.hosts)))
	for _, hv := range t.hosts {
		w.u(uint64(len(hv)))
		for _, h := range hv {
			w.u(uint64(h.host))
			w.u(uint64(h.source))
		}
	}
	return w.b
}

// LoadTables reads tables written by MarshalTables, with no publication known yet: a decoder learns each one as it
// decodes it, as a reader of the store would.
func LoadTables(b []byte) (*Tables, error) {
	t := NewTables()
	r := reader{b: b}
	n := int(r.u())
	for i := 0; i < n && r.err == nil; i++ {
		t.str(string(r.raw()))
	}
	n = int(r.u())
	for i := 0; i < n && r.err == nil; i++ {
		m := int(r.u())
		keys := make([]string, m)
		for j := range keys {
			id := int(r.u())
			if id >= len(t.strs) {
				return nil, fmt.Errorf("slim: a shape key past the dictionary")
			}
			keys[j] = t.strs[id]
		}
		t.shape(keys)
	}
	n = int(r.u())
	for i := 0; i < n && r.err == nil; i++ {
		m := int(r.u())
		s := &valSet{}
		for j := 0; j < m && r.err == nil; j++ {
			if r.i+20 > len(r.b) {
				return nil, errShort
			}
			var a [20]byte
			copy(a[:], r.b[r.i:r.i+20])
			r.i += 20
			s.addr = append(s.addr, a)
			s.power = append(s.power, r.s())
			s.hexes = append(s.hexes, fmt.Sprintf("%x", a))
		}
		t.setEntry(s)
	}
	n = int(r.u())
	for i := 0; i < n && r.err == nil; i++ {
		m := int(r.u())
		hv := make([]hostEntry, m)
		for j := range hv {
			hv[j].host = int(r.u())
			hv[j].source = int(r.u())
		}
		t.hostEntry(hv)
	}
	if r.err != nil {
		return nil, r.err
	}
	if r.i != len(b) {
		return nil, fmt.Errorf("slim: %d bytes left after the tables", len(b)-r.i)
	}
	return t, nil
}

// CountStrings adds a record's strings to Repeats, for the dictionary choice.
func (t *Tables) CountStrings(line []byte) error {
	v, err := Parse(line)
	if err != nil {
		return err
	}
	var walk func(*Value)
	walk = func(v *Value) {
		switch v.Kind {
		case Str:
			t.Repeats[v.S]++
		case Obj, Arr:
			for _, x := range v.Vals {
				walk(x)
			}
		}
	}
	walk(v)
	return nil
}
