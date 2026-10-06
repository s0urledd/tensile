package slim

import (
	"bytes"
	"errors"
)

// reachVersion is the first value of an endpoint check record in slim form.
const reachVersion = 1

// EncodeReachability writes one endpoint check record (a line of reachability.jsonl) in slim form. Nothing is
// derived: every field is kept, each value as the format writes it (a key list by its shape, a repeated string by its
// dictionary entry, a time as nanoseconds from the record's previous one). The record is returned only when it reads
// back to the line byte for byte; otherwise the error says so and the caller keeps the line. The bytes it adds to the
// tables are in Pending.
func (t *Tables) EncodeReachability(line []byte) ([]byte, error) {
	v, err := Parse(line)
	if err != nil {
		return nil, err
	}
	if v.Kind != Obj {
		return nil, errors.New("slim: an endpoint check record is not an object")
	}
	t.mu.Lock()
	e := &enc{t: t}
	e.w.u(reachVersion)
	e.value("", v)
	b := e.w.b
	back, err := t.readReachability(b)
	t.mu.Unlock()
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(back, line) {
		return nil, errors.New("slim: an endpoint check record that does not read back to its line")
	}
	return b, nil
}

// DecodeReachability reads an endpoint check record in slim form back to its line.
func (t *Tables) DecodeReachability(b []byte) ([]byte, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.readReachability(b)
}

// readReachability decodes under the tables' lock.
func (t *Tables) readReachability(b []byte) ([]byte, error) {
	d := &dec{t: t, r: reader{b: b}}
	if v := d.r.u(); d.r.err == nil && v != reachVersion {
		return nil, errors.New("slim: an endpoint check record of an unknown version")
	}
	v := d.value()
	if d.r.err != nil {
		return nil, d.r.err
	}
	if len(d.r.b) != d.r.i {
		return nil, errors.New("slim: bytes after an endpoint check record")
	}
	return Emit(v), nil
}
