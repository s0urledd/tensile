// Package slim is a prototype of a smaller record format for mainnet: every field a record carries is kept, except
// what can be computed again from other fields (the row assignment, which follows from the validator set and the
// commitment, and the copies of a publication's facts in each of its readings). A derivable field is dropped only
// when the computation gives back exactly what the record holds; when it does not, the record's own value is kept as
// an exception. Decoding computes the dropped fields again and writes the record back byte for byte.
//
// Not wired into anything: cmd/slim-measure runs it over exported records to test it and to measure it.
package slim

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
)

// Kind is the JSON type of a Value.
type Kind uint8

const (
	Null Kind = iota
	Bool
	Num
	Str
	Obj
	Arr
	derived // a field the decoder computes again; only in an encoder's working tree
)

// Value is one JSON value with its object keys in their written order, so a record can be written back exactly.
type Value struct {
	Kind Kind
	B    bool
	S    string // Num: the literal as written; Str: the string
	Keys []string
	Vals []*Value // Obj: one per key; Arr: the items

	// a derived field's recipe, when it needs one: a publication's validator table, or how a row list is rebuilt
	vt     *valTable
	rowsBy rowsKind
	pos    []int
	shadow string
}

// Parse reads one JSON document, keeping key order and number literals.
func Parse(b []byte) (*Value, error) {
	d := json.NewDecoder(bytes.NewReader(b))
	d.UseNumber()
	v, err := parseValue(d)
	if err != nil {
		return nil, err
	}
	if _, err := d.Token(); err != io.EOF {
		return nil, fmt.Errorf("trailing data after the document")
	}
	return v, nil
}

func parseValue(d *json.Decoder) (*Value, error) {
	t, err := d.Token()
	if err != nil {
		return nil, err
	}
	return parseFrom(d, t)
}

func parseFrom(d *json.Decoder, t json.Token) (*Value, error) {
	switch x := t.(type) {
	case nil:
		return &Value{Kind: Null}, nil
	case bool:
		return &Value{Kind: Bool, B: x}, nil
	case json.Number:
		return &Value{Kind: Num, S: string(x)}, nil
	case string:
		return &Value{Kind: Str, S: x}, nil
	case json.Delim:
		switch x {
		case '{':
			v := &Value{Kind: Obj}
			for d.More() {
				kt, err := d.Token()
				if err != nil {
					return nil, err
				}
				k, ok := kt.(string)
				if !ok {
					return nil, fmt.Errorf("object key is %T", kt)
				}
				val, err := parseValue(d)
				if err != nil {
					return nil, err
				}
				v.Keys = append(v.Keys, k)
				v.Vals = append(v.Vals, val)
			}
			_, err := d.Token() // '}'
			return v, err
		case '[':
			v := &Value{Kind: Arr}
			for d.More() {
				val, err := parseValue(d)
				if err != nil {
					return nil, err
				}
				v.Vals = append(v.Vals, val)
			}
			_, err := d.Token() // ']'
			return v, err
		}
	}
	return nil, fmt.Errorf("unexpected token %v", t)
}

// Emit writes v as encoding/json writes it: compact, strings escaped the same way (HTML characters included).
func Emit(v *Value) []byte {
	var buf bytes.Buffer
	emit(&buf, v)
	return buf.Bytes()
}

func emit(buf *bytes.Buffer, v *Value) {
	switch v.Kind {
	case Null:
		buf.WriteString("null")
	case Bool:
		if v.B {
			buf.WriteString("true")
		} else {
			buf.WriteString("false")
		}
	case Num:
		buf.WriteString(v.S)
	case Str:
		b, _ := json.Marshal(v.S)
		buf.Write(b)
	case Obj:
		buf.WriteByte('{')
		for i, k := range v.Keys {
			if i > 0 {
				buf.WriteByte(',')
			}
			b, _ := json.Marshal(k)
			buf.Write(b)
			buf.WriteByte(':')
			emit(buf, v.Vals[i])
		}
		buf.WriteByte('}')
	case Arr:
		buf.WriteByte('[')
		for i, x := range v.Vals {
			if i > 0 {
				buf.WriteByte(',')
			}
			emit(buf, x)
		}
		buf.WriteByte(']')
	default:
		panic("slim: emit of an underived field")
	}
}

// Get returns the value at a key of an object, or nil.
func (v *Value) Get(k string) *Value {
	if v == nil || v.Kind != Obj {
		return nil
	}
	for i, x := range v.Keys {
		if x == k {
			return v.Vals[i]
		}
	}
	return nil
}

// Path walks keys from v; nil when one is missing.
func (v *Value) Path(keys ...string) *Value {
	for _, k := range keys {
		v = v.Get(k)
	}
	return v
}

// Same reports whether two values write out identically.
func Same(a, b *Value) bool {
	if a == nil || b == nil {
		return a == b
	}
	return bytes.Equal(Emit(a), Emit(b))
}

func clone(v *Value) *Value {
	if v == nil {
		return nil
	}
	c := *v
	c.Keys = append([]string(nil), v.Keys...)
	c.Vals = make([]*Value, len(v.Vals))
	for i, x := range v.Vals {
		c.Vals[i] = clone(x)
	}
	return &c
}

func num(n int64) *Value  { return &Value{Kind: Num, S: fmt.Sprint(n)} }
func str(s string) *Value { return &Value{Kind: Str, S: s} }
func boolean(b bool) *Value {
	return &Value{Kind: Bool, B: b}
}
