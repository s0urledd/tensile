package recordcheck

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/probe"
)

// Another vantage's heartbeats, exported as vantages/de-1/reachability.jsonl, are checked as reachability lines by
// the member's base name: each line is found under its own vantage's key and written back byte for byte, and the
// member is reproducible from the store.
func TestVantageMemberIsCheckedAgainstTheStore(t *testing.T) {
	f := newFixture(t)
	var lines [][]byte
	for _, own := range f.reachLines {
		var m probe.Measurement
		if err := json.Unmarshal(own, &m); err != nil {
			t.Fatal(err)
		}
		m.Vantage = "de-1"
		l, _ := json.Marshal(m)
		if ok, err := f.st.InsertReachability(m, l); err != nil || !ok {
			t.Fatalf("reachability from de-1: %v %v", ok, err)
		}
		lines = append(lines, l)
	}
	const member = "vantages/de-1/reachability.jsonl"
	if !Checked(member) {
		t.Fatalf("%s is not checked", member)
	}
	data := t.TempDir()
	writeFiles(t, data, map[string][]byte{member: file(lines...)})
	dir, e := exportDay(t, data, time.Date(2026, 10, 6, 4, 0, 0, 0, time.UTC))
	r, err := CheckDay(context.Background(), f.st, dir, e)
	if err != nil {
		t.Fatal(err)
	}
	var got *FileReport
	for i := range r.Files {
		if r.Files[i].Name == member {
			got = &r.Files[i]
		}
	}
	if got == nil {
		t.Fatalf("the export's %s was not checked: %+v", member, r)
	}
	if !got.Reproducible || got.Identical != int64(len(lines)) || got.RebuiltSHA != digest(file(lines...)) {
		t.Fatalf("%s: %+v", member, *got)
	}
	if !r.ExportIntact || !r.Reproducible {
		t.Fatalf("day: %+v", r)
	}
	// The key carries the vantage: the same checks said to come from a vantage the store never heard from are not
	// found under another vantage's rows.
	var stray [][]byte
	for _, l := range lines {
		stray = append(stray, bytes.Replace(l, []byte(`"vantage":"de-1"`), []byte(`"vantage":"fr-2"`), 1))
	}
	r2, err := CheckLines(context.Background(), f.st, "vantages/fr-2/reachability.jsonl", bytes.NewReader(file(stray...)), 1)
	if err != nil || r2.Missing != int64(len(stray)) || r2.Reproducible {
		t.Fatalf("fr-2: %+v %v", r2, err)
	}
}
