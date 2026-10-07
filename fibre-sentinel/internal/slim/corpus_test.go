package slim

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	assign "github.com/plsgiveup/fibre/fibre-assign"
)

var updateCorpus = flag.Bool("update-corpus", false, "write testdata/corpus-slim.json.gz again from the corpus lines")

// The corpus is real Mocha records, frozen with their slim form: 24 publications (2026-09-25 to 2026-10-05, 80 to
// 84 validators, 256 KiB to 128 MiB blobs; seven from the 8,472-blob burst of 2026-09-28) and 49 of their readings,
// one of every shape and outcome the record holds (budget NOT_PROBED readings of the sampled-out days 2026-09-25/26
// among them). Their rows were computed by the scanner under celestia-app v10.2.0-mocha, so the slim form decodes to
// them only while the compiled fibre-assign assigns exactly as that build did.
const (
	corpusPubs = "testdata/corpus-publications.jsonl.gz"
	corpusMeas = "testdata/corpus-measurements.jsonl.gz"
	corpusSlim = "testdata/corpus-slim.json.gz"
)

// corpusForm is the corpus in its slim form: the table entries, then each record as encoded in that order.
type corpusForm struct {
	Entries      []corpusEntry `json:"entries"`
	Publications [][]byte      `json:"publications"`
	Measurements [][]byte      `json:"measurements"`
}

type corpusEntry struct {
	Kind int    `json:"kind"`
	ID   int    `json:"id"`
	Body []byte `json:"body"`
}

func gunzipLines(t *testing.T, path string) [][]byte {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	z, err := gzip.NewReader(f)
	if err != nil {
		t.Fatal(err)
	}
	var out [][]byte
	sc := bufio.NewScanner(z)
	sc.Buffer(nil, 64<<20)
	for sc.Scan() {
		out = append(out, append([]byte(nil), sc.Bytes()...))
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

func promiseHashOf(t *testing.T, line []byte) string {
	t.Helper()
	var x struct {
		PromiseHash string `json:"promise_hash"`
	}
	if err := json.Unmarshal(line, &x); err != nil || x.PromiseHash == "" {
		t.Fatalf("a corpus line without its promise hash: %v", err)
	}
	return x.PromiseHash
}

// sameRowsAsRecord fails unless the compiled fibre-assign gives every validator of corpus publication i the rows the
// scanner wrote into the record. It reads only the corpus lines, never the frozen form, so -update-corpus cannot
// write a changed assignment into the frozen form: under one, the encoder would keep each validator's rows as an
// exception, the new frozen form would read back, and the slim records every store holds would still decode to rows
// the record never had.
func sameRowsAsRecord(t *testing.T, i int, line []byte, p *Pub) {
	t.Helper()
	v, err := Parse(line)
	if err != nil {
		t.Fatal(err)
	}
	vs := v.Path("assignment", "validators")
	if vs == nil || len(vs.Vals) != len(p.rows) {
		t.Fatalf("publication %d (%s): %d assigned validators, the record lists another number", i, p.Hash, len(p.rows))
	}
	for j, x := range vs.Vals {
		rows := x.Get("rows")
		if rows == nil {
			t.Fatalf("publication %d (%s): validator %d has no rows in the record, so the corpus would not test them", i, p.Hash, j)
		}
		if !Same(rows, rowList(p.rows[j])) {
			t.Errorf("publication %d (%s): fibre-assign under %s gives validator %d other rows than the record's", i, p.Hash, assign.PinnedCelestiaAppCommit, j)
		}
	}
}

// encodeCorpus writes the corpus slim into fresh tables, as a store that ingests it in its order does.
func encodeCorpus(t *testing.T, pubs, meas [][]byte) corpusForm {
	t.Helper()
	w := NewTables()
	var out corpusForm
	byHash := map[string]*Pub{}
	for i, l := range pubs {
		b, p, err := w.EncodePublication(l)
		if err != nil {
			t.Fatalf("publication %d: %v", i, err)
		}
		if p.set == nil {
			t.Fatalf("publication %d (%s): its rows are not derived (%v), so the corpus would not test the assignment", i, p.Hash, p.PinErr())
		}
		sameRowsAsRecord(t, i, l, p)
		byHash[p.Hash] = p
		out.Publications = append(out.Publications, b)
	}
	lookup := func(h string) *Pub { return byHash[h] }
	for i, l := range meas {
		p := byHash[promiseHashOf(t, l)]
		if p == nil {
			t.Fatalf("reading %d: its publication is not in the corpus", i)
		}
		b, err := w.EncodeMeasurement(l, p, lookup)
		if err != nil {
			t.Fatalf("reading %d: %v", i, err)
		}
		out.Measurements = append(out.Measurements, b)
	}
	for _, e := range w.Pending() {
		out.Entries = append(out.Entries, corpusEntry{e.Kind, e.ID, e.Body})
	}
	return out
}

func readCorpusForm(t *testing.T) corpusForm {
	t.Helper()
	f, err := os.Open(corpusSlim)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	z, err := gzip.NewReader(f)
	if err != nil {
		t.Fatal(err)
	}
	b, err := io.ReadAll(z)
	if err != nil {
		t.Fatal(err)
	}
	var c corpusForm
	if err := json.Unmarshal(b, &c); err != nil {
		t.Fatal(err)
	}
	return c
}

func writeCorpusForm(t *testing.T, c corpusForm) {
	t.Helper()
	b, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	z, _ := gzip.NewWriterLevel(&buf, gzip.BestCompression)
	z.Write(b)
	if err := z.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.FromSlash(corpusSlim), buf.Bytes(), 0o644); err != nil {
		t.Fatal(err)
	}
}

// The corpus encodes to the frozen bytes and the frozen bytes decode to the corpus lines, in a fresh process's tables.
// A change to fibre-assign's output for any of these records (or to the slim format) fails here: the stored slim
// records of every store would decode to other rows than the record holds, and the past would change.
func TestTheFrozenCorpusReadsBackUnchanged(t *testing.T) {
	pubs, meas := gunzipLines(t, corpusPubs), gunzipLines(t, corpusMeas)
	got := encodeCorpus(t, pubs, meas)
	if *updateCorpus {
		if t.Failed() {
			t.Fatalf("not writing %s: the corpus does not encode as its records say", corpusSlim)
		}
		writeCorpusForm(t, got)
		t.Logf("wrote %s: %d entries, %d publications, %d readings", corpusSlim, len(got.Entries), len(got.Publications), len(got.Measurements))
	}
	want := readCorpusForm(t)

	// encoding: today's encoder gives the frozen bytes
	if len(got.Publications) != len(want.Publications) || len(got.Measurements) != len(want.Measurements) {
		t.Fatalf("%d publications and %d readings encoded, %d and %d frozen", len(got.Publications), len(got.Measurements), len(want.Publications), len(want.Measurements))
	}
	for i := range got.Publications {
		if !bytes.Equal(got.Publications[i], want.Publications[i]) {
			t.Errorf("publication %d (%s) encodes to %d bytes, not the %d frozen", i, promiseHashOf(t, pubs[i]), len(got.Publications[i]), len(want.Publications[i]))
		}
	}
	for i := range got.Measurements {
		if !bytes.Equal(got.Measurements[i], want.Measurements[i]) {
			t.Errorf("reading %d (%s) encodes to %d bytes, not the %d frozen", i, promiseHashOf(t, meas[i]), len(got.Measurements[i]), len(want.Measurements[i]))
		}
	}
	if fmt.Sprint(got.Entries) != fmt.Sprint(want.Entries) {
		t.Errorf("the tables hold %d entries, not the %d frozen (or not the same ones)", len(got.Entries), len(want.Entries))
	}

	// decoding: the frozen bytes, read with the frozen tables in a fresh Tables, give each line byte for byte
	r := NewTables()
	for _, e := range want.Entries {
		if err := r.Add(Entry{e.Kind, e.ID, e.Body}); err != nil {
			t.Fatal(err)
		}
	}
	byHash := map[string]*Pub{}
	for i, b := range want.Publications {
		line, p, err := r.DecodePublication(b)
		if err != nil {
			t.Fatalf("publication %d: %v", i, err)
		}
		if !bytes.Equal(line, pubs[i]) {
			t.Errorf("publication %d (%s) decodes to another line (%d bytes, the record's is %d)", i, p.Hash, len(line), len(pubs[i]))
		}
		byHash[p.Hash] = p
	}
	lookup := func(h string) *Pub { return byHash[h] }
	for i, b := range want.Measurements {
		line, err := r.DecodeMeasurement(b, byHash[promiseHashOf(t, meas[i])], lookup)
		if err != nil {
			t.Fatalf("reading %d: %v", i, err)
		}
		if !bytes.Equal(line, meas[i]) {
			t.Errorf("reading %d decodes to another line\n got %.300s\nwant %.300s", i, line, meas[i])
		}
	}
}
