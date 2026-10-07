package record

import (
	"archive/tar"
	"bufio"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"
)

// testExport is one export the tests build: its day and the logical bytes
// [from, to) of the file its member holds.
type testExport struct {
	day      string
	from, to int64
}

// retireFixture is a record of eight days, four lines a day, whose first
// six days are exported one tarball a day, archived twice at midday: the
// first segment is lines [0, 6), all of day 0's export and half of day
// 1's; the second is lines [6, 14), which starts inside day 1's member,
// spans day 2's and ends inside day 3's.
type retireFixture struct {
	path    string
	member  string
	want    []byte  // every byte written to the file
	off     []int64 // off[i] is where line i starts; the last is the end
	expDir  string
	days    []testExport
	exports []string // tarball names, day 0 first
	segs    []Segment
}

func newRetireFixture(t *testing.T, file string) *retireFixture {
	t.Helper()
	data := t.TempDir()
	f := &retireFixture{
		path:   filepath.Join(data, filepath.FromSlash(file)),
		member: file,
		expDir: filepath.Join(data, "exports"),
	}
	if err := os.MkdirAll(filepath.Dir(f.path), 0o755); err != nil {
		t.Fatal(err)
	}
	var lines []string
	for d := 0; d < 8; d++ {
		for i := 0; i < 4; i++ {
			l := lineAt(t0.Add(time.Duration(d)*24*time.Hour+time.Duration(i)*6*time.Hour), "a", d*4+i)
			f.off = append(f.off, int64(len(f.want)))
			f.want = append(f.want, l...)
			lines = append(lines, l)
		}
	}
	f.off = append(f.off, int64(len(f.want)))
	appendLines(t, f.path, lines...)
	var entries []map[string]any
	for d := 0; d < 6; d++ {
		e := testExport{day: t0.AddDate(0, 0, d).Format("2006-01-02"), from: f.off[4*d], to: f.off[4*d+4]}
		f.days = append(f.days, e)
		name := "tensile-v-" + e.day + ".tar.gz"
		f.exports = append(f.exports, name)
		entries = append(entries, f.writeTarball(t, d, f.want[e.from:e.to], t0.AddDate(0, 0, d+1)))
	}
	f.writeIndex(t, entries)
	archiveAt(t, f.path, t0.Add(36*time.Hour))
	archiveAt(t, f.path, t0.Add(84*time.Hour))
	idx, err := LoadIndex(f.path)
	if err != nil {
		t.Fatal(err)
	}
	f.segs = idx.Segments
	if len(f.segs) != 2 || f.segs[0].To != f.off[6] || f.segs[1].From != f.off[6] || f.segs[1].To != f.off[14] {
		t.Fatalf("segments %+v, want [0, %d) and [%d, %d)", f.segs, f.off[6], f.off[6], f.off[14])
	}
	return f
}

// writeTarball writes day d's export tarball the way observer/export
// builds one, gzip over a PAX tar, with data as the file's member between
// decoys (another file, and for a vantage's file one under its base name)
// and manifest.json last. It returns the tarball's index entry, which
// lists every member's source range, size and digest and the tarball's.
func (f *retireFixture) writeTarball(t *testing.T, d int, data []byte, at time.Time) map[string]any {
	t.Helper()
	e := f.days[d]
	member := func(name string, b []byte, from, to int64) map[string]any {
		sum := sha256.Sum256(b)
		return map[string]any{"name": name, "time_field": "scheduled_at", "lines": bytes.Count(b, []byte{'\n'}),
			"late_lines": 0, "bytes": len(b), "sha256": hex.EncodeToString(sum[:]), "source_from": from, "source_to": to}
	}
	type mem struct {
		name string
		b    []byte
	}
	decoy := []byte(lineAt(at, "decoy", d))
	mems := []mem{{"publications.jsonl", decoy}}
	files := []map[string]any{member("publications.jsonl", decoy, int64(d*len(decoy)), int64((d+1)*len(decoy)))}
	if base := path.Base(f.member); base != f.member {
		mems = append(mems, mem{base, decoy})
		files = append(files, member(base, decoy, int64(d*len(decoy)), int64((d+1)*len(decoy))))
	}
	mems = append(mems, mem{f.member, data})
	files = append(files, member(f.member, data, e.from, e.to))
	manifest, err := json.Marshal(map[string]any{"vantage": "v", "day": e.day, "files": files})
	if err != nil {
		t.Fatal(err)
	}
	mems = append(mems, mem{"manifest.json", manifest})
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	for _, m := range mems {
		if err := tw.WriteHeader(&tar.Header{Name: m.name, Mode: 0o644, Size: int64(len(m.b)), ModTime: at, Format: tar.FormatPAX}); err != nil {
			t.Fatal(err)
		}
		if _, err := tw.Write(m.b); err != nil {
			t.Fatal(err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(f.expDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(f.expDir, f.exports[d]), buf.Bytes(), 0o644); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(buf.Bytes())
	return map[string]any{"name": f.exports[d], "bytes": buf.Len(), "sha256": hex.EncodeToString(sum[:]),
		"vantage": "v", "day": e.day, "files": files}
}

// writeIndex writes exports/index.json, newest day first as the builder
// keeps it.
func (f *retireFixture) writeIndex(t *testing.T, entries []map[string]any) {
	t.Helper()
	sort.Slice(entries, func(i, j int) bool { return entries[i]["day"].(string) > entries[j]["day"].(string) })
	raw, err := json.MarshalIndent(entries, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(f.expDir, "index.json"), raw, 0o644); err != nil {
		t.Fatal(err)
	}
}

// editIndex rewrites exports/index.json through edit.
func (f *retireFixture) editIndex(t *testing.T, edit func(entries []map[string]any)) {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(f.expDir, "index.json"))
	if err != nil {
		t.Fatal(err)
	}
	var entries []map[string]any
	if err := json.Unmarshal(raw, &entries); err != nil {
		t.Fatal(err)
	}
	edit(entries)
	f.writeIndex(t, entries)
}

// rewriteDay writes day d's tarball with data as the file's member and
// gives it an index entry that agrees with it, the member's source range
// kept: exports consistent in themselves that hold other bytes.
func (f *retireFixture) rewriteDay(t *testing.T, d int, data []byte) {
	t.Helper()
	fresh := f.writeTarball(t, d, data, t0.AddDate(0, 0, d+2))
	f.editIndex(t, func(es []map[string]any) {
		for i, x := range es {
			if x["name"] == f.exports[d] {
				es[i] = fresh
			}
		}
	})
}

// entry is the index entry of the tarball called name.
func entry(entries []map[string]any, name string) map[string]any {
	for _, e := range entries {
		if e["name"] == name {
			return e
		}
	}
	return nil
}

// retired is what the archiver would record for segment i: the exports
// whose member overlaps it, oldest first.
func (f *retireFixture) retired(t *testing.T, i int) Retired {
	t.Helper()
	sg := f.segs[i]
	var names []string
	for d, e := range f.days {
		if e.from < sg.To && e.to > sg.From {
			names = append(names, f.exports[d])
		}
	}
	rel, err := filepath.Rel(ArchiveDir(f.path), f.expDir)
	if err != nil {
		t.Fatal(err)
	}
	return Retired{Exports: names, Member: f.member, ExportsDir: filepath.ToSlash(rel), Proof: "test"}
}

func (f *retireFixture) retire(t *testing.T, i int) {
	t.Helper()
	if err := Retire(f.path, f.segs[i].Name, f.retired(t, i)); err != nil {
		t.Fatal(err)
	}
}

func (f *retireFixture) segFile(i int) string {
	return filepath.Join(ArchiveDir(f.path), f.segs[i].Name)
}

// checkReads reads the whole record and reads it from offsets before,
// inside, at the edges of and after each segment, each byte for byte what
// was written.
func (f *retireFixture) checkReads(t *testing.T) {
	t.Helper()
	if got := readAll(t, f.path); !bytes.Equal(got, f.want) {
		t.Fatalf("the record reads\n%s\nwant\n%s", got, f.want)
	}
	s, err := Open(f.path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	s0, s1 := f.segs[0], f.segs[1]
	for _, off := range []int64{0, 1, f.off[4] + 5, s0.To - 1, s0.To, s0.To + 7, f.off[9] + 2, s1.To - 1, s1.To, s.Base() + 3, s.End()} {
		r, err := s.ReaderFrom(off)
		if err != nil {
			t.Fatalf("offset %d: %v", off, err)
		}
		got, err := io.ReadAll(r)
		if err != nil {
			t.Fatalf("offset %d: %v", off, err)
		}
		if !bytes.Equal(got, f.want[off:]) {
			t.Fatalf("offset %d reads %q", off, got)
		}
	}
}

// A retired segment, of a top-level file or of a vantage's nested one,
// reads back from the exports byte for byte, as the whole record and from
// every offset before, inside, across and after the retired ranges, with
// its file gone, the index naming where its bytes are, and Verify passing.
func TestRetiredSegmentsReadFromExports(t *testing.T) {
	skipUnsupported(t)
	// The vantage's member name has a directory, and its exports directory
	// is two levels further up.
	for _, tc := range []struct{ name, file, rel string }{
		{"top-level", "measurements.jsonl", "../../exports"},
		{"vantage", "vantages/de-1/reachability.jsonl", "../../../../exports"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newRetireFixture(t, tc.file)
			if r := f.retired(t, 1); r.ExportsDir != tc.rel || len(r.Exports) != 3 {
				t.Fatalf("retired %+v, want three exports in %s", r, tc.rel)
			}
			// the middle segment first: it starts and ends inside members
			f.retire(t, 1)
			f.checkReads(t)
			f.retire(t, 0)
			f.checkReads(t)
			idx, err := LoadIndex(f.path)
			if err != nil {
				t.Fatal(err)
			}
			for i, sg := range idx.Segments {
				if _, err := os.Stat(f.segFile(i)); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("segment %s's file is still there: %v", sg.Name, err)
				}
				want := f.retired(t, i)
				if r := sg.Retired; r == nil || r.Member != tc.file || r.ExportsDir != tc.rel || strings.Join(r.Exports, ",") != strings.Join(want.Exports, ",") || r.At.IsZero() {
					t.Fatalf("segment %s retired as %+v, want %+v", sg.Name, r, want)
				}
			}
			if n, err := Verify(f.path); err != nil || n != 2 {
				t.Fatalf("verify: %d %v", n, err)
			}
		})
	}
}

// A retired segment whose exports do not give its exact bytes back ends
// the read with an error that is not os.ErrNotExist before a single byte
// of the segment comes out, so a reader that takes each line as it comes
// (the ingest's tail) never stores a wrong one, and Verify fails too.
func TestRetiredSegmentReadFailsOnBadExports(t *testing.T) {
	skipUnsupported(t)
	// line9 is where the digit after "2" in line 9's year is in day 2's
	// member (lines 8 to 11, all inside the retired segment).
	line9 := func(f *retireFixture) int {
		return int(f.off[9]-f.days[2].from) + len(`{"scheduled_at":"2`)
	}
	for _, tc := range []struct {
		name string
		// atOpen: the plan refuses the exports, so the record does not
		// even open.
		atOpen bool
		spoil  func(t *testing.T, f *retireFixture)
	}{
		// Only the tarball's digest tells.
		{"tarball rewritten", false, func(t *testing.T, f *retireFixture) {
			e := f.days[2]
			f.writeTarball(t, 2, f.want[e.from:e.to], t0.AddDate(0, 0, 9))
		}},
		// Bit rot or a replaced tarball, the index as it was: the member's
		// digest tells, only once the member has been read whole.
		{"member altered inside the range", false, func(t *testing.T, f *retireFixture) {
			e := f.days[2]
			data := append([]byte(nil), f.want[e.from:e.to]...)
			data[line9(f)] = '9'
			f.writeTarball(t, 2, data, t0.AddDate(0, 0, 3))
		}},
		// Only the member's digest tells, so the member must be read whole.
		{"member altered outside the range", false, func(t *testing.T, f *retireFixture) {
			e := f.days[1]
			data := append([]byte(nil), f.want[e.from:e.to]...)
			data[len(`{"scheduled_at":"2`)] = '9' // line 4, before the segment's first line 6
			fresh := f.writeTarball(t, 1, data, t0.AddDate(0, 0, 2))
			f.editIndex(t, func(es []map[string]any) {
				old := entry(es, f.exports[1])
				old["bytes"], old["sha256"] = fresh["bytes"], fresh["sha256"]
			})
		}},
		// Tarball and index agree with each other: only the segment's own
		// digest tells.
		{"member and index altered inside the range", false, func(t *testing.T, f *retireFixture) {
			e := f.days[2]
			data := append([]byte(nil), f.want[e.from:e.to]...)
			data[line9(f)] = '9'
			f.rewriteDay(t, 2, data)
		}},
		// Every digest holds but the bytes land at the wrong offsets: only
		// the segment's digest tells.
		{"range shifted", false, func(t *testing.T, f *retireFixture) {
			f.editIndex(t, func(es []map[string]any) {
				for _, d := range []int{2, 3} {
					for _, m := range entry(es, f.exports[d])["files"].([]any) {
						m := m.(map[string]any)
						if m["name"] == f.member {
							m["source_from"] = m["source_from"].(float64) - 5
							m["source_to"] = m["source_to"].(float64) - 5
						}
					}
				}
			})
		}},
		// A member with a byte dropped from its source range (as the
		// builder drops a blank line's), its digests consistent.
		{"member short of its source range", true, func(t *testing.T, f *retireFixture) {
			e := f.days[2]
			data := append([]byte(nil), f.want[e.from:e.to]...)
			f.rewriteDay(t, 2, append(data[:line9(f)], data[line9(f)+1:]...))
		}},
		{"export missing", true, func(t *testing.T, f *retireFixture) {
			if err := os.Remove(filepath.Join(f.expDir, f.exports[3])); err != nil {
				t.Fatal(err)
			}
		}},
		{"gap", true, func(t *testing.T, f *retireFixture) {
			idx, err := LoadIndex(f.path)
			if err != nil {
				t.Fatal(err)
			}
			idx.Segments[1].Retired.Exports = []string{f.exports[1], f.exports[3]}
			if err := idx.save(ArchiveDir(f.path)); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newRetireFixture(t, "measurements.jsonl")
			f.retire(t, 1)
			f.checkReads(t)
			tc.spoil(t, f)
			r, err := OpenAll(f.path)
			if tc.atOpen && err == nil {
				r.Close()
				t.Fatal("the record opened; the exports should be refused before anything is read")
			}
			var got []byte
			if err == nil {
				br := bufio.NewReader(r)
				for err == nil {
					var line []byte
					line, err = br.ReadBytes('\n')
					got = append(got, line...)
				}
				r.Close()
			}
			if errors.Is(err, io.EOF) {
				t.Fatalf("the record read without an error (%d bytes)", len(got))
			}
			if errors.Is(err, os.ErrNotExist) {
				t.Fatalf("the error says the record file does not exist: %v", err)
			}
			if int64(len(got)) > f.segs[1].From || !bytes.Equal(got, f.want[:len(got)]) {
				t.Fatalf("%d bytes came out before the error, the retired segment starts at %d; the first that differ: %q", len(got), f.segs[1].From, firstDiff(got, f.want))
			}
			if _, err := Verify(f.path); err == nil {
				t.Fatal("verify passed")
			}
			t.Logf("read fails: %v", err)
		})
	}
}

// firstDiff is the line of got where it first differs from want.
func firstDiff(got, want []byte) []byte {
	i := 0
	for i < len(got) && i < len(want) && got[i] == want[i] {
		i++
	}
	start := bytes.LastIndexByte(got[:i], '\n') + 1
	end := len(got)
	if j := bytes.IndexByte(got[i:], '\n'); j >= 0 {
		end = i + j + 1
	}
	return got[start:end]
}

// The second pass of a read from the exports hands out only blocks the
// first pass proved, so exports changed between the passes, even in
// agreement with their index, end the read before a changed byte comes out.
func TestExportsChangedBetweenPasses(t *testing.T) {
	skipUnsupported(t)
	f := newRetireFixture(t, "measurements.jsonl")
	f.retire(t, 1)
	idx, err := LoadIndex(f.path)
	if err != nil {
		t.Fatal(err)
	}
	sg := idx.Segments[1]
	dir := exportsDirOf(f.path, sg.Retired)
	parts, err := planFromExports("before", dir, sg, nil)
	if err != nil {
		t.Fatal(err)
	}
	sums, err := proveFromExports(sg, parts, nil)
	if err != nil {
		t.Fatal(err)
	}
	e := f.days[2]
	data := append([]byte(nil), f.want[e.from:e.to]...)
	data[int(f.off[9]-e.from)+len(`{"scheduled_at":"2`)] = '9'
	f.rewriteDay(t, 2, data)
	if parts, err = planFromExports("after", dir, sg, nil); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err := emitFromExports(&out, sg, parts, sums); err == nil {
		t.Fatal("the second pass handed out bytes the first did not prove")
	} else {
		t.Logf("second pass: %v", err)
	}
	if got := out.Bytes(); !bytes.Equal(got, f.want[sg.From:sg.From+int64(len(got))]) {
		t.Fatalf("the second pass handed out %q", got)
	}
}

// Retire removes nothing the exports do not prove, leaving the file and
// the index as they were, and refuses a segment whose file is gone without
// being retired, which a read and Verify then fail on by name (not as a
// missing record file), as well as one the index does not list.
func TestRetireRefusesWhatTheExportsDoNotProve(t *testing.T) {
	skipUnsupported(t)
	f := newRetireFixture(t, "measurements.jsonl")
	gap := f.retired(t, 1)
	gap.Exports = []string{f.exports[1], f.exports[3]}
	if err := Retire(f.path, f.segs[1].Name, gap); err == nil {
		t.Fatal("retired over a gap in the exports")
	}
	// day 2's member altered inside the range, with an index that agrees
	// with the altered tarball: only the segment's own digest tells
	e := f.days[2]
	data := append([]byte(nil), f.want[e.from:e.to]...)
	data[len(`{"scheduled_at":"2`)] = '9'
	f.rewriteDay(t, 2, data)
	err := Retire(f.path, f.segs[1].Name, f.retired(t, 1))
	if err == nil {
		t.Fatal("retired a segment the exports hold other bytes for")
	}
	t.Logf("refused: %v", err)
	if _, err := os.Stat(f.segFile(1)); err != nil {
		t.Fatalf("the refused segment's file: %v", err)
	}
	idx, err := LoadIndex(f.path)
	if err != nil {
		t.Fatal(err)
	}
	if idx.Segments[1].Retired != nil {
		t.Fatal("a refused segment is recorded as retired")
	}
	f.checkReads(t)

	if err := os.Remove(f.segFile(0)); err != nil {
		t.Fatal(err)
	}
	if err := Retire(f.path, f.segs[0].Name, f.retired(t, 0)); err == nil {
		t.Fatal("retired a segment whose file was already gone")
	}
	if _, err := OpenAll(f.path); err == nil || !strings.Contains(err.Error(), f.segs[0].Name) || errors.Is(err, os.ErrNotExist) {
		t.Fatalf("a read over a lost segment: %v", err)
	}
	// observer-archive -verify takes os.ErrNotExist to mean the record
	// file is missing, which it passes.
	if _, err := Verify(f.path); err == nil || !strings.Contains(err.Error(), f.segs[0].Name) || errors.Is(err, os.ErrNotExist) {
		t.Fatalf("verify over a lost segment: %v", err)
	}
	if err := Retire(f.path, "000009-2026-09-09.jsonl.gz", f.retired(t, 0)); err == nil {
		t.Fatal("retired a segment the index does not list")
	}
}

// A Retire that stopped after saving the index and before removing the
// file leaves a record that still reads from the file, and a second Retire
// checks the exports again, removes the file only once they are there, and
// keeps the Retired given first.
func TestRetireAfterACrashBeforeTheFileWent(t *testing.T) {
	skipUnsupported(t)
	f := newRetireFixture(t, "measurements.jsonl")
	// The index as the stopped Retire saved it; the exports are away, so
	// the record can only read from the file.
	want := f.retired(t, 1)
	want.At = t0
	idx, err := LoadIndex(f.path)
	if err != nil {
		t.Fatal(err)
	}
	idx.Segments[1].Retired = &want
	if err := idx.save(ArchiveDir(f.path)); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(f.expDir, f.expDir+".away"); err != nil {
		t.Fatal(err)
	}
	f.checkReads(t)
	if err := Retire(f.path, f.segs[1].Name, Retired{}); err == nil {
		t.Fatal("removed the file with the exports away")
	}
	if _, err := os.Stat(f.segFile(1)); err != nil {
		t.Fatalf("the file: %v", err)
	}
	if err := os.Rename(f.expDir+".away", f.expDir); err != nil {
		t.Fatal(err)
	}
	if err := Retire(f.path, f.segs[1].Name, Retired{}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(f.segFile(1)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("the file is still there: %v", err)
	}
	f.checkReads(t)
	if err := Retire(f.path, f.segs[1].Name, Retired{}); err != nil {
		t.Fatalf("a repeated Retire: %v", err)
	}
	idx, err = LoadIndex(f.path)
	if err != nil {
		t.Fatal(err)
	}
	if r := idx.Segments[1].Retired; r == nil || !r.At.Equal(t0) || strings.Join(r.Exports, ",") != strings.Join(want.Exports, ",") {
		t.Fatalf("retired as %+v, want %+v", r, want)
	}
}

// A build from before retirement rewrites index.json without the retired
// records whenever it archives (its Segment has no Retired, and an unknown
// field does not survive a decode and an encode). The records survive in
// retired.json, which it never opens: the record still reads whole, Verify
// passes, Retire stays idempotent, and the next archive run writes them
// into the index again.
func TestRetiredRecordsSurviveAnOlderBuildsIndex(t *testing.T) {
	skipUnsupported(t)
	f := newRetireFixture(t, "measurements.jsonl")
	f.retire(t, 0)
	f.retire(t, 1)
	// The index as an older observer-archive saves it after its own run.
	type olderSegment struct {
		Name       string    `json:"name"`
		From       int64     `json:"from"`
		To         int64     `json:"to"`
		Lines      int64     `json:"lines"`
		SHA256     string    `json:"sha256"`
		GzSHA256   string    `json:"gz_sha256"`
		GzBytes    int64     `json:"gz_bytes"`
		Cutoff     time.Time `json:"cutoff"`
		ArchivedAt time.Time `json:"archived_at"`
	}
	type olderIndex struct {
		Version     int            `json:"version"`
		File        string         `json:"file"`
		TimeField   string         `json:"time_field"`
		LiveSince   time.Time      `json:"live_since"`
		Segments    []olderSegment `json:"segments"`
		Generations []Generation   `json:"generations"`
	}
	ip := filepath.Join(ArchiveDir(f.path), IndexFile)
	raw, err := os.ReadFile(ip)
	if err != nil {
		t.Fatal(err)
	}
	var older olderIndex
	if err := json.Unmarshal(raw, &older); err != nil {
		t.Fatal(err)
	}
	if raw, err = json.MarshalIndent(older, "", "  "); err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(raw, []byte(`"retired"`)) {
		t.Fatal("the older index still carries the retired records; the test no longer shows anything")
	}
	if err := os.WriteFile(ip, raw, 0o644); err != nil {
		t.Fatal(err)
	}

	f.checkReads(t)
	if n, err := Verify(f.path); err != nil || n != 2 {
		t.Fatalf("verify: %d %v", n, err)
	}
	idx, err := LoadIndex(f.path)
	if err != nil {
		t.Fatal(err)
	}
	for i, sg := range idx.Segments {
		if want := f.retired(t, i); sg.Retired == nil || strings.Join(sg.Retired.Exports, ",") != strings.Join(want.Exports, ",") {
			t.Fatalf("segment %s: retired %+v, want %+v", sg.Name, sg.Retired, want)
		}
	}
	if err := Retire(f.path, f.segs[1].Name, Retired{}); err != nil {
		t.Fatalf("Retire of a segment retired before: %v", err)
	}
	// The next run of this build saves the index with the records again.
	archiveAt(t, f.path, t0.Add(132*time.Hour))
	if raw, err = os.ReadFile(ip); err != nil {
		t.Fatal(err)
	}
	var saved Index
	if err := json.Unmarshal(raw, &saved); err != nil {
		t.Fatal(err)
	}
	if len(saved.Segments) != 3 || saved.Segments[0].Retired == nil || saved.Segments[1].Retired == nil || saved.Segments[2].Retired != nil {
		t.Fatalf("the index saved by the next run: %s", raw)
	}
	f.checkReads(t)

	// A record for another segment by that name (other range or digest) is
	// not taken for this one's.
	var other Index
	if err := json.Unmarshal(raw, &other); err != nil {
		t.Fatal(err)
	}
	other.Segments[2].SHA256 = strings.Repeat("0", 64)
	if err := keepRetired(ArchiveDir(f.path), filepath.Base(f.path), Segment{Name: other.Segments[2].Name, From: other.Segments[2].From,
		To: other.Segments[2].To, SHA256: other.Segments[2].SHA256, Retired: &Retired{Exports: f.exports[:1], Member: f.member, ExportsDir: "../../exports"}}); err != nil {
		t.Fatal(err)
	}
	if idx, err = LoadIndex(f.path); err != nil || idx.Segments[2].Retired != nil {
		t.Fatalf("a record of other bytes taken for segment 3: %+v %v", idx.Segments[2].Retired, err)
	}
}

// A Stream opened before a Retire reads and verifies the retired segment
// from the exports rather than calling it lost, since Retire saves the
// index before it removes the file and a reader that finds the file gone
// looks at the index again.
func TestStreamOpenedBeforeRetire(t *testing.T) {
	skipUnsupported(t)
	f := newRetireFixture(t, "measurements.jsonl")
	s, err := Open(f.path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	f.retire(t, 1)
	r, err := s.ReaderFrom(0)
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(r)
	if err != nil || !bytes.Equal(got, f.want) {
		t.Fatalf("read %d bytes, %v", len(got), err)
	}
	// observer-archive -verify run while -retire runs
	if n, err := s.verify(); err != nil || n != 2 {
		t.Fatalf("verify over the index from before the Retire: %d %v", n, err)
	}
}

// An export whose member of the file is empty where the exports before it
// end, as a quiet day leaves it, may be named among them: Retire proves
// the segment, and the segment reads back without that tarball, which none
// of its bytes come from.
func TestRetireOverAnEmptyMember(t *testing.T) {
	skipUnsupported(t)
	f := newRetireFixture(t, "measurements.jsonl")
	// The exports whose member overlaps [From, To) include an empty one
	// strictly inside it.
	at := f.off[12]
	f.days = append(f.days, testExport{day: "2026-09-03-quiet", from: at, to: at})
	f.exports = append(f.exports, "tensile-v-quiet.tar.gz")
	quiet := f.writeTarball(t, len(f.days)-1, nil, t0.AddDate(0, 0, 4))
	raw, err := os.ReadFile(filepath.Join(f.expDir, "index.json"))
	if err != nil {
		t.Fatal(err)
	}
	var es []map[string]any
	if err := json.Unmarshal(raw, &es); err != nil {
		t.Fatal(err)
	}
	f.writeIndex(t, append(es, quiet))
	r := f.retired(t, 1)
	r.Exports = []string{f.exports[1], f.exports[2], f.exports[6], f.exports[3]}
	if err := Retire(f.path, f.segs[1].Name, r); err != nil {
		t.Fatal(err)
	}
	f.checkReads(t)
	if err := os.Remove(filepath.Join(f.expDir, f.exports[6])); err != nil {
		t.Fatal(err)
	}
	f.checkReads(t)
	if n, err := Verify(f.path); err != nil || n != 2 {
		t.Fatalf("verify: %d %v", n, err)
	}
	// An empty member anywhere but where the exports have reached is still
	// out of order and refused.
	r = f.retired(t, 0)
	r.Exports = []string{f.exports[0], f.exports[6], f.exports[1]}
	if err := Retire(f.path, f.segs[0].Name, r); err == nil {
		t.Fatal("retired over an empty member out of order")
	}
}

// LogicalEnd is the live file's base plus its size: the file's size before
// any rotation, the same after one, growing with what is appended; a live
// file the index does not describe is an error rather than an end a copier
// would resume at, and a missing file is os.ErrNotExist.
func TestLogicalEnd(t *testing.T) {
	skipUnsupported(t)
	path := filepath.Join(t.TempDir(), "reachability.jsonl")
	if _, err := LogicalEnd(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("missing file: %v", err)
	}
	var total int64
	for d := 0; d < 4; d++ {
		l := lineAt(t0.Add(time.Duration(d)*24*time.Hour), "a", d)
		appendLines(t, path, l)
		total += int64(len(l))
	}
	if end, err := LogicalEnd(path); err != nil || end != total {
		t.Fatalf("before a rotation: %d %v, want %d", end, err, total)
	}
	archiveAt(t, path, t0.Add(2*24*time.Hour))
	if info, err := os.Stat(path); err != nil || info.Size() >= total {
		t.Fatalf("nothing was rotated: %v", err)
	}
	if end, err := LogicalEnd(path); err != nil || end != total {
		t.Fatalf("after a rotation: %d %v, want %d", end, err, total)
	}
	l := lineAt(t0.Add(4*24*time.Hour), "a", 4)
	appendLines(t, path, l)
	if end, err := LogicalEnd(path); err != nil || end != total+int64(len(l)) {
		t.Fatalf("after an append: %d %v, want %d", end, err, total+int64(len(l)))
	}
	if err := os.WriteFile(path, []byte(lineAt(t0, "other", 0)), 0o644); err != nil {
		t.Fatal(err)
	}
	if end, err := LogicalEnd(path); err == nil {
		t.Fatalf("a replaced live file ends at %d", end)
	}
}

// Bytes of reachability.jsonl read back from two consecutive days' real
// export tarballs, from inside the first day's member to inside the
// second's, are byte for byte the two members joined, and a range across a
// day the exports lack fails as a gap.
func TestRealExportsReadBack(t *testing.T) {
	// Opt-in: TENSILE_RECORD_EXPORTS names a directory of real exports
	// (index.json and the tarballs) and TENSILE_RECORD_MEMBERS one holding
	// each day's member as extracted, <day>/reachability.jsonl.
	dir, members := os.Getenv("TENSILE_RECORD_EXPORTS"), os.Getenv("TENSILE_RECORD_MEMBERS")
	if dir == "" || members == "" {
		t.Skip("set TENSILE_RECORD_EXPORTS and TENSILE_RECORD_MEMBERS to read real exports")
	}
	const member = "reachability.jsonl"
	idx, err := loadExportsIndex(dir)
	if err != nil {
		t.Fatal(err)
	}
	type day struct {
		name, day string
		m         exportMember
	}
	var days []day
	for name, es := range idx {
		for _, m := range es[0].Files {
			if m.Name == member && m.Bytes > 0 {
				if _, err := os.Stat(filepath.Join(dir, name)); err == nil {
					d := strings.TrimSuffix(name, ".tar.gz")
					days = append(days, day{name, d[len(d)-len("2006-01-02"):], m})
				}
			}
		}
	}
	sort.Slice(days, func(i, j int) bool { return days[i].m.From < days[j].m.From })
	read := func(sg Segment) ([]byte, error) {
		r, err := openFromExports(sg.Name, dir, sg, map[string]exportsIndex{dir: idx})
		if err != nil {
			return nil, err
		}
		defer r.Close()
		return io.ReadAll(r)
	}
	pair, gap := -1, -1
	for i := 0; i+1 < len(days); i++ {
		if days[i].m.To == days[i+1].m.From && pair < 0 {
			pair = i
		}
		if days[i].m.To < days[i+1].m.From && gap < 0 {
			gap = i
		}
	}
	if pair < 0 {
		t.Fatal("no two consecutive exports in " + dir)
	}
	a, b := days[pair], days[pair+1]
	var src []byte
	for _, d := range []day{a, b} {
		raw, err := os.ReadFile(filepath.Join(members, d.day, member))
		if err != nil {
			t.Fatal(err)
		}
		if int64(len(raw)) != d.m.Bytes {
			t.Fatalf("%s's member is %d bytes, the index says %d", d.day, len(raw), d.m.Bytes)
		}
		src = append(src, raw...)
	}
	from, to := a.m.From+a.m.Bytes/3, b.m.From+b.m.Bytes/2
	want := src[from-a.m.From : to-a.m.From]
	sum := sha256.Sum256(want)
	sg := Segment{Name: "real", From: from, To: to, Lines: int64(bytes.Count(want, []byte{'\n'})), SHA256: hex.EncodeToString(sum[:]),
		Retired: &Retired{Exports: []string{a.name, b.name}, Member: member, ExportsDir: "exports"}}
	got, err := read(sg)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("%d bytes read back from %s and %s differ from the members joined", len(got), a.name, b.name)
	}
	t.Logf("read back [%d, %d) of %s, %d bytes, from %s and %s", from, to, member, len(got), a.name, b.name)
	if gap >= 0 {
		g, h := days[gap], days[gap+1]
		sg := Segment{Name: "gap", From: g.m.To - 10, To: h.m.From + 10, SHA256: "-",
			Retired: &Retired{Exports: []string{g.name, h.name}, Member: member, ExportsDir: "exports"}}
		if _, err := read(sg); err == nil {
			t.Fatalf("read across the gap between %s and %s", g.name, h.name)
		} else {
			t.Logf("across %s and %s: %v", g.name, h.name, err)
		}
	}
}
