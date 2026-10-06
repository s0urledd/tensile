package record

import (
	"archive/tar"
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

// A retired segment reads back from the exports byte for byte: the whole
// record and every offset before, inside, across and after the retired
// ranges, with one segment retired and with both, for a top-level file and
// a vantage's nested one (whose member name has a directory and whose
// exports directory is two levels further up). The gzip files are gone,
// the index says where the bytes are, and Verify passes.
func TestRetiredSegmentsReadFromExports(t *testing.T) {
	skipUnsupported(t)
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

// A retired segment whose exports do not give its exact bytes back fails
// the read with an error, never with wrong bytes that end cleanly: a
// tarball rebuilt around the same member (only its digest tells), a
// member altered outside the range the segment needs (only its digest
// tells, so the member must be read whole), a missing tarball, exports
// that leave a gap, and member ranges shifted so that every digest holds
// but the bytes land at the wrong offsets (only the segment's digest
// tells). Whatever was read before the error stops short of the segment's
// end, no error passes for a missing record file (os.ErrNotExist), and
// Verify fails too.
func TestRetiredSegmentReadFailsOnBadExports(t *testing.T) {
	skipUnsupported(t)
	for _, tc := range []struct {
		name  string
		spoil func(t *testing.T, f *retireFixture)
	}{
		{"tarball rewritten", func(t *testing.T, f *retireFixture) {
			e := f.days[2]
			f.writeTarball(t, 2, f.want[e.from:e.to], t0.AddDate(0, 0, 9))
		}},
		{"member altered outside the range", func(t *testing.T, f *retireFixture) {
			e := f.days[1]
			data := append([]byte(nil), f.want[e.from:e.to]...)
			data[len(`{"scheduled_at":"2`)] = '9' // line 4, before the segment's first line 6
			fresh := f.writeTarball(t, 1, data, t0.AddDate(0, 0, 2))
			f.editIndex(t, func(es []map[string]any) {
				old := entry(es, f.exports[1])
				old["bytes"], old["sha256"] = fresh["bytes"], fresh["sha256"]
			})
		}},
		{"export missing", func(t *testing.T, f *retireFixture) {
			if err := os.Remove(filepath.Join(f.expDir, f.exports[3])); err != nil {
				t.Fatal(err)
			}
		}},
		{"gap", func(t *testing.T, f *retireFixture) {
			idx, err := LoadIndex(f.path)
			if err != nil {
				t.Fatal(err)
			}
			idx.Segments[1].Retired.Exports = []string{f.exports[1], f.exports[3]}
			if err := idx.save(ArchiveDir(f.path)); err != nil {
				t.Fatal(err)
			}
		}},
		{"range shifted", func(t *testing.T, f *retireFixture) {
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
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newRetireFixture(t, "measurements.jsonl")
			f.retire(t, 1)
			f.checkReads(t)
			tc.spoil(t, f)
			r, err := OpenAll(f.path)
			var got []byte
			if err == nil {
				got, err = io.ReadAll(r)
				r.Close()
			}
			if err == nil {
				t.Fatalf("the record read without an error (%d bytes)", len(got))
			}
			if errors.Is(err, os.ErrNotExist) {
				t.Fatalf("the error says the record file does not exist: %v", err)
			}
			if int64(len(got)) >= f.segs[1].To {
				t.Fatalf("%d bytes were read before the error; the retired segment ends at %d", len(got), f.segs[1].To)
			}
			if n := min(int64(len(got)), f.segs[1].From); !bytes.Equal(got[:n], f.want[:n]) {
				t.Fatalf("bytes before the retired segment read wrong: %q", got[:n])
			}
			if _, err := Verify(f.path); err == nil {
				t.Fatal("verify passed")
			}
			t.Logf("read fails: %v", err)
		})
	}
}

// Retire proves the bytes before it removes anything: exports that leave a
// gap, or that are consistent in themselves but hold other bytes for the
// segment's range, are refused and the file and index are left as they
// were. A segment whose file is gone without being retired is refused, and
// a read over it fails naming it (not as a missing record file); one the
// index does not list is refused too.
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
	fresh := f.writeTarball(t, 2, data, t0.AddDate(0, 0, 3))
	f.editIndex(t, func(es []map[string]any) {
		for i, x := range es {
			if x["name"] == f.exports[2] {
				es[i] = fresh
			}
		}
	})
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
	if err := Retire(f.path, "000009-2026-09-09.jsonl.gz", f.retired(t, 0)); err == nil {
		t.Fatal("retired a segment the index does not list")
	}
}

// A Retire that stopped after saving the index and before removing the
// file leaves a record that still reads, from the file (the exports can be
// away); a Retire then checks the exports again, refusing while they are
// away, and removes the file once they are back. The Retired given first
// stands, and a further Retire changes nothing.
func TestRetireAfterACrashBeforeTheFileWent(t *testing.T) {
	skipUnsupported(t)
	f := newRetireFixture(t, "measurements.jsonl")
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

// A Stream opened before a Retire reads the retired segment from the
// exports: Retire saves the index before it removes the file, and a reader
// that finds the file gone looks at the index again.
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
// export tarballs, over a range that starts inside the first day's member
// and ends inside the second's, are byte for byte the two members joined;
// a range across a day the exports lack is a gap and fails. Opt-in:
// TENSILE_RECORD_EXPORTS names a directory of real exports (index.json and
// the tarballs) and TENSILE_RECORD_MEMBERS one holding each day's member as
// extracted, <day>/reachability.jsonl.
func TestRealExportsReadBack(t *testing.T) {
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
