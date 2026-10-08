package export

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"runtime"
	"strings"
	"testing"
	"time"
)

// countLines is the builder's line count over a whole member, as collect
// counts: a line counts when bytes.TrimSpace leaves something of it.
func countLines(b []byte) int64 {
	var n int64
	for _, l := range bytes.Split(b, []byte{'\n'}) {
		if len(bytes.TrimSpace(l)) > 0 {
			n++
		}
	}
	return n
}

// The line count taken as a member streams by is the builder's, however the
// stream is cut: white space beyond ASCII, a rune cut in two by the end of a
// read, bytes that are not UTF-8, a last line without a newline.
func TestTheStreamedLineCountIsTheBuilders(t *testing.T) {
	for _, in := range []string{
		"", "\n", "a", "a\n", " \n\t\r\n", "x\n\n y \n", "\v\f\n", "\x00\n",
		" \n", " x\n", "　\n", "  \n", " ", " \u0085 \n",
		"\xe2\n", "\xe2\x80", "\xe2\x80\x83", "\xef\xbf\xbd\n", "\xc2", "\xff\xfe\n",
		`{"started_at":"2026-09-10T10:00:00Z"}` + "\r\n" + `{"b":1}` + "\n\n",
		strings.Repeat(" ", 100) + " \n" + strings.Repeat(" ", 50) + "z",
	} {
		want := countLines([]byte(in))
		sum := sha256.Sum256([]byte(in))
		for _, chunk := range []int{1, 2, 3, 5, 7, len(in) + 1} {
			s := newSummer()
			for b := []byte(in); len(b) > 0; {
				n := min(chunk, len(b))
				if _, err := s.Write(b[:n]); err != nil {
					t.Fatal(err)
				}
				b = b[n:]
			}
			got := s.sum()
			if got.Lines != want || got.Bytes != int64(len(in)) || got.SHA256 != hex.EncodeToString(sum[:]) {
				t.Errorf("%q in writes of %d: %+v, want %d lines", in, chunk, got, want)
			}
		}
	}
}

// zeros reads as many zero bytes as it is asked for.
type zeros struct{}

func (zeros) Read(p []byte) (int, error) {
	clear(p)
	return len(p), nil
}

// A tarball of large members, a decompression bomb among them (members of
// zeros, a few hundred kilobytes of gzip for each tens of megabytes), is
// checked without its members being held: each is digested, measured and
// counted as it streams by. ReadArchive held every member whole, up to 4 GiB
// each and with no limit on how many, so a file like this took the
// verifier's memory before any check ran.
func TestReadArchiveDoesNotHoldWhatItChecks(t *testing.T) {
	const size = 24 << 20
	h := sha256.New()
	if _, err := io.Copy(h, io.LimitReader(zeros{}, size)); err != nil {
		t.Fatal(err)
	}
	digest := hex.EncodeToString(h.Sum(nil))
	state := []byte(`{"chain_id":"mocha-4"}`)
	ss := sha256.Sum256(state)
	man := Manifest{Vantage: "v", Day: "2026-10-08", Rule: rule,
		State: &Member{Name: StateFile, Lines: 1, Bytes: int64(len(state)), SHA256: hex.EncodeToString(ss[:])}}
	names := []string{"measurements.jsonl", "reachability.jsonl", "publications.jsonl"}
	for _, n := range names {
		man.Files = append(man.Files, Member{Name: n, TimeField: "t", Lines: 1, Bytes: size, SHA256: digest})
	}
	var buf bytes.Buffer
	zw, err := gzip.NewWriterLevel(&buf, gzip.BestSpeed)
	if err != nil {
		t.Fatal(err)
	}
	tw := tar.NewWriter(zw)
	if err := addMember(tw, StateFile, state, time.Now()); err != nil {
		t.Fatal(err)
	}
	for _, n := range names {
		if err := tw.WriteHeader(&tar.Header{Name: n, Mode: 0o644, Size: size, Format: tar.FormatPAX}); err != nil {
			t.Fatal(err)
		}
		if _, err := io.Copy(tw, io.LimitReader(zeros{}, size)); err != nil {
			t.Fatal(err)
		}
	}
	manJSON, err := json.Marshal(man)
	if err != nil {
		t.Fatal(err)
	}
	if err := addMember(tw, "manifest.json", manJSON, time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}

	old := maxHeld
	maxHeld = 1 << 20
	t.Cleanup(func() { maxHeld = old })
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	a, err := ReadArchive(buf.Bytes())
	runtime.ReadMemStats(&after)
	if err != nil {
		t.Fatal(err)
	}
	if got := after.TotalAlloc - before.TotalAlloc; got > 16<<20 {
		t.Fatalf("reading %d MiB of members allocated %d MiB", 3*size>>20, got>>20)
	}
	if p := a.CheckMembers(); len(p) != 0 {
		t.Fatalf("members: %v", p)
	}
	if a.Held || len(a.Members) != 5 || a.Members[StateFile] != nil || a.Members["measurements.jsonl"] != nil || !bytes.Equal(a.Members["manifest.json"], manJSON) {
		t.Fatalf("held %v, %d member(s)", a.Held, len(a.Members))
	}
	if a.Sums["measurements.jsonl"].Bytes != size || a.Manifest.Day != "2026-10-08" {
		t.Fatalf("sums %+v, manifest %+v", a.Sums, a.Manifest)
	}
}
