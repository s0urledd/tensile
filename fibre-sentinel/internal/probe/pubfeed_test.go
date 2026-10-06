package probe

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/record"
	"github.com/plsgiveup/fibre/fibre-sentinel/internal/scan"
)

// The shadow candidates are every promise the feed still holds, whatever
// phase it is in: a promise past its must_serve_until but not yet past its
// post-deadline probe still has a shard on disk, and the store answers from
// it. Filtering candidates by "in window" would file the older promise's
// rows as a fault in its last minutes.
func TestShadowersFor_KeepsPromisesPastTheirDeadline(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "publications.jsonl")
	now := time.Now().UTC()
	mk := func(hash string, msu time.Time, rows []int) scan.Publication {
		return scan.Publication{
			PromiseHash: hash, MustServeUntil: msu, SettlementTime: msu.Add(-4 * time.Hour),
			Promise: scan.PromiseFields{Commitment: "cc"},
			Assignment: scan.AssignmentTable{Validators: []scan.ValidatorAssignment{
				{Address: "v1", RowCount: len(rows), Rows: rows},
			}},
		}
	}
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	enc := json.NewEncoder(f)
	// older promise: deadline passed a minute ago, shard still on disk
	if err := enc.Encode(mk("old", now.Add(-time.Minute), []int{1, 2})); err != nil {
		t.Fatal(err)
	}
	// the promise being probed: same blob, different rows
	if err := enc.Encode(mk("new", now.Add(time.Hour), []int{3, 4, 5})); err != nil {
		t.Fatal(err)
	}
	f.Close()

	feed := newPubFeed(path)
	if _, err := feed.refresh(); err != nil {
		t.Fatal(err)
	}
	cands := feed.shadowersFor("new", "cc", "v1")
	if len(cands) != 1 || cands[0].PromiseHash != "old" || len(cands[0].Rows) != 2 {
		t.Fatalf("candidates for the new promise = %+v, want the older promise past its deadline", cands)
	}
	if got := feed.shadowersFor("old", "cc", "v1"); len(got) != 1 || got[0].PromiseHash != "new" {
		t.Fatalf("candidates for the old promise = %+v, want the newer one", got)
	}
	if got := feed.shadowersFor("new", "cc", "v2"); len(got) != 0 {
		t.Fatalf("a validator with no rows in the other promise got candidates: %+v", got)
	}
	// forgetting the older promise (its post probe done, shard pruned) drops it
	feed.forget("old")
	if got := feed.shadowersFor("new", "cc", "v1"); len(got) != 0 {
		t.Fatalf("forgotten promise still a candidate: %+v", got)
	}
}

var feedT0 = time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)

// feedPub is publication i, settled i hours after feedT0. Its signer is a
// length of its own, so lines differ in size and an offset carried into the
// wrong file lands inside a line rather than on a boundary by chance.
func feedPub(i int) scan.Publication {
	at := feedT0.Add(time.Duration(i) * time.Hour)
	return scan.Publication{
		PromiseHash: fmt.Sprintf("p%02d", i), SettlementTime: at, MustServeUntil: at.Add(4 * time.Hour),
		Signer: strings.Repeat("s", i%7+1), Promise: scan.PromiseFields{Commitment: fmt.Sprintf("c%02d", i)},
	}
}

// appendPubs appends publications from..to-1 to path as the scanner does,
// one write each. The file is closed again, so no handle is left open that
// would stop a rename over it on Windows.
func appendPubs(t *testing.T, path string, from, to int) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	for i := from; i < to; i++ {
		b, err := json.Marshal(feedPub(i))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.Write(append(b, '\n')); err != nil {
			t.Fatal(err)
		}
	}
}

func feedHashes(f *pubFeed) []string {
	var out []string
	for _, p := range f.all() {
		out = append(out, p.PromiseHash)
	}
	return out
}

func pubRange(from, to int, except ...int) []string {
	var out []string
	for i := from; i < to; i++ {
		if !slices.Contains(except, i) {
			out = append(out, fmt.Sprintf("p%02d", i))
		}
	}
	return out
}

// rotatePubs rotates path so its live file keeps publication keepFrom and
// every one after it. "archive" is record.Archive itself, where flock lets
// it run. "rename" does by hand what it does to the live file and the
// index, and runs on every platform: the tail is copied to a new file, the
// index gains a generation naming the copy by its first line, and the copy
// is renamed over the path.
func rotatePubs(t *testing.T, how, path string, keepFrom int) {
	t.Helper()
	cutoff := feedPub(keepFrom).SettlementTime
	if how == "archive" {
		res, err := record.Archive(path, record.Options{Cutoff: cutoff, TimeField: "settlement_time", Limit: -1, Now: cutoff})
		if errors.Is(err, record.ErrUnsupported) {
			t.Skip("record.Archive needs flock, which this platform does not have; the rename variant runs the same checks")
		}
		if err != nil || res.Skipped != "" {
			t.Fatalf("archive: %+v %v", res, err)
		}
		return
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	digest := func(b []byte) string {
		sum := sha256.Sum256(b[:bytes.IndexByte(b, '\n')+1])
		return hex.EncodeToString(sum[:])
	}
	cut := 0
	for _, l := range bytes.SplitAfter(raw, []byte{'\n'}) {
		var p scan.Publication
		if json.Unmarshal(l, &p) != nil || !p.SettlementTime.Before(cutoff) {
			break
		}
		cut += len(l)
	}
	idx, err := record.LoadIndex(path)
	if err != nil {
		t.Fatal(err)
	}
	base, found := int64(0), len(idx.Generations) == 0
	for _, g := range idx.Generations {
		if g.Head == digest(raw) {
			base, found = g.Base, true
		}
	}
	if !found {
		t.Fatal("the live file matches no generation of the index the test wrote")
	}
	if len(idx.Generations) == 0 {
		idx.Generations = []record.Generation{{Base: 0, Head: digest(raw)}}
	}
	idx.Generations = append(idx.Generations, record.Generation{Base: base + int64(cut), Head: digest(raw[cut:])})
	b, err := json.Marshal(idx)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(record.ArchiveDir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(record.ArchiveDir(path), record.IndexFile), b, 0o644); err != nil {
		t.Fatal(err)
	}
	tmp := path + ".rotate.tmp"
	if err := os.WriteFile(tmp, raw[cut:], 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(tmp, path); err != nil {
		t.Fatal(err)
	}
}

// A rotated publications.jsonl is told by its identity, not its size. When
// the live file the archiver leaves is longer than the offset read in the
// old one, which a size check takes for the same file, and when it is
// shorter, the feed reads on at the first line it had not read: each later
// publication is added once, every one it holds stays (archived or not),
// none it has forgotten comes back, and it counts lines in the new file.
func TestPubFeedReadsOnAcrossRotation(t *testing.T) {
	for _, how := range []string{"archive", "rename"} {
		t.Run(how+"/longer", func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "publications.jsonl")
			appendPubs(t, path, 0, 10)
			feed := newPubFeed(path)
			if n, err := feed.refresh(); err != nil || n != 10 {
				t.Fatalf("first refresh: %d %v", n, err)
			}
			for _, h := range []string{"p00", "p01", "p05"} {
				feed.forget(h) // read on schedule: done with
			}
			appendPubs(t, path, 10, 30)
			read := feed.offset
			rotatePubs(t, how, path, 3)
			if info, err := os.Stat(path); err != nil || info.Size() <= read {
				t.Fatalf("the new live file is not longer than the %d bytes read (%v); the test proves nothing", read, err)
			}
			if n, err := feed.refresh(); err != nil || n != 20 {
				t.Fatalf("refresh after the rotation added %d (%v), want the 20 written since", n, err)
			}
			if got, want := feedHashes(feed), pubRange(2, 30, 5); !slices.Equal(got, want) {
				t.Fatalf("held %v\nwant %v", got, want)
			}
			appendPubs(t, path, 30, 31)
			if n, err := feed.refresh(); err != nil || n != 1 {
				t.Fatalf("the next append: %d %v", n, err)
			}
			f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
			if err != nil {
				t.Fatal(err)
			}
			f.WriteString("{not json}\n")
			f.Close()
			// p03..p30 are the new file's first 28 lines
			if _, err := feed.refresh(); err == nil || !strings.Contains(err.Error(), "line 29:") {
				t.Fatalf("a bad line after the rotation: %v, want it named as line 29", err)
			}
		})
		t.Run(how+"/shorter", func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "publications.jsonl")
			appendPubs(t, path, 0, 10)
			feed := newPubFeed(path)
			if n, err := feed.refresh(); err != nil || n != 10 {
				t.Fatalf("first refresh: %d %v", n, err)
			}
			for _, h := range []string{"p00", "p01", "p09"} {
				feed.forget(h)
			}
			appendPubs(t, path, 10, 12)
			read := feed.offset
			rotatePubs(t, how, path, 9)
			if info, err := os.Stat(path); err != nil || info.Size() >= read {
				t.Fatalf("the new live file is not shorter than the %d bytes read (%v); the test proves nothing", read, err)
			}
			if n, err := feed.refresh(); err != nil || n != 2 {
				t.Fatalf("refresh after the rotation added %d (%v), want the 2 written since", n, err)
			}
			if got, want := feedHashes(feed), append(pubRange(2, 9), pubRange(10, 12)...); !slices.Equal(got, want) {
				t.Fatalf("held %v\nwant %v", got, want)
			}
		})
	}
}

// A file put in the path's place outside the archiver, which no generation
// of the index names, is a record of its own: it is read from its start,
// even when it is longer than the offset read in the file before it.
func TestPubFeedReloadsAFileTheArchiverDidNotWrite(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "publications.jsonl")
	appendPubs(t, path, 0, 3)
	feed := newPubFeed(path)
	if n, err := feed.refresh(); err != nil || n != 3 {
		t.Fatalf("first refresh: %d %v", n, err)
	}
	other := filepath.Join(dir, "other.jsonl")
	appendPubs(t, other, 40, 50)
	if err := os.Rename(other, path); err != nil {
		t.Fatal(err)
	}
	if n, err := feed.refresh(); err != nil || n != 10 {
		t.Fatalf("refresh of the replaced file: %d %v", n, err)
	}
	if got, want := feedHashes(feed), pubRange(40, 50); !slices.Equal(got, want) {
		t.Fatalf("held %v, want %v", got, want)
	}
}
