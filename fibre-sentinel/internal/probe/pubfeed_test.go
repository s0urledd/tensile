package probe

import (
	"bytes"
	"compress/gzip"
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
// index, and runs on every platform: the lines cut go to a gzip segment,
// the index gains its entry and a generation naming the tail by its first
// line, and a copy of the tail is renamed over the path. "inplace" writes
// the tail over the live file itself instead, so the path names the same
// file before and after, which is what a file created on a freed inode
// looks like to anything that compares identities. "unlisted" is "rename"
// without the segment, an index that does not say how many lines were cut
// and has nothing to read them back from.
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
	cut, lines := 0, int64(0)
	for _, l := range bytes.SplitAfter(raw, []byte{'\n'}) {
		var p scan.Publication
		if json.Unmarshal(l, &p) != nil || !p.SettlementTime.Before(cutoff) {
			break
		}
		cut += len(l)
		lines++
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
	if err := os.MkdirAll(record.ArchiveDir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if how != "unlisted" {
		sg := record.Segment{
			Name: fmt.Sprintf("%06d-hand.jsonl.gz", len(idx.Segments)+1), From: base, To: base + int64(cut), Lines: lines,
		}
		var gz bytes.Buffer
		z := gzip.NewWriter(&gz)
		if _, err := z.Write(raw[:cut]); err != nil {
			t.Fatal(err)
		}
		if err := z.Close(); err != nil {
			t.Fatal(err)
		}
		sum, gzSum := sha256.Sum256(raw[:cut]), sha256.Sum256(gz.Bytes())
		sg.SHA256, sg.GzSHA256, sg.GzBytes = hex.EncodeToString(sum[:]), hex.EncodeToString(gzSum[:]), int64(gz.Len())
		if err := os.WriteFile(filepath.Join(record.ArchiveDir(path), sg.Name), gz.Bytes(), 0o644); err != nil {
			t.Fatal(err)
		}
		idx.Segments = append(idx.Segments, sg)
	}
	idx.Generations = append(idx.Generations, record.Generation{Base: base + int64(cut), Head: digest(raw[cut:])})
	b, err := json.Marshal(idx)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(record.ArchiveDir(path), record.IndexFile), b, 0o644); err != nil {
		t.Fatal(err)
	}
	if how == "inplace" {
		if err := os.WriteFile(path, raw[cut:], 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	tmp := path + ".rotate.tmp"
	if err := os.WriteFile(tmp, raw[cut:], 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(tmp, path); err != nil {
		t.Fatal(err)
	}
}

// A rotated publications.jsonl is read on by logical offset, whatever file
// the path names. When the live file the archiver leaves is longer than
// the offset read in the old one, which a size check takes for the same
// file, when it is shorter, when the cut passed what the feed had read,
// and when two rotations came between refreshes, the feed reads on at the
// first byte it had not read: each later publication is added once, the
// ones cut before it read them from the segment, every one it holds stays
// (archived or not), none it has forgotten comes back, and a bad line is
// named by its line in the live file, counted by the index's segments. An
// index that lists no segment for the lines cut ("unlisted") cannot count
// them, so the line is named in the whole record, and a feed behind such a
// cut stops at the lines it cannot read rather than step over them.
func TestPubFeedReadsOnAcrossRotation(t *testing.T) {
	for _, how := range []string{"archive", "rename", "inplace", "unlisted"} {
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
			appendBad(t, path)
			// p03..p30 are the new file's first 28 lines, p00..p30 the record's first 31
			if want := badLine(how, 29, 32); !feedFails(feed, want) {
				t.Fatalf("a bad line after the rotation is not named as %q", want)
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
		// The cut at (keep 5) or past (keep 7) the end of what the feed had
		// read: the lines it had not read that went to the segment are read
		// from there, then the new file's.
		for _, keep := range []int{5, 7} {
			t.Run(fmt.Sprintf("%s/behind-%d", how, keep), func(t *testing.T) {
				path := filepath.Join(t.TempDir(), "publications.jsonl")
				appendPubs(t, path, 0, 5)
				feed := newPubFeed(path)
				if n, err := feed.refresh(); err != nil || n != 5 {
					t.Fatalf("first refresh: %d %v", n, err)
				}
				appendPubs(t, path, 5, 10)
				rotatePubs(t, how, path, keep)
				if how == "unlisted" && keep == 7 {
					// p05 and p06 are in no segment the index lists
					for range 2 {
						if n, err := feed.refresh(); err == nil || n != 0 {
							t.Fatalf("refresh behind an unlisted cut added %d (%v), want an error", n, err)
						}
					}
					if got, want := feedHashes(feed), pubRange(0, 5); !slices.Equal(got, want) {
						t.Fatalf("held %v\nwant %v", got, want)
					}
					return
				}
				if n, err := feed.refresh(); err != nil || n != 5 {
					t.Fatalf("refresh after the rotation added %d (%v), want the 5 written since", n, err)
				}
				if got, want := feedHashes(feed), pubRange(0, 10); !slices.Equal(got, want) {
					t.Fatalf("held %v\nwant %v", got, want)
				}
				appendBad(t, path)
				if want := badLine(how, 10-keep+1, 11); !feedFails(feed, want) {
					t.Fatalf("a bad line after the rotation is not named as %q", want)
				}
			})
		}
		t.Run(how+"/twice", func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "publications.jsonl")
			appendPubs(t, path, 0, 10)
			feed := newPubFeed(path)
			if n, err := feed.refresh(); err != nil || n != 10 {
				t.Fatalf("first refresh: %d %v", n, err)
			}
			for _, h := range []string{"p00", "p01", "p07"} {
				feed.forget(h)
			}
			appendPubs(t, path, 10, 12)
			rotatePubs(t, how, path, 3)
			appendPubs(t, path, 12, 15)
			rotatePubs(t, how, path, 6)
			if n, err := feed.refresh(); err != nil || n != 5 {
				t.Fatalf("refresh after two rotations added %d (%v), want the 5 written since", n, err)
			}
			if got, want := feedHashes(feed), pubRange(2, 15, 7); !slices.Equal(got, want) {
				t.Fatalf("held %v\nwant %v", got, want)
			}
			appendBad(t, path)
			// p06..p14 are the new file's first 9 lines, p00..p14 the record's first 15
			if want := badLine(how, 10, 16); !feedFails(feed, want) {
				t.Fatalf("a bad line after two rotations is not named as %q", want)
			}
		})
	}
}

// A file created after a rotation can compare equal to one the feed read
// before it: ext4 gives a freed inode to the next file created, so after
// two rotations between refreshes the live file can carry the first one's
// identity, and a feed that told files apart by identity read on at its
// old offset in the new file, or reloaded it whole when it was shorter.
// The "inplace" rotation gives that view on every platform. Whether the
// live file after two rotations is shorter or longer than the offset read
// in the first, and after a third that cut past what the feed had read,
// every publication written since is added once, the ones the cut took
// before the feed read them from the segment.
func TestPubFeedReadsByLogicalOffset(t *testing.T) {
	for _, tc := range []struct {
		name string
		upTo int // publications written before the second rotation
	}{{"shorter", 15}, {"longer", 20}} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "publications.jsonl")
			appendPubs(t, path, 0, 10)
			first, err := os.Stat(path)
			if err != nil {
				t.Fatal(err)
			}
			feed := newPubFeed(path)
			if n, err := feed.refresh(); err != nil || n != 10 {
				t.Fatalf("first refresh: %d %v", n, err)
			}
			appendPubs(t, path, 10, 12)
			rotatePubs(t, "inplace", path, 3)
			appendPubs(t, path, 12, tc.upTo)
			rotatePubs(t, "inplace", path, 6)
			now, err := os.Stat(path)
			if err != nil || !os.SameFile(first, now) {
				t.Fatalf("the live file is not the one read first (%v); the test proves nothing", err)
			}
			if shorter := now.Size() < first.Size(); shorter != (tc.name == "shorter") {
				t.Fatalf("the live file is %d bytes against the %d read; the test proves nothing", now.Size(), first.Size())
			}
			if n, err := feed.refresh(); err != nil || n != tc.upTo-10 {
				t.Fatalf("refresh after two rotations added %d (%v), want the %d written since", n, err, tc.upTo-10)
			}
			if got, want := feedHashes(feed), pubRange(0, tc.upTo); !slices.Equal(got, want) {
				t.Fatalf("held %v\nwant %v", got, want)
			}
			// the third cut takes three lines the feed has not read
			appendPubs(t, path, tc.upTo, tc.upTo+5)
			rotatePubs(t, "inplace", path, tc.upTo+3)
			if n, err := feed.refresh(); err != nil || n != 5 {
				t.Fatalf("refresh after a cut past what was read added %d (%v), want the 5 written since", n, err)
			}
			if got, want := feedHashes(feed), pubRange(0, tc.upTo+5); !slices.Equal(got, want) {
				t.Fatalf("held %v\nwant %v", got, want)
			}
			appendBad(t, path)
			if want := "line 3:"; !feedFails(feed, want) {
				t.Fatalf("a bad line after three rotations is not named as %q", want)
			}
		})
	}
}

// A bad line the feed reads back from a segment, cut before the feed read
// it, is named by the segment and its line there, after the lines before
// it in the segment are taken in, and stops every refresh after.
func TestPubFeedNamesABadLineInASegment(t *testing.T) {
	path := filepath.Join(t.TempDir(), "publications.jsonl")
	appendPubs(t, path, 0, 5)
	feed := newPubFeed(path)
	if n, err := feed.refresh(); err != nil || n != 5 {
		t.Fatalf("first refresh: %d %v", n, err)
	}
	appendPubs(t, path, 5, 7)
	// dated like p07, so the cut takes it, but without a promise hash
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, err = fmt.Fprintf(f, "{\"settlement_time\":%q}\n", feedPub(7).SettlementTime.Format(time.RFC3339))
	f.Close()
	if err != nil {
		t.Fatal(err)
	}
	appendPubs(t, path, 7, 10)
	rotatePubs(t, "rename", path, 8)
	// the segment holds p00..p06, the bad line and p07
	want := filepath.Join(record.ArchiveDir(path), "000001-hand.jsonl.gz") + " line 8:"
	if n, err := feed.refresh(); n != 2 || err == nil || !strings.Contains(err.Error(), want) {
		t.Fatalf("refresh over the bad line added %d (%v), want p05 and p06 and the line named as %q", n, err, want)
	}
	if !feedFails(feed, want) {
		t.Fatalf("a second refresh did not stop at the bad line again")
	}
	if got, want := feedHashes(feed), pubRange(0, 7); !slices.Equal(got, want) {
		t.Fatalf("held %v\nwant %v", got, want)
	}
}

// badLine is how the feed names a bad line that is line live of the live
// file and line all of the whole record: by its line in the live file, or
// in the whole record where the index lists no segments to count the lines
// before the live file by ("unlisted").
func badLine(how string, live, all int) string {
	if how == "unlisted" {
		return fmt.Sprintf("line %d of the whole record", all)
	}
	return fmt.Sprintf("line %d:", live)
}

// feedFails reports whether a refresh fails naming want, and fails again
// the same way: a bad line is never stepped over.
func feedFails(f *pubFeed, want string) bool {
	for range 2 {
		if n, err := f.refresh(); n != 0 || err == nil || !strings.Contains(err.Error(), want) {
			return false
		}
	}
	return true
}

// appendBad appends a line that is not JSON, to see which line number the
// feed names it by.
func appendBad(t *testing.T, path string) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.WriteString("{not json}\n"); err != nil {
		t.Fatal(err)
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
