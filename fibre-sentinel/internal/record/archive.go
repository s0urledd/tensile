package record

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// LockFile, under archive/, is held exclusively by an archive run for its
// whole length. deploy/backup.sh takes it too (flock(1)), so a backup's cut
// and copy never straddle a rotation.
const LockFile = ".lock"

// Options is one archive run over one file.
type Options struct {
	// Cutoff: lines before the first line dated at or after it move to the
	// archive. A line with no readable TimeField stops the cut as if it
	// were new, so nothing is archived on a guess.
	Cutoff    time.Time
	TimeField string
	// Limit caps the logical offset archived up to (the export's offset
	// for this file, so only exported bytes move); < 0 means no cap.
	Limit int64
	// DryRun finds the cut and reports it without writing anything.
	DryRun bool
	Now    time.Time
	// hook is called between the steps, for the crash tests.
	hook func(step string) error
}

// Result is what one run did to one file.
type Result struct {
	File     string
	Base     int64 // the live file's base before the run
	Cut      int64 // bytes moved to the archive
	Lines    int64 // lines moved
	Live     int64 // live file bytes before the run
	LiveKept int64 // live file bytes after it (at the swap)
	Segment  string
	GzBytes  int64
	Skipped  string // why nothing moved; empty when something did
}

// ErrUnsupported is Archive on a platform without flock.
var ErrUnsupported = errors.New("record: rotation needs flock, which this platform does not have")

// Archive moves the lines of path before o.Cutoff into a new gzip segment
// and replaces the live file with the rest. Each step is durable before the
// next begins, and the source bytes are not given up until the segment
// holding them has been fsynced, read back and matched by digest:
//
//  1. under archive/.lock (one run at a time; the backup takes it too),
//     undo whatever a crashed run left half done;
//  2. find the cut: the start of the first line dated at or after the
//     cutoff (or without a date), never past the last line and never past
//     o.Limit, so the live file always keeps at least one line;
//  3. write [0, cut) to <seq>-<cutoff day>.jsonl.gz.tmp, fsync, read it
//     back, compare the SHA-256 of the uncompressed bytes;
//  4. copy [cut, end) of the live file to <file>.rotate.tmp and fsync it,
//     without a lock (it can be large);
//  5. take the exclusive flock on the live file: every writer's in-flight
//     line completes first and no new one starts. Copy what was appended
//     since step 4, fsync, write the index with the new segment and the
//     new generation, rename the segment into place and fsync its
//     directory, rename the copy over the live file, fsync the directory,
//     release. Writers find the path moved and reopen it.
//
// The segment keeps its temp name until the index naming it is saved, so a
// run that fails or stops before then leaves only temp files, which it
// removes itself or the next run does; a segment file under its own name is
// always one an index lists. A crash after the index but before the live
// file's rename leaves an index naming a generation that is not the live
// file; readers find their base by the live file's first line, so they read
// the old file as it is, and the next run drops the segment and generation
// that never took effect, with the segment's file. Nothing is ever deleted
// from the live file except by the rename that replaces it with a copy of
// its tail.
//
// A live file that ends inside a line at the swap is left as it is, and
// the run reports it in Skipped rather than as an error: no tail is copied
// over a torn line, and the line's writer finishes or cuts it (the
// observer's own writers when they restart, deploy/vantage-pull.sh, which
// appends whole lines only, with its next pull), so a later run archives
// the file. It is not the run's failure, and the other files' archiving and
// the retirement after it go on.
func Archive(path string, o Options) (Result, error) {
	res := Result{File: filepath.Base(path)}
	if !rotationSupported {
		return res, ErrUnsupported
	}
	if o.Now.IsZero() {
		o.Now = time.Now()
	}
	if o.TimeField == "" {
		return res, errors.New("record: no time field")
	}
	adir := ArchiveDir(path)
	if err := os.MkdirAll(adir, 0o755); err != nil {
		return res, err
	}
	lk, err := os.OpenFile(filepath.Join(filepath.Dir(adir), LockFile), os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return res, err
	}
	defer lk.Close()
	// Owned like the data directory, whoever runs this: the service user
	// must be able to take the lock and write here on the next run.
	if di, err := os.Stat(filepath.Dir(path)); err == nil {
		for _, p := range []string{filepath.Dir(adir), adir, lk.Name()} {
			if err := ownLike(p, di); err != nil {
				return res, err
			}
		}
	}
	if err := lockExclusive(lk); err != nil {
		return res, fmt.Errorf("archive lock: %w", err)
	}
	defer unlock(lk)

	f, err := os.Open(path)
	if os.IsNotExist(err) {
		res.Skipped = "no such file"
		return res, nil
	}
	if err != nil {
		return res, err
	}
	defer f.Close()

	idx, base, err := recoverIndex(path, f, o.DryRun)
	if err != nil {
		return res, err
	}
	res.Base = base
	info, err := f.Stat()
	if err != nil {
		return res, err
	}
	res.Live = info.Size()

	cut, lines, err := findCut(f, o.Cutoff, o.TimeField, limitIn(o.Limit, base))
	if err != nil {
		return res, err
	}
	if cut == 0 {
		res.Skipped = "nothing to archive before " + o.Cutoff.UTC().Format(time.RFC3339)
		if o.Limit >= 0 && o.Limit <= base {
			res.Skipped += " that the export has passed"
		}
		return res, nil
	}
	res.Cut, res.Lines = cut, lines
	if o.DryRun {
		res.LiveKept = res.Live - cut
		return res, nil
	}
	if err := step(o, "start"); err != nil {
		return res, err
	}

	// 3. the segment
	seg := Segment{
		Name:       fmt.Sprintf("%06d-%s.jsonl.gz", len(idx.Segments)+1, o.Cutoff.UTC().Format("2006-01-02")),
		From:       base,
		To:         base + cut,
		Lines:      lines,
		Cutoff:     o.Cutoff.UTC(),
		ArchivedAt: o.Now.UTC(),
	}
	segPath := filepath.Join(adir, seg.Name)
	segTmp := segPath + ".tmp"
	placed := false
	defer func() {
		// A run that ends before the segment is renamed into place takes its
		// temp file back: the live file still holds its bytes.
		if !placed {
			_ = os.Remove(segTmp)
		}
	}()
	if err := writeSegment(f, cut, segTmp, &seg); err != nil {
		return res, err
	}
	res.Segment, res.GzBytes = seg.Name, seg.GzBytes
	if err := step(o, "segment"); err != nil {
		return res, err
	}

	// 4. the tail, unlocked
	tmpPath := path + ".rotate.tmp"
	_ = os.Remove(tmpPath)
	tmp, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, info.Mode().Perm())
	if err != nil {
		return res, err
	}
	// the replacement takes the live file's owner and mode, whatever the
	// umask or the user running this: the writers reopen it for append
	if err := errors.Join(ownLike(tmpPath, info), tmp.Chmod(info.Mode().Perm())); err != nil {
		tmp.Close()
		_ = os.Remove(tmpPath)
		return res, err
	}
	defer func() {
		tmp.Close()
		_ = os.Remove(tmpPath) // gone already after a successful rename
	}()
	end1, err := lastLineEnd(f, info.Size())
	if err != nil {
		return res, err
	}
	if _, err := io.Copy(tmp, io.NewSectionReader(f, cut, end1-cut)); err != nil {
		return res, err
	}
	if err := tmp.Sync(); err != nil {
		return res, err
	}
	if err := step(o, "tail"); err != nil {
		return res, err
	}

	// 5. the swap, under the writers' lock
	if err := lockExclusive(f); err != nil {
		return res, fmt.Errorf("lock %s: %w", path, err)
	}
	defer unlock(f)
	now, err := os.Stat(path)
	if err != nil {
		return res, err
	}
	held, err := f.Stat()
	if err != nil {
		return res, err
	}
	if !os.SameFile(now, held) {
		return res, fmt.Errorf("%s was replaced during the run; nothing swapped (run again)", path)
	}
	end2 := held.Size()
	if end2 < end1 {
		return res, fmt.Errorf("%s shrank from %d to %d bytes during the run; nothing swapped", path, end1, end2)
	}
	if end2 > end1 {
		last := make([]byte, 1)
		if _, err := f.ReadAt(last, end2-1); err != nil {
			return res, err
		}
		if last[0] != '\n' {
			// Every writer appends whole lines under the shared lock, which
			// this run now holds exclusively, so a torn line here was left
			// by a writer that died mid-line, or by a copier that stopped
			// part-way: its restart, or the copier's next run, finishes or
			// cuts the line. Nothing is swapped over it, and this run's
			// segment is taken back.
			return Result{File: res.File, Base: res.Base, Live: res.Live,
				Skipped: "ends inside a line (a torn write), so nothing was swapped; its writer finishes or cuts the line (the observer's own writers when they restart, vantage-pull with its next pull), and a later run archives the file"}, nil
		}
		if _, err := io.Copy(tmp, io.NewSectionReader(f, end1, end2-end1)); err != nil {
			return res, err
		}
	}
	if err := tmp.Sync(); err != nil {
		return res, err
	}
	head, err := headOf(f, cut)
	if err != nil {
		return res, err
	}
	if len(idx.Generations) == 0 {
		oldHead, err := headOf(f, 0)
		if err != nil {
			return res, err
		}
		idx.Generations = append(idx.Generations, Generation{Base: 0, Head: oldHead, At: o.Now.UTC()})
	}
	idx.Version = indexVersion
	idx.File = filepath.Base(path)
	idx.TimeField = o.TimeField
	idx.Segments = append(idx.Segments, seg)
	idx.Generations = append(idx.Generations, Generation{Base: base + cut, Head: head, At: o.Now.UTC()})
	if o.Cutoff.After(idx.LiveSince) {
		idx.LiveSince = o.Cutoff.UTC()
	}
	if err := idx.save(adir); err != nil {
		return res, err
	}
	if err := step(o, "index"); err != nil {
		return res, err
	}
	// The segment under its own name, durable before the live file gives up
	// its bytes: a crash between the two renames leaves a segment the next
	// run drops, with the index entry that never took effect.
	if err := os.Rename(segTmp, segPath); err != nil {
		return res, err
	}
	placed = true
	if err := syncDir(adir); err != nil {
		return res, err
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return res, err
	}
	if err := syncDir(filepath.Dir(path)); err != nil {
		return res, err
	}
	res.LiveKept = end2 - cut
	return res, nil
}

func step(o Options, name string) error {
	if o.hook == nil {
		return nil
	}
	return o.hook(name)
}

// limitIn turns a logical cap into one on the live file's own offsets.
func limitIn(limit, base int64) int64 {
	if limit < 0 {
		return -1
	}
	return max(limit-base, 0)
}

// recoverIndex loads path's index, finds the live file's generation and
// takes back what a crashed run left: segments and generations past the
// live file's base (written, never swapped in) and temp files. It refuses a
// live file that matches no generation of an index that has segments: the
// file was replaced by something this archive does not describe, and
// cutting it would misplace every offset.
func recoverIndex(path string, f *os.File, dry bool) (*Index, int64, error) {
	idx, err := LoadIndex(path)
	if err != nil {
		return nil, 0, err
	}
	if len(idx.Generations) == 0 {
		if len(idx.Segments) > 0 {
			return nil, 0, fmt.Errorf("%s: archive index has segments but no generations", path)
		}
		return idx, 0, nil
	}
	head, err := headOf(f, 0)
	if err != nil {
		return nil, 0, err
	}
	base, ok := idx.base(head)
	if !ok {
		return nil, 0, fmt.Errorf("%s: the live file's first line matches no generation in %s; it was replaced outside the archiver, refusing to cut it", path, filepath.Join(ArchiveDir(path), IndexFile))
	}
	adir := ArchiveDir(path)
	var segs []Segment
	var stale []string
	at := int64(0)
	for _, sg := range idx.Segments {
		if sg.To > base {
			stale = append(stale, sg.Name)
			continue
		}
		if sg.From != at {
			return nil, 0, fmt.Errorf("%s: archive segments leave a gap at logical byte %d", path, at)
		}
		at = sg.To
		segs = append(segs, sg)
	}
	if at != base {
		return nil, 0, fmt.Errorf("%s: archive segments end at %d, the live file starts at %d", path, at, base)
	}
	var gens []Generation
	for _, g := range idx.Generations {
		if g.Base <= base {
			gens = append(gens, g)
		}
	}
	changed := len(segs) != len(idx.Segments) || len(gens) != len(idx.Generations)
	idx.Segments, idx.Generations = segs, gens
	if dry {
		return idx, base, nil
	}
	if changed {
		if err := idx.save(adir); err != nil {
			return nil, 0, err
		}
		for _, n := range stale {
			_ = os.Remove(filepath.Join(adir, n))
		}
	}
	// Leftovers of a run that stopped before its index: never referenced.
	_ = os.Remove(path + ".rotate.tmp")
	if ents, err := os.ReadDir(adir); err == nil {
		for _, e := range ents {
			if strings.HasSuffix(e.Name(), ".tmp") {
				_ = os.Remove(filepath.Join(adir, e.Name()))
			}
		}
	}
	return idx, base, nil
}

// findCut scans f from the start for the first line dated at or after
// cutoff, or undated, and returns its offset and the number of lines before
// it. The cut never passes the start of the last complete line, nor limit
// (>= 0).
func findCut(f *os.File, cutoff time.Time, field string, limit int64) (int64, int64, error) {
	r := bufio.NewReaderSize(io.NewSectionReader(f, 0, 1<<62), 1<<20)
	var pos, lines, lastStart, lastLines int64
	for {
		raw, err := r.ReadBytes('\n')
		if err != nil {
			if errors.Is(err, io.EOF) {
				// no line at or after the cutoff: keep the last complete one
				return lastStart, lastLines, nil
			}
			return 0, 0, err
		}
		if limit >= 0 && pos+int64(len(raw)) > limit {
			return pos, lines, nil
		}
		t, ok := lineTime(raw, field)
		if !ok || !t.Before(cutoff) {
			return pos, lines, nil
		}
		lastStart, lastLines = pos, lines
		pos += int64(len(raw))
		lines++
	}
}

// lineTime reads field from one JSON line as a time.
func lineTime(line []byte, field string) (time.Time, bool) {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(bytes.TrimSpace(line), &m); err != nil {
		return time.Time{}, false
	}
	raw, ok := m[field]
	if !ok {
		return time.Time{}, false
	}
	var t time.Time
	if err := json.Unmarshal(raw, &t); err != nil || t.IsZero() {
		return time.Time{}, false
	}
	return t, true
}

// writeSegment writes f's first n bytes gzipped to tmp, fsyncs it, reads it
// back and requires the same bytes. The caller renames it into place once
// the index naming it is saved.
func writeSegment(f *os.File, n int64, tmp string, seg *Segment) error {
	out, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	raw := sha256.New()
	gzh := sha256.New()
	cw := &countWriter{w: io.MultiWriter(out, gzh)}
	z, err := gzip.NewWriterLevel(cw, gzip.BestCompression)
	if err != nil {
		out.Close()
		return err
	}
	if _, err := io.Copy(io.MultiWriter(z, raw), io.NewSectionReader(f, 0, n)); err != nil {
		out.Close()
		return err
	}
	if err := z.Close(); err != nil {
		out.Close()
		return err
	}
	if err := out.Sync(); err != nil {
		out.Close()
		return err
	}
	if err := out.Close(); err != nil {
		return err
	}
	seg.SHA256 = hex.EncodeToString(raw.Sum(nil))
	seg.GzSHA256 = hex.EncodeToString(gzh.Sum(nil))
	seg.GzBytes = cw.n
	if err := VerifySegment(tmp, *seg); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("segment did not read back: %w", err)
	}
	return nil
}

type countWriter struct {
	w io.Writer
	n int64
}

func (c *countWriter) Write(b []byte) (int, error) {
	n, err := c.w.Write(b)
	c.n += int64(n)
	return n, err
}

// VerifySegment reads the gzip file at path and checks it against seg: the
// file's digest and size, and the digest, length and line count of what it
// decompresses to.
func VerifySegment(path string, seg Segment) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	gzh := sha256.New()
	cr := &countReader{r: io.TeeReader(f, gzh)}
	z, err := gzip.NewReader(cr)
	if err != nil {
		return fmt.Errorf("%s: %w", seg.Name, err)
	}
	raw := sha256.New()
	lc := &lineCounter{h: raw}
	n, err := io.Copy(lc, z)
	if err != nil {
		return fmt.Errorf("%s: %w", seg.Name, err)
	}
	if _, err := io.Copy(io.Discard, cr); err != nil {
		return err
	}
	switch {
	case n != seg.To-seg.From:
		return fmt.Errorf("%s: %d bytes, index says %d", seg.Name, n, seg.To-seg.From)
	case hex.EncodeToString(raw.Sum(nil)) != seg.SHA256:
		return fmt.Errorf("%s: sha256 of the lines differs from the index", seg.Name)
	case lc.lines != seg.Lines:
		return fmt.Errorf("%s: %d lines, index says %d", seg.Name, lc.lines, seg.Lines)
	case seg.GzSHA256 != "" && hex.EncodeToString(gzh.Sum(nil)) != seg.GzSHA256:
		return fmt.Errorf("%s: sha256 of the file differs from the index", seg.Name)
	case seg.GzBytes != 0 && cr.n != seg.GzBytes:
		return fmt.Errorf("%s: file is %d bytes, index says %d", seg.Name, cr.n, seg.GzBytes)
	}
	return nil
}

type countReader struct {
	r io.Reader
	n int64
}

func (c *countReader) Read(b []byte) (int, error) {
	n, err := c.r.Read(b)
	c.n += int64(n)
	return n, err
}

type lineCounter struct {
	h     hash.Hash
	lines int64
}

func (l *lineCounter) Write(b []byte) (int, error) {
	l.lines += int64(bytes.Count(b, []byte{'\n'}))
	return l.h.Write(b)
}

// Verify checks every segment of path's archive against its index and that
// they run without a gap up to the live file's base. A retired segment
// whose file is gone is checked by reading it back from the exports. It
// returns the number of segments checked.
func Verify(path string) (int, error) {
	s, err := Open(path)
	if err != nil {
		return 0, err
	}
	defer s.Close()
	return s.verify()
}

// verify is Verify over the index s was opened with. A Retire that runs
// meanwhile removes a segment's file after it saves the index, so a file
// found gone is looked up in the index as it is now (retiredNow) before the
// segment is called lost.
func (s *Stream) verify() (int, error) {
	path := s.path
	idx := s.Index()
	if len(idx.Generations) > 0 {
		head, err := headOf(s.live, 0)
		if err != nil {
			return 0, err
		}
		if _, ok := idx.base(head); !ok {
			return 0, fmt.Errorf("%s: the live file's first line matches no generation in the index", path)
		}
	}
	at, n := int64(0), 0
	for _, sg := range idx.Segments {
		if sg.To > s.base {
			continue // written by a run that never swapped; dropped by the next one
		}
		if sg.From != at {
			return n, fmt.Errorf("%s: gap in the archive at logical byte %d", path, at)
		}
		err := VerifySegment(filepath.Join(ArchiveDir(path), sg.Name), sg)
		if errors.Is(err, os.ErrNotExist) {
			if sg = retiredNow(path, sg); sg.Retired == nil {
				// Not wrapped: to a caller, os.ErrNotExist from Verify
				// means the live file is missing, which is no failure.
				return n, fmt.Errorf("%s: segment %s is gone and was never retired: %v", path, sg.Name, err)
			}
			err = checkFromExports(path, sg)
		}
		if err != nil {
			return n, err
		}
		at = sg.To
		n++
	}
	if at != s.base {
		return n, fmt.Errorf("%s: archive ends at %d, live file starts at %d", path, at, s.base)
	}
	return n, nil
}
