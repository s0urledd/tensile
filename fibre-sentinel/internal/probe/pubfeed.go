package probe

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/record"
	"github.com/plsgiveup/fibre/fibre-sentinel/internal/scan"
)

// pubFeed tails publications.jsonl incrementally: every cycle it reads only
// the bytes appended since the last one, keeps the publications whose
// schedule can still matter, and forgets the rest. Parsing the whole file
// every 30 s (the previous behaviour) grows without bound: with row lists a
// record is ~100 KB, a week at one publication a minute is gigabytes of JSON
// per cycle.
//
// A trailing partial line (the scanner mid-write, or a torn tail after a
// crash) is left unread until it is complete. A malformed complete line is
// a hard error, as before: a corrupt record must not be silently dropped.
//
// observer-archive rotates the file: it moves the older lines into a
// segment and puts a copy of the rest in the file's place, under the same
// path. The feed tells by the file's identity, since the copy can be longer
// than the offset read in the old file as easily as shorter (refresh).
//
// The prober's cycle loop refreshes and forgets while its readings, on
// goroutines of their own, look up shadowing promises: mu guards every
// field below it.
type pubFeed struct {
	path string

	mu sync.RWMutex
	// read is the file read last (nil before the first refresh), head the
	// SHA-256 of its first line once read ("" until then), and offset and
	// line how far into it the feed has read.
	read   os.FileInfo
	head   string
	offset int64
	line   int64

	pubs  map[string]scan.Publication // by promise hash
	order []string                    // hashes in file order
	// byCommit indexes live publications by blob commitment: two promises
	// over the same blob share a shard on every validator, which is what
	// makes SHADOWED_SHARD a finding rather than a guess.
	byCommit map[string][]string
}

func newPubFeed(path string) *pubFeed {
	return &pubFeed{path: path, pubs: map[string]scan.Publication{}, byCommit: map[string][]string{}}
}

// shadowersFor lists the other live promises over the same commitment and
// the rows each assigns to addr. Empty when this promise is the only one.
func (f *pubFeed) shadowersFor(hash, commitment, addr string) []ShadowCandidate {
	f.mu.RLock()
	defer f.mu.RUnlock()
	var out []ShadowCandidate
	for _, h := range f.byCommit[commitment] {
		if h == hash {
			continue
		}
		p, ok := f.pubs[h]
		if !ok {
			continue
		}
		for _, v := range p.Assignment.Validators {
			if v.Address == addr && len(v.Rows) > 0 {
				out = append(out, ShadowCandidate{PromiseHash: h, Rows: v.Rows})
				break
			}
		}
	}
	return out
}

// refresh reads new complete records. It returns how many were added.
//
// When the path names another file than the one read last, the archiver
// rotated it (rotated says how the feed goes on in the new one). When the
// same file shrank, it was rewritten in place, and everything is reloaded
// from the start.
func (f *pubFeed) refresh() (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	fh, err := os.Open(f.path)
	if err != nil {
		return 0, fmt.Errorf("open %s: %w", f.path, err)
	}
	defer fh.Close()
	info, err := fh.Stat()
	if err != nil {
		return 0, err
	}
	switch {
	case f.read != nil && !os.SameFile(f.read, info):
		if err := f.rotated(fh, info.Size()); err != nil {
			return 0, err
		}
	case info.Size() < f.offset:
		f.reset()
	}
	f.read = info
	if _, err := fh.Seek(f.offset, io.SeekStart); err != nil {
		return 0, err
	}
	r := bufio.NewReaderSize(fh, 1<<20)
	added := 0
	for {
		raw, err := r.ReadBytes('\n')
		if err != nil {
			if errors.Is(err, io.EOF) {
				break // partial trailing line: wait for the newline
			}
			return added, err
		}
		if f.offset == 0 {
			f.head = lineSHA256(raw)
		}
		f.line++
		f.offset += int64(len(raw))
		trimmed := bytes.TrimSpace(raw)
		if len(trimmed) == 0 {
			continue
		}
		var p scan.Publication
		if err := json.Unmarshal(trimmed, &p); err != nil {
			return added, fmt.Errorf("%s line %d: %w", f.path, f.line, err)
		}
		if p.PromiseHash == "" {
			return added, fmt.Errorf("%s line %d: publication without promise_hash", f.path, f.line)
		}
		if _, dup := f.pubs[p.PromiseHash]; !dup {
			f.order = append(f.order, p.PromiseHash)
			f.byCommit[p.Promise.Commitment] = append(f.byCommit[p.Promise.Commitment], p.PromiseHash)
			added++
		}
		f.pubs[p.PromiseHash] = p
	}
	return added, nil
}

// rotated moves the feed onto fh, size bytes long, which has taken the
// place of the file read last. The feed starts again at the new file's
// first byte, and the lines of it that it had already read in the old file
// are skipped rather than decoded again: the archive index places both
// files in the one logical record, each by the digest of its first line
// (as record.Open does), so the logical offset the feed had reached falls
// at a known byte of the new file. A publication still held keeps its
// place, and one already forgotten (its schedule done, its keys dropped
// from the measurement store) is not brought back to be planned again,
// which would write a NOT_PROBED row for a reading already on record.
// Every line past that byte is new.
//
// Nothing whose schedule can still matter is lost. The feed keeps every
// publication it holds, the archived ones too, until the cycle forgets it.
// And the lines archived are dated before a cutoff at least the longest
// retention window plus a day old (observer-archive refuses a shorter
// -keep), so had the feed not read some of them yet, their schedules ended
// a day ago: every publication that can still be read is in the live file.
//
// A file the index does not place (put there outside the archiver), or one
// that does not hold what the feed had read, is a record of its own and is
// reloaded from scratch, as a file rewritten in place is.
func (f *pubFeed) rotated(fh *os.File, size int64) error {
	if f.offset == 0 {
		// nothing of the old file was read: every line of the new one is new
		f.line, f.head = 0, ""
		return nil
	}
	idx, err := record.LoadIndex(f.path)
	if err != nil {
		return err
	}
	head, err := firstLineSHA256(fh)
	if err != nil {
		return err
	}
	oldBase, okOld := generationBase(idx, f.head)
	newBase, okNew := generationBase(idx, head)
	if !okOld || !okNew || newBase < oldBase {
		f.reset()
		return nil
	}
	skip := oldBase + f.offset - newBase
	if skip <= 0 {
		// The rotation archived every line the feed had read, and any it
		// had not read yet are older than the cutoff (above).
		f.offset, f.line, f.head = 0, 0, ""
		return nil
	}
	if skip > size {
		// the new file does not reach what was read: not this record's tail
		f.reset()
		return nil
	}
	last := make([]byte, 1)
	if _, err := fh.ReadAt(last, skip-1); err != nil {
		return err
	}
	if last[0] != '\n' {
		// not at a line's end: the new file is not a copy of the old one's tail
		f.reset()
		return nil
	}
	// f.line counts the lines of logical bytes [oldBase, oldBase+offset),
	// and the segments cut since hold [oldBase, newBase) and say how many
	// lines each has: the new file's lines before skip are the difference.
	// Counting them by reading would hold mu, which the readings wait on,
	// over most of a live file of days of publications on every rotation.
	cut, ok := segmentLines(idx, oldBase, newBase)
	lines := f.line - cut
	if !ok || lines < 0 {
		// segments the index does not list: count the lines themselves
		if lines, err = countLines(fh, skip); err != nil {
			return err
		}
	}
	f.offset, f.line, f.head = skip, lines, head
	return nil
}

// segmentLines is the number of lines in logical bytes [from, to), read
// from the segments of idx that hold them. False when the segments listed
// do not hold exactly that range, one after another.
func segmentLines(idx *record.Index, from, to int64) (int64, bool) {
	var lines int64
	at := from
	for _, sg := range idx.Segments {
		if sg.To <= from || sg.From >= to {
			continue
		}
		if sg.From != at || sg.To > to {
			return 0, false
		}
		lines += sg.Lines
		at = sg.To
	}
	return lines, at == to
}

// reset forgets everything read, to read the file again from its start.
func (f *pubFeed) reset() {
	f.offset, f.line, f.head = 0, 0, ""
	f.pubs = map[string]scan.Publication{}
	f.order = nil
	f.byCommit = map[string][]string{}
}

// generationBase is the logical offset of the live file whose first line
// has this digest: the base of the newest generation in idx with that
// head. False when none has it.
func generationBase(idx *record.Index, head string) (int64, bool) {
	if head == "" {
		return 0, false
	}
	for i := len(idx.Generations) - 1; i >= 0; i-- {
		if idx.Generations[i].Head == head {
			return idx.Generations[i].Base, true
		}
	}
	return 0, false
}

// lineSHA256 is the digest a generation names its file's first line by
// (newline included).
func lineSHA256(line []byte) string {
	sum := sha256.Sum256(line)
	return hex.EncodeToString(sum[:])
}

// firstLineSHA256 is lineSHA256 of fh's first line, read without moving
// fh's offset; "" when fh holds no complete line.
func firstLineSHA256(fh *os.File) (string, error) {
	r := bufio.NewReaderSize(io.NewSectionReader(fh, 0, 1<<62), 1<<16)
	line, err := r.ReadBytes('\n')
	if errors.Is(err, io.EOF) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	return lineSHA256(line), nil
}

// countLines counts the newlines in r's first n bytes. Only an index that
// does not list the segments a rotation cut needs it (rotated).
func countLines(r io.ReaderAt, n int64) (int64, error) {
	buf := make([]byte, 1<<20)
	var lines int64
	for off := int64(0); off < n; {
		k, err := r.ReadAt(buf[:min(int64(len(buf)), n-off)], off)
		lines += int64(bytes.Count(buf[:k], []byte{'\n'}))
		off += int64(k)
		if err != nil && off < n {
			return 0, err
		}
	}
	return lines, nil
}

// forget drops one publication from memory.
func (f *pubFeed) forget(hash string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.pubs[hash]; !ok {
		return
	}
	c := f.pubs[hash].Promise.Commitment
	delete(f.pubs, hash)
	for j, h := range f.order {
		if h == hash {
			f.order = append(f.order[:j], f.order[j+1:]...)
			break
		}
	}
	hs := f.byCommit[c]
	for j, h := range hs {
		if h == hash {
			f.byCommit[c] = append(hs[:j], hs[j+1:]...)
			break
		}
	}
	if len(f.byCommit[c]) == 0 {
		delete(f.byCommit, c)
	}
}

// all returns the live publications in file order.
func (f *pubFeed) all() []scan.Publication {
	f.mu.RLock()
	defer f.mu.RUnlock()
	out := make([]scan.Publication, 0, len(f.order))
	for _, h := range f.order {
		out = append(out, f.pubs[h])
	}
	return out
}

// size is how many publications are live.
func (f *pubFeed) size() int {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return len(f.pubs)
}
