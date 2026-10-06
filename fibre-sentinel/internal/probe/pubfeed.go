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
	"path/filepath"
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
// a hard error, as before: a corrupt record must not be silently dropped,
// so the line is not consumed either, and every refresh stops at it again.
//
// observer-archive rotates the file: it moves the older lines into a
// segment and puts a copy of the rest in the file's place, under the same
// path. The feed keeps a logical offset (internal/record), which names the
// same byte whichever file holds it: every refresh opens the record as it
// is now (record.Open places the live file by its first line in the
// archive index) and reads on from that offset. Lines cut before the feed
// read them come back from their segment, or from the daily exports once
// it is retired, through the same reader, so no line is skipped or read
// twice. The feed does not tell files apart by identity: a file a rotation
// creates can be given the inode of one the feed read and closed before
// (ext4 hands a freed inode to the next file at once), and then compares
// equal to it while holding other bytes at every offset.
//
// The feed begins at the live file's first byte, not the record's. The
// lines archived are dated before a cutoff at least the longest retention
// window plus a day old (observer-archive refuses a shorter -keep), so
// every publication whose schedule can still matter is in the live file,
// and a prober that starts reads none of the archive.
//
// The prober's cycle loop refreshes and forgets while its readings, on
// goroutines of their own, look up shadowing promises. mu guards the
// publications and is held only to take in what a refresh has read, never
// over the read itself, which can be long: the first load of a live file
// of days of publications, or lines read back from the archive.
type pubFeed struct {
	path string

	// reading is held for the whole of a refresh, so two never read the
	// same lines, and guards the fields up to mu. offset is the logical
	// offset of the first byte the feed has not read, and lines the number
	// of lines before it in the whole record (-1 when the index does not
	// say how many lines precede the byte the feed began at). last is the
	// SHA-256 of the line that ends at offset, which starts at lastFrom: ""
	// when nothing has been read, and the next refresh places the feed
	// afresh.
	reading  sync.Mutex
	offset   int64
	lines    int64
	last     string
	lastFrom int64

	mu    sync.RWMutex
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
// A record that does not go on from what the feed has read (goesOn: the
// file was rewritten in place, or put in the path's place outside the
// archiver) is a record of its own: everything held is forgotten and it is
// read from its live file's start.
func (f *pubFeed) refresh() (int, error) {
	f.reading.Lock()
	defer f.reading.Unlock()
	s, err := record.Open(f.path)
	if err != nil {
		return 0, fmt.Errorf("open %s: %w", f.path, err)
	}
	defer s.Close()
	same, err := f.goesOn(s)
	if err != nil {
		return 0, err
	}
	at, lines := f.offset, f.lines
	if !same || f.last == "" {
		at, lines = s.Base(), linesBefore(s.Index(), s.Base())
	}
	src, err := s.ReaderFrom(at)
	if err != nil {
		return 0, fmt.Errorf("%s from logical byte %d: %w", f.path, at, err)
	}
	r := bufio.NewReaderSize(src, 1<<20)
	var (
		got      []scan.Publication
		last     []byte
		lastFrom int64
		stop     error
	)
	for {
		raw, err := r.ReadBytes('\n')
		if err != nil {
			if !errors.Is(err, io.EOF) {
				stop = err
			}
			break // a partial trailing line waits for its newline
		}
		if trimmed := bytes.TrimSpace(raw); len(trimmed) > 0 {
			var p scan.Publication
			if err := json.Unmarshal(trimmed, &p); err != nil {
				stop = fmt.Errorf("%s: %w", f.lineName(s, at, lines), err)
				break
			}
			if p.PromiseHash == "" {
				stop = fmt.Errorf("%s: publication without promise_hash", f.lineName(s, at, lines))
				break
			}
			got = append(got, p)
		}
		last, lastFrom = raw, at
		at += int64(len(raw))
		if lines >= 0 {
			lines++
		}
	}
	added := f.take(got, !same)
	f.offset, f.lines = at, lines
	switch {
	case last != nil:
		f.last, f.lastFrom = lineSHA256(last), lastFrom
	case !same:
		f.last = ""
	}
	return added, stop
}

// goesOn reports whether the record s opens goes on from what the feed has
// read, by the line read last: it must still end at the feed's offset.
// While that line is in the live file it is read again and compared, which
// is one line, not a file. Once a rotation has moved it to the archive, the
// live file starts at or after its end: record.Open placed it there by its
// own first line in the index (it gives a live file the index does not
// place base 0, so such a file is always compared). A record shorter than
// what was read does not go on, nor one whose live file starts inside the
// line read last. With nothing read yet there is nothing to compare, and
// the feed is placed afresh in any case.
func (f *pubFeed) goesOn(s *record.Stream) (bool, error) {
	base := s.Base()
	switch {
	case f.last == "":
		return true, nil
	case s.End() < f.offset:
		return false, nil
	case f.lastFrom < base:
		return f.offset <= base, nil
	}
	r, err := s.ReaderFrom(f.lastFrom)
	if err != nil {
		return false, err
	}
	h := sha256.New()
	if _, err := io.CopyN(h, r, f.offset-f.lastFrom); errors.Is(err, io.EOF) {
		return false, nil // cut short since it was opened
	} else if err != nil {
		return false, err
	}
	return hex.EncodeToString(h.Sum(nil)) == f.last, nil
}

// take adds the publications a refresh read, in file order, after
// forgetting everything held when the record is read from its start again.
// It returns how many were not held yet.
func (f *pubFeed) take(got []scan.Publication, reload bool) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	if reload {
		f.pubs = map[string]scan.Publication{}
		f.order = nil
		f.byCommit = map[string][]string{}
	}
	added := 0
	for _, p := range got {
		if _, dup := f.pubs[p.PromiseHash]; !dup {
			f.order = append(f.order, p.PromiseHash)
			f.byCommit[p.Promise.Commitment] = append(f.byCommit[p.Promise.Commitment], p.PromiseHash)
			added++
		}
		f.pubs[p.PromiseHash] = p
	}
	return added
}

// lineName names the line that starts at logical byte from, with before
// lines ahead of it in the whole record (-1 when that is not known), for
// an error. It is named by its line in the file that holds it, the live
// file or a segment, counted from the lines the index says the segments
// before that file hold; by its line in the whole record when the index
// does not list those segments; by its byte when not even that is known.
// Counting the lines by reading them again would hold up the refresh for a
// nicety.
func (f *pubFeed) lineName(s *record.Stream, from, before int64) string {
	idx := s.Index()
	file, start := f.path, s.Base()
	if from < start {
		for _, sg := range idx.Segments {
			if sg.From <= from && from < sg.To {
				file, start = filepath.Join(record.ArchiveDir(f.path), sg.Name), sg.From
				break
			}
		}
	}
	if before < 0 {
		return fmt.Sprintf("%s logical byte %d", f.path, from)
	}
	if ahead := linesBefore(idx, start); ahead >= 0 {
		return fmt.Sprintf("%s line %d", file, before-ahead+1)
	}
	return fmt.Sprintf("%s line %d of the whole record", f.path, before+1)
}

// linesBefore is the number of lines in logical bytes [0, at), from the
// segments of idx that hold them; -1 when the segments listed do not hold
// exactly that range, one after another.
func linesBefore(idx *record.Index, at int64) int64 {
	var lines, end int64
	for _, sg := range idx.Segments {
		if sg.From >= at {
			continue // after it, or written by a run that never swapped
		}
		if sg.From != end || sg.To > at {
			return -1
		}
		lines += sg.Lines
		end = sg.To
	}
	if end != at {
		return -1
	}
	return lines
}

// lineSHA256 is the digest of one line, newline included.
func lineSHA256(line []byte) string {
	sum := sha256.Sum256(line)
	return hex.EncodeToString(sum[:])
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
