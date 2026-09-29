package probe

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"

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
// The prober's cycle loop refreshes and forgets while its readings, on
// goroutines of their own, look up shadowing promises: mu guards every
// field below it.
type pubFeed struct {
	path string

	mu     sync.RWMutex
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

// refresh reads new complete records. It returns how many were added. If the
// file shrank (rewritten), everything is reloaded from the start.
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
	if info.Size() < f.offset {
		f.offset, f.line = 0, 0
		f.pubs = map[string]scan.Publication{}
		f.order = nil
		f.byCommit = map[string][]string{}
	}
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
