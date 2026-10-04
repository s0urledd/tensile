package main

import (
	"errors"
	"os"
	"strings"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/observer/ingest"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
)

// fastTick is the light read between full passes: what a blob the chain has
// just settled needs before /v1/blobs can serve its row, and nothing else.
//
// The full pass tails every file the observer keeps and then does the work
// that follows from them, every -interval (ten seconds). A blob the scanner
// wrote a moment after a pass had read publications.jsonl waited for the next
// one, so the overview's newest blob reached the store up to ten seconds after
// its block, on top of the scanner's second. Between passes this reads, every
// -fast-every (a second) and within a tenth of a second of a change to one of
// them (-fast-watch, watch.go), the three files a just-settled blob's row is
// made of, in the order the pass reads them:
//
//   - state.json, the scanner's checkpoint, first. The scanner syncs
//     publications.jsonl before it rewrites state.json, so a checkpoint read
//     first never names a block whose publications are not read straight
//     after it; read the other way round, the scan frontier the late shadow
//     verdicts are judged against could run ahead of the publications in the
//     store. Read here too so the checkpoint the snapshots name
//     (record_through) does not fall a pass behind the publications this
//     stores.
//   - publications.jsonl: the row, and its assignments.
//   - payments.jsonl: its charge, and the account that paid, which the row
//     names as its publisher where a payment is on record. Without it a new
//     row would read "no payment recorded" until the next pass.
//
// Ordering. Everything this leaves in the store is what a full pass that
// stopped after these three files would leave, which the pass has always had
// to survive (a crash, a restart), and the invariants the pass's comments set
// out hold across it:
//
//   - A publication is born withheld when a range already in the store covers
//     it (store.UpsertPublication), and a range that lands later raises the
//     hold on every publication already stored, in the transaction that
//     records it (store.InsertParamUncertainty). A publication stored here
//     before its range is in the place of one stored at the head of a pass,
//     and the pass already reads publications before ranges.
//   - Nothing here reads ranges, corrections, measurements or sampling
//     decisions, or syncs a hold: no measurement of a publication stored here
//     can reach the store before the next full pass, which brings the ranges
//     and corrections in before the measurements and syncs the holds after
//     both, as it always has.
//   - A payment is an insert on its own dedupe key, from which nothing is
//     derived at insert time.
//
// Cost. It runs on the collector's one goroutine, between passes, so it never
// writes beside a pass. When nothing is new it costs a stat of each file:
// a file whose identity, size and modification time are what they were when
// this last read it to its end is not opened and the store is not asked. A
// file that changed is tailed from its cursor, which reads only what was
// appended and writes the cursor only after reading a complete line, so a half
// written line costs a cursor read and no write. The rest of what a pass does
// (the WAL checkpoint, the holds, the corrections, the rollups, the exports,
// the chain polls) stays in the pass.
//
// Health. A failure to read publications or payments is logged and reported on
// the status file as a pass reports one ("ingest: ..."), so /v1/health sees
// it; this never reports the collector healthy, which only a full pass whose
// chain poll succeeded does. A failure that persists is logged when it first
// appears and when it changes, not every second (the pass logs it every ten
// seconds regardless), and the status file is told every time. A failure to
// read state.json is logged only, as the pass logs it.
type fastTick struct {
	st    *store.Store
	logf  func(string, ...any)
	fail  func(string) // the status file's Error
	state fastFile
	files []fastFile
	// lastErr is the failures the last tick logged, joined.
	lastErr string
}

type fastFile struct {
	what string
	path string
	tail func(*store.Store, string, time.Time) (ingest.Result, error)
	// seen is the file as it was when it was last read to its end; nil
	// before the first read, or while the file is missing.
	seen os.FileInfo
}

func newFastTick(st *store.Store, statePath, pubsPath, payPath string, logf func(string, ...any), fail func(string)) *fastTick {
	return &fastTick{
		st:    st,
		logf:  logf,
		fail:  fail,
		state: fastFile{what: "state", path: statePath},
		files: []fastFile{
			{what: "publications", path: pubsPath, tail: ingest.Publications},
			{what: "payments", path: payPath, tail: ingest.Payments},
		},
	}
}

// run is one tick.
func (f *fastTick) run(now time.Time) {
	// every failure, for the log; failed, the ones the status file is told
	var errs, failed []string
	if fi, same := unchanged(f.state.seen, f.state.path); !same {
		if err := ingest.State(f.st, f.state.path, now); err != nil {
			errs = append(errs, "state: "+err.Error())
		} else {
			f.state.seen = fi
		}
	}
	for i := range f.files {
		ff := &f.files[i]
		fi, same := unchanged(ff.seen, ff.path)
		if same {
			continue
		}
		r, err := ff.tail(f.st, ff.path, now)
		if err != nil {
			e := ff.what + ": " + err.Error()
			errs, failed = append(errs, e), append(failed, e)
			continue
		}
		logTail(f.logf, ff.what, r)
		if r.Deferred == "" {
			ff.seen = fi
		}
	}
	if msg := strings.Join(errs, "; "); msg != f.lastErr {
		for _, e := range errs {
			f.logf("%s", e)
		}
		if msg == "" {
			f.logf("fast tick: reading without errors again")
		}
		f.lastErr = msg
	}
	if len(failed) > 0 {
		f.fail("ingest: " + strings.Join(failed, "; "))
	}
}

// unchanged returns the file at path and whether it is seen as it was: the
// same file, with the same size and modification time. The record files are
// only appended to, and an archive rotation replaces the live file with
// another, so a file that is all three is one with nothing new in it. A file
// that is missing is unchanged when it was missing before; one that cannot be
// stat'ed for another reason never is, and the read that follows says why.
func unchanged(seen os.FileInfo, path string) (os.FileInfo, bool) {
	fi, err := os.Stat(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, seen == nil
		}
		return nil, false
	}
	return fi, seen != nil && os.SameFile(seen, fi) && seen.Size() == fi.Size() && seen.ModTime().Equal(fi.ModTime())
}

// logTail logs one tail of a file as the pass always has: what it added, and
// any line it had to step over.
func logTail(logf func(string, ...any), what string, r ingest.Result) {
	if r.Inserted > 0 {
		logf("%s: +%d (read %d, line %d)", what, r.Inserted, r.Read, r.Line)
	}
	if r.Skipped > 0 {
		logf("%s: WARNING skipped %d undecodable line(s); last: %s", what, r.Skipped, r.LastSkipped)
	}
}
