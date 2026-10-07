package collect

import (
	"sort"
	"strings"
	"sync"
	"time"
)

// WorkErrorsKey is the status-file detail the work list is published under.
const WorkErrorsKey = "work_errors"

// WorkErrors is the collector's list of the work that is failing apart from
// the ingest: the exports, the registry write, the corrections, the hold
// sync, the rollups, the run heartbeat, the hosting lookup and the like. It
// is published on the status file as work_errors, one entry per failing
// stage with its latest error and the time it started failing, and an entry
// goes when its stage next succeeds.
//
// The status file's OK cannot carry these. It says that the last poll pass
// ingested every file and reached the chain, it is set every minute, and an
// error written by any of this work in between was overwritten by it: an
// export that stopped for good (an operator's index.json gone), a hold sync
// that kept failing, a registry line that could not be written, all read as
// "alive, last cycle failed" with ok:true, and nothing alerted. /v1/health
// reads this list instead, and fails while an entry has stood long enough.
//
// Safe for concurrent use: the avatar resolver reports from its own
// goroutine. A nil *WorkErrors records nothing.
type WorkErrors struct {
	live Status
	mu   sync.Mutex
	errs map[string]workError
}

type workError struct {
	msg   string
	since time.Time
}

// NewWorkErrors is an empty list published to live (nil: kept in memory
// only). The empty list is published at once, so a reader can tell "nothing
// is failing" from a collector that predates the list.
func NewWorkErrors(live Status) *WorkErrors {
	w := &WorkErrors{live: live, errs: map[string]workError{}}
	w.mu.Lock()
	w.publish()
	w.mu.Unlock()
	return w
}

// Report records how stage went at now: a nil err clears it; any other is
// its latest error, dated from the first failure since the stage last
// succeeded. The status file is rewritten only when the list changes.
func (w *WorkErrors) Report(stage string, err error, now time.Time) {
	if w == nil {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	old, had := w.errs[stage]
	switch {
	case err == nil && !had:
		return
	case err == nil:
		delete(w.errs, stage)
	case had && old.msg == err.Error():
		return
	case had:
		w.errs[stage] = workError{msg: err.Error(), since: old.since}
	default:
		w.errs[stage] = workError{msg: err.Error(), since: now.UTC()}
	}
	w.publish()
}

// Failing names the stages failing now, sorted, each with its error.
func (w *WorkErrors) Failing() []string {
	if w == nil {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	out := make([]string, 0, len(w.errs))
	for stage, e := range w.errs {
		out = append(out, stage+": "+e.msg)
	}
	sort.Strings(out)
	return out
}

// Since is when stage started failing; zero when it is not failing.
func (w *WorkErrors) Since(stage string) time.Time {
	if w == nil {
		return time.Time{}
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.errs[stage].since
}

// publish writes the list, a fresh map every time: the status writer
// marshals what it holds under its own lock, and a map this side kept
// changing would race it. Called with w.mu held.
func (w *WorkErrors) publish() {
	if w.live == nil {
		return
	}
	m := make(map[string]any, len(w.errs))
	for stage, e := range w.errs {
		m[stage] = map[string]any{"error": strings.TrimSpace(e.msg), "since": e.since.Format(time.RFC3339)}
	}
	w.live.Set(WorkErrorsKey, m)
}
