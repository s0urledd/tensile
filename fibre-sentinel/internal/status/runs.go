package status

import (
	"encoding/json"
	"os"
	"path/filepath"
	"runtime/debug"
	"strings"
	"time"
)

// Run event kinds, as written to runs.jsonl.
const (
	RunStarted = "run_started"
	RunStopped = "run_stopped"
)

// RunEvent is one process start or clean stop, appended to
// <dataDir>/runs.jsonl by the component itself. It is the durable record of
// which build, with which configuration, was producing rows when: a verdict
// is a function of the wire result and the code, and the code's
// configuration (the prune tolerance behind a phase, the schedule points,
// the timeouts) is part of that function. The collector replays the file
// into observer_runs, so it survives a database rebuild, and every daily
// export carries it.
//
// A crash writes no stop event: the run's status file stops updating, and
// the row stays open, which is what a crash looks like.
type RunEvent struct {
	Kind      string         `json:"kind"`
	Component string         `json:"component"`
	Vantage   string         `json:"vantage,omitempty"`
	Version   string         `json:"version,omitempty"`
	PID       int            `json:"pid"`
	Hostname  string         `json:"hostname,omitempty"`
	At        time.Time      `json:"at"`
	Config    map[string]any `json:"config,omitempty"`
	Reason    string         `json:"reason,omitempty"`
}

// RunsFile is the run record's file name under the data directory.
const RunsFile = "runs.jsonl"

// RecordRuns makes Start and Stop append run events to runs.jsonl, carrying
// config: the flags and derived settings the process runs under, as the
// operator would need them to reproduce a verdict. Call it before Start.
func (w *Writer) RecordRuns(config map[string]any) {
	if w == nil {
		return
	}
	w.mu.Lock()
	w.config = config
	w.recordRuns = true
	w.mu.Unlock()
}

// appendRun writes one event. Errors are swallowed: the record is
// best-effort evidence, and a full disk must not stop the process that
// produces the primary record.
func (w *Writer) appendRun(kind, reason string, at time.Time) {
	if w.dataDir == "" || !w.recordRuns {
		return
	}
	e := RunEvent{Kind: kind, Component: w.r.Component, Vantage: w.r.Vantage, Version: w.r.Version,
		PID: w.r.PID, Hostname: w.r.Hostname, At: at.UTC(), Reason: reason}
	if kind == RunStarted {
		e.Config = w.config
	}
	b, err := json.Marshal(e)
	if err != nil {
		return
	}
	f, err := os.OpenFile(filepath.Join(w.dataDir, RunsFile), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return
	}
	defer f.Close()
	if _, err := f.Write(append(b, '\n')); err == nil {
		_ = f.Sync()
	}
}

// revision is the build's revision stamped at link time
// (-ldflags "-X github.com/plsgiveup/fibre/fibre-sentinel/internal/status.revision=<rev>"),
// for builds that carry no VCS information: from a tarball, a Docker context
// without .git, or -buildvcs=false. `make build` and deploy/Dockerfile set it.
var revision string

// BuildRevision is the VCS revision the binary was built from, "-dirty" when
// the tree had uncommitted changes. Without VCS information it is the
// link-time revision, and "unknown" only when neither is there: every
// measurement carries this string, and "unknown" in the record means no one
// can say which code produced a verdict.
func BuildRevision() string {
	bi, ok := debug.ReadBuildInfo()
	if !ok {
		return stamped()
	}
	rev, dirty := "", false
	for _, kv := range bi.Settings {
		switch kv.Key {
		case "vcs.revision":
			rev = kv.Value
		case "vcs.modified":
			dirty = kv.Value == "true"
		}
	}
	if rev == "" {
		return stamped()
	}
	if len(rev) > 12 {
		rev = rev[:12]
	}
	if dirty {
		rev += "-dirty"
	}
	return rev
}

// stamped is the link-time revision, or "unknown" without one.
func stamped() string {
	if r := strings.TrimSpace(revision); r != "" {
		return r
	}
	return "unknown"
}
