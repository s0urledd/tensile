// Package status is the one-file liveness record every observer process
// keeps: <data-dir>/status/<component>.json, rewritten atomically on every
// change and at least every Interval while the process is alive.
//
// The processes share nothing but a directory: the scanner and prober never
// open the database, the collector never sees the prober's loop. Before this
// file the dashboard could say whether the collector was alive, and nothing
// about the other three; a dead prober looked like a quiet chain. The API
// reads these files straight from disk, so the answer to "is the observer
// working" does not itself depend on the collector working.
package status

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// Interval is how often a live process rewrites its file even when nothing
// happened. A reader treats a file older than a few of these as a dead
// process.
const Interval = 15 * time.Second

// StaleAfter is the age past which a status file means "not running".
const StaleAfter = 2 * time.Minute

// CadenceKey is the Detail entry in which a long-running component states,
// once at its start, the longest its work loop should go between two
// completed cycles, in whole seconds. The file itself is rewritten every
// Interval by a goroutine of its own whatever the loop is doing, so a fresh
// file says the process is alive, not that it is working: a loop blocked
// for an hour keeps its file fresh and its last OK an hour old. With the
// cadence a reader can tell the two apart (/v1/health fails a component
// whose last OK is older than a few cadences) without hard-coding each
// component's pace.
const CadenceKey = "cadence_s"

// defaults are the Detail entries every Writer this process makes from now
// on starts with (SetDefault).
var defaults struct {
	mu sync.Mutex
	m  map[string]any
}

// SetDefault records a Detail entry that every Writer this process creates
// after it starts with. It is for what a component's main knows and the
// library that keeps its status file does not: the scanner's writer is made
// inside internal/scan, and its main states the scanner's cadence through
// this before building it.
func SetDefault(key string, v any) {
	defaults.mu.Lock()
	defer defaults.mu.Unlock()
	if defaults.m == nil {
		defaults.m = map[string]any{}
	}
	defaults.m[key] = v
}

// Report is the file's content.
type Report struct {
	Component string    `json:"component"`
	Vantage   string    `json:"vantage,omitempty"`
	Version   string    `json:"version,omitempty"`
	PID       int       `json:"pid"`
	Hostname  string    `json:"hostname,omitempty"`
	StartedAt time.Time `json:"started_at"`
	UpdatedAt time.Time `json:"updated_at"`
	// StoppedAt is set by a clean shutdown; a stale UpdatedAt without it is a
	// crash or a kill.
	StoppedAt  *time.Time `json:"stopped_at,omitempty"`
	StopReason string     `json:"stop_reason,omitempty"`

	// OK is whether the last unit of work succeeded. LastError is the most
	// recent failure and stays visible after recovery, dated, so a reader can
	// see what went wrong an hour ago.
	OK          bool       `json:"ok"`
	LastOKAt    *time.Time `json:"last_ok_at,omitempty"`
	LastError   string     `json:"last_error,omitempty"`
	LastErrorAt *time.Time `json:"last_error_at,omitempty"`

	// Height is the component's own progress in chain heights: the
	// scanner's last scanned height, the collector's view of the chain tip.
	Height int64 `json:"height,omitempty"`
	// Detail carries per-component counters (rows written, endpoints seen).
	Detail map[string]any `json:"detail,omitempty"`
	// Disk is the data directory's filesystem, so a filling disk is visible
	// before a write fails.
	Disk *Disk `json:"disk,omitempty"`
}

// Disk is free and total bytes of the filesystem holding the data dir.
type Disk struct {
	FreeBytes  uint64  `json:"free_bytes"`
	TotalBytes uint64  `json:"total_bytes"`
	FreeShare  float64 `json:"free_share"`
}

// Writer keeps one component's file up to date.
type Writer struct {
	path    string
	dataDir string
	mu      sync.Mutex
	r       Report
	lastW   time.Time
	dirty   bool
	pending bool // a delayed write is scheduled
	stop    chan struct{}
	stopped bool
	// recordRuns: Start and Stop also append to runs.jsonl (see runs.go).
	recordRuns bool
	config     map[string]any
}

// New returns a Writer for component in dataDir. An empty dataDir yields a
// Writer that records nothing, for tests.
func New(dataDir, component, vantage, version string) *Writer {
	w := &Writer{dataDir: dataDir, stop: make(chan struct{})}
	host, _ := os.Hostname()
	now := time.Now().UTC()
	w.r = Report{Component: component, Vantage: vantage, Version: version, PID: os.Getpid(), Hostname: host,
		StartedAt: now, UpdatedAt: now, OK: true, Detail: map[string]any{}}
	defaults.mu.Lock()
	for k, v := range defaults.m {
		w.r.Detail[k] = v
	}
	defaults.mu.Unlock()
	if dataDir != "" {
		w.path = filepath.Join(dataDir, "status", component+".json")
	}
	return w
}

// Start writes the file now and keeps it fresh every Interval until Stop.
func (w *Writer) Start() {
	if w == nil {
		return
	}
	w.mu.Lock()
	w.appendRun(RunStarted, "", w.r.StartedAt)
	w.mu.Unlock()
	w.flush(true)
	go func() {
		t := time.NewTicker(Interval)
		defer t.Stop()
		for {
			select {
			case <-w.stop:
				return
			case <-t.C:
				w.flush(true)
			}
		}
	}()
}

// Stop records a clean shutdown and writes the file one last time.
func (w *Writer) Stop(reason string) {
	if w == nil {
		return
	}
	w.mu.Lock()
	if w.stopped {
		w.mu.Unlock()
		return
	}
	w.stopped = true
	now := time.Now().UTC()
	w.r.StoppedAt = &now
	w.r.StopReason = reason
	close(w.stop)
	w.appendRun(RunStopped, reason, now)
	w.mu.Unlock()
	w.flush(true)
}

// OK marks the last unit of work as succeeded.
func (w *Writer) OK() {
	if w == nil {
		return
	}
	w.mu.Lock()
	now := time.Now().UTC()
	w.r.OK = true
	w.r.LastOKAt = &now
	w.dirty = true
	w.mu.Unlock()
	w.flush(false)
}

// Error marks the last unit of work as failed and keeps the message.
func (w *Writer) Error(msg string) {
	if w == nil {
		return
	}
	w.mu.Lock()
	now := time.Now().UTC()
	w.r.OK = false
	w.r.LastError = msg
	w.r.LastErrorAt = &now
	w.dirty = true
	w.mu.Unlock()
	w.flush(false)
}

// Progress records the component's height.
func (w *Writer) Progress(height int64) {
	if w == nil {
		return
	}
	w.mu.Lock()
	w.r.Height = height
	w.dirty = true
	w.mu.Unlock()
	w.flush(false)
}

// Set records one detail counter.
func (w *Writer) Set(key string, v any) {
	if w == nil {
		return
	}
	w.mu.Lock()
	w.r.Detail[key] = v
	w.dirty = true
	w.mu.Unlock()
	w.flush(false)
}

// flush writes the file if forced or if it is dirty. Writes are at most one
// a second: a change inside that second is written by a delayed flush, so a
// busy loop cannot turn one status file into thousands of renames and no
// change waits longer than a second to reach disk.
func (w *Writer) flush(force bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.path == "" {
		return
	}
	if !force && !w.dirty {
		return
	}
	if since := time.Since(w.lastW); !force && since < time.Second {
		if !w.pending {
			w.pending = true
			time.AfterFunc(time.Second-since, func() {
				w.mu.Lock()
				w.pending = false
				w.mu.Unlock()
				w.flush(false)
			})
		}
		return
	}
	w.r.UpdatedAt = time.Now().UTC()
	w.r.Disk = diskOf(w.dataDir)
	b, err := json.MarshalIndent(w.r, "", "  ")
	if err != nil {
		return
	}
	_ = os.MkdirAll(filepath.Dir(w.path), 0o755)
	tmp := w.path + ".tmp"
	if err := os.WriteFile(tmp, append(b, '\n'), 0o644); err != nil {
		return
	}
	_ = os.Rename(tmp, w.path)
	w.lastW = time.Now()
	w.dirty = false
}

// ReadAll returns every component's report under dataDir, sorted by name.
// A missing directory is an empty list, not an error.
func ReadAll(dataDir string) ([]Report, error) {
	dir := filepath.Join(dataDir, "status")
	entries, err := os.ReadDir(dir)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var out []Report
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		b, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			continue
		}
		var r Report
		if err := json.Unmarshal(b, &r); err != nil {
			return nil, fmt.Errorf("%s: %w", e.Name(), err)
		}
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Component < out[j].Component })
	return out, nil
}

// Alive is whether a report describes a running process: not stopped, and
// refreshed within StaleAfter of now.
func (r Report) Alive(now time.Time) bool {
	return r.StoppedAt == nil && now.Sub(r.UpdatedAt) < StaleAfter
}

// Cadence is the cadence the component stated at its start (CadenceKey),
// false when it stated none: an older build, or a component without a
// work loop of its own.
func (r Report) Cadence() (time.Duration, bool) {
	var secs float64
	switch v := r.Detail[CadenceKey].(type) {
	case float64: // read back from the file
		secs = v
	case int: // set in this process
		secs = float64(v)
	default:
		return 0, false
	}
	if secs <= 0 {
		return 0, false
	}
	return time.Duration(secs * float64(time.Second)), true
}

// ReadOne reads one component's report from a status directory, false when
// it is missing or unreadable.
func ReadOne(dir, component string) (Report, bool) {
	var r Report
	b, err := os.ReadFile(filepath.Join(dir, component+".json"))
	if err != nil || json.Unmarshal(b, &r) != nil {
		return Report{}, false
	}
	return r, true
}
