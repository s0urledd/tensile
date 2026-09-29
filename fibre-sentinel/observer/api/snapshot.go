package api

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// The window aggregates are snapshots, not live queries.
//
// Both figures the overview waits for are aggregates over the whole window. The
// network summary is the verdict tally, the obligation-counted rate, the
// attestation coverage, the per-point breakdown and the reconstructability
// of the newest reconstructSample publications; the
// validator list is much the same tally again, per validator. On a store with
// 2,200 publications and 714,000 probes those took 27.6s and 4.9s, and the page
// asked for both every thirty seconds, per viewer, with cache: 'no-store'. A
// covering index and a batched reconstructability pass cut that a long way, but
// not to anything worth serving per request: they are aggregates over hundreds
// of thousands of rows and no index makes that free.
//
// So they are computed on a schedule instead. A reader gets the last snapshot
// immediately, whatever else is happening, and the snapshot carries the moment
// it was taken and how long it took, so its age is published rather than
// implied. That is the honest shape for this product anyway: every other number
// on the site is a stored observation with a timestamp, and now these are too.
//
// Refreshing happens in the background, one at a time per window, and a stale
// snapshot keeps being served while its replacement is computed, so a reader
// never waits for an aggregate. Every window is warmed at startup, so the first
// visitor does not wait either. After that a keeper refreshes every window as
// its TTL runs out (keepSnapshotsFresh), read or not: a refresh triggered only
// by a read served the triggering reader a figure as old as the last visit.
//
// What this does not yet do: a refresh still recomputes the reconstructability
// of every publication in the sample, and a publication whose retention window
// closed hours ago can never change verdict again. Caching those per promise
// hash would make a refresh nearly free. It is not done here because "can never
// change" has to account for probes ingested late — after a collector restart
// with a backlog, say — and getting that wrong would publish a stale verdict
// about a named validator. It wants its own change, with the invalidation
// reasoned through rather than bolted on.

// ttlFor is how old a snapshot may be before a read starts a refresh, scaled by
// how much a minute of new data can actually move the figure.
//
// A day's window turns over in a day, so a minute is well inside its
// resolution. A thirty-day window does not meaningfully change in a minute, and
// refreshing it as often would spend the same seconds of work to move a figure
// in its third decimal place. None of these is tighter than the probe schedule
// that produces the data, which moves in minutes: refreshing faster than the
// measurements arrive buys nothing and costs a core.
func ttlFor(name string) time.Duration {
	switch name {
	case "24h":
		return time.Minute
	case "7d":
		return 5 * time.Minute
	case "30d":
		return 15 * time.Minute
	default: // "all", and anything added later
		return 30 * time.Minute
	}
}

// warmWindows is every window the dashboard offers, computed once at startup so
// that no visitor is the one who pays for a cold aggregate.
var warmWindows = []string{"24h", "7d", "30d", "all"}

// snapshotTimeout bounds a background refresh. One that cannot finish in this
// time is abandoned rather than left to pile up behind the next; the previous
// snapshot keeps being served and the next read tries again.
//
// The "all" window gets longer, because its cost is the only one that grows
// without bound: the windowed spans cover a fixed number of days, while "all"
// covers every raw row the store still holds, which is every row written since
// the observer started until retention begins to prune. Measured on a fixture
// of 80 validators and three days of publications (691k probe rows), the whole
// warm-up of twelve snapshots took 94 seconds; the same arithmetic at a month
// of that rate puts the "all" window past five minutes. A refresh abandoned on
// the timeout is not a wrong figure — the previous snapshot keeps being served
// with its age shown — but it is a figure that silently stops moving, so the
// bound is generous and slowness is logged before it becomes a freeze.
const (
	snapshotTimeout    = 5 * time.Minute
	snapshotTimeoutAll = 20 * time.Minute
	// slowRefresh is when a refresh is worth a log line: at this point the
	// operator should be lowering -retain-raw (deploy/README.md, "Backups,
	// retention, rebuild") rather than waiting for the timeout.
	slowRefresh = 45 * time.Second
)

func timeoutFor(name string) time.Duration {
	if name == "all" {
		return snapshotTimeoutAll
	}
	return snapshotTimeout
}

// logf is the bit of a logger a cache needs, so it does not depend on the whole
// Server and stays usable with nothing.
type logf func(format string, args ...any)

type snap[T any] struct {
	v T
	// rev is the value revision() returned when this snapshot was
	// computed. A snapshot taken under a different revision is not stale,
	// it is wrong, and it is dropped rather than served while a refresh
	// runs behind it.
	rev string
	at  time.Time
	ms  int64
}

// snapshotCache holds one computed value per window, refreshed on read.
//
// With dir set, every snapshot is also written to <dir>/<label>-<window>.json
// and read back at construction, so a restarted API serves the last figures
// at once, with their real age, instead of making the first visitors wait
// minutes for the cold aggregates while the warm-up runs behind them.
type snapshotCache[T any] struct {
	label      string
	compute    func(context.Context, Window) (T, error)
	born       time.Time
	dir        string
	mu         sync.Mutex
	entries    map[string]*snap[T]
	refreshing map[string]bool
	// revision returns a token that changes when something happened that a
	// cached aggregate cannot survive. The TTLs here run to thirty minutes,
	// and a withheld fault republished for half an hour after the hold
	// landed is exactly the accusation the hold exists to stop, so the
	// answer is invalidation and not a shorter TTL. nil means nothing can
	// invalidate this cache.
	revision func() string
	// bg counts the background computations in flight (warm-up and
	// refreshes), so Server.Close can wait for their files to land before
	// the directory they write to goes away.
	bg sync.WaitGroup
}

// persisted is the on-disk form of one snapshot.
type persisted[T any] struct {
	Label  string    `json:"label"`
	Window string    `json:"window"`
	At     time.Time `json:"at"`
	Ms     int64     `json:"ms"`
	Rev    string    `json:"rev,omitempty"`
	Value  T         `json:"value"`
}

// persistTo enables the on-disk copy and loads whatever a previous process
// left there. A file that does not parse (an older build's shape) is skipped;
// the warm-up replaces it.
func (c *snapshotCache[T]) persistTo(dir string, log logf) {
	if dir == "" {
		return
	}
	c.dir = dir
	_ = os.MkdirAll(dir, 0o755)
	loaded := 0
	for _, name := range warmWindows {
		b, err := os.ReadFile(c.file(name))
		if err != nil {
			continue
		}
		var p persisted[T]
		if err := json.Unmarshal(b, &p); err != nil || p.Label != c.label || p.Window != name {
			continue
		}
		c.mu.Lock()
		c.entries[name] = &snap[T]{v: p.Value, rev: p.Rev, at: p.At, ms: p.Ms}
		c.mu.Unlock()
		loaded++
	}
	if loaded > 0 && log != nil {
		log("%s: %d snapshot(s) loaded from disk; serving them while the warm-up runs", c.label, loaded)
	}
}

func (c *snapshotCache[T]) file(window string) string {
	return filepath.Join(c.dir, c.label+"-"+window+".json")
}

// persist writes one snapshot, atomically. A failure is logged by the
// caller's next refresh at worst; the in-memory copy is what is served.
func (c *snapshotCache[T]) persist(window string, s *snap[T]) {
	if c.dir == "" {
		return
	}
	b, err := json.Marshal(persisted[T]{Label: c.label, Window: window, At: s.at, Ms: s.ms, Rev: s.rev, Value: s.v})
	if err != nil {
		return
	}
	tmp := c.file(window) + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return
	}
	_ = os.Rename(tmp, c.file(window))
}

func newSnapshotCache[T any](label string, compute func(context.Context, Window) (T, error)) *snapshotCache[T] {
	return &snapshotCache[T]{
		label: label, compute: compute, born: time.Now(),
		entries: map[string]*snap[T]{}, refreshing: map[string]bool{},
	}
}

// ttl is ttlFor scaled down while the cache is young. The long windows' TTLs
// assume a long history, where a minute of new data cannot move an "all"
// figure; on a fresh deployment the whole history is minutes old and the
// "all" tile showed one publication for half an hour while 24h showed eight.
// A tenth of the cache's age, never under a minute, converges on the table
// above within hours and costs nothing once it has.
func (c *snapshotCache[T]) ttl(name string) time.Duration {
	t := ttlFor(name)
	if young := time.Since(c.born) / 10; young < t {
		if young < time.Minute {
			return time.Minute
		}
		return young
	}
	return t
}

// get returns the snapshot for win with the moment it was taken and what it
// cost, computing inline only when there is nothing at all to serve. A stale
// snapshot is returned as it stands and a refresh is started behind it.
func (c *snapshotCache[T]) rev() string {
	if c.revision == nil {
		return ""
	}
	return c.revision()
}

func (c *snapshotCache[T]) get(ctx context.Context, log logf, win Window) (T, time.Time, int64, error) {
	var zero T
	rev := c.rev()
	c.mu.Lock()
	s := c.entries[win.Name]
	if s != nil && s.rev != rev {
		// Computed under a different revision: a hold landed, or a
		// correction moved a verdict. Serving it while a refresh runs
		// behind it would republish the figure that changed.
		delete(c.entries, win.Name)
		s = nil
	}
	if s != nil && time.Since(s.at) >= c.ttl(win.Name) && !c.refreshing[win.Name] {
		c.refreshing[win.Name] = true
		c.bg.Add(1)
		go func() {
			defer c.bg.Done()
			c.background(log, win)
		}()
	}
	c.mu.Unlock()
	if s != nil {
		return s.v, s.at, s.ms, nil
	}

	// Nothing to serve: this reader pays. Claiming the window first means a
	// burst of first-time readers produces one computation, not one each.
	got, claimed := c.await(ctx, win, rev)
	if got != nil {
		return got.v, got.at, got.ms, nil
	}
	if !claimed {
		return zero, time.Time{}, 0, ctx.Err()
	}
	got, err := c.fill(ctx, win)
	if err != nil {
		return zero, time.Time{}, 0, err
	}
	return got.v, got.at, got.ms, nil
}

// await blocks until win has a snapshot computed under rev, or claims the
// window for the caller to compute once nothing is computing it, or gives up
// with the caller.
//
// Only a snapshot under rev will do. The computation a reader finds running
// may have started before a hold that this reader has already seen, and the
// figure it lands carries the verdicts that hold withdrew; taking whatever
// landed next served it once for every reader waiting on it.
func (c *snapshotCache[T]) await(ctx context.Context, win Window, rev string) (*snap[T], bool) {
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()
	for {
		c.mu.Lock()
		if s := c.entries[win.Name]; s != nil && s.rev == rev {
			c.mu.Unlock()
			return s, false
		}
		if !c.refreshing[win.Name] {
			c.refreshing[win.Name] = true
			c.mu.Unlock()
			return nil, true
		}
		c.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, false
		case <-tick.C:
		}
	}
}

// background recomputes away from any request: the reader that triggered it has
// long since been served, so its context must not be the one that goes away.
func (c *snapshotCache[T]) background(log logf, win Window) {
	ctx, cancel := context.WithTimeout(context.Background(), timeoutFor(win.Name))
	defer cancel()
	started := time.Now()
	_, err := c.fill(ctx, win)
	took := time.Since(started)
	if log == nil {
		return
	}
	if err != nil {
		// A failed refresh is not a failed request: the previous snapshot is
		// still being served, so this is logged and left for the next read.
		log("%s snapshot refresh (%s): after %s: %v", c.label, win.Name, took.Round(time.Second), err)
		return
	}
	if took >= slowRefresh {
		// Not an error: the figure is correct and was served the whole time.
		// It is the trend that matters, because the cost of this window grows
		// with the history behind it and the end of that growth is a window
		// that stops refreshing at all.
		log("%s snapshot refresh (%s) took %s (over %s); consider lowering -retain-raw", c.label, win.Name, took.Round(time.Second), slowRefresh)
	}
}

func (c *snapshotCache[T]) fill(ctx context.Context, win Window) (*snap[T], error) {
	start := time.Now()
	// The revision the figures were computed under is the one read before
	// the queries ran. Read after, a hold landing mid-compute stamped a
	// pre-hold figure with the post-hold revision, and get served it as
	// current until the TTL ran out: the withheld fault republished.
	rev := c.rev()
	v, err := c.compute(ctx, win)
	c.mu.Lock()
	defer c.mu.Unlock()
	c.refreshing[win.Name] = false
	if err != nil {
		return nil, err
	}
	s := &snap[T]{v: v, rev: rev, at: start, ms: time.Since(start).Milliseconds()}
	c.entries[win.Name] = s
	c.persist(win.Name, s)
	return s, nil
}

// warm computes every window once, in the background, one at a time.
// Sequential on purpose: these are the heaviest queries the process runs, and
// starting four at once against a cold page cache makes each of them slower
// than running them in turn.
func (c *snapshotCache[T]) warm(log logf, now time.Time) {
	c.bg.Add(1)
	go func() {
		defer c.bg.Done()
		for _, name := range warmWindows {
			c.mu.Lock()
			busy := c.refreshing[name]
			if !busy {
				c.refreshing[name] = true
			}
			c.mu.Unlock()
			if busy {
				continue // a reader got there first
			}
			c.background(log, windowFor(name, now))
		}
	}()
}

// wait blocks until every background computation in flight has finished
// and persisted its snapshot.
func (c *snapshotCache[T]) wait() { c.bg.Wait() }

// windowFor builds the Window parseWindow would build for a name.
func windowFor(name string, now time.Time) Window {
	span := windows[name]
	w := Window{Name: name, Span: span, End: now}
	if span > 0 {
		w.Start = now.Add(-span)
	}
	return w
}

// keeperInterval is how often the keeper looks for windows past their TTL.
// The shortest TTL is a minute, so a looser tick would let the 24h window
// run past it.
const keeperInterval = time.Minute

// refreshDue recomputes, one at a time, every window whose snapshot has
// outlived its TTL, was computed under another revision, or was dropped for
// one. It does what a read would have started, without waiting for the read:
// a stale snapshot is served whole to the reader who triggers its refresh,
// and on a quiet site that reader may be the first of the morning, handed a
// figure from the evening before.
func (c *snapshotCache[T]) refreshDue(log logf, now time.Time) {
	rev := c.rev()
	var due []string
	c.mu.Lock()
	for _, name := range warmWindows {
		s := c.entries[name]
		if c.refreshing[name] {
			continue
		}
		if s == nil || s.rev != rev || now.Sub(s.at) >= c.ttl(name) {
			c.refreshing[name] = true
			due = append(due, name)
		}
	}
	if len(due) > 0 {
		c.bg.Add(1)
	}
	c.mu.Unlock()
	if len(due) == 0 {
		return
	}
	defer c.bg.Done()
	for _, name := range due {
		// each window ends when its own computation starts, not at the tick
		c.background(log, windowFor(name, time.Now()))
	}
}

// keepSnapshotsFresh runs refreshDue for every cache until Close. The caches
// take turns, as the warm-up does: these are the heaviest queries the
// process runs, and a tick that finds the previous one still computing
// skips the windows it holds rather than stacking a second copy.
func (s *Server) keepSnapshotsFresh(every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-s.stop:
			return
		case now := <-t.C:
			s.net.refreshDue(s.logf(), now)
			s.vals.refreshDue(s.logf(), now)
			s.market.refreshDue(s.logf(), now)
		}
	}
}
