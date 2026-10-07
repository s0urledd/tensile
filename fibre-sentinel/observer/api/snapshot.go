package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime/debug"
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
// Refreshing happens in the background, never two computations of one window
// at once, and a stale snapshot keeps being served while its replacement is
// computed, so a reader never waits for an aggregate. Every window is warmed
// at startup, so the first visitor does not wait either. A window with
// nothing to serve at all (the first computation after a start with no file
// under the current revision, or the recomputation after a hold dropped its
// snapshot) makes a reader wait a few seconds at most (firstReadWait); past
// that the reader is told the figure is being computed and asks again,
// rather than holding a request open for minutes. After that keepers
// refresh every window as its TTL runs out, read or not: a refresh triggered
// only by a read served the triggering reader a figure as old as the last
// visit. A keeper computes its windows one after another, so what a reader
// watches move each has a keeper of its own — the 24h validator list, the
// publisher-side summary and the network summary's 24h window — and one more
// takes the longer windows in turn (newKeepers). A slow computation on one
// keeper never holds back another's.
//
// What this does not yet do: a refresh still recomputes the reconstructability
// of every publication in the sample, and a publication whose retention window
// closed hours ago can never change verdict again. Caching those per promise
// hash would make a refresh nearly free. It is not done here because "can never
// change" has to account for probes ingested late — after a collector restart
// with a backlog, say — and getting that wrong would publish a stale verdict
// about a named validator. It wants its own change, with the invalidation
// reasoned through rather than bolted on.
//
// Nor does it stop a snapshot's cost from growing with its window. The
// validator list and the network summary are aggregates over the window's
// rows (publications, probes, heartbeats), which grow with blobs, so the long
// windows cost the most; they are also the ones refreshed least often (ttlFor).
// The day's windows are not cheap either, and the 24h validator list already
// costs more than its TTL (liveTTL).

// ttlFor is how old a snapshot may be before it is refreshed, for a window its
// cache gives no TTL of its own (snapshotCache.ttls): a minute for 24h, five
// minutes for 7d, fifteen for 30d and "all".
//
// Only the current figures need to be fresh. The day's window is the one a
// reader watches move; the week, the month and the whole history are read,
// not watched, and recomputing them every minute would keep a core busy to
// move figures nobody is waiting on. A cache takes a window faster where a
// reader does watch it: the validator list's 24h window and every window of
// the publisher-side summary are on the live lane (liveTTL), and the network
// summary's "all" window, which holds the overview's Available figure, is
// refreshed every five minutes (networkAllTTL).
//
// The TTL is a floor, not a promise. A window whose computation outgrows it
// waits twice its computation instead (stale), and each keeper computes its
// windows one after another, so a window can be older than its TTL by what
// the computations ahead of it on its keeper cost. Every snapshot says when
// it was taken (computed_at).
//
// Nor are the windows one moment. Each is refreshed on its own schedule, so
// the windows of one cache are taken minutes apart, and a longer window can
// count fewer blobs, endorsements or readings than a shorter one until its
// next refresh.
func ttlFor(name string) time.Duration {
	switch name {
	case "24h":
		return time.Minute
	case "7d":
		return 5 * time.Minute
	default: // "30d", "all", and anything added later
		return 15 * time.Minute
	}
}

// liveTTL is the live lane's TTL, for the 24h validator list and every window
// of the publisher-side summary: they are refreshed at most every ten seconds,
// the collector's pass interval, since nothing they count lands more often.
//
// Like every TTL it is a floor (stale): a window whose computation takes
// longer waits twice its computation. The publisher-side windows cost well
// under a second and are refreshed every ten seconds or so. The 24h validator
// list is not: on the observer's store in late September 2026 it took 15 to 22
// seconds a computation, so it was refreshed about every 35 to 45 seconds, and
// only a cheaper computation brings that closer to ten.
const liveTTL = 10 * time.Second

// networkAllTTL is how old the network summary's "all" window may get. It is
// the overview's Available figure, which reads every settlement so far and is
// shown whatever period the page is set to, so it is kept fresher than the
// other long windows.
const networkAllTTL = 5 * time.Minute

// liveInterval is how often the live lane's keepers look. A tick that finds
// a window a few milliseconds short of the TTL waits one tick, not one TTL.
const liveInterval = 2 * time.Second

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
	// slowRefresh is when a refresh is worth a log line, well before the
	// timeout: the window has outgrown its refresh, which the per-day
	// rollups (rather than deleting rows) are there to answer.
	slowRefresh = 45 * time.Second
)

func timeoutFor(name string) time.Duration {
	if name == "all" {
		return snapshotTimeoutAll
	}
	return snapshotTimeout
}

// firstReadWait is how long a reader waits for a window that has nothing to
// serve: its first computation after a start, or its recomputation after a
// hold, the activation or a new methodology dropped its snapshot. The cheap
// windows land well inside it. The expensive ones (the longer windows of the
// validator list and the network summary) take minutes on a store of any
// size, and a reader held for all of that reached the site proxy's
// 60-second timeout and got a bare 502. Past this wait the reader gets
// errComputing (a 503 with Retry-After) and asks again; the computation is
// the cache's, not the reader's, and carries on in the background.
const firstReadWait = 8 * time.Second

// errComputing is get's answer when a window has nothing to serve and its
// computation did not land within the reader's wait.
var errComputing = errors.New("snapshot is being computed")

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
	dir        string
	mu         sync.Mutex
	entries    map[string]*snap[T]
	refreshing map[string]bool
	// revision returns a token that changes when something happened that a
	// cached aggregate cannot survive. A withheld fault republished for
	// even a minute after the hold landed is exactly the accusation the
	// hold exists to stop, and a slow window can be served for longer than
	// its TTL, so the answer is invalidation and not a shorter TTL. nil
	// means nothing can invalidate this cache.
	revision func() string
	// bg counts the background computations in flight (warm-up and
	// refreshes), so Server.Close can wait for their files to land before
	// the directory they write to goes away.
	bg sync.WaitGroup
	// now is the clock a window ends at and a snapshot is dated by (Server.now); nil is the wall clock.
	now func() time.Time
	// ttls overrides ttlFor for the windows it names. It is set before the
	// cache is shared and only read after.
	ttls map[string]time.Duration
	// accept, when set, vets a snapshot read back from disk: an older
	// build's file may parse and still lack something this build serves.
	accept func(T) bool
	// vantage is the vantage the figures are computed for (Server.vantage):
	// the heartbeat counts are this vantage's own rows. It is written into
	// every file, and a file computed for another vantage is not loaded.
	// That matters once files come from another process (WarmSnapshots)
	// started by hand with flags that may not match the unit's.
	vantage string
	// failed is the error of each window's last computation, when it
	// failed, so a reader waiting for that computation is told rather
	// than left to wait out firstReadWait.
	failed map[string]error
	// firstWait overrides firstReadWait (tests).
	firstWait time.Duration

	// What /v1/health's snapshots check reads (snapshotsCheck): when the
	// cache was made, each window's last successful computation (when it
	// ended, on the wall clock) and how many have failed since, in a row.
	// A failed refresh is not a failed request, and only logged: before
	// these, a window whose every refresh failed was served, older by the
	// hour, with nothing anywhere failing.
	started  time.Time
	lastOK   map[string]time.Time
	failures map[string]int
	// base is what every background computation runs under; halt cancels
	// it, so a Close does not wait out a refresh's timeout.
	base context.Context
	halt context.CancelFunc
}

// persisted is the on-disk form of one snapshot.
type persisted[T any] struct {
	Label   string    `json:"label"`
	Window  string    `json:"window"`
	At      time.Time `json:"at"`
	Ms      int64     `json:"ms"`
	Rev     string    `json:"rev,omitempty"`
	Vantage string    `json:"vantage,omitempty"`
	Value   T         `json:"value"`
}

// persistTo enables the on-disk copy and loads whatever a previous process
// left there. A file that does not parse (an older build's shape) is skipped;
// the warm-up replaces it.
//
// A file is loaded whatever revision it was computed under, and served only
// under the same one: current drops a snapshot whose revision is not the
// store's, before any reader sees it. So a file warmed by a new build
// (WarmSnapshots) is served by that build and not by an older one, whose
// verdict.MethodologyVersion differs, and a hold or the activation landing
// between the warm-up and the switch makes the file be recomputed rather
// than served.
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
		if p.Vantage != "" && p.Vantage != c.vantage {
			// Computed for another vantage: its heartbeat counts are
			// another machine's. A file from before the vantage was
			// written (empty) was written by this process's unit.
			if log != nil {
				log("%s-%s: snapshot file computed for vantage %q, not %q; not loaded", c.label, name, p.Vantage, c.vantage)
			}
			continue
		}
		if c.accept != nil && !c.accept(p.Value) {
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

// persist writes one snapshot, atomically. The API's own refreshes ignore a
// failure: the in-memory copy is what is served, and the next refresh writes
// again. WarmSnapshots, whose whole product is the file, does not.
func (c *snapshotCache[T]) persist(window string, s *snap[T]) error {
	if c.dir == "" {
		return nil
	}
	b, err := json.Marshal(persisted[T]{Label: c.label, Window: window, At: s.at, Ms: s.ms, Rev: s.rev, Vantage: c.vantage, Value: s.v})
	if err != nil {
		return err
	}
	tmp := c.file(window) + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, c.file(window))
}

// clock is the cache's now: the server's clock when it has one.
func (c *snapshotCache[T]) clock() time.Time {
	if c.now != nil {
		return c.now()
	}
	return time.Now()
}

func newSnapshotCache[T any](label string, compute func(context.Context, Window) (T, error)) *snapshotCache[T] {
	base, halt := context.WithCancel(context.Background())
	return &snapshotCache[T]{
		label: label, compute: compute,
		entries: map[string]*snap[T]{}, refreshing: map[string]bool{}, failed: map[string]error{},
		started: time.Now(), lastOK: map[string]time.Time{}, failures: map[string]int{},
		base: base, halt: halt,
	}
}

// ttl is how old this cache lets the window get: its own override when it has
// one, ttlFor otherwise.
func (c *snapshotCache[T]) ttl(name string) time.Duration {
	if t, ok := c.ttls[name]; ok {
		return t
	}
	return ttlFor(name)
}

// stale reports whether s has outlived its window's refresh interval at now.
// The interval is the TTL, or twice what s took to compute when that is
// longer: a computation that outgrows its TTL then runs at most half the
// time instead of back to back, which would pin a core, keep a reader open on
// the database almost continuously (starving the collector's checkpoints)
// and rewrite the snapshot file without pause.
func (c *snapshotCache[T]) stale(s *snap[T], name string, now time.Time) bool {
	wait := c.ttl(name)
	if busy := 2 * time.Duration(s.ms) * time.Millisecond; busy > wait {
		wait = busy
	}
	return now.Sub(s.at) >= wait
}

// rev is the revision the cache's snapshots must carry to be served now.
func (c *snapshotCache[T]) rev() string {
	if c.revision == nil {
		return ""
	}
	return c.revision()
}

// current is the snapshot win may serve now under rev, or nil, with a
// refresh started behind one that has gone stale. With startMissing a window
// with nothing to serve starts its computation in the background too, for a
// caller that will not wait for it.
func (c *snapshotCache[T]) current(log logf, win Window, rev string, startMissing bool) *snap[T] {
	c.mu.Lock()
	defer c.mu.Unlock()
	s := c.entries[win.Name]
	if s != nil && s.rev != rev {
		// Computed under a different revision: a hold landed, or a
		// correction moved a verdict. Serving it while a refresh runs
		// behind it would republish the figure that changed.
		delete(c.entries, win.Name)
		s = nil
	}
	due := s != nil && c.stale(s, win.Name, c.clock())
	if (due || (s == nil && startMissing)) && !c.refreshing[win.Name] {
		c.refreshing[win.Name] = true
		c.bg.Add(1)
		go func() {
			defer c.bg.Done()
			c.background(log, win)
		}()
	}
	return s
}

// peek is get for a reader that must not wait: the snapshot if win has one,
// else ok false and the computation started for the next reader.
func (c *snapshotCache[T]) peek(log logf, win Window) (v T, at time.Time, ms int64, ok bool) {
	if s := c.current(log, win, c.rev(), true); s != nil {
		return s.v, s.at, s.ms, true
	}
	return v, at, ms, false
}

// get returns the snapshot for win with the moment it was taken and what it
// cost. A stale snapshot is returned as it stands and a refresh is started
// behind it.
//
// When there is nothing at all to serve, the computation is started in the
// background, one for any number of readers, and the reader waits for it at
// most firstReadWait; then it gets errComputing and the computation carries
// on. It used to be computed inline, on the reader's request, and the
// longest windows took minutes: past the site proxy's timeout, so the reader
// got a 502 and the computation was cancelled with the request.
func (c *snapshotCache[T]) get(ctx context.Context, log logf, win Window) (T, time.Time, int64, error) {
	var zero T
	rev := c.rev()
	if s := c.current(log, win, rev, true); s != nil {
		return s.v, s.at, s.ms, nil
	}
	wait := c.firstWait
	if wait <= 0 {
		wait = firstReadWait
	}
	wctx, cancel := context.WithTimeout(ctx, wait)
	defer cancel()
	got, err := c.await(wctx, log, win, rev)
	switch {
	case got != nil:
		return got.v, got.at, got.ms, nil
	case err != nil:
		return zero, time.Time{}, 0, err
	case ctx.Err() != nil:
		return zero, time.Time{}, 0, ctx.Err()
	default:
		return zero, time.Time{}, 0, errComputing
	}
}

// await blocks until win has a snapshot computed under rev, or the
// computation it waited for fails (its error), or ctx ends (nil, nil).
// Whenever nothing is computing the window and nothing under rev is there to
// serve, it starts a computation in the background.
//
// Only a snapshot under rev will do. The computation a reader finds running
// may have started before a hold that this reader has already seen, and the
// figure it lands carries the verdicts that hold withdrew; taking whatever
// landed next served it once for every reader waiting on it. When such a
// computation lands, the revision is read again before starting the next,
// because the revision may instead have moved on since this reader read it.
func (c *snapshotCache[T]) await(ctx context.Context, log logf, win Window, rev string) (*snap[T], error) {
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()
	for {
		c.mu.Lock()
		s, busy, failed := c.entries[win.Name], c.refreshing[win.Name], c.failed[win.Name]
		c.mu.Unlock()
		if s != nil && s.rev == rev {
			return s, nil
		}
		if !busy {
			if failed != nil {
				return nil, failed
			}
			rev = c.rev()
			if s := c.current(log, win, rev, true); s != nil {
				return s, nil
			}
		}
		select {
		case <-ctx.Done():
			return nil, nil
		case <-tick.C:
		}
	}
}

// background recomputes away from any request: the reader that triggered it has
// long since been served, so its context must not be the one that goes away.
func (c *snapshotCache[T]) background(log logf, win Window) {
	ctx, cancel := context.WithTimeout(c.base, timeoutFor(win.Name))
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
		log("%s snapshot refresh (%s) took %s (over %s); the window's rows outgrow its refresh", c.label, win.Name, took.Round(time.Second), slowRefresh)
		return
	}
	if ttl := c.ttl(win.Name); took >= ttl {
		// The window now refreshes every 2×took rather than every TTL (stale):
		// its age is still published, but it is older than it is meant to be.
		log("%s snapshot refresh (%s) took %s, longer than its %s refresh interval", c.label, win.Name, took.Round(time.Millisecond), ttl)
	}
}

// computeOnce computes win under the revision the store holds as it starts.
//
// A panic in it is a failed computation. Every computation runs on a
// goroutine of its own (the warm-up, a keeper, a reader's first computation
// of a window), where nothing recovers it the way net/http recovers one on a
// request: it would end the whole API and leave the window marked
// refreshing. As an error it is logged by background, returned to a reader
// waiting on it as a 500, and the next refresh tries again.
func (c *snapshotCache[T]) computeOnce(ctx context.Context, win Window) (s *snap[T], err error) {
	defer func() {
		if p := recover(); p != nil {
			s, err = nil, fmt.Errorf("panic: %v\n%s", p, debug.Stack())
		}
	}()
	start, at := time.Now(), c.clock()
	// The revision the figures were computed under is the one read before
	// the queries ran. Read after, a hold landing mid-compute stamped a
	// pre-hold figure with the post-hold revision, and get served it as
	// current until the TTL ran out: the withheld fault republished.
	rev := c.rev()
	v, err := c.compute(ctx, win)
	if err != nil {
		return nil, err
	}
	return &snap[T]{v: v, rev: rev, at: at, ms: time.Since(start).Milliseconds()}, nil
}

func (c *snapshotCache[T]) fill(ctx context.Context, win Window) (*snap[T], error) {
	s, err := c.computeOnce(ctx, win)
	if err != nil {
		c.mu.Lock()
		c.refreshing[win.Name] = false
		c.failed[win.Name] = err
		c.failures[win.Name]++
		c.mu.Unlock()
		return nil, err
	}
	// Written outside the lock, so a reader of any window never waits for a
	// file write, and before the snapshot is served, so whoever is handed it
	// finds its file already on disk. The window stays claimed (refreshing)
	// until the file has landed, so two writes of the same file never
	// overlap. A failed write is not a failed refresh (persist).
	_ = c.persist(win.Name, s)
	c.mu.Lock()
	c.entries[win.Name] = s
	c.refreshing[win.Name] = false
	delete(c.failed, win.Name)
	delete(c.failures, win.Name)
	c.lastOK[win.Name] = time.Now()
	c.mu.Unlock()
	return s, nil
}

// precompute computes every window once, one after another, and writes each
// to dir; the first failure ends it. It is the warm-up of a process that
// serves nothing (WarmSnapshots), so unlike warm it waits, and a file that
// could not be written is an error: the file is all it is for. Nothing is
// read from dir, so whatever an earlier run left there is replaced.
func (c *snapshotCache[T]) precompute(ctx context.Context, dir string, log logf) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	c.dir = dir
	for _, name := range warmWindows {
		s, err := c.computeOnce(ctx, windowFor(name, c.clock()))
		if err != nil {
			return fmt.Errorf("%s %s: %w", c.label, name, err)
		}
		if err := c.persist(name, s); err != nil {
			return fmt.Errorf("%s %s: %w", c.label, name, err)
		}
		if log != nil {
			log("%s %s: computed in %s, written to %s", c.label, name, (time.Duration(s.ms) * time.Millisecond).Round(time.Millisecond), c.file(name))
		}
	}
	return nil
}

// warm computes every window once, in the background, one at a time.
// Sequential on purpose: starting four at once against a cold page cache
// makes each of them slower than running them in turn.
//
// A window a reader computed while the warm-up was busy with the ones before
// it is newer than the warm-up and is left as it is: recomputed with the
// window as of the warm-up's start, it replaced a snapshot ending later
// with one ending earlier, and two answers read from the same cache a moment
// apart (the publishers' table and the market board beside it) described
// two moments. A window the warm-up does compute ends when the warm-up
// reaches it. now is when the warm-up began; a snapshot older than it (one
// loaded from disk) is recomputed.
func (c *snapshotCache[T]) warm(log logf, now time.Time) {
	c.bg.Add(1)
	go func() {
		defer c.bg.Done()
		for _, name := range warmWindows {
			c.mu.Lock()
			s := c.entries[name]
			busy := c.refreshing[name] || (s != nil && !s.at.Before(now))
			if !busy {
				c.refreshing[name] = true
			}
			c.mu.Unlock()
			if busy {
				continue // a reader got there first
			}
			c.background(log, windowFor(name, c.clock()))
		}
	}()
}

// wait blocks until every background computation in flight has finished
// and persisted its snapshot.
func (c *snapshotCache[T]) wait() { c.bg.Wait() }

// stop cancels every background computation in flight and any started
// after: for a server being closed, whose store goes next.
func (c *snapshotCache[T]) stop() { c.halt() }

// windowFor builds the Window parseWindow would build for a name.
func windowFor(name string, now time.Time) Window {
	span := windows[name]
	w := Window{Name: name, Span: span, End: now}
	if span > 0 {
		w.Start = now.Add(-span)
	}
	return w
}

// keeperInterval is how often the keepers off the live lane look for windows
// past their TTL. It is well under the shortest TTL they hold (a minute) on
// purpose: a window's age runs from the moment its computation started, and a
// tick as long as the TTL found a window computed a few seconds into the
// previous tick a few seconds short of due, and left it for another whole TTL.
const keeperInterval = 5 * time.Second

// lanes is how the keepers pace themselves: the live lane's TTL, how often
// the live lane's keepers look (liveEvery) and how often the others do
// (slowEvery). NewWithVantage uses defaultLanes unless it is handed others
// (withLanes, for tests).
type lanes struct{ liveTTL, liveEvery, slowEvery time.Duration }

var defaultLanes = lanes{liveTTL: liveTTL, liveEvery: liveInterval, slowEvery: keeperInterval}

// withLanes sets the keepers' pace.
func withLanes(l lanes) Option { return func(s *Server) { s.lanes = l } }

// refreshDue recomputes, one at a time and in the order given (warmWindows
// when only is empty: shortest first), every window whose snapshot is stale,
// was computed under another revision, or was dropped for one. It does what a
// read would have started, without waiting for the read: a stale snapshot is
// served whole to the reader who triggers its refresh, and on a quiet site
// that reader may be the first of the morning, handed a figure from the
// evening before.
func (c *snapshotCache[T]) refreshDue(log logf, now time.Time, only ...string) {
	names := warmWindows
	if len(only) > 0 {
		names = only
	}
	rev := c.rev()
	var due []string
	c.mu.Lock()
	for _, name := range names {
		s := c.entries[name]
		if c.refreshing[name] {
			continue
		}
		if s == nil || s.rev != rev || c.stale(s, name, now) {
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
		c.background(log, windowFor(name, c.clock()))
	}
}

// refresher is what a keeper asks of a cache.
type refresher interface {
	refreshDue(log logf, now time.Time, only ...string)
}

// keeperJob is one cache's windows on a keeper, refreshed in the order given.
type keeperJob struct {
	cache   refresher
	windows []string
}

// keeper is one goroutine of the schedule: it looks every `every` and
// refreshes the due windows of its jobs one after another, never two at once.
// A tick that finds the previous one still computing skips the windows it
// holds rather than stacking a second copy.
type keeper struct {
	name  string
	every time.Duration
	jobs  []keeperJob
}

// newKeepers is the schedule: which keeper refreshes which window. Every
// window of the three caches is on exactly one keeper, and NewWithVantage
// starts one goroutine per keeper (keep).
//
// What a reader watches move each has a keeper of its own, so nothing
// computed elsewhere holds it back. On one shared keeper the publisher-side
// summary, a fraction of a second a window, waited out every computation of
// the 24h validator list, and the network's 24h window waited behind the
// longer windows whenever they came due, for over half a minute at a time.
// The longer windows share a keeper and take turns, as the warm-up does:
// their TTLs are minutes, and a turn costs them seconds.
func (s *Server) newKeepers() []keeper {
	long := []string{"7d", "30d", "all"}
	return []keeper{
		{name: "validators 24h", every: s.lanes.liveEvery, jobs: []keeperJob{{s.vals, []string{"24h"}}}},
		{name: "market", every: s.lanes.liveEvery, jobs: []keeperJob{{s.market, warmWindows}}},
		{name: "network 24h", every: s.lanes.slowEvery, jobs: []keeperJob{{s.net, []string{"24h"}}}},
		{name: "longer windows", every: s.lanes.slowEvery, jobs: []keeperJob{{s.net, long}, {s.vals, long}}},
	}
}

// keep runs one keeper until Close.
func (s *Server) keep(k keeper) {
	t := time.NewTicker(k.every)
	defer t.Stop()
	for {
		select {
		case <-s.stop:
			return
		case <-t.C:
			for _, j := range k.jobs {
				// The clock, not the tick: a tick delivered late, behind a
				// long refresh, would make every window look younger than
				// it is.
				j.cache.refreshDue(s.logf(), s.now(), j.windows...)
			}
		}
	}
}
