package probe

import (
	"container/heap"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	celfibre "github.com/celestiaorg/celestia-app/v10/fibre"
	assign "github.com/plsgiveup/fibre/fibre-assign"
	"github.com/plsgiveup/fibre/fibre-sentinel/internal/record"
	"github.com/plsgiveup/fibre/fibre-sentinel/internal/scan"
	"github.com/plsgiveup/fibre/fibre-sentinel/internal/status"
)

// Config controls a prober run.
type Config struct {
	RPCURL           string
	PublicationsPath string // publications.jsonl written by the scanner
	DataDir          string // where measurements.jsonl lives
	// RegistryPath is the collector's registry.jsonl, the durable record of
	// every host this observer saw a validator register. Default
	// <DataDir>/registry.jsonl; the file is optional.
	RegistryPath string
	Vantage      string // this prober's vantage-point name

	Schedule ScheduleConfig
	// Timeouts bound the steps of one request; Download is the whole
	// request's time (ClientRPCTimeout), which the steps sit inside.
	Timeouts StepTimeouts

	// run mode
	Once     bool          // read every blob due right now, then exit
	Drain    bool          // run until every known blob's reading is in the past
	Deadline time.Duration // whole-run wall-clock cap (0 = none)
	// ReadNow reads every publication in the feed whose window is still
	// open at once, whatever its scheduled time, and exits: a dry run
	// against a scratch data directory, never the live one.
	ReadNow bool

	// pacing (every wait is bounded by MaxSleep)
	MaxSleep     time.Duration // longest sleep between cycles
	MinSleep     time.Duration // shortest, to avoid a busy loop
	RPCTimeout   time.Duration
	HostCacheTTL time.Duration

	// RunConfig is what this run was configured with, as the operator's
	// flags and derived settings, recorded in runs.jsonl on start (see
	// status.RunEvent) so a row's verdict can be traced to the settings
	// that produced it. nil records the run without a configuration.
	RunConfig map[string]any

	// Concurrency is how many requests are in flight at once across every
	// reading (default 256), and BlobConcurrency how many blobs are being
	// read at once (default 16). They bound this observer's load and only
	// delay: a request waits for room and its time starts once it is let
	// go. A reading that cannot start before its latest start is not made,
	// and its blob was not read by Tensile; a request of a full reading
	// that cannot start before RequestStartMargin is not made either
	// (NOT_PROBED). The most urgent reading due starts first (popDue).
	//
	// A full reading asks every endorser, and a validator that times out
	// holds a request for the whole 15 s (30 with the re-dial), so the
	// count is set well above what the healthy requests need: memory is
	// bounded by InFlightBytes, and the load on any one validator by
	// BlobConcurrency, not by this.
	Concurrency     int
	BlobConcurrency int

	// AskEveryEndorser reads every validator the settled promise names as a
	// signer (Prober.wants), each for its own rows, instead of stopping once
	// the rows reconstruct the blob: every endorser is asked on every blob,
	// so none is left unasked by the order. Validators that did not endorse
	// are not asked. The blob's slot is held until every answer is in, so
	// BlobConcurrency still bounds the readings in progress. Its readings
	// are labelled FullReadLabel, and a validator whose answer did not serve
	// is asked again later (retry.go).
	AskEveryEndorser bool

	// RequestStartMargin is how long before must_serve_until the last
	// request of a full reading may start, the reading's own or a later
	// attempt (default 60 s). One that cannot start by then is not made, and
	// its validator's row says so (NOT_PROBED): this observer's gap. The
	// client's re-dial within a request is part of it and follows it, past
	// the margin or not.
	RequestStartMargin time.Duration
	// RetrySpacing is how long after a validator's answer in a full reading
	// did not serve it is asked again (default 90 s), up to FullReadRetries
	// times.
	RetrySpacing time.Duration

	// AllowUnroutableHosts dials a registered host that resolves to loopback
	// or a private range. A local devnet needs it; a public vantage must not
	// have it, because the host is whatever a validator put on chain and
	// dialling it would make this observer a port scanner and a DNS resolver
	// driven from the chain, publishing what it found.
	AllowUnroutableHosts bool

	// InFlightBytes bounds the shard bytes being downloaded at once, which
	// the request count alone does not: DownloadShard is a unary RPC, so an
	// in-flight request holds its whole shard, twice. Each request is
	// charged the most it may receive (recvLimitFor: what its shard should
	// weigh, a tenth to spare, a mebibyte at least), so a server sending more
	// than its shard cannot hold more than the budget. A request larger than
	// the whole budget still runs, alone. Zero takes the default.
	InFlightBytes int64
	// LinkMbps is this observer's measured receive rate, megabits a second
	// (0: not set). When set, InFlightBytes is held to what the link moves
	// in half a request's time (LinkBytesCap): every byte in flight at once
	// then arrives well inside the client's 15 s, so a request that runs out
	// of time ran out of it at the validator's end, not because this
	// observer's own link was full.
	LinkMbps int
	// MaxReadMbps is the reading-rate ceiling, megabits a second (0: no
	// ceiling; sentinel-probe sets DefaultMaxReadMbps): every request, the
	// first pass's and every later attempt's, is charged its shard's
	// expected bytes against a token bucket of this rate (ceiling.go) and
	// let go once the bucket has them, so the readings leave room on the
	// observer's port. The wait only delays, up to the
	// request's start cutoff, and is not part of the request's time.
	// LinkMbps bounds the bytes in flight at once and this the rate they
	// are let go at; with both set, this belongs below the link.
	MaxReadMbps int

	// BackfillMissed bounds how far back a (re)started prober writes
	// NOT_PROBED rows for readings it never made. Zero, the default, is no
	// bound. Either way a publication whose rows may have been archived
	// (observer-archive, days after its reading) is not planned again
	// (archivedFrom).
	BackfillMissed time.Duration

	// Order puts a reading's validators in the order it asks them in; nil
	// is the Fibre client's own (Resolver.clientOrder). Tests only.
	Order func(ctx context.Context, pub scan.Publication, targets []Target) ([]readTarget, error)
}

func (c Config) withDefaults() Config {
	if c.RegistryPath == "" {
		c.RegistryPath = filepath.Join(c.DataDir, "registry.jsonl")
	}
	if c.MaxSleep <= 0 {
		c.MaxSleep = 30 * time.Second
	}
	if c.MinSleep <= 0 {
		c.MinSleep = time.Second
	}
	if c.RPCTimeout <= 0 {
		c.RPCTimeout = 15 * time.Second
	}
	if c.HostCacheTTL <= 0 {
		c.HostCacheTTL = 60 * time.Second
	}
	if c.Concurrency <= 0 {
		c.Concurrency = DefaultConcurrency
	}
	if c.BlobConcurrency <= 0 {
		c.BlobConcurrency = 16
	}
	if c.InFlightBytes <= 0 {
		c.InFlightBytes = defaultInFlightBytes
	}
	if c.RequestStartMargin <= 0 {
		c.RequestStartMargin = time.Minute
	}
	if c.RetrySpacing <= 0 {
		c.RetrySpacing = 90 * time.Second
	}
	if c.Timeouts.Download <= 0 {
		c.Timeouts.Download = ClientRPCTimeout
	}
	if limit := LinkBytesCap(c.LinkMbps, c.Timeouts.Download); limit > 0 && limit < c.InFlightBytes {
		c.InFlightBytes = limit
	}
	// The request's whole time is the client's, whatever the shard weighs.
	c.Timeouts.MinDownloadBytesPerSec = -1
	if c.BackfillMissed < 0 {
		c.BackfillMissed = 0
	}
	if c.MaxReadMbps < 0 {
		c.MaxReadMbps = 0
	}
	if c.Vantage == "" {
		c.Vantage = "local"
	}
	return c
}

// DefaultConcurrency is the requests in flight at once when Config sets
// none.
const DefaultConcurrency = 256

// LinkBytesCap is the shard bytes a link of mbps megabits a second moves in
// half of requestTime: the most that may be in flight at once for every
// one of them to arrive with half the request's time to spare. 0 when mbps
// is not set.
func LinkBytesCap(mbps int, requestTime time.Duration) int64 {
	if mbps <= 0 || requestTime <= 0 {
		return 0
	}
	return int64(float64(mbps) * 1e6 / 8 * requestTime.Seconds() / 2)
}

// Prober turns the scanner's publications into readings of blobs and raw
// measurements. The queue of readings is never persisted: it is re-derived
// from publications.jsonl and measurements.jsonl every cycle, so a restart
// resumes exactly; a reading that was under way when the process stopped
// is made again if its window still allows.
type Prober struct {
	status *status.Writer

	cfg   Config
	log   *scan.Logger
	chain *scan.Chain
	// observer is stamped on every row: the build, the assignment pin, and
	// the chain's app version as last polled. pinMu guards the version.
	observer ObserverInfo
	pinMu    sync.Mutex
	// gaps is the scanner's own record of blocks it could not read, from
	// state.json, re-read every cycle. A reading whose verdict would rest
	// on "no other promise owns these rows" consults it first.
	// scanned is how far the scanner has read, as the chain's clock. The
	// cycle loop writes both while readings read them: scanMu guards them.
	scanMu   sync.Mutex
	gaps     []scan.ScanGap
	scanned  scannedMark
	resolver *Resolver
	// targetsFor is resolver.TargetsFor; tests put their own validators here.
	targetsFor func(ctx context.Context, pub scan.Publication) ([]Target, error)
	store      *MeasurementStore
	feed       *pubFeed
	registry   *hostRegistry
	chainID    string

	clockMu     sync.Mutex
	clockOffset time.Duration // observer clock - latest block time

	codersMu sync.Mutex
	coders   map[[2]int]*Coder // keyed by (originalRows, totalRows)

	// liveSince is the newest time from which the prober's own files are
	// whole in their live copies: lines dated earlier may have moved to
	// archive/ (internal/record), where the restart index does not read.
	liveSince time.Time
	// cycleErrs counts the failures recorded in the cycle now running, so
	// the end of the cycle can tell an OK cycle from one that merely
	// finished.
	cycleErrs atomic.Int64
	// skippedPubs are publications logged once as not readable (wrong chain,
	// failed settlement tx).
	skippedPubs map[string]bool

	// the reading's load: requests in flight and bytes in flight, the
	// reading-rate ceiling (nil: none), and the requests and budget bytes
	// that hold room while the ceiling paces them (not yet in flight)
	reqs       chan struct{}
	bytes      *byteSem
	ceiling    *readCeiling
	pacedReqs  atomic.Int64
	pacedBytes atomic.Int64
	sched      *readQueue
	counters   readCounters
	// retries are the later attempts of full readings (retry.go).
	retries *retryQueue
	// reach is when this observer's requests last reached a server and its
	// resolver last answered, for telling its own network's or resolver's
	// failure from a validator's (ownside.go).
	reach reachLog
	// lookupHost is the resolver the check of this observer's own resolver
	// asks (resolverUp); nil is this machine's own. Tests only.
	lookupHost func(ctx context.Context, host string) ([]string, error)
}

// New builds a Prober.
func New(cfg Config, log *scan.Logger) (*Prober, error) {
	cfg = cfg.withDefaults()
	ch, err := scan.NewChain(cfg.RPCURL, cfg.RPCTimeout, log)
	if err != nil {
		return nil, err
	}
	st, err := OpenMeasurementStore(cfg.DataDir)
	if err != nil {
		return nil, err
	}
	p := &Prober{
		observer:    ObserverInfo{Build: status.BuildRevision(), AssignPin: assign.PinnedCelestiaAppCommit},
		cfg:         cfg,
		log:         log,
		chain:       ch,
		resolver:    NewResolver(ch, cfg.HostCacheTTL),
		store:       st,
		feed:        newPubFeed(cfg.PublicationsPath),
		registry:    newHostRegistry(cfg.RegistryPath),
		coders:      map[[2]int]*Coder{},
		skippedPubs: map[string]bool{},
	}
	p.targetsFor = p.resolver.TargetsFor
	p.initPace()
	return p, nil
}

// initPace sets up the reading's limits; tests that build a Prober by hand
// call it too.
func (p *Prober) initPace() {
	p.reqs = make(chan struct{}, p.cfg.Concurrency)
	p.bytes = newByteSem(p.cfg.InFlightBytes)
	p.ceiling = newReadCeiling(ReadCeiling(p.cfg.MaxReadMbps))
	p.sched = newReadQueue()
	p.retries = newRetryQueue()
}

func (p *Prober) schedCfg() ScheduleConfig { return p.cfg.Schedule.withDefaults() }

func (p *Prober) coderFor(originalRows, totalRows int) (*Coder, error) {
	p.codersMu.Lock()
	defer p.codersMu.Unlock()
	k := [2]int{originalRows, totalRows}
	if c, ok := p.coders[k]; ok {
		return c, nil
	}
	c, err := NewCoder(originalRows, totalRows)
	if err != nil {
		return nil, err
	}
	p.coders[k] = c
	return c, nil
}

// Run executes the prober.
func (p *Prober) Run(parent context.Context) error {
	ctx := parent
	if p.cfg.Deadline > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(parent, p.cfg.Deadline)
		defer cancel()
	}
	defer p.store.Close()

	st := status.New(p.cfg.DataDir, "prober", p.cfg.Vantage, status.BuildRevision())
	st.RecordRuns(p.cfg.RunConfig)
	st.Start()
	defer st.Stop("exit")
	p.status = st
	// The planning loop below completes a cycle at least every MaxSleep and
	// its work; /v1/health fails it once its last OK is a few of these old.
	st.Set(status.CadenceKey, int((cadenceSleeps*p.cfg.MaxSleep+time.Second-1)/time.Second))

	// The chain is asked once at startup for its id; an RPC that is down at
	// boot is waited for rather than fatal.
	var id string
	var tip int64
	for attempt := 0; ; attempt++ {
		var err error
		id, tip, err = p.chain.Status(ctx)
		if err == nil {
			break
		}
		if ctx.Err() != nil {
			p.log.Printf("stopped (signal) before the chain answered")
			return nil
		}
		wait := time.Duration(1<<uint(min(attempt, 5))) * time.Second
		if attempt < 3 || attempt%10 == 0 {
			p.log.Printf("WARNING: initial status: %v (attempt %d, retry in %s)", err, attempt+1, wait)
		}
		st.Error(fmt.Sprintf("initial status: %v", err))
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(wait):
		}
	}
	p.chainID = id
	p.measureClock(ctx)
	p.pollAppVersion(ctx)
	ceiling := "none"
	if rate, burst := ReadCeiling(p.cfg.MaxReadMbps); rate > 0 {
		ceiling = fmt.Sprintf("%d Mbit/s (burst %.1f MB)", p.cfg.MaxReadMbps, burst/1e6)
	}
	p.log.Printf("prober up: vantage=%s chain_id=%s tip=%d rpc=%s pubs=%s data=%s concurrency=%d blobs=%d in-flight=%d MiB read-ceiling=%s",
		p.cfg.Vantage, id, tip, p.cfg.RPCURL, p.cfg.PublicationsPath, p.store.Path(), p.cfg.Concurrency, p.cfg.BlobConcurrency,
		p.cfg.InFlightBytes>>20, ceiling)

	// The dispatcher starts every reading when it is due, and the retry
	// runner every later attempt of a full reading, while this loop keeps
	// the queue and the chain state fresh.
	dctx, stopDispatch := context.WithCancel(ctx)
	var dispatched sync.WaitGroup
	dispatched.Add(2)
	go func() {
		defer dispatched.Done()
		p.dispatch(dctx)
	}()
	go func() {
		defer dispatched.Done()
		p.runRetries(dctx)
	}()
	defer func() {
		stopDispatch()
		dispatched.Wait()
	}()

	for {
		p.cycleErrs.Store(0)
		if err := ctx.Err(); err != nil {
			if errors.Is(err, context.Canceled) {
				p.log.Printf("stopped (signal): %d readings this run", p.counters.done.Load())
				return nil
			}
			p.log.Fatalf("run deadline hit: %v", err)
		}

		p.measureClock(ctx)
		p.pollAppVersion(ctx)
		p.loadGaps()
		p.pollScanned(ctx)
		p.refreshRegistry()

		if added, err := p.feed.refresh(); err != nil {
			p.log.Fatalf("load publications: %v", err)
		} else if added > 0 {
			p.log.Printf("publications: +%d (%d live)", added, p.feed.size())
		}

		p.liveSince = record.LiveSince(p.store.Path())
		now := time.Now()
		due, missed, finished := p.planReads(p.feed.all(), now)
		for _, j := range missed {
			p.counters.missed.add(now)
			p.recordNotRead(ctx, j, "not read in time: the reading could not start before its deadline (this observer's own gap)")
		}
		if len(missed) > 0 {
			if err := p.store.Sync(); err != nil {
				p.log.Fatalf("sync measurements: %v", err)
			}
		}
		for _, h := range finished {
			p.feed.forget(h)
			p.store.Forget(h)
			p.retries.forget(h)
		}
		for _, j := range due {
			p.sched.push(j)
		}
		// The readings run on the dispatcher's goroutine, beside this loop:
		// a dispatcher whose every slot is held by readings that never end
		// leaves the due ones queued, this loop skipping them as queued,
		// and nothing written, not even the readings' gaps. A reading past
		// its last start is normally popped at once and written as not
		// read; one still queued long after is a dispatcher that is stuck.
		if n, oldest := p.sched.overdue(now); n > 0 && now.Sub(oldest) > dispatchStallAfter {
			p.fail(fmt.Sprintf("readings stalled: %d reading(s) still queued past their last start, the oldest by %s", n, now.Sub(oldest).Round(time.Second)))
		}
		if p.cycleErrs.Load() == 0 {
			st.OK()
		}
		st.Set("publications_live", p.feed.size())
		st.Set("clock_offset_ms", p.clockOffsetMS())
		st.Set("reads", p.readStatus())
		// What /v1/health judges the readings by: a cycle of this loop
		// succeeds whatever the readings do, so a reading error was
		// overwritten by the next cycle's OK within half a minute, and every
		// reading could fail with the check green.
		st.Set("reading_errors_15m", p.counters.failed.within(now, readingErrorsWindow))
		st.Set("readings_15m", p.counters.made.within(now, readingErrorsWindow))

		if (p.cfg.Once || p.cfg.ReadNow) && p.sched.quiet(time.Now()) && p.retries.idle() {
			p.log.Printf("done: %d readings this run", p.counters.done.Load())
			return nil
		}
		if p.cfg.Drain && p.sched.idle() && p.retries.idle() {
			p.log.Printf("done (--drain): every known reading is in the past; %d readings this run", p.counters.done.Load())
			return nil
		}
		wait := p.cfg.MaxSleep
		if p.cfg.Once || p.cfg.ReadNow {
			wait = p.cfg.MinSleep
		}
		if !p.sleep(ctx, wait) {
			p.log.Printf("stopped (signal): %d readings this run", p.counters.done.Load())
			return nil
		}
	}
}

// cadenceSleeps is the cadence the prober states (status.CadenceKey), in
// MaxSleeps: a cycle sleeps at most one, and its work takes a little more.
const cadenceSleeps = 3

// readingErrorsWindow is the span of the status file's reading counts.
const readingErrorsWindow = 15 * time.Minute

// dispatchStallAfter is how long a reading may stay queued past its last
// start before the prober reports its dispatcher stuck. A reading holds its
// slot at most until its blob's deadline, ten minutes after its start, so
// with every slot held a reading past its last start waits at most about
// that long for one.
const dispatchStallAfter = 15 * time.Minute

// clockSkewWarn is the offset from chain time past which every verdict this
// vantage produces is suspect: the phase boundaries are only seconds wide.
const clockSkewWarn = 30 * time.Second

// measureClock records the observer's clock offset against the chain's latest
// block time. Every phase decision uses the local clock, so a drifted vantage
// would silently mislabel readings; the offset is stamped on every
// measurement and a large one is logged.
func (p *Prober) measureClock(ctx context.Context) {
	blockTime, err := p.chain.LatestBlockTime(ctx)
	if err != nil {
		p.log.Printf("clock check: %v (keeping previous offset)", err)
		if p.status != nil {
			p.fail(fmt.Sprintf("chain unreachable: %v", err))
		}
		return
	}
	offset := time.Since(blockTime)
	p.clockMu.Lock()
	prev := p.clockOffset
	p.clockOffset = offset
	p.clockMu.Unlock()
	if abs(offset) > clockSkewWarn && abs(prev) <= clockSkewWarn {
		p.log.Printf("WARNING: observer clock is %s from the chain's latest block time; phase boundaries are seconds wide, check NTP", offset.Round(time.Second))
	}
}

// pollAppVersion reads the chain's app version and marks the assignment pin
// stale when the chain has moved above the pinned celestia-app major. A poll
// that fails keeps the previous reading: "unknown" must not flip verdicts
// either way.
func (p *Prober) pollAppVersion(ctx context.Context) {
	v, err := p.chain.AppVersion(ctx)
	if err != nil {
		p.log.Printf("app version: %v (keeping previous reading)", err)
		return
	}
	p.pinMu.Lock()
	prev := p.observer
	p.observer.AppVersion = v
	p.observer.PinStale = v > assign.PinnedCelestiaAppMajor
	cur := p.observer
	p.pinMu.Unlock()
	if cur.PinStale && !prev.PinStale {
		p.log.Printf("WARNING: chain app version %d is above the pinned celestia-app major %d; readings are recorded as PROBE_ERROR (assignment pin stale) until this build is re-pinned", v, assign.PinnedCelestiaAppMajor)
	}
	if p.status != nil {
		p.status.Set("app_version", v)
		p.status.Set("pin_stale", cur.PinStale)
	}
}

// refreshRegistry seeds the resolver's last-known hosts from registry.jsonl
// (see hostRegistry). Errors are logged, never fatal: the file is the
// collector's and optional.
func (p *Prober) refreshRegistry() {
	applied, skipped, err := p.registry.refresh(p.resolver)
	if err != nil {
		p.log.Printf("registry: %v", err)
		return
	}
	if applied > 0 || skipped > 0 {
		p.log.Printf("registry: +%d host records (%d skipped), %d validators with a known host", applied, skipped, p.resolver.knownHosts())
	}
	if p.status != nil && (applied > 0 || skipped > 0) {
		p.status.Set("known_hosts", p.resolver.knownHosts())
	}
}

// loadGaps re-reads the scanner's gap list from state.json. A missing or
// unreadable file keeps the previous list: the prober must not start
// accusing because the scanner's state was mid-write.
func (p *Prober) loadGaps() {
	b, err := os.ReadFile(filepath.Join(p.cfg.DataDir, "state.json"))
	if err != nil {
		return
	}
	var st struct {
		Gaps              []scan.ScanGap `json:"gaps"`
		LastScannedHeight int64          `json:"last_scanned_height"`
		LastScannedTime   time.Time      `json:"last_scanned_time"`
	}
	if err := json.Unmarshal(b, &st); err != nil {
		p.log.Printf("state.json: %v (keeping previous gap list)", err)
		return
	}
	p.scanMu.Lock()
	p.gaps = st.Gaps
	p.scanned.height = st.LastScannedHeight
	if !st.LastScannedTime.IsZero() {
		// The scanner records the frontier's block time itself; no RPC
		// round trip is needed to place it on the chain's clock.
		p.scanned.timedFor, p.scanned.at = st.LastScannedHeight, st.LastScannedTime.UTC()
	}
	mark := p.scanned
	p.scanMu.Unlock()
	if !st.LastScannedTime.IsZero() && p.status != nil {
		p.status.Set("scanned_until", mark.at.Format(time.RFC3339))
	}
}

// scannedMark is the scanner's frontier on the chain's clock.
type scannedMark struct {
	height   int64
	timedFor int64     // the height `at` was read for
	at       time.Time // block time of timedFor; zero when never read
}

// known reports whether the frontier has a block time for its current height.
func (m scannedMark) known() bool { return m.height > 0 && m.timedFor == m.height && !m.at.IsZero() }

// pollScanned reads the block time of the scanner's frontier when the
// frontier moved. A failed read keeps the previous mark, which then reads
// as stale: the conservative direction.
func (p *Prober) pollScanned(ctx context.Context) {
	p.scanMu.Lock()
	m := p.scanned
	p.scanMu.Unlock()
	if m.height <= 0 || m.timedFor == m.height {
		return
	}
	blk, err := p.chain.Block(ctx, m.height)
	if err != nil {
		p.log.Printf("scanner frontier #%d: %v (keeping previous mark)", m.height, err)
		return
	}
	at := blk.Time.UTC()
	p.scanMu.Lock()
	if p.scanned.height == m.height {
		// the frontier did not move while the block was read
		p.scanned.timedFor, p.scanned.at = m.height, at
	}
	p.scanMu.Unlock()
	if p.status != nil {
		p.status.Set("scanned_until", at.Format(time.RFC3339))
	}
}

// shadowPending says why an unmatched genuine-rows answer cannot be judged
// at the reading. The Fibre store keeps every (commitment, promise) shard
// side by side and Get(commitment) returns the first readable one in
// promise-hash order (celestia-app fibre/store.go), so which promise
// answers is decided by hash order, not by time: a promise uploaded before
// this reading and settled after it, up to payment_promise_timeout after
// its creation, can own the returned rows and is not in the feed yet. The
// verdict is deferred to the collector's late judgement (probe_amendments).
func shadowPending(now time.Time, pub scan.Publication, m scannedMark) string {
	frontier := "scanner frontier unknown"
	if m.known() {
		frontier = fmt.Sprintf("scanned to #%d (%s)", m.height, m.at.UTC().Format(time.RFC3339))
	}
	timeout := time.Duration(pub.ParamsAtPublication.PaymentPromiseTimeoutSeconds) * time.Second
	if timeout <= 0 {
		return ShadowGapPendingPrefix + ": a promise uploaded before this reading may settle after it (payment promise timeout not on record); " + frontier
	}
	return fmt.Sprintf(ShadowGapPendingPrefix+": a promise uploaded before this reading may settle until %s and own these rows; %s",
		now.Add(timeout).UTC().Format(time.RFC3339), frontier)
}

// shadowBlindness names why the shadow candidate set for pub is incomplete
// at this reading. It always is (see shadowPending); a scan gap inside the
// interval a shadowing promise could have settled in is named first
// because it is permanent.
func (p *Prober) shadowBlindness(pub scan.Publication) string {
	now := time.Now().UTC()
	p.scanMu.Lock()
	gaps, mark := p.gaps, p.scanned
	p.scanMu.Unlock()
	if g := shadowGapFor(gaps, now, shardLifetime(pub, p.schedCfg().PruneTolerance)); g != "" {
		return g
	}
	return shadowPending(now, pub, mark)
}

// shardLifetime is the longest a shard over a commitment can outlive the
// settlement of the promise that stored it: creation precedes settlement,
// the store prunes at max(expiry, creation + retention), and the prober
// tolerates prune lag on top.
func shardLifetime(pub scan.Publication, tolerance time.Duration) time.Duration {
	r := time.Duration(pub.ParamsAtPublication.ShardRetentionSeconds) * time.Second
	t := time.Duration(pub.ParamsAtPublication.PaymentPromiseTimeoutSeconds) * time.Second
	if t > r {
		r = t
	}
	return r + tolerance
}

// shadowGapFor names the first scan gap that overlaps (probeAt - lifetime,
// probeAt], the interval in which a promise whose shard could still be on
// disk at probeAt would have settled. "" when no gap does.
func shadowGapFor(gaps []scan.ScanGap, probeAt time.Time, lifetime time.Duration) string {
	if lifetime <= 0 {
		return ""
	}
	earliest := probeAt.Add(-lifetime)
	for _, g := range gaps {
		from, to := g.Spans()
		if to.After(earliest) && !from.After(probeAt) {
			return fmt.Sprintf(ShadowGapScanPrefix+" #%d-#%d (%s to %s) overlaps the shard lifetime", g.From, g.To,
				from.UTC().Format(time.RFC3339), to.UTC().Format(time.RFC3339))
		}
	}
	return ""
}

// observerInfo is the stamp for the next row.
func (p *Prober) observerInfo() ObserverInfo {
	p.pinMu.Lock()
	defer p.pinMu.Unlock()
	return p.observer
}

func (p *Prober) clockOffsetMS() int64 {
	p.clockMu.Lock()
	defer p.clockMu.Unlock()
	return p.clockOffset.Milliseconds()
}

func abs(d time.Duration) time.Duration {
	if d < 0 {
		return -d
	}
	return d
}

func (p *Prober) sleep(ctx context.Context, d time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(d):
		return true
	}
}

// readJob is one blob's reading waiting to start.
type readJob struct {
	pub    scan.Publication
	point  SchedulePoint
	start  time.Time // when it is due
	latest time.Time // the last moment it may start
	index  int       // heap position
}

// probeable reports whether a publication belongs to this prober at all, and
// logs once when it does not.
func (p *Prober) probeable(pub scan.Publication) bool {
	reason := ""
	switch {
	case pub.Assignment.Error != "":
		return false // no assignment table -> nothing to read (logged by the scanner)
	case pub.SettlementTxCode != 0:
		reason = fmt.Sprintf("settlement tx failed (code %d); no obligation", pub.SettlementTxCode)
	case pub.Promise.ChainID != "" && p.chainID != "" && pub.Promise.ChainID != p.chainID:
		reason = fmt.Sprintf("promise chain_id %q is not this RPC's chain %q", pub.Promise.ChainID, p.chainID)
	}
	if reason == "" {
		return true
	}
	if !p.skippedPubs[pub.PromiseHash] {
		p.skippedPubs[pub.PromiseHash] = true
		p.log.Printf("skip %s: %s", short(pub.PromiseHash), reason)
	}
	return false
}

// planReads sorts every publication whose reading is not on record into
// due (queued, to start at its time), missed (its reading can no longer
// start: recorded NOT_PROBED) and finished (nothing will be recorded for it
// again, or it was read on the schedule of its time: forgotten).
func (p *Prober) planReads(pubs []scan.Publication, now time.Time) (due, missed []*readJob, finished []string) {
	cfg := p.schedCfg()
	var horizon time.Time // zero: no horizon, every missed reading gets its rows
	if p.cfg.BackfillMissed > 0 {
		horizon = now.Add(-p.cfg.BackfillMissed)
	}
	for _, pub := range pubs {
		if !p.probeable(pub) {
			continue
		}
		if pub.SettlementTime.Before(cfg.Since) {
			// read on the schedule of its time
			finished = append(finished, pub.PromiseHash)
			continue
		}
		pt := p.readPoint(pub)
		if p.store.HandledPoint(p.cfg.Vantage, pub.PromiseHash, pt.At) || archivedFrom(pub, pt, p.liveSince) {
			// The reading is on record (or may be, in the archive, which
			// the restart index does not read): nothing to do again, once
			// every later attempt it owes is made or recorded as not made.
			if p.retriesOwed(pub, pt) {
				continue
			}
			finished = append(finished, pub.PromiseHash)
			continue
		}
		if p.sched.has(pub.PromiseHash) {
			continue
		}
		j := &readJob{pub: pub, point: pt, start: pt.At, latest: latestStart(pub, pt, cfg)}
		switch {
		case p.cfg.ReadNow && now.Before(pub.MustServeUntil.Add(-cfg.ReadDeadline)):
			j.start, j.latest = now, pub.MustServeUntil.Add(-cfg.ReadDeadline)
			due = append(due, j)
		case p.cfg.ReadNow:
			finished = append(finished, pub.PromiseHash)
		case !now.After(j.latest):
			due = append(due, j)
		case pt.At.After(horizon):
			missed = append(missed, j)
		default:
			// behind the backfill horizon: left without a row
			finished = append(finished, pub.PromiseHash)
		}
	}
	return due, missed, finished
}

// archiveSkew is how far a row's own date may sit before the publication's
// settlement time.
const archiveSkew = time.Hour

// archivedFrom reports whether a row of pub may be dated before liveSince,
// and so may be in the archive rather than the live file. Every row the
// prober writes for pub is dated at its reading (a measurement's
// scheduled_at), which is after its settlement.
func archivedFrom(pub scan.Publication, pt SchedulePoint, liveSince time.Time) bool {
	if liveSince.IsZero() {
		return false
	}
	earliest := pub.SettlementTime
	if earliest.IsZero() || pt.At.Before(earliest) {
		earliest = pt.At
	}
	return earliest.Add(-archiveSkew).Before(liveSince)
}

// dispatch starts every queued reading when it is due, as many at once as
// BlobConcurrency allows. A reading that cannot start before its latest
// start is not made: its blob was not read by Tensile.
func (p *Prober) dispatch(ctx context.Context) {
	slots := make(chan struct{}, p.cfg.BlobConcurrency)
	var wg sync.WaitGroup
	defer wg.Wait()
	for {
		// A slot first, then the reading: the one popped is the most urgent
		// of those due at the moment it can start (popDue).
		select {
		case slots <- struct{}{}:
		case <-ctx.Done():
			return
		}
		var j *readJob
		for j == nil {
			var wait time.Duration
			if j, wait = p.sched.popDue(time.Now()); j != nil {
				break
			}
			if wait <= 0 || wait > time.Second {
				wait = time.Second
			}
			select {
			case <-ctx.Done():
				<-slots
				return
			case <-p.sched.wake:
			case <-time.After(wait):
			}
		}
		if time.Now().After(j.latest) {
			<-slots
			p.lateReading(ctx, j)
			continue
		}
		wg.Add(1)
		go func(j *readJob) {
			defer wg.Done()
			var once sync.Once
			release := func() { once.Do(func() { <-slots }) }
			defer release()
			p.readBlob(ctx, j, release)
		}(j)
	}
}

// lateReading records a reading that could not start in time: its blob was
// not read by Tensile.
func (p *Prober) lateReading(ctx context.Context, j *readJob) {
	defer p.sched.done(j.pub.PromiseHash)
	p.counters.missed.add(time.Now())
	p.recordNotRead(ctx, j, "not read in time: the reading could not start before its deadline (this observer's own gap)")
	if err := p.store.Sync(); err != nil {
		p.log.Fatalf("sync measurements: %v", err)
	}
}

// readBlob makes one blob's reading and writes it. release gives the blob's
// slot back; it is called as soon as the rows are enough, while requests
// already on their way finish in the background.
func (p *Prober) readBlob(ctx context.Context, j *readJob, release func()) {
	if lag := time.Since(j.start); lag > time.Minute {
		p.counters.late.add(time.Now())
	}
	b, err := p.newBlobReading(ctx, j.pub, j.point)
	if err != nil {
		p.log.Printf("reading %s: %v (retry next cycle)", short(j.pub.PromiseHash), err)
		// Counted, and dated on the status file, but not against the
		// planning loop's cycle: one blob that cannot be read would
		// otherwise hold every cycle short of OK until its window closed.
		// /v1/health judges the readings by their own counts.
		p.counters.failed.add(time.Now())
		if p.status != nil {
			p.status.Error(fmt.Sprintf("reading: %v", err))
		}
		p.sched.done(j.pub.PromiseHash)
		return
	}
	b.run(ctx, release)
	if ctx.Err() != nil {
		// Stopped: nothing is written, and the reading is made again after
		// a restart if its window allows.
		p.sched.done(j.pub.PromiseHash)
		return
	}
	p.finish(b)
}

// finish writes a reading's rows, all together. The caller has taken the
// reading off the queue (popDue); finish gives it back. A full reading's
// validators whose answer did not serve are then queued to be asked again
// (retry.go): the rows are on record first, so the record stays in time
// order, and the blob is held as owing retries meanwhile (retryQueue.hold),
// so a cycle does not forget it in between.
func (p *Prober) finish(b *blobReading) {
	defer p.sched.done(b.pub.PromiseHash)
	if b.full {
		b.ownSide()
	}
	result, clientErr := b.result()
	ms := b.rows(result, clientErr)
	if b.full {
		p.retries.hold(b.pub.PromiseHash)
		defer p.retries.release(b.pub.PromiseHash)
	}
	if len(ms) > 0 {
		if err := p.store.AppendReading(ms); err != nil {
			p.log.Fatalf("append reading: %v", err)
		}
	}
	if b.full {
		p.retryAfterReading(b, ms)
	}
	p.counters.done.Add(1)
	p.counters.made.add(time.Now())
	what := result
	if clientErr != "" {
		what += " (" + clientErr + ")"
	}
	p.log.Printf("READ %s: %s, %d distinct rows held (%d needed), %d validators asked of %d",
		short(b.pub.PromiseHash), what, b.have(), b.pub.Assignment.ProtocolParams.OriginalRows, len(ms), len(b.targets))
	for _, m := range ms {
		p.logMeasurement(m)
	}
}

// inputFor is one request's input: the Fibre client's rules, the blob's
// shared verifier.
func (p *Prober) inputFor(pub scan.Publication, t Target, pt SchedulePoint, commitment [32]byte, rec ShardVerifier, shadowGap string) Input {
	return Input{
		Vantage:             p.cfg.Vantage,
		ChainID:             p.chainID,
		PromiseHash:         pub.PromiseHash,
		Commitment:          commitment,
		CommitmentHex:       pub.Promise.Commitment,
		BlobVersion:         pub.Promise.BlobVersion,
		MustServeUntil:      pub.MustServeUntil,
		ValidatorSetHeight:  pub.Assignment.ValidatorSetHeight,
		Target:              t,
		AllowUnroutableHost: p.cfg.AllowUnroutableHosts,
		SchedulePoint:       pt,
		PruneTolerance:      p.schedCfg().PruneTolerance,
		ExpectedShardBytes:  ShardBytes(pub.Promise.BlobSize, pub.Assignment.ProtocolParams.OriginalRows, t.RowCount),
		MaxMessageSize:      maxMessageSizeFor(pub.Assignment.ProtocolParams),
		ClockOffsetMS:       p.clockOffsetMS(),
		Shadowers:           p.feed.shadowersFor(pub.PromiseHash, pub.Promise.Commitment, t.AddressHex),
		ShadowGap:           shadowGap,
		Observer:            p.observerInfo(),
		ClientRules:         true,
		RequestTimeout:      p.cfg.Timeouts.Download,
		Verifier:            rec,
	}
}

// admitBy waits for room for one request under this observer's limits, in
// this order: a request slot, the byte budget (budgetBytes: the shard, and
// a later attempt's own verifier), and the reading-rate ceiling (wireBytes:
// the shard alone, charged last, so it is let go when it is charged). It
// waits only until by (a zero by is no bound) or until ctx ends: when room
// did not come in time it returns the reason, errLimitsFull or errRateHeld
// (ctx's error when it ended), and holds nothing. Otherwise the request is
// let go, its own time starts now, and release gives the room back. load is
// this observer's load once it is let go and how long it waited, the
// ceiling's part apart (recorded on the row, and in the status file).
func (p *Prober) admitBy(ctx context.Context, by time.Time, budgetBytes, wireBytes int64) (release func(), load *LoadInfo, err error) {
	start := time.Now()
	bounded := !by.IsZero()
	if bounded && !start.Before(by) {
		return nil, nil, errLimitsFull
	}
	var expired <-chan time.Time // nil: no bound
	if bounded {
		t := time.NewTimer(time.Until(by))
		defer t.Stop()
		expired = t.C
	}
	select {
	case p.reqs <- struct{}{}:
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	case <-expired:
		p.counters.admitWait(start, time.Since(start), 0)
		return nil, nil, errLimitsFull
	}
	if !p.bytes.acquireBy(ctx, budgetBytes, by) {
		<-p.reqs
		if ctx.Err() != nil {
			return nil, nil, ctx.Err()
		}
		p.counters.admitWait(start, time.Since(start), 0)
		return nil, nil, errLimitsFull
	}
	release = func() {
		p.bytes.release(budgetBytes)
		<-p.reqs
	}
	paced := time.Now()
	wait, charged, ok := p.ceiling.reserve(wireBytes, by)
	if !ok {
		release()
		p.counters.admitWait(start, time.Since(start), 0)
		return nil, nil, errRateHeld
	}
	if wait > 0 {
		p.pacedReqs.Add(1)
		p.pacedBytes.Add(budgetBytes)
		t := time.NewTimer(wait)
		select {
		case <-t.C:
		case <-ctx.Done():
			t.Stop()
		}
		p.pacedReqs.Add(-1)
		p.pacedBytes.Add(-budgetBytes)
		if ctx.Err() != nil {
			p.ceiling.refund(charged)
			release()
			return nil, nil, ctx.Err()
		}
	}
	now := time.Now()
	total, rate := now.Sub(start), now.Sub(paced)
	if wait <= 0 {
		rate = 0
	}
	p.counters.admitWait(start, total, rate)
	// In flight: let go and not done, so not the requests the ceiling is
	// still pacing.
	load = &LoadInfo{RequestsInFlight: len(p.reqs) - int(p.pacedReqs.Load()), ShardBytesInFlight: p.bytes.inFlight() - p.pacedBytes.Load(),
		AdmitWaitMS: total.Milliseconds(), RateWaitMS: rate.Milliseconds()}
	return release, load, nil
}

// requestStartBy is the last moment a request of pub's full reading may
// start: must_serve_until less RequestStartMargin.
func (p *Prober) requestStartBy(pub scan.Publication) time.Time {
	return pub.MustServeUntil.Add(-p.cfg.RequestStartMargin)
}

// notStartedReason is the NOT_PROBED reason of a request of a full reading
// (what: "request" or "retry") that could not start in time.
func (p *Prober) notStartedReason(what string) string {
	return fmt.Sprintf("not read in time: the %s could not start %s before must_serve_until (this observer's own gap)",
		what, p.cfg.RequestStartMargin)
}

// readPoint is when, and under which label, pub is read: ReadPoint, under
// readLabel.
func (p *Prober) readPoint(pub scan.Publication) SchedulePoint {
	pt := ReadPoint(pub, p.schedCfg())
	pt.Label = p.readLabel()
	return pt
}

// readLabel is the label this prober's readings carry: FullReadLabel when
// it asks every endorser, EnoughReadLabel when it stops once the rows are
// enough. Neither is EndReadLabel, which from FullReadSince on reads as a
// full reading.
func (p *Prober) readLabel() string {
	if p.cfg.AskEveryEndorser {
		return FullReadLabel
	}
	return EnoughReadLabel
}

// defaultInFlightBytes is the shard-byte ceiling: half a gibibyte of shards
// being transferred at once, which with the receive buffer and the
// unmarshalled copy is about a gibibyte resident at the peak.
//
// It does not count what a reading holds besides its transfers: each blob
// being read keeps its verifier (about 4.3 MiB at K = 4096, N = 12288) and
// the rows of the first shard it verified (up to about 7 MiB at mocha's
// largest shard) until it finishes. Readings are bounded by
// BlobConcurrency, so at 16 running that is under 200 MiB more. A later
// attempt of a full reading builds a verifier of its own, and is charged
// it here beside its shard (verifierBytes).
const defaultInFlightBytes = 512 << 20

// byteSem admits work by weight as well as by count. A single item heavier
// than the whole budget is admitted alone rather than deadlocking, which is
// the case that matters: one validator holding every row of a large blob.
// Waiters are admitted first come, first served: only the one at the head
// of the queue may take room, so a large shard (the validators that hold
// the most rows) is not passed over by smaller ones until its cutoff. A
// waiter that gives up leaves the queue.
type byteSem struct {
	mu    sync.Mutex
	cond  *sync.Cond
	limit int64
	held  int64
	queue []*byte // the waiters, in arrival order; each is a ticket of its own
}

// leave takes ticket w out of the queue. The caller holds mu.
func (b *byteSem) leave(w *byte) {
	for i, q := range b.queue {
		if q == w {
			b.queue = append(b.queue[:i], b.queue[i+1:]...)
			return
		}
	}
}

// blocked reports whether ticket w must still wait for nBytes: it is not at
// the head of the queue, or the room is not free (an item heavier than the
// budget goes once nothing else is held). The caller holds mu.
func (b *byteSem) blocked(w *byte, nBytes int64) bool {
	return b.queue[0] != w || (b.held > 0 && b.held+nBytes > b.limit)
}

func newByteSem(limit int64) *byteSem {
	b := &byteSem{limit: limit}
	b.cond = sync.NewCond(&b.mu)
	return b
}

// inFlight is the shard bytes held now.
func (b *byteSem) inFlight() int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.held
}

func (b *byteSem) acquire(nBytes int64) {
	if nBytes <= 0 {
		nBytes = 1
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	w := new(byte)
	b.queue = append(b.queue, w)
	for b.blocked(w, nBytes) {
		b.cond.Wait()
	}
	b.leave(w)
	b.held += nBytes
	// the next in line may fit too
	b.cond.Broadcast()
}

// acquireBy is acquire bounded by a deadline (a zero by is none) and a
// context: false, with nothing held, when the bytes were not free in time.
func (b *byteSem) acquireBy(ctx context.Context, nBytes int64, by time.Time) bool {
	if nBytes <= 0 {
		nBytes = 1
	}
	wake := func() {
		b.mu.Lock()
		b.cond.Broadcast()
		b.mu.Unlock()
	}
	bounded := !by.IsZero()
	if bounded {
		t := time.AfterFunc(time.Until(by), wake)
		defer t.Stop()
	}
	stop := context.AfterFunc(ctx, wake)
	defer stop()
	b.mu.Lock()
	defer b.mu.Unlock()
	w := new(byte)
	b.queue = append(b.queue, w)
	for b.blocked(w, nBytes) {
		if ctx.Err() != nil || (bounded && !time.Now().Before(by)) {
			// out of line: the one behind may be at the head now
			b.leave(w)
			b.cond.Broadcast()
			return false
		}
		b.cond.Wait()
	}
	b.leave(w)
	b.held += nBytes
	// the next in line may fit too
	b.cond.Broadcast()
	return true
}

func (b *byteSem) release(nBytes int64) {
	if nBytes <= 0 {
		nBytes = 1
	}
	b.mu.Lock()
	b.held -= nBytes
	if b.held < 0 {
		b.held = 0
	}
	b.mu.Unlock()
	b.cond.Broadcast()
}

// fail records a cycle error: on the status file, where /v1/health reads it,
// and on the cycle's own counter, so the end of the cycle does not report OK
// over it. Safe from any goroutine.
func (p *Prober) fail(msg string) {
	p.cycleErrs.Add(1)
	if p.status != nil {
		p.status.Error(msg)
	}
}

// recordNotRead writes a NOT_PROBED row for every endorsing validator of a
// blob whose reading was not made: this observer's gap, counted neither
// way. Rows are appended without fsync; the caller syncs once per batch.
func (p *Prober) recordNotRead(ctx context.Context, j *readJob, reason string) {
	pub := j.pub
	targets, err := p.targetsFor(ctx, pub)
	if err != nil {
		// The publication record already names every assigned validator
		// and its row count, so the rows can still be written without the
		// chain. Writing nothing made the publication disappear.
		p.log.Printf("not-read %s: targets unresolved: %v (%s); recording from the publication record instead",
			short(pub.PromiseHash), err, reason)
		for _, v := range pub.Assignment.Validators {
			p.recordNotProbedTarget(pub, j.point, Target{
				AddressHex: v.Address,
				Assigned:   v.RowCount > 0,
				Attested:   v.Attested,
				RowCount:   v.RowCount,
				// A record from before signature verification says nothing
				// about attestation, and "says nothing" must not be stored
				// as "did not attest".
				AttestationUnknown: !pub.HasAttestation(),
			}, reason+"; targets could not be resolved: "+err.Error())
		}
		return
	}
	for _, t := range targets {
		p.recordNotProbedTarget(pub, j.point, t, reason)
	}
}

// wants reports whether a target is read at all: only the validators the
// settled promise names as signers (or every assigned one, on a record from
// before signatures were verified). A target it declines gets no row: a
// validator with nothing to answer for on this promise is not a gap.
func (p *Prober) wants(t Target) bool {
	return t.Assigned && (t.AttestationUnknown || t.Attested)
}

func (p *Prober) notProbedRow(pub scan.Publication, pt SchedulePoint, t Target, reason string) Measurement {
	now := time.Now().UTC()
	return Measurement{
		SchemaVersion: MeasurementSchemaVersion, Vantage: p.cfg.Vantage,
		PromiseHash: pub.PromiseHash, Commitment: pub.Promise.Commitment,
		BlobVersion: pub.Promise.BlobVersion, MustServeUntil: pub.MustServeUntil,
		ValidatorSetHeight: pub.Assignment.ValidatorSetHeight,
		ValidatorAddress:   t.AddressHex, ValidatorHost: t.Host, HostSource: t.HostSource,
		Assigned: t.Assigned, Attested: t.Attested, AttestationUnknown: t.AttestationUnknown,
		AssignedRowCount: t.RowCount,
		ScheduleLabel:    pt.Label, ScheduledAt: pt.At.UTC(),
		StartedAt: now, FinishedAt: now,
		Phase: PhaseAt(pt.At, pub, p.cfg.Schedule), Outcome: OutcomeMissed,
		Classification: ClassNotProbed, ClassificationReason: reason,
		Observer: func() *ObserverInfo { o := p.observerInfo(); return &o }(),
	}
}

// recordNotProbedTarget writes one NOT_PROBED measurement for a single target.
func (p *Prober) recordNotProbedTarget(pub scan.Publication, pt SchedulePoint, t Target, reason string) {
	if !p.wants(t) || p.store.Has(p.cfg.Vantage, pub.PromiseHash, t.AddressHex, pt.At) {
		return
	}
	m := p.notProbedRow(pub, pt, t, reason)
	if err := p.store.AppendDeferred(m); err != nil {
		p.log.Fatalf("append not-read measurement: %v", err)
	}
	p.logMeasurement(m)
}

func (p *Prober) logMeasurement(m Measurement) {
	tail := ""
	if m.Download.Attempted {
		tail = fmt.Sprintf(" rows=%d/%d commit=%v assign=%v", m.Download.RowsReturned, m.Download.RowsExpected, m.Download.CommitmentVerified, m.Download.AssignmentVerified)
	}
	if m.Attempt > 0 {
		tail += fmt.Sprintf(" attempt=%d", m.Attempt)
	}
	p.log.Printf("PROBE %s val=%s %s[%s] -> %s / %s (%dms)%s",
		short(m.PromiseHash), short(m.ValidatorAddress), m.ScheduleLabel, m.Phase,
		m.Outcome, m.Classification, m.TotalDurationMS, tail)
}

// maxMessageSizeFor returns the gRPC receive bound implied by a publication's
// own recorded protocol params: the Fibre client's bound, so a blob encoded
// under params this binary was not built against is still read. It mirrors
// celestia-app's ProtocolParams.MaxMessageSize.
func maxMessageSizeFor(pp scan.ProtocolParamsSnapshot) int {
	if pp.TotalRows <= 0 || pp.OriginalRows <= 0 {
		return 0 // fall back to the pinned defaults
	}
	celPP := celfibre.DefaultProtocolParams
	celPP.Rows = pp.OriginalRows
	celPP.EncodingRatio = float64(pp.OriginalRows) / float64(pp.TotalRows)
	if got := celPP.MaxMessageSize(); got > 0 {
		return got
	}
	return 0
}

func short(s string) string {
	if len(s) > 12 {
		return s[:12]
	}
	return s
}

// ---- the queue ----

// readQueue holds the readings waiting to start, earliest first, and the
// blobs queued or being read, so a cycle does not queue one twice.
type readQueue struct {
	mu     sync.Mutex
	h      readHeap
	active map[string]bool
	taken  int // readings popped and not yet done
	wake   chan struct{}
}

func newReadQueue() *readQueue {
	return &readQueue{active: map[string]bool{}, wake: make(chan struct{}, 1)}
}

func (q *readQueue) push(j *readJob) {
	q.mu.Lock()
	q.active[j.pub.PromiseHash] = true
	heap.Push(&q.h, j)
	q.mu.Unlock()
	select {
	case q.wake <- struct{}{}:
	default:
	}
}

// popDue returns, of the readings due, the one whose last start comes
// first, or how long until the earliest is due.
func (q *readQueue) popDue(now time.Time) (*readJob, time.Duration) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.h) == 0 {
		return nil, 0
	}
	if j := q.h[0]; j.start.After(now) {
		return nil, j.start.Sub(now)
	}
	best := -1
	for i, j := range q.h {
		if j.start.After(now) {
			continue
		}
		if b := q.h[max(best, 0)]; best < 0 || j.latest.Before(b.latest) || (j.latest.Equal(b.latest) && j.start.Before(b.start)) {
			best = i
		}
	}
	q.taken++
	return heap.Remove(&q.h, best).(*readJob), 0
}

// done ends a popped reading and forgets the blob.
func (q *readQueue) done(hash string) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.taken > 0 {
		q.taken--
	}
	delete(q.active, hash)
}

func (q *readQueue) has(hash string) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.active[hash]
}

func (q *readQueue) len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.h)
}

// overdue is how many queued readings are past their last start at now,
// and the earliest of those last starts.
func (q *readQueue) overdue(now time.Time) (n int, oldest time.Time) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for _, j := range q.h {
		if !j.latest.Before(now) {
			continue
		}
		n++
		if oldest.IsZero() || j.latest.Before(oldest) {
			oldest = j.latest
		}
	}
	return n, oldest
}

// running is how many readings are under way.
func (q *readQueue) running() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.taken
}

// idle reports that nothing is queued or under way.
func (q *readQueue) idle() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.h) == 0 && q.taken == 0
}

// quiet reports that nothing is under way or due now: the end of a run
// that reads only what is due.
func (q *readQueue) quiet(now time.Time) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.taken > 0 {
		return false
	}
	for _, j := range q.h {
		if !j.start.After(now) {
			return false
		}
	}
	return true
}

type readHeap []*readJob

func (h readHeap) Len() int           { return len(h) }
func (h readHeap) Less(i, j int) bool { return h[i].start.Before(h[j].start) }
func (h readHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index, h[j].index = i, j
}
func (h *readHeap) Push(x any) {
	j := x.(*readJob)
	j.index = len(*h)
	*h = append(*h, j)
}
func (h *readHeap) Pop() any {
	old := *h
	n := len(old)
	j := old[n-1]
	*h = old[:n-1]
	return j
}

// ---- the status file's reads block ----

// readCounters are what the status file says about the reading.
type readCounters struct {
	done   atomic.Int64
	late   recentEvents // readings that started more than a minute after their time
	missed recentEvents // readings not made in time
	// notStarted: requests of full readings that could not start before
	// their cutoff (NOT_PROBED rows).
	notStarted recentEvents
	// the later attempts of full readings: made (a request of their own),
	// answered by another attempt's request to the same endpoint (shared),
	// and owed but not made (NOT_PROBED rows), in all and by validator.
	retriesMade, retriesShared, retriesNotMade recentEvents
	// readings written (made) and readings that could not be begun
	// (failed: newBlobReading's error), for /v1/health
	made, failed recentEvents

	mu         sync.Mutex
	notMadeBy  map[string]*recentEvents
	waits      []admitSample // the last admitSamples waits for room
	waitsStart int
}

// admitSample is one request's wait for room (Prober.admitBy), and the part
// of it the reading-rate ceiling held it.
type admitSample struct {
	at         time.Time
	wait, rate time.Duration
}

// admitSamples is how many waits the status file's percentile is drawn
// from.
const admitSamples = 4096

// retryNotMade counts an attempt that was owed and not made.
func (c *readCounters) retryNotMade(now time.Time, validator string) {
	c.retriesNotMade.add(now)
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.notMadeBy == nil {
		c.notMadeBy = map[string]*recentEvents{}
	}
	r := c.notMadeBy[validator]
	if r == nil {
		r = &recentEvents{}
		c.notMadeBy[validator] = r
	}
	r.add(now)
}

// notMadeByValidator is, for the last hour, how many attempts owed to each
// validator were not made: a validator whose attempts often were not made
// is judged on fewer blobs than it owed.
func (c *readCounters) notMadeByValidator(now time.Time) map[string]int {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := map[string]int{}
	for v, r := range c.notMadeBy {
		if n := r.lastHour(now); n > 0 {
			out[v] = n
		} else {
			delete(c.notMadeBy, v)
		}
	}
	return out
}

// admitWait records how long a request waited for room, from at, and how
// much of it the reading-rate ceiling held it.
func (c *readCounters) admitWait(at time.Time, wait, rate time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	s := admitSample{at: at, wait: wait, rate: rate}
	if len(c.waits) < admitSamples {
		c.waits = append(c.waits, s)
		return
	}
	c.waits[c.waitsStart] = s
	c.waitsStart = (c.waitsStart + 1) % admitSamples
}

// admitWaitP95 is the 95th percentile of the waits for room recorded, of the
// last admitSamples within the hour, the same of the ceiling's part of them,
// and how many there were.
func (c *readCounters) admitWaitP95(now time.Time) (wait, rate time.Duration, n int) {
	c.mu.Lock()
	var ws, rs []time.Duration
	cut := now.Add(-time.Hour)
	for _, s := range c.waits {
		if !s.at.Before(cut) {
			ws, rs = append(ws, s.wait), append(rs, s.rate)
		}
	}
	c.mu.Unlock()
	if len(ws) == 0 {
		return 0, 0, 0
	}
	return p95(ws), p95(rs), len(ws)
}

// p95 is the 95th percentile of ds (sorted in place).
func p95(ds []time.Duration) time.Duration {
	sort.Slice(ds, func(i, j int) bool { return ds[i] < ds[j] })
	return ds[(len(ds)*95+99)/100-1]
}

// recentEvents counts events of the last hour.
type recentEvents struct {
	mu sync.Mutex
	at []time.Time
}

func (r *recentEvents) add(t time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.at = append(r.at, t)
	r.trim(t)
}

func (r *recentEvents) trim(now time.Time) {
	cut := now.Add(-time.Hour)
	i := 0
	for i < len(r.at) && r.at[i].Before(cut) {
		i++
	}
	r.at = r.at[i:]
}

func (r *recentEvents) lastHour(now time.Time) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.trim(now)
	return len(r.at)
}

// within counts the events of the last d, at most an hour.
func (r *recentEvents) within(now time.Time, d time.Duration) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.trim(now)
	cut := now.Add(-d)
	n := 0
	for i := len(r.at) - 1; i >= 0 && r.at[i].After(cut); i-- {
		n++
	}
	return n
}

// readStatus is the status file's reads block: what is queued and under
// way, the later attempts of full readings owed, and how the last hour
// went.
func (p *Prober) readStatus() map[string]any {
	now := time.Now()
	queued, waiting, inFlight := p.retries.counts()
	wait, rate, n := p.counters.admitWaitP95(now)
	return map[string]any{
		"queued":           p.sched.len(),
		"in_progress":      p.sched.running(),
		"started_late":     p.counters.late.lastHour(now),
		"missed_last_hour": p.counters.missed.lastHour(now),
		// requests of full readings that could not start before their
		// cutoff, and the wait for room of the last requests (up to 4096,
		// within the hour), all of it and the reading-rate ceiling's part
		"requests_not_started_last_hour": p.counters.notStarted.lastHour(now),
		"admit_wait_p95_ms":              wait.Milliseconds(),
		"rate_wait_p95_ms":               rate.Milliseconds(),
		"admit_wait_samples":             n,
		// the later attempts: queued (not yet due), waiting for their
		// validator's one attempt in flight, being made; and the last hour's
		"retries_queued":                          queued,
		"retries_waiting":                         waiting,
		"retries_in_flight":                       inFlight,
		"retries_made_last_hour":                  p.counters.retriesMade.lastHour(now),
		"retries_shared_last_hour":                p.counters.retriesShared.lastHour(now),
		"retries_not_made_last_hour":              p.counters.retriesNotMade.lastHour(now),
		"retries_not_made_by_validator_last_hour": p.counters.notMadeByValidator(now),
	}
}
