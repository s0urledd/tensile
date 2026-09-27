package probe

import (
	"encoding/json"
	"os"
	"path/filepath"

	"context"
	"encoding/hex"
	"errors"
	"fmt"
	assign "github.com/plsgiveup/fibre/fibre-assign"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	celfibre "github.com/celestiaorg/celestia-app/v10/fibre"
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
	Timeouts StepTimeouts

	IncludeUnassigned bool

	// EndorsedOnly probes only the validators whose signature the settled
	// promise carries: the shards the chain records them as having taken on.
	// A promise recorded before signatures were verified says nothing either
	// way, and is probed over every assigned validator as before.
	EndorsedOnly bool

	// Policy, if set, decides which publications are probed (sampling), which
	// probes fit the byte and request budgets, and when to back off. nil
	// means probe everything, the pre-policy behaviour.
	Policy Policy

	// run mode
	Once     bool          // probe everything due right now, then exit
	Drain    bool          // run until every known publication's schedule is fully in the past
	Deadline time.Duration // whole-run wall-clock cap (0 = none)

	// pacing (every wait is bounded by MaxSleep)
	MaxSleep    time.Duration // longest sleep between cycles
	MinSleep    time.Duration // shortest, to avoid a busy loop
	MaxLateness time.Duration // a slot older than this is recorded MISSED, not probed
	// MaxLatenessFraction raises MaxLateness to this share of a publication's
	// own window when that is longer: 0.05 is twelve minutes on a four-hour
	// window. The phase is decided from the actual start, so a late probe is
	// judged in the phase it ran in; the bound only keeps a stalled cycle
	// from running a whole window's schedule in one burst. With a hundred
	// validators a point, a fixed ninety seconds was shorter than the time
	// a handful of dead endpoints (dial, TLS and retry timeouts) hold the
	// worker pool, and the tail of every point was filed as NOT_PROBED.
	MaxLatenessFraction float64
	RPCTimeout          time.Duration
	HostCacheTTL        time.Duration

	// RunConfig is what this run was configured with, as the operator's
	// flags and derived settings, recorded in runs.jsonl on start (see
	// status.RunEvent) so a row's verdict can be traced to the settings
	// that produced it. nil records the run without a configuration.
	RunConfig map[string]any

	// Concurrency is how many probes run at once across all validators
	// (default 16). A single validator never sees more than one
	// connection from this vantage at a time, whatever this value is. At 8,
	// a burst of blobs settled within minutes put their tail points past
	// the pool's rate: tail probes started a median 20 s late on Mocha and
	// up to 65 s, which left a timed-out download no room for its retry.
	Concurrency int

	// AllowUnroutableHosts dials a registered host that resolves to loopback
	// or a private range. A local devnet needs it; a public vantage must not
	// have it, because the host is whatever a validator put on chain and
	// dialling it would make this observer a port scanner and a DNS resolver
	// driven from the chain, publishing what it found.
	AllowUnroutableHosts bool

	// InFlightBytes bounds the shard bytes being downloaded at once, which
	// Concurrency alone does not. DownloadShard is a unary RPC: an in-flight
	// probe holds the whole shard as a gRPC receive buffer and again as the
	// unmarshalled message, and the receive limit is deliberately raised to
	// 110% of the expected shard so a large one is not refused. A shard is
	// blob_size x total_rows / original_rows for a validator assigned every
	// row, so eight workers against 128 MiB blobs on a small provider set is
	// gigabytes resident with nothing anywhere to stop it. A probe larger
	// than the whole budget still runs, alone. Zero takes the default.
	InFlightBytes int64

	// BackfillMissed bounds how far back a (re)started prober writes
	// NOT_PROBED markers for slots it never ran. Zero, the default, is no
	// bound: every elapsed slot of every publication the feed holds gets
	// its row, because an obligation without a row is absent from the
	// obligation total, where it should be counted as unobserved. A
	// positive value is for a fresh prober pointed at a data directory
	// with days of history, which would otherwise spend its first minutes
	// writing markers; slots behind it are left without a row. Either way a
	// publication whose rows may have been archived (observer-archive,
	// days after its schedule ended) is not planned again (archivedFrom).
	BackfillMissed time.Duration

	// RetryTransportTimeout re-runs a probe once when the first attempt fails
	// with a transport timeout (TCP connect timeout, TLS handshake timeout, or
	// a gRPC Unavailable whose cause is a timeout), or with a download that
	// ran out of time at a point in the tail of the retention window, the
	// reading the served verdict rests on (shouldRetry). The server's default
	// connection cap is filled by a 16-signer upload, so a probe arriving
	// during an upload waits for a slot and can time out without saying
	// anything about retention. The retry costs one extra request and no
	// bytes, runs RetryDelay or more after the first attempt (queued, not
	// slept on in a worker), and is skipped when it would land in a
	// different schedule phase than the first attempt, or when the policy,
	// asked again at the moment it would run, denies it. Both attempts are recorded in the final measurement's
	// Retry field.
	RetryTransportTimeout bool
	RetryDelay            time.Duration // default 20s
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
	if c.MaxLateness <= 0 {
		c.MaxLateness = 90 * time.Second
	}
	if c.MaxLatenessFraction == 0 {
		c.MaxLatenessFraction = 0.05
	}
	if c.MaxLatenessFraction < 0 {
		c.MaxLatenessFraction = 0
	}
	if c.RPCTimeout <= 0 {
		c.RPCTimeout = 15 * time.Second
	}
	if c.HostCacheTTL <= 0 {
		c.HostCacheTTL = 60 * time.Second
	}
	if c.RetryDelay <= 0 {
		c.RetryDelay = 20 * time.Second
	}
	if c.Concurrency <= 0 {
		c.Concurrency = 16
	}
	if c.InFlightBytes <= 0 {
		c.InFlightBytes = defaultInFlightBytes
	}
	// BackfillMissed <= 0 means no horizon: every elapsed slot of every
	// publication the feed holds gets its NOT_PROBED row, so an obligation
	// the prober never reached is counted as unobserved rather than
	// vanishing from the total.
	if c.BackfillMissed < 0 {
		c.BackfillMissed = 0
	}
	if c.Vantage == "" {
		c.Vantage = "local"
	}
	return c
}

// Prober turns the scanner's publications into scheduled probes and raw
// measurements. The pending-probe queue is never persisted: it is re-derived
// from publications.jsonl + measurements.jsonl every cycle, so a restart
// resumes exactly (per target, so a point interrupted half-way is finished
// for the remaining validators).
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
	// state.json, re-read every cycle. A probe whose verdict would rest on
	// "no other promise owns these rows" consults it first.
	gaps []scan.ScanGap
	// scanned is how far the scanner has read, as the chain's clock: the
	// block time of last_scanned_height. Promises settled after it are not
	// in the feed, so until it passes a publication's settlement plus the
	// payment-promise timeout the shadow candidate set is incomplete.
	scanned  scannedMark
	resolver *Resolver
	store    *MeasurementStore
	// sampled is the record of publications the load policy drew out of
	// the sample (sampledout.go): one line each instead of a NOT_PROBED row
	// per validator per point. nil in tests that build a Prober by hand,
	// which then write the rows as before.
	sampled  *SampledOutStore
	feed     *pubFeed
	registry *hostRegistry
	chainID  string
	// requests is where a FAULT is queued for a second vantage to confirm
	// (confirm.go); nil in tests that build a Prober by hand.
	requests *requestLog

	clockMu     sync.Mutex
	clockOffset time.Duration // observer clock - latest block time

	coders map[[2]int]*Coder // keyed by (originalRows, totalRows)

	// complete marks (vantage, promise, point) slots every target of which
	// has a row; plan skips them without resolving targets again.
	complete map[string]bool
	// liveSince is the newest time from which the prober's own files are
	// whole in their live copies: lines dated earlier may have moved to
	// archive/ (internal/record), where the restart index does not read.
	// Zero when nothing was ever archived. Read at the top of every cycle.
	liveSince time.Time
	// sweep counts runDue calls, and rotates the order work is dispatched in
	// so a sweep that runs out of time does not drop the same validators each
	// cycle.
	sweep uint64
	// cycleErrs counts the failures recorded in the cycle now running, so
	// the end of the cycle can tell an OK cycle from one that merely
	// finished. Reset at the top of each cycle; written from the probe
	// goroutines, so atomic.
	cycleErrs atomic.Int64
	// skippedPubs are publications logged once as not probeable (wrong chain,
	// failed settlement tx).
	skippedPubs map[string]bool

	valMu    sync.Mutex
	valLocks map[string]*sync.Mutex // one connection per validator at a time
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
	so, err := OpenSampledOutStore(cfg.DataDir)
	if err != nil {
		st.Close()
		return nil, err
	}
	return &Prober{
		sampled:     so,
		observer:    ObserverInfo{Build: status.BuildRevision(), AssignPin: assign.PinnedCelestiaAppCommit},
		cfg:         cfg,
		log:         log,
		chain:       ch,
		resolver:    NewResolver(ch, cfg.HostCacheTTL),
		store:       st,
		feed:        newPubFeed(cfg.PublicationsPath),
		registry:    newHostRegistry(cfg.RegistryPath),
		requests:    &requestLog{path: ConfirmRequestsPath(cfg.DataDir)},
		coders:      map[[2]int]*Coder{},
		complete:    map[string]bool{},
		skippedPubs: map[string]bool{},
		valLocks:    map[string]*sync.Mutex{},
	}, nil
}

func (p *Prober) schedCfg() ScheduleConfig { return p.cfg.Schedule.withDefaults() }

func (p *Prober) coderFor(originalRows, totalRows int) (*Coder, error) {
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

func (p *Prober) validatorLock(addr string) *sync.Mutex {
	p.valMu.Lock()
	defer p.valMu.Unlock()
	l, ok := p.valLocks[addr]
	if !ok {
		l = &sync.Mutex{}
		p.valLocks[addr] = l
	}
	return l
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
	if p.sampled != nil {
		defer p.sampled.Close()
	}
	if p.requests != nil {
		defer p.requests.close()
	}

	st := status.New(p.cfg.DataDir, "prober", p.cfg.Vantage, status.BuildRevision())
	st.RecordRuns(p.cfg.RunConfig)
	st.Start()
	defer st.Stop("exit")
	p.status = st

	// The chain is asked once at startup for its id; an RPC that is down at
	// boot used to be fatal, and under a supervisor that is a restart every
	// five seconds until it comes back. Wait for it instead.
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
	p.log.Printf("prober up: vantage=%s chain_id=%s tip=%d rpc=%s pubs=%s data=%s concurrency=%d",
		p.cfg.Vantage, id, tip, p.cfg.RPCURL, p.cfg.PublicationsPath, p.store.Path(), p.cfg.Concurrency)

	probed := 0
	for {
		// A fresh cycle starts with no failures against it.
		p.cycleErrs.Store(0)
		if err := ctx.Err(); err != nil {
			if errors.Is(err, context.Canceled) {
				p.log.Printf("stopped (signal): %d probes this run", probed)
				return nil
			}
			p.log.Fatalf("run deadline hit: %v", err)
		}

		p.measureClock(ctx)
		p.pollAppVersion(ctx)
		p.loadGaps()
		p.pollScanned(ctx)
		p.refreshRegistry()
		p.publishProjection()

		if added, err := p.feed.refresh(); err != nil {
			p.log.Fatalf("load publications: %v", err)
		} else if added > 0 {
			p.log.Printf("publications: +%d (%d live)", added, len(p.feed.pubs))
		}

		p.liveSince = p.recordLiveSince()
		now := time.Now()
		due, future, missed, dropped, finished := p.plan(p.feed.all(), now)

		for _, mj := range missed {
			p.recordNotProbed(ctx, mj, "scheduled point elapsed before the prober ran it")
		}
		p.recordDropped(ctx, dropped)
		if len(missed)+len(dropped) > 0 {
			if err := p.store.Sync(); err != nil {
				p.log.Fatalf("sync measurements: %v", err)
			}
		}
		for _, h := range finished {
			p.feed.forget(h)
			p.store.Forget(h)
			if p.cfg.Policy != nil {
				p.cfg.Policy.Forget(h)
			}
			p.forgetPoints(h)
		}

		if len(due) > 0 {
			probed += p.runDue(ctx, due)
		}
		// Only a cycle in which nothing failed is an OK cycle. Marking every
		// cycle OK here overwrote the errors the cycle had just recorded —
		// the chain being unreachable, targets that would not resolve — so
		// /v1/health could never say the prober was failing, only that it was
		// dead. The prober is the process whose output becomes a public
		// statement about an operator; a silent degradation in it is the one
		// this observer can least afford.
		if p.cycleErrs.Load() == 0 {
			st.OK()
		}
		st.Set("probes_this_run", probed)
		st.Set("publications_live", len(p.feed.pubs))
		st.Set("clock_offset_ms", p.clockOffsetMS())

		if p.cfg.Once {
			p.log.Printf("done (--once): %d probes, %d missed slots", probed, len(missed))
			return nil
		}

		if len(future) == 0 {
			if p.cfg.Drain {
				p.log.Printf("done (--drain): every known schedule is in the past; %d probes this run", probed)
				return nil
			}
			// follow mode: nothing to do; poll for new publications.
			if !p.sleep(ctx, p.cfg.MaxSleep) {
				p.log.Printf("stopped (signal): %d probes this run", probed)
				return nil
			}
			continue
		}

		wait := time.Until(future[0].point.At)
		if wait > p.cfg.MaxSleep {
			wait = p.cfg.MaxSleep
		}
		if wait < p.cfg.MinSleep {
			wait = p.cfg.MinSleep
		}
		if !p.sleep(ctx, wait) {
			p.log.Printf("stopped (signal): %d probes this run", probed)
			return nil
		}
	}
}

// clockSkewWarn is the offset from chain time past which every verdict this
// vantage produces is suspect: the phase boundaries are only 30 s (grace
// offset) and 150 s (prune tolerance) wide.
const clockSkewWarn = 30 * time.Second

// measureClock records the observer's clock offset against the chain's latest
// block time. Every phase decision uses the local clock, so a drifted vantage
// would silently mislabel probes; the offset is stamped on every measurement
// and a large one is logged.
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
		p.log.Printf("WARNING: chain app version %d is above the pinned celestia-app major %d; probes are recorded as PROBE_ERROR (assignment pin stale) until this build is re-pinned", v, assign.PinnedCelestiaAppMajor)
	}
	if p.status != nil {
		p.status.Set("app_version", v)
		p.status.Set("pin_stale", cur.PinStale)
	}
}

// publishProjection puts the load policy's last projection, the inputs
// behind the admission probability, in the status file. The policy is
// asked through an interface so this package need not import it.
func (p *Prober) publishProjection() {
	if p.status == nil || p.cfg.Policy == nil {
		return
	}
	pj, ok := p.cfg.Policy.(interface{ ProjectionDetail() map[string]any })
	if !ok {
		return
	}
	if d := pj.ProjectionDetail(); d != nil {
		p.status.Set("sampling", d)
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
	p.gaps = st.Gaps
	p.scanned.height = st.LastScannedHeight
	if !st.LastScannedTime.IsZero() {
		// The scanner records the frontier's block time itself; no RPC
		// round trip is needed to place it on the chain's clock.
		p.scanned.timedFor, p.scanned.at = st.LastScannedHeight, st.LastScannedTime.UTC()
		if p.status != nil {
			p.status.Set("scanned_until", p.scanned.at.Format(time.RFC3339))
		}
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
	if p.scanned.height <= 0 || p.scanned.timedFor == p.scanned.height {
		return
	}
	blk, err := p.chain.Block(ctx, p.scanned.height)
	if err != nil {
		p.log.Printf("scanner frontier #%d: %v (keeping previous mark)", p.scanned.height, err)
		return
	}
	p.scanned.timedFor, p.scanned.at = p.scanned.height, blk.Time.UTC()
	if p.status != nil {
		p.status.Set("scanned_until", p.scanned.at.Format(time.RFC3339))
	}
}

// shadowLagFor names the scanner's lag when it makes the shadow candidate
// set for pub incomplete. A promise whose shard preceded this one on disk
// was created no later than this publication settled, and must settle within
// the payment-promise timeout of its creation; so every candidate has
// settled by settlement + timeout, and until the scanner has read that far
// a promise the feed does not hold may still own the returned rows.
// shadowPending says why an unmatched genuine-rows answer cannot be judged
// at the probe. The Fibre store keeps every (commitment, promise) shard
// side by side and Get(commitment) returns the first readable one in
// promise-hash order (celestia-app fibre/store.go), so which promise
// answers is decided by hash order, not by time: a promise uploaded before
// this probe and settled after it, up to payment_promise_timeout after its
// creation, can own the returned rows and is not in the feed yet. The
// candidate set is complete only once the scanner has read past
// probe time + payment_promise_timeout, which is never at probe time; the
// verdict is deferred to the collector's late judgement (probe_amendments).
func shadowPending(now time.Time, pub scan.Publication, m scannedMark) string {
	frontier := "scanner frontier unknown"
	if m.known() {
		frontier = fmt.Sprintf("scanned to #%d (%s)", m.height, m.at.UTC().Format(time.RFC3339))
	}
	timeout := time.Duration(pub.ParamsAtPublication.PaymentPromiseTimeoutSeconds) * time.Second
	if timeout <= 0 {
		return ShadowGapPendingPrefix + ": a promise uploaded before this probe may settle after it (payment promise timeout not on record); " + frontier
	}
	return fmt.Sprintf(ShadowGapPendingPrefix+": a promise uploaded before this probe may settle until %s and own these rows; %s",
		now.Add(timeout).UTC().Format(time.RFC3339), frontier)
}

// shadowBlindness names why the shadow candidate set for pub is incomplete
// at this probe. It always is (see shadowPending); a scan gap inside the
// interval a shadowing promise could have settled in is named first
// because it is permanent, where the pending case resolves once the
// scanner passes probe time + payment_promise_timeout.
func (p *Prober) shadowBlindness(pub scan.Publication) string {
	now := time.Now().UTC()
	if g := shadowGapFor(p.gaps, now, shardLifetime(pub, p.schedCfg().PruneTolerance)); g != "" {
		return g
	}
	return shadowPending(now, pub, p.scanned)
}

// shardLifetime is the longest a shard over a commitment can outlive the
// settlement of the promise that stored it: creation precedes settlement,
// the store prunes at max(expiry, creation + retention), and the prober
// tolerates prune lag on top. A promise settled earlier than this before a
// probe cannot still have a shard on disk at the probe.
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

// job is one (publication, schedule point) slot; runDue expands it to
// validators.
type job struct {
	pub   scan.Publication
	point SchedulePoint
}

// Policy is the prober's load policy hook (see observer/policy).
type Policy interface {
	// Admit decides whether a publication is probed at all. alreadyStarted
	// is true when any of its points was already handled, in which case the
	// decision must stay "yes" so a schedule is never half-recorded.
	Admit(pub scan.Publication, alreadyStarted bool) (ok bool, reason string)
	// BeforeProbe is asked right before one probe, with the validator's
	// lock held. It may deny it (recorded as NOT_PROBED with reason); an
	// allowed probe runs in full. There is no "handshake only" answer: every
	// scheduled point that runs downloads the shard, whatever the
	// validator's earlier probes returned. An allow reserves the request against the caps and the
	// spacing at once, so two concurrent asks can never both be admitted
	// into the same slot; every allow is settled by exactly one AfterProbe
	// (the probe ran) or one Release (it did not).
	BeforeProbe(pub scan.Publication, t Target, now time.Time) (allow bool, reason string)
	// AfterProbe accounts the bytes and requests a probe consumed, settling
	// the reservation its BeforeProbe made.
	AfterProbe(pub scan.Publication, m Measurement)
	// Release gives back the reservation of a probe BeforeProbe admitted
	// that is not going to run.
	Release(pub scan.Publication, t Target)
	// SamplingFor reports what this publication's admission decision was made
	// with, so every row can carry it and the sample can be audited after the
	// day secret is revealed.
	SamplingFor(pub scan.Publication) (prob float64, binding, commitment string)
	// Forget releases whatever the policy holds for a publication whose
	// schedule is finished. Without it the sticky admit/deny map grows for
	// the life of the process and its bound is reached by history rather
	// than by how many publications are actually in flight.
	Forget(promiseHash string)
}

// skipped is a slot the policy decided not to probe.
type skipped struct {
	job    job
	reason string
}

// probeable reports whether a publication belongs to this prober at all, and
// logs once when it does not.
func (p *Prober) probeable(pub scan.Publication) bool {
	reason := ""
	switch {
	case pub.Assignment.Error != "":
		return false // no assignment table -> nothing to probe (logged by the scanner)
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

// plan splits every not-yet-complete schedule point into due (probe now),
// future (sleep until), missed (record a MISSED marker) and dropped (policy
// refused the publication). finished lists publications whose whole schedule
// is behind the backfill horizon: nothing will ever be recorded for them
// again, so they can be forgotten.
func (p *Prober) plan(pubs []scan.Publication, now time.Time) (due, future, missed []job, dropped []skipped, finished []string) {
	var horizon time.Time // zero: no horizon, every elapsed slot gets a row
	if p.cfg.BackfillMissed > 0 {
		horizon = now.Add(-p.cfg.BackfillMissed)
	}
	for _, pub := range pubs {
		if !p.probeable(pub) {
			continue
		}
		if p.sampled != nil && p.sampled.Has(p.cfg.Vantage, pub.PromiseHash) {
			// Drawn out of the sample and recorded so: nothing is left to
			// write for it, and putting it to the policy again after a
			// restart would draw against today's load.
			finished = append(finished, pub.PromiseHash)
			continue
		}
		points := ScheduleFor(pub, p.cfg.Schedule)
		if archivedFrom(pub, points, p.liveSince) {
			// Some of its rows may be in the archive, which the restart
			// index does not read: planning it would write a second row
			// for a slot that has one. The archive keeps days more than
			// any schedule is long, so its schedule ended long ago.
			finished = append(finished, pub.PromiseHash)
			continue
		}
		late := p.latenessFor(pub)
		var pending []SchedulePoint
		started := false
		allPast := true
		for _, pt := range points {
			key := pointKey(p.cfg.Vantage, pub.PromiseHash, pt.At)
			if p.complete[key] {
				started = true
				continue
			}
			if p.store.HandledPoint(p.cfg.Vantage, pub.PromiseHash, pt.At) {
				started = true // at least one target has a row; finish the rest
			}
			if pt.At.After(horizon) {
				allPast = false
			}
			pending = append(pending, pt)
		}
		if len(pending) == 0 || allPast {
			// every point is complete, or so old that nothing will be
			// written for it: forget the publication.
			finished = append(finished, pub.PromiseHash)
			continue
		}
		if p.cfg.Policy != nil {
			if ok, reason := p.cfg.Policy.Admit(pub, started); !ok {
				for _, pt := range pending {
					if pt.At.After(horizon) {
						dropped = append(dropped, skipped{job{pub, pt}, reason})
					}
				}
				continue
			}
		}
		for _, pt := range pending {
			switch {
			case now.Before(pt.At):
				future = append(future, job{pub, pt})
			case now.Sub(pt.At) <= late:
				due = append(due, job{pub, pt})
			case pt.At.After(horizon):
				missed = append(missed, job{pub, pt})
			default:
				// behind the backfill horizon: left without a row
				p.complete[pointKey(p.cfg.Vantage, pub.PromiseHash, pt.At)] = true
			}
		}
	}
	sort.SliceStable(future, func(i, j int) bool { return future[i].point.At.Before(future[j].point.At) })
	sort.SliceStable(due, func(i, j int) bool { return due[i].point.At.Before(due[j].point.At) })
	return due, future, missed, dropped, finished
}

// archiveSkew is how far a row's own date may sit before the publication's
// settlement time: a sampling decision is dated by the prober's clock, the
// settlement by the block's.
const archiveSkew = time.Hour

// archivedFrom reports whether a row of pub may be dated before liveSince,
// and so may be in the archive rather than the live file. Every row the
// prober writes for pub is dated at one of its schedule points (a
// measurement's scheduled_at) or after its settlement (a sampling
// decision's decided_at, less the clock skew). When the earlier of the two
// is at or after liveSince, every one of them is live: the archiver cuts
// before the first line dated at or after its cutoff, and never past it.
func archivedFrom(pub scan.Publication, points []SchedulePoint, liveSince time.Time) bool {
	if liveSince.IsZero() {
		return false
	}
	earliest := pub.SettlementTime
	for _, pt := range points {
		if earliest.IsZero() || pt.At.Before(earliest) {
			earliest = pt.At
		}
	}
	return earliest.Add(-archiveSkew).Before(liveSince)
}

// recordLiveSince is the newer of the live-since times of the files the
// restart index reads: both are whole in their live copies from then on.
func (p *Prober) recordLiveSince() time.Time {
	t := record.LiveSince(p.store.Path())
	if p.sampled != nil {
		if s := record.LiveSince(p.sampled.Path()); s.After(t) {
			t = s
		}
	}
	return t
}

// latenessFor is how long past its scheduled time a slot of pub may still
// be probed: MaxLateness, or MaxLatenessFraction of the publication's own
// window when that is longer. The phase is decided from the actual start
// (PhaseAt), so a late probe is judged in the phase it ran in, never the
// one it was planned for.
//
// An in-window slot is additionally never allowed to run past
// must_serve_until. The allowance is a fraction of the publication's own
// window — twelve minutes on mocha's four hours — and the last in-window
// point sits 2m30s before the deadline, so without this the reading that
// exists to catch an early prune could run nine minutes after the obligation
// ended, where NOT_FOUND is TOLERATED by construction. The whole schedule
// change that moved that point to the deadline would then have bought
// nothing whenever the prober was busy. A slot that cannot run in time is
// recorded NOT_PROBED instead: an obligation nobody observed is unobserved,
// which is a figure this observer publishes, and not `served`.
func (p *Prober) latenessFor(pub scan.Publication) time.Duration {
	return p.latenessAt(pub, SchedulePoint{})
}

func (p *Prober) latenessAt(pub scan.Publication, pt SchedulePoint) time.Duration {
	late := p.cfg.MaxLateness
	if p.cfg.MaxLatenessFraction > 0 {
		window := pub.MustServeUntil.Sub(pub.SettlementTime)
		if window <= 0 {
			window = fallbackSpan(pub)
		}
		if f := time.Duration(float64(window) * p.cfg.MaxLatenessFraction); f > late {
			late = f
		}
	}
	if pt.Phase == PhaseInWindow && !pt.At.IsZero() {
		if room := pub.MustServeUntil.Sub(pt.At); room > 0 && room < late {
			late = room
		}
	}
	return late
}

// work is one (publication, point, validator) probe ready to run.
type work struct {
	job        job
	target     Target
	coder      *Coder
	commitment [32]byte
	key        string    // point key, for completion tracking
	deadline   time.Time // the last moment it may start (see latenessAt)
}

// retryReq is a first attempt that earned the transport-timeout retry: it
// waits in the sweep's retry queue, holding no worker and no lock, until at.
type retryReq struct {
	it    work
	in    Input
	first Measurement
	at    time.Time
}

// runDue probes every due slot. Slots are grouped by publication so the
// validator set / host registry is resolved once per group; the resulting
// (point, validator) probes then run on a pool of Concurrency workers, with
// at most one in-flight probe per validator. A group whose targets cannot be
// resolved is left for the next cycle (not marked, so it retries until it
// either succeeds or ages into MISSED). A point is marked complete only when
// every one of its targets has a row.
func (p *Prober) runDue(ctx context.Context, due []job) int {
	byPub := map[string][]job{}
	order := []string{}
	for _, j := range due {
		if _, ok := byPub[j.pub.PromiseHash]; !ok {
			order = append(order, j.pub.PromiseHash)
		}
		byPub[j.pub.PromiseHash] = append(byPub[j.pub.PromiseHash], j)
	}

	var items []work
	pointItems := map[string]int{}
	for _, ph := range order {
		jobs := byPub[ph]
		pub := jobs[0].pub

		targets, err := p.resolver.TargetsFor(ctx, pub, p.cfg.IncludeUnassigned)
		if err != nil {
			p.log.Printf("resolve targets for %s: %v (retry next cycle)", short(ph), err)
			p.fail(fmt.Sprintf("resolve targets: %v", err))
			continue
		}
		coder, cerr := p.coderFor(pub.Assignment.ProtocolParams.OriginalRows, pub.Assignment.ProtocolParams.TotalRows)
		if cerr != nil {
			p.log.Printf("coder for %s: %v (retry next cycle)", short(ph), cerr)
			continue
		}
		var commitment [32]byte
		cb, cerr := hex.DecodeString(pub.Promise.Commitment)
		if cerr != nil || len(cb) != len(commitment) {
			// The resolver checks this too, so today this cannot fire. A
			// zero commitment would ask every validator for a blob nobody
			// has and publish the whole set as failing, which is too bad an
			// outcome to leave guarded only by a check somewhere else.
			p.log.Printf("publication %s: commitment %q is not 32 hex bytes; skipping", short(ph), pub.Promise.Commitment)
			continue
		}
		copy(commitment[:], cb)

		for _, j := range jobs {
			key := pointKey(p.cfg.Vantage, ph, j.point.At)
			for _, t := range targets {
				if !p.wants(t) || p.store.Has(p.cfg.Vantage, ph, t.AddressHex, j.point.At) {
					continue
				}
				items = append(items, work{job: j, target: t, coder: coder, commitment: commitment, key: key,
					deadline: j.point.At.Add(p.latenessAt(pub, j.point))})
				pointItems[key]++
			}
			if pointItems[key] == 0 {
				p.complete[key] = true // every target already recorded
			}
		}
	}
	if len(items) == 0 {
		return 0
	}
	// Targets come out of the resolver in validator-set order, and a sweep
	// that runs out of time drops whatever is left. Probing in that order
	// every cycle took the coverage from the same validators every time — the
	// tail of the set by voting power — so their published rates rested on
	// systematically less evidence than everyone else's, and nothing said so.
	// A per-cycle rotation spreads the loss instead of concentrating it.
	orderItems(items, p.sweep)
	p.sweep++

	var (
		mu       sync.Mutex
		n        int
		done     = map[string]int{}
		sem      = make(chan struct{}, p.cfg.Concurrency)
		bytesSem = newByteSem(p.cfg.InFlightBytes)
		wg       sync.WaitGroup
		rwg      sync.WaitGroup
		canceled bool
		retries  = make(chan retryReq, len(items))
		retried  = make(chan struct{})
	)
	// Charged what the probe may actually receive, not what the shard
	// should weigh: the two were different, and the budget was keeping the
	// wrong one.
	weight := func(it work) int64 {
		return int64(recvLimitFor(Input{
			ExpectedShardBytes: ShardBytes(it.job.pub.Promise.BlobSize, it.job.pub.Assignment.ProtocolParams.OriginalRows, it.target.RowCount),
			MaxMessageSize:     maxMessageSizeFor(it.job.pub.Assignment.ProtocolParams),
		}))
	}
	// The retry queue. A retry used to sleep RetryDelay inside the worker
	// that ran the first attempt, holding the worker, its byte budget and
	// the validator's lock: under a correlated outage, when every probe
	// times out, the whole pool sat asleep and the deadline-bound points
	// behind it went unprobed. Queued here it holds nothing while it waits,
	// and takes a worker like any other probe when its time comes.
	go func() {
		defer close(retried)
		for r := range retries {
			if ctx.Err() == nil {
				sleepCtx(ctx, time.Until(r.at))
			}
			sem <- struct{}{}
			want := weight(r.it)
			bytesSem.acquire(want)
			rwg.Add(1)
			go func(r retryReq, want int64) {
				defer rwg.Done()
				defer func() { <-sem }()
				defer bytesSem.release(want)
				p.runRetry(ctx, r)
				mu.Lock()
				done[r.it.key]++
				mu.Unlock()
			}(r, want)
		}
	}()
	for _, it := range items {
		if ctx.Err() != nil {
			canceled = true
			break
		}
		sem <- struct{}{}
		want := weight(it)
		bytesSem.acquire(want)
		wg.Add(1)
		go func(it work, want int64) {
			defer wg.Done()
			defer func() { <-sem }()
			defer bytesSem.release(want)
			probedOne, retry := p.runOne(ctx, it)
			if retry != nil {
				retries <- *retry
			}
			mu.Lock()
			if probedOne {
				n++
			}
			if retry == nil {
				done[it.key]++
			}
			mu.Unlock()
		}(it, want)
	}
	wg.Wait()
	close(retries) // every first attempt is in; the queue drains, then stops
	<-retried
	rwg.Wait()
	if !canceled && ctx.Err() == nil {
		for key, want := range pointItems {
			if done[key] == want {
				p.complete[key] = true
			}
		}
	}
	return n
}

// defaultInFlightBytes is the shard-byte ceiling: half a gibibyte of shards
// being transferred at once, which with the receive buffer and the
// unmarshalled copy is about a gibibyte resident at the peak.
const defaultInFlightBytes = 512 << 20

// byteSem admits work by weight as well as by count. A single item heavier
// than the whole budget is admitted alone rather than deadlocking, which is
// the case that matters: one validator assigned every row of a large blob.
type byteSem struct {
	mu    sync.Mutex
	cond  *sync.Cond
	limit int64
	held  int64
}

func newByteSem(limit int64) *byteSem {
	b := &byteSem{limit: limit}
	b.cond = sync.NewCond(&b.mu)
	return b
}

func (b *byteSem) acquire(nBytes int64) {
	if nBytes <= 0 {
		nBytes = 1
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	for b.held > 0 && b.held+nBytes > b.limit {
		b.cond.Wait()
	}
	b.held += nBytes
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

// runOne runs a single probe end to end: lateness re-check, policy gate,
// per-validator serialisation, the probe itself, and the record. It reports
// whether a network probe was carried out and, when the first attempt earned
// the transport-timeout retry, the retry to queue: the record is then left
// to runRetry.
func (p *Prober) runOne(ctx context.Context, it work) (bool, *retryReq) {
	j, t, pub := it.job, it.target, it.job.pub
	ph := pub.PromiseHash

	// The slot was due when planned; a long cycle must not silently probe it
	// in a later phase. Past the lateness allowance it is a gap, not a
	// verdict.
	if allowed := p.latenessAt(pub, j.point); time.Since(j.point.At) > allowed {
		late := time.Since(j.point.At)
		reason := fmt.Sprintf("elapsed while the cycle ran (%s late)", late.Round(time.Second))
		if j.point.Phase == PhaseInWindow && !time.Now().Before(pub.MustServeUntil) {
			reason = fmt.Sprintf("elapsed while the cycle ran (%s late); the obligation ended before this reading could be taken, so it is unobserved rather than judged in a later phase", late.Round(time.Second))
		}
		p.recordNotProbedTarget(pub, j.point, t, reason)
		return false, nil
	}
	// The validator's lock is taken before the policy is asked, and the
	// policy is told about the probe before the lock goes, so for one
	// validator "ask, probe, account" is a single step. The policy used to be
	// asked first and the lock taken after: with eight workers the first
	// burst of blobs put one validator in several work items, all of them
	// were admitted in the same instant on a state that included none of the
	// others, and then ran back to back behind the lock with no spacing and
	// up to seven shards over the per-validator caps. The policy now also
	// reserves the slot when it admits (see policy.BeforeProbe), which makes
	// the caps hold whoever calls it; the lock is what makes the spacing a
	// spacing between request starts, because an admission taken while an
	// earlier probe of the validator was still running would otherwise be
	// dated from the admission, not from when it got to go out.
	lock := p.validatorLock(t.AddressHex)
	lock.Lock()
	if p.cfg.Policy != nil {
		if allow, reason := p.cfg.Policy.BeforeProbe(pub, t, time.Now()); !allow {
			lock.Unlock()
			p.recordNotProbedTarget(pub, j.point, t, reason)
			return false, nil
		}
	}
	in := Input{
		Vantage:             p.cfg.Vantage,
		ChainID:             p.chainID,
		PromiseHash:         ph,
		Commitment:          it.commitment,
		CommitmentHex:       pub.Promise.Commitment,
		BlobVersion:         pub.Promise.BlobVersion,
		MustServeUntil:      pub.MustServeUntil,
		ValidatorSetHeight:  pub.Assignment.ValidatorSetHeight,
		Target:              t,
		AllowUnroutableHost: p.cfg.AllowUnroutableHosts,
		SchedulePoint:       j.point,
		PruneTolerance:      p.schedCfg().PruneTolerance,
		ExpectedShardBytes:  ShardBytes(pub.Promise.BlobSize, pub.Assignment.ProtocolParams.OriginalRows, t.RowCount),
		MaxMessageSize:      maxMessageSizeFor(pub.Assignment.ProtocolParams),
		ClockOffsetMS:       p.clockOffsetMS(),
		Shadowers:           p.feed.shadowersFor(ph, pub.Promise.Commitment, t.AddressHex),
		ShadowGap:           p.shadowBlindness(pub),
		Observer:            p.observerInfo(),
	}

	// Checked again under the lock: waiting for this validator's previous
	// probe, and then the policy's spacing, can carry the slot past its
	// allowance, and a probe started then is judged in whatever phase it
	// lands in — for the last in-window point, past the deadline, where
	// NOT_FOUND is tolerated by construction. The policy admitted it, so its
	// reservation is given back: the request never goes out.
	if allowed := p.latenessAt(pub, j.point); time.Since(j.point.At) > allowed {
		lock.Unlock()
		if p.cfg.Policy != nil {
			p.cfg.Policy.Release(pub, t)
		}
		p.recordNotProbedTarget(pub, j.point, t, fmt.Sprintf("elapsed waiting for this validator's previous probe (%s late)", time.Since(j.point.At).Round(time.Second)))
		return false, nil
	}
	m := Run(ctx, in, it.coder, p.cfg.Timeouts)
	if p.cfg.RetryTransportTimeout && shouldRetry(m, pub, p.cfg.Schedule, p.cfg.RetryDelay, time.Now()) {
		// The first attempt is a request the endpoint received, accounted
		// now, not when the retry is done: every other probe of this
		// validator in the meantime, and the retry's own policy check,
		// decide on the policy's state. Told
		// before the lock goes, so the next probe of the validator is
		// asked about on a state that includes it.
		if p.cfg.Policy != nil {
			p.cfg.Policy.AfterProbe(pub, m)
		}
		lock.Unlock()
		p.log.Printf("probe %s %s: %s (%s); retrying once in %s", short(ph), t.Host, m.Outcome, m.RawError, p.cfg.RetryDelay)
		return true, &retryReq{it: it, in: in, first: m, at: time.Now().Add(p.cfg.RetryDelay)}
	}
	p.finish(ctx, it, in, m, false, lock, false)
	return true, nil
}

// runRetry runs a queued retry, or records the first attempt alone when the
// retry would now land in another phase, the sweep is being stopped, or the
// policy, asked again now, says no.
//
// The policy decided the first attempt, and the retry waited at least
// RetryDelay since: meanwhile other probes of the validator can have used up
// a budget, and a retry run on the old decision was a request the policy
// would have refused.
func (p *Prober) runRetry(ctx context.Context, r retryReq) {
	pub, t := r.it.job.pub, r.it.target
	// Asked under the validator's lock, as in runOne, so the answer and the
	// request it admits are one step for this validator.
	lock := p.validatorLock(t.AddressHex)
	lock.Lock()
	skip := ""
	admitted := false
	if p.cfg.Policy != nil && ctx.Err() == nil {
		if allow, reason := p.cfg.Policy.BeforeProbe(pub, t, time.Now()); allow {
			admitted = true
		} else {
			skip = reason
		}
	}
	m := r.first
	switch {
	case skip != "":
		m.ClassificationReason += "; retry not run: " + skip
	case ctx.Err() == nil && PhaseAt(time.Now(), pub, p.cfg.Schedule) == r.first.Phase:
		m = retryOnce(ctx, r.in, r.it.coder, p.cfg.Timeouts, r.first, time.Since(r.first.FinishedAt).Round(time.Second))
		p.finish(ctx, r.it, r.in, m, false, lock, false)
		return
	}
	// Admitted, but the phase moved on or the sweep is stopping while the
	// policy was being asked: the reservation goes back with the request.
	if admitted {
		p.cfg.Policy.Release(pub, t)
	}
	// The retry is not run, and neither is the evidence probe of the
	// settlement host: it is a request too, and whatever stopped the retry
	// (the policy's answer, the phase that moved on, the sweep stopping)
	// stops it as well. The first attempt is recorded as it stands; it was
	// accounted when it was queued.
	p.finish(ctx, r.it, r.in, m, true, lock, true)
}

// finish completes a probe whose validator lock is held: the policy is told
// about m, the settlement-host evidence probe runs when the validator
// re-registered and noHostProbe is false, then the lock goes and the row is
// written. accounted is true when the policy has already been told about m
// (a first attempt, accounted when its retry was queued).
func (p *Prober) finish(ctx context.Context, it work, in Input, m Measurement, noHostProbe bool, lock *sync.Mutex, accounted bool) {
	t, pub := it.target, it.job.pub
	m.HostAtSettlement = t.HostAtSettlement
	// Accounted while the lock is still held, so the next probe of this
	// validator (and the evidence probe below) is asked about on a state
	// that already includes this one. It used to be told after the lock
	// went, which left a window in which the next probe was admitted
	// without it.
	if p.cfg.Policy != nil && !accounted {
		p.cfg.Policy.AfterProbe(pub, m)
	}
	if hostChanged(t) && !noHostProbe && m.Outcome != OutcomeServedOK && m.Classification != ClassProbeError {
		// The validator re-registered since the promise settled and its
		// current host did not serve: ask the host the upload went to, as
		// evidence, on the same lock so the validator still sees one
		// connection at a time. The verdict stays the current host's.
		//
		// It is a request like any other, to the same validator, so it asks
		// the policy like any other and is accounted like any other. It used
		// to do neither: the one extra request the observer makes of a
		// validator whose current host is already failing went out with no
		// spacing after the probe that just failed, past the caps.
		ht := t
		ht.Host, ht.HostSource = t.HostAtSettlement, "settlement"
		skip := ""
		if p.cfg.Policy != nil {
			if allow, reason := p.cfg.Policy.BeforeProbe(pub, ht, time.Now()); !allow {
				skip = reason
			}
		}
		if skip != "" {
			m.ClassificationReason += "; " + hostChangeNote(t, nil) + "; the host registered at settlement was not probed: " + skip
		} else {
			hp, e := settlementProbe(ctx, in, it.coder, p.cfg.Timeouts)
			if p.cfg.Policy != nil {
				p.cfg.Policy.AfterProbe(pub, e)
			}
			m.SettlementHost = hp
			m.ClassificationReason += "; " + hostChangeNote(t, m.SettlementHost)
		}
	}
	lock.Unlock()

	p.stampSampling(&m, pub)
	if err := p.store.Append(m); err != nil {
		p.log.Fatalf("append measurement: %v", err)
	}
	p.logMeasurement(m)
	// After the row is on disk, so a request never names a row that is not.
	p.requestConfirmation(pub, t, m)
}

// hostChanged reports whether the validator's current host differs from the
// one registered when the promise settled, both being known.
func hostChanged(t Target) bool {
	return t.HostAtSettlement != "" && t.Host != "" && t.Host != t.HostAtSettlement && t.HostSource != "settlement"
}

// settlementProbe runs the evidence probe of the settlement host. It returns
// the evidence for the row and the probe's own measurement, which is what the
// policy accounts the request from.
func settlementProbe(ctx context.Context, in Input, coder *Coder, to StepTimeouts) (*HostProbe, Measurement) {
	in.Target.Host, in.Target.HostSource = in.Target.HostAtSettlement, "settlement"
	e := Run(ctx, in, coder, to)
	return &HostProbe{Host: in.Target.Host, Outcome: e.Outcome, RowsReturned: e.Download.RowsReturned,
		CommitmentVerified: e.Download.CommitmentVerified, AssignmentVerified: e.Download.AssignmentVerified,
		DurationMS: e.TotalDurationMS, RawError: e.RawError}, e
}

// hostChangeNote is appended to the reason of a row whose validator moved.
func hostChangeNote(t Target, hp *HostProbe) string {
	note := fmt.Sprintf("host changed since settlement (%s -> %s)", t.HostAtSettlement, t.Host)
	if hp == nil {
		return note
	}
	if hp.Outcome == OutcomeServedOK {
		return note + "; the host registered at settlement still serves the exact rows, so the data was left behind, not lost"
	}
	return note + "; the host registered at settlement answered " + string(hp.Outcome)
}

// recordDropped records the slots the policy refused. A publication the
// sampler drew out whole is recorded once, as a SampledOut decision naming
// its points; the rows it stands for are the ones recordNotProbed would have
// written, and every reader expands it to them (SampledOut.Expand, the
// store's sampled_out_rows). Anything else refused, and every refusal when
// the decision cannot stand for its rows, keeps the per-target rows:
//   - with IncludeUnassigned the targets include validators with no rows,
//     which the publication record the decision expands against does not
//     list;
//   - a refusal whose reason is not a sampling draw is not a decision
//     about the whole publication.
func (p *Prober) recordDropped(ctx context.Context, dropped []skipped) {
	type group struct {
		pub    scan.Publication
		reason string
		points []SchedulePoint
	}
	var order []string
	byPub := map[string]*group{}
	for _, d := range dropped {
		if p.sampled == nil || p.cfg.IncludeUnassigned || !strings.HasPrefix(d.reason, SampledOutReasonPrefix) {
			p.recordNotProbed(ctx, d.job, d.reason)
			continue
		}
		g, ok := byPub[d.job.pub.PromiseHash]
		if !ok {
			g = &group{pub: d.job.pub, reason: d.reason}
			byPub[d.job.pub.PromiseHash] = g
			order = append(order, d.job.pub.PromiseHash)
		}
		g.points = append(g.points, d.job.point)
	}
	for _, h := range order {
		g := byPub[h]
		p.recordSampledOut(g.pub, g.points, g.reason)
	}
}

// recordSampledOut writes the one decision a sampled-out publication is
// recorded with, and marks its points complete.
func (p *Prober) recordSampledOut(pub scan.Publication, points []SchedulePoint, reason string) {
	d := SampledOut{
		SchemaVersion: SampledOutSchemaVersion, Kind: SampledOutKind, Vantage: p.cfg.Vantage,
		PromiseHash: pub.PromiseHash, Commitment: pub.Promise.Commitment, BlobVersion: pub.Promise.BlobVersion,
		SettlementTime: pub.SettlementTime.UTC(), MustServeUntil: pub.MustServeUntil,
		ValidatorSetHeight: pub.Assignment.ValidatorSetHeight,
		DecidedAt:          time.Now().UTC(), Reason: reason,
	}
	if p.cfg.Policy != nil {
		prob, binding, commitment := p.cfg.Policy.SamplingFor(pub)
		d.Sampling = SamplingDecision{P: prob, Binding: binding, DayCommitment: commitment}
	}
	for _, pt := range points {
		d.Points = append(d.Points, SampledOutPoint{Label: pt.Label, At: pt.At.UTC(), Phase: PhaseAt(pt.At, pub, p.cfg.Schedule)})
	}
	for _, v := range pub.Assignment.Validators {
		if v.RowCount > 0 {
			d.Validators++
		}
	}
	obs := p.observerInfo()
	d.Observer = &obs
	if err := p.sampled.Append(d); err != nil {
		p.log.Fatalf("append sampling decision: %v", err)
	}
	for _, pt := range points {
		p.complete[pointKey(p.cfg.Vantage, pub.PromiseHash, pt.At)] = true
	}
	p.log.Printf("SAMPLED-OUT %s p=%.3f binding=%s: %d points x %d validators recorded as one decision",
		short(pub.PromiseHash), d.Sampling.P, d.Sampling.Binding, len(d.Points), d.Validators)
}

// recordNotProbed marks one (publication, point) slot NOT_PROBED for every
// target, with the given reason (elapsed, or a policy decision). Rows are
// appended without fsync; the caller syncs once per batch.
func (p *Prober) recordNotProbed(ctx context.Context, j job, reason string) {
	pub := j.pub
	key := pointKey(p.cfg.Vantage, pub.PromiseHash, j.point.At)
	targets, err := p.resolver.TargetsFor(ctx, pub, p.cfg.IncludeUnassigned)
	if err != nil {
		// The chain call failed, but the publication record already names
		// every assigned validator and its row count, so a row per validator
		// can still be written without touching the chain. Writing nothing
		// made the publication disappear: no probe row, and no gap counter
		// moved either, so a reader saw a clean window with no sign that a
		// whole publication had gone unobserved.
		p.log.Printf("not-probed %s %s: targets unresolved: %v (%s); recording from the publication record instead",
			short(pub.PromiseHash), j.point.Label, err, reason)
		for _, v := range pub.Assignment.Validators {
			p.recordNotProbedTarget(pub, j.point, Target{
				AddressHex: v.Address,
				Assigned:   v.RowCount > 0,
				Attested:   v.Attested,
				RowCount:   v.RowCount,
				// A record from before signature verification says nothing
				// about attestation, and "says nothing" must not be stored
				// as "did not attest": the store writes NULL for the first
				// and 0 for the second, and 0 puts the row outside the
				// obligation population instead of into the unknown count.
				AttestationUnknown: !pub.HasAttestation(),
			}, reason+"; targets could not be resolved: "+err.Error())
		}
		p.complete[key] = true
		return
	}
	for _, t := range targets {
		p.recordNotProbedTarget(pub, j.point, t, reason)
	}
	p.complete[key] = true
}

// wants reports whether a target is read at all: under EndorsedOnly only the
// validators the settled promise names as signers. A target it declines gets
// no row, probed or NOT_PROBED alike: a validator with nothing to answer for
// on this promise is not a gap in its coverage.
func (p *Prober) wants(t Target) bool {
	return !p.cfg.EndorsedOnly || t.AttestationUnknown || t.Attested
}

// recordNotProbedTarget writes one NOT_PROBED measurement for a single target.
func (p *Prober) recordNotProbedTarget(pub scan.Publication, pt SchedulePoint, t Target, reason string) {
	if !p.wants(t) || p.store.Has(p.cfg.Vantage, pub.PromiseHash, t.AddressHex, pt.At) {
		return
	}
	m := Measurement{
		SchemaVersion: MeasurementSchemaVersion, Vantage: p.cfg.Vantage,
		PromiseHash: pub.PromiseHash, Commitment: pub.Promise.Commitment,
		BlobVersion: pub.Promise.BlobVersion, MustServeUntil: pub.MustServeUntil,
		ValidatorSetHeight: pub.Assignment.ValidatorSetHeight,
		ValidatorAddress:   t.AddressHex, ValidatorHost: t.Host, HostSource: t.HostSource,
		Assigned: t.Assigned, Attested: t.Attested, AttestationUnknown: t.AttestationUnknown,
		AssignedRowCount: t.RowCount,
		ScheduleLabel:    pt.Label, ScheduledAt: pt.At.UTC(),
		StartedAt: time.Now().UTC(), FinishedAt: time.Now().UTC(),
		Phase: PhaseAt(pt.At, pub, p.cfg.Schedule), Outcome: OutcomeMissed,
		Classification: ClassNotProbed, ClassificationReason: reason,
	}
	p.stampSampling(&m, pub)
	if err := p.store.AppendDeferred(m); err != nil {
		p.log.Fatalf("append not-probed measurement: %v", err)
	}
	p.logMeasurement(m)
}

// stampSampling records the admission decision on a row. Without it the
// commit-and-reveal audit could only be carried out against the publications
// that were denied, which is the half that needs it least.
func (p *Prober) stampSampling(m *Measurement, pub scan.Publication) {
	if p.cfg.Policy == nil {
		return
	}
	prob, binding, commitment := p.cfg.Policy.SamplingFor(pub)
	if commitment == "" {
		return // no draw was made: a policy that does not sample
	}
	m.Sampling = &SamplingDecision{P: prob, Binding: binding, DayCommitment: commitment}
}

func (p *Prober) logMeasurement(m Measurement) {
	tail := ""
	if m.Download.Attempted {
		tail = fmt.Sprintf(" rows=%d/%d commit=%v assign=%v", m.Download.RowsReturned, m.Download.RowsExpected, m.Download.CommitmentVerified, m.Download.AssignmentVerified)
	}
	p.log.Printf("PROBE %s val=%s %s[%s] assigned=%v phase=%s -> %s / %s (%dms lat=%dms)%s",
		short(m.PromiseHash), short(m.ValidatorAddress), m.ScheduleLabel, m.Phase, m.Assigned,
		m.Phase, m.Outcome, m.Classification, m.TotalDurationMS, m.LatenessMS, tail)
	if m.Classification == ClassFault {
		p.log.Printf("  FAULT reason: %s | raw: %s", m.ClassificationReason, truncate(m.RawError, 160))
	}
}

// maxMessageSizeFor returns the gRPC receive bound implied by a publication's
// own recorded protocol params, so a blob encoded under params this binary was
// not built against is still judged rather than refused by our own limit.
// It mirrors celestia-app's ProtocolParams.MaxMessageSize.
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

// forgetPoints drops the per-point "every target recorded" markers for a
// publication whose schedule is over. The map was only ever added to, at six
// call sites, so a long-running vantage grew one entry per publication per
// schedule point for as long as the process lived — while the two maps beside
// it were already being released here.
func (p *Prober) forgetPoints(promiseHash string) {
	prefix := p.cfg.Vantage + "|" + promiseHash + "|"
	for k := range p.complete {
		if strings.HasPrefix(k, prefix) {
			delete(p.complete, k)
		}
	}
}

// orderItems is the order a sweep starts its probes in: earliest deadline
// first, with the per-sweep rotation surviving as the order within a
// deadline. The rotation alone ran a sweep in whatever order it landed on,
// so the last in-window point of one blob, allowed 2m30s before its deadline
// cuts it off, could wait behind a dozen early points of others allowed
// twelve minutes, and come out NOT_PROBED: the one reading that catches an
// early prune, lost to scheduling. The sort is stable, so the rotation still
// spreads any loss among the validators of one point.
func orderItems(items []work, sweep uint64) {
	rotateItems(items, sweep)
	sort.SliceStable(items, func(i, j int) bool { return items[i].deadline.Before(items[j].deadline) })
}

// rotateItems rotates the work list by a per-sweep offset, keeping each
// publication's points together so the schedule still runs in order within a
// blob. It is a rotation rather than a shuffle so the order stays
// reproducible from the sweep number alone.
func rotateItems(items []work, sweep uint64) {
	if len(items) < 2 {
		return
	}
	off := int(sweep % uint64(len(items)))
	if off == 0 {
		return
	}
	rotated := make([]work, 0, len(items))
	rotated = append(rotated, items[off:]...)
	rotated = append(rotated, items[:off]...)
	copy(items, rotated)
}

func short(s string) string {
	if len(s) > 12 {
		return s[:12]
	}
	return s
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}

// shouldRetry decides whether a first attempt deserves the one retry: the
// outcome must be a transport timeout, or a download that ran out of time at
// a point in the tail of the retention window, and the retry, started after
// delay, must still fall in the same schedule phase as the first attempt (a
// retry that crossed from in_window into grace would change the verdict, not
// just the evidence).
//
// A download timeout is otherwise left alone: the server answered and the
// transfer stalled, so running it again early in the window buys nothing a
// later point will not show. In the tail it is the reading the served verdict
// rests on. Without a second try one stalled transfer a minute before the
// deadline leaves an obligation the validator kept, on every earlier point,
// out of the rate as not observed.
func shouldRetry(m Measurement, pub scan.Publication, sc ScheduleConfig, delay time.Duration, now time.Time) bool {
	if m.Retry != nil {
		return false // already retried
	}
	tailDeadline := m.Outcome == OutcomeRPCDeadline && m.Phase == PhaseInWindow && InEndSegment(m.ScheduledAt, pub)
	if !m.transportTimeout() && !tailDeadline {
		return false
	}
	return PhaseAt(now.Add(delay), pub, sc) == m.Phase
}

// transportTimeout reports whether the measurement failed because a
// connection could not be established in time: TCP connect timeout, TLS
// handshake timeout, or gRPC Unavailable caused by a timeout. A download that
// started and then ran out of time is not a transport timeout.
func (m Measurement) transportTimeout() bool {
	switch m.Outcome {
	case OutcomeTCPTimeout:
		return true
	case OutcomeTLSFail:
		return m.TLS.Attempted && isTimeoutText(m.TLS.Error)
	case OutcomeRPCUnavailable:
		return m.Download.Attempted && isTimeoutText(m.Download.Error)
	}
	return false
}

func isTimeoutText(s string) bool {
	ls := strings.ToLower(s)
	return strings.Contains(ls, "timeout") || strings.Contains(ls, "deadline exceeded")
}

// retryOnce runs the probe a second time and returns the second measurement
// with the first attempt attached. The second attempt's timings and verdict
// stand on their own; the first is evidence.
func retryOnce(ctx context.Context, in Input, coder *Coder, to StepTimeouts, first Measurement, delay time.Duration) Measurement {
	m := Run(ctx, in, coder, to)
	m.Retry = &RetryInfo{
		Attempts:        2,
		DelayMS:         delay.Milliseconds(),
		FirstStartedAt:  first.StartedAt,
		FirstOutcome:    first.Outcome,
		FirstError:      first.RawError,
		FirstDurationMS: first.TotalDurationMS,
	}
	if m.Outcome == first.Outcome {
		// Deliberately not "persisted": the retry goes back to the same
		// address from the same vantage, so a repeat is one observation
		// twice, not two agreeing observations. Only a second vantage could
		// corroborate, and there is not one.
		m.ClassificationReason += "; same vantage and address, retried after " + delay.String()
	} else {
		m.ClassificationReason += "; first attempt " + string(first.Outcome) + ", retried after " + delay.String()
	}
	return m
}

// sleepCtx waits d or until ctx is done; it reports whether the full wait
// completed.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
