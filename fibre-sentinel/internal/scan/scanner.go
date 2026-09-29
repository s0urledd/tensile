package scan

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/cosmos/cosmos-sdk/types/bech32"
	"math"
	"strings"
	"time"

	celfibre "github.com/celestiaorg/celestia-app/v10/fibre"
	fibretypes "github.com/celestiaorg/celestia-app/v10/x/fibre/types"
	cmttypes "github.com/cometbft/cometbft/types"
	assign "github.com/plsgiveup/fibre/fibre-assign"
	"github.com/plsgiveup/fibre/fibre-sentinel/internal/status"
)

// Config controls a scan run. Zero values fall back to the defaults in Run.
type Config struct {
	// RunConfig is what this run was configured with, recorded in
	// runs.jsonl on start (status.RunEvent). nil records the run alone.
	RunConfig map[string]any
	RPCURL    string
	DataDir   string

	// StartHeight is where a FRESH scan begins (ignored on resume). 0 or
	// negative means "latest height at startup".
	StartHeight int64
	// MaxHeight, if > 0, stops the scan after this height instead of the tip.
	MaxHeight int64

	// Follow keeps polling for new blocks after the tip is reached.
	Follow        bool
	FollowTimeout time.Duration // give up (fatal) if no new block within this; 0 = never, warn instead
	PollInterval  time.Duration // gap between tip polls

	RPCTimeout time.Duration // per-RPC-call timeout
	Deadline   time.Duration // whole-run wall-clock cap (0 = none)

	IncludeFailed bool // also record MsgPayForFibre txs that failed (code != 0)
	StoreRows     bool // include full per-validator row index lists in records

	CheckpointEvery int // Sync+SaveState every N processed heights (default 20)

	// SkipHeights are heights the operator told the scanner not to read
	// (-skip-heights): each is recorded as a scan gap with SkipReason and
	// never processed. The way out of a crash loop on one block; see skip.go.
	SkipHeights []HeightRange
}

// Scanner is the chain scanner: discovery + recording only, no probing.
type Scanner struct {
	// lastBlockTime is the block time of the newest header read, saved in
	// state.json as last_scanned_time.
	lastBlockTime time.Time
	// hosts is every Fibre host registration on record, from the chain's
	// set_fibre_provider_info events, seeded from the bonded registry at
	// the scan's start: host_at_settlement comes from here, never from a
	// state query at the settlement height.
	hosts  *HostHistory
	cfg    Config
	log    *Logger
	chain  *Chain
	store  *Store
	status *status.Writer
	gaps   []ScanGap

	params      *ParamHistory
	chainID     string
	startHeight int64 // resolved fresh-scan start (persisted across restarts)
	// lastReconcile is the height of the last params reconcile in this
	// process that actually read state; a silent change found at the next
	// one landed after it.
	lastReconcile int64
	// reconcileFailingSince is the first height of the current run of
	// failed reconciles, or zero when the last one read state. One
	// check_skipped record covers a whole run rather than one per check.
	reconcileFailingSince int64
	// unavailableRun counts heights declared unavailable back to back. The
	// first costs the full grace, because a node briefly behind and a node
	// that will never have the height look the same for the first minutes;
	// the ones after it do not, because by then the answer is known.
	unavailableRun int

	// fibreInactive is set when the x/fibre module does not answer queries
	// (the chain is on an app version before Fibre). The scanner keeps
	// following blocks so it is already in place at activation, retries
	// the params seed every inactiveRetryEvery heights, and seeds at once if
	// a MsgPayForFibre shows up.
	fibreInactive bool
	// activationSeen is set at the first block this process reads that ran
	// at FibreAppVersion or later; the seeds are tried there once rather
	// than at the next inactiveRetryEvery boundary.
	activationSeen bool

	// reseedFor is the end of the event-losing gap the bonded registry was
	// last re-read for in this process (or tried: reseedTriedAt is the
	// height of that try), so a gap costs one AllBondedFibreProviders query,
	// and one every inactiveRetryEvery heights while that query fails,
	// rather than one per block. reseedOneTried remembers the validators
	// already re-read on their own for that gap, so a failing query is not
	// repeated for every publication they are in.
	reseedFor      int64
	reseedTriedAt  int64
	reseedOneTried map[string]int64

	// valSets caches the validator set per promise height: the fibre-assign
	// view used for row assignment and the raw members (with consensus keys)
	// used to verify signatures, kept in one entry so the two can never
	// disagree about which heights are cached. valSetOrder is insertion
	// order, which is what eviction walks.
	valSets     map[int64]valSetEntry
	valSetOrder []int64
}

// New builds a Scanner. It opens the store and dials the RPC lazily in Run.
func New(cfg Config, log *Logger) (*Scanner, error) {
	if cfg.RPCTimeout <= 0 {
		cfg.RPCTimeout = 15 * time.Second
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 2 * time.Second
	}
	if cfg.CheckpointEvery <= 0 {
		cfg.CheckpointEvery = 20
	}
	ch, err := NewChain(cfg.RPCURL, cfg.RPCTimeout, log)
	if err != nil {
		return nil, err
	}
	st, err := OpenStore(cfg.DataDir)
	if err != nil {
		return nil, err
	}
	return &Scanner{
		status: status.New(cfg.DataDir, "scanner", "", status.BuildRevision()),
		cfg:    cfg,
		log:    log,
		chain:  ch,
		store:  st,
		// An empty history from the start. A resume replaces it with the
		// persisted one; a fresh scan seeds into it — and when the seed
		// cannot be read (x/valaddr not there yet, the node down) it stays
		// empty and is asked Seeded() for the first state.json. Left nil,
		// that first question was a nil dereference: every scanner started
		// on an empty data directory died in resume, on any chain, while
		// one with a state.json to load never noticed.
		hosts:          NewHostHistory(),
		valSets:        map[int64]valSetEntry{},
		reseedOneTried: map[string]int64{},
	}, nil
}

// Run executes the scan. It returns nil on a clean finish (tip or MaxHeight
// reached in non-follow mode). Any timeout or unrecoverable error calls
// log.Fatalf, which dumps the log ring and exits the process.
func (s *Scanner) Run(parent context.Context) error {
	ctx := parent
	if s.cfg.Deadline > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(parent, s.cfg.Deadline)
		defer cancel()
	}
	defer s.store.Close()
	s.status.RecordRuns(s.cfg.RunConfig)
	s.status.Start()
	defer s.status.Stop("exit")

	var chainID string
	var tip int64
	if err := s.retryRPC(ctx, "initial status", func() error {
		var err error
		chainID, tip, err = s.chain.Status(ctx)
		return err
	}); err != nil {
		s.log.Fatalf("initial status: %v", err)
	}
	s.chainID = chainID
	s.log.Printf("connected: chain_id=%s tip=%d rpc=%s", chainID, tip, s.cfg.RPCURL)

	next, err := s.resume(ctx, tip)
	if err != nil {
		s.log.Fatalf("resume: %v", err)
	}

	target := tip
	if s.cfg.MaxHeight > 0 && s.cfg.MaxHeight < target {
		target = s.cfg.MaxHeight
	}
	s.log.Printf("scanning from height %d to %d (follow=%v)", next, target, s.cfg.Follow)
	// Said at every start, so the journal shows the skips in force, and
	// which of them the scan has already passed (inert, safe to unset).
	for _, r := range s.cfg.SkipHeights {
		state := "ahead: will be recorded as a scan gap, not read"
		if r.To < next {
			state = "already behind the scan: inert, the flag can be removed"
		}
		s.log.Printf("WARNING: -skip-heights %s (%s)", r, state)
	}

	sinceCheckpoint := 0
	totalPubs := 0
	for {
		for h := next; h <= target; h++ {
			if err := ctx.Err(); err != nil {
				if errors.Is(err, context.Canceled) {
					return s.stopClean(next-1, "signal", totalPubs)
				}
				s.log.Fatalf("run deadline hit mid-scan at height %d: %v", h, err)
			}
			n := s.processBlock(ctx, h)
			totalPubs += n
			s.status.OK()
			if !s.fibreInactive && h%paramReconcileEvery == 0 {
				s.reconcileParams(ctx, h)
			}
			next = h + 1
			sinceCheckpoint++
			if sinceCheckpoint >= s.cfg.CheckpointEvery || h == target {
				s.checkpoint(next - 1)
				sinceCheckpoint = 0
			}
		}

		if !s.cfg.Follow {
			s.checkpoint(next - 1)
			s.log.Printf("done: scanned through height %d, %d publications recorded this run", next-1, totalPubs)
			return nil
		}

		// follow mode: wait (bounded) for a higher tip.
		newTarget, err := s.waitForHeight(ctx, target+1)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return s.stopClean(next-1, "signal", totalPubs)
			}
			s.log.Fatalf("follow wait: %v", err)
		}
		target = newTarget
		if s.cfg.MaxHeight > 0 && s.cfg.MaxHeight < target {
			target = s.cfg.MaxHeight
			if next > target {
				s.checkpoint(next - 1)
				s.log.Printf("done: reached MaxHeight %d in follow mode", s.cfg.MaxHeight)
				return nil
			}
		}
	}
}

// resume decides the first height to scan and restores param history.
func (s *Scanner) resume(ctx context.Context, tip int64) (int64, error) {
	st, err := s.store.LoadState()
	if err != nil {
		return 0, err
	}
	if st != nil {
		if st.ChainID != "" && st.ChainID != s.chainID {
			return 0, fmt.Errorf("data dir belongs to chain %q but RPC is chain %q", st.ChainID, s.chainID)
		}
		s.params = LoadParamHistory(st.ParamHistory)
		s.fibreInactive = len(st.ParamHistory) == 0
		s.startHeight = st.StartHeight
		s.gaps = DedupeGaps(st.Gaps)
		s.hosts = LoadHostHistory(st.HostHistory, st.HostSeeded, st.HostSeedAt)
		if !st.HostSeeded {
			// a data dir from before the host history: seed now, at the
			// resume height, and say so
			s.seedHosts(ctx, st.LastScannedHeight+1)
		}
		s.lastReconcile = st.LastReconcileHeight
		s.reconcileFailingSince = st.ReconcileFailingSince
		resumeAt := st.LastScannedHeight + 1
		s.store.SetSettledCoverFrom(resumeAt)
		s.log.Printf("resuming: last_scanned=%d, %d param-history entries%s", st.LastScannedHeight, len(st.ParamHistory),
			map[bool]string{true: " (x/fibre not active yet)", false: ""}[s.fibreInactive])
		return resumeAt, nil
	}

	// fresh scan.
	start := s.cfg.StartHeight
	if start <= 0 {
		start = tip
	}
	if start < 1 {
		start = 1
	}
	seed, err := s.seedParamsFor(ctx, start)
	switch {
	case err == nil:
		s.params = NewParamHistory(start, seed)
		s.log.Printf("fresh scan: start=%d seed params: promise_timeout=%s shard_retention=%s withdrawal_delay=%s",
			start, seed.PaymentPromiseTimeout, seed.ShardRetention, seed.WithdrawalDelay)
	case IsModuleInactive(err):
		s.params = LoadParamHistory(nil)
		s.fibreInactive = true
		s.log.Printf("fresh scan: start=%d, x/fibre is not active on this chain yet (%v); following blocks without params and retrying every %d heights",
			start, err, inactiveRetryEvery)
	default:
		return 0, fmt.Errorf("seed params at height %d: %w", start, err)
	}
	s.startHeight = start
	s.store.SetSettledCoverFrom(start)
	s.seedHosts(ctx, start)

	// persist the seed immediately so a crash before the first block still
	// resumes with the right history.
	seeded, seedAt := s.hosts.Seeded()
	if err := s.store.SaveState(PersistState{
		ChainID:               s.chainID,
		StartHeight:           start,
		LastScannedHeight:     start - 1,
		ParamFingerprint:      assign.ParamsV10BlobV0.Fingerprint(),
		ParamHistory:          s.params.Entries(),
		LastReconcileHeight:   s.lastReconcile,
		ReconcileFailingSince: s.reconcileFailingSince,
		HostHistory:           s.hosts.Entries(),
		HostSeeded:            seeded,
		HostSeedAt:            seedAt,
	}); err != nil {
		return 0, err
	}
	return start, nil
}

// stopClean persists progress and returns nil — used when the operator stops
// the scanner (SIGINT/SIGTERM). Resuming later picks up from here.
func (s *Scanner) stopClean(lastScanned int64, reason string, totalPubs int) error {
	s.checkpoint(lastScanned)
	s.log.Printf("stopped (%s): scanned through height %d, %d publications recorded this run", reason, lastScanned, totalPubs)
	s.status.Stop(reason)
	return nil
}

func (s *Scanner) checkpoint(lastScanned int64) {
	if err := s.store.Sync(); err != nil {
		s.log.Fatalf("sync publications: %v", err)
	}
	seeded, seedAt := s.hosts.Seeded()
	if err := s.store.SaveState(PersistState{
		ChainID:               s.chainID,
		StartHeight:           s.startHeight,
		LastScannedHeight:     lastScanned,
		LastScannedTime:       s.lastBlockTime,
		ParamFingerprint:      assign.ParamsV10BlobV0.Fingerprint(),
		ParamHistory:          s.params.Entries(),
		LastReconcileHeight:   s.lastReconcile,
		ReconcileFailingSince: s.reconcileFailingSince,
		Gaps:                  s.gaps,
		HostHistory:           s.hosts.Entries(),
		HostSeeded:            seeded,
		HostSeedAt:            seedAt,
	}); err != nil {
		s.log.Fatalf("save state: %v", err)
	}
	s.status.Progress(lastScanned)
}

// recordGap notes a height the node could not serve and lets the scan move
// on. A MsgPayForFibre in that block is lost to this observer, and the gap is
// published rather than hidden: state.json carries the ranges, the API and
// the dashboard show them. Consecutive heights merge into one range. Returns
// false when err is not that kind of failure.
func (s *Scanner) recordGap(h int64, err error, blockTime time.Time) bool {
	return s.recordGapRead(h, err, blockTime, false)
}

// recordPublicationGap is recordGap for a height whose block and
// block_results were read and processed — params, host registrations,
// escrow — and only a publication in it could not be built (its validator
// set at the promise height is pruned). The gap is published like any
// other, but it is marked as having lost no events: a host registration in
// that block is on record, so host_at_settlement must not turn unknown
// because of it.
func (s *Scanner) recordPublicationGap(h int64, err error, blockTime time.Time) bool {
	return s.recordGapRead(h, err, blockTime, true)
}

func (s *Scanner) recordGapRead(h int64, err error, blockTime time.Time, eventsRead bool) bool {
	var ue *ErrHeightUnavailable
	if !errors.As(err, &ue) {
		return false
	}
	s.noteGap(h, "height unavailable from the RPC node (pruned, or storage.discard_abci_responses = true)", ue.Err.Error(), blockTime, eventsRead)
	s.log.Printf("WARNING: GAP h=%d not scanned: %v; recorded and moving on (%d gap ranges so far)", h, ue.Err, len(s.gaps))
	s.status.Error(fmt.Sprintf("gap at h=%d: %v", h, ue.Err))
	return true
}

// addGap puts h on the gap list. It extends the newest range when h follows
// it directly and the reason is the same; a different reason starts a new
// range, so a height the operator skipped never hides inside a run the node
// could not serve, or the other way round.
func (s *Scanner) addGap(h int64, reason, lastErr string, blockTime time.Time) {
	s.noteGap(h, reason, lastErr, blockTime, false)
}

// noteGap is addGap with the HostEventsRead mark. A range only ever holds
// heights of one kind: a gap that lost events never absorbs one that did
// not, or host attribution would ignore a height whose registrations were
// never read (or distrust one whose registrations were).
func (s *Scanner) noteGap(h int64, reason, lastErr string, blockTime time.Time, eventsRead bool) {
	var bt *time.Time
	if !blockTime.IsZero() {
		t := blockTime.UTC()
		bt = &t
	}
	// A height already on record as this kind of gap is not added again:
	// two publications in one block whose validator sets are both pruned
	// each land here, and the second used to open a second range h-h.
	for i := range s.gaps {
		if g := &s.gaps[i]; h >= g.From && h <= g.To && g.Reason == reason && g.HostEventsRead == eventsRead {
			g.LastError = lastErr
			return
		}
	}
	if n := len(s.gaps); n > 0 && s.gaps[n-1].To == h-1 && s.gaps[n-1].Reason == reason && s.gaps[n-1].HostEventsRead == eventsRead {
		s.gaps[n-1].To = h
		s.gaps[n-1].LastError = lastErr
		if bt != nil {
			s.gaps[n-1].ToTime = bt
		}
	} else {
		s.gaps = append(s.gaps, ScanGap{From: h, To: h, Reason: reason, LastError: lastErr, At: time.Now().UTC(), FromTime: bt, ToTime: bt, HostEventsRead: eventsRead})
	}
}

// waitForHeight polls Status until the tip reaches want, or FollowTimeout
// elapses (fatal). With FollowTimeout 0 it waits forever and logs a warning
// every five minutes: a halted chain (upgrade, outage) is the node's problem,
// and the observer should be there when blocks resume. Returns the observed
// tip (>= want). Transient RPC errors are retried, not fatal.
func (s *Scanner) waitForHeight(ctx context.Context, want int64) (int64, error) {
	start := time.Now()
	var deadline time.Time
	if s.cfg.FollowTimeout > 0 {
		deadline = start.Add(s.cfg.FollowTimeout)
	}
	nextWarn := start.Add(5 * time.Minute)
	for {
		var tip int64
		err := s.retryRPC(ctx, "status while following", func() error {
			var err error
			_, tip, err = s.chain.Status(ctx)
			return err
		})
		if err != nil {
			return 0, fmt.Errorf("status while following: %w", err)
		}
		if tip >= want {
			return tip, nil
		}
		s.status.Set("chain_tip", tip)
		// Caught up, the tip is the block just scanned and its time is known:
		// the dashboard's block ticker reads both from this file every few
		// seconds, rather than from state.json, which moves every 20 blocks.
		if tip == want-1 && !s.lastBlockTime.IsZero() {
			s.status.Set("tip_block_time", s.lastBlockTime.UTC())
		}
		if !deadline.IsZero() && time.Now().After(deadline) {
			return 0, fmt.Errorf("no new block: tip stuck at %d, waited %s for height %d", tip, s.cfg.FollowTimeout, want)
		}
		if time.Now().After(nextWarn) {
			s.log.Printf("WARNING: no new block for %s (tip %d, waiting for %d); still following", time.Since(start).Round(time.Second), tip, want)
			nextWarn = time.Now().Add(5 * time.Minute)
		}
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(s.cfg.PollInterval):
		}
	}
}

// rpcBackoff is the wait before the next attempt at a transient RPC failure:
// 1, 2, 4, 8, 16, 30, 30, ... seconds. Public endpoints hiccup, and at the
// tip CometBFT stores the block before the FinalizeBlock response, so
// block_results for a height /status just reported can be "not found" for a
// moment.
func rpcBackoff(attempt int) time.Duration {
	if attempt >= 5 {
		return 30 * time.Second
	}
	return time.Duration(1<<uint(attempt)) * time.Second
}

// unavailableGrace is how long a height that the node says it does not have
// is retried before the scanner records a gap and moves on. A node that is
// still catching up, or restarting, answers "not available" for a while and
// then has the height; a node that pruned it, or runs with
// storage.discard_abci_responses = true, never will.
var unavailableGrace = 10 * time.Minute

// unavailableRunGrace is the grace applied to a height immediately after one
// already declared unavailable. The full grace exists to tell a node that is
// briefly behind from one that will never have the height; once a run has
// been established, the second answer is known, and paying ten minutes per
// height meant a thousand-block hole — the span the chain allows between a
// promise and its settlement — took a week to cross, one height at a time,
// with the feed stopped throughout.
const unavailableRunGrace = 20 * time.Second

// rpcWarnEvery is how often a still-failing retry is logged as a WARNING,
// so a long outage leaves a trail without a line every few seconds.
const rpcWarnEvery = 5 * time.Minute

// ErrHeightUnavailable wraps an RPC error that means the node cannot serve
// this height at all: pruned, or ABCI responses discarded. It is returned
// only after unavailableGrace of retries.
type ErrHeightUnavailable struct {
	Height int64
	Err    error
}

func (e *ErrHeightUnavailable) Error() string {
	return fmt.Sprintf("height %d unavailable from this node after %s: %v", e.Height, unavailableGrace, e.Err)
}

func (e *ErrHeightUnavailable) Unwrap() error { return e.Err }

// retryRPC runs fn until it succeeds. A transient failure (the node is down,
// a timeout, a tip race) is retried for as long as it takes, with capped
// backoff and a WARNING every few minutes: an RPC outage is the node's
// problem, and the scanner should be there when it comes back rather than
// exit and be restarted in a loop by the supervisor. It gives up at once on
// a context cancellation or on x/fibre being inactive, and after
// unavailableGrace on a height the node says it does not have.
func (s *Scanner) retryRPC(ctx context.Context, what string, fn func() error) error {
	return s.retryRPCAt(ctx, what, 0, fn)
}

func (s *Scanner) retryRPCAt(ctx context.Context, what string, height int64, fn func() error) error {
	start := time.Now()
	nextWarn := start.Add(rpcWarnEvery)
	for attempt := 0; ; attempt++ {
		err := fn()
		if err == nil {
			if attempt > 0 {
				s.log.Printf("%s: recovered after %d attempts, %s", what, attempt+1, time.Since(start).Round(time.Second))
			}
			// The run of unavailable heights, if there was one, is over: the
			// next hole is judged on the full grace again.
			s.unavailableRun = 0
			return nil
		}
		if ctx.Err() != nil {
			return err
		}
		if IsModuleInactive(err) {
			return err
		}
		// A recovered panic the node repeats is not an outage. Proven to be
		// the module not existing yet, it returned above; otherwise it is
		// given a few tries and handed back, so a caller that can skip (a
		// reconcile, a registry seed) does, instead of holding the scan on
		// an answer no wait changes.
		var ae *ABCIError
		panicked := errors.As(err, &ae) && ae.Code == abciPanicCode
		if panicked && attempt+1 >= appPanicTries {
			return err
		}
		unavailable := IsHeightUnavailable(err)
		if unavailable {
			grace := unavailableGrace
			if s.unavailableRun > 0 {
				grace = unavailableRunGrace
			}
			if time.Since(start) >= grace {
				s.unavailableRun++
				return &ErrHeightUnavailable{Height: height, Err: err}
			}
		}
		wait := rpcBackoff(attempt)
		if unavailable {
			wait = 30 * time.Second
		}
		if panicked {
			wait = appPanicWait
		}
		if attempt < 3 || time.Now().After(nextWarn) {
			level := ""
			if attempt >= 3 {
				level = "WARNING: "
				nextWarn = time.Now().Add(rpcWarnEvery)
			}
			s.log.Printf("%s%s: %v (attempt %d, failing for %s, retry in %s)", level, what, err, attempt+1, time.Since(start).Round(time.Second), wait)
			s.status.Error(fmt.Sprintf("%s: %v", what, err))
		}
		select {
		case <-ctx.Done():
			return err
		case <-time.After(wait):
		}
	}
}

// processBlock scans one height: first apply any fibre-param updates, then
// record every single-message MsgPayForFibre tx. Returns publications recorded.
// inactiveRetryEvery is how often (in heights) the scanner re-asks for
// x/fibre params while the module is inactive.
const inactiveRetryEvery = 100

// appPanicTries is how many times a query answered with a recovered panic,
// at a height where the module exists, is asked before the error is returned,
// appPanicWait apart: the answer is the app's, not load, so backing off buys
// nothing.
const appPanicTries = 5

var appPanicWait = 2 * time.Second

// IsModuleInactive reports whether an ABCI query error means the queried
// module does not exist on the chain (app version before Fibre), or did not
// exist yet at the height asked about. The second is what a node already on
// the Fibre binary says about a height from before the upgrade: a recovered
// panic, not "unknown query path", and without this every such query was
// retried for as long as the process lived.
func IsModuleInactive(err error) bool {
	if err == nil {
		return false
	}
	var ae *ABCIError
	if errors.As(err, &ae) && ae.BeforeModule {
		return true
	}
	if errors.As(err, &ae) && ae.Code == 6 && (ae.Codespace == "sdk" || ae.Codespace == "") {
		return true // cosmos-sdk ErrUnknownRequest
	}
	msg := err.Error()
	return strings.Contains(msg, "unknown query path") || strings.Contains(msg, "unknown request")
}

// paramReconcileEvery is how often, in blocks, the scanner re-reads x/fibre
// params from state and compares them with the history it built from events.
// A governance proposal announces its change with an event; an upgrade
// handler or a store migration that calls SetParams emits nothing, and
// without this check such a change would stay invisible for the life of the
// data dir, with every later window computed from the old retention. The
// interval is also the most a silent change can go unnoticed: a promise
// settled between the change and the next check carries a window computed
// from the old params (see reconcileParams), so the check is cheap and
// frequent: one Params query per sixty blocks.
const paramReconcileEvery = 60

// reconcileParams compares the params in state after block h with the
// history's view of block h and, on a difference, records the live params as
// in force from the next block, which is the earliest point the scanner can
// vouch for and the only placement that keeps the earliest-bound rule
// conservative for promises evaluated from here on (an earlier placement
// would let a lengthened window stand alone as the only candidate for a
// promise the server may have validated against the old, shorter one).
//
// The change really landed somewhere in (last check, h], where "last
// check" is the last one that actually read state: a reconcile whose RPC
// failed leaves the marker alone, so an outage widens the interval
// instead of hiding part of it. Publications
// settled in that interval were recorded with the old params and are not
// rewritten: when the window got shorter, the observer's must_serve_until
// for them is later than the server's prune time, and a NOT_FOUND between
// the two would be published as a fault. The log names the interval and
// counts those publications. Such a reading is judged as the client meets
// it: every validator has pruned, so the blob is Unavailable and every
// endorser not served. What keeps that off the record is the uncertainty
// range recorded here, which holds every publication in it
// (RETENTION_UNVERIFIED) until the params of every height are read; a
// re-scan from the interval's start rewrites nothing (the record is
// append-only), so the log line and the range are the record.
func (s *Scanner) reconcileParams(ctx context.Context, h int64) {
	s.reconcileParamsWith(h, func() (fibretypes.Params, error) {
		var live fibretypes.Params
		err := s.retryRPC(ctx, fmt.Sprintf("params reconcile at height %d", h), func() error {
			var err error
			live, err = s.chain.FibreParamsAt(ctx, h)
			return err
		})
		return live, err
	}, func(at int64) (fibretypes.Params, error) {
		// One try per height, no retry loop: the read is bounded work
		// inside the block loop and a range that cannot be read now is
		// recorded unresolvable rather than stalling the scan. A later
		// pass can close it.
		return s.chain.FibreParamsAt(ctx, at)
	})
}

// reconcileParamsWith is reconcileParams with the two state reads injected,
// so a test can fail either without a chain.
func (s *Scanner) reconcileParamsWith(h int64, readState func() (fibretypes.Params, error), readAt func(int64) (fibretypes.Params, error)) {
	since := s.lastReconcile
	unknownSince := since == 0
	if unknownSince {
		// No reconcile on record: this observer has never checked, so the
		// interval a silent change could have landed in is the whole scan.
		// Assuming one reconcile period understated it, and this log line is
		// the only record of which publications carry the old deadline.
		since = s.startHeight - 1
	}
	live, err := readState()
	if err != nil {
		// The marker stays where it was. It is the start of the interval a
		// silent change could have landed in, and a check that did not
		// happen narrows nothing: moving it here would drop
		// (previous check, h] out of the interval the next successful
		// check reports, and that interval is the only record of which
		// publications carry a deadline computed from the old params.
		// Under a long RPC outage the interval widens, which is the truth.
		s.log.Printf("h=%d: params reconcile skipped: %v (the uncertainty interval still starts at height %d)", h, err, since+1)
		// One record per run of failures, not one per check. The run's
		// start is persisted, so a restart mid-outage does not write a
		// second record for a stretch already on the record. It holds
		// nothing: a check that could not happen is not evidence that
		// anything changed, only that the observer could not look.
		if s.reconcileFailingSince == 0 {
			if s.emitUncertainty(ParamUncertainty{
				Kind:               UncertaintyCheckSkipped,
				FromHeight:         since + 1,
				ToHeight:           h,
				IntervalStartKnown: !unknownSince,
				LastError:          err.Error(),
			}) == nil {
				// Latched only once the line is on disk, so a failed write
				// is retried at the next check rather than swallowed.
				s.reconcileFailingSince = since + 1
			}
		}
		return
	}
	s.reconcileFailingSince = 0
	cur := s.params.at(h, math.MaxInt)
	if cur != nil && paramsEqual(cur.Params, live) {
		s.lastReconcile = h
		return
	}
	{
		direction, detail := "unchanged", "unchanged"
		var before ParamsSnapshot
		var beforeS int64
		afterW, _ := windowFrom(live, time.Time{})
		if cur != nil {
			before, beforeS = cur.ParamsJSON, windowSeconds(cur.Params)
			oldW, _ := windowFrom(cur.Params, time.Time{})
			switch {
			case afterW.Before(oldW):
				direction = "shorter"
				detail = "SHORTER: their recorded must_serve_until may be later than the server's prune time, and an in-window NOT_FOUND between the two would be a false fault"
			case afterW.After(oldW):
				direction = "longer"
				detail = "longer: their recorded must_serve_until is earlier than the server's prune time, which can only produce SERVED_PAST_WINDOW, never a fault"
			}
		}
		n := int64(s.store.CountSettledBetween(since+1, h))
		// The count covers only publications this process appended, so it
		// is a floor whenever the range starts before this process's first
		// height — after any restart, not only when no earlier reconcile is
		// on record.
		isFloor := since+1 < s.store.SettledCoverFrom()
		counted := fmt.Sprintf("%d publication(s) settled in that interval were recorded with the old params", n)
		if isFloor {
			counted = fmt.Sprintf("at least %d publication(s) settled in that interval were recorded with the old params "+
				"(the count covers only this process's appends)", n)
		}
		u := ParamUncertainty{
			Kind:                 UncertaintySilentChange,
			FromHeight:           since + 1,
			ToHeight:             h,
			EffectiveFromHeight:  h + 1,
			IntervalStartKnown:   !unknownSince,
			Before:               before,
			After:                snapshotParams(live, h+1, -1, "reconcile"),
			Direction:            direction,
			WindowBeforeS:        beforeS,
			WindowAfterS:         windowSeconds(live),
			PublicationsAffected: n,
			IsFloor:              isFloor,
		}
		s.resolveUncertainty(&u, readAt)
		// The record is the only thing that makes this range knowable to
		// anything downstream, and nothing here may move past it until it
		// is on disk. So neither the param history nor the reconcile
		// marker is touched before the append succeeds: leaving both where
		// they are means the next check sees the same disagreement and
		// tries again, over a range that has only grown. Advancing either
		// one first would lose the range for good — the next check would
		// find state and history in agreement and have nothing to report,
		// while the publications inside it kept a deadline nobody knows to
		// distrust.
		if err := s.emitUncertainty(u); err != nil {
			s.log.Printf("WARNING: h=%d: x/fibre params in state differ from the event history over heights %d-%d, and the record of it could not be written (%v); "+
				"neither the params history nor the reconcile marker is advanced, so the next check re-detects it over a wider range",
				h, since+1, h, err)
			return
		}
		s.params.add(h+1, -1, "reconcile", live)
		// The proven values go in at the height each was really in force
		// from, which is earlier than the h+1 a single read can vouch for.
		for _, v := range u.Values {
			s.params.add(v.FromHeight, -1, "verified", v.Params.toParams())
		}
		s.lastReconcile = h
		s.log.Printf("WARNING: h=%d: x/fibre params in state differ from the event history (promise_timeout=%s shard_retention=%s withdrawal_delay=%s in state); "+
			"a change landed without an event somewhere in heights %d-%d, recorded as in force from height %d; %s; %s; the retention window is %s",
			h, live.PaymentPromiseTimeout, live.ShardRetention, live.WithdrawalDelay, since+1, h, h+1, counted, u.resolutionNote(), detail)
	}
}

// maxVerifyHeights bounds the reads one detection may spend closing a
// range. A healthy range is paramReconcileEvery heights, so the bound only
// bites after an RPC outage or on a first reconcile with no marker on
// record, where the range can be the whole scan. Beyond it the range is
// recorded unresolvable and its verdicts stay held, which is the honest
// answer: the observer did not read those heights.
const maxVerifyHeights = 5000

// resolveUncertainty tries to close a range the only way it can be closed:
// by reading x/fibre params at every height in it. A bisection would locate
// a transition, but it cannot prove there was no third value in between,
// and a third value is the whole reason the two endpoints are not enough —
// a window that dipped shorter between them would leave exactly the false
// fault this record exists to prevent. So it is every height or none.
//
// It runs inline, at detection, because sixty heights is about a second of
// reads against a node that certainly still has state that recent: the node
// the scanner is already following. That keeps the common case closed
// within one reconcile instead of waiting on a separate process, and it is
// why nothing downstream has to hold a verdict for long.
func (s *Scanner) resolveUncertainty(u *ParamUncertainty, readAt func(int64) (fibretypes.Params, error)) {
	if readAt == nil {
		return
	}
	from, to := u.FromHeight-1, u.ToHeight
	if from < 1 {
		from = 1
	}
	span := to - from + 1
	now := time.Now().UTC()
	if span > maxVerifyHeights {
		u.Resolution = ResolutionUnresolvable
		u.ResolvedAt = &now
		u.ResolveMethod = "exhaustive_read"
		u.ResolveError = fmt.Sprintf("the range is %d heights, over the %d this scanner will read in one pass; nothing was read, so nothing is proven", span, maxVerifyHeights)
		return
	}
	var values []ResolvedValue
	skipped := int64(0)
	for at := from; at <= to; at++ {
		p, err := readAt(at)
		if err != nil && at == from && at < to && IsModuleInactive(err) {
			// The range starts at the params seed, the module's first
			// block, and the height before it has no x/fibre to read. The
			// seed itself was read after that block, so the reads from
			// FromHeight on are the whole answer.
			skipped++
			continue
		}
		if err != nil {
			u.Resolution = ResolutionUnresolvable
			u.ResolvedAt = &now
			u.ResolveMethod = "exhaustive_read"
			u.HeightsRead = at - from
			u.ResolveError = fmt.Sprintf("params at height %d: %v", at, err)
			u.Values = nil // a partial read proves nothing about the heights it skipped
			return
		}
		if n := len(values); n > 0 && paramsEqual(values[n-1].Params.toParams(), p) {
			continue
		}
		values = append(values, ResolvedValue{FromHeight: at, Params: snapshotParams(p, at, -1, "verified")})
	}
	u.Resolution = ResolutionVerified
	u.ResolvedAt = &now
	u.ResolveMethod = "exhaustive_read"
	u.HeightsRead = span - skipped
	u.Values = values
	// The proven values are NOT put into the history here. The caller does
	// that, and only once the record is on disk: a history that has moved
	// past a range nothing recorded is a range that can never be found
	// again.
}

func (u ParamUncertainty) resolutionNote() string {
	switch u.Resolution {
	case ResolutionVerified:
		return fmt.Sprintf("params were read at all %d heights in the range (%d distinct value(s)), so the deadlines it covers can be corrected; "+
			"its obligations stay held until the collector has applied those corrections",
			u.HeightsRead, len(u.Values))
	case ResolutionUnresolvable:
		return "the range could not be closed (" + u.ResolveError + "), so the obligations it covers are held out of every rate until it is"
	}
	return "the range is open, so the obligations it covers are held out of every rate"
}

// emitUncertainty stamps a record and appends it, returning whatever went
// wrong. The caller is expected to hold its own progress back on an error:
// this record is the only thing that makes the range knowable downstream.
func (s *Scanner) emitUncertainty(u ParamUncertainty) error {
	u.SchemaVersion = ParamUncertaintySchemaVersion
	u.ChainID = s.chainID
	u.ID = u.Key(s.chainID)
	u.DetectedAt = time.Now().UTC()
	if !s.lastBlockTime.IsZero() {
		t := s.lastBlockTime.UTC()
		u.ToTime = &t
	}
	if s.store == nil {
		return nil // reconcile_test.go builds a Scanner with no store
	}
	return s.store.AppendParamUncertainty(u)
}

func windowSeconds(p fibretypes.Params) int64 {
	w := p.PaymentPromiseTimeout
	if p.ShardRetention > w {
		w = p.ShardRetention
	}
	return int64(w / time.Second)
}

// seedParamsFor returns the params in force at the first tx of block h, which
// is the state after block h-1. The history keys a seed as (h, -1), "from the
// first tx of block h"; querying at h itself answers with the state after
// block h, which is wrong for a publication that shares block h with a param
// change landing later in the same block. At h = 1, and when x/fibre only
// became active in block h, h-1 answers "inactive" and the state after block
// h is the best there is. Transient RPC errors are retried; "inactive" is
// returned at once.
func (s *Scanner) seedParamsFor(ctx context.Context, h int64) (fibretypes.Params, error) {
	var seed fibretypes.Params
	fetch := func(at int64) error {
		return s.retryRPC(ctx, fmt.Sprintf("params at height %d", at), func() error {
			var err error
			seed, err = s.chain.FibreParamsAt(ctx, at)
			return err
		})
	}
	if h > 1 {
		err := fetch(h - 1)
		if err == nil || !IsModuleInactive(err) {
			return seed, err
		}
	}
	return seed, fetch(h)
}

// trySeed asks for params at h and, on success, starts the history there.
func (s *Scanner) trySeed(ctx context.Context, h int64) bool {
	seed, err := s.seedParamsFor(ctx, h)
	if err != nil {
		if !IsModuleInactive(err) {
			s.log.Printf("params at h=%d: %v (still treating x/fibre as inactive)", h, err)
		}
		return false
	}
	s.params = NewParamHistory(h, seed)
	s.fibreInactive = false
	// The seed is a read of state, so the first reconcile's interval starts
	// here. Left at zero it started at the scan's start height, which on a
	// scanner that lived through activation is the whole pre-Fibre stretch:
	// a range too long to verify, whose heights have no params to read.
	s.lastReconcile = h - 1
	s.log.Printf("x/fibre ACTIVE at h=%d: promise_timeout=%s shard_retention=%s withdrawal_delay=%s",
		h, seed.PaymentPromiseTimeout, seed.ShardRetention, seed.WithdrawalDelay)
	return true
}

func (s *Scanner) processBlock(ctx context.Context, h int64) int {
	// An operator skip comes before anything else that touches the block:
	// the height is listed because reading it kills the process, so none of
	// it is read — not the params, not the registrations, not the escrow
	// movements. It is a scan gap, recorded and published like any other.
	if s.skipListed(h) {
		s.skipHeight(ctx, h)
		return 0
	}
	if s.fibreInactive && (h%inactiveRetryEvery == 0 || h == s.startHeight) {
		s.trySeed(ctx, h)
	}
	// The host registry lives in x/valaddr, which does not exist before
	// app version 10. A scanner started on a pre-activation chain therefore
	// could not seed, and seeding ran only once per process from resume() —
	// so a scanner that lived through activation had no host history and no
	// back-fill path for the rest of its life, because lazySeed returns
	// while unseeded. Only a restart fixed it, and nothing said so. Retried
	// on the same cadence as the params seed until it takes.
	if seeded, _ := s.hosts.Seeded(); !seeded && h%inactiveRetryEvery == 0 {
		s.seedHosts(ctx, h)
	}
	// A gap that lost events is behind the scan: read the registry again so
	// host_at_settlement recovers from here on (see reseedHosts).
	s.maybeReseedHosts(ctx, h)
	var blk *Block
	var res *BlockResults
	if err := s.retryRPCAt(ctx, fmt.Sprintf("fetch block %d", h), h, func() error {
		var err error
		blk, err = s.chain.Block(ctx, h)
		return err
	}); err != nil {
		if s.recordGap(h, err, time.Time{}) {
			return 0
		}
		s.log.Fatalf("fetch block %d: %v", h, err)
	}
	// The frontier on the chain's clock, persisted with the next checkpoint:
	// the deferred shadow verdict is drawn against it.
	s.lastBlockTime = blk.Time.UTC()
	// The first block run at FibreAppVersion is the first whose state holds
	// x/fibre and x/valaddr: seed both there, once, instead of waiting for
	// the next inactiveRetryEvery boundary with activation already past.
	if !s.activationSeen && blk.AppVersion >= FibreAppVersion {
		s.activationSeen = true
		if s.fibreInactive {
			s.trySeed(ctx, h)
		}
		if seeded, _ := s.hosts.Seeded(); !seeded {
			s.seedHosts(ctx, h)
		}
	}
	if err := s.retryRPCAt(ctx, fmt.Sprintf("fetch block_results %d", h), h, func() error {
		var err error
		res, err = s.chain.BlockResults(ctx, h)
		return err
	}); err != nil {
		// The header was read a moment ago: the gap gets the chain's clock.
		if s.recordGap(h, err, blk.Time) {
			return 0
		}
		s.log.Fatalf("fetch block_results %d: %v", h, err)
	}
	if len(res.TxCodes) != len(blk.Txs) {
		s.log.Fatalf("block %d: %d txs but %d results%s", h, len(blk.Txs), len(res.TxCodes), skipHint(h))
	}

	// 1) param updates first, so must_serve_until for a PayForFibre later in
	//    the same block sees the new value. Only a successful tx changes
	//    anything: the keeper emits nothing on failure, so an
	//    EventUpdateFibreParams on a failed tx is not a params change — and
	//    acting on one would move must_serve_until for every publication
	//    after it, which is the deadline this observer judges against. The
	//    host-registration loop below has always applied this rule; this one
	//    did not.
	for i, evs := range res.TxEvents {
		if res.TxCodes[i] != 0 {
			continue
		}
		for _, ev := range evs {
			p, isUpdate, perr := parseUpdateFibreParams(ev)
			if !isUpdate {
				continue
			}
			if perr != nil {
				s.log.Fatalf("block %d tx %d: %v%s", h, i, perr, skipHint(h))
			}
			if s.params.AddTxEvent(h, i, p) {
				s.log.Printf("param update @ h=%d tx=%d: promise_timeout=%s shard_retention=%s withdrawal_delay=%s",
					h, i, p.PaymentPromiseTimeout, p.ShardRetention, p.WithdrawalDelay)
			}
		}
	}
	for _, ev := range res.FinalizeEvts {
		p, isUpdate, perr := parseUpdateFibreParams(ev)
		if !isUpdate {
			continue
		}
		if perr != nil {
			s.log.Fatalf("block %d finalize events: %v%s", h, perr, skipHint(h))
		}
		if s.params.AddFinalizeEvent(h, p) {
			s.log.Printf("param update @ h=%d (finalize, effective h=%d): promise_timeout=%s shard_retention=%s",
				h, h+1, p.PaymentPromiseTimeout, p.ShardRetention)
		}
	}

	// 1b) Fibre host registrations, from the same results: a validator's
	//     host at settlement is the newest registration at or before the
	//     settlement tx. Only a successful tx registers; the keeper emits
	//     nothing on failure, and a failed tx's events are not trusted.
	for i, evs := range res.TxEvents {
		if res.TxCodes[i] != 0 {
			continue
		}
		for _, ev := range evs {
			addr, host, isReg, perr := parseSetFibreProviderInfo(ev)
			if !isReg {
				continue
			}
			if perr != nil {
				s.log.Fatalf("block %d tx %d: %v%s", h, i, perr, skipHint(h))
			}
			if e, added := s.hosts.AddTxEvent(h, i, addr, host); added {
				s.log.Printf("host registration @ h=%d tx=%d: %s -> %s", h, i, addr, host)
				if err := s.store.AppendHostEvent(HostEvent{HostEntry: e, Time: blk.Time.UTC()}); err != nil {
					s.log.Fatalf("host_history: %v", err)
				}
			}
		}
	}

	// 2) MsgPayForFibre txs.
	recorded := 0
	for i, raw := range blk.Txs {
		_, isFibre, perr := fibretypes.TryParseFibreTx(raw)
		if !isFibre {
			continue
		}
		txHash := hexstr(cmttypes.Tx(raw).Hash())
		code := res.TxCodes[i]
		if perr != nil {
			s.log.Printf("h=%d tx=%d (%s): malformed fibre tx, skipped: %v", h, i, txHash[:12], perr)
			continue
		}
		if code != 0 && !s.cfg.IncludeFailed {
			s.log.Printf("h=%d tx=%d (%s): MsgPayForFibre failed code=%d, skipped (use -include-failed to record)", h, i, txHash[:12], code)
			continue
		}
		if s.store.Seen(txHash) {
			continue
		}
		if s.fibreInactive && !s.trySeed(ctx, h) {
			s.log.Fatalf("h=%d tx=%d: MsgPayForFibre seen but x/fibre params cannot be read%s", h, i, skipHint(h))
		}
		msg, derr := decodePayForFibre(raw)
		if derr != nil || msg == nil {
			s.log.Printf("h=%d tx=%d (%s): could not decode MsgPayForFibre: %v", h, i, txHash[:12], derr)
			continue
		}
		pub, berr := s.buildPublication(ctx, msg, blk, i, txHash, code)
		if berr != nil {
			// A height this node cannot serve is a gap, not a crash. The
			// validator set is fetched at the PROMISE height, which the
			// chain allows to be up to PaymentPromiseHeightWindow blocks
			// below the settlement height, so a state-synced node or one
			// whose retention starts inside that span cannot build these
			// records at all. Exiting on it produced a restart loop no
			// supervisor could break — resume() comes back to the same
			// height and hits the same publication — and recorded nothing,
			// so the feed stopped with the site showing an unbroken window.
			// Recorded as a gap at the settlement height instead: the
			// obligations in it are unobserved and say so, and the scan
			// moves on.
			if s.recordPublicationGap(h, berr, blk.Time) {
				continue
			}
			s.log.Fatalf("h=%d tx=%d: build publication: %v%s", h, i, berr, skipHint(h))
		}
		if err := s.store.AppendPublication(pub); err != nil {
			s.log.Fatalf("h=%d tx=%d: append publication: %v", h, i, err)
		}
		recorded++
		a := pub.Assignment
		s.log.Printf("RECORDED h=%d tx=%d promise=%s commit=%s blob_v%d size=%d valset_h=%d assign[with_rows=%d sigma=%d distinct=%d overlaps=%d] must_serve_until=%s",
			h, i, pub.PromiseHash[:12], pub.Promise.Commitment[:12], pub.Promise.BlobVersion, pub.Promise.BlobSize,
			pub.Promise.Height, a.ValidatorsWithRows, a.Sigma, a.Distinct, a.WrapOverlaps, pub.MustServeUntil.Format(time.RFC3339))
		if a.Error != "" {
			s.log.Printf("  assignment note: %s", a.Error)
		}
	}

	// 3) Escrow movements: deposits, withdrawals, and what each promise
	//    charged. Recorded whether or not x/fibre params could be read; they
	//    need only the tx bytes.
	recorded += s.recordEconomy(blk, res, h)
	return recorded
}

func (s *Scanner) buildPublication(ctx context.Context, msg *fibretypes.MsgPayForFibre, blk *Block, txIndex int, txHash string, code uint32) (Publication, error) {
	pp := msg.PaymentPromise

	// promise hash (on-chain identity).
	var internal celfibre.PaymentPromise
	if err := internal.FromProto(&pp); err != nil {
		return Publication{}, fmt.Errorf("promise FromProto: %w", err)
	}
	hash, err := internal.Hash()
	if err != nil {
		return Publication{}, fmt.Errorf("promise hash: %w", err)
	}

	nsVersion := uint8(0)
	nsID := ""
	if len(pp.Namespace) > 0 {
		nsVersion = pp.Namespace[0]
		nsID = hexstr(pp.Namespace[1:])
	}

	fields := PromiseFields{
		ChainID:           pp.ChainId,
		Height:            pp.Height,
		Namespace:         hexstr(pp.Namespace),
		NamespaceVersion:  nsVersion,
		NamespaceID:       nsID,
		BlobSize:          pp.BlobSize,
		BlobVersion:       pp.BlobVersion,
		Commitment:        hexstr(pp.Commitment),
		CreationTimestamp: pp.CreationTimestamp.UTC(),
		SignerPublicKey:   hexstr(pp.SignerPublicKey.Key),
		Signature:         hexstr(pp.Signature),
	}

	mustServe, paramsSnap, basis, ambiguous, ok := s.params.MustServeUntilForPromise(pp.CreationTimestamp, pp.Height, blk.Height, txIndex)
	if !ok {
		return Publication{}, fmt.Errorf("no param history entry in effect at height %d tx %d", blk.Height, txIndex)
	}
	if ambiguous {
		s.log.Printf("publication %s: fibre params changed between promise height %d and settlement %d; earlier must_serve_until recorded", hexstr(pp.Commitment)[:8], pp.Height, blk.Height)
	}

	// assignment table over the validator set at the PROMISE height. A
	// failure to fetch the set is an RPC problem, retried and then fatal,
	// never frozen into the record: a record with an assignment error is
	// skipped by the prober for good, and there is no re-scan path.
	var table AssignmentTable
	var set valSetEntry
	if verr := s.retryRPC(ctx, fmt.Sprintf("validator set at height %d", pp.Height), func() error {
		var err error
		set, err = s.validatorSet(ctx, pp.Height)
		return err
	}); verr != nil {
		return Publication{}, fmt.Errorf("validator set at height %d: %w", pp.Height, verr)
	}
	vals := set.vals

	// Which validators does this promise PROVE stored their shard? The chain
	// does not answer that: its signature check stops once two thirds of the
	// stake has verified, leaving later entries unchecked, so the observer
	// verifies every signature itself against the consensus keys at the
	// promise height.
	signBytes, sberr := internal.SignBytes()
	if sberr != nil {
		return Publication{}, fmt.Errorf("promise sign bytes: %w", sberr)
	}
	// The members come back with the set, never from a second lookup: a
	// cache miss here once produced an empty member list, and an empty list
	// verifies nothing, which recorded every validator on the blob as
	// unattested while claiming to be evidence.
	if len(set.members) == 0 {
		return Publication{}, fmt.Errorf("validator set at height %d has no members", pp.Height)
	}
	att := verifyAttestations(signBytes, msg.ValidatorSignatures, set.members)
	if att.Unmatched > 0 || att.OutOfPosition > 0 {
		s.log.Printf("h=%d tx=%d promise %s: %d signature entries, %d verified, %d matched no validator, %d out of position",
			blk.Height, txIndex, hexstr(hash)[:12], att.Entries, att.Verified, att.Unmatched, att.OutOfPosition)
	}

	{
		var commitment [32]byte
		copy(commitment[:], pp.Commitment)
		table = buildAssignmentTable(commitment, pp.BlobVersion, pp.Height, vals, s.cfg.StoreRows, att)
	}
	// The host each validator had registered when the promise settled: the
	// endpoint the upload went to, from the chain's own events (HostHistory),
	// never from a state query. A validator that re-registers later is
	// probed at its new host, and the row can say the host changed.
	for i := range table.Validators {
		v := &table.Validators[i]
		if v.RowCount > 0 && !s.hosts.Known(v.Address) {
			s.lazySeed(ctx, v.Address, blk.Height)
		}
		v.Host, v.HostSource = s.hosts.HostAt(v.Address, blk.Height, txIndex, s.gaps)
		// Unknown only because an event-losing gap lies behind this
		// validator's newest entry, and the bulk re-read did not cover it
		// (not bonded then, or that query failed): read its registration
		// on its own, once per gap, and ask again.
		if v.RowCount > 0 && v.HostSource == HostUnknownGap && s.reseedOne(ctx, v.Address, blk.Height) {
			v.Host, v.HostSource = s.hosts.HostAt(v.Address, blk.Height, txIndex, s.gaps)
		}
	}

	return Publication{
		SchemaVersion:           SchemaVersion,
		PromiseHash:             hexstr(hash),
		SettlementHeight:        blk.Height,
		SettlementTime:          blk.Time.UTC(),
		SettlementTxHash:        txHash,
		SettlementTxIndex:       txIndex,
		SettlementTxCode:        code,
		Signer:                  msg.Signer,
		ValidatorSignatureCount: len(msg.ValidatorSignatures),
		Promise:                 fields,
		ParamsAtPublication:     paramsSnap,
		MustServeUntil:          mustServe,
		MustServeUntilBasis:     basis,
		MustServeUntilAmbiguous: ambiguous,
		Assignment:              table,
		RecordedAt:              time.Now().UTC(),
	}, nil
}

// valSetEntry is one cached validator set at one height.
type valSetEntry struct {
	vals    []assign.Validator
	members []ValSetMember
}

// maxValSetHeights bounds the validator-set cache. Publications arrive at
// many distinct promise heights, so an unbounded map would grow for the life
// of the process.
const maxValSetHeights = 256

// validatorSet fetches (and caches) the consensus validator set at height, as
// fibre-assign validators plus the raw members, verifying each address
// derives from its ed25519 key.
//
// Eviction is by insertion order, not by height. A promise height is not the
// scan height: the chain accepts a promise up to PaymentPromiseHeightWindow
// blocks old, so a late-settling publication can ask for a height below
// everything already cached. Dropping the lowest heights evicted exactly that
// entry in the same call that inserted it.
func (s *Scanner) validatorSet(ctx context.Context, height int64) (valSetEntry, error) {
	if e, ok := s.valSets[height]; ok {
		return e, nil
	}
	members, err := s.chain.ValidatorSet(ctx, height)
	if err != nil {
		return valSetEntry{}, err
	}
	if len(members) == 0 {
		return valSetEntry{}, fmt.Errorf("empty validator set")
	}
	out := make([]assign.Validator, 0, len(members))
	for _, m := range members {
		var a assign.Address
		if len(m.Address) != len(a) {
			return valSetEntry{}, fmt.Errorf("consensus address is %d bytes, want %d", len(m.Address), len(a))
		}
		copy(a[:], m.Address)
		if len(m.PubKey) == 32 {
			if derived, derr := assign.AddressFromEd25519PubKey(m.PubKey); derr == nil && derived != a {
				s.log.Printf("WARNING: validator address %s does not match sha256(pubkey)[:20]=%s at height %d", a, derived, height)
			}
		}
		out = append(out, assign.Validator{Address: a, VotingPower: m.VotingPower})
	}
	e := valSetEntry{vals: out, members: members}
	s.valSets[height] = e
	s.valSetOrder = append(s.valSetOrder, height)
	for len(s.valSetOrder) > maxValSetHeights {
		delete(s.valSets, s.valSetOrder[0])
		s.valSetOrder = s.valSetOrder[1:]
	}
	return e, nil
}

// seedHosts reads the bonded registry once, as the scan starts, and seeds
// the host history with it at startHeight: registrations older than the
// scan are known only from this. The state at startHeight is asked for
// first; a node that has pruned it answers with the current registry,
// which is what a registration older than the scan looks like either way.
// seedHosts reads the bonded registry into the host history, and leaves the
// history it was given alone until it has something to put there.
//
// It used to empty s.hosts as its first statement and only then make the
// query — which has no retry, unlike every other chain read here — so one
// timeout at startup discarded the persisted history AND left seeded false.
// The next checkpoint wrote the empty history back over state.json, and
// lazySeed, the one back-fill path for a validator the seed missed, returns
// immediately while unseeded: the loss was permanent for the life of the
// process, and the process would not have noticed.
func (s *Scanner) seedHosts(ctx context.Context, startHeight int64) {
	at, source := startHeight, HostFromSeed
	var provs []FibreProvider
	err := s.retryRPC(ctx, fmt.Sprintf("bonded registry at height %d", startHeight), func() error {
		var e error
		provs, e = s.chain.BondedFibreProvidersAt(ctx, startHeight)
		return e
	})
	if err != nil && !IsModuleInactive(err) {
		// The start's state is pruned: read the registry at the tip and
		// record it at the tip's height. Settlements between the start and
		// the tip with no event of their own are unknown, never this value.
		_, tip, terr := s.chain.Status(ctx)
		if terr == nil {
			err = s.retryRPC(ctx, fmt.Sprintf("bonded registry at height %d", tip), func() error {
				var e error
				provs, e = s.chain.BondedFibreProvidersAt(ctx, tip)
				return e
			})
			at, source = tip, HostFromSeedCurrent
		}
	}
	if err != nil {
		s.log.Printf("WARNING: bonded registry could not be read to seed the host history (%v); host_at_settlement is unknown until a validator registers again, and this is retried every %d heights", err, inactiveRetryEvery)
		return
	}
	// Only now: the entries already on record are kept, and the seed is
	// merged into them.
	s.hosts.Seed(at, provs, source)
	for _, e := range s.hosts.Entries() {
		if err := s.store.AppendHostEvent(HostEvent{HostEntry: e, Time: time.Now().UTC()}); err != nil {
			s.log.Printf("host_history: %v", err)
			break
		}
	}
	s.log.Printf("host history seeded at h=%d (%s) with %d registrations", at, source, len(provs))
}

// lazySeed reads one validator's registration the first time it appears in
// an assignment with nothing on record: the bonded seed misses a validator
// that was jailed or unbonding when the scan started, and its registration
// (which outlives bonding) needs no new event to stay in force. The state
// at the settlement height is asked for; every change since the seed
// height would be an event on record, so the answer holds from the seed
// height on. When that state is pruned the current one is read and
// recorded at the tip, which covers this settlement only if the scan is at
// the tip. A query error leaves the validator unknown this time.
func (s *Scanner) lazySeed(ctx context.Context, consAddrHex string, h int64) {
	seeded, seedAt := s.hosts.Seeded()
	if !seeded {
		return
	}
	bech, err := bech32.ConvertAndEncode("celestiavalcons", mustHexBytes(consAddrHex))
	if err != nil {
		return
	}
	host, _, err := s.chain.FibreProviderInfoAt(ctx, bech, h)
	if err == nil {
		e := s.hosts.SeedOne(consAddrHex, host, HostFromSeedLazy, seedAt)
		s.appendHost(e)
		s.log.Printf("host registration read for %s at h=%d: %q (in force since the seed at h=%d)", consAddrHex, h, host, seedAt)
		return
	}
	_, tip, terr := s.chain.Status(ctx)
	if terr != nil {
		s.log.Printf("WARNING: registration of %s could not be read (%v; %v); host_at_settlement unknown for now", consAddrHex, err, terr)
		return
	}
	host, _, err = s.chain.FibreProviderInfoAt(ctx, bech, tip)
	if err != nil {
		s.log.Printf("WARNING: registration of %s could not be read (%v); host_at_settlement unknown for now", consAddrHex, err)
		return
	}
	e := s.hosts.SeedOne(consAddrHex, host, HostFromSeedCurrent, tip)
	s.appendHost(e)
	s.log.Printf("host registration of %s read at the tip h=%d (state at h=%d pruned): %q, in force from h=%d", consAddrHex, tip, h, host, tip)
}

// maybeReseedHosts runs reseedHosts when an event-losing gap ends below h
// and the registry has not been re-read for it: once at the first height
// after the gap, then every inactiveRetryEvery heights while the read
// fails. Nothing happens before the first seed (the seed itself is then
// the answer) or when no such gap exists.
func (s *Scanner) maybeReseedHosts(ctx context.Context, h int64) {
	if seeded, _ := s.hosts.Seeded(); !seeded {
		return
	}
	end, ok := LastHostLossGapEnd(s.gaps, h)
	if !ok || s.reseedFor == end {
		return
	}
	if s.hosts.ReseededAt(end + 1) {
		// done by an earlier process: remembered, so the entries are not
		// walked again on every block
		s.reseedFor = end
		return
	}
	if s.reseedTriedAt != 0 && s.reseedTriedAt > end && h-s.reseedTriedAt < inactiveRetryEvery {
		return
	}
	s.reseedHosts(ctx, h, end)
}

// reseedHosts reads the bonded registry again after a scan gap that lost
// events, so host attribution recovers instead of staying unknown_gap for
// every validator until each one happens to register again.
//
// Why it is needed: host_at_settlement is the newest registration on
// record, and a gap whose blocks were never read may hold one that is not
// on record. HostAt is right to answer unknown_gap across such a gap — but
// with nothing after it, that answer never ended: after one transient gap
// the whole feed's host attribution was unknown for good.
//
// What it records: the registry as it stood after block h-1 — not h, whose
// registrations may come after a settlement in it — as in force from
// gapEnd+1. That is exact, not a guess: every block from gapEnd+1 to h-1
// was read, events and all (gapEnd is the newest event-losing gap below
// h), and a registration is never removed, so a validator whose host
// changed in that span has an event on record and is left alone (Reseed),
// and for every other one the value read is the value since gapEnd+1.
// Settlements inside or before the gap keep their unknown_gap: nothing
// here says what happened in it.
//
// One attempt, no retryRPC: a state that is pruned or slow to answer is
// retried later on maybeReseedHosts's cadence, and each validator still in
// the dark is re-read on its own when a publication needs it (reseedOne),
// so the scan is never held here.
func (s *Scanner) reseedHosts(ctx context.Context, h, gapEnd int64) {
	s.reseedTriedAt = h
	read := h - 1
	provs, err := s.chain.BondedFibreProvidersAt(ctx, read)
	if err != nil {
		s.log.Printf("WARNING: bonded registry at h=%d could not be read to re-seed the host history after the gap ending at h=%d (%v); host_at_settlement stays unknown until it is read (retried every %d heights, and per validator as needed)", read, gapEnd, err, inactiveRetryEvery)
		return
	}
	s.reseedFor = gapEnd
	added := s.hosts.Reseed(gapEnd+1, read, provs)
	for _, e := range added {
		s.appendHost(e)
	}
	s.log.Printf("host history re-seeded after the gap ending at h=%d: registry read at h=%d, %d of %d registrations recorded from h=%d", gapEnd, read, len(added), len(provs), gapEnd+1)
}

// reseedOne reads one validator's registration after an event-losing gap:
// the one the bulk re-read did not cover. Same rule as reseedHosts, for one
// validator, read with FibreProviderInfo (registration outlives bonding,
// so it answers for a validator AllBondedFibreProviders leaves out), at
// h-1, recorded from the gap's end. An explicit "not registered" is
// recorded as "none": it is the chain's answer, and nothing changed since
// the gap's end, or an event would be on record. Asked once per validator
// per gap in this process, and only after the bulk re-read for the gap
// worked; reports whether an entry was added.
func (s *Scanner) reseedOne(ctx context.Context, consAddrHex string, h int64) bool {
	if seeded, _ := s.hosts.Seeded(); !seeded {
		return false
	}
	end, ok := LastHostLossGapEnd(s.gaps, h)
	if !ok {
		return false
	}
	// Only once the bulk re-read for this gap has worked. Until then the
	// state after the gap is most likely not being served (a pruned or
	// catching-up node), and one query per validator per publication
	// against it, each up to the RPC timeout, would hold the scan for
	// nothing; the bulk read is retried on its own cadence.
	if s.reseedFor != end {
		return false
	}
	if s.reseedOneTried == nil {
		s.reseedOneTried = map[string]int64{}
	}
	if t, tried := s.reseedOneTried[consAddrHex]; tried && t == end {
		return false
	}
	s.reseedOneTried[consAddrHex] = end
	bech, err := bech32.ConvertAndEncode("celestiavalcons", mustHexBytes(consAddrHex))
	if err != nil {
		return false
	}
	read := h - 1
	host, _, err := s.chain.FibreProviderInfoAt(ctx, bech, read)
	if err != nil {
		s.log.Printf("WARNING: registration of %s at h=%d could not be read after the gap ending at h=%d (%v); host_at_settlement unknown_gap for it until it registers or the registry is re-read", consAddrHex, read, end, err)
		return false
	}
	e, added := s.hosts.ReseedOne(consAddrHex, host, end+1, read)
	if !added {
		return false
	}
	s.appendHost(e)
	s.log.Printf("host registration of %s re-read at h=%d after the gap ending at h=%d: %q, in force from h=%d", consAddrHex, read, end, host, end+1)
	return true
}

func (s *Scanner) appendHost(e HostEntry) {
	if err := s.store.AppendHostEvent(HostEvent{HostEntry: e, Time: time.Now().UTC()}); err != nil {
		s.log.Fatalf("host_history: %v", err)
	}
}

func mustHexBytes(h string) []byte {
	b, err := hex.DecodeString(h)
	if err != nil {
		return nil
	}
	return b
}
