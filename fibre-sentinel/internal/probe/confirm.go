package probe

// Not-served readings confirmed from a second vantage.
//
// A not-served reading rests on one reading from one place, and a timeout
// or a refused connection there may be this observer's own path. So it
// counts against a validator only once a second location has confirmed it
// (verdict.ConfirmNotServed); until then it counts neither way. The primary
// prober appends a ConfirmRequest for every row that can count not served
// (ConfirmationDue: an endorsing validator whose rows did not come back
// from a blob that could not be reconstructed, at a reading the
// correlated-failure guard does not set aside for good) to
// <data-dir>/vantage-requests.jsonl; deploy/vantage-pull.sh copies new
// lines to each second vantage, where sentinel-probe -confirm-requests
// (Confirmer) fetches exactly those rows from that validator once, with the
// same probe under the same client rules (Run: DNS, TCP, TLS, the
// consensus-key identity check, DownloadShard and the row verification
// against the commitment and the assignment; the client's RPCTimeout and
// its one re-dial), and appends the result to its own measurements.jsonl,
// stamped with its own vantage. The collector reads that file back and
// applies the rule in observer/verdict (ConfirmNotServed).
//
// Only failures are re-checked, never routine probes, so the load a second
// vantage adds is one request per not-served row. A request it cannot
// answer before its deadline (must_serve_until at the latest) lapses, and
// that row never counts.
//
// The vantage keeps no state of the primary's: the request names the
// promise, the blob, the validator and the host, and everything a verdict
// rests on is read again independently. The consensus key comes from the
// validator set at the promise height on the vantage's own RPC, and the
// assignment is recomputed from it with fibre-assign; a request whose rows
// disagree with that recomputation is refused, not probed.

import (
	"bufio"
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	assign "github.com/plsgiveup/fibre/fibre-assign"
	"github.com/plsgiveup/fibre/fibre-sentinel/internal/scan"
	"github.com/plsgiveup/fibre/fibre-sentinel/internal/status"
)

// ConfirmRequestsFile is where the primary prober appends its confirmation
// requests, under its data dir.
const ConfirmRequestsFile = "vantage-requests.jsonl"

// ConfirmWindow is how long after a not-served reading started a confirming
// probe from another vantage still answers it; the probe must also start
// before must_serve_until. A confirming probe started later is no answer,
// and the reading never counts.
const ConfirmWindow = 20 * time.Minute

// ConfirmRequestSchemaVersion is bumped when the request's JSON shape
// changes.
const ConfirmRequestSchemaVersion = 1

// ConfirmRequest asks another vantage to fetch one failed probe's rows once.
type ConfirmRequest struct {
	SchemaVersion int       `json:"schema_version"`
	RequestedAt   time.Time `json:"requested_at"`
	// Deadline is the last moment a confirming probe may start: the
	// reading's start plus ConfirmWindow, or must_serve_until if that is
	// sooner, since only an answer from inside the window confirms.
	Deadline    time.Time `json:"deadline"`
	FromVantage string    `json:"from_vantage"`
	ChainID     string    `json:"chain_id"`

	// the blob
	PromiseHash        string                      `json:"promise_hash"`
	Commitment         string                      `json:"commitment"`
	BlobVersion        uint32                      `json:"blob_version"`
	BlobSize           uint32                      `json:"blob_size"`
	MustServeUntil     time.Time                   `json:"must_serve_until"`
	ValidatorSetHeight int64                       `json:"validator_set_height"`
	ProtocolParams     scan.ProtocolParamsSnapshot `json:"protocol_params"`
	// PruneToleranceS is the primary's grace/post boundary, so the vantage
	// judges its answer in the same phases.
	PruneToleranceS int64 `json:"prune_tolerance_s"`

	// the validator and the host that failed
	ValidatorAddress   string `json:"validator_address"`
	ValidatorHost      string `json:"validator_host"`
	Attested           bool   `json:"attested"`
	AttestationUnknown bool   `json:"attestation_unknown,omitempty"`
	// AssignedRows are the rows the primary asked for and did not get back
	// verified: the whole assignment, since DownloadShard returns a shard.
	// The vantage recomputes them and refuses a request that disagrees.
	AssignedRows []int `json:"assigned_rows"`

	// the probe that failed
	ScheduleLabel  string         `json:"schedule_label"`
	ScheduledAt    time.Time      `json:"scheduled_at"`
	StartedAt      time.Time      `json:"started_at"`
	Outcome        Outcome        `json:"outcome"`
	Classification Classification `json:"classification"`
}

// Key is the slot the request is about, without a vantage: promise,
// validator and schedule point. A confirming row carries the same three.
func (r ConfirmRequest) Key() string {
	return r.PromiseHash + "|" + r.ValidatorAddress + "|" + r.ScheduledAt.UTC().Format(time.RFC3339Nano)
}

func (r ConfirmRequest) validate() error {
	switch {
	case r.PromiseHash == "" || r.ValidatorAddress == "" || r.ValidatorHost == "":
		return errors.New("request without promise_hash, validator_address or validator_host")
	case r.ScheduledAt.IsZero() || r.StartedAt.IsZero() || r.Deadline.IsZero():
		return errors.New("request without scheduled_at, started_at or deadline")
	case r.ValidatorSetHeight <= 0:
		return errors.New("request without validator_set_height")
	case r.ProtocolParams.OriginalRows <= 0 || r.ProtocolParams.TotalRows <= r.ProtocolParams.OriginalRows:
		return errors.New("request without usable protocol params")
	case len(r.AssignedRows) == 0:
		return errors.New("request without assigned rows")
	}
	if b, err := hex.DecodeString(r.Commitment); err != nil || len(b) != 32 {
		return fmt.Errorf("bad commitment %q", r.Commitment)
	}
	return nil
}

// NewConfirmRequest builds the request for a not-served row.
func NewConfirmRequest(pub scan.Publication, t Target, m Measurement, chainID string, pruneTolerance time.Duration, now time.Time) ConfirmRequest {
	deadline := m.StartedAt.Add(ConfirmWindow)
	if pub.MustServeUntil.Before(deadline) {
		deadline = pub.MustServeUntil
	}
	return ConfirmRequest{
		SchemaVersion: ConfirmRequestSchemaVersion, RequestedAt: now.UTC(), Deadline: deadline.UTC(),
		FromVantage: m.Vantage, ChainID: chainID,
		PromiseHash: pub.PromiseHash, Commitment: pub.Promise.Commitment, BlobVersion: pub.Promise.BlobVersion,
		BlobSize: pub.Promise.BlobSize, MustServeUntil: pub.MustServeUntil.UTC(),
		ValidatorSetHeight: pub.Assignment.ValidatorSetHeight, ProtocolParams: pub.Assignment.ProtocolParams,
		PruneToleranceS:  int64(pruneTolerance / time.Second),
		ValidatorAddress: m.ValidatorAddress, ValidatorHost: m.ValidatorHost,
		Attested: m.Attested, AttestationUnknown: m.AttestationUnknown,
		AssignedRows:  append([]int(nil), t.AssignedRows...),
		ScheduleLabel: m.ScheduleLabel, ScheduledAt: m.ScheduledAt.UTC(), StartedAt: m.StartedAt.UTC(),
		Outcome: m.Outcome, Classification: m.Classification,
	}
}

// requestLog appends confirmation requests, one fsynced line each.
type requestLog struct {
	path string
	mu   sync.Mutex
	f    *os.File
}

func (l *requestLog) append(r ConfirmRequest) error {
	b, err := json.Marshal(r)
	if err != nil {
		return err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.f == nil {
		f, err := os.OpenFile(l.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			return err
		}
		l.f = f
	}
	if _, err := l.f.Write(append(b, '\n')); err != nil {
		return err
	}
	return l.f.Sync()
}

func (l *requestLog) close() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.f != nil {
		_ = l.f.Close()
		l.f = nil
	}
}

// requestConfirmation queues a not-served row for a second vantage. A
// failure to write it is logged and nothing more: without a request the row
// is never confirmed, and never counts.
func (p *Prober) requestConfirmation(pub scan.Publication, t Target, m Measurement) {
	if !ConfirmationDue(m) || p.requests == nil {
		return
	}
	now := time.Now()
	r := NewConfirmRequest(pub, t, m, p.chainID, p.schedCfg().PruneTolerance, now)
	if !now.Before(r.Deadline) {
		return // nothing another vantage could still say
	}
	if err := p.requests.append(r); err != nil {
		p.log.Printf("WARNING: confirmation request for %s %s not written: %v", short(m.PromiseHash), short(m.ValidatorAddress), err)
	}
}

// ---- the confirming side ----

// ConfirmChain is what the confirmer reads from the chain; *scan.Chain
// satisfies it.
type ConfirmChain interface {
	Status(ctx context.Context) (string, int64, error)
	ValidatorSet(ctx context.Context, height int64) ([]scan.ValSetMember, error)
	LatestBlockTime(ctx context.Context) (time.Time, error)
	AppVersion(ctx context.Context) (uint64, error)
}

// ConfirmConfig controls a confirmer run.
type ConfirmConfig struct {
	RequestsPath string // requests.jsonl copied in from the primary
	DataDir      string // measurements.jsonl is written here
	Vantage      string
	Timeouts     StepTimeouts
	// PollEvery is how often the requests file is read again.
	PollEvery time.Duration
	// MaxPerHour bounds the confirming probes of any one validator in an
	// hour. A validator that lost everything is not served on every blob,
	// and the second vantage must not turn that into a download storm
	// against it; its requests past the cap wait, and those whose deadline
	// passes meanwhile lapse, so those readings never count.
	MaxPerHour int
	// Workers is how many validators are asked at once (default 8). One
	// validator is asked one request at a time, the oldest deadline first,
	// as the primary asks it.
	Workers int
	// AllowUnroutableHosts: tests and a local devnet only, as for the prober.
	AllowUnroutableHosts bool
	Once                 bool // one pass over what is due, then exit
	RunConfig            map[string]any
}

// Confirmer answers confirmation requests from the primary.
type Confirmer struct {
	cfg   ConfirmConfig
	log   *scan.Logger
	chain ConfirmChain
	store *MeasurementStore

	// run is the probe; Run outside tests.
	run func(context.Context, Input, *Coder, StepTimeouts) Measurement
	now func() time.Time

	// what the chain was last read as (refreshChain), under mu: the pass's
	// feed refreshes it while the workers read it (chainFacts)
	chainID     string
	clockOffset time.Duration
	observer    ObserverInfo

	offset int64 // bytes of the requests file consumed; the pass's feed alone reads it
	status *status.Writer

	// mu guards what a pass's workers share: the requests not yet probed,
	// the starts of each validator's probes of the last hour, the coders,
	// the chain facts.
	mu      sync.Mutex
	pending map[string]ConfirmRequest // by Key, not yet probed
	spent   map[string][]time.Time    // by validator
	coders  map[[2]int]*Coder
}

// NewConfirmer builds a Confirmer over a chain client.
func NewConfirmer(cfg ConfirmConfig, chain ConfirmChain, log *scan.Logger) (*Confirmer, error) {
	if cfg.PollEvery <= 0 {
		cfg.PollEvery = 20 * time.Second
	}
	if cfg.MaxPerHour <= 0 {
		cfg.MaxPerHour = 60
	}
	if cfg.Workers <= 0 {
		cfg.Workers = 8
	}
	if cfg.Vantage == "" {
		return nil, errors.New("a confirming vantage needs a name")
	}
	if cfg.RequestsPath == "" {
		return nil, errors.New("no requests file")
	}
	st, err := OpenMeasurementStore(cfg.DataDir)
	if err != nil {
		return nil, err
	}
	return &Confirmer{
		cfg: cfg, log: log, chain: chain, store: st, run: Run, now: time.Now,
		observer: ObserverInfo{Build: status.BuildRevision(), AssignPin: assign.PinnedCelestiaAppCommit},
		coders:   map[[2]int]*Coder{}, pending: map[string]ConfirmRequest{}, spent: map[string][]time.Time{},
	}, nil
}

// Close closes the measurement store.
func (c *Confirmer) Close() error { return c.store.Close() }

// Run answers requests until ctx ends (or once, with Once).
func (c *Confirmer) Run(ctx context.Context) error {
	c.status = status.New(c.cfg.DataDir, "confirm", c.cfg.Vantage, status.BuildRevision())
	c.status.RecordRuns(c.cfg.RunConfig)
	c.status.Start()
	defer c.status.Stop("exit")
	for {
		if err := c.pass(ctx); err != nil {
			c.log.Printf("confirm: %v", err)
			c.status.Error(err.Error())
		} else {
			c.status.OK()
		}
		c.status.Set("pending", c.pendingLen())
		if c.cfg.Once {
			return nil
		}
		if !sleepCtx(ctx, c.cfg.PollEvery) {
			return nil
		}
	}
}

// pass reads new requests and probes what is due: Workers validators at a
// time, one request per validator at a time, the oldest deadline first,
// each validator within its hourly cap.
//
// The queue is fed while the pass runs: every PollEvery the requests file
// is read again, and what is new joins the queue, as does a request left
// for later (a validator over its cap, a chain read that failed). A
// validator that hangs holds one worker for two RPC timeouts per request,
// and a pass that only read the file at its start kept every request that
// arrived meanwhile, for any validator, waiting behind that backlog while
// the other workers sat idle, often past its deadline. The pass ends once
// nothing is queued and nothing is in flight.
func (c *Confirmer) pass(ctx context.Context) error {
	if err := c.readRequests(); err != nil {
		return fmt.Errorf("read %s: %w", c.cfg.RequestsPath, err)
	}
	if c.pendingLen() == 0 {
		return nil
	}
	if err := c.refreshChain(ctx); err != nil {
		return err
	}

	var (
		mu    sync.Mutex
		cond  = sync.NewCond(&mu)
		queue []ConfirmRequest
		// active is every request in the queue or in flight, by Key.
		active   = map[string]bool{}
		busy     = map[string]bool{}
		inflight int
		firstErr error
		wg       sync.WaitGroup
	)
	// enqueue adds every pending request not already queued or in flight,
	// and keeps the queue oldest deadline first: the request closest to
	// expiring is the one a cap or a queue is most likely to cost. mu is
	// held.
	enqueue := func() {
		c.mu.Lock()
		for k, r := range c.pending {
			if !active[k] {
				active[k] = true
				queue = append(queue, r)
			}
		}
		c.mu.Unlock()
		sort.Slice(queue, func(i, j int) bool {
			if !queue[i].Deadline.Equal(queue[j].Deadline) {
				return queue[i].Deadline.Before(queue[j].Deadline)
			}
			return queue[i].Key() < queue[j].Key()
		})
	}
	mu.Lock()
	enqueue()
	mu.Unlock()
	// next takes the oldest request whose validator is not being asked, and
	// waits while every one left is for a validator that is, or while the
	// queue is empty and a request is still in flight (the feed may bring
	// more meanwhile).
	next := func() (ConfirmRequest, bool) {
		mu.Lock()
		defer mu.Unlock()
		for {
			if ctx.Err() != nil || firstErr != nil || (len(queue) == 0 && inflight == 0) {
				return ConfirmRequest{}, false
			}
			for i, r := range queue {
				if !busy[r.ValidatorAddress] {
					queue = append(queue[:i], queue[i+1:]...)
					busy[r.ValidatorAddress] = true
					inflight++
					return r, true
				}
			}
			cond.Wait()
		}
	}
	for w := 0; w < c.cfg.Workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				r, ok := next()
				if !ok {
					return
				}
				err := c.answer(ctx, r)
				mu.Lock()
				busy[r.ValidatorAddress] = false
				inflight--
				// Answered or dropped, it has left pending; left for later,
				// the next feed queues it again.
				delete(active, r.Key())
				if err != nil && firstErr == nil {
					firstErr = err
				}
				cond.Broadcast()
				mu.Unlock()
			}
		}()
	}
	// The feed: new requests, and those left for later, join the queue
	// while the pass runs.
	fed := make(chan struct{})
	feedDone := make(chan struct{})
	go func() {
		defer close(feedDone)
		tick := time.NewTicker(c.cfg.PollEvery)
		defer tick.Stop()
		for {
			select {
			case <-fed:
				return
			case <-ctx.Done():
				return
			case <-tick.C:
			}
			if err := c.readRequests(); err != nil {
				c.log.Printf("confirm: read %s: %v", c.cfg.RequestsPath, err)
				continue
			}
			if err := c.refreshChain(ctx); err != nil {
				c.log.Printf("confirm: %v", err)
			}
			if c.status != nil {
				c.status.Set("pending", c.pendingLen())
			}
			mu.Lock()
			enqueue()
			cond.Broadcast()
			mu.Unlock()
		}
	}()
	// A worker waiting on a busy validator is woken when that request ends;
	// a stop wakes every one of them.
	stop := context.AfterFunc(ctx, func() {
		mu.Lock()
		cond.Broadcast()
		mu.Unlock()
	})
	wg.Wait()
	close(fed)
	<-feedDone
	stop()
	return firstErr
}

// refreshChain reads the chain's id once, and its clock and app version
// every time; a read that fails keeps the previous value.
func (c *Confirmer) refreshChain(ctx context.Context) error {
	id, _, _ := c.chainFacts()
	if id == "" {
		got, _, err := c.chain.Status(ctx)
		if err != nil {
			return fmt.Errorf("chain status: %w", err)
		}
		id = got
	}
	bt, btErr := c.chain.LatestBlockTime(ctx)
	v, vErr := c.chain.AppVersion(ctx)
	c.mu.Lock()
	defer c.mu.Unlock()
	c.chainID = id
	if btErr == nil {
		c.clockOffset = c.now().Sub(bt)
	}
	if vErr == nil {
		c.observer.AppVersion = v
		c.observer.PinStale = v > assign.PinnedCelestiaAppMajor
	}
	return nil
}

// chainFacts is what the chain was last read as: its id, this vantage's
// clock offset from it, and the build and app version answers carry.
func (c *Confirmer) chainFacts() (string, time.Duration, ObserverInfo) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.chainID, c.clockOffset, c.observer
}

// answer probes one request, or drops it once its deadline has passed. A
// request left for later (the chain did not answer, the validator's cap,
// a shutdown) stays pending. The error is a failure to write the answer.
func (c *Confirmer) answer(ctx context.Context, r ConfirmRequest) error {
	now := c.now()
	if !now.Before(r.Deadline) {
		c.log.Printf("confirm %s %s %s: deadline %s passed before it could be probed; the reading is never counted",
			short(r.PromiseHash), short(r.ValidatorAddress), r.ScheduleLabel, r.Deadline.Format(time.RFC3339))
		c.forget(r)
		return nil
	}
	in, coder, err := c.input(ctx, r)
	var m Measurement
	switch {
	case errors.Is(err, errChainRead):
		// The chain did not answer; nothing is known about the validator
		// yet. Asked again next pass, until the deadline.
		c.log.Printf("confirm %s %s: %v; retried next pass", short(r.PromiseHash), short(r.ValidatorAddress), err)
		return nil
	case err != nil:
		// The chain does not bear the request out: refused, with no
		// connection to the validator, and recorded, so the refusal is on
		// the record rather than only in a log.
		m = c.refused(r, err)
	default:
		if !c.admit(r.ValidatorAddress, now) {
			return nil // over this validator's hourly cap: it waits for a later pass
		}
		m = c.run(ctx, in, coder, c.cfg.Timeouts)
		if redials(m.Outcome) && ctx.Err() == nil {
			// the client's one re-dial (blobread.go, redials)
			first := m
			m = c.run(ctx, in, coder, c.cfg.Timeouts)
			m.Retry = &RetryInfo{Attempts: 2, DelayMS: m.StartedAt.Sub(first.StartedAt).Milliseconds(), FirstStartedAt: first.StartedAt,
				FirstOutcome: first.Outcome, FirstError: first.RawError, FirstDurationMS: first.TotalDurationMS}
		}
		if ctx.Err() != nil && m.Classification == ClassProbeError {
			return nil // abandoned by shutdown: asked again on the next start
		}
		m.ClassificationReason = fmt.Sprintf("confirmation from %s of the %s not-served reading recorded by %s at %s: %s",
			c.cfg.Vantage, r.Outcome, r.FromVantage, r.StartedAt.UTC().Format(time.RFC3339), m.ClassificationReason)
	}
	if err := c.store.Append(m); err != nil {
		return fmt.Errorf("append measurement: %w", err)
	}
	c.forget(r)
	c.log.Printf("CONFIRM %s val=%s %s (%s from %s) -> %s / %s",
		short(r.PromiseHash), short(r.ValidatorAddress), r.ScheduleLabel, r.Outcome, r.FromVantage, m.Outcome, m.Classification)
	return nil
}

func (c *Confirmer) forget(r ConfirmRequest) {
	c.mu.Lock()
	delete(c.pending, r.Key())
	c.mu.Unlock()
}

func (c *Confirmer) pendingLen() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.pending)
}

// admit applies a validator's hourly cap and records the start.
func (c *Confirmer) admit(validator string, now time.Time) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	keep := c.spent[validator][:0]
	for _, t := range c.spent[validator] {
		if now.Sub(t) < time.Hour {
			keep = append(keep, t)
		}
	}
	if len(keep) >= c.cfg.MaxPerHour {
		c.spent[validator] = keep
		return false
	}
	c.spent[validator] = append(keep, now)
	return true
}

// readRequests reads the lines added to the requests file since the last
// pass. The file is copied in by sftp and can be replaced by a fresh full
// copy, so a file shorter than the offset is read again from the start;
// every request already answered is in measurements.jsonl and is skipped.
// A trailing line without its newline is a copy in progress, left for the
// next pass.
func (c *Confirmer) readRequests() error {
	f, err := os.Open(c.cfg.RequestsPath)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return err
	}
	if info.Size() < c.offset {
		c.offset = 0
	}
	if _, err := f.Seek(c.offset, io.SeekStart); err != nil {
		return err
	}
	r := bufio.NewReaderSize(f, 1<<16)
	for {
		line, err := r.ReadBytes('\n')
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		c.offset += int64(len(line))
		line = bytes.TrimSpace(line)
		if len(line) == 0 {
			continue
		}
		var req ConfirmRequest
		if err := json.Unmarshal(line, &req); err != nil {
			c.log.Printf("confirm: undecodable request line stepped over: %v", err)
			continue
		}
		if err := req.validate(); err != nil {
			c.log.Printf("confirm: request stepped over: %v", err)
			continue
		}
		if req.FromVantage == c.cfg.Vantage {
			c.log.Printf("confirm: request from this vantage's own name %q stepped over", req.FromVantage)
			continue
		}
		if c.store.Has(c.cfg.Vantage, req.PromiseHash, req.ValidatorAddress, req.ScheduledAt) {
			continue // answered before a restart
		}
		if !c.now().Before(req.Deadline) {
			continue // too late to say anything
		}
		c.mu.Lock()
		c.pending[req.Key()] = req
		c.mu.Unlock()
	}
}

// refused records a request the chain does not bear out as PROBE_ERROR: no
// answer, so the reading is not confirmed.
func (c *Confirmer) refused(r ConfirmRequest, err error) Measurement {
	now := c.now().UTC()
	_, offset, _ := c.chainFacts()
	return Measurement{
		SchemaVersion: MeasurementSchemaVersion, Vantage: c.cfg.Vantage,
		PromiseHash: r.PromiseHash, Commitment: r.Commitment, BlobVersion: r.BlobVersion,
		MustServeUntil: r.MustServeUntil, ValidatorSetHeight: r.ValidatorSetHeight,
		ValidatorAddress: r.ValidatorAddress, ValidatorHost: r.ValidatorHost, HostSource: "confirm_request",
		Assigned: true, Attested: r.Attested, AttestationUnknown: r.AttestationUnknown, AssignedRowCount: len(r.AssignedRows),
		ScheduleLabel: r.ScheduleLabel, ScheduledAt: r.ScheduledAt, StartedAt: now, FinishedAt: now,
		LatenessMS: now.Sub(r.ScheduledAt).Milliseconds(),
		Phase:      PhaseAtWindow(now, r.MustServeUntil, time.Duration(r.PruneToleranceS)*time.Second),
		Outcome:    OutcomeProbeError, Classification: ClassProbeError,
		ClassificationReason: "confirmation not probed: " + err.Error(),
		RawError:             err.Error(), ClockOffsetMS: offset.Milliseconds(),
	}
}

// errChainRead marks an input that failed because the chain did not answer,
// which is retried, as opposed to one the chain answered against, which is
// refused.
var errChainRead = errors.New("chain read failed")

// input resolves a request into a probe Input, reading the consensus key
// and the assignment from the chain rather than taking them from the
// request.
func (c *Confirmer) input(ctx context.Context, r ConfirmRequest) (Input, *Coder, error) {
	chainID, offset, observer := c.chainFacts()
	if r.ChainID != "" && chainID != "" && r.ChainID != chainID {
		return Input{}, nil, fmt.Errorf("request is for chain %s, this vantage reads %s", r.ChainID, chainID)
	}
	members, err := c.chain.ValidatorSet(ctx, r.ValidatorSetHeight)
	if err != nil {
		return Input{}, nil, fmt.Errorf("%w: validator set at height %d: %v", errChainRead, r.ValidatorSetHeight, err)
	}
	var commitment [32]byte
	cb, _ := hex.DecodeString(r.Commitment)
	copy(commitment[:], cb)
	vals := make([]assign.Validator, 0, len(members))
	var target *assign.Address
	var pub ed25519.PublicKey
	for _, m := range members {
		var a assign.Address
		if len(m.Address) != len(a) {
			return Input{}, nil, fmt.Errorf("consensus address %d bytes, want 20", len(m.Address))
		}
		copy(a[:], m.Address)
		vals = append(vals, assign.Validator{Address: a, VotingPower: m.VotingPower})
		if strings.EqualFold(a.String(), r.ValidatorAddress) {
			aa := a
			target = &aa
			if len(m.PubKey) == ed25519.PublicKeySize {
				pub = ed25519.PublicKey(append([]byte(nil), m.PubKey...))
			}
		}
	}
	if target == nil {
		return Input{}, nil, fmt.Errorf("validator %s is not in the set at height %d", r.ValidatorAddress, r.ValidatorSetHeight)
	}
	pp := r.ProtocolParams
	sm, err := assign.Assign(commitment, vals, assign.ProtocolParams{
		OriginalRows: pp.OriginalRows, TotalRows: pp.TotalRows, MinRowsPerValidator: pp.MinRowsPerValidator,
		LivenessThreshold: assign.Fraction{Numerator: pp.LivenessThresholdNum, Denominator: pp.LivenessThresholdDen},
	})
	if err != nil {
		return Input{}, nil, fmt.Errorf("recompute assignment: %v", err)
	}
	rows := sm[*target]
	if !sameRows(rows, r.AssignedRows) {
		return Input{}, nil, fmt.Errorf("the assignment recomputed from the chain (%d rows) is not the one the request names (%d rows)", len(rows), len(r.AssignedRows))
	}
	k := [2]int{pp.OriginalRows, pp.TotalRows}
	c.mu.Lock()
	coder, ok := c.coders[k]
	if !ok {
		if coder, err = NewCoder(pp.OriginalRows, pp.TotalRows); err != nil {
			c.mu.Unlock()
			return Input{}, nil, fmt.Errorf("coder: %v", err)
		}
		c.coders[k] = coder
	}
	c.mu.Unlock()
	return Input{
		Vantage: c.cfg.Vantage, ChainID: chainID, PromiseHash: r.PromiseHash,
		Commitment: commitment, CommitmentHex: r.Commitment, BlobVersion: r.BlobVersion,
		MustServeUntil: r.MustServeUntil, ValidatorSetHeight: r.ValidatorSetHeight,
		Target: Target{
			Address: *target, AddressHex: target.String(), PubKey: pub, Host: r.ValidatorHost, HostSource: "confirm_request",
			Assigned: true, Attested: r.Attested, AttestationUnknown: r.AttestationUnknown,
			AssignedRows: append([]int(nil), rows...), RowCount: len(rows),
		},
		AllowUnroutableHost: c.cfg.AllowUnroutableHosts,
		SchedulePoint:       SchedulePoint{At: r.ScheduledAt, Label: r.ScheduleLabel, Phase: PhaseInWindow},
		PruneTolerance:      time.Duration(r.PruneToleranceS) * time.Second,
		ExpectedShardBytes:  ShardBytes(r.BlobSize, pp.OriginalRows, len(rows)),
		MaxMessageSize:      maxMessageSizeFor(pp),
		ClockOffsetMS:       offset.Milliseconds(),
		Observer:            observer,
		// read as the Fibre client reads, like the reading it confirms
		ClientRules:    true,
		RequestTimeout: ClientRPCTimeout,
	}, coder, nil
}

func sameRows(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	x := append([]int(nil), a...)
	y := append([]int(nil), b...)
	sort.Ints(x)
	sort.Ints(y)
	for i := range x {
		if x[i] != y[i] {
			return false
		}
	}
	return true
}

// ConfirmRequestsPath is the primary's request file under dataDir.
func ConfirmRequestsPath(dataDir string) string { return filepath.Join(dataDir, ConfirmRequestsFile) }
