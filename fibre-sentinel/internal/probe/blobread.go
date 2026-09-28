package probe

// Reading a blob the way celestia-app's Fibre client downloads one
// (fibre/download.go, fibre/client_download.go at the pinned commit):
//
//   - the validators are asked in the order validator.Set.Select gives,
//     over the whole set at the promise height: shuffled by stake, higher
//     stake first, the group whose rows do not overlap before the rest;
//     here only the endorsing ones, since the others owe the blob nothing;
//   - the next validator is asked while the rows still wanted outnumber the
//     rows already on their way (Reconstructor.Want against the reserved
//     ExpectedRows), so a failure hands its reservation to the next one;
//   - every shard is checked against the commitment by one Reconstructor
//     the whole reading shares, the first with its own row combination and
//     the rest against it, and a validator whose rows do not verify is
//     skipped as the client skips it;
//   - a request that is already on its way when the rows are enough is let
//     finish, as the client waits for it;
//   - each request, dial and DownloadShard together, gets the client's
//     RPCTimeout (15 s), and is made again at once after a failed dial or
//     an unreachable or timed-out peer (fibre/internal/grpc/client_cache.go).
//
// Two things differ, both on this observer's side of the line. Each
// validator is asked over one connection at a time from this observer
// (PerValidator), and a validator already busy with another blob's request
// is passed over and come back to, so one slow validator never holds a
// blob up. And when every endorsing validator has been asked and the rows
// are still short, the reading is made once more a minute later
// (ScheduleConfig.RetryAfter), asking again only the ones that did not
// serve and keeping the rows already verified: a blob is Unavailable only
// after that second pass.
//
// Nothing is written until the reading is final: then one row per
// validator asked, together (MeasurementStore.AppendReading). A validator
// the reading never asked has no row.

import (
	"context"
	"encoding/hex"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/celestiaorg/celestia-app/v10/pkg/rsema1d"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/scan"
)

// The outcome of a whole reading, as ReadInfo.BlobResult records it.
const (
	ReadAvailable   = "available"
	ReadUnavailable = "unavailable"
	ReadIncomplete  = "incomplete"
)

// readTarget is one endorsing validator in the order the reading asks.
type readTarget struct {
	Target
	order    int
	expected int // ExpectedRows: the rows it holds
}

// answer is one validator's part in a reading.
type answer struct {
	t        readTarget
	m        Measurement // the latest attempt
	attempts []Attempt   // the earlier ones
	pass     int
	asked    bool // the request was made (not left for want of time)
	redialed bool
	served   bool // rows came back and verified
	haveAt   int  // distinct rows the reading held once this answer was in
}

// blobReading is the state of one blob's reading across its passes.
type blobReading struct {
	p          *Prober
	pub        scan.Publication
	point      SchedulePoint
	coder      *Coder
	commitment [32]byte
	rec        *rsema1d.Reconstructor
	targets    []readTarget
	shadowGap  string

	mu      sync.Mutex
	answers map[string]*answer
	pass    int
}

// newBlobReading resolves who to ask and in what order.
func (p *Prober) newBlobReading(ctx context.Context, pub scan.Publication, pt SchedulePoint) (*blobReading, error) {
	pp := pub.Assignment.ProtocolParams
	coder, err := p.coderFor(pp.OriginalRows, pp.TotalRows)
	if err != nil {
		return nil, fmt.Errorf("coder: %w", err)
	}
	var commitment [32]byte
	cb, err := hex.DecodeString(pub.Promise.Commitment)
	if err != nil || len(cb) != len(commitment) {
		// A zero commitment would ask every validator for a blob nobody has
		// and publish the whole set as failing.
		return nil, fmt.Errorf("commitment %q is not 32 hex bytes", pub.Promise.Commitment)
	}
	copy(commitment[:], cb)
	rec, err := coder.c.NewReconstructor(rsema1d.Commitment(commitment))
	if err != nil {
		return nil, fmt.Errorf("reconstructor: %w", err)
	}
	targets, err := p.targetsFor(ctx, pub)
	if err != nil {
		return nil, fmt.Errorf("resolve targets: %w", err)
	}
	var endorsed []Target
	for _, t := range targets {
		if p.wants(t) {
			endorsed = append(endorsed, t)
		}
	}
	order := p.cfg.Order
	if order == nil {
		order = p.resolver.clientOrder
	}
	ordered, err := order(ctx, pub, endorsed)
	if err != nil {
		return nil, fmt.Errorf("client order: %w", err)
	}
	return &blobReading{p: p, pub: pub, point: pt, coder: coder, commitment: commitment, rec: rec,
		targets: ordered, shadowGap: p.shadowBlindness(pub), answers: map[string]*answer{}}, nil
}

// have is how many distinct verified rows the reading holds.
func (b *blobReading) have() int { return b.rec.Have() }

// enough reports whether the rows reconstruct the blob.
func (b *blobReading) enough() bool { return b.rec.Want() == 0 }

// run is one pass over candidates: the client's dispatch. It returns once
// no request is in flight and either the rows are enough or every
// candidate has been asked (or could not be, the reading's time being up).
// enough is called once, the moment the rows suffice.
func (b *blobReading) run(ctx context.Context, pass int, candidates []readTarget, onEnough func()) {
	b.mu.Lock()
	b.pass = pass
	b.mu.Unlock()
	cutoff := b.pub.MustServeUntil.Add(-b.p.schedCfg().RequestCutoff)
	var (
		mu       sync.Mutex
		inflight int
		running  int
		next     int
		passed   []readTarget // validators busy with another reading, to come back to
		wg       sync.WaitGroup
		signal   = make(chan struct{}, 1)
		once     sync.Once
	)
	notify := func() {
		select {
		case signal <- struct{}{}:
		default:
		}
	}
	tick := time.NewTicker(200 * time.Millisecond)
	defer tick.Stop()
	for {
		mu.Lock()
		want := b.rec.Want()
		if want == 0 {
			once.Do(onEnough)
		}
		late := !time.Now().Before(cutoff) || ctx.Err() != nil
		exhausted := next >= len(candidates) && len(passed) == 0
		if running == 0 && (want == 0 || exhausted || late) {
			mu.Unlock()
			break
		}
		launched := false
		if want > inflight && !late {
			var v readTarget
			found := false
			// The client's cursor, passing over a validator busy with
			// another reading of this observer's.
			for next < len(candidates) && !found {
				c := candidates[next]
				next++
				if b.p.reserveValidator(c.AddressHex) {
					v, found = c, true
				} else {
					passed = append(passed, c)
				}
			}
			// Then the ones passed over, once free.
			for i := 0; !found && i < len(passed); i++ {
				if b.p.reserveValidator(passed[i].AddressHex) {
					v, found = passed[i], true
					passed = append(passed[:i], passed[i+1:]...)
				}
			}
			if found {
				inflight += v.expected
				running++
				launched = true
				wg.Add(1)
				go func(v readTarget) {
					defer wg.Done()
					a := b.ask(ctx, v, pass, cutoff)
					b.p.releaseValidator(v.AddressHex)
					mu.Lock()
					inflight -= v.expected
					running--
					mu.Unlock()
					b.record(a)
					notify()
				}(v)
			}
		}
		mu.Unlock()
		if launched {
			continue
		}
		select {
		case <-signal:
		case <-tick.C:
		case <-ctx.Done():
		}
	}
	wg.Wait()
	if b.enough() {
		once.Do(onEnough)
	}
}

// record keeps a validator's answer, with its earlier ones.
func (b *blobReading) record(a *answer) {
	b.mu.Lock()
	defer b.mu.Unlock()
	a.haveAt = b.rec.Have()
	if prev, ok := b.answers[a.t.AddressHex]; ok {
		a.attempts = append(append(prev.attempts, attemptOf(prev.m, prev.pass)), a.attempts...)
		a.redialed = a.redialed || prev.redialed
	}
	b.answers[a.t.AddressHex] = a
}

// ask makes one validator's request, and the client's one re-dial.
func (b *blobReading) ask(ctx context.Context, v readTarget, pass int, cutoff time.Time) *answer {
	p := b.p
	a := &answer{t: v, pass: pass}
	in := p.inputFor(b.pub, v.Target, b.point, b.commitment, b.rec, b.shadowGap)
	release := p.admit(in.ExpectedShardBytes)
	defer release()
	// The request's time starts once this observer has let it go, never
	// while it waited for room on this side.
	if !time.Now().Before(cutoff) || ctx.Err() != nil {
		a.m = p.notAsked(b.pub, b.point, v.Target, "not asked: the reading ran out of time before this request could start (this observer's own backlog); an answer from past the deadline would say nothing about the obligation")
		return a
	}
	a.asked = true
	m := Run(ctx, in, b.coder, p.cfg.Timeouts)
	if redials(m.Outcome) && ctx.Err() == nil && time.Now().Before(cutoff) {
		a.attempts = append(a.attempts, attemptOf(m, pass))
		a.redialed = true
		m = Run(ctx, in, b.coder, p.cfg.Timeouts)
	}
	a.m = m
	a.served = m.Download.CommitmentVerified
	return a
}

// redials reports the answers after which the client drops the connection
// and asks again at once, re-resolving the host: a dial that failed, or a
// peer that was unreachable or timed out (client_cache.go, isUnreachable).
// An answer from a server that responded is final.
func redials(o Outcome) bool {
	switch o {
	case OutcomeDNSFail, OutcomeTCPRefused, OutcomeTCPTimeout, OutcomeTCPUnreachable,
		OutcomeTLSFail, OutcomeIdentityFail, OutcomeRPCUnavailable, OutcomeRPCTimeout:
		return true
	}
	return false
}

func attemptOf(m Measurement, pass int) Attempt {
	return Attempt{Pass: pass, StartedAt: m.StartedAt, Outcome: m.Outcome, RawError: m.RawError, DurationMS: m.TotalDurationMS}
}

// unserved are the endorsing validators whose rows did not come back.
func (b *blobReading) unserved() []readTarget {
	b.mu.Lock()
	defer b.mu.Unlock()
	var out []readTarget
	for _, t := range b.targets {
		if a, ok := b.answers[t.AddressHex]; !ok || !a.served {
			out = append(out, t)
		}
	}
	return out
}

// allAsked reports whether every endorsing validator was asked in the
// latest pass it was due in.
func (b *blobReading) allAsked() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, t := range b.targets {
		if a, ok := b.answers[t.AddressHex]; !ok || !a.asked {
			return false
		}
	}
	return true
}

// shortEvenWithGaps reports whether the rows stay short of the blob even if
// every validator whose answer was this observer's own failure (a local
// resolver, a shard this build could not handle) had served in full: the
// verdict's Unavailable test (verdict.Reading.Unavailable), so a reading
// is recorded Unavailable only when the verdict will say so.
func (b *blobReading) shortEvenWithGaps() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	potential := 0
	for _, t := range b.targets {
		if a, ok := b.answers[t.AddressHex]; !ok || a.m.Outcome == OutcomeProbeError {
			potential += t.RowCount
		}
	}
	return b.rec.Have()+potential < b.pub.Assignment.ProtocolParams.OriginalRows
}

// rows is the reading's final record: one row per validator asked, with
// where it sat in the reading. result is ReadAvailable, ReadUnavailable or
// ReadIncomplete. On an incomplete reading a validator whose rows did not
// come back is this observer's gap, not the validator's answer: the reading
// that would have decided it was not made.
func (b *blobReading) rows(result, why string) []Measurement {
	b.mu.Lock()
	defer b.mu.Unlock()
	var out []Measurement
	for _, t := range b.targets {
		a, ok := b.answers[t.AddressHex]
		if !ok {
			continue
		}
		m := a.m
		m.HostAtSettlement = t.HostAtSettlement
		if len(a.attempts) > 0 {
			first := a.attempts[0]
			m.Retry = &RetryInfo{Attempts: len(a.attempts) + 1, DelayMS: m.StartedAt.Sub(first.StartedAt).Milliseconds(),
				FirstStartedAt: first.StartedAt, FirstOutcome: first.Outcome, FirstError: first.RawError, FirstDurationMS: first.DurationMS}
		}
		if result == ReadIncomplete && !a.served && m.Outcome != OutcomeProbeError {
			if m.Retry == nil {
				m.Retry = &RetryInfo{Attempts: 1, FirstStartedAt: m.StartedAt, FirstOutcome: m.Outcome, FirstError: m.RawError, FirstDurationMS: m.TotalDurationMS}
			}
			m.RawError = fmt.Sprintf("%s (answered %s: %s)", why, m.Outcome, m.RawError)
			m.Outcome = OutcomeProbeError
			m.Classification, m.ClassificationReason = Classify(Evidence{Assigned: m.Assigned, Attested: m.Attested,
				AttestationUnknown: m.AttestationUnknown, Phase: m.Phase, Outcome: m.Outcome})
			m.ClassificationReason += "; " + why
		}
		m.Read = &ReadInfo{Pass: a.pass, Order: t.order, Redialed: a.redialed, NovelRows: a.m.novel,
			BlobHaveAfter: a.haveAt, BlobResult: result, Attempts: a.attempts}
		out = append(out, m)
	}
	return out
}

// clientOrder is Resolver's ordering of a reading's targets: the client's
// own (validator.Set.Select, clientorder.go), falling back to voting power
// when the set cannot be built.
func (r *Resolver) clientOrder(ctx context.Context, pub scan.Publication, targets []Target) ([]readTarget, error) {
	members, err := r.validatorSet(ctx, pub.Assignment.ValidatorSetHeight)
	if err != nil {
		return nil, err
	}
	order, expected, err := ClientOrder(members, pub.Assignment.ValidatorSetHeight, pub.Assignment.ProtocolParams)
	if err != nil {
		return nil, err
	}
	return orderTargets(targets, order, expected), nil
}

// orderTargets puts targets in the given order (addresses, lower-case hex),
// any the order does not name after it by voting power, and gives each its
// expected rows (its row count when the order has none for it).
func orderTargets(targets []Target, order []string, expected map[string]int) []readTarget {
	pos := make(map[string]int, len(order))
	for i, a := range order {
		pos[a] = i
	}
	out := make([]readTarget, 0, len(targets))
	for _, t := range targets {
		rt := readTarget{Target: t, expected: t.RowCount}
		if n, ok := expected[t.AddressHex]; ok {
			rt.expected = n
		}
		out = append(out, rt)
	}
	sort.SliceStable(out, func(i, j int) bool {
		pi, iok := pos[out[i].AddressHex]
		pj, jok := pos[out[j].AddressHex]
		switch {
		case iok && jok:
			return pi < pj
		case iok != jok:
			return iok
		}
		return out[i].VotingPower > out[j].VotingPower
	})
	for i := range out {
		out[i].order = i
	}
	return out
}
