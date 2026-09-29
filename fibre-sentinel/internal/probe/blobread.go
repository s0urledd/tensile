package probe

// Reading a blob the way celestia-app's Fibre client downloads one
// (fibre/download.go, fibre/client_download.go at the pinned commit):
//
//   - the validators are asked in the order validator.Set.Select gives,
//     over the whole set at the promise height: shuffled by stake, higher
//     stake first, the group whose rows do not overlap before the rest.
//     That is every validator the assignment gives rows, endorsing or not,
//     as the client asks them: a blob is Unavailable only when the whole
//     set could not hand over enough rows. Only an endorsing validator owes
//     the blob, so only one can be counted not served;
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
// blob up; once only such validators are left and even their rows could
// not make the blob whole, the pass stops waiting for them, and they are
// this observer's gap. And when every validator has been asked and the
// rows are still short, the reading is made once more a minute later
// (ScheduleConfig.RetryAfter), asking again only the ones that did not
// serve and keeping the rows already verified: a blob is Unavailable only
// after that second pass has asked every one of them again whose rows
// could have made it whole.
//
// A validator the pass that decides an Unavailable reading was due to ask
// and did not is passed over (OutcomePassedOver): never counted, and
// counted by the correlated-failure guard as a validator that failed
// (GuardPassedOver), since at a slower pace it would have been asked and
// the ones passed over are the ones this observer's requests still wait on.
// Without that, whether a reading was set aside would depend on how busy
// this observer was.
//
// Nothing is written until the reading is final: then one row per
// validator asked, and on an Unavailable reading one per validator passed
// over, together (MeasurementStore.AppendReading). A validator the reading
// did not need to ask has no row.

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

// readTarget is one validator in the order the reading asks.
type readTarget struct {
	Target
	order    int
	expected int // ExpectedRows: the rows it holds
	// endorsing: its endorsement is on the settled promise (Prober.wants),
	// so it owes the blob and its answer can count for or against it.
	endorsing bool
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
	// finalPass and finalDue are the pass that decided an Unavailable
	// reading and the validators it was due to ask (settle).
	finalPass int
	finalDue  []readTarget
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
	var assigned []Target
	for _, t := range targets {
		if t.Assigned {
			assigned = append(assigned, t)
		}
	}
	order := p.cfg.Order
	if order == nil {
		order = p.resolver.clientOrder
	}
	ordered, err := order(ctx, pub, assigned)
	if err != nil {
		return nil, fmt.Errorf("client order: %w", err)
	}
	for i := range ordered {
		ordered[i].endorsing = p.wants(ordered[i].Target)
	}
	return &blobReading{p: p, pub: pub, point: pt, coder: coder, commitment: commitment, rec: rec,
		targets: ordered, shadowGap: p.shadowBlindness(pub), answers: map[string]*answer{}}, nil
}

// endorsing is how many of the reading's validators endorsed the promise.
func (b *blobReading) endorsing() int {
	n := 0
	for _, t := range b.targets {
		if t.endorsing {
			n++
		}
	}
	return n
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
		// Only validators busy with other readings are left, and even their
		// rows, with those of every validator without an answer of its own,
		// could not make the blob whole: waiting for them could not change
		// what this pass comes to, so the pass does not hold its blob for
		// them.
		decided := running == 0 && want > 0 && !late && next >= len(candidates) && len(passed) > 0 &&
			b.shortEvenWithGaps(pass, candidates)
		if running == 0 && (want == 0 || exhausted || late || decided) {
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
	// The re-dial belongs to the request: RequestCutoff leaves room for it
	// (two RPCTimeouts before the NotFoundGuard band), so it is made even
	// once the cutoff for starting a new request has passed.
	if redials(m.Outcome) && ctx.Err() == nil {
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

// unserved are the validators whose rows did not come back.
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

// askedIn reports whether every validator a pass was due to ask (due: every
// validator in the first pass, the ones unserved when it began in the
// second) was asked in that pass. An answer from an earlier pass does not
// do: a second pass cut short leaves the reading incomplete, never
// Unavailable on the first pass's word.
func (b *blobReading) askedIn(pass int, due []readTarget) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, t := range due {
		if a, ok := b.answers[t.AddressHex]; !ok || !a.asked || a.pass != pass {
			return false
		}
	}
	return true
}

// shortEvenWithGaps reports whether the rows stay short of the blob even if
// every validator without an answer of its own (OwnAnswer: none, or this
// observer's own failure, a local resolver or a shard this build could not
// handle) had served in full, and so had every validator a pass was due to
// ask (due) and did not ask in it (busy with another of this observer's
// readings until the pass ended). It is the verdict's Unavailable test
// (verdict.Reading.Unavailable) over the same predicate: a validator not
// asked in the pass that decides is recorded passed over, this observer's
// own gap (rows), so a reading is recorded Unavailable exactly when the
// verdict will say so.
func (b *blobReading) shortEvenWithGaps(pass int, due []readTarget) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	skipped := b.notAskedInLocked(pass, due)
	potential := 0
	for _, t := range b.targets {
		a, ok := b.answers[t.AddressHex]
		if !ok || !OwnAnswer(a.m.Phase, a.m.Classification, a.m.Download.CommitmentVerified) || skipped[t.AddressHex] {
			potential += t.RowCount
		}
	}
	return b.rec.Have()+potential < b.pub.Assignment.ProtocolParams.OriginalRows
}

// notAskedInLocked is the validators a pass was due to ask that it did not
// ask, whose rows are not in hand from an earlier pass. b.mu is held.
func (b *blobReading) notAskedInLocked(pass int, due []readTarget) map[string]bool {
	out := map[string]bool{}
	for _, t := range due {
		if a, ok := b.answers[t.AddressHex]; ok && !a.served && (a.pass != pass || !a.asked) {
			out[t.AddressHex] = true
		}
	}
	return out
}

// settle records the pass that decided the reading and the validators it
// was due to ask, for rows.
func (b *blobReading) settle(pass int, due []readTarget) {
	b.mu.Lock()
	b.finalPass, b.finalDue = pass, due
	b.mu.Unlock()
}

// rows is the reading's final record: one row per validator asked, and on
// an Unavailable reading one per validator passed over, with where it sat
// in the reading. result is ReadAvailable, ReadUnavailable or
// ReadIncomplete. On an incomplete reading a validator whose rows did not
// come back is this observer's gap, not the validator's answer: the reading
// that would have decided it was not made.
func (b *blobReading) rows(result, why string) []Measurement {
	b.mu.Lock()
	defer b.mu.Unlock()
	// On an Unavailable reading, a validator the deciding pass was due to
	// ask and did not answered once only, or never: it is passed over
	// (OutcomePassedOver), this observer's gap as the Unavailable test took
	// it (shortEvenWithGaps), with a row either way, so the
	// correlated-failure guard meets it (GuardPassedOver).
	var skipped map[string]bool
	if result == ReadUnavailable {
		skipped = b.notAskedInLocked(b.finalPass, b.finalDue)
		for _, t := range b.finalDue {
			if _, ok := b.answers[t.AddressHex]; !ok {
				skipped[t.AddressHex] = true
			}
		}
	}
	var out []Measurement
	for _, t := range b.targets {
		a, ok := b.answers[t.AddressHex]
		if !ok {
			if skipped[t.AddressHex] {
				out = append(out, b.passedOverLocked(t, result))
			}
			continue
		}
		m := a.m
		m.HostAtSettlement = t.HostAtSettlement
		if len(a.attempts) > 0 {
			first := a.attempts[0]
			m.Retry = &RetryInfo{Attempts: len(a.attempts) + 1, DelayMS: m.StartedAt.Sub(first.StartedAt).Milliseconds(),
				FirstStartedAt: first.StartedAt, FirstOutcome: first.Outcome, FirstError: first.RawError, FirstDurationMS: first.DurationMS}
		}
		if skipped[t.AddressHex] {
			reason := passedOverBusy
			if a.pass == b.finalPass && !a.asked {
				reason = passedOverLate
			}
			if a.asked {
				if m.Retry == nil {
					m.Retry = &RetryInfo{Attempts: 1, FirstStartedAt: m.StartedAt, FirstOutcome: m.Outcome, FirstError: m.RawError, FirstDurationMS: m.TotalDurationMS}
				}
				m.RawError = fmt.Sprintf("%s (answered %s in pass %d: %s)", reason, m.Outcome, a.pass, m.RawError)
			} else {
				m.RawError = reason
			}
			m.Outcome = OutcomePassedOver
			m.Classification, m.ClassificationReason = Classify(Evidence{Assigned: m.Assigned, Attested: m.Attested,
				AttestationUnknown: m.AttestationUnknown, Phase: m.Phase, Outcome: m.Outcome})
			m.ClassificationReason += "; " + reason
			m.Read = &ReadInfo{Pass: a.pass, Order: t.order, Redialed: a.redialed, NovelRows: a.m.novel,
				BlobHaveAfter: a.haveAt, BlobResult: result, Attempts: a.attempts}
			out = append(out, m)
			continue
		}
		gap := ""
		if result == ReadIncomplete && !a.served {
			gap = why
		}
		if gap != "" && m.Outcome != OutcomeProbeError {
			if m.Retry == nil {
				m.Retry = &RetryInfo{Attempts: 1, FirstStartedAt: m.StartedAt, FirstOutcome: m.Outcome, FirstError: m.RawError, FirstDurationMS: m.TotalDurationMS}
			}
			m.RawError = fmt.Sprintf("%s (answered %s: %s)", gap, m.Outcome, m.RawError)
			m.Outcome = OutcomeProbeError
			m.Classification, m.ClassificationReason = Classify(Evidence{Assigned: m.Assigned, Attested: m.Attested,
				AttestationUnknown: m.AttestationUnknown, Phase: m.Phase, Outcome: m.Outcome})
			m.ClassificationReason += "; " + gap
		}
		m.Read = &ReadInfo{Pass: a.pass, Order: t.order, Redialed: a.redialed, NovelRows: a.m.novel,
			BlobHaveAfter: a.haveAt, BlobResult: result, Attempts: a.attempts}
		out = append(out, m)
	}
	return out
}

// Why a validator was passed over, on its row (OutcomePassedOver).
const (
	passedOverBusy  = "passed over: not asked again in the second pass, busy with another of this observer's readings when the pass ended, and its rows could not have made the blob whole either way (this observer's own gap)"
	passedOverLate  = "passed over: not asked again in the second pass, whose time ran out before this request could start (this observer's own backlog), and its rows could not have made the blob whole either way"
	passedOverNever = "passed over: not asked in either pass (busy with another of this observer's readings, or out of time), and its rows could not have made the blob whole either way (this observer's own gap)"
)

// passedOverLocked is the row of a validator an Unavailable reading never
// asked. b.mu is held.
func (b *blobReading) passedOverLocked(t readTarget, result string) Measurement {
	m := b.p.notProbedRow(b.pub, b.point, t.Target, passedOverNever)
	m.HostAtSettlement = t.HostAtSettlement
	m.Outcome, m.RawError = OutcomePassedOver, passedOverNever
	m.Classification, m.ClassificationReason = Classify(Evidence{Assigned: m.Assigned, Attested: m.Attested,
		AttestationUnknown: m.AttestationUnknown, Phase: m.Phase, Outcome: m.Outcome})
	m.ClassificationReason += "; " + passedOverNever
	m.Read = &ReadInfo{Pass: b.finalPass, Order: t.order, BlobHaveAfter: b.rec.Have(), BlobResult: result}
	return m
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
