package probe

// Reading a blob the way celestia-app's Fibre client downloads one
// (fibre/download.go, fibre/client_download.go at the pinned commit):
//
//   - the validators are asked in the order validator.Set.Select gives,
//     over the whole set at the promise height: shuffled by stake, higher
//     stake first, the group whose rows do not overlap before the rest;
//   - the next validator is asked while the rows still wanted outnumber the
//     rows already on their way (Reconstructor.Want against the reserved
//     ExpectedRows), so a failure hands its reservation to the next one;
//   - every shard is checked against the commitment by one Reconstructor
//     the whole reading shares, and a validator whose rows do not verify is
//     skipped as the client skips it;
//   - a request already on its way when the rows are enough is let finish,
//     as the client waits for it;
//   - each request, lookup, dial and DownloadShard together, gets the
//     client's RPCTimeout (15 s), and is made again at once after it failed
//     before an answer came back: a failed lookup or dial, or an
//     unreachable or timed-out peer (fibre/internal/grpc/client_cache.go).
//
// The reading ends as the client's Download does: Available, the rows
// reconstruct the blob; or Unavailable with the client's error, "no shards
// retrieved" when no verified row came back, "not enough shards to
// reconstruct blob" when some did, fewer than it takes. The only other end
// is a reading that did not happen: not a single request reached a server
// (Reached), because this observer's own network was down (ReadNotRead).
//
// This observer's limits on requests and shard bytes in flight
// (Config.Concurrency, Config.InFlightBytes) only delay a request: its time
// starts once it is let go, it is never dropped, and it carries the phase
// the reading started in (Input.ReadingPhase), so a request held back is
// judged as the client, which asks at once, would have made it. The client
// has no limit per validator, and neither has the reading.
//
// With Config.AskEveryEndorser the reading asks every endorsing validator
// for its own rows, all of them, instead of stopping at enough: the same
// request, the same checks, one per endorser. Validators that did not
// endorse are not asked.
//
// Nothing is written until the reading ends: then one row per validator
// asked, together (MeasurementStore.AppendReading). A validator the reading
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

// The end of a whole reading, as ReadInfo.BlobResult records it.
const (
	ReadAvailable   = "available"
	ReadUnavailable = "unavailable"
	// ReadNotRead: not a single request reached a server (Reached holds
	// for none of them); the blob was not read by Tensile.
	ReadNotRead = "not_read"
)

// The Fibre client's errors for a blob it could not reconstruct
// (fibre.ErrNotFound, fibre.ErrNotEnoughShards), as ReadInfo.BlobError
// records them and the site shows them.
const (
	ClientErrNoShards        = "no shards retrieved"
	ClientErrNotEnoughShards = "not enough shards to reconstruct blob"
)

// ClientError is the client's error for a blob that could not be
// reconstructed, from the distinct verified rows that came back.
func ClientError(have int) string {
	if have == 0 {
		return ClientErrNoShards
	}
	return ClientErrNotEnoughShards
}

// readTarget is one validator in the order the reading asks.
type readTarget struct {
	Target
	order    int
	expected int // ExpectedRows: the rows it holds
}

// answer is one validator's part in a reading.
type answer struct {
	t      readTarget
	m      Measurement  // the last attempt
	first  *Measurement // the attempt before a re-dial
	served bool         // rows came back and verified
	haveAt int          // distinct rows the reading held once this answer was in
}

// blobReading is the state of one blob's reading.
type blobReading struct {
	p          *Prober
	pub        scan.Publication
	point      SchedulePoint
	coder      *Coder
	commitment [32]byte
	rec        *rsema1d.Reconstructor
	targets    []readTarget
	shadowGap  string
	// phase is the reading's, taken when it started; every request carries
	// it (Input.ReadingPhase).
	phase Phase

	mu      sync.Mutex
	answers map[string]*answer
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
		// A zero commitment would ask every validator for a blob nobody has.
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
		if t.Assigned && (!p.cfg.AskEveryEndorser || p.wants(t)) {
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
	return &blobReading{p: p, pub: pub, point: pt, coder: coder, commitment: commitment, rec: rec,
		targets: ordered, shadowGap: p.shadowBlindness(pub), answers: map[string]*answer{},
		phase: PhaseAt(time.Now().UTC(), pub, p.schedCfg())}, nil
}

// have is how many distinct verified rows the reading holds.
func (b *blobReading) have() int { return b.rec.Have() }

// enough reports whether the rows reconstruct the blob.
func (b *blobReading) enough() bool { return b.rec.Want() == 0 }

// run is the client's dispatch over the whole ordered set. It returns once
// no request is in flight and either the rows are enough or every
// validator has been asked (or the reading was stopped). onEnough is called
// once, the moment the rows suffice. With Config.AskEveryEndorser every
// target is asked whatever the rows, and onEnough waits for the last answer.
func (b *blobReading) run(ctx context.Context, onEnough func()) {
	all := b.p.cfg.AskEveryEndorser
	var (
		mu       sync.Mutex
		inflight int
		running  int
		next     int
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
	for {
		mu.Lock()
		want := b.rec.Want()
		if want == 0 && !all {
			once.Do(onEnough)
		}
		stopped := ctx.Err() != nil
		if running == 0 && ((want == 0 && !all) || next >= len(b.targets) || stopped) {
			mu.Unlock()
			break
		}
		launched := false
		if (all || want > inflight) && next < len(b.targets) && !stopped {
			v := b.targets[next]
			next++
			inflight += v.expected
			running++
			launched = true
			wg.Add(1)
			go func(v readTarget) {
				defer wg.Done()
				a := b.ask(ctx, v)
				mu.Lock()
				inflight -= v.expected
				running--
				mu.Unlock()
				b.record(a)
				notify()
			}(v)
		}
		mu.Unlock()
		if launched {
			continue
		}
		if stopped {
			// Requests still in flight end at once on a stopped context;
			// wait for them without spinning.
			<-signal
			continue
		}
		select {
		case <-signal:
		case <-ctx.Done():
		}
	}
	wg.Wait()
	if b.enough() {
		once.Do(onEnough)
	}
}

// record keeps a validator's answer.
func (b *blobReading) record(a *answer) {
	b.mu.Lock()
	defer b.mu.Unlock()
	a.haveAt = b.rec.Have()
	b.answers[a.t.AddressHex] = a
}

// ask makes one validator's request, and the client's one re-dial. The
// request waits for room under this observer's limits (admit), its time
// starts once it is let go, and it carries the reading's phase.
func (b *blobReading) ask(ctx context.Context, v readTarget) *answer {
	p := b.p
	a := &answer{t: v}
	in := p.inputFor(b.pub, v.Target, b.point, b.commitment, b.rec, b.shadowGap)
	in.ReadingPhase = b.phase
	release := p.admit(in.ExpectedShardBytes)
	defer release()
	m := Run(ctx, in, b.coder, p.cfg.Timeouts)
	if redials(m) && ctx.Err() == nil {
		first := m
		a.first = &first
		m = Run(ctx, in, b.coder, p.cfg.Timeouts)
	}
	a.m = m
	a.served = m.Download.CommitmentVerified
	return a
}

// redials reports the requests after which the client drops the
// connection and asks again at once, re-resolving the host
// (client_cache.go): one that failed before a server answered, that is a
// lookup or a dial that failed, whatever the cause (gRPC reports each as
// Unavailable), or a peer that was unreachable or timed out
// (isUnreachable). An answer from a server that responded is final, and a
// request that never started (no key to check the certificate against)
// would fail the same way again.
func redials(m Measurement) bool {
	switch m.Outcome {
	case OutcomeDNSFail, OutcomeTCPRefused, OutcomeTCPTimeout, OutcomeTCPUnreachable,
		OutcomeTLSFail, OutcomeIdentityFail, OutcomeRPCUnavailable, OutcomeRPCTimeout:
		return true
	case OutcomeProbeError:
		return !m.TCP.OK && (m.DNS.Attempted || m.TCP.Attempted)
	}
	return false
}

// result is what the reading came to: ReadAvailable; ReadUnavailable with
// the client's error; or ReadNotRead when not a single request reached a
// server (Reached holds for none).
func (b *blobReading) result() (result, clientErr string) {
	if b.enough() {
		return ReadAvailable, ""
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, a := range b.answers {
		if Reached(a.m.TCP.OK, a.m.Outcome, a.m.Download.CommitmentVerified) {
			return ReadUnavailable, ClientError(b.rec.Have())
		}
	}
	return ReadNotRead, ""
}

// rows is the reading's record: one row per validator asked, with where it
// sat in the reading and what the reading came to.
func (b *blobReading) rows(result, clientErr string) []Measurement {
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
		if f := a.first; f != nil {
			m.Retry = &RetryInfo{Attempts: 2, DelayMS: m.StartedAt.Sub(f.StartedAt).Milliseconds(),
				FirstStartedAt: f.StartedAt, FirstOutcome: f.Outcome, FirstError: f.RawError, FirstDurationMS: f.TotalDurationMS}
		}
		m.Read = &ReadInfo{Order: t.order, NovelRows: a.m.novel, BlobHaveAfter: a.haveAt, BlobResult: result, BlobError: clientErr}
		out = append(out, m)
	}
	return out
}

// clientOrder is Resolver's ordering of a reading's targets: the client's
// own (validator.Set.Select, clientorder.go). When the set cannot be built
// the reading does not start: it is tried again next cycle, and recorded
// NOT_PROBED once its latest start has passed.
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
