package probe

// The later attempts of a full reading.
//
// A validator whose answer in a full reading did not serve (FullServed) is
// asked again, up to FullReadRetries more times, each RetrySpacing after
// its last answer ended, while the attempt can start before
// must_serve_until less RequestStartMargin. An attempt that could not start
// by then because of the validator's own time is not owed: the row before
// it carries no NextAttemptDue, no job is queued and no row is written for
// it, and that row is the validator's last answer. This observer's own
// delays before that answer (a late reading, the waits for room, a lane,
// a restart: the job's shift) are taken out first: an attempt they alone
// push past the cutoff stays owed and is recorded as not made, this
// observer's gap, never the validator's last answer. A later answer that
// serves makes the validator served; every attempt is its own row
// (Measurement.Attempt), so the record keeps each failure and the reason
// shown is the last one.
//
// The attempts live apart from the reading. The reading writes its rows
// and gives back its blob slot (BlobConcurrency) and its Reconstructor when
// its first pass ends, as any reading does; an attempt holds neither. It
// is one request, without the client's re-dial (the attempt is itself the
// asking again), made under this observer's request and byte limits and
// its reading-rate ceiling (admitBy; the byte budget is charged its own
// verifier too, the ceiling its shard alone), whose rows are verified against
// the commitment on their own (a Reconstructor of that request alone, let
// go when it ends), and it appends its own row.
//
// At most one attempt is in flight to a validator at a time: the attempts
// due wait in the validator's lane, and one worker per lane makes them, the
// one whose time runs out first first. A validator whose endpoint fails
// before any blob is asked for (no such host, a connect refused, timed out
// or unroutable, a failed handshake or certificate: shareable) fails every
// attempt waiting for it the same way at that moment, so the answer of that
// one request is the answer of every attempt of the validator that was due
// when it started (Measurement.SharedFrom): a validator whose endpoint is
// down is judged on every blob it owes an attempt on, not on the few one
// slow request at a time could reach.
//
// An attempt that was owed and could not be made (its cutoff passed while
// it waited for the validator's earlier attempt or for this observer's
// limits or reading-rate ceiling, this observer's own delays before the
// earlier answer left no time, the validator could not be resolved, or a
// restart abandoned it) is written NOT_PROBED: this observer's gap, never
// the validator's. The
// queue of attempts is not persisted: a restarted prober finds the
// attempts its record still owes (MeasurementStore.PendingAttempts) and
// makes them, or records them as not made once their time is gone.

import (
	"bytes"
	"container/heap"
	"context"
	"encoding/hex"
	"fmt"
	"sync"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/scan"
)

// retryJob is one validator's next attempt in a full reading.
type retryJob struct {
	pub   scan.Publication
	point SchedulePoint
	// target is the validator as the reading asked it. After a restart only
	// its record (address, rows, endorsement) is known: resolved is false,
	// and the host and key are resolved when the attempt is made.
	target   Target
	resolved bool
	order    int
	attempt  int
	due      time.Time // the row before it: NextAttemptDue
	cutoff   time.Time // the last moment the attempt may start
	// result and clientErr are what the reading's first pass came to,
	// repeated on the attempt's row.
	result, clientErr string
	// recovered: the attempt was owed by a reading an earlier run made.
	recovered bool
	// shift is this observer's own delay before the answer this attempt
	// follows, summed over the validator's answers so far: how late the
	// reading started and each request waited for room, and how late each
	// earlier attempt started after it was due (its lane, its limits, a
	// restart). Owed-ness is decided with it taken out (nextAttemptDue).
	shift time.Duration
	index int
}

// wake is when the job is looked at: when it is due, or at its cutoff if
// that comes first (only an attempt an earlier run owed under other
// settings), so an attempt that cannot be made is recorded before the
// window closes.
func (j *retryJob) wake() time.Time {
	if j.cutoff.Before(j.due) {
		return j.cutoff
	}
	return j.due
}

// retryLane is the attempts due to one validator, waiting for its one
// attempt in flight, and whether a worker is making them.
type retryLane struct {
	jobs    []*retryJob
	running bool
}

// retryQueue holds the attempts not yet due (h), the ones due waiting for
// their validator (lanes), and which blobs still owe one.
type retryQueue struct {
	mu    sync.Mutex
	h     retryHeap
	lanes map[string]*retryLane
	// pending counts, per blob, the attempts queued, waiting or under way
	// and the readings writing their rows (hold); owned is every blob whose
	// owed attempts this run has taken on, so a restart's are recovered once.
	pending  map[string]int
	owned    map[string]bool
	waiting  int // attempts in the lanes
	inFlight int // attempts being made
	// targets resolves each blob's validators once for the attempts an
	// earlier run owed (resolveTarget), at most resolveSem at a time.
	targets    map[string]*blobTargets
	resolveSem chan struct{}
	wakeC      chan struct{}
}

func newRetryQueue() *retryQueue {
	return &retryQueue{lanes: map[string]*retryLane{}, pending: map[string]int{}, owned: map[string]bool{},
		targets: map[string]*blobTargets{}, resolveSem: make(chan struct{}, 4), wakeC: make(chan struct{}, 1)}
}

func (q *retryQueue) push(j *retryJob) {
	q.mu.Lock()
	q.pending[j.pub.PromiseHash]++
	q.owned[j.pub.PromiseHash] = true
	heap.Push(&q.h, j)
	q.mu.Unlock()
	select {
	case q.wakeC <- struct{}{}:
	default:
	}
}

// popDue returns the job whose wake time has come first, or how long until
// the earliest one's.
func (q *retryQueue) popDue(now time.Time) (*retryJob, time.Duration) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.h) == 0 {
		return nil, 0
	}
	if w := q.h[0].wake(); w.After(now) {
		return nil, w.Sub(now)
	}
	return heap.Pop(&q.h).(*retryJob), 0
}

// enqueue puts a due job in its validator's lane, and reports whether the
// lane needs a worker started.
func (q *retryQueue) enqueue(j *retryJob) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	v := j.target.AddressHex
	l := q.lanes[v]
	if l == nil {
		l = &retryLane{}
		q.lanes[v] = l
	}
	l.jobs = append(l.jobs, j)
	q.waiting++
	if l.running {
		return false
	}
	l.running = true
	return true
}

// next takes the lane's job whose cutoff comes first (then the one due
// first), or ends the lane's worker when none is left.
func (q *retryQueue) next(v string) *retryJob {
	q.mu.Lock()
	defer q.mu.Unlock()
	l := q.lanes[v]
	if l == nil {
		return nil
	}
	if len(l.jobs) == 0 {
		delete(q.lanes, v)
		return nil
	}
	best := 0
	for i, j := range l.jobs {
		b := l.jobs[best]
		if j.cutoff.Before(b.cutoff) || (j.cutoff.Equal(b.cutoff) && j.due.Before(b.due)) {
			best = i
		}
	}
	j := l.jobs[best]
	l.jobs = append(l.jobs[:best], l.jobs[best+1:]...)
	q.waiting--
	return j
}

// lane is a copy of the jobs waiting in a validator's lane.
func (q *retryQueue) lane(v string) []*retryJob {
	q.mu.Lock()
	defer q.mu.Unlock()
	if l := q.lanes[v]; l != nil {
		return append([]*retryJob(nil), l.jobs...)
	}
	return nil
}

// take removes these jobs from a validator's lane. Only the lane's worker
// removes jobs, so every one is still there.
func (q *retryQueue) take(v string, js []*retryJob) {
	if len(js) == 0 {
		return
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	l := q.lanes[v]
	if l == nil {
		return
	}
	drop := map[*retryJob]bool{}
	for _, j := range js {
		drop[j] = true
	}
	kept := l.jobs[:0]
	for _, j := range l.jobs {
		if drop[j] {
			q.waiting--
			continue
		}
		kept = append(kept, j)
	}
	l.jobs = kept
}

// drain empties a stopped lane: its jobs are owed again after a restart.
func (q *retryQueue) drain(v string) []*retryJob {
	q.mu.Lock()
	defer q.mu.Unlock()
	l := q.lanes[v]
	if l == nil {
		return nil
	}
	delete(q.lanes, v)
	q.waiting -= len(l.jobs)
	return l.jobs
}

// started and ended count the attempts being made.
func (q *retryQueue) started() {
	q.mu.Lock()
	q.inFlight++
	q.mu.Unlock()
}

func (q *retryQueue) ended() {
	q.mu.Lock()
	q.inFlight--
	q.mu.Unlock()
}

// done ends a job.
func (q *retryQueue) done(hash string) { q.release(hash) }

// hold marks a blob as owing attempts while its reading writes its rows
// and queues them; release ends it.
func (q *retryQueue) hold(hash string) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.pending[hash]++
	q.owned[hash] = true
}

func (q *retryQueue) release(hash string) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.pending[hash]--; q.pending[hash] <= 0 {
		delete(q.pending, hash)
	}
}

// claim takes on a blob's owed attempts, and reports whether this is the
// first time: a reading an earlier run made.
func (q *retryQueue) claim(hash string) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.owned[hash] {
		return false
	}
	q.owned[hash] = true
	return true
}

// owes reports whether a blob has an attempt queued, waiting or under way.
func (q *retryQueue) owes(hash string) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.pending[hash] > 0
}

// forget drops a finished blob.
func (q *retryQueue) forget(hash string) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.pending[hash] == 0 {
		delete(q.owned, hash)
		delete(q.targets, hash)
	}
}

// counts is how many attempts are queued (not yet due), waiting for their
// validator, and being made.
func (q *retryQueue) counts() (queued, waiting, inFlight int) {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.h), q.waiting, q.inFlight
}

// idle reports that no attempt is queued, waiting or under way.
func (q *retryQueue) idle() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.h) == 0 && len(q.pending) == 0
}

type retryHeap []*retryJob

func (h retryHeap) Len() int           { return len(h) }
func (h retryHeap) Less(i, j int) bool { return h[i].wake().Before(h[j].wake()) }
func (h retryHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index, h[j].index = i, j
}
func (h *retryHeap) Push(x any) {
	j := x.(*retryJob)
	j.index = len(*h)
	*h = append(*h, j)
}
func (h *retryHeap) Pop() any {
	old := *h
	n := len(old)
	j := old[n-1]
	*h = old[:n-1]
	return j
}

// runRetries moves every queued attempt to its validator's lane when it is
// due and starts the lane's worker, and syncs the attempts' rows about once
// a second (AppendDeferredRows). A stopped run drops the queue: the record
// says what is owed.
func (p *Prober) runRetries(ctx context.Context) {
	var wg sync.WaitGroup
	defer func() {
		wg.Wait()
		p.syncAttempts()
	}()
	for {
		j, wait := p.retries.popDue(time.Now())
		if j != nil {
			if p.retries.enqueue(j) {
				v := j.target.AddressHex
				wg.Add(1)
				go func() {
					defer wg.Done()
					p.retryLane(ctx, v)
				}()
			}
			continue
		}
		p.syncAttempts()
		if wait <= 0 || wait > time.Second {
			wait = time.Second
		}
		select {
		case <-ctx.Done():
			return
		case <-p.retries.wakeC:
		case <-time.After(wait):
		}
	}
}

// syncAttempts makes the attempts' rows written since the last sync durable.
func (p *Prober) syncAttempts() {
	if err := p.store.Sync(); err != nil {
		p.log.Fatalf("sync attempts: %v", err)
	}
}

// retryLane makes the attempts due to one validator, one at a time, until
// its lane is empty.
func (p *Prober) retryLane(ctx context.Context, v string) {
	for {
		if ctx.Err() != nil {
			for _, j := range p.retries.drain(v) {
				p.retries.done(j.pub.PromiseHash)
			}
			return
		}
		j := p.retries.next(v)
		if j == nil {
			return
		}
		p.retryOne(ctx, j)
	}
}

// retryOne makes one attempt, gives its answer to every other attempt of
// the validator it speaks for (shareable), and records them.
func (p *Prober) retryOne(ctx context.Context, j *retryJob) {
	m, ok := p.attempt(ctx, j)
	if !ok {
		p.retries.done(j.pub.PromiseHash)
		return
	}
	jobs, rows := []*retryJob{j}, []Measurement{m}
	if shareable(m) {
		for _, o := range p.sharers(ctx, j, m) {
			jobs, rows = append(jobs, o), append(rows, p.sharedRow(o, m))
		}
	}
	p.recordAttempts(jobs, rows)
}

// retryAfterReading queues the next attempt of every validator of a full
// reading whose row says it is owed one (NextAttemptDue).
func (p *Prober) retryAfterReading(b *blobReading, ms []Measurement) {
	result, clientErr := "", ""
	if len(ms) > 0 && ms[0].Read != nil {
		result, clientErr = ms[0].Read.BlobResult, ms[0].Read.BlobError
	}
	byAddr := map[string]readTarget{}
	for _, t := range b.targets {
		byAddr[t.AddressHex] = t
	}
	for _, m := range ms {
		if !retryOpen(m) {
			continue
		}
		t := byAddr[m.ValidatorAddress]
		p.retries.push(&retryJob{pub: b.pub, point: b.point, target: t.Target, resolved: true, order: t.order,
			attempt: m.Attempt + 1, due: *m.NextAttemptDue, cutoff: b.startBy, result: result, clientErr: clientErr,
			shift: b.shiftOf(m)})
	}
}

// retriesOwed takes on the attempts a blob's full reading still owes, the
// first time a cycle sees its reading on record, and reports whether any is
// queued or under way: the blob is not finished until they are made or
// recorded as not made.
func (p *Prober) retriesOwed(pub scan.Publication, pt SchedulePoint) bool {
	if p.retries.claim(pub.PromiseHash) {
		p.recoverRetries(pub, pt)
	}
	return p.retries.owes(pub.PromiseHash)
}

// recoverRetries queues the attempts an earlier run owed on pub's full
// reading: those it made and could not follow, because it stopped. Each
// is made at the point of the reading it follows, as the record has it,
// and its validator is resolved again when it is made (resolveTarget), once
// for the blob.
func (p *Prober) recoverRetries(pub scan.Publication, pt SchedulePoint) {
	for _, mk := range p.store.PendingAttempts(pub.PromiseHash) {
		t := Target{AddressHex: mk.Validator, Host: mk.Host, HostAtSettlement: mk.HostAtSettlement, Assigned: mk.Assigned,
			Attested: mk.Attested, AttestationUnknown: mk.AttestationUnknown, RowCount: mk.RowCount}
		point := SchedulePoint{At: mk.ScheduledAt, Phase: pt.Phase, Label: FullReadLabel}
		p.retries.push(&retryJob{pub: pub, point: point, target: t, order: mk.Order, attempt: mk.Attempt + 1,
			due: mk.Due, cutoff: p.requestStartBy(pub), result: mk.BlobResult, clientErr: mk.BlobError, recovered: true})
	}
}

// recordAttempts writes the rows of these attempts together, without an
// fsync of their own (runRetries syncs), and queues each validator's next
// attempt when its row says one is owed.
func (p *Prober) recordAttempts(jobs []*retryJob, rows []Measurement) {
	shifts := make([]time.Duration, len(rows))
	for i := range rows {
		shifts[i] = jobs[i].shiftAfter(rows[i])
		rows[i].NextAttemptDue = p.nextAttemptDue(rows[i], jobs[i].cutoff, shifts[i])
	}
	if err := p.store.AppendDeferredRows(rows); err != nil {
		p.log.Fatalf("append attempt: %v", err)
	}
	now := time.Now()
	for i, m := range rows {
		j := jobs[i]
		p.logMeasurement(m)
		switch {
		case m.Outcome == OutcomeMissed:
			p.counters.retryNotMade(now, j.target.AddressHex)
		case m.SharedFrom != "":
			p.counters.retriesShared.add(now)
		default:
			p.counters.retriesMade.add(now)
		}
		if m.NextAttemptDue != nil {
			next := *j
			next.attempt, next.due, next.index, next.recovered = m.Attempt+1, *m.NextAttemptDue, 0, false
			next.shift = shifts[i]
			p.retries.push(&next)
		}
		p.retries.done(j.pub.PromiseHash)
	}
}

// shiftAfter is this observer's own delay before m, the answer of job j's
// attempt: the delay before the answer it follows, and how late its request
// started after it was due (its lane, this observer's limits, a restart).
func (j *retryJob) shiftAfter(m Measurement) time.Duration {
	return j.shift + max(0, m.StartedAt.Sub(j.due))
}

// nextAttemptDue is when the validator of a row of a full reading is to be
// asked again: RetrySpacing after the row's answer ended, for an answer
// that did not serve and is not an attempt that could not be made, while an
// attempt is left and it can start before cutoff, or could have but for
// shift, this observer's own delay before the answer (blobReading.shiftOf,
// retryJob.shiftAfter). An attempt only those delays push past the cutoff
// stays owed: it is recorded as not made (NOT_PROBED, this observer's gap)
// when its cutoff comes, never left out so that the answer before it is the
// validator's last. nil when none is owed.
func (p *Prober) nextAttemptDue(m Measurement, cutoff time.Time, shift time.Duration) *time.Time {
	if m.ScheduleLabel != FullReadLabel || m.Attempt >= FullReadRetries || m.Classification == ClassNotProbed || m.fullServed() {
		return nil
	}
	due := m.FinishedAt.Add(p.cfg.RetrySpacing)
	if !due.Add(-max(0, shift)).Before(cutoff) {
		return nil
	}
	return &due
}

// attempt makes job j's request, or the row saying it could not be made.
// ok is false when the run stopped first.
func (p *Prober) attempt(ctx context.Context, j *retryJob) (Measurement, bool) {
	notMade := func(reason string) (Measurement, bool) {
		m := p.notProbedRow(j.pub, j.point, j.target, reason)
		return j.stamp(m), true
	}
	if !time.Now().Before(j.cutoff) {
		why := "its validator's earlier attempt was still under way"
		switch {
		case j.recovered:
			why = "abandoned by a restart of this observer"
		case !j.due.Before(j.cutoff):
			why = "this observer's own delays before the earlier answer left no time before the cutoff"
		}
		return notMade(p.notStartedReason("retry") + ": " + why)
	}
	if !j.resolved {
		t, err := p.resolveTarget(ctx, j)
		if ctx.Err() != nil {
			return Measurement{}, false
		}
		if err != nil {
			return notMade("not read: the retry's validator could not be resolved: " + err.Error() + " (this observer's own gap)")
		}
		j.target, j.resolved = t, true
	}
	j.target = p.currentHost(ctx, j.pub, j.target)

	pp := j.pub.Assignment.ProtocolParams
	coder, err := p.coderFor(pp.OriginalRows, pp.TotalRows)
	if err != nil {
		return notMade("not read: no coder for this blob: " + err.Error() + " (this observer's own gap)")
	}
	var commitment [32]byte
	cb, err := hex.DecodeString(j.pub.Promise.Commitment)
	if err != nil || len(cb) != len(commitment) {
		return notMade(fmt.Sprintf("not read: commitment %q is not 32 hex bytes (this observer's own gap)", j.pub.Promise.Commitment))
	}
	copy(commitment[:], cb)
	// No verifier is passed: the request checks its rows against the
	// commitment with one of its own, let go when it ends, and charged to
	// the byte budget with its shard.
	in := p.inputFor(j.pub, j.target, j.point, commitment, nil, p.shadowBlindness(j.pub))
	// The verifier is charged to the byte budget, not to the reading-rate
	// ceiling: it is memory, not bytes on the wire. The budget is charged
	// the most the request may receive (and the rest of a wider bound when
	// it asks for an answer again under one: Prober.widen), the ceiling
	// what its shard should weigh (blobReading.ask).
	release, load, err := p.admitBy(ctx, j.cutoff, int64(recvLimitFor(in))+verifierBytes(pp.OriginalRows, pp.TotalRows), in.ExpectedShardBytes)
	if err != nil {
		if ctx.Err() != nil {
			return Measurement{}, false
		}
		return notMade(p.notStartedReason("retry") + ": " + err.Error())
	}
	defer release()
	p.retries.started()
	defer p.retries.ended()
	// One request: the attempt is itself the asking again, so it carries no
	// re-dial of its own.
	m := Run(ctx, in, coder, p.cfg.Timeouts)
	if ctx.Err() != nil {
		return Measurement{}, false
	}
	p.reach.note(m)
	m.ObserverLoad = load
	p.ownSide(ctx, &m)
	return j.stamp(m), true
}

// stamp marks a row as the job's attempt: its number, where the upload
// went, and the reading it follows.
func (j *retryJob) stamp(m Measurement) Measurement {
	m.Attempt = j.attempt
	m.HostAtSettlement = j.target.HostAtSettlement
	m.Read = &ReadInfo{Order: j.order, BlobResult: j.result, BlobError: j.clientErr}
	return m
}

// shareable reports an answer that is about the validator's endpoint, not
// about any blob: the validator's failure (not served, not this observer's
// gap) before its identity was verified, so before any blob could be asked
// for (DownloadShard rides the verified session): no such host, no host, a
// connect refused, timed out or unroutable, a handshake that failed or did
// not finish in time, a certificate not endorsed or out of its window.
func shareable(m Measurement) bool {
	return !m.Identity.OK && !m.fullServed() && !m.fullGap()
}

// sharers takes from j's validator's lane every attempt that m, the answer
// of j's request, also answers: due when the request started and still
// able to start then, its validator resolved, at the same host (and, for a
// certificate, under the same consensus key).
func (p *Prober) sharers(ctx context.Context, j *retryJob, m Measurement) []*retryJob {
	v := j.target.AddressHex
	var take []*retryJob
	for _, o := range p.retries.lane(v) {
		if !o.resolved || o.due.After(m.StartedAt) || !m.StartedAt.Before(o.cutoff) {
			continue
		}
		o.target = p.currentHost(ctx, o.pub, o.target)
		if o.target.Host != m.ValidatorHost {
			continue
		}
		if m.Outcome == OutcomeIdentityFail && !bytes.Equal(o.target.PubKey, j.target.PubKey) {
			continue
		}
		take = append(take, o)
	}
	p.retries.take(v, take)
	return take
}

// sharedRow is attempt o's row from the answer m of another attempt's
// request to the same endpoint (shareable): the endpoint's layers and
// outcome as they were, o's blob, point and attempt, and SharedFrom naming
// the request.
func (p *Prober) sharedRow(o *retryJob, m Measurement) Measurement {
	in := p.inputFor(o.pub, o.target, o.point, [32]byte{}, nil, "")
	s := newMeasurement(in, m.StartedAt)
	s.FinishedAt, s.TotalDurationMS = m.FinishedAt, m.TotalDurationMS
	s.DNS, s.TCP, s.TLS, s.Identity = m.DNS, m.TCP, m.TLS, m.Identity
	if d := m.Download; d.Attempted {
		// The call that never got its session: no rows, and o's own count.
		s.Download = DownloadResult{Attempted: true, DurationMS: d.DurationMS, RowsExpected: o.target.RowCount,
			RPCCode: d.RPCCode, RPC: d.RPC, Error: d.Error}
	}
	s.Outcome = m.Outcome
	s.SharedFrom = m.DedupeKey()
	s.RawError = clip(fmt.Sprintf("the validator's endpoint failed before any blob was asked for, on request %s for blob %s, made while this attempt was due and waiting for it: %s",
		s.SharedFrom, short(m.PromiseHash), m.RawError))
	s.ObserverLoad = m.ObserverLoad
	classifyRow(&s)
	return o.stamp(s)
}

// blobTargets is one blob's validators resolved once for the attempts an
// earlier run owed, shared by every one of them.
type blobTargets struct {
	done   chan struct{}
	at     time.Time
	byAddr map[string]Target
	err    error
}

// resolveFailureTTL is how long a blob's failed resolution answers its
// owed attempts before it is tried again.
const resolveFailureTTL = 30 * time.Second

// resolveTarget resolves the validator of an attempt an earlier run owed:
// the blob's targets once for every such attempt of it (not once each), at
// most four blobs at a time, so a restart that owes many attempts does not
// send the chain a burst of the same queries.
func (p *Prober) resolveTarget(ctx context.Context, j *retryJob) (Target, error) {
	q := p.retries
	hash := j.pub.PromiseHash
	q.mu.Lock()
	bt := q.targets[hash]
	leader := false
	// done is closed after the result is written, so the result is read
	// only once done is seen closed.
	if bt == nil || (isClosed(bt.done) && bt.err != nil && time.Since(bt.at) > resolveFailureTTL) {
		bt = &blobTargets{done: make(chan struct{})}
		q.targets[hash] = bt
		leader = true
	}
	q.mu.Unlock()
	if leader {
		func() {
			defer close(bt.done)
			select {
			case q.resolveSem <- struct{}{}:
			case <-ctx.Done():
				bt.err, bt.at = ctx.Err(), time.Now()
				return
			}
			defer func() { <-q.resolveSem }()
			ts, err := p.targetsFor(ctx, j.pub)
			bt.at = time.Now()
			if err != nil {
				bt.err = err
				return
			}
			bt.byAddr = make(map[string]Target, len(ts))
			for _, t := range ts {
				bt.byAddr[t.AddressHex] = t
			}
		}()
	} else {
		select {
		case <-bt.done:
		case <-ctx.Done():
			return Target{}, ctx.Err()
		}
	}
	if bt.err != nil {
		return Target{}, bt.err
	}
	t, ok := bt.byAddr[j.target.AddressHex]
	if !ok {
		return Target{}, fmt.Errorf("validator %s is not among the promise's targets", j.target.AddressHex)
	}
	if t.HostAtSettlement == "" {
		t.HostAtSettlement = j.target.HostAtSettlement
	}
	return t, nil
}

func isClosed(c chan struct{}) bool {
	select {
	case <-c:
		return true
	default:
		return false
	}
}

// currentHost is t with the host the registry gives its validator now:
// an attempt is made where a client would be sent at the time, not where
// the reading found it minutes before. The registry is the resolver's
// cached one (HostCacheTTL); t is kept as it is when there is no resolver
// or no registry to read.
func (p *Prober) currentHost(ctx context.Context, pub scan.Publication, t Target) Target {
	if p.resolver == nil || p.resolver.chain == nil {
		return t
	}
	hosts, err := p.resolver.hostMap(ctx)
	if err != nil {
		return t
	}
	host, source, at := p.resolver.hostFor(hosts, t.AddressHex)
	t.Host, t.HostSource, t.HostSeenAt = withSettlementFallback(host, source, at, t.HostAtSettlement, pub.SettlementTime)
	return t
}

// verifierBytes is what a request's own verifier holds while it checks the
// rows (an rsema1d Reconstructor for one request: the RLC shards and the
// decoder's work space, taken as four 64-byte words per row of the code,
// and a root per original row: about 4.1 MiB at K = 4096, N = 12288),
// charged to the byte budget beside the shard. A reading's requests share
// one Reconstructor and are not charged it (defaultInFlightBytes).
func verifierBytes(originalRows, totalRows int) int64 {
	if totalRows <= 0 {
		return 0
	}
	return int64(4*totalRows*64 + originalRows*32)
}
