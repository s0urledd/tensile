package probe

// The later attempts of a full reading.
//
// A validator whose answer in a full reading did not serve (FullServed) is
// asked again, up to FullReadRetries more times, each RetrySpacing after
// its last answer ended, while the attempt can still start before
// must_serve_until less RequestStartMargin. A later answer that serves
// makes it served; every attempt is its own row (Measurement.Attempt), so
// the record keeps each failure and the reason shown is the last one.
//
// The attempts live apart from the reading. The reading writes its rows
// and gives back its blob slot (BlobConcurrency) and its Reconstructor when
// its first pass ends, as any reading does; an attempt holds neither. It
// is one request, made under this observer's request and byte limits
// (admitBy), whose rows are verified against the commitment on their own
// (a Reconstructor of that request alone, let go when it ends), and it
// appends its own row. At most one attempt is in flight to a validator at
// a time (retryQueue.slot): a validator busy with uploads is not asked
// more while it is busy.
//
// An attempt that cannot start in time (its validator's earlier attempt
// still running, this observer's limits full, or the time already gone) is
// not made, and its row says so: NOT_PROBED, this observer's gap, never
// the validator's. So is one a restart abandoned: the queue of attempts is
// not persisted, and a restarted prober finds the attempts its record
// still owes (MeasurementStore.PendingAttempts) and makes them, or records
// them as not made once their time is gone (recoverRetries).

import (
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
	due      time.Time // RetrySpacing after the last answer ended
	cutoff   time.Time // the last moment the attempt may start
	// result and clientErr are what the reading's first pass came to,
	// repeated on the attempt's row.
	result, clientErr string
	// recovered: the attempt was owed by a reading an earlier run made.
	recovered bool
	index     int
}

// wake is when the job is looked at: when it is due, or at its cutoff if
// that comes first, so an attempt that cannot be made is recorded before
// the window closes.
func (j *retryJob) wake() time.Time {
	if j.cutoff.Before(j.due) {
		return j.cutoff
	}
	return j.due
}

// retryQueue holds the attempts waiting to be made, earliest first, and
// which blobs still owe one.
type retryQueue struct {
	mu sync.Mutex
	h  retryHeap
	// pending counts, per blob, the attempts queued or under way and the
	// readings writing their rows (hold); owned is every blob whose owed
	// attempts this run has taken on, so a restart's are recovered once.
	pending map[string]int
	owned   map[string]bool
	// slots allow one attempt in flight per validator.
	slots map[string]chan struct{}
	wakeC chan struct{}
}

func newRetryQueue() *retryQueue {
	return &retryQueue{pending: map[string]int{}, owned: map[string]bool{}, slots: map[string]chan struct{}{},
		wakeC: make(chan struct{}, 1)}
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

// pop returns the job whose wake time has come first, or how long until
// the earliest one's.
func (q *retryQueue) pop(now time.Time) (*retryJob, time.Duration) {
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

// done ends a popped job.
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

// owes reports whether a blob has an attempt queued or under way.
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
	}
}

// queued is how many attempts wait to be made.
func (q *retryQueue) queued() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.h)
}

// idle reports that no attempt is queued or under way.
func (q *retryQueue) idle() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.h) == 0 && len(q.pending) == 0
}

// slot is the one attempt in flight a validator may have.
func (q *retryQueue) slot(validator string) chan struct{} {
	q.mu.Lock()
	defer q.mu.Unlock()
	s, ok := q.slots[validator]
	if !ok {
		s = make(chan struct{}, 1)
		q.slots[validator] = s
	}
	return s
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

// runRetries makes every queued attempt when it is due, each in its own
// goroutine; the limits it runs under are admitBy's and the validator's
// slot. A stopped run drops the queue: the record says what is owed.
func (p *Prober) runRetries(ctx context.Context) {
	var wg sync.WaitGroup
	defer wg.Wait()
	for {
		j, wait := p.retries.pop(time.Now())
		if j != nil {
			wg.Add(1)
			go func(j *retryJob) {
				defer wg.Done()
				p.retry(ctx, j)
			}(j)
			continue
		}
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

// retryAfterReading queues the next attempt of every validator of a full
// reading whose answer did not serve.
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
			attempt: m.Attempt + 1, due: m.FinishedAt.Add(p.cfg.RetrySpacing), cutoff: b.startBy,
			result: result, clientErr: clientErr})
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
// reading: those it made and could not follow, because it stopped.
func (p *Prober) recoverRetries(pub scan.Publication, pt SchedulePoint) {
	for _, mk := range p.store.PendingAttempts(pub.PromiseHash) {
		if !mk.ScheduledAt.Equal(pt.At) {
			continue
		}
		t := Target{AddressHex: mk.Validator, Host: mk.Host, HostAtSettlement: mk.HostAtSettlement, Assigned: mk.Assigned,
			Attested: mk.Attested, AttestationUnknown: mk.AttestationUnknown, RowCount: mk.RowCount}
		point := pt
		point.Label = FullReadLabel
		p.retries.push(&retryJob{pub: pub, point: point, target: t, order: mk.Order, attempt: mk.Attempt + 1,
			due: mk.FinishedAt.Add(p.cfg.RetrySpacing), cutoff: p.requestStartBy(pub),
			result: mk.BlobResult, clientErr: mk.BlobError, recovered: true})
	}
}

// retry makes one attempt and records it, and queues the next one when it
// did not serve. A stopped run records nothing: the restart owes it.
func (p *Prober) retry(ctx context.Context, j *retryJob) {
	defer p.retries.done(j.pub.PromiseHash)
	m, ok := p.attempt(ctx, j)
	if !ok {
		return
	}
	if err := p.store.AppendReading([]Measurement{m}); err != nil {
		p.log.Fatalf("append attempt: %v", err)
	}
	p.logMeasurement(m)
	if !retryOpen(m) {
		return
	}
	next := *j
	next.attempt, next.due, next.index = j.attempt+1, m.FinishedAt.Add(p.cfg.RetrySpacing), 0
	p.retries.push(&next)
}

// attempt makes job j's request, or the row saying it could not be made.
// ok is false when the run stopped first.
func (p *Prober) attempt(ctx context.Context, j *retryJob) (Measurement, bool) {
	notMade := func(reason string) (Measurement, bool) {
		m := p.notProbedRow(j.pub, j.point, j.target, reason)
		return j.stamp(m), true
	}
	late := p.notStartedReason("retry")
	if j.recovered {
		late = "abandoned by a restart of this observer; " + late
	}
	if !time.Now().Before(j.cutoff) {
		return notMade(late)
	}
	if !j.resolved {
		t, err := p.retryTarget(ctx, j)
		if ctx.Err() != nil {
			return Measurement{}, false
		}
		if err != nil {
			return notMade("not read: the retry's validator could not be resolved: " + err.Error() + " (this observer's own gap)")
		}
		j.target, j.resolved = t, true
	}
	// One attempt in flight per validator, waited for until the cutoff.
	slot := p.retries.slot(j.target.AddressHex)
	wait := time.NewTimer(time.Until(j.cutoff))
	select {
	case slot <- struct{}{}:
		wait.Stop()
	case <-ctx.Done():
		wait.Stop()
		return Measurement{}, false
	case <-wait.C:
		return notMade(late)
	}
	defer func() { <-slot }()

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
	// commitment with one of its own, let go when it ends.
	in := p.inputFor(j.pub, j.target, j.point, commitment, nil, p.shadowBlindness(j.pub))
	release, ok := p.admitBy(ctx, j.cutoff, in.ExpectedShardBytes)
	if !ok {
		if ctx.Err() != nil {
			return Measurement{}, false
		}
		return notMade(late)
	}
	defer release()
	m := Run(ctx, in, coder, p.cfg.Timeouts)
	var first *Measurement
	if redials(m) && ctx.Err() == nil && time.Now().Before(j.cutoff) {
		f := m
		first = &f
		m = Run(ctx, in, coder, p.cfg.Timeouts)
	}
	if ctx.Err() != nil {
		return Measurement{}, false
	}
	if first != nil {
		m.Retry = &RetryInfo{Attempts: 2, DelayMS: m.StartedAt.Sub(first.StartedAt).Milliseconds(),
			FirstStartedAt: first.StartedAt, FirstOutcome: first.Outcome, FirstError: first.RawError, FirstDurationMS: first.TotalDurationMS}
	}
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

// retryTarget resolves the validator of an attempt an earlier run owed.
func (p *Prober) retryTarget(ctx context.Context, j *retryJob) (Target, error) {
	targets, err := p.targetsFor(ctx, j.pub)
	if err != nil {
		return Target{}, err
	}
	for _, t := range targets {
		if t.AddressHex == j.target.AddressHex {
			if t.HostAtSettlement == "" {
				t.HostAtSettlement = j.target.HostAtSettlement
			}
			return t, nil
		}
	}
	return Target{}, fmt.Errorf("validator %s is not among the promise's targets", j.target.AddressHex)
}
