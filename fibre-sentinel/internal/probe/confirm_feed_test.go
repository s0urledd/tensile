package probe

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	assign "github.com/plsgiveup/fibre/fibre-assign"
)

// Regression: a request that arrives while a pass is draining one hanging
// validator's backlog is asked at once by an idle worker, not after the
// backlog. Scaled 1/100: validator H takes 300 ms a request (30 s, its
// request and the client's re-dial), six of its requests are queued when
// the pass starts, and 100 ms in a request for validator X arrives with
// 900 ms left (about nine minutes, what an end reading leaves). Eight
// workers, seven of them idle. A pass that read the requests file only at
// its start left X waiting behind H until it lapsed.
func TestARequestArrivingMidPassIsNotHeldBehindAHangingValidator(t *testing.T) {
	fx := newConfirmFixture(t, "203.0.113.9:7980")
	var vals []assign.Validator
	for _, m := range fx.chain.set {
		var a assign.Address
		copy(a[:], m.Address)
		vals = append(vals, assign.Validator{Address: a, VotingPower: m.VotingPower})
	}
	var commitment [32]byte
	commitment[0] = 7
	sm, err := assign.Assign(commitment, vals, assign.ProtocolParams{OriginalRows: 4, TotalRows: 8, MinRowsPerValidator: 1,
		LivenessThreshold: assign.Fraction{Numerator: 1, Denominator: 3}})
	if err != nil {
		t.Fatal(err)
	}
	h, x := vals[0], vals[1]
	dir := t.TempDir()
	reqPath := filepath.Join(dir, "requests.jsonl")
	start := time.Now()
	var reqs []ConfirmRequest
	for i := 0; i < 6; i++ {
		r := fx.req
		r.ValidatorAddress, r.AssignedRows = h.Address.String(), sm[h.Address]
		r.ScheduledAt = r.ScheduledAt.Add(time.Duration(i) * time.Second)
		r.Deadline = start.Add(10 * time.Second)
		reqs = append(reqs, r)
	}
	writeRequests(t, reqPath, reqs...)
	c := newTestConfirmer(t, dir, reqPath, fx.chain)
	c.cfg.PollEvery, c.cfg.Workers = 20*time.Millisecond, 8
	var mu sync.Mutex
	xAt := time.Duration(-1)
	var hDone, hAtOnce, hNow int
	var hLast time.Duration
	c.run = func(_ context.Context, in Input, _ *Coder, _ StepTimeouts) Measurement {
		if in.Target.AddressHex == h.Address.String() {
			mu.Lock()
			hNow++
			hAtOnce = max(hAtOnce, hNow)
			mu.Unlock()
			time.Sleep(300 * time.Millisecond)
			mu.Lock()
			hNow--
			hDone++
			hLast = time.Since(start)
			mu.Unlock()
		} else {
			mu.Lock()
			xAt = time.Since(start)
			mu.Unlock()
		}
		return Measurement{SchemaVersion: MeasurementSchemaVersion, Vantage: in.Vantage, PromiseHash: in.PromiseHash,
			ValidatorAddress: in.Target.AddressHex, ScheduledAt: in.SchedulePoint.At, StartedAt: time.Now(),
			Outcome: OutcomeNotFound, Classification: ClassFault}
	}
	go func() {
		time.Sleep(100 * time.Millisecond)
		r := fx.req
		r.ValidatorAddress, r.AssignedRows = x.Address.String(), sm[x.Address]
		r.PromiseHash = "otherblob"
		r.Deadline = time.Now().Add(900 * time.Millisecond)
		writeRequests(t, reqPath, r)
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := c.pass(ctx); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if xAt < 0 {
		t.Fatalf("X's request (arrived at 100 ms, deadline 1 s) was never asked: it lapsed behind H's backlog")
	}
	if xAt >= hLast {
		t.Errorf("X asked at %v, after H's backlog ended at %v", xAt, hLast)
	}
	if hDone != 6 || hAtOnce != 1 {
		t.Errorf("H: %d requests answered, at most %d at once; want all 6, one at a time", hDone, hAtOnce)
	}
	if c.pendingLen() != 0 {
		t.Errorf("%d requests still pending after the pass", c.pendingLen())
	}
	t.Logf("X asked at %v; H's six answered by %v", xAt, hLast)
}
