package probe

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	assign "github.com/plsgiveup/fibre/fibre-assign"
)

// The second location asks several validators at once, one request per
// validator at a time, and caps each validator's requests in an hour on
// its own: a validator not served on every blob does not use up the hour
// of the others.
func TestConfirmerAsksValidatorsInParallelOneRequestEach(t *testing.T) {
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
	// Three requests for each of the three validators, one second apart.
	var reqs []ConfirmRequest
	for _, v := range vals {
		for i := 0; i < 3; i++ {
			r := fx.req
			r.ValidatorAddress, r.AssignedRows = v.Address.String(), sm[v.Address]
			r.ScheduledAt = r.ScheduledAt.Add(time.Duration(i) * time.Second)
			reqs = append(reqs, r)
		}
	}
	dir := t.TempDir()
	reqPath := filepath.Join(dir, "requests.jsonl")
	writeRequests(t, reqPath, reqs...)
	c := newTestConfirmer(t, dir, reqPath, fx.chain)
	c.cfg.MaxPerHour, c.cfg.Workers = 2, 3
	var (
		mu               sync.Mutex
		running, maxRun  int
		perVal, maxPer   = map[string]int{}, map[string]int{}
		probed           = map[string]int{}
		together         = make(chan struct{})
		togetherReleased sync.Once
	)
	c.run = func(_ context.Context, in Input, _ *Coder, _ StepTimeouts) Measurement {
		mu.Lock()
		running++
		perVal[in.Target.AddressHex]++
		probed[in.Target.AddressHex]++
		maxRun = max(maxRun, running)
		maxPer[in.Target.AddressHex] = max(maxPer[in.Target.AddressHex], perVal[in.Target.AddressHex])
		if running >= 2 {
			togetherReleased.Do(func() { close(together) })
		}
		mu.Unlock()
		// hold the request until a second one runs beside it, or a while
		select {
		case <-together:
		case <-time.After(2 * time.Second):
		}
		mu.Lock()
		running--
		perVal[in.Target.AddressHex]--
		mu.Unlock()
		return Measurement{SchemaVersion: MeasurementSchemaVersion, Vantage: in.Vantage, PromiseHash: in.PromiseHash,
			ValidatorAddress: in.Target.AddressHex, ScheduledAt: in.SchedulePoint.At, StartedAt: time.Now(),
			Outcome: OutcomeNotFound, Classification: ClassFault}
	}
	if err := c.pass(context.Background()); err != nil {
		t.Fatal(err)
	}
	if maxRun < 2 {
		t.Errorf("at most %d request at once, want validators asked in parallel", maxRun)
	}
	for v, n := range maxPer {
		if n != 1 {
			t.Errorf("validator %s had %d requests at once, want 1", v[:8], n)
		}
	}
	for _, v := range vals {
		if n := probed[v.Address.String()]; n != 2 {
			t.Errorf("validator %s asked %d times, want its cap of 2", v.Address.String()[:8], n)
		}
	}
	if c.pendingLen() != 3 {
		t.Errorf("%d pending, want each validator's third request waiting", c.pendingLen())
	}
	ms, _ := LoadMeasurements(filepath.Join(dir, "measurements.jsonl"))
	if len(ms) != 6 {
		t.Errorf("%d answers written, want 6", len(ms))
	}
}
