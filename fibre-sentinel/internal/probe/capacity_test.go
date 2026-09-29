package probe

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	fibretypes "github.com/celestiaorg/celestia-app/v10/x/fibre/types"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// fakeCancelled answers with a CANCELLED status of the server's own, while
// the request is still alive on the client's side.
func fakeCancelled(context.Context, int, *fibretypes.BlobShard) (*fibretypes.DownloadShardResponse, error) {
	return nil, status.Error(codes.Canceled, "cancelled by the server")
}

// Regression, on mocha's schedule scaled by 1/5: thirty blobs 600 ms apart
// (20 a minute), each of them Unavailable, with one validator that never
// answers and holds each request for two RPC timeouts (the request and the
// client's re-dial). Every reading asks it, as the client would, runs
// beside the others, and ends in time: Unavailable with the client's
// error, the hanging validator's row its own timeout.
func TestAnUnavailableBlobIsReadWhileAValidatorTimesOut(t *testing.T) {
	const rpcTimeout = 3 * time.Second
	vals := []fakeVal{
		{rows: rowsOf(0, 2), serve: fakeHangs},
		{rows: rowsOf(1, 2), serve: fakeNotFound},
		{rows: rowsOf(2, 2), serve: fakeNotFound},
	}
	for i := 0; i < 5; i++ {
		vals = append(vals, fakeVal{rows: []int{6 + i}, serve: fakeServes})
	}
	f := newReadFixture(t, 8, 32, vals)
	const n = 30
	var fs []*readFixture
	for i := 0; i < n; i++ {
		c := *f
		c.pub.PromiseHash = fmt.Sprintf("%02x", i) + strings.Repeat("cd", 31)
		fs = append(fs, &c)
	}
	p := readProber(t, fs...)
	p.cfg.Timeouts.Download = rpcTimeout
	scale := func(d time.Duration) time.Duration { return d / 5 }
	p.cfg.Schedule = ScheduleConfig{EndReadOffset: scale(10 * time.Minute), ReadDeadline: scale(3 * time.Minute), PruneTolerance: 150 * time.Second}
	cfg := p.schedCfg()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); p.dispatch(ctx) }()
	t0 := time.Now().Add(50 * time.Millisecond)
	for i, c := range fs {
		st := t0.Add(time.Duration(i) * 600 * time.Millisecond)
		c.pub.MustServeUntil = st.Add(cfg.EndReadOffset)
		c.pub.SettlementTime = st.Add(-time.Hour)
		p.sched.push(&readJob{pub: c.pub, point: SchedulePoint{At: st, Phase: PhaseInWindow, Label: EndReadLabel}, start: st,
			latest: c.pub.MustServeUntil.Add(-cfg.ReadDeadline)})
	}
	for deadline := time.Now().Add(3 * time.Minute); !p.sched.idle(); {
		if time.Now().After(deadline) {
			cancel()
			<-done
			t.Fatal("the readings did not finish")
		}
		time.Sleep(50 * time.Millisecond)
	}
	cancel()
	<-done
	ms, err := LoadMeasurements(p.store.Path())
	if err != nil {
		t.Fatal(err)
	}
	if len(ms) != n*len(vals) {
		t.Fatalf("%d rows, want every validator of every blob", len(ms))
	}
	hung := f.targets[0].AddressHex
	tally := map[string]int{}
	for _, m := range ms {
		if m.Read == nil || m.Classification == ClassNotProbed || m.Classification == ClassProbeError {
			t.Fatalf("%s at %s: %s / %s (%s): %s", m.ValidatorAddress[:4], m.PromiseHash[:4], m.Outcome, m.Classification, m.ClassificationReason, m.RawError)
		}
		if m.ValidatorAddress == hung {
			if m.Outcome != OutcomeRPCTimeout || m.Classification != ClassUnreachable || m.Retry == nil {
				t.Errorf("the hanging validator at %s: %s / %s, retry %+v", m.PromiseHash[:4], m.Outcome, m.Classification, m.Retry)
			}
			tally[m.Read.BlobResult+" "+m.Read.BlobError]++
		}
	}
	if tally[ReadUnavailable+" "+ClientErrNotEnoughShards] != n {
		t.Fatalf("readings: %v, want all %d Unavailable (not enough shards)", tally, n)
	}
}

// Of the readings due, the one whose last start comes first starts first.
func TestTheMostUrgentReadingStartsFirst(t *testing.T) {
	q := newReadQueue()
	now := time.Now()
	first := &readJob{start: now.Add(-2 * time.Second), latest: now.Add(3 * time.Minute)}
	first.pub.PromiseHash = "first"
	urgent := &readJob{start: now.Add(-time.Second), latest: now.Add(40 * time.Second)}
	urgent.pub.PromiseHash = "urgent"
	later := &readJob{start: now.Add(time.Hour), latest: now.Add(time.Second)}
	later.pub.PromiseHash = "later"
	q.push(first)
	q.push(urgent)
	q.push(later)
	for _, want := range []string{"urgent", "first"} {
		j, _ := q.popDue(now)
		if j == nil || j.pub.PromiseHash != want {
			got := "nothing"
			if j != nil {
				got = j.pub.PromiseHash
			}
			t.Fatalf("popped %s, want %s", got, want)
		}
	}
	if j, wait := q.popDue(now); j != nil || wait <= 0 {
		t.Fatalf("popped a reading not yet due (wait %s)", wait)
	}
}
