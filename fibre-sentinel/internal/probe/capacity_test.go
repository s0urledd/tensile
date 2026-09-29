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

// A CANCELLED status the server sends is its error, as celestia-app's
// client takes it (a failed shard, skipped): the validator was reached and
// did not hand over its rows. It is never this observer's gap, which would
// leave its rows as ones that might have served, turn an Unavailable blob
// into an incomplete reading and every not-found endorser beside it into
// this observer's gap too.
func TestACancelledAnswerIsTheServers(t *testing.T) {
	vals := []fakeVal{
		{rows: rowsOf(0, 4), serve: fakeNotFound},
		{rows: rowsOf(1, 4), serve: fakeCancelled},
	}
	for i := 0; i < 4; i++ {
		vals = append(vals, fakeVal{rows: []int{8 + i}, serve: fakeServes})
	}
	f := newReadFixture(t, 8, 16, vals)
	p := readProber(t, f)
	ms := byValidator(readNow(t, p, f.pub))
	nf, cc := ms[f.targets[0].AddressHex], ms[f.targets[1].AddressHex]
	if cc.Outcome != OutcomeServerError || cc.Classification != ClassServerError || cc.Read.BlobResult != ReadUnavailable {
		t.Fatalf("the CANCELLED validator: %s / %s (%s), read %+v; want SERVER_ERROR on an Unavailable reading", cc.Outcome, cc.Classification, cc.RawError, cc.Read)
	}
	if nf.Classification != ClassFault || nf.Read.BlobResult != ReadUnavailable {
		t.Fatalf("the not-found endorser: %s (%s), want FAULT on an Unavailable reading", nf.Classification, nf.ClassificationReason)
	}
	if got := readRequests(t, ConfirmRequestsPath(p.cfg.DataDir)); len(got) != 2 {
		t.Fatalf("%d confirmation requests, want both not-served endorsers", len(got))
	}
}

// Regression: an Unavailable blob is recorded Unavailable while a validator
// times out and blobs arrive at mocha's rate. The schedule is mocha's scaled
// by 0.2 (a 3 s request, a blob every 600 ms: 20 a minute). One validator
// hangs; with one request per validator at a time it can take one reading's
// request in each 6 s (the request and the client's re-dial), so most
// readings find it busy. Its rows could not make any blob whole, so a pass
// does not wait for it, and every blob is read Unavailable, with the hung
// validator this observer's gap wherever the deciding pass did not ask it.
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
	p.cfg.Schedule = ScheduleConfig{
		EndReadOffset: scale(10 * time.Minute), ReadDeadline: scale(3 * time.Minute),
		RetryAfter: scale(time.Minute), RetryDeadline: scale(90 * time.Second), RequestCutoff: scale(time.Minute),
		PruneTolerance: 150 * time.Second,
	}
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
	result := map[string]string{}
	hung := f.targets[0].AddressHex
	for _, m := range ms {
		if m.Read == nil {
			t.Fatalf("a row without its reading: %s %s (%s)", m.PromiseHash[:4], m.Classification, m.ClassificationReason)
		}
		result[m.PromiseHash] = m.Read.BlobResult
		if m.ValidatorAddress == hung && m.Classification != ClassUnreachable &&
			!(m.Classification == ClassProbeError && strings.Contains(m.ClassificationReason, "not asked again")) {
			t.Errorf("the hung validator at %s: %s (%s)", m.PromiseHash[:4], m.Classification, m.ClassificationReason)
		}
	}
	tally := map[string]int{}
	for _, c := range fs {
		tally[result[c.pub.PromiseHash]]++
	}
	if tally[ReadUnavailable] != n {
		t.Fatalf("readings: %v, want all %d Unavailable", tally, n)
	}
	// Every not-found endorser is sent for confirmation; the hung validator
	// only where both passes asked it.
	reqs := readRequests(t, ConfirmRequestsPath(p.cfg.DataDir))
	per := map[string]int{}
	for _, r := range reqs {
		per[r.ValidatorAddress]++
	}
	if per[f.targets[1].AddressHex] != n || per[f.targets[2].AddressHex] != n {
		t.Errorf("requests per validator: %v, want %d for each not-found endorser", per, n)
	}
	for _, m := range ms {
		if m.ValidatorAddress == hung && m.Classification == ClassProbeError {
			for _, r := range reqs {
				if r.PromiseHash == m.PromiseHash && r.ValidatorAddress == hung {
					t.Errorf("%s: a request for the hung validator the deciding pass did not ask", m.PromiseHash[:4])
				}
			}
		}
	}
}

// Of the readings due, the one whose last start comes first starts first:
// a second pass before a first pass that can still wait.
func TestTheMostUrgentReadingStartsFirst(t *testing.T) {
	q := newReadQueue()
	now := time.Now()
	first := &readJob{start: now.Add(-2 * time.Second), latest: now.Add(3 * time.Minute)}
	first.pub.PromiseHash = "first"
	second := &readJob{start: now.Add(-time.Second), latest: now.Add(40 * time.Second), second: &blobReading{}}
	second.pub.PromiseHash = "second"
	later := &readJob{start: now.Add(time.Hour), latest: now.Add(time.Second)}
	later.pub.PromiseHash = "later"
	q.push(first)
	q.push(second)
	q.push(later)
	for _, want := range []string{"second", "first"} {
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
