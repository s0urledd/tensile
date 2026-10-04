package probe

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	fibretypes "github.com/celestiaorg/celestia-app/v10/x/fibre/types"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/scan"
)

// readFull reads each blob as a full reading (AskEveryEndorser), with the
// dispatcher and the retry runner both running, until every reading and
// every later attempt is written, and returns the rows.
func readFull(t *testing.T, p *Prober, pubs ...scan.Publication) []Measurement {
	t.Helper()
	p.cfg.AskEveryEndorser = true
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		p.dispatch(ctx)
	}()
	go func() {
		defer wg.Done()
		p.runRetries(ctx)
	}()
	cfg := p.schedCfg()
	now := time.Now()
	for _, pb := range pubs {
		p.sched.push(&readJob{pub: pb, point: p.readPoint(pb), start: now, latest: pb.MustServeUntil.Add(-cfg.ReadDeadline)})
	}
	waitFor(t, 90*time.Second, func() bool { return p.sched.idle() && p.retries.idle() })
	cancel()
	wg.Wait()
	ms, err := LoadMeasurements(p.store.Path())
	if err != nil {
		t.Fatal(err)
	}
	return ms
}

// waitFor polls cond until it holds, failing the test after d.
func waitFor(t *testing.T, d time.Duration, cond func() bool) {
	t.Helper()
	for deadline := time.Now().Add(d); !cond(); {
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// attemptsOf is each validator's rows, in the order they were written.
func attemptsOf(ms []Measurement) map[string][]Measurement {
	out := map[string][]Measurement{}
	for _, m := range ms {
		out[m.ValidatorAddress] = append(out[m.ValidatorAddress], m)
	}
	return out
}

// A full reading is labelled full, from the planner's point to every row,
// and a validator that serves is asked once: its one row is the reading's
// own attempt, keyed as every row before attempts existed.
func TestAFullReadingIsLabelledFull(t *testing.T) {
	var vals []fakeVal
	for i := 0; i < 4; i++ {
		vals = append(vals, fakeVal{rows: rowsOf(i, 2), serve: fakeServes})
	}
	f := newReadFixture(t, 4, 16, vals)
	p := readProber(t, f)
	p.cfg.AskEveryEndorser = true
	if pt := p.readPoint(f.pub); pt.Label != FullReadLabel || !pt.At.Equal(ReadPoint(f.pub, p.schedCfg()).At) {
		t.Fatalf("planned point %+v, want the end point labelled full", pt)
	}
	ms := readFull(t, p, f.pub)
	if len(ms) != 4 {
		t.Fatalf("%d rows, want one per endorser", len(ms))
	}
	for i, m := range ms {
		if m.ScheduleLabel != FullReadLabel || m.Attempt != 0 || !m.Download.CommitmentVerified {
			t.Fatalf("row %d: label %q attempt %d %s", i, m.ScheduleLabel, m.Attempt, m.Outcome)
		}
		if k := m.DedupeKey(); strings.Count(k, "|") != 3 || AttemptOfKey(k) != 0 {
			t.Fatalf("row %d: key %q", i, k)
		}
	}
	for i, n := range f.calls {
		if n.Load() != 1 {
			t.Fatalf("validator %d asked %d times, want once", i, n.Load())
		}
	}
}

// A validator whose answer did not serve is asked again, each attempt a row
// of its own at the reading's point: one that answered NOT_FOUND and then
// served has two rows, the second served; one rate-limited twice and then
// served has three; one that answers NOT_FOUND every time is asked three
// times in all, and no more; one that served at once is asked once.
func TestAValidatorThatDidNotServeIsAskedAgain(t *testing.T) {
	f := newReadFixture(t, 4, 16, []fakeVal{
		{rows: rowsOf(0, 2), serve: fakeServes},
		{rows: rowsOf(1, 2), serve: firstThen(1, fakeNotFound, fakeServes)},
		{rows: rowsOf(2, 2), serve: firstThen(2, fakeThrottled, fakeServes)},
		{rows: rowsOf(3, 2), serve: fakeNotFound},
	})
	p := readProber(t, f)
	p.cfg.RetrySpacing = 50 * time.Millisecond
	ms := readFull(t, p, f.pub)
	by := attemptsOf(ms)
	want := []struct {
		calls int32
		outs  []Outcome
	}{
		{1, []Outcome{OutcomeServedOK}},
		{2, []Outcome{OutcomeNotFound, OutcomeServedOK}},
		{3, []Outcome{OutcomeThrottled, OutcomeThrottled, OutcomeServedOK}},
		{3, []Outcome{OutcomeNotFound, OutcomeNotFound, OutcomeNotFound}},
	}
	first := ms[0].Read.BlobResult
	keys := map[string]bool{}
	for i, w := range want {
		tg := f.targets[i]
		if n := f.calls[i].Load(); n != w.calls {
			t.Errorf("validator %d asked %d times, want %d", i, n, w.calls)
		}
		rows := by[tg.AddressHex]
		if len(rows) != len(w.outs) {
			t.Fatalf("validator %d: %d rows, want %d", i, len(rows), len(w.outs))
		}
		for k, m := range rows {
			if m.Attempt != k || m.Outcome != w.outs[k] || m.ScheduleLabel != FullReadLabel || !m.ScheduledAt.Equal(rows[0].ScheduledAt) {
				t.Fatalf("validator %d attempt %d: attempt %d %s label %q at %s", i, k, m.Attempt, m.Outcome, m.ScheduleLabel, m.ScheduledAt)
			}
			if m.Read == nil || m.Read.BlobResult != first || m.Read.Order != rows[0].Read.Order {
				t.Fatalf("validator %d attempt %d: read %+v, want the reading's", i, k, m.Read)
			}
			if k > 0 && m.StartedAt.Sub(rows[k-1].FinishedAt) < p.cfg.RetrySpacing {
				t.Fatalf("validator %d attempt %d started %s after the last ended, want at least %s", i, k,
					m.StartedAt.Sub(rows[k-1].FinishedAt), p.cfg.RetrySpacing)
			}
			if m.Phase != PhaseInWindow {
				t.Fatalf("validator %d attempt %d: phase %s", i, k, m.Phase)
			}
			key := m.DedupeKey()
			if keys[key] || AttemptOfKey(key) != k {
				t.Fatalf("validator %d attempt %d: key %q", i, k, key)
			}
			keys[key] = true
		}
	}
	if s := p.store.PendingAttempts(f.pub.PromiseHash); len(s) != 0 {
		t.Fatalf("attempts still owed after the reading: %+v", s)
	}
}

// A later attempt that could not start before must_serve_until less the
// margin is not owed: the rule asks again only while the retry can start
// in time. The answer before it carries no next attempt, no job is queued
// and no row is written for it, so that answer is the validator's last.
func TestALaterAttemptThatCannotStartInTimeIsNotOwed(t *testing.T) {
	f := newReadFixture(t, 4, 16, []fakeVal{
		{rows: rowsOf(0, 2), serve: fakeServes},
		{rows: rowsOf(1, 2), serve: fakeNotFound},
	})
	p := readProber(t, f)
	p.cfg.RequestStartMargin = time.Until(f.pub.MustServeUntil) - 1500*time.Millisecond
	p.cfg.RetrySpacing = 4 * time.Second
	ms := readFull(t, p, f.pub)
	rows := attemptsOf(ms)[f.targets[1].AddressHex]
	if len(rows) != 1 {
		t.Fatalf("%d rows for the validator that did not serve, want its one answer: no attempt was owed", len(rows))
	}
	if m := rows[0]; m.NextAttemptDue != nil || m.Outcome != OutcomeNotFound {
		t.Fatalf("its answer: %s, next attempt due %v, want none", m.Outcome, m.NextAttemptDue)
	}
	if n := f.calls[1].Load(); n != 1 {
		t.Fatalf("the validator was asked %d times, want once", n)
	}
	if s := p.store.PendingAttempts(f.pub.PromiseHash); len(s) != 0 {
		t.Fatalf("attempts owed: %+v", s)
	}
}

// An attempt that was owed and could not start in time is not made, and
// its row says so (NOT_PROBED, this observer's gap), written before the
// window closes: here two blobs owe the same validator an attempt, one in
// flight to it at a time, and the first runs out its whole time past the
// second's cutoff.
func TestAnOwedAttemptThatCannotStartInTimeIsNotProbed(t *testing.T) {
	serve := func(ctx context.Context, call int, honest *fibretypes.BlobShard) (*fibretypes.DownloadShardResponse, error) {
		if call == 3 { // the first later attempt: it hangs
			return fakeHangs(ctx, call, honest)
		}
		return fakeNotFound(ctx, call, honest)
	}
	f := newReadFixture(t, 4, 8, []fakeVal{{rows: []int{0, 1, 2, 3}, serve: serve}})
	g := *f
	g.pub.PromiseHash = strings.Repeat("e4", 32)
	p := readProber(t, f, &g)
	p.cfg.Timeouts.Download = 5 * time.Second
	p.cfg.RetrySpacing = 300 * time.Millisecond
	p.cfg.RequestStartMargin = time.Until(f.pub.MustServeUntil) - 2500*time.Millisecond
	ms := readFull(t, p, f.pub, g.pub)
	var made, notMade int
	for _, m := range ms {
		if m.Attempt == 0 {
			if m.Outcome != OutcomeNotFound || m.NextAttemptDue == nil {
				t.Fatalf("the reading's own answer: %s, next due %v", m.Outcome, m.NextAttemptDue)
			}
			continue
		}
		switch {
		case m.Attempt == 1 && m.Outcome == OutcomeRPCTimeout && m.NextAttemptDue == nil:
			made++
		case m.Attempt == 1 && m.Classification == ClassNotProbed &&
			strings.Contains(m.ClassificationReason, "earlier attempt was still under way") && m.StartedAt.Before(f.pub.MustServeUntil):
			notMade++
		default:
			t.Fatalf("attempt %d: %s / %s %q", m.Attempt, m.Outcome, m.Classification, m.ClassificationReason)
		}
	}
	if made != 1 || notMade != 1 {
		t.Fatalf("%d attempts made and %d not made, want one of each", made, notMade)
	}
	if n := f.calls[0].Load(); n != 3 {
		t.Fatalf("the validator was asked %d times, want 3", n)
	}
}

// A request of a full reading that cannot start before must_serve_until
// less the margin is not made: with room for one request, the slow
// validator asked first holds it past the cutoff, and the other one's row
// says its request could not start (NOT_PROBED); it is never asked.
func TestARequestThatCannotStartInTimeIsNotProbed(t *testing.T) {
	f := newReadFixture(t, 4, 16, []fakeVal{
		{rows: rowsOf(0, 4), serve: fakeSlow(1200 * time.Millisecond)},
		{rows: rowsOf(1, 4), serve: fakeSlow(1200 * time.Millisecond)},
	})
	p := readProber(t, f)
	p.cfg.Concurrency = 1
	p.initPace()
	p.cfg.RequestStartMargin = time.Until(f.pub.MustServeUntil) - 400*time.Millisecond
	p.cfg.RetrySpacing = 50 * time.Millisecond
	by := attemptsOf(readFull(t, p, f.pub))
	served, held := 0, 0
	for i, tg := range f.targets {
		rows := by[tg.AddressHex]
		if len(rows) != 1 {
			t.Fatalf("validator %d: %d rows, want one", i, len(rows))
		}
		switch m := rows[0]; {
		case m.Outcome == OutcomeServedOK && f.calls[i].Load() == 1:
			served++
		case m.Classification == ClassNotProbed && strings.Contains(m.ClassificationReason, "request could not start") && f.calls[i].Load() == 0:
			held++
		default:
			t.Fatalf("validator %d: %s / %s %q, asked %d times", i, m.Outcome, m.Classification, m.ClassificationReason, f.calls[i].Load())
		}
	}
	if served != 1 || held != 1 {
		t.Fatalf("%d served, %d held back, want one of each", served, held)
	}
}

// The later attempts hold no blob slot: with room for one blob at a time,
// both blobs' readings end, and their slots are free, while each still owes
// an attempt; the attempts are then made, and serve.
func TestRetriesHoldNoBlobSlot(t *testing.T) {
	f := newReadFixture(t, 4, 16, []fakeVal{
		{rows: rowsOf(0, 4), serve: fakeServes},
		{rows: rowsOf(1, 2), serve: firstThen(1, fakeNotFound, fakeServes)},
	})
	g := newReadFixture(t, 4, 16, []fakeVal{
		{rows: rowsOf(0, 4), serve: fakeServes},
		{rows: rowsOf(1, 2), serve: firstThen(1, fakeNotFound, fakeServes)},
	})
	p := readProber(t, f, g)
	p.cfg.BlobConcurrency = 1
	p.cfg.AskEveryEndorser = true
	p.cfg.RetrySpacing = 3 * time.Second
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		p.dispatch(ctx)
	}()
	go func() {
		defer wg.Done()
		p.runRetries(ctx)
	}()
	now := time.Now()
	for _, pb := range []scan.Publication{f.pub, g.pub} {
		p.sched.push(&readJob{pub: pb, point: p.readPoint(pb), start: now, latest: pb.MustServeUntil.Add(-time.Minute)})
	}
	waitFor(t, 20*time.Second, p.sched.idle)
	if p.retries.idle() || !p.retries.owes(f.pub.PromiseHash) || !p.retries.owes(g.pub.PromiseHash) {
		t.Fatal("both readings ended, one slot between them, and no attempt is owed: the attempts ran inside the readings")
	}
	waitFor(t, 30*time.Second, p.retries.idle)
	cancel()
	wg.Wait()
	ms, err := LoadMeasurements(p.store.Path())
	if err != nil {
		t.Fatal(err)
	}
	by := attemptsOf(ms)
	for _, fx := range []*readFixture{f, g} {
		rows := by[fx.targets[1].AddressHex]
		if len(rows) != 2 || rows[1].Attempt != 1 || !rows[1].Download.CommitmentVerified {
			t.Fatalf("%d rows for the validator asked again: %+v", len(rows), rows)
		}
	}
}

// At most one later attempt is in flight to a validator: three blobs whose
// one validator did not serve owe it six attempts between them, due at
// about the same time, and they reach it one at a time.
func TestOneRetryInFlightPerValidator(t *testing.T) {
	var busy, most atomic.Int32
	serve := func(ctx context.Context, call int, honest *fibretypes.BlobShard) (*fibretypes.DownloadShardResponse, error) {
		if call <= 3 { // the three readings' own requests
			return nil, status.Error(codes.NotFound, "no blob shard found")
		}
		n := busy.Add(1)
		defer busy.Add(-1)
		for {
			m := most.Load()
			if n <= m || most.CompareAndSwap(m, n) {
				break
			}
		}
		select {
		case <-time.After(250 * time.Millisecond):
		case <-ctx.Done():
		}
		return nil, status.Error(codes.NotFound, "no blob shard found")
	}
	f := newReadFixture(t, 4, 8, []fakeVal{{rows: []int{0, 1, 2, 3}, serve: serve}})
	b2, b3 := *f, *f
	b2.pub.PromiseHash, b3.pub.PromiseHash = strings.Repeat("e2", 32), strings.Repeat("e3", 32)
	p := readProber(t, f, &b2, &b3)
	p.cfg.RetrySpacing = 300 * time.Millisecond
	ms := readFull(t, p, f.pub, b2.pub, b3.pub)
	if len(ms) != 9 {
		t.Fatalf("%d rows, want three attempts on each of three blobs", len(ms))
	}
	if n := f.calls[0].Load(); n != 9 {
		t.Fatalf("the validator was asked %d times, want 9", n)
	}
	if m := most.Load(); m != 1 {
		t.Fatalf("%d later attempts reached the validator at once, want 1", m)
	}
}

// A restart does not cost a validator its later attempts: the record says
// which attempts a full reading still owes, and a restarted prober makes
// them (here the attempt serves) before it lets the blob go; one whose time
// is gone is recorded as not made, abandoned by the restart, never left as
// the validator's last failure.
func TestARestartMakesTheAttemptsItOwes(t *testing.T) {
	for _, late := range []bool{false, true} {
		name := "in time"
		if late {
			name = "too late"
		}
		t.Run(name, func(t *testing.T) {
			f := newReadFixture(t, 4, 16, []fakeVal{
				{rows: rowsOf(0, 4), serve: fakeServes},
				{rows: rowsOf(1, 2), serve: firstThen(1, fakeNotFound, fakeServes)},
			})
			before := readProber(t, f)
			before.cfg.AskEveryEndorser = true
			before.cfg.RetrySpacing = 50 * time.Millisecond
			// The dispatcher alone: the reading is made, its attempt is
			// queued and never run, as when the process stops.
			if ms := readNow(t, before, f.pub); len(ms) != 2 {
				t.Fatalf("first run: %d rows", len(ms))
			}
			dir := before.cfg.DataDir
			if err := before.store.Close(); err != nil {
				t.Fatal(err)
			}

			p := readProber(t, f)
			_ = p.store.Close()
			st, err := OpenMeasurementStore(dir)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { st.Close() })
			p.store, p.cfg.DataDir = st, dir
			p.cfg.AskEveryEndorser = true
			p.cfg.RetrySpacing = 50 * time.Millisecond
			if late {
				p.cfg.RequestStartMargin = time.Until(f.pub.MustServeUntil) + time.Minute
			}
			owed := st.PendingAttempts(f.pub.PromiseHash)
			if len(owed) != 1 || owed[0].Validator != f.targets[1].AddressHex || owed[0].Attempt != 0 || owed[0].Due.IsZero() {
				t.Fatalf("owed after the restart: %+v", owed)
			}
			due, missed, finished := p.planReads([]scan.Publication{f.pub}, time.Now())
			if len(due)+len(missed)+len(finished) != 0 || !p.retries.owes(f.pub.PromiseHash) {
				t.Fatalf("planned %d due, %d missed, %d finished; owes %v", len(due), len(missed), len(finished), p.retries.owes(f.pub.PromiseHash))
			}
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan struct{})
			go func() {
				defer close(done)
				p.runRetries(ctx)
			}()
			waitFor(t, 30*time.Second, p.retries.idle)
			cancel()
			<-done
			if _, _, finished := p.planReads([]scan.Publication{f.pub}, time.Now()); len(finished) != 1 {
				t.Fatal("the blob is not let go once its attempts are made")
			}
			ms, err := LoadMeasurements(st.Path())
			if err != nil {
				t.Fatal(err)
			}
			rows := attemptsOf(ms)[f.targets[1].AddressHex]
			if len(rows) != 2 || rows[1].Attempt != 1 {
				t.Fatalf("%d rows for the validator owed an attempt", len(rows))
			}
			switch m := rows[1]; {
			case !late && !m.Download.CommitmentVerified:
				t.Fatalf("the attempt made after the restart: %s", m.Outcome)
			case late && (m.Classification != ClassNotProbed || !strings.Contains(m.ClassificationReason, "restart")):
				t.Fatalf("the attempt too late after the restart: %s / %s %q", m.Outcome, m.Classification, m.ClassificationReason)
			}
			wantCalls := int32(2)
			if late {
				wantCalls = 1
			}
			if n := f.calls[1].Load(); n != wantCalls {
				t.Fatalf("the validator was asked %d times, want %d", n, wantCalls)
			}
		})
	}
}

// The store keeps, per full reading, the validators whose last row says
// they are owed another attempt (NextAttemptDue), from the rows as they are
// appended and again from the file on open; a served answer, an attempt not
// made, the last attempt, an answer whose next attempt could not have
// started in time, and every row of another label leave nothing owed.
func TestTheStoreKnowsTheAttemptsStillOwed(t *testing.T) {
	dir := t.TempDir()
	st, err := OpenMeasurementStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	at := time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC)
	cutoff := at.Add(9 * time.Minute)
	p := testProber(t)
	row := func(v string, label string, attempt int, o Outcome, c Classification, verified bool) Measurement {
		m := Measurement{Vantage: "v1", PromiseHash: "p1", ValidatorAddress: v, ScheduleLabel: label, ScheduledAt: at,
			StartedAt: at.Add(time.Duration(attempt) * time.Minute), FinishedAt: at.Add(time.Duration(attempt)*time.Minute + time.Second),
			Attempt: attempt, Outcome: o, Classification: c, Phase: PhaseInWindow, Read: &ReadInfo{Order: 3, BlobResult: ReadAvailable}}
		m.Download.CommitmentVerified = verified
		m.NextAttemptDue = p.nextAttemptDue(m, cutoff, 0)
		return m
	}
	late := row("late", FullReadLabel, 0, OutcomeNotFound, ClassFault, false)
	late.FinishedAt = cutoff.Add(-time.Second) // 90 s on is past the cutoff
	late.NextAttemptDue = p.nextAttemptDue(late, cutoff, 0)
	ms := []Measurement{
		row("open", FullReadLabel, 0, OutcomeNotFound, ClassFault, false),
		row("served", FullReadLabel, 0, OutcomeNotFound, ClassFault, false),
		row("served", FullReadLabel, 1, OutcomeServedOK, ClassHealthy, true),
		row("short", FullReadLabel, 0, OutcomePartial, ClassUnmatchedGenuine, true),
		row("notmade", FullReadLabel, 0, OutcomeTCPTimeout, ClassUnreachable, false),
		row("notmade", FullReadLabel, 1, OutcomeMissed, ClassNotProbed, false),
		row("last", FullReadLabel, 0, OutcomeNotFound, ClassFault, false),
		row("last", FullReadLabel, 1, OutcomeNotFound, ClassFault, false),
		row("last", FullReadLabel, 2, OutcomeNotFound, ClassFault, false),
		row("old", EndReadLabel, 0, OutcomeNotFound, ClassFault, false),
		row("gap", FullReadLabel, 0, OutcomeProbeError, ClassProbeError, false),
		late,
	}
	for _, m := range ms {
		owed := m.NextAttemptDue != nil
		want := m.ScheduleLabel == FullReadLabel && m.Attempt < FullReadRetries && m.Classification != ClassNotProbed &&
			!m.fullServed() && m.ValidatorAddress != "late"
		if owed != want {
			t.Fatalf("%s attempt %d (%s): owed %v, want %v", m.ValidatorAddress, m.Attempt, m.Outcome, owed, want)
		}
		if owed && !m.NextAttemptDue.Equal(m.FinishedAt.Add(p.cfg.RetrySpacing)) {
			t.Fatalf("%s: due %s, want %s after its answer ended", m.ValidatorAddress, m.NextAttemptDue, p.cfg.RetrySpacing)
		}
	}
	if err := st.AppendReading(ms); err != nil {
		t.Fatal(err)
	}
	want := "gap open short"
	got := func(st *MeasurementStore) string {
		var vs []string
		for _, m := range st.PendingAttempts("p1") {
			if m.Order != 3 || m.BlobResult != ReadAvailable || !m.ScheduledAt.Equal(at) || !m.Due.Equal(at.Add(time.Second+p.cfg.RetrySpacing)) {
				t.Fatalf("mark %+v", m)
			}
			vs = append(vs, m.Validator)
		}
		return strings.Join(vs, " ")
	}
	if g := got(st); g != want {
		t.Fatalf("owed %q, want %q", g, want)
	}
	if !st.Has("v1", "p1", "last", at) {
		t.Fatal("the reading's own attempt is not on record")
	}
	st.Close()
	again, err := OpenMeasurementStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer again.Close()
	if g := got(again); g != want {
		t.Fatalf("after reopening: owed %q, want %q", g, want)
	}
	again.Forget("p1")
	if len(again.PendingAttempts("p1")) != 0 {
		t.Fatal("a forgotten publication still owes attempts")
	}
}

// Which rows belong to a full reading, which answers serve there, and how a
// dedupe key names its attempt.
func TestTheFullReadingWords(t *testing.T) {
	for _, c := range []struct {
		label string
		at    time.Time
		full  bool
	}{
		{FullReadLabel, FullReadSince.Add(-time.Hour), true},
		{EndReadLabel, FullReadSince, true},
		{EndReadLabel, FullReadSince.Add(-time.Nanosecond), false},
		{EnoughReadLabel, FullReadSince.Add(time.Hour), false},
		{"w4", FullReadSince.Add(time.Hour), false},
		{"grace", FullReadSince.Add(time.Hour), false},
	} {
		if got := FullReading(c.label, c.at); got != c.full {
			t.Errorf("FullReading(%q, %s) = %v", c.label, c.at, got)
		}
	}
	for label, end := range map[string]bool{FullReadLabel: true, EndReadLabel: true, EnoughReadLabel: true, "w4": false, "post": false} {
		if EndOfWindowLabel(label) != end {
			t.Errorf("EndOfWindowLabel(%q) = %v", label, !end)
		}
	}
	// Served is the validator's own rows, or another settled promise's
	// exactly; rows of the blob that are not its own and that no settled
	// promise explains are this observer's gap; a short shard of its own
	// rows is neither (it is not served).
	for _, c := range []struct {
		verified    bool
		o           Outcome
		cls         Classification
		subset      bool
		served, gap bool
	}{
		{true, OutcomeServedOK, ClassHealthy, false, true, false},
		{true, OutcomeWrongRows, ClassShadowedShard, false, true, false},
		{true, OutcomePartial, ClassShadowedShard, false, true, false},
		{true, OutcomeWrongRows, ClassUnmatchedGenuine, false, false, true},
		{true, OutcomePartial, ClassUnmatchedGenuine, false, false, true},
		{true, OutcomePartial, ClassUnmatchedGenuine, true, false, false},
		{true, OutcomePartial, ClassProbeError, true, false, true},
		{false, OutcomeInvalidRows, ClassFault, false, false, false},
		{false, OutcomeNotFound, ClassFault, false, false, false},
		{false, OutcomeTCPTimeout, ClassProbeError, false, false, true},
		{false, OutcomeMissed, ClassNotProbed, false, false, true},
	} {
		if got := FullServed(c.verified, c.o, c.cls); got != c.served {
			t.Errorf("FullServed(%v, %s, %s) = %v", c.verified, c.o, c.cls, got)
		}
		if got := FullGap(c.verified, c.o, c.cls, c.subset); got != c.gap {
			t.Errorf("FullGap(%v, %s, %s, subset %v) = %v", c.verified, c.o, c.cls, c.subset, got)
		}
	}
	m := Measurement{Vantage: "v", PromiseHash: "p", ValidatorAddress: "a", ScheduledAt: FullReadSince}
	if k := m.DedupeKey(); k != "v|p|a|2026-10-02T16:09:49Z" || AttemptOfKey(k) != 0 {
		t.Fatalf("attempt 0 key %q", k)
	}
	m.Attempt = 2
	if k := m.DedupeKey(); k != "v|p|a|2026-10-02T16:09:49Z|2" || AttemptOfKey(k) != 2 {
		t.Fatalf("attempt 2 key %q", k)
	}
}
