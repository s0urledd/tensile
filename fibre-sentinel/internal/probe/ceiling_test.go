package probe

import (
	"context"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

// The ceiling spreads a burst: forty requests of 10 kB asked for at once,
// under a ceiling of 200 kB a second with a 20 kB burst, are let go over
// about 1.9 s, and in no second are more bytes let go than the burst and a
// second of the rate (with room for a timer that fires late). Without the
// ceiling all 400 kB would go at once. Each request's wait is on its row.
func TestTheCeilingSpreadsABurst(t *testing.T) {
	p := testProber(t)
	const (
		rate, burst = 200_000.0, 20_000.0
		each, n     = 10_000, 40
	)
	p.ceiling = newReadCeiling(rate, burst)
	type letGo struct {
		at   time.Time
		load *LoadInfo
	}
	lets := make(chan letGo, n)
	start := time.Now()
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			release, load, err := p.admitBy(context.Background(), time.Time{}, each, each)
			if err != nil {
				t.Error(err)
				return
			}
			lets <- letGo{time.Now(), load}
			release() // the transfer itself takes no time here
		}()
	}
	wg.Wait()
	close(lets)
	var all []letGo
	for l := range lets {
		all = append(all, l)
	}
	if len(all) != n {
		t.Fatalf("%d of %d requests let go", len(all), n)
	}
	sort.Slice(all, func(i, j int) bool { return all[i].at.Before(all[j].at) })
	for i := range all {
		bytes := 0
		for j := i; j < len(all) && all[j].at.Sub(all[i].at) < time.Second; j++ {
			bytes += each
		}
		if most := burst + rate + 3*each; float64(bytes) > most {
			t.Fatalf("%d bytes let go in the second from %s, want at most %.0f", bytes, all[i].at.Sub(start), most)
		}
	}
	span := all[n-1].at.Sub(start)
	if want := time.Duration((n*each - burst) / rate * float64(time.Second)); span < want-50*time.Millisecond || span > 3*want {
		t.Fatalf("the burst was let go over %s, want about %s", span, want)
	}
	last := all[n-1].load
	if last.RateWaitMS < 1500 || last.AdmitWaitMS < last.RateWaitMS {
		t.Fatalf("the last request's load %+v, want the ceiling's wait of about 1.9 s on its row", last)
	}
	if _, rateP95, samples := p.counters.admitWaitP95(time.Now()); samples != n || rateP95 < time.Second {
		t.Fatalf("status: ceiling wait p95 %s over %d samples", rateP95, samples)
	}
}

// The ceiling's wait is not the request's time: four validators that serve
// at once, under a ceiling that lets one shard go every 300 ms, are let go
// one after another, the last about 900 ms after the first, and each row's
// own time is the request's alone. Every one served: waiting on this
// observer's ceiling is never counted against a validator.
func TestTheCeilingsWaitIsNotTheRequestsTime(t *testing.T) {
	var vals []fakeVal
	for i := 0; i < 4; i++ {
		vals = append(vals, fakeVal{rows: rowsOf(i, 4), serve: fakeServes})
	}
	f := newReadFixture(t, 4, 16, vals)
	p := readProber(t, f)
	shard := float64(ShardBytes(f.pub.Promise.BlobSize, 4, 4))
	p.ceiling = newReadCeiling(shard/0.3, shard)
	ms := readFull(t, p, f.pub)
	if len(ms) != 4 {
		t.Fatalf("%d rows, want one per validator", len(ms))
	}
	sort.Slice(ms, func(i, j int) bool { return ms[i].StartedAt.Before(ms[j].StartedAt) })
	for i, m := range ms {
		if m.Outcome != OutcomeServedOK || m.Classification != ClassHealthy || m.Attempt != 0 || m.ObserverLoad == nil {
			t.Fatalf("row %d: %s / %s, attempt %d, load %+v", i, m.Outcome, m.Classification, m.Attempt, m.ObserverLoad)
		}
		if m.TotalDurationMS >= 400 { // the last two waited 600 and 900 ms
			t.Fatalf("row %d took %d ms of its own, its ceiling wait %d ms: the wait was counted in the request's time",
				i, m.TotalDurationMS, m.ObserverLoad.RateWaitMS)
		}
	}
	last := ms[3]
	if last.ObserverLoad.RateWaitMS < 800 || last.StartedAt.Sub(ms[0].StartedAt) < 800*time.Millisecond {
		t.Fatalf("the last request waited %d ms and started %s after the first, want about 900 ms",
			last.ObserverLoad.RateWaitMS, last.StartedAt.Sub(ms[0].StartedAt))
	}
	if st := p.readStatus(); st["rate_wait_p95_ms"].(int64) < 800 || st["requests_not_started_last_hour"].(int) != 0 {
		t.Fatalf("status %+v", st)
	}
}

// A request whose turn under the ceiling would come at or after its start
// cutoff is not made and is not charged: NOT_PROBED, this observer's gap,
// and no later attempt is owed on it. In the reading's first pass, of two
// validators with a ceiling that holds one shard and refills a byte a
// second, one is asked and the other's row says the ceiling held it; a
// validator that did not serve at the reading is owed an attempt, and when
// the ceiling cannot let that attempt go before the cutoff it is written as
// an attempt not made, never as the validator's failure.
func TestAWaitPastTheCutoffIsNotProbed(t *testing.T) {
	t.Run("reading", func(t *testing.T) {
		f := newReadFixture(t, 4, 16, []fakeVal{{rows: rowsOf(0, 4), serve: fakeServes}, {rows: rowsOf(1, 4), serve: fakeServes}})
		p := readProber(t, f)
		shard := float64(ShardBytes(f.pub.Promise.BlobSize, 4, 4))
		p.ceiling = newReadCeiling(1, shard)
		p.cfg.RequestStartMargin = time.Until(f.pub.MustServeUntil) - 3*time.Second
		start := time.Now()
		by := attemptsOf(readFull(t, p, f.pub))
		if took := time.Since(start); took > 2*time.Second {
			t.Fatalf("the reading took %s: a request the ceiling could not let go in time waited for it", took)
		}
		served, held := 0, 0
		for i, tg := range f.targets {
			rows := by[tg.AddressHex]
			if len(rows) != 1 {
				t.Fatalf("validator %d: %d rows, want one", i, len(rows))
			}
			switch m := rows[0]; {
			case m.Outcome == OutcomeServedOK && f.calls[i].Load() == 1:
				served++
			case m.Classification == ClassNotProbed && f.calls[i].Load() == 0 && m.NextAttemptDue == nil &&
				strings.Contains(m.ClassificationReason, "request could not start") && strings.Contains(m.ClassificationReason, "reading-rate ceiling"):
				held++
			default:
				t.Fatalf("validator %d: %s / %s %q, asked %d times", i, m.Outcome, m.Classification, m.ClassificationReason, f.calls[i].Load())
			}
		}
		if served != 1 || held != 1 {
			t.Fatalf("%d served, %d held by the ceiling, want one of each", served, held)
		}
		if st := p.readStatus(); st["requests_not_started_last_hour"].(int) != 1 {
			t.Fatalf("status %+v, want the request not started", st)
		}
	})
	t.Run("attempt", func(t *testing.T) {
		f := newReadFixture(t, 4, 16, []fakeVal{
			{rows: rowsOf(0, 4), serve: fakeServes},
			{rows: rowsOf(1, 4), serve: firstThen(1, fakeNotFound, fakeServes)},
		})
		p := readProber(t, f)
		shard := float64(ShardBytes(f.pub.Promise.BlobSize, 4, 4))
		p.ceiling = newReadCeiling(1, 2*shard) // the reading's two requests, then nothing in time
		p.cfg.RetrySpacing = 50 * time.Millisecond
		p.cfg.RequestStartMargin = time.Until(f.pub.MustServeUntil) - 3*time.Second
		rows := attemptsOf(readFull(t, p, f.pub))[f.targets[1].AddressHex]
		if len(rows) != 2 || f.calls[1].Load() != 1 {
			t.Fatalf("%d rows, asked %d times, want the reading's answer and one attempt not made", len(rows), f.calls[1].Load())
		}
		if m := rows[0]; m.Outcome != OutcomeNotFound || m.NextAttemptDue == nil {
			t.Fatalf("the reading's answer: %s, next attempt due %v", m.Outcome, m.NextAttemptDue)
		}
		m := rows[1]
		if m.Attempt != 1 || m.Classification != ClassNotProbed || m.NextAttemptDue != nil ||
			!strings.Contains(m.ClassificationReason, "retry could not start") || !strings.Contains(m.ClassificationReason, "reading-rate ceiling") {
			t.Fatalf("attempt 1: attempt %d %s / %s %q, next %v", m.Attempt, m.Outcome, m.Classification, m.ClassificationReason, m.NextAttemptDue)
		}
		if st := p.readStatus(); st["retries_not_made_last_hour"].(int) != 1 {
			t.Fatalf("status %+v, want the attempt not made", st)
		}
	})
}

// A request larger than the bucket's burst still runs: it is charged the
// burst and let go once the bucket is full, as the byte budget lets a shard
// larger than itself go alone. A request whose turn would come at or after
// its cutoff is refused and charged nothing, and a charge given back (the
// run stopped while it waited) is the bucket's again.
func TestARequestLargerThanTheBurstStillRuns(t *testing.T) {
	c := newReadCeiling(1000, 1000)
	clock := time.Unix(1_000_000, 0)
	c.now = func() time.Time { return clock }
	c.at = clock
	step := func(n int64, by time.Time, want time.Duration, wantOK bool) float64 {
		t.Helper()
		wait, charged, ok := c.reserve(n, by)
		if ok != wantOK || (ok && (wait < want-time.Millisecond || wait > want+time.Millisecond)) {
			t.Fatalf("%d bytes: wait %s ok %v, want %s ok %v", n, wait, ok, want, wantOK)
		}
		return charged
	}
	if charged := step(5000, time.Time{}, 0, true); charged != 1000 {
		t.Fatalf("an oversize request charged %.0f, want the burst", charged)
	}
	step(500, time.Time{}, 500*time.Millisecond, true)
	step(5000, time.Time{}, 1500*time.Millisecond, true) // once the bucket is full again
	step(100, clock.Add(time.Second), 0, false)          // its turn would be 1.6 s away
	charged := step(100, time.Time{}, 1600*time.Millisecond, true)
	c.refund(charged)
	step(100, time.Time{}, 1600*time.Millisecond, true)
	clock = clock.Add(time.Minute) // the bucket fills, to its burst and no more
	step(1000, time.Time{}, 0, true)
	step(1, time.Time{}, time.Millisecond, true)
}

// 0 is no ceiling: nothing is paced, and a gigabyte is let go at once. The
// default is 400 Mbit/s, 50 MB a second, with a burst of about a second of
// it and at least readBurstFloor.
func TestNoCeilingAtZero(t *testing.T) {
	if rate, burst := ReadCeiling(0); rate != 0 || burst != 0 || newReadCeiling(rate, burst) != nil {
		t.Fatalf("0 Mbit/s: rate %.0f, burst %.0f", rate, burst)
	}
	p := testProber(t)
	if p.cfg.MaxReadMbps != 0 || p.ceiling != nil {
		t.Fatalf("a configuration without a ceiling has one: %d Mbit/s", p.cfg.MaxReadMbps)
	}
	start := time.Now()
	for i := 0; i < 3; i++ {
		release, load, err := p.admitBy(context.Background(), time.Now().Add(time.Second), 1<<30, 1<<30)
		if err != nil || load.RateWaitMS != 0 {
			t.Fatalf("request %d: %v, load %+v", i, err, load)
		}
		release()
	}
	if took := time.Since(start); took > 500*time.Millisecond {
		t.Fatalf("three gigabytes took %s to let go without a ceiling", took)
	}
	if rate, burst := ReadCeiling(DefaultMaxReadMbps); rate != 50e6 || burst != readBurstFloor {
		t.Fatalf("the default: rate %.0f, burst %.0f", rate, burst)
	}
	if rate, burst := ReadCeiling(4000); rate != 500e6 || burst != rate {
		t.Fatalf("4000 Mbit/s: rate %.0f, burst %.0f, want a second of the rate", rate, burst)
	}
	p.cfg.MaxReadMbps = DefaultMaxReadMbps
	p.initPace()
	if p.ceiling == nil || p.ceiling.rate != 50e6 {
		t.Fatal("the default ceiling is not set")
	}
}
