package probe

import (
	"strings"
	"testing"
	"time"
)

// This observer's own wait (here the reading-rate ceiling; equally a full
// byte budget, a late start after a restart, or a lane) pushes a
// validator's first answer late. The later attempt it would have been owed
// on time stays owed, and is recorded as not made (NOT_PROBED, this
// observer's gap) when its cutoff comes: the one failure is never the
// validator's last answer because of a wait of this observer's. On time,
// the same validator is asked again and serves.
func TestThisObserversWaitDoesNotCostAValidatorItsAttempt(t *testing.T) {
	run := func(t *testing.T, wait time.Duration) []Measurement {
		f := newReadFixture(t, 4, 16, []fakeVal{{rows: rowsOf(0, 4), serve: firstThen(1, fakeNotFound, fakeServes)}})
		p := readProber(t, f)
		shard := float64(ShardBytes(f.pub.Promise.BlobSize, 4, 4))
		p.cfg.RequestStartMargin = time.Until(f.pub.MustServeUntil) - 6*time.Second // the cutoff in 6 s
		p.cfg.RetrySpacing = 2 * time.Second
		if wait > 0 {
			p.ceiling = newReadCeiling(shard/wait.Seconds(), shard)
			p.ceiling.reserve(int64(shard), time.Time{}) // the bucket spent by another blob's request
		}
		return attemptsOf(readFull(t, p, f.pub))[f.targets[0].AddressHex]
	}
	t.Run("on time", func(t *testing.T) {
		rows := run(t, 0)
		if len(rows) != 2 || rows[0].Outcome != OutcomeNotFound || rows[0].NextAttemptDue == nil || rows[1].Outcome != OutcomeServedOK {
			t.Fatalf("rows %s, want not found then served", outcomesOf(rows))
		}
	})
	t.Run("held 4.5 s by the ceiling", func(t *testing.T) {
		rows := run(t, 4500*time.Millisecond)
		if len(rows) != 2 {
			t.Fatalf("rows %s, want the first answer and the attempt not made", outcomesOf(rows))
		}
		first, next := rows[0], rows[1]
		if first.Outcome != OutcomeNotFound || first.NextAttemptDue == nil || first.ObserverLoad == nil || first.ObserverLoad.AdmitWaitMS < 4000 {
			t.Fatalf("the first answer: %s, next attempt due %v, load %+v; want not found, still owed an attempt", first.Outcome, first.NextAttemptDue, first.ObserverLoad)
		}
		if next.Attempt != 1 || next.Classification != ClassNotProbed || !strings.Contains(next.RawError+next.ClassificationReason, "own delays") {
			t.Fatalf("the owed attempt: attempt %d, %s, %q; want attempt 1 not made, for this observer's own delays", next.Attempt, next.Classification, next.RawError)
		}
	})
}

// The client's re-dial is part of its request: a request let go near its
// start cutoff that fails before an answer comes back is made again at
// once, as the client makes it, even when the re-dial starts past the
// cutoff. Without it the one failed try would be the validator's last
// answer.
func TestTheRedialFollowsARequestLetGoNearTheCutoff(t *testing.T) {
	f := newReadFixture(t, 4, 16, []fakeVal{{rows: rowsOf(0, 4), serve: firstThen(1, fakeHangs, fakeServes)}})
	p := readProber(t, f)
	shard := float64(ShardBytes(f.pub.Promise.BlobSize, 4, 4))
	p.cfg.RequestStartMargin = time.Until(f.pub.MustServeUntil) - 3*time.Second // the cutoff in 3 s
	p.ceiling = newReadCeiling(shard/1.5, shard)
	p.ceiling.reserve(int64(shard), time.Time{}) // let go 1.5 s on; the 2 s request ends past the cutoff
	rows := attemptsOf(readFull(t, p, f.pub))[f.targets[0].AddressHex]
	if len(rows) != 1 || rows[0].Outcome != OutcomeServedOK || rows[0].Retry == nil {
		t.Fatalf("rows %s, want one row served on the client's re-dial", outcomesOf(rows))
	}
}

// Owed-ness is decided with this observer's own delay taken out: an answer
// whose next attempt would be due past the cutoff only because of that
// delay still owes it, and the due time is the real one.
func TestOwedNessTakesThisObserversDelayOut(t *testing.T) {
	p := testProber(t)
	at := time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC)
	cutoff := at.Add(9 * time.Minute)
	m := Measurement{ScheduleLabel: FullReadLabel, StartedAt: at, FinishedAt: cutoff.Add(-30 * time.Second), Outcome: OutcomeNotFound,
		Classification: ClassFault, Phase: PhaseInWindow}
	if p.nextAttemptDue(m, cutoff, 0) != nil {
		t.Fatal("an answer 30 s before the cutoff owes an attempt 90 s on, with no delay of this observer's")
	}
	due := p.nextAttemptDue(m, cutoff, 2*time.Minute)
	if due == nil || !due.Equal(m.FinishedAt.Add(p.cfg.RetrySpacing)) {
		t.Fatalf("delayed 2 minutes by this observer: due %v, want %v", due, m.FinishedAt.Add(p.cfg.RetrySpacing))
	}
	j := &retryJob{due: at, shift: time.Minute}
	if got := j.shiftAfter(Measurement{StartedAt: at.Add(30 * time.Second)}); got != 90*time.Second {
		t.Fatalf("an attempt started 30 s after it was due, after a minute's delay: shift %s, want 1m30s", got)
	}
	if got := j.shiftAfter(Measurement{StartedAt: at.Add(-time.Second)}); got != time.Minute {
		t.Fatalf("an attempt started before it was due: shift %s, want 1m0s", got)
	}
}

func outcomesOf(rows []Measurement) string {
	var out []string
	for _, m := range rows {
		s := string(m.Outcome) + "/" + string(m.Classification)
		if m.NextAttemptDue != nil {
			s += " (owed)"
		}
		out = append(out, s)
	}
	return "[" + strings.Join(out, ", ") + "]"
}
