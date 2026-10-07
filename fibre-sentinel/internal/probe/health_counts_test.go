package probe

import (
	"context"
	"testing"
	"time"
)

// A reading that cannot even begin (a coder, target or client-order error)
// is counted for the status file's reading_errors_15m, and none as made:
// before, its error was the status file's last error until the planning
// loop's next OK, half a minute later, and /v1/health never saw it.
func TestAReadingThatCannotBeginIsCounted(t *testing.T) {
	p := testProber(t)
	p.sched = newReadQueue()
	now := time.Now()
	j := &readJob{pub: onChain(pub(now.Add(-time.Hour), now.Add(time.Hour)), "0a"), start: now, latest: now.Add(time.Minute)}
	j.pub.Promise.Commitment = "not a commitment"
	p.sched.push(j)
	if got, _ := p.sched.popDue(now); got != j {
		t.Fatal("the reading was not due")
	}
	p.readBlob(context.Background(), j, func() {})
	if n := p.counters.failed.within(time.Now(), readingErrorsWindow); n != 1 {
		t.Fatalf("failed readings in the window: %d, want 1", n)
	}
	if n := p.counters.made.within(time.Now(), readingErrorsWindow); n != 0 {
		t.Fatalf("made readings in the window: %d, want 0", n)
	}
	if p.sched.has("0a") {
		t.Fatal("the failed reading is still held by the queue")
	}
}

// Events older than the span asked for are not counted in it.
func TestRecentEventsWithin(t *testing.T) {
	var r recentEvents
	now := time.Now()
	r.add(now.Add(-40 * time.Minute))
	r.add(now.Add(-16 * time.Minute))
	r.add(now.Add(-14 * time.Minute))
	r.add(now.Add(-time.Second))
	if n := r.within(now, 15*time.Minute); n != 2 {
		t.Fatalf("within 15m: %d, want 2", n)
	}
	if n := r.lastHour(now); n != 4 {
		t.Fatalf("last hour: %d, want 4", n)
	}
}

// A reading still queued past its last start is overdue: the dispatcher
// pops such a reading at its next free slot and writes it as not read, so
// one that stays queued means no slot came free.
func TestAQueuedReadingPastItsLastStartIsOverdue(t *testing.T) {
	q := newReadQueue()
	now := time.Now()
	late := &readJob{start: now.Add(-25 * time.Minute), latest: now.Add(-16 * time.Minute)}
	late.pub.PromiseHash = "late"
	later := &readJob{start: now.Add(-20 * time.Minute), latest: now.Add(-time.Minute)}
	later.pub.PromiseHash = "later"
	due := &readJob{start: now.Add(-time.Minute), latest: now.Add(2 * time.Minute)}
	due.pub.PromiseHash = "due"
	if n, _ := q.overdue(now); n != 0 {
		t.Fatalf("an empty queue has %d overdue", n)
	}
	q.push(late)
	q.push(later)
	q.push(due)
	n, oldest := q.overdue(now)
	if n != 2 || !oldest.Equal(late.latest) {
		t.Fatalf("overdue: %d, oldest %s; want 2, %s", n, oldest, late.latest)
	}
	if now.Sub(oldest) <= dispatchStallAfter {
		t.Fatal("the test's oldest reading must be past the stall bound")
	}
}
