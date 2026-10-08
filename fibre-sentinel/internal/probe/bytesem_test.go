package probe

import (
	"context"
	"testing"
	"time"
)

// queued is how many waiters the byte budget holds in line.
func (b *byteSem) queued() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.queue)
}

// The byte budget lets its waiters go in turn: a large shard at the head of
// the line is not passed over by a small one that would fit, and once it
// goes the one behind it goes too when there is room.
func TestTheByteBudgetLetsWaitersGoInTurn(t *testing.T) {
	b := newByteSem(100)
	b.acquire(60)
	got := make(chan int64, 2)
	go func() { b.acquire(80); got <- 80 }()
	waitFor(t, 5*time.Second, func() bool { return b.queued() == 1 })
	go func() {
		if b.acquireBy(context.Background(), 10, time.Now().Add(5*time.Second)) {
			got <- 10
		}
	}()
	waitFor(t, 5*time.Second, func() bool { return b.queued() == 2 })
	select {
	case n := <-got:
		t.Fatalf("%d bytes went while the 80 before them waited", n)
	case <-time.After(100 * time.Millisecond):
	}
	b.release(60)
	if first := <-got; first != 80 {
		t.Fatalf("%d bytes went first, want the 80 at the head", first)
	}
	if second := <-got; second != 10 {
		t.Fatalf("then %d bytes, want 10", second)
	}
	if b.inFlight() != 90 || b.queued() != 0 {
		t.Fatalf("held %d with %d in line, want 90 and none", b.inFlight(), b.queued())
	}
}

// A waiter that gives up (its cutoff, or a stopped run) leaves the line, and
// the one behind it goes when it fits.
func TestAWaiterThatGivesUpLeavesTheLine(t *testing.T) {
	b := newByteSem(100)
	b.acquire(60)
	gaveUp := make(chan bool, 1)
	go func() { gaveUp <- !b.acquireBy(context.Background(), 80, time.Now().Add(time.Second)) }()
	waitFor(t, 5*time.Second, func() bool { return b.queued() == 1 })
	ctx, cancel := context.WithCancel(context.Background())
	stopped := make(chan bool, 1)
	go func() { stopped <- !b.acquireBy(ctx, 70, time.Time{}) }()
	waitFor(t, 5*time.Second, func() bool { return b.queued() == 2 })
	small := make(chan bool, 1)
	go func() { small <- b.acquireBy(context.Background(), 10, time.Now().Add(5*time.Second)) }()
	waitFor(t, 5*time.Second, func() bool { return b.queued() == 3 })
	if !<-gaveUp {
		t.Fatal("the 80 at the head did not give up at its cutoff")
	}
	cancel()
	if !<-stopped {
		t.Fatal("the 70 did not give up when the run stopped")
	}
	if !<-small {
		t.Fatal("the 10 behind them did not go once they left the line")
	}
	if b.inFlight() != 70 || b.queued() != 0 {
		t.Fatalf("held %d with %d in line, want 70 and none", b.inFlight(), b.queued())
	}
}

// A request already let go takes more of the budget, for an answer over its
// receive bound, only when it is free now (Prober.widen): it never waits,
// it goes ahead of a waiter, and what it took comes back when it ends.
func TestARequestTakesMoreBudgetOnlyWhenItIsFree(t *testing.T) {
	p := &Prober{cfg: Config{Concurrency: 4, InFlightBytes: 100}}
	p.initPace()
	p.bytes.acquire(60)
	waiting := make(chan bool, 1)
	go func() { waiting <- p.bytes.acquireBy(context.Background(), 50, time.Now().Add(5*time.Second)) }()
	waitFor(t, 5*time.Second, func() bool { return p.bytes.queued() == 1 })
	release, ok := p.widen(30)
	if !ok || p.bytes.inFlight() != 90 {
		t.Fatalf("widen by 30 beside 60 of 100: ok %v, held %d", ok, p.bytes.inFlight())
	}
	if _, ok := p.widen(20); ok {
		t.Fatalf("widen by 20 with 10 free was let go")
	}
	release()
	p.bytes.release(60)
	if !<-waiting || p.bytes.inFlight() != 50 {
		t.Fatalf("the waiter did not go once the room came back: held %d", p.bytes.inFlight())
	}
}
