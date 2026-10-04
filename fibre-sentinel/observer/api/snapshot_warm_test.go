package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
)

// A window with nothing to serve makes its reader wait a few seconds at most.
// The longest windows take minutes to compute, and a reader held for all of
// it reached the site proxy's timeout; the reader is told the figure is being
// computed instead, the computation carries on, and it is one computation
// however many readers ask.
func TestAColdWindowIsWaitedForABoundedTime(t *testing.T) {
	var n atomic.Int32
	release := make(chan struct{})
	c := newSnapshotCache("test", func(ctx context.Context, win Window) (int, error) {
		n.Add(1)
		<-release
		return 7, nil
	})
	c.firstWait = 100 * time.Millisecond
	win := testWindow("all")

	start := time.Now()
	if _, _, _, err := c.get(context.Background(), nil, win); !errors.Is(err, errComputing) {
		t.Fatalf("a read of a window still being computed returned %v, want errComputing", err)
	}
	if took := time.Since(start); took > 2*time.Second {
		t.Fatalf("the reader waited %s; the wait is bounded at %s", took, c.firstWait)
	}
	if _, _, _, err := c.get(context.Background(), nil, win); !errors.Is(err, errComputing) {
		t.Fatalf("second read: %v", err)
	}
	close(release)
	c.firstWait = 5 * time.Second
	v, _, _, err := c.get(context.Background(), nil, win)
	if err != nil || v != 7 {
		t.Fatalf("after the computation landed: %d, %v", v, err)
	}
	if got := n.Load(); got != 1 {
		t.Fatalf("%d computations for three readers of one window, want 1", got)
	}
}

// A computation that fails is the waiting reader's answer: it is not left to
// wait out the bound, and not told that the figure is still coming.
func TestAFailedComputationIsReturnedToTheReaderWaitingOnIt(t *testing.T) {
	boom := errors.New("boom")
	c := newSnapshotCache("test", func(ctx context.Context, win Window) (int, error) {
		return 0, boom
	})
	c.firstWait = 5 * time.Second
	start := time.Now()
	if _, _, _, err := c.get(context.Background(), nil, testWindow("24h")); !errors.Is(err, boom) {
		t.Fatalf("got %v, want the computation's error", err)
	}
	if took := time.Since(start); took > 2*time.Second {
		t.Fatalf("the reader waited %s for a computation that had already failed", took)
	}
}

// A computation runs on a goroutine of its own, where nothing recovers a
// panic the way net/http does on a request: one ended the whole API and left
// the window marked refreshing. It is a failed computation instead, returned
// to the reader waiting on it, and the next read computes the window again.
func TestAPanickingComputationFailsWithoutEndingTheProcess(t *testing.T) {
	var n atomic.Int32
	c := newSnapshotCache("test", func(ctx context.Context, win Window) (int, error) {
		if n.Add(1) == 1 {
			panic("boom")
		}
		return 7, nil
	})
	c.firstWait = 5 * time.Second
	win := testWindow("24h")
	if _, _, _, err := c.get(context.Background(), nil, win); err == nil || !strings.Contains(err.Error(), "panic: boom") {
		t.Fatalf("got %v, want the panic as the computation's error", err)
	}
	v, _, _, err := c.get(context.Background(), nil, win)
	if err != nil || v != 7 {
		t.Fatalf("the next read: %d, %v; want the window computed again", v, err)
	}
}

// The 503 says what it is, so the site can show a figure being computed and
// ask again instead of calling the API down.
func TestAWindowBeingComputedIsA503WithRetryAfter(t *testing.T) {
	s := newSnapshotServer(t)
	release := make(chan struct{})
	defer close(release)
	s.net = newSnapshotCache("network", func(ctx context.Context, win Window) (*networkResponse, error) {
		<-release
		return &networkResponse{}, nil
	})
	s.net.firstWait = 50 * time.Millisecond
	rec := httptest.NewRecorder()
	s.handleNetwork(rec, httptest.NewRequest(http.MethodGet, "/v1/network?window=all", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status %d, want 503", rec.Code)
	}
	if got := rec.Header().Get("Retry-After"); got == "" {
		t.Fatal("no Retry-After")
	}
	var body struct {
		Computing bool   `json:"computing"`
		Window    string `json:"window"`
		Error     string `json:"error"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if !body.Computing || body.Window != "all" || body.Error == "" {
		t.Fatalf("body %s", rec.Body.String())
	}
}

// WarmSnapshots is how a new build starts on figures computed under its own
// rules: it writes every window of every cache, and a process started on that
// directory serves them, under the revision they were computed under and for
// the vantage they were computed for, and not otherwise.
func TestWarmSnapshotsFillsEveryWindowForTheNextProcess(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "observer.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	dir := filepath.Join(t.TempDir(), "snapshots.next")
	if err := WarmSnapshots(context.Background(), st, VantageInfo{Name: "eu1"}, nil, WithSnapshotDir(dir)); err != nil {
		t.Fatal(err)
	}
	for _, label := range []string{"network", "validators", "market"} {
		for _, w := range warmWindows {
			if _, err := os.Stat(filepath.Join(dir, label+"-"+w+".json")); err != nil {
				t.Errorf("%s-%s was not written: %v", label, w, err)
			}
		}
	}

	next := newServer(st, VantageInfo{Name: "eu1"}, nil, WithSnapshotDir(dir))
	next.net.persistTo(next.snapshotsIn(), nil)
	next.vals.persistTo(next.snapshotsIn(), nil)
	win := testWindow("all")
	if s := next.net.current(nil, win, next.net.rev(), false); s == nil {
		t.Fatal("the warmed network snapshot was not served by the next process")
	}
	if s := next.vals.current(nil, win, next.vals.rev(), false); s == nil {
		t.Fatal("the warmed validator snapshot was not served by the next process")
	}

	// Under another revision (here the activation landing between the
	// warm-up and the switch) the file is dropped, not served.
	if err := st.SetMeta("fibre_active", "yes", time.Now()); err != nil {
		t.Fatal(err)
	}
	if s := next.net.current(nil, win, next.net.rev(), false); s != nil {
		t.Fatal("a snapshot warmed under an earlier revision was served")
	}

	// Warmed for another vantage (a hand-run warm-up without -vantage), it is
	// not loaded at all.
	other := newServer(st, VantageInfo{Name: "local"}, nil, WithSnapshotDir(dir))
	other.vals.persistTo(other.snapshotsIn(), nil)
	other.vals.mu.Lock()
	n := len(other.vals.entries)
	other.vals.mu.Unlock()
	if n != 0 {
		t.Fatalf("%d snapshot(s) computed for vantage eu1 were loaded by vantage local", n)
	}
}

// The warm-up leaves a window a reader computed while it was busy with the
// ones before it, rather than replace it with one that ends at the warm-up's
// start; a window it does compute ends when it reaches it. Before, the
// publishers' table and the market board, read a moment apart from one cache,
// could describe two moments.
func TestWarmLeavesAWindowAReaderComputed(t *testing.T) {
	c := newSnapshotCache("test", func(_ context.Context, w Window) (time.Time, error) { return w.End, nil })
	started := time.Now()
	time.Sleep(5 * time.Millisecond)
	read := windowFor("7d", time.Now())
	got, _, _, err := c.get(context.Background(), nil, read)
	if err != nil || !got.Equal(read.End) {
		t.Fatalf("the reader's 7d: %v %v", got, err)
	}
	c.warm(nil, started)
	c.wait()
	c.mu.Lock()
	week, all := c.entries["7d"], c.entries["all"]
	c.mu.Unlock()
	if week == nil || !week.v.Equal(read.End) {
		t.Fatalf("the warm-up replaced the reader's 7d window, ending %v, with one ending %v", read.End, week)
	}
	if all == nil || all.v.Before(read.End) {
		t.Fatalf("the warm-up computed the all window as of its start, not as of when it reached it: %v", all)
	}
}
