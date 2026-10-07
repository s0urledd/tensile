package api

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// A window whose every refresh fails was served, older by the hour, while
// only the log said so. Three failures in a row now fail the snapshots
// check, by window and without the error's text; a success clears them.
func TestSnapshotsCheckFailsAWindowWhoseRefreshesKeepFailing(t *testing.T) {
	boom := errors.New("readings: statuses: reconstructable H: slim: decode publication 0a")
	fail := true
	c := newSnapshotCache("network", func(context.Context, Window) (*networkResponse, error) {
		if fail {
			return nil, boom
		}
		return &networkResponse{}, nil
	})
	s := &Server{net: c}
	now := time.Now()
	if chk := s.snapshotsCheck(now); !chk.OK {
		t.Fatalf("a cache just made: %q", chk.Detail)
	}
	for i := 0; i < 3; i++ {
		if _, err := c.fill(context.Background(), windowFor("24h", now)); err == nil {
			t.Fatal("the computation did not fail")
		}
	}
	chk := s.snapshotsCheck(now)
	if chk.OK || !strings.HasPrefix(chk.Detail, "network 24h: 3 refreshes in a row failed") {
		t.Fatalf("snapshots: ok=%v %q", chk.OK, chk.Detail)
	}
	if strings.Contains(chk.Detail, "decode") {
		t.Fatalf("the detail carries the error: %q", chk.Detail)
	}
	fail = false
	if _, err := c.fill(context.Background(), windowFor("24h", now)); err != nil {
		t.Fatal(err)
	}
	if chk := s.snapshotsCheck(now); !chk.OK {
		t.Fatalf("after a success: %q", chk.Detail)
	}
}

// A window not refreshed for twice its interval, and at least fifteen
// minutes, fails; so does one never computed in that long since the start.
// A longer window's interval is longer.
func TestSnapshotsCheckFailsAWindowNotRefreshedForLong(t *testing.T) {
	c := newSnapshotCache("validators", func(context.Context, Window) (validatorSnapshot, error) {
		return validatorSnapshot{}, nil
	})
	s := &Server{vals: c}
	now := time.Now()
	for _, w := range []string{"7d", "30d", "all"} {
		if _, err := c.fill(context.Background(), windowFor(w, now)); err != nil {
			t.Fatal(err)
		}
	}
	chk := s.snapshotsCheck(now.Add(20 * time.Minute))
	if chk.OK {
		t.Fatal("twenty minutes without a refresh passed")
	}
	for _, want := range []string{"validators 24h: not computed in the 20m0s since the API started (bound 15m0s)",
		"validators 7d: last refreshed 20m0s ago (bound 15m0s)"} {
		if !strings.Contains(chk.Detail, want) {
			t.Errorf("detail lacks %q: %q", want, chk.Detail)
		}
	}
	if strings.Contains(chk.Detail, "30d") || strings.Contains(chk.Detail, "validators all") {
		t.Errorf("a fifteen-minute window is due only after thirty: %q", chk.Detail)
	}
}

// Close used to leave the refreshes running on the background context with
// their own timeouts (twenty minutes for "all"), and the API's main closed
// the store under them: "sql: database is closed" in the log on every
// restart, as if a refresh had failed. stop cancels a computation in
// flight, so waiting for it ends at once.
func TestStoppingACacheEndsARefreshInFlight(t *testing.T) {
	started := make(chan struct{})
	c := newSnapshotCache("network", func(ctx context.Context, _ Window) (*networkResponse, error) {
		close(started)
		<-ctx.Done()
		return nil, ctx.Err()
	})
	c.refreshing["all"] = true
	c.bg.Add(1)
	go func() {
		defer c.bg.Done()
		c.background(nil, windowFor("all", time.Now()))
	}()
	<-started
	done := make(chan struct{})
	go func() {
		c.stop()
		c.wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("a refresh in flight outlived the cache's stop")
	}
}
