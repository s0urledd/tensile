package api

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/observer/feed"
)

// A feed past its TTL is served at once and rebuilt behind it, one rebuild
// however many readers find it stale; only the first reader after a start
// waits for a build. A feed that stops being answerable (the validator is no
// longer on record) is dropped by its rebuild, not served on.
func TestAStaleFeedIsServedAtOnceAndRebuiltBehindIt(t *testing.T) {
	s := &Server{}
	ownFeeds(t, s)
	var builds atomic.Int32
	release := make(chan struct{})
	status := atomic.Int32{}
	status.Store(http.StatusOK)
	build := func(ctx context.Context, authority string, now time.Time) (*feed.Feed, int, error) {
		n := builds.Add(1)
		if n == 2 {
			<-release
		}
		return &feed.Feed{ID: "tag:test,2026:x", Title: fmt.Sprintf("build %d", n)}, int(status.Load()), nil
	}
	serve := func() *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		s.serveFeed(rec, httptest.NewRequest(http.MethodGet, "/v1/feed.atom", nil), build)
		return rec
	}
	age := func() { ageFeeds(s) }

	if b := serve().Body.String(); !strings.Contains(b, "build 1") {
		t.Fatalf("first read: %s", b)
	}
	age()
	for i := 0; i < 3; i++ {
		start := time.Now()
		if b := serve().Body.String(); !strings.Contains(b, "build 1") {
			t.Fatalf("stale read %d did not get the feed as it stands: %s", i, b)
		}
		if took := time.Since(start); took > time.Second {
			t.Fatalf("a stale read waited %s for the rebuild", took)
		}
	}
	close(release)
	s.bg.Wait()
	if n := builds.Load(); n != 2 {
		t.Fatalf("%d builds, want 2: one at the start, one rebuild for three stale readers", n)
	}
	if b := serve().Body.String(); !strings.Contains(b, "build 2") {
		t.Fatalf("after the rebuild: %s", b)
	}

	status.Store(http.StatusNotFound)
	age()
	serve() // stale: served, and the rebuild finds nothing to answer with
	s.bg.Wait()
	if code := serve().Code; code != http.StatusNotFound {
		t.Fatalf("a feed whose rebuild answers 404 was still served (%d)", code)
	}
}

// ownFeeds gives a test a feed cache with nothing cached for s, before and
// after it runs. The cache is the package's and keyed by the server's address:
// a server another test made, once the garbage collector frees it, can leave
// its feeds under the address a new one is given.
func ownFeeds(t *testing.T, s *Server) {
	forget := func() {
		prefix := fmt.Sprintf("%p|", s)
		feedCache.Lock()
		defer feedCache.Unlock()
		for k := range feedCache.m {
			if strings.HasPrefix(k, prefix) {
				delete(feedCache.m, k)
			}
		}
		for k := range feedCache.refreshing {
			if strings.HasPrefix(k, prefix) {
				delete(feedCache.refreshing, k)
			}
		}
	}
	forget()
	// no wait for s.bg here: a test that failed early can leave a rebuild
	// blocked for good; while it runs it holds s, so no other server is given
	// the address
	t.Cleanup(forget)
}

// ageFeeds makes every feed s has cached older than the TTL.
func ageFeeds(s *Server) {
	prefix := fmt.Sprintf("%p|", s)
	feedCache.Lock()
	defer feedCache.Unlock()
	for k, c := range feedCache.m {
		if strings.HasPrefix(k, prefix) {
			c.at = c.at.Add(-2 * feedTTL)
			feedCache.m[k] = c
		}
	}
}

// A rebuild runs on a goroutine of its own, where nothing recovers a panic
// the way net/http does on a request: one ended the whole API and left the
// feed marked refreshing. It is a failed rebuild instead: the feed is served
// as it stands, and the next reader past the TTL starts another rebuild.
func TestAPanickingFeedRebuildKeepsTheFeedAndTheProcess(t *testing.T) {
	s := &Server{}
	ownFeeds(t, s)
	var builds atomic.Int32
	build := func(ctx context.Context, authority string, now time.Time) (*feed.Feed, int, error) {
		n := builds.Add(1)
		if n == 2 {
			panic("boom")
		}
		return &feed.Feed{ID: "tag:test,2026:x", Title: fmt.Sprintf("build %d", n)}, http.StatusOK, nil
	}
	serve := func() *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		s.serveFeed(rec, httptest.NewRequest(http.MethodGet, "/v1/feed.atom", nil), build)
		return rec
	}

	serve()
	ageFeeds(s)
	serve() // stale: served, and the rebuild behind it panics
	s.bg.Wait()
	if rec := serve(); rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "build 1") {
		t.Fatalf("after a panicking rebuild: %d %s", rec.Code, rec.Body.String())
	}
	s.bg.Wait()
	if n := builds.Load(); n != 3 {
		t.Fatalf("%d builds, want 3: the feed stayed stale, so the next reader started another rebuild", n)
	}
	if b := serve().Body.String(); !strings.Contains(b, "build 3") {
		t.Fatalf("after the next rebuild: %s", b)
	}
}
