package api

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/observer/feed"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
)

// feedsOf is the keys s has feeds cached under.
func feedsOf(s *Server) []string {
	prefix := fmt.Sprintf("%p|", s)
	feedCache.Lock()
	defer feedCache.Unlock()
	var out []string
	for k := range feedCache.m {
		if strings.HasPrefix(k, prefix) {
			out = append(out, k)
		}
	}
	return out
}

// A validator's feed is one feed however its address is written. Keyed by
// the path as typed, every spelling (a hex address in upper and lower case,
// space around it, its operator form) missed the cache and built the feed on
// the request: a reader cycling the spellings of one address had the API
// rebuild that validator's feed on every request, and filled the cache.
func TestAValidatorFeedIsKeptOnceWhateverItsSpelling(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "observer.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	v := fmt.Sprintf("%040x", 0xabcdef)
	// a heartbeat puts the validator on record for its feed
	at := store.TS(time.Now().Add(-time.Hour))
	if _, err := st.DB().Exec(`INSERT INTO reachability (dedupe_key, vantage, validator_address, validator_host, height, scheduled_at, started_at,
		dns_ok, tcp_ok, tcp_ms, tls_ok, tls_ms, identity_ok, identity_reason, outcome, raw_error, total_duration_ms, raw_json)
		VALUES ('k', 'test', ?, 'h:7980', 1, ?, ?, 1, 1, 10, 0, 20, 0, '', 'TLS_HANDSHAKE_FAIL', '', 30, '{}')`, v, at, at); err != nil {
		t.Fatal(err)
	}
	s := NewWithVantage(st, VantageInfo{Name: "test"}, nil)
	defer s.Close()
	ownFeeds(t, s)
	var bodies []string
	for _, spelling := range []string{v, strings.ToUpper(v), v[:20] + strings.ToUpper(v[20:]), "%20" + v} {
		rec := httptest.NewRecorder()
		s.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/validators/"+spelling+"/feed.atom", nil))
		if rec.Code != http.StatusOK {
			t.Fatalf("%s: %d %s", spelling, rec.Code, rec.Body.String())
		}
		bodies = append(bodies, rec.Body.String())
	}
	if keys := feedsOf(s); len(keys) != 1 {
		t.Fatalf("one validator's feed is cached under %d keys, want 1: %v", len(keys), keys)
	}
	for i, b := range bodies[1:] {
		if b != bodies[0] {
			t.Errorf("spelling %d answered another feed:\n%s\nwant\n%s", i+1, b, bodies[0])
		}
	}
}

// Readers of a feed with nothing cached wait for one build. Each of them
// used to build it on its own request, so a burst after a start (or after
// the cache was emptied) built the two-second network feed once per reader.
func TestReadersOfAFeedNotCachedWaitForOneBuild(t *testing.T) {
	s := &Server{}
	ownFeeds(t, s)
	var builds atomic.Int32
	release := make(chan struct{})
	build := func(ctx context.Context, authority string, now time.Time) (*feed.Feed, int, error) {
		builds.Add(1)
		<-release
		return &feed.Feed{ID: "tag:test,2026:x", Title: "the one build"}, http.StatusOK, nil
	}
	recs := make([]*httptest.ResponseRecorder, 8)
	var wg sync.WaitGroup
	for i := range recs {
		recs[i] = httptest.NewRecorder()
		wg.Add(1)
		go func(rec *httptest.ResponseRecorder) {
			defer wg.Done()
			s.serveFeed(rec, httptest.NewRequest(http.MethodGet, "/v1/feed.atom", nil), networkFeedName, build)
		}(recs[i])
	}
	for deadline := time.Now().Add(10 * time.Second); builds.Load() == 0; {
		if time.Now().After(deadline) {
			t.Fatal("no build started")
		}
		time.Sleep(time.Millisecond)
	}
	// the other readers arrive while the first build runs
	time.Sleep(200 * time.Millisecond)
	close(release)
	wg.Wait()
	s.bg.Wait()
	if n := builds.Load(); n != 1 {
		t.Fatalf("%d builds for %d readers of a feed not cached, want 1", n, len(recs))
	}
	for i, rec := range recs {
		if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "the one build") {
			t.Errorf("reader %d: %d %s", i, rec.Code, rec.Body.String())
		}
	}
}

// A full cache gives up the feed built longest ago, never the network's while
// a validator's is there to give up. It used to empty itself whole, the
// network feed with the rest.
func TestAFullFeedCacheKeepsTheNetworkFeed(t *testing.T) {
	s := &Server{}
	ownFeeds(t, s)
	now := time.Now()
	network := feedKey(s, "h", networkFeedName)
	storeFeed(network, cachedFeed{body: []byte("network"), at: now.Add(-time.Hour)})
	for i := 0; ; i++ {
		feedCache.Lock()
		full := len(feedCache.m) >= feedCacheMax
		feedCache.Unlock()
		if full {
			break
		}
		storeFeed(feedKey(s, "h", fmt.Sprintf("v:%040x", i)), cachedFeed{at: now.Add(-30*time.Minute + time.Duration(i)*time.Millisecond)})
	}
	added := feedKey(s, "h", "v:"+strings.Repeat("f", 40))
	storeFeed(added, cachedFeed{at: now})
	feedCache.Lock()
	defer feedCache.Unlock()
	if n := len(feedCache.m); n != feedCacheMax {
		t.Errorf("%d feeds kept after one more was stored in a full cache, want %d", n, feedCacheMax)
	}
	if string(feedCache.m[network].body) != "network" {
		t.Error("a full cache gave up the network feed, the oldest, while validators' feeds were there to give up")
	}
	if _, ok := feedCache.m[added]; !ok {
		t.Error("the feed stored last is not kept")
	}
}
