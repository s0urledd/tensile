package api

import (
	"context"
	"encoding/json"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
)

// /v1/namespaces sums every publication on record, so it is computed in the
// background and served from memory, not once per request: a publication
// that lands is in the computation the first reader past namespacesTTL sets
// going, which that reader does not wait for. A computation that fails
// leaves the last answer served for at most namespacesStale and is an error
// after that, never an answer that silently stops moving.
func TestNamespacesAreComputedInTheBackground(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "observer.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	now := time.Date(2026, 9, 28, 16, 0, 0, 0, time.UTC)
	r := rand.New(rand.NewSource(1))
	pubs, _ := fxLoad(r, now, 20, 0)
	fxInsert(t, st.DB(), pubs, nil)
	clock := now
	s := newServer(st, VantageInfo{Name: "test"}, nil, withClock(func() time.Time { return clock }))
	defer s.bg.Wait() // a computation set going, before the store closes
	blobs := func() int64 {
		t.Helper()
		rec := httptest.NewRecorder()
		s.handleNamespaces(rec, httptest.NewRequest(http.MethodGet, "/v1/namespaces", nil))
		if rec.Code != 200 {
			t.Fatalf("namespaces: %d %s", rec.Code, rec.Body)
		}
		var out struct {
			Namespaces []namespaceRow `json:"namespaces"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
			t.Fatal(err)
		}
		var n int64
		for _, ns := range out.Namespaces {
			n += ns.Blobs
		}
		return n
	}
	// landed waits for the computation under way, if any, to land.
	landed := func() {
		t.Helper()
		s.namespaces.mu.Lock()
		f := s.namespaces.flight
		s.namespaces.mu.Unlock()
		if f != nil {
			<-f.done
		}
	}
	// The first reader after a start waits for the first answer.
	if n := blobs(); n != 20 {
		t.Fatalf("%d blobs, want 20", n)
	}
	more, _ := fxLoad(r, now, 5, 1000)
	fxInsert(t, st.DB(), more, nil)
	clock = now.Add(namespacesTTL / 2)
	if n := blobs(); n != 20 {
		t.Fatalf("within the TTL: %d blobs, want the 20 computed before", n)
	}
	s.namespaces.mu.Lock()
	idle := s.namespaces.flight == nil
	s.namespaces.mu.Unlock()
	if !idle {
		t.Fatal("a reader within the TTL set a computation going")
	}
	// Past it, the reader is served the last answer and sets the next going.
	clock = now.Add(namespacesTTL)
	if n := blobs(); n != 20 {
		t.Fatalf("past the TTL: %d blobs, want the last answer's 20 while the next is computed", n)
	}
	landed()
	if n := blobs(); n != 25 {
		t.Fatalf("once the computation landed: %d blobs, want 25", n)
	}

	// The computation fails: the last answer stands for a while, then the
	// failure is the answer.
	if _, err := st.DB().Exec(`ALTER TABLE publications RENAME TO publications_away`); err != nil {
		t.Fatal(err)
	}
	clock = now.Add(namespacesTTL + namespacesStale/2)
	if n := blobs(); n != 25 {
		t.Fatalf("a failing computation within namespacesStale: %d blobs, want the last answer's 25", n)
	}
	landed()
	if n := blobs(); n != 25 {
		t.Fatalf("after a failed computation, within namespacesStale: %d blobs, want 25", n)
	}
	landed()
	clock = now.Add(namespacesTTL + namespacesStale + time.Second)
	if _, err := s.namespaceRows(context.Background()); err == nil {
		t.Fatal("a failure lasting past namespacesStale was answered with the old figures")
	}
	if _, err := st.DB().Exec(`ALTER TABLE publications_away RENAME TO publications`); err != nil {
		t.Fatal(err)
	}
	if n := blobs(); n != 25 {
		t.Fatalf("once it computes again: %d blobs, want 25", n)
	}
}
