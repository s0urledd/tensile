package main

import (
	"context"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/scan"
)

// The shutdown returns only once every request has ended: one still
// running when the drain runs out is cancelled and waited for. main used
// to return as soon as ListenAndServe did, at the start of the drain, and
// closed the store under the requests still being answered.
func TestShutdownWaitsForTheRequestsItCancels(t *testing.T) {
	base, cancel := context.WithCancel(context.Background())
	defer cancel()
	var inflight sync.WaitGroup
	entered := make(chan struct{})
	var ended time.Time
	srv := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			inflight.Add(1)
			defer inflight.Done()
			close(entered)
			<-r.Context().Done() // a long export, say, until it is cancelled
			time.Sleep(100 * time.Millisecond)
			ended = time.Now()
		}),
		BaseContext: func(net.Listener) context.Context { return base },
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = srv.Serve(ln) }()
	go func() {
		if resp, err := http.Get("http://" + ln.Addr().String() + "/v1/exports/x"); err == nil {
			resp.Body.Close()
		}
	}()
	<-entered
	done := make(chan time.Time)
	go func() {
		shutdown(srv, cancel, &inflight, 200*time.Millisecond, 5*time.Second, scan.NewLogger(10))
		done <- time.Now()
	}()
	select {
	case at := <-done:
		if ended.IsZero() || at.Before(ended) {
			t.Fatalf("shutdown returned at %s, before the request ended (%s)", at, ended)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("shutdown did not return")
	}
}
