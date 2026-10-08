package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/probe"
)

// A heartbeat that stopped mid-write (a full disk, a crash) left half a
// record at the end of its file. The next run cuts it before it appends, so
// its first record is a line of its own, not glued onto the fragment and
// lost with it when the collector steps over the line.
func TestTheNextRunCutsATornTail(t *testing.T) {
	path := filepath.Join(t.TempDir(), "reachability.jsonl")
	if err := os.WriteFile(path, []byte("{\"vantage\":\"a\"}\n{\"vantage\":\"b\",\"sche"), 0o644); err != nil {
		t.Fatal(err)
	}
	out, cut, err := openOutput(path)
	if err != nil {
		t.Fatal(err)
	}
	if cut == 0 {
		t.Fatal("the torn tail was not cut")
	}
	if _, err := out.Write([]byte("{\"vantage\":\"c\"}\n")); err != nil {
		t.Fatal(err)
	}
	if err := out.Close(); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := string(b); got != "{\"vantage\":\"a\"}\n{\"vantage\":\"c\"}\n" {
		t.Fatalf("the file reads %q", got)
	}

	// A first start, with no file yet, opens one.
	out, cut, err = openOutput(filepath.Join(t.TempDir(), "reachability.jsonl"))
	if err != nil || cut != 0 {
		t.Fatalf("a new file: cut %d, %v", cut, err)
	}
	out.Close()
}

// One request is bounded as a whole: its steps' bounds and a few seconds,
// the identity check's at probe's default when the heartbeat sets none.
func TestARequestIsBoundedAsAWhole(t *testing.T) {
	got := requestBound(probe.StepTimeouts{DNS: 5 * time.Second, TCP: 5 * time.Second, TLS: 10 * time.Second})
	if want := 20*time.Second + probe.DefaultStepTimeouts().Identity + 5*time.Second; got != want {
		t.Fatalf("bound %s, want %s", got, want)
	}
}
