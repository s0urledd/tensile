package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// watched is a data directory with the fast tick's three files watched, as
// the collector watches them.
func watched(t *testing.T) (dir string, fw *fileWatch) {
	t.Helper()
	dir = t.TempDir()
	for _, f := range []string{"state.json", "publications.jsonl", "payments.jsonl"} {
		if err := os.WriteFile(filepath.Join(dir, f), nil, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	fw, err := watchFiles([]string{filepath.Join(dir, "state.json"), filepath.Join(dir, "publications.jsonl"), filepath.Join(dir, "payments.jsonl")},
		watchQuiet, watchMaxWait, t.Logf)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(fw.Close)
	// let the watch settle before the test writes: a create of the files
	// just above must not count as one of the test's changes
	time.Sleep(100 * time.Millisecond)
	drain(fw)
	return dir, fw
}

func drain(fw *fileWatch) {
	for {
		select {
		case <-fw.C:
		case <-time.After(300 * time.Millisecond):
			return
		}
	}
}

// woken waits up to within for a tick, and says how long it took.
func woken(t *testing.T, fw *fileWatch, within time.Duration, what string) time.Duration {
	t.Helper()
	start := time.Now()
	select {
	case <-fw.C:
		return time.Since(start)
	case <-time.After(within):
		t.Fatalf("%s: no tick within %s", what, within)
	}
	return 0
}

// quietFor fails on a tick within d.
func quietFor(t *testing.T, fw *fileWatch, d time.Duration, what string) {
	t.Helper()
	select {
	case <-fw.C:
		t.Fatalf("%s: a tick", what)
	case <-time.After(d):
	}
}

// A line appended to publications.jsonl wakes the tick within a tenth of a
// second or so, and so does one appended to payments.jsonl.
func TestAWriteWakesTheFastTick(t *testing.T) {
	dir, fw := watched(t)
	appendLine(t, filepath.Join(dir, "publications.jsonl"), `{"promise_hash":"a"}`+"\n")
	if took := woken(t, fw, 2*time.Second, "an append to publications.jsonl"); took > time.Second {
		t.Fatalf("woken %s after the append", took)
	}
	quietFor(t, fw, 300*time.Millisecond, "one append")
	appendLine(t, filepath.Join(dir, "payments.jsonl"), `{"dedupe_key":"a"}`+"\n")
	woken(t, fw, 2*time.Second, "an append to payments.jsonl")
}

// A block's records, many appends and a new state.json in a few
// milliseconds, are one tick, not one per write. On a loaded machine the
// writes themselves can stall: a burst that takes longer than watchMaxWait
// gets a tick at least that often while it lasts, and a stall longer than
// watchQuiet ends it, so the bound is one tick for each of those, and one
// more for the event delivery's own jitter. Forty-one writes stay a few
// ticks at most.
func TestABurstOfWritesIsOneTick(t *testing.T) {
	dir, fw := watched(t)
	// the ticks, taken as the collector's loop takes them: each at once,
	// from before the first write
	var ticks atomic.Int32
	stop := make(chan struct{})
	counted := make(chan struct{})
	go func() {
		defer close(counted)
		for {
			select {
			case <-fw.C:
				ticks.Add(1)
			case <-stop:
				return
			}
		}
	}()
	start, last, stalls := time.Now(), time.Now(), 0
	step := func() {
		if now := time.Now(); now.Sub(last) >= watchQuiet {
			stalls++
		}
		last = time.Now()
	}
	for i := 0; i < 20; i++ {
		appendLine(t, filepath.Join(dir, "publications.jsonl"), fmt.Sprintf(`{"promise_hash":"%d"}`+"\n", i))
		step()
		appendLine(t, filepath.Join(dir, "payments.jsonl"), fmt.Sprintf(`{"dedupe_key":"%d"}`+"\n", i))
		step()
	}
	replace(t, filepath.Join(dir, "state.json"), `{"last_scanned_height":7}`)
	step()
	took := time.Since(start)
	// quiet for 300 ms after the first tick ends the count
	for n := int32(-1); ; {
		time.Sleep(300 * time.Millisecond)
		cur := ticks.Load()
		if cur > 0 && cur == n || time.Since(start) > 3*time.Second {
			break
		}
		n = cur
	}
	close(stop)
	<-counted
	n := int(ticks.Load())
	if n == 0 {
		t.Fatal("the burst: no tick")
	}
	if most := 2 + stalls + int(took/watchMaxWait); n > most {
		t.Fatalf("41 writes in %s (%d stalls) were %d ticks, want at most %d", took, stalls, n, most)
	}
	t.Logf("41 writes in %s: %d tick(s)", took, n)
}

// A file written without pause still gets a tick at least every
// watchMaxWait or so: the wait for quiet is bounded.
func TestAFileNeverQuietIsStillRead(t *testing.T) {
	dir, fw := watched(t)
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			f, err := os.OpenFile(filepath.Join(dir, "publications.jsonl"), os.O_WRONLY|os.O_APPEND, 0o644)
			if err == nil {
				fmt.Fprintf(f, `{"promise_hash":"%d"}`+"\n", i)
				f.Close()
			}
			time.Sleep(5 * time.Millisecond)
		}
	}()
	defer func() { close(stop); <-done }()
	woken(t, fw, time.Second, "the first of a steady stream of appends")
	woken(t, fw, time.Second, "the stream, still going")
}

// The archive rotation renames a copy of the file's tail over the live file,
// and the scanner writes state.json by renaming a new copy over the old: a
// file replaced under its name wakes the tick, and so does the file written
// after the rename. A file removed and made again does too. The copies on
// their way in, under other names, do not, nor does any other file in the
// directory.
func TestARotatedOrRecreatedFileStillWakesTheTick(t *testing.T) {
	dir, fw := watched(t)
	pubs := filepath.Join(dir, "publications.jsonl")

	appendLine(t, filepath.Join(dir, "observer.db-wal"), "page")
	appendLine(t, filepath.Join(dir, "measurements.jsonl"), "{}\n")
	appendLine(t, filepath.Join(dir, "publications.jsonl.rotate.tmp"), "{}\n")
	quietFor(t, fw, 300*time.Millisecond, "writes to other files")

	replace(t, pubs, `{"promise_hash":"tail"}`+"\n")
	woken(t, fw, 2*time.Second, "publications.jsonl replaced by a rename")
	drain(fw)
	appendLine(t, pubs, `{"promise_hash":"after"}`+"\n")
	woken(t, fw, 2*time.Second, "an append to the file that replaced it")
	drain(fw)

	replace(t, filepath.Join(dir, "state.json"), `{"last_scanned_height":9}`)
	woken(t, fw, 2*time.Second, "state.json replaced by a rename")
	drain(fw)

	if err := os.Remove(pubs); err != nil {
		t.Fatal(err)
	}
	woken(t, fw, 2*time.Second, "publications.jsonl removed")
	drain(fw)
	appendLine(t, pubs, `{"promise_hash":"new"}`+"\n")
	woken(t, fw, 2*time.Second, "publications.jsonl made again")
}

// A directory that cannot be watched is an error at the start, which the
// collector logs once before it reads on the timer alone.
func TestAWatchThatCannotStartIsAnError(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "no-such-dir", "publications.jsonl")
	if fw, err := watchFiles([]string{missing}, watchQuiet, watchMaxWait, func(string, ...any) {}); err == nil {
		fw.Close()
		t.Fatal("a watch on a directory that does not exist started")
	} else if !strings.Contains(err.Error(), "no-such-dir") {
		t.Fatalf("error does not name the directory: %v", err)
	}
}

// replace writes body to a temporary file beside path and renames it over
// path, as the rotation and the scanner's state.json both do.
func replace(t *testing.T, path, body string) {
	t.Helper()
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(tmp, path); err != nil {
		t.Fatal(err)
	}
}
