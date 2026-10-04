package main

import (
	"errors"
	"fmt"
	"path/filepath"
	"time"

	"github.com/fsnotify/fsnotify"
)

// fileWatch wakes the fast tick (fast.go) when one of its files changes,
// instead of leaving the change for the next tick of its timer.
//
// The fast tick reads state.json, publications.jsonl and payments.jsonl every
// -fast-every (a second). A blob the scanner wrote just after a tick waited
// for the next one, so on average half a second, and up to a whole one,
// passed between the scanner writing a blob and the store holding it. The
// files say when they change: the kernel reports every write, create and
// rename in their directory (inotify, through fsnotify), and this turns the
// ones that name the fast tick's files into a tick.
//
// The directory, not the files. A file watch follows the file's inode, and
// the record files do not keep theirs: the archive rotation renames a copy of
// a file's tail over the live one, and the scanner writes state.json by
// renaming a new copy over the old. A directory watch sees the new file
// arrive under the same name. Events for every other file in the directory
// (the database's own writes, the prober's measurements) are dropped here, on
// this goroutine, which only reads names, so the collector's loop never sees
// them.
//
// A burst is one tick. A block's records arrive as a few appends to
// publications.jsonl and payments.jsonl, an fsync, and a new state.json, a
// few milliseconds apart; a tick at the first append would store the blob
// and leave its payment and checkpoint to a second tick a moment later. The
// first change in a burst starts a short wait, watchQuiet, that each further
// change starts over, and the tick is due when the files have been quiet for
// that long or watchMaxWait after the first change, whichever comes first:
// a file appended to without pause still gets a tick at least that often.
// The tick itself runs on the collector's own loop, as every tick does, so it
// never writes beside a pass: C holds at most one tick, and a burst while a
// pass runs is one tick after it.
//
// The timer stays. The fast tick still runs every -fast-every, so a change
// this misses (an event the kernel dropped when its queue was full, a watch
// lost to an error) costs what it cost before this existed. An error from
// the watcher wakes a tick at once, since an event may have been lost with
// it, and the directories are watched again; a directory that cannot be
// watched again is retried every watchRetry, and the log says when the watch
// is lost and when it is back, once each.
type fileWatch struct {
	// C receives one value when changes to the files have settled.
	C <-chan struct{}

	c     chan struct{}
	w     *fsnotify.Watcher
	names map[string]bool // the files' cleaned absolute paths
	dirs  []string        // their directories, each once
	logf  func(string, ...any)
	quiet time.Duration
	max   time.Duration
	done  chan struct{}
}

// watchQuiet and watchMaxWait bound a burst: see fileWatch.
const (
	watchQuiet   = 25 * time.Millisecond
	watchMaxWait = 100 * time.Millisecond
	watchRetry   = 10 * time.Second
)

// watchFiles watches the directories of paths, and sends on C when one of
// paths is written, created, replaced or removed. An error is a watch that
// could not start; the caller runs on its timer alone.
func watchFiles(paths []string, quiet, maxWait time.Duration, logf func(string, ...any)) (*fileWatch, error) {
	w, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, err
	}
	c := make(chan struct{}, 1)
	fw := &fileWatch{C: c, c: c, w: w, names: map[string]bool{}, logf: logf, quiet: quiet, max: maxWait, done: make(chan struct{})}
	for _, p := range paths {
		abs, err := filepath.Abs(p)
		if err != nil {
			abs = p
		}
		abs = filepath.Clean(abs)
		fw.names[abs] = true
		dir := filepath.Dir(abs)
		seen := false
		for _, d := range fw.dirs {
			seen = seen || d == dir
		}
		if !seen {
			fw.dirs = append(fw.dirs, dir)
		}
	}
	for _, d := range fw.dirs {
		if err := w.Add(d); err != nil {
			_ = w.Close()
			return nil, fmt.Errorf("watch %s: %w", d, err)
		}
	}
	go fw.loop()
	return fw, nil
}

// Dirs is the directories watched.
func (fw *fileWatch) Dirs() []string { return fw.dirs }

// Close stops the watch.
func (fw *fileWatch) Close() {
	_ = fw.w.Close()
	<-fw.done
}

func (fw *fileWatch) wake() {
	select {
	case fw.c <- struct{}{}:
	default: // one is already waiting for the loop
	}
}

func (fw *fileWatch) loop() {
	defer close(fw.done)
	var (
		settle    *time.Timer
		settleC   <-chan time.Time // non-nil while a burst is settling
		first     time.Time        // its first change
		retryC    <-chan time.Time // non-nil while a directory is not watched
		lost      bool
		overflown bool
	)
	// rearm watches every directory again; it says whether all are.
	rearm := func(cause error) {
		var failed error
		for _, d := range fw.dirs {
			if err := fw.w.Add(d); err != nil {
				failed = fmt.Errorf("watch %s: %w", d, err)
			}
		}
		switch {
		case failed != nil && !lost:
			fw.logf("WARNING: fast tick: file watch lost (%v; %v): reading the files on the timer until it is back", cause, failed)
			lost = true
		case failed == nil && lost:
			fw.logf("fast tick: file watch back")
			lost = false
		}
		retryC = nil
		if lost {
			retryC = time.After(watchRetry)
		}
	}
	for {
		select {
		case ev, ok := <-fw.w.Events:
			if !ok {
				return
			}
			name := filepath.Clean(ev.Name)
			if !fw.names[name] {
				for _, d := range fw.dirs {
					if name == d && (ev.Has(fsnotify.Remove) || ev.Has(fsnotify.Rename)) {
						// the directory itself went, and its watch with it
						rearm(fmt.Errorf("%s removed or renamed", d))
					}
				}
				continue
			}
			if ev.Op == fsnotify.Chmod {
				continue
			}
			now := time.Now()
			if settleC == nil {
				first = now
			}
			wait := fw.quiet
			if left := first.Add(fw.max).Sub(now); left < wait {
				wait = max(left, 0)
			}
			if settle == nil {
				settle = time.NewTimer(wait)
			} else {
				settle.Reset(wait)
			}
			settleC = settle.C
		case <-settleC:
			settleC = nil
			fw.wake()
		case err, ok := <-fw.w.Errors:
			if !ok {
				return
			}
			// whatever the error, an event may have gone with it: a tick
			// finds out, and costs a stat of each file if nothing changed
			fw.wake()
			if errors.Is(err, fsnotify.ErrEventOverflow) && !overflown {
				fw.logf("WARNING: fast tick: the kernel dropped file events (its queue was full); a tick reads whatever they were")
				overflown = true
			}
			rearm(err)
		case <-retryC:
			rearm(errors.New("a directory was not watched"))
			if !lost {
				fw.wake() // the files may have changed while it was not
			}
		}
	}
}
