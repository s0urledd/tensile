package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/pace"
	"github.com/plsgiveup/fibre/fibre-sentinel/internal/record"
)

// TestMain keeps the test runner's own disk out of every test: a run reads
// the I/O pressure through pressure, and on a busy Linux runner
// /proc/pressure/io would pause the runs the tests make. The tests about
// the pace put their own source there.
func TestMain(m *testing.M) {
	pressure = quiet
	os.Exit(m.Run())
}

func quiet() (pace.Pressure, error) { return pace.Pressure{}, nil }

// pacing is a disk that is busy at the start of every wait and calm at the
// next reading, so that each wait pauses once and is logged, with a short
// poll; reads records each reading as an event beside the ones the test
// adds, and inside says whether a lock-holding step is running, which no
// reading may happen during.
type pacing struct {
	t      *testing.T
	n      int
	events []string
	inside string
}

func newPacing(t *testing.T) *pacing {
	p := &pacing{t: t}
	pressure, pollEvery = p.read, time.Millisecond
	t.Cleanup(func() { pressure, pollEvery = quiet, pace.DefaultPoll })
	return p
}

func (p *pacing) read() (pace.Pressure, error) {
	if p.inside != "" {
		p.t.Errorf("the I/O pressure was read during %s", p.inside)
	}
	p.n++
	p.events = append(p.events, "pressure")
	if p.n%2 == 1 {
		return pace.Pressure{Some: 9, Full: 1}, nil
	}
	return pace.Pressure{Some: 0.5}, nil
}

// An archive run waits for a calm disk before each file, and never while
// record.Archive runs: the pace is read before each call, logged with the
// pressure, and not at all with -pace-some 0. Pace settings under which a
// pause could not end but by its longest are refused.
func TestArchivePacesBetweenFiles(t *testing.T) {
	dir := dataDir(t, 12)
	p := newPacing(t)
	var archived []string
	archiveFile = func(path string, o record.Options) (record.Result, error) {
		name, _ := filepath.Rel(dir, path)
		name = filepath.ToSlash(name)
		p.inside = "record.Archive of " + name
		defer func() { p.inside = "" }()
		p.events = append(p.events, "archive "+name)
		archived = append(archived, name)
		return record.Result{File: filepath.Base(path), Skipped: "stand-in"}, nil
	}
	t.Cleanup(func() { archiveFile = record.Archive })

	code, out, errs := runArgs(t, "-data-dir", dir, "-pace-calm-for", "0")
	if code != 0 || len(archived) != len(Files) {
		t.Fatalf("%d, archived %v\n%s%s", code, archived, out, errs)
	}
	var want []string
	for _, f := range Files {
		want = append(want, "pressure", "pressure", "archive "+f.Name)
		for _, line := range []string{
			"pace| archiving " + f.Name + ": the disk is busy (some avg10 9.00 > 6, full avg10 1.00); pausing until some avg10 stays below 2 for 0s, 30m0s at most\n",
			"pace| archiving " + f.Name + ": going on after ",
		} {
			if !strings.Contains(out, line) {
				t.Fatalf("no %q in:\n%s", line, out)
			}
		}
	}
	if strings.Join(p.events, ",") != strings.Join(want, ",") {
		t.Fatalf("events %v, want %v", p.events, want)
	}
	if !strings.Contains(out, fmt.Sprintf("pace| paused %d time(s), ", len(Files))) {
		t.Fatalf("no summary:\n%s", out)
	}

	p.events, archived = nil, nil
	code, out, errs = runArgs(t, "-data-dir", dir, "-pace-some", "0")
	if code != 0 || len(archived) != len(Files) || p.n != 2*len(Files) || strings.Contains(out, "pace|") {
		t.Fatalf("-pace-some 0: %d, %d reading(s) in all, archived %v\n%s%s", code, p.n, archived, out, errs)
	}

	for _, bad := range [][]string{{"-pace-calm", "0"}, {"-pace-max-wait", "0"}, {"-pace-full", "-1"}, {"-pace-calm-for", "-1s"}} {
		if code, _, errs := runArgs(t, append([]string{"-data-dir", dir}, bad...)...); code != 2 || !strings.Contains(errs, "pace:") {
			t.Fatalf("%v accepted: %d %s", bad, code, errs)
		}
	}
}

// A -retire run waits for a calm disk before each export it reads, inside
// the store check of a day the ledger does not hold, and right before each
// segment it retires, and never while record.Retire runs, which holds
// archive/.lock; it retires what it retires unpaced.
func TestRetirePacesWhereItHoldsNothing(t *testing.T) {
	d, retire := archivedByHand(t)
	p := newPacing(t)
	retireSegment = func(path, segment string, r record.Retired) error {
		name, _ := filepath.Rel(d.dir, path)
		name = filepath.ToSlash(name)
		p.inside = "record.Retire of " + name + " " + segment
		defer func() { p.inside = "" }()
		p.events = append(p.events, "retire "+name+" "+segment)
		return retire(path, segment, r)
	}
	t.Cleanup(func() { retireSegment = record.Retire })

	code, out, errs := runAt(t, retireAt, "-data-dir", d.dir, "-retire", "-db", d.db, "-pace-calm-for", "0")
	var rep Report
	raw, err := os.ReadFile(filepath.Join(d.dir, record.Dir, ReportFile))
	if err == nil {
		err = json.Unmarshal(raw, &rep)
	}
	if code != 0 || err != nil || rep.RetiredNow != 9 || len(rep.Checked) != 2 {
		t.Fatalf("%d %v %+v\n%s%s", code, err, rep, out, errs)
	}
	for _, line := range []string{
		"pace| reading the 2026-10-01 export: the disk is busy (some avg10 9.00 > 6, ",
		"pace| checking the 2026-10-01 export against the store: the disk is busy (",
		"pace| checking the 2026-10-02 export against the store: the disk is busy (",
	} {
		if !strings.Contains(out, line) {
			t.Fatalf("no %q in:\n%s", line, out)
		}
	}
	// Each segment retired was waited for, and nothing came between the wait
	// and record.Retire.
	retired := 0
	for i, ev := range p.events {
		name, ok := strings.CutPrefix(ev, "retire ")
		if !ok {
			continue
		}
		retired++
		if i < 2 || p.events[i-1] != "pressure" || p.events[i-2] != "pressure" {
			t.Fatalf("%s was not waited for right before: %v", ev, p.events[max(i-3, 0):i+1])
		}
		if !strings.Contains(out, "pace| retiring "+name+": the disk is busy (") {
			t.Fatalf("no wait logged before retiring %s:\n%s", name, out)
		}
	}
	if retired != 9 {
		t.Fatalf("%d segment(s) retired, want 9: %v", retired, p.events)
	}
	d.readsWhole(t)
}
