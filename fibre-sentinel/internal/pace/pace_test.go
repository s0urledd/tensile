package pace

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"strings"
	"testing"
	"time"
)

// fakeClock is the time a Pacer sees: a sleep moves it on at once.
type fakeClock struct {
	t     time.Time
	slept time.Duration
}

func (c *fakeClock) now() time.Time { return c.t }

func (c *fakeClock) sleep(ctx context.Context, d time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.slept += d
	c.t = c.t.Add(d)
	return nil
}

// script is a Source that gives the readings in order, one per read, and
// the last one from then on; reads counts them.
type script struct {
	readings []Pressure
	reads    int
}

func (s *script) read() (Pressure, error) {
	r := s.readings[min(s.reads, len(s.readings)-1)]
	s.reads++
	return r, nil
}

// paced is a Pacer with the defaults observer-archive gives it, reading src
// on a fake clock, and the lines it logs.
func paced(src Source) (*Pacer, *fakeClock, *[]string) {
	c := &fakeClock{t: time.Date(2026, 10, 7, 4, 40, 0, 0, time.UTC)}
	var log []string
	p := &Pacer{Some: 6, Full: 4, Calm: 2, CalmFor: 20 * time.Second, MaxWait: 30 * time.Minute, Poll: 5 * time.Second,
		Source: src, Log: func(s string) { log = append(log, s) }}
	p.now, p.sleep = c.now, c.sleep
	return p, c, &log
}

func busy(some float64) Pressure { return Pressure{Some: some} }

// The pressure is read from the file's own format: avg10 of the "some" and
// "full" lines, a missing "full" line as 0, and anything else as an error
// rather than a quiet disk.
func TestParse(t *testing.T) {
	p, err := Parse([]byte("some avg10=8.21 avg60=3.10 avg300=0.94 total=123456789\nfull avg10=4.50 avg60=1.00 avg300=0.20 total=23456789\n"))
	if err != nil || p != (Pressure{Some: 8.21, Full: 4.5}) {
		t.Fatalf("%+v %v", p, err)
	}
	if p, err := Parse([]byte("some avg10=0.00 avg60=0.00 avg300=0.00 total=0\n")); err != nil || p != (Pressure{}) {
		t.Fatalf("no full line: %+v %v", p, err)
	}
	for _, bad := range []string{"", "full avg10=1.00 avg60=0.00 avg300=0.00 total=0\n", "some avg10=x avg60=0.00\n", "some avg60=0.00 total=0\n"} {
		if p, err := Parse([]byte(bad)); err == nil {
			t.Fatalf("%q read as %+v", bad, p)
		}
	}
}

// A disk at or under the thresholds holds nothing back: one read, no
// sleep, no line in the log.
func TestQuietDiskGoesOnAtOnce(t *testing.T) {
	for _, r := range []Pressure{{}, {Some: 6, Full: 4}, {Some: 5.99, Full: 3.5}} {
		s := &script{readings: []Pressure{r}}
		p, c, log := paced(s.read)
		if err := p.Wait(context.Background(), "archiving measurements.jsonl"); err != nil || s.reads != 1 || c.slept != 0 || len(*log) != 0 {
			t.Fatalf("%+v: %v, %d read(s), slept %s, log %q", r, err, s.reads, c.slept, *log)
		}
	}
}

// A busy disk holds the caller until "some avg10" has stayed below the calm
// level for the whole calm time: a reading above it in between starts the
// calm time again. The pause is logged with the pressure that started it
// and how long it lasted.
func TestPausesUntilCalmLongEnough(t *testing.T) {
	// read at 0s, then every 5s: calm from 10s, broken at 20s, calm again
	// from 25s, which is 20s long at 45s
	s := &script{readings: []Pressure{busy(8.5), busy(7), busy(1.5), busy(1), busy(3), busy(1), busy(1), busy(1), busy(1), busy(0.5)}}
	p, c, log := paced(s.read)
	if err := p.Wait(context.Background(), "archiving measurements.jsonl"); err != nil {
		t.Fatal(err)
	}
	if c.slept != 45*time.Second || s.reads != 10 {
		t.Fatalf("paused %s over %d read(s), want 45s over 10", c.slept, s.reads)
	}
	want := []string{
		"archiving measurements.jsonl: the disk is busy (some avg10 8.50 > 6, full avg10 0.00); pausing until some avg10 stays below 2 for 20s, 30m0s at most",
		"archiving measurements.jsonl: going on after 45s paused (some avg10 0.50, full avg10 0.00; some avg10 was 8.50 at most)",
	}
	if strings.Join(*log, "\n") != strings.Join(want, "\n") {
		t.Fatalf("log:\n%s\nwant:\n%s", strings.Join(*log, "\n"), strings.Join(want, "\n"))
	}
	if n, d := p.Paused(); n != 1 || d != 45*time.Second {
		t.Fatalf("paused %d time(s), %s", n, d)
	}
	// With no calm time, the first calm reading ends the pause.
	s = &script{readings: []Pressure{busy(9), busy(1.9)}}
	p, c, _ = paced(s.read)
	p.CalmFor = 0
	if err := p.Wait(context.Background(), "x"); err != nil || c.slept != 5*time.Second || s.reads != 2 {
		t.Fatalf("no calm time: %v, slept %s, %d read(s)", err, c.slept, s.reads)
	}
}

// "full avg10" over its threshold starts a pause on its own, and the log
// says which value was over; with Full 0 it is left out.
func TestFullAloneStartsAPause(t *testing.T) {
	s := &script{readings: []Pressure{{Some: 5, Full: 4.5}, {}}}
	p, c, log := paced(s.read)
	p.CalmFor = 0
	if err := p.Wait(context.Background(), "checking the 2026-10-05 export against the store"); err != nil || c.slept == 0 {
		t.Fatalf("%v, slept %s", err, c.slept)
	}
	if !strings.Contains((*log)[0], "(some avg10 5.00, full avg10 4.50 > 4)") {
		t.Fatalf("log: %q", *log)
	}
	s = &script{readings: []Pressure{{Some: 5, Full: 4.5}}}
	p, c, _ = paced(s.read)
	p.Full = 0
	if err := p.Wait(context.Background(), "x"); err != nil || c.slept != 0 {
		t.Fatalf("full left out: %v, slept %s", err, c.slept)
	}
}

// A disk that stays busy holds the caller for MaxWait and no longer; the
// caller then goes on, and the log says it went on with the disk still
// busy.
func TestLongestPauseThenGoesOn(t *testing.T) {
	s := &script{readings: []Pressure{busy(12), {Some: 7.25, Full: 3}}}
	p, c, log := paced(s.read)
	if err := p.Wait(context.Background(), "retiring measurements.jsonl 000004-2026-10-04.jsonl.gz"); err != nil {
		t.Fatal(err)
	}
	if c.slept != 30*time.Minute || s.reads != 1+30*60/5 {
		t.Fatalf("paused %s over %d read(s), want 30m over %d", c.slept, s.reads, 1+30*60/5)
	}
	want := "retiring measurements.jsonl 000004-2026-10-04.jsonl.gz: going on after 30m0s paused, the longest a pause may last, though the disk was not calm for 20s (some avg10 7.25, full avg10 3.00; some avg10 was 12.00 at most)"
	if len(*log) != 2 || (*log)[1] != want {
		t.Fatalf("log: %q", *log)
	}
	// A cap that is not a whole number of polls is kept to.
	s = &script{readings: []Pressure{busy(12)}}
	p, c, _ = paced(s.read)
	p.MaxWait = 12 * time.Second
	if err := p.Wait(context.Background(), "x"); err != nil || c.slept != 12*time.Second {
		t.Fatalf("12s cap: %v, slept %s", err, c.slept)
	}
}

// Where there is no pressure to read (no /proc/pressure/io: not Linux, or no
// pressure stall information) nothing is paused or logged; a pressure that
// cannot be read for another reason never stops the work either, and is
// logged once. A nil Pacer, or one with Some 0, reads nothing.
func TestNothingToReadNeverHolds(t *testing.T) {
	missing := func() (Pressure, error) {
		return Pressure{}, &fs.PathError{Op: "open", Path: ProcFile, Err: fs.ErrNotExist}
	}
	p, c, log := paced(missing)
	for i := 0; i < 3; i++ {
		if err := p.Wait(context.Background(), "x"); err != nil {
			t.Fatal(err)
		}
	}
	if c.slept != 0 || len(*log) != 0 {
		t.Fatalf("no pressure file: slept %s, log %q", c.slept, *log)
	}
	broken := func() (Pressure, error) { return Pressure{}, errors.New("permission denied") }
	p, c, log = paced(broken)
	for i := 0; i < 3; i++ {
		if err := p.Wait(context.Background(), "x"); err != nil {
			t.Fatal(err)
		}
	}
	if c.slept != 0 || len(*log) != 1 || !strings.Contains((*log)[0], "could not be read (permission denied)") {
		t.Fatalf("unreadable: slept %s, log %q", c.slept, *log)
	}
	// One that stops being readable during a pause ends it.
	reads := 0
	flaky := func() (Pressure, error) {
		reads++
		if reads == 1 {
			return busy(10), nil
		}
		return Pressure{}, errors.New("read error")
	}
	p, c, log = paced(flaky)
	if err := p.Wait(context.Background(), "x"); err != nil || c.slept != 5*time.Second || len(*log) != 2 ||
		!strings.Contains((*log)[1], "going on after 5s paused: the I/O pressure could not be read (read error)") {
		t.Fatalf("unreadable during a pause: %v, slept %s, log %q", err, c.slept, *log)
	}

	var none *Pacer
	if err := none.Wait(context.Background(), "x"); err != nil {
		t.Fatal(err)
	}
	if n, d := none.Paused(); n != 0 || d != 0 {
		t.Fatal(n, d)
	}
	s := &script{readings: []Pressure{busy(50)}}
	p, c, _ = paced(s.read)
	p.Some = 0
	if err := p.Wait(context.Background(), "x"); err != nil || s.reads != 0 || c.slept != 0 {
		t.Fatalf("off: %v, %d read(s), slept %s", err, s.reads, c.slept)
	}
}

// A context that ends during a pause ends the wait with its error.
func TestContextEndsAPause(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	reads := 0
	src := func() (Pressure, error) {
		reads++
		if reads == 3 {
			cancel()
		}
		return busy(10), nil
	}
	p, _, log := paced(src)
	if err := p.Wait(ctx, "x"); !errors.Is(err, context.Canceled) || reads != 3 || !strings.Contains((*log)[len(*log)-1], "stopped after 10s paused") {
		t.Fatalf("%v after %d read(s), log %q", err, reads, *log)
	}
	if err := p.Wait(ctx, "x"); !errors.Is(err, context.Canceled) || reads != 3 {
		t.Fatalf("an ended context: %v, %d read(s)", err, reads)
	}
}

// Settings under which a pause could not end but by its longest, or would
// end as it starts, are refused; pacing turned off needs nothing else.
func TestCheck(t *testing.T) {
	ok := Pacer{Some: 6, Full: 4, Calm: 2, CalmFor: 20 * time.Second, MaxWait: 30 * time.Minute}
	if err := ok.Check(); err != nil {
		t.Fatal(err)
	}
	for name, edit := range map[string]func(*Pacer){
		"negative some":  func(p *Pacer) { p.Some = -1 },
		"negative full":  func(p *Pacer) { p.Full = -1 },
		"calm 0":         func(p *Pacer) { p.Calm = 0 },
		"negative calm":  func(p *Pacer) { p.CalmFor = -time.Second },
		"max wait 0":     func(p *Pacer) { p.MaxWait = 0 },
		"negative a max": func(p *Pacer) { p.MaxWait = -time.Second },
	} {
		p := ok
		edit(&p)
		if err := p.Check(); err == nil {
			t.Fatalf("%s accepted", name)
		}
	}
	off := Pacer{}
	if err := off.Check(); err != nil {
		t.Fatal(err)
	}
}

// Proc says os.ErrNotExist where there is no pressure file, and Parse reads
// the kernel's own where there is one.
func TestProc(t *testing.T) {
	raw, err := os.ReadFile(ProcFile)
	switch {
	case errors.Is(err, os.ErrNotExist):
		if _, err := Proc(); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("no %s: %v", ProcFile, err)
		}
	case err != nil:
		t.Skipf("%s is there but cannot be read here: %v", ProcFile, err)
	default:
		if p, err := Parse(raw); err != nil || p.Some < 0 || p.Full < 0 {
			t.Fatalf("%s:\n%s\nread as %+v, %v", ProcFile, raw, p, err)
		}
	}
}
