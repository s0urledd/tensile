// Package pace holds a heavy job back while the disk is busy with other
// work. The observer's disk is shared with a validator, and on NVMe with the
// "none" I/O scheduler neither IOSchedulingClass=idle nor ionice lowers a
// job's reads or writes: the device takes requests in the order they come.
// What holds is to stop issuing them. The kernel's pressure stall
// information for I/O (/proc/pressure/io) says what share of the last ten
// seconds some task ("some") or every non-idle task at once ("full") spent
// waiting on I/O; a Pacer reads it at the points where its caller may stop,
// and waits there while it is high.
//
// The rule is the one the deploys pause by: hold while "some avg10" is above
// Some or "full avg10" above Full, and go on once "some avg10" has stayed
// below Calm for CalmFor, reading it every Poll. A pause never lasts longer
// than MaxWait: the job is due every night, and one that waited for good
// would leave the live files growing and nothing retired. It then goes on
// and says so.
package pace

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// ProcFile is where Linux reports the pressure on I/O. It does not exist on
// other systems, nor on a kernel built or booted without pressure stall
// information.
const ProcFile = "/proc/pressure/io"

// DefaultPoll is how often a pause reads the pressure again: avg10 moves
// over ten seconds, so a few seconds between readings miss nothing.
const DefaultPoll = 5 * time.Second

// The defaults are the rule the deploys pause their own heavy work by: hold
// while "some avg10" is above 6 or "full avg10" above 4, and go on once
// "some avg10" has stayed below 2 for 20 seconds. No single pause lasts
// longer than half an hour: a nightly job that waited for as long as the
// validator kept the disk busy might not end at all.
const (
	DefaultSome    = 6.0
	DefaultFull    = 4.0
	DefaultCalm    = 2.0
	DefaultCalmFor = 20 * time.Second
	DefaultMaxWait = 30 * time.Minute
)

// Pressure is one reading: avg10 of the "some" and the "full" line, in
// percent of the last ten seconds.
type Pressure struct {
	Some, Full float64
}

// Source reads the pressure now. An error that is os.ErrNotExist means there
// is none to read, and a Pacer then never waits.
type Source func() (Pressure, error)

// Proc reads ProcFile.
func Proc() (Pressure, error) {
	raw, err := os.ReadFile(ProcFile)
	if err != nil {
		return Pressure{}, err
	}
	return Parse(raw)
}

// Parse reads avg10 of the "some" and "full" lines of a pressure file:
//
//	some avg10=1.53 avg60=0.87 avg300=0.40 total=12345678
//	full avg10=0.20 avg60=0.10 avg300=0.05 total=2345678
//
// A file without a "full" line reads as full 0 (the cpu file had none before
// Linux 5.13); one without a "some" line, or a line without a readable
// avg10, is an error rather than a quiet disk.
func Parse(raw []byte) (Pressure, error) {
	var p Pressure
	some := false
	for _, line := range strings.Split(string(raw), "\n") {
		f := strings.Fields(line)
		if len(f) == 0 {
			continue
		}
		var dst *float64
		switch f[0] {
		case "some":
			dst, some = &p.Some, true
		case "full":
			dst = &p.Full
		default:
			continue
		}
		found := false
		for _, kv := range f[1:] {
			v, ok := strings.CutPrefix(kv, "avg10=")
			if !ok {
				continue
			}
			x, err := strconv.ParseFloat(v, 64)
			if err != nil {
				return Pressure{}, fmt.Errorf("%s avg10: %w", f[0], err)
			}
			*dst, found = x, true
		}
		if !found {
			return Pressure{}, fmt.Errorf("a %q line without avg10", f[0])
		}
	}
	if !some {
		return Pressure{}, errors.New(`no "some" line`)
	}
	return p, nil
}

// Pacer waits while the disk is busy (Wait). A nil Pacer, or one whose Some
// is 0, never waits. One goroutine uses a Pacer at a time.
type Pacer struct {
	// Some and Full: a pause starts when "some avg10" is above Some or
	// "full avg10" is above Full. Full 0 leaves "full" out.
	Some, Full float64
	// Calm and CalmFor: a pause ends once "some avg10" has stayed below Calm
	// for CalmFor.
	Calm    float64
	CalmFor time.Duration
	// MaxWait: a pause ends after this long whatever the pressure, and the
	// log says it did.
	MaxWait time.Duration
	// Poll is how often a pause reads the pressure (DefaultPoll when 0).
	Poll time.Duration
	// Source reads the pressure (Proc when nil).
	Source Source
	// Log takes a line when a pause starts, with the pressure that started
	// it, and one when it ends, with how long it lasted and why it ended.
	// Nil logs nothing.
	Log func(string)

	// now and sleep are the clock; the tests put a fake one here.
	now   func() time.Time
	sleep func(context.Context, time.Duration) error

	warned bool
	pauses int
	paused time.Duration
}

// Check refuses settings under which a pause could never end but by
// MaxWait, or would end at once.
func (p *Pacer) Check() error {
	switch {
	case p == nil || p.Some == 0:
		return nil
	case p.Some < 0 || p.Full < 0 || p.Calm < 0 || p.CalmFor < 0 || p.MaxWait < 0 || p.Poll < 0:
		return errors.New("pace: no setting may be negative")
	case p.Calm == 0:
		return errors.New(`pace: a calm of 0 is never reached ("some avg10" is never below 0), so every pause would last the longest it may`)
	case p.MaxWait == 0:
		return errors.New("pace: a longest pause of 0 would end every pause as it starts")
	}
	return nil
}

// Wait returns at once while the disk is not busy. Otherwise it holds the
// caller until the disk is calm again or MaxWait has passed, and logs the
// pause. what names the work held back ("archiving measurements.jsonl").
//
// The error is ctx's, when ctx ends first. A pressure that cannot be read
// never stops the caller: the pace spares the validator, and the work must
// still be done. The first such error is logged, once; a missing ProcFile is
// not an error, there is just nothing to pace by.
func (p *Pacer) Wait(ctx context.Context, what string) error {
	if p == nil || p.Some <= 0 {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	at, err := p.read()
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) && !p.warned {
			p.warned = true
			p.logf("%s: the I/O pressure could not be read (%v); going on, and not pausing on any later read that fails either", what, err)
		}
		return nil
	}
	if !p.busy(at) {
		return nil
	}
	start := p.clock()
	p.logf("%s: the disk is busy (%s); pausing until some avg10 stays below %s for %s, %s at most", what, p.describe(at), num(p.Calm), p.CalmFor, p.MaxWait)
	peak := at.Some
	var calmSince time.Time
	calm := false
	for {
		left := p.MaxWait - p.clock().Sub(start)
		if err := p.doSleep(ctx, max(min(p.poll(), left), 0)); err != nil {
			p.logf("%s: stopped after %s paused: %v", what, p.end(start), err)
			return err
		}
		now := p.clock()
		at, err = p.read()
		if err != nil {
			p.logf("%s: going on after %s paused: the I/O pressure could not be read (%v)", what, p.end(start), err)
			return nil
		}
		peak = max(peak, at.Some)
		if at.Some < p.Calm {
			if !calm {
				calm, calmSince = true, now
			}
			if now.Sub(calmSince) >= p.CalmFor {
				p.logf("%s: going on after %s paused (%s; some avg10 was %s at most)", what, p.end(start), p.values(at), num2(peak))
				return nil
			}
		} else {
			calm = false
		}
		if now.Sub(start) >= p.MaxWait {
			p.logf("%s: going on after %s paused, the longest a pause may last, though the disk was not calm for %s (%s; some avg10 was %s at most)", what, p.end(start), p.CalmFor, p.values(at), num2(peak))
			return nil
		}
	}
}

// Paused is how many pauses there were and how long they lasted in all.
func (p *Pacer) Paused() (int, time.Duration) {
	if p == nil {
		return 0, 0
	}
	return p.pauses, p.paused
}

func (p *Pacer) busy(at Pressure) bool {
	return at.Some > p.Some || (p.Full > 0 && at.Full > p.Full)
}

// describe is a reading with the threshold each value is over.
func (p *Pacer) describe(at Pressure) string {
	some, full := "some avg10 "+num2(at.Some), "full avg10 "+num2(at.Full)
	if at.Some > p.Some {
		some += " > " + num(p.Some)
	}
	if p.Full > 0 && at.Full > p.Full {
		full += " > " + num(p.Full)
	}
	return some + ", " + full
}

func (p *Pacer) values(at Pressure) string {
	return "some avg10 " + num2(at.Some) + ", full avg10 " + num2(at.Full)
}

// end counts a pause that started at start and ends now, and is how long it
// lasted, for the log.
func (p *Pacer) end(start time.Time) time.Duration {
	d := p.clock().Sub(start)
	p.pauses++
	p.paused += d
	if d >= time.Second {
		return d.Round(time.Second)
	}
	return d.Round(time.Millisecond)
}

func (p *Pacer) read() (Pressure, error) {
	if p.Source != nil {
		return p.Source()
	}
	return Proc()
}

func (p *Pacer) poll() time.Duration {
	if p.Poll > 0 {
		return p.Poll
	}
	return DefaultPoll
}

func (p *Pacer) clock() time.Time {
	if p.now != nil {
		return p.now()
	}
	return time.Now()
}

func (p *Pacer) doSleep(ctx context.Context, d time.Duration) error {
	if p.sleep != nil {
		return p.sleep(ctx, d)
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

func (p *Pacer) logf(format string, a ...any) {
	if p.Log != nil {
		p.Log(fmt.Sprintf(format, a...))
	}
}

// num is a threshold as it was given (6, 0.5); num2 a reading as the kernel
// gives it (8.21).
func num(x float64) string  { return strconv.FormatFloat(x, 'f', -1, 64) }
func num2(x float64) string { return strconv.FormatFloat(x, 'f', 2, 64) }
