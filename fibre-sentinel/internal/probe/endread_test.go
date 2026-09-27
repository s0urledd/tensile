package probe

import (
	"testing"
	"time"
)

// Under EndReadOffset the schedule is one reading at the end of the window
// and nothing else: no earlier reading and none after the deadline.
func TestEndReadSchedule(t *testing.T) {
	settle := time.Date(2026, 9, 27, 0, 0, 0, 0, time.UTC)
	msu := settle.Add(4 * time.Hour)
	pts := ScheduleFor(pub(settle, msu), ScheduleConfig{EndReadOffset: 10 * time.Minute})
	if len(pts) != 1 {
		t.Fatalf("got %d points, want 1: %+v", len(pts), pts)
	}
	if pt := pts[0]; !pt.At.Equal(msu.Add(-10*time.Minute)) || pt.Phase != PhaseInWindow || pt.Label != "end" {
		t.Fatalf("point %+v, want in_window \"end\" 10m before must_serve_until", pt)
	}
}

// A publication settled before EndReadSince keeps the schedule it was read
// under, so switching a prober with history to the end reading plans nothing
// new for the past.
func TestEndReadSince(t *testing.T) {
	since := time.Date(2026, 9, 27, 12, 0, 0, 0, time.UTC)
	cfg := ScheduleConfig{EndReadOffset: 10 * time.Minute, EndReadSince: since,
		InWindowFractions: DefaultInWindowFractions}
	old := ScheduleFor(pub(since.Add(-time.Hour), since.Add(3*time.Hour)), cfg)
	if len(old) != len(DefaultInWindowFractions)+2 || old[0].Label != "w1" {
		t.Fatalf("publication settled before the switch: %+v, want its old schedule", old)
	}
	cur := ScheduleFor(pub(since, since.Add(4*time.Hour)), cfg)
	if len(cur) != 1 || cur[0].Label != "end" {
		t.Fatalf("publication settled at the switch: %+v, want the end reading", cur)
	}
}

// A window shorter than the offset is still read once: LastPointMargin
// before its end, or half way through when even that is before settlement.
func TestEndReadScheduleShortWindow(t *testing.T) {
	settle := time.Date(2026, 9, 27, 0, 0, 0, 0, time.UTC)
	cfg := ScheduleConfig{EndReadOffset: 10 * time.Minute}

	msu := settle.Add(8 * time.Minute)
	if pts := ScheduleFor(pub(settle, msu), cfg); len(pts) != 1 || !pts[0].At.Equal(msu.Add(-150*time.Second)) {
		t.Fatalf("8-minute window: %+v, want one point 150s before the end", pts)
	}
	msu = settle.Add(2 * time.Minute)
	if pts := ScheduleFor(pub(settle, msu), cfg); len(pts) != 1 || !pts[0].At.Equal(settle.Add(time.Minute)) {
		t.Fatalf("2-minute window: %+v, want one point half way through", pts)
	}
}

// Under EndorsedOnly only the validators whose signature the settled promise
// carries are read; a promise recorded before signatures were verified says
// nothing either way and is read over every assigned validator.
func TestEndorsedOnly(t *testing.T) {
	p := &Prober{cfg: Config{EndorsedOnly: true}}
	if !p.wants(Target{AddressHex: "aa", Assigned: true, Attested: true}) {
		t.Fatal("an endorsed validator is not read")
	}
	if p.wants(Target{AddressHex: "bb", Assigned: true}) {
		t.Fatal("a validator the settled promise does not name as a signer is read")
	}
	if !p.wants(Target{AddressHex: "cc", Assigned: true, AttestationUnknown: true}) {
		t.Fatal("a promise recorded before signatures were verified is not read")
	}
	all := &Prober{}
	if !all.wants(Target{AddressHex: "bb", Assigned: true}) {
		t.Fatal("without EndorsedOnly an unendorsed validator is not read")
	}
}

// A negative MinDownloadBytesPerSec makes the download deadline flat, as the
// chain's own client holds DownloadShard to RPCTimeout whatever the shard
// weighs; left at zero it keeps the size-scaled default.
func TestFlatDownloadDeadline(t *testing.T) {
	flat := StepTimeouts{Download: 15 * time.Second, MinDownloadBytesPerSec: -1}.withDefaults()
	if got := flat.downloadDeadline(100 << 20); got != 15*time.Second {
		t.Fatalf("flat deadline for 100 MiB = %s, want 15s", got)
	}
	scaled := StepTimeouts{Download: 25 * time.Second}.withDefaults()
	if got := scaled.downloadDeadline(10 << 20); got != 35*time.Second {
		t.Fatalf("scaled deadline for 10 MiB = %s, want 35s", got)
	}
}
