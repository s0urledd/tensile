package probe

import (
	"testing"
	"time"
)

// A blob is read ten minutes before its must_serve_until, and the reading
// may start up to three minutes before it.
func TestReadPoint(t *testing.T) {
	settle := time.Date(2026, 9, 29, 0, 0, 0, 0, time.UTC)
	msu := settle.Add(4 * time.Hour)
	p := pub(settle, msu)
	cfg := ScheduleConfig{}
	pt := ReadPoint(p, cfg)
	if !pt.At.Equal(msu.Add(-10*time.Minute)) || pt.Label != EndReadLabel || pt.Phase != PhaseInWindow {
		t.Fatalf("point = %+v", pt)
	}
	if last := latestStart(p, pt, cfg); !last.Equal(msu.Add(-3 * time.Minute)) {
		t.Fatalf("latest start %s", last)
	}
}

// A window shorter than the offset is read half way through, and may start
// until half way from there to its end.
func TestReadPointShortWindow(t *testing.T) {
	settle := time.Date(2026, 9, 29, 0, 0, 0, 0, time.UTC)
	p := pub(settle, settle.Add(6*time.Minute))
	pt := ReadPoint(p, ScheduleConfig{})
	if !pt.At.Equal(settle.Add(3 * time.Minute)) {
		t.Fatalf("point at %s", pt.At.Sub(settle))
	}
	if last := latestStart(p, pt, ScheduleConfig{}); !last.Equal(settle.Add(4*time.Minute + 30*time.Second)) {
		t.Fatalf("latest start at %s", last.Sub(settle))
	}
}

// A record whose settlement is not before its must_serve_until gets a
// window of its own retention, not a constant.
func TestReadPointDegenerateWindowUsesRecordParams(t *testing.T) {
	msu := time.Now().UTC()
	p := pub(msu.Add(time.Minute), msu)
	p.ParamsAtPublication.ShardRetentionSeconds = 3 * 60
	pt := ReadPoint(p, ScheduleConfig{})
	if want := msu.Add(-90 * time.Second); !pt.At.Equal(want) {
		t.Fatalf("point %s before must_serve_until, want 90s", msu.Sub(pt.At))
	}
}
