package probe

import (
	"testing"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/record"
	"github.com/plsgiveup/fibre/fibre-sentinel/internal/scan"
)

// proberAt is testProber over an existing data dir, as a restart sees it.
func proberAt(t *testing.T, dir string) *Prober {
	t.Helper()
	st, err := OpenMeasurementStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	cfg := Config{Vantage: "v1", DataDir: dir}.withDefaults() // no backfill horizon
	p := &Prober{cfg: cfg, log: scan.NewLogger(50), store: st, chainID: "chain-1",
		coders: map[[2]int]*Coder{}, skippedPubs: map[string]bool{}}
	p.initPace()
	return p
}

// A prober restarted after its measurements were archived loads only the
// live file, and must not take an archived reading for one never made:
// with no backfill horizon it would write a NOT_PROBED row for it. A
// publication whose rows may be archived is finished; one read after the
// archive's cutoff has its rows live and is planned as before.
func TestRestartAfterArchiveDoesNotReplanArchivedReadings(t *testing.T) {
	dir := t.TempDir()
	now := time.Now().UTC()
	old := onChain(pub(now.Add(-10*24*time.Hour), now.Add(-10*24*time.Hour+4*time.Hour)), "01d")
	recent := onChain(pub(now.Add(-time.Hour), now.Add(3*time.Hour)), "4ec")

	p := proberAt(t, dir)
	oldPt := ReadPoint(old, p.schedCfg())
	if err := p.store.Append(rowFor(old, "v1", "aa", oldPt)); err != nil {
		t.Fatal(err)
	}
	if err := p.store.Append(rowFor(recent, "v1", "aa", SchedulePoint{At: now})); err != nil {
		t.Fatal(err)
	}
	p.store.Close()

	res, err := record.Archive(p.store.Path(), record.Options{
		Cutoff: now.Add(-7 * 24 * time.Hour), TimeField: "scheduled_at", Limit: -1})
	if err != nil || res.Lines != 1 {
		t.Fatalf("archive: %+v %v", res, err)
	}

	r := proberAt(t, dir)
	if r.store.HandledPoint("v1", old.PromiseHash, oldPt.At) {
		t.Fatal("an archived row is in the restart index; the test proves nothing")
	}
	// Without the archive's horizon the old reading looks never made: this
	// is the duplicate the horizon prevents.
	if _, missed, _ := r.planReads([]scan.Publication{old}, now); len(missed) != 1 {
		t.Fatalf("control: %d missed without the horizon, want 1", len(missed))
	}

	r.liveSince = record.LiveSince(r.store.Path())
	if r.liveSince.IsZero() {
		t.Fatal("no live-since read from the archive index")
	}
	due, missed, finished := r.planReads([]scan.Publication{old, recent}, now)
	if len(missed) != 0 || len(finished) != 1 || finished[0] != old.PromiseHash {
		t.Fatalf("missed %v finished %v", hashes(missed), finished)
	}
	if len(due) != 1 || due[0].pub.PromiseHash != recent.PromiseHash {
		t.Fatalf("the live publication was not planned: %v", hashes(due))
	}
}

// archivedFrom draws the line at the earlier of the reading and the
// settlement, less the clock skew.
func TestArchivedFrom(t *testing.T) {
	since := time.Date(2026, 9, 18, 0, 0, 0, 0, time.UTC)
	p := pub(since.Add(2*time.Hour), since.Add(6*time.Hour))
	pt := ReadPoint(p, ScheduleConfig{})
	if archivedFrom(p, pt, time.Time{}) {
		t.Fatal("nothing archived yet, yet archived")
	}
	if archivedFrom(p, pt, since) {
		t.Fatal("settled two hours after the cutoff: every row is live")
	}
	p = pub(since.Add(30*time.Minute), since.Add(4*time.Hour))
	if !archivedFrom(p, ReadPoint(p, ScheduleConfig{}), since) {
		t.Fatal("within the clock skew of the cutoff: a row may be archived")
	}
}
