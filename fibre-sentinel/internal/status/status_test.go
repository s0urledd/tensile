package status

import (
	"path/filepath"
	"testing"
	"time"
)

func TestWriterRoundTrip(t *testing.T) {
	dir := t.TempDir()
	w := New(dir, "scanner", "eu1", "1.0")
	w.Start()
	w.Progress(42)
	w.Error("boom")
	w.Set("rows", 7)
	time.Sleep(1200 * time.Millisecond) // the delayed write lands within a second
	rs, err := ReadAll(dir)
	if err != nil || len(rs) != 1 {
		t.Fatalf("read: %v %d", err, len(rs))
	}
	r := rs[0]
	if r.Component != "scanner" || r.Vantage != "eu1" || r.Height != 42 || r.OK || r.LastError != "boom" || r.Detail["rows"] != float64(7) {
		t.Fatalf("report: %+v", r)
	}
	if !r.Alive(time.Now()) {
		t.Fatal("a fresh file must read as alive")
	}
	if diskSupported && (r.Disk == nil || r.Disk.TotalBytes == 0) {
		t.Fatal("disk figures missing")
	}
	w.OK()
	w.Stop("test")
	rs, _ = ReadAll(dir)
	if rs[0].StoppedAt == nil || rs[0].StopReason != "test" || !rs[0].OK || rs[0].LastError != "boom" {
		t.Fatalf("after stop: %+v", rs[0])
	}
	if rs[0].Alive(time.Now()) {
		t.Fatal("a stopped process must not read as alive")
	}
	if rs[0].Alive(time.Now().Add(StaleAfter)) {
		t.Fatal("an old file must not read as alive")
	}
	if _, err := ReadAll(filepath.Join(dir, "nope")); err != nil {
		t.Fatalf("missing dir must be empty, not an error: %v", err)
	}
	var nilW *Writer
	nilW.OK()
	nilW.Error("x")
	nilW.Stop("x")
	New("", "x", "", "").OK() // no dir: records nothing, never panics
}

// A main states a detail its library's writer starts with (the scanner's
// cadence), and a reader gets the cadence back from the file as a duration.
func TestDefaultsReachTheWriterAndTheCadenceReadsBack(t *testing.T) {
	dir := t.TempDir()
	SetDefault(CadenceKey, 60)
	defer func() {
		defaults.mu.Lock()
		delete(defaults.m, CadenceKey)
		defaults.mu.Unlock()
	}()
	w := New(dir, "scanner", "", "t")
	w.Start()
	defer w.Stop("test")
	if d, ok := (Report{Detail: w.r.Detail}).Cadence(); !ok || d != time.Minute {
		t.Fatalf("in-process cadence: %s %v", d, ok)
	}
	r, ok := ReadOne(filepath.Join(dir, "status"), "scanner")
	if !ok {
		t.Fatal("no status file")
	}
	if d, ok := r.Cadence(); !ok || d != time.Minute {
		t.Fatalf("cadence read back: %s %v (detail %v)", d, ok, r.Detail)
	}
	if _, ok := (Report{}).Cadence(); ok {
		t.Fatal("a report without a cadence must say so")
	}
	if New(dir, "other", "", "t").r.Detail[CadenceKey] != 60 {
		t.Fatal("every writer made after SetDefault starts with it")
	}
}
