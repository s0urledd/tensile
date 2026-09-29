package probe

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/scan"
)

func testProber(t *testing.T) *Prober {
	t.Helper()
	dir := t.TempDir()
	st, err := OpenMeasurementStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	cfg := Config{Vantage: "v1", DataDir: dir, BackfillMissed: time.Hour}.withDefaults()
	p := &Prober{
		cfg: cfg, log: scan.NewLogger(200), store: st, chainID: "chain-1",
		coders: map[[2]int]*Coder{}, skippedPubs: map[string]bool{},
	}
	p.initPace()
	return p
}

func rowFor(pub scan.Publication, vantage, addr string, pt SchedulePoint) Measurement {
	return Measurement{
		SchemaVersion: MeasurementSchemaVersion, Vantage: vantage, PromiseHash: pub.PromiseHash,
		ValidatorAddress: addr, ScheduledAt: pt.At, ScheduleLabel: pt.Label, StartedAt: pt.At,
		Phase: PhaseInWindow, Outcome: OutcomeServedOK, Classification: ClassHealthy,
	}
}

func onChain(p scan.Publication, hash string) scan.Publication {
	p.PromiseHash = hash
	p.Promise.ChainID = "chain-1"
	return p
}

// Each blob is planned once: queued while its reading can still start,
// recorded as not read once it cannot, and left alone behind the backfill
// horizon, once its reading is on record, or when it was settled before
// the reading began.
func TestPlanReads(t *testing.T) {
	p := testProber(t)
	now := time.Now().UTC()
	open := onChain(pub(now.Add(-time.Hour), now.Add(time.Hour)), "0a")
	closing := onChain(pub(now.Add(-time.Hour), now.Add(2*time.Minute)), "0b")
	old := onChain(pub(now.Add(-5*time.Hour), now.Add(-2*time.Hour)), "0c")
	read := onChain(pub(now.Add(-time.Hour), now.Add(30*time.Minute)), "0d")
	if err := p.store.Append(rowFor(read, "v1", "aa", ReadPoint(read, p.schedCfg()))); err != nil {
		t.Fatal(err)
	}

	due, missed, finished := p.planReads([]scan.Publication{open, closing, old, read}, now)
	if len(due) != 1 || due[0].pub.PromiseHash != "0a" {
		t.Fatalf("due = %v", hashes(due))
	}
	if j := due[0]; !j.start.Equal(open.MustServeUntil.Add(-10*time.Minute)) || !j.latest.Equal(open.MustServeUntil.Add(-3*time.Minute)) {
		t.Errorf("reading at %s, latest start %s", j.start, j.latest)
	}
	if len(missed) != 1 || missed[0].pub.PromiseHash != "0b" {
		t.Fatalf("missed = %v", hashes(missed))
	}
	if len(finished) != 2 || finished[0] != "0c" || finished[1] != "0d" {
		t.Fatalf("finished = %v", finished)
	}

	// With no horizon, a reading never made is recorded however old.
	p.cfg.BackfillMissed = 0
	_, missed, _ = p.planReads([]scan.Publication{old}, now)
	if len(missed) != 1 {
		t.Fatalf("no horizon: missed = %v", hashes(missed))
	}

	// A publication settled before the reading began was read on the
	// schedule of its time.
	p.cfg.Schedule.Since = now
	due, missed, finished = p.planReads([]scan.Publication{onChain(open, "0e")}, now)
	if len(due)+len(missed) != 0 || len(finished) != 1 {
		t.Fatalf("before Since: due %v missed %v finished %v", hashes(due), hashes(missed), finished)
	}
}

// A dry run reads every blob whose window still leaves room for a request
// at once, and nothing else.
func TestPlanReadsReadNow(t *testing.T) {
	p := testProber(t)
	p.cfg.ReadNow = true
	now := time.Now().UTC()
	open := onChain(pub(now.Add(-time.Hour), now.Add(time.Hour)), "0a")
	closing := onChain(pub(now.Add(-time.Hour), now.Add(30*time.Second)), "0b")
	due, missed, finished := p.planReads([]scan.Publication{open, closing}, now)
	if len(due) != 1 || !due[0].start.Equal(now) || len(missed) != 0 || len(finished) != 1 {
		t.Fatalf("due %v missed %v finished %v", hashes(due), hashes(missed), finished)
	}
}

// Publications from another chain or with a failed settlement tx are never read.
func TestPlanReadsSkipsForeignAndFailed(t *testing.T) {
	p := testProber(t)
	now := time.Now().UTC()
	foreign := pub(now.Add(-time.Minute), now.Add(time.Hour))
	foreign.Promise.ChainID = "other-chain"
	failed := onChain(pub(now.Add(-time.Minute), now.Add(time.Hour)), "ff00")
	failed.SettlementTxCode = 5
	due, missed, _ := p.planReads([]scan.Publication{foreign, failed}, now)
	if len(due)+len(missed) != 0 {
		t.Fatalf("foreign/failed publications planned: due=%d missed=%d", len(due), len(missed))
	}
}

func hashes(js []*readJob) []string {
	var out []string
	for _, j := range js {
		out = append(out, j.pub.PromiseHash)
	}
	return out
}

func TestMeasurementStore_TornTailIsRepaired(t *testing.T) {
	dir := t.TempDir()
	st, err := OpenMeasurementStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	pubA := pub(time.Now(), time.Now().Add(time.Hour))
	pt := SchedulePoint{At: time.Now().UTC(), Label: "w1"}
	if err := st.Append(rowFor(pubA, "v1", "aa", pt)); err != nil {
		t.Fatal(err)
	}
	st.Close()
	f, _ := os.OpenFile(filepath.Join(dir, "measurements.jsonl"), os.O_WRONLY|os.O_APPEND, 0)
	f.WriteString(`{"vantage":"v1","promise_hash":"abc`)
	f.Close()

	st2, err := OpenMeasurementStore(dir)
	if err != nil {
		t.Fatalf("torn tail must be repaired, got %v", err)
	}
	defer st2.Close()
	if !st2.Has("v1", pubA.PromiseHash, "aa", pt.At) {
		t.Fatal("intact row lost")
	}
	ms, err := LoadMeasurements(st2.Path())
	if err != nil || len(ms) != 1 {
		t.Fatalf("after repair: %d rows, err %v", len(ms), err)
	}
	// a second append lands on a clean line
	if err := st2.Append(rowFor(pubA, "v1", "bb", pt)); err != nil {
		t.Fatal(err)
	}
	ms, err = LoadMeasurements(st2.Path())
	if err != nil || len(ms) != 2 {
		t.Fatalf("after append: %d rows, err %v", len(ms), err)
	}
	st2.Forget(pubA.PromiseHash)
	if st2.Has("v1", pubA.PromiseHash, "aa", pt.At) {
		t.Fatal("Forget did not drop the keys")
	}
}

func TestPubFeed_IncrementalAndTornLine(t *testing.T) {
	path := filepath.Join(t.TempDir(), "publications.jsonl")
	feed := newPubFeed(path)
	if _, err := feed.refresh(); err == nil {
		t.Fatal("missing file should error")
	}
	write := func(s string) {
		f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			t.Fatal(err)
		}
		f.WriteString(s)
		f.Close()
	}
	write(`{"promise_hash":"aa","settlement_height":1}` + "\n")
	write(`{"promise_hash":"bb","settlement_hei`) // torn / in progress
	n, err := feed.refresh()
	if err != nil || n != 1 {
		t.Fatalf("first refresh: n=%d err=%v", n, err)
	}
	write(`ght":2}` + "\n")
	n, err = feed.refresh()
	if err != nil || n != 1 {
		t.Fatalf("second refresh: n=%d err=%v", n, err)
	}
	if all := feed.all(); len(all) != 2 || all[1].PromiseHash != "bb" || all[1].SettlementHeight != 2 {
		t.Fatalf("all = %+v", all)
	}
	feed.forget("aa")
	if all := feed.all(); len(all) != 1 || all[0].PromiseHash != "bb" {
		t.Fatalf("after forget = %+v", all)
	}
	// a malformed complete line is a hard error
	write("{not json}\n")
	if _, err := feed.refresh(); err == nil {
		t.Fatal("malformed complete line must error")
	}
	// rewrite (shrink) -> reload from zero
	os.WriteFile(path, []byte(`{"promise_hash":"cc"}`+"\n"), 0o644)
	if _, err := feed.refresh(); err != nil {
		t.Fatal(err)
	}
	if all := feed.all(); len(all) != 1 || all[0].PromiseHash != "cc" {
		t.Fatalf("after rewrite = %+v", all)
	}
}

// Concurrency is a count of probes, which says nothing about memory:
// DownloadShard is unary, so one in-flight probe holds the whole shard twice,
// and a validator assigned every row of a large blob holds hundreds of MiB.
// The byte budget is what keeps eight of those from being resident at once,
// and it must never deadlock on an item bigger than itself.
func TestByteSemBoundsInFlightBytes(t *testing.T) {
	const limit = 100
	b := newByteSem(limit)

	// Under the limit, several at once.
	b.acquire(40)
	b.acquire(40)
	third := make(chan struct{})
	go func() { b.acquire(40); close(third) }()
	select {
	case <-third:
		t.Fatal("a third item was admitted past the budget")
	case <-time.After(50 * time.Millisecond):
	}
	b.release(40)
	select {
	case <-third:
	case <-time.After(2 * time.Second):
		t.Fatal("the waiter was not woken when room was freed")
	}
	b.release(40)
	b.release(40)

	// An item heavier than the whole budget runs alone rather than waiting
	// for room that can never exist.
	done := make(chan struct{})
	go func() { b.acquire(limit * 10); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("an item larger than the budget deadlocked")
	}
	b.release(limit * 10)

	// And it did hold the budget while it ran: the next item waits.
	b.acquire(limit * 10)
	after := make(chan struct{})
	go func() { b.acquire(1); close(after) }()
	select {
	case <-after:
		t.Fatal("an item was admitted beside one that had taken the whole budget")
	case <-time.After(50 * time.Millisecond):
	}
	b.release(limit * 10)
	select {
	case <-after:
	case <-time.After(2 * time.Second):
		t.Fatal("the waiter was not woken")
	}
}
