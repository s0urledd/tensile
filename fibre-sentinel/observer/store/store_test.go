package store_test

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/scan"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/ingest"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
)

const sampleDir = "../testdata"

func open(t *testing.T) *store.Store {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "observer.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	return st
}

// TestIngestSampleIsIdempotent ingests the committed devnet fixture twice and
// checks the second pass inserts nothing.
func TestIngestSampleIsIdempotent(t *testing.T) {
	st := open(t)
	now := time.Now()

	pubs, err := ingest.Publications(st, filepath.Join(sampleDir, "publications.jsonl"), now)
	if err != nil {
		t.Fatal(err)
	}
	if pubs.Inserted == 0 || pubs.Inserted != pubs.Read {
		t.Fatalf("publications: read=%d inserted=%d", pubs.Read, pubs.Inserted)
	}
	meas, err := ingest.Measurements(st, filepath.Join(sampleDir, "measurements.jsonl"), now)
	if err != nil {
		t.Fatal(err)
	}
	if meas.Inserted != 60 {
		t.Fatalf("measurements: want 60 inserted (testdata/README.md), got read=%d inserted=%d", meas.Read, meas.Inserted)
	}
	if err := ingest.State(st, filepath.Join(sampleDir, "state.json"), now); err != nil {
		t.Fatal(err)
	}

	c, err := st.Count(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if c.Publications != pubs.Inserted || c.Probes != 60 || c.Assignments != pubs.Inserted*4 {
		t.Fatalf("counts after first pass: %+v", c)
	}

	// second pass: cursors are at EOF, nothing read.
	pubs2, err := ingest.Publications(st, filepath.Join(sampleDir, "publications.jsonl"), now)
	if err != nil {
		t.Fatal(err)
	}
	if pubs2.Read != 0 || pubs2.Inserted != 0 {
		t.Fatalf("second pass read=%d inserted=%d", pubs2.Read, pubs2.Inserted)
	}

	// reset the cursor and re-read: rows are re-read but not re-inserted.
	if err := st.SetCursor(filepath.Join(sampleDir, "measurements.jsonl"), 0, 0, now); err != nil {
		t.Fatal(err)
	}
	meas2, err := ingest.Measurements(st, filepath.Join(sampleDir, "measurements.jsonl"), now)
	if err != nil {
		t.Fatal(err)
	}
	if meas2.Read != 60 || meas2.Inserted != 0 {
		t.Fatalf("re-read read=%d inserted=%d", meas2.Read, meas2.Inserted)
	}

	// the sample run's classification distribution is documented in the README.
	var n int
	if err := st.DB().QueryRow(`SELECT COUNT(*) FROM probes WHERE classification = 'FAULT'`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 9 {
		t.Fatalf("FAULT rows: want 9, got %d", n)
	}
	if v, _ := st.Meta("chain_id"); v != "fibre-devnet" {
		t.Fatalf("meta chain_id = %q", v)
	}
	var params int
	if err := st.DB().QueryRow(`SELECT COUNT(*) FROM params_history`).Scan(&params); err != nil {
		t.Fatal(err)
	}
	if params == 0 {
		t.Fatal("no params_history rows")
	}
}

func TestEndpointHistory(t *testing.T) {
	st := open(t)
	ctx := context.Background()
	t0 := time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)

	snap := []scan.FibreProvider{
		{ConsAddressBech32: "celestiavalcons1aaa", Host: "1.2.3.4:7980"},
		{ConsAddressBech32: "celestiavalcons1bbb", Host: "5.6.7.8:7980"},
	}
	opened, closed, err := st.ObserveEndpoints(ctx, snap, 100, t0)
	if err != nil || opened != 2 || closed != 0 {
		t.Fatalf("first snapshot: opened=%d closed=%d err=%v", opened, closed, err)
	}
	// same snapshot again: nothing opens or closes, last_seen advances.
	opened, closed, err = st.ObserveEndpoints(ctx, snap, 110, t0.Add(time.Minute))
	if err != nil || opened != 0 || closed != 0 {
		t.Fatalf("repeat snapshot: opened=%d closed=%d err=%v", opened, closed, err)
	}
	// validator bbb changes host, validator aaa disappears.
	snap = []scan.FibreProvider{{ConsAddressBech32: "celestiavalcons1bbb", Host: "9.9.9.9:7980"}}
	opened, closed, err = st.ObserveEndpoints(ctx, snap, 120, t0.Add(2*time.Minute))
	if err != nil || opened != 1 || closed != 2 {
		t.Fatalf("change snapshot: opened=%d closed=%d err=%v", opened, closed, err)
	}
	cur, err := st.CurrentEndpoints(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(cur) != 1 || cur[0].Host != "9.9.9.9:7980" || cur[0].LastSeenHeight != 120 {
		t.Fatalf("current endpoints: %+v", cur)
	}
	var total int
	if err := st.DB().QueryRow(`SELECT COUNT(*) FROM endpoints`).Scan(&total); err != nil {
		t.Fatal(err)
	}
	if total != 3 {
		t.Fatalf("history rows: want 3, got %d", total)
	}
}

func TestRuns(t *testing.T) {
	st := open(t)
	t0 := time.Now()
	id, err := st.StartRun("collector", "local", "test", t0)
	if err != nil || id == 0 {
		t.Fatal(err)
	}
	if err := st.Heartbeat(id, t0.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := st.StopRun(id, t0.Add(2*time.Second), "test"); err != nil {
		t.Fatal(err)
	}
	var stopped *string
	if err := st.DB().QueryRow(`SELECT stopped_at FROM observer_runs WHERE id = ?`, id).Scan(&stopped); err != nil {
		t.Fatal(err)
	}
	if stopped == nil {
		t.Fatal("stopped_at not set")
	}
}

func TestTimestampOrdering(t *testing.T) {
	base := time.Date(2026, 9, 15, 12, 0, 0, 0, time.UTC)
	a := store.TS(base)                             // zero nanoseconds
	b := store.TS(base.Add(500 * time.Millisecond)) // half a second later
	c := store.TS(base.Add(time.Second))
	if !(a < b && b < c) {
		t.Fatalf("not chronological as text: %q %q %q", a, b, c)
	}
	if len(a) != len(b) || len(b) != len(c) {
		t.Fatalf("not fixed width: %q %q %q", a, b, c)
	}
	if _, err := time.Parse(time.RFC3339Nano, b); err != nil {
		t.Fatalf("not RFC 3339: %v", err)
	}
}

func TestOpenReadOnly(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "observer.db")
	if _, err := store.OpenReadOnly(path); err == nil {
		t.Fatal("read-only open of a missing database must fail")
	}
	st, err := store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ingest.Publications(st, filepath.Join(sampleDir, "publications.jsonl"), time.Now()); err != nil {
		t.Fatal(err)
	}
	st.Close()
	ro, err := store.OpenReadOnly(path)
	if err != nil {
		t.Fatal(err)
	}
	defer ro.Close()
	c, err := ro.Count(context.Background())
	if err != nil || c.Publications == 0 {
		t.Fatalf("counts via read-only handle: %+v err=%v", c, err)
	}
	if _, err := ro.DB().Exec(`INSERT INTO meta (key, value, updated_at) VALUES ('x','y','z')`); err == nil {
		t.Fatal("write through a read-only handle succeeded")
	}
}

// registry.jsonl replay: the events one store produced rebuild the same
// endpoint history in an empty store, and replaying them again changes
// nothing.
func TestEndpointEventsReplay(t *testing.T) {
	a := open(t)
	ctx := context.Background()
	t0 := time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)
	var log []store.EndpointEvent
	step := func(snap []scan.FibreProvider, h int64, at time.Time) {
		evs, err := a.ObserveEndpointEvents(ctx, snap, h, at)
		if err != nil {
			t.Fatal(err)
		}
		log = append(log, evs...)
	}
	step([]scan.FibreProvider{{ConsAddressBech32: "celestiavalcons1aaa", Host: "1.2.3.4:7980"}, {ConsAddressBech32: "celestiavalcons1bbb", Host: "5.6.7.8:7980"}}, 100, t0)
	step([]scan.FibreProvider{{ConsAddressBech32: "celestiavalcons1bbb", Host: "9.9.9.9:7980"}}, 120, t0.Add(2*time.Minute))
	if len(log) != 5 {
		t.Fatalf("want 5 events (2 opens, 2 closes, 1 open), got %d: %+v", len(log), log)
	}

	b := open(t)
	changed := 0
	for _, e := range log {
		ok, err := b.ReplayEndpointEvent(e)
		if err != nil {
			t.Fatal(err)
		}
		if ok {
			changed++
		}
	}
	if changed != 5 {
		t.Fatalf("replay changed %d rows, want 5", changed)
	}
	for _, e := range log {
		if ok, err := b.ReplayEndpointEvent(e); err != nil || ok {
			t.Fatalf("second replay must be a no-op: ok=%v err=%v", ok, err)
		}
	}
	rowsOf := func(s *store.Store) string {
		rows, err := s.DB().Query(`SELECT validator_cons_address, host, first_seen_at, first_seen_height, COALESCE(closed_at,''), COALESCE(closed_height,0)
			FROM endpoints ORDER BY validator_cons_address, first_seen_at`)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		out := ""
		for rows.Next() {
			var addr, host, first, closed string
			var fh, ch int64
			if err := rows.Scan(&addr, &host, &first, &fh, &closed, &ch); err != nil {
				t.Fatal(err)
			}
			out += addr + "|" + host + "|" + first + "|" + closed + "\n"
		}
		return out
	}
	if rowsOf(a) != rowsOf(b) {
		t.Fatalf("replayed history differs:\n%s\n--\n%s", rowsOf(a), rowsOf(b))
	}
	cur, err := b.CurrentEndpoints(ctx)
	if err != nil || len(cur) != 1 || cur[0].Host != "9.9.9.9:7980" {
		t.Fatalf("current after replay: %+v %v", cur, err)
	}
}

// A validator's Keybase picture is held per identity, refreshed after the
// max age, and only a picture that is actually held is served.
func TestAvatars(t *testing.T) {
	st, err := store.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	now := time.Date(2026, 9, 18, 12, 0, 0, 0, time.UTC)
	ids := []scan.ValidatorIdentity{
		{ConsAddressHex: "aa", Moniker: "with picture", Identity: "D27EE330254D4F6A", Status: "BOND_STATUS_BONDED"},
		{ConsAddressHex: "bb", Moniker: "same keybase", Identity: "D27EE330254D4F6A", Status: "BOND_STATUS_BONDED"},
		{ConsAddressHex: "cc", Moniker: "no identity", Identity: "", Status: "BOND_STATUS_BONDED"},
		{ConsAddressHex: "dd", Moniker: "a name, not a suffix", Identity: "huginn.tech", Status: "BOND_STATUS_BONDED"},
	}
	if _, err := st.UpsertValidatorIdentities(ids, now); err != nil {
		t.Fatal(err)
	}
	due, err := st.AvatarsDue(ctx, now, 24*time.Hour, 100)
	if err != nil || len(due) != 1 || due[0] != "D27EE330254D4F6A" {
		t.Fatalf("due = %v (%v), want the one well-formed suffix once", due, err)
	}
	if err := st.PutAvatar("D27EE330254D4F6A", "ok", "https://x/pic.jpg", "image/jpeg", []byte("jpeg"), now); err != nil {
		t.Fatal(err)
	}
	if due, _ := st.AvatarsDue(ctx, now.Add(time.Hour), 24*time.Hour, 100); len(due) != 0 {
		t.Fatalf("freshly resolved identity due again: %v", due)
	}
	if due, _ := st.AvatarsDue(ctx, now.Add(25*time.Hour), 24*time.Hour, 100); len(due) != 1 {
		t.Fatalf("stale identity not due: %v", due)
	}
	ct, data, checked, ok, err := st.Avatar(ctx, "D27EE330254D4F6A")
	if err != nil || !ok || ct != "image/jpeg" || string(data) != "jpeg" || !checked.Equal(now) {
		t.Fatalf("avatar = %q %q %s ok=%v err=%v", ct, data, checked, ok, err)
	}
	if err := st.PutAvatar("D27EE330254D4F6A", "none", "", "", nil, now.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if _, _, _, ok, _ := st.Avatar(ctx, "D27EE330254D4F6A"); ok {
		t.Fatal("a picture Keybase no longer has is still served")
	}
}

// SQLite's auto-checkpoint copies WAL pages back but never resets the file,
// and cannot reset one while a reader holds a snapshot. The API holds one
// through every snapshot refresh, so a batch ingest left the -wal at its
// high-water mark for good — on the same volume as the database, counted by
// the health check's disk threshold.
func TestCheckpointWALTruncatesTheLog(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "observer.db")
	st, err := store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()

	for i := 0; i < 3000; i++ {
		if _, err := st.DB().ExecContext(ctx,
			`INSERT INTO meta (key, value, updated_at) VALUES (?, ?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value`,
			"k"+strconv.Itoa(i), strings.Repeat("x", 200), "2026-09-18T00:00:00.000000000Z"); err != nil {
			t.Fatal(err)
		}
	}
	wal := path + "-wal"
	before := int64(0)
	if fi, err := os.Stat(wal); err == nil {
		before = fi.Size()
	}
	if before == 0 {
		t.Skip("no write-ahead log on this build")
	}

	busy, _, _, err := st.CheckpointWAL(ctx)
	if err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	if busy {
		t.Fatal("checkpoint reported busy with no reader open")
	}
	// The frame counts depend on whether SQLite's own auto-checkpoint got
	// there first, which is timing. What has to hold is the file: the log is
	// reset, which is the thing the auto-checkpoint never does.
	after := int64(0)
	if fi, err := os.Stat(wal); err == nil {
		after = fi.Size()
	}
	if after >= before {
		t.Fatalf("the log was %d bytes and is %d after the checkpoint", before, after)
	}

	// A reader in the way makes it a no-op rather than an error, which the
	// caller treats as "try again next pass": TestCheckpointWALDoesNotWaitForAReader.
}

// TestCheckpointWALDoesNotWaitForAReader: a reader holding a snapshot older
// than the last commit (the API, through a snapshot refresh) keeps the log
// from being reset. The checkpoint copies what it can and says busy at
// once; it must not wait out the busy timeout, because the collector's one
// goroutine (the fast tick included) waits with it. Once the reader is gone
// the log is reset, and the connection keeps its busy timeout for every
// other statement.
func TestCheckpointWALDoesNotWaitForAReader(t *testing.T) {
	path := filepath.Join(t.TempDir(), "observer.db")
	st, err := store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	write := func(from, n int) {
		t.Helper()
		for i := from; i < from+n; i++ {
			if err := st.SetMeta("k"+strconv.Itoa(i), strings.Repeat("x", 200), time.Now()); err != nil {
				t.Fatal(err)
			}
		}
	}
	write(0, 200)

	ro, err := store.OpenReadOnly(path)
	if err != nil {
		t.Fatal(err)
	}
	defer ro.Close()
	tx, err := ro.DB().BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	var n int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM meta`).Scan(&n); err != nil { // the snapshot is taken here
		t.Fatal(err)
	}
	write(200, 200) // committed past the reader's snapshot

	start := time.Now()
	busy, _, _, err := st.CheckpointWAL(ctx)
	took := time.Since(start)
	if err != nil {
		t.Fatalf("checkpoint with a reader in the way: %v", err)
	}
	if !busy {
		t.Error("checkpoint reported done with a reader holding an older snapshot")
	}
	if took > 2*time.Second {
		t.Errorf("checkpoint waited %v for the reader", took)
	}
	var timeout int
	if err := st.DB().QueryRowContext(ctx, `PRAGMA busy_timeout`).Scan(&timeout); err != nil || timeout != 5000 {
		t.Errorf("busy timeout after the checkpoint: %d, %v", timeout, err)
	}

	tx.Rollback()
	busy, _, _, err = st.CheckpointWAL(ctx)
	if err != nil || busy {
		t.Fatalf("checkpoint with the reader gone: busy %v, %v", busy, err)
	}
	if fi, err := os.Stat(path + "-wal"); err == nil && fi.Size() != 0 {
		t.Errorf("the log is %d bytes after a checkpoint with no reader", fi.Size())
	}
}

// The retention pass deletes rows; SQLite moves those pages to its free list
// and never shrinks the file on its own. deploy/README.md tells an operator
// that pruning is the answer to a full disk, so it has to return something
// they can see. auto_vacuum(incremental) is set at creation — it cannot be
// turned on later without a full VACUUM — and the collector releases pages
// after a prune.
func TestReclaimSpaceReturnsFreedPages(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "observer.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()

	var mode int
	if err := st.DB().QueryRowContext(ctx, `PRAGMA auto_vacuum`).Scan(&mode); err != nil {
		t.Fatal(err)
	}
	if mode != 2 {
		t.Fatalf("auto_vacuum = %d, want 2 (incremental); it cannot be set after the first table exists", mode)
	}

	for i := 0; i < 4000; i++ {
		if _, err := st.DB().ExecContext(ctx,
			`INSERT INTO meta (key, value, updated_at) VALUES (?, ?, ?)`,
			"k"+strconv.Itoa(i), strings.Repeat("x", 400), "2026-09-18T00:00:00.000000000Z"); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := st.DB().ExecContext(ctx, `DELETE FROM meta WHERE key LIKE 'k%'`); err != nil {
		t.Fatal(err)
	}
	var free int64
	if err := st.DB().QueryRowContext(ctx, `PRAGMA freelist_count`).Scan(&free); err != nil {
		t.Fatal(err)
	}
	if free == 0 {
		t.Skip("the delete freed no pages on this build")
	}
	freed, err := st.ReclaimSpace(ctx, 20000)
	if err != nil {
		t.Fatalf("reclaim: %v", err)
	}
	if freed == 0 {
		t.Fatalf("%d pages on the free list and none was returned", free)
	}
	// And it is idempotent: nothing left to give back is not an error.
	if again, err := st.ReclaimSpace(ctx, 20000); err != nil || again != 0 {
		t.Fatalf("second reclaim freed %d, err %v", again, err)
	}
}

// Every path that moves the cache revision must produce a new token each
// time it runs, whatever clock the caller is holding. The collector stamps
// one time.Now() at the top of a pass and threads it through everything it
// ingests, so a revision built from that clock repeated itself — and a
// snapshot computed under it stayed valid across the second bump.
func TestTheCacheRevisionChangesOnEveryBump(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "observer.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	// One clock, as a collector pass holds it.
	now := time.Now()
	seen := map[string]bool{}
	var last string
	for i := 0; i < 5; i++ {
		if err := st.BumpParamHoldsRev(now); err != nil {
			t.Fatal(err)
		}
		v, err := st.Meta(store.MetaParamHoldsRev)
		if err != nil {
			t.Fatal(err)
		}
		if v == "" {
			t.Fatal("the revision is empty after a bump")
		}
		if seen[v] {
			t.Fatalf("bump %d produced %q again; two bumps under one clock must not collide", i+1, v)
		}
		seen[v] = true
		if last != "" {
			a, err1 := strconv.ParseInt(last, 10, 64)
			b, err2 := strconv.ParseInt(v, 10, 64)
			if err1 != nil || err2 != nil || b <= a {
				t.Fatalf("the revision did not rise: %q then %q", last, v)
			}
		}
		last = v
	}
}

// A store carrying the earlier timestamp value keeps rising from it rather
// than restarting, so the token never repeats one an API process already
// holds.
func TestTheCacheRevisionRisesFromATimestampLeftByTheOldScheme(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "observer.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	now := time.Now()
	stamp := strconv.FormatInt(now.UTC().UnixNano(), 10)
	if err := st.SetMeta(store.MetaParamHoldsRev, stamp, now); err != nil {
		t.Fatal(err)
	}
	if err := st.BumpParamHoldsRev(now); err != nil {
		t.Fatal(err)
	}
	v, err := st.Meta(store.MetaParamHoldsRev)
	if err != nil {
		t.Fatal(err)
	}
	old, err := strconv.ParseInt(stamp, 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	got, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		t.Fatalf("the revision is not an integer after the change: %q", v)
	}
	if got != old+1 {
		t.Fatalf("revision = %d, want %d: it must carry on from the timestamp, not restart", got, old+1)
	}
}

// A value that is not a number at all still yields a changed token rather
// than an error; the API treats it as opaque and only compares equality.
func TestTheCacheRevisionSurvivesAnUnreadableValue(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "observer.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	now := time.Now()
	if err := st.SetMeta(store.MetaParamHoldsRev, "not-a-number", now); err != nil {
		t.Fatal(err)
	}
	if err := st.BumpParamHoldsRev(now); err != nil {
		t.Fatal(err)
	}
	v, err := st.Meta(store.MetaParamHoldsRev)
	if err != nil {
		t.Fatal(err)
	}
	if v == "not-a-number" || v == "" {
		t.Fatalf("revision = %q after a bump over an unreadable value", v)
	}
}
