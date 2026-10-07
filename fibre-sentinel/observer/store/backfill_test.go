package store_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/probe"
	"github.com/plsgiveup/fibre/fibre-sentinel/internal/scan"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
)

// backfillRecords are records a store holds, with their lines, in the order they are inserted.
type backfillRecords struct {
	pubs       []scan.Publication
	pubLines   [][]byte
	reads      []probe.Measurement
	readLines  [][]byte
	reach      []probe.Measurement
	reachLines [][]byte
}

// earlierRecords are what a store held before the slim record: publications, one kept as its line by the slim form
// (a host carrying a byte that is not UTF-8); readings of each, of the validator's own rows, of part of them in
// another order, of rows of no promise, one with an error carrying such a byte, and one of a publication the store
// does not hold; and endpoint checks, one not as Go writes it.
func earlierRecords(t *testing.T) *backfillRecords {
	t.Helper()
	r := &backfillRecords{}
	p1 := slimPublication(t, 0x51, 7)
	p2 := slimPublication(t, 0x52, 5)
	p2.Assignment.Validators[1].Host = "10.0.0.2:79\xff80" // a chain string nothing checked
	p3 := slimPublication(t, 0x53, 6)
	for _, p := range []scan.Publication{p1, p2, p3} {
		r.addPub(p)
	}
	for i, v := range p1.Assignment.Validators {
		rows := u32(v.Rows)
		switch i {
		case 2: // part of its rows, in another order
			rows = []uint32{rows[3], rows[1], rows[0]}
		case 4: // rows of no promise
			rows = []uint32{16383, 5, 9001}
		}
		m := slimReading(p1, v, rows)
		if i == 5 {
			m.Outcome, m.Classification = probe.OutcomeServerError, probe.ClassFault
			m.RawError = "rpc error: code = Internal desc = shard \xfe\xff not found" // a remote server's message
		}
		r.addRead(m)
	}
	for _, v := range p2.Assignment.Validators[:3] {
		r.addRead(slimReading(p2, v, u32(v.Rows)))
	}
	for _, v := range p3.Assignment.Validators {
		r.addRead(slimReading(p3, v, u32(v.Rows)))
	}
	absent := slimPublication(t, 0x54, 4)
	r.addRead(slimReading(absent, absent.Assignment.Validators[0], u32(absent.Assignment.Validators[0].Rows)))
	r.addChecks(time.Date(2026, 10, 5, 0, 2, 11, 0, time.UTC), 5, 3)
	return r
}

// laterRecords are what a store stored after it: a publication, readings of it, endpoint checks.
func laterRecords(t *testing.T) *backfillRecords {
	t.Helper()
	r := &backfillRecords{}
	p := slimPublication(t, 0x55, 5)
	r.addPub(p)
	for _, v := range p.Assignment.Validators {
		r.addRead(slimReading(p, v, u32(v.Rows)))
	}
	r.addChecks(time.Date(2026, 10, 7, 9, 0, 0, 0, time.UTC), 2, -1)
	return r
}

func (r *backfillRecords) addPub(p scan.Publication) {
	l, _ := json.Marshal(p)
	r.pubs, r.pubLines = append(r.pubs, p), append(r.pubLines, l)
}

func (r *backfillRecords) addRead(m probe.Measurement) {
	l, _ := json.Marshal(m)
	r.reads, r.readLines = append(r.reads, m), append(r.readLines, l)
}

// addChecks adds n endpoint checks from at, five minutes apart, the one at odd (if any) written not as Go writes it.
func (r *backfillRecords) addChecks(at time.Time, n, odd int) {
	for i := 0; i < n; i++ {
		m := probe.Measurement{SchemaVersion: 2, Vantage: "ut-1", ValidatorAddress: "ac2a961260b80fc88ddb9e52f1a957e2f5d60d64",
			ValidatorHost: "173.201.36.182:7980", ScheduleLabel: "heartbeat", ScheduledAt: at.Add(time.Duration(i) * 5 * time.Minute),
			StartedAt: at.Add(time.Duration(i)*5*time.Minute + 291025838), Phase: probe.PhasePost, Outcome: probe.OutcomeReachable,
			Classification: probe.ClassNotProbed, TotalDurationMS: 71}
		m.TCP = probe.StepResult{Attempted: true, OK: true, DurationMS: 35, Detail: "-> 173.201.36.182:7980"}
		l, _ := json.Marshal(m)
		if i == odd {
			l = bytes.Replace(l, []byte(`"vantage":"ut-1"`), []byte(`"vantage": "ut-1"`), 1)
		}
		r.reach, r.reachLines = append(r.reach, m), append(r.reachLines, l)
	}
}

func reachKey(m probe.Measurement) string {
	return m.Vantage + "|" + m.ValidatorAddress + "|" + m.ScheduledAt.UTC().Format(time.RFC3339Nano)
}

// insert stores the records with this build's insert paths, in order.
func (r *backfillRecords) insert(t *testing.T, st *store.Store) {
	t.Helper()
	for i, p := range r.pubs {
		if ok, err := st.UpsertPublication(p, r.pubLines[i]); err != nil || !ok {
			t.Fatalf("publication %d: %v %v", i, ok, err)
		}
	}
	for i, m := range r.reads {
		if ok, err := st.InsertProbe(m, r.readLines[i]); err != nil || !ok {
			t.Fatalf("reading %d: %v %v", i, ok, err)
		}
	}
	for i, m := range r.reach {
		if ok, err := st.InsertReachability(m, r.reachLines[i]); err != nil || !ok {
			t.Fatalf("endpoint check %d: %v %v", i, ok, err)
		}
	}
}

// asEarlier writes every row of the records as a build before the slim record wrote it, with SQL: the lines in
// raw_json, the lists in rows_json and row_indices; and forgets the slim tables.
func (r *backfillRecords) asEarlier(t *testing.T, st *store.Store) {
	t.Helper()
	exec := func(q string, args ...any) {
		t.Helper()
		if _, err := st.DB().Exec(q, args...); err != nil {
			t.Fatal(err)
		}
	}
	for i, p := range r.pubs {
		exec(`UPDATE publications SET raw_json = ? WHERE promise_hash = ?`, string(r.pubLines[i]), p.PromiseHash)
		for _, v := range p.Assignment.Validators {
			l, _ := json.Marshal(v.Rows)
			exec(`UPDATE assignments SET rows_json = ? WHERE promise_hash = ? AND validator_address = ?`, string(l), p.PromiseHash, v.Address)
		}
	}
	for i, m := range r.reads {
		var idx any
		if len(m.Download.RowIndices) > 0 {
			l, _ := json.Marshal(m.Download.RowIndices)
			idx = string(l)
		}
		exec(`UPDATE probes SET raw_json = ?, row_indices = ? WHERE dedupe_key = ?`, string(r.readLines[i]), idx, m.DedupeKey())
	}
	for i, m := range r.reach {
		exec(`UPDATE reachability SET raw_json = ? WHERE dedupe_key = ?`, string(r.reachLines[i]), reachKey(m))
	}
	exec(`DELETE FROM slim_entries`)
}

// earlierStore is a store as the collector finds it: rows a build before the slim record wrote, then rows this build
// wrote since, in their slim forms; and a store with the same records that this build wrote in the same order (the
// reference). In the first, one reading's record was emptied by an earlier build's retention, and one assignment's
// list is written in other bytes than Go writes it; the reference empties the same record.
func earlierStore(t *testing.T) (path string, ref *store.Store, early, late *backfillRecords) {
	t.Helper()
	early, late = earlierRecords(t), laterRecords(t)
	dir := t.TempDir()
	ref, err := store.Open(filepath.Join(dir, "reference.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ref.Close() })
	early.insert(t, ref)
	late.insert(t, ref)

	path = filepath.Join(dir, "observer.db")
	st, err := store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	early.insert(t, st)
	early.asEarlier(t, st)
	st.Close()
	// this build, started on that store, stores what comes next
	if st, err = store.Open(path); err != nil {
		t.Fatal(err)
	}
	late.insert(t, st)
	stripped := early.reads[len(early.reads)-2].DedupeKey()
	for _, db := range []*sql.DB{st.DB(), ref.DB()} {
		if _, err := db.Exec(`UPDATE probes SET raw_json = '' WHERE dedupe_key = ?`, stripped); err != nil {
			t.Fatal(err)
		}
	}
	odd := early.pubs[2]
	if _, err := st.DB().Exec(`UPDATE assignments SET rows_json = ? WHERE promise_hash = ? AND validator_address = ?`,
		spacedList(odd.Assignment.Validators[0].Rows), odd.PromiseHash, odd.Assignment.Validators[0].Address); err != nil {
		t.Fatal(err)
	}
	st.Close()
	return path, ref, early, late
}

// spacedList is a list as JSON, written with a space after each comma (as Go does not).
func spacedList(rows []int) string {
	parts := make([]string, len(rows))
	for i, r := range rows {
		parts[i] = fmt.Sprint(r)
	}
	return "[" + strings.Join(parts, ", ") + "]"
}

// sameAsReference fails unless every row of got holds what the reference holds: each record reading back to its line
// (got read through rd, a reader that loads the slim tables from the store, as another process does), kept slim
// exactly where the reference keeps it, and each row list as the same bytes (but the one written in other bytes,
// which is as it was). The reading counts are the reference's.
func sameAsReference(t *testing.T, rd, ref *store.Store, recs ...*backfillRecords) {
	t.Helper()
	ctx := context.Background()
	slimOf := func(db *sql.DB, q string, args ...any) ([]byte, bool) {
		t.Helper()
		var raw []byte
		if err := db.QueryRow(q, args...).Scan(&raw); err != nil {
			t.Fatal(err)
		}
		return raw, len(raw) > 0 && raw[0] != '{'
	}
	for _, r := range recs {
		for i, p := range r.pubs {
			q := `SELECT raw_json FROM publications WHERE promise_hash = ?`
			raw, isSlim := slimOf(rd.DB(), q, p.PromiseHash)
			_, refSlim := slimOf(ref.DB(), q, p.PromiseHash)
			if got, err := rd.Record(ctx, rd.DB(), raw); err != nil || !bytes.Equal(got, r.pubLines[i]) || isSlim != refSlim {
				t.Errorf("publication %s: slim %v (reference %v), back: %v", p.PromiseHash[:8], isSlim, refSlim, err)
			}
			for _, v := range p.Assignment.Validators {
				q := `SELECT rows_json FROM assignments WHERE promise_hash = ? AND validator_address = ?`
				var got, want sql.NullString
				if err := rd.DB().QueryRow(q, p.PromiseHash, v.Address).Scan(&got); err != nil {
					t.Fatal(err)
				}
				if err := ref.DB().QueryRow(q, p.PromiseHash, v.Address).Scan(&want); err != nil {
					t.Fatal(err)
				}
				if got.String == spacedList(v.Rows) {
					continue
				}
				if got != want {
					t.Errorf("assignment %s/%s: %.40q, the reference %.40q", p.PromiseHash[:8], v.Address[:8], got.String, want.String)
				}
			}
		}
		for i, m := range r.reads {
			q := `SELECT raw_json FROM probes WHERE dedupe_key = ?`
			raw, isSlim := slimOf(rd.DB(), q, m.DedupeKey())
			refRaw, refSlim := slimOf(ref.DB(), q, m.DedupeKey())
			want := r.readLines[i]
			if len(refRaw) == 0 {
				want = nil // emptied by the retention of an earlier build
			}
			if got, err := rd.ProbeRecord(ctx, rd.DB(), m.PromiseHash, raw); err != nil || !bytes.Equal(got, want) || isSlim != refSlim {
				t.Errorf("reading %d: slim %v (reference %v), back: %v\n got %.200s\nwant %.200s", i, isSlim, refSlim, err, got, want)
			}
			q = `SELECT row_indices FROM probes WHERE dedupe_key = ?`
			var got, ref2 sql.NullString
			if err := rd.DB().QueryRow(q, m.DedupeKey()).Scan(&got); err != nil {
				t.Fatal(err)
			}
			if err := ref.DB().QueryRow(q, m.DedupeKey()).Scan(&ref2); err != nil {
				t.Fatal(err)
			}
			if got != ref2 {
				t.Errorf("reading %d: row_indices %.40q, the reference %.40q", i, got.String, ref2.String)
			}
		}
		for i, m := range r.reach {
			q := `SELECT raw_json FROM reachability WHERE dedupe_key = ?`
			raw, isSlim := slimOf(rd.DB(), q, reachKey(m))
			_, refSlim := slimOf(ref.DB(), q, reachKey(m))
			if got, err := rd.ReachRecord(ctx, rd.DB(), raw); err != nil || !bytes.Equal(got, r.reachLines[i]) || isSlim != refSlim {
				t.Errorf("endpoint check %d: slim %v (reference %v), back: %v", i, isSlim, refSlim, err)
			}
		}
	}
	if a, b := dumpRows(t, rd.DB(), `SELECT promise_hash, scheduled_at, exact FROM reading_rows ORDER BY 1, 2`),
		dumpRows(t, ref.DB(), `SELECT promise_hash, scheduled_at, exact FROM reading_rows ORDER BY 1, 2`); a != b {
		t.Errorf("reading_rows:\n%s\nthe reference:\n%s", a, b)
	}
}

func dumpRows(t *testing.T, db *sql.DB, q string) string {
	t.Helper()
	rows, err := db.Query(q)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var a, b string
		var n int64
		if err := rows.Scan(&a, &b, &n); err != nil {
			t.Fatal(err)
		}
		out = append(out, fmt.Sprintf("%s %s %d", a, b, n))
	}
	return strings.Join(out, "\n")
}

// column is what the backfill did to one column, as Tables gives it.
func column(t *testing.T, b *store.Backfill, table, col string) store.BackfillColumn {
	t.Helper()
	for _, tb := range b.Tables() {
		if tb.Table == table {
			return *tb.Columns[col]
		}
	}
	t.Fatalf("no table %s", table)
	return store.BackfillColumn{}
}

// The backfill writes every row an earlier build stored as this build inserts it: the slim record where it reads back
// to its line, the mark where the list is the validator's own assignment, every other value as it was. A store
// converted reads back, in another process, to what a store this build wrote from the same lines in the same order
// holds: each record to its line, slim where that one keeps it slim, each row list in the same bytes, the same reading
// counts. What it left as it was is counted with why. It runs on the collector's store, which has one connection, while
// another goroutine reads it (as the collector's Keybase lookup does), and nothing waits on itself; once done, it says
// so in meta and costs nothing more.
func TestTheBackfillWritesWhatAnInsertWrites(t *testing.T) {
	path, ref, early, late := earlierStore(t)
	st, err := store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// another goroutine on the one connection, all the while
	stopReader := make(chan struct{})
	var wg sync.WaitGroup
	var readerErr error
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stopReader:
				return
			default:
			}
			if _, err := st.Count(ctx); err != nil {
				readerErr = err
				return
			}
			time.Sleep(time.Millisecond)
		}
	}()
	b := st.NewBackfill()
	b.SetBatch(4, 1<<20)
	turn, err := b.Run(ctx, 0, nil)
	close(stopReader)
	wg.Wait()
	if err != nil {
		t.Fatalf("backfill: %v", err)
	}
	if readerErr != nil {
		t.Fatalf("the reader beside it: %v", readerErr)
	}
	if !turn.Done || !turn.DoneNow || turn.Batches < 10 {
		t.Fatalf("turn %+v", turn)
	}
	if at, err := st.Meta(store.MetaBackfillDoneAt); err != nil || at == "" {
		t.Fatalf("done at %q, %v", at, err)
	}

	rd, err := store.OpenReadOnly(path)
	if err != nil {
		t.Fatal(err)
	}
	defer rd.Close()
	sameAsReference(t, rd, ref, early, late)

	odd := early.pubs[2]
	var list string
	if err := st.DB().QueryRow(`SELECT rows_json FROM assignments WHERE promise_hash = ? AND validator_address = ?`,
		odd.PromiseHash, odd.Assignment.Validators[0].Address).Scan(&list); err != nil || list != spacedList(odd.Assignment.Validators[0].Rows) {
		t.Fatalf("the list in other bytes: %.40q, %v", list, err)
	}

	for _, c := range []struct {
		table, col      string
		converted, kept int64
		why             map[string]int64
	}{
		{"publications", "raw_json", 2, 1, map[string]int64{"the insert path keeps it as its line": 1}},
		{"assignments", "rows_json", 17, 1, map[string]int64{"its new form does not read back to it": 1}},
		{"probes", "raw_json", 14, 2, map[string]int64{"the insert path keeps it as its line": 2}},
		{"probes", "row_indices", 14, 3, map[string]int64{"not the validator's own assignment in its order": 2, "its publication is not on record": 1}},
		{"reachability", "raw_json", 4, 1, map[string]int64{"the insert path keeps it as its line": 1}},
	} {
		got := column(t, b, c.table, c.col)
		if got.Converted != c.converted || got.KeptAsIs != c.kept || fmt.Sprint(got.Reasons) != fmt.Sprint(c.why) || len(got.Examples) != int(c.kept) {
			t.Errorf("%s.%s: converted %d, kept %d %v %q; want %d, %d %v", c.table, c.col, got.Converted, got.KeptAsIs, got.Reasons, got.Examples, c.converted, c.kept, c.why)
		}
	}
	tables := b.Tables()
	if tables[0].Seen != 4 || tables[3].Seen != 7 {
		t.Errorf("rows read: publications %d, reachability %d", tables[0].Seen, tables[3].Seen)
	}
	status := b.Status()
	if status["done_at"] == nil || status["freed_bytes"] == nil || status["probes"] == nil {
		t.Errorf("status %v", status)
	}

	// done: a later turn, in this process or the next, reads nothing
	again, err := b.Run(ctx, 0, nil)
	if err != nil || again.Batches != 0 || again.DoneNow || !again.Done {
		t.Fatalf("a turn once done: %+v %v", again, err)
	}
	next := st.NewBackfill()
	again, err = next.Run(ctx, 0, func() bool { t.Fatal("a backfill done asked whether to stop"); return true })
	if err != nil || again.Batches != 0 || !again.Done || !next.Done() {
		t.Fatalf("the next process: %+v %v", again, err)
	}
	if s := next.Status(); len(s) != 1 || s["done_at"] == nil {
		t.Fatalf("the next process's status: %v", s)
	}
}

// A collector that stops between batches goes on, in its next process, from the batch after the last it did: the rows
// done are not read again, and the store ends as one converted in a single run. A turn ends when its stop says so,
// asked before each batch.
func TestTheBackfillGoesOnWhereItStopped(t *testing.T) {
	path, ref, early, late := earlierStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	st, err := store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	b := st.NewBackfill()
	b.SetBatch(5, 1<<20)
	asked := 0
	turn, err := b.Run(ctx, 0, func() bool { asked++; return asked > 3 })
	if err != nil || turn.Batches != 3 || turn.Done || len(turn.Finished) != 1 || turn.Finished[0] != "publications" {
		t.Fatalf("the first turn: %+v %v", turn, err)
	}
	tables := b.Tables()
	if tables[0].Done != tables[0].Total || tables[1].Seen != 10 {
		t.Fatalf("after three batches: %+v", tables[:2])
	}
	var at string
	if err := st.DB().QueryRow(`SELECT value FROM meta WHERE key = ?`, store.MetaBackfillPrefix+"assignments").Scan(&at); err != nil || at != fmt.Sprint(tables[1].Done) {
		t.Fatalf("assignments progress in meta %q, %v; done %d", at, err, tables[1].Done)
	}
	st.Close()

	st, err = store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	b = st.NewBackfill()
	b.SetBatch(5, 1<<20)
	if turn, err = b.Run(ctx, 0, nil); err != nil || !turn.DoneNow {
		t.Fatalf("the second turn: %+v %v", turn, err)
	}
	tables = b.Tables()
	if tables[0].Seen != 0 || tables[1].Seen != 18-10+5 {
		t.Fatalf("the next process read publications %d, assignments %d", tables[0].Seen, tables[1].Seen)
	}
	rd, err := store.OpenReadOnly(path)
	if err != nil {
		t.Fatal(err)
	}
	defer rd.Close()
	sameAsReference(t, rd, ref, early, late)
}

// A batch that fails leaves the store as it was: its rows, the table entries it added (in the store and in the
// process's tables) and the progress. The turn stops with the error, and the next turn does that batch again and goes
// on; the store then reads back, in another process, as one converted without a failure. Both an update refused in
// the middle of a batch and a commit refused at its end.
func TestAFailedBatchLeavesTheStoreAsItWas(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	count := func(st *store.Store, q string, args ...any) int64 {
		t.Helper()
		var n int64
		if err := st.DB().QueryRow(q, args...).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	exec := func(st *store.Store, q string) {
		t.Helper()
		if _, err := st.DB().Exec(q); err != nil {
			t.Fatal(err)
		}
	}

	t.Run("an update refused", func(t *testing.T) {
		path, ref, early, late := earlierStore(t)
		st, err := store.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		defer st.Close()
		// the sixth reading's update fails; the batches are of four rows
		victim := early.reads[5].DedupeKey()
		exec(st, `CREATE TRIGGER fail_update BEFORE UPDATE OF raw_json ON probes WHEN OLD.dedupe_key = '`+victim+`'
			BEGIN SELECT RAISE(ABORT, 'injected'); END`)
		b := st.NewBackfill()
		b.SetBatch(4, 1<<20)
		var entries, progress int64
		b.OnCommit(func([]store.BackfillValue) error {
			entries = count(st, `SELECT COUNT(*) FROM slim_entries`)
			progress = count(st, `SELECT COALESCE(MAX(CAST(value AS INTEGER)), 0) FROM meta WHERE key = ?`, store.MetaBackfillPrefix+"probes")
			return nil
		})
		turn, err := b.Run(ctx, 0, nil)
		if err == nil || !strings.Contains(err.Error(), "injected") || turn.Done {
			t.Fatalf("a refused update: %+v %v", turn, err)
		}
		if n := count(st, `SELECT COUNT(*) FROM slim_entries`); n != entries {
			t.Fatalf("%d table entries after the failed batch, %d before it", n, entries)
		}
		if n := count(st, `SELECT COALESCE(MAX(CAST(value AS INTEGER)), 0) FROM meta WHERE key = ?`, store.MetaBackfillPrefix+"probes"); n != progress || n != 4 {
			t.Fatalf("probes progress %d after the failed batch, %d before it", n, progress)
		}
		// the failed batch's rows are as they were: lines
		if n := count(st, `SELECT COUNT(*) FROM probes WHERE rowid > 4 AND rowid <= 8 AND substr(raw_json, 1, 1) = '{'`); n != 4 {
			t.Fatalf("%d of the failed batch's four readings kept their lines", n)
		}
		exec(st, `DROP TRIGGER fail_update`)
		b.OnCommit(nil)
		if turn, err := b.Run(ctx, 0, nil); err != nil || !turn.DoneNow {
			t.Fatalf("the next turn: %+v %v", turn, err)
		}
		rd, err := store.OpenReadOnly(path)
		if err != nil {
			t.Fatal(err)
		}
		defer rd.Close()
		sameAsReference(t, rd, ref, early, late)
	})

	t.Run("a commit refused", func(t *testing.T) {
		path, ref, early, late := earlierStore(t)
		st, err := store.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		defer st.Close()
		before := count(st, `SELECT COUNT(*) FROM slim_entries`)
		// a constraint checked only at commit, broken by every entry written
		for _, q := range []string{
			`CREATE TABLE fail_parent (id INTEGER PRIMARY KEY)`,
			`CREATE TABLE fail_child (id INTEGER REFERENCES fail_parent (id) DEFERRABLE INITIALLY DEFERRED)`,
			`CREATE TRIGGER fail_commit AFTER INSERT ON slim_entries BEGIN INSERT INTO fail_child VALUES (1); END`,
		} {
			exec(st, q)
		}
		b := st.NewBackfill()
		b.SetBatch(4, 1<<20)
		turn, err := b.Run(ctx, 0, nil)
		if err == nil || turn.Batches != 0 {
			t.Fatalf("a refused commit: %+v %v", turn, err)
		}
		exec(st, `DROP TRIGGER fail_commit`)
		if n := count(st, `SELECT COUNT(*) FROM slim_entries`); n != before {
			t.Fatalf("%d table entries after the failed commit, %d before it", n, before)
		}
		if n := count(st, `SELECT COUNT(*) FROM publications WHERE substr(raw_json, 1, 1) = '{'`); n != 3 {
			t.Fatalf("%d publications kept their lines, want the three", n)
		}
		if n := count(st, `SELECT COUNT(*) FROM meta WHERE key LIKE 'slim_backfill_%'`); n != 0 {
			t.Fatalf("%d progress keys after the failed commit", n)
		}
		if turn, err := b.Run(ctx, 0, nil); err != nil || !turn.DoneNow {
			t.Fatalf("the next turn: %+v %v", turn, err)
		}
		rd, err := store.OpenReadOnly(path)
		if err != nil {
			t.Fatal(err)
		}
		defer rd.Close()
		sameAsReference(t, rd, ref, early, late)
	})
}

// A turn spends its budget and no more than the batch it is in; a budget too short for one batch still does one.
func TestATurnKeepsToItsBudget(t *testing.T) {
	path, _, _, _ := earlierStore(t)
	st, err := store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	b := st.NewBackfill()
	b.SetBatch(1, 1<<20)
	turn, err := b.Run(context.Background(), time.Nanosecond, nil)
	if err != nil || turn.Batches != 1 || turn.Done {
		t.Fatalf("%+v %v", turn, err)
	}
	// and a batch reads no row after the one that brings it past its bytes
	b.SetBatch(300, 1)
	turn, err = b.Run(context.Background(), time.Nanosecond, nil)
	if err != nil || turn.Batches != 1 || b.Tables()[0].Seen != 2 {
		t.Fatalf("a batch of one row's bytes: %+v %v, %d read", turn, err, b.Tables()[0].Seen)
	}
}
