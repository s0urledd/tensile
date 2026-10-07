package store_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/probe"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
)

// Encoding is one writer at a time, from the tables' count before a record is encoded to the commit or rollback of
// the transaction that keeps what it added. Two encoders interleaved used to share one count: the first's transaction
// wrote the second's entries, and when it rolled back it forgot them both, so the second committed a record naming
// entries no table held, which the next encoding numbered for other strings. Here readings that each add a string of
// their own are stored from several goroutines while some of their transactions fail; every one that was stored must
// read back as written in a process that loads the tables from the store.
func TestEncodersOnSeveralGoroutinesKeepEveryEntryTheirRecordsName(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "observer.db")
	st, err := store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	pub := slimPublication(t, 0x61, 5)
	pubLine, _ := json.Marshal(pub)
	if _, err := st.UpsertPublication(pub, pubLine); err != nil {
		t.Fatal(err)
	}
	// a transaction that fails after its entries were written in it
	if _, err := st.DB().Exec(`CREATE TRIGGER fail_some BEFORE INSERT ON probes WHEN NEW.schedule_label = 'fail'
		BEGIN SELECT RAISE(ABORT, 'injected'); END`); err != nil {
		t.Fatal(err)
	}
	const workers, each = 8, 40
	type stored struct {
		key  string
		line []byte
	}
	var mu sync.Mutex
	var kept []stored
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < each; i++ {
				v := pub.Assignment.Validators[(w+i)%len(pub.Assignment.Validators)]
				m := slimReading(pub, v, u32(v.Rows))
				m.ScheduledAt = m.ScheduledAt.Add(time.Duration(w*each+i) * time.Second)
				m.Outcome, m.Classification = probe.OutcomeServerError, probe.ClassFault
				m.RawError = fmt.Sprintf("worker %d reading %d: a message no other reading carries", w, i)
				if (w+i)%3 == 0 {
					m.ScheduleLabel = "fail"
				}
				l, _ := json.Marshal(m)
				if ok, err := st.InsertProbe(m, l); err == nil && ok {
					mu.Lock()
					kept = append(kept, stored{m.DedupeKey(), l})
					mu.Unlock()
				}
			}
		}(w)
	}
	wg.Wait()
	st.Close()
	if len(kept) == 0 {
		t.Fatal("nothing was stored")
	}
	ro, err := store.OpenReadOnly(path)
	if err != nil {
		t.Fatal(err)
	}
	defer ro.Close()
	bad := 0
	for _, k := range kept {
		var raw []byte
		if err := ro.DB().QueryRow(`SELECT raw_json FROM probes WHERE dedupe_key = ?`, k.key).Scan(&raw); err != nil {
			t.Fatal(err)
		}
		got, err := ro.ProbeRecord(ctx, ro.DB(), pub.PromiseHash, raw)
		if err != nil || !bytes.Equal(got, k.line) {
			if bad++; bad <= 3 {
				t.Errorf("%s: %v\n got %.200s\nwant %.200s", k.key, err, got, k.line)
			}
		}
	}
	if bad > 0 {
		t.Fatalf("%d of %d stored readings do not read back", bad, len(kept))
	}
}

// A publication stored already, whose line adds table entries (a record written again with a string no record carried
// before), keeps them only if its transaction commits. One whose commit fails forgets them, so the next record that
// names them writes them, and a process that loads the tables from the store reads that record back.
func TestAFailedCommitOfAStoredPublicationKeepsNoEntry(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "observer.db")
	st, err := store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	pub := slimPublication(t, 0x63, 5)
	line, _ := json.Marshal(pub)
	if ok, err := st.UpsertPublication(pub, line); err != nil || !ok {
		t.Fatalf("publication: %v %v", ok, err)
	}
	// a commit that fails: a constraint checked only at commit, broken by every entry written
	for _, q := range []string{
		`CREATE TABLE fail_parent (id INTEGER PRIMARY KEY)`,
		`CREATE TABLE fail_child (id INTEGER REFERENCES fail_parent (id) DEFERRABLE INITIALLY DEFERRED)`,
		`CREATE TRIGGER fail_commit AFTER INSERT ON slim_entries BEGIN INSERT INTO fail_child VALUES (1); END`,
	} {
		if _, err := st.DB().Exec(q); err != nil {
			t.Fatal(err)
		}
	}
	const signer = "celestia1asignernorecordcarriedbefore"
	again := pub
	again.Signer = signer
	againLine, _ := json.Marshal(again)
	if ok, err := st.UpsertPublication(again, againLine); err == nil || ok {
		t.Fatalf("the commit was to fail: %v %v", ok, err)
	}
	if _, err := st.DB().Exec(`DROP TRIGGER fail_commit`); err != nil {
		t.Fatal(err)
	}

	next := slimPublication(t, 0x64, 5)
	next.Signer = signer
	nextLine, _ := json.Marshal(next)
	if ok, err := st.UpsertPublication(next, nextLine); err != nil || !ok {
		t.Fatalf("the next publication: %v %v", ok, err)
	}
	st.Close()
	ro, err := store.OpenReadOnly(path)
	if err != nil {
		t.Fatal(err)
	}
	defer ro.Close()
	var raw []byte
	if err := ro.DB().QueryRow(`SELECT raw_json FROM publications WHERE promise_hash = ?`, next.PromiseHash).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	if got, err := ro.Record(ctx, ro.DB(), raw); err != nil || !bytes.Equal(got, nextLine) {
		t.Fatalf("the next publication in another process: %v\n got %.200s\nwant %.200s", err, got, nextLine)
	}
}
