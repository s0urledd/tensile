package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/probe"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/export"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/recordcheck"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
)

// heartbeats is a store holding three of 2026-10-05's heartbeats and a data directory whose reachability.jsonl holds
// their lines, as the collector and the heartbeat leave them.
func heartbeats(t *testing.T) (*store.Store, string) {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "observer.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	at := time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)
	var lines []byte
	for i := 0; i < 3; i++ {
		s := at.Add(time.Duration(i) * time.Minute)
		m := probe.Measurement{SchemaVersion: 2, Vantage: "ut-1", ValidatorAddress: fmt.Sprintf("%040x", i+1), ValidatorHost: "10.0.0.1:7980",
			ScheduledAt: s, StartedAt: s.Add(time.Second), FinishedAt: s.Add(2 * time.Second), Phase: probe.PhaseInWindow,
			Outcome: probe.OutcomeServedOK, Classification: probe.ClassHealthy, TotalDurationMS: 1000}
		l, _ := json.Marshal(m)
		if ok, err := st.InsertReachability(m, l); err != nil || !ok {
			t.Fatalf("heartbeat %d: %v %v", i, ok, err)
		}
		lines = append(append(lines, l...), '\n')
	}
	data := t.TempDir()
	if err := os.WriteFile(filepath.Join(data, "reachability.jsonl"), lines, 0o644); err != nil {
		t.Fatal(err)
	}
	return st, data
}

// exportAt builds 2026-10-05's export from data as the collector does, at now, and returns the exports dir and the
// day's index entry.
func exportAt(t *testing.T, data string, now time.Time) (string, export.Entry) {
	t.Helper()
	dir := filepath.Join(data, "exports")
	b := &export.Builder{DataDir: data, Dir: dir, Vantage: "ut-1", Build: "t", Hour: 3}
	if built, err := b.Run(now); err != nil || len(built) != 1 {
		t.Fatalf("built %v err %v", built, err)
	}
	idx, err := export.ReadIndex(dir)
	if err != nil || len(idx) != 1 {
		t.Fatalf("index %+v err %v", idx, err)
	}
	return dir, idx[0]
}

// With -ledger, a day is checked and recorded once; a later run lists it from the ledger without checking it, unless
// it is named with -day or its tarball was rebuilt since; and a day the ledger holds as not reproducible fails the run
// as one checked would.
func TestLedgerRunsCheckOnlyWhatTheLedgerDoesNotHold(t *testing.T) {
	st, data := heartbeats(t)
	dir, e := exportAt(t, data, time.Date(2026, 10, 6, 4, 0, 0, 0, time.UTC))
	ledger := filepath.Join(dir, recordcheck.LedgerFile)
	ctx := context.Background()
	run := func(days ...string) ([]recordcheck.DayReport, int, string) {
		t.Helper()
		var out bytes.Buffer
		reports, code, err := verifyDays(ctx, st, dir, days, ledger, "", "2026-10-06", &out, nil)
		if err != nil {
			t.Fatal(err)
		}
		return reports, code, out.String()
	}
	held := func() recordcheck.LedgerDay {
		t.Helper()
		l, err := recordcheck.ReadLedger(ledger)
		if err != nil {
			t.Fatal(err)
		}
		return l[e.Name]
	}

	reports, code, out := run()
	if len(reports) != 1 || !reports[0].Reproducible || code != 0 {
		t.Fatalf("the first run: %+v exit %d\n%s", reports, code, out)
	}
	first := held()
	if first.SHA256 != e.SHA256 || !first.Reproducible() {
		t.Fatalf("the first run did not record the day: %+v", first)
	}

	if reports, code, out = run(); len(reports) != 0 || code != 0 || !strings.Contains(out, "day| 2026-10-05 "+e.Name+": unchanged since checked at ") {
		t.Fatalf("a day the ledger holds was checked again: %+v exit %d\n%s", reports, code, out)
	}
	if reports, _, out = run("2026-10-05"); len(reports) != 1 || held().CheckedAt.Equal(first.CheckedAt) {
		t.Fatalf("a day named with -day was not checked and recorded: %+v\n%s", reports, out)
	}

	// The day rebuilt (the export's state lost, the same lines exported at another hour) is another tarball.
	if err := os.Remove(filepath.Join(dir, "state.json")); err != nil {
		t.Fatal(err)
	}
	_, e2 := exportAt(t, data, time.Date(2026, 10, 6, 7, 0, 0, 0, time.UTC))
	if e2.SHA256 == e.SHA256 {
		t.Fatal("the rebuilt tarball has the old digest")
	}
	if reports, code, out = run(); len(reports) != 1 || code != 0 || held().SHA256 != e2.SHA256 {
		t.Fatalf("the rebuilt day was not checked again and recorded: %+v exit %d, ledger %s\n%s", reports, code, held().SHA256, out)
	}

	// The ledger says the day's heartbeats are not all in the store: the run lists it, says why and fails.
	not := held()
	not.Files = map[string]recordcheck.LedgerMember{"reachability.jsonl": {Lines: 3, Identical: 2, Why: "1 lines missing"}}
	if err := recordcheck.MergeLedger(ledger, map[string]recordcheck.LedgerDay{e.Name: not}); err != nil {
		t.Fatal(err)
	}
	if reports, code, out = run(); len(reports) != 0 || code != 1 || !strings.Contains(out, "NOT reproducible") || !strings.Contains(out, "file| reachability.jsonl: 1 lines missing") {
		t.Fatalf("a day held as not reproducible: %+v exit %d\n%s", reports, code, out)
	}
}

// A day's check gives the disk back where it holds nothing, as observer-archive -retire's does: the pause is asked
// before each member of the tarball, and one that ends with an error (the run stopped while it waited) ends the run
// with it, exit status 2, before the day is recorded.
func TestDaysCheckedPauseWhereTheyHoldNothing(t *testing.T) {
	st, data := heartbeats(t)
	dir, e := exportAt(t, data, time.Date(2026, 10, 6, 4, 0, 0, 0, time.UTC))
	ctx := context.Background()
	ledger := filepath.Join(dir, recordcheck.LedgerFile)
	calls := 0
	var out bytes.Buffer
	reports, code, err := verifyDays(ctx, st, dir, nil, ledger, "", "2026-10-06", &out, func(context.Context) error { calls++; return nil })
	if err != nil || code != 0 || len(reports) != 1 || calls < len(e.Files) {
		t.Fatalf("%d pause(s) for %d members: %+v exit %d %v\n%s", calls, len(e.Files), reports, code, err, out.String())
	}
	stopped := errors.New("stopped while it waited")
	other := filepath.Join(t.TempDir(), recordcheck.LedgerFile)
	if _, code, err := verifyDays(ctx, st, dir, []string{e.Day}, other, "", "2026-10-06", &out, func(context.Context) error { return stopped }); !errors.Is(err, stopped) || code != 2 {
		t.Fatalf("a pause that ended with an error: exit %d %v", code, err)
	}
	if _, err := os.Stat(other); !os.IsNotExist(err) {
		t.Fatalf("a day whose check was stopped was recorded: %v", err)
	}
}
