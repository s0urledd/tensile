package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/pace"
	"github.com/plsgiveup/fibre/fibre-sentinel/internal/scan"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/collect"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
)

// The slim backfill takes its turn after the ingest: never while an ingest is due, never while the disk is busy
// (said once, and again when it is not), and with -slim-backfill-budget 0 never at all. A turn converts, reports on
// the status file and in the log, and once every table is done says so once; after that a turn does nothing.
func TestTheSlimBackfillTakesItsTurnAfterTheIngest(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 10, 8, 12, 0, 0, 0, time.UTC)
	st := openStore(t)
	p := scan.Publication{SchemaVersion: 3, PromiseHash: strings.Repeat("ab", 32), SettlementHeight: 10, Signer: "celestia1xyz"}
	line, _ := json.Marshal(p)
	if _, err := st.UpsertPublication(p, line); err != nil {
		t.Fatal(err)
	}
	// as a build before the slim record stored it
	if _, err := st.DB().Exec(`UPDATE publications SET raw_json = ?`, string(line)); err != nil {
		t.Fatal(err)
	}
	isLine := func() bool {
		var raw []byte
		if err := st.DB().QueryRow(`SELECT raw_json FROM publications`).Scan(&raw); err != nil {
			t.Fatal(err)
		}
		if back, err := st.Record(ctx, st.DB(), raw); err != nil || !bytes.Equal(back, line) {
			t.Fatalf("the publication reads back: %v", err)
		}
		return raw[0] == '{'
	}
	var logs []string
	logf := func(f string, a ...any) { logs = append(logs, fmt.Sprintf(f, a...)) }
	live, work := &fakeLive{}, collect.NewWorkErrors(nil)
	notDue := func() bool { return false }

	if newSlimBackfill(st, 0, logf, live, work) != nil {
		t.Fatal("-slim-backfill-budget 0 runs the backfill")
	}
	var off *slimBackfill
	off.run(ctx, now, notDue)

	sb := newSlimBackfill(st, time.Second, logf, live, work)
	pressure := pace.Pressure{Some: 9}
	sb.pacer.Source = func() (pace.Pressure, error) { return pressure, nil }
	sb.run(ctx, now, func() bool { return true })
	if !isLine() || len(logs) != 0 || live.get("slim_backfill") != nil {
		t.Fatalf("a turn while an ingest is due: log %q, status %v", logs, live.get("slim_backfill"))
	}
	sb.run(ctx, now, notDue)
	sb.run(ctx, now, notDue)
	if !isLine() || len(logs) != 1 || !strings.Contains(logs[0], "the disk is busy (some avg10 9.00 > 6, full avg10 0.00)") {
		t.Fatalf("turns while the disk is busy: log %q", logs)
	}

	pressure = pace.Pressure{Some: 0.5}
	sb.run(ctx, now, notDue)
	all := strings.Join(logs, "\n")
	for _, want := range []string{"the disk is no longer busy", "slim backfill: publications done: 1 row(s) read", "slim backfill: every table done"} {
		if !strings.Contains(all, want) {
			t.Errorf("the log says nothing of %q:\n%s", want, all)
		}
	}
	if isLine() {
		t.Error("the publication keeps its line")
	}
	status, _ := live.get("slim_backfill").(map[string]any)
	if status["done_at"] == nil || status["publications"] == nil {
		t.Fatalf("status %v", status)
	}
	if at, err := st.Meta(store.MetaBackfillDoneAt); err != nil || at == "" {
		t.Fatalf("done at %q, %v", at, err)
	}
	if f := work.Failing(); len(f) != 0 {
		t.Fatalf("failing %q", f)
	}
	said := len(logs)
	live.Set("slim_backfill", nil)
	sb.run(ctx, now.Add(time.Hour), notDue)
	if len(logs) != said || live.get("slim_backfill") != nil {
		t.Fatalf("a turn once done: log %q, status %v", logs[said:], live.get("slim_backfill"))
	}
}
