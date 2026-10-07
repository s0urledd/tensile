package main

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/pace"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/collect"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
)

// The slim backfill (store.Backfill): the rows stored before the slim record,
// written in the slim forms a row this build inserts gets, a batch at a time,
// in the collector's own loop, after a pass. Here, and in no other process:
// the collector is the one writer of the slim tables, and a second process
// adding entries would number them differently.
//
// Ingest first. A turn is taken only after a pass that ingested every file,
// and only while no ingest is due: neither the next pass (its ticker has
// fired) nor the fast tick for a change the file watch saw to one of its
// files (without the watch, the fast tick's timer). The same is asked before
// every batch, so a blob the scanner has just written waits one batch at
// most, a fraction of a second.
//
// The disk. A turn is skipped while /proc/pressure/io says the disk is busy,
// by the rule the deploys hold their heavy work back by (internal/pace: "some
// avg10" above 6, or "full avg10" above 4): the store's disk is shared with a
// validator. It is read again before every batch, never with a transaction
// open, and a turn that finds the disk busy ends there rather than waits:
// the next pass comes round in seconds.
//
// A turn does -slim-backfill-budget of work at most (and the batch it is in
// when the budget runs out); -slim-backfill-budget 0 turns the backfill off.
// Once every table is done the store says so (meta slim_backfill_done_at),
// the log says so once, and a turn costs nothing.
//
// It reports on the status file (detail slim_backfill: each table's rowid
// done and the last to do, the values converted and kept as they are, the
// bytes given back to the filesystem) and in the log: where it stands every
// backfillLogEvery, each table when it is done, and the end. A turn that
// fails goes to the work list ("slim backfill"), and the next turn begins
// again at the batch that failed, which left the store as it was.
type slimBackfill struct {
	b      *store.Backfill
	budget time.Duration
	pacer  *pace.Pacer
	logf   logf
	live   liveStatus
	work   *collect.WorkErrors
	// busy: the last turn was skipped for a busy disk, which the log has said
	busy    bool
	lastLog time.Time
	lastErr string
}

// backfillLogEvery is how often the log says where the backfill stands.
const backfillLogEvery = 10 * time.Minute

// newSlimBackfill is the stage, or nil when budget is 0 (off).
func newSlimBackfill(st *store.Store, budget time.Duration, logf logf, live liveStatus, work *collect.WorkErrors) *slimBackfill {
	if budget <= 0 {
		return nil
	}
	return &slimBackfill{b: st.NewBackfill(), budget: budget, logf: logf, live: live, work: work,
		pacer: &pace.Pacer{Some: pace.DefaultSome, Full: pace.DefaultFull, Log: func(s string) { logf("%s", s) }}}
}

// run is one turn at now, right after a pass that ingested every file. due
// says whether an ingest is due.
func (sb *slimBackfill) run(ctx context.Context, now time.Time, due func() bool) {
	if sb == nil || sb.b.Done() || due() {
		return
	}
	if at, busy := sb.pacer.Busy("slim backfill"); busy {
		if !sb.busy {
			sb.logf("slim backfill: the disk is busy (%s); a turn is taken once it is not", sb.pacer.Describe(at))
			sb.busy = true
		}
		return
	}
	if sb.busy {
		sb.logf("slim backfill: the disk is no longer busy; going on")
		sb.busy = false
	}
	turn, err := sb.b.Run(ctx, sb.budget, func() bool {
		if due() {
			return true
		}
		_, busy := sb.pacer.Busy("slim backfill")
		return busy
	})
	if err != nil {
		if ctx.Err() != nil {
			return // the collector is stopping: the batch left the store as it was
		}
		if err.Error() != sb.lastErr {
			sb.logf("slim backfill: %v (the next turn begins again at this batch, which changed nothing)", err)
			sb.lastErr = err.Error()
		}
		sb.work.Report("slim backfill", err, now)
		sb.live.Set("slim_backfill", sb.b.Status())
		return
	}
	if sb.lastErr != "" {
		sb.logf("slim backfill: going on without errors again")
		sb.lastErr = ""
	}
	sb.work.Report("slim backfill", nil, now)
	if turn.Batches == 0 && !turn.DoneNow {
		return
	}
	tables := sb.b.Tables()
	for _, name := range turn.Finished {
		for _, tb := range tables {
			if tb.Table == name {
				sb.logf("slim backfill: %s done: %s", name, columnsLine(tb))
			}
		}
	}
	switch {
	case turn.DoneNow:
		sb.logf("slim backfill: every table done; since %s: %s; %s given back to the filesystem. From now on it reads nothing",
			sb.b.Since().UTC().Format(time.RFC3339), totalsLine(tables), mb(sb.b.Freed()))
	case now.Sub(sb.lastLog) >= backfillLogEvery:
		sb.logf("slim backfill: %s; since %s: %s; %s given back to the filesystem",
			whereLine(tables), sb.b.Since().UTC().Format(time.RFC3339), totalsLine(tables), mb(sb.b.Freed()))
		sb.lastLog = now
	}
	sb.live.Set("slim_backfill", sb.b.Status())
}

// whereLine is each table's last rowid done of the last to do.
func whereLine(tables []store.BackfillTable) string {
	parts := make([]string, len(tables))
	for i, tb := range tables {
		parts[i] = fmt.Sprintf("%s %d/%d", tb.Table, tb.Done, tb.Total)
	}
	return "rowids done " + strings.Join(parts, ", ")
}

// totalsLine is the values converted and kept as they are, over every table.
func totalsLine(tables []store.BackfillTable) string {
	var conv, kept int64
	for _, tb := range tables {
		for _, c := range tb.Columns {
			conv, kept = conv+c.Converted, kept+c.KeptAsIs
		}
	}
	return fmt.Sprintf("%d value(s) converted, %d of an earlier form kept as they are", conv, kept)
}

// columnsLine is what was done to each column of a table, with why any value
// was kept as it is and the first few such rows.
func columnsLine(tb store.BackfillTable) string {
	var parts []string
	for _, name := range sortedKeys(tb.Columns) {
		c := tb.Columns[name]
		s := fmt.Sprintf("%s %d converted, %d kept as they are", name, c.Converted, c.KeptAsIs)
		if c.KeptAsIs > 0 {
			s += " (" + c.ReasonsLine() + "; e.g. " + strings.Join(c.Examples, " | ") + ")"
		}
		parts = append(parts, s)
	}
	return fmt.Sprintf("%d row(s) read; ", tb.Seen) + strings.Join(parts, "; ")
}

func sortedKeys(m map[string]*store.BackfillColumn) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// mb is a size in megabytes, for the log.
func mb(n int64) string { return fmt.Sprintf("%.1f MB", float64(n)/1e6) }
