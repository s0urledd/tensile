// Package collect is the collector's pass over the record: what
// observer-collector does every -interval to the store from the files the
// scanner and the prober append to, in the order it has to be done.
//
// It is main.go's pass, moved here unchanged so that something other than
// the collector's own main loop can drive it: the API's equality harness
// (observer/api) runs the collector exactly as it runs in production, pass
// after pass under a clock of its own, and compares what the API publishes
// after each one. What the collector does besides (the chain polls, the
// escrow and endpoint history, the exports, the hosting lookups, the run
// heartbeat) needs a node or the network and stays in main.go, after Pass.
// So does the fast tick between passes (fast.go, watch.go), which tails
// state.json, the publications and the payments only, in the same select
// loop as Pass, so it never writes beside one.
package collect

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/probe"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/correct"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/ingest"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/rollup"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
)

// Paths are the files a pass reads, each defaulted by observer-collector
// from its -data-dir.
type Paths struct {
	State, Publications, Measurements, SampledOut, Reachability, VantagesDir string
	Registry, Payments, Runs, SamplingSecrets, HostHistory, Amendments       string
	ParamUncertainty, Corrections                                            string
}

// Status is the part of the collector's status file (internal/status) a
// pass reports to.
type Status interface {
	Error(msg string)
	Set(key string, v any)
}

// Collector is one collector's pass state.
type Collector struct {
	St      *store.Store
	Paths   Paths
	Vantage string
	// Logf is the collector's log; Live its status file, which may be nil.
	Logf func(format string, args ...any)
	Live Status
	// AmendFile is amendments.jsonl, open for appending: late shadow
	// verdicts are this collector's own judgement and go to it before the
	// store (judgeLate).
	AmendFile *os.File
	// PruneTolerance is -prune-tolerance.
	PruneTolerance time.Duration
	// Corrector moves the deadlines and verdicts a verified params range
	// proves, writing corrections.jsonl.
	Corrector *correct.Corrector
	// Retention is the rollup policy (rows are kept for good: the rollup
	// deletes nothing), run every RetentionEvery (0 never).
	Retention      rollup.Config
	RetentionEvery time.Duration

	lastRetention time.Time
	// lastWaiting is the day the rollup last said it waits on.
	lastWaiting string
	// reclaimPending: the first pass gives back whatever the schema
	// migration freed: the sampled-out collapse (migration 24) deletes most
	// of the probe rows a store holds, and a DELETE alone never shrinks the
	// file.
	reclaimPending  bool
	frontierMissing bool
}

// New is a Collector whose first pass gives back the pages a migration
// freed.
func New(c Collector) *Collector {
	c.reclaimPending = true
	return &c
}

func (c *Collector) logf(format string, args ...any) {
	if c.Logf != nil {
		c.Logf(format, args...)
	}
}

func (c *Collector) liveError(msg string) {
	if c.Live != nil {
		c.Live.Error(msg)
	}
}

func (c *Collector) liveSet(key string, v any) {
	if c.Live != nil {
		c.Live.Set(key, v)
	}
}

// DefaultPaths are the files under dataDir, as observer-collector names
// them by default.
func DefaultPaths(dataDir string) Paths {
	return Paths{
		State:            filepath.Join(dataDir, "state.json"),
		Publications:     filepath.Join(dataDir, "publications.jsonl"),
		Measurements:     filepath.Join(dataDir, "measurements.jsonl"),
		SampledOut:       filepath.Join(dataDir, probe.SampledOutFile),
		Reachability:     filepath.Join(dataDir, "reachability.jsonl"),
		VantagesDir:      filepath.Join(dataDir, ingest.VantagesDir),
		Registry:         filepath.Join(dataDir, "registry.jsonl"),
		Payments:         filepath.Join(dataDir, "payments.jsonl"),
		Runs:             filepath.Join(dataDir, "runs.jsonl"),
		SamplingSecrets:  filepath.Join(dataDir, "sampling-secrets.jsonl"),
		HostHistory:      filepath.Join(dataDir, "host_history.jsonl"),
		Amendments:       filepath.Join(dataDir, "amendments.jsonl"),
		ParamUncertainty: filepath.Join(dataDir, "param_uncertainty.jsonl"),
		Corrections:      filepath.Join(dataDir, "corrections.jsonl"),
	}
}

// Pass does one pass at now: every file tailed into the store, the
// sampled-out rows collapsed, the write-ahead log checkpointed, the late
// shadow verdicts judged, the verified params ranges corrected, the holds
// synced, and the rollup pass run when it is due. It returns what failed
// to ingest; the collector names that on its status file last, after the
// chain polls, so that a chain-status OK does not overwrite it.
func (c *Collector) Pass(ctx context.Context, now time.Time) []string {
	st := c.St
	if err := ingest.State(st, c.Paths.State, now); err != nil {
		c.logf("state: %v", err)
	}
	// Every ingest failure used to be a log line and nothing else, while
	// the collector's OK flag was set by the unrelated chain-status poll
	// — so a collector that could read the tip and could not read a
	// single file reported itself healthy, and the site went on serving
	// the last figures with nothing saying the record had stopped
	// reaching it.
	var passErrs []string
	fail := func(what string, err error) {
		c.logf("%s: %v", what, err)
		passErrs = append(passErrs, fmt.Sprintf("%s: %v", what, err))
	}
	if r, err := ingest.Publications(st, c.Paths.Publications, now); err != nil {
		fail("publications", err)
	} else {
		if r.Inserted > 0 {
			c.logf("publications: +%d (read %d, line %d)", r.Inserted, r.Read, r.Line)
		}
		if r.Skipped > 0 {
			c.logf("publications: WARNING skipped %d undecodable line(s); last: %s", r.Skipped, r.LastSkipped)
		}
	}
	// Before the measurements, deliberately. A range says how to read
	// the rows of the publications it covers, and InsertProbe decides
	// there and then whether a row is born withheld. Ingesting the
	// ranges after the measurements left every row of the pass that
	// first carried a range readable as FAULT until the hold sync at
	// the end of that pass — and for as long as the collector stayed
	// down, if it stopped in between.
	//
	// The ranges come in before the corrections that close them, and
	// both come in before the holds are synced, so a range and the
	// correction that lifts it can never be half-applied in one pass.
	if r, err := ingest.ParamUncertainty(st, c.Paths.ParamUncertainty, now); err != nil {
		fail("param uncertainty", err)
	} else if r.Inserted > 0 {
		c.logf("param uncertainty: +%d range(s) (read %d, line %d)", r.Inserted, r.Read, r.Line)
	}
	if r, err := ingest.Corrections(st, c.Paths.Corrections, now); err != nil {
		fail("corrections", err)
	} else if r.Inserted > 0 {
		c.logf("corrections: +%d deadline/verdict correction(s) replayed (read %d, line %d)", r.Inserted, r.Read, r.Line)
	}
	if r, err := ingest.Measurements(st, c.Paths.Measurements, now); err != nil {
		fail("measurements", err)
	} else {
		if r.Inserted > 0 {
			c.logf("measurements: +%d (read %d, line %d)", r.Inserted, r.Read, r.Line)
		}
		if r.Skipped > 0 {
			c.logf("measurements: WARNING skipped %d undecodable line(s); last: %s", r.Skipped, r.LastSkipped)
		}
	}
	// After the measurements: a decision is not stored beside rows its
	// vantage already wrote for the promise (store.InsertSampledOut).
	if r, err := ingest.SampledOut(st, c.Paths.SampledOut, now); err != nil {
		fail("sampling decisions", err)
	} else {
		if r.Inserted > 0 {
			c.logf("sampling decisions: +%d sampled-out publication(s) (read %d, line %d)", r.Inserted, r.Read, r.Line)
		}
		if r.Skipped > 0 {
			c.logf("sampling decisions: WARNING skipped %d undecodable line(s); last: %s", r.Skipped, r.LastSkipped)
		}
	}
	// Rows a prober wrote for a sampled-out publication before the
	// decision had a record of its own become that decision: the
	// migration did it for the store it found, this does it for rows
	// read since (a store rebuilt from an older measurements.jsonl).
	// The pages go back to the filesystem, here and on the first pass
	// after the migration freed them.
	if n, rows, err := st.CollapseSampledOut(ctx); err != nil {
		fail("sampled-out rows", err)
	} else if n > 0 {
		c.logf("sampled-out rows: %d row(s) recorded as %d decision(s)", rows, n)
		c.reclaimPending = true
	}
	if c.reclaimPending {
		if freed, err := st.ReclaimSpace(ctx, 20000); err != nil {
			c.logf("reclaim: %v", err)
			c.reclaimPending = false
		} else if freed > 0 {
			c.logf("reclaim: %d page(s) returned to the filesystem", freed)
		} else {
			c.reclaimPending = false
		}
	}
	if r, err := ingest.Reachability(st, c.Paths.Reachability, now); err != nil {
		fail("reachability", err)
	} else {
		if r.Inserted > 0 {
			c.logf("reachability: +%d (read %d, line %d)", r.Inserted, r.Read, r.Line)
		}
		if r.Skipped > 0 {
			c.logf("reachability: WARNING skipped %d undecodable line(s); last: %s", r.Skipped, r.LastSkipped)
		}
	}
	// Other vantages' heartbeats, copied in whole by rsync. They confirm
	// or contradict this observer's own failed checks (the API's
	// reachabilityNow) and are counted in no published figure. Listed
	// again every pass, so a vantage that starts sending is picked up
	// without a restart.
	if files, err := ingest.VantageFiles(c.Paths.VantagesDir); err != nil {
		fail("vantages", err)
	} else {
		for _, f := range files {
			name := filepath.Base(filepath.Dir(f))
			if r, err := ingest.VantageReachability(st, f, c.Vantage, now); err != nil {
				fail("reachability from "+name, err)
			} else {
				if r.Inserted > 0 {
					c.logf("reachability from %s: +%d (read %d, line %d)", name, r.Inserted, r.Read, r.Line)
				}
				if r.Skipped > 0 {
					c.logf("reachability from %s: WARNING skipped %d undecodable line(s); last: %s", name, r.Skipped, r.LastSkipped)
				}
			}
		}
	}
	if r, err := ingest.Registry(st, c.Paths.Registry, now); err != nil {
		fail("registry", err)
	} else if r.Inserted > 0 {
		c.logf("registry: +%d endpoint event(s) replayed (read %d, line %d)", r.Inserted, r.Read, r.Line)
	}
	if r, err := ingest.Payments(st, c.Paths.Payments, now); err != nil {
		fail("payments", err)
	} else {
		if r.Inserted > 0 {
			c.logf("payments: +%d (read %d, line %d)", r.Inserted, r.Read, r.Line)
		}
		if r.Skipped > 0 {
			c.logf("payments: WARNING skipped %d undecodable line(s); last: %s", r.Skipped, r.LastSkipped)
		}
	}
	if r, err := ingest.Runs(st, c.Paths.Runs, now); err != nil {
		fail("runs", err)
	} else if r.Inserted > 0 {
		c.logf("runs: +%d run event(s) replayed (read %d, line %d)", r.Inserted, r.Read, r.Line)
	}
	if r, err := ingest.SamplingSecrets(st, c.Paths.SamplingSecrets, now); err != nil {
		fail("sampling secrets", err)
	} else if r.Inserted > 0 {
		c.logf("sampling secrets: +%d day(s) revealed (read %d, line %d)", r.Inserted, r.Read, r.Line)
	}
	if r, err := ingest.HostEvents(st, c.Paths.HostHistory, now); err != nil {
		fail("host history", err)
	} else if r.Inserted > 0 {
		c.logf("host history: +%d registration(s) (read %d, line %d)", r.Inserted, r.Read, r.Line)
	}
	if r, err := ingest.Amendments(st, c.Paths.Amendments, now); err != nil {
		fail("amendments", err)
	} else if r.Inserted > 0 {
		c.logf("amendments: +%d late verdict(s) replayed (read %d, line %d)", r.Inserted, r.Read, r.Line)
	}
	// Copy the write-ahead log back and truncate it while nothing is
	// reading. A pass that ingested a backlog can leave hundreds of
	// megabytes of WAL behind otherwise, and SQLite will not reset it on
	// its own while the API holds a snapshot.
	if busy, inLog, done, err := st.CheckpointWAL(ctx); err != nil {
		c.logf("wal checkpoint: %v", err)
	} else if !busy && done > 0 && inLog > 2000 {
		c.logf("wal checkpoint: %d of %d frame(s) written back and the log truncated", done, inLog)
	}
	c.judgeLate(ctx, now)
	// Corrections first, then the hold sync. A verified range only
	// stops holding once every deadline it covers has actually been
	// re-derived, so the flag can never be cleared on a row the
	// correction has not reached — and a publication covered by two
	// overlapping ranges keeps its hold until the second one closes
	// too, because SyncParamHolds recomputes the flag from the ranges
	// rather than clearing it per range.
	if n, err := c.Corrector.Run(ctx, now); err != nil {
		c.logf("corrections: %v", err)
		c.liveError(fmt.Sprintf("corrections: %v", err))
	} else if n > 0 {
		c.liveSet("param_corrections", n)
		bumpHoldsRevision(st, now)
	}
	if changed, err := st.SyncParamHolds(ctx); err != nil {
		c.logf("param holds: %v", err)
		c.liveError(fmt.Sprintf("param holds: %v", err))
	} else if changed > 0 {
		c.logf("param holds: %d row(s) changed", changed)
		bumpHoldsRevision(st, now)
	}
	if c.RetentionEvery > 0 && now.Sub(c.lastRetention) >= c.RetentionEvery {
		c.lastRetention = now
		if rep, err := rollup.Run(ctx, st, now, c.Retention); err != nil {
			c.logf("retention: %v", err)
			c.liveError(fmt.Sprintf("retention: %v", err))
		} else {
			if len(rep.RolledDays) > 0 {
				c.logf("retention: rolled up %d day(s) through %s (%d obligations still pending at roll)", len(rep.RolledDays), rep.RolledDays[len(rep.RolledDays)-1], rep.PendingAtRoll)
				c.liveSet("rollup_through", rep.RolledDays[len(rep.RolledDays)-1])
			}
			if rep.PendingAtRoll > 0 {
				c.logf("retention: WARNING %d obligation(s) were still pending when their day was rolled; -rollup-after is shorter than a retention window", rep.PendingAtRoll)
			}
			// A day that is not final holds every later one: say so once
			// when it starts holding, and keep it in the status file.
			if rep.Waiting != c.lastWaiting {
				if rep.Waiting != "" {
					c.logf("retention: rollup waiting on %s: %s", rep.Waiting, rep.WaitingWhy)
				}
				c.lastWaiting = rep.Waiting
			}
			c.liveSet("rollup_waiting", strings.TrimSpace(rep.Waiting+" "+rep.WaitingWhy))
		}
	}
	return passErrs
}

// judgeLate draws the deferred shadow verdicts the scanner's frontier now
// allows, and records each in amendments.jsonl before the store.
func (c *Collector) judgeLate(ctx context.Context, now time.Time) {
	st := c.St
	v, err := st.Meta("last_scanned_time")
	if err != nil || v == "" {
		if !c.frontierMissing {
			c.logf("late verdicts: the scanner's frontier time is not on record yet (state.json last_scanned_time); deferred shadow verdicts wait")
			c.frontierMissing = true
		}
		return
	}
	frontier, err := time.Parse(store.TimeLayout, v)
	if err != nil {
		c.logf("late verdicts: bad last_scanned_time %q", v)
		return
	}
	ams, err := st.LateShadowVerdicts(ctx, frontier, now, c.PruneTolerance)
	if err != nil {
		c.logf("late verdicts: %v", err)
		c.liveError(fmt.Sprintf("late verdicts: %v", err))
		return
	}
	applied := 0
	for _, a := range ams {
		// The line is appended and fsynced BEFORE the amendment is
		// applied. The two can only fail in one direction: a line whose
		// amendment did not apply replays as a no-op, because
		// ApplyAmendment is idempotent on (dedupe_key, judged_at) and a
		// rebuild replays this file before anything is re-judged. An
		// applied amendment with no line is the other way round, and it
		// is permanent: the store would carry a verdict that changed
		// with nothing on record saying why, and the export would no
		// longer reproduce it. That is the state this ordering exists to
		// prevent, and it is the same ordering the store uses for its own
		// records.
		b, err := json.Marshal(a)
		if err != nil {
			c.logf("amendments: marshal %s: %v", a.DedupeKey, err)
			continue
		}
		if _, err := c.AmendFile.Write(append(b, '\n')); err != nil {
			c.logf("amendments: write: %v", err)
			c.liveError(fmt.Sprintf("amendments write: %v", err))
			continue
		}
		if err := c.AmendFile.Sync(); err != nil {
			c.logf("amendments: sync: %v", err)
			c.liveError(fmt.Sprintf("amendments sync: %v", err))
			continue
		}
		ok, err := st.ApplyAmendment(a)
		if err != nil {
			c.logf("late verdicts: apply %s: %v", a.DedupeKey, err)
			continue
		}
		if !ok {
			continue
		}
		applied++
		c.logf("late verdict: %s %s %s: %s -> %s", a.PromiseHash[:min(12, len(a.PromiseHash))], a.ValidatorAddress, a.ScheduledAt.UTC().Format(time.RFC3339), a.From, a.To)
	}
	if applied > 0 {
		c.liveSet("late_verdicts", applied)
	}
}

// bumpHoldsRevision tells the API that a hold was raised or lifted, or a
// verdict moved. Its cached aggregates run to a fifteen-minute TTL, and a
// withheld fault republished for minutes after the hold landed is the
// accusation the hold exists to stop.
//
// It goes through the store's counter, like every other path that moves
// this key. Writing now.UnixNano() here gave two bumps in one collector
// pass the same token, because the pass stamps one time.Now() and threads
// it through everything it ingests — and a token that does not change does
// not invalidate the snapshot holding the withdrawn fault.
func bumpHoldsRevision(st *store.Store, now time.Time) {
	if err := st.BumpParamHoldsRev(now); err != nil {
		log.Printf("param holds revision: %v", err)
	}
}
