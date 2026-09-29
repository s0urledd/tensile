// observer-collector feeds the observer store from the sentinel tools'
// append-only files and from the chain's Fibre endpoint registry.
//
// It does not scan the chain itself: sentinel-scan keeps writing
// publications.jsonl and state.json, sentinel-probe keeps writing
// measurements.jsonl, and this process tails those files into SQLite with
// byte-offset cursors. On top of that it polls AllBondedFibreProviders and
// keeps the endpoint history, and it records its own run span so the
// dashboard can show observer downtime as a gap.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/probe"
	"github.com/plsgiveup/fibre/fibre-sentinel/internal/scan"
	"github.com/plsgiveup/fibre/fibre-sentinel/internal/status"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/correct"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/export"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/ingest"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/keybase"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/rollup"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
)

func main() {
	var (
		rpc       = flag.String("rpc", "http://127.0.0.1:26657", "CometBFT RPC endpoint (endpoint registry polls)")
		dataDir   = flag.String("data-dir", "./sentinel-data", "dir holding publications.jsonl, state.json and measurements.jsonl")
		dbPath    = flag.String("db", "", "SQLite database path (default <data-dir>/observer.db)")
		vantage   = flag.String("vantage", "local", "vantage name recorded on this run")
		interval  = flag.Duration("interval", 10*time.Second, "how often to tail the files")
		epEvery   = flag.Duration("endpoints-every", 60*time.Second, "how often to poll AllBondedFibreProviders (0 = never)")
		escEvery  = flag.Duration("escrow-every", 5*time.Minute, "how often to read every known publisher's escrow balance (one state query each; 0 = never)")
		rpcTO     = flag.Duration("rpc-timeout", 15*time.Second, "per-RPC-call timeout")
		once      = flag.Bool("once", false, "ingest everything currently on disk, poll endpoints once, then exit")
		logLines  = flag.Int("log-ring", 300, "log lines kept in memory for the crash dump")
		pubsPath  = flag.String("publications", "", "path to publications.jsonl (default <data-dir>/publications.jsonl)")
		statePath = flag.String("state", "", "path to state.json (default <data-dir>/state.json)")
		measPath  = flag.String("measurements", "", "path to measurements.jsonl (default <data-dir>/measurements.jsonl)")
		reachPath = flag.String("reachability", "", "path to reachability.jsonl (default <data-dir>/reachability.jsonl)")
		soPath    = flag.String("sampling-decisions", "", "path to sampling_decisions.jsonl, the prober's record of publications its load policy sampled out, one line each (default <data-dir>/sampling_decisions.jsonl)")
		vantDir   = flag.String("vantages-dir", "", "dir other vantages' heartbeats are copied to, <name>/reachability.jsonl each (default <data-dir>/vantages)")
		payPath   = flag.String("payments", "", "path to payments.jsonl (default <data-dir>/payments.jsonl)")
		regPath   = flag.String("registry", "", "path to registry.jsonl, this collector's own endpoint-history log (default <data-dir>/registry.jsonl)")
		runsPath  = flag.String("runs", "", "path to runs.jsonl, every component's record of its starts, stops and configuration (default <data-dir>/runs.jsonl)")
		secPath   = flag.String("sampling-secrets", "", "path to sampling-secrets.jsonl, the prober's revealed day secrets (default <data-dir>/sampling-secrets.jsonl)")
		amendPath = flag.String("amendments", "", "path to amendments.jsonl, this collector's own log of late shadow verdicts (default <data-dir>/amendments.jsonl)")
		hostsPath = flag.String("host-history", "", "path to host_history.jsonl, the scanner's record of Fibre host registrations from the chain's events (default <data-dir>/host_history.jsonl)")
		uncPath   = flag.String("param-uncertainty", "", "path to param_uncertainty.jsonl, the scanner's record of the height ranges it could not say which x/fibre params were in force over (default <data-dir>/param_uncertainty.jsonl)")
		corrPath  = flag.String("corrections", "", "path to corrections.jsonl, this collector's own log of the deadlines and verdicts a verified params range moved (default <data-dir>/corrections.jsonl)")
		pruneTol  = flag.Duration("prune-tolerance", 5*time.Minute, "how long past must_serve_until a promise's shard is still taken to be on disk when judging a deferred shadow verdict")
		expDir    = flag.String("exports-dir", "", "where the daily export tarballs are built (default <data-dir>/exports)")
		expHour   = flag.Int("export-hour", 3, "UTC hour after which a day's export is built, the grace for late rows (-1 = never build exports)")
		expKey    = flag.String("export-signing-key", os.Getenv(export.SigningKeyEnv), "ed25519 private key (PKCS#8 PEM, e.g. from openssl genpkey -algorithm ed25519) that signs every daily export's manifest digest; default $"+export.SigningKeyEnv+"; empty = exports are unsigned, exactly as before (docs/exports-signing.md)")
		retainRaw = flag.Duration("retain-raw", rollup.Default().RetainRaw, "keep probe and heartbeat rows this long; older rolled days are pruned, whole days at a time (0 = keep forever)")
		retainRJ  = flag.Duration("retain-raw-json", rollup.Default().RetainRawJSON, "keep a row's raw_json (the bulk of it) this long; every typed column stays (0 = keep forever)")
		rollAfter = flag.Duration("rollup-after", rollup.Default().RollupAfter, "compute a day's obligation and probe rollups this long after the day ends; must clear every retention window (0 = never roll up, so never prune)")
		retEvery  = flag.Duration("retention-every", time.Hour, "how often the retention pass runs")
		avEvery   = flag.Duration("avatars-every", time.Hour, "how often to look for validator Keybase pictures to fetch or refresh (0 = never)")
		avMaxAge  = flag.Duration("avatar-max-age", 24*time.Hour, "re-resolve a validator's Keybase picture after this long")
	)
	flag.Parse()

	if *dbPath == "" {
		*dbPath = filepath.Join(*dataDir, "observer.db")
	}
	if *pubsPath == "" {
		*pubsPath = filepath.Join(*dataDir, "publications.jsonl")
	}
	if *statePath == "" {
		*statePath = filepath.Join(*dataDir, "state.json")
	}
	if *measPath == "" {
		*measPath = filepath.Join(*dataDir, "measurements.jsonl")
	}
	if *soPath == "" {
		*soPath = filepath.Join(*dataDir, probe.SampledOutFile)
	}
	if *reachPath == "" {
		*reachPath = filepath.Join(*dataDir, "reachability.jsonl")
	}
	if *vantDir == "" {
		*vantDir = filepath.Join(*dataDir, ingest.VantagesDir)
	}
	if *payPath == "" {
		*payPath = filepath.Join(*dataDir, "payments.jsonl")
	}
	if *regPath == "" {
		*regPath = filepath.Join(*dataDir, "registry.jsonl")
	}
	if *runsPath == "" {
		*runsPath = filepath.Join(*dataDir, status.RunsFile)
	}
	if *secPath == "" {
		*secPath = filepath.Join(*dataDir, "sampling-secrets.jsonl")
	}
	if *expDir == "" {
		*expDir = filepath.Join(*dataDir, "exports")
	}
	if *amendPath == "" {
		*amendPath = filepath.Join(*dataDir, "amendments.jsonl")
	}
	if *uncPath == "" {
		*uncPath = filepath.Join(*dataDir, "param_uncertainty.jsonl")
	}
	if *corrPath == "" {
		*corrPath = filepath.Join(*dataDir, "corrections.jsonl")
	}
	if *hostsPath == "" {
		*hostsPath = filepath.Join(*dataDir, "host_history.jsonl")
	}

	log := scan.NewLogger(*logLines)

	st, err := store.Open(*dbPath)
	if err != nil {
		log.Fatalf("open store: %v", err)
	}
	defer st.Close()

	var chain *scan.Chain
	if *epEvery > 0 {
		chain, err = scan.NewChain(*rpc, *rpcTO, log)
		if err != nil {
			log.Fatalf("rpc client: %v", err)
		}
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// The commit this binary was built from, not a hand-maintained constant.
	// It goes into runs.jsonl, which is how a verifier ties a row in the
	// export to the code that produced it: every other component already
	// stamps the revision, and this one wrote "0.1.0" — a string that has
	// never changed and identifies nothing. The same value already went into
	// the export manifest from this very file, so one process was publishing
	// two different answers to "what built this".
	version := status.BuildRevision()
	runID, err := st.StartRun("collector", *vantage, version, time.Now())
	if err != nil {
		log.Fatalf("start run: %v", err)
	}
	live := status.New(*dataDir, "collector", *vantage, version)
	live.Start()
	defer live.Stop("exit")

	// The endpoint history has no source but the live polls, so every
	// opening and closing is appended here as well as written to the
	// database; on a rebuild the file is replayed before the first poll.
	regFile, err := os.OpenFile(*regPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		log.Fatalf("open %s: %v", *regPath, err)
	}
	defer regFile.Close()
	appendRegistry := func(evs []store.EndpointEvent) {
		for _, e := range evs {
			b, err := json.Marshal(e)
			if err != nil {
				continue
			}
			if _, err := regFile.Write(append(b, '\n')); err != nil {
				log.Printf("registry: write: %v", err)
				live.Error(fmt.Sprintf("registry write: %v", err))
				return
			}
		}
		if len(evs) > 0 {
			_ = regFile.Sync()
		}
	}
	var exporter *export.Builder
	if *expHour >= 0 {
		exporter = &export.Builder{DataDir: *dataDir, Dir: *expDir, Vantage: *vantage, Build: status.BuildRevision(), Hour: *expHour, Logf: log.Printf}
		// A configured key that cannot be loaded stops the collector rather
		// than falling back to unsigned: an operator who asked for signed
		// exports would otherwise publish unsigned ones without noticing.
		if *expKey != "" {
			signer, err := export.LoadSigner(*expKey)
			if err != nil {
				log.Fatalf("export signing key: %v", err)
			}
			exporter.Signer = signer
			log.Printf("exports are signed by %s", export.Fingerprint(signer.PublicKey()))
		}
	}
	// Late shadow verdicts are this collector's own judgement and, like the
	// endpoint history, have no source but this process: every one is
	// appended here as well as written to the database, and replayed on a
	// rebuild before anything is re-judged.
	amendFile, err := os.OpenFile(*amendPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		log.Fatalf("open %s: %v", *amendPath, err)
	}
	defer amendFile.Close()
	frontierMissing := false
	judgeLate := func(now time.Time) {
		v, err := st.Meta("last_scanned_time")
		if err != nil || v == "" {
			if !frontierMissing {
				log.Printf("late verdicts: the scanner's frontier time is not on record yet (state.json last_scanned_time); deferred shadow verdicts wait")
				frontierMissing = true
			}
			return
		}
		frontier, err := time.Parse(store.TimeLayout, v)
		if err != nil {
			log.Printf("late verdicts: bad last_scanned_time %q", v)
			return
		}
		ams, err := st.LateShadowVerdicts(ctx, frontier, now, *pruneTol)
		if err != nil {
			log.Printf("late verdicts: %v", err)
			live.Error(fmt.Sprintf("late verdicts: %v", err))
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
				log.Printf("amendments: marshal %s: %v", a.DedupeKey, err)
				continue
			}
			if _, err := amendFile.Write(append(b, '\n')); err != nil {
				log.Printf("amendments: write: %v", err)
				live.Error(fmt.Sprintf("amendments write: %v", err))
				continue
			}
			if err := amendFile.Sync(); err != nil {
				log.Printf("amendments: sync: %v", err)
				live.Error(fmt.Sprintf("amendments sync: %v", err))
				continue
			}
			ok, err := st.ApplyAmendment(a)
			if err != nil {
				log.Printf("late verdicts: apply %s: %v", a.DedupeKey, err)
				continue
			}
			if !ok {
				continue
			}
			applied++
			log.Printf("late verdict: %s %s %s: %s -> %s", a.PromiseHash[:min(12, len(a.PromiseHash))], a.ValidatorAddress, a.ScheduledAt.UTC().Format(time.RFC3339), a.From, a.To)
		}
		if applied > 0 {
			live.Set("late_verdicts", applied)
		}
	}
	corrFile, err := os.OpenFile(*corrPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		log.Fatalf("open %s: %v", *corrPath, err)
	}
	defer corrFile.Close()
	corr := correct.New(st, corrFile, *pruneTol)
	hostingPass := newHostingPass(st, *dataDir, *vantage, log.Printf) // hosting.go

	log.Printf("collector up: run=%d vantage=%s db=%s data=%s exports=%s", runID, *vantage, *dbPath, *dataDir, *expDir)

	retention := rollup.Config{RetainRaw: *retainRaw, RetainRawJSON: *retainRJ, RollupAfter: *rollAfter, Vantage: *vantage}
	var lastRetention time.Time
	var lastEscrow time.Time
	// The first pass gives back whatever the schema migration freed: the
	// sampled-out collapse (migration 24) deletes most of the probe rows a
	// store holds, and a DELETE alone never shrinks the file.
	reclaimPending := true
	pass := func(pollEndpoints bool) {
		now := time.Now()
		if err := ingest.State(st, *statePath, now); err != nil {
			log.Printf("state: %v", err)
		}
		// Every ingest failure used to be a log line and nothing else, while
		// the collector's OK flag was set by the unrelated chain-status poll
		// — so a collector that could read the tip and could not read a
		// single file reported itself healthy, and the site went on serving
		// the last figures with nothing saying the record had stopped
		// reaching it.
		var passErrs []string
		fail := func(what string, err error) {
			log.Printf("%s: %v", what, err)
			passErrs = append(passErrs, fmt.Sprintf("%s: %v", what, err))
		}
		if r, err := ingest.Publications(st, *pubsPath, now); err != nil {
			fail("publications", err)
		} else {
			if r.Inserted > 0 {
				log.Printf("publications: +%d (read %d, line %d)", r.Inserted, r.Read, r.Line)
			}
			if r.Skipped > 0 {
				log.Printf("publications: WARNING skipped %d undecodable line(s); last: %s", r.Skipped, r.LastSkipped)
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
		if r, err := ingest.ParamUncertainty(st, *uncPath, now); err != nil {
			fail("param uncertainty", err)
		} else if r.Inserted > 0 {
			log.Printf("param uncertainty: +%d range(s) (read %d, line %d)", r.Inserted, r.Read, r.Line)
		}
		if r, err := ingest.Corrections(st, *corrPath, now); err != nil {
			fail("corrections", err)
		} else if r.Inserted > 0 {
			log.Printf("corrections: +%d deadline/verdict correction(s) replayed (read %d, line %d)", r.Inserted, r.Read, r.Line)
		}
		if r, err := ingest.Measurements(st, *measPath, now); err != nil {
			fail("measurements", err)
		} else {
			if r.Inserted > 0 {
				log.Printf("measurements: +%d (read %d, line %d)", r.Inserted, r.Read, r.Line)
			}
			if r.Skipped > 0 {
				log.Printf("measurements: WARNING skipped %d undecodable line(s); last: %s", r.Skipped, r.LastSkipped)
			}
		}
		// After the measurements: a decision is not stored beside rows its
		// vantage already wrote for the promise (store.InsertSampledOut).
		if r, err := ingest.SampledOut(st, *soPath, now); err != nil {
			fail("sampling decisions", err)
		} else {
			if r.Inserted > 0 {
				log.Printf("sampling decisions: +%d sampled-out publication(s) (read %d, line %d)", r.Inserted, r.Read, r.Line)
			}
			if r.Skipped > 0 {
				log.Printf("sampling decisions: WARNING skipped %d undecodable line(s); last: %s", r.Skipped, r.LastSkipped)
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
			log.Printf("sampled-out rows: %d row(s) recorded as %d decision(s)", rows, n)
			reclaimPending = true
		}
		if reclaimPending {
			if freed, err := st.ReclaimSpace(ctx, 20000); err != nil {
				log.Printf("reclaim: %v", err)
				reclaimPending = false
			} else if freed > 0 {
				log.Printf("reclaim: %d page(s) returned to the filesystem", freed)
			} else {
				reclaimPending = false
			}
		}
		if r, err := ingest.Reachability(st, *reachPath, now); err != nil {
			fail("reachability", err)
		} else {
			if r.Inserted > 0 {
				log.Printf("reachability: +%d (read %d, line %d)", r.Inserted, r.Read, r.Line)
			}
			if r.Skipped > 0 {
				log.Printf("reachability: WARNING skipped %d undecodable line(s); last: %s", r.Skipped, r.LastSkipped)
			}
		}
		// Other vantages' heartbeats, copied in whole by rsync. They confirm
		// or contradict this observer's own failed checks (the API's
		// reachabilityNow) and are counted in no published figure. Listed
		// again every pass, so a vantage that starts sending is picked up
		// without a restart.
		if files, err := ingest.VantageFiles(*vantDir); err != nil {
			fail("vantages", err)
		} else {
			for _, f := range files {
				name := filepath.Base(filepath.Dir(f))
				if r, err := ingest.VantageReachability(st, f, *vantage, now); err != nil {
					fail("reachability from "+name, err)
				} else {
					if r.Inserted > 0 {
						log.Printf("reachability from %s: +%d (read %d, line %d)", name, r.Inserted, r.Read, r.Line)
					}
					if r.Skipped > 0 {
						log.Printf("reachability from %s: WARNING skipped %d undecodable line(s); last: %s", name, r.Skipped, r.LastSkipped)
					}
				}
			}
		}
		if r, err := ingest.Registry(st, *regPath, now); err != nil {
			fail("registry", err)
		} else if r.Inserted > 0 {
			log.Printf("registry: +%d endpoint event(s) replayed (read %d, line %d)", r.Inserted, r.Read, r.Line)
		}
		if r, err := ingest.Payments(st, *payPath, now); err != nil {
			fail("payments", err)
		} else {
			if r.Inserted > 0 {
				log.Printf("payments: +%d (read %d, line %d)", r.Inserted, r.Read, r.Line)
			}
			if r.Skipped > 0 {
				log.Printf("payments: WARNING skipped %d undecodable line(s); last: %s", r.Skipped, r.LastSkipped)
			}
		}
		if r, err := ingest.Runs(st, *runsPath, now); err != nil {
			fail("runs", err)
		} else if r.Inserted > 0 {
			log.Printf("runs: +%d run event(s) replayed (read %d, line %d)", r.Inserted, r.Read, r.Line)
		}
		if r, err := ingest.SamplingSecrets(st, *secPath, now); err != nil {
			fail("sampling secrets", err)
		} else if r.Inserted > 0 {
			log.Printf("sampling secrets: +%d day(s) revealed (read %d, line %d)", r.Inserted, r.Read, r.Line)
		}
		if r, err := ingest.HostEvents(st, *hostsPath, now); err != nil {
			fail("host history", err)
		} else if r.Inserted > 0 {
			log.Printf("host history: +%d registration(s) (read %d, line %d)", r.Inserted, r.Read, r.Line)
		}
		if r, err := ingest.Amendments(st, *amendPath, now); err != nil {
			fail("amendments", err)
		} else if r.Inserted > 0 {
			log.Printf("amendments: +%d late verdict(s) replayed (read %d, line %d)", r.Inserted, r.Read, r.Line)
		}
		// Copy the write-ahead log back and truncate it while nothing is
		// reading. A pass that ingested a backlog can leave hundreds of
		// megabytes of WAL behind otherwise, and SQLite will not reset it on
		// its own while the API holds a snapshot.
		if busy, inLog, done, err := st.CheckpointWAL(ctx); err != nil {
			log.Printf("wal checkpoint: %v", err)
		} else if !busy && done > 0 && inLog > 2000 {
			log.Printf("wal checkpoint: %d of %d frame(s) written back and the log truncated", done, inLog)
		}
		judgeLate(now)
		// Corrections first, then the hold sync. A verified range only
		// stops holding once every deadline it covers has actually been
		// re-derived, so the flag can never be cleared on a row the
		// correction has not reached — and a publication covered by two
		// overlapping ranges keeps its hold until the second one closes
		// too, because SyncParamHolds recomputes the flag from the ranges
		// rather than clearing it per range.
		if n, err := corr.Run(ctx, now); err != nil {
			log.Printf("corrections: %v", err)
			live.Error(fmt.Sprintf("corrections: %v", err))
		} else if n > 0 {
			live.Set("param_corrections", n)
			bumpHoldsRevision(st, now)
		}
		if changed, err := st.SyncParamHolds(ctx); err != nil {
			log.Printf("param holds: %v", err)
			live.Error(fmt.Sprintf("param holds: %v", err))
		} else if changed > 0 {
			log.Printf("param holds: %d row(s) changed", changed)
			bumpHoldsRevision(st, now)
		}
		if *retEvery > 0 && time.Since(lastRetention) >= *retEvery {
			lastRetention = now
			if rep, err := rollup.Run(ctx, st, now, retention); err != nil {
				log.Printf("retention: %v", err)
				live.Error(fmt.Sprintf("retention: %v", err))
			} else {
				if len(rep.RolledDays) > 0 {
					log.Printf("retention: rolled up %d day(s) through %s (%d obligations still pending at roll)", len(rep.RolledDays), rep.RolledDays[len(rep.RolledDays)-1], rep.PendingAtRoll)
					live.Set("rollup_through", rep.RolledDays[len(rep.RolledDays)-1])
				}
				if rep.PendingAtRoll > 0 {
					log.Printf("retention: WARNING %d obligation(s) were still pending when their day was rolled; -rollup-after is shorter than a retention window", rep.PendingAtRoll)
				}
				if rep.RawJSONDropped > 0 {
					log.Printf("retention: dropped raw_json from %d row(s)", rep.RawJSONDropped)
				}
				if len(rep.PrunedDays) > 0 {
					log.Printf("retention: pruned %d row(s) of %d day(s) through %s", rep.PrunedRows, len(rep.PrunedDays), rep.PrunedDays[len(rep.PrunedDays)-1])
					live.Set("raw_from", rep.PrunedDays[len(rep.PrunedDays)-1])
					// Give the pages back. A DELETE moves them to SQLite's
					// free list and the file never shrinks on its own, so
					// the pruning the operator was told to rely on when a
					// disk fills would have reclaimed nothing they could see.
					if freed, err := st.ReclaimSpace(ctx, 20000); err != nil {
						log.Printf("retention: reclaim: %v", err)
					} else if freed > 0 {
						log.Printf("retention: %d page(s) returned to the filesystem", freed)
					}
				}
			}
		}
		if exporter != nil {
			if built, err := exporter.Run(now); err != nil {
				log.Printf("export: %v", err)
				live.Error(fmt.Sprintf("export: %v", err))
			} else if len(built) > 0 {
				live.Set("last_export", built[len(built)-1])
			}
		}
		if chain != nil && *escEvery > 0 && time.Since(lastEscrow) >= *escEvery {
			// Escrow balances, one state query per publisher the payments
			// table has seen. There is no list-all-escrow query, so an account
			// that deposited but never appeared in a payment we ingested is
			// not polled; it also has nothing to show. On its own, slower
			// clock: a balance moves when a payment lands, and the payments
			// themselves arrive through the file, not this poll.
			lastEscrow = now
			// Balance and withdrawal queue of every known publisher, both
			// read from one height (withdrawals.go), then the block time
			// of every params change not yet dated.
			pollEscrow(ctx, chain, st, now, log.Printf)
			fillParamTimes(ctx, chain, st, log.Printf)
			// The total, from the module account every escrow lives in: exact
			// where the sum above is a floor, since it covers only accounts
			// this observer has seen publish.
			if bal, err := chain.ModuleBalance(ctx, scan.FibreModuleName, "utia"); err != nil {
				log.Printf("escrow: module balance: %v", err)
			} else {
				_ = st.SetMeta("escrow_module_utia", itoa(bal), now)
				_ = st.SetMeta("escrow_module_polled_at", store.TS(now), now)
			}
		}
		if pollEndpoints && chain != nil {
			// Whether Fibre exists on this chain at all, recorded rather than
			// inferred. x/fibre and x/valaddr are introduced in app version
			// 10, so below that every Fibre query fails for a reason that has
			// nothing to do with any validator — and a site that cannot tell
			// "the module is not there" from "the module is there and nobody
			// registered" will show the second while the first is true. Both
			// the version and the verdict are stored, so the page can say
			// which chain it is watching and what state that chain is in.
			if av, err := chain.AppVersion(ctx); err != nil {
				log.Printf("app version: %v", err)
			} else {
				_ = st.SetMeta("app_version", itoa(int64(av)), now)
				active, known := fibreActive(av, func() error {
					_, err := chain.FibreParamsAt(ctx, 0)
					return err
				})
				if known {
					_ = st.SetMeta("fibre_active", active, now)
				} else {
					log.Printf("fibre_active: app v%d but x/fibre did not answer; left as it was", av)
				}
				_ = st.SetMeta("fibre_app_version", itoa(scan.FibreAppVersion), now)
				// Until Fibre is live, the one Fibre-relevant fact the chain
				// carries is who has signalled for the version that brings
				// it. Read from x/signal on the same cadence as the rest;
				// the site shows the tally, the scheduled height and, per
				// validator, whether it has signalled. Dropped the moment
				// the chain is on that version.
				if av < scan.FibreAppVersion {
					if sig, err := chain.UpgradeSignal(ctx, scan.FibreAppVersion); err != nil {
						log.Printf("upgrade signal: %v", err)
					} else {
						if sig.Missing == nil {
							sig.Missing = []string{} // nobody missing is a list, not null
						}
						missing, _ := json.Marshal(sig.Missing)
						for k, v := range map[string]string{
							"signal_version":            fmt.Sprint(sig.Version),
							"signal_voting_power":       fmt.Sprint(sig.VotingPower),
							"signal_threshold_power":    fmt.Sprint(sig.ThresholdPower),
							"signal_total_voting_power": fmt.Sprint(sig.TotalVotingPower),
							"signal_upgrade_height":     fmt.Sprint(sig.UpgradeHeight),
							"signal_missing":            string(missing),
							"signal_polled_at":          store.TS(now),
						} {
							_ = st.SetMeta(k, v, now)
						}
					}
				}
			}
			chainID, height, tipTime, err := chain.StatusAt(ctx)
			if err != nil {
				log.Printf("endpoints: status: %v", err)
				live.Error(fmt.Sprintf("chain status: %v", err))
			} else if len(passErrs) == 0 {
				live.OK()
				live.Progress(height)
				// The chain's own identity and tip, recorded here rather than
				// only by the scanner: before Fibre activates there are no
				// publications to carry them, and "which chain is this, and how
				// far along is it" is the whole content of the site until then.
				// Kept separate from last_scanned_height, which is how far the
				// SCANNER has read; conflating the two would report the chain's
				// progress as our own.
				_ = st.SetMeta("chain_id", chainID, now)
				_ = st.SetMeta("chain_height", itoa(height), now)
				// The tip's own clock, so /v1/health can tell a chain that is
				// running from one that stopped. Everything else here is
				// drawn from the same node, and a node whose height stands
				// still looks identical to a network at rest.
				if !tipTime.IsZero() {
					_ = st.SetMeta("chain_tip_time", store.TS(tipTime), now)
					// And the anchors behind the chain's recent block time,
					// which the site needs to say when a scheduled upgrade
					// height is due (see store.NotePace).
					if err := st.NotePace(height, tipTime, now); err != nil {
						log.Printf("chain pace: %v", err)
					}
				}

				if provs, err := chain.BondedFibreProviders(ctx); err != nil {
					// Before v10 the module does not exist; that is a normal
					// state, logged but not fatal. fibre_active above says
					// which of the two this is.
					log.Printf("endpoints: %v", err)
				} else if evs, err := st.ObserveEndpointEvents(ctx, provs, height, now); err != nil {
					log.Printf("endpoints: store: %v", err)
				} else {
					if len(evs) > 0 {
						opened, closed := 0, 0
						for _, e := range evs {
							if e.Kind == store.EndpointOpened {
								opened++
							} else {
								closed++
							}
						}
						log.Printf("endpoints: h=%d registered=%d opened=%d closed=%d", height, len(provs), opened, closed)
						appendRegistry(evs)
					}
					_ = st.SetMeta("endpoints_height", itoa(height), now)
					_ = st.SetMeta("endpoints_registered", itoa(int64(len(provs))), now)
				}
			}
			// Validator names, from the chain's own staking module rather
			// than from an explorer's API. A reader recognises a validator by
			// the name its operator chose, not by twenty hex characters, and
			// taking that name from a third-party index would make this
			// observer depend on somebody else's coverage and terms.
			if ids, err := chain.ValidatorIdentities(ctx); err != nil {
				log.Printf("validator identities: %v", err)
			} else if n, err := st.UpsertValidatorIdentities(ids, now); err != nil {
				log.Printf("validator identities: store: %v", err)
			} else if n > 0 {
				log.Printf("validator identities: %d of %d stored", n, len(ids))
				_ = st.SetMeta("validator_identities", itoa(int64(n)), now)
			}
		}
		if pollEndpoints {
			hostingPass(ctx, now) // provider/country of each open endpoint; local files only
		}
		// A pass that failed to ingest says so on the status file, which is
		// what /v1/health reads. Named last so it is not overwritten by the
		// chain-status poll's OK.
		if len(passErrs) > 0 {
			live.Error("ingest: " + strings.Join(passErrs, "; "))
		}
		if err := st.Heartbeat(runID, time.Now()); err != nil {
			log.Printf("heartbeat: %v", err)
			live.Error(fmt.Sprintf("store heartbeat: %v", err))
		}
		if c, err := st.Count(ctx); err == nil {
			live.Set("publications", c.Publications)
			live.Set("probes", c.Probes)
		}
	}

	pass(true)
	if *once {
		c, _ := st.Count(ctx)
		log.Printf("done (--once): publications=%d assignments=%d probes=%d open_endpoints=%d", c.Publications, c.Assignments, c.Probes, c.OpenEndpoints)
		_ = st.StopRun(runID, time.Now(), "once")
		return
	}

	// Validator pictures: the identity field on chain is a Keybase key
	// suffix, and the picture behind it is fetched here, once a day per
	// identity, into the store, so the site can show it without a reader
	// ever contacting Keybase. Sequential and spaced, because Keybase is
	// somebody else's API and the whole set is a hundred lookups a day.
	kb := keybase.New()
	resolveAvatars := func(now time.Time) {
		due, err := st.AvatarsDue(ctx, now, *avMaxAge, 200)
		if err != nil {
			log.Printf("avatars: %v", err)
			return
		}
		got, none, failed := 0, 0, 0
		for i, id := range due {
			if ctx.Err() != nil {
				return
			}
			if i > 0 {
				time.Sleep(300 * time.Millisecond)
			}
			u, err := kb.Lookup(ctx, id)
			switch {
			case errors.Is(err, keybase.ErrNoPicture):
				none++
				_ = st.PutAvatar(id, "none", "", "", nil, time.Now())
				continue
			case err != nil:
				failed++
				_ = st.PutAvatar(id, "error", err.Error(), "", nil, time.Now())
				continue
			}
			ct, data, err := kb.Fetch(ctx, u)
			if err != nil {
				failed++
				_ = st.PutAvatar(id, "error", u+": "+err.Error(), "", nil, time.Now())
				continue
			}
			if err := st.PutAvatar(id, "ok", u, ct, data, time.Now()); err != nil {
				log.Printf("avatars: store %s: %v", id, err)
				continue
			}
			got++
		}
		if len(due) > 0 {
			log.Printf("avatars: %d identities checked: %d pictures, %d without one, %d failed", len(due), got, none, failed)
		}
	}
	if *avEvery > 0 {
		resolveAvatars(time.Now())
	}

	tick := time.NewTicker(*interval)
	defer tick.Stop()
	lastEP, lastAV := time.Now(), time.Now()
	for {
		select {
		case <-ctx.Done():
			log.Printf("stopped (signal)")
			_ = st.StopRun(runID, time.Now(), "signal")
			live.Stop("signal")
			return
		case <-tick.C:
			poll := *epEvery > 0 && time.Since(lastEP) >= *epEvery
			pass(poll)
			if poll {
				lastEP = time.Now()
			}
			if *avEvery > 0 && time.Since(lastAV) >= *avEvery {
				resolveAvatars(time.Now())
				lastAV = time.Now()
			}
		}
	}
}

// fibreActive is the fibre_active verdict: "yes" only once the chain is on
// FibreAppVersion AND x/fibre answers at the tip. The version alone said yes
// too early: the last pre-upgrade block sets app version 10, so a node on
// the old binary, halted at the upgrade height, reports v10 with no module
// behind it, and so does the new binary before its first block. known is
// false when the query failed for another reason (the node is down, busy),
// which says nothing either way.
func fibreActive(appVersion uint64, queryFibre func() error) (verdict string, known bool) {
	if appVersion < scan.FibreAppVersion {
		return "no", true
	}
	err := queryFibre()
	switch {
	case err == nil:
		return "yes", true
	case scan.IsModuleInactive(err):
		return "no", true
	}
	return "", false
}

func itoa(i int64) string {
	if i == 0 {
		return "0"
	}
	neg := i < 0
	if neg {
		i = -i
	}
	var b [20]byte
	n := len(b)
	for i > 0 {
		n--
		b[n] = byte('0' + i%10)
		i /= 10
	}
	if neg {
		n--
		b[n] = '-'
	}
	return string(b[n:])
}
