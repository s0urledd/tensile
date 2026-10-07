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
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/collect"
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
		fastEvery = flag.Duration("fast-every", time.Second, "how often, between those passes, to read only state.json, publications.jsonl and payments.jsonl, so a new blob is served within about this long of the scanner writing it; nothing is opened while they have not changed (0 = only in the full pass)")
		fastWatch = flag.Bool("fast-watch", true, "also run that read as soon as one of those files changes, from the kernel's file events on their directory (a burst of writes is one read, at most 100 ms after its first); the -fast-every timer stays as the fallback")
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
		// -retain-raw and -retain-raw-json went with the prune and the raw_json strip they set (2026-10-04): every row is
		// kept for good, and a unit that still passes either fails at start rather than believe it prunes.
		rollAfter = flag.Duration("rollup-after", rollup.Default().RollupAfter, "compute a day's obligation and probe rollups this long after the day ends; must clear every retention window (0 = never roll up)")
		retEvery  = flag.Duration("retention-every", time.Hour, "how often the rollup pass runs")
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
	// The bound /v1/health holds the status file's OK to: it is set by a
	// clean poll pass, one every -endpoints-every. Without a chain poll
	// nothing sets it, and no bound is published.
	if *epEvery > 0 {
		live.Set("cadence_s", int(epEvery.Round(time.Second)/time.Second))
	}
	// The work that can fail apart from the ingest, which the OK above
	// must not hide (observer/collect/work.go).
	work := collect.NewWorkErrors(live)

	// The endpoint history has no source but the live polls, so every
	// opening and closing is appended here as well as written to the
	// database; on a rebuild the file is replayed before the first poll.
	regFile, err := os.OpenFile(*regPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		log.Fatalf("open %s: %v", *regPath, err)
	}
	defer regFile.Close()
	appendRegistry := func(evs []store.EndpointEvent) {
		if len(evs) == 0 {
			return
		}
		var failed error
		for _, e := range evs {
			b, err := json.Marshal(e)
			if err != nil {
				continue
			}
			if _, err := regFile.Write(append(b, '\n')); err != nil {
				log.Printf("registry: write: %v", err)
				live.Error(fmt.Sprintf("registry write: %v", err))
				failed = fmt.Errorf("write: %w", err)
				break
			}
		}
		if failed == nil {
			if err := regFile.Sync(); err != nil {
				log.Printf("registry: sync: %v", err)
				failed = fmt.Errorf("sync: %w", err)
			}
		}
		work.Report("registry", failed, time.Now())
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
	corrFile, err := os.OpenFile(*corrPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		log.Fatalf("open %s: %v", *corrPath, err)
	}
	defer corrFile.Close()
	corr := correct.New(st, corrFile, *pruneTol)
	hostingPass := newHostingPass(st, *dataDir, *vantage, log.Printf) // hosting.go

	log.Printf("collector up: run=%d vantage=%s db=%s data=%s exports=%s", runID, *vantage, *dbPath, *dataDir, *expDir)

	retention := rollup.Config{RollupAfter: *rollAfter, Vantage: *vantage}
	// The record-keeping half of a pass: every file into the store, then the
	// collapse, the late verdicts, the corrections, the holds and the
	// retention pass (observer/collect).
	coll := collect.New(collect.Collector{
		St: st,
		Paths: collect.Paths{
			State: *statePath, Publications: *pubsPath, Measurements: *measPath, SampledOut: *soPath,
			Reachability: *reachPath, VantagesDir: *vantDir, Registry: *regPath, Payments: *payPath,
			Runs: *runsPath, SamplingSecrets: *secPath, HostHistory: *hostsPath, Amendments: *amendPath,
			ParamUncertainty: *uncPath, Corrections: *corrPath,
		},
		Vantage: *vantage, Logf: log.Printf, Live: live, AmendFile: amendFile, PruneTolerance: *pruneTol,
		Corrector: corr, Retention: retention, RetentionEvery: *retEvery, Work: work,
	})
	var poll *chainPoll
	if chain != nil {
		poll = &chainPoll{chain: chain, st: st, live: live, logf: log.Printf, appendRegistry: appendRegistry} // chainpoll.go
	}
	var lastEscrow time.Time
	var escrowAt escrowRound // where the next escrow poll goes on from (withdrawals.go)
	var exportHeld string    // the hold last logged, so it is said once
	pass := func(pollEndpoints bool) {
		now := time.Now()
		passErrs := coll.Pass(ctx, now)
		if exporter != nil {
			built, held, err := exportStep(exporter.Run, coll.VantagesFromOtherChain(), now)
			switch {
			case held:
				if err.Error() != exportHeld {
					log.Printf("export: %v", err)
					exportHeld = err.Error()
				}
			case err != nil:
				log.Printf("export: %v", err)
				live.Error(fmt.Sprintf("export: %v", err))
			case len(built) > 0:
				live.Set("last_export", built[len(built)-1])
			}
			if !held && exportHeld != "" {
				log.Printf("export: no longer held")
				exportHeld = ""
			}
			work.Report("export", err, now)
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
			// read from one committed height (withdrawals.go), then the
			// block time of every params change not yet dated; each within
			// escrowBudget, since this runs in the loop the ingest runs in.
			// A poll the budget cuts short leaves the rest of its round to
			// the next one.
			p := pollEscrow(ctx, chain, st, &escrowAt, now, log.Printf)
			fillParamTimes(ctx, chain, st, log.Printf)
			// The total, from the module account every escrow lives in: exact
			// where the sum above is a floor, since it covers only accounts
			// this observer has seen publish.
			bal, err := chain.ModuleBalance(ctx, scan.FibreModuleName, "utia")
			if err != nil {
				log.Printf("escrow: module balance: %v", err)
			} else {
				_ = st.SetMeta("escrow_module_utia", itoa(bal), now)
				_ = st.SetMeta("escrow_module_polled_at", store.TS(now), now)
			}
			if p.OK && err == nil {
				// The poll the finished round began in: every balance
				// on record was read at or after it.
				_ = st.SetMeta("escrow_polled_at", store.TS(p.Since), now)
			}
		}
		if pollEndpoints && poll != nil {
			poll.run(ctx, now, len(passErrs) == 0, coll.RegistryReplayed())
		}
		if pollEndpoints {
			// provider/country of each open endpoint; local files only
			work.Report("hosting", hostingPass(ctx, now), now)
		}
		// A pass that failed to ingest says so on the status file, which is
		// what /v1/health reads. Named last so it is not overwritten by the
		// chain-status poll's OK.
		if len(passErrs) > 0 {
			live.Error("ingest: " + strings.Join(passErrs, "; "))
		}
		err := st.Heartbeat(runID, time.Now())
		if err != nil {
			log.Printf("heartbeat: %v", err)
			live.Error(fmt.Sprintf("store heartbeat: %v", err))
		}
		work.Report("heartbeat", err, now)
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

	// Validator pictures, from Keybase, on a goroutine of their own
	// (avatars.go): nothing here waits on Keybase. Stopped, and waited
	// for, before the store is closed.
	if *avEvery > 0 {
		avCtx, stopAv := context.WithCancel(ctx)
		avDone := make(chan struct{})
		av := newAvatarResolver(st, keybase.New(), *avEvery, *avMaxAge, log.Printf, live, work)
		go av.loop(avCtx, *avEvery, avDone)
		defer func() {
			stopAv()
			<-avDone
		}()
	}

	tick := time.NewTicker(*interval)
	defer tick.Stop()
	// The fast tick (fast.go) runs in this same loop, so it never writes
	// beside a pass: one that falls due while a pass runs waits for it, and
	// then reads only what came in after the pass read the files. A nil
	// channel is never ready, so -fast-every 0 leaves the pass alone, as
	// before. The file watch (watch.go) only says when a tick is due sooner
	// than its timer: the tick still runs here, and the timer keeps running
	// beside it, so a watch that cannot start, or is lost, costs what the
	// timer costs.
	var fastC <-chan time.Time
	var wakeC <-chan struct{}
	fast := newFastTick(st, *statePath, *pubsPath, *payPath, log.Printf, live.Error)
	if *fastEvery > 0 {
		ft := time.NewTicker(*fastEvery)
		defer ft.Stop()
		fastC = ft.C
		watching := ""
		if *fastWatch {
			fw, err := watchFiles([]string{*statePath, *pubsPath, *payPath}, watchQuiet, watchMaxWait, log.Printf)
			if err != nil {
				log.Printf("WARNING: fast tick: cannot watch the files (%v): reading them on the timer alone", err)
			} else {
				defer fw.Close()
				wakeC = fw.C
				watching = fmt.Sprintf(", and within %s of a change to one of them (watching %s)", watchMaxWait, strings.Join(fw.Dirs(), ", "))
			}
		}
		log.Printf("fast tick: state.json, publications and payments every %s between passes%s", *fastEvery, watching)
	}
	lastEP := time.Now()
	for {
		select {
		case <-ctx.Done():
			log.Printf("stopped (signal)")
			_ = st.StopRun(runID, time.Now(), "signal")
			live.Stop("signal")
			return
		case <-fastC:
			fast.run(time.Now())
		case <-wakeC:
			fast.run(time.Now())
		case <-tick.C:
			due := *epEvery > 0 && time.Since(lastEP) >= *epEvery
			pass(due)
			if due {
				lastEP = time.Now()
			}
		}
	}
}

// exportStep runs the daily export build (run), unless the pass just stopped
// a vantage file at a row from another chain (otherChain, by vantage name):
// then held is true and err says why, for the log and the work list.
//
// The ingest keeps such a file out of the store, but an export copies the
// vantage files whole, and an export once built and signed is never taken
// back. A day held is not lost: the builder builds every day from the last
// one it built, so the exports catch up once the file is gone. A row of this
// chain that the guard refused for a moment (a validator that registered
// since the last chain poll) holds the build for a pass or two at most.
func exportStep(run func(time.Time) ([]string, error), otherChain []string, now time.Time) (built []string, held bool, err error) {
	if len(otherChain) > 0 {
		return nil, true, fmt.Errorf("held: the heartbeat file of vantage %s holds rows from another chain, which an export would copy whole; built again once it is removed", strings.Join(otherChain, ", "))
	}
	built, err = run(now)
	return built, false, err
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
