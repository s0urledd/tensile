// Command sentinel-probe turns the scanner's publications into scheduled probes
// and raw measurements. It reads <data-dir>/publications.jsonl (written by
// sentinel-scan), derives a per-publication probe schedule from each record's
// must_serve_until, probes the assigned validators at those times, and appends
// one raw Measurement per probe to <data-dir>/measurements.jsonl.
//
// The pending-probe queue is never persisted: it is re-derived from the
// publications and the existing measurements every cycle, so a restart resumes
// exactly. Every wait is bounded — the loop never sleeps more than -max-sleep,
// and every probe layer (DNS/TCP/TLS/identity/download) has its own timeout.
//
// Every FAULT is also queued in <data-dir>/vantage-requests.jsonl for a second
// vantage to confirm. With -confirm-requests the command is that second
// vantage instead: it answers those requests (internal/probe/confirm.go).
package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/probe"
	"github.com/plsgiveup/fibre/fibre-sentinel/internal/scan"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/policy"
)

// flagConfig is every flag as set for this run, by name, with durations as
// their string form. It is recorded in runs.jsonl (status.RunEvent) so a
// verdict can be traced to the settings that produced it: the prune
// tolerance behind a phase, the schedule points, the timeouts, the policy.
func flagConfig() map[string]any {
	m := map[string]any{}
	flag.VisitAll(func(f *flag.Flag) {
		m[f.Name] = f.Value.String()
	})
	return m
}

func main() {
	var (
		rpc      = flag.String("rpc", "http://127.0.0.1:26657", "CometBFT RPC endpoint")
		dataDir  = flag.String("data-dir", "./sentinel-data", "dir holding publications.jsonl; measurements.jsonl is written here")
		pubsPath = flag.String("publications", "", "path to publications.jsonl (default <data-dir>/publications.jsonl)")
		regPath  = flag.String("registry", "", "path to the collector's registry.jsonl, the durable record of each validator's last registered host (default <data-dir>/registry.jsonl; optional)")
		vantage  = flag.String("vantage", "local", "name of this vantage point (recorded on every measurement)")

		once     = flag.Bool("once", false, "probe everything currently due, then exit")
		drain    = flag.Bool("drain", false, "run until every known publication's schedule is in the past, then exit")
		deadline = flag.Duration("deadline", 0, "whole-run wall-clock cap (0 = none)")

		inWindow = flag.Int("in-window-probes", 4, "number of probes inside [settlement, must_serve_until]")
		endRead  = flag.Bool("end-read", false, "read every endorsed shard once, at the end of its retention window (-end-read-offset before must_serve_until, late up to the deadline if it must), and nothing before or after; "+
			"only the validators whose signature the settled promise carries; DownloadShard held to a flat -download-timeout of 15s, the RPCTimeout celestia-app's own client reads with, unless that flag is given")
		endOffset = flag.Duration("end-read-offset", 10*time.Minute, "with -end-read: how long before must_serve_until the reading is scheduled")
		// The sampling audit rests on a master secret that outlives the
		// process: rows carry a commitment to the day's secret and the
		// secret is revealed later. Without a file the secret is new on
		// every start, so the commitments already published can never be
		// checked. Refused unless this says otherwise.
		ephemeralSampling = flag.Bool("allow-ephemeral-sampling", false,
			"permit a process-local sampling master secret (tests only: day commitments will not survive a restart)")
		graceOff = flag.Duration("grace-offset", 30*time.Second, "grace probe: must_serve_until + this")
		pruneTol = flag.Duration("prune-tolerance", 150*time.Second, "NOT_FOUND is normal until must_serve_until + this (devnet prune lag ~1m45s)")
		postMrg  = flag.Duration("post-margin", 60*time.Second, "post probe: must_serve_until + prune-tolerance + this")

		unassigned  = flag.Bool("probe-unassigned", false, "also probe validators not assigned the shard (their NOT_FOUND is the baseline)")
		maxSleep    = flag.Duration("max-sleep", 30*time.Second, "longest sleep between cycles")
		maxLateness = flag.Duration("max-lateness", 90*time.Second, "a schedule point older than this is recorded MISSED instead of probed (raised to -max-lateness-fraction of the blob's window when that is longer)")
		lateFrac    = flag.Float64("max-lateness-fraction", 0.05, "lateness allowance as a share of each blob's own retention window, when larger than -max-lateness (0.05 is 12 min on a 4 h window); negative disables")
		rpcTO       = flag.Duration("rpc-timeout", 15*time.Second, "per-RPC-call timeout")
		concurrency = flag.Int("concurrency", 16, "probes in flight across all validators (never more than one per validator)")
		inFlightMiB = flag.Int64("in-flight-mib", 512, "shard bytes in flight at once, MiB; a count of probes does not bound memory when one shard can be hundreds of MiB")
		localHosts  = flag.Bool("allow-unroutable-hosts", false,
			"dial registered hosts on loopback or a private range (a local devnet; never a public vantage)")
		backfill    = flag.Duration("backfill-missed", 0, "on (re)start, write NOT_PROBED markers only for slots newer than this; 0 (default) writes one for every elapsed slot of every publication still on record, so an obligation the prober never reached is counted as unobserved rather than missing from the total")
		retryTO     = flag.Bool("retry-transport-timeout", true, "retry a probe once when the first attempt fails with a transport timeout (slot blocking during uploads), or with a download timeout in the last quarter of the window")
		retryDelay  = flag.Duration("retry-delay", 20*time.Second, "wait before the transport-timeout retry")
		dnsTO       = flag.Duration("dns-timeout", 5*time.Second, "")
		tcpTO       = flag.Duration("tcp-timeout", 5*time.Second, "")
		tlsTO       = flag.Duration("tls-timeout", 10*time.Second, "")
		dlTO        = flag.Duration("download-timeout", 25*time.Second, "")
		logLines    = flag.Int("log-ring", 400, "log lines kept in memory for the crash dump")
		policyPath  = flag.String("policy", "", "probe load policy YAML (observer/policy); \"default\" applies the built-in defaults; empty = no policy (probe everything)")
		revealAfter = flag.Duration("reveal-after", policy.DefaultRevealAfter, "publish each day's sampling secret this long after the day ends, to <data-dir>/sampling-secrets.jsonl (0 = never)")

		// Confirm mode, on a second vantage: no publications, no schedule,
		// no policy. It answers the primary's confirmation requests (one
		// probe per FAULT) and writes <data-dir>/measurements.jsonl.
		confirmReqs  = flag.String("confirm-requests", "", "confirm mode: answer the confirmation requests in this file (the primary's vantage-requests.jsonl, copied in) instead of probing publications")
		confirmMax   = flag.Int("confirm-max-per-hour", 60, "confirm mode: confirming probes in any hour at most; requests past it wait, and lapse at their deadline")
		confirmEvery = flag.Duration("confirm-poll", 20*time.Second, "confirm mode: how often the requests file is read again")
	)
	flag.Parse()

	if *pubsPath == "" {
		*pubsPath = filepath.Join(*dataDir, "publications.jsonl")
	}

	log := scan.NewLogger(*logLines)

	if *confirmReqs != "" {
		chain, err := scan.NewChain(*rpc, *rpcTO, log)
		if err != nil {
			log.Fatalf("rpc client: %v", err)
		}
		c, err := probe.NewConfirmer(probe.ConfirmConfig{
			RequestsPath: *confirmReqs, DataDir: *dataDir, Vantage: *vantage,
			Timeouts:  probe.StepTimeouts{DNS: *dnsTO, TCP: *tcpTO, TLS: *tlsTO, Download: *dlTO},
			PollEvery: *confirmEvery, MaxPerHour: *confirmMax, AllowUnroutableHosts: *localHosts,
			Once: *once, RunConfig: flagConfig(),
		}, chain, log)
		if err != nil {
			log.Fatalf("init: %v", err)
		}
		defer c.Close()
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		log.Printf("confirm mode: vantage=%s requests=%s data=%s rpc=%s", *vantage, *confirmReqs, *dataDir, *rpc)
		if err := c.Run(ctx); err != nil {
			log.Fatalf("run: %v", err)
		}
		return
	}

	fracs := probe.InWindowFractions(*inWindow)
	sched := probe.ScheduleConfig{InWindowFractions: fracs, GraceOffset: *graceOff, PruneTolerance: *pruneTol, PostMargin: *postMrg}
	timeouts := probe.StepTimeouts{DNS: *dnsTO, TCP: *tcpTO, TLS: *tlsTO, Download: *dlTO}
	points := float64(len(fracs) + 2) // in-window points, the grace point and the post point
	if *endRead {
		if *endOffset <= 0 {
			log.Fatalf("-end-read-offset must be positive")
		}
		sched.EndReadOffset = *endOffset
		points = 1 // one reading per shard
		dlSet := false
		flag.Visit(func(f *flag.Flag) { dlSet = dlSet || f.Name == "download-timeout" })
		if !dlSet {
			timeouts.Download = endReadDownloadTimeout
		}
		timeouts.MinDownloadBytesPerSec = -1 // flat, as the chain's client
	}

	var pol probe.Policy
	var revealer *policy.Policy
	if *policyPath != "" {
		path := *policyPath
		if path == "default" {
			path = ""
		}
		cfg, err := policy.Load(path)
		if err != nil {
			log.Fatalf("policy: %v", err)
		}
		// one request per point per validator per publication
		cfg.PointsPerPublication = points
		// The byte side of the projection: every point, the post one too.
		// NOT_FOUND is what the post point expects, but a validator that
		// keeps the blob past the tolerance serves it there in full, and
		// the projection is a ceiling, so it must count that transfer.
		cfg.DownloadsPerPublication = points
		// The master secret lives in the data directory unless the policy
		// names somewhere else. It is the only path the unit can write
		// (fibre-probe@.service: ProtectSystem=strict with
		// ReadWritePaths=<data dir>), and one per vantage rather than one
		// shared by every network on the host.
		if cfg.Sampling.MasterSecretFile == "" && *dataDir != "" {
			cfg.Sampling.MasterSecretFile = filepath.Join(*dataDir, "sampling-master.key")
		}
		cfg.Sampling.AllowEphemeralSecret = *ephemeralSampling
		// The budget and backoff state beside it, so a restart (or a restart
		// loop) does not start every validator on a fresh budget. Same
		// directory for the same reason: the only one the unit can write.
		if *dataDir != "" {
			cfg.State.File = filepath.Join(*dataDir, policy.BudgetStateFile)
		}
		p, err := policy.New(cfg)
		if err != nil {
			log.Fatalf("policy: %v", err)
		}
		if p.EphemeralSecret() {
			log.Printf("WARNING: the sampling master secret is process-local: the day commitments stamped on every row " +
				"and served by /v1/sampling cannot be verified after this process restarts. For anything but a test, " +
				"point sampling.master_secret_file at a file the process can keep (deploy/README.md).")
		}
		p.SetLogger(log.Printf)
		pol = p
		revealer = p
	}

	pr, err := probe.New(probe.Config{
		Policy:              pol,
		RPCURL:              *rpc,
		PublicationsPath:    *pubsPath,
		RegistryPath:        *regPath,
		DataDir:             *dataDir,
		Vantage:             *vantage,
		Schedule:            sched,
		Timeouts:            timeouts,
		IncludeUnassigned:   *unassigned,
		EndorsedOnly:        *endRead,
		Once:                *once,
		Drain:               *drain,
		Deadline:            *deadline,
		MaxSleep:            *maxSleep,
		MaxLateness:         *maxLateness,
		MaxLatenessFraction: *lateFrac,
		RPCTimeout:          *rpcTO,

		RetryTransportTimeout: *retryTO,
		RetryDelay:            *retryDelay,
		Concurrency:           *concurrency,
		InFlightBytes:         *inFlightMiB << 20,
		AllowUnroutableHosts:  *localHosts,
		BackfillMissed:        *backfill,
		RunConfig:             flagConfig(),
	}, log)
	if err != nil {
		log.Fatalf("init: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if revealer != nil {
		go revealer.RevealLoop(ctx, filepath.Join(*dataDir, policy.SecretsFile), *revealAfter, log.Printf)
	}
	err = pr.Run(ctx)
	// What was spent since the last timed save goes to disk before the
	// process ends, clean exit or not.
	if revealer != nil {
		if ferr := revealer.Flush(); ferr != nil {
			log.Printf("WARNING: probe budget state not saved on exit: %v", ferr)
		}
	}
	if err != nil {
		log.Fatalf("run: %v", err)
	}
}

// endReadDownloadTimeout is the per-call deadline celestia-app's Fibre client
// gives DownloadShard (fibre.ClientConfig RPCTimeout, default 15s): under
// -end-read a validator is read the way the chain's own client reads it.
const endReadDownloadTimeout = 15 * time.Second
