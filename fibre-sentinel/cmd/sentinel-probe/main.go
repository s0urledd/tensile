// Command sentinel-probe reads the blobs the scanner found. It reads
// <data-dir>/publications.jsonl (written by sentinel-scan) and reads each
// blob once, -end-read-offset before its must_serve_until, the way
// celestia-app's Fibre client downloads it: the validators the assignment
// gives rows, in the client's order, 15 s per request, rows verified
// against the commitment, until the rows reconstruct the blob; when they do
// not, once more a minute later. Every reading ends with one Measurement per
// validator asked, and on an Unavailable reading one per validator the
// deciding pass passed over (PASSED_OVER), appended together to
// <data-dir>/measurements.jsonl.
//
// The queue of readings is never persisted: it is re-derived from the
// publications and the existing measurements every cycle, so a restart
// resumes exactly.
//
// Every not-served row of a blob that could not be read (an endorsing
// validator whose rows did not come back, probe.ConfirmationDue) is also
// queued in <data-dir>/vantage-requests.jsonl for a second vantage to
// confirm, unless the correlated-failure guard sets the reading aside for
// good (probe.GuardSetsAsideForGood); it counts only once confirmed. With
// -confirm-requests the
// command is that second vantage instead: it answers those requests
// (internal/probe/confirm.go).
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
// verdict can be traced to the settings that produced it.
func flagConfig() map[string]any {
	m := map[string]any{}
	flag.VisitAll(func(f *flag.Flag) {
		m[f.Name] = f.Value.String()
	})
	return m
}

func main() {
	def := probe.DefaultScheduleConfig()
	var (
		rpc      = flag.String("rpc", "http://127.0.0.1:26657", "CometBFT RPC endpoint")
		dataDir  = flag.String("data-dir", "./sentinel-data", "dir holding publications.jsonl; measurements.jsonl is written here")
		pubsPath = flag.String("publications", "", "path to publications.jsonl (default <data-dir>/publications.jsonl)")
		regPath  = flag.String("registry", "", "path to the collector's registry.jsonl, the durable record of each validator's last registered host (default <data-dir>/registry.jsonl; optional)")
		vantage  = flag.String("vantage", "local", "name of this vantage point (recorded on every measurement)")

		once     = flag.Bool("once", false, "read every blob due now, then exit")
		drain    = flag.Bool("drain", false, "run until every known blob's reading is in the past, then exit")
		deadline = flag.Duration("deadline", 0, "whole-run wall-clock cap (0 = none)")
		readNow  = flag.Bool("read-now", false, "dry run: read every blob whose window is still open at once, whatever its scheduled time, then exit; use a scratch -data-dir, never the live one")

		endRead    = flag.Bool("end-read", true, "accepted for the unit files that pass it; every blob is read this way")
		endOffset  = flag.Duration("end-read-offset", def.EndReadOffset, "how long before must_serve_until a blob is read")
		endSince   = flag.String("end-read-since", "", "RFC 3339 time; publications settled before it were read on the schedule of their time and are not read again (empty = every publication)")
		readDL     = flag.Duration("read-deadline", def.ReadDeadline, "a reading that cannot start this long before must_serve_until is not made (NOT_PROBED)")
		retryAfter = flag.Duration("retry-after", def.RetryAfter, "when the rows are short after every validator was asked, the second pass starts this much later")
		pruneTol   = flag.Duration("prune-tolerance", def.PruneTolerance, "NOT_FOUND is normal until must_serve_until + this (devnet prune lag ~1m45s)")

		maxSleep    = flag.Duration("max-sleep", 30*time.Second, "longest sleep between cycles")
		rpcTO       = flag.Duration("rpc-timeout", 15*time.Second, "per-RPC-call timeout")
		concurrency = flag.Int("concurrency", 64, "requests in flight at once, across every reading")
		blobs       = flag.Int("blob-concurrency", 16, "blobs being read at once")
		perVal      = flag.Int("per-validator", 1, "requests one validator has from this observer at a time")
		inFlightMiB = flag.Int64("in-flight-mib", 512, "shard bytes in flight at once, MiB; a count of requests does not bound memory when one shard can be hundreds of MiB")
		localHosts  = flag.Bool("allow-unroutable-hosts", false,
			"dial registered hosts on loopback or a private range (a local devnet; never a public vantage)")
		backfill = flag.Duration("backfill-missed", 0, "on (re)start, write NOT_PROBED rows only for readings newer than this that were not made; 0 (default) writes them for every one still on record")
		dnsTO    = flag.Duration("dns-timeout", 5*time.Second, "bound on the DNS lookup, inside the request's time (a lookup that times out is this observer's resolver)")
		dlTO     = flag.Duration("download-timeout", probe.ClientRPCTimeout, "one request's whole time, connect, TLS and DownloadShard: the Fibre client's RPCTimeout")
		logLines = flag.Int("log-ring", 400, "log lines kept in memory for the crash dump")

		policyPath  = flag.String("policy", "", "policy YAML (observer/policy): only its sampling master secret is read, to reveal the day secrets of earlier draws; \"default\" puts the secret at <data-dir>/sampling-master.key; empty = no reveals")
		revealAfter = flag.Duration("reveal-after", policy.DefaultRevealAfter, "publish each day's sampling secret this long after the day ends, to <data-dir>/sampling-secrets.jsonl (0 = never)")

		// Confirm mode, on a second vantage: no publications and no
		// schedule. It answers the primary's confirmation requests (one
		// request per not-served row) and writes <data-dir>/measurements.jsonl.
		confirmReqs  = flag.String("confirm-requests", "", "confirm mode: answer the confirmation requests in this file (the primary's vantage-requests.jsonl, copied in) instead of reading blobs")
		confirmMax   = flag.Int("confirm-max-per-hour", 60, "confirm mode: confirming requests to any one validator in an hour at most; requests past it wait, and lapse at their deadline")
		confirmWork  = flag.Int("confirm-workers", 8, "confirm mode: validators asked at once; one validator is asked one request at a time")
		confirmEvery = flag.Duration("confirm-poll", 20*time.Second, "confirm mode: how often the requests file is read again")
	)
	flag.Parse()

	if *pubsPath == "" {
		*pubsPath = filepath.Join(*dataDir, "publications.jsonl")
	}

	log := scan.NewLogger(*logLines)
	// The connect and the TLS handshake are bounded by the request's time
	// alone, as the client's are (probe.Input.ClientRules).
	timeouts := probe.StepTimeouts{DNS: *dnsTO, Download: *dlTO, MinDownloadBytesPerSec: -1}

	if *confirmReqs != "" {
		chain, err := scan.NewChain(*rpc, *rpcTO, log)
		if err != nil {
			log.Fatalf("rpc client: %v", err)
		}
		c, err := probe.NewConfirmer(probe.ConfirmConfig{
			RequestsPath: *confirmReqs, DataDir: *dataDir, Vantage: *vantage,
			Timeouts:  timeouts,
			PollEvery: *confirmEvery, MaxPerHour: *confirmMax, Workers: *confirmWork, AllowUnroutableHosts: *localHosts,
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

	if !*endRead {
		log.Printf("-end-read=false is ignored: every blob is read once, at the end of its window")
	}
	if *endOffset <= 0 || *readDL <= 0 || *retryAfter <= 0 {
		log.Fatalf("-end-read-offset, -read-deadline and -retry-after must be positive")
	}
	sched := probe.ScheduleConfig{PruneTolerance: *pruneTol, EndReadOffset: *endOffset, ReadDeadline: *readDL, RetryAfter: *retryAfter}
	if *endSince != "" {
		t, err := time.Parse(time.RFC3339, *endSince)
		if err != nil {
			log.Fatalf("-end-read-since: %v", err)
		}
		sched.Since = t
	}

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
		// The secret lives in the data directory unless the policy names
		// somewhere else: the only path the unit can write.
		if cfg.Sampling.MasterSecretFile == "" && *dataDir != "" {
			cfg.Sampling.MasterSecretFile = filepath.Join(*dataDir, "sampling-master.key")
		}
		p, err := policy.New(cfg)
		if err != nil {
			log.Fatalf("policy: %v", err)
		}
		revealer = p
	}

	pr, err := probe.New(probe.Config{
		RPCURL:               *rpc,
		PublicationsPath:     *pubsPath,
		RegistryPath:         *regPath,
		DataDir:              *dataDir,
		Vantage:              *vantage,
		Schedule:             sched,
		Timeouts:             timeouts,
		Once:                 *once,
		Drain:                *drain,
		ReadNow:              *readNow,
		Deadline:             *deadline,
		MaxSleep:             *maxSleep,
		RPCTimeout:           *rpcTO,
		Concurrency:          *concurrency,
		BlobConcurrency:      *blobs,
		PerValidator:         *perVal,
		InFlightBytes:        *inFlightMiB << 20,
		AllowUnroutableHosts: *localHosts,
		BackfillMissed:       *backfill,
		RunConfig:            flagConfig(),
	}, log)
	if err != nil {
		log.Fatalf("init: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if revealer != nil && !*readNow {
		go revealer.RevealLoop(ctx, filepath.Join(*dataDir, policy.SecretsFile), *revealAfter, log.Printf)
	}
	if err := pr.Run(ctx); err != nil {
		log.Fatalf("run: %v", err)
	}
}
