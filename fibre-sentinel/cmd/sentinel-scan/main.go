// Command sentinel-scan is the Fibre Sentinel chain scanner: discovery and
// recording only.
//
// It walks the chain height by height, finds transactions whose sole message is
// MsgPayForFibre, decodes the full PaymentPromise, tracks the fibre module
// params from EventUpdateFibreParams, and writes one JSON record per publication
// to <data-dir>/publications.jsonl. It never probes a validator.
//
// Every RPC call is timeout-bounded. Follow mode gives up (non-zero exit, with a
// dump of the last log lines) if the chain stops producing blocks — it never
// hangs. It reads a new block as soon as the node announces its header on the
// RPC websocket (-subscribe), with the tip poll behind it as the safety net,
// every -subscribed-poll, and every -poll while the subscription is down.
package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/scan"
	"github.com/plsgiveup/fibre/fibre-sentinel/internal/status"
)

func main() {
	var (
		rpc         = flag.String("rpc", "http://127.0.0.1:26657", "CometBFT RPC endpoint")
		dataDir     = flag.String("data-dir", "./sentinel-data", "directory for state.json + publications.jsonl")
		startHeight = flag.Int64("start-height", 0, "fresh-scan start height (0 = tip at startup); ignored on resume")
		maxHeight   = flag.Int64("max-height", 0, "stop after this height (0 = run to tip)")
		follow      = flag.Bool("follow", false, "keep scanning new blocks after reaching the tip")
		followTO    = flag.Duration("follow-timeout", 0, "in follow mode, fail if no new block within this (0 = never; a halted chain is warned about every 5 minutes)")
		pollEvery   = flag.Duration("poll", 2*time.Second, "follow-mode tip poll interval; with -subscribe, while the block subscription is down")
		subscribe   = flag.Bool("subscribe", true, "in follow mode, subscribe to the node's new block headers on its RPC websocket (/websocket, tm.event='NewBlockHeader') and read each new block as soon as it is announced; the tip poll keeps running behind it (false = poll only)")
		subPoll     = flag.Duration("subscribed-poll", 5*time.Second, "follow-mode tip poll interval while the block subscription is up: the safety net for an announcement the node did not deliver (never shorter than -poll)")
		rpcTO       = flag.Duration("rpc-timeout", 15*time.Second, "per-RPC-call timeout")
		deadline    = flag.Duration("deadline", 0, "whole-run wall-clock cap (0 = none)")
		includeFail = flag.Bool("include-failed", false, "also record MsgPayForFibre txs that failed on chain")
		storeRows   = flag.Bool("rows", true, "include full per-validator row-index lists in each record")
		checkpoint  = flag.Int("checkpoint-every", 20, "fsync + persist cursor every N heights")
		logLines    = flag.Int("log-ring", 300, "log lines kept in memory for the crash dump")
		skipHeights = flag.String("skip-heights", "", "heights not to read, recorded and published as scan gaps: comma-separated heights and ranges a-b (e.g. 1234,2000-2005); the way out of a crash loop on one block, see deploy/README.md")
	)
	flag.Parse()

	log := scan.NewLogger(*logLines)
	if !*storeRows {
		log.Printf("WARNING: -rows=false omits the per-validator row lists, so the observer cannot compute which distinct rows were served; the dashboard's reconstructability verdict will read \"unknown\" for every blob recorded in this run")
	}

	// -skip-heights is the operator's way past a block the scanner exits on
	// (every such exit names it). A value that does not parse is fatal
	// rather than read as "no skips" or as part of a list: skipping the
	// wrong height, or silently none, is the one thing it must never do.
	// An empty value is no skips, which is what the systemd unit passes
	// when SKIP_HEIGHTS is unset. The raw value lands in runs.jsonl with
	// the rest of the run config, so every skip has an audit trail beside
	// the gap it produced.
	skips, err := scan.ParseHeightRanges(*skipHeights)
	if err != nil {
		log.Fatalf("-skip-heights: %v", err)
	}

	// The scanner completes a cycle per block, seconds apart, and so states
	// a minute as its cadence; /v1/health fails it once its last completed
	// block is a few of these old while the chain has newer ones.
	status.SetDefault(status.CadenceKey, 60)

	runCfg := map[string]any{}
	flag.VisitAll(func(f *flag.Flag) { runCfg[f.Name] = f.Value.String() })
	s, err := scan.New(scan.Config{
		RunConfig:       runCfg,
		RPCURL:          *rpc,
		DataDir:         *dataDir,
		StartHeight:     *startHeight,
		MaxHeight:       *maxHeight,
		Follow:          *follow,
		FollowTimeout:   *followTO,
		PollInterval:    *pollEvery,
		Subscribe:       *subscribe,
		SubscribedPoll:  *subPoll,
		RPCTimeout:      *rpcTO,
		Deadline:        *deadline,
		IncludeFailed:   *includeFail,
		StoreRows:       *storeRows,
		CheckpointEvery: *checkpoint,
		SkipHeights:     skips,
	}, log)
	if err != nil {
		log.Fatalf("init: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := s.Run(ctx); err != nil {
		log.Fatalf("run: %v", err)
	}
}
