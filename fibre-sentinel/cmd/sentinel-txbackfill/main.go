// Command sentinel-txbackfill fills failed_txs.jsonl and tx_costs.jsonl for
// the heights before the scanner began writing them: from Fibre's
// activation up to the height before each file's first line. It reads each
// block's results, and the block itself when the results say a line can
// come from it (scan.ResultsNeedBlock), through the scanner's own RPC client
// (scan.Chain), and builds the lines with the scanner's own functions
// (scan.FailedTxRecords, scan.TxCostRecords), so a staged line is the line
// the scanner would have written for that block, but for recorded_at, which
// is the time of the backfill.
//
// It runs in two steps.
//
// Staging (the default) writes the lines into -out-dir, never into the data
// directory: failed_txs.jsonl and tx_costs.jsonl there, each sorted by
// height and tx index, each key once, leaving out every key the live files
// in -data-dir already hold. A height at or above the local node's oldest
// block (-local-rpc, read once at the start) is asked of that node; every
// other one of the archive nodes in -rpc, in turn. Each node is asked at
// most -concurrency things at once; a failed ask (an error, a 429, a 5xx)
// is asked again of the next node, after a backoff. The lines of a height
// are fsynced as they are written, and progress.json says how far the
// staging got and how many bytes of each file that is: a run that stopped
// (a signal, or a height no node would serve) is run again with the same
// flags and goes on from there, and a run over a finished staging changes
// nothing.
//
// Merging (-merge) appends the staged lines to the live files in -data-dir,
// with the scanner stopped: it refuses while a sentinel-scan process of that
// data directory runs. Every staged line must decode to a usable record of
// its range, in order, before anything is appended; a key the live file
// already holds is left out, so the merge too can be run again. The lines
// are appended through the scanner's own store (scan.Store), under its
// rules: a torn final line cut first, every line written whole.
//
// Nothing else changes for them. Dated by their blocks, the merged lines go
// into the next daily export as late lines (observer/export), and every
// export already built stays as it is; the collector ingests them as it does
// any line; the backup copies the files. deploy/README.md, "Backfilling
// failed transactions and costs", has the commands.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

func main() {
	var (
		rpcs     = flag.String("rpc", "", "archive RPC endpoints, comma-separated (http:// or https://), for the heights below the local node's oldest block; asked in turn")
		localRPC = flag.String("local-rpc", "http://127.0.0.1:26657", "the local node, asked for every height from its oldest block (/status earliest_block_height, read at the start) to its newest; empty: the archives only")
		from     = flag.Int64("from", 0, "first height to stage: Fibre's activation")
		failedTo = flag.Int64("failed-to", 0, "last height whose failed transactions are staged: the one before failed_txs.jsonl's first line (0: none)")
		costsTo  = flag.Int64("costs-to", 0, "last height whose transaction costs are staged: the one before tx_costs.jsonl's first line (0: none)")
		outDir   = flag.String("out-dir", "", "staging directory: the staged files and progress.json")
		dataDir  = flag.String("data-dir", "", "the observer's data directory: only read when staging (its chain id and the keys its files hold); appended to with -merge")
		conc     = flag.Int("concurrency", 4, "requests at once per RPC endpoint")
		rpcTO    = flag.Duration("rpc-timeout", 30*time.Second, "per-request timeout")
		every    = flag.Int64("every", 10000, "a progress line every this many heights")
		merge    = flag.Bool("merge", false, "append the finished staging in -out-dir to the live files in -data-dir; fibre-scan must be stopped")
	)
	flag.Parse()
	if *outDir == "" || *dataDir == "" {
		fail(errors.New("-out-dir and -data-dir are required"))
	}
	if *merge {
		if err := mergeStaging(*outDir, *dataDir, "/proc", os.Stdout); err != nil {
			fail(err)
		}
		return
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	cfg := stageConfig{From: *from, FailedTo: *failedTo, CostsTo: *costsTo, OutDir: *outDir, DataDir: *dataDir, Every: *every, Out: os.Stdout}
	if err := cfg.check(); err != nil {
		fail(err)
	}
	s, err := openStaging(cfg)
	if err != nil {
		fail(err)
	}
	defer s.close()
	if s.complete() {
		s.report("staging complete, nothing to do")
		return
	}
	var archives []string
	for _, u := range strings.Split(*rpcs, ",") {
		if u = strings.TrimSpace(u); u != "" {
			archives = append(archives, u)
		}
	}
	f, err := dialFetcher(ctx, s.p.ChainID, *localRPC, archives, *conc, *rpcTO)
	if err != nil {
		fail(err)
	}
	if err := s.run(ctx, f); err != nil {
		fail(err)
	}
}

func fail(err error) {
	fmt.Fprintf(os.Stderr, "sentinel-txbackfill: %v\n", err)
	os.Exit(1)
}
