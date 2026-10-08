package scan

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	fibretypes "github.com/celestiaorg/celestia-app/v10/x/fibre/types"
	valaddrtypes "github.com/celestiaorg/celestia-app/v10/x/valaddr/types"
	abci "github.com/cometbft/cometbft/abci/types"
	cmtjson "github.com/cometbft/cometbft/libs/json"
	coretypes "github.com/cometbft/cometbft/rpc/core/types"
)

// rpcNode answers every JSON-RPC call with what reply gives for its method
// and height: a result, encoded as CometBFT encodes it, or, when rpcErr is
// set, an RPC error carrying it as its data, the way CometBFT reports a
// height it cannot serve. calls counts the calls per method.
type rpcNode struct {
	srv   *httptest.Server
	mu    sync.Mutex
	calls map[string]int
}

func newRPCNode(t *testing.T, reply func(method string, height int64) (result any, rpcErr string)) *rpcNode {
	t.Helper()
	n := &rpcNode{calls: map[string]int{}}
	n.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ID     json.RawMessage `json:"id"`
			Method string          `json:"method"`
			Params map[string]any  `json:"params"`
		}
		body, _ := io.ReadAll(r.Body)
		if err := json.Unmarshal(body, &req); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		var height int64
		if h, ok := req.Params["height"].(string); ok {
			fmt.Sscan(h, &height)
		}
		n.mu.Lock()
		n.calls[req.Method]++
		n.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		result, rpcErr := reply(req.Method, height)
		if rpcErr != "" {
			data, _ := json.Marshal(rpcErr)
			fmt.Fprintf(w, `{"jsonrpc":"2.0","id":%s,"error":{"code":-32603,"message":"Internal error","data":%s}}`, req.ID, data)
			return
		}
		var raw []byte
		if s, ok := result.(string); ok {
			raw = []byte(s)
		} else {
			var err error
			if raw, err = cmtjson.Marshal(result); err != nil {
				http.Error(w, err.Error(), 500)
				return
			}
		}
		fmt.Fprintf(w, `{"jsonrpc":"2.0","id":%s,"result":%s}`, req.ID, raw)
	}))
	t.Cleanup(n.srv.Close)
	return n
}

func (n *rpcNode) count(method string) int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.calls[method]
}

// scannerOn is a bare scanner on node, enough for its retry loop.
func scannerOn(t *testing.T, url string) *Scanner {
	t.Helper()
	log := NewLogger(100)
	c, err := NewChain(url, 2*time.Second, log)
	if err != nil {
		t.Fatal(err)
	}
	return &Scanner{log: log, chain: c}
}

func (l *Logger) lines() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return strings.Join(l.ring, "\n")
}

// CometBFT answers /block with no block and no error when the block's parts
// went missing after its meta was read: a pruning node removing its base
// block at that moment. Chain.Block read the empty answer and the process
// died on a nil pointer, with no dump and no skip hint; it is an error now,
// which the scanner's retry handles like any other.
func TestANodeAnsweringNoBlockIsAnErrorNotACrash(t *testing.T) {
	node := newRPCNode(t, func(method string, height int64) (any, string) {
		return coretypes.ResultBlock{}, ""
	})
	c, err := NewChain(node.srv.URL, 2*time.Second, NewLogger(10))
	if err != nil {
		t.Fatal(err)
	}
	blk, err := c.Block(context.Background(), 7)
	if err == nil || !strings.Contains(err.Error(), "block 7: empty response") || blk != nil {
		t.Fatalf("an answer with no block: %v, %+v", err, blk)
	}
}

// "could not find results for height" below the tip is a node that never
// kept them (it ran with storage.discard_abci_responses = true when it made
// that height, or came from a snapshot made that way): retried as the tip
// race, it held the scanner on that height for good, no gap recorded, a
// warning every five minutes. Now it is unavailable like a pruned height:
// the grace, then a gap.
func TestMissingResultsBelowTheTipAreAGapNotAnEndlessRetry(t *testing.T) {
	node := newRPCNode(t, func(method string, height int64) (any, string) {
		switch method {
		case "status":
			return statusJSON(1, 1000), ""
		case "block_results":
			return nil, fmt.Sprintf("could not find results for height #%d", height)
		}
		return nil, "unexpected " + method
	})
	s := scannerOn(t, node.srv.URL)
	prev := unavailableGrace
	unavailableGrace = 0
	defer func() { unavailableGrace = prev }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := s.retryRPCAt(ctx, "fetch block_results 500", 500, func() error {
		_, err := s.chain.BlockResults(ctx, 500)
		return err
	})
	var ue *ErrHeightUnavailable
	if !errors.As(err, &ue) || ue.Height != 500 {
		t.Fatalf("missing results 500 blocks below the tip: %v (block_results asked %d times)", err, node.count("block_results"))
	}
	if !s.recordGap(500, err, time.Time{}) || len(s.gaps) != 1 || s.gaps[0].From != 500 {
		t.Fatalf("not recorded as a gap: %+v", s.gaps)
	}
}

// At the tip the same answer is the race between the node storing a block
// (which /status then names) and storing its results: it clears on the
// next attempt, and is not worth a log line or a status error each time
// (724 lines in a week on Mocha, every one answered a second later).
func TestMissingResultsAtTheTipAreTheRaceAndStayQuiet(t *testing.T) {
	var mu sync.Mutex
	failed := 0
	node := newRPCNode(t, func(method string, height int64) (any, string) {
		switch method {
		case "status":
			return statusJSON(1, 500), ""
		case "block_results":
			mu.Lock()
			defer mu.Unlock()
			if failed == 0 {
				failed++
				return nil, fmt.Sprintf("could not find results for height #%d", height)
			}
			return coretypes.ResultBlockResults{Height: height}, ""
		}
		return nil, "unexpected " + method
	})
	s := scannerOn(t, node.srv.URL)
	ctx := context.Background()
	start := time.Now()
	err := s.retryRPCAt(ctx, "fetch block_results 500", 500, func() error {
		_, err := s.chain.BlockResults(ctx, 500)
		return err
	})
	if err != nil {
		t.Fatalf("the tip race did not clear: %v", err)
	}
	if took := time.Since(start); took > 5*time.Second {
		t.Fatalf("the tip race waited %s, as for an unavailable height", took)
	}
	if got := s.log.lines(); strings.Contains(got, "block_results") {
		t.Fatalf("the tip race was logged:\n%s", got)
	}
	if node.count("block_results") != 2 || node.count("status") != 1 {
		t.Fatalf("calls: %v", node.calls)
	}
}

// The params reconcile at the block just read meets the race one step
// later: cosmos-sdk's "cannot query with height in the future" (code 26).
// Quiet for two attempts like the results race; a third failure is logged.
func TestAHeightInTheFutureIsTheTipRace(t *testing.T) {
	future := &ABCIError{Path: "/celestia.fibre.v1.Query/Params", Height: 900, Code: 26, Codespace: "sdk",
		Log: "cannot query with height in the future; please provide a valid height: invalid height"}
	if !IsHeightInFuture(future) || !IsHeightInFuture(fmt.Errorf("params reconcile: %w", future)) {
		t.Fatal("code 26 not read as a height in the future")
	}
	if IsHeightInFuture(&ABCIError{Code: 18, Codespace: "sdk", Log: "failed to load state at height 5"}) || IsHeightInFuture(nil) {
		t.Fatal("another code read as a height in the future")
	}
	if IsHeightUnavailable(future) {
		t.Fatal("a height in the future is not one the node will never have")
	}
	if !quietTipRace(0, true) || !quietTipRace(1, true) || quietTipRace(2, true) || quietTipRace(0, false) {
		t.Fatal("quiet attempts of a tip race")
	}

	s := &Scanner{log: NewLogger(20)}
	calls := 0
	err := s.retryRPC(context.Background(), "params reconcile at height 900", func() error {
		calls++
		if calls == 1 {
			return future
		}
		return nil
	})
	if err != nil || calls != 2 {
		t.Fatalf("err=%v calls=%d", err, calls)
	}
	if got := s.log.lines(); got != "" {
		t.Fatalf("logged:\n%s", got)
	}
}

// lazySeed reads a newcomer's registration the first time it appears in an
// assignment. It asked for the state at the settlement height, which in
// follow mode is the tip, and until the app commits the tip the node
// answers "cannot query with height in the future"; the fallback asked the
// tip again, so both failed and that publication's host stayed unknown for
// good. The state after block h-1 is committed once block h is read, and
// the validator has no event in block h (it would be on record), so it is
// the same registration.
func TestLazySeedReadsTheCommittedStateBeforeTheSettlement(t *testing.T) {
	node := newFakeNode(t)
	node.tip = 100
	_, addr := consAddr(t, 0xcc)
	found, err := (&valaddrtypes.QueryFibreProviderInfoResponse{Found: true, Info: &valaddrtypes.FibreProviderInfo{Host: "c.example:7980"}}).Marshal()
	if err != nil {
		t.Fatal(err)
	}
	node.answer[pathProviderInfo] = func(h int64) abci.ResponseQuery {
		if h >= 100 || h <= 0 {
			return abci.ResponseQuery{Code: 26, Codespace: "sdk", Log: "cannot query with height in the future; please provide a valid height: invalid height", Height: h}
		}
		return abci.ResponseQuery{Value: found, Height: h}
	}
	s := freshScanner(t, node, t.TempDir())
	defer s.store.Close()
	s.hosts = LoadHostHistory(nil, true, 50)

	s.lazySeed(context.Background(), addr, 100)
	if host, src := s.hosts.HostAt(addr, 100, 0, nil); host != "c.example:7980" || src != HostFromSeedLazy {
		t.Fatalf("host at settlement 100: %q (%s)", host, src)
	}
}

// paramsBelow answers x/fibre params with the default window (1000) at
// every height from base on and at the latest (0), and with the SDK's
// pruned-state error below base.
func paramsBelow(t *testing.T, base int64) func(int64) abci.ResponseQuery {
	t.Helper()
	ok, err := (&fibretypes.QueryParamsResponse{Params: fibretypes.DefaultParams()}).Marshal()
	if err != nil {
		t.Fatal(err)
	}
	return func(h int64) abci.ResponseQuery {
		if h > 0 && h < base {
			return abci.ResponseQuery{Code: 18, Codespace: "sdk", Log: fmt.Sprintf("failed to load state at height %d; version does not exist (latest height: 9000): invalid request", h), Height: h}
		}
		return abci.ResponseQuery{Value: ok, Height: h}
	}
}

// A START_HEIGHT below the node's oldest block (a node restored from a
// state-sync snapshot at 5000, the scan told to start at Fibre's first
// block): the seed read the state at START_HEIGHT-1 for the ten minutes of
// the unavailable grace and the scanner exited, before anything was saved,
// at every restart. It is refused at once, with the height that would work.
func TestAStartHeightBelowTheNodesHistoryIsRefusedAtOnce(t *testing.T) {
	node := newFakeNode(t)
	node.base, node.tip = 5000, 9000
	node.answer[pathFibreParams] = paramsBelow(t, 5000)
	prev := unavailableGrace
	unavailableGrace = 0
	defer func() { unavailableGrace = prev }()
	s := freshScanner(t, node, t.TempDir()) // START_HEIGHT 50
	defer s.store.Close()

	_, err := s.resume(context.Background(), 9000)
	if err == nil || !strings.Contains(err.Error(), "START_HEIGHT 50") || !strings.Contains(err.Error(), "starts at block 5000") ||
		!strings.Contains(err.Error(), "set START_HEIGHT to 6000 or later") {
		t.Fatalf("resume: %v", err)
	}
	if n := node.count(pathFibreParams); n != 1 {
		t.Fatalf("params asked %d times: only the tip's window should have been read", n)
	}
	if st, err := s.store.LoadState(); err != nil || st != nil {
		t.Fatalf("a refused start wrote state: %+v %v", st, err)
	}
}

// Above the node's oldest block but within a promise window of it: the
// seed works, and every settlement whose promise is older than the node's
// history would have become a scan gap for good. Refused the same way.
func TestAStartHeightWithinAPromiseWindowOfTheNodesHistoryIsRefused(t *testing.T) {
	node := newFakeNode(t)
	node.base, node.tip = 5000, 9000
	node.answer[pathFibreParams] = paramsBelow(t, 5000)
	s := freshScanner(t, node, t.TempDir())
	s.cfg.StartHeight = 5500
	defer s.store.Close()

	if _, err := s.resume(context.Background(), 9000); err == nil || !strings.Contains(err.Error(), "set START_HEIGHT to 6000 or later") {
		t.Fatalf("resume: %v", err)
	}

	// a start the node can serve is a start, as before
	s2 := freshScanner(t, node, t.TempDir())
	s2.cfg.StartHeight = 6000
	defer s2.store.Close()
	if start, err := s2.resume(context.Background(), 9000); err != nil || start != 6000 {
		t.Fatalf("resume at 6000: %d %v", start, err)
	}
}

// From the tip, on a node that has not held a promise window of blocks yet
// (state-synced a moment ago), the scan waits until it has, says why, and
// starts at the tip it then finds.
func TestAFreshScanFromTheTipWaitsForAPromiseWindowOfHistory(t *testing.T) {
	node := newFakeNode(t)
	node.base = 5000
	node.answer[pathFibreParams] = paramsBelow(t, 5000)
	tips := []int64{5300, 5700, 6100}
	node.tipNext = func() int64 {
		tip := tips[0]
		if len(tips) > 1 {
			tips = tips[1:]
		}
		return tip
	}
	prev := historyWaitEvery
	historyWaitEvery = 10 * time.Millisecond
	defer func() { historyWaitEvery = prev }()
	s := freshScanner(t, node, t.TempDir())
	s.cfg.StartHeight = 0
	defer s.store.Close()

	start, err := s.resume(context.Background(), 5300)
	if err != nil || start != 6100 {
		t.Fatalf("resume: start=%d err=%v", start, err)
	}
	if got := s.log.lines(); !strings.Contains(got, "fresh scan from the tip is waiting") || !strings.Contains(got, "once the tip reaches 6000") {
		t.Fatalf("the wait was not said:\n%s", got)
	}
	if st, err := s.store.LoadState(); err != nil || st == nil || st.StartHeight != 6100 {
		t.Fatalf("state after the wait: %+v %v", st, err)
	}

	// a stop while it waits is a stop, not a failure
	node.mu.Lock()
	node.tipNext = func() int64 { return 5300 }
	node.mu.Unlock()
	s3 := freshScanner(t, node, t.TempDir())
	s3.cfg.StartHeight = 0
	defer s3.store.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	if _, err := s3.resume(ctx, 5300); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("resume under a stop: %v", err)
	}
}

// A scanner further behind the tip than the node keeps state (after a long
// outage) meets pruned state at every params check: the blocks are still
// served, the state after them is not. Waited out under the unavailable
// grace, each check cost ten minutes, and a scan making about sixty blocks
// per ten minutes never caught up with a chain making a hundred. The check
// fails at once now, recorded once for the run, and the tip race is still
// retried.
func TestAParamsCheckOnPrunedStateDoesNotWaitForIt(t *testing.T) {
	node := newFakeNode(t)
	node.tip = 9000
	node.answer[pathFibreParams] = paramsBelow(t, 5000)
	dir := t.TempDir()
	s := freshScanner(t, node, dir)
	defer s.store.Close()
	s.params = NewParamHistory(100, fibretypes.DefaultParams())
	s.startHeight, s.lastReconcile = 100, 120
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	start := time.Now()
	s.reconcileParams(ctx, 180)
	s.reconcileParams(ctx, 240)
	if took := time.Since(start); took > 5*time.Second || ctx.Err() != nil {
		t.Fatalf("two checks on pruned state took %s (ctx: %v); the grace was waited out", took, ctx.Err())
	}
	if n := node.count(pathFibreParams); n != 2 {
		t.Fatalf("params asked %d times for two checks, want once each", n)
	}
	got := readUncertainty(t, filepath.Join(dir, "param_uncertainty.jsonl"))
	if len(got) != 1 || got[0].Kind != UncertaintyCheckSkipped || got[0].FromHeight != 121 || got[0].ToHeight != 180 ||
		!strings.Contains(got[0].LastError, "failed to load state at height 180") {
		t.Fatalf("records: %+v, want one check_skipped 121-180 naming the pruned state", got)
	}
	if s.lastReconcile != 120 || s.reconcileFailingSince != 121 {
		t.Fatalf("marker=%d failing_since=%d, want 120 and 121", s.lastReconcile, s.reconcileFailingSince)
	}

	// Near the tip the state is there; the tip race (code 26) on the way is
	// asked again, as before.
	ok, err := (&fibretypes.QueryParamsResponse{Params: fibretypes.DefaultParams()}).Marshal()
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	raced := false
	node.mu.Lock()
	node.answer[pathFibreParams] = func(h int64) abci.ResponseQuery {
		mu.Lock()
		defer mu.Unlock()
		if !raced {
			raced = true
			return abci.ResponseQuery{Code: 26, Codespace: "sdk", Log: "cannot query with height in the future; please provide a valid height: invalid height", Height: h}
		}
		return abci.ResponseQuery{Value: ok, Height: h}
	}
	node.mu.Unlock()
	s.reconcileParams(ctx, 8940)
	if s.lastReconcile != 8940 || s.reconcileFailingSince != 0 {
		t.Fatalf("after the tip race: marker=%d failing_since=%d", s.lastReconcile, s.reconcileFailingSince)
	}
	if got := readUncertainty(t, filepath.Join(dir, "param_uncertainty.jsonl")); len(got) != 1 {
		t.Fatalf("a check that read state wrote a record: %+v", got)
	}
}

// A stop (every deploy restarts the scanner) that lands on a params check
// is not a check that failed: before, it left a check_skipped range in the
// record and the exports, and the next process carried its latch.
func TestAStopDuringAParamsCheckRecordsNothing(t *testing.T) {
	node := newFakeNode(t)
	node.answer[pathFibreParams] = paramsBelow(t, 1)
	dir := t.TempDir()
	s := freshScanner(t, node, dir)
	defer s.store.Close()
	s.params = NewParamHistory(100, fibretypes.DefaultParams())
	s.startHeight, s.lastReconcile = 100, 120
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	s.reconcileParams(ctx, 180)
	if got := readUncertainty(t, filepath.Join(dir, "param_uncertainty.jsonl")); len(got) != 0 {
		t.Fatalf("a stop wrote %d record(s): %+v", len(got), got)
	}
	if s.lastReconcile != 120 || s.reconcileFailingSince != 0 {
		t.Fatalf("marker=%d failing_since=%d after a stop, want 120 and 0", s.lastReconcile, s.reconcileFailingSince)
	}
}

// CometBFT answers a read below its oldest block with that block's height
// ("height 42 is not available, lowest height is 5000"), and the oldest
// block only moves up. A validator set at a promise height a state-synced
// node does not hold was retried for the ten minutes of the grace, once
// for every such publication; it is unavailable at once now.
func TestAHeightBelowTheNodesOldestBlockIsUnavailableAtOnce(t *testing.T) {
	node := newRPCNode(t, func(method string, height int64) (any, string) {
		if method == "validators" {
			return nil, fmt.Sprintf("height %d is not available, lowest height is 5000", height)
		}
		return nil, "unexpected " + method
	})
	s := scannerOn(t, node.srv.URL)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	start := time.Now()
	err := s.retryRPCAt(ctx, "validator set at height 42", 42, func() error {
		_, err := s.chain.ValidatorSet(ctx, 42)
		return err
	})
	var ue *ErrHeightUnavailable
	if !errors.As(err, &ue) || ue.Height != 42 || ue.Base != 5000 {
		t.Fatalf("a height below the node's oldest block: %v", err)
	}
	if took := time.Since(start); took > 5*time.Second || node.count("validators") != 1 {
		t.Fatalf("took %s and %d asks, want one ask and no wait", took, node.count("validators"))
	}
	if !s.recordPublicationGap(100, err, time.Time{}) || len(s.gaps) != 1 || !s.gaps[0].HostEventsRead {
		t.Fatalf("not recorded as a publication gap: %+v", s.gaps)
	}

	for msg, want := range map[string]int64{
		"validators h=42 page=1: RPC error -32603 - Internal error: height 42 is not available, lowest height is 5000": 5000,
		"height 7 is not available, lowest height is 12)":                                                              12,
		"height 42 is not available": 0,
		"lowest height is":           0,
	} {
		if got, ok := nodeBase(errors.New(msg)); got != want || ok != (want > 0) {
			t.Errorf("nodeBase(%q) = %d %v, want %d", msg, got, ok, want)
		}
	}
}

// After the first height of a run of unavailable ones the grace is the run
// grace, and the wait between two asks never runs past it: a fixed thirty
// seconds made each height cost thirty, not the twenty the run grace says.
func TestARunOfUnavailableHeightsCostsTheRunGraceNotMore(t *testing.T) {
	node := newRPCNode(t, func(method string, height int64) (any, string) {
		if method == "block_results" {
			return nil, "node is not persisting finalize block responses"
		}
		return nil, "unexpected " + method
	})
	s := scannerOn(t, node.srv.URL)
	prev := unavailableRunGrace
	unavailableRunGrace = 300 * time.Millisecond
	defer func() { unavailableRunGrace = prev }()
	s.unavailableRun = 1
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	start := time.Now()
	err := s.retryRPCAt(ctx, "fetch block_results 500", 500, func() error {
		_, err := s.chain.BlockResults(ctx, 500)
		return err
	})
	var ue *ErrHeightUnavailable
	if !errors.As(err, &ue) || ue.Height != 500 {
		t.Fatalf("a height in a run: %v", err)
	}
	if took := time.Since(start); took > 5*time.Second {
		t.Fatalf("a height in a run cost %s with a run grace of %s", took, unavailableRunGrace)
	}
}
