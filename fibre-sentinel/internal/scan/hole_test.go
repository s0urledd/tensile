package scan

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	abci "github.com/cometbft/cometbft/abci/types"
	coretypes "github.com/cometbft/cometbft/rpc/core/types"
	cmttypes "github.com/cometbft/cometbft/types"
)

// stateAt writes a state.json for a scanner that has scanned through last
// on test-1, x/fibre not active, its host history seeded.
func stateAt(t *testing.T, dir string, last int64) {
	t.Helper()
	st, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if err := st.SaveState(PersistState{ChainID: "test-1", StartHeight: 900, LastScannedHeight: last, HostSeeded: true, HostSeedAt: 900}); err != nil {
		t.Fatal(err)
	}
}

// A scanner resumed on a node whose history starts above its cursor (the
// node rebuilt from a state-sync snapshot while the scanner was down) met
// every height below the node's oldest block one by one: ten minutes for
// the first, thirty seconds for each after it, the feed stalled for the
// whole stretch. The stretch is one gap at once now, and the scan starts
// at the node's oldest block. A height the operator listed keeps its own
// reason inside it, as when it is met on its own.
func TestAResumeBelowTheNodesOldestBlockRecordsTheHoleAtOnce(t *testing.T) {
	node := newFakeNode(t)
	node.base, node.tip = 5000, 9000
	dir := t.TempDir()
	stateAt(t, dir, 1000)
	s := freshScanner(t, node, dir)
	defer s.store.Close()
	s.cfg.SkipHeights = []HeightRange{{2000, 2001}}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	next, err := s.resume(ctx, 9000)
	if err != nil || next != 5000 {
		t.Fatalf("resume: next=%d err=%v, want 5000", next, err)
	}
	type span struct {
		from, to int64
		reason   string
	}
	var got []span
	for _, g := range s.gaps {
		got = append(got, span{g.From, g.To, g.Reason})
	}
	want := []span{{1001, 1999, unavailableReason}, {2000, 2001, SkipReason}, {2002, 4999, unavailableReason}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("gaps %+v, want %+v", got, want)
	}

	// A MaxHeight below the node's oldest block stops the gap there.
	dir2 := t.TempDir()
	stateAt(t, dir2, 1000)
	s2 := freshScanner(t, node, dir2)
	defer s2.store.Close()
	s2.cfg.MaxHeight = 3000
	if next, err := s2.resume(ctx, 9000); err != nil || next != 3001 || len(s2.gaps) != 1 || s2.gaps[0].To != 3000 {
		t.Fatalf("resume under MaxHeight 3000: next=%d err=%v gaps=%+v", next, err, s2.gaps)
	}

	// A node that holds the cursor changes nothing.
	node.mu.Lock()
	node.base = 1
	node.mu.Unlock()
	dir3 := t.TempDir()
	stateAt(t, dir3, 1000)
	s3 := freshScanner(t, node, dir3)
	defer s3.store.Close()
	if next, err := s3.resume(ctx, 9000); err != nil || next != 1001 || len(s3.gaps) != 0 {
		t.Fatalf("resume on a node holding the cursor: next=%d err=%v gaps=%+v", next, err, s3.gaps)
	}
}

// The same hole met in the middle of a run (the node pruned, or replaced,
// under a running scanner): the block read that names the node's oldest
// block is answered at once, and the rest of the hole is recorded without
// asking for it.
func TestAHoleBelowTheNodesOldestBlockMetMidRunIsCrossedAtOnce(t *testing.T) {
	const base, tip = 1100, 1105
	var mu sync.Mutex
	blockReads := map[int64]int{}
	node := newRPCNode(t, func(method string, height int64) (any, string) {
		switch method {
		case "status":
			// /status still says the node holds the cursor: the hole is
			// only met when a block is asked for.
			return statusJSON(1, tip), ""
		case "block":
			mu.Lock()
			blockReads[height]++
			mu.Unlock()
			if height < base {
				return nil, fmt.Sprintf("height %d is not available, lowest height is %d", height, base)
			}
			return coretypes.ResultBlock{Block: &cmttypes.Block{Header: cmttypes.Header{ChainID: "test-1", Height: height,
				Time: time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC).Add(time.Duration(height) * 6 * time.Second)}}}, ""
		case "block_results":
			return coretypes.ResultBlockResults{Height: height}, ""
		case "abci_query":
			return coretypes.ResultABCIQuery{Response: abci.ResponseQuery{Code: 6, Codespace: "sdk", Log: "unknown query path: unknown request", Height: height}}, ""
		}
		return nil, "unexpected " + method
	})
	dir := t.TempDir()
	stateAt(t, dir, 1000)
	s, err := New(Config{RPCURL: node.srv.URL, DataDir: dir, RPCTimeout: 2 * time.Second}, NewLogger(400))
	if err != nil {
		t.Fatal(err)
	}
	// A cancel rather than a deadline: a scan held in the hole stops
	// cleanly, and the checks below say why.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	time.AfterFunc(30*time.Second, cancel)
	if err := s.Run(ctx); err != nil {
		t.Fatalf("run: %v", err)
	}

	st, err := s.store.LoadState()
	if err != nil || st == nil {
		t.Fatalf("state: %v", err)
	}
	if st.LastScannedHeight != tip {
		t.Fatalf("last scanned %d, want %d", st.LastScannedHeight, tip)
	}
	if len(st.Gaps) != 1 || st.Gaps[0].From != 1001 || st.Gaps[0].To != base-1 || st.Gaps[0].Reason != unavailableReason {
		t.Fatalf("gaps %+v, want one gap 1001-%d", st.Gaps, base-1)
	}
	mu.Lock()
	defer mu.Unlock()
	if blockReads[1001] != 1 {
		t.Fatalf("block 1001 asked %d times, want once", blockReads[1001])
	}
	for h := int64(1002); h < base; h++ {
		if blockReads[h] != 0 {
			t.Fatalf("block %d, below the node's oldest block, was asked for", h)
		}
	}
	for h := int64(base); h <= tip; h++ {
		if blockReads[h] != 1 {
			t.Fatalf("block %d asked %d times, want once", h, blockReads[h])
		}
	}
}
