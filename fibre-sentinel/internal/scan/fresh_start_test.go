package scan

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
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
	"github.com/cosmos/cosmos-sdk/types/bech32"
)

// fakeNode is a JSON-RPC node with an ABCI query table: every path not in
// the table is "unknown query path" with code 6, which is what a chain
// below app version 10 says to any x/fibre or x/valaddr query. Its /status
// names base as its oldest block and tip as its newest (1 and 60 unless a
// test sets them; tipNext, when set, gives the tip of each next call).
type fakeNode struct {
	srv     *httptest.Server
	mu      sync.Mutex
	calls   map[string]int
	answer  map[string]func(height int64) abci.ResponseQuery
	base    int64
	tip     int64
	tipNext func() int64
}

func newFakeNode(t *testing.T) *fakeNode {
	t.Helper()
	n := &fakeNode{calls: map[string]int{}, answer: map[string]func(int64) abci.ResponseQuery{}, base: 1, tip: 60}
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
		w.Header().Set("Content-Type", "application/json")
		if req.Method == "status" {
			n.mu.Lock()
			n.calls["status"]++
			if n.tipNext != nil {
				n.tip = n.tipNext()
			}
			base, tip := n.base, n.tip
			n.mu.Unlock()
			fmt.Fprintf(w, `{"jsonrpc":"2.0","id":%s,"result":%s}`, req.ID, statusJSON(base, tip))
			return
		}
		if req.Method != "abci_query" {
			fmt.Fprintf(w, `{"jsonrpc":"2.0","id":%s,"error":{"code":-32601,"message":"Method not found"}}`, req.ID)
			return
		}
		path, _ := req.Params["path"].(string)
		var height int64
		if h, ok := req.Params["height"].(string); ok {
			fmt.Sscan(h, &height)
		}
		n.mu.Lock()
		n.calls[path]++
		f := n.answer[path]
		n.mu.Unlock()
		resp := abci.ResponseQuery{Code: 6, Codespace: "sdk", Log: "unknown query path: unknown request", Height: height}
		if f != nil {
			resp = f(height)
		}
		raw, err := cmtjson.Marshal(coretypes.ResultABCIQuery{Response: resp})
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		fmt.Fprintf(w, `{"jsonrpc":"2.0","id":%s,"result":%s}`, req.ID, raw)
	}))
	t.Cleanup(n.srv.Close)
	return n
}

// statusJSON is a /status answer from a synced node on test-1 holding
// blocks base to tip.
func statusJSON(base, tip int64) string {
	return fmt.Sprintf(`{"node_info":{"network":"test-1","protocol_version":{"p2p":"8","block":"11","app":"10"},"id":"","listen_addr":"","version":"","channels":"","moniker":"fake","other":{"tx_index":"on","rpc_address":""}},"sync_info":{"latest_block_hash":"","latest_app_hash":"","latest_block_height":"%d","latest_block_time":"2026-09-24T18:00:00Z","earliest_block_hash":"","earliest_app_hash":"","earliest_block_height":"%d","earliest_block_time":"2026-09-21T00:00:00Z","catching_up":false},"validator_info":{"address":"","pub_key":null,"voting_power":"0"}}`, tip, base)
}

func (n *fakeNode) count(path string) int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.calls[path]
}

func okValue(b []byte) func(int64) abci.ResponseQuery {
	return func(h int64) abci.ResponseQuery { return abci.ResponseQuery{Code: 0, Value: b, Height: h} }
}

const (
	pathFibreParams = "/celestia.fibre.v1.Query/Params"
	pathProviders   = "/celestia.valaddr.v1.Query/AllBondedFibreProviders"
)

// consAddr is a validator consensus address in both forms the code uses.
func consAddr(t *testing.T, seed byte) (bech, hexAddr string) {
	t.Helper()
	raw := make([]byte, 20)
	for i := range raw {
		raw[i] = seed
	}
	b, err := bech32.ConvertAndEncode("celestiavalcons", raw)
	if err != nil {
		t.Fatal(err)
	}
	return b, strings.ToLower(hex.EncodeToString(raw))
}

// freshScanner is a scanner as sentinel-scan builds it, on an empty data
// directory: no state.json, nothing on record.
func freshScanner(t *testing.T, node *fakeNode, dir string) *Scanner {
	t.Helper()
	s, err := New(Config{RPCURL: node.srv.URL, DataDir: dir, StartHeight: 50, RPCTimeout: 2 * time.Second}, NewLogger(50))
	if err != nil {
		t.Fatal(err)
	}
	s.chainID = "test-1"
	return s
}

// sentinel-scan built from this branch died on every empty data directory:
// New never set hosts, resume seeds into it on a fresh scan, and when the
// seed cannot be read — here, a chain that has no x/fibre or x/valaddr yet,
// which is every chain before the upgrade — the history is still asked
// Seeded() for the first state.json, on a nil pointer. A process with a
// state.json to load never noticed, so the running services did not.
func TestAFreshScanOnAChainWithoutFibreStartsAndWritesItsFirstState(t *testing.T) {
	node := newFakeNode(t)
	dir := t.TempDir()
	s := freshScanner(t, node, dir)
	defer s.store.Close()

	start, err := s.resume(context.Background(), 60)
	if err != nil || start != 50 {
		t.Fatalf("resume: start=%d err=%v", start, err)
	}
	if !s.fibreInactive {
		t.Fatal("x/fibre answered unknown query path and the scanner did not read it as inactive")
	}
	if seeded, _ := s.hosts.Seeded(); seeded {
		t.Fatal("no registry to read, yet the host history says it was seeded")
	}
	st, err := s.store.LoadState()
	if err != nil || st == nil {
		t.Fatalf("state.json after a fresh start: %+v %v", st, err)
	}
	if st.LastScannedHeight != 49 || st.StartHeight != 50 || st.HostSeeded || len(st.HostHistory) != 0 || len(st.ParamHistory) != 0 {
		t.Fatalf("first state.json: %+v", st)
	}
}

// The other fresh start: x/valaddr answers, the registry is seeded into the
// history at the start height, and the seed reaches state.json and
// host_history.jsonl before the first block is read.
func TestAFreshScanSeedsTheHostHistoryFromTheRegistryAndPersistsIt(t *testing.T) {
	node := newFakeNode(t)
	bechA, addrA := consAddr(t, 0xaa)
	bechB, addrB := consAddr(t, 0xbb)
	params, err := (&fibretypes.QueryParamsResponse{Params: fibretypes.DefaultParams()}).Marshal()
	if err != nil {
		t.Fatal(err)
	}
	provs, err := (&valaddrtypes.QueryAllBondedFibreProvidersResponse{Providers: []valaddrtypes.FibreProvider{
		{ValidatorConsensusAddress: bechA, Info: valaddrtypes.FibreProviderInfo{Host: "a.example:9090"}},
		{ValidatorConsensusAddress: bechB, Info: valaddrtypes.FibreProviderInfo{Host: "b.example:9090"}},
	}}).Marshal()
	if err != nil {
		t.Fatal(err)
	}
	node.answer[pathFibreParams] = okValue(params)
	node.answer[pathProviders] = okValue(provs)
	dir := t.TempDir()
	s := freshScanner(t, node, dir)

	start, err := s.resume(context.Background(), 60)
	if err != nil || start != 50 {
		t.Fatalf("resume: start=%d err=%v", start, err)
	}
	if s.fibreInactive {
		t.Fatal("params were served and the scanner still calls x/fibre inactive")
	}
	seeded, at := s.hosts.Seeded()
	if !seeded || at != 50 {
		t.Fatalf("seeded=%v at=%d, want the registry read at the start height", seeded, at)
	}
	want := map[string]string{addrA: "a.example:9090", addrB: "b.example:9090"}
	entries := s.hosts.Entries()
	if len(entries) != 2 {
		t.Fatalf("entries: %+v", entries)
	}
	for _, e := range entries {
		if e.Source != HostFromSeed || e.FromHeight != 50 || want[e.ConsAddress] != e.Host {
			t.Fatalf("seed entry: %+v", e)
		}
	}
	st, err := s.store.LoadState()
	if err != nil || st == nil || !st.HostSeeded || st.HostSeedAt != 50 || len(st.HostHistory) != 2 || st.LastScannedHeight != 49 {
		t.Fatalf("first state.json: %+v %v", st, err)
	}
	// the seed is on the record too, one event per entry
	raw, err := os.ReadFile(filepath.Join(dir, "host_history.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(raw)), "\n")
	if len(lines) != 2 {
		t.Fatalf("host_history.jsonl: %q", raw)
	}
	for _, l := range lines {
		var ev HostEvent
		if err := json.Unmarshal([]byte(l), &ev); err != nil || ev.Source != HostFromSeed || want[ev.ConsAddress] != ev.Host || ev.Time.IsZero() {
			t.Fatalf("host event %q: %+v %v", l, ev, err)
		}
	}
	s.store.Close()
}

// The first checkpoint carries the history — the seed and every event read
// since — and a second process resumes with exactly that: the entries, the
// seeded flag and its height, no second read of the registry.
func TestTheFirstCheckpointCarriesTheHostHistoryAndAResumeLoadsIt(t *testing.T) {
	node := newFakeNode(t)
	bechA, addrA := consAddr(t, 0xaa)
	bechB, addrB := consAddr(t, 0xbb)
	params, _ := (&fibretypes.QueryParamsResponse{Params: fibretypes.DefaultParams()}).Marshal()
	provs, _ := (&valaddrtypes.QueryAllBondedFibreProvidersResponse{Providers: []valaddrtypes.FibreProvider{
		{ValidatorConsensusAddress: bechA, Info: valaddrtypes.FibreProviderInfo{Host: "a.example:9090"}},
		{ValidatorConsensusAddress: bechB, Info: valaddrtypes.FibreProviderInfo{Host: "b.example:9090"}},
	}}).Marshal()
	node.answer[pathFibreParams] = okValue(params)
	node.answer[pathProviders] = okValue(provs)
	dir := t.TempDir()
	s := freshScanner(t, node, dir)
	if _, err := s.resume(context.Background(), 60); err != nil {
		t.Fatal(err)
	}
	// a registration read from block 51 before the first checkpoint
	if _, added := s.hosts.AddTxEvent(51, 0, addrA, "a2.example:9090"); !added {
		t.Fatal("event not added")
	}
	s.checkpoint(51)
	s.store.Close()

	st, err := s.store.LoadState()
	if err != nil || st == nil {
		t.Fatal(err)
	}
	if st.LastScannedHeight != 51 || !st.HostSeeded || st.HostSeedAt != 50 || len(st.HostHistory) != 3 {
		t.Fatalf("checkpointed state: %+v", st)
	}

	s2 := freshScanner(t, node, dir)
	defer s2.store.Close()
	resumeAt, err := s2.resume(context.Background(), 70)
	if err != nil || resumeAt != 52 {
		t.Fatalf("second process: resumeAt=%d err=%v", resumeAt, err)
	}
	if seeded, at := s2.hosts.Seeded(); !seeded || at != 50 {
		t.Fatalf("second process: seeded=%v at=%d", seeded, at)
	}
	got := s2.hosts.Entries()
	if len(got) != len(st.HostHistory) {
		t.Fatalf("second process loaded %d entries, state.json has %d", len(got), len(st.HostHistory))
	}
	for i := range got {
		if got[i] != st.HostHistory[i] {
			t.Fatalf("entry %d: loaded %+v, persisted %+v", i, got[i], st.HostHistory[i])
		}
	}
	if n := node.count(pathProviders); n != 1 {
		t.Fatalf("the registry was read %d times; a resume with a seeded history must not read it again", n)
	}
	if host, src := s2.hosts.HostAt(addrA, 52, 0, nil); host != "a2.example:9090" || src != HostFromEvent {
		t.Fatalf("A at 52: %s %s", host, src)
	}
	if host, src := s2.hosts.HostAt(addrB, 52, 0, nil); host != "b.example:9090" || src != HostFromSeed {
		t.Fatalf("B at 52: %s %s", host, src)
	}
}
