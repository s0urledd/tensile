package scan

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	abci "github.com/cometbft/cometbft/abci/types"
	cmtjson "github.com/cometbft/cometbft/libs/json"
	cmtversion "github.com/cometbft/cometbft/proto/tendermint/version"
	coretypes "github.com/cometbft/cometbft/rpc/core/types"
	cmttypes "github.com/cometbft/cometbft/types"
	"github.com/gorilla/websocket"
)

// headsNode is a node with no Fibre on it (every ABCI query is "unknown query
// path") that the follow loop can run against end to end: /status at a tip
// the test moves, empty blocks and results at every height, and the
// /websocket a CometBFT node serves, where the test announces headers, drops
// the connection, cancels the subscription, or refuses new ones.
type headsNode struct {
	srv *httptest.Server

	mu      sync.Mutex
	tip     int64
	reads   []int64 // every block height asked for, in order
	results map[int64]int
	subs    int // subscriptions acknowledged
	queries []string
	refuse  bool
	conns   []*headsConn
}

type headsConn struct {
	c  *websocket.Conn
	mu sync.Mutex // gorilla allows one writer at a time
}

func (c *headsConn) send(v string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.c.WriteMessage(websocket.TextMessage, []byte(v))
}

func newHeadsNode(t *testing.T, tip int64) *headsNode {
	t.Helper()
	n := &headsNode{tip: tip, results: map[int64]int{}}
	blockTime := time.Date(2026, 10, 4, 12, 0, 0, 0, time.UTC)
	header := func(h int64) *cmttypes.Header {
		return &cmttypes.Header{Version: cmtversion.Consensus{Block: 11, App: FibreAppVersion}, ChainID: "test-1", Height: h,
			Time: blockTime.Add(time.Duration(h) * 6 * time.Second)}
	}
	up := websocket.Upgrader{}
	mux := http.NewServeMux()
	mux.HandleFunc("/websocket", func(w http.ResponseWriter, r *http.Request) {
		n.mu.Lock()
		refuse := n.refuse
		n.mu.Unlock()
		if refuse {
			http.Error(w, "websocket off", http.StatusServiceUnavailable)
			return
		}
		c, err := up.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer c.Close()
		var req struct {
			ID     json.RawMessage   `json:"id"`
			Method string            `json:"method"`
			Params map[string]string `json:"params"`
		}
		if err := c.ReadJSON(&req); err != nil || req.Method != "subscribe" {
			return
		}
		hc := &headsConn{c: c}
		n.mu.Lock()
		n.queries = append(n.queries, req.Params["query"])
		n.mu.Unlock()
		if err := hc.send(fmt.Sprintf(`{"jsonrpc":"2.0","id":%s,"result":{}}`, req.ID)); err != nil {
			return
		}
		n.mu.Lock()
		n.subs++
		n.conns = append(n.conns, hc)
		n.mu.Unlock()
		for {
			if _, _, err := c.ReadMessage(); err != nil {
				return
			}
		}
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
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
		tip := n.tip
		switch req.Method {
		case "block":
			n.reads = append(n.reads, height)
		case "block_results":
			n.results[height]++
		}
		n.mu.Unlock()
		var result any
		switch req.Method {
		case "status":
			fmt.Fprintf(w, `{"jsonrpc":"2.0","id":%s,"result":{"node_info":{"network":"test-1","protocol_version":{"p2p":"8","block":"11","app":"10"},"id":"","listen_addr":"","version":"","channels":"","moniker":"fake","other":{"tx_index":"on","rpc_address":""}},"sync_info":{"latest_block_hash":"","latest_app_hash":"","latest_block_height":"%d","latest_block_time":"2026-10-04T12:00:00Z","earliest_block_hash":"","earliest_app_hash":"","earliest_block_height":"1","earliest_block_time":"2026-10-01T00:00:00Z","catching_up":false},"validator_info":{"address":"","pub_key":null,"voting_power":"0"}}}`, req.ID, tip)
			return
		case "header":
			result = coretypes.ResultHeader{Header: header(height)}
		case "block":
			if height > tip {
				fmt.Fprintf(w, `{"jsonrpc":"2.0","id":%s,"error":{"code":-32603,"message":"Internal error","data":"height %d must be less than or equal to the current blockchain height %d"}}`, req.ID, height, tip)
				return
			}
			result = coretypes.ResultBlock{Block: &cmttypes.Block{Header: *header(height)}}
		case "block_results":
			result = coretypes.ResultBlockResults{Height: height}
		case "abci_query":
			result = coretypes.ResultABCIQuery{Response: abci.ResponseQuery{Code: 6, Codespace: "sdk", Log: "unknown query path: unknown request", Height: height}}
		default:
			fmt.Fprintf(w, `{"jsonrpc":"2.0","id":%s,"error":{"code":-32601,"message":"Method not found"}}`, req.ID)
			return
		}
		raw, err := cmtjson.Marshal(result)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"jsonrpc":"2.0","id":%s,"result":%s}`, req.ID, raw)
	})
	n.srv = httptest.NewServer(mux)
	t.Cleanup(func() {
		n.drop()
		n.srv.Close()
	})
	return n
}

func (n *headsNode) setTip(h int64) {
	n.mu.Lock()
	n.tip = h
	n.mu.Unlock()
}

func (n *headsNode) setRefuse(v bool) {
	n.mu.Lock()
	n.refuse = v
	n.mu.Unlock()
}

// announce sends the NewBlockHeader event for h, as CometBFT words it, to
// every live subscriber.
func (n *headsNode) announce(h int64) {
	n.send(fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"result":{"query":"tm.event='NewBlockHeader'","data":{"type":"tendermint/event/NewBlockHeader","value":{"header":{"version":{"block":"11","app":"10"},"chain_id":"test-1","height":"%d","time":"2026-10-04T12:00:00Z"}}},"events":{"tm.event":["NewBlockHeader"]}}}`, h))
}

// cancel is CometBFT cancelling a subscriber: an error on the subscription's
// id, and the connection left open.
func (n *headsNode) cancel() {
	n.send(`{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"Server error","data":"subscription was canceled (reason: client is not pulling messages fast enough)"}}`)
	n.mu.Lock()
	n.conns = nil
	n.mu.Unlock()
}

func (n *headsNode) send(msg string) {
	n.mu.Lock()
	conns := append([]*headsConn(nil), n.conns...)
	n.mu.Unlock()
	for _, c := range conns {
		_ = c.send(msg)
	}
}

// drop closes every subscriber's connection, as a node restart does.
func (n *headsNode) drop() {
	n.mu.Lock()
	conns := n.conns
	n.conns = nil
	n.mu.Unlock()
	for _, c := range conns {
		_ = c.c.Close()
	}
}

func (n *headsNode) subscriptions() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.subs
}

func (n *headsNode) read(h int64) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	for _, r := range n.reads {
		if r == h {
			return true
		}
	}
	return false
}

// readsInOrder fails unless every height from first to last was read exactly
// once, in order, and its results once.
func (n *headsNode) readsInOrder(t *testing.T, first, last int64) {
	t.Helper()
	n.mu.Lock()
	defer n.mu.Unlock()
	want := []int64{}
	for h := first; h <= last; h++ {
		want = append(want, h)
	}
	if fmt.Sprint(n.reads) != fmt.Sprint(want) {
		t.Fatalf("blocks read %v, want each of %d-%d once, in order", n.reads, first, last)
	}
	for h := first; h <= last; h++ {
		if n.results[h] != 1 {
			t.Fatalf("block_results %d read %d times", h, n.results[h])
		}
	}
}

// waitFor polls cond until it holds or within passes, and says how long it
// took.
func waitFor(t *testing.T, within time.Duration, what string, cond func() bool) time.Duration {
	t.Helper()
	start := time.Now()
	for !cond() {
		if time.Since(start) > within {
			t.Fatalf("%s: not within %s", what, within)
		}
		time.Sleep(5 * time.Millisecond)
	}
	return time.Since(start)
}

// followScanner runs a following scanner from height 10 against node until
// the test ends, and returns it with what stops it.
func followScanner(t *testing.T, node *headsNode, poll, subscribedPoll time.Duration) (*Scanner, func()) {
	t.Helper()
	s, err := New(Config{RPCURL: node.srv.URL, DataDir: t.TempDir(), StartHeight: 10, Follow: true, Subscribe: true,
		PollInterval: poll, SubscribedPoll: subscribedPoll, RPCTimeout: 2 * time.Second}, NewLogger(400))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()
	var once sync.Once
	stop := func() { once.Do(func() { halt(t, cancel, done) }) }
	t.Cleanup(stop)
	return s, stop
}

func halt(t *testing.T, cancel context.CancelFunc, done chan error) {
	{
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("run: %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Error("the scanner did not stop")
		}
	}
}

// subscriptionLines is what the scanner has logged about its subscription.
func subscriptionLines(s *Scanner) []string {
	s.log.mu.Lock()
	defer s.log.mu.Unlock()
	var out []string
	for _, l := range s.log.ring {
		if strings.Contains(l, "block subscription") {
			out = append(out, l)
		}
	}
	return out
}

// A header the node announces is read at once: with both polls far longer
// than the test, the announcement is the only thing that can have woken the
// loop. The subscription asks for headers only.
func TestAnAnnouncedHeaderIsReadAtOnce(t *testing.T) {
	node := newHeadsNode(t, 12)
	s, _ := followScanner(t, node, 30*time.Second, 30*time.Second)
	waitFor(t, 10*time.Second, "caught up and subscribed", func() bool { return node.read(12) && node.subscriptions() == 1 && s.heads.Up() })
	node.mu.Lock()
	q := append([]string(nil), node.queries...)
	node.mu.Unlock()
	if len(q) != 1 || q[0] != "tm.event='NewBlockHeader'" {
		t.Fatalf("subscribed to %q, want new block headers only", q)
	}

	for h := int64(13); h <= 15; h++ {
		node.setTip(h)
		node.announce(h)
		took := waitFor(t, 2*time.Second, fmt.Sprintf("height %d read after its announcement", h), func() bool { return node.read(h) })
		if took > time.Second {
			t.Fatalf("height %d read %s after its announcement", h, took)
		}
	}
	node.readsInOrder(t, 10, 15)
	if s.heads.Announced() != 15 {
		t.Fatalf("announced %d", s.heads.Announced())
	}
}

// An announcement decides nothing about which heights are read: one for a
// height already read, the same height twice, a burst for several heights,
// and one ahead of what /status says, each leave every height read once, in
// order, and none past the tip /status gives.
func TestAnnouncementsNeverReadAHeightTwiceOrOutOfOrder(t *testing.T) {
	node := newHeadsNode(t, 12)
	s, _ := followScanner(t, node, 30*time.Second, 30*time.Second)
	waitFor(t, 10*time.Second, "caught up and subscribed", func() bool { return node.read(12) && s.heads.Up() })

	// a stale announcement, and the newest one twice
	node.announce(11)
	node.announce(12)
	node.announce(12)
	time.Sleep(200 * time.Millisecond)
	// a burst: three heights at once, announced out of order
	node.setTip(15)
	node.announce(15)
	node.announce(13)
	node.announce(14)
	waitFor(t, 2*time.Second, "the burst read", func() bool { return node.read(15) })
	// announced before /status has it: polled for at -poll's pace (30 s
	// here), so not read, and never asked of the node beyond its tip
	node.announce(16)
	time.Sleep(300 * time.Millisecond)
	if node.read(16) {
		t.Fatal("height 16 read before /status named it")
	}
	node.setTip(17)
	node.announce(17)
	waitFor(t, 2*time.Second, "16 and 17 read", func() bool { return node.read(17) })
	time.Sleep(200 * time.Millisecond)
	node.readsInOrder(t, 10, 17)
}

// A subscription that drops (a node restart) or that the node cancels falls
// back to polling at -poll and is made again with backoff, without a line per
// failed attempt; once it is back, the loop polls at the slower safety pace
// again and announcements wake it.
func TestALostSubscriptionFallsBackToPollingAndComesBack(t *testing.T) {
	headsBackoff = func(int) time.Duration { return 50 * time.Millisecond }
	t.Cleanup(func() { headsBackoff = rpcBackoff })
	node := newHeadsNode(t, 12)
	s, _ := followScanner(t, node, 100*time.Millisecond, 30*time.Second)
	waitFor(t, 10*time.Second, "caught up and subscribed", func() bool { return node.read(12) && s.heads.Up() })
	time.Sleep(300 * time.Millisecond) // a pause begun at -poll before the subscription was up ends
	// up: a block nobody announces waits for the 30 s safety poll
	node.setTip(13)
	time.Sleep(500 * time.Millisecond)
	if node.read(13) {
		t.Fatal("height 13 read without an announcement while the subscription was up: the loop is polling at -poll")
	}
	node.announce(13)
	waitFor(t, 2*time.Second, "13 read on its announcement", func() bool { return node.read(13) })

	// the node goes away and refuses new subscriptions for a while
	node.setRefuse(true)
	node.drop()
	waitFor(t, 2*time.Second, "the subscription seen down", func() bool { return !s.heads.Up() })
	node.setTip(14)
	waitFor(t, 2*time.Second, "14 read by polling at -poll", func() bool { return node.read(14) })
	time.Sleep(500 * time.Millisecond) // several refused attempts at 50 ms
	if n := len(subscriptionLines(s)); n != 2 {
		t.Fatalf("log lines about the subscription while it failed again and again: %q", subscriptionLines(s))
	}
	node.setRefuse(false)
	waitFor(t, 2*time.Second, "subscribed again", func() bool { return node.subscriptions() == 2 && s.heads.Up() })
	time.Sleep(300 * time.Millisecond) // the pause begun at -poll ends; the next one is the safety poll's
	node.setTip(15)
	time.Sleep(500 * time.Millisecond)
	if node.read(15) {
		t.Fatal("height 15 read without an announcement once the subscription was back")
	}
	node.announce(15)
	waitFor(t, 2*time.Second, "15 read on its announcement", func() bool { return node.read(15) })

	// the node cancels the subscription (a slow subscriber), and the
	// connection stays open: the scanner leaves it and subscribes again
	node.cancel()
	waitFor(t, 2*time.Second, "subscribed a third time", func() bool { return node.subscriptions() == 3 && s.heads.Up() })
	node.setTip(16)
	node.announce(16)
	waitFor(t, 2*time.Second, "16 read on its announcement", func() bool { return node.read(16) })

	node.readsInOrder(t, 10, 16)
	lines := subscriptionLines(s)
	if len(lines) != 5 || !strings.Contains(lines[0], "block subscription up:") || !strings.Contains(lines[1], "block subscription lost") ||
		!strings.Contains(lines[2], "up again") || !strings.Contains(lines[3], "subscription was canceled") || !strings.Contains(lines[4], "up again") {
		t.Fatalf("one line per change of state, got %q", lines)
	}
}

// A node with no websocket (or a proxy that does not pass one) costs nothing
// but the speed-up: the scanner follows by polling at -poll, as before.
func TestNoWebsocketFollowsByPolling(t *testing.T) {
	headsBackoff = func(int) time.Duration { return 50 * time.Millisecond }
	t.Cleanup(func() { headsBackoff = rpcBackoff })
	node := newHeadsNode(t, 12)
	node.setRefuse(true)
	s, _ := followScanner(t, node, 100*time.Millisecond, 30*time.Second)
	waitFor(t, 10*time.Second, "caught up", func() bool { return node.read(12) })
	node.setTip(14)
	waitFor(t, 2*time.Second, "13 and 14 read by polling", func() bool { return node.read(14) })
	node.readsInOrder(t, 10, 14)
	if lines := subscriptionLines(s); len(lines) != 1 || !strings.Contains(lines[0], "block subscription unavailable") {
		t.Fatalf("log: %q", lines)
	}
}

// A subscription that stays open but stops delivering (the node gave up on
// it without a word) shows on the tip: two heights past the last announced
// and quiet for headsQuietAfter, it is dropped and made again.
func TestASubscriptionThatGoesQuietIsMadeAgain(t *testing.T) {
	headsBackoff = func(int) time.Duration { return 50 * time.Millisecond }
	headsQuietAfter = 300 * time.Millisecond
	t.Cleanup(func() { headsBackoff, headsQuietAfter = rpcBackoff, 30*time.Second })
	node := newHeadsNode(t, 12)
	s, _ := followScanner(t, node, 100*time.Millisecond, 100*time.Millisecond)
	waitFor(t, 10*time.Second, "caught up and subscribed", func() bool { return node.read(12) && s.heads.Up() })
	node.announce(12)
	// one height unannounced is a block in flight, not a quiet subscription
	node.setTip(13)
	waitFor(t, 2*time.Second, "13 read by the safety poll", func() bool { return node.read(13) })
	time.Sleep(500 * time.Millisecond)
	if node.subscriptions() != 1 {
		t.Fatal("subscription made again with only one height unannounced")
	}
	node.setTip(14)
	waitFor(t, 3*time.Second, "subscribed again", func() bool { return node.subscriptions() == 2 && s.heads.Up() })
	node.readsInOrder(t, 10, 14)
	if lines := subscriptionLines(s); len(lines) != 3 || !strings.Contains(lines[1], "no block header announced for") {
		t.Fatalf("log: %q", lines)
	}
}

// The websocket address is the RPC's own, with ws for http, wss for https,
// under its path, and a user and password moved into a header.
func TestHeadsURL(t *testing.T) {
	for _, c := range []struct{ rpc, ws, auth string }{
		{"http://127.0.0.1:26657", "ws://127.0.0.1:26657/websocket", ""},
		{"tcp://127.0.0.1:26657", "ws://127.0.0.1:26657/websocket", ""},
		{"https://rpc.example.com/", "wss://rpc.example.com/websocket", ""},
		{"https://rpc.example.com/celestia?x=1", "wss://rpc.example.com/celestia/websocket", ""},
		{"https://u:p@rpc.example.com", "wss://rpc.example.com/websocket", "Basic dTpw"},
	} {
		ws, hdr, err := headsURL(c.rpc)
		if err != nil || ws != c.ws || hdr.Get("Authorization") != c.auth {
			t.Errorf("%s: %q %q %v, want %q %q", c.rpc, ws, hdr.Get("Authorization"), err, c.ws, c.auth)
		}
	}
	for _, bad := range []string{"unix:///run/node.sock", "127.0.0.1:26657", "http://"} {
		if _, _, err := headsURL(bad); err == nil {
			t.Errorf("%s: no error", bad)
		}
	}
}

// A burst of announcements while the loop is busy is one wake-up, holding
// the newest height; a "poll now" (0) is never lost to a height.
func TestNotifyMergesAnnouncements(t *testing.T) {
	h := &heads{wake: make(chan int64, 1)}
	h.notify(5)
	h.notify(7)
	h.notify(6)
	if got := <-h.wake; got != 7 {
		t.Fatalf("pending %d, want the newest, 7", got)
	}
	h.notify(9)
	h.notify(0)
	h.notify(10)
	if got := <-h.wake; got != 0 {
		t.Fatalf("pending %d, want 0 (poll now)", got)
	}
	select {
	case got := <-h.wake:
		t.Fatalf("a second wake-up pending: %d", got)
	default:
	}
}
