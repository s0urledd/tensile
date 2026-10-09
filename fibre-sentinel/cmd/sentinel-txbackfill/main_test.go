package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	errorsmod "cosmossdk.io/errors"
	fibretypes "github.com/celestiaorg/celestia-app/v10/x/fibre/types"
	abci "github.com/cometbft/cometbft/abci/types"
	cmtjson "github.com/cometbft/cometbft/libs/json"
	cmtversion "github.com/cometbft/cometbft/proto/tendermint/version"
	coretypes "github.com/cometbft/cometbft/rpc/core/types"
	cmttypes "github.com/cometbft/cometbft/types"
	codectypes "github.com/cosmos/cosmos-sdk/codec/types"
	sdk "github.com/cosmos/cosmos-sdk/types"
	sdkerrors "github.com/cosmos/cosmos-sdk/types/errors"
	cosmostx "github.com/cosmos/cosmos-sdk/types/tx"
	banktypes "github.com/cosmos/cosmos-sdk/x/bank/types"
	"github.com/plsgiveup/fibre/fibre-sentinel/internal/failedtx"
	"github.com/plsgiveup/fibre/fibre-sentinel/internal/scan"
	"github.com/plsgiveup/fibre/fibre-sentinel/internal/txcost"
)

// The chain the tests stage: heights 100 to 160 of chain test-1, a few of
// them holding transactions.

const chainID = "test-1"

var (
	depositURL = sdk.MsgTypeURL(&fibretypes.MsgDepositToEscrow{})
	sendURL    = sdk.MsgTypeURL(&banktypes.MsgSend{})
)

type marshaler interface{ Marshal() ([]byte, error) }

// rawTx is an SDK tx carrying msgs, each an Any of its type URL.
func rawTx(t *testing.T, url string, m marshaler) []byte {
	t.Helper()
	v, err := m.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	body := cosmostx.TxBody{Messages: []*codectypes.Any{{TypeUrl: url, Value: v}}}
	bb, err := body.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	raw := cosmostx.TxRaw{BodyBytes: bb, AuthInfoBytes: []byte{}, Signatures: [][]byte{{1}}}
	out, err := raw.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// feeEvents are the ante handler's fee event; actionEvent the message
// event the SDK gives each top-level message of a successful tx.
func feeEvents(fee string) []abci.Event {
	return []abci.Event{{Type: sdk.EventTypeTx, Attributes: []abci.EventAttribute{{Key: sdk.AttributeKeyFee, Value: fee}, {Key: sdk.AttributeKeyFeePayer, Value: "celestia1payer"}}}}
}

func actionEvent(url string) abci.Event {
	return abci.Event{Type: sdk.EventTypeMessage, Attributes: []abci.EventAttribute{{Key: sdk.AttributeKeyAction, Value: url}, {Key: "msg_index", Value: "0"}}}
}

type fakeTx struct {
	raw []byte
	res *abci.ExecTxResult
}

// fakeChain is the transactions at each height; every other height is an
// empty block.
type fakeChain map[int64][]fakeTx

func blockTime(h int64) time.Time {
	return time.Date(2026, 9, 25, 0, 0, 0, 123456789, time.UTC).Add(time.Duration(h) * 6 * time.Second)
}

func testChain(t *testing.T) fakeChain {
	t.Helper()
	deposit := rawTx(t, depositURL, &fibretypes.MsgDepositToEscrow{Signer: "celestia1owner", Amount: sdk.NewInt64Coin("utia", 1_000_000)})
	other := rawTx(t, depositURL, &fibretypes.MsgDepositToEscrow{Signer: "celestia1other", Amount: sdk.NewInt64Coin("utia", 7)})
	send := rawTx(t, sendURL, &banktypes.MsgSend{FromAddress: "celestia1owner", ToAddress: "celestia1other", Amount: sdk.NewCoins(sdk.NewInt64Coin("utia", 5))})
	ok := func(url string, used int64) *abci.ExecTxResult {
		return &abci.ExecTxResult{GasWanted: 200000, GasUsed: used, Events: append(feeEvents("100utia"), actionEvent(url))}
	}
	failed := func(used uint64) *abci.ExecTxResult {
		err := errorsmod.Wrapf(sdkerrors.ErrInsufficientFunds, "failed to execute message; message index: %d", 0)
		return sdkerrors.ResponseExecTxResultWithEvents(err, 200000, used, feeEvents("2000utia"), false)
	}
	return fakeChain{
		101: {{send, ok(sendURL, 50000)}, {deposit, ok(depositURL, 60000)}}, // a cost line at index 1
		104: {{send, ok(sendURL, 50001)}},                                   // nothing, the block unread
		105: {{deposit, failed(91000)}},                                     // a failed line
		120: {{send, failed(40000)}},                                        // the block read, nothing
		130: {{other, ok(depositURL, 61000)}, {deposit, failed(92000)}},     // a cost line and a failed one
		145: {{deposit, ok(depositURL, 62000)}},                             // the local node's
		150: {{other, failed(93000)}},                                       // the local node's
		157: {{deposit, failed(94000)}},                                     // above -failed-to: none
		158: {{other, ok(depositURL, 63000)}},                               // a cost line
	}
}

// scanBlock is height h as the scanner reads it.
func (c fakeChain) scanBlock(h int64) (*scan.Block, *scan.BlockResults) {
	blk := &scan.Block{Height: h, Time: blockTime(h), AppVersion: 10}
	res := &scan.BlockResults{Height: h}
	for _, tx := range c[h] {
		blk.Txs = append(blk.Txs, tx.raw)
		res.TxCodes = append(res.TxCodes, tx.res.Code)
		res.TxEvents = append(res.TxEvents, tx.res.Events)
		res.TxCodespace = append(res.TxCodespace, tx.res.Codespace)
		res.TxLog = append(res.TxLog, tx.res.Log)
		res.TxGasWanted = append(res.TxGasWanted, tx.res.GasWanted)
		res.TxGasUsed = append(res.TxGasUsed, tx.res.GasUsed)
	}
	return blk, res
}

// expected is what the scanner's functions write for heights from..to,
// each line ending in a newline, recorded_at masked (masked).
func (c fakeChain) expected(t *testing.T, from, failedTo, costsTo int64) (failed, costs string) {
	t.Helper()
	for h := from; h <= max(failedTo, costsTo); h++ {
		blk, res := c.scanBlock(h)
		if h <= failedTo {
			recs, _ := scan.FailedTxRecords(blk, res, time.Time{}, nil)
			for _, r := range recs {
				b, _ := json.Marshal(r)
				failed += string(b) + "\n"
			}
		}
		if h <= costsTo {
			recs, _ := scan.TxCostRecords(blk, res, time.Time{}, nil)
			for _, r := range recs {
				b, _ := json.Marshal(r)
				costs += string(b) + "\n"
			}
		}
	}
	return masked(failed), masked(costs)
}

var stamp = regexp.MustCompile(`"recorded_at":"[^"]*"`)

func masked(s string) string { return stamp.ReplaceAllString(s, `"recorded_at":"T"`) }

func statusJSON(base, tip int64) string {
	return fmt.Sprintf(`{"node_info":{"network":%q,"protocol_version":{"p2p":"8","block":"11","app":"10"},"id":"","listen_addr":"","version":"","channels":"","moniker":"fake","other":{"tx_index":"on","rpc_address":""}},"sync_info":{"latest_block_hash":"","latest_app_hash":"","latest_block_height":"%d","latest_block_time":"2026-09-24T18:00:00Z","earliest_block_hash":"","earliest_app_hash":"","earliest_block_height":"%d","earliest_block_time":"2026-09-21T00:00:00Z","catching_up":false},"validator_info":{"address":"","pub_key":null,"voting_power":"0"}}`, chainID, tip, base)
}

// fakeNode serves the chain's heights base..tip over JSON-RPC, as CometBFT
// does, each answer after delay; fail, when it gives a message, makes the
// call an RPC error. It counts its calls per method and height, and the
// most it was asked at once.
type fakeNode struct {
	srv       *httptest.Server
	base, tip int64
	mu        sync.Mutex
	calls     map[string]map[int64]int
	now, most int
	fail      func(method string, h int64) string
}

func newFakeNode(t *testing.T, c fakeChain, base, tip int64) *fakeNode {
	t.Helper()
	n := &fakeNode{base: base, tip: tip, calls: map[string]map[int64]int{}}
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
		var h int64
		if s, ok := req.Params["height"].(string); ok {
			fmt.Sscan(s, &h)
		}
		n.mu.Lock()
		if n.calls[req.Method] == nil {
			n.calls[req.Method] = map[int64]int{}
		}
		n.calls[req.Method][h]++
		n.now++
		n.most = max(n.most, n.now)
		fail := n.fail
		n.mu.Unlock()
		defer func() {
			n.mu.Lock()
			n.now--
			n.mu.Unlock()
		}()
		time.Sleep(time.Duration(h%7) * time.Millisecond) // answers out of turn
		w.Header().Set("Content-Type", "application/json")
		reply := func(raw []byte) { fmt.Fprintf(w, `{"jsonrpc":"2.0","id":%s,"result":%s}`, req.ID, raw) }
		rpcErr := ""
		if fail != nil {
			rpcErr = fail(req.Method, h)
		}
		if rpcErr == "" && req.Method != "status" && (h < n.base || h > n.tip) {
			rpcErr = fmt.Sprintf("height %d is not available, lowest height is %d", h, n.base)
		}
		if rpcErr != "" {
			data, _ := json.Marshal(rpcErr)
			fmt.Fprintf(w, `{"jsonrpc":"2.0","id":%s,"error":{"code":-32603,"message":"Internal error","data":%s}}`, req.ID, data)
			return
		}
		var result any
		switch req.Method {
		case "status":
			reply([]byte(statusJSON(n.base, n.tip)))
			return
		case "block":
			var txs cmttypes.Txs
			for _, tx := range c[h] {
				txs = append(txs, tx.raw)
			}
			result = coretypes.ResultBlock{Block: &cmttypes.Block{
				Header: cmttypes.Header{Version: cmtversion.Consensus{Block: 11, App: 10}, ChainID: chainID, Height: h, Time: blockTime(h)},
				Data:   cmttypes.Data{Txs: txs},
			}}
		case "block_results":
			var results []*abci.ExecTxResult
			for _, tx := range c[h] {
				results = append(results, tx.res)
			}
			result = coretypes.ResultBlockResults{Height: h, TxsResults: results}
		default:
			http.Error(w, "unexpected "+req.Method, 400)
			return
		}
		raw, err := cmtjson.Marshal(result)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		reply(raw)
	}))
	t.Cleanup(n.srv.Close)
	return n
}

func (n *fakeNode) setFail(fail func(method string, h int64) string) {
	n.mu.Lock()
	n.fail = fail
	n.mu.Unlock()
}

// asked is the heights the node was asked method for.
func (n *fakeNode) asked(method string) map[int64]int {
	n.mu.Lock()
	defer n.mu.Unlock()
	out := map[int64]int{}
	for h, k := range n.calls[method] {
		out[h] = k
	}
	return out
}

// dataDir is an observer data directory of test-1 whose record files hold
// lines (name to lines, each with its newline).
func dataDir(t *testing.T, files map[string]string) string {
	t.Helper()
	dir := t.TempDir()
	files["state.json"] = `{"schema_version":1,"chain_id":"test-1","last_scanned_height":600}`
	for name, body := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

// stageOnce runs one staging of 100..160 (failed transactions to 155) over
// a local node holding 140..200 and two archives, and returns its error and
// what it printed.
func stageOnce(t *testing.T, cfg stageConfig, local *fakeNode, archives ...*fakeNode) (string, error) {
	t.Helper()
	var out bytes.Buffer
	cfg.Out = &out
	s, err := openStaging(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()
	var urls []string
	for _, a := range archives {
		urls = append(urls, a.srv.URL)
	}
	f, err := dialFetcher(context.Background(), chainID, local.srv.URL, urls, 2, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	f.sleep = func(context.Context, time.Duration) error { return nil }
	err = s.run(context.Background(), f)
	return out.String(), err
}

func readFile(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func config(t *testing.T, data string) stageConfig {
	return stageConfig{From: 100, FailedTo: 155, CostsTo: 160, OutDir: t.TempDir(), DataDir: data, Every: 20}
}

// A staging writes the scanner's own lines for every height, in height and
// tx index order whatever order the answers came in: the local node asked
// for the heights it holds, the archives for the rest, in turn, none asked
// more than -concurrency things at once, and a block read only when its
// results say a line can come from it. Run again, a finished staging
// changes nothing.
func TestStageWritesTheScannersLinesInOrder(t *testing.T) {
	c := testChain(t)
	local, a, b := newFakeNode(t, c, 140, 200), newFakeNode(t, c, 1, 200), newFakeNode(t, c, 1, 200)
	cfg := config(t, dataDir(t, map[string]string{}))
	out, err := stageOnce(t, cfg, local, a, b)
	if err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
	wantFailed, wantCosts := c.expected(t, 100, 155, 160)
	failed, costs := readFile(t, filepath.Join(cfg.OutDir, failedtx.FileName)), readFile(t, filepath.Join(cfg.OutDir, txcost.FileName))
	if masked(failed) != wantFailed || masked(costs) != wantCosts {
		t.Fatalf("staged:\n%s%s\nwant:\n%s%s", failed, costs, wantFailed, wantCosts)
	}
	if strings.Count(wantFailed, "\n") != 3 || strings.Count(wantCosts, "\n") != 4 || strings.Contains(failed, `"recorded_at":"0001`) {
		t.Fatalf("the fixture's lines: %d failed, %d costs:\n%s%s", strings.Count(wantFailed, "\n"), strings.Count(wantCosts, "\n"), failed, costs)
	}
	for h := int64(100); h <= 160; h++ {
		l, ar := local.asked("block_results")[h], a.asked("block_results")[h]+b.asked("block_results")[h]
		if h >= 140 && (l != 1 || ar != 0) || h < 140 && (l != 0 || ar != 1) {
			t.Errorf("h=%d: block_results asked of the local node %d time(s), of the archives %d", h, l, ar)
		}
		blocks := local.asked("block")[h] + a.asked("block")[h] + b.asked("block")[h]
		if want := map[int64]bool{101: true, 105: true, 120: true, 130: true, 145: true, 150: true, 157: true, 158: true}[h]; (blocks == 1) != want || blocks > 1 {
			t.Errorf("h=%d: the block read %d time(s)", h, blocks)
		}
	}
	if len(a.asked("block_results")) == 0 || len(b.asked("block_results")) == 0 {
		t.Error("the archives were not both asked")
	}
	for name, n := range map[string]*fakeNode{"local": local, "a": a, "b": b} {
		if n.most > 2 {
			t.Errorf("%s was asked %d things at once, -concurrency 2", name, n.most)
		}
	}
	p, err := loadProgress(cfg.OutDir)
	if err != nil || !p.complete() || p.Failed.Lines != 3 || p.Costs.Lines != 4 || p.Failed.Bytes != int64(len(failed)) || p.Next != 161 {
		t.Fatalf("progress %+v %v", p, err)
	}
	if !strings.Contains(out, "h=119: 20 of 61 heights staged") || !strings.Contains(out, "staging complete") {
		t.Errorf("progress lines:\n%s", out)
	}

	out, err = stageOnce(t, cfg, local, a, b)
	if err != nil || !strings.Contains(out, "nothing to do") {
		t.Fatalf("again: %v\n%s", err, out)
	}
	if readFile(t, filepath.Join(cfg.OutDir, failedtx.FileName)) != failed || readFile(t, filepath.Join(cfg.OutDir, txcost.FileName)) != costs {
		t.Fatal("a finished staging run again changed its files")
	}
}

// A key the live files hold is left out of the staging, and counted.
func TestStageLeavesOutTheLiveFilesKeys(t *testing.T) {
	c := testChain(t)
	local, a := newFakeNode(t, c, 140, 200), newFakeNode(t, c, 1, 200)
	cfg := config(t, dataDir(t, map[string]string{
		failedtx.FileName: `{"dedupe_key":"h105:0","height":105}` + "\n" + `{"dedupe_key":"h900:0","height":900}` + "\n",
		txcost.FileName:   `{"dedupe_key":"h101:1","height":101}` + "\n" + `{"dedupe_key":"h158:0","height":158}` + "\n" + `{"dedupe_key":"h901:0"`, // still being written
	}))
	if out, err := stageOnce(t, cfg, local, a); err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
	failed, costs := readFile(t, filepath.Join(cfg.OutDir, failedtx.FileName)), readFile(t, filepath.Join(cfg.OutDir, txcost.FileName))
	for _, k := range []string{"h105:0", "h101:1", "h158:0"} {
		if strings.Contains(failed+costs, `"dedupe_key":"`+k+`"`) {
			t.Errorf("%s, on record already, was staged", k)
		}
	}
	if p, _ := loadProgress(cfg.OutDir); p.Live != 3 || p.Failed.Lines != 2 || p.Costs.Lines != 2 {
		t.Fatalf("progress %+v", p)
	}
}

// A height no endpoint serves stops the run where the lines before it end;
// the same command run again goes on from it, past any line written after
// the last save, and ends with the files a run that never stopped writes.
func TestStageGoesOnFromWhereItStopped(t *testing.T) {
	c := testChain(t)
	local, a, b := newFakeNode(t, c, 140, 200), newFakeNode(t, c, 1, 200), newFakeNode(t, c, 1, 200)
	broken := func(method string, h int64) string {
		if h == 125 {
			return "could not find results for height #125"
		}
		return ""
	}
	a.setFail(broken)
	b.setFail(broken)
	cfg := config(t, dataDir(t, map[string]string{}))
	out, err := stageOnce(t, cfg, local, a, b)
	if err == nil || !strings.Contains(err.Error(), "height 125") || !strings.Contains(err.Error(), "go on from 125") {
		t.Fatalf("a height no node serves: %v\n%s", err, out)
	}
	p, _ := loadProgress(cfg.OutDir)
	failedPath := filepath.Join(cfg.OutDir, failedtx.FileName)
	if p.Next != 125 || p.Failed.Lines != 1 || p.Costs.Lines != 1 || readFile(t, failedPath)[p.Failed.Bytes-1] != '\n' {
		t.Fatalf("stopped at %+v", p)
	}
	// lines of heights after the mark, as a crash between a write and the
	// next save leaves them
	f, err := os.OpenFile(failedPath, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	f.WriteString(`{"dedupe_key":"h130:1","height":130}` + "\n" + `{"dedupe_key":"h1`)
	f.Close()

	a.setFail(nil)
	b.setFail(nil)
	if out, err := stageOnce(t, cfg, local, a, b); err != nil {
		t.Fatalf("again: %v\n%s", err, out)
	}
	wantFailed, wantCosts := c.expected(t, 100, 155, 160)
	if masked(readFile(t, failedPath)) != wantFailed || masked(readFile(t, filepath.Join(cfg.OutDir, txcost.FileName))) != wantCosts {
		t.Fatalf("after going on:\n%s%s\nwant:\n%s%s", readFile(t, failedPath), readFile(t, filepath.Join(cfg.OutDir, txcost.FileName)), wantFailed, wantCosts)
	}
	// Other ranges are another staging.
	other := cfg
	other.CostsTo = 170
	if _, err := openStaging(other); err == nil || !strings.Contains(err.Error(), "another -out-dir") {
		t.Fatalf("a staging opened with other ranges: %v", err)
	}
}

// The local node is asked first for the heights it held at the start and
// the archives after it, in turn; below its oldest block, and above its
// newest, the archives alone, starting from the next one each time.
func TestCandidates(t *testing.T) {
	local, a, b, c := &endpoint{url: "local"}, &endpoint{url: "a"}, &endpoint{url: "b"}, &endpoint{url: "c"}
	f := &fetcher{local: local, base: 140, tip: 200, archives: []*endpoint{a, b, c}}
	names := func(eps []*endpoint) string {
		var s []string
		for _, e := range eps {
			s = append(s, e.url)
		}
		return strings.Join(s, ",")
	}
	var starts []string
	for _, h := range []int64{139, 1, 201} {
		got := names(f.candidates(h))
		if len(got) != len("a,b,c") || strings.Contains(got, "local") {
			t.Errorf("h=%d: %s", h, got)
		}
		starts = append(starts, got[:1])
	}
	if strings.Join(starts, "") != "bca" {
		t.Errorf("the archives were not taken in turn: %v", starts)
	}
	for _, h := range []int64{140, 170, 200} {
		if got := names(f.candidates(h)); !strings.HasPrefix(got, "local,") || len(got) != len("local,a,b,c") {
			t.Errorf("h=%d: %s", h, got)
		}
	}
	if got := names((&fetcher{local: local, base: 140, tip: 200}).candidates(100)); got != "" {
		t.Errorf("no archives, below the local node: %q", got)
	}
}

// A backoff doubles from a second to half a minute, and is at least 10 s
// after a 429.
func TestBackoff(t *testing.T) {
	plain := fmt.Errorf("post failed: connection refused")
	rationed := fmt.Errorf("error in json rpc client, with http response metadata: (Status: 429 Too Many Requests, Protocol HTTP/1.1)")
	for _, c := range []struct {
		a    int
		err  error
		want time.Duration
	}{{0, plain, time.Second}, {2, plain, 4 * time.Second}, {5, plain, 30 * time.Second}, {40, plain, 30 * time.Second},
		{0, rationed, 10 * time.Second}, {4, rationed, 16 * time.Second}} {
		if got := backoff(c.a, c.err); got != c.want {
			t.Errorf("backoff(%d, %v) = %s, want %s", c.a, c.err, got, c.want)
		}
	}
}
