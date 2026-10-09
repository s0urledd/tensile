package scan

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	abci "github.com/cometbft/cometbft/abci/types"
	cmted25519 "github.com/cometbft/cometbft/crypto/ed25519"
	cmttypes "github.com/cometbft/cometbft/types"
	"github.com/cosmos/cosmos-sdk/crypto/keys/secp256k1"
	sdk "github.com/cosmos/cosmos-sdk/types"
	banktypes "github.com/cosmos/cosmos-sdk/x/bank/types"
	"github.com/plsgiveup/fibre/fibre-sentinel/internal/failedtx"
	"github.com/plsgiveup/fibre/fibre-sentinel/internal/txcost"
)

// What sentinel-txbackfill relies on in this package: steps 4 and 5 as
// pure functions of a block and its results, the rule that says from the
// results alone whether a block can give a line, the store over files whose
// lines are not in height order, and the store appending a staged line as
// it is.

var recordedAtRe = regexp.MustCompile(`"recorded_at":"[^"]*"`)

func maskRecordedAt(b []byte) string {
	return recordedAtRe.ReplaceAllString(string(b), `"recorded_at":"T"`)
}

// marshalLines is recs as the store writes them, a line each, recorded_at
// masked.
func marshalLines[T any](t *testing.T, recs []T) string {
	t.Helper()
	var out []byte
	for _, r := range recs {
		b, err := json.Marshal(r)
		if err != nil {
			t.Fatal(err)
		}
		out = append(append(out, b...), '\n')
	}
	return maskRecordedAt(out)
}

// FailedTxRecords and TxCostRecords over a block, read from the node as
// processBlock read it, give the bytes steps 4 and 5 wrote for it, line for
// line, but for recorded_at: the lines a backfill writes are the scanner's.
func TestTheStepFunctionsGiveWhatProcessBlockWrote(t *testing.T) {
	key := secp256k1.GenPrivKey()
	var txs [][]byte
	var results []*abci.ExecTxResult
	for _, c := range failedCases(t, key) {
		txs, results = append(txs, c.raw), append(results, c.res)
	}
	for _, c := range costCases(t, key) {
		txs, results = append(txs, c.raw), append(results, c.res)
	}
	dir := t.TempDir()
	s := failScanner(t, dir, txs, results, cmttypes.NewValidator(cmted25519.GenPrivKey().PubKey(), 10))
	ctx := context.Background()
	s.processBlock(ctx, 100)
	blk, err := s.chain.Block(ctx, 100)
	if err != nil {
		t.Fatal(err)
	}
	res, err := s.chain.BlockResults(ctx, 100)
	if err != nil {
		t.Fatal(err)
	}
	if !ResultsNeedBlock(res) {
		t.Fatal("the block's results say no line can come from it")
	}
	at := time.Date(2026, 9, 25, 1, 2, 3, 0, time.UTC)
	failed, fp := FailedTxRecords(blk, res, at, nil)
	costs, cp := TxCostRecords(blk, res, at, nil)
	for name, c := range map[string]struct {
		got  string
		recs int
	}{failedtx.FileName: {marshalLines(t, failed), len(failed)}, txcost.FileName: {marshalLines(t, costs), len(costs)}} {
		b, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			t.Fatal(err)
		}
		if c.recs < 5 || c.got != maskRecordedAt(b) {
			t.Errorf("%s: the function gives %d line(s):\n%s\nprocessBlock wrote:\n%s", name, c.recs, c.got, b)
		}
	}
	for _, r := range failed {
		if !r.RecordedAt.Equal(at) {
			t.Fatalf("recorded_at %s, want %s", r.RecordedAt, at)
		}
	}
	if len(fp) != 0 || len(cp) != 0 {
		t.Errorf("problems in a block that has none: %v %v", fp, cp)
	}
	// The keys a skip names are passed over; the rest are the same lines.
	skip := func(k string) bool { return k == failed[0].DedupeKey || k == costs[0].DedupeKey }
	f2, _ := FailedTxRecords(blk, res, at, skip)
	c2, _ := TxCostRecords(blk, res, at, skip)
	if !reflect.DeepEqual(f2, failed[1:]) || !reflect.DeepEqual(c2, costs[1:]) {
		t.Fatalf("with a skip: %d and %d lines, want %d and %d", len(f2), len(c2), len(failed)-1, len(costs)-1)
	}
}

// sdkActions is the "message" event the SDK gives each top-level message
// of a successful tx (baseapp createEvents): its type URL as "action".
func sdkActions(raw []byte) []abci.Event {
	var out []abci.Event
	for _, m := range decodeTxMsgs(raw) {
		out = append(out, abci.Event{Type: sdk.EventTypeMessage, Attributes: []abci.EventAttribute{
			{Key: sdk.AttributeKeyAction, Value: m.typeURL}, {Key: "msg_index", Value: strconv.Itoa(m.index)}}})
	}
	return out
}

// Every transaction of the failure and cost cases, alone in a block, with
// the results the SDK gives it: whenever steps 4 or 5 write a line for the
// block, ResultsNeedBlock says the block is needed. A block of successes
// none of whose top-level messages is Fibre's or a MsgExec, and an empty
// one, are not.
func TestResultsNeedBlockWheneverALineCanCome(t *testing.T) {
	key := secp256k1.GenPrivKey()
	type tx struct {
		name string
		raw  []byte
		res  *abci.ExecTxResult
	}
	var all []tx
	for _, c := range failedCases(t, key) {
		all = append(all, tx{c.name, c.raw, c.res})
	}
	for _, c := range costCases(t, key) {
		all = append(all, tx{c.name, c.raw, c.res})
	}
	lines := 0
	for _, c := range all {
		evs := append([]abci.Event(nil), c.res.Events...)
		if c.res.Code == 0 {
			evs = append(evs, sdkActions(c.raw)...)
		}
		blk := &Block{Height: 100, Time: failTime, Txs: []cmttypes.Tx{c.raw}, AppVersion: FibreAppVersion}
		res := &BlockResults{Height: 100, TxCodes: []uint32{c.res.Code}, TxEvents: [][]abci.Event{evs}, TxCodespace: []string{c.res.Codespace},
			TxLog: []string{c.res.Log}, TxGasWanted: []int64{c.res.GasWanted}, TxGasUsed: []int64{c.res.GasUsed}}
		f, _ := FailedTxRecords(blk, res, failTime, nil)
		k, _ := TxCostRecords(blk, res, failTime, nil)
		if len(f)+len(k) > 0 {
			lines++
			if !ResultsNeedBlock(res) {
				t.Errorf("%s: %d line(s), and the results say the block is not needed", c.name, len(f)+len(k))
			}
		}
	}
	if lines < 10 {
		t.Fatalf("only %d of the cases give a line: the proof needs them", lines)
	}

	send := rawTx(t, sendMsg{&banktypes.MsgSend{FromAddress: "celestia1a", ToAddress: "celestia1b", Amount: sdk.NewCoins(sdk.NewInt64Coin("utia", 1))}})
	ok := &BlockResults{Height: 100, TxCodes: []uint32{0, 0}, TxEvents: [][]abci.Event{
		append(anteEvents("100utia"), sdkActions(send)...),
		append(anteEvents("200utia"), abci.Event{Type: sdk.EventTypeMessage, Attributes: []abci.EventAttribute{{Key: sdk.AttributeKeyAction, Value: "/celestia.blob.v1.MsgPayForBlobs"}}}),
	}}
	if ResultsNeedBlock(ok) || ResultsNeedBlock(&BlockResults{Height: 100}) {
		t.Fatal("a block of transfers and blob payments, or an empty one, is needed")
	}
}

// mochaAnswer is the result of a saved JSON-RPC answer in testdata.
func mochaAnswer(t *testing.T, name string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatal(err)
	}
	var env struct {
		Result json.RawMessage `json:"result"`
	}
	if err := json.Unmarshal(b, &env); err != nil || len(env.Result) == 0 {
		t.Fatalf("%s: %v", name, err)
	}
	return string(env.Result)
}

// The rule on Mocha's own answers (testdata/README.md): a block with a
// successful MsgDepositToEscrow and a blob payment is needed for the
// deposit alone, whose results carry its type URL as the message's action,
// and the block read through the scanner's client gives the deposit's cost
// line as the chain recorded it; the block whose first tx failed is needed
// for its failure.
func TestResultsNeedBlockOnAMochaBlock(t *testing.T) {
	const h = 1107336
	block, results := mochaAnswer(t, "block_1107336.json"), mochaAnswer(t, "block_results_1107336.json")
	node := newRPCNode(t, func(method string, height int64) (any, string) {
		switch {
		case method == "block" && height == h:
			return block, ""
		case method == "block_results" && height == h:
			return results, ""
		}
		return nil, "unexpected " + method
	})
	c, err := NewChain(node.srv.URL, 5*time.Second, NewLogger(10))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	blk, err := c.Block(ctx, h)
	if err != nil {
		t.Fatal(err)
	}
	res, err := c.BlockResults(ctx, h)
	if err != nil {
		t.Fatal(err)
	}
	if len(blk.Txs) != 2 || len(res.TxCodes) != 2 || res.TxCodes[0] != 0 || res.TxCodes[1] != 0 || blk.AppVersion != FibreAppVersion {
		t.Fatalf("the fixture is not two successes at app version 10: %d txs, codes %v, app %d", len(blk.Txs), res.TxCodes, blk.AppVersion)
	}
	if !ResultsNeedBlock(res) || !successMayCarry(res.TxEvents[0]) || successMayCarry(res.TxEvents[1]) {
		t.Fatalf("the deposit's results say %v, the blob payment's %v", successMayCarry(res.TxEvents[0]), successMayCarry(res.TxEvents[1]))
	}
	at := time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)
	costs, problems := TxCostRecords(blk, res, at, nil)
	failed, _ := FailedTxRecords(blk, res, at, nil)
	payer := "celestia1duzcz4082hm8765pmaxe6y79h8gsaxufxzs277"
	want := []txcost.Record{{SchemaVersion: txcost.SchemaVersion, DedupeKey: "h1107336:0", Height: h, TxIndex: 0,
		Time: time.Date(2026, 9, 25, 16, 10, 53, 714766302, time.UTC), TxHash: "d7a03499d0f0b5a09f4312afff0a44790cba75684dadfdd2ac45608d8724ca25",
		GasWanted: 81370, GasUsed: 74054, Fee: "326utia", FeePayer: payer,
		Messages: []failedtx.Msg{{Index: 0, TypeURL: msgDepositTypeURL, Signer: payer}}, RecordedAt: at}}
	if !reflect.DeepEqual(costs, want) || len(problems) != 0 || len(failed) != 0 {
		g, _ := json.Marshal(costs)
		w, _ := json.Marshal(want)
		t.Fatalf("cost lines:\n got %s\nwant %s\nfailed %d, problems %v", g, w, len(failed), problems)
	}

	withFailure := mochaAnswer(t, "block_results_failed.json")
	node2 := newRPCNode(t, func(method string, height int64) (any, string) {
		if method == "block_results" {
			return withFailure, ""
		}
		return nil, "unexpected " + method
	})
	c2, err := NewChain(node2.srv.URL, 5*time.Second, NewLogger(10))
	if err != nil {
		t.Fatal(err)
	}
	r2, err := c2.BlockResults(ctx, 1498161)
	if err != nil {
		t.Fatal(err)
	}
	if !ResultsNeedBlock(r2) || successMayCarry(r2.TxEvents[1]) {
		t.Fatal("the block with a failed tx is not needed, or its blob payment is read as Fibre's")
	}
}

// failedLine and costLine are a failed_txs.jsonl and a tx_costs.jsonl line
// at (h, i) as the scanner writes them, and as sentinel-txbackfill stages
// them.
func failedLine(t *testing.T, h int64, i int) (failedtx.Record, []byte) {
	t.Helper()
	at := time.Date(2026, 9, 25, 0, 0, 0, 0, time.UTC).Add(time.Duration(h) * time.Second)
	r := failedtx.Record{SchemaVersion: failedtx.SchemaVersion, DedupeKey: failedtx.Key(h, i), Height: h, Time: at, AppVersion: 10,
		TxHash: strings.Repeat("0a", 32), TxIndex: i, Code: 5, Codespace: "sdk", Log: "failed to execute message; message index: 0: x",
		GasWanted: 200000, GasUsed: 90000, AntePassed: true, Fee: "2000utia", RecordedAt: at.Add(time.Second),
		Messages: []failedtx.Msg{{Index: 0, TypeURL: msgDepositTypeURL, Signer: "celestia1x", Detail: &failedtx.MsgDetail{Publisher: "celestia1x", Amount: "5utia"}}}}
	b, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	return r, b
}

func costLine(t *testing.T, h int64, i int) (txcost.Record, []byte) {
	t.Helper()
	at := time.Date(2026, 9, 25, 0, 0, 0, 0, time.UTC).Add(time.Duration(h) * time.Second)
	r := txcost.Record{SchemaVersion: txcost.SchemaVersion, DedupeKey: txcost.Key(h, i), Height: h, TxIndex: i, Time: at,
		TxHash: strings.Repeat("0b", 32), GasWanted: 100000, GasUsed: 60000, Fee: "300utia", FeePayer: "celestia1x",
		Messages: []failedtx.Msg{{Index: 0, TypeURL: msgDepositTypeURL, Signer: "celestia1x"}}, RecordedAt: at.Add(time.Second)}
	b, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	return r, b
}

// A store reopened over failed_txs.jsonl and tx_costs.jsonl whose older
// lines were appended after newer ones (a backfill's merge) opens, loses
// none of them, holds the keys above its checkpoint alone, whatever their
// place in the file, and still writes each key once and forgets it at the
// checkpoint. A torn line after the backfilled ones is cut as any is.
func TestAStoreOpensOverLinesAppendedBelowNewerOnes(t *testing.T) {
	dir := t.TempDir()
	st, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.SaveState(PersistState{ChainID: "test-1", LastScannedHeight: 1000}); err != nil {
		t.Fatal(err)
	}
	var failed, costs []byte
	for _, p := range []struct {
		h int64
		i int
	}{{1001, 0}, {1500, 2}, {500, 1}, {20, 0}, {1200, 0}, {19, 3}} {
		_, f := failedLine(t, p.h, p.i)
		_, c := costLine(t, p.h, p.i)
		failed = append(append(failed, f...), '\n')
		costs = append(append(costs, c...), '\n')
	}
	st.Close()
	failedPath, costsPath := filepath.Join(dir, failedtx.FileName), filepath.Join(dir, txcost.FileName)
	torn := []byte(`{"schema_version":1,"dedupe_key":"h18:0","hei`)
	if err := os.WriteFile(failedPath, append(append([]byte(nil), failed...), torn...), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(costsPath, costs, 0o644); err != nil {
		t.Fatal(err)
	}

	st, err = OpenStore(dir)
	if err != nil {
		t.Fatalf("a store over lines out of height order: %v", err)
	}
	defer func() { st.Close() }()
	above := []string{"h1001:0", "h1500:2", "h1200:0"}
	for _, k := range above {
		if !st.FailedTxSeen(k) || !st.TxCostSeen(k) {
			t.Errorf("%s, above the checkpoint, is not held", k)
		}
	}
	for _, k := range []string{"h500:1", "h20:0", "h19:3", "h18:0"} {
		if st.FailedTxSeen(k) || st.TxCostSeen(k) {
			t.Errorf("%s, at or below the checkpoint, is held", k)
		}
	}
	if len(st.failSeen) != 3 || len(st.costSeen) != 3 {
		t.Fatalf("held %d and %d keys", len(st.failSeen), len(st.costSeen))
	}
	if got, _ := os.ReadFile(failedPath); string(got) != string(failed) {
		t.Fatalf("failed_txs.jsonl after the open:\n%s\nwant (the torn line cut, nothing else):\n%s", got, failed)
	}
	if got, _ := os.ReadFile(costsPath); string(got) != string(costs) {
		t.Fatal("tx_costs.jsonl changed at the open")
	}

	// A re-scan above the checkpoint writes nothing again; a new key once.
	for _, p := range []struct {
		h int64
		i int
	}{{1001, 0}, {1500, 2}, {1600, 0}, {1600, 0}} {
		f, _ := failedLine(t, p.h, p.i)
		c, _ := costLine(t, p.h, p.i)
		if err := st.AppendFailedTx(f); err != nil {
			t.Fatal(err)
		}
		if err := st.AppendTxCost(c); err != nil {
			t.Fatal(err)
		}
	}
	if err := st.Sync(); err != nil {
		t.Fatal(err)
	}
	if n := wholeLines(t, failedPath); n != 7 {
		t.Fatalf("failed_txs.jsonl: %d lines, want 7", n)
	}
	if n := wholeLines(t, costsPath); n != 7 {
		t.Fatalf("tx_costs.jsonl: %d lines, want 7", n)
	}
	// The checkpoint prunes by height, not by place in the file.
	if err := st.SaveState(PersistState{ChainID: "test-1", LastScannedHeight: 1300}); err != nil {
		t.Fatal(err)
	}
	if len(st.failSeen) != 2 || !st.FailedTxSeen("h1500:2") || !st.FailedTxSeen("h1600:0") || len(st.costSeen) != 2 {
		t.Fatalf("after the checkpoint at 1300: %v %v", st.failSeen, st.costSeen)
	}
}

// AppendFailedTxLine and AppendTxCostLine append the line they are given,
// byte for byte, even where marshalling its record again would give other
// bytes (a string the chain gave with invalid UTF-8, which the first
// marshal wrote as �), skip a key held above the checkpoint as the
// marshalling appends do, and leave the caller's bytes as they were.
func TestAStagedLineIsAppendedAsItIs(t *testing.T) {
	dir := t.TempDir()
	st, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if err := st.SaveState(PersistState{ChainID: "test-1", LastScannedHeight: 1000}); err != nil {
		t.Fatal(err)
	}
	f, _ := failedLine(t, 30, 0)
	f.Messages[0].Detail.Host = "bad\xffhost"
	fl, err := json.Marshal(f)
	if err != nil {
		t.Fatal(err)
	}
	var back failedtx.Record
	if err := json.Unmarshal(fl, &back); err != nil {
		t.Fatal(err)
	}
	if again, _ := json.Marshal(back); string(again) == string(fl) {
		t.Fatal("the fixture's line marshals again to the same bytes: it proves nothing")
	}
	c, cl := costLine(t, 31, 2)
	buf := append(append([]byte(nil), fl...), "|tail"...)
	if err := st.AppendFailedTxLine(back, buf[:len(fl)]); err != nil {
		t.Fatal(err)
	}
	if err := st.AppendTxCostLine(c, cl); err != nil {
		t.Fatal(err)
	}
	if string(buf[len(fl):]) != "|tail" {
		t.Fatalf("the caller's bytes after the line were written over: %q", buf[len(fl):])
	}
	// above the checkpoint and held: skipped
	hf, hl := failedLine(t, 1001, 0)
	if err := st.AppendFailedTx(hf); err != nil {
		t.Fatal(err)
	}
	if err := st.AppendFailedTxLine(hf, hl); err != nil {
		t.Fatal(err)
	}
	if err := st.Sync(); err != nil {
		t.Fatal(err)
	}
	if got, _ := os.ReadFile(filepath.Join(dir, failedtx.FileName)); string(got) != string(fl)+"\n"+string(hl)+"\n" {
		t.Fatalf("failed_txs.jsonl:\n%q", got)
	}
	if got, _ := os.ReadFile(filepath.Join(dir, txcost.FileName)); string(got) != string(cl)+"\n" {
		t.Fatalf("tx_costs.jsonl:\n%q", got)
	}
}
