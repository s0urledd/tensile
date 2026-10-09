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

	fibretypes "github.com/celestiaorg/celestia-app/v10/x/fibre/types"
	valaddrtypes "github.com/celestiaorg/celestia-app/v10/x/valaddr/types"
	"github.com/celestiaorg/go-square/v4/share"
	squaretx "github.com/celestiaorg/go-square/v4/tx"
	abci "github.com/cometbft/cometbft/abci/types"
	cmted25519 "github.com/cometbft/cometbft/crypto/ed25519"
	cmttypes "github.com/cometbft/cometbft/types"
	"github.com/cosmos/cosmos-sdk/crypto/keys/secp256k1"
	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/cosmos/cosmos-sdk/types/bech32"
	sdkerrors "github.com/cosmos/cosmos-sdk/types/errors"
	banktypes "github.com/cosmos/cosmos-sdk/x/bank/types"
	"github.com/plsgiveup/fibre/fibre-sentinel/internal/failedtx"
	"github.com/plsgiveup/fibre/fibre-sentinel/internal/txcost"
)

// costCase is one transaction of the detection block: its bytes, its
// result, and the line tx_costs.jsonl must hold for it (nil: none). The
// line's place (height, index, key, hash, time) is filled by the test that
// places the transaction.
type costCase struct {
	name string
	raw  []byte
	res  *abci.ExecTxResult
	want *txcost.Record
}

// costCases is every kind of transaction the cost record tells apart, as
// one block holds them: a success of each Fibre kind (the set-host also
// inside a MsgExec), with a zero fee and with no fee event at all, a
// transfer, the two wrappers whose inner tx carries a deposit, bytes that
// are no SDK tx, and a failed deposit.
func costCases(t *testing.T, key *secp256k1.PrivKey) []costCase {
	t.Helper()
	owner, err := bech32.ConvertAndEncode(accountHRP, key.PubKey().Address())
	if err != nil {
		t.Fatal(err)
	}
	other := "celestia1qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqq"
	pp, pp2 := testPromise(t, key, 1<<20), testPromise(t, key, 1<<19)
	op1, op2 := valoper(t, 'c'), valoper(t, 'd')
	send := sendMsg{&banktypes.MsgSend{FromAddress: owner, ToAddress: other, Amount: sdk.NewCoins(sdk.NewInt64Coin("utia", 5))}}
	deposit := depositMsg{&fibretypes.MsgDepositToEscrow{Signer: owner, Amount: sdk.NewInt64Coin("utia", 1_000_000)}}
	inner := rawTx(t, deposit)
	blob, err := share.NewV0Blob(share.MustNewV0Namespace([]byte("costs-t")), []byte("blob data"))
	if err != nil {
		t.Fatal(err)
	}
	blobTx, err := squaretx.MarshalBlobTx(inner, blob)
	if err != nil {
		t.Fatal(err)
	}
	wrapped, err := squaretx.MarshalIndexWrapper(inner, 4)
	if err != nil {
		t.Fatal(err)
	}
	ok := func(gasWanted, gasUsed uint64, events ...abci.Event) *abci.ExecTxResult {
		return resultOf(nil, gasWanted, gasUsed, events...)
	}
	return []costCase{
		{"[MsgPayForFibre]", rawTx(t, payMsg{&fibretypes.MsgPayForFibre{Signer: owner, PaymentPromise: pp}}), ok(900000, 800000, anteEvents("900utia")...),
			&txcost.Record{GasWanted: 900000, GasUsed: 800000, Fee: "900utia", FeePayer: "celestia1payer",
				Messages: []failedtx.Msg{{Index: 0, TypeURL: fibretypes.MsgPayForFibreTypeURL, Signer: owner}}}},
		{"[MsgDepositToEscrow], a zero fee", rawTx(t, depositMsg{&fibretypes.MsgDepositToEscrow{Signer: owner, Amount: sdk.NewInt64Coin("utia", 6_000_000)}}),
			ok(100000, 60000, anteEvents("")...),
			&txcost.Record{GasWanted: 100000, GasUsed: 60000, Fee: "", FeePayer: "celestia1payer",
				Messages: []failedtx.Msg{{Index: 0, TypeURL: msgDepositTypeURL, Signer: owner}}}},
		{"[MsgSend, MsgRequestWithdrawal]", rawTx(t, send, withdrawMsg{&fibretypes.MsgRequestWithdrawal{Signer: owner, Amount: sdk.NewInt64Coin("utia", 7)}}),
			ok(120000, 70000, anteEvents("150utia")...),
			&txcost.Record{GasWanted: 120000, GasUsed: 70000, Fee: "150utia", FeePayer: "celestia1payer",
				Messages: []failedtx.Msg{{Index: 0, TypeURL: send.typeURL()}, {Index: 1, TypeURL: msgWithdrawalTypeURL, Signer: owner}}}},
		{"[MsgSetFibreProviderInfo]", rawTx(t, hostMsg{&valaddrtypes.MsgSetFibreProviderInfo{Signer: op1, Host: "c.example:7980"}}),
			ok(100000, 61000, append(anteEvents("100utia"), regEvent(bech(t, "c"), "c.example:7980"))...),
			&txcost.Record{GasWanted: 100000, GasUsed: 61000, Fee: "100utia", FeePayer: "celestia1payer",
				Messages: []failedtx.Msg{{Index: 0, TypeURL: msgSetHostTypeURL, Signer: op1}}}},
		{"[MsgExec[MsgSetFibreProviderInfo]]", rawTx(t, execOf(t, hostMsg{&valaddrtypes.MsgSetFibreProviderInfo{Signer: op2, Host: "d.example:7980"}})),
			ok(140000, 90000, append(anteEvents("140utia"), regEvent(bech(t, "d"), "d.example:7980"))...),
			&txcost.Record{GasWanted: 140000, GasUsed: 90000, Fee: "140utia", FeePayer: "celestia1payer",
				Messages: []failedtx.Msg{{Index: 0, TypeURL: failedtx.ExecTypeURL, Inner: []failedtx.Msg{{Index: 0, TypeURL: msgSetHostTypeURL, Signer: op2}}}}}},
		{"[MsgPaymentPromiseTimeout], no fee event", rawTx(t, timeoutMsg{&fibretypes.MsgPaymentPromiseTimeout{Signer: other, PaymentPromise: pp2}}),
			ok(300000, 120000),
			&txcost.Record{GasWanted: 300000, GasUsed: 120000,
				Messages: []failedtx.Msg{{Index: 0, TypeURL: msgTimeoutTypeURL, Signer: other}}}},
		{"[MsgSend]", rawTx(t, send), ok(80000, 50000, anteEvents("80utia")...), nil},
		{"a BlobTx whose tx carries a deposit", blobTx, ok(100000, 90000, anteEvents("300utia")...), nil},
		{"an IndexWrapper whose tx carries a deposit", wrapped, ok(100000, 90000, anteEvents("300utia")...), nil},
		{"not an SDK tx", []byte("not an sdk tx at all"), ok(0, 0), nil},
		{"a failed deposit", rawTx(t, deposit), resultOf(inMsg(sdkerrors.ErrInsufficientFunds, 0), 200000, 91234, anteEvents("2000utia")...), nil},
	}
}

// placeCosts is the cost lines cases must give in a block at height 100.
func placeCosts(cases []costCase) []txcost.Record {
	var want []txcost.Record
	for i, c := range cases {
		if c.want == nil {
			continue
		}
		w := *c.want
		w.SchemaVersion = txcost.SchemaVersion
		w.DedupeKey = txcost.Key(100, i)
		w.Height, w.TxIndex, w.Time = 100, i, failTime
		w.TxHash = hexstr(cmttypes.Tx(c.raw).Hash())
		want = append(want, w)
	}
	return want
}

func casesBlock(cases []costCase) ([][]byte, []*abci.ExecTxResult) {
	var txs [][]byte
	var results []*abci.ExecTxResult
	for _, c := range cases {
		txs = append(txs, c.raw)
		results = append(results, c.res)
	}
	return txs, results
}

// readCosts decodes tx_costs.jsonl, every line whole.
func readCosts(t *testing.T, dir string) []txcost.Record {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(dir, txcost.FileName))
	if err != nil {
		t.Fatal(err)
	}
	var out []txcost.Record
	for i, line := range strings.Split(strings.TrimSuffix(string(b), "\n"), "\n") {
		var r txcost.Record
		if err := json.Unmarshal([]byte(line), &r); err != nil {
			t.Fatalf("line %d does not decode: %q", i+1, line)
		}
		if strings.Contains(line, `"detail"`) {
			t.Fatalf("line %d carries a message's detail: %s", i+1, line)
		}
		out = append(out, r)
	}
	return out
}

// Case 1: the ante handler's fee event on a block_results answer from Mocha
// (testdata/README.md), served as the node served it: its fee and the
// account it was taken from, for the failed tx and the successful one alike;
// anteFee still reads v1's values from it.
func TestAnteFeeEventReadsARealBlockResultsAnswer(t *testing.T) {
	b, err := os.ReadFile(filepath.Join("testdata", "block_results_failed.json"))
	if err != nil {
		t.Fatal(err)
	}
	var env struct {
		Result json.RawMessage `json:"result"`
	}
	if err := json.Unmarshal(b, &env); err != nil || len(env.Result) == 0 {
		t.Fatalf("fixture: %v", err)
	}
	var head struct {
		Height string `json:"height"`
	}
	if err := json.Unmarshal(env.Result, &head); err != nil {
		t.Fatal(err)
	}
	height, err := strconv.ParseInt(head.Height, 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	node := newRPCNode(t, func(method string, h int64) (any, string) {
		if method == "block_results" && h == height {
			return string(env.Result), ""
		}
		return nil, "unexpected " + method
	})
	c, err := NewChain(node.srv.URL, 5*time.Second, NewLogger(10))
	if err != nil {
		t.Fatal(err)
	}
	res, err := c.BlockResults(context.Background(), height)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.TxEvents) != 2 || res.TxCodes[0] == 0 || res.TxCodes[1] != 0 {
		t.Fatalf("the fixture is not a failed tx and a successful one: codes %v", res.TxCodes)
	}
	for i, w := range []struct{ fee, payer string }{
		{"213utia", "celestia1d2ktc37cme7ydk30ylzhamutcynhdvyewp3j8q"},
		{"238utia", "celestia1jd2pngzcphvdc4zpyjtxjm8u9cpq438vd4zaaf"},
	} {
		if found, fee, payer := anteFeeEvent(res.TxEvents[i]); !found || fee != w.fee || payer != w.payer {
			t.Errorf("tx %d: anteFeeEvent = %v %q %q, want %q %q", i, found, fee, payer, w.fee, w.payer)
		}
		if passed, fee := anteFee(res.TxEvents[i]); !passed || fee != w.fee {
			t.Errorf("tx %d: anteFee = %v %q, want %q", i, passed, fee, w.fee)
		}
	}
}

// anteFeeEvent takes the payer from the fee event itself, and none when that
// event has no payer, though another event names one.
func TestAnteFeeEventTakesThePayerOfTheFeeEvent(t *testing.T) {
	for _, c := range []struct {
		name       string
		evs        []abci.Event
		found      bool
		fee, payer string
	}{
		{"none", nil, false, "", ""},
		{"a zero fee", anteEvents(""), true, "", "celestia1payer"},
		{"a fee", anteEvents("2000utia"), true, "2000utia", "celestia1payer"},
		{"no payer on the fee event", []abci.Event{
			{Type: "tx", Attributes: []abci.EventAttribute{{Key: "fee", Value: "1utia"}}},
			{Type: "tx", Attributes: []abci.EventAttribute{{Key: "fee_payer", Value: "celestia1other"}}},
		}, true, "1utia", ""},
		{"a payer on another event type", []abci.Event{{Type: "transfer", Attributes: []abci.EventAttribute{{Key: "fee", Value: "1utia"}, {Key: "fee_payer", Value: "x"}}}}, false, "", ""},
		{"the first one", append(anteEvents("1utia"), abci.Event{Type: "tx", Attributes: []abci.EventAttribute{{Key: "fee", Value: "2utia"}, {Key: "fee_payer", Value: "celestia1second"}}}), true, "1utia", "celestia1payer"},
	} {
		if found, fee, payer := anteFeeEvent(c.evs); found != c.found || fee != c.fee || payer != c.payer {
			t.Errorf("%s: %v %q %q", c.name, found, fee, payer)
		}
	}
}

// Case 2: one block with every kind. Exactly the successful transactions
// that carried a Fibre message are on record, each with its gas, the fee
// and payer the ante handler named, and its messages with their signers,
// without what they asked for; the failed deposit is in failed_txs.jsonl
// alone.
func TestEverySuccessfulFibreTransactionOfABlockHasItsCost(t *testing.T) {
	key := secp256k1.GenPrivKey()
	cases := costCases(t, key)
	txs, results := casesBlock(cases)
	want := placeCosts(cases)
	dir := t.TempDir()
	s := failScanner(t, dir, txs, results, cmttypes.NewValidator(cmted25519.GenPrivKey().PubKey(), 10))
	s.processBlock(context.Background(), 100)
	got := readCosts(t, dir)
	if len(got) != len(want) {
		t.Fatalf("%d lines, want %d: %+v", len(got), len(want), got)
	}
	for i := range got {
		if got[i].RecordedAt.IsZero() {
			t.Errorf("line %d without recorded_at", i+1)
		}
		got[i].RecordedAt = time.Time{}
		if !reflect.DeepEqual(got[i], want[i]) {
			g, _ := json.Marshal(got[i])
			w, _ := json.Marshal(want[i])
			t.Errorf("line %d (%s):\n got %s\nwant %s", i+1, want[i].DedupeKey, g, w)
		}
	}
	failed := readFailed(t, dir)
	if len(failed) != 1 || failed[0].TxIndex != len(cases)-1 {
		t.Fatalf("failed_txs.jsonl: %+v", failed)
	}
	for _, r := range got {
		if r.TxIndex == failed[0].TxIndex {
			t.Fatal("the failed deposit has a cost line")
		}
	}
	// The scanner's own log names no tx cost: a line per tx would be one
	// per blob payment on mainnet.
	if logs := s.log.lines(); strings.Contains(logs, "tx cost") {
		t.Errorf("logged a tx cost:\n%s", logs)
	}
}

// Case 3: MayCarry is never the reason a line is missing: every tx whose
// messages carry a Fibre one, at either level, holds a Fibre type URL in its
// bytes.
func TestMayCarryNeverDropsALine(t *testing.T) {
	key := secp256k1.GenPrivKey()
	var raws [][]byte
	for _, c := range costCases(t, key) {
		raws = append(raws, c.raw)
	}
	for _, c := range failedCases(t, key) {
		raws = append(raws, c.raw)
	}
	carried := 0
	for i, raw := range raws {
		msgs, _, ok := failedTxMsgs(raw)
		if !ok || !failedtx.Carries(msgs) {
			continue
		}
		carried++
		if !failedtx.MayCarry(raw) {
			t.Errorf("tx %d carries a Fibre message and MayCarry is false: %+v", i, msgs)
		}
	}
	if carried < 10 {
		t.Fatalf("only %d of the txs carry a Fibre message: the proof needs them", carried)
	}
}

// Case 4, the proof that the cost record changes nothing else: the same
// blocks (case 2's, and a block of successes with and without every failed
// transaction of failed_test.go after it) scanned with step 5 and without.
// Every other record the scanner writes is the same bytes, but for the wall
// clock in recorded_at, and so is processBlock's count. Only the scan with
// the step has a tx_costs.jsonl.
func TestTxCostsChangeNoOtherRecord(t *testing.T) {
	t.Cleanup(func() { txCostsOn = true })
	key := secp256k1.GenPrivKey()
	owner, err := bech32.ConvertAndEncode(accountHRP, key.PubKey().Address())
	if err != nil {
		t.Fatal(err)
	}
	val := cmttypes.NewValidator(cmted25519.GenPrivKey().PubKey(), 10)
	caseTxs, caseRes := casesBlock(costCases(t, key))
	aTxs := [][]byte{
		rawTx(t, payMsg{&fibretypes.MsgPayForFibre{Signer: owner, PaymentPromise: testPromise(t, key, 1<<20)}}),
		rawTx(t, depositMsg{&fibretypes.MsgDepositToEscrow{Signer: owner, Amount: sdk.NewInt64Coin("utia", 6_000_000)}}),
		rawTx(t, hostMsg{&valaddrtypes.MsgSetFibreProviderInfo{Signer: valoper(t, 'a'), Host: "a.example:7980"}}),
	}
	aRes := []*abci.ExecTxResult{
		resultOf(nil, 900000, 800000, anteEvents("900utia")...),
		resultOf(nil, 100000, 60000, anteEvents("100utia")...),
		resultOf(nil, 100000, 60000, append(anteEvents("100utia"), regEvent(bech(t, "a"), "a.example:7980"))...),
	}
	bTxs := append([][]byte(nil), aTxs...)
	bRes := append([]*abci.ExecTxResult(nil), aRes...)
	for _, c := range failedCases(t, key) {
		if c.res.Code == 0 {
			continue
		}
		bTxs = append(bTxs, c.raw)
		bRes = append(bRes, c.res)
	}

	stamp := regexp.MustCompile(`"recorded_at":"[^"]*"`)
	read := func(dir, name string) ([]byte, bool) {
		t.Helper()
		b, err := os.ReadFile(filepath.Join(dir, name))
		if os.IsNotExist(err) {
			return nil, false
		}
		if err != nil {
			t.Fatal(err)
		}
		return stamp.ReplaceAll(b, []byte(`"recorded_at":"T"`)), true
	}
	for _, blk := range []struct {
		name string
		txs  [][]byte
		res  []*abci.ExecTxResult
	}{
		{"case 2's block", caseTxs, caseRes},
		{"a block of successes", aTxs, aRes},
		{"the same block with every failure after it", bTxs, bRes},
	} {
		run := func(on bool) (string, int) {
			t.Helper()
			txCostsOn = on
			defer func() { txCostsOn = true }()
			dir := t.TempDir()
			s := failScanner(t, dir, blk.txs, blk.res, val)
			n := s.processBlock(context.Background(), 100)
			s.checkpoint(100)
			return dir, n
		}
		with, nWith := run(true)
		without, nWithout := run(false)
		if nWith != nWithout || nWith == 0 {
			t.Fatalf("%s: processBlock counted %d with the step and %d without", blk.name, nWith, nWithout)
		}
		for _, f := range []string{"publications.jsonl", "payments.jsonl", "host_history.jsonl", "state.json"} {
			a, okA := read(with, f)
			b, okB := read(without, f)
			if !okA || !okB || len(a) == 0 {
				t.Fatalf("%s: %s is missing or empty: the proof needs a record in it", blk.name, f)
			}
			if string(a) != string(b) {
				t.Errorf("%s: %s differs:\nwith    %s\nwithout %s", blk.name, f, a, b)
			}
		}
		for _, f := range []string{failedtx.FileName, "param_uncertainty.jsonl"} {
			a, okA := read(with, f)
			b, okB := read(without, f)
			if okA != okB || string(a) != string(b) {
				t.Errorf("%s: %s differs (present %v and %v):\nwith    %s\nwithout %s", blk.name, f, okA, okB, a, b)
			}
		}
		if _, err := os.Stat(filepath.Join(without, txcost.FileName)); !os.IsNotExist(err) {
			t.Fatalf("%s: the scan without the step wrote %s: %v", blk.name, txcost.FileName, err)
		}
		if got := readCosts(t, with); len(got) == 0 {
			t.Fatalf("%s: the scan with the step wrote no cost line", blk.name)
		}
	}
}

// Case 5: a block read again, in the same process or after a restart below
// it, adds no line; the seen-set holds only the keys above the checkpoint;
// a torn last line is cut at open and written again whole.
func TestATxCostIsRecordedOnceAndItsKeyForgottenAtTheCheckpoint(t *testing.T) {
	cases := costCases(t, secp256k1.GenPrivKey())
	txs := [][]byte{cases[1].raw, cases[2].raw} // the deposit and the withdrawal request
	results := []*abci.ExecTxResult{cases[1].res, cases[2].res}
	dir := t.TempDir()
	path := filepath.Join(dir, txcost.FileName)
	s := failScanner(t, dir, txs, results, nil)
	ctx := context.Background()
	k0, k1 := txcost.Key(100, 0), txcost.Key(100, 1)
	keysOnce := func(when string) {
		t.Helper()
		got := readCosts(t, dir)
		if len(got) != 2 || got[0].DedupeKey != k0 || got[1].DedupeKey != k1 {
			t.Fatalf("%s: %d lines %+v", when, len(got), got)
		}
	}

	s.processBlock(ctx, 100)
	s.processBlock(ctx, 100)
	keysOnce("the block read twice")

	// A restart with the checkpoint below the block: its keys are loaded.
	if err := s.store.SaveState(PersistState{ChainID: "test-1", LastScannedHeight: 99}); err != nil {
		t.Fatal(err)
	}
	if !s.store.TxCostSeen(k0) || !s.store.TxCostSeen(k1) {
		t.Fatal("a checkpoint below the block forgot its keys")
	}
	s.store.Close()
	st, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	s.store = st
	if !st.TxCostSeen(k0) || !st.TxCostSeen(k1) {
		t.Fatal("the keys above the checkpoint were not loaded")
	}
	s.processBlock(ctx, 100)
	keysOnce("the block read again after a restart")

	// The checkpoint at the block: the set empties, and a restart loads none.
	if err := st.SaveState(PersistState{ChainID: "test-1", LastScannedHeight: 100}); err != nil {
		t.Fatal(err)
	}
	if len(st.costSeen) != 0 || st.TxCostSeen(k0) {
		t.Fatalf("the checkpoint kept %d keys", len(st.costSeen))
	}
	st.Close()
	st, err = OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	s.store = st
	if len(st.costSeen) != 0 {
		t.Fatalf("a restart at the checkpoint loaded %d keys", len(st.costSeen))
	}

	// A torn last line, as a crash in the middle of the write leaves it,
	// with the checkpoint below the block.
	if err := st.SaveState(PersistState{ChainID: "test-1", LastScannedHeight: 99}); err != nil {
		t.Fatal(err)
	}
	st.Close()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	first := strings.Index(string(b), "\n") + 1
	if err := os.WriteFile(path, b[:first+(len(b)-first)/2], 0o644); err != nil {
		t.Fatal(err)
	}
	st, err = OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	s.store = st
	if after, _ := os.ReadFile(path); string(after) != string(b[:first]) {
		t.Fatalf("the torn line was not cut: %q", after)
	}
	if !st.TxCostSeen(k0) || st.TxCostSeen(k1) {
		t.Fatal("after the cut, only the whole line's key is on record")
	}
	s.processBlock(ctx, 100)
	keysOnce("the torn line written again")
	if n := wholeLines(t, path); n != 2 {
		t.Fatalf("%d lines", n)
	}
}

// Case 6: a torn tail is cut at open and the whole line before it is
// loaded; a line appended, synced and checkpointed is read back by the
// next open with no key held; a key appended twice is one line; a store
// that appends nothing leaves no file.
func TestTxCostsAreRepairedAtOpenAndSyncedAtTheCheckpoint(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, txcost.FileName)
	whole := `{"schema_version":1,"dedupe_key":"h5:0","height":5}` + "\n"
	if err := os.WriteFile(path, []byte(whole+`{"schema_version":1,"dedupe_ke`), 0o644); err != nil {
		t.Fatal(err)
	}
	st, err := OpenStore(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if b, _ := os.ReadFile(path); string(b) != whole {
		t.Fatalf("torn tail not truncated: %q", b)
	}
	if !st.TxCostSeen("h5:0") {
		t.Fatal("the whole line's key was not loaded")
	}
	if st.TxCostsPath() != path {
		t.Fatalf("TxCostsPath = %s", st.TxCostsPath())
	}
	r := txcost.Record{SchemaVersion: txcost.SchemaVersion, DedupeKey: txcost.Key(6, 1), Height: 6, TxIndex: 1, Time: failTime,
		TxHash: strings.Repeat("ab", 32), GasWanted: 100, GasUsed: 90, Fee: "1utia", FeePayer: "celestia1payer",
		Messages: []failedtx.Msg{{Index: 0, TypeURL: msgDepositTypeURL, Signer: "celestia1pub"}}, RecordedAt: failTime.Add(time.Second)}
	for i := 0; i < 2; i++ {
		if err := st.AppendTxCost(r); err != nil {
			t.Fatal(err)
		}
	}
	if err := st.AppendTxCost(txcost.Record{Height: 7}); err == nil {
		t.Fatal("a cost line without a key was written")
	}
	if err := st.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := st.SaveState(PersistState{ChainID: "x", LastScannedHeight: 6}); err != nil {
		t.Fatal(err)
	}
	if st.TxCostSeen(r.DedupeKey) || st.TxCostSeen("h5:0") {
		t.Fatal("the checkpoint kept a key at or below it")
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	st, err = OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if len(st.costSeen) != 0 {
		t.Fatalf("a reopen at the checkpoint holds %d keys", len(st.costSeen))
	}
	got := readCosts(t, dir)
	if len(got) != 2 || !reflect.DeepEqual(got[1], r) {
		t.Fatalf("read back: %+v", got)
	}

	empty := t.TempDir()
	st2, err := OpenStore(empty)
	if err != nil {
		t.Fatal(err)
	}
	if err := st2.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := st2.SaveState(PersistState{ChainID: "x", LastScannedHeight: 9}); err != nil {
		t.Fatal(err)
	}
	if err := st2.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(empty, txcost.FileName)); !os.IsNotExist(err) {
		t.Fatalf("a store that recorded no cost has %s: %v", txcost.FileName, err)
	}
}

// Case 7: a line of tx_costs.jsonl that does not decode stops the open,
// naming the file and the line, as one in failed_txs.jsonl does.
func TestAnUndecodableTxCostLineStopsTheOpen(t *testing.T) {
	dir := t.TempDir()
	body := `{"schema_version":1,"dedupe_key":"h5:0","height":5}` + "\n" + `{"schema_version":1,"dedupe_` + "x\n" + `{"schema_version":1,"dedupe_key":"h6:0","height":6}` + "\n"
	if err := os.WriteFile(filepath.Join(dir, txcost.FileName), []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	st, err := OpenStore(dir)
	if err == nil {
		st.Close()
		t.Fatal("a store opened over an undecodable tx_costs.jsonl line")
	}
	if !strings.Contains(err.Error(), txcost.FileName) || !strings.Contains(err.Error(), "line 2") {
		t.Fatalf("the error does not name the file and line: %v", err)
	}
}

// execHoldingURL is a MsgExec's bytes that hold a Fibre type URL (as its
// grantee) and then a message cut short: they do not decode.
func execHoldingURL() []byte {
	b := append([]byte{0x0a, byte(len(msgDepositTypeURL))}, msgDepositTypeURL...)
	return append(b, 0x12, 0x05, 'a')
}

// Case 7: a message of a success that does not decode is logged and never
// stops the scan. A MsgExec that does not decode has no inner messages, so
// its tx has a cost line only when another message carries Fibre; one
// whose bytes hold no Fibre type URL is not even decoded.
func TestASuccessfulMessageThatDoesNotDecodeIsLoggedNotFatal(t *testing.T) {
	owner := "celestia1owner"
	deposit := depositMsg{&fibretypes.MsgDepositToEscrow{Signer: owner, Amount: sdk.NewInt64Coin("utia", 5)}}
	txs := [][]byte{
		rawTx(t, rawMsg{failedtx.ExecTypeURL, execHoldingURL()}),
		rawTx(t, rawMsg{failedtx.ExecTypeURL, garbage}, deposit),
		rawTx(t, rawMsg{failedtx.ExecTypeURL, garbage}),
	}
	ok := resultOf(nil, 100000, 40000, anteEvents("100utia")...)
	dir := t.TempDir()
	s := failScanner(t, dir, txs, []*abci.ExecTxResult{ok, ok, ok}, nil)
	s.processBlock(context.Background(), 100)
	got := readCosts(t, dir)
	want := []failedtx.Msg{{Index: 0, TypeURL: failedtx.ExecTypeURL}, {Index: 1, TypeURL: msgDepositTypeURL, Signer: owner}}
	if len(got) != 1 || got[0].TxIndex != 1 || !reflect.DeepEqual(got[0].Messages, want) {
		t.Fatalf("cost lines: %+v", got)
	}
	logs := s.log.lines()
	for _, w := range []string{"h=100 tx=0: tx cost: msg 0: MsgExec did not decode", "h=100 tx=1: tx cost: msg 0: MsgExec did not decode"} {
		if !strings.Contains(logs, w) {
			t.Errorf("not logged: %q", w)
		}
	}
	if strings.Contains(logs, "h=100 tx=2: tx cost") {
		t.Error("a tx that holds no Fibre type URL was decoded")
	}
}
