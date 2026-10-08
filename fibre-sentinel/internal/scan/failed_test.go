package scan

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	errorsmod "cosmossdk.io/errors"
	fibretypes "github.com/celestiaorg/celestia-app/v10/x/fibre/types"
	valaddrtypes "github.com/celestiaorg/celestia-app/v10/x/valaddr/types"
	"github.com/celestiaorg/go-square/v4/share"
	squaretx "github.com/celestiaorg/go-square/v4/tx"
	abci "github.com/cometbft/cometbft/abci/types"
	cmted25519 "github.com/cometbft/cometbft/crypto/ed25519"
	cmtversion "github.com/cometbft/cometbft/proto/tendermint/version"
	coretypes "github.com/cometbft/cometbft/rpc/core/types"
	cmttypes "github.com/cometbft/cometbft/types"
	codectypes "github.com/cosmos/cosmos-sdk/codec/types"
	"github.com/cosmos/cosmos-sdk/crypto/keys/secp256k1"
	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/cosmos/cosmos-sdk/types/bech32"
	sdkerrors "github.com/cosmos/cosmos-sdk/types/errors"
	cosmostx "github.com/cosmos/cosmos-sdk/types/tx"
	"github.com/cosmos/cosmos-sdk/x/authz"
	banktypes "github.com/cosmos/cosmos-sdk/x/bank/types"
	gogojsonpb "github.com/cosmos/gogoproto/jsonpb"
	"github.com/plsgiveup/fibre/fibre-sentinel/internal/failedtx"
)

// The messages a failed transaction can carry beside the Fibre ones.
type sendMsg struct{ *banktypes.MsgSend }
type hostMsg struct {
	*valaddrtypes.MsgSetFibreProviderInfo
}
type execMsg struct{ *authz.MsgExec }

// rawMsg is a message of any type URL with the bytes given, whether they
// decode or not.
type rawMsg struct {
	url   string
	value []byte
}

func (sendMsg) typeURL() string           { return sdk.MsgTypeURL(&banktypes.MsgSend{}) }
func (hostMsg) typeURL() string           { return msgSetHostTypeURL }
func (execMsg) typeURL() string           { return failedtx.ExecTypeURL }
func (m rawMsg) typeURL() string          { return m.url }
func (m rawMsg) Marshal() ([]byte, error) { return m.value, nil }

// execOf wraps msgs in a MsgExec, as a grantee submits them.
func execOf(t *testing.T, msgs ...proto) execMsg {
	t.Helper()
	e := &authz.MsgExec{Grantee: "celestia1grantee"}
	for _, m := range msgs {
		b, err := m.Marshal()
		if err != nil {
			t.Fatal(err)
		}
		e.Msgs = append(e.Msgs, &codectypes.Any{TypeUrl: m.typeURL(), Value: b})
	}
	return execMsg{e}
}

// garbage is bytes no message decodes: a length-prefixed field cut short.
var garbage = []byte{0x0a, 0x05, 'a'}

// failTime is the block time of every block here.
var failTime = time.Date(2026, 10, 10, 8, 1, 2, 123456789, time.UTC)

func valoper(t *testing.T, seed byte) string {
	t.Helper()
	s, err := bech32.ConvertAndEncode("celestiavaloper", bytes.Repeat([]byte{seed}, 20))
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// anteEvents are what the ante handler leaves on a result when the whole
// chain passed: the fee event (fee "" for a zero fee) and the sequence one.
func anteEvents(fee string) []abci.Event {
	return []abci.Event{
		{Type: sdk.EventTypeTx, Attributes: []abci.EventAttribute{{Key: sdk.AttributeKeyFee, Value: fee, Index: true}, {Key: "fee_payer", Value: "celestia1payer", Index: true}}},
		{Type: sdk.EventTypeTx, Attributes: []abci.EventAttribute{{Key: "acc_seq", Value: "celestia1payer/6", Index: true}}},
	}
}

// resultOf is the result the node returns for a tx that failed with err
// (nil: succeeded), through the SDK's own ABCIInfo.
func resultOf(err error, gasWanted, gasUsed uint64, events ...abci.Event) *abci.ExecTxResult {
	if err == nil {
		return &abci.ExecTxResult{GasWanted: int64(gasWanted), GasUsed: int64(gasUsed), Events: events}
	}
	return sdkerrors.ResponseExecTxResultWithEvents(err, gasWanted, gasUsed, events, false)
}

// inMsg is err as runMsgs returns it for the message at index i
// (cosmos-sdk baseapp/baseapp.go:927).
func inMsg(err error, i int) error {
	return errorsmod.Wrapf(err, "failed to execute message; message index: %d", i)
}

// bodyWrappers are a BlobTx and an IndexWrapper whose tx field holds the
// TxBody of msg, not a whole TxRaw. Read as a TxRaw, such a wrapper gives
// its tx field as the body bytes, so it decodes as a tx carrying msg: only
// the wrapper check in failedTxMsgs keeps it off the record.
func bodyWrappers(t *testing.T, msg proto, blob *share.Blob) (blobTx, indexWrapper []byte) {
	t.Helper()
	var raw cosmostx.TxRaw
	if err := raw.Unmarshal(rawTx(t, msg)); err != nil {
		t.Fatal(err)
	}
	blobTx, err := squaretx.MarshalBlobTx(raw.BodyBytes, blob)
	if err != nil {
		t.Fatal(err)
	}
	indexWrapper, err = squaretx.MarshalIndexWrapper(raw.BodyBytes, 4)
	if err != nil {
		t.Fatal(err)
	}
	return blobTx, indexWrapper
}

// failedCase is one transaction of case 2's block: its bytes, its result,
// and the line failed_txs.jsonl must hold for it (nil: none). The line's
// place (height, index, key, hash, time, app version) is filled by the
// test that places the transaction.
type failedCase struct {
	name string
	raw  []byte
	res  *abci.ExecTxResult
	want *failedtx.Record
}

// failedCases is every kind of failed transaction the record tells apart,
// and a successful deposit, as one block holds them. The failed set-host
// carries a set_fibre_provider_info event and the failed MsgPayForFibre an
// EventUpdateFibreParams, which only a successful tx may apply.
func failedCases(t *testing.T, key *secp256k1.PrivKey) []failedCase {
	t.Helper()
	owner, err := bech32.ConvertAndEncode(accountHRP, key.PubKey().Address())
	if err != nil {
		t.Fatal(err)
	}
	other := "celestia1qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqq"
	pp := testPromise(t, key, 1<<20)
	pp2 := testPromise(t, key, 1<<19)
	hash, _, _, err := promiseCharge(&pp)
	if err != nil {
		t.Fatal(err)
	}
	hash2, _, _, err := promiseCharge(&pp2)
	if err != nil {
		t.Fatal(err)
	}
	op1, op2 := valoper(t, 'v'), valoper(t, 'w')
	send := sendMsg{&banktypes.MsgSend{FromAddress: owner, ToAddress: other, Amount: sdk.NewCoins(sdk.NewInt64Coin("utia", 5))}}
	deposit := depositMsg{&fibretypes.MsgDepositToEscrow{Signer: owner, Amount: sdk.NewInt64Coin("utia", 1_000_000)}}
	inner := rawTx(t, deposit)
	blob, err := share.NewV0Blob(share.MustNewV0Namespace([]byte("failed-t")), []byte("blob data"))
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
	blobTxBody, wrappedBody := bodyWrappers(t, deposit, blob)
	p := params(time.Second, time.Second, time.Second)
	js, err := (&gogojsonpb.Marshaler{}).MarshalToString(&p)
	if err != nil {
		t.Fatal(err)
	}
	paramsEv := abci.Event{Type: eventUpdateFibreParamsType, Attributes: []abci.EventAttribute{{Key: "params", Value: js}}}

	depositRes := resultOf(inMsg(errorsmod.Wrapf(errorsmod.Wrapf(sdkerrors.ErrInsufficientFunds, "spendable balance %s is smaller than %s", "10utia", "1000000utia"), "failed to transfer funds to escrow"), 1), 200000, 91234, anteEvents("2000utia")...)
	hostRes := resultOf(inMsg(valaddrtypes.ErrInvalidValidator.Wrapf("validator not found: %v", op1), 0), 150000, 61000, append(anteEvents(""), regEvent(bech(t, "b"), "evil.example:1"))...)
	execRes := resultOf(errorsmod.Wrapf(sdkerrors.ErrWrongSequence, "account sequence mismatch, expected %d, got %d", 7, 6), 150000, 0)
	panicRes := resultOf(errorsmod.Wrap(sdkerrors.ErrPanic, fmt.Sprintf("recovered: %v\nstack:\n%v", "boom", "goroutine 7 [running]:\nmain.main()\n\t/home/node/app.go:10 +0x1d")), 900000, 812345, append(anteEvents("1000utia"), paramsEv)...)
	escrowRes := resultOf(inMsg(errorsmod.Wrapf(sdkerrors.ErrInvalidRequest, "payment promise %s has not timed out", "x"), 1), 300000, 120000, anteEvents("500utia")...)

	return []failedCase{
		{"[MsgSend, MsgDepositToEscrow], the deposit failed", rawTx(t, send, deposit), depositRes, &failedtx.Record{
			Code: 5, Codespace: "sdk", Log: depositRes.Log, GasWanted: 200000, GasUsed: 91234, AntePassed: true, Fee: "2000utia",
			Messages: []failedtx.Msg{
				{Index: 0, TypeURL: "/cosmos.bank.v1beta1.MsgSend"},
				{Index: 1, TypeURL: msgDepositTypeURL, Signer: owner, Detail: &failedtx.MsgDetail{Publisher: owner, Amount: "1000000utia"}},
			}}},
		{"MsgSetFibreProviderInfo, zero fee", rawTx(t, hostMsg{&valaddrtypes.MsgSetFibreProviderInfo{Signer: op1, Host: "h1.example:7980"}}), hostRes, &failedtx.Record{
			Code: 2, Codespace: "valaddr", Log: hostRes.Log, GasWanted: 150000, GasUsed: 61000, AntePassed: true, Fee: "",
			Messages: []failedtx.Msg{
				{Index: 0, TypeURL: msgSetHostTypeURL, Signer: op1, Detail: &failedtx.MsgDetail{Host: "h1.example:7980"}},
			}}},
		{"MsgExec[MsgSetFibreProviderInfo], ante failed", rawTx(t, execOf(t, hostMsg{&valaddrtypes.MsgSetFibreProviderInfo{Signer: op2, Host: "h2.example:7980"}})), execRes, &failedtx.Record{
			Code: 32, Codespace: "sdk", Log: execRes.Log, GasWanted: 150000, GasUsed: 0, AntePassed: false, Fee: "",
			Messages: []failedtx.Msg{
				{Index: 0, TypeURL: failedtx.ExecTypeURL, Inner: []failedtx.Msg{
					{Index: 0, TypeURL: msgSetHostTypeURL, Signer: op2, Detail: &failedtx.MsgDetail{Host: "h2.example:7980"}},
				}},
			}}},
		{"MsgPayForFibre, a panic", rawTx(t, payMsg{&fibretypes.MsgPayForFibre{Signer: owner, PaymentPromise: pp2}}), panicRes, &failedtx.Record{
			Code: 111222, Codespace: "undefined", Log: "recovered: boom", LogCut: true, GasWanted: 900000, GasUsed: 812345, AntePassed: true, Fee: "1000utia",
			Messages: []failedtx.Msg{
				{Index: 0, TypeURL: fibretypes.MsgPayForFibreTypeURL, Signer: owner, Detail: &failedtx.MsgDetail{
					Publisher: owner, PromiseHash: hash2, Namespace: hex.EncodeToString(pp2.Namespace), BlobSize: 1 << 19}},
			}}},
		{"[MsgRequestWithdrawal, MsgPaymentPromiseTimeout]", rawTx(t,
			withdrawMsg{&fibretypes.MsgRequestWithdrawal{Signer: owner, Amount: sdk.NewInt64Coin("utia", 7)}},
			timeoutMsg{&fibretypes.MsgPaymentPromiseTimeout{Signer: other, PaymentPromise: pp}}), escrowRes, &failedtx.Record{
			Code: 18, Codespace: "sdk", Log: escrowRes.Log, GasWanted: 300000, GasUsed: 120000, AntePassed: true, Fee: "500utia",
			Messages: []failedtx.Msg{
				{Index: 0, TypeURL: msgWithdrawalTypeURL, Signer: owner, Detail: &failedtx.MsgDetail{Publisher: owner, Amount: "7utia"}},
				{Index: 1, TypeURL: msgTimeoutTypeURL, Signer: other, Detail: &failedtx.MsgDetail{Publisher: owner, PromiseHash: hash}},
			}}},
		{"MsgSend alone", rawTx(t, send), resultOf(inMsg(sdkerrors.ErrInsufficientFunds, 0), 100000, 50000, anteEvents("300utia")...), nil},
		{"a BlobTx whose tx carries a deposit", blobTx, resultOf(sdkerrors.ErrOutOfGas, 100000, 100100, anteEvents("300utia")...), nil},
		{"an IndexWrapper whose tx carries a deposit", wrapped, resultOf(sdkerrors.ErrOutOfGas, 100000, 100100, anteEvents("300utia")...), nil},
		{"a BlobTx whose tx field is a deposit's TxBody", blobTxBody, resultOf(sdkerrors.ErrOutOfGas, 100000, 100100, anteEvents("300utia")...), nil},
		{"an IndexWrapper whose tx field is a deposit's TxBody", wrappedBody, resultOf(sdkerrors.ErrOutOfGas, 100000, 100100, anteEvents("300utia")...), nil},
		{"not an SDK tx", []byte("not an sdk tx at all"), resultOf(sdkerrors.ErrTxDecode, 0, 0), nil},
		{"a successful deposit", rawTx(t, depositMsg{&fibretypes.MsgDepositToEscrow{Signer: owner, Amount: sdk.NewInt64Coin("utia", 3)}}), resultOf(nil, 100000, 60000, anteEvents("300utia")...), nil},
	}
}

// failScanner is a scanner over a node whose block h, of any height, holds
// txs with results, at failTime under app version 10, with one validator
// (val) for any promise height.
func failScanner(t *testing.T, dir string, txs [][]byte, results []*abci.ExecTxResult, val *cmttypes.Validator) *Scanner {
	t.Helper()
	return failScannerAt(t, dir, FibreAppVersion, txs, results, val)
}

// failScannerAt is failScanner with every block's header at app version app.
func failScannerAt(t *testing.T, dir string, app uint64, txs [][]byte, results []*abci.ExecTxResult, val *cmttypes.Validator) *Scanner {
	t.Helper()
	if len(txs) != len(results) {
		t.Fatalf("%d txs, %d results", len(txs), len(results))
	}
	blockTxs := make(cmttypes.Txs, len(txs))
	for i, raw := range txs {
		blockTxs[i] = raw
	}
	node := newRPCNode(t, func(method string, height int64) (any, string) {
		switch method {
		case "block":
			return coretypes.ResultBlock{Block: &cmttypes.Block{
				Header: cmttypes.Header{Version: cmtversion.Consensus{Block: 11, App: app}, ChainID: "test-1", Height: height, Time: failTime},
				Data:   cmttypes.Data{Txs: blockTxs},
			}}, ""
		case "block_results":
			return coretypes.ResultBlockResults{Height: height, TxsResults: results}, ""
		case "validators":
			return coretypes.ResultValidators{BlockHeight: height, Validators: []*cmttypes.Validator{val}, Count: 1, Total: 1}, ""
		case "status":
			return statusJSON(1, 200), ""
		}
		return nil, "unexpected " + method
	})
	s, err := New(Config{RPCURL: node.srv.URL, DataDir: dir, RPCTimeout: 2 * time.Second, StoreRows: true}, NewLogger(500))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.store.Close() })
	s.chainID = "test-1"
	s.startHeight = 1
	s.params = NewParamHistory(1, fibretypes.DefaultParams())
	s.hosts = LoadHostHistory(nil, true, 1)
	return s
}

// readFailed decodes failed_txs.jsonl, every line whole.
func readFailed(t *testing.T, dir string) []failedtx.Record {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(dir, failedtx.FileName))
	if err != nil {
		t.Fatal(err)
	}
	var out []failedtx.Record
	for i, line := range strings.Split(strings.TrimSuffix(string(b), "\n"), "\n") {
		var r failedtx.Record
		if err := json.Unmarshal([]byte(line), &r); err != nil {
			t.Fatalf("line %d does not decode: %q", i+1, line)
		}
		out = append(out, r)
	}
	return out
}

// Case 2: one block with every kind. Exactly the failed transactions that
// carried a Fibre message are on record, each with what the chain returned
// for it and what each Fibre message asked for.
func TestEveryFailedFibreTransactionOfABlockIsRecorded(t *testing.T) {
	key := secp256k1.GenPrivKey()
	cases := failedCases(t, key)
	var txs [][]byte
	var results []*abci.ExecTxResult
	var want []failedtx.Record
	for i, c := range cases {
		txs = append(txs, c.raw)
		results = append(results, c.res)
		if c.want == nil {
			continue
		}
		w := *c.want
		w.SchemaVersion = failedtx.SchemaVersion
		w.DedupeKey = failedtx.Key(100, i)
		w.Height, w.TxIndex, w.Time, w.AppVersion = 100, i, failTime, FibreAppVersion
		w.TxHash = hexstr(cmttypes.Tx(c.raw).Hash())
		want = append(want, w)
	}
	dir := t.TempDir()
	s := failScanner(t, dir, txs, results, nil)
	if n := s.processBlock(context.Background(), 100); n != 1 {
		t.Fatalf("processBlock = %d, want the successful deposit alone", n)
	}
	got := readFailed(t, dir)
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
	// The successful deposit is a payment, and no failure is.
	pays, err := LoadPayments(s.store.PaymentsPath())
	if err != nil {
		t.Fatal(err)
	}
	if len(pays) != 1 || pays[0].TxIndex != len(cases)-1 || pays[0].AmountUtia != 3 {
		t.Fatalf("payments: %+v", pays)
	}
	pubs, err := LoadPublications(s.store.PublicationsPath())
	if err != nil || len(pubs) != 0 {
		t.Fatalf("publications: %+v %v", pubs, err)
	}
}

// D4: BlobTx and IndexWrapper bytes are skipped before anything reads them
// as an SDK tx. A wrapper whose tx field is a deposit's TxBody decodes as a
// tx carrying the deposit, so the wrapper check alone keeps it, failed, off
// the record.
func TestAWrapperIsNeverReadAsAFailedTx(t *testing.T) {
	key := secp256k1.GenPrivKey()
	owner, err := bech32.ConvertAndEncode(accountHRP, key.PubKey().Address())
	if err != nil {
		t.Fatal(err)
	}
	deposit := depositMsg{&fibretypes.MsgDepositToEscrow{Signer: owner, Amount: sdk.NewInt64Coin("utia", 1_000_000)}}
	blob, err := share.NewV0Blob(share.MustNewV0Namespace([]byte("failed-w")), []byte("blob data"))
	if err != nil {
		t.Fatal(err)
	}
	blobTx, wrapped := bodyWrappers(t, deposit, blob)
	for name, raw := range map[string][]byte{"BlobTx": blobTx, "IndexWrapper": wrapped} {
		// Without the check, these bytes are a failed deposit.
		if got := decodeTxMsgs(raw); len(got) != 1 || got[0].typeURL != msgDepositTypeURL {
			t.Fatalf("%s: decodeTxMsgs read %+v, not the deposit: the fixture does not reach the check", name, got)
		}
		if msgs, problems, ok := failedTxMsgs(raw); ok || msgs != nil || problems != nil {
			t.Errorf("%s: failedTxMsgs = %+v %v %v, want nothing", name, msgs, problems, ok)
		}
	}

	fail := resultOf(sdkerrors.ErrOutOfGas, 100000, 100100, anteEvents("300utia")...)
	dir := t.TempDir()
	s := failScanner(t, dir, [][]byte{blobTx, wrapped}, []*abci.ExecTxResult{fail, fail}, nil)
	if n := s.processBlock(context.Background(), 100); n != 0 {
		t.Fatalf("processBlock = %d", n)
	}
	if _, err := os.Stat(filepath.Join(dir, failedtx.FileName)); !os.IsNotExist(err) {
		t.Fatalf("a wrapper was recorded as a failed tx: %v", err)
	}
}

// A line carries the app version of the block it failed in, from that
// block's header, not a constant: the rules the block ran under. A block
// run under a version the reason table is not pinned to is read with no
// reason and no message index, though its log names one.
func TestAFailedTxCarriesItsBlocksAppVersion(t *testing.T) {
	c := failedCases(t, secp256k1.GenPrivKey())[0] // [MsgSend, MsgDepositToEscrow], the deposit failed
	for _, w := range []struct {
		app     uint64
		explain failedtx.Explanation
	}{
		{FibreAppVersion, failedtx.Explanation{Reason: "Insufficient funds", MsgIndex: 1}},
		{FibreAppVersion + 1, failedtx.Explanation{Reason: "", MsgIndex: -1}},
	} {
		dir := t.TempDir()
		s := failScannerAt(t, dir, w.app, [][]byte{c.raw}, []*abci.ExecTxResult{c.res}, nil)
		s.processBlock(context.Background(), 100)
		got := readFailed(t, dir)
		if len(got) != 1 || got[0].AppVersion != w.app {
			t.Fatalf("a block at app v%d: %+v", w.app, got)
		}
		if e := failedtx.Explain(got[0]); e != w.explain {
			t.Errorf("a block at app v%d is read as %+v, want %+v", w.app, e, w.explain)
		}
	}
}

// Every Fibre message keeps what it asked for, at the top of the tx and
// inside a MsgExec alike.
func TestEachFibreMessageKeepsWhatItAskedFor(t *testing.T) {
	key := secp256k1.GenPrivKey()
	owner, err := bech32.ConvertAndEncode(accountHRP, key.PubKey().Address())
	if err != nil {
		t.Fatal(err)
	}
	pp := testPromise(t, key, 3<<20)
	hash, _, _, err := promiseCharge(&pp)
	if err != nil {
		t.Fatal(err)
	}
	op := valoper(t, 'x')
	kinds := []proto{
		payMsg{&fibretypes.MsgPayForFibre{Signer: "celestia1submitter", PaymentPromise: pp}},
		depositMsg{&fibretypes.MsgDepositToEscrow{Signer: owner, Amount: sdk.NewInt64Coin("utia", 42)}},
		withdrawMsg{&fibretypes.MsgRequestWithdrawal{Signer: owner, Amount: sdk.NewInt64Coin("utia", 9)}},
		timeoutMsg{&fibretypes.MsgPaymentPromiseTimeout{Signer: "celestia1anyone", PaymentPromise: pp}},
		hostMsg{&valaddrtypes.MsgSetFibreProviderInfo{Signer: op, Host: "x.example:7980"}},
	}
	wantKinds := []failedtx.Msg{
		{Index: 0, TypeURL: fibretypes.MsgPayForFibreTypeURL, Signer: "celestia1submitter", Detail: &failedtx.MsgDetail{
			Publisher: owner, PromiseHash: hash, Namespace: hex.EncodeToString(pp.Namespace), BlobSize: 3 << 20}},
		{Index: 1, TypeURL: msgDepositTypeURL, Signer: owner, Detail: &failedtx.MsgDetail{Publisher: owner, Amount: "42utia"}},
		{Index: 2, TypeURL: msgWithdrawalTypeURL, Signer: owner, Detail: &failedtx.MsgDetail{Publisher: owner, Amount: "9utia"}},
		{Index: 3, TypeURL: msgTimeoutTypeURL, Signer: "celestia1anyone", Detail: &failedtx.MsgDetail{Publisher: owner, PromiseHash: hash}},
		{Index: 4, TypeURL: msgSetHostTypeURL, Signer: op, Detail: &failedtx.MsgDetail{Host: "x.example:7980"}},
	}

	msgs, problems, ok := failedTxMsgs(rawTx(t, kinds...))
	if !ok || len(problems) != 0 || !reflect.DeepEqual(msgs, wantKinds) {
		t.Fatalf("top level: ok=%v problems=%v\n got %+v\nwant %+v", ok, problems, msgs, wantKinds)
	}

	msgs, problems, ok = failedTxMsgs(rawTx(t, sendMsg{&banktypes.MsgSend{FromAddress: owner}}, execOf(t, kinds...)))
	want := []failedtx.Msg{{Index: 0, TypeURL: sdk.MsgTypeURL(&banktypes.MsgSend{})}, {Index: 1, TypeURL: failedtx.ExecTypeURL, Inner: wantKinds}}
	if !ok || len(problems) != 0 || !reflect.DeepEqual(msgs, want) {
		t.Fatalf("inside a MsgExec: ok=%v problems=%v\n got %+v\nwant %+v", ok, problems, msgs, want)
	}
	if !failedtx.Carries(msgs) || failedtx.Carries(msgs[:1]) {
		t.Fatal("Carries does not see the Fibre messages inside the MsgExec")
	}

	// A promise whose hash and owner cannot be derived keeps what the
	// message itself says, and the problem is named.
	bad := pp
	bad.SignerPublicKey = secp256k1.PubKey{Key: []byte{1, 2, 3}}
	bad.Commitment = []byte{1}
	msgs, problems, ok = failedTxMsgs(rawTx(t, payMsg{&fibretypes.MsgPayForFibre{Signer: owner, PaymentPromise: bad}}))
	wantBad := []failedtx.Msg{{Index: 0, TypeURL: fibretypes.MsgPayForFibreTypeURL, Signer: owner, Detail: &failedtx.MsgDetail{
		Namespace: hex.EncodeToString(bad.Namespace), BlobSize: 3 << 20}}}
	if !ok || len(problems) != 1 || !reflect.DeepEqual(msgs, wantBad) {
		t.Fatalf("an underivable promise: ok=%v problems=%v\n got %+v", ok, problems, msgs)
	}
	if p := problems[0].Error(); !strings.Contains(p, "publisher") || !strings.Contains(p, "promise hash") {
		t.Fatalf("the problem does not name what was left out: %v", p)
	}
}

// A message the chain refused can carry a signer, a host or a denom as long
// as its tx. Each string a Fibre message gives the record is kept to
// failedtx.MaxFieldBytes, cut on a rune start, and the message is marked;
// one within the cap, at the top or inside a MsgExec, is kept whole.
func TestAFibreMessagesLongStringsAreCut(t *testing.T) {
	long := strings.Repeat("a", failedtx.MaxFieldBytes-1) + "é" + strings.Repeat("b", 4096)
	kept := strings.Repeat("a", failedtx.MaxFieldBytes-1)
	op := valoper(t, 'y')
	coin := sdk.NewInt64Coin("utia", 5)
	coin.Denom = long
	msgs, problems, ok := failedTxMsgs(rawTx(t,
		hostMsg{&valaddrtypes.MsgSetFibreProviderInfo{Signer: long, Host: long}},
		depositMsg{&fibretypes.MsgDepositToEscrow{Signer: op, Amount: coin}},
		execOf(t, hostMsg{&valaddrtypes.MsgSetFibreProviderInfo{Signer: op, Host: "y.example:7980"}}, hostMsg{&valaddrtypes.MsgSetFibreProviderInfo{Signer: op, Host: long}}),
	))
	want := []failedtx.Msg{
		{Index: 0, TypeURL: msgSetHostTypeURL, Signer: kept, Detail: &failedtx.MsgDetail{Host: kept}, Cut: true},
		// "5" and the denom: cut at the cap, where the é starts.
		{Index: 1, TypeURL: msgDepositTypeURL, Signer: op, Detail: &failedtx.MsgDetail{Publisher: op, Amount: "5" + kept}, Cut: true},
		{Index: 2, TypeURL: failedtx.ExecTypeURL, Inner: []failedtx.Msg{
			{Index: 0, TypeURL: msgSetHostTypeURL, Signer: op, Detail: &failedtx.MsgDetail{Host: "y.example:7980"}},
			{Index: 1, TypeURL: msgSetHostTypeURL, Signer: op, Detail: &failedtx.MsgDetail{Host: kept}, Cut: true},
		}},
	}
	if !ok || len(problems) != 0 || !reflect.DeepEqual(msgs, want) {
		t.Fatalf("ok=%v problems=%v\n got %+v\nwant %+v", ok, problems, msgs, want)
	}
}

// Case 6: a message that does not decode is logged and never stops the
// scan. A MsgExec that does not decode has no inner messages, so its tx is
// recorded only when another message carries Fibre; a Fibre message that
// does not decode keeps its type URL and nothing else, and its tx is
// recorded all the same.
func TestAMessageThatDoesNotDecodeIsLoggedNotFatal(t *testing.T) {
	owner := "celestia1owner"
	deposit := depositMsg{&fibretypes.MsgDepositToEscrow{Signer: owner, Amount: sdk.NewInt64Coin("utia", 5)}}
	badExec := rawMsg{failedtx.ExecTypeURL, garbage}
	badDeposit := rawMsg{msgDepositTypeURL, garbage}
	txs := [][]byte{
		rawTx(t, badExec),
		rawTx(t, badExec, deposit),
		rawTx(t, badDeposit),
		rawTx(t, execOf(t, badDeposit)),
	}
	fail := resultOf(inMsg(sdkerrors.ErrUnauthorized, 0), 100000, 40000, anteEvents("100utia")...)
	results := []*abci.ExecTxResult{fail, fail, fail, fail}
	dir := t.TempDir()
	s := failScanner(t, dir, txs, results, nil)
	if n := s.processBlock(context.Background(), 100); n != 0 {
		t.Fatalf("processBlock = %d", n)
	}
	got := readFailed(t, dir)
	if len(got) != 3 {
		t.Fatalf("%d lines, want 3: %+v", len(got), got)
	}
	want := [][]failedtx.Msg{
		{{Index: 0, TypeURL: failedtx.ExecTypeURL}, {Index: 1, TypeURL: msgDepositTypeURL, Signer: owner, Detail: &failedtx.MsgDetail{Publisher: owner, Amount: "5utia"}}},
		{{Index: 0, TypeURL: msgDepositTypeURL}},
		{{Index: 0, TypeURL: failedtx.ExecTypeURL, Inner: []failedtx.Msg{{Index: 0, TypeURL: msgDepositTypeURL}}}},
	}
	for i, r := range got {
		if r.TxIndex != i+1 || !reflect.DeepEqual(r.Messages, want[i]) {
			t.Errorf("line %d: tx %d messages %+v", i+1, r.TxIndex, r.Messages)
		}
	}
	logs := s.log.lines()
	for _, w := range []string{"h=100 tx=0: failed tx: msg 0: MsgExec did not decode", "h=100 tx=1: failed tx: msg 0: MsgExec did not decode",
		"h=100 tx=2: failed tx: msg 0: MsgDepositToEscrow did not decode", "h=100 tx=3: failed tx: msg 0 inner 0: MsgDepositToEscrow did not decode"} {
		if !strings.Contains(logs, w) {
			t.Errorf("not logged: %q", w)
		}
	}
}

// Case 6: a line of failed_txs.jsonl that does not decode stops the open,
// naming the file and the line, as one in publications.jsonl does: the
// checkpoint never passes a failure the record lost.
func TestAnUndecodableFailedTxLineStopsTheOpen(t *testing.T) {
	dir := t.TempDir()
	body := `{"schema_version":1,"dedupe_key":"h5:0","height":5}` + "\n" + `{"schema_version":1,"dedupe_` + "x\n" + `{"schema_version":1,"dedupe_key":"h6:0","height":6}` + "\n"
	if err := os.WriteFile(filepath.Join(dir, failedtx.FileName), []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	st, err := OpenStore(dir)
	if err == nil {
		st.Close()
		t.Fatal("a store opened over an undecodable failed_txs.jsonl line")
	}
	if !strings.Contains(err.Error(), failedtx.FileName) || !strings.Contains(err.Error(), "line 2") {
		t.Fatalf("the error does not name the file and line: %v", err)
	}
}

// Case 4: a block read again, in the same process or after a restart below
// it, adds no line; the seen-set holds only the keys above the checkpoint;
// a torn last line is cut at open and written again whole.
func TestAFailedTxIsRecordedOnceAndItsKeyForgottenAtTheCheckpoint(t *testing.T) {
	cases := failedCases(t, secp256k1.GenPrivKey())
	txs := [][]byte{cases[0].raw, cases[1].raw}
	results := []*abci.ExecTxResult{cases[0].res, cases[1].res}
	dir := t.TempDir()
	path := filepath.Join(dir, failedtx.FileName)
	s := failScanner(t, dir, txs, results, nil)
	ctx := context.Background()
	k0, k1 := failedtx.Key(100, 0), failedtx.Key(100, 1)
	keysOnce := func(when string) {
		t.Helper()
		got := readFailed(t, dir)
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
	if !s.store.FailedTxSeen(k0) || !s.store.FailedTxSeen(k1) {
		t.Fatal("a checkpoint below the block forgot its keys")
	}
	s.store.Close()
	st, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	s.store = st
	if !st.FailedTxSeen(k0) || !st.FailedTxSeen(k1) {
		t.Fatal("the keys above the checkpoint were not loaded")
	}
	s.processBlock(ctx, 100)
	keysOnce("the block read again after a restart")

	// The checkpoint at the block: the set empties, and a restart loads none.
	if err := st.SaveState(PersistState{ChainID: "test-1", LastScannedHeight: 100}); err != nil {
		t.Fatal(err)
	}
	if len(st.failSeen) != 0 || st.FailedTxSeen(k0) {
		t.Fatalf("the checkpoint kept %d keys", len(st.failSeen))
	}
	st.Close()
	st, err = OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	s.store = st
	if len(st.failSeen) != 0 {
		t.Fatalf("a restart at the checkpoint loaded %d keys", len(st.failSeen))
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
	if !st.FailedTxSeen(k0) || st.FailedTxSeen(k1) {
		t.Fatal("after the cut, only the whole line's key is on record")
	}
	s.processBlock(ctx, 100)
	keysOnce("the torn line written again")
	if n := wholeLines(t, path); n != 2 {
		t.Fatalf("%d lines", n)
	}
}

// Case 3, the proof that a failure changes nothing else: block B is block A
// (a settlement, a deposit, a host registration) with every failed tx of
// case 2 after it. The failed host registration carries a registration
// event and the failed settlement a params update, as a success would. The
// publications, payments, host history and state the scanner writes are
// the same bytes for both, but for the wall clock in recorded_at, and so
// is processBlock's count. Only B has a failed_txs.jsonl.
func TestFailuresChangeNoOtherRecord(t *testing.T) {
	key := secp256k1.GenPrivKey()
	owner, err := bech32.ConvertAndEncode(accountHRP, key.PubKey().Address())
	if err != nil {
		t.Fatal(err)
	}
	val := cmttypes.NewValidator(cmted25519.GenPrivKey().PubKey(), 10)
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
	added := 0
	for _, c := range failedCases(t, key) {
		if c.res.Code == 0 {
			continue
		}
		bTxs = append(bTxs, c.raw)
		bRes = append(bRes, c.res)
		if c.want != nil {
			added++
		}
	}

	run := func(txs [][]byte, res []*abci.ExecTxResult) (string, int) {
		dir := t.TempDir()
		s := failScanner(t, dir, txs, res, val)
		n := s.processBlock(context.Background(), 100)
		s.checkpoint(100)
		return dir, n
	}
	dirA, nA := run(aTxs, aRes)
	dirB, nB := run(bTxs, bRes)
	if nA != nB || nA != 3 {
		t.Fatalf("processBlock counted %d for A and %d for B, want 3 (a publication, a deposit, a charge)", nA, nB)
	}
	stamp := regexp.MustCompile(`"recorded_at":"[^"]*"`)
	for _, f := range []string{"publications.jsonl", "payments.jsonl", "host_history.jsonl", "state.json"} {
		a, err := os.ReadFile(filepath.Join(dirA, f))
		if err != nil {
			t.Fatal(err)
		}
		b, err := os.ReadFile(filepath.Join(dirB, f))
		if err != nil {
			t.Fatal(err)
		}
		if len(a) == 0 {
			t.Fatalf("%s is empty: the proof needs a record in it", f)
		}
		a = stamp.ReplaceAll(a, []byte(`"recorded_at":"T"`))
		b = stamp.ReplaceAll(b, []byte(`"recorded_at":"T"`))
		if string(a) != string(b) {
			t.Errorf("%s differs:\nA %s\nB %s", f, a, b)
		}
	}
	if _, err := os.Stat(filepath.Join(dirA, failedtx.FileName)); !os.IsNotExist(err) {
		t.Fatalf("block A wrote %s: %v", failedtx.FileName, err)
	}
	if got := readFailed(t, dirB); len(got) != added {
		t.Fatalf("block B recorded %d failures, want %d", len(got), added)
	}
}

// Case 1: a block_results answer from Mocha (testdata/README.md), served as
// the node served it. The scanner reads every tx's codespace, log and gas
// as the answer holds them, and the ante handler's fee event as well.
func TestARealBlockResultsAnswerIsReadWhole(t *testing.T) {
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
	var want struct {
		Height     string `json:"height"`
		TxsResults []struct {
			Code      uint32 `json:"code"`
			Log       string `json:"log"`
			Codespace string `json:"codespace"`
			GasWanted string `json:"gas_wanted"`
			GasUsed   string `json:"gas_used"`
			Events    []struct {
				Type       string `json:"type"`
				Attributes []struct {
					Key   string `json:"key"`
					Value string `json:"value"`
				} `json:"attributes"`
			} `json:"events"`
		} `json:"txs_results"`
	}
	if err := json.Unmarshal(env.Result, &want); err != nil {
		t.Fatal(err)
	}
	height, err := strconv.ParseInt(want.Height, 10, 64)
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
	n := len(want.TxsResults)
	if n == 0 || len(res.TxCodes) != n || len(res.TxEvents) != n || len(res.TxCodespace) != n || len(res.TxLog) != n || len(res.TxGasWanted) != n || len(res.TxGasUsed) != n {
		t.Fatalf("%d results in the answer; read %d codes, %d events, %d codespaces, %d logs, %d/%d gas", n,
			len(res.TxCodes), len(res.TxEvents), len(res.TxCodespace), len(res.TxLog), len(res.TxGasWanted), len(res.TxGasUsed))
	}
	failed := 0
	for i, w := range want.TxsResults {
		gw, err := strconv.ParseInt(w.GasWanted, 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		gu, err := strconv.ParseInt(w.GasUsed, 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		if res.TxCodes[i] != w.Code || res.TxCodespace[i] != w.Codespace || res.TxLog[i] != w.Log || res.TxGasWanted[i] != gw || res.TxGasUsed[i] != gu {
			t.Errorf("tx %d read as %d %q %q %d/%d, the answer holds %d %q %q %d/%d", i,
				res.TxCodes[i], res.TxCodespace[i], res.TxLog[i], res.TxGasWanted[i], res.TxGasUsed[i], w.Code, w.Codespace, w.Log, gw, gu)
		}
		var wantPassed bool
		var wantFee string
	events:
		for _, ev := range w.Events {
			if ev.Type != "tx" {
				continue
			}
			for _, a := range ev.Attributes {
				if a.Key == "fee" {
					wantPassed, wantFee = true, a.Value
					break events
				}
			}
		}
		if passed, fee := anteFee(res.TxEvents[i]); passed != wantPassed || fee != wantFee {
			t.Errorf("tx %d: anteFee = %v %q, the answer holds %v %q", i, passed, fee, wantPassed, wantFee)
		}
		if w.Code != 0 {
			failed++
		}
	}
	if failed == 0 {
		t.Fatal("the fixture holds no failed tx")
	}

	// The failed tx in it ran out of gas after its ante passed: its fee was
	// taken, and its log reads as out of gas with no message named.
	if res.TxCodes[0] != 11 || res.TxCodespace[0] != "sdk" {
		t.Fatalf("tx 0: %s/%d", res.TxCodespace[0], res.TxCodes[0])
	}
	if passed, fee := anteFee(res.TxEvents[0]); !passed || fee != "213utia" {
		t.Fatalf("tx 0 fee: %v %q", passed, fee)
	}
	if stored, cut := failedtx.CutLog(res.TxCodespace[0], res.TxCodes[0], res.TxLog[0]); cut || stored != res.TxLog[0] {
		t.Fatalf("an out-of-gas log was cut: %q", stored)
	}
	r := failedtx.Record{AppVersion: FibreAppVersion, Code: res.TxCodes[0], Codespace: res.TxCodespace[0], Log: res.TxLog[0]}
	if e := failedtx.Explain(r); e.Reason != "Out of gas" || e.MsgIndex != -1 {
		t.Fatalf("Explain = %+v", e)
	}
}

// Case 7: BlockResults copies each tx's codespace, log and gas, in the
// order of TxCodes, and a gas figure the node gives as negative stays so.
func TestBlockResultsKeepsEachTxsCodespaceLogAndGas(t *testing.T) {
	results := []*abci.ExecTxResult{
		{Code: 0, GasWanted: 100, GasUsed: 90},
		{Code: 5, Codespace: "sdk", Log: "insufficient funds", GasWanted: 200, GasUsed: 150, Events: anteEvents("2utia")},
		{Code: 2, Codespace: "valaddr", Log: "invalid validator", GasWanted: -1, GasUsed: 300},
	}
	node := newRPCNode(t, func(method string, h int64) (any, string) {
		if method == "block_results" {
			return coretypes.ResultBlockResults{Height: h, TxsResults: results}, ""
		}
		return nil, "unexpected " + method
	})
	c, err := NewChain(node.srv.URL, 2*time.Second, NewLogger(10))
	if err != nil {
		t.Fatal(err)
	}
	res, err := c.BlockResults(context.Background(), 7)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.TxCodes) != 3 || len(res.TxCodespace) != 3 || len(res.TxLog) != 3 || len(res.TxGasWanted) != 3 || len(res.TxGasUsed) != 3 {
		t.Fatalf("lengths: %+v", res)
	}
	for i, r := range results {
		if res.TxCodes[i] != r.Code || res.TxCodespace[i] != r.Codespace || res.TxLog[i] != r.Log || res.TxGasWanted[i] != r.GasWanted || res.TxGasUsed[i] != r.GasUsed {
			t.Errorf("tx %d: %d %q %q %d/%d", i, res.TxCodes[i], res.TxCodespace[i], res.TxLog[i], res.TxGasWanted[i], res.TxGasUsed[i])
		}
	}
	if passed, fee := anteFee(res.TxEvents[1]); !passed || fee != "2utia" {
		t.Errorf("tx 1 fee: %v %q", passed, fee)
	}
	if passed, fee := anteFee(res.TxEvents[2]); passed || fee != "" {
		t.Errorf("tx 2, no ante event: %v %q", passed, fee)
	}
}

// anteFee takes the first tx event with a fee attribute, and nothing from
// another event type or another attribute.
func TestAnteFee(t *testing.T) {
	for _, c := range []struct {
		name   string
		evs    []abci.Event
		passed bool
		fee    string
	}{
		{"none", nil, false, ""},
		{"a zero fee", anteEvents(""), true, ""},
		{"a fee", anteEvents("2000utia"), true, "2000utia"},
		{"a fee attribute on another event", []abci.Event{{Type: "transfer", Attributes: []abci.EventAttribute{{Key: "fee", Value: "1utia"}}}}, false, ""},
		{"a tx event without a fee", []abci.Event{{Type: "tx", Attributes: []abci.EventAttribute{{Key: "acc_seq", Value: "a/1"}}}}, false, ""},
		{"the first one", append(anteEvents("1utia"), anteEvents("2utia")...), true, "1utia"},
	} {
		if passed, fee := anteFee(c.evs); passed != c.passed || fee != c.fee {
			t.Errorf("%s: %v %q", c.name, passed, fee)
		}
	}
}
