package scan

import (
	"context"
	"sync"
	"testing"
	"time"

	fibretypes "github.com/celestiaorg/celestia-app/v10/x/fibre/types"
	abci "github.com/cometbft/cometbft/abci/types"
	cmted25519 "github.com/cometbft/cometbft/crypto/ed25519"
	cmtversion "github.com/cometbft/cometbft/proto/tendermint/version"
	coretypes "github.com/cometbft/cometbft/rpc/core/types"
	cmttypes "github.com/cometbft/cometbft/types"
	"github.com/cosmos/cosmos-sdk/crypto/keys/secp256k1"
	"github.com/cosmos/cosmos-sdk/types/bech32"
)

// payBlockScanner is a scanner on a node whose block 100 holds one
// MsgPayForFibre (promise height 42, one validator). stopOn names the
// method whose call stops the scan: the node cancels the scan's context
// while it answers that call, and the call fails, as a SIGTERM landing in
// the middle of it does. The scanner's host history is seeded and empty,
// so the validator is a newcomer whose registration is read on its own.
func payBlockScanner(t *testing.T, stopOn string) (*Scanner, context.Context, string) {
	t.Helper()
	key := secp256k1.GenPrivKey()
	owner, err := bech32.ConvertAndEncode(accountHRP, key.PubKey().Address())
	if err != nil {
		t.Fatal(err)
	}
	raw := rawTx(t, payMsg{&fibretypes.MsgPayForFibre{Signer: owner, PaymentPromise: testPromise(t, key, 1<<20)}})
	val := cmttypes.NewValidator(cmted25519.GenPrivKey().PubKey(), 10)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	var once sync.Once
	node := newRPCNode(t, func(method string, height int64) (any, string) {
		if method == stopOn {
			once.Do(cancel)
			return nil, "connection reset by peer"
		}
		switch method {
		case "block":
			return coretypes.ResultBlock{Block: &cmttypes.Block{
				Header: cmttypes.Header{Version: cmtversion.Consensus{Block: 11, App: FibreAppVersion}, ChainID: "test-1", Height: height,
					Time: time.Date(2026, 9, 24, 18, 0, 0, 0, time.UTC)},
				Data: cmttypes.Data{Txs: cmttypes.Txs{raw}},
			}}, ""
		case "block_results":
			return coretypes.ResultBlockResults{Height: height, TxsResults: []*abci.ExecTxResult{{Code: 0}}}, ""
		case "validators":
			return coretypes.ResultValidators{BlockHeight: height, Validators: []*cmttypes.Validator{val}, Count: 1, Total: 1}, ""
		case "status":
			return statusJSON(1, 200), ""
		}
		return nil, "unexpected " + method
	})
	s, err := New(Config{RPCURL: node.srv.URL, DataDir: t.TempDir(), RPCTimeout: 2 * time.Second, StoreRows: true}, NewLogger(200))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.store.Close() })
	s.chainID = "test-1"
	s.params = NewParamHistory(1, fibretypes.DefaultParams())
	s.hosts = LoadHostHistory(nil, true, 1)
	return s, ctx, hexstr(cmttypes.Tx(raw).Hash())
}

// A stop while the validator set at the promise height is read is a stop,
// as one during the block read is: the block is not finished, nothing of
// it is recorded as a gap, and the scan stops before it. It was a crash
// dump telling the operator to skip a healthy block (and here, the test
// binary exiting with it).
func TestAStopWhileAValidatorSetIsReadStopsCleanly(t *testing.T) {
	s, ctx, txHash := payBlockScanner(t, "validators")
	if n := s.processBlock(ctx, 100); n != blockStopped {
		t.Fatalf("processBlock = %d, want blockStopped", n)
	}
	if s.store.Seen(txHash) || len(s.gaps) != 0 {
		t.Fatalf("a stop left a record: seen=%v gaps=%+v", s.store.Seen(txHash), s.gaps)
	}
}

// A stop while a newcomer's registration is read: the read fails, so the
// host would be unknown_no_seed, and that publication was appended with it
// and checkpointed, unknown for good. It is not appended now; the block is
// read again on restart, registration and all.
func TestAStopWhileARegistrationIsReadAppendsNothing(t *testing.T) {
	s, ctx, txHash := payBlockScanner(t, "abci_query")
	if n := s.processBlock(ctx, 100); n != blockStopped {
		t.Fatalf("processBlock = %d, want blockStopped", n)
	}
	if s.store.Seen(txHash) {
		t.Fatal("the publication was appended with the host its stopped read left unknown")
	}

	// The same block with no stop is recorded, as before.
	s2, ctx2, txHash2 := payBlockScanner(t, "none")
	if n := s2.processBlock(ctx2, 100); n < 1 || !s2.store.Seen(txHash2) {
		t.Fatalf("processBlock without a stop = %d, seen=%v", n, s2.store.Seen(txHash2))
	}
}

// A stop while the params seed is read for the first MsgPayForFibre on a
// chain the scanner still calls x/fibre-inactive: the read fails, and that
// was the fatal "params cannot be read" with a skip-heights hint.
func TestAStopWhileTheParamsSeedIsReadStopsCleanly(t *testing.T) {
	s, ctx, txHash := payBlockScanner(t, "abci_query")
	s.fibreInactive = true
	s.activationSeen = true // the seed is tried in the publication loop, not at activation
	if n := s.processBlock(ctx, 101); n != blockStopped {
		t.Fatalf("processBlock = %d, want blockStopped", n)
	}
	if s.store.Seen(txHash) || len(s.gaps) != 0 {
		t.Fatalf("a stop left a record: seen=%v gaps=%+v", s.store.Seen(txHash), s.gaps)
	}
}
