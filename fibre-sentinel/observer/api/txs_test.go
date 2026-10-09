package api

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	errorsmod "cosmossdk.io/errors"
	valaddrtypes "github.com/celestiaorg/celestia-app/v10/x/valaddr/types"
	"github.com/cosmos/cosmos-sdk/types/bech32"
	sdkerrors "github.com/cosmos/cosmos-sdk/types/errors"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/failedtx"
	"github.com/plsgiveup/fibre/fibre-sentinel/internal/scan"
	"github.com/plsgiveup/fibre/fibre-sentinel/internal/txcost"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
)

// The transaction page's lookup, over a store the collector filled from a
// data dir: observer/testdata's record with failedtx_counts_test.go's escrow
// movements and host registrations (ctDataDir), and beside them the
// movements, registrations, failures and cost lines below, each named by
// what it proves. The validators and the withdrawal queue are written to the
// store directly, as the collector's chain polls would.

// tqHash is a transaction hash: 64 hex characters, lower case.
func tqHash(name string) string {
	h := sha256.Sum256([]byte("tx page " + name))
	return hex.EncodeToString(h[:])
}

// tqBech is the bech32 address of twenty bytes b under prefix.
func tqBech(prefix string, b byte) string {
	a, err := bech32.ConvertAndEncode(prefix, bytes.Repeat([]byte{b}, 20))
	if err != nil {
		panic(err)
	}
	return a
}

// observer/testdata's second and third publications: the second settled
// with no cost line on record; under the third's settlement hash a cost
// line that does not decode is stored, and a failed settlement of its
// promise is on record.
const (
	tqPub2Promise = "c5f5b9a384be560f1be39ff41318f4d32f644f8d4c7b845e12f49feb5ec91034"
	tqPub2Tx      = "94d8dc0208587f6cb876fb8cbc53fea33956638b012ed6bd3c642f770e6fb640"
	tqPub3Promise = "07a71cf238b4718c335d0548710d1ea840bc366a49f1c6a88227d1a84cba4615"
	tqPub3Tx      = "5a21b439edb0355b38c40b19bbb7aa293e4a653134729efe37c8d900571d9291"
)

// The validators: A and B register endpoints; the operator C moved from the
// consensus key Old to New, so Old is superseded; None is bonded with no
// registration on record.
var (
	tqValA    = strings.Repeat("a1", 20)
	tqValB    = strings.Repeat("b2", 20)
	tqValOld  = strings.Repeat("c3", 20)
	tqValNew  = strings.Repeat("c4", 20)
	tqValNone = strings.Repeat("d5", 20)

	tqOpA        = tqBech("celestiavaloper", 0xa1)
	tqOpB        = tqBech("celestiavaloper", 0xb2)
	tqOpC        = tqBech("celestiavaloper", 0xc3)
	tqOpNone     = tqBech("celestiavaloper", 0xd5)
	tqOpStranger = tqBech("celestiavaloper", 0x99) // the operator form of an account no validator has
	tqGrantee    = tqBech("celestia", 0xee)        // a grantee that pays for a delegated registration
	tqStranger   = tqBech("celestia", 0x77)        // an account with no publisher page
)

// The successes.
var (
	tqSettleNoPub = tqHash("a settlement whose publication is not on record")
	tqWdPending   = tqHash("a withdrawal request still queued")
	tqWdPaid      = tqHash("a withdrawal request paid out")
	tqWdConsumed  = tqHash("a withdrawal request settlements used")
	tqWdUnattr    = tqHash("a withdrawal request the observer cannot attribute")
	tqReg         = tqHash("A's first registration")
	tqChg         = tqHash("A's change")
	tqSame        = tqHash("A's same address again")
	tqExecHost    = tqHash("B's change through a grantee")
	tqTwoHosts    = tqHash("A and B in one transaction")
	tqMixed       = tqHash("a deposit and B's registration")
	tqGhost       = tqHash("a deposit whose row is not on record")
)

// The failures.
var (
	tqFPFF          = tqHash("a final failed settlement of a promise that settled later")
	tqFPFFOpen      = tqHash("that settlement, stopped before the ante")
	tqFPFFStranger  = tqHash("a final failed settlement naming an account with no page")
	tqFDepBig       = tqHash("a final failed deposit above 2^53 utia")
	tqFDep          = tqHash("a final failed deposit")
	tqFTimeout      = tqHash("a final failed timeout")
	tqFCut          = tqHash("a final failed settlement whose strings were cut")
	tqFHost         = tqHash("a final failed registration by A's operator")
	tqFHostExec     = tqHash("that registration inside a MsgExec")
	tqFHostStranger = tqHash("a final failed registration by no validator")
	tqFHostSuper    = tqHash("a final failed registration by the operator of a superseded key")
	tqFTwoHosts     = tqHash("a final failed registration by A's and B's operators")
	tqFTwoHostsOpen = tqHash("that registration by both, stopped before the ante")
	tqFHostTwice    = tqHash("a final failed transaction with two of A's registrations")
	tqFBroken       = tqHash("a failure whose record does not decode")
)

// tqHosts are the endpoints the registrations name.
const (
	tqHostA1 = "10.0.0.1:7980"
	tqHostA2 = "10.0.0.2:7980"
	tqHostA3 = "10.0.0.3:7980"
	tqHostB1 = "10.0.1.1:7980"
	tqHostB2 = "10.0.1.2:7980"
	tqHostB3 = "10.0.1.3:7980"
	tqHostB4 = "10.0.1.4:7980"
	tqHostO1 = "10.0.2.1:7980"
	tqHostN1 = "10.0.2.2:7980"
)

func tqMsg(i int, kind, signer string) failedtx.Msg {
	return failedtx.Msg{Index: i, TypeURL: kindTypeURL[kind], Signer: signer}
}

func tqHostMsg(i int, op, host string) failedtx.Msg {
	return failedtx.Msg{Index: i, TypeURL: kindTypeURL[failedtx.KindSetHost], Signer: op, Detail: &failedtx.MsgDetail{Host: host}}
}

func tqPFFMsg(pub, promise string) failedtx.Msg {
	return failedtx.Msg{Index: 0, TypeURL: kindTypeURL[failedtx.KindSettlement], Signer: pub,
		Detail: &failedtx.MsgDetail{Publisher: pub, PromiseHash: promise, Namespace: ctNamespace, BlobSize: 4096}}
}

// tqCost is the cost line the scanner writes for the successful
// transaction hash at (h, i).
func tqCost(hash string, h int64, i int, payer string, msgs ...failedtx.Msg) txcost.Record {
	at := ctBlockTime(h)
	return txcost.Record{SchemaVersion: txcost.SchemaVersion, DedupeKey: txcost.Key(h, i), Height: h, TxIndex: i, Time: at, TxHash: hash,
		GasWanted: 200000, GasUsed: 50000 + h, Fee: "2000utia", FeePayer: payer, Messages: msgs, RecordedAt: at.Add(time.Second)}
}

func tqPay(hash, kind string, h int64, i, msg int, amount uint64) scan.Payment {
	at := ctBlockTime(h)
	return scan.Payment{SchemaVersion: 1, DedupeKey: fmt.Sprintf("%s:%d", hash, msg), Kind: kind, Height: h, Time: at, TxHash: hash,
		TxIndex: i, MsgIndex: msg, Publisher: ctPublisher, Denom: "utia", AmountUtia: amount, RecordedAt: at.Add(time.Second)}
}

func tqEvent(cons string, h int64, fromTx int, host, source string) scan.HostEvent {
	return scan.HostEvent{HostEntry: scan.HostEntry{FromHeight: h, FromTxIndex: fromTx, ConsAddress: cons, Host: host, Source: source},
		Time: ctBlockTime(h)}
}

func tqPayments() []any {
	settle := tqPay(tqSettleNoPub, scan.PaymentSettlement, 70, 0, 0, 5000)
	settle.Processor, settle.PromiseHash, settle.Namespace, settle.BlobSize = ctPublisher, strings.Repeat("ab", 32), ctNamespace, 1000
	out := []any{settle}
	for _, w := range []struct {
		hash   string
		h      int64
		amount uint64
	}{{tqWdPending, 71, 2_000_000}, {tqWdPaid, 72, 3_000_000}, {tqWdConsumed, 73, 4_000_000}, {tqWdUnattr, 74, 5_000_000}} {
		p := tqPay(w.hash, scan.PaymentWithdrawalRequest, w.h, 0, 0, w.amount)
		avail := ctBlockTime(w.h).Add(12 * time.Hour)
		p.AvailableAt = &avail
		out = append(out, p)
	}
	return append(out, tqPay(tqMixed, scan.PaymentDeposit, 66, 0, 0, 1_000_000))
}

func tqHostEvents() []any {
	return []any{
		tqEvent(tqValA, 1, -1, "", scan.HostNone),
		tqEvent(tqValA, 50, 1, tqHostA1, scan.HostFromEvent),
		tqEvent(tqValA, 55, 3, tqHostA2, scan.HostFromEvent),
		tqEvent(tqValA, 58, 1, tqHostA2, scan.HostFromEvent),
		tqEvent(tqValA, 64, 1, tqHostA3, scan.HostFromEvent),
		tqEvent(tqValB, 1, -1, tqHostB1, scan.HostFromSeed),
		tqEvent(tqValB, 62, 1, tqHostB2, scan.HostFromEvent),
		tqEvent(tqValB, 64, 1, tqHostB3, scan.HostFromEvent),
		tqEvent(tqValB, 66, 1, tqHostB3, scan.HostFromEvent),
		tqEvent(tqValB, 68, 1, tqHostB4, scan.HostFromEvent), // its transaction's cost is not on record
		tqEvent(tqValOld, 1, -1, tqHostO1, scan.HostFromSeed),
		tqEvent(tqValNew, 1, -1, tqHostN1, scan.HostFromSeed),
	}
}

func tqCosts() []any {
	pff := tqMsg(0, failedtx.KindSettlement, ctPublisher)
	return []any{
		tqCost(ctSettledTx, 32, 0, ctPublisher, pff),
		tqCost(tqSettleNoPub, 70, 0, ctPublisher, pff),
		tqCost(ctDepositTx, 20, 1, ctPublisher, failedtx.Msg{Index: 0, TypeURL: "/cosmos.bank.v1beta1.MsgSend"}, tqMsg(1, failedtx.KindDeposit, ctPublisher)),
		tqCost(tqWdPending, 71, 0, ctPublisher, tqMsg(0, failedtx.KindWithdrawalRequest, ctPublisher)),
		tqCost(tqWdPaid, 72, 0, ctPublisher, tqMsg(0, failedtx.KindWithdrawalRequest, ctPublisher)),
		tqCost(ctTimeoutTx, 60, 0, ctPublisher, tqMsg(0, failedtx.KindTimeout, ctPublisher)),
		tqCost(tqReg, 50, 0, tqBech("celestia", 0xa1), tqMsg(0, failedtx.KindSetHost, tqOpA)),
		tqCost(tqChg, 55, 2, tqBech("celestia", 0xa1), tqMsg(0, failedtx.KindSetHost, tqOpA)),
		tqCost(tqSame, 58, 0, tqBech("celestia", 0xa1), tqMsg(0, failedtx.KindSetHost, tqOpA)),
		tqCost(tqExecHost, 62, 0, tqGrantee,
			failedtx.Msg{Index: 0, TypeURL: failedtx.ExecTypeURL, Inner: []failedtx.Msg{tqMsg(0, failedtx.KindSetHost, tqOpB)}}),
		tqCost(tqTwoHosts, 64, 0, tqBech("celestia", 0xa1), tqMsg(0, failedtx.KindSetHost, tqOpA), tqMsg(1, failedtx.KindSetHost, tqOpB)),
		tqCost(tqMixed, 66, 0, ctPublisher, tqMsg(0, failedtx.KindDeposit, ctPublisher), tqMsg(1, failedtx.KindSetHost, tqOpB)),
		tqCost(tqGhost, 67, 0, ctPublisher, tqMsg(0, failedtx.KindDeposit, ctPublisher)),
	}
}

// tqFailureRecords are the failures, each with the chain's own error value
// passed through ABCIInfo as the node does (ctFailure).
func tqFailureRecords() []failedtx.Record {
	invalid := errorsmod.Wrap(sdkerrors.ErrInvalidRequest, "payment promise height must be positive")
	short := errorsmod.Wrapf(errorsmod.Wrapf(sdkerrors.ErrInsufficientFunds, "spendable balance %s is smaller than %s", "10utia", "50000000utia"),
		"failed to transfer funds to escrow")
	noValidator := func(op string) error {
		return ctAtMessage(valaddrtypes.ErrInvalidValidator.Wrapf("validator not found: %v", op), 0)
	}
	deposit := func(amount string) failedtx.Msg {
		return failedtx.Msg{Index: 0, TypeURL: kindTypeURL[failedtx.KindDeposit], Signer: ctPublisher,
			Detail: &failedtx.MsgDetail{Publisher: ctPublisher, Amount: amount}}
	}
	timeout := failedtx.Msg{Index: 0, TypeURL: kindTypeURL[failedtx.KindTimeout], Signer: tqGrantee,
		Detail: &failedtx.MsgDetail{Publisher: ctPublisher, PromiseHash: strings.Repeat("7e", 32)}}
	cut := tqPFFMsg(ctPublisher, strings.Repeat("cd", 32))
	cut.Cut = true
	exec := failedtx.Msg{Index: 0, TypeURL: failedtx.ExecTypeURL, Inner: []failedtx.Msg{tqHostMsg(0, tqOpA, "10.9.9.8:7980")}}
	two := []failedtx.Msg{tqHostMsg(0, tqOpA, "10.9.9.1:7980"), tqHostMsg(1, tqOpB, "10.9.9.2:7980")}
	return []failedtx.Record{
		// the deposit's hash, stopped before the ante at a lower height: the success wins
		ctFailure(ctDepositTx, 19, 0, invalid, false, "", deposit("50000000utia")),
		ctFailure(tqFPFF, 75, 0, ctAtMessage(invalid, 0), true, "2000utia", tqPFFMsg(ctPublisher, tqPub3Promise)),
		ctFailure(tqFPFFOpen, 76, 0, invalid, false, "", tqPFFMsg(ctPublisher, tqPub3Promise)),
		ctFailure(tqFPFFStranger, 77, 0, ctAtMessage(invalid, 0), true, "2000utia", tqPFFMsg(tqStranger, strings.Repeat("cd", 32))),
		ctFailure(tqFDepBig, 78, 0, ctAtMessage(short, 0), true, "2000utia", deposit("9007199254740993utia")),
		ctFailure(tqFDep, 79, 0, ctAtMessage(short, 0), true, "2000utia", deposit("1000000utia")),
		ctFailure(tqFTimeout, 84, 0, ctAtMessage(invalid, 0), true, "2000utia", timeout),
		ctFailure(tqFCut, 85, 0, ctAtMessage(invalid, 0), true, "2000utia", cut),
		ctFailure(tqFHost, 86, 0, noValidator(tqOpA), true, "2000utia", tqHostMsg(0, tqOpA, "10.9.9.9:7980")),
		ctFailure(tqFHostExec, 87, 0, noValidator(tqOpA), true, "2000utia", exec),
		ctFailure(tqFHostStranger, 88, 0, noValidator(tqOpStranger), true, "2000utia", tqHostMsg(0, tqOpStranger, "10.9.9.7:7980")),
		ctFailure(tqFHostSuper, 89, 0, noValidator(tqOpC), true, "2000utia", tqHostMsg(0, tqOpC, "10.9.9.6:7980")),
		ctFailure(tqFTwoHosts, 90, 0, noValidator(tqOpA), true, "2000utia", two...),
		ctFailure(tqFTwoHostsOpen, 91, 0, invalid, false, "", two...),
		ctFailure(tqFHostTwice, 92, 0, noValidator(tqOpA), true, "2000utia", tqHostMsg(0, tqOpA, "10.9.8.1:7980"), tqHostMsg(1, tqOpA, "10.9.8.2:7980")),
	}
}

// tqRecord is the failure record of hash.
func tqRecord(t *testing.T, hash string) failedtx.Record {
	t.Helper()
	for _, r := range tqFailureRecords() {
		if r.TxHash == hash && r.AntePassed {
			return r
		}
	}
	for _, r := range tqFailureRecords() {
		if r.TxHash == hash {
			return r
		}
	}
	t.Fatalf("no failure %s", hash)
	return failedtx.Record{}
}

// tqAppend appends lines to a record file ctDataDir wrote.
func tqAppend(t *testing.T, path string, vs ...any) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	for _, v := range vs {
		b, err := json.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.Write(append(b, '\n')); err != nil {
			t.Fatal(err)
		}
	}
}

func tqExec(t *testing.T, st *store.Store, q string, args ...any) {
	t.Helper()
	if _, err := st.DB().Exec(q, args...); err != nil {
		t.Fatalf("%s: %v", q, err)
	}
}

// tqFix is the fixture's store and the API over it.
type tqFix struct {
	st  *store.Store
	srv *Server
	now time.Time
}

func tqFixture(t *testing.T) tqFix {
	t.Helper()
	now := time.Date(2026, 9, 7, 0, 0, 0, 0, time.UTC)
	dir := ctDataDir(t, false)
	tqAppend(t, filepath.Join(dir, "payments.jsonl"), tqPayments()...)
	tqAppend(t, filepath.Join(dir, "host_history.jsonl"), tqHostEvents()...)
	var fails []any
	for _, r := range tqFailureRecords() {
		fails = append(fails, r)
	}
	ctWriteLines(t, filepath.Join(dir, failedtx.FileName), fails...)
	ctWriteLines(t, filepath.Join(dir, txcost.FileName), tqCosts()...)
	st := ctCollect(t, dir, now)
	if n := ctCount(t, st, "tx_costs"); n != int64(len(tqCosts())) {
		t.Fatalf("%d cost rows, want %d", n, len(tqCosts()))
	}

	// The staking set as the collector reads it: C's newer key is the one
	// its operator address stands for now.
	ts := func(d time.Duration) string { return store.TS(now.Add(d)) }
	for _, v := range []struct{ cons, op, moniker, identity, updated string }{
		{tqValA, tqOpA, "Alpha", "AAAA1111", ts(0)},
		{tqValB, tqOpB, "Beta", "", ts(0)},
		{tqValOld, tqOpC, "Old key", "", ts(-48 * time.Hour)},
		{tqValNew, tqOpC, "New key", "", ts(0)},
		{tqValNone, tqOpNone, "Unregistered", "", ts(0)},
	} {
		tqExec(t, st, `INSERT INTO validator_identities (cons_address, operator_address, moniker, identity, status, first_seen_at, updated_at)
			VALUES (?, ?, ?, ?, 'BOND_STATUS_BONDED', ?, ?)`, v.cons, v.op, v.moniker, v.identity, ts(-72*time.Hour), v.updated)
	}
	tqExec(t, st, `INSERT INTO validator_avatars (identity, status, checked_at) VALUES ('AAAA1111', 'ok', ?)`, ts(0))

	// The withdrawal queue as the collector reads it from state: one still
	// queued, one paid out, one settlements used (part of it before), one
	// the observer could not attribute.
	bt := func(h int64) string { return store.TS(ctBlockTime(h)) }
	q := `INSERT INTO withdrawal_queue (publisher, requested_at, available_at, denom, first_amount_utia, amount_utia,
		first_seen_height, first_seen_at, last_seen_height, last_seen_at, gone_height, gone_at, outcome, outcome_reason,
		paid_key, paid_height, paid_at, paid_utia, recorded_at) VALUES (?, ?, ?, 'utia', ?, ?, ?, ?, ?, ?, ?, ?, ?, '', ?, ?, ?, ?, ?)`
	avail := func(h int64) string { return store.TS(ctBlockTime(h).Add(12 * time.Hour)) }
	tqExec(t, st, q, ctPublisher, bt(71), avail(71), 2_000_000, 2_000_000, 71, bt(71), 71, bt(71), nil, nil, nil, nil, nil, nil, nil, bt(71))
	tqExec(t, st, q, ctPublisher, bt(72), avail(72), 3_000_000, 3_000_000, 72, bt(72), 79, bt(79), 80, bt(80), store.WithdrawalExecuted,
		"payout", 81, bt(81), 3_000_000, bt(81))
	tqExec(t, st, q, ctPublisher, bt(73), avail(73), 4_000_000, 1_000_000, 73, bt(73), 81, bt(81), 82, bt(82), store.WithdrawalConsumed,
		nil, nil, nil, nil, bt(82))
	tqExec(t, st, q, ctPublisher, bt(74), avail(74), 5_000_000, 5_000_000, 74, bt(74), 82, bt(82), 83, bt(83), store.WithdrawalUnattributed,
		nil, nil, nil, nil, bt(83))

	// Rows this build cannot read: a cost line under the third
	// publication's settlement hash, at another position, and a failure.
	tqExec(t, st, `INSERT INTO tx_costs (height, tx_index, dedupe_key, tx_hash, time, raw_json) VALUES (41, 5, 'h41:5', ?, ?, '{')`,
		tqPub3Tx, bt(41))
	tqExec(t, st, `INSERT INTO failed_txs (dedupe_key, tx_hash, height, tx_index, time, code, codespace, raw_json)
		VALUES ('h93:0', ?, 93, 0, ?, 5, 'sdk', '{"schema_version":1,"tx_hash":')`, tqFBroken, bt(93))
	return tqFix{st: st, srv: ctServer(t, st, now), now: now}
}

// tqGot is one answer of the API.
type tqGot struct {
	status int
	cache  string
	body   []byte
	keys   map[string]json.RawMessage
	a      txAnswer
	effect map[string]json.RawMessage
	rel    map[string]json.RawMessage
}

func tqAsk(t *testing.T, srv *Server, path string) tqGot {
	t.Helper()
	resp := ctAsk(srv, path)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	g := tqGot{status: resp.StatusCode, cache: resp.Header.Get("Cache-Control"), body: body}
	if err := json.Unmarshal(body, &g.keys); err != nil {
		t.Fatalf("%s: %v: %s", path, err, body)
	}
	if g.status == 200 && strings.HasPrefix(path, "/v1/txs/") {
		if err := json.Unmarshal(body, &g.a); err != nil {
			t.Fatalf("%s: %v: %s", path, err, body)
		}
		if err := json.Unmarshal(g.keys["effect"], &g.effect); err != nil {
			t.Fatalf("%s: effect: %v: %s", path, err, body)
		}
		if err := json.Unmarshal(g.keys["related"], &g.rel); err != nil {
			t.Fatalf("%s: related: %v: %s", path, err, body)
		}
	}
	return g
}

// tqJSON is v as the API writes it.
func tqJSON(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// tqTx asks for hash's transaction and checks what every answer shares:
// 200, the hash, the status and kind, and the cache policy.
func tqTx(t *testing.T, f tqFix, hash, status, kind string) tqGot {
	t.Helper()
	g := tqAsk(t, f.srv, "/v1/txs/"+hash)
	if g.status != 200 {
		t.Fatalf("%s: %d %s", hash, g.status, g.body)
	}
	if g.a.TxHash != hash || g.a.Status != status || g.a.Kind != kind {
		t.Fatalf("%s: tx_hash %s, status %s, kind %s; want %s %s: %s", hash, g.a.TxHash, g.a.Status, g.a.Kind, status, kind, g.body)
	}
	_, hasFinal := g.keys["final"]
	if hasFinal != (status == "failed") {
		t.Errorf("%s: final present %v on a %s: %s", hash, hasFinal, status, g.body)
	}
	want := "public, max-age=15"
	if g.a.Final != nil && !*g.a.Final {
		want = "no-store"
	}
	if g.cache != want {
		t.Errorf("%s: Cache-Control %q, want %q", hash, g.cache, want)
	}
	return g
}

// tqWant checks the raw value of key in m against want, as the API writes
// want; a nil want is the key absent.
func tqWant(t *testing.T, what string, m map[string]json.RawMessage, key string, want any) {
	t.Helper()
	raw, ok := m[key]
	if want == nil {
		if ok {
			t.Errorf("%s: %s is %s, want it absent", what, key, raw)
		}
		return
	}
	if !ok {
		t.Errorf("%s: %s absent, want %s", what, key, tqJSON(t, want))
		return
	}
	if string(raw) != tqJSON(t, want) {
		t.Errorf("%s: %s is %s, want %s", what, key, raw, tqJSON(t, want))
	}
}

// tqKeys is m's keys, sorted.
func tqKeys(m map[string]json.RawMessage) string {
	var ks []string
	for k := range m {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	return strings.Join(ks, ",")
}

// Every way a transaction is found: by its cost line (each kind, several
// kinds, a delegated registration, a deposit beside another message), by
// its publication or its movements alone before cost lines, and as a
// failure; a hash nothing knows, a bad hash, and a hash written with 0x in
// upper case.
func TestTxPageFindsATransactionEveryWay(t *testing.T) {
	f := tqFixture(t)
	pff := kindTypeURL[failedtx.KindSettlement]

	// A blob payment with its cost line: the publication's effect, and the
	// blob as the blob list carries it.
	g := tqTx(t, f, ctSettledTx, "success", failedtx.KindSettlement)
	if g.a.Height != 32 || g.a.TxIndex != 0 || g.a.Cost == nil || *g.a.Cost != (txCost{GasWanted: 200000, GasUsed: 50032, Fee: "2000utia", FeePayer: ctPublisher}) {
		t.Errorf("settlement: %s", g.body)
	}
	if got, want := tqJSON(t, g.a.Messages), tqJSON(t, []txMsg{{Index: 0, TypeURL: pff, Fibre: true, Signer: ctPublisher}}); got != want {
		t.Errorf("settlement messages %s, want %s", got, want)
	}
	if got := tqKeys(g.effect); got != "blob_size,blob_version,commitment,fee_paid_utia,namespace,promise_hash,settled,timed_out" {
		t.Errorf("settlement effect keys %s", got)
	}
	tqWant(t, "settlement", g.effect, "promise_hash", ctPromise)
	tqWant(t, "settlement", g.effect, "fee_paid_utia", 695000)
	tqWant(t, "settlement", g.effect, "settled", true)
	tqWant(t, "settlement", g.effect, "timed_out", false)
	tqWant(t, "settlement", g.rel, "publisher", ctPublisher)
	list := tqAsk(t, f.srv, "/v1/blobs?tx="+ctSettledTx)
	var rows []map[string]json.RawMessage
	if err := json.Unmarshal(list.keys["blobs"], &rows); err != nil || len(rows) != 1 {
		t.Fatalf("/v1/blobs?tx=: %v %s", err, list.body)
	}
	var blob map[string]json.RawMessage
	if err := json.Unmarshal(g.rel["blob"], &blob); err != nil {
		t.Fatalf("related.blob: %v: %s", err, g.body)
	}
	for _, k := range []string{"promise_hash", "commitment", "blob_version", "must_serve_until", "reconstructable"} {
		if k == "reconstructable" && string(rows[0][k]) == "null" {
			tqWant(t, "related.blob", blob, k, nil)
			continue
		}
		if string(blob[k]) != string(rows[0][k]) {
			t.Errorf("related.blob %s is %s, the blob row's %s", k, blob[k], rows[0][k])
		}
	}
	tqWant(t, "related.blob", blob, "settlement_height", nil)

	// A blob payment whose publication is not on record: its payment row.
	g = tqTx(t, f, tqSettleNoPub, "success", failedtx.KindSettlement)
	if got, want := string(g.keys["effect"]), tqJSON(t, map[string]any{"promise_hash": strings.Repeat("ab", 32), "namespace": ctNamespace,
		"blob_size": 1000, "fee_paid_utia": 5000}); got != want {
		t.Errorf("settlement without its publication: effect %s, want %s", got, want)
	}
	if got, want := string(g.keys["related"]), tqJSON(t, txRelated{Publisher: ctPublisher}); got != want {
		t.Errorf("settlement without its publication: related %s, want %s", got, want)
	}

	// [MsgSend, deposit]: both messages, the one deposit row's amount; and
	// a failure of the same hash at a lower height loses to the success.
	g = tqTx(t, f, ctDepositTx, "success", failedtx.KindDeposit)
	if got, want := tqJSON(t, g.a.Messages), tqJSON(t, []txMsg{{Index: 0, TypeURL: "/cosmos.bank.v1beta1.MsgSend"},
		{Index: 1, TypeURL: kindTypeURL[failedtx.KindDeposit], Fibre: true, Signer: ctPublisher}}); got != want {
		t.Errorf("deposit messages %s, want %s", got, want)
	}
	if g.a.Height != 20 || g.a.TxIndex != 1 || g.a.Failure != nil {
		t.Errorf("deposit: %s", g.body)
	}
	if got, want := string(g.keys["effect"]), `{"amount_utia":50000000}`; got != want {
		t.Errorf("deposit effect %s, want %s", got, want)
	}
	tqWant(t, "deposit", g.rel, "publisher", ctPublisher)

	// A timeout: the amount charged and the promise.
	g = tqTx(t, f, ctTimeoutTx, "success", failedtx.KindTimeout)
	if got, want := string(g.keys["effect"]), tqJSON(t, map[string]any{"amount_utia": 695000, "promise_hash": strings.Repeat("7e", 32)}); got != want {
		t.Errorf("timeout effect %s, want %s", got, want)
	}
	tqWant(t, "timeout", g.rel, "publisher", ctPublisher)

	// A movement cost line with no payment row cannot come from the chain
	// (a successful movement is top-level and has its row): nothing is said.
	g = tqTx(t, f, tqGhost, "success", failedtx.KindDeposit)
	if string(g.keys["effect"]) != "{}" || string(g.keys["related"]) != "{}" {
		t.Errorf("a deposit with no row: effect %s, related %s", g.keys["effect"], g.keys["related"])
	}

	// A publication with no cost line: its one message, whole; no cost.
	g = tqTx(t, f, tqPub2Tx, "success", failedtx.KindSettlement)
	row := tqAsk(t, f.srv, "/v1/blobs?tx="+tqPub2Tx)
	if err := json.Unmarshal(row.keys["blobs"], &rows); err != nil || len(rows) != 1 {
		t.Fatalf("/v1/blobs?tx=%s: %v %s", tqPub2Tx, err, row.body)
	}
	var signer, settledAt string
	_ = json.Unmarshal(rows[0]["signer"], &signer)
	_ = json.Unmarshal(rows[0]["settlement_time"], &settledAt)
	if got, want := tqJSON(t, g.a.Messages), tqJSON(t, []txMsg{{Index: 0, TypeURL: pff, Fibre: true, Signer: signer}}); got != want || signer == "" {
		t.Errorf("a publication alone: messages %s, want %s", got, want)
	}
	for _, k := range []string{"cost", "messages_partial", "failure"} {
		tqWant(t, "a publication alone", g.keys, k, nil)
	}
	if g.a.Height != 36 || g.a.TxIndex != 0 || !g.a.Time.Equal(parseTS(settledAt)) {
		t.Errorf("a publication alone: block %d/%d at %s, want 36/0 at %s", g.a.Height, g.a.TxIndex, g.a.Time, settledAt)
	}
	tqWant(t, "a publication alone", g.effect, "promise_hash", tqPub2Promise)
	tqWant(t, "a publication alone", g.effect, "fee_paid_utia", nil) // no payment of it on record

	// A movement with no cost line: the record's Fibre messages alone.
	g = tqTx(t, f, ctWithdrawTx, "success", failedtx.KindWithdrawalRequest)
	if got, want := tqJSON(t, g.a.Messages), tqJSON(t, []txMsg{{Index: 0, TypeURL: kindTypeURL[failedtx.KindWithdrawalRequest], Fibre: true,
		Signer: ctPublisher}}); got != want || !g.a.MessagesPartial {
		t.Errorf("a movement alone: messages %s (partial %v), want %s", got, g.a.MessagesPartial, want)
	}
	tqWant(t, "a movement alone", g.keys, "cost", nil)
	if got, want := string(g.keys["effect"]), `{"amount_utia":1000000}`; got != want {
		t.Errorf("a movement alone: effect %s, want %s (no queue row: no withdrawal)", got, want)
	}

	// A failure.
	g = tqTx(t, f, tqFDep, "failed", failedtx.KindDeposit)
	rec := tqRecord(t, tqFDep)
	if g.a.Cost == nil || *g.a.Cost != (txCost{GasWanted: rec.GasWanted, GasUsed: rec.GasUsed, Fee: "2000utia"}) || g.a.Failure == nil ||
		g.a.Failure.Code != rec.Code || g.a.Failure.Codespace != rec.Codespace || g.a.Failure.Log != rec.Log ||
		g.a.Failure.Reason != failedtx.Explain(rec).Reason || g.a.Failure.FailedMsgIndex == nil || *g.a.Failure.FailedMsgIndex != 0 {
		t.Errorf("a failure: %s", g.body)
	}
	if strings.Contains(string(g.body), `"detail"`) || strings.Contains(string(g.body), "fee_payer") {
		t.Errorf("a failure publishes its messages' detail or a payer: %s", g.body)
	}

	// A hash nothing knows; a bad hash; 0x and upper case.
	miss := tqAsk(t, f.srv, "/v1/txs/"+tqHash("nothing"))
	if miss.status != 404 || miss.cache != "no-store" || string(miss.keys["error"]) != `"no Fibre transaction with this hash on record"` {
		t.Errorf("a miss: %d %q %s", miss.status, miss.cache, miss.body)
	}
	for _, bad := range []string{"nope", ctDepositTx[:62], ctDepositTx + "00", "0x" + strings.Repeat("zz", 32)} {
		if b := tqAsk(t, f.srv, "/v1/txs/"+bad); b.status != 400 || string(b.keys["error"]) != `"hash must be 64 hex characters"` || b.cache != "no-store" {
			t.Errorf("%s: %d %q %s", bad, b.status, b.cache, b.body)
		}
	}
	up := tqAsk(t, f.srv, "/v1/txs/0x"+strings.ToUpper(ctDepositTx))
	if up.status != 200 || up.a.TxHash != ctDepositTx || up.a.Status != "success" {
		t.Errorf("0x and upper case: %d %s", up.status, up.body)
	}
}

// A withdrawal request's place in the queue, by each outcome, and the
// registrations' words by the same walk as the endpoint history.
func TestTxPageSuccessEffects(t *testing.T) {
	f := tqFixture(t)
	bt := func(h int64) string { return store.TS(ctBlockTime(h)) }
	avail := func(h int64) string { return store.TS(ctBlockTime(h).Add(12 * time.Hour)) }
	i64 := func(n int64) *int64 { return &n }
	str := func(s string) *string { return &s }
	for _, c := range []struct {
		hash   string
		amount int64
		want   *txWithdrawal
	}{
		{tqWdPending, 2_000_000, &txWithdrawal{AvailableAt: avail(71), Outcome: "pending"}},
		// paid 9 blocks after the request, 1.6 s a block
		{tqWdPaid, 3_000_000, &txWithdrawal{AvailableAt: avail(72), Outcome: "paid", PaidHeight: i64(81), PaidAt: str(bt(81)), PayoutDelayS: i64(14)}},
		{tqWdConsumed, 4_000_000, &txWithdrawal{AvailableAt: avail(73), Outcome: "consumed", ReducedUtia: 3_000_000}},
		{tqWdUnattr, 5_000_000, &txWithdrawal{AvailableAt: avail(74)}},
	} {
		g := tqTx(t, f, c.hash, "success", failedtx.KindWithdrawalRequest)
		tqWant(t, c.hash, g.effect, "amount_utia", c.amount)
		tqWant(t, c.hash, g.effect, "withdrawal", c.want)
		tqWant(t, c.hash, g.rel, "publisher", ctPublisher)
	}

	valA := txValidator{Address: tqValA, OperatorAddress: tqOpA, Moniker: "Alpha", AvatarURL: "/v1/avatars/AAAA1111"}
	valB := txValidator{Address: tqValB, OperatorAddress: tqOpB, Moniker: "Beta"}
	for _, c := range []struct {
		hash    string
		effect  map[string]any
		related txRelated
	}{
		{tqReg, map[string]any{"action": "registered", "host": tqHostA1}, txRelated{Validator: &valA}},
		{tqChg, map[string]any{"action": "changed", "host": tqHostA2, "previous_host": tqHostA1}, txRelated{Validator: &valA}},
		{tqSame, map[string]any{"action": "same", "host": tqHostA2, "previous_host": tqHostA2}, txRelated{Validator: &valA}},
		// through a grantee, who paid: the validator is the registration's
		{tqExecHost, map[string]any{"action": "changed", "host": tqHostB2, "previous_host": tqHostB1}, txRelated{Validator: &valB}},
		// for two validators: both, each with its own row in its history
		{tqTwoHosts, map[string]any{}, txRelated{Validators: []txValidator{valA, valB}}},
	} {
		g := tqTx(t, f, c.hash, "success", failedtx.KindSetHost)
		if got, want := string(g.keys["effect"]), tqJSON(t, c.effect); got != want {
			t.Errorf("%s: effect %s, want %s", c.hash, got, want)
		}
		if got, want := string(g.keys["related"]), tqJSON(t, c.related); got != want {
			t.Errorf("%s: related %s, want %s", c.hash, got, want)
		}
	}
	g := tqTx(t, f, tqExecHost, "success", failedtx.KindSetHost)
	if g.a.Cost == nil || g.a.Cost.FeePayer != tqGrantee {
		t.Errorf("a delegated registration: cost %s, want the grantee paying", g.keys["cost"])
	}
	if got, want := tqJSON(t, g.a.Messages), tqJSON(t, []txMsg{{Index: 0, TypeURL: failedtx.ExecTypeURL,
		Inner: []txMsg{{Index: 0, TypeURL: kindTypeURL[failedtx.KindSetHost], Fibre: true, Signer: tqOpB}}}}); got != want {
		t.Errorf("a delegated registration: messages %s, want %s", got, want)
	}

	// A deposit and a registration in one transaction.
	g = tqTx(t, f, tqMixed, "success", txKindSeveral)
	if got, want := string(g.keys["effect"]), "{}"; got != want {
		t.Errorf("several kinds: effect %s", got)
	}
	if got, want := string(g.keys["related"]), tqJSON(t, txRelated{Publisher: ctPublisher, Validator: &valB}); got != want {
		t.Errorf("several kinds: related %s, want %s", got, want)
	}

	// The registrations' words are the endpoint history's, row for row
	// (but for the transactions that registered for two validators, or
	// moved escrow too, whose effect is {}).
	for _, v := range []string{tqValA, tqValB} {
		rows, _, err := f.srv.endpointHistory(context.Background(), v)
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range rows {
			if r.TxHash == "" || r.Outcome == endpointFailed || r.TxHash == tqTwoHosts || r.TxHash == tqMixed {
				continue
			}
			g := tqAsk(t, f.srv, "/v1/txs/"+r.TxHash)
			tqWant(t, r.TxHash, g.effect, "action", r.Outcome)
			tqWant(t, r.TxHash, g.effect, "host", r.Host)
			if r.Outcome == endpointChanged {
				tqWant(t, r.TxHash, g.effect, "previous_host", r.PreviousHost)
			}
		}
	}
}

// A failure says what it asked for, and links only what the lists would
// show: a final failure's top-level message, a publisher with a page,
// never a failed timeout's publisher, a validator by its current operator
// address. Any other account it names is plain text.
func TestTxPageFailureEffectsAndLinks(t *testing.T) {
	f := tqFixture(t)
	var commitment string
	var version int
	if err := f.st.DB().QueryRow(`SELECT commitment, blob_version FROM publications WHERE promise_hash = ?`, tqPub3Promise).Scan(&commitment, &version); err != nil {
		t.Fatal(err)
	}
	h40 := int64(40)
	valA := txValidator{Address: tqValA, OperatorAddress: tqOpA, Moniker: "Alpha", AvatarURL: "/v1/avatars/AAAA1111"}
	valB := txValidator{Address: tqValB, OperatorAddress: tqOpB, Moniker: "Beta"}
	pffAsked := map[string]any{"promise_hash": tqPub3Promise, "namespace": ctNamespace, "blob_size": 4096}
	with := func(m map[string]any, k string, v any) map[string]any {
		out := map[string]any{k: v}
		for a, b := range m {
			out[a] = b
		}
		return out
	}
	for _, c := range []struct {
		name, hash, kind string
		final            bool
		effect           map[string]any
		related          txRelated
	}{
		{"a final top-level settlement: its publisher, and the blob its promise settled in later", tqFPFF, failedtx.KindSettlement, true,
			pffAsked, txRelated{Publisher: ctPublisher, Blob: &txBlob{PromiseHash: tqPub3Promise, Commitment: commitment, BlobVersion: version, SettlementHeight: &h40}}},
		{"the same, not final: no link, the publisher as the message names it", tqFPFFOpen, failedtx.KindSettlement, false,
			with(pffAsked, "publisher", ctPublisher), txRelated{}},
		{"a final settlement naming an account with no publisher page", tqFPFFStranger, failedtx.KindSettlement, true,
			map[string]any{"promise_hash": strings.Repeat("cd", 32), "namespace": ctNamespace, "blob_size": 4096, "publisher": tqStranger}, txRelated{}},
		{"a deposit above 2^53 utia: the request verbatim alone", tqFDepBig, failedtx.KindDeposit, true,
			map[string]any{"requested": "9007199254740993utia"}, txRelated{Publisher: ctPublisher}},
		{"a deposit", tqFDep, failedtx.KindDeposit, true,
			map[string]any{"requested": "1000000utia", "requested_utia": 1000000}, txRelated{Publisher: ctPublisher}},
		{"a final timeout: never a link, its publisher as the message names it", tqFTimeout, failedtx.KindTimeout, true,
			map[string]any{"promise_hash": strings.Repeat("7e", 32), "publisher": ctPublisher}, txRelated{}},
		{"a settlement whose strings were cut: no publisher either way", tqFCut, failedtx.KindSettlement, true,
			map[string]any{"promise_hash": strings.Repeat("cd", 32), "namespace": ctNamespace, "blob_size": 4096}, txRelated{}},
		{"a final top-level registration by A's current operator", tqFHost, failedtx.KindSetHost, true,
			map[string]any{"requested_host": "10.9.9.9:7980", "attempted": "change", "host_at_block": tqHostA3}, txRelated{Validator: &valA}},
		{"the same inside a MsgExec: what A had, no link", tqFHostExec, failedtx.KindSetHost, true,
			map[string]any{"requested_host": "10.9.9.8:7980", "attempted": "change", "host_at_block": tqHostA3}, txRelated{}},
		{"a registration by no validator", tqFHostStranger, failedtx.KindSetHost, true,
			map[string]any{"requested_host": "10.9.9.7:7980", "not_a_validator": true}, txRelated{}},
		{"a registration by the operator of a superseded key: its current key", tqFHostSuper, failedtx.KindSetHost, true,
			map[string]any{"requested_host": "10.9.9.6:7980", "attempted": "change", "host_at_block": tqHostN1},
			txRelated{Validator: &txValidator{Address: tqValNew, OperatorAddress: tqOpC, Moniker: "New key"}}},
		{"a final registration by A's and B's operators: both", tqFTwoHosts, failedtx.KindSetHost, true,
			map[string]any{}, txRelated{Validators: []txValidator{valA, valB}}},
		{"the same, not final: none", tqFTwoHostsOpen, failedtx.KindSetHost, false, map[string]any{}, txRelated{}},
	} {
		g := tqTx(t, f, c.hash, "failed", c.kind)
		if g.a.Final == nil || *g.a.Final != c.final {
			t.Errorf("%s: final %s", c.name, g.keys["final"])
		}
		if got, want := string(g.keys["effect"]), tqJSON(t, c.effect); got != want {
			t.Errorf("%s: effect %s, want %s", c.name, got, want)
		}
		if got, want := string(g.keys["related"]), tqJSON(t, c.related); got != want {
			t.Errorf("%s: related %s, want %s", c.name, got, want)
		}
	}
	// A cut message's signer is not published.
	g := tqTx(t, f, tqFCut, "failed", failedtx.KindSettlement)
	if len(g.a.Messages) != 1 || g.a.Messages[0].Signer != "" {
		t.Errorf("a cut message: %s", g.keys["messages"])
	}
}

// A row this build cannot read is a row fault, never a 500: a cost line
// that does not decode falls through to the publication, a failure that
// does not decode answers no transaction; each is counted once.
func TestTxPageRowsThatDoNotDecode(t *testing.T) {
	f := tqFixture(t)
	before, _, _ := f.srv.UndecodableRows()
	g := tqTx(t, f, tqPub3Tx, "success", failedtx.KindSettlement)
	if g.a.Cost != nil || g.a.Height != 40 {
		t.Errorf("the publication behind an undecodable cost line: %s", g.body)
	}
	tqWant(t, "the publication behind an undecodable cost line", g.effect, "promise_hash", tqPub3Promise)
	if n, _, _ := f.srv.UndecodableRows(); n != before+1 {
		t.Errorf("row faults %d after the cost line, want %d", n, before+1)
	}
	miss := tqAsk(t, f.srv, "/v1/txs/"+tqFBroken)
	if miss.status != 404 || miss.cache != "no-store" {
		t.Errorf("an undecodable failure: %d %q %s", miss.status, miss.cache, miss.body)
	}
	if n, _, _ := f.srv.UndecodableRows(); n != before+2 {
		t.Errorf("row faults %d after the failure, want %d", n, before+2)
	}
}

// failedtx.OperatorForm, which lists a failure under its signer's operator
// address, and the API's operatorForm, which resolves one, read every
// address alike. The account form is accepted by both as defence only: the
// chain makes no failure final whose registration's signer is in it.
func TestFailedTxOperatorFormIsTheAPIs(t *testing.T) {
	raw := bytes.Repeat([]byte{0x42}, 20)
	enc := func(prefix string, b []byte) string {
		s, err := bech32.ConvertAndEncode(prefix, b)
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	for _, s := range []string{
		enc("celestiavaloper", raw),
		strings.ToUpper(enc("celestiavaloper", raw)),
		" " + enc("celestiavaloper", raw) + " ",
		enc("celestia", raw), // defence only
		enc("celestiavalcons", raw),
		enc("celestiavalconspub", raw),
		enc("celestia", bytes.Repeat([]byte{0x42}, 32)),
		enc("celestiavaloper", bytes.Repeat([]byte{0x42}, 32)),
		ctValoper, tqOpA, tqGrantee, ctPublisher,
		"celestiavaloper1notanaddress", "garbage", "",
	} {
		got, gotOK := failedtx.OperatorForm(s)
		want, wantOK := operatorForm(s)
		if got != want || gotOK != wantOK {
			t.Errorf("%q: failedtx.OperatorForm %q %v, operatorForm %q %v", s, got, gotOK, want, wantOK)
		}
	}
	if op, ok := failedtx.OperatorForm(enc("celestia", raw)); !ok || op != enc("celestiavaloper", raw) {
		t.Errorf("the account form of the same bytes: %q %v", op, ok)
	}
}
