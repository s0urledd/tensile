package api

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	errorsmod "cosmossdk.io/errors"
	sdkerrors "github.com/cosmos/cosmos-sdk/types/errors"
	"github.com/cosmos/cosmos-sdk/x/authz"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/failedtx"
	"github.com/plsgiveup/fibre/fibre-sentinel/internal/scan"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
)

// A publisher's transactions (/v1/publishers/{addr}/txs, pubtxs.go), over
// the transaction page's store (tqFixture): ctPublisher's movements and
// failures there, and beside them the ones below, each named by what it
// proves.

// ptHash is a transaction hash of this file's rows.
func ptHash(name string) string { return tqHash("publisher txs " + name) }

var (
	ptTwoDeposits = ptHash("two deposits in one transaction")
	ptWdLate      = ptHash("a request still queued long after it became payable")
	ptWdGone      = ptHash("a request gone from the queue, its outcome not settled")
	ptStranger    = ptHash("a deposit of another account")
	ptFWithdrawal = ptHash("a final failed withdrawal request")
	ptFExecDep    = ptHash("a final failed deposit inside a MsgExec")
	ptFExecPFF    = ptHash("a final failed blob payment inside a MsgExec, after a send")
	ptFOpenWd     = ptHash("a withdrawal request stopped before the ante")
	ptFTwoMsgs    = ptHash("a final failed deposit and withdrawal request in one transaction")
)

// ptRow is a row as the tests name it: s<height>:<tx index>:<msg index> for
// a success, f<height>:<tx index>:<msg index> for a failure.
type ptRow = string

// ptAll and ptEscrow are ctPublisher's whole lists, newest first.
var (
	ptAll = []ptRow{
		"f109:0:0", "f107:0:1", "f105:0:0", "s102:0:0", "s101:0:0", "s100:2:1", "s100:2:0", "f79:0:0", "f78:0:0", "f76:0:0", "f75:0:0",
		"s74:0:0", "s73:0:0", "s72:0:0", "s71:0:0", "s70:0:0", "s66:0:0", "s60:0:0", "s45:0:0", "s32:0:0", "s20:1:0",
	}
	ptEscrow = []ptRow{
		"f109:0:0", "f105:0:0", "s102:0:0", "s101:0:0", "s100:2:1", "s100:2:0", "f79:0:0", "f78:0:0",
		"s74:0:0", "s73:0:0", "s72:0:0", "s71:0:0", "s66:0:0", "s45:0:0", "s20:1:0",
	}
)

// ptFixture is tqFixture with this file's rows.
func ptFixture(t *testing.T) tqFix {
	t.Helper()
	f := tqFixture(t)
	bt := func(h int64) string { return store.TS(ctBlockTime(h)) }
	pay := func(p scan.Payment) {
		t.Helper()
		raw, err := json.Marshal(p)
		if err != nil {
			t.Fatal(err)
		}
		if ok, err := f.st.UpsertPayment(p, raw); err != nil || !ok {
			t.Fatalf("payment %s: inserted %v, %v", p.DedupeKey, ok, err)
		}
	}
	first := tqPay(ptTwoDeposits, scan.PaymentDeposit, 100, 2, 0, 7_000_000)
	second := tqPay(ptTwoDeposits, scan.PaymentDeposit, 100, 2, 1, 8_000_000)
	pay(first)
	pay(second)
	pay(tqPay(ptWdLate, scan.PaymentWithdrawalRequest, 101, 0, 0, 6_000_000))
	pay(tqPay(ptWdGone, scan.PaymentWithdrawalRequest, 102, 0, 0, 9_000_000))
	stranger := tqPay(ptStranger, scan.PaymentDeposit, 103, 0, 0, 1_000)
	stranger.Publisher = tqStranger
	pay(stranger)
	// the payouts: tqWdPaid's, which its queue row was attributed, and one
	// of ptWdGone's amount, beside it, that no queue row was attributed
	for _, x := range []struct {
		h      int64
		amount uint64
	}{{81, 3_000_000}, {104, 9_000_000}} {
		at := ctBlockTime(x.h)
		pay(scan.Payment{SchemaVersion: 1, DedupeKey: fmt.Sprintf("h%d:executed:0", x.h), Kind: scan.PaymentWithdrawalExecuted, Height: x.h, Time: at,
			TxIndex: -1, Publisher: ctPublisher, Denom: "utia", AmountUtia: x.amount, RecordedAt: at.Add(time.Second)})
	}
	// ptWdLate queued at the last read, payable a second after it was asked
	// for, long before the server's clock; ptWdGone missed from h104, no
	// outcome settled for it yet
	q := `INSERT INTO withdrawal_queue (publisher, requested_at, available_at, denom, first_amount_utia, amount_utia,
		first_seen_height, first_seen_at, last_seen_height, last_seen_at, gone_height, gone_at, outcome, outcome_reason,
		paid_key, paid_height, paid_at, paid_utia, recorded_at) VALUES (?, ?, ?, 'utia', ?, ?, ?, ?, ?, ?, ?, ?, ?, '', ?, ?, ?, ?, ?)`
	tqExec(t, f.st, q, ctPublisher, bt(101), store.TS(ctBlockTime(101).Add(time.Second)), 6_000_000, 6_000_000, 101, bt(101), 110, bt(110),
		nil, nil, nil, nil, nil, nil, nil, bt(110))
	tqExec(t, f.st, q, ctPublisher, bt(102), store.TS(ctBlockTime(102).Add(time.Second)), 9_000_000, 9_000_000, 102, bt(102), 103, bt(103),
		104, bt(104), nil, nil, nil, nil, nil, bt(104))

	short := ctAtMessage(errorsmod.Wrapf(sdkerrors.ErrInsufficientFunds, "insufficient available balance: have %s, need %s", "1utia", "50000000utia"), 0)
	msg := func(i int, kind string, amount string) failedtx.Msg {
		return failedtx.Msg{Index: i, TypeURL: kindTypeURL[kind], Signer: ctPublisher, Detail: &failedtx.MsgDetail{Publisher: ctPublisher, Amount: amount}}
	}
	exec := func(i int, inner failedtx.Msg) failedtx.Msg {
		inner.Index = 0
		return failedtx.Msg{Index: i, TypeURL: failedtx.ExecTypeURL, Inner: []failedtx.Msg{inner}}
	}
	pff := tqPFFMsg(ctPublisher, strings.Repeat("9f", 32))
	for _, r := range []failedtx.Record{
		ctFailure(ptFWithdrawal, 105, 0, short, true, "2000utia", msg(0, failedtx.KindWithdrawalRequest, "50000000utia")),
		ctFailure(ptFExecDep, 106, 1, ctAtMessage(authz.ErrNoAuthorizationFound, 0), true, "2000utia", exec(0, msg(0, failedtx.KindDeposit, "1000utia"))),
		ctFailure(ptFExecPFF, 107, 0, ctAtMessage(authz.ErrNoAuthorizationFound, 1), true, "2000utia",
			failedtx.Msg{Index: 0, TypeURL: "/cosmos.bank.v1beta1.MsgSend"}, exec(1, pff)),
		ctFailure(ptFOpenWd, 108, 0, errorsmod.Wrap(sdkerrors.ErrInvalidRequest, "bad"), false, "", msg(0, failedtx.KindWithdrawalRequest, "5utia")),
		ctFailure(ptFTwoMsgs, 109, 0, short, true, "2000utia", msg(0, failedtx.KindDeposit, "3utia"), msg(1, failedtx.KindWithdrawalRequest, "4utia")),
	} {
		raw, err := json.Marshal(r)
		if err != nil {
			t.Fatal(err)
		}
		if ok, err := f.st.InsertFailedTx(r, raw); err != nil || !ok {
			t.Fatalf("failure %s: inserted %v, %v", r.DedupeKey, ok, err)
		}
	}
	return f
}

// ptPage is an answer of the route.
type ptPage struct {
	Publisher   string            `json:"publisher"`
	View        string            `json:"view"`
	Limit       int               `json:"limit"`
	Offset      int               `json:"offset"`
	Total       int64             `json:"total"`
	FailedTotal *int              `json:"failed_total"`
	Txs         []json.RawMessage `json:"txs"`
	Sums        *pubTxSums        `json:"sums"`
}

// ptAsk asks path and reads its answer: 200, or the test stops.
func ptAsk(t *testing.T, f tqFix, path string) (ptPage, tqGot) {
	t.Helper()
	g := tqAsk(t, f.srv, path)
	if g.status != 200 {
		t.Fatalf("%s: %d %s", path, g.status, g.body)
	}
	var p ptPage
	if err := json.Unmarshal(g.body, &p); err != nil {
		t.Fatalf("%s: %v", path, err)
	}
	return p, g
}

// ids are the page's rows as ptRow names them.
func (p ptPage) ids(t *testing.T) []ptRow {
	t.Helper()
	var out []ptRow
	for _, raw := range p.Txs {
		var x pubTx
		if err := json.Unmarshal(raw, &x); err != nil {
			t.Fatal(err)
		}
		out = append(out, fmt.Sprintf("%s%d:%d:%d", x.Status[:1], x.Height, x.TxIndex, x.MsgIndex))
	}
	return out
}

// row is the raw row id names, in a page that holds it.
func (p ptPage) row(t *testing.T, id ptRow) string {
	t.Helper()
	for i, x := range p.ids(t) {
		if x == id {
			return string(p.Txs[i])
		}
	}
	t.Fatalf("no row %s in %v", id, p.ids(t))
	return ""
}

// Every page of both views, at every offset and several sizes, is the stretch
// of the one list it should be: the account's successes and failures, newest
// first by height, transaction and message, a failure on a page's edge on
// that page and on no other; total counts both, failed_total the failures.
// Another account's movements, a payout, a failure that is not final or
// inside a MsgExec (but a blob payment's), a failed timeout and a failure
// whose strings were cut are in neither list.
func TestPublisherTxsPagesTheWholeRecordNewestFirst(t *testing.T) {
	f := ptFixture(t)
	for _, v := range []struct {
		view   string
		want   []ptRow
		failed int
	}{{"all", ptAll, 7}, {"escrow", ptEscrow, 4}} {
		n := len(v.want)
		for _, limit := range []int{1, 2, 3, 5, 7, n, 100} {
			for offset := 0; offset <= n+1; offset++ {
				path := fmt.Sprintf("/v1/publishers/%s/txs?view=%s&limit=%d&offset=%d", ctPublisher, v.view, limit, offset)
				p, _ := ptAsk(t, f, path)
				lo, hi := min(offset, n), min(offset+limit, n)
				if got, want := strings.Join(p.ids(t), ","), strings.Join(v.want[lo:hi], ","); got != want {
					t.Errorf("%s: rows %s, want %s", path, got, want)
				}
				if p.Total != int64(n) || p.FailedTotal == nil || *p.FailedTotal != v.failed || p.Publisher != ctPublisher || p.View != v.view ||
					p.Limit != limit || p.Offset != offset {
					t.Errorf("%s: %s", path, tqJSON(t, p))
				}
			}
		}
	}
	// the defaults: all, 25 rows from the first; the address in upper case
	p, _ := ptAsk(t, f, "/v1/publishers/"+strings.ToUpper(ctPublisher)+"/txs")
	if p.View != "all" || p.Limit != 25 || p.Offset != 0 || strings.Join(p.ids(t), ",") != strings.Join(ptAll, ",") {
		t.Errorf("defaults: %s", tqJSON(t, p))
	}
	// an account with nothing on record: an empty list, not an error
	p, g := ptAsk(t, f, "/v1/publishers/"+tqBech("celestia", 0x55)+"/txs?view=escrow")
	if p.Total != 0 || *p.FailedTotal != 0 || len(p.Txs) != 0 || !strings.Contains(string(g.body), `"txs":[]`) || p.Sums == nil || *p.Sums != (pubTxSums{}) {
		t.Errorf("an account with nothing: %s", g.body)
	}
	// another account's own movement is its own
	if p, _ := ptAsk(t, f, "/v1/publishers/"+tqStranger+"/txs"); strings.Join(p.ids(t), ",") != "s103:0:0,f77:0:0" {
		t.Errorf("the stranger's: %v", p.ids(t))
	}
}

// Each row is what its record says: a success its movement's amount (a
// request's amount unsigned, as it was asked for) and promise, no failure
// fields; a failure its record's code, reason and finality, no amount, its
// message's index the top-level one (a MsgExec's for a message inside it),
// and of a transaction with several listed messages the first.
func TestPublisherTxsRowsAreWhatTheRecordSays(t *testing.T) {
	f := ptFixture(t)
	p, _ := ptAsk(t, f, "/v1/publishers/"+ctPublisher+"/txs?limit=100")
	bt := func(h int64) string { return store.TS(ctBlockTime(h)) }
	i64 := func(n int64) *int64 { return &n }
	ok := func(kind string, h int64, i, m int, at, hash string, amount int64, promise string) pubTx {
		return pubTx{Kind: kind, Status: "success", Height: h, TxIndex: i, MsgIndex: m, Time: at, TxHash: hash, AmountUtia: i64(amount), PromiseHash: promise}
	}
	failed := func(kind string, h int64, m int, hash, promise string) pubTx {
		var r failedtx.Record
		for _, x := range append(tqFailureRecords(), ptFailures(t, f)...) {
			if x.TxHash == hash {
				r = x
			}
		}
		code, final := r.Code, r.AntePassed
		return pubTx{Kind: kind, Status: "failed", Height: h, TxIndex: r.TxIndex, MsgIndex: m, Time: bt(h), TxHash: hash, PromiseHash: promise,
			Code: &code, Codespace: r.Codespace, Reason: failedtx.Explain(r).Reason, Final: &final}
	}
	settled := time.Date(2026, 9, 6, 17, 0, 27, 494519078, time.UTC)
	for id, want := range map[ptRow]pubTx{
		"s20:1:0":  ok("deposit", 20, 1, 0, bt(20), ctDepositTx, 50_000_000, ""),
		"s32:0:0":  ok("settlement", 32, 0, 0, store.TS(settled), ctSettledTx, 695_000, ctPromise),
		"s60:0:0":  ok("timeout", 60, 0, 0, bt(60), ctTimeoutTx, 695_000, strings.Repeat("7e", 32)),
		"s100:2:1": ok("deposit", 100, 2, 1, bt(100), ptTwoDeposits, 8_000_000, ""),
		// no queue row: requested before the queue was read, say
		"s45:0:0":  ok("withdrawal_request", 45, 0, 0, bt(45), ctWithdrawTx, 1_000_000, ""),
		"f75:0:0":  failed("settlement", 75, 0, tqFPFF, tqPub3Promise),
		"f76:0:0":  failed("settlement", 76, 0, tqFPFFOpen, tqPub3Promise),
		"f79:0:0":  failed("deposit", 79, 0, tqFDep, ""),
		"f105:0:0": failed("withdrawal_request", 105, 0, ptFWithdrawal, ""),
		"f107:0:1": failed("settlement", 107, 1, ptFExecPFF, strings.Repeat("9f", 32)),
		"f109:0:0": failed("deposit", 109, 0, ptFTwoMsgs, ""),
	} {
		if got, w := p.row(t, id), tqJSON(t, want); got != w {
			t.Errorf("%s:\n got %s\nwant %s", id, got, w)
		}
	}
	if !strings.Contains(p.row(t, "f76:0:0"), `"final":false`) || !strings.Contains(p.row(t, "f75:0:0"), `"final":true`) {
		t.Errorf("finality: %s %s", p.row(t, "f76:0:0"), p.row(t, "f75:0:0"))
	}
	for _, raw := range p.Txs {
		var x map[string]json.RawMessage
		if err := json.Unmarshal(raw, &x); err != nil {
			t.Fatal(err)
		}
		_, amount := x["amount_utia"]
		_, code := x["code"]
		if failed := string(x["status"]) == `"failed"`; amount == failed || code != failed {
			t.Errorf("a %s row with amount %v, code %v: %s", x["status"], amount, code, raw)
		}
	}
}

// ptFailures are the failure records ptFixture added, as stored.
func ptFailures(t *testing.T, f tqFix) []failedtx.Record {
	t.Helper()
	var out []failedtx.Record
	for _, h := range []string{ptFWithdrawal, ptFExecDep, ptFExecPFF, ptFOpenWd, ptFTwoMsgs} {
		var raw string
		if err := f.st.DB().QueryRow(`SELECT raw_json FROM failed_txs WHERE tx_hash = ?`, h).Scan(&raw); err != nil {
			t.Fatal(err)
		}
		var r failedtx.Record
		if err := json.Unmarshal([]byte(raw), &r); err != nil {
			t.Fatal(err)
		}
		out = append(out, r)
	}
	return out
}

// A withdrawal request's payout is its queue row's, found by the request's
// block time: paid only where the queue history attributed a payout on
// record to it, with that payout's block, time and amount and the delay
// after the request; pending while it is queued, however long it has been
// payable, and while it is gone with no outcome settled, even with a payout
// of its amount beside it; consumed or unattributed as the history says;
// none where the history never saw the withdrawal.
func TestPublisherTxsPayoutIsTheQueueHistorys(t *testing.T) {
	f := ptFixture(t)
	p, _ := ptAsk(t, f, "/v1/publishers/"+ctPublisher+"/txs?view=escrow&limit=100")
	bt := func(h int64) string { return store.TS(ctBlockTime(h)) }
	avail := func(h int64) string { return store.TS(ctBlockTime(h).Add(12 * time.Hour)) }
	i64 := func(n int64) *int64 { return &n }
	str := func(s string) *string { return &s }
	for id, want := range map[ptRow]*pubTxPayout{
		"s45:0:0": nil,
		"s71:0:0": {State: "pending", AvailableAt: avail(71)},
		// paid 9 blocks after the request, 1.6 s a block
		"s72:0:0":  {State: "paid", AvailableAt: avail(72), PaidHeight: i64(81), PaidAt: str(bt(81)), PaidUtia: i64(3_000_000), PayoutDelayS: i64(14)},
		"s73:0:0":  {State: "consumed", AvailableAt: avail(73)},
		"s74:0:0":  {State: "unattributed", AvailableAt: avail(74)},
		"s101:0:0": {State: "pending", AvailableAt: store.TS(ctBlockTime(101).Add(time.Second))},
		"s102:0:0": {State: "pending", AvailableAt: store.TS(ctBlockTime(102).Add(time.Second))},
	} {
		var x struct {
			Payout json.RawMessage `json:"payout"`
		}
		if err := json.Unmarshal([]byte(p.row(t, id)), &x); err != nil {
			t.Fatal(err)
		}
		if want == nil {
			if x.Payout != nil {
				t.Errorf("%s: payout %s, want none", id, x.Payout)
			}
			continue
		}
		if got, w := string(x.Payout), tqJSON(t, want); got != w {
			t.Errorf("%s: payout %s, want %s", id, got, w)
		}
	}
	// no other row has one
	for i, raw := range p.Txs {
		if id := p.ids(t)[i]; strings.Contains(string(raw), `"payout"`) && !strings.HasPrefix(id, "s") {
			t.Errorf("%s has a payout: %s", id, raw)
		}
	}
}

// Escrow's sums are the account's successful movements over its whole
// record, whatever the page: what it deposited, its blobs' fees and their
// count, its timed-out promises' charges, and every payout to it, attributed
// or not; no failure is in them. All carries none.
func TestPublisherTxsEscrowSumsTheWholeRecord(t *testing.T) {
	f := ptFixture(t)
	want := pubTxSums{DepositedUtia: 66_000_000, Settlements: 2, FeesUtia: 700_000, Timeouts: 1, ChargedUtia: 695_000, WithdrawnUtia: 12_000_000}
	for _, path := range []string{"?view=escrow", "?view=escrow&limit=1&offset=14", "?view=escrow&offset=99"} {
		p, _ := ptAsk(t, f, "/v1/publishers/"+ctPublisher+"/txs"+path)
		if p.Sums == nil || *p.Sums != want {
			t.Errorf("%s: sums %s, want %s", path, tqJSON(t, p.Sums), tqJSON(t, want))
		}
	}
	if _, g := ptAsk(t, f, "/v1/publishers/"+ctPublisher+"/txs?view=all"); g.keys["sums"] != nil {
		t.Errorf("all: sums %s", g.keys["sums"])
	}
}

// A page holding a failure that is not final is not cached; any other keeps
// the default.
func TestPublisherTxsPageWithAnOpenFailureIsNotCached(t *testing.T) {
	f := ptFixture(t)
	// all: f109 f107 f105 s102 s101 s100 s100 f79 f78 f76 f75 ...
	for _, c := range []struct{ q, cache string }{
		{"?limit=3", "public, max-age=15"},
		{"?limit=25", "no-store"},
		{"?limit=1&offset=9", "no-store"},
		{"?limit=2&offset=10", "public, max-age=15"},
		{"?view=escrow&limit=100", "public, max-age=15"},
	} {
		if _, g := ptAsk(t, f, "/v1/publishers/"+ctPublisher+"/txs"+c.q); g.cache != c.cache {
			t.Errorf("%s: Cache-Control %q, want %q", c.q, g.cache, c.cache)
		}
	}
}

// What the route refuses, each a 400 that is not cached.
func TestPublisherTxsRefusals(t *testing.T) {
	f := ptFixture(t)
	for _, path := range []string{
		"/v1/publishers/celestiavaloper1d2ktc37cme7ydk30ylzhamutcynhdvyet7nt3x/txs",
		"/v1/publishers/nope/txs",
		"/v1/publishers/" + ctPublisher + "/txs?view=blobs",
		"/v1/publishers/" + ctPublisher + "/txs?limit=0",
		"/v1/publishers/" + ctPublisher + "/txs?limit=101",
		"/v1/publishers/" + ctPublisher + "/txs?offset=-1",
		"/v1/publishers/" + ctPublisher + "/txs?offset=100001",
		"/v1/publishers/" + ctPublisher + "/txs?offset=x",
	} {
		if g := tqAsk(t, f.srv, path); g.status != 400 || g.cache != "no-store" {
			t.Errorf("%s: %d %q %s", path, g.status, g.cache, g.body)
		}
	}
}
