package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/scan"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
)

const (
	pubA = "celestia1d3mmg652pxj776dyqwlsrc93y64088g6ux8deq"
	pubB = "celestia1e3mmg652pxj776dyqwlsrc93y64088g6ux8deq"
	pubC = "celestia1f3mmg652pxj776dyqwlsrc93y64088g6ux8deq"
	pubD = "celestia1g3mmg652pxj776dyqwlsrc93y64088g6ux8deq"
	pubE = "celestia1h3mmg652pxj776dyqwlsrc93y64088g6ux8deq"
)

// fakeChain answers the escrow poll from fixed state, one "block" at a time.
// height is the tip /status names; like a real node's, it is saved before
// the app has committed it, and a query at it is refused (code 26) while
// tipUncommitted is set. blockTime is the time of the block below it, the
// one the poll reads.
type fakeChain struct {
	height         int64
	blockTime      time.Time
	tipUncommitted bool
	escrow         scan.Escrow
	queue          []scan.PendingWithdrawal
	queueErr       error
	answerAt       int64 // height the Withdrawals answer claims; 0 = the asked height
	asked          []int64
	headers        map[int64]time.Time
	// stall makes every state query wait for its context to end.
	stall bool
	// limited: only the next answers state queries are answered, and the
	// rest stall, as on a node too slow for one budget.
	limited bool
	answers int
	// escrowRefused is a publisher whose escrow read is refused.
	escrowRefused string
}

func (f *fakeChain) StatusAt(context.Context) (string, int64, time.Time, error) {
	return "test", f.height, f.blockTime.Add(6 * time.Second), nil
}

// query is what every state query meets first.
func (f *fakeChain) query(ctx context.Context, height int64) error {
	f.asked = append(f.asked, height)
	if f.stall || (f.limited && f.answers == 0) {
		<-ctx.Done()
		return ctx.Err()
	}
	f.answers--
	if f.tipUncommitted && height >= f.height {
		return &scan.ABCIError{Code: 26, Codespace: "sdk", Log: fmt.Sprintf("cannot query with height in the future; please provide a valid height: %d", height)}
	}
	return nil
}

func (f *fakeChain) EscrowAccount(ctx context.Context, signer string, height int64) (scan.Escrow, error) {
	if err := f.query(ctx, height); err != nil {
		return scan.Escrow{}, err
	}
	if signer == f.escrowRefused {
		return scan.Escrow{}, errors.New("rpc error: code = Internal")
	}
	e := f.escrow
	e.Signer, e.Height = signer, height
	return e, nil
}

func (f *fakeChain) Withdrawals(ctx context.Context, signer string, height int64) ([]scan.PendingWithdrawal, int64, error) {
	if err := f.query(ctx, height); err != nil {
		return nil, 0, err
	}
	if f.queueErr != nil {
		return nil, 0, f.queueErr
	}
	at := height
	if f.answerAt != 0 {
		at = f.answerAt
	}
	return append([]scan.PendingWithdrawal(nil), f.queue...), at, nil
}

func (f *fakeChain) HeaderTime(_ context.Context, h int64) (time.Time, error) {
	if t, ok := f.headers[h]; ok {
		return t, nil
	}
	if h == f.height-1 && !f.blockTime.IsZero() {
		return f.blockTime, nil
	}
	return time.Time{}, fmt.Errorf("height %d is not available, lowest height is 500", h)
}

func quiet(string, ...any) {}

func openStore(t *testing.T) *store.Store {
	t.Helper()
	st, err := store.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	return st
}

func pay(t *testing.T, st *store.Store, p scan.Payment) {
	t.Helper()
	p.SchemaVersion, p.Denom = 1, "utia"
	if p.Publisher == "" {
		p.Publisher = pubA
	}
	if _, err := st.UpsertPayment(p, []byte("{}")); err != nil {
		t.Fatal(err)
	}
}

type qrow struct {
	amount, first int64
	gone          sql.NullInt64
	outcome       sql.NullString
	paidHeight    sql.NullInt64
	lastSeen      int64
}

func queueRow(t *testing.T, st *store.Store, requested time.Time) qrow {
	t.Helper()
	var r qrow
	if err := st.DB().QueryRow(`SELECT amount_utia, first_amount_utia, gone_height, outcome, paid_height, last_seen_height
		FROM withdrawal_queue WHERE publisher = ? AND requested_at = ?`, pubA, store.TS(requested)).
		Scan(&r.amount, &r.first, &r.gone, &r.outcome, &r.paidHeight, &r.lastSeen); err != nil {
		t.Fatalf("row %s: %v", requested, err)
	}
	return r
}

// The whole life of two withdrawals, read the way the collector reads them:
// one is shrunk by a settlement shortfall and later paid, the other is
// consumed whole before it was ever payable. Neither fact is in any event.
func TestPollEscrowFollowsTheQueue(t *testing.T) {
	st := openStore(t)
	ctx := context.Background()
	t0 := time.Date(2026, 9, 25, 0, 0, 0, 0, time.UTC)
	r1, r2 := t0, t0.Add(time.Minute)
	delay := 24 * time.Hour
	pay(t, st, scan.Payment{DedupeKey: "dep", Kind: scan.PaymentDeposit, Height: 90, Time: t0.Add(-time.Hour), AmountUtia: 1000})

	c := &fakeChain{height: 100, blockTime: t0.Add(2 * time.Minute),
		escrow: scan.Escrow{Denom: "utia", BalanceUtia: 1000, AvailableUtia: 700, Found: true},
		queue: []scan.PendingWithdrawal{
			{Signer: pubA, Denom: "utia", AmountUtia: 200, RequestedAt: r1, AvailableAt: r1.Add(delay)},
			{Signer: pubA, Denom: "utia", AmountUtia: 100, RequestedAt: r2, AvailableAt: r2.Add(delay)},
		}}
	var round escrowRound
	p := pollEscrow(ctx, c, st, &round, time.Now(), quiet)
	if p.Height != 99 || p.Escrows != 1 || p.Queues != 1 || p.Change.Opened != 2 || !p.OK {
		t.Fatalf("first poll: %+v", p)
	}
	for _, h := range c.asked {
		if h != 99 {
			t.Fatalf("every read of a poll must be at the block below the tip it started from; asked %v", c.asked)
		}
	}
	var wh sql.NullInt64
	var pending sql.NullInt64
	if err := st.DB().QueryRow(`SELECT withdrawals_height, pending_utia FROM escrow_accounts WHERE publisher = ?`, pubA).Scan(&wh, &pending); err != nil {
		t.Fatal(err)
	}
	if wh.Int64 != 99 || pending.Int64 != 300 {
		t.Fatalf("escrow row: withdrawals_height=%v pending=%v", wh, pending)
	}

	// Block 110, still before either is payable. The state now shows r1
	// shrunk and r2 gone. Only ReduceWithdrawalsForPayment lowers or
	// deletes a queued withdrawal before it is payable, so this is what
	// settlement shortfalls leave behind; the poll must read it as such
	// without any event saying so. (The module's own order is oldest
	// first; the observer does not rely on it, only on the state.)
	c.height, c.blockTime = 110, t0.Add(10*time.Minute)
	c.escrow.BalanceUtia, c.escrow.AvailableUtia = 650, 500
	c.queue = []scan.PendingWithdrawal{{Signer: pubA, Denom: "utia", AmountUtia: 150, RequestedAt: r1, AvailableAt: r1.Add(delay)}}
	p = pollEscrow(ctx, c, st, &round, time.Now(), quiet)
	if p.Change.Reduced != 1 || p.Change.Closed != 1 || p.Resolved != 1 {
		t.Fatalf("second poll: %+v", p)
	}
	if r := queueRow(t, st, r1); r.amount != 150 || r.first != 200 || r.gone.Valid {
		t.Fatalf("r1 after the shortfall: %+v", r)
	}
	if r := queueRow(t, st, r2); !r.gone.Valid || r.gone.Int64 != 109 || r.outcome.String != store.WithdrawalConsumed {
		t.Fatalf("r2 missed before available_at must be consumed: %+v", r)
	}

	// A queue read that fails must leave the queue alone. An empty list
	// would have closed r1.
	c.height, c.blockTime = 120, t0.Add(20*time.Minute)
	c.queueErr = errors.New("rpc: connection reset")
	p = pollEscrow(ctx, c, st, &round, time.Now(), quiet)
	if p.Escrows != 1 || p.Queues != 0 {
		t.Fatalf("failed queue read: %+v", p)
	}
	if r := queueRow(t, st, r1); r.gone.Valid || r.lastSeen != 109 {
		t.Fatalf("a failed read changed r1: %+v", r)
	}
	c.queueErr = nil

	// A node that answers from another height is not stored either.
	c.answerAt = 118
	if p = pollEscrow(ctx, c, st, &round, time.Now(), quiet); p.Queues != 0 {
		t.Fatalf("answer from another height was stored: %+v", p)
	}
	c.answerAt = 0

	// Block 200, a day later: r1 has gone. It was payable, so it waits
	// for the scanner to have read the blocks its payout could be in.
	c.height, c.blockTime = 200, r1.Add(delay).Add(time.Hour)
	c.queue = nil
	c.escrow.BalanceUtia, c.escrow.AvailableUtia = 500, 500
	_ = st.SetMeta("last_scanned_height", "150", time.Now())
	p = pollEscrow(ctx, c, st, &round, time.Now(), quiet)
	if p.Change.Closed != 1 || p.Resolved != 0 {
		t.Fatalf("third poll: %+v", p)
	}
	if r := queueRow(t, st, r1); !r.gone.Valid || r.outcome.Valid {
		t.Fatalf("r1 must wait for the scanner: %+v", r)
	}

	// The scanner reaches past 200 and the payout is on record: the only
	// payout of 150 to this account between heights 109 and 199.
	pay(t, st, scan.Payment{DedupeKey: "h180:executed:0", Kind: scan.PaymentWithdrawalExecuted, Height: 180, Time: r1.Add(delay).Add(6 * time.Second), TxIndex: -1, AmountUtia: 150})
	_ = st.SetMeta("last_scanned_height", "260", time.Now())
	p = pollEscrow(ctx, c, st, &round, time.Now(), quiet)
	if p.Resolved != 1 {
		t.Fatalf("fourth poll: %+v", p)
	}
	if r := queueRow(t, st, r1); r.outcome.String != store.WithdrawalExecuted || r.paidHeight.Int64 != 180 {
		t.Fatalf("r1 must be attributed to the payout at 180: %+v", r)
	}

	// A read from a node behind the last one is ignored whole.
	c.height = 150
	c.queue = []scan.PendingWithdrawal{{Signer: pubA, Denom: "utia", AmountUtia: 150, RequestedAt: r1, AvailableAt: r1.Add(delay)}}
	if p = pollEscrow(ctx, c, st, &round, time.Now(), quiet); p.Queues != 0 {
		t.Fatalf("stale read stored: %+v", p)
	}
	if r := queueRow(t, st, r1); r.outcome.String != store.WithdrawalExecuted {
		t.Fatalf("stale read reopened r1: %+v", r)
	}
}

// No publisher, no chain call, and nothing left unread.
func TestPollEscrowWithoutPublishers(t *testing.T) {
	st := openStore(t)
	c := &fakeChain{height: 1, blockTime: time.Now()}
	if p := pollEscrow(context.Background(), c, st, &escrowRound{}, time.Now(), quiet); p.Publishers != 0 || len(c.asked) != 0 || !p.OK {
		t.Fatalf("poll with nobody to poll: %+v asked=%v", p, c.asked)
	}
}

// /status names a block the app has not committed yet: a query at it is
// refused ("cannot query with height in the future"). The poll reads the
// block below, so it is not refused, and dates the queue by that block.
func TestPollEscrowReadsCommittedState(t *testing.T) {
	st := openStore(t)
	t0 := time.Date(2026, 9, 25, 0, 0, 0, 0, time.UTC)
	pay(t, st, scan.Payment{DedupeKey: "dep", Kind: scan.PaymentDeposit, Height: 90, Time: t0.Add(-time.Hour), AmountUtia: 1000})
	pay(t, st, scan.Payment{DedupeKey: "dep-b", Kind: scan.PaymentDeposit, Height: 91, Time: t0.Add(-time.Hour), AmountUtia: 10, Publisher: pubB})
	c := &fakeChain{height: 100, blockTime: t0, tipUncommitted: true,
		escrow: scan.Escrow{Denom: "utia", BalanceUtia: 1000, AvailableUtia: 1000, Found: true}}
	var logs []string
	p := pollEscrow(context.Background(), c, st, &escrowRound{}, time.Now(), func(f string, a ...any) { logs = append(logs, fmt.Sprintf(f, a...)) })
	if !p.OK || p.Escrows != 2 || p.Queues != 2 || p.Height != 99 {
		t.Fatalf("poll at an uncommitted tip: %+v logs=%v", p, logs)
	}
	if v, _ := st.Meta("withdrawals_polled_at"); v != store.TS(t0) {
		t.Fatalf("withdrawals_polled_at = %q, want the time of the block read (%s)", v, store.TS(t0))
	}
}

// A node that takes queries and never answers holds the poll for the
// budget, not for two -rpc-timeouts per publisher; the poll is not done.
func TestPollEscrowStopsAtItsBudget(t *testing.T) {
	st := openStore(t)
	t0 := time.Date(2026, 9, 25, 0, 0, 0, 0, time.UTC)
	for i, p := range []string{pubA, pubB, pubC} {
		pay(t, st, scan.Payment{DedupeKey: fmt.Sprintf("dep-%d", i), Kind: scan.PaymentDeposit, Height: 90, Time: t0, AmountUtia: 10, Publisher: p})
	}
	defer func(b time.Duration) { escrowBudget = b }(escrowBudget)
	escrowBudget = 50 * time.Millisecond
	c := &fakeChain{height: 100, blockTime: t0, stall: true}
	start := time.Now()
	p := pollEscrow(context.Background(), c, st, &escrowRound{}, time.Now(), quiet)
	if took := time.Since(start); took > 2*time.Second {
		t.Fatalf("a stalled node held the poll for %s", took)
	}
	if p.OK || p.Escrows != 0 || p.Publishers != 3 {
		t.Fatalf("stalled poll: %+v", p)
	}
}

// More publishers than one budget covers: each poll goes on from where the
// last one stopped, so every account is read within a few polls, and a
// round counts as done once the poll that reaches the end of the list has
// read it, dated by the poll the round began in (what escrow_polled_at
// says). A publisher whose read is refused is passed over, and its round
// is not done.
func TestPollEscrowGoesOnWhereTheLastPollStopped(t *testing.T) {
	st := openStore(t)
	t0 := time.Date(2026, 9, 25, 0, 0, 0, 0, time.UTC)
	for i, p := range []string{pubE, pubC, pubA, pubD, pubB} {
		pay(t, st, scan.Payment{DedupeKey: fmt.Sprintf("dep-%d", i), Kind: scan.PaymentDeposit, Height: 90, Time: t0, AmountUtia: 10, Publisher: p})
	}
	defer func(b time.Duration) { escrowBudget = b }(escrowBudget)
	escrowBudget = 50 * time.Millisecond
	c := &fakeChain{blockTime: t0, limited: true,
		escrow: scan.Escrow{Denom: "utia", BalanceUtia: 10, AvailableUtia: 10, Found: true}}
	var round escrowRound
	// A poll at tip+1 reads at tip, at minute tip. The node answers two
	// publishers' worth of queries (an escrow account and a queue each)
	// unless told otherwise.
	at := func(tip int64) time.Time { return t0.Add(time.Duration(tip) * time.Minute) }
	poll := func(tip int64, answers int) escrowPoll {
		c.height, c.answers = tip+1, answers
		return pollEscrow(context.Background(), c, st, &round, at(tip), quiet)
	}
	readAt := func() map[string]int64 {
		got := map[string]int64{}
		rows, err := st.DB().Query(`SELECT publisher, height FROM escrow_accounts`)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		for rows.Next() {
			var p string
			var h int64
			if err := rows.Scan(&p, &h); err != nil {
				t.Fatal(err)
			}
			got[p] = h
		}
		return got
	}
	want := func(step string, w map[string]int64) {
		t.Helper()
		got := readAt()
		for p, h := range w {
			if got[p] != h {
				t.Fatalf("%s: escrow accounts read at %v, want %s at %d", step, got, p, h)
			}
		}
	}

	if p := poll(100, 4); p.OK || p.Escrows != 2 {
		t.Fatalf("first poll: %+v", p)
	}
	want("first poll", map[string]int64{pubA: 100, pubB: 100})
	if p := poll(110, 4); p.OK || p.Escrows != 2 {
		t.Fatalf("second poll: %+v", p)
	}
	want("second poll", map[string]int64{pubA: 100, pubB: 100, pubC: 110, pubD: 110})
	// The third reaches the end: the round that began with the first poll
	// is done, and the next begins from the top in the same poll.
	p := poll(120, 4)
	if !p.OK || !p.Since.Equal(at(100)) || p.Escrows != 2 {
		t.Fatalf("third poll: %+v, want the round of the first poll done", p)
	}
	want("third poll", map[string]int64{pubA: 120, pubB: 100, pubC: 110, pubD: 110, pubE: 120})
	if v, _ := st.Meta("escrow_accounts"); v != "5" {
		t.Fatalf("escrow_accounts = %q, want the 5 of the finished round", v)
	}
	if p := poll(130, 4); p.OK {
		t.Fatalf("fourth poll: %+v", p)
	}
	want("fourth poll", map[string]int64{pubB: 130, pubC: 130, pubD: 110})
	if p := poll(140, 4); !p.OK || !p.Since.Equal(at(120)) {
		t.Fatalf("fifth poll: %+v, want the round of the third poll done", p)
	}
	want("fifth poll", map[string]int64{pubA: 120, pubD: 140, pubE: 140})

	// One account the node refuses: the rest are read, the round is not
	// done, and the next is.
	c.escrowRefused = pubC
	if p := poll(150, 100); p.OK || p.Escrows != 4 {
		t.Fatalf("a poll with one account refused: %+v", p)
	}
	want("refused", map[string]int64{pubA: 150, pubB: 150, pubC: 130, pubD: 150, pubE: 150})
	c.escrowRefused = ""
	if p := poll(160, 100); !p.OK || !p.Since.Equal(at(160)) || p.Escrows != 5 {
		t.Fatalf("the next poll: %+v", p)
	}
}

// Params heights get their block time from the header; a pruned header
// leaves the height undated and costs no log line.
func TestFillParamTimes(t *testing.T) {
	st := openStore(t)
	t1 := time.Date(2026, 9, 24, 20, 15, 3, 0, time.UTC)
	if err := st.UpsertParams([]scan.ParamEntry{
		{FromHeight: 400, FromTxIndex: -1, Source: "seed"},
		{FromHeight: 600, FromTxIndex: 3, Source: "event"},
	}); err != nil {
		t.Fatal(err)
	}
	c := &fakeChain{headers: map[int64]time.Time{600: t1}}
	logged := 0
	n := fillParamTimes(context.Background(), c, st, func(string, ...any) { logged++ })
	if n != 1 || logged != 0 {
		t.Fatalf("dated %d, logged %d", n, logged)
	}
	var at sql.NullString
	if err := st.DB().QueryRow(`SELECT effective_from_time FROM params_history WHERE effective_from_height = 600`).Scan(&at); err != nil || at.String != store.TS(t1) {
		t.Fatalf("600: %v %v", at, err)
	}
	if err := st.DB().QueryRow(`SELECT effective_from_time FROM params_history WHERE effective_from_height = 400`).Scan(&at); err != nil || at.Valid {
		t.Fatalf("pruned 400 must stay undated: %v %v", at, err)
	}
	// Only the undated height is asked again.
	hs, _ := st.ParamHeightsWithoutTime(context.Background())
	if len(hs) != 1 || hs[0] != 400 {
		t.Fatalf("undated: %v", hs)
	}
}
