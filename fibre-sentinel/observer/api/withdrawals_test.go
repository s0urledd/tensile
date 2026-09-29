package api_test

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/scan"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/api"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
)

type wRow struct {
	RequestedAt   string  `json:"requested_at"`
	AvailableAt   string  `json:"available_at"`
	Amount        int64   `json:"amount_utia"`
	First         int64   `json:"first_amount_utia"`
	Reduced       int64   `json:"reduced_utia"`
	GoneHeight    *int64  `json:"gone_height"`
	Outcome       *string `json:"outcome"`
	OutcomeReason string  `json:"outcome_reason"`
	PaidHeight    *int64  `json:"paid_height"`
	PayoutDelayS  *int64  `json:"payout_delay_s"`
	PayoutLagS    *int64  `json:"payout_lag_s"`
}

type pSum struct {
	Count      int64   `json:"count"`
	Utia       int64   `json:"utia"`
	Next       *string `json:"next_available_at"`
	Reduced    int64   `json:"reduced_utia"`
	ReadHeight *int64  `json:"read_height"`
	ReadAt     *string `json:"read_at"`
}

// queueServer is marketServer plus a withdrawal history for samplePublisher,
// served by a fresh server so the market snapshot is computed over it:
//
//   - wX, requested 25 h before the sample settlement, left the queue after
//     it became payable and is paid by the sample's payout at height 15
//     (1000 utia): executed, delay 25 h 4 min, lag 64 min.
//   - w0, seen at 25 and gone at 30, long before its available_at:
//     consumed by a settlement shortfall.
//   - w1 (250 → 200, reduced by 50) and w2 (100) are queued at 30, and the
//     escrow read at the same height agrees: balance − available = 300.
func queueServer(t *testing.T) (*httptest.Server, *store.Store, time.Time) {
	t.Helper()
	_, st := marketServer(t, nil)
	now := time.Now()
	// marketServer's settlement time, recovered from its payout (written
	// four minutes after it) so the delays below come out exact.
	var paidAt string
	if err := st.DB().QueryRow(`SELECT time FROM payments WHERE dedupe_key = 'h15:executed:0'`).Scan(&paidAt); err != nil {
		t.Fatal(err)
	}
	paid, err := time.Parse(store.TimeLayout, paidAt)
	if err != nil {
		t.Fatal(err)
	}
	settle := paid.Add(-4 * time.Minute)
	ctx := context.Background()
	w := func(req time.Time, amt uint64) scan.PendingWithdrawal {
		return scan.PendingWithdrawal{Signer: samplePublisher, Denom: "utia", AmountUtia: amt, RequestedAt: req, AvailableAt: req.Add(24 * time.Hour)}
	}
	obs := func(h int64, at time.Time, ws ...scan.PendingWithdrawal) {
		t.Helper()
		if _, err := st.ObserveWithdrawals(store.WithdrawalRead{Publisher: samplePublisher, Height: h, BlockTime: at, Withdrawals: ws}, now); err != nil {
			t.Fatal(err)
		}
	}
	wX := w(settle.Add(-25*time.Hour), 1000)
	w0 := w(settle.Add(-time.Minute).Truncate(time.Second), 40)
	w1 := w(settle.Add(time.Minute).Truncate(time.Second), 250)
	w2 := w(settle.Add(2*time.Minute).Truncate(time.Second), 100)
	obs(12, settle.Add(-2*time.Minute), wX)
	obs(16, settle.Add(5*time.Minute))
	obs(25, settle.Add(10*time.Minute), w0, w1, w2)
	w1.AmountUtia = 200
	if err := st.UpsertEscrowAccount(scan.Escrow{Signer: samplePublisher, Denom: "utia", BalanceUtia: 1000, AvailableUtia: 700, Height: 30, Found: true}, now); err != nil {
		t.Fatal(err)
	}
	obs(30, settle.Add(20*time.Minute), w1, w2)
	if _, err := st.ResolveWithdrawals(ctx, 100); err != nil {
		t.Fatal(err)
	}
	_ = st.SetMeta("withdrawals_polled_height", "30", now)
	_ = st.SetMeta("withdrawals_polled_at", store.TS(settle.Add(20*time.Minute)), now)
	ts := httptest.NewServer(api.NewWithVantage(st, api.VantageInfo{Name: "test"}, nil))
	t.Cleanup(ts.Close)
	return ts, st, settle
}

func TestPublisherWithdrawalQueue(t *testing.T) {
	ts, st, settle := queueServer(t)
	var d struct {
		Publisher struct {
			Pending *pSum `json:"pending_withdrawals"`
		} `json:"publisher"`
		Withdrawals *struct {
			pSum
			Pending   []wRow `json:"pending"`
			LeftQueue []wRow `json:"left_queue"`
			Check     *struct {
				Height     int64 `json:"height"`
				Diff       int64 `json:"balance_minus_available_utia"`
				Pending    int64 `json:"pending_utia"`
				Consistent bool  `json:"consistent"`
			} `json:"check"`
		} `json:"withdrawals"`
	}
	if code := get(t, ts, "/v1/publishers/"+samplePublisher+"?window=24h", &d); code != 200 {
		t.Fatalf("publisher: %d", code)
	}
	q := d.Withdrawals
	if q == nil {
		t.Fatal("no withdrawals section")
	}
	if q.Count != 2 || q.Utia != 300 || q.Reduced != 50 {
		t.Fatalf("summary: %+v", q.pSum)
	}
	// the read it rests on is the publisher row's
	if p := d.Publisher.Pending; p == nil || p.ReadHeight == nil || *p.ReadHeight != 30 || p.ReadAt == nil {
		t.Fatalf("publisher row read: %+v", p)
	}
	wantNext := store.TS(settle.Add(time.Minute).Truncate(time.Second).Add(24 * time.Hour))
	if q.Next == nil || *q.Next != wantNext {
		t.Fatalf("next_available_at %v, want %s", q.Next, wantNext)
	}
	if len(q.Pending) != 2 || q.Pending[0].Amount != 200 || q.Pending[0].First != 250 || q.Pending[0].Reduced != 50 || q.Pending[0].Outcome != nil {
		t.Fatalf("pending rows: %+v", q.Pending)
	}
	if q.Check == nil || !q.Check.Consistent || q.Check.Diff != 300 || q.Check.Height != 30 {
		t.Fatalf("check: %+v", q.Check)
	}
	if len(q.LeftQueue) != 2 {
		t.Fatalf("left queue: %+v", q.LeftQueue)
	}
	byOutcome := map[string]wRow{}
	for _, r := range q.LeftQueue {
		if r.Outcome == nil {
			t.Fatalf("unresolved row: %+v", r)
		}
		byOutcome[*r.Outcome] = r
	}
	ex, ok := byOutcome[store.WithdrawalExecuted]
	if !ok || ex.PaidHeight == nil || *ex.PaidHeight != 15 || ex.PayoutDelayS == nil || ex.PayoutLagS == nil {
		t.Fatalf("executed: %+v", ex)
	}
	if want := int64((25*time.Hour + 4*time.Minute) / time.Second); *ex.PayoutDelayS != want {
		t.Fatalf("payout delay %d, want %d", *ex.PayoutDelayS, want)
	}
	if want := int64(64 * time.Minute / time.Second); *ex.PayoutLagS != want {
		t.Fatalf("payout lag %d, want %d", *ex.PayoutLagS, want)
	}
	if c, ok := byOutcome[store.WithdrawalConsumed]; !ok || c.PayoutDelayS != nil || c.OutcomeReason == "" {
		t.Fatalf("consumed: %+v", c)
	}
	if d.Publisher.Pending == nil || d.Publisher.Pending.Count != 2 {
		t.Fatalf("publisher row pending: %+v", d.Publisher.Pending)
	}

	// An escrow read that disagrees with the queue at the same height is
	// published as inconsistent, not smoothed over.
	if err := st.UpsertEscrowAccount(scan.Escrow{Signer: samplePublisher, Denom: "utia", BalanceUtia: 1000, AvailableUtia: 800, Height: 30, Found: true}, time.Now()); err != nil {
		t.Fatal(err)
	}
	get(t, ts, "/v1/publishers/"+samplePublisher, &d)
	if d.Withdrawals.Check == nil || d.Withdrawals.Check.Consistent {
		t.Fatalf("inconsistent reads passed: %+v", d.Withdrawals.Check)
	}
	// And reads from different heights are not compared at all.
	if err := st.UpsertEscrowAccount(scan.Escrow{Signer: samplePublisher, Denom: "utia", BalanceUtia: 1000, AvailableUtia: 800, Height: 31, Found: true}, time.Now()); err != nil {
		t.Fatal(err)
	}
	get(t, ts, "/v1/publishers/"+samplePublisher, &d)
	if d.Withdrawals.Check != nil {
		t.Fatalf("reads at 31 and 30 compared: %+v", d.Withdrawals.Check)
	}
}

// An account whose queue was never read says so (null), rather than showing
// an empty queue.
func TestPublisherQueueNeverRead(t *testing.T) {
	ts, _, _ := queueServer(t)
	var list struct {
		Publishers []struct {
			Publisher string `json:"publisher"`
			Pending   *pSum  `json:"pending_withdrawals"`
		} `json:"publishers"`
	}
	if code := get(t, ts, "/v1/publishers?window=24h", &list); code != 200 {
		t.Fatalf("list: %d", code)
	}
	seen := 0
	for _, p := range list.Publishers {
		switch p.Publisher {
		case samplePublisher:
			seen++
			if p.Pending == nil || p.Pending.Count != 2 || p.Pending.Utia != 300 {
				t.Fatalf("sample: %+v", p.Pending)
			}
		case otherPublisher:
			seen++
			if p.Pending != nil {
				t.Fatalf("never-read queue shown as %+v", p.Pending)
			}
		}
	}
	if seen != 2 {
		t.Fatalf("publishers: %+v", list.Publishers)
	}
	var d struct {
		Withdrawals *map[string]any `json:"withdrawals"`
	}
	get(t, ts, "/v1/publishers/"+otherPublisher, &d)
	if d.Withdrawals != nil {
		t.Fatalf("never-read queue detail: %v", *d.Withdrawals)
	}
}

func TestMarketWithdrawalQueue(t *testing.T) {
	ts, _, _ := queueServer(t)
	var m struct {
		Q *struct {
			Pending      pSum                        `json:"pending"`
			Publishers   int64                       `json:"publishers"`
			Executed     struct{ Count, Utia int64 } `json:"executed"`
			Consumed     struct{ Count, Utia int64 } `json:"consumed"`
			Unattributed struct{ Count int64 }       `json:"unattributed"`
			PayoutDelay  struct {
				Count   int64  `json:"count"`
				MedianS *int64 `json:"median_s"`
				Lag     *int64 `json:"median_lag_s"`
			} `json:"payout_delay"`
		} `json:"withdrawal_queue"`
	}
	if code := get(t, ts, "/v1/market?window=24h", &m); code != 200 {
		t.Fatalf("market: %d", code)
	}
	q := m.Q
	if q == nil {
		t.Fatal("no withdrawal_queue")
	}
	if q.Pending.Count != 2 || q.Pending.Utia != 300 || q.Publishers != 1 || q.Pending.ReadHeight == nil || *q.Pending.ReadHeight != 30 || q.Pending.Next == nil {
		t.Fatalf("pending: %+v publishers=%d", q.Pending, q.Publishers)
	}
	if q.Executed.Count != 1 || q.Executed.Utia != 1000 || q.Consumed.Count != 1 || q.Consumed.Utia != 40 || q.Unattributed.Count != 0 {
		t.Fatalf("outcomes: %+v", q)
	}
	if q.PayoutDelay.Count != 1 || q.PayoutDelay.MedianS == nil || *q.PayoutDelay.MedianS != int64((25*time.Hour+4*time.Minute)/time.Second) || q.PayoutDelay.Lag == nil {
		t.Fatalf("payout delay: %+v", q.PayoutDelay)
	}
	// A pinned window does not pretend to know the queue as it stood then.
	var pinned struct {
		Q *map[string]any `json:"withdrawal_queue"`
	}
	asOf := time.Now().Add(-time.Minute).UTC().Format(time.RFC3339)
	if code := get(t, ts, "/v1/market?window=24h&as_of="+asOf, &pinned); code != 200 {
		t.Fatalf("as_of market: %d", code)
	}
	if pinned.Q != nil {
		t.Fatalf("as_of market carries a current queue: %v", *pinned.Q)
	}
}
