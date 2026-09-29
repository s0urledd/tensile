package api

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
)

// The withdrawal queue on /v1/market, /v1/publishers and
// /v1/publishers/{addr}.
//
// Where it comes from, and why not from the payments table: see
// observer/store/withdrawals.go. In short, x/fibre shrinks or deletes queued
// withdrawals without an event whenever a settlement finds the available
// balance short (x/fibre/keeper/msg_server.go:313-320 at the pinned
// commit), so the collector reads the queue from state with the module's
// own Withdrawals query and keeps successive reads as history.
//
// What each field is:
//
//   - pending: withdrawals the last read of the account still had queued.
//     The queue is state "now" (as of read_height), never a window figure.
//   - next_available_at: the earliest available_at among them, the moment
//     the begin-blocker may first pay one out. It is the chain's own
//     timestamp (requested_at + the withdrawal_delay in force when it was
//     requested), not an estimate.
//   - reduced_utia: how much of a still-queued withdrawal a settlement
//     shortfall has already taken, first sighting minus now. A floor: a
//     reduction before the first sighting is invisible.
//   - outcome of one that left the queue: executed (attributed to exactly
//     one payout), consumed (left before it could be paid, so a shortfall
//     took it whole) or unattributed (the observer cannot tell honestly).
//   - payout_delay_s: paid_at - requested_at, only for executed ones. The
//     module pays in the first block whose time reaches available_at
//     (x/fibre/keeper/abci.go:52-71), so this is the withdrawal_delay in
//     force at the request plus payout_lag_s, the wait for that block.
//
// The queue is read for every publisher already seen in a payment, at one
// height per poll, with the escrow balances (EscrowAccount) at the same
// height; payouts come from the begin-block EventWithdrawFromEscrowExecuted.
// A withdrawal that left the queue is executed only when exactly one payout
// of its last-seen amount lands between the two reads that bracket its
// departure.

// pendingQueue is one account's queue, or the market's, as last read.
type pendingQueue struct {
	Count           int64   `json:"count"`
	Utia            int64   `json:"utia"`
	NextAvailableAt *string `json:"next_available_at"`
	// ReducedUtia is the part of the pending amount's first sightings a
	// settlement shortfall has already taken: a floor, since a reduction
	// before the first sighting is not visible.
	ReducedUtia int64 `json:"reduced_utia"`
}

// pendingSummary is pendingQueue with the read it rests on.
type pendingSummary struct {
	pendingQueue
	// ReadHeight and ReadAt are the height of the read the figures rest on
	// and that block's time. On the market they are the latest poll's;
	// an account whose own read failed on that poll keeps its older one.
	ReadHeight *int64  `json:"read_height"`
	ReadAt     *string `json:"read_at"`
}

// withdrawalRow is one withdrawal, queued or gone.
type withdrawalRow struct {
	RequestedAt     string  `json:"requested_at"`
	AvailableAt     string  `json:"available_at"`
	Denom           string  `json:"denom"`
	AmountUtia      int64   `json:"amount_utia"`
	FirstAmountUtia int64   `json:"first_amount_utia"`
	ReducedUtia     int64   `json:"reduced_utia"`
	FirstSeenHeight int64   `json:"first_seen_height"`
	FirstSeenAt     string  `json:"first_seen_at"`
	LastSeenHeight  int64   `json:"last_seen_height"`
	LastSeenAt      string  `json:"last_seen_at"`
	GoneHeight      *int64  `json:"gone_height,omitempty"`
	GoneAt          *string `json:"gone_at,omitempty"`
	// Outcome is absent while queued, and while a gone withdrawal waits
	// for the scanner to reach the blocks its payout could be in.
	Outcome       *string `json:"outcome,omitempty"`
	OutcomeReason string  `json:"outcome_reason,omitempty"`
	PaidHeight    *int64  `json:"paid_height,omitempty"`
	PaidAt        *string `json:"paid_at,omitempty"`
	PaidUtia      *int64  `json:"paid_utia,omitempty"`
	PayoutDelayS  *int64  `json:"payout_delay_s,omitempty"`
	PayoutLagS    *int64  `json:"payout_lag_s,omitempty"`
}

// queueCheck compares an account's escrow read with its queue read. The
// module keeps balance - available equal to the queued sum; the two reads
// can only be compared when they came from the same height.
type queueCheck struct {
	Height                    int64 `json:"height"`
	BalanceMinusAvailableUtia int64 `json:"balance_minus_available_utia"`
	PendingUtia               int64 `json:"pending_utia"`
	Consistent                bool  `json:"consistent"`
}

// publisherWithdrawals is the "withdrawals" object of /v1/publishers/{addr}.
// The read it rests on is the page's publisher.pending_withdrawals.
type publisherWithdrawals struct {
	pendingQueue
	Pending []withdrawalRow `json:"pending"`
	// LeftQueue is the most recent withdrawals that left the queue,
	// newest departure first.
	LeftQueue []withdrawalRow `json:"left_queue"`
	Check     *queueCheck     `json:"check"`
}

// payoutDelay summarises request→payout over executed withdrawals.
type payoutDelay struct {
	Count     int64  `json:"count"`
	MedianS   *int64 `json:"median_s"`
	MinS      *int64 `json:"min_s"`
	MaxS      *int64 `json:"max_s"`
	MedianLag *int64 `json:"median_lag_s"`
}

// outcomeSum counts withdrawals that left the queue with one outcome.
type outcomeSum struct {
	Count int64 `json:"count"`
	Utia  int64 `json:"utia"`
}

// withdrawalQueue is the "withdrawal_queue" object of /v1/market.
type withdrawalQueue struct {
	Pending pendingSummary `json:"pending"`
	// Publishers is how many accounts have at least one withdrawal queued.
	Publishers int64 `json:"publishers"`
	// Left the queue in the window (by the block time of the first read
	// that no longer had it), by outcome. Waiting ones have no outcome
	// yet and are counted in Unresolved.
	Executed     outcomeSum  `json:"executed"`
	Consumed     outcomeSum  `json:"consumed"`
	Unattributed outcomeSum  `json:"unattributed"`
	Unresolved   outcomeSum  `json:"unresolved"`
	PayoutDelay  payoutDelay `json:"payout_delay"`
}

func nullStr(v sql.NullString) *string {
	if !v.Valid || v.String == "" {
		return nil
	}
	s := v.String
	return &s
}

func nullInt(v sql.NullInt64) *int64 {
	if !v.Valid {
		return nil
	}
	n := v.Int64
	return &n
}

// secondsBetween is b - a in whole seconds, for two stored timestamps.
func secondsBetween(a, b string) (int64, bool) {
	ta, err := time.Parse(store.TimeLayout, a)
	if err != nil {
		return 0, false
	}
	tb, err := time.Parse(store.TimeLayout, b)
	if err != nil {
		return 0, false
	}
	return int64(tb.Sub(ta) / time.Second), true
}

const withdrawalCols = `requested_at, available_at, denom, amount_utia, first_amount_utia,
	first_seen_height, first_seen_at, last_seen_height, last_seen_at, gone_height, gone_at,
	outcome, outcome_reason, paid_height, paid_at, paid_utia`

func scanWithdrawal(rows *sql.Rows) (withdrawalRow, error) {
	var w withdrawalRow
	var goneH, paidH, paidU sql.NullInt64
	var goneAt, outcome, paidAt sql.NullString
	if err := rows.Scan(&w.RequestedAt, &w.AvailableAt, &w.Denom, &w.AmountUtia, &w.FirstAmountUtia,
		&w.FirstSeenHeight, &w.FirstSeenAt, &w.LastSeenHeight, &w.LastSeenAt, &goneH, &goneAt,
		&outcome, &w.OutcomeReason, &paidH, &paidAt, &paidU); err != nil {
		return w, err
	}
	w.ReducedUtia = w.FirstAmountUtia - w.AmountUtia
	if w.ReducedUtia < 0 {
		w.ReducedUtia = 0
	}
	w.GoneHeight, w.GoneAt = nullInt(goneH), nullStr(goneAt)
	w.Outcome = nullStr(outcome)
	w.PaidHeight, w.PaidAt, w.PaidUtia = nullInt(paidH), nullStr(paidAt), nullInt(paidU)
	if w.Outcome != nil && *w.Outcome == store.WithdrawalExecuted && w.PaidAt != nil {
		if d, ok := secondsBetween(w.RequestedAt, *w.PaidAt); ok {
			w.PayoutDelayS = &d
		}
		if d, ok := secondsBetween(w.AvailableAt, *w.PaidAt); ok {
			w.PayoutLagS = &d
		}
	}
	return w, nil
}

func (s *Server) withdrawalRows(ctx context.Context, where string, limit int, args ...any) ([]withdrawalRow, error) {
	rows, err := s.st.DB().QueryContext(ctx, `SELECT `+withdrawalCols+` FROM withdrawal_queue WHERE `+where+
		fmt.Sprintf(` LIMIT %d`, limit), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []withdrawalRow{}
	for rows.Next() {
		w, err := scanWithdrawal(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, w)
	}
	return out, rows.Err()
}

// pendingByPublisher is every polled account's queue summary, keyed by
// address. An account whose queue was never read is absent, which the
// caller shows as "not read" rather than as an empty queue.
func (s *Server) pendingByPublisher(ctx context.Context, only string) (map[string]*pendingSummary, error) {
	filter, args := "", []any{}
	if only != "" {
		filter, args = " AND e.publisher = ?", append(args, only)
	}
	rows, err := s.st.DB().QueryContext(ctx, `SELECT e.publisher, e.withdrawals_height, e.withdrawals_at,
			COUNT(q.requested_at), COALESCE(SUM(q.amount_utia), 0), MIN(q.available_at),
			COALESCE(SUM(q.first_amount_utia - q.amount_utia), 0)
		FROM escrow_accounts e
		LEFT JOIN withdrawal_queue q ON q.publisher = e.publisher AND q.gone_height IS NULL
		WHERE e.withdrawals_height IS NOT NULL`+filter+`
		GROUP BY e.publisher, e.withdrawals_height, e.withdrawals_at`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]*pendingSummary{}
	for rows.Next() {
		var pub string
		var h sql.NullInt64
		var at, next sql.NullString
		p := &pendingSummary{}
		if err := rows.Scan(&pub, &h, &at, &p.Count, &p.Utia, &next, &p.ReducedUtia); err != nil {
			return nil, err
		}
		p.ReadHeight, p.ReadAt, p.NextAvailableAt = nullInt(h), nullStr(at), nullStr(next)
		out[pub] = p
	}
	return out, rows.Err()
}

// attachPending sets each row's pending_withdrawals.
func (s *Server) attachPending(ctx context.Context, rows []publisherRow) error {
	if len(rows) == 0 {
		return nil
	}
	only := ""
	if len(rows) == 1 {
		only = rows[0].Publisher
	}
	m, err := s.pendingByPublisher(ctx, only)
	if err != nil {
		return fmt.Errorf("pending withdrawals: %w", err)
	}
	for i := range rows {
		rows[i].PendingWithdrawals = m[rows[i].Publisher]
	}
	return nil
}

// publisherWithdrawalDetail is the queue section of one publisher's page.
// It returns nil when the account's queue has never been read.
func (s *Server) publisherWithdrawalDetail(ctx context.Context, addr string) (*publisherWithdrawals, error) {
	m, err := s.pendingByPublisher(ctx, addr)
	if err != nil {
		return nil, err
	}
	sum := m[addr]
	if sum == nil {
		return nil, nil
	}
	out := &publisherWithdrawals{pendingQueue: sum.pendingQueue}
	if out.Pending, err = s.withdrawalRows(ctx, `publisher = ? AND gone_height IS NULL ORDER BY available_at, requested_at`, 500, addr); err != nil {
		return nil, err
	}
	if out.LeftQueue, err = s.withdrawalRows(ctx, `publisher = ? AND gone_height IS NOT NULL ORDER BY gone_height DESC, requested_at DESC`, 50, addr); err != nil {
		return nil, err
	}
	var found, bal, avail, h, wh, pending sql.NullInt64
	err = s.st.DB().QueryRowContext(ctx, `SELECT found, balance_utia, available_utia, height, withdrawals_height, pending_utia
		FROM escrow_accounts WHERE publisher = ?`, addr).Scan(&found, &bal, &avail, &h, &wh, &pending)
	if err != nil && err != sql.ErrNoRows {
		return nil, err
	}
	if err == nil && found.Int64 == 1 && h.Valid && wh.Valid && h.Int64 == wh.Int64 {
		diff := bal.Int64 - avail.Int64
		out.Check = &queueCheck{Height: h.Int64, BalanceMinusAvailableUtia: diff, PendingUtia: pending.Int64, Consistent: diff == pending.Int64}
	}
	return out, nil
}

// withdrawalQueueSummary is the market's queue section. Outcomes and payout
// delays are the window's; pending is the current queue.
func (s *Server) withdrawalQueueSummary(ctx context.Context, win Window) (*withdrawalQueue, error) {
	db := s.st.DB()
	q := &withdrawalQueue{}
	var next sql.NullString
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*), COALESCE(SUM(amount_utia), 0), MIN(available_at),
			COALESCE(SUM(first_amount_utia - amount_utia), 0), COUNT(DISTINCT publisher)
		FROM withdrawal_queue WHERE gone_height IS NULL`).
		Scan(&q.Pending.Count, &q.Pending.Utia, &next, &q.Pending.ReducedUtia, &q.Publishers); err != nil {
		return nil, fmt.Errorf("pending: %w", err)
	}
	q.Pending.NextAvailableAt = nullStr(next)
	if v, err := s.st.Meta("withdrawals_polled_height"); err == nil && v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			q.Pending.ReadHeight = &n
		}
	}
	if v, err := s.st.Meta("withdrawals_polled_at"); err == nil && v != "" {
		q.Pending.ReadAt = &v
	}

	start, end := win.startArg(), win.endArg()
	rows, err := db.QueryContext(ctx, `SELECT COALESCE(outcome, ''), COUNT(*), COALESCE(SUM(COALESCE(paid_utia, amount_utia)), 0)
		FROM withdrawal_queue WHERE gone_height IS NOT NULL AND gone_at >= ? AND gone_at <= ?
		GROUP BY COALESCE(outcome, '')`, start, end)
	if err != nil {
		return nil, fmt.Errorf("outcomes: %w", err)
	}
	for rows.Next() {
		var o string
		var c outcomeSum
		if err := rows.Scan(&o, &c.Count, &c.Utia); err != nil {
			rows.Close()
			return nil, err
		}
		switch o {
		case store.WithdrawalExecuted:
			q.Executed = c
		case store.WithdrawalConsumed:
			q.Consumed = c
		case store.WithdrawalUnattributed:
			q.Unattributed = c
		default:
			q.Unresolved = c
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// Delays of the payouts in the window, by payout time.
	drows, err := db.QueryContext(ctx, `SELECT requested_at, available_at, paid_at FROM withdrawal_queue
		WHERE outcome = ? AND paid_at IS NOT NULL AND paid_at >= ? AND paid_at <= ?`, store.WithdrawalExecuted, start, end)
	if err != nil {
		return nil, fmt.Errorf("payout delay: %w", err)
	}
	var delays, lags []int64
	for drows.Next() {
		var req, avail, paid string
		if err := drows.Scan(&req, &avail, &paid); err != nil {
			drows.Close()
			return nil, err
		}
		if d, ok := secondsBetween(req, paid); ok {
			delays = append(delays, d)
		}
		if d, ok := secondsBetween(avail, paid); ok {
			lags = append(lags, d)
		}
	}
	drows.Close()
	if err := drows.Err(); err != nil {
		return nil, err
	}
	q.PayoutDelay = summariseDelays(delays, lags)
	return q, nil
}

// summariseDelays is count, median, min and max of the delays and the median
// of the lags. The median of an even count is the lower middle value: a
// value that occurred, not an average of two that did.
func summariseDelays(delays, lags []int64) payoutDelay {
	out := payoutDelay{Count: int64(len(delays))}
	med := func(v []int64) *int64 {
		if len(v) == 0 {
			return nil
		}
		c := append([]int64(nil), v...)
		sort.Slice(c, func(i, j int) bool { return c[i] < c[j] })
		m := c[(len(c)-1)/2]
		return &m
	}
	if len(delays) > 0 {
		c := append([]int64(nil), delays...)
		sort.Slice(c, func(i, j int) bool { return c[i] < c[j] })
		lo, hi := c[0], c[len(c)-1]
		out.MinS, out.MaxS = &lo, &hi
		out.MedianS = med(delays)
	}
	out.MedianLag = med(lags)
	return out
}
