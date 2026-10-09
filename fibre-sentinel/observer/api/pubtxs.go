package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"

	"github.com/cosmos/cosmos-sdk/types/bech32"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/failedtx"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
)

// /v1/publishers/{addr}/txs is one account's Fibre transactions, successful
// and failed, newest first by (height, tx index, message index), a page at a
// time over its whole record: what its publisher page lists under All
// (view=all) and under Escrow (view=escrow).
//
// A success is a payments row of the account, the account the module
// charged or credited: a blob payment (settlement), a deposit, a withdrawal
// request, and a timeout of one of its promises (whoever submitted it, the
// charge is the account's). One row per message, as the payments table has
// them. A payout is no transaction of its own (the begin-blocker pays it,
// with no hash): it is the payout of the request it paid, on that request's
// row (payout, below). Escrow is the deposits and the withdrawal requests.
//
// A failure is one row per failed transaction, under the lists' rules:
//
//   - a failed blob payment as the Blobs list lists it among the account's
//     blobs (/v1/blobs?include_failed=1&publisher=, failedblobs.go): by its
//     promise's publisher, final or not, at the top level or inside a
//     MsgExec, so All and the publisher page's Blobs hold the same failed
//     payments;
//   - a failed deposit or withdrawal request by its signer, as a validator's
//     endpoint history lists its failed registrations: a final failure's
//     top-level message only (failed_tx_msgs, migration 31).
//
// A failed timeout is listed under no account: anyone can time out anyone's
// promise. A failure moved nothing: it carries no amount and is in no sum,
// and its code, codespace, reason and finality are its record's. A page
// holding a failure that is not final is not cached, as /v1/txs/{hash} does
// not cache one: its transaction could still take effect in a later block.
//
// A successful withdrawal request carries its payout when its withdrawal is
// on record in the queue history (withdrawal_queue, store/withdrawals.go).
// The link is exact: x/fibre keys a withdrawal by its signer and the block
// time of its request (msg_server.go: requestedTimestamp is ctx.BlockTime(),
// and a second request of the signer in the same block is refused), block
// times only grow, and the payments row and the queue row store that block
// time in the one layout (store.TS), so the row is sought by its primary
// key (publisher, requested_at = the request's time). Its state:
//
//   - pending: queued at the last read of the queue, or gone from it with
//     its outcome not settled yet (the payout may be in a block the scanner
//     has not read): no payout is on record, whatever the time;
//   - paid: the queue history attributed exactly one payout on record to it
//     (outcome executed: the begin-blocker's EventWithdrawFromEscrowExecuted
//     of its amount between the two reads that bracket its departure), with
//     that payout's height, time and amount; never inferred from the time;
//   - consumed: it left the queue before it could be paid, so settlements
//     that found the available balance short used it up (outcome consumed);
//   - unattributed: it left the queue once payable, but no single payout
//     on record can be said to be its own.
//
// A request whose withdrawal the queue history never saw (one requested and
// gone between two reads, or before the queue was read) has no payout.
//
// The escrow's statement (view=escrow, sums) is every successful movement of
// the account over its whole record: what it deposited, what its blobs'
// fees and its timed-out promises were charged, and what was paid out to
// it. Deposited − fees − charged − withdrawn is the balance the chain holds,
// once the escrow read and the scanner stand at the same block.

// The views, by the kinds of payments rows each lists, and the failures.
const (
	pubTxViewAll    = "all"
	pubTxViewEscrow = "escrow"
	// the payments kinds each view lists, as SQL literals: a withdrawal
	// payout is no transaction
	pubTxKindsAll    = `'settlement', 'deposit', 'withdrawal_request', 'timeout'`
	pubTxKindsEscrow = `'deposit', 'withdrawal_request'`
)

// A page of /v1/publishers/{addr}/txs: 25 rows unless asked, 100 at most, and
// the offset /v1/blobs stops at.
const (
	pubTxPageDefault = 25
	pubTxPageMax     = 100
)

// The payout states of a withdrawal request.
const (
	payoutPending      = "pending"
	payoutPaid         = "paid"
	payoutConsumed     = "consumed"
	payoutUnattributed = "unattributed"
)

// The statements of the route, each a seek of an index, kept as constants so
// the plan test reads what the route runs.
const (
	// the account's counts and sums over its whole record, in one walk of
	// its payments_publisher_time range
	pubTxSumsSQL = `SELECT
			COALESCE(SUM(CASE WHEN kind = 'settlement' THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN kind = 'settlement' THEN amount_utia ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN kind = 'deposit' THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN kind = 'deposit' THEN amount_utia ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN kind = 'withdrawal_request' THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN kind = 'withdrawal_executed' THEN amount_utia ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN kind = 'timeout' THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN kind = 'timeout' THEN amount_utia ELSE 0 END), 0)
		FROM payments WHERE publisher = ?`
	// the final failed top-level deposits and withdrawal requests the
	// account signed, newest first, with each failure's record: the primary
	// key's order (failed_tx_msgs holds only those messages)
	pubTxFailedMsgsSQL = `SELECT m.height, m.tx_index, m.msg_index, m.type_url, f.tx_hash, f.time, f.raw_json
		FROM failed_tx_msgs m JOIN failed_txs f ON f.dedupe_key = m.dedupe_key
		WHERE m.account = ? AND m.type_url IN (?, ?)
		ORDER BY m.height DESC, m.tx_index DESC, m.msg_index DESC`
	// a withdrawal request's queue row, by its primary key
	pubTxQueueSQL = `SELECT available_at, gone_height, outcome, paid_height, paid_at, paid_utia
		FROM withdrawal_queue WHERE publisher = ? AND requested_at = ?`
)

// pubTxPageSQL is a page of the account's successes of kinds, newest first.
// The block time stands for the height in the order (block times only grow,
// and a block's rows share its time), so payments_publisher_time gives the
// order and only the rows of one block are sorted among themselves.
func pubTxPageSQL(kinds string) string {
	return `SELECT kind, height, tx_index, msg_index, time, tx_hash, promise_hash, amount_utia
		FROM payments WHERE publisher = ? AND +kind IN (` + kinds + `)
		ORDER BY time DESC, height DESC, tx_index DESC, msg_index DESC LIMIT ? OFFSET ?`
}

// pubTxBeforeSQL counts, up to a bound, the account's successes of kinds that
// stand before a failure at (height, tx index): in a later block, or in its
// block at a later index. The failure's block time bounds the range of
// payments_publisher_time read, so the count reads the rows a page needs,
// never the account's whole record. Its arguments: the account, the
// failure's time, its height twice, its index, the bound.
func pubTxBeforeSQL(kinds string) string {
	return `SELECT COUNT(*) FROM (SELECT 1 FROM payments WHERE publisher = ? AND +kind IN (` + kinds + `)
		AND time >= ? AND (height > ? OR (height = ? AND tx_index > ?)) LIMIT ?)`
}

// pubTxPayout is what became of a successful withdrawal request.
type pubTxPayout struct {
	State string `json:"state"` // pending | paid | consumed | unattributed
	// AvailableAt is when it became payable: the chain's own timestamp, the
	// request's block time plus the withdrawal delay in force then.
	AvailableAt string `json:"available_at"`
	// paid only: the payout on record, and how long after the request it
	// came (whole seconds)
	PaidHeight   *int64  `json:"paid_height,omitempty"`
	PaidAt       *string `json:"paid_at,omitempty"`
	PaidUtia     *int64  `json:"paid_utia,omitempty"`
	PayoutDelayS *int64  `json:"payout_delay_s,omitempty"`
}

// pubTx is one transaction of the account (one message of a success, one
// failed transaction).
type pubTx struct {
	Kind     string `json:"kind"`   // settlement | deposit | withdrawal_request | timeout
	Status   string `json:"status"` // success | failed
	Height   int64  `json:"height"`
	TxIndex  int    `json:"tx_index"`
	MsgIndex int    `json:"msg_index"`
	Time     string `json:"time"`
	TxHash   string `json:"tx_hash"`
	// AmountUtia is what a success moved: a blob's fee, a timed-out
	// promise's charge, a deposit, the amount a withdrawal requested
	// (moved from available into the queue; only its payout leaves the
	// balance). Absent on a failure, which moved nothing.
	AmountUtia  *int64 `json:"amount_utia,omitempty"`
	PromiseHash string `json:"promise_hash,omitempty"`
	// a failure's: the chain's code, failedtx.Explain's reason where it has
	// one, and whether it is final (/v1/txs/{hash}'s final)
	Code      *uint32 `json:"code,omitempty"`
	Codespace string  `json:"codespace,omitempty"`
	Reason    string  `json:"reason,omitempty"`
	Final     *bool   `json:"final,omitempty"`
	// Payout: a successful withdrawal request's, when its withdrawal is on
	// record in the queue history
	Payout *pubTxPayout `json:"payout,omitempty"`
}

// pubTxSums is the escrow's statement over the account's whole record: its
// successful movements alone.
type pubTxSums struct {
	DepositedUtia int64 `json:"deposited_utia"`
	Settlements   int64 `json:"settlements"`
	FeesUtia      int64 `json:"fees_utia"`
	Timeouts      int64 `json:"timeouts"`
	ChargedUtia   int64 `json:"charged_utia"`
	// WithdrawnUtia is what was paid out to it: every payout, attributed
	// to its request or not
	WithdrawnUtia int64 `json:"withdrawn_utia"`
}

// pubTxsAnswer is /v1/publishers/{addr}/txs.
type pubTxsAnswer struct {
	Publisher string `json:"publisher"`
	View      string `json:"view"`
	Limit     int    `json:"limit"`
	Offset    int    `json:"offset"`
	// Total is every transaction the view holds, successful and failed;
	// FailedTotal the failed ones among them.
	Total       int64      `json:"total"`
	FailedTotal int        `json:"failed_total"`
	Txs         []pubTx    `json:"txs"`
	Sums        *pubTxSums `json:"sums,omitempty"` // view=escrow only
}

func (s *Server) handlePublisherTxs(w http.ResponseWriter, r *http.Request) {
	// Bech32 may be written in upper case; the store holds it in lower.
	addr := strings.ToLower(strings.TrimSpace(r.PathValue("addr")))
	if hrp, _, err := bech32.DecodeAndConvert(addr); err != nil || hrp != "celestia" {
		writeErr(w, 400, "publisher must be a celestia1... account address")
		return
	}
	q := r.URL.Query()
	view := q.Get("view")
	switch view {
	case "":
		view = pubTxViewAll
	case pubTxViewAll, pubTxViewEscrow:
	default:
		writeErr(w, 400, "view must be all or escrow")
		return
	}
	limit, err := parseLimit(r, pubTxPageDefault, pubTxPageMax)
	if err != nil {
		writeErr(w, 400, err.Error())
		return
	}
	offset := 0
	if raw := q.Get("offset"); raw != "" {
		o, err := strconv.Atoi(raw)
		if err != nil || o < 0 || o > maxBlobOffset {
			writeErr(w, 400, fmt.Sprintf("offset must be an integer from 0 to %d", maxBlobOffset))
			return
		}
		offset = o
	}
	a, err := s.publisherTxs(r.Context(), addr, view, limit, offset)
	if err != nil {
		s.writeInternal(w, r.URL.Path, err)
		return
	}
	for _, t := range a.Txs {
		if t.Final != nil && !*t.Final {
			w.Header().Set("Cache-Control", "no-store")
			break
		}
	}
	writeJSON(w, 200, a)
}

// publisherTxs is the page of view at offset: the failures of the view placed
// among its successes by counting (pubTxFailsOnPage), the successes of the
// page read at the offset that leaves, and the two merged by place.
func (s *Server) publisherTxs(ctx context.Context, addr, view string, limit, offset int) (*pubTxsAnswer, error) {
	a := &pubTxsAnswer{Publisher: addr, View: view, Limit: limit, Offset: offset, Txs: []pubTx{}}
	var sums pubTxSums
	var deposits, requests int64
	if err := s.st.DB().QueryRowContext(ctx, pubTxSumsSQL, addr).Scan(&sums.Settlements, &sums.FeesUtia, &deposits, &sums.DepositedUtia,
		&requests, &sums.WithdrawnUtia, &sums.Timeouts, &sums.ChargedUtia); err != nil {
		return nil, err
	}
	kinds := pubTxKindsAll
	successes := sums.Settlements + deposits + requests + sums.Timeouts
	if view == pubTxViewEscrow {
		kinds, successes = pubTxKindsEscrow, deposits+requests
		a.Sums = &sums
	}
	fails, err := s.publisherFailures(ctx, addr, view == pubTxViewAll)
	if err != nil {
		return nil, err
	}
	a.Total, a.FailedTotal = successes+int64(len(fails)), len(fails)
	k, m, err := s.pubTxFailsOnPage(ctx, addr, kinds, fails, offset, limit)
	if err != nil {
		return nil, err
	}
	oks, err := s.pubTxSuccesses(ctx, addr, kinds, limit-m, offset-k)
	if err != nil {
		return nil, err
	}
	// the page: its successes and its failures, merged by place; a success
	// and a failure never share a transaction
	here := fails[k : k+m]
	for i, j := 0, 0; i < len(oks) || j < len(here); {
		if j < len(here) && (i == len(oks) || standsBefore(here[j].Height, here[j].TxIndex, oks[i].Height, oks[i].TxIndex)) {
			a.Txs = append(a.Txs, here[j])
			j++
			continue
		}
		a.Txs = append(a.Txs, oks[i])
		i++
	}
	return a, nil
}

// pubTxSuccesses is a page of the account's successes of kinds, newest first,
// each withdrawal request with its payout.
func (s *Server) pubTxSuccesses(ctx context.Context, addr, kinds string, limit, offset int) ([]pubTx, error) {
	if limit <= 0 {
		return nil, nil
	}
	rows, err := s.st.DB().QueryContext(ctx, pubTxPageSQL(kinds), addr, limit, offset)
	if err != nil {
		return nil, err
	}
	var out []pubTx
	for rows.Next() {
		t := pubTx{Status: "success"}
		var amount int64
		if err := rows.Scan(&t.Kind, &t.Height, &t.TxIndex, &t.MsgIndex, &t.Time, &t.TxHash, &t.PromiseHash, &amount); err != nil {
			rows.Close()
			return nil, err
		}
		t.TxHash = strings.ToLower(t.TxHash)
		t.AmountUtia = &amount
		out = append(out, t)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for i := range out {
		if out[i].Kind != failedtx.KindWithdrawalRequest {
			continue
		}
		if out[i].Payout, err = s.pubTxPayoutOf(ctx, addr, out[i].Time); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// pubTxPayoutOf is the payout of addr's withdrawal requested at the block time
// at, or nil when the queue history never saw it.
func (s *Server) pubTxPayoutOf(ctx context.Context, addr, at string) (*pubTxPayout, error) {
	var p pubTxPayout
	var gone, paidH, paidU sql.NullInt64
	var outcome, paidAt sql.NullString
	err := s.st.DB().QueryRowContext(ctx, pubTxQueueSQL, addr, at).Scan(&p.AvailableAt, &gone, &outcome, &paidH, &paidAt, &paidU)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	p.State = payoutPending
	switch {
	case !gone.Valid:
		// still queued at the last read
	case outcome.String == store.WithdrawalExecuted && paidH.Valid && paidAt.Valid && paidU.Valid:
		p.State = payoutPaid
		p.PaidHeight, p.PaidAt, p.PaidUtia = nullInt(paidH), &paidAt.String, nullInt(paidU)
		if d, ok := secondsBetween(at, paidAt.String); ok {
			p.PayoutDelayS = &d
		}
	case outcome.String == store.WithdrawalConsumed:
		p.State = payoutConsumed
	case outcome.String == store.WithdrawalUnattributed:
		p.State = payoutUnattributed
	}
	// any other: gone, its outcome not settled yet, so no payout is on
	// record yet
	return &p, nil
}

// publisherFailures are the account's failed transactions, newest first, one
// per transaction: its final failed top-level deposits and withdrawal
// requests, and with blobs its failed blob payments as the Blobs list lists
// them. A transaction listed both ways is its message with the lower index.
// A record that does not decode is a row fault and left out.
func (s *Server) publisherFailures(ctx context.Context, addr string, blobs bool) ([]pubTx, error) {
	byTx := map[[2]int64]pubTx{}
	keep := func(t pubTx) {
		key := [2]int64{t.Height, int64(t.TxIndex)}
		if had, ok := byTx[key]; !ok || t.MsgIndex < had.MsgIndex {
			byTx[key] = t
		}
	}
	rows, err := s.st.DB().QueryContext(ctx, pubTxFailedMsgsSQL, addr,
		kindTypeURL[failedtx.KindDeposit], kindTypeURL[failedtx.KindWithdrawalRequest])
	if err != nil {
		return nil, err
	}
	type msgRow struct {
		height          int64
		txIndex, msgIdx int
		typeURL         string
		hash, at, raw   string
	}
	var msgs []msgRow
	for rows.Next() {
		var m msgRow
		if err := rows.Scan(&m.height, &m.txIndex, &m.msgIdx, &m.typeURL, &m.hash, &m.at, &m.raw); err != nil {
			rows.Close()
			return nil, err
		}
		msgs = append(msgs, m)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for _, m := range msgs {
		var r failedtx.Record
		if err := json.Unmarshal([]byte(m.raw), &r); err != nil {
			if f, ok := isRowFault(rowFault(ctx, strings.ToLower(m.hash), "failed transaction", err)); ok {
				s.noteRowFault(f)
				continue
			}
			return nil, err
		}
		code, final := r.Code, r.AntePassed
		keep(pubTx{Kind: failedtx.Kind(m.typeURL), Status: "failed", Height: m.height, TxIndex: m.txIndex, MsgIndex: m.msgIdx, Time: m.at,
			TxHash: strings.ToLower(m.hash), Code: &code, Codespace: r.Codespace, Reason: failedtx.Explain(r).Reason, Final: &final})
	}
	if blobs {
		pays, err := s.failedPaysFor(ctx, blobsAsk{publisher: addr})
		if err != nil {
			return nil, err
		}
		for _, f := range pays {
			code, final := f.Code, f.Final
			keep(pubTx{Kind: failedtx.KindSettlement, Status: "failed", Height: f.SettlementHeight, TxIndex: f.SettlementTxIndex, MsgIndex: f.msgIndex,
				Time: f.SettlementTime, TxHash: f.SettlementTxHash, PromiseHash: f.PromiseHash, Code: &code, Codespace: f.Codespace, Reason: f.Reason, Final: &final})
		}
	}
	out := make([]pubTx, 0, len(byTx))
	for _, t := range byTx {
		out = append(out, t)
	}
	sort.Slice(out, func(i, j int) bool {
		return standsBefore(out[i].Height, out[i].TxIndex, out[j].Height, out[j].TxIndex)
	})
	return out, nil
}

// pubTxFailsOnPage places fails (newest first) among the account's successes
// of kinds: k of them stand before the page's first row, m on the page. A
// failure's place is its index among the failures plus the successes before
// it, which grows with the index: k is found by halving, m by counting on
// from k (failedOnPage's way, over payments).
func (s *Server) pubTxFailsOnPage(ctx context.Context, addr, kinds string, fails []pubTx, offset, limit int) (k, m int, err error) {
	q := pubTxBeforeSQL(kinds)
	// ahead reports whether failure j stands before the at-th row of the
	// list: fewer than at-j successes stand before it
	ahead := func(j, at int) (bool, error) {
		room := at - j
		if room <= 0 {
			return false, nil
		}
		f := fails[j]
		var n int
		if err := s.st.DB().QueryRowContext(ctx, q, addr, f.Time, f.Height, f.Height, f.TxIndex, room).Scan(&n); err != nil {
			return false, err
		}
		return n < room, nil
	}
	lo, hi := 0, min(len(fails), offset)
	for lo < hi {
		mid := (lo + hi) / 2
		in, err := ahead(mid, offset)
		if err != nil {
			return 0, 0, err
		}
		if in {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	k = lo
	for j := k; j < len(fails); j++ {
		in, err := ahead(j, offset+limit)
		if err != nil {
			return 0, 0, err
		}
		if !in {
			break
		}
		m++
	}
	return k, m, nil
}

// topIndexOf is the index in its transaction of the top-level message that is
// m or carries it (a MsgExec): where a message firstOfKind found stands.
func topIndexOf(ms []failedtx.Msg, m *failedtx.Msg) int {
	for i := range ms {
		if &ms[i] == m {
			return ms[i].Index
		}
		for j := range ms[i].Inner {
			if &ms[i].Inner[j] == m {
				return ms[i].Index
			}
		}
	}
	return 0
}
