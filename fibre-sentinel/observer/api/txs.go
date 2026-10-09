package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	fibretypes "github.com/celestiaorg/celestia-app/v10/x/fibre/types"
	valaddrtypes "github.com/celestiaorg/celestia-app/v10/x/valaddr/types"
	sdk "github.com/cosmos/cosmos-sdk/types"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/failedtx"
	"github.com/plsgiveup/fibre/fibre-sentinel/internal/txcost"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
)

// /v1/txs/{hash} is one Fibre transaction by its hash, successful or failed:
// its block, its messages, what it cost, what it did or asked for, why it
// failed, and the pages it touches. The site's transaction page reads it.
//
// A success is read from the scanner's record of what it cost
// (tx_costs.jsonl, migration 31) and joined to what the chain record says it
// did: the publication it settled, the escrow movements it made, the
// endpoint registrations it wrote. A success from before that record began
// is found by its publication or its movements alone, with no cost. A
// failure is read from failed_txs.jsonl (failedtx.go). A success wins over a
// failure of the same hash: a failure that did not pass the ante handler
// could land again, and only a success is what the chain kept.
//
// A failure's related links follow the lists' rules: only a final failure,
// only its top-level message, never a failed timeout's publisher, and a
// validator only by its current operator address. An account those rules
// would not link is given as plain text (effect.publisher), so a name a
// stranger planted in a message is shown as the message names it and
// nothing more.

// The statements of the lookup, each a seek, kept as constants so the plan
// test reads what the route runs.
const (
	// a success's cost line by its hash: tx_costs_tx seeks the hash and
	// gives the order
	txCostByHashSQL = `SELECT raw_json FROM tx_costs WHERE tx_hash = ? ORDER BY height DESC, tx_index DESC LIMIT 1`
	// the cost line at a settlement's position, for the blob page
	txCostAtSQL = `SELECT raw_json FROM tx_costs WHERE height = ? AND tx_index = ?`
	// a transaction's escrow movements, in message order (payments_tx)
	paymentsByTxSQL = `SELECT kind, height, tx_index, msg_index, time, publisher, processor, promise_hash, namespace, blob_size, amount_utia, COALESCE(available_at, '')
		FROM payments WHERE tx_hash = ? ORDER BY msg_index`
	// the endpoint registrations a transaction wrote, one per validator it
	// registered for (host_events_at; from_tx_index is the tx index + 1)
	hostEventsAtSQL = `SELECT cons_address FROM host_events WHERE from_height = ? AND from_tx_index = ? AND source = 'event' ORDER BY cons_address`
	// whether an account has a publisher page: the publisher page's own 404 rule
	publisherKnownSQL = `SELECT 1 FROM payments WHERE publisher = ? LIMIT 1`
	// the publication a failed settlement's promise settled in later
	settledLaterSQL = `SELECT promise_hash, commitment, blob_version, settlement_height FROM publications WHERE promise_hash = ?`
	// a validator as a related link names it: the validator list's rule for
	// its avatar
	txValidatorSQL = `SELECT moniker, EXISTS (SELECT 1 FROM validator_avatars a WHERE UPPER(a.identity) = UPPER(vi.identity) AND a.status = 'ok'), identity
		FROM validator_identities vi WHERE vi.cons_address = ?`
)

// txKindSeveral is the kind of a transaction whose Fibre messages are of
// more than one kind.
const txKindSeveral = "several"

// kindTypeURL is the type URL of each Fibre kind, by the type URL the SDK
// gives the registered Go types (failedtx.Kind's inverse).
var kindTypeURL = map[string]string{
	failedtx.KindSettlement:        sdk.MsgTypeURL(&fibretypes.MsgPayForFibre{}),
	failedtx.KindDeposit:           sdk.MsgTypeURL(&fibretypes.MsgDepositToEscrow{}),
	failedtx.KindWithdrawalRequest: sdk.MsgTypeURL(&fibretypes.MsgRequestWithdrawal{}),
	failedtx.KindTimeout:           sdk.MsgTypeURL(&fibretypes.MsgPaymentPromiseTimeout{}),
	failedtx.KindSetHost:           sdk.MsgTypeURL(&valaddrtypes.MsgSetFibreProviderInfo{}),
}

// txAnswer is /v1/txs/{hash}.
type txAnswer struct {
	TxHash string `json:"tx_hash"`
	Status string `json:"status"` // success | failed
	// Final is a failure's ante_passed, false included: the fee and every
	// signer's sequence were taken, so the same transaction can never be
	// in a block again. Absent on a success.
	Final   *bool     `json:"final,omitempty"`
	Height  int64     `json:"height"`
	TxIndex int       `json:"tx_index"`
	Time    time.Time `json:"time"`
	// Kind is failedtx's kind of its Fibre messages, top level and one level
	// inside a MsgExec, or "several".
	Kind     string  `json:"kind"`
	Messages []txMsg `json:"messages"`
	// MessagesPartial: a success found by its movements alone, whose record
	// holds only its Fibre messages.
	MessagesPartial bool       `json:"messages_partial,omitempty"`
	Cost            *txCost    `json:"cost,omitempty"`
	Failure         *txFailure `json:"failure,omitempty"`
	// Effect is what it did, or for a failure what it asked for, per kind;
	// {} when nothing is known.
	Effect  map[string]any `json:"effect"`
	Related txRelated      `json:"related"`
}

// txMsg is one message of the transaction, in its order.
type txMsg struct {
	Index   int    `json:"index"`
	TypeURL string `json:"type_url"`
	Fibre   bool   `json:"fibre"` // failedtx.IsFibre
	// Signer: a Fibre message's, as written; absent when the message's
	// strings were cut (failedtx.CutField)
	Signer string  `json:"signer,omitempty"`
	Inner  []txMsg `json:"inner,omitempty"`
}

// txCost is the transaction's own gas and fee: the cost line's for a
// success, the failure record's for a failure (which keeps no payer).
type txCost struct {
	GasWanted int64  `json:"gas_wanted"`
	GasUsed   int64  `json:"gas_used"`
	Fee       string `json:"fee,omitempty"`
	FeePayer  string `json:"fee_payer,omitempty"`
}

// txFailure is a failure's error, as /v1/blobs?tx= answers it (failedtx.Explain).
type txFailure struct {
	Code           uint32 `json:"code"`
	Codespace      string `json:"codespace"`
	Reason         string `json:"reason,omitempty"`
	FailedMsgIndex *int   `json:"failed_msg_index,omitempty"`
	Log            string `json:"log"`
	LogCut         bool   `json:"log_cut,omitempty"`
}

// txRelated are the pages the transaction touches.
type txRelated struct {
	Publisher string  `json:"publisher,omitempty"`
	Blob      *txBlob `json:"blob,omitempty"`
	// Validator: an endpoint registration for one validator; Validators:
	// for several, by consensus address (never both).
	Validator  *txValidator  `json:"validator,omitempty"`
	Validators []txValidator `json:"validators,omitempty"`
}

// txBlob is the blob a settlement settled: for a success, its row's own
// values, as /v1/blobs carries them, for Tensile's reading of it beside the
// link; for a failed settlement whose promise settled later, where.
type txBlob struct {
	PromiseHash      string       `json:"promise_hash"`
	Commitment       string       `json:"commitment"`
	BlobVersion      int          `json:"blob_version"`
	SettlementHeight *int64       `json:"settlement_height,omitempty"`
	MustServeUntil   string       `json:"must_serve_until,omitempty"`
	Reconstructable  *reconstruct `json:"reconstructable,omitempty"`
}

// txValidator is a validator a registration names.
type txValidator struct {
	Address string `json:"address"` // consensus address, lower-case hex
	// OperatorAddress is operatorAddrs': absent for a superseded key.
	OperatorAddress string `json:"operator_address,omitempty"`
	Moniker         string `json:"moniker,omitempty"`
	AvatarURL       string `json:"avatar_url,omitempty"`
}

// txWithdrawal is a withdrawal request's place in the queue: the
// withdrawal_queue row of its publisher requested at its block time.
type txWithdrawal struct {
	AvailableAt string `json:"available_at,omitempty"`
	// Outcome: pending while queued, paid when a payout was attributed to
	// it, consumed when settlements took it; absent when the observer cannot
	// tell (unattributed) and while undecided.
	Outcome      string  `json:"outcome,omitempty"`
	PaidHeight   *int64  `json:"paid_height,omitempty"`
	PaidAt       *string `json:"paid_at,omitempty"`
	PayoutDelayS *int64  `json:"payout_delay_s,omitempty"`
	ReducedUtia  int64   `json:"reduced_utia,omitempty"`
}

// txPayment is one payments row of a transaction.
type txPayment struct {
	Kind        string
	Height      int64
	TxIndex     int
	MsgIndex    int
	Time        string
	Publisher   string
	Processor   string
	PromiseHash string
	Namespace   string
	BlobSize    int64
	AmountUtia  int64
	AvailableAt string
}

func (s *Server) handleTx(w http.ResponseWriter, r *http.Request) {
	hash, ok := hash32(r.PathValue("hash"))
	if !ok || hash == "" {
		writeErr(w, 400, "hash must be 64 hex characters")
		return
	}
	a, err := s.txByHash(r.Context(), hash)
	if err != nil {
		s.writeInternal(w, r.URL.Path, err)
		return
	}
	if a == nil {
		// not cached (statusWriter): a hash asked a second before its block
		// was read is found on the next asking
		writeErr(w, 404, "no Fibre transaction with this hash on record")
		return
	}
	// A failure that did not pass the ante handler could still land: its
	// answer can change. A success or a final failure cannot, but for a blob
	// payment's reading, which the usual 15 s covers.
	if a.Final != nil && !*a.Final {
		w.Header().Set("Cache-Control", "no-store")
	}
	writeJSON(w, 200, a)
}

// txByHash is the answer for hash (lower-case hex), or nil when no Fibre
// transaction with it is on record: a cost line, then a publication, then
// escrow movements, then a failure.
func (s *Server) txByHash(ctx context.Context, hash string) (*txAnswer, error) {
	rec, err := s.txCostByHash(ctx, hash)
	if err != nil {
		return nil, err
	}
	if rec != nil {
		return s.txFromCost(ctx, hash, *rec)
	}
	blobs, err := s.blobRows(ctx, blobByTxSQL, 1, hash, strings.ToUpper(hash))
	if err != nil {
		return nil, err
	}
	if len(blobs) > 0 {
		return txFromBlob(hash, blobs[0]), nil
	}
	pays, err := s.txPayments(ctx, hash)
	if err != nil {
		return nil, err
	}
	if len(pays) > 0 {
		return s.txFromPayments(ctx, hash, pays)
	}
	failed, err := s.failedRecord(ctx, hash)
	if err != nil {
		return nil, err
	}
	if failed != nil {
		return s.txFromFailure(ctx, hash, *failed)
	}
	return nil, nil
}

// txCostByHash is the newest cost line of hash, or nil. A line that does not
// decode is a row fault, answered as none.
func (s *Server) txCostByHash(ctx context.Context, hash string) (*txcost.Record, error) {
	var raw string
	err := s.st.DB().QueryRowContext(ctx, txCostByHashSQL, hash).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return s.decodeTxCost(ctx, hash, raw)
}

// decodeTxCost reads a stored cost line; one that does not decode is a row
// fault (noteRowFault), answered as no line.
func (s *Server) decodeTxCost(ctx context.Context, hash, raw string) (*txcost.Record, error) {
	var rec txcost.Record
	if err := json.Unmarshal([]byte(raw), &rec); err != nil {
		if f, ok := isRowFault(rowFault(ctx, hash, "transaction cost", err)); ok {
			s.noteRowFault(f)
			return nil, nil
		}
		return nil, err
	}
	return &rec, nil
}

// blobTxCost is /v1/blobs/{hash}'s tx_cost: the settlement transaction's own
// gas and fee, and how many top-level messages the fee paid for.
type blobTxCost struct {
	GasWanted int64  `json:"gas_wanted"`
	GasUsed   int64  `json:"gas_used"`
	Fee       string `json:"fee,omitempty"`
	FeePayer  string `json:"fee_payer,omitempty"`
	Messages  int    `json:"messages"`
}

// txCostAt is the cost line of the transaction at (height, txIndex) when its
// hash is hash, or nil: none before the record began, none for a line at
// that position of another transaction.
func (s *Server) txCostAt(ctx context.Context, height int64, txIndex int, hash string) (*blobTxCost, error) {
	var raw string
	err := s.st.DB().QueryRowContext(ctx, txCostAtSQL, height, txIndex).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	rec, err := s.decodeTxCost(ctx, hash, raw)
	if err != nil || rec == nil {
		return nil, err
	}
	if hash == "" || !strings.EqualFold(rec.TxHash, hash) {
		return nil, nil
	}
	return &blobTxCost{GasWanted: rec.GasWanted, GasUsed: rec.GasUsed, Fee: rec.Fee, FeePayer: rec.FeePayer, Messages: len(rec.Messages)}, nil
}

// failedRecord is the newest failed inclusion of hash, as failedTx reads it,
// or nil.
func (s *Server) failedRecord(ctx context.Context, hash string) (*failedtx.Record, error) {
	var raw string
	err := s.st.DB().QueryRowContext(ctx, failedTxByHashSQL, hash).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var r failedtx.Record
	if err := json.Unmarshal([]byte(raw), &r); err != nil {
		if f, ok := isRowFault(rowFault(ctx, hash, "failed transaction", err)); ok {
			s.noteRowFault(f)
			return nil, nil
		}
		return nil, err
	}
	return &r, nil
}

// txPayments are hash's escrow movements, in message order. A payout has no
// transaction, so only the four kinds a message makes are kept.
func (s *Server) txPayments(ctx context.Context, hash string) ([]txPayment, error) {
	rows, err := s.st.DB().QueryContext(ctx, paymentsByTxSQL, hash)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []txPayment
	for rows.Next() {
		var p txPayment
		if err := rows.Scan(&p.Kind, &p.Height, &p.TxIndex, &p.MsgIndex, &p.Time, &p.Publisher, &p.Processor, &p.PromiseHash, &p.Namespace,
			&p.BlobSize, &p.AmountUtia, &p.AvailableAt); err != nil {
			return nil, err
		}
		if kindTypeURL[p.Kind] == "" {
			continue
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// txMsgsOf are a record's messages as the answer lists them.
func txMsgsOf(ms []failedtx.Msg) []txMsg {
	out := make([]txMsg, 0, len(ms))
	for _, m := range ms {
		a := txMsg{Index: m.Index, TypeURL: m.TypeURL, Fibre: failedtx.IsFibre(m.TypeURL)}
		if a.Fibre && !m.Cut {
			a.Signer = m.Signer
		}
		if len(m.Inner) > 0 {
			a.Inner = txMsgsOf(m.Inner)
		}
		out = append(out, a)
	}
	return out
}

// txKindOf is the kind of a transaction's Fibre messages, top level and one
// level inside a MsgExec: the one kind, or "several".
func txKindOf(ms []failedtx.Msg) string {
	kinds := map[string]bool{}
	for _, m := range ms {
		if k := failedtx.Kind(m.TypeURL); k != "" {
			kinds[k] = true
		}
		for _, in := range m.Inner {
			if k := failedtx.Kind(in.TypeURL); k != "" {
				kinds[k] = true
			}
		}
	}
	return kindOfSet(kinds)
}

func kindOfSet(kinds map[string]bool) string {
	if len(kinds) > 1 {
		return txKindSeveral
	}
	for k := range kinds {
		return k
	}
	return ""
}

// firstOfKind is the first message of kind, top level first, else one inside
// a MsgExec; top says which.
func firstOfKind(ms []failedtx.Msg, kind string) (first *failedtx.Msg, top bool) {
	for i := range ms {
		if failedtx.Kind(ms[i].TypeURL) == kind {
			return &ms[i], true
		}
	}
	for i := range ms {
		for j := range ms[i].Inner {
			if failedtx.Kind(ms[i].Inner[j].TypeURL) == kind {
				return &ms[i].Inner[j], false
			}
		}
	}
	return nil, false
}

// txFromCost is a success with its cost line.
func (s *Server) txFromCost(ctx context.Context, hash string, rec txcost.Record) (*txAnswer, error) {
	a := &txAnswer{TxHash: hash, Status: "success", Height: rec.Height, TxIndex: rec.TxIndex, Time: rec.Time.UTC(),
		Kind: txKindOf(rec.Messages), Messages: txMsgsOf(rec.Messages),
		Cost:   &txCost{GasWanted: rec.GasWanted, GasUsed: rec.GasUsed, Fee: rec.Fee, FeePayer: rec.FeePayer},
		Effect: map[string]any{}}
	if a.Kind == failedtx.KindSettlement {
		blobs, err := s.blobRows(ctx, blobByTxSQL, 1, hash, strings.ToUpper(hash))
		if err != nil {
			return nil, err
		}
		if len(blobs) > 0 {
			settlementEffect(a, blobs[0])
			return a, nil
		}
	}
	pays, err := s.txPayments(ctx, hash)
	if err != nil {
		return nil, err
	}
	return a, s.txSuccess(ctx, a, pays)
}

// txFromBlob is a settlement found by its publication, before cost lines:
// a PFF transaction holds exactly one message, so its list is whole.
func txFromBlob(hash string, b blobRow) *txAnswer {
	a := &txAnswer{TxHash: hash, Status: "success", Height: b.SettlementHeight, TxIndex: b.SettlementTxIndex, Time: parseTS(b.SettlementTime),
		Kind:     failedtx.KindSettlement,
		Messages: []txMsg{{Index: 0, TypeURL: kindTypeURL[failedtx.KindSettlement], Fibre: true, Signer: b.Signer}},
		Effect:   map[string]any{}}
	settlementEffect(a, b)
	return a
}

// txFromPayments is a movement found by its payments rows, before cost
// lines: its messages are the record's Fibre messages alone.
func (s *Server) txFromPayments(ctx context.Context, hash string, pays []txPayment) (*txAnswer, error) {
	kinds := map[string]bool{}
	msgs := make([]txMsg, 0, len(pays))
	for _, p := range pays {
		kinds[p.Kind] = true
		signer := p.Publisher
		if p.Kind == failedtx.KindSettlement || p.Kind == failedtx.KindTimeout {
			signer = p.Processor
		}
		msgs = append(msgs, txMsg{Index: p.MsgIndex, TypeURL: kindTypeURL[p.Kind], Fibre: true, Signer: signer})
	}
	a := &txAnswer{TxHash: hash, Status: "success", Height: pays[0].Height, TxIndex: pays[0].TxIndex, Time: parseTS(pays[0].Time),
		Kind: kindOfSet(kinds), Messages: msgs, MessagesPartial: true, Effect: map[string]any{}}
	return a, s.txSuccess(ctx, a, pays)
}

// settlementEffect is a settlement's effect and links from its publication:
// the blob as its /v1/blobs row carries it.
func settlementEffect(a *txAnswer, b blobRow) {
	a.Effect["promise_hash"] = b.PromiseHash
	a.Effect["commitment"] = b.Commitment
	a.Effect["blob_version"] = b.BlobVersion
	a.Effect["namespace"] = b.Namespace
	a.Effect["blob_size"] = b.BlobSize
	if c := b.Charge; c != nil {
		a.Effect["fee_paid_utia"] = c.FeeUtia
		a.Effect["settled"] = c.Settled
		a.Effect["timed_out"] = c.TimedOut
	}
	a.Related.Publisher = b.Publisher
	a.Related.Blob = &txBlob{PromiseHash: b.PromiseHash, Commitment: b.Commitment, BlobVersion: b.BlobVersion,
		MustServeUntil: b.MustServeUntil, Reconstructable: b.Reconstructable}
}

// txSuccess fills a success's effect and links by its kind from its
// payments rows and the registrations it wrote. A settlement with a
// publication is settlementEffect's.
func (s *Server) txSuccess(ctx context.Context, a *txAnswer, pays []txPayment) error {
	switch a.Kind {
	case failedtx.KindSettlement:
		for _, p := range pays {
			if p.Kind == failedtx.KindSettlement {
				a.Effect["promise_hash"] = p.PromiseHash
				a.Effect["namespace"] = p.Namespace
				a.Effect["blob_size"] = p.BlobSize
				a.Effect["fee_paid_utia"] = p.AmountUtia
				// the row is this MsgPayForFibre's own settlement, blobCharge's
				// settled; the chain takes no timeout of a promise it settled
				a.Effect["settled"] = true
				a.Effect["timed_out"] = false
				a.Related.Publisher = p.Publisher
				break
			}
		}
	case failedtx.KindDeposit, failedtx.KindWithdrawalRequest, failedtx.KindTimeout:
		// A successful movement is a top-level message, so it has its row
		// (a MsgExec cannot carry one); with none, nothing is said.
		var first *txPayment
		var sum int64
		for i := range pays {
			if pays[i].Kind != a.Kind {
				continue
			}
			if first == nil {
				first = &pays[i]
			}
			sum += pays[i].AmountUtia
		}
		if first == nil {
			return nil
		}
		a.Effect["amount_utia"] = sum
		a.Related.Publisher = first.Publisher
		switch a.Kind {
		case failedtx.KindTimeout:
			a.Effect["promise_hash"] = first.PromiseHash
		case failedtx.KindWithdrawalRequest:
			w, err := s.txWithdrawal(ctx, first.Publisher, first.Time)
			if err != nil {
				return err
			}
			if w != nil {
				a.Effect["withdrawal"] = w
			}
		}
	case failedtx.KindSetHost:
		cons, err := s.hostEventsAt(ctx, a.Height, a.TxIndex+1)
		if err != nil {
			return err
		}
		if len(cons) == 1 {
			steps, err := s.hostWalk(ctx, cons[0])
			if err != nil {
				return err
			}
			if action, host, prev, ok := walkEventAt(steps, a.Height, a.TxIndex+1); ok {
				a.Effect["action"] = action
				a.Effect["host"] = host
				if action != endpointRegistered {
					a.Effect["previous_host"] = prev
				}
			}
		}
		return s.relateValidators(ctx, a, cons)
	case txKindSeveral:
		pub, same := "", true
		for _, p := range pays {
			if pub == "" {
				pub = p.Publisher
			} else if p.Publisher != pub {
				same = false
			}
		}
		if same {
			a.Related.Publisher = pub
		}
		cons, err := s.hostEventsAt(ctx, a.Height, a.TxIndex+1)
		if err != nil {
			return err
		}
		return s.relateValidators(ctx, a, cons)
	}
	return nil
}

// relateValidators links the validators cons (in cons_address order): one
// as validator, several as validators.
func (s *Server) relateValidators(ctx context.Context, a *txAnswer, cons []string) error {
	if len(cons) == 0 {
		return nil
	}
	vs, err := s.txValidators(ctx, cons)
	if err != nil {
		return err
	}
	if len(vs) == 1 {
		a.Related.Validator = &vs[0]
	} else {
		a.Related.Validators = vs
	}
	return nil
}

// hostEventsAt are the validators whose registration the transaction at
// (height, fromTxIndex - 1) wrote, by consensus address.
func (s *Server) hostEventsAt(ctx context.Context, height int64, fromTxIndex int) ([]string, error) {
	rows, err := s.st.DB().QueryContext(ctx, hostEventsAtSQL, height, fromTxIndex)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var c string
		if err := rows.Scan(&c); err != nil {
			return nil, err
		}
		out = append(out, strings.ToLower(c))
	}
	return out, rows.Err()
}

// txValidators names each of cons as the related links do, in its order.
func (s *Server) txValidators(ctx context.Context, cons []string) ([]txValidator, error) {
	ops, err := s.operatorAddrs(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]txValidator, 0, len(cons))
	for _, c := range cons {
		v := txValidator{Address: c, OperatorAddress: ops[c]}
		var avatar int
		var identity string
		err := s.st.DB().QueryRowContext(ctx, txValidatorSQL, c).Scan(&v.Moniker, &avatar, &identity)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return nil, err
		}
		if avatar == 1 {
			v.AvatarURL = "/v1/avatars/" + strings.ToUpper(identity)
		}
		out = append(out, v)
	}
	return out, nil
}

// txWithdrawal is the queue row of publisher requested at the block time at
// (both parsed, compared as instants: the publisher page's own match), or
// nil when none is on record.
func (s *Server) txWithdrawal(ctx context.Context, publisher, at string) (*txWithdrawal, error) {
	t := parseTS(at)
	if t.IsZero() {
		return nil, nil
	}
	rows, err := s.withdrawalRows(ctx, `publisher = ? AND requested_at >= ? AND requested_at <= ? ORDER BY requested_at`, 10,
		publisher, store.TS(t.Add(-time.Second)), store.TS(t.Add(time.Second)))
	if err != nil {
		return nil, err
	}
	for _, w := range rows {
		if !parseTS(w.RequestedAt).Equal(t) {
			continue
		}
		out := &txWithdrawal{AvailableAt: w.AvailableAt, PaidHeight: w.PaidHeight, PaidAt: w.PaidAt, PayoutDelayS: w.PayoutDelayS}
		switch {
		case w.GoneHeight == nil:
			out.Outcome = "pending"
		case w.Outcome != nil && *w.Outcome == store.WithdrawalExecuted:
			out.Outcome = "paid"
		case w.Outcome != nil && *w.Outcome == store.WithdrawalConsumed:
			out.Outcome = "consumed"
		}
		if w.ReducedUtia > 0 {
			out.ReducedUtia = w.ReducedUtia
		}
		return out, nil
	}
	return nil, nil
}

// requestedUtia is a requested coin in utia, exact in a JSON number: "N utia"
// with N at most 2^53.
var requestedUtia = regexp.MustCompile(`^(\d+)utia$`)

// txFromFailure is a failure: what it asked for, and the pages the lists'
// rules let it link.
func (s *Server) txFromFailure(ctx context.Context, hash string, r failedtx.Record) (*txAnswer, error) {
	final := r.AntePassed
	e := failedtx.Explain(r)
	a := &txAnswer{TxHash: hash, Status: "failed", Final: &final, Height: r.Height, TxIndex: r.TxIndex, Time: r.Time.UTC(),
		Kind: txKindOf(r.Messages), Messages: txMsgsOf(r.Messages),
		Cost:    &txCost{GasWanted: r.GasWanted, GasUsed: r.GasUsed, Fee: r.Fee},
		Failure: &txFailure{Code: r.Code, Codespace: r.Codespace, Reason: e.Reason, Log: r.Log, LogCut: r.LogCut},
		Effect:  map[string]any{}}
	if e.MsgIndex >= 0 {
		i := e.MsgIndex
		a.Failure.FailedMsgIndex = &i
	}
	first, top := firstOfKind(r.Messages, a.Kind)
	if first == nil {
		return a, nil
	}
	// Only a final failure's top-level message is listed, so only it links.
	linked := final && top
	d := first.Detail
	if d == nil {
		d = &failedtx.MsgDetail{}
	}
	switch a.Kind {
	case failedtx.KindSettlement:
		putString(a.Effect, "promise_hash", d.PromiseHash)
		putString(a.Effect, "namespace", d.Namespace)
		if d.BlobSize > 0 {
			a.Effect["blob_size"] = d.BlobSize
		}
		if linked {
			if err := s.linkPublisher(ctx, a, *first); err != nil {
				return nil, err
			}
			if d.PromiseHash != "" {
				var b txBlob
				var h int64
				err := s.st.DB().QueryRowContext(ctx, settledLaterSQL, d.PromiseHash).Scan(&b.PromiseHash, &b.Commitment, &b.BlobVersion, &h)
				switch {
				case errors.Is(err, sql.ErrNoRows):
				case err != nil:
					return nil, err
				default:
					b.SettlementHeight = &h
					a.Related.Blob = &b
				}
			}
		}
	case failedtx.KindDeposit, failedtx.KindWithdrawalRequest:
		if d.Amount != "" {
			a.Effect["requested"] = d.Amount
			if m := requestedUtia.FindStringSubmatch(d.Amount); m != nil {
				if n, err := strconv.ParseUint(m[1], 10, 64); err == nil && n <= 1<<53 {
					a.Effect["requested_utia"] = int64(n)
				}
			}
		}
		if linked {
			if err := s.linkPublisher(ctx, a, *first); err != nil {
				return nil, err
			}
		}
	case failedtx.KindTimeout:
		// Anyone can time out anyone's promise: a failed one links no
		// publisher.
		putString(a.Effect, "promise_hash", d.PromiseHash)
	case failedtx.KindSetHost:
		if err := s.failedSetHostEffect(ctx, a, r, first, d, linked); err != nil {
			return nil, err
		}
	}
	switch a.Kind {
	case failedtx.KindSettlement, failedtx.KindDeposit, failedtx.KindWithdrawalRequest, failedtx.KindTimeout:
		// the publisher as the message names it, where no link does
		if a.Related.Publisher == "" && d.Publisher != "" && !first.Cut {
			a.Effect["publisher"] = d.Publisher
		}
	}
	return a, nil
}

// putString sets key to v when v is not empty.
func putString(m map[string]any, key, v string) {
	if v != "" {
		m[key] = v
	}
}

// linkPublisher links the publisher a listed failure's message names, when
// that account has a publisher page.
func (s *Server) linkPublisher(ctx context.Context, a *txAnswer, m failedtx.Msg) error {
	acct, ok := failedtx.ListAccount(m)
	if !ok {
		return nil
	}
	var one int
	err := s.st.DB().QueryRowContext(ctx, publisherKnownSQL, acct).Scan(&one)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return nil
	case err != nil:
		return err
	}
	a.Related.Publisher = acct
	return nil
}

// failedSetHostEffect is a failed endpoint registration's effect and links.
// Signed by one operator: what it asked for, and, when the signer is a
// validator's operator address, what the validator had at that block.
// Signed at the top level by several: each is listed in its own validator's
// history, and the messages name each request.
func (s *Server) failedSetHostEffect(ctx context.Context, a *txAnswer, r failedtx.Record, first *failedtx.Msg, d *failedtx.MsgDetail, linked bool) error {
	var ops []string
	seen := map[string]bool{}
	for _, m := range r.Messages {
		if failedtx.Kind(m.TypeURL) != failedtx.KindSetHost {
			continue
		}
		if op, ok := failedtx.ListAccount(m); ok && !seen[op] {
			seen[op] = true
			ops = append(ops, op)
		}
	}
	if len(ops) > 1 {
		if !r.AntePassed {
			return nil
		}
		var cons []string
		named := map[string]bool{}
		for _, op := range ops {
			c, ok, err := s.validatorOf(ctx, op)
			if err != nil {
				return err
			}
			if ok && !named[c] {
				named[c] = true
				cons = append(cons, c)
			}
		}
		if len(cons) == 0 {
			return nil
		}
		sort.Strings(cons)
		vs, err := s.txValidators(ctx, cons)
		if err != nil {
			return err
		}
		a.Related.Validators = vs
		return nil
	}
	putString(a.Effect, "requested_host", d.Host)
	if first.Signer == "" {
		return nil
	}
	cons, ok := "", false
	if op, valid := failedtx.OperatorForm(first.Signer); valid && !first.Cut {
		var err error
		if cons, ok, err = s.validatorOf(ctx, op); err != nil {
			return err
		}
	}
	if !ok {
		a.Effect["not_a_validator"] = true
		return nil
	}
	steps, err := s.hostWalk(ctx, cons)
	if err != nil {
		return err
	}
	at := hostAt(steps, r.Height, r.TxIndex)
	a.Effect["attempted"] = attemptedOf(at)
	putString(a.Effect, "host_at_block", at)
	if linked {
		vs, err := s.txValidators(ctx, []string{cons})
		if err != nil {
			return err
		}
		a.Related.Validator = &vs[0]
	}
	return nil
}

// validatorOf is the consensus address an operator address stands for now
// (resolveAddr: the current identity), ok false when the staking set has
// never named it.
func (s *Server) validatorOf(ctx context.Context, op string) (string, bool, error) {
	c, err := s.resolveAddr(ctx, op)
	var ae *addrError
	switch {
	case errors.As(err, &ae):
		return "", false, nil
	case err != nil:
		return "", false, err
	}
	return c, true, nil
}
