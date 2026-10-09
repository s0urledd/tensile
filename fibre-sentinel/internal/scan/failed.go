package scan

import (
	"errors"
	"fmt"
	"time"

	celfibre "github.com/celestiaorg/celestia-app/v10/fibre"
	fibretypes "github.com/celestiaorg/celestia-app/v10/x/fibre/types"
	valaddrtypes "github.com/celestiaorg/celestia-app/v10/x/valaddr/types"
	squaretx "github.com/celestiaorg/go-square/v4/tx"
	abci "github.com/cometbft/cometbft/abci/types"
	cmttypes "github.com/cometbft/cometbft/types"
	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/cosmos/cosmos-sdk/x/authz"
	"github.com/plsgiveup/fibre/fibre-sentinel/internal/failedtx"
)

// A transaction that failed in a block took no effect: no publication, no
// escrow movement, no registration. It is recorded apart, in
// failed_txs.jsonl, when it carried a Fibre message, so that a lookup of its
// hash can say what the chain returned for it. Nothing the scanner counts
// or records elsewhere reads it.

// msgSetHostTypeURL is MsgSetFibreProviderInfo's type URL.
var msgSetHostTypeURL = sdk.MsgTypeURL(&valaddrtypes.MsgSetFibreProviderInfo{})

// failedTxMsgs lists a tx's messages for failed_txs.jsonl. ok is false for
// BlobTx and IndexWrapper bytes (squaretx.UnmarshalBlobTx / UnmarshalIndexWrapper,
// checked first, as fibretypes.TryParseFibreTx does) and for bytes that are
// not an SDK tx (decodeTxMsgs returns nothing).
//
// Every top-level message is listed with its TxBody index; a MsgExec also
// lists its messages, one level. A Fibre message, at either level, carries
// what it asked for (describeFibre). problems are the messages that did not
// decode, a MsgExec or a Fibre one: each is listed all the same, without
// what could not be read, and the caller logs them.
func failedTxMsgs(raw []byte) (msgs []failedtx.Msg, problems []error, ok bool) {
	if _, isBlobTx, _ := squaretx.UnmarshalBlobTx(raw); isBlobTx {
		return nil, nil, false
	}
	if _, isIndexWrapper := squaretx.UnmarshalIndexWrapper(raw); isIndexWrapper {
		return nil, nil, false
	}
	txMsgs := decodeTxMsgs(raw)
	if len(txMsgs) == 0 {
		return nil, nil, false
	}
	msgs = make([]failedtx.Msg, 0, len(txMsgs))
	for _, m := range txMsgs {
		msg := failedtx.Msg{Index: m.index, TypeURL: m.typeURL}
		if m.typeURL == failedtx.ExecTypeURL {
			var exec authz.MsgExec
			if err := exec.Unmarshal(m.value); err != nil {
				problems = append(problems, fmt.Errorf("msg %d: MsgExec did not decode: %w", m.index, err))
			} else {
				for j, a := range exec.Msgs {
					if a == nil {
						continue
					}
					in := failedtx.Msg{Index: j, TypeURL: a.TypeUrl}
					if err := describeFibre(&in, a.Value); err != nil {
						problems = append(problems, fmt.Errorf("msg %d inner %d: %w", m.index, j, err))
					}
					msg.Inner = append(msg.Inner, in)
				}
			}
		} else if err := describeFibre(&msg, m.value); err != nil {
			problems = append(problems, fmt.Errorf("msg %d: %w", m.index, err))
		}
		msgs = append(msgs, msg)
	}
	return msgs, problems, true
}

// describeFibre fills a Fibre message's Signer and Detail from its own
// bytes: what it asked for, never what happened. Any other message is left
// as it is. A message that does not decode is left without either; a
// promise whose publisher or hash cannot be derived keeps the rest. The
// error says what was left out. Each string is kept to
// failedtx.MaxFieldBytes (cutFields).
func describeFibre(m *failedtx.Msg, value []byte) error {
	d := &failedtx.MsgDetail{}
	var err error
	switch m.TypeURL {
	case fibretypes.MsgPayForFibreTypeURL:
		var msg fibretypes.MsgPayForFibre
		if uerr := msg.Unmarshal(value); uerr != nil {
			return fmt.Errorf("MsgPayForFibre did not decode: %w", uerr)
		}
		m.Signer = msg.Signer
		d.Namespace = hexstr(msg.PaymentPromise.Namespace)
		d.BlobSize = int64(msg.PaymentPromise.BlobSize)
		err = promiseDetail(d, &msg.PaymentPromise)
	case msgTimeoutTypeURL:
		// The timeout's promise holds a namespace and a size too; MsgDetail
		// keeps those for MsgPayForFibre only, as its field comments say.
		var msg fibretypes.MsgPaymentPromiseTimeout
		if uerr := msg.Unmarshal(value); uerr != nil {
			return fmt.Errorf("MsgPaymentPromiseTimeout did not decode: %w", uerr)
		}
		m.Signer = msg.Signer
		err = promiseDetail(d, &msg.PaymentPromise)
	case msgDepositTypeURL:
		var msg fibretypes.MsgDepositToEscrow
		if uerr := msg.Unmarshal(value); uerr != nil {
			return fmt.Errorf("MsgDepositToEscrow did not decode: %w", uerr)
		}
		m.Signer = msg.Signer
		d.Publisher, d.Amount = msg.Signer, coinText(msg.Amount)
	case msgWithdrawalTypeURL:
		var msg fibretypes.MsgRequestWithdrawal
		if uerr := msg.Unmarshal(value); uerr != nil {
			return fmt.Errorf("MsgRequestWithdrawal did not decode: %w", uerr)
		}
		m.Signer = msg.Signer
		d.Publisher, d.Amount = msg.Signer, coinText(msg.Amount)
	case msgSetHostTypeURL:
		var msg valaddrtypes.MsgSetFibreProviderInfo
		if uerr := msg.Unmarshal(value); uerr != nil {
			return fmt.Errorf("MsgSetFibreProviderInfo did not decode: %w", uerr)
		}
		m.Signer = msg.Signer
		// d.Validator stays empty: the message has no validator field. It
		// registers the host for its signer, the operator address, which
		// m.Signer holds.
		d.Host = msg.Host
	default:
		return nil
	}
	cutFields(m, d)
	if *d != (failedtx.MsgDetail{}) {
		m.Detail = d
	}
	return err
}

// cutFields keeps every string a Fibre message gave the record to
// failedtx.MaxFieldBytes and marks the message when one was longer. A
// value the chain accepts is far shorter; a refused message that a
// proposer put in a block without CheckTx can carry one as long as its tx.
func cutFields(m *failedtx.Msg, d *failedtx.MsgDetail) {
	for _, f := range []*string{&m.Signer, &d.Publisher, &d.PromiseHash, &d.Namespace, &d.Amount, &d.Host} {
		var cut bool
		if *f, cut = failedtx.CutField(*f); cut {
			m.Cut = true
		}
	}
}

// promiseDetail fills the escrow owner and the promise hash of a promise a
// failed message carried, the way a settled one's are derived (PublisherOf,
// promiseCharge). Either that cannot be derived is left empty and named in
// the error.
func promiseDetail(d *failedtx.MsgDetail, pp *fibretypes.PaymentPromise) error {
	var errs []error
	if owner, err := PublisherOf(hexstr(pp.SignerPublicKey.Key)); err != nil {
		errs = append(errs, fmt.Errorf("publisher: %w", err))
	} else {
		d.Publisher = owner
	}
	var internal celfibre.PaymentPromise
	if err := internal.FromProto(pp); err != nil {
		errs = append(errs, fmt.Errorf("promise hash: %w", err))
	} else if h, err := internal.Hash(); err != nil {
		errs = append(errs, fmt.Errorf("promise hash: %w", err))
	} else {
		d.PromiseHash = hexstr(h)
	}
	return errors.Join(errs...)
}

// coinText is a coin as the message carries it ("1000000utia"); empty when
// the message carries no amount.
func coinText(c sdk.Coin) string {
	if c.Amount.IsNil() {
		return ""
	}
	return c.String()
}

// anteFeeEvent reads the ante handler's fee event: the first event with
// Type == sdk.EventTypeTx ("tx") that has an attribute Key ==
// sdk.AttributeKeyFee ("fee"). found says one exists; fee is its value and
// payer that event's sdk.AttributeKeyFeePayer ("fee_payer") value, "" when
// absent.
func anteFeeEvent(evs []abci.Event) (found bool, fee, payer string) {
	for _, ev := range evs {
		if ev.Type != sdk.EventTypeTx {
			continue
		}
		for _, a := range ev.Attributes {
			if a.Key != sdk.AttributeKeyFee {
				continue
			}
			for _, p := range ev.Attributes {
				if p.Key == sdk.AttributeKeyFeePayer {
					payer = p.Value
					break
				}
			}
			return true, a.Value, payer
		}
	}
	return false, "", ""
}

// anteFee is anteFeeEvent without the payer: failed_txs.jsonl keeps none.
func anteFee(evs []abci.Event) (passed bool, fee string) {
	found, fee, _ := anteFeeEvent(evs)
	return found, fee
}

// TxProblem is a message of one of a block's txs that did not decode
// (failedTxMsgs): the line keeps the tx without what could not be read, and
// the caller logs it.
type TxProblem struct {
	TxIndex int
	Err     error
}

// FailedTxRecords is what step 4 writes for one block, a pure function of
// the block and its results: the failed_txs.jsonl line of every tx that
// failed in it while carrying a Fibre message, in tx order, each with
// recordedAt as its recorded_at, and the decode problems met on the way.
// skip, when not nil, names a key already on record: its tx is passed over
// before it is decoded, as the scanner passes over a key its seen-set
// holds.
//
// The scanner writes these lines as it reads each block (recordFailedTxs),
// and sentinel-txbackfill writes the same lines, from this same function,
// for blocks from before the scanner's record began.
func FailedTxRecords(blk *Block, res *BlockResults, recordedAt time.Time, skip func(key string) bool) ([]failedtx.Record, []TxProblem) {
	var recs []failedtx.Record
	var problems []TxProblem
	for i, raw := range blk.Txs {
		code := res.TxCodes[i]
		if code == 0 || (skip != nil && skip(failedtx.Key(blk.Height, i))) {
			continue
		}
		msgs, errs, ok := failedTxMsgs(raw)
		for _, err := range errs {
			problems = append(problems, TxProblem{TxIndex: i, Err: err})
		}
		if !ok || !failedtx.Carries(msgs) {
			continue
		}
		logText, cut := failedtx.CutLog(res.TxCodespace[i], code, res.TxLog[i])
		passed, fee := anteFee(res.TxEvents[i])
		recs = append(recs, failedtx.Record{
			SchemaVersion: failedtx.SchemaVersion,
			DedupeKey:     failedtx.Key(blk.Height, i),
			Height:        blk.Height,
			Time:          blk.Time.UTC(),
			AppVersion:    blk.AppVersion,
			TxHash:        hexstr(cmttypes.Tx(raw).Hash()),
			TxIndex:       i,
			Code:          code,
			Codespace:     res.TxCodespace[i],
			Log:           logText,
			LogCut:        cut,
			GasWanted:     res.TxGasWanted[i],
			GasUsed:       res.TxGasUsed[i],
			AntePassed:    passed,
			Fee:           fee,
			Messages:      msgs,
			RecordedAt:    recordedAt,
		})
	}
	return recs, problems
}

// recordFailedTxs is step 4 of processBlock: every failed tx of the block
// that carries a Fibre message (FailedTxRecords), appended to
// failed_txs.jsonl. A decode problem is logged and never fatal; a failed
// write is Fatalf (D14), so the cursor never passes an unrecorded failure.
// It adds nothing to processBlock's count.
func (s *Scanner) recordFailedTxs(blk *Block, res *BlockResults, h int64) {
	recs, problems := FailedTxRecords(blk, res, time.Now().UTC(), s.store.FailedTxSeen)
	for _, p := range problems {
		s.log.Printf("h=%d tx=%d: failed tx: %v", h, p.TxIndex, p.Err)
	}
	for _, rec := range recs {
		if err := s.store.AppendFailedTx(rec); err != nil {
			s.log.Fatalf("h=%d tx=%d: append failed tx: %v", h, rec.TxIndex, err)
		}
		s.log.Printf("FAILED TX h=%d tx=%d (%s) %s/%d gas=%d/%d ante_passed=%v msgs=%d",
			h, rec.TxIndex, rec.TxHash[:12], rec.Codespace, rec.Code, rec.GasUsed, rec.GasWanted, rec.AntePassed, len(rec.Messages))
	}
}
