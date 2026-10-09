package scan

import (
	"time"

	abci "github.com/cometbft/cometbft/abci/types"
	cmttypes "github.com/cometbft/cometbft/types"
	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/plsgiveup/fibre/fibre-sentinel/internal/failedtx"
	"github.com/plsgiveup/fibre/fibre-sentinel/internal/txcost"
)

// What a successful transaction that carried a Fibre message cost: its gas,
// the fee the ante handler took and who paid it, and its messages, recorded
// in tx_costs.jsonl for the transaction page and the blob page. What it did
// is in publications.jsonl, payments.jsonl and host_history.jsonl already;
// nothing the scanner counts or records elsewhere reads this file.

// txCostsOn is false only in the byte-identity test (txcost_test.go), which
// runs the same blocks with and without step 5; nothing else sets it.
var txCostsOn = true

// TxCostRecords is what step 5 writes for one block, a pure function of the
// block and its results: the tx_costs.jsonl line of every successful tx
// that carries a Fibre message (failedtx.Carries over failedTxMsgs, v1's D4
// rule), with its gas and the ante handler's fee and fee payer, in tx
// order, each with recordedAt as its recorded_at, and the decode problems
// met on the way. skip, when not nil, names a key already on record: its tx
// is passed over before it is decoded, as the scanner passes over a key its
// seen-set holds.
//
// failedtx.MayCarry skips, before any decoding, every tx whose bytes hold
// none of the five type URLs: such a tx cannot carry a Fibre message at
// either level, so the filter changes no line.
//
// The scanner writes these lines as it reads each block (recordTxCosts),
// and sentinel-txbackfill writes the same lines, from this same function,
// for blocks from before the scanner's record began.
func TxCostRecords(blk *Block, res *BlockResults, recordedAt time.Time, skip func(key string) bool) ([]txcost.Record, []TxProblem) {
	var recs []txcost.Record
	var problems []TxProblem
	for i, raw := range blk.Txs {
		if res.TxCodes[i] != 0 || !failedtx.MayCarry(raw) || (skip != nil && skip(txcost.Key(blk.Height, i))) {
			continue
		}
		msgs, errs, ok := failedTxMsgs(raw)
		for _, err := range errs {
			problems = append(problems, TxProblem{TxIndex: i, Err: err})
		}
		if !ok || !failedtx.Carries(msgs) {
			continue
		}
		_, fee, payer := anteFeeEvent(res.TxEvents[i])
		recs = append(recs, txcost.Record{
			SchemaVersion: txcost.SchemaVersion,
			DedupeKey:     txcost.Key(blk.Height, i),
			Height:        blk.Height,
			TxIndex:       i,
			Time:          blk.Time.UTC(),
			TxHash:        hexstr(cmttypes.Tx(raw).Hash()),
			GasWanted:     res.TxGasWanted[i],
			GasUsed:       res.TxGasUsed[i],
			Fee:           fee,
			FeePayer:      payer,
			Messages:      txcost.Strip(msgs),
			RecordedAt:    recordedAt,
		})
	}
	return recs, problems
}

// recordTxCosts is step 5 of processBlock: every successful tx of the block
// that carries a Fibre message (TxCostRecords), appended to tx_costs.jsonl.
// A decode problem is logged and never fatal; a failed write is Fatalf, so
// the cursor never passes an unrecorded cost. It adds nothing to
// processBlock's count and writes no log line per tx.
func (s *Scanner) recordTxCosts(blk *Block, res *BlockResults, h int64) {
	recs, problems := TxCostRecords(blk, res, time.Now().UTC(), s.store.TxCostSeen)
	for _, p := range problems {
		s.log.Printf("h=%d tx=%d: tx cost: %v", h, p.TxIndex, p.Err)
	}
	for _, rec := range recs {
		if err := s.store.AppendTxCost(rec); err != nil {
			s.log.Fatalf("h=%d tx=%d: append tx cost: %v", h, rec.TxIndex, err)
		}
	}
}

// ResultsNeedBlock reports whether steps 4 and 5 can write a line for the
// block res holds the results of, read from the results alone: true when a
// tx failed in it (step 4 decodes every failed tx's bytes), or when a
// successful tx's top-level message is one of the five Fibre messages or a
// MsgExec (successMayCarry). When it is false, FailedTxRecords and
// TxCostRecords give no line whatever the block's txs are, so a reader that
// wants only those lines can leave the block unread. sentinel-txbackfill
// does: most of a Mocha block's bytes are blobs, and only a few blocks in a
// hundred hold a failure or a Fibre success.
//
// The rule rests on baseapp.runMsgs (cosmos-sdk baseapp.go, createEvents):
// every top-level message of a successful tx is given a "message" event
// whose "action" is sdk.MsgTypeURL of the message, which is the type URL
// its Any carried in the tx bytes (an Any whose URL the registry does not
// resolve fails the tx, which is then a failed one). A message inside a
// MsgExec is given none, the MsgExec's own action standing for it, so
// every successful MsgExec counts. TestResultsNeedBlockOnAMochaBlock holds
// the rule to a block_results answer from Mocha.
func ResultsNeedBlock(res *BlockResults) bool {
	for i, code := range res.TxCodes {
		if code != 0 || successMayCarry(res.TxEvents[i]) {
			return true
		}
	}
	return false
}

// successMayCarry reports a "message" event among a successful tx's events
// whose "action" is a Fibre type URL or MsgExec's.
func successMayCarry(evs []abci.Event) bool {
	for _, ev := range evs {
		if ev.Type != sdk.EventTypeMessage {
			continue
		}
		for _, a := range ev.Attributes {
			if a.Key == sdk.AttributeKeyAction && (failedtx.IsFibre(a.Value) || a.Value == failedtx.ExecTypeURL) {
				return true
			}
		}
	}
	return false
}
