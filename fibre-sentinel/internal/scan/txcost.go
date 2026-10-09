package scan

import (
	"time"

	cmttypes "github.com/cometbft/cometbft/types"
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

// recordTxCosts is step 5 of processBlock: every successful tx of the block
// that carries a Fibre message (failedtx.Carries over failedTxMsgs, v1's D4
// rule), with its gas and the ante handler's fee and fee payer, appended to
// tx_costs.jsonl. A decode problem is logged and never fatal; a failed write
// is Fatalf, so the cursor never passes an unrecorded cost. It adds nothing
// to processBlock's count and writes no log line per tx.
//
// failedtx.MayCarry skips, before any decoding, every tx whose bytes hold
// none of the five type URLs: such a tx cannot carry a Fibre message at
// either level, so the filter changes no line.
func (s *Scanner) recordTxCosts(blk *Block, res *BlockResults, h int64) {
	now := time.Now().UTC()
	for i, raw := range blk.Txs {
		if res.TxCodes[i] != 0 || !failedtx.MayCarry(raw) || s.store.TxCostSeen(txcost.Key(blk.Height, i)) {
			continue
		}
		msgs, problems, ok := failedTxMsgs(raw)
		for _, p := range problems {
			s.log.Printf("h=%d tx=%d: tx cost: %v", h, i, p)
		}
		if !ok || !failedtx.Carries(msgs) {
			continue
		}
		_, fee, payer := anteFeeEvent(res.TxEvents[i])
		rec := txcost.Record{
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
			RecordedAt:    now,
		}
		if err := s.store.AppendTxCost(rec); err != nil {
			s.log.Fatalf("h=%d tx=%d: append tx cost: %v", h, i, err)
		}
	}
}
