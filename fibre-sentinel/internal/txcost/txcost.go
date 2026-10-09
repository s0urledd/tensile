// Package txcost is the record of what a successful Fibre transaction cost
// (tx_costs.jsonl): its gas, the fee the ante handler took and who paid it,
// and its messages. Successes only: a failure is in failed_txs.jsonl
// (internal/failedtx), which carries its own gas and fee.
package txcost

import (
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/failedtx"
)

// FileName is the record file under the data dir.
const FileName = "tx_costs.jsonl"

// SchemaVersion is bumped when a field changes meaning.
const SchemaVersion = 1

// Record is one line of tx_costs.jsonl. The JSON field order is this order.
type Record struct {
	SchemaVersion int       `json:"schema_version"`
	DedupeKey     string    `json:"dedupe_key"` // Key(Height, TxIndex)
	Height        int64     `json:"height"`
	TxIndex       int       `json:"tx_index"`
	Time          time.Time `json:"time"`    // block time, UTC: the export's TimeField
	TxHash        string    `json:"tx_hash"` // lower-case hex SHA-256 of the tx bytes
	GasWanted     int64     `json:"gas_wanted"`
	GasUsed       int64     `json:"gas_used"`
	// Fee and FeePayer are the ante handler's "tx" event's "fee" and
	// "fee_payer" attributes, verbatim: the fee as the chain printed it
	// ("8000utia"; empty for a zero fee) and the account it was taken from.
	// Both empty when the result carries no such event.
	Fee      string `json:"fee,omitempty"`
	FeePayer string `json:"fee_payer,omitempty"`
	// Messages: every top-level message in TxBody order, a MsgExec's one
	// level inside it, each Fibre message with its signer; failedtx.Msg
	// without Detail (Strip): what a success did is in publications,
	// payments and host_history.
	Messages   []failedtx.Msg `json:"messages"`
	RecordedAt time.Time      `json:"recorded_at"`
}

// Key is a cost line's dedupe key: failedtx.Key, one per (height, tx index).
func Key(height int64, txIndex int) string { return failedtx.Key(height, txIndex) }

// Strip is msgs with Detail removed at both levels; Index, TypeURL, Inner,
// Signer and Cut are kept. It never changes msgs.
func Strip(msgs []failedtx.Msg) []failedtx.Msg {
	if msgs == nil {
		return nil
	}
	out := make([]failedtx.Msg, len(msgs))
	for i, m := range msgs {
		m.Detail = nil
		m.Inner = Strip(m.Inner)
		out[i] = m
	}
	return out
}
