package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/failedtx"
)

// A transaction that failed in a block while carrying a Fibre message
// settled no blob, moved no escrow and registered no host. The scanner keeps
// it in failed_txs.jsonl alone (internal/failedtx), and the collector in a
// table of its own (migration 30) that no count, rollup or figure reads.
// /v1/blobs?tx= is the one route that reads it: a lookup by the hash alone
// that finds no publication answers what the chain returned for it
// (failed_tx), so a reader holding the hash learns why there is no blob.
//
// The answer is the record's raw facts the site shows, and the two the
// record leaves to its reader, the reason and the failing message, read by
// failedtx.Explain: the record holds only what the chain returned, and a
// reading of it is corrected there without rewriting it. What each message
// asked for (the record's signer and detail) is not published.

// failedTxByHashSQL is a transaction's newest failed inclusion. failed_txs_tx
// (migration 30) seeks the hash and gives the order: no sort.
const failedTxByHashSQL = `SELECT raw_json FROM failed_txs WHERE tx_hash = ? ORDER BY height DESC, tx_index DESC LIMIT 1`

// failedTxMsg is one message of a failed transaction, in its order.
type failedTxMsg struct {
	Index   int           `json:"index"`
	TypeURL string        `json:"type_url"`
	Fibre   bool          `json:"fibre"` // failedtx.IsFibre
	Inner   []failedTxMsg `json:"inner,omitempty"`
}

// failedTxAnswer is /v1/blobs?tx='s failed_tx: only the fields the site reads.
type failedTxAnswer struct {
	Height    int64     `json:"height"`
	Time      time.Time `json:"time"`
	Code      uint32    `json:"code"`
	Codespace string    `json:"codespace"`
	// Reason and FailedMsgIndex are failedtx.Explain's: absent when it has
	// no reason that rests on exact information, or the log names no
	// message.
	Reason         string        `json:"reason,omitempty"`
	FailedMsgIndex *int          `json:"failed_msg_index,omitempty"`
	Messages       []failedTxMsg `json:"messages"`
	GasWanted      int64         `json:"gas_wanted"`
	GasUsed        int64         `json:"gas_used"`
	// AntePassed: the fee and every signer's sequence were taken, so the
	// same transaction can never be in a block again, and the answer is
	// final.
	AntePassed bool   `json:"ante_passed"`
	Fee        string `json:"fee,omitempty"`
	Log        string `json:"log"`
	LogCut     bool   `json:"log_cut,omitempty"`
}

// failedTxMsgs are a record's messages as the answer lists them: each with
// whether it is one of the five Fibre messages, a MsgExec's own one level in.
func failedTxMsgs(ms []failedtx.Msg) []failedTxMsg {
	out := make([]failedTxMsg, 0, len(ms))
	for _, m := range ms {
		a := failedTxMsg{Index: m.Index, TypeURL: m.TypeURL, Fibre: failedtx.IsFibre(m.TypeURL)}
		if len(m.Inner) > 0 {
			a.Inner = failedTxMsgs(m.Inner)
		}
		out = append(out, a)
	}
	return out
}

// failedTx is the newest failed inclusion of hash (lower-case hex), or nil.
// A row that does not decode is a row fault (noteRowFault, what "failed transaction"):
// answered as no record, never a 500. A database error is returned.
func (s *Server) failedTx(ctx context.Context, hash string) (*failedTxAnswer, error) {
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
	e := failedtx.Explain(r)
	out := &failedTxAnswer{
		Height: r.Height, Time: r.Time.UTC(), Code: r.Code, Codespace: r.Codespace, Reason: e.Reason,
		Messages: failedTxMsgs(r.Messages), GasWanted: r.GasWanted, GasUsed: r.GasUsed,
		AntePassed: r.AntePassed, Fee: r.Fee, Log: r.Log, LogCut: r.LogCut,
	}
	if e.MsgIndex >= 0 {
		i := e.MsgIndex
		out.FailedMsgIndex = &i
	}
	return out, nil
}
