// Package failedtx is the record of a transaction that failed in a block
// while carrying a Fibre message (failed_txs.jsonl), and how it is read.
package failedtx

import (
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	errorsmod "cosmossdk.io/errors"
	fibretypes "github.com/celestiaorg/celestia-app/v10/x/fibre/types"
	valaddrtypes "github.com/celestiaorg/celestia-app/v10/x/valaddr/types"
	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/cosmos/cosmos-sdk/x/authz"
)

// FileName is the record file under the data dir.
const FileName = "failed_txs.jsonl"

// SchemaVersion is bumped when a field changes meaning.
const SchemaVersion = 1

// MaxLogBytes caps the stored log (CutLog).
const MaxLogBytes = 8 << 10

// Msg is one message of the transaction, in TxBody order.
type Msg struct {
	Index   int    `json:"index"`    // position in TxBody.messages (or in MsgExec.msgs for Inner)
	TypeURL string `json:"type_url"` // the Any's type URL, verbatim
	// Inner: the messages of a /cosmos.authz.v1beta1.MsgExec, one level, in
	// order; absent for every other message and for a MsgExec that did not decode.
	Inner []Msg `json:"inner,omitempty"`
	// Fibre messages only (IsFibre), decoded from the message itself; empty when it did not decode.
	Signer string     `json:"signer,omitempty"` // the message's signer field, bech32 as written
	Detail *MsgDetail `json:"detail,omitempty"`
}

// MsgDetail is what the message asked for, never what happened (the tx failed: none of it took effect).
type MsgDetail struct {
	Publisher   string `json:"publisher,omitempty"`    // PFF and timeout: the escrow owner, from the promise's signer pubkey (scan.PublisherOf); deposit and withdrawal: the signer
	PromiseHash string `json:"promise_hash,omitempty"` // PFF and timeout
	Namespace   string `json:"namespace,omitempty"`    // PFF: the promise's namespace, hex as publications.jsonl writes it
	BlobSize    int64  `json:"blob_size,omitempty"`    // PFF: the promise's blob size
	Amount      string `json:"amount,omitempty"`       // deposit and withdrawal: the coin as the message carries it, e.g. "1000000utia"
	Host        string `json:"host,omitempty"`         // MsgSetFibreProviderInfo: the requested host, verbatim
	Validator   string `json:"validator,omitempty"`    // MsgSetFibreProviderInfo: the validator it registers for, as the message names it
}

// Record is one line of failed_txs.jsonl.
type Record struct {
	SchemaVersion int       `json:"schema_version"`
	DedupeKey     string    `json:"dedupe_key"` // Key(Height, TxIndex)
	Height        int64     `json:"height"`
	Time          time.Time `json:"time"`        // block time, UTC: the export's TimeField
	AppVersion    uint64    `json:"app_version"` // the block header's app version: the rules the block ran under
	TxHash        string    `json:"tx_hash"`     // lower-case hex SHA-256 of the tx bytes
	TxIndex       int       `json:"tx_index"`
	Code          uint32    `json:"code"` // never 0
	Codespace     string    `json:"codespace"`
	Log           string    `json:"log"` // as the node returned it, but for CutLog
	LogCut        bool      `json:"log_cut,omitempty"`
	GasWanted     int64     `json:"gas_wanted"` // as returned: may be negative (uint64 overflow)
	GasUsed       int64     `json:"gas_used"`   // may exceed GasWanted (out of gas)
	// AntePassed: the result carries a "tx" event with a "fee" attribute,
	// which the SDK keeps in a failed result only when the whole ante chain
	// passed. The fee and every signer's sequence were then taken, so the
	// same bytes can never be in a block again.
	AntePassed bool `json:"ante_passed"`
	// Fee is that attribute's value as the chain printed it ("2000utia");
	// empty for a zero fee, and always empty when AntePassed is false.
	Fee        string    `json:"fee,omitempty"`
	Messages   []Msg     `json:"messages"`
	RecordedAt time.Time `json:"recorded_at"`
}

// Key is a failed inclusion's dedupe key: one per (height, tx index).
func Key(height int64, txIndex int) string { return fmt.Sprintf("h%d:%d", height, txIndex) }

// fibreTypeURLs are the five owner-listed messages, by the type URL the SDK
// gives their registered Go types.
var fibreTypeURLs = map[string]bool{
	sdk.MsgTypeURL(&fibretypes.MsgPayForFibre{}):            true,
	sdk.MsgTypeURL(&fibretypes.MsgDepositToEscrow{}):        true,
	sdk.MsgTypeURL(&fibretypes.MsgRequestWithdrawal{}):      true,
	sdk.MsgTypeURL(&fibretypes.MsgPaymentPromiseTimeout{}):  true,
	sdk.MsgTypeURL(&valaddrtypes.MsgSetFibreProviderInfo{}): true,
}

// pffTypeURL is MsgPayForFibre's type URL.
var pffTypeURL = sdk.MsgTypeURL(&fibretypes.MsgPayForFibre{})

// IsFibre reports one of the five owner-listed type URLs. The set is built
// with sdk.MsgTypeURL from the registered Go types:
//
//	&fibretypes.MsgPayForFibre{}, &fibretypes.MsgDepositToEscrow{},
//	&fibretypes.MsgRequestWithdrawal{}, &fibretypes.MsgPaymentPromiseTimeout{},
//	&valaddrtypes.MsgSetFibreProviderInfo{}
func IsFibre(typeURL string) bool { return fibreTypeURLs[typeURL] }

// ExecTypeURL is sdk.MsgTypeURL(&authz.MsgExec{}) ("/cosmos.authz.v1beta1.MsgExec").
var ExecTypeURL = sdk.MsgTypeURL(&authz.MsgExec{})

// Carries reports whether any message, or any Inner message, IsFibre.
func Carries(msgs []Msg) bool {
	for _, m := range msgs {
		if IsFibre(m.TypeURL) {
			return true
		}
		for _, in := range m.Inner {
			if IsFibre(in.TypeURL) {
				return true
			}
		}
	}
	return false
}

// CarriesPFF reports whether any message is MsgPayForFibre.
func CarriesPFF(msgs []Msg) bool {
	for _, m := range msgs {
		if m.TypeURL == pffTypeURL {
			return true
		}
	}
	return false
}

// panicStack is where the SDK's panic log starts the node's stack trace
// (cosmos-sdk baseapp/recovery.go: "recovered: <v>\nstack:\n<stack>").
const panicStack = "\nstack:\n"

// CutLog is the log as stored:
//  1. codespace "undefined" and code 111222 (errorsmod.ErrPanic): cut at
//     the first "\nstack:\n";
//  2. then, past MaxLogBytes: cut to the last UTF-8 rune start <= MaxLogBytes.
//
// cut reports either.
func CutLog(codespace string, code uint32, log string) (stored string, cut bool) {
	stored = log
	if codespace == errorsmod.ErrPanic.Codespace() && code == errorsmod.ErrPanic.ABCICode() {
		if i := strings.Index(stored, panicStack); i >= 0 {
			stored, cut = stored[:i], true
		}
	}
	if len(stored) > MaxLogBytes {
		n := MaxLogBytes
		for n > 0 && !utf8.RuneStart(stored[n]) {
			n--
		}
		stored, cut = stored[:n], true
	}
	return stored, cut
}
