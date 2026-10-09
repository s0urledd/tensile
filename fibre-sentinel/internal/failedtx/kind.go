package failedtx

import (
	"bytes"
	"strings"

	fibretypes "github.com/celestiaorg/celestia-app/v10/x/fibre/types"
	valaddrtypes "github.com/celestiaorg/celestia-app/v10/x/valaddr/types"
	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/cosmos/cosmos-sdk/types/bech32"
)

// The kinds the API names a Fibre transaction by, one per Fibre message type.
const (
	KindSettlement        = "settlement"         // MsgPayForFibre
	KindDeposit           = "deposit"            // MsgDepositToEscrow
	KindWithdrawalRequest = "withdrawal_request" // MsgRequestWithdrawal
	KindTimeout           = "timeout"            // MsgPaymentPromiseTimeout
	KindSetHost           = "set_host"           // MsgSetFibreProviderInfo
)

// kinds is the kind of each of the five Fibre type URLs (fibreTypeURLs), by
// the type URL the SDK gives their registered Go types.
var kinds = map[string]string{
	sdk.MsgTypeURL(&fibretypes.MsgPayForFibre{}):            KindSettlement,
	sdk.MsgTypeURL(&fibretypes.MsgDepositToEscrow{}):        KindDeposit,
	sdk.MsgTypeURL(&fibretypes.MsgRequestWithdrawal{}):      KindWithdrawalRequest,
	sdk.MsgTypeURL(&fibretypes.MsgPaymentPromiseTimeout{}):  KindTimeout,
	sdk.MsgTypeURL(&valaddrtypes.MsgSetFibreProviderInfo{}): KindSetHost,
}

// Kind is the kind of one of the five Fibre type URLs, "" for any other.
func Kind(typeURL string) string { return kinds[typeURL] }

// fibreURLBytes are the five Fibre type URLs as bytes, for MayCarry.
var fibreURLBytes = func() [][]byte {
	out := make([][]byte, 0, len(fibreTypeURLs))
	for u := range fibreTypeURLs {
		out = append(out, []byte(u))
	}
	return out
}()

// MayCarry reports whether raw tx bytes contain one of the five Fibre type
// URLs (bytes.Contains, each URL in turn). A tx carrying a Fibre message at
// the top level or inside a MsgExec holds the URL's bytes verbatim, so this
// is a filter before decoding, never the rule (Carries is).
//
// An Any's type_url is a proto string, written as its UTF-8 bytes; the
// TxBody sits verbatim in the TxRaw's body_bytes, and a MsgExec's messages
// are Anys inside its own value bytes, verbatim again. So MayCarry false
// means Carries false over whatever the bytes decode to.
func MayCarry(raw []byte) bool {
	for _, u := range fibreURLBytes {
		if bytes.Contains(raw, u) {
			return true
		}
	}
	return false
}

// OperatorForm is s, a 20-byte bech32 address under an account prefix
// ("celestia") or an operator prefix ("…valoper"), in its operator form
// (celestiavaloper1…, lower case); ok is false for anything else. The same
// rule as observer/api's operatorForm (a test holds the two together).
func OperatorForm(s string) (string, bool) {
	hrp, raw, err := bech32.DecodeAndConvert(strings.TrimSpace(strings.ToLower(s)))
	if err != nil || len(raw) != 20 {
		return "", false
	}
	var valoper string
	switch {
	case strings.HasSuffix(hrp, "valoper"):
		valoper = hrp
	case !strings.Contains(hrp, "val") && !strings.HasSuffix(hrp, "pub"):
		// an account prefix: the operator prefix is it plus "valoper", the
		// SDK's own convention; valcons and valconspub are no operator
		valoper = hrp + "valoper"
	default:
		return "", false
	}
	out, err := bech32.ConvertAndEncode(valoper, raw)
	if err != nil {
		return "", false
	}
	return out, true
}

// ListAccount is the account a top-level Fibre message of a final failure
// is listed under (failed_tx_msgs.account):
//
//	MsgPayForFibre, MsgPaymentPromiseTimeout, MsgDepositToEscrow,
//	MsgRequestWithdrawal: Detail.Publisher (the escrow owner; for a deposit
//	or a withdrawal, the signer);
//	MsgSetFibreProviderInfo: OperatorForm(Signer) (a final failure's signer
//	is always in the operator form, since ValidateBasic runs before the ante
//	handler; the account form is accepted as defence only).
//
// ok is false for any other message, a message with Cut set (a cut string
// is no address the chain accepted), a message without Detail (it did not
// decode, or asked for nothing), an empty account, and a signer
// OperatorForm refuses.
func ListAccount(m Msg) (account string, ok bool) {
	if m.Cut || m.Detail == nil {
		return "", false
	}
	switch Kind(m.TypeURL) {
	case KindSettlement, KindTimeout, KindDeposit, KindWithdrawalRequest:
		if m.Detail.Publisher == "" {
			return "", false
		}
		return m.Detail.Publisher, true
	case KindSetHost:
		return OperatorForm(m.Signer)
	}
	return "", false
}

// MsgRow is one failed_tx_msgs row of a record: the account a top-level
// Fibre message is listed under, the message's index and type URL.
type MsgRow struct {
	Account  string
	MsgIndex int
	TypeURL  string
}

// MsgRows are the failed_tx_msgs rows of r: none unless r.AntePassed; then
// one per top-level message (never Inner) with IsFibre(TypeURL) and
// ListAccount ok, in message order. The store writes exactly these, from
// the record it keeps (D7).
func MsgRows(r Record) []MsgRow {
	if !r.AntePassed {
		return nil
	}
	var out []MsgRow
	for _, m := range r.Messages {
		if !IsFibre(m.TypeURL) {
			continue
		}
		if acct, ok := ListAccount(m); ok {
			out = append(out, MsgRow{Account: acct, MsgIndex: m.Index, TypeURL: m.TypeURL})
		}
	}
	return out
}
