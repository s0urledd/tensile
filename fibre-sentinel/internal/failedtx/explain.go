package failedtx

import (
	"errors"
	"regexp"
	"strconv"
	"strings"

	errorsmod "cosmossdk.io/errors"
	fibretypes "github.com/celestiaorg/celestia-app/v10/x/fibre/types"
	valaddrtypes "github.com/celestiaorg/celestia-app/v10/x/valaddr/types"
	sdkerrors "github.com/cosmos/cosmos-sdk/types/errors"
	"github.com/cosmos/cosmos-sdk/x/authz"
)

// Explanation is what a record says beyond its raw facts, read from them
// here and nowhere else: the record holds only what the chain returned, so
// this reading can be corrected without rewriting it.
type Explanation struct {
	// Reason is the plain wording, or "" when there is none that rests on
	// exact information: an app version the table is not pinned to, or a
	// (codespace, code) the table does not hold.
	Reason   string
	MsgIndex int // the failing message's Index, or -1 when the log does not state one
}

// pinnedAppVersions are the app versions whose registered errors and runMsgs
// log prefix the tests check against the pinned module (celestia-app v10.4.0).
// A new chain version is added only after its errors and the prefix are
// checked again.
var pinnedAppVersions = map[uint64]bool{10: true}

// Explain reads a record:
//
//	app version not pinned: {"", -1};
//	Reason   = Reason(r.Codespace, r.Code);
//	MsgIndex = N when r.Log matches msgIndexRe AND some r.Messages[k].Index == N; else -1.
//
// Nothing else in the log is read.
func Explain(r Record) Explanation {
	if !pinnedAppVersions[r.AppVersion] {
		return Explanation{MsgIndex: -1}
	}
	e := Explanation{Reason: Reason(r.Codespace, r.Code), MsgIndex: -1}
	m := msgIndexRe.FindStringSubmatch(r.Log)
	if m == nil {
		return e
	}
	n, err := strconv.Atoi(m[1])
	if err != nil {
		return e
	}
	for _, msg := range r.Messages {
		if msg.Index == n {
			e.MsgIndex = n
			break
		}
	}
	return e
}

// msgIndexRe is the cosmos-sdk baseapp.go:927 prefix, anchored at the start of the log.
var msgIndexRe = regexp.MustCompile(`^failed to execute message; message index: (0|[1-9][0-9]{0,5}): `)

// Reason: the table entry for (codespace, code), or "".
func Reason(codespace string, code uint32) string {
	return reasons[reasonKey{codespace, code}]
}

// reasonKey is a registered error's identity on chain: a code means nothing
// without its codespace (sdk/2, valaddr/2, fibre/2 and authz/2 are four
// different errors).
type reasonKey struct {
	codespace string
	code      uint32
}

// reasonErrs are the registered errors whose description is the reason, in
// this order: cosmos-sdk's root errors (codes 2-41), x/valaddr's, x/fibre's
// (genesis-only at v10.4.0, listed for completeness) and x/authz's.
var reasonErrs = []*errorsmod.Error{
	sdkerrors.ErrTxDecode, sdkerrors.ErrInvalidSequence, sdkerrors.ErrUnauthorized,
	sdkerrors.ErrInsufficientFunds, sdkerrors.ErrUnknownRequest, sdkerrors.ErrInvalidAddress,
	sdkerrors.ErrInvalidPubKey, sdkerrors.ErrUnknownAddress, sdkerrors.ErrInvalidCoins,
	sdkerrors.ErrOutOfGas, sdkerrors.ErrMemoTooLarge, sdkerrors.ErrInsufficientFee,
	sdkerrors.ErrTooManySignatures, sdkerrors.ErrNoSignatures, sdkerrors.ErrJSONMarshal,
	sdkerrors.ErrJSONUnmarshal, sdkerrors.ErrInvalidRequest, sdkerrors.ErrTxInMempoolCache,
	sdkerrors.ErrMempoolIsFull, sdkerrors.ErrTxTooLarge, sdkerrors.ErrKeyNotFound,
	sdkerrors.ErrWrongPassword, sdkerrors.ErrorInvalidSigner, sdkerrors.ErrorInvalidGasAdjustment,
	sdkerrors.ErrInvalidHeight, sdkerrors.ErrInvalidVersion, sdkerrors.ErrInvalidChainID,
	sdkerrors.ErrInvalidType, sdkerrors.ErrTxTimeoutHeight, sdkerrors.ErrUnknownExtensionOptions,
	sdkerrors.ErrWrongSequence, sdkerrors.ErrPackAny, sdkerrors.ErrUnpackAny,
	sdkerrors.ErrLogic, sdkerrors.ErrConflict, sdkerrors.ErrNotSupported,
	sdkerrors.ErrNotFound, sdkerrors.ErrIO, sdkerrors.ErrAppConfig,
	sdkerrors.ErrInvalidGasLimit,

	valaddrtypes.ErrInvalidHostAddress, valaddrtypes.ErrInvalidValidator,

	fibretypes.ErrInvalidSigner, fibretypes.ErrDuplicateSigner, fibretypes.ErrInvalidBalance,
	fibretypes.ErrInvalidAmount, fibretypes.ErrInvalidTimestamp, fibretypes.ErrInvalidHash,
	fibretypes.ErrDuplicateHash,

	authz.ErrNoAuthorizationFound, authz.ErrInvalidExpirationTime, authz.ErrUnknownAuthorizationType,
	authz.ErrNoGrantKeyFound, authz.ErrAuthorizationExpired, authz.ErrGranteeIsGranter,
	authz.ErrAuthorizationNumOfSigners, authz.ErrNegativeMaxTokens,
}

// Two overrides, where the registered text would misdescribe the error.
const (
	// ABCIInfo gives undefined/1 to any error whose Cause() chain reaches
	// no registered code (an errors.New, or a registered error wrapped with
	// fmt.Errorf("%w"), which has Unwrap but no Cause). Its registered text,
	// "internal", is not what happened.
	reasonNoCode = "The chain gave this error no code"
	// errorsmod.ErrPanic: the node recovered a panic while running the tx.
	reasonPanic = "Execution panicked"
)

// reasons is the table: each var's registered description in sentence case,
// keyed by its (Codespace(), ABCICode()), and the two overrides.
var reasons = func() map[reasonKey]string {
	m := make(map[reasonKey]string, len(reasonErrs)+2)
	for _, e := range reasonErrs {
		m[reasonKey{e.Codespace(), e.ABCICode()}] = sentence(e.Error())
	}
	space, code, _ := errorsmod.ABCIInfo(errors.New("an error with no code"), false)
	m[reasonKey{space, code}] = reasonNoCode
	m[reasonKey{errorsmod.ErrPanic.Codespace(), errorsmod.ErrPanic.ABCICode()}] = reasonPanic
	return m
}()

// sentence is a registered description in sentence case: its first letter
// upper-cased ("tx parse error" reads "Tx parse error").
func sentence(d string) string {
	if d == "" {
		return d
	}
	return strings.ToUpper(d[:1]) + d[1:]
}
