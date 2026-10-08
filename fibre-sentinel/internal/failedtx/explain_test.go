package failedtx

import (
	"fmt"
	"strings"
	"testing"

	errorsmod "cosmossdk.io/errors"
	fibretypes "github.com/celestiaorg/celestia-app/v10/x/fibre/types"
	valaddrtypes "github.com/celestiaorg/celestia-app/v10/x/valaddr/types"
	sdkerrors "github.com/cosmos/cosmos-sdk/types/errors"
	"github.com/cosmos/cosmos-sdk/x/authz"
)

// The fixtures here are built from the pinned module's own error values and
// passed through the SDK's own ABCIInfo, as the node does: the codes,
// codespaces and logs are what the chain returns, never copied text.

// result is the record of a tx that failed with err at app version 10.
func result(err error, msgs ...Msg) Record {
	r := sdkerrors.ResponseExecTxResultWithEvents(err, 200000, 91234, nil, false)
	return Record{AppVersion: 10, Code: r.Code, Codespace: r.Codespace, Log: r.Log, GasWanted: r.GasWanted, GasUsed: r.GasUsed, Messages: msgs}
}

// inMessage is err as runMsgs returns it for the message at index i. The
// format string is the one literal copied: cosmos-sdk baseapp/baseapp.go:927
// (runMsgs, unexported).
func inMessage(err error, i int) error {
	return errorsmod.Wrapf(err, "failed to execute message; message index: %d", i)
}

var (
	// x/fibre keeper msg_server.go:53, over the bank's spendable-balance error.
	depositErr = errorsmod.Wrapf(errorsmod.Wrapf(sdkerrors.ErrInsufficientFunds, "spendable balance %s is smaller than %s", "10utia", "1000000utia"), "failed to transfer funds to escrow")
	// x/fibre keeper msg_server.go:79.
	withdrawalErr = errorsmod.Wrapf(sdkerrors.ErrNotFound, "escrow account not found for signer: %s", "celestia1abc")
	// x/valaddr keeper msg_server.go.
	hostErr = valaddrtypes.ErrInvalidValidator.Wrapf("validator not found: %v", "celestiavaloper1abc")
	// x/authz: no grant for the inner message.
	delegatedErr = authz.ErrNoAuthorizationFound
	// baseapp/recovery.go:58-63.
	outOfGasErr = errorsmod.Wrap(sdkerrors.ErrOutOfGas, fmt.Sprintf("out of gas in location: %v; gasWanted: %d, gasUsed: %d", "WritePerByte", 200000, 200133))
	// baseapp/recovery.go:72-77.
	panicErr = errorsmod.Wrap(sdkerrors.ErrPanic, fmt.Sprintf("recovered: %v\nstack:\n%v", "boom", "goroutine 1 [running]:"))
	// A registered error wrapped the standard way has no Cause(): no code.
	noCodeErr = fmt.Errorf("wrapped: %w", sdkerrors.ErrInsufficientFunds)
)

var (
	sendDeposit = []Msg{{Index: 0, TypeURL: urlSend}, {Index: 1, TypeURL: urlDeposit}}
	withdrawal  = []Msg{{Index: 0, TypeURL: urlWithdraw}}
	setHost     = []Msg{{Index: 0, TypeURL: urlSetHost}}
	delegated   = []Msg{{Index: 0, TypeURL: urlExec, Inner: []Msg{{Index: 0, TypeURL: urlSetHost}}}}
	pff         = []Msg{{Index: 0, TypeURL: urlPFF}}
)

func TestExplainTheFailuresOfEachKind(t *testing.T) {
	for _, c := range []struct {
		name   string
		r      Record
		code   string // codespace/code, as the chain returns it
		reason string
		index  int
	}{
		{"deposit, second message", result(inMessage(depositErr, 1), sendDeposit...), "sdk/5", "Insufficient funds", 1},
		{"withdrawal", result(inMessage(withdrawalErr, 0), withdrawal...), "sdk/38", "Not found", 0},
		{"host", result(inMessage(hostErr, 0), setHost...), "valaddr/2", "Invalid validator", 0},
		{"delegated host: the outer index", result(inMessage(delegatedErr, 0), delegated...), "authz/2", "Authorization not found", 0},
		{"out of gas names no message", result(outOfGasErr, pff...), "sdk/11", "Out of gas", -1},
		{"a panic names no message", result(panicErr, pff...), "undefined/111222", "Execution panicked", -1},
		{"no code", result(inMessage(noCodeErr, 1), sendDeposit...), "undefined/1", "The chain gave this error no code", 1},
	} {
		t.Run(c.name, func(t *testing.T) {
			if got := fmt.Sprintf("%s/%d", c.r.Codespace, c.r.Code); got != c.code {
				t.Fatalf("the fixture failed as %s, want %s (log %q)", got, c.code, c.r.Log)
			}
			got := Explain(c.r)
			if got.Reason != c.reason || got.MsgIndex != c.index {
				t.Fatalf("Explain = %+v, want {%q %d} (log %q)", got, c.reason, c.index, c.r.Log)
			}
		})
	}
}

// The log the deposit fixture gives is the one the record's documentation
// shows: the runMsgs prefix, then each wrap, then the registered text.
func TestTheDepositLogIsTheChainsWording(t *testing.T) {
	r := result(inMessage(depositErr, 1), sendDeposit...)
	want := "failed to execute message; message index: 1: failed to transfer funds to escrow: spendable balance 10utia is smaller than 1000000utia: insufficient funds"
	if r.Log != want {
		t.Fatalf("log %q", r.Log)
	}
}

// Every table entry is its registered error: keyed by the var's own
// (Codespace(), ABCICode()), worded as its own Error() in sentence case,
// no key twice, and the two overrides beside them.
func TestTheReasonTableIsTheRegisteredErrors(t *testing.T) {
	seen := map[reasonKey]bool{}
	for _, e := range reasonErrs {
		k := reasonKey{e.Codespace(), e.ABCICode()}
		if seen[k] {
			t.Errorf("%s/%d listed twice", k.codespace, k.code)
		}
		seen[k] = true
		d := e.Error()
		want := strings.ToUpper(d[:1]) + d[1:]
		if got := Reason(e.Codespace(), e.ABCICode()); got != want {
			t.Errorf("%s/%d: %q, want %q", k.codespace, k.code, got, want)
		}
	}
	if len(reasons) != len(reasonErrs)+2 {
		t.Errorf("%d entries for %d errors and 2 overrides", len(reasons), len(reasonErrs))
	}
	// cosmos-sdk's root errors, codes 2 to 41, every one.
	for code := uint32(2); code <= 41; code++ {
		if Reason(sdkerrors.RootCodespace, code) == "" {
			t.Errorf("sdk/%d has no reason", code)
		}
	}
	for _, e := range []*errorsmod.Error{fibretypes.ErrInvalidSigner, fibretypes.ErrDuplicateHash, valaddrtypes.ErrInvalidHostAddress, authz.ErrNegativeMaxTokens} {
		if !seen[reasonKey{e.Codespace(), e.ABCICode()}] {
			t.Errorf("%s/%d is not in the table", e.Codespace(), e.ABCICode())
		}
	}
}

func TestTheSameCodeInDifferentCodespaces(t *testing.T) {
	for _, c := range []struct {
		space string
		want  string
	}{
		{"sdk", "Tx parse error"},
		{"valaddr", "Invalid validator"},
		{"fibre", "Duplicate signer"},
		{"authz", "Authorization not found"},
	} {
		if got := Reason(c.space, 2); got != c.want {
			t.Errorf("%s/2 = %q, want %q", c.space, got, c.want)
		}
	}
	if got := Reason(errorsmod.UndefinedCodespace, 1); got != "The chain gave this error no code" {
		t.Errorf("undefined/1 = %q", got)
	}
	if got := Reason(errorsmod.UndefinedCodespace, 111222); got != "Execution panicked" {
		t.Errorf("undefined/111222 = %q", got)
	}
}

func TestACodeNotInTheTableHasNoReason(t *testing.T) {
	if got := Reason("fibre", 9); got != "" {
		t.Errorf("fibre/9 = %q", got)
	}
	if got := Reason("", 5); got != "" {
		t.Errorf("an empty codespace's code 5 = %q", got)
	}
	r := result(inMessage(depositErr, 1), sendDeposit...)
	r.Codespace, r.Code = "fibre", 9
	if got := Explain(r); got.Reason != "" || got.MsgIndex != 1 {
		t.Errorf("fibre/9 with a message index: %+v", got)
	}
}

// A record from an app version the table is not pinned to gets neither a
// reason nor an index, whatever its log says.
func TestAnUnpinnedAppVersionGetsNoReading(t *testing.T) {
	for _, v := range []uint64{11, 0, 9} {
		r := result(inMessage(depositErr, 1), sendDeposit...)
		r.AppVersion = v
		if got := Explain(r); got != (Explanation{Reason: "", MsgIndex: -1}) {
			t.Errorf("app version %d: %+v", v, got)
		}
	}
}

func TestTheMessageIndexIsReadOnlyFromTheAnchoredPrefix(t *testing.T) {
	// The prefix at the start names a listed message.
	if got := Explain(result(inMessage(depositErr, 1), sendDeposit...)); got.MsgIndex != 1 {
		t.Errorf("index 1 of [send, deposit]: %d", got.MsgIndex)
	}
	// An index no listed message has: the message the log names is not one
	// the record holds (out of range, or a nil Any the scanner skipped).
	if got := Explain(result(inMessage(depositErr, 5), sendDeposit...)); got.MsgIndex != -1 {
		t.Errorf("index 5 of two messages: %d", got.MsgIndex)
	}
	gap := []Msg{{Index: 0, TypeURL: urlSend}, {Index: 2, TypeURL: urlDeposit}}
	if got := Explain(result(inMessage(depositErr, 1), gap...)); got.MsgIndex != -1 {
		t.Errorf("index 1 of messages 0 and 2: %d", got.MsgIndex)
	}
	if got := Explain(result(inMessage(depositErr, 2), gap...)); got.MsgIndex != 2 {
		t.Errorf("index 2 of messages 0 and 2: %d", got.MsgIndex)
	}
	// A node run with --trace prints the stack first: the prefix is not at
	// the start, and the log gives no index.
	traced := sdkerrors.ResponseExecTxResultWithEvents(inMessage(depositErr, 1), 200000, 91234, nil, true)
	if !strings.Contains(traced.Log, "failed to execute message; message index: 1: ") || strings.HasPrefix(traced.Log, "failed to execute message") {
		t.Fatalf("the --trace fixture is not a traced log: %q", traced.Log)
	}
	r := Record{AppVersion: 10, Code: traced.Code, Codespace: traced.Codespace, Log: traced.Log, Messages: sendDeposit}
	if got := Explain(r); got.MsgIndex != -1 || got.Reason != "Insufficient funds" {
		t.Errorf("a traced log: %+v", got)
	}
	// The other prefix, a message that ran and whose events failed, names
	// no failing message.
	r = result(errorsmod.Wrapf(sdkerrors.ErrJSONMarshal, "failed to create message events; message index: %d", 1), sendDeposit...)
	if got := Explain(r); got.MsgIndex != -1 {
		t.Errorf("the events prefix gave index %d", got.MsgIndex)
	}
	// Out of gas and a panic name none, whatever the messages.
	for _, err := range []error{outOfGasErr, panicErr} {
		if got := Explain(result(err, sendDeposit...)); got.MsgIndex != -1 {
			t.Errorf("%v gave index %d", err, got.MsgIndex)
		}
	}
}
