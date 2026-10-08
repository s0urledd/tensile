package failedtx

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	errorsmod "cosmossdk.io/errors"
	sdkerrors "github.com/cosmos/cosmos-sdk/types/errors"
)

const (
	urlPFF      = "/celestia.fibre.v1.MsgPayForFibre"
	urlDeposit  = "/celestia.fibre.v1.MsgDepositToEscrow"
	urlWithdraw = "/celestia.fibre.v1.MsgRequestWithdrawal"
	urlTimeout  = "/celestia.fibre.v1.MsgPaymentPromiseTimeout"
	urlSetHost  = "/celestia.valaddr.v1.MsgSetFibreProviderInfo"
	urlExec     = "/cosmos.authz.v1beta1.MsgExec"
	urlSend     = "/cosmos.bank.v1beta1.MsgSend"
)

// The five owner-listed messages are Fibre, by their literal type URLs, and
// nothing else is: not the params update, not the MsgExec that may carry
// one, not a transfer.
func TestIsFibreHoldsForExactlyTheFiveMessages(t *testing.T) {
	for _, u := range []string{urlPFF, urlDeposit, urlWithdraw, urlTimeout, urlSetHost} {
		if !IsFibre(u) {
			t.Errorf("%s is not Fibre", u)
		}
	}
	for _, u := range []string{"/celestia.fibre.v1.MsgUpdateFibreParams", urlExec, urlSend, "", "celestia.fibre.v1.MsgPayForFibre"} {
		if IsFibre(u) {
			t.Errorf("%q is Fibre", u)
		}
	}
	if len(fibreTypeURLs) != 5 {
		t.Errorf("%d Fibre type URLs, want 5", len(fibreTypeURLs))
	}
	if ExecTypeURL != urlExec {
		t.Errorf("ExecTypeURL = %q", ExecTypeURL)
	}
}

func TestCarriesTopLevelInnerAndNone(t *testing.T) {
	top := []Msg{{Index: 0, TypeURL: urlSend}, {Index: 1, TypeURL: urlDeposit}}
	inner := []Msg{{Index: 0, TypeURL: urlExec, Inner: []Msg{{Index: 0, TypeURL: urlSetHost}}}}
	none := []Msg{{Index: 0, TypeURL: urlSend}, {Index: 1, TypeURL: urlExec, Inner: []Msg{{Index: 0, TypeURL: urlSend}}}}
	pff := []Msg{{Index: 0, TypeURL: urlPFF}}
	if !Carries(top) || !Carries(inner) || !Carries(pff) {
		t.Error("a Fibre message, at the top or inside a MsgExec, is not carried")
	}
	if Carries(none) || Carries(nil) {
		t.Error("a tx with no Fibre message carries one")
	}
	if !CarriesPFF(pff) {
		t.Error("a MsgPayForFibre is not a PFF")
	}
	if CarriesPFF(top) || CarriesPFF(inner) || CarriesPFF(none) {
		t.Error("a tx with no MsgPayForFibre carries one")
	}
}

func TestKey(t *testing.T) {
	if got := Key(12, 3); got != "h12:3" {
		t.Fatalf("Key(12, 3) = %q", got)
	}
}

// The panic log is built as cosmos-sdk's recovery middleware builds it
// (baseapp/recovery.go) and passed through the same ABCIInfo the node uses.
func TestCutLogCutsAPanicAtItsStack(t *testing.T) {
	err := errorsmod.Wrap(sdkerrors.ErrPanic, fmt.Sprintf("recovered: %v\nstack:\n%v", "invalid coin denominations; 1000tia, utia", "goroutine 1 [running]:\nmain.main()\n\t/home/node/app.go:10 +0x1d"))
	r := sdkerrors.ResponseExecTxResultWithEvents(err, 200000, 91234, nil, false)
	if r.Codespace != "undefined" || r.Code != 111222 || !strings.Contains(r.Log, "/home/node/app.go") {
		t.Fatalf("the panic fixture is not a panic: %s/%d %q", r.Codespace, r.Code, r.Log)
	}
	stored, cut := CutLog(r.Codespace, r.Code, r.Log)
	if stored != "recovered: invalid coin denominations; 1000tia, utia" || !cut {
		t.Fatalf("panic log stored as %q, cut=%v", stored, cut)
	}

	// The same text under any other code is not a panic and is kept whole.
	other := "failed to execute message; message index: 0: a\nstack:\nb: insufficient funds"
	if stored, cut := CutLog("sdk", 5, other); stored != other || cut {
		t.Fatalf("a non-panic log was cut: %q, %v", stored, cut)
	}
	if stored, cut := CutLog("undefined", 1, other); stored != other || cut {
		t.Fatalf("undefined/1 was cut as a panic: %q, %v", stored, cut)
	}
}

func TestCutLogKeepsAWholeRuneUnderTheCap(t *testing.T) {
	long := strings.Repeat("a", MaxLogBytes-1) + "é" + strings.Repeat("b", 100)
	stored, cut := CutLog("sdk", 5, long)
	if !cut || len(stored) != MaxLogBytes-1 || !utf8.ValidString(stored) || stored != strings.Repeat("a", MaxLogBytes-1) {
		t.Fatalf("cut=%v len=%d valid=%v", cut, len(stored), utf8.ValidString(stored))
	}
	// A rune that ends exactly at the cap is kept.
	exact := strings.Repeat("a", MaxLogBytes-2) + "é" + "z"
	if stored, cut := CutLog("sdk", 5, exact); !cut || stored != strings.Repeat("a", MaxLogBytes-2)+"é" {
		t.Fatalf("a rune ending at the cap: cut=%v len=%d", cut, len(stored))
	}
	// A panic whose message alone passes the cap is cut both ways.
	huge := "recovered: " + strings.Repeat("x", 2*MaxLogBytes) + "\nstack:\ngoroutine 1"
	if stored, cut := CutLog("undefined", 111222, huge); !cut || len(stored) != MaxLogBytes || strings.Contains(stored, "goroutine") {
		t.Fatalf("a long panic: cut=%v len=%d", cut, len(stored))
	}
}

func TestCutLogLeavesAShortLogAlone(t *testing.T) {
	for _, l := range []string{"", "out of gas in location: WritePerByte; gasWanted: 53233, gasUsed: 53641: out of gas", strings.Repeat("a", MaxLogBytes)} {
		if stored, cut := CutLog("sdk", 11, l); stored != l || cut {
			t.Errorf("a log within the cap changed: len %d -> %d, cut=%v", len(l), len(stored), cut)
		}
	}
}

func TestRecordJSONRoundTrip(t *testing.T) {
	full := Record{
		SchemaVersion: SchemaVersion, DedupeKey: Key(1300123, 3), Height: 1300123,
		Time: time.Date(2026, 10, 10, 8, 1, 2, 123456789, time.UTC), AppVersion: 10,
		TxHash: strings.Repeat("5f", 32), TxIndex: 3, Code: 5, Codespace: "sdk",
		Log: "failed to execute message; message index: 1: insufficient funds", LogCut: true,
		GasWanted: 200000, GasUsed: 91234, AntePassed: true, Fee: "2000utia",
		Messages: []Msg{
			{Index: 0, TypeURL: urlSend},
			{Index: 1, TypeURL: urlDeposit, Signer: "celestia1abc", Detail: &MsgDetail{Publisher: "celestia1abc", Amount: "1000000utia"}},
			{Index: 2, TypeURL: urlExec, Inner: []Msg{{Index: 0, TypeURL: urlSetHost, Signer: "celestiavaloper1x", Detail: &MsgDetail{Host: "h.example:7980", Validator: "celestiavaloper1x"}}}},
		},
		RecordedAt: time.Date(2026, 10, 10, 8, 1, 4, 500000000, time.UTC),
	}
	b, err := json.Marshal(full)
	if err != nil {
		t.Fatal(err)
	}
	var back Record
	if err := json.Unmarshal(b, &back); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(full, back) {
		t.Fatalf("round trip changed the record:\n%+v\n%+v", full, back)
	}
	for _, k := range []string{`"log_cut":true`, `"fee":"2000utia"`, `"inner":[`, `"detail":{"publisher":"celestia1abc","amount":"1000000utia"}`} {
		if !strings.Contains(string(b), k) {
			t.Errorf("%s missing from %s", k, b)
		}
	}

	// The optional keys are absent when empty: no log cut, no fee, a message
	// with no inner messages, no signer and no detail.
	bare := full
	bare.LogCut, bare.Fee, bare.AntePassed = false, "", false
	bare.Messages = []Msg{{Index: 0, TypeURL: urlSetHost}}
	b, err = json.Marshal(bare)
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"log_cut", `"fee"`, "inner", "signer", "detail"} {
		if strings.Contains(string(b), k) {
			t.Errorf("%s present in %s", k, b)
		}
	}
	if !strings.Contains(string(b), `"ante_passed":false`) || !strings.Contains(string(b), `"messages":[{"index":0,"type_url":"/celestia.valaddr.v1.MsgSetFibreProviderInfo"}]`) {
		t.Errorf("the bare record lost a key it always carries: %s", b)
	}
}
