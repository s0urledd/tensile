package failedtx

import (
	"bytes"
	"reflect"
	"strings"
	"testing"

	"github.com/cosmos/cosmos-sdk/types/bech32"
)

// addr is seed's 20 (or n) bytes under hrp.
func addr(t *testing.T, hrp string, seed byte, n int) string {
	t.Helper()
	s, err := bech32.ConvertAndEncode(hrp, bytes.Repeat([]byte{seed}, n))
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestKindNamesTheFiveMessagesAndNothingElse(t *testing.T) {
	for url, want := range map[string]string{
		urlPFF: KindSettlement, urlDeposit: KindDeposit, urlWithdraw: KindWithdrawalRequest, urlTimeout: KindTimeout, urlSetHost: KindSetHost,
	} {
		if got := Kind(url); got != want {
			t.Errorf("Kind(%s) = %q, want %q", url, got, want)
		}
	}
	for _, url := range []string{urlExec, urlSend, "/celestia.fibre.v1.MsgUpdateFibreParams", ""} {
		if got := Kind(url); got != "" {
			t.Errorf("Kind(%q) = %q", url, got)
		}
	}
	if len(kinds) != len(fibreTypeURLs) {
		t.Fatalf("%d kinds for %d Fibre type URLs", len(kinds), len(fibreTypeURLs))
	}
	for url := range fibreTypeURLs {
		if Kind(url) == "" {
			t.Errorf("the Fibre type URL %s has no kind", url)
		}
	}
}

// MayCarry finds each of the five URLs wherever it sits in the bytes, and
// nothing in bytes that hold none of them, a near miss included.
func TestMayCarrySeesEachURLAtAnyOffset(t *testing.T) {
	pad := []byte{0x0a, 0x2b, 0x00, 0xff, 'x'}
	for _, url := range []string{urlPFF, urlDeposit, urlWithdraw, urlTimeout, urlSetHost} {
		u := []byte(url)
		for name, raw := range map[string][]byte{
			"alone":  u,
			"first":  append(append([]byte(nil), u...), pad...),
			"inside": append(append(append([]byte(nil), pad...), u...), pad...),
			"last":   append(append([]byte(nil), pad...), u...),
		} {
			if !MayCarry(raw) {
				t.Errorf("%s %s: not seen", url, name)
			}
		}
	}
	for _, raw := range [][]byte{
		nil, {}, pad,
		[]byte(urlExec + urlSend),
		[]byte("/celestia.fibre.v1.MsgUpdateFibreParams"),
		[]byte(urlPFF[:len(urlPFF)-1]),                           // one byte short
		[]byte(strings.ToUpper(urlDeposit)),                      // not the URL's bytes
		[]byte("/celestia.valaddr.v1.MsgSetFibreProvider" + "X"), // a near miss
	} {
		if MayCarry(raw) {
			t.Errorf("%q: seen", raw)
		}
	}
}

// The account and operator spellings of one validator's 20 bytes are one
// operator address; nothing else is one.
func TestOperatorForm(t *testing.T) {
	op := addr(t, "celestiavaloper", 7, 20)
	acct := addr(t, "celestia", 7, 20)
	for _, in := range []string{op, acct, strings.ToUpper(op), strings.ToUpper(acct), " " + op + "\n"} {
		got, ok := OperatorForm(in)
		if !ok || got != op {
			t.Errorf("OperatorForm(%q) = %q %v, want %q", in, got, ok, op)
		}
	}
	for _, in := range []string{
		addr(t, "celestiavalcons", 7, 20),
		addr(t, "celestiavalconspub", 7, 20),
		addr(t, "celestiapub", 7, 20),
		addr(t, "celestia", 7, 32),
		addr(t, "celestiavaloper", 7, 32),
		"celestiavaloper1x", "garbage", "",
	} {
		if got, ok := OperatorForm(in); ok || got != "" {
			t.Errorf("OperatorForm(%q) = %q %v, want refused", in, got, ok)
		}
	}
}

func TestListAccount(t *testing.T) {
	op := addr(t, "celestiavaloper", 9, 20)
	pub := "celestia1pub"
	for _, c := range []struct {
		name string
		m    Msg
		want string
		ok   bool
	}{
		{"a settlement", Msg{TypeURL: urlPFF, Signer: "celestia1submitter", Detail: &MsgDetail{Publisher: pub, PromiseHash: "aa"}}, pub, true},
		{"a timeout", Msg{TypeURL: urlTimeout, Signer: "celestia1anyone", Detail: &MsgDetail{Publisher: pub, PromiseHash: "aa"}}, pub, true},
		{"a deposit", Msg{TypeURL: urlDeposit, Signer: pub, Detail: &MsgDetail{Publisher: pub, Amount: "5utia"}}, pub, true},
		{"a withdrawal request", Msg{TypeURL: urlWithdraw, Signer: pub, Detail: &MsgDetail{Publisher: pub, Amount: "5utia"}}, pub, true},
		{"a set-host signed by an operator", Msg{TypeURL: urlSetHost, Signer: strings.ToUpper(op), Detail: &MsgDetail{Host: "h:7980"}}, op, true},
		{"a settlement with no publisher", Msg{TypeURL: urlPFF, Signer: pub, Detail: &MsgDetail{Namespace: "00"}}, "", false},
		{"a cut deposit", Msg{TypeURL: urlDeposit, Signer: pub, Detail: &MsgDetail{Publisher: pub, Amount: "5utia"}, Cut: true}, "", false},
		{"a cut set-host", Msg{TypeURL: urlSetHost, Signer: op, Detail: &MsgDetail{Host: "h:7980"}, Cut: true}, "", false},
		{"a settlement that did not decode", Msg{TypeURL: urlPFF}, "", false},
		{"a set-host without detail", Msg{TypeURL: urlSetHost, Signer: op}, "", false},
		{"a set-host whose signer is no address", Msg{TypeURL: urlSetHost, Signer: "celestiavaloper1x", Detail: &MsgDetail{Host: "h:7980"}}, "", false},
		{"a transfer", Msg{TypeURL: urlSend, Signer: pub, Detail: &MsgDetail{Publisher: pub}}, "", false},
		{"a MsgExec", Msg{TypeURL: urlExec, Inner: []Msg{{TypeURL: urlDeposit, Signer: pub, Detail: &MsgDetail{Publisher: pub}}}}, "", false},
	} {
		if got, ok := ListAccount(c.m); got != c.want || ok != c.ok {
			t.Errorf("%s: %q %v, want %q %v", c.name, got, ok, c.want, c.ok)
		}
	}
}

// Defence only: the chain never makes final a set-host whose signer is in
// the account form (ValidateBasic refuses it before the ante handler), but
// one read as such is listed under the operator it names.
func TestListAccountTakesAnAccountFormSetHostSignerToItsOperator_DefenceOnly(t *testing.T) {
	op := addr(t, "celestiavaloper", 9, 20)
	acct := addr(t, "celestia", 9, 20)
	if got, ok := ListAccount(Msg{TypeURL: urlSetHost, Signer: acct, Detail: &MsgDetail{Host: "h:7980"}}); !ok || got != op {
		t.Fatalf("ListAccount = %q %v, want %q", got, ok, op)
	}
}

// A final failure lists each of its top-level Fibre messages that names an
// account, in order; a message inside a MsgExec, a transfer and a non-final
// failure list nothing.
func TestMsgRows(t *testing.T) {
	op := addr(t, "celestiavaloper", 3, 20)
	other := addr(t, "celestiavaloper", 4, 20)
	r := Record{AntePassed: true, Messages: []Msg{
		{Index: 0, TypeURL: urlSend},
		{Index: 1, TypeURL: urlDeposit, Signer: "celestia1pub", Detail: &MsgDetail{Publisher: "celestia1pub", Amount: "5utia"}},
		{Index: 2, TypeURL: urlExec, Inner: []Msg{{Index: 0, TypeURL: urlSetHost, Signer: other, Detail: &MsgDetail{Host: "x:7980"}}}},
		{Index: 3, TypeURL: urlSetHost, Signer: op, Detail: &MsgDetail{Host: "h:7980"}},
		{Index: 4, TypeURL: urlTimeout, Signer: "celestia1anyone", Detail: &MsgDetail{Publisher: "celestia1owner", PromiseHash: "aa"}},
	}}
	want := []MsgRow{
		{Account: "celestia1pub", MsgIndex: 1, TypeURL: urlDeposit},
		{Account: op, MsgIndex: 3, TypeURL: urlSetHost},
		{Account: "celestia1owner", MsgIndex: 4, TypeURL: urlTimeout},
	}
	if got := MsgRows(r); !reflect.DeepEqual(got, want) {
		t.Fatalf("a final failure:\n got %+v\nwant %+v", got, want)
	}
	r.AntePassed = false
	if got := MsgRows(r); len(got) != 0 {
		t.Fatalf("a non-final failure lists %+v", got)
	}
	if got := MsgRows(Record{AntePassed: true, Messages: []Msg{{Index: 0, TypeURL: urlSend}}}); len(got) != 0 {
		t.Fatalf("a transfer lists %+v", got)
	}
}
