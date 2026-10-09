package txcost

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/failedtx"
)

const (
	urlPFF     = "/celestia.fibre.v1.MsgPayForFibre"
	urlDeposit = "/celestia.fibre.v1.MsgDepositToEscrow"
	urlSetHost = "/celestia.valaddr.v1.MsgSetFibreProviderInfo"
	urlExec    = "/cosmos.authz.v1beta1.MsgExec"
	urlSend    = "/cosmos.bank.v1beta1.MsgSend"
)

func TestKeyIsTheFailedTxKey(t *testing.T) {
	if got := Key(12, 3); got != "h12:3" || got != failedtx.Key(12, 3) {
		t.Fatalf("Key(12, 3) = %q", got)
	}
}

// Strip drops what a message asked for at both levels and keeps everything
// else; the messages it was given are left as they were.
func TestStripRemovesDetailAtBothLevelsAndNothingElse(t *testing.T) {
	in := []failedtx.Msg{
		{Index: 0, TypeURL: urlSend},
		{Index: 1, TypeURL: urlDeposit, Signer: "celestia1pub", Detail: &failedtx.MsgDetail{Publisher: "celestia1pub", Amount: "5utia"}},
		{Index: 2, TypeURL: urlExec, Inner: []failedtx.Msg{
			{Index: 0, TypeURL: urlSetHost, Signer: "celestiavaloper1v", Detail: &failedtx.MsgDetail{Host: "h.example:7980"}, Cut: true},
			{Index: 1, TypeURL: urlSend},
		}},
	}
	before, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	got := Strip(in)
	want := []failedtx.Msg{
		{Index: 0, TypeURL: urlSend},
		{Index: 1, TypeURL: urlDeposit, Signer: "celestia1pub"},
		{Index: 2, TypeURL: urlExec, Inner: []failedtx.Msg{
			{Index: 0, TypeURL: urlSetHost, Signer: "celestiavaloper1v", Cut: true},
			{Index: 1, TypeURL: urlSend},
		}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Strip:\n got %+v\nwant %+v", got, want)
	}
	after, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	if string(after) != string(before) {
		t.Fatalf("Strip changed its input:\n%s\n%s", before, after)
	}
	if Strip(nil) != nil {
		t.Fatal("Strip(nil) is not nil")
	}
}

// A line is written in the field order of Record, with no fee or payer when
// the chain printed none and no detail on any message.
func TestARecordIsWrittenAsThePinnedLine(t *testing.T) {
	at := time.Date(2026, 10, 8, 13, 25, 59, 888800317, time.UTC)
	r := Record{SchemaVersion: SchemaVersion, DedupeKey: Key(1497140, 2), Height: 1497140, TxIndex: 2, Time: at,
		TxHash:    "5da67b2a8865c75a572f5abb3070a2d3377a23baf371f705e1db1b312e21754e",
		GasWanted: 400000, GasUsed: 219118, Fee: "8000utia", FeePayer: "celestia1jw8afsj3j0c23fxs09nu8pq5asxwes5e3kkxdx",
		Messages: Strip([]failedtx.Msg{{Index: 0, TypeURL: urlPFF, Signer: "celestia1jw8afsj3j0c23fxs09nu8pq5asxwes5e3kkxdx",
			Detail: &failedtx.MsgDetail{Publisher: "celestia1jw8afsj3j0c23fxs09nu8pq5asxwes5e3kkxdx"}}}),
		RecordedAt: time.Date(2026, 10, 8, 13, 26, 1, 200000000, time.UTC)}
	b, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"schema_version":1,"dedupe_key":"h1497140:2","height":1497140,"tx_index":2,"time":"2026-10-08T13:25:59.888800317Z",` +
		`"tx_hash":"5da67b2a8865c75a572f5abb3070a2d3377a23baf371f705e1db1b312e21754e","gas_wanted":400000,"gas_used":219118,` +
		`"fee":"8000utia","fee_payer":"celestia1jw8afsj3j0c23fxs09nu8pq5asxwes5e3kkxdx",` +
		`"messages":[{"index":0,"type_url":"/celestia.fibre.v1.MsgPayForFibre","signer":"celestia1jw8afsj3j0c23fxs09nu8pq5asxwes5e3kkxdx"}],` +
		`"recorded_at":"2026-10-08T13:26:01.2Z"}`
	if string(b) != want {
		t.Fatalf("line:\n got %s\nwant %s", b, want)
	}

	var back Record
	if err := json.Unmarshal(b, &back); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(back, r) {
		t.Fatalf("round trip:\n got %+v\nwant %+v", back, r)
	}

	// A zero fee and no payer: neither key is written.
	r.Fee, r.FeePayer = "", ""
	b, err = json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	want = `{"schema_version":1,"dedupe_key":"h1497140:2","height":1497140,"tx_index":2,"time":"2026-10-08T13:25:59.888800317Z",` +
		`"tx_hash":"5da67b2a8865c75a572f5abb3070a2d3377a23baf371f705e1db1b312e21754e","gas_wanted":400000,"gas_used":219118,` +
		`"messages":[{"index":0,"type_url":"/celestia.fibre.v1.MsgPayForFibre","signer":"celestia1jw8afsj3j0c23fxs09nu8pq5asxwes5e3kkxdx"}],` +
		`"recorded_at":"2026-10-08T13:26:01.2Z"}`
	if string(b) != want {
		t.Fatalf("line without a fee:\n got %s\nwant %s", b, want)
	}
	var noFee Record
	if err := json.Unmarshal(b, &noFee); err != nil || !reflect.DeepEqual(noFee, r) {
		t.Fatalf("round trip without a fee: %+v %v", noFee, err)
	}
}
