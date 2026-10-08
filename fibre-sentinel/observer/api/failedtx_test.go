package api_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	errorsmod "cosmossdk.io/errors"
	sdkerrors "github.com/cosmos/cosmos-sdk/types/errors"
	"github.com/cosmos/cosmos-sdk/x/authz"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/failedtx"
	"github.com/plsgiveup/fibre/fibre-sentinel/internal/scan"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/api"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/ingest"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
)

// The failed transactions the lookup is asked for, each named by what it
// proves. Their records are built from the chain's own error values, passed
// through the SDK's ABCIInfo as the node does, so the codes, codespaces and
// logs are what a block's results carry.
var (
	ftxFinal  = ftxHash("final")  // [MsgSend, MsgDepositToEscrow], the deposit failed: final, a fee paid
	ftxOpen   = ftxHash("open")   // a settlement stopped before it ran: not final, nothing taken
	ftxExec   = ftxHash("exec")   // MsgExec[a host registration, a send], no grant: final, a zero fee
	ftxGas    = ftxHash("gas")    // out of gas: names no message
	ftxLater  = ftxHash("later")  // at app version 11, which no reason is pinned to
	ftxPanic  = ftxHash("panic")  // a panic, its stack cut
	ftxTwice  = ftxHash("twice")  // failed before the ante, then again after it: the newer wins
	ftxBroken = ftxHash("broken") // a row whose record does not decode
)

// ftxSettled is the fixture's settlement, f1, whose transaction also failed
// once before it settled.
var ftxSettled = txOf("f1")

// ftxAt is long past, so no answer the fixture gives moves with the clock.
var ftxAt = time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)

// ftxHash is a transaction hash: 64 hex characters, in lower case as the
// scanner writes it.
func ftxHash(name string) string {
	h := sha256.Sum256([]byte("failed tx " + name))
	return hex.EncodeToString(h[:])
}

// ftxAtMessage is err as runMsgs returns it for the message at index i. The
// format string is cosmos-sdk baseapp/baseapp.go:927 (runMsgs, unexported).
func ftxAtMessage(err error, i int) error {
	return errorsmod.Wrapf(err, "failed to execute message; message index: %d", i)
}

// ftxRecord is the line the scanner writes for a transaction that failed
// with err in block height at txIndex: the code, codespace and log the SDK
// gives err, the log cut as the scanner stores it. fee is the ante's fee
// event, "" for none or for a zero fee.
func ftxRecord(hash string, height int64, txIndex int, err error, ante bool, fee string, msgs ...failedtx.Msg) failedtx.Record {
	res := sdkerrors.ResponseExecTxResultWithEvents(err, 200000, 91234, nil, false)
	log, cut := failedtx.CutLog(res.Codespace, res.Code, res.Log)
	at := ftxAt.Add(time.Duration(height) * time.Second)
	return failedtx.Record{SchemaVersion: failedtx.SchemaVersion, DedupeKey: failedtx.Key(height, txIndex), Height: height, Time: at,
		AppVersion: 10, TxHash: hash, TxIndex: txIndex, Code: res.Code, Codespace: res.Codespace, Log: log, LogCut: cut,
		GasWanted: res.GasWanted, GasUsed: res.GasUsed, AntePassed: ante, Fee: fee, Messages: msgs, RecordedAt: at.Add(2 * time.Second)}
}

const (
	ftxURLSend    = "/cosmos.bank.v1beta1.MsgSend"
	ftxURLDeposit = "/celestia.fibre.v1.MsgDepositToEscrow"
	ftxURLPFF     = "/celestia.fibre.v1.MsgPayForFibre"
	ftxURLSetHost = "/celestia.valaddr.v1.MsgSetFibreProviderInfo"
)

// ftxRecords are the fixture's failed transactions, in the order the
// scanner wrote them. Each Fibre message carries what it asked for (signer
// and detail), which the answer must not publish.
func ftxRecords() []failedtx.Record {
	deposit := failedtx.Msg{Index: 1, TypeURL: ftxURLDeposit, Signer: samplePublisher,
		Detail: &failedtx.MsgDetail{Publisher: samplePublisher, Amount: "1000000utia"}}
	pff := failedtx.Msg{Index: 0, TypeURL: ftxURLPFF, Signer: samplePublisher,
		Detail: &failedtx.MsgDetail{Publisher: samplePublisher, PromiseHash: "f1", Namespace: fixtureNS, BlobSize: 262144}}
	host := failedtx.Msg{Index: 0, TypeURL: ftxURLSetHost, Signer: "celestiavaloper1v",
		Detail: &failedtx.MsgDetail{Host: "fibre.example.org:7980"}}
	send := failedtx.Msg{Index: 0, TypeURL: ftxURLSend}
	// x/fibre keeper msg_server.go:53, over the bank's spendable-balance error.
	short := errorsmod.Wrapf(errorsmod.Wrapf(sdkerrors.ErrInsufficientFunds, "spendable balance %s is smaller than %s", "10utia", "1000000utia"),
		"failed to transfer funds to escrow")
	// a message's ValidateBasic, run before the ante handler: no index
	invalid := errorsmod.Wrap(sdkerrors.ErrInvalidRequest, "payment promise height must be positive")
	// baseapp/recovery.go:58-63 and 72-77
	outOfGas := errorsmod.Wrap(sdkerrors.ErrOutOfGas, fmt.Sprintf("out of gas in location: %v; gasWanted: %d, gasUsed: %d", "WritePerByte", 200000, 200133))
	panicked := errorsmod.Wrap(sdkerrors.ErrPanic, fmt.Sprintf("recovered: %v\nstack:\n%v", "invalid coin denominations", "goroutine 1 [running]:\nmain.main()"))

	alone := deposit
	alone.Index = 0
	exec := failedtx.Msg{Index: 0, TypeURL: failedtx.ExecTypeURL, Inner: []failedtx.Msg{host, {Index: 1, TypeURL: ftxURLSend}}}
	later := ftxRecord(ftxLater, 124, 0, ftxAtMessage(short, 0), true, "2000utia", alone)
	later.AppVersion = 11
	return []failedtx.Record{
		ftxRecord(ftxFinal, 120, 3, ftxAtMessage(short, 1), true, "2000utia", send, deposit),
		ftxRecord(ftxOpen, 121, 0, invalid, false, "", pff),
		ftxRecord(ftxExec, 122, 1, ftxAtMessage(authz.ErrNoAuthorizationFound, 0), true, "", exec),
		ftxRecord(ftxGas, 123, 0, outOfGas, true, "2000utia", pff),
		later,
		ftxRecord(ftxPanic, 125, 0, panicked, true, "2000utia", pff),
		// f1's transaction, failed once before it settled at 200
		ftxRecord(ftxSettled, 150, 0, invalid, false, "", pff),
		ftxRecord(ftxTwice, 160, 2, invalid, false, "", alone),
		ftxRecord(ftxTwice, 170, 0, ftxAtMessage(short, 0), true, "3000utia", alone),
	}
}

// ftxFixture is a store with one settlement, f1, in fixtureNS, and, with
// failures, the failed transactions of ftxRecords, ingested from
// failed_txs.jsonl as the collector does, beside a row whose record does
// not decode.
func ftxFixture(t *testing.T, failures bool) (*httptest.Server, *api.Server) {
	t.Helper()
	dir := t.TempDir()
	st, err := store.Open(filepath.Join(dir, "observer.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	settled := ftxAt.Add(200 * time.Second)
	pub := scan.Publication{
		SchemaVersion: scan.AttestationSchemaVersion, PromiseHash: "f1", SettlementHeight: 200, SettlementTime: settled,
		SettlementTxHash: ftxSettled, MustServeUntil: settled.Add(time.Hour), RecordedAt: settled, Signer: samplePublisher,
		Promise: scan.PromiseFields{ChainID: "t", Height: 199, Commitment: strings.Repeat("f1", 32), CreationTimestamp: settled, BlobSize: 262144,
			// observer/testdata's publisher key: samplePublisher
			SignerPublicKey: "0356684b7c1265cc78d8c8745fa9e145f36ab9852a85ed90d0eeed8240df01e53f", Namespace: fixtureNS},
		Assignment: scan.AssignmentTable{
			ProtocolParams:     scan.ProtocolParamsSnapshot{OriginalRows: 4096, TotalRows: 16384},
			ValidatorSetHeight: 199, TotalVotingPower: 10, Sigma: 148, Distinct: 148, ValidatorsWithRows: 1,
			Validators: []scan.ValidatorAssignment{{Address: sampleValidator, VotingPower: 10, RowCount: 148}},
		},
	}
	raw, err := json.Marshal(pub)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.UpsertPublication(pub, raw); err != nil {
		t.Fatal(err)
	}
	if failures {
		var lines []byte
		for _, r := range ftxRecords() {
			b, err := json.Marshal(r)
			if err != nil {
				t.Fatal(err)
			}
			lines = append(append(lines, b...), '\n')
		}
		path := filepath.Join(dir, failedtx.FileName)
		if err := os.WriteFile(path, lines, 0o644); err != nil {
			t.Fatal(err)
		}
		if r, err := ingest.FailedTxs(st, path, ftxAt.Add(time.Hour)); err != nil || r.Inserted != int64(len(ftxRecords())) {
			t.Fatalf("ingest failed txs: inserted=%d err=%v", r.Inserted, err)
		}
		// A row a newer build wrote, say, that this one cannot read.
		if _, err := st.DB().Exec(`INSERT INTO failed_txs (dedupe_key, tx_hash, height, tx_index, time, code, codespace, raw_json)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?)`, failedtx.Key(180, 0), ftxBroken, 180, 0, store.TS(ftxAt.Add(180*time.Second)), 5, "sdk",
			`{"schema_version":1,"tx_hash":`); err != nil {
			t.Fatal(err)
		}
	}
	srv := api.NewWithVantage(st, api.VantageInfo{Name: "test"}, nil)
	t.Cleanup(srv.Close)
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)
	return ts, srv
}

// ftxAnswer is one asking of a route: its status, Cache-Control, body and
// the body's top-level keys, raw.
type ftxAnswer struct {
	status int
	cache  string
	body   []byte
	keys   map[string]json.RawMessage
}

func ftxAsk(t *testing.T, ts *httptest.Server, path string) ftxAnswer {
	t.Helper()
	resp, err := http.Get(ts.URL + path)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	a := ftxAnswer{status: resp.StatusCode, cache: resp.Header.Get("Cache-Control"), body: body}
	if err := json.Unmarshal(body, &a.keys); err != nil {
		t.Fatalf("%s: %v: %s", path, err, body)
	}
	return a
}

// failed is the answer's failed_tx, decoded, and its keys; nil when absent.
func (a ftxAnswer) failed(t *testing.T) (*ftxFailed, []string) {
	t.Helper()
	raw, ok := a.keys["failed_tx"]
	if !ok {
		return nil, nil
	}
	var f ftxFailed
	if err := json.Unmarshal(raw, &f); err != nil {
		t.Fatal(err)
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	return &f, sortedKeys(m)
}

type ftxMsg struct {
	Index   int      `json:"index"`
	TypeURL string   `json:"type_url"`
	Fibre   bool     `json:"fibre"`
	Inner   []ftxMsg `json:"inner"`
}

type ftxFailed struct {
	Height         int64    `json:"height"`
	Time           string   `json:"time"`
	Code           uint32   `json:"code"`
	Codespace      string   `json:"codespace"`
	Reason         *string  `json:"reason"`
	FailedMsgIndex *int     `json:"failed_msg_index"`
	Messages       []ftxMsg `json:"messages"`
	GasWanted      int64    `json:"gas_wanted"`
	GasUsed        int64    `json:"gas_used"`
	AntePassed     bool     `json:"ante_passed"`
	Fee            *string  `json:"fee"`
	Log            string   `json:"log"`
	LogCut         *bool    `json:"log_cut"`
}

func sortedKeys(m map[string]json.RawMessage) []string {
	var ks []string
	for k := range m {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	return ks
}

// ftxNewest is the newest record of hash in ftxRecords.
func ftxNewest(t *testing.T, hash string) failedtx.Record {
	t.Helper()
	var out failedtx.Record
	for _, r := range ftxRecords() {
		if r.TxHash == hash && r.Height > out.Height {
			out = r
		}
	}
	if out.TxHash == "" {
		t.Fatalf("no record of %s", hash)
	}
	return out
}

// A lookup by the hash of a Fibre transaction that failed in a block, and
// that no publication carries, answers no blob and failed_tx: the block,
// the chain's code and error, the reason and the failing message read from
// them, the messages, the gas and whether the fee was taken. Only those: the
// answer has the list's own keys and failed_tx, and failed_tx none but
// these, so what the record keeps beside them (each message's signer and
// what it asked for) is not published.
func TestAFailedTransactionAnswersWhatTheChainReturned(t *testing.T) {
	ts, _ := ftxFixture(t, true)
	rec := ftxNewest(t, ftxFinal)
	a := ftxAsk(t, ts, "/v1/blobs?tx="+strings.ToUpper(ftxFinal))
	if a.status != 200 {
		t.Fatalf("status %d: %s", a.status, a.body)
	}
	if got, want := strings.Join(sortedKeys(a.keys), ","), "blobs,failed_tx,limit,namespace,offset,total,truncated,tx"; got != want {
		t.Fatalf("answer keys %s, want %s", got, want)
	}
	if string(a.keys["blobs"]) != "[]" || string(a.keys["total"]) != "0" || string(a.keys["tx"]) != `"`+ftxFinal+`"` {
		t.Fatalf("blobs %s, total %s, tx %s", a.keys["blobs"], a.keys["total"], a.keys["tx"])
	}
	f, keys := a.failed(t)
	if f == nil {
		t.Fatalf("no failed_tx: %s", a.body)
	}
	if got, want := strings.Join(keys, ","), "ante_passed,code,codespace,failed_msg_index,fee,gas_used,gas_wanted,height,log,messages,reason,time"; got != want {
		t.Fatalf("failed_tx keys %s, want %s", got, want)
	}
	wantLog := "failed to execute message; message index: 1: failed to transfer funds to escrow: spendable balance 10utia is smaller than 1000000utia: insufficient funds"
	if f.Height != 120 || f.Time != rec.Time.Format(time.RFC3339Nano) || f.Code != 5 || f.Codespace != "sdk" ||
		f.Reason == nil || *f.Reason != "Insufficient funds" || f.FailedMsgIndex == nil || *f.FailedMsgIndex != 1 ||
		f.GasWanted != 200000 || f.GasUsed != 91234 || !f.AntePassed || f.Fee == nil || *f.Fee != "2000utia" || f.Log != wantLog || f.LogCut != nil {
		t.Fatalf("failed_tx: %s", a.keys["failed_tx"])
	}
	msgs, _ := json.Marshal(f.Messages)
	if want := `[{"index":0,"type_url":"/cosmos.bank.v1beta1.MsgSend","fibre":false,"inner":null},` +
		`{"index":1,"type_url":"/celestia.fibre.v1.MsgDepositToEscrow","fibre":true,"inner":null}]`; string(msgs) != want {
		t.Fatalf("messages %s, want %s", msgs, want)
	}
	for _, k := range []string{`"signer"`, `"detail"`, `"amount"`, `"publisher"`, `"app_version"`, `"dedupe_key"`, `"recorded_at"`} {
		if bytes.Contains(a.keys["failed_tx"], []byte(k)) {
			t.Errorf("failed_tx publishes %s: %s", k, a.keys["failed_tx"])
		}
	}
}

// The reason and the failing message are failedtx.Explain's reading of the
// record, and absent where it has none: an error whose log names no message
// (before the ante, out of gas, a panic), and every reading of a record from
// an app version the reasons are not pinned to.
func TestAFailedTransactionsReasonAndMessageAreExplainsReading(t *testing.T) {
	ts, _ := ftxFixture(t, true)
	for _, c := range []struct {
		name, hash, reason string
		index              int
	}{
		{"a deposit, the second message", ftxFinal, "Insufficient funds", 1},
		{"stopped before the ante", ftxOpen, "Invalid request", -1},
		{"a delegated registration: the outer message", ftxExec, "Authorization not found", 0},
		{"out of gas", ftxGas, "Out of gas", -1},
		{"app version 11", ftxLater, "", -1},
		{"a panic", ftxPanic, "Execution panicked", -1},
	} {
		t.Run(c.name, func(t *testing.T) {
			e := failedtx.Explain(ftxNewest(t, c.hash))
			if e.Reason != c.reason || e.MsgIndex != c.index {
				t.Fatalf("the fixture reads %+v, want {%q %d}", e, c.reason, c.index)
			}
			a := ftxAsk(t, ts, "/v1/blobs?tx="+c.hash+"&limit=25")
			f, keys := a.failed(t)
			if f == nil {
				t.Fatalf("no failed_tx: %s", a.body)
			}
			has := map[string]bool{}
			for _, k := range keys {
				has[k] = true
			}
			if c.reason == "" {
				if has["reason"] {
					t.Errorf("a reason where Explain has none: %s", a.keys["failed_tx"])
				}
			} else if f.Reason == nil || *f.Reason != c.reason {
				t.Errorf("reason %v, want %q", f.Reason, c.reason)
			}
			if c.index < 0 {
				if has["failed_msg_index"] {
					t.Errorf("a failing message where the log names none: %s", a.keys["failed_tx"])
				}
			} else if f.FailedMsgIndex == nil || *f.FailedMsgIndex != c.index {
				t.Errorf("failed_msg_index %v, want %d", f.FailedMsgIndex, c.index)
			}
		})
	}
	// the panic's stack is not kept, and the answer says it was cut
	a := ftxAsk(t, ts, "/v1/blobs?tx="+ftxPanic)
	f, _ := a.failed(t)
	if f == nil || f.LogCut == nil || !*f.LogCut || f.Log != "recovered: invalid coin denominations" {
		t.Fatalf("a panic: %s", a.keys["failed_tx"])
	}
}

// Each message says whether it is one of the five Fibre messages; a
// MsgExec lists its own messages, one level in, each with the same flag,
// and is not Fibre itself.
func TestAFailedTransactionsMessagesSayWhichAreFibre(t *testing.T) {
	ts, _ := ftxFixture(t, true)
	a := ftxAsk(t, ts, "/v1/blobs?tx="+ftxExec)
	f, _ := a.failed(t)
	if f == nil {
		t.Fatalf("no failed_tx: %s", a.body)
	}
	msgs, _ := json.Marshal(f.Messages)
	want := `[{"index":0,"type_url":"/cosmos.authz.v1beta1.MsgExec","fibre":false,"inner":[` +
		`{"index":0,"type_url":"/celestia.valaddr.v1.MsgSetFibreProviderInfo","fibre":true,"inner":null},` +
		`{"index":1,"type_url":"/cosmos.bank.v1beta1.MsgSend","fibre":false,"inner":null}]}]`
	if string(msgs) != want {
		t.Fatalf("messages %s, want %s", msgs, want)
	}
	// a message with nothing inside it has no inner key at all
	if bytes.Contains(ftxAsk(t, ts, "/v1/blobs?tx="+ftxFinal).keys["failed_tx"], []byte(`"inner"`)) {
		t.Error("an inner key on messages that hold none")
	}
}

// ante_passed says whether the fee and the sequence were taken, and fee is
// what the chain printed for it, absent for a zero fee and whenever the
// ante did not pass. Only a final answer is cached.
func TestAFailedTransactionsFeeAndFinality(t *testing.T) {
	ts, _ := ftxFixture(t, true)
	for _, c := range []struct {
		name, hash string
		ante       bool
		fee, cache string
	}{
		{"a fee paid", ftxFinal, true, "2000utia", "public, max-age=15"},
		{"a zero fee", ftxExec, true, "", "public, max-age=15"},
		{"nothing taken", ftxOpen, false, "", "no-store"},
	} {
		a := ftxAsk(t, ts, "/v1/blobs?tx="+c.hash)
		f, keys := a.failed(t)
		if f == nil {
			t.Fatalf("%s: no failed_tx: %s", c.name, a.body)
		}
		hasFee := false
		for _, k := range keys {
			hasFee = hasFee || k == "fee"
		}
		if f.AntePassed != c.ante || hasFee != (c.fee != "") || (f.Fee != nil && *f.Fee != c.fee) || a.cache != c.cache {
			t.Errorf("%s: ante_passed %v, fee %v, Cache-Control %q; want %v %q %q", c.name, f.AntePassed, f.Fee, a.cache, c.ante, c.fee, c.cache)
		}
	}
}

// A transaction that failed more than once (stopped before the ante, so
// included again) answers its newest inclusion.
func TestTheNewestFailedInclusionWins(t *testing.T) {
	ts, _ := ftxFixture(t, true)
	a := ftxAsk(t, ts, "/v1/blobs?tx="+ftxTwice)
	f, _ := a.failed(t)
	if f == nil || f.Height != 170 || !f.AntePassed || f.Code != 5 || f.Fee == nil || *f.Fee != "3000utia" || a.cache != "public, max-age=15" {
		t.Fatalf("failed twice: %q %s", a.cache, a.body)
	}
}

// A settlement under the hash wins over a failure under it: the answer is
// the one a store without failures gives, byte for byte, headers included.
func TestASettlementUnderTheSameHashWinsOverItsFailure(t *testing.T) {
	with, _ := ftxFixture(t, true)
	without, _ := ftxFixture(t, false)
	for _, path := range []string{"/v1/blobs?tx=" + ftxSettled, "/v1/blobs?tx=" + ftxSettled + "&limit=25", "/v1/blobs?tx=0x" + strings.ToUpper(ftxSettled) + "&limit=25&offset=0"} {
		a, b := ftxAsk(t, with, path), ftxAsk(t, without, path)
		if _, ok := a.keys["failed_tx"]; ok {
			t.Errorf("%s: failed_tx beside its settlement: %s", path, a.body)
		}
		if a.status != 200 || string(a.keys["total"]) != "1" || a.status != b.status || a.cache != b.cache || !bytes.Equal(a.body, b.body) {
			t.Errorf("%s: with failures %d %q %s\nwithout %d %q %s", path, a.status, a.cache, a.body, b.status, b.cache, b.body)
		}
	}
	// and with no failures on record, a hash a failure would have answered is a miss
	if a := ftxAsk(t, without, "/v1/blobs?tx="+ftxFinal); a.cache != "no-store" || a.keys["failed_tx"] != nil {
		t.Errorf("no failures on record: %q %s", a.cache, a.body)
	}
}

// A failed row whose record does not decode is that row's fault: the
// lookup answers as if there were none (no failed_tx, not cached), never a
// 500, and the row is counted once among the undecodable rows /v1/health
// reports.
func TestAFailedRowThatDoesNotDecodeIsNoAnswer(t *testing.T) {
	ts, srv := ftxFixture(t, true)
	if n, _, _ := srv.UndecodableRows(); n != 0 {
		t.Fatalf("%d undecodable rows before the lookup", n)
	}
	for i := 0; i < 2; i++ {
		a := ftxAsk(t, ts, "/v1/blobs?tx="+ftxBroken)
		if a.status != 200 || a.cache != "no-store" || a.keys["failed_tx"] != nil || string(a.keys["total"]) != "0" {
			t.Fatalf("an undecodable row: %d %q %s", a.status, a.cache, a.body)
		}
	}
	if n, last, _ := srv.UndecodableRows(); n != 1 || !strings.Contains(last, "failed transaction of "+ftxBroken) {
		t.Fatalf("undecodable rows %d, last %q; want 1, naming the failed transaction", n, last)
	}
}
