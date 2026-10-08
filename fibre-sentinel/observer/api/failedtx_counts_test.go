package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	errorsmod "cosmossdk.io/errors"
	valaddrtypes "github.com/celestiaorg/celestia-app/v10/x/valaddr/types"
	sdkerrors "github.com/cosmos/cosmos-sdk/types/errors"
	"github.com/cosmos/cosmos-sdk/x/authz"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/failedtx"
	"github.com/plsgiveup/fibre/fibre-sentinel/internal/scan"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/collect"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/correct"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
)

// The proof that a failed transaction is in no figure and changes no
// answer but its own lookup. Two data dirs hold the same record, read by
// the real collector the way it runs: observer/testdata's publications and
// readings, with the escrow movements and host registrations of its
// publisher and validators. The second also holds failed_txs.jsonl, whose
// failures carry the record's own identifiers: the settled transaction's
// hash, the settled promise and namespace, the deposit's hash and height, a
// host event's height and index. Every route the golden harness asks over a
// record answers the same from both, and every table a figure is counted
// from holds the same rows.

// observer/testdata's record, which the failures collide with: its first
// publication (promise, settlement, namespace), its publisher, and two of
// its validators with the hosts its readings reached them at.
const (
	ctPromise   = "e1bc7a4c4d4f857dd805f20d6fd48b50f409e180aa8e001e1f9f869fc8431d61"
	ctSettledTx = "09215b92e74f21c2b177bd2a8d8f3535522f41fe5765b82306f0b438bb2c0af2"
	ctNamespace = "00000000000000000000000000000000000000000000000000534e5400"
	ctPublisher = "celestia1d3mmg652pxj776dyqwlsrc93y64088g6ux8deq"
	ctValidator = "7730f065f885965a04e6c6f41ef9a29d547f1c99" // at 127.0.0.1:7980
	ctOther     = "577b1362af2946edb3a1c3b4c7bc3e2e9889ffe0" // at 127.0.0.1:7983
	// ctValoper is the operator address a host registration names.
	ctValoper = "celestiavaloper1yg3zyg3zyg3zyg3zyg3zyg3zyg3zyg3z64pdkn"
)

// The transactions the escrow movements and the failures are under.
var (
	ctDepositTx  = strings.Repeat("d0", 32) // a deposit that went through; a failure is recorded under its hash and height too
	ctWithdrawTx = strings.Repeat("d1", 32)
	ctTimeoutTx  = strings.Repeat("d2", 32)
	ctPFFTx      = strings.Repeat("f1", 32) // a settlement of the settled promise, failed
	ctHostTx     = strings.Repeat("f2", 32)
	ctExecTx     = strings.Repeat("f3", 32)
	ctPanicTx    = strings.Repeat("f4", 32)
)

// ctBlockTime is the time of block h in observer/testdata's devnet, about
// 1.6 s a block (block 32 is at 17:00:27).
func ctBlockTime(h int64) time.Time {
	return time.Date(2026, 9, 6, 16, 59, 36, 0, time.UTC).Add(time.Duration(h) * 1600 * time.Millisecond)
}

// ctFailure is the line the scanner writes for a transaction that failed
// with err in block height at txIndex: the code, codespace and log the SDK
// gives err (ABCIInfo, as the node does), the log cut as stored.
func ctFailure(hash string, height int64, txIndex int, err error, ante bool, fee string, msgs ...failedtx.Msg) failedtx.Record {
	res := sdkerrors.ResponseExecTxResultWithEvents(err, 200000, 91234, nil, false)
	log, cut := failedtx.CutLog(res.Codespace, res.Code, res.Log)
	at := ctBlockTime(height)
	return failedtx.Record{SchemaVersion: failedtx.SchemaVersion, DedupeKey: failedtx.Key(height, txIndex), Height: height, Time: at,
		AppVersion: 10, TxHash: hash, TxIndex: txIndex, Code: res.Code, Codespace: res.Codespace, Log: log, LogCut: cut,
		GasWanted: res.GasWanted, GasUsed: res.GasUsed, AntePassed: ante, Fee: fee, Messages: msgs, RecordedAt: at.Add(2 * time.Second)}
}

// ctAtMessage is err as runMsgs returns it for the message at index i
// (cosmos-sdk baseapp/baseapp.go:927).
func ctAtMessage(err error, i int) error {
	return errorsmod.Wrapf(err, "failed to execute message; message index: %d", i)
}

// ctFailures are the failed transactions, each colliding with what the
// record holds where it can.
func ctFailures() []any {
	pff := failedtx.Msg{Index: 0, TypeURL: "/celestia.fibre.v1.MsgPayForFibre", Signer: ctPublisher,
		Detail: &failedtx.MsgDetail{Publisher: ctPublisher, PromiseHash: ctPromise, Namespace: ctNamespace, BlobSize: 262144}}
	send := failedtx.Msg{Index: 0, TypeURL: "/cosmos.bank.v1beta1.MsgSend"}
	deposit := failedtx.Msg{Index: 1, TypeURL: "/celestia.fibre.v1.MsgDepositToEscrow", Signer: ctPublisher,
		Detail: &failedtx.MsgDetail{Publisher: ctPublisher, Amount: "50000000utia"}}
	host := failedtx.Msg{Index: 0, TypeURL: "/celestia.valaddr.v1.MsgSetFibreProviderInfo", Signer: ctValoper,
		Detail: &failedtx.MsgDetail{Host: "127.0.0.1:7990"}}
	exec := failedtx.Msg{Index: 0, TypeURL: failedtx.ExecTypeURL, Inner: []failedtx.Msg{host}}
	invalid := errorsmod.Wrap(sdkerrors.ErrInvalidRequest, "payment promise height must be positive")
	short := errorsmod.Wrapf(errorsmod.Wrapf(sdkerrors.ErrInsufficientFunds, "spendable balance %s is smaller than %s", "10utia", "50000000utia"),
		"failed to transfer funds to escrow")
	panicked := errorsmod.Wrap(sdkerrors.ErrPanic, fmt.Sprintf("recovered: %v\nstack:\n%v", "invalid coin denominations", "goroutine 1 [running]:\nmain.main()"))
	return []any{
		// the settled transaction's hash, at a lower height, stopped before the ante
		ctFailure(ctSettledTx, 30, 0, invalid, false, "", pff),
		// a settlement of the same promise in the same namespace, failed
		ctFailure(ctPFFTx, 31, 2, ctAtMessage(invalid, 0), true, "2000utia", pff),
		// under the successful deposit's hash, height and index
		ctFailure(ctDepositTx, 20, 1, ctAtMessage(short, 1), true, "2000utia", send, deposit),
		// at a host event's height and index
		ctFailure(ctHostTx, 25, 0, ctAtMessage(valaddrtypes.ErrInvalidValidator.Wrapf("validator not found: %v", ctValoper), 0), true, "2000utia", host),
		// a delegated host registration with no grant
		ctFailure(ctExecTx, 26, 0, ctAtMessage(authz.ErrNoAuthorizationFound, 0), true, "2000utia", exec),
		ctFailure(ctPanicTx, 41, 0, panicked, true, "2000utia", pff),
	}
}

// ctFresh are the failures' hashes no publication carries, by the height
// each failed at.
var ctFresh = map[string]int64{ctPFFTx: 31, ctDepositTx: 20, ctHostTx: 25, ctExecTx: 26, ctPanicTx: 41}

func ctWriteLines(t *testing.T, path string, vs ...any) {
	t.Helper()
	var b []byte
	for _, v := range vs {
		j, err := json.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		b = append(append(b, j...), '\n')
	}
	if err := os.WriteFile(path, b, 0o644); err != nil {
		t.Fatal(err)
	}
}

// ctDataDir is a data dir with observer/testdata's record, the same escrow
// movements (a deposit, a withdrawal request, the first publication's
// settlement charge and a timeout) and host registrations (a seed and an
// event for each of two validators), and, with failures, failed_txs.jsonl.
func ctDataDir(t *testing.T, failures bool) string {
	t.Helper()
	dir := t.TempDir()
	for _, name := range []string{"publications.jsonl", "measurements.jsonl", "state.json"} {
		b, err := os.ReadFile(filepath.Join("..", "testdata", name))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, name), b, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	available := ctBlockTime(45).Add(12*time.Hour + 10*time.Minute)
	settled := time.Date(2026, 9, 6, 17, 0, 27, 494519078, time.UTC)
	ctWriteLines(t, filepath.Join(dir, "payments.jsonl"),
		scan.Payment{SchemaVersion: 1, DedupeKey: ctDepositTx + ":0", Kind: scan.PaymentDeposit, Height: 20, Time: ctBlockTime(20),
			TxHash: ctDepositTx, TxIndex: 1, Publisher: ctPublisher, Denom: "utia", AmountUtia: 50_000_000, RecordedAt: ctBlockTime(20).Add(time.Second)},
		scan.Payment{SchemaVersion: 1, DedupeKey: ctSettledTx + ":0", Kind: scan.PaymentSettlement, Height: 32, Time: settled,
			TxHash: ctSettledTx, Publisher: ctPublisher, Processor: ctPublisher, PromiseHash: ctPromise, Namespace: ctNamespace, BlobSize: 262144,
			GasUnits: 695_000, Denom: "utia", AmountUtia: 695_000, RecordedAt: settled.Add(time.Second)},
		scan.Payment{SchemaVersion: 1, DedupeKey: ctWithdrawTx + ":0", Kind: scan.PaymentWithdrawalRequest, Height: 45, Time: ctBlockTime(45),
			TxHash: ctWithdrawTx, Publisher: ctPublisher, Denom: "utia", AmountUtia: 1_000_000, AvailableAt: &available, RecordedAt: ctBlockTime(45).Add(time.Second)},
		scan.Payment{SchemaVersion: 1, DedupeKey: ctTimeoutTx + ":0", Kind: scan.PaymentTimeout, Height: 60, Time: ctBlockTime(60),
			TxHash: ctTimeoutTx, Publisher: ctPublisher, Processor: ctPublisher, PromiseHash: strings.Repeat("7e", 32), Namespace: ctNamespace,
			BlobSize: 262144, GasUnits: 695_000, Denom: "utia", AmountUtia: 695_000, RecordedAt: ctBlockTime(60).Add(time.Second)})
	ctWriteLines(t, filepath.Join(dir, "host_history.jsonl"),
		scan.HostEvent{HostEntry: scan.HostEntry{FromHeight: 1, FromTxIndex: -1, ConsAddress: ctValidator, Host: "127.0.0.1:7980", Source: "seed"}, Time: ctBlockTime(1)},
		scan.HostEvent{HostEntry: scan.HostEntry{FromHeight: 1, FromTxIndex: -1, ConsAddress: ctOther, Host: "127.0.0.1:7983", Source: "seed"}, Time: ctBlockTime(1)},
		scan.HostEvent{HostEntry: scan.HostEntry{FromHeight: 25, FromTxIndex: 0, ConsAddress: ctValidator, Host: "127.0.0.1:7980", Source: "event"}, Time: ctBlockTime(25)},
		scan.HostEvent{HostEntry: scan.HostEntry{FromHeight: 27, FromTxIndex: 1, ConsAddress: ctOther, Host: "127.0.0.1:7983", Source: "event"}, Time: ctBlockTime(27)})
	if failures {
		ctWriteLines(t, filepath.Join(dir, failedtx.FileName), ctFailures()...)
	}
	return dir
}

// ctCollect is the store the collector builds from dir: two passes, as it
// runs them every interval, under one clock. Any ingest error fails.
func ctCollect(t *testing.T, dir string, now time.Time) *store.Store {
	t.Helper()
	st, err := store.Open(filepath.Join(dir, "observer.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	amend, err := os.OpenFile(filepath.Join(dir, "amendments.jsonl"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { amend.Close() })
	corr, err := os.OpenFile(filepath.Join(dir, "corrections.jsonl"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { corr.Close() })
	c := collect.New(collect.Collector{St: st, Paths: collect.DefaultPaths(dir), Vantage: "ut-1", AmendFile: amend,
		PruneTolerance: 2 * time.Minute, Corrector: correct.New(st, corr, 2*time.Minute)})
	for i := 0; i < 2; i++ {
		if errs := c.Pass(context.Background(), now); len(errs) > 0 {
			t.Fatalf("pass %d over %s: %v", i+1, dir, errs)
		}
	}
	return st
}

// ctServer is the API over st as the golden harness builds it: its clock
// fixed before anything starts, and the readings memo built to the end.
func ctServer(t *testing.T, st *store.Store, now time.Time) *Server {
	t.Helper()
	srv := NewWithVantage(st, VantageInfo{Name: "ut-1"}, nil, withClock(func() time.Time { return now }))
	t.Cleanup(srv.Close)
	if err := srv.readings.update(context.Background(), srv, 0); err != nil {
		t.Fatalf("readings: %v", err)
	}
	return srv
}

// ctAsk is goldenAsk keeping the whole answer, headers included: asked
// until it is neither being computed nor rationed, for at most two minutes.
func ctAsk(srv *Server, r string) *http.Response {
	deadline := time.Now().Add(2 * time.Minute)
	for {
		rec := httptest.NewRecorder()
		srv.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, r, nil))
		busy := rec.Code == http.StatusServiceUnavailable || rec.Code == http.StatusTooManyRequests ||
			(rec.Code == http.StatusOK && strings.HasPrefix(r, "/v1/publishers") && bytes.Contains(rec.Body.Bytes(), []byte(`"readings":null`)))
		if !busy || time.Now().After(deadline) {
			return rec.Result()
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// ctComputeMs is the one part of an answer two computations of the same
// figures may differ in, as golden-compare allows: how long it took.
var ctComputeMs = regexp.MustCompile(`"compute_ms":\d+`)

// ctWhole is an answer as compared: its status, every header, and its body
// with the computation's duration zeroed.
func ctWhole(t *testing.T, resp *http.Response) string {
	t.Helper()
	var b strings.Builder
	fmt.Fprintf(&b, "status %d\n", resp.StatusCode)
	var names []string
	for k := range resp.Header {
		names = append(names, k)
	}
	sort.Strings(names)
	for _, k := range names {
		fmt.Fprintf(&b, "%s: %s\n", k, strings.Join(resp.Header[k], ", "))
	}
	body := new(bytes.Buffer)
	if _, err := body.ReadFrom(resp.Body); err != nil {
		t.Fatal(err)
	}
	b.WriteString("\n")
	b.Write(ctComputeMs.ReplaceAll(body.Bytes(), []byte(`"compute_ms":0`)))
	return b.String()
}

func ctCount(t *testing.T, st *store.Store, table string) int64 {
	t.Helper()
	var n int64
	if err := st.DB().QueryRow(`SELECT COUNT(*) FROM ` + table).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// Failed transactions enter no count and change no answer but the lookup
// of their own hash: with failures colliding with the record's own
// identifiers, every route the golden harness asks answers byte for byte
// what it answers without them, status and headers included; the counts
// and the tables the figures are read from are the same and none is empty;
// and only the hashes of the failures, asked alone, answer failed_tx.
func TestFailedTransactionsEnterNoCountAndNoOtherAnswer(t *testing.T) {
	now := time.Date(2026, 9, 7, 0, 0, 0, 0, time.UTC)
	stA, stB := ctCollect(t, ctDataDir(t, false), now), ctCollect(t, ctDataDir(t, true), now)
	if n := ctCount(t, stB, "failed_txs"); n != int64(len(ctFailures())) {
		t.Fatalf("%d failed rows with the file, want %d", n, len(ctFailures()))
	}
	if n := ctCount(t, stA, "failed_txs"); n != 0 {
		t.Fatalf("%d failed rows without the file", n)
	}

	// The counts, and the rows every figure is counted from.
	ctx := context.Background()
	ca, err := stA.Count(ctx)
	if err != nil {
		t.Fatal(err)
	}
	cb, err := stB.Count(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if ca != cb {
		t.Errorf("Count: %+v without failures, %+v with", ca, cb)
	}
	for _, table := range []string{"publications", "assignments", "payments", "host_events", "probes"} {
		if na, nb := ctCount(t, stA, table), ctCount(t, stB, table); na != nb || na == 0 {
			t.Errorf("%s: %d rows without failures, %d with", table, na, nb)
		}
	}

	srvA, srvB := ctServer(t, stA, now), ctServer(t, stB, now)
	reqs := goldenRequests(t, stA, now)
	if other := goldenRequests(t, stB, now); strings.Join(other, "\n") != strings.Join(reqs, "\n") {
		t.Fatalf("the two stores give different requests:\n%s\n---\n%s", strings.Join(reqs, "\n"), strings.Join(other, "\n"))
	}
	asked := map[string]bool{}
	for _, r := range reqs {
		asked[r] = true
	}
	// beside them: the settled transaction as the site asks it, and the
	// hosts, which read the host events
	for _, r := range []string{"/v1/blobs?tx=" + ctSettledTx, "/v1/blobs?tx=" + ctSettledTx + "&limit=25", "/v1/hosting"} {
		if !asked[r] {
			reqs, asked[r] = append(reqs, r), true
		}
	}
	if len(reqs) < 50 {
		t.Fatalf("only %d requests", len(reqs))
	}
	for _, r := range reqs {
		a, b := ctWhole(t, ctAsk(srvA, r)), ctWhole(t, ctAsk(srvB, r))
		if a != b {
			t.Errorf("%s answers differently with failures on record:\n%s\n---\n%s", r, a, b)
		}
		if strings.Contains(b, `"failed_tx"`) {
			t.Errorf("%s answers a failed_tx", r)
		}
	}

	// The deposit's hash: no publication carries it, so the failure under
	// it is the one difference. The same list, with failed_tx beside it,
	// and kept by a cache, as the failure is final.
	dep := "/v1/blobs?tx=" + ctDepositTx + "&limit=25"
	ra, rb := ctAsk(srvA, dep), ctAsk(srvB, dep)
	var ma, mb map[string]json.RawMessage
	if err := json.NewDecoder(ra.Body).Decode(&ma); err != nil {
		t.Fatal(err)
	}
	if err := json.NewDecoder(rb.Body).Decode(&mb); err != nil {
		t.Fatal(err)
	}
	var f struct {
		Height         int64  `json:"height"`
		Code           uint32 `json:"code"`
		Codespace      string `json:"codespace"`
		FailedMsgIndex *int   `json:"failed_msg_index"`
		AntePassed     bool   `json:"ante_passed"`
	}
	if _, ok := ma["failed_tx"]; ok {
		t.Fatal("the deposit's hash answers failed_tx without failures on record")
	}
	if err := json.Unmarshal(mb["failed_tx"], &f); err != nil || f.Height != 20 || f.Code != 5 || f.Codespace != "sdk" ||
		f.FailedMsgIndex == nil || *f.FailedMsgIndex != 1 || !f.AntePassed {
		t.Fatalf("the deposit's hash with failures on record: %s (%v)", mb["failed_tx"], err)
	}
	delete(mb, "failed_tx")
	ja, _ := json.Marshal(ma)
	jb, _ := json.Marshal(mb)
	if !bytes.Equal(ja, jb) || ra.StatusCode != 200 || rb.StatusCode != 200 || ra.Header.Get("Content-Type") != rb.Header.Get("Content-Type") ||
		ra.Header.Get("Cache-Control") != "no-store" || rb.Header.Get("Cache-Control") != "public, max-age=15" {
		t.Errorf("the deposit's hash: without failures %d %q %s\nwith %d %q %s", ra.StatusCode, ra.Header.Get("Cache-Control"), ja,
			rb.StatusCode, rb.Header.Get("Cache-Control"), jb)
	}

	// Each failure no publication carries answers failed_tx, at its own
	// height, with failures on record, and only then.
	var fresh []string
	for h := range ctFresh {
		fresh = append(fresh, h)
	}
	sort.Strings(fresh)
	for _, h := range fresh {
		for _, c := range []struct {
			srv  *Server
			want bool
		}{{srvA, false}, {srvB, true}} {
			var m map[string]json.RawMessage
			resp := ctAsk(c.srv, "/v1/blobs?tx="+h)
			if err := json.NewDecoder(resp.Body).Decode(&m); err != nil {
				t.Fatal(err)
			}
			raw, has := m["failed_tx"]
			if has != c.want || string(m["total"]) != "0" {
				t.Errorf("%s (failures on record: %v): failed_tx %v, total %s", h, c.want, has, m["total"])
				continue
			}
			if has {
				var g struct {
					Height int64 `json:"height"`
				}
				if err := json.Unmarshal(raw, &g); err != nil || g.Height != ctFresh[h] {
					t.Errorf("%s: failed_tx %s, want height %d", h, raw, ctFresh[h])
				}
			}
		}
	}
}
