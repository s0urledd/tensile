package api_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http/httptest"
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
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
)

// The Blobs list with its failed blob payments (/v1/blobs?include_failed=1,
// failedblobs.go). The fixture is ten blobs, b100 to b109, each its block's
// transaction 1, and the failed transactions of fbFailures around them: six
// failed blob payments (one at the list's top, one inside a MsgExec, one not
// final, one second in its transaction, one whose message's strings were
// cut), two failures that carried no MsgPayForFibre, and a row whose record
// does not decode.

const (
	// fbSampleKey is observer/testdata's publisher key: samplePublisher's.
	fbSampleKey = "0356684b7c1265cc78d8c8745fa9e145f36ab9852a85ed90d0eeed8240df01e53f"
	// fbOtherKey is a second publisher's key (the curve's generator).
	fbOtherKey = "0279be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798"
)

// fbNS is the fixture's second namespace; fixtureNS is its first.
var fbNS = strings.Repeat("00", 18) + strings.Repeat("0b", 11)

// fbHash is a failed transaction's hash, in lower case as the scanner
// writes it.
func fbHash(name string) string { return ftxHash("blobs list " + name) }

// fbBroken is the hash of the row whose record does not decode.
var fbBroken = fbHash("broken")

// fbOther is the second publisher, fbOtherKey's account.
func fbOther(t *testing.T) string {
	t.Helper()
	a, err := scan.PublisherOf(fbOtherKey)
	if err != nil {
		t.Fatal(err)
	}
	return a
}

// fbRow is a row of the list as the fixture builds it: a blob (its promise
// hash) or a failed payment ("f<height>:<index>"), where it stands, and what
// the filters read of it.
type fbRow struct {
	id     string
	height int64
	index  int
	ns     string
	pub    string // "" for a failure that names none
	failed bool
}

// fbBlobs are the fixture's blobs: b102 and b106 in fbNS paid by other,
// b105 in fixtureNS paid by other, the rest in fixtureNS paid by
// samplePublisher.
func fbBlobs(other string) []fbRow {
	var out []fbRow
	for h := int64(100); h <= 109; h++ {
		ns, pub := fixtureNS, samplePublisher
		switch h {
		case 102, 106:
			ns, pub = fbNS, other
		case 105:
			pub = other
		}
		out = append(out, fbRow{id: fmt.Sprintf("b%d", h), height: h, index: 1, ns: ns, pub: pub})
	}
	return out
}

// fbPFF is a MsgPayForFibre at index in its transaction, as the scanner
// records it.
func fbPFF(index int, ns, pub, promise string) failedtx.Msg {
	return failedtx.Msg{Index: index, TypeURL: ftxURLPFF, Signer: pub,
		Detail: &failedtx.MsgDetail{Publisher: pub, PromiseHash: promise, Namespace: ns, BlobSize: 262144}}
}

// fbOutOfGas is baseapp/recovery.go:58-63's error.
func fbOutOfGas() error {
	return errorsmod.Wrap(sdkerrors.ErrOutOfGas, fmt.Sprintf("out of gas in location: %v; gasWanted: %d, gasUsed: %d", "WritePerByte", 200000, 200133))
}

// fbFailure is one failed transaction of the fixture: its record, and the
// row it is in the list (with its promise hash), or nil for one that
// carried no MsgPayForFibre.
type fbFailure struct {
	rec     failedtx.Record
	row     *fbRow
	promise string
}

func fbFailures(other string) []fbFailure {
	invalid := errorsmod.Wrap(sdkerrors.ErrInvalidRequest, "payment promise height must be positive")
	short := errorsmod.Wrapf(sdkerrors.ErrInsufficientFunds, "spendable balance %s is smaller than %s", "10utia", "1000000utia")
	exec := func(inner ...failedtx.Msg) failedtx.Msg {
		return failedtx.Msg{Index: 0, TypeURL: failedtx.ExecTypeURL, Inner: inner}
	}
	row := func(h int64, i int, ns, pub string) *fbRow {
		return &fbRow{id: fmt.Sprintf("f%d:%d", h, i), height: h, index: i, ns: ns, pub: pub, failed: true}
	}
	cut := fbPFF(0, fixtureNS, samplePublisher, "q99")
	cut.Cut = true
	deposit := failedtx.Msg{Index: 0, TypeURL: ftxURLDeposit, Signer: samplePublisher,
		Detail: &failedtx.MsgDetail{Publisher: samplePublisher, Amount: "1000000utia"}}
	setHost := failedtx.Msg{Index: 0, TypeURL: ftxURLSetHost, Signer: "celestiavaloper1v", Detail: &failedtx.MsgDetail{Host: "fibre.example.org:7980"}}
	return []fbFailure{
		// the list's newest row, in the second namespace, paid by the second publisher
		{ftxRecord(fbHash("111"), 111, 0, fbOutOfGas(), true, "2000utia", fbPFF(0, fbNS, other, "q111")), row(111, 0, fbNS, other), "q111"},
		// b109's block, its transaction 0: after b109 in the list
		{ftxRecord(fbHash("109"), 109, 0, fbOutOfGas(), true, "2000utia", fbPFF(0, fixtureNS, samplePublisher, "q109")), row(109, 0, fixtureNS, samplePublisher), "q109"},
		// a MsgPayForFibre inside a MsgExec that had no grant: final, a zero fee
		{ftxRecord(fbHash("106"), 106, 0, ftxAtMessage(authz.ErrNoAuthorizationFound, 0), true, "", exec(fbPFF(0, fbNS, other, "q106"))),
			row(106, 0, fbNS, other), "q106"},
		// stopped before the ante: not final; b104's block, its transaction 2, so before b104 in the list
		{ftxRecord(fbHash("104"), 104, 2, invalid, false, "", fbPFF(0, fixtureNS, samplePublisher, "q104")), row(104, 2, fixtureNS, samplePublisher), "q104"},
		// no MsgPayForFibre: a deposit, and an endpoint registration inside a MsgExec
		{ftxRecord(fbHash("103"), 103, 0, ftxAtMessage(short, 0), true, "2000utia", deposit), nil, ""},
		{ftxRecord(fbHash("102"), 102, 0, ftxAtMessage(authz.ErrNoAuthorizationFound, 0), true, "", exec(setHost)), nil, ""},
		// the MsgPayForFibre second in its transaction, after a send
		{ftxRecord(fbHash("101"), 101, 0, ftxAtMessage(short, 1), true, "2000utia", failedtx.Msg{Index: 0, TypeURL: ftxURLSend}, fbPFF(1, fixtureNS, samplePublisher, "q101")),
			row(101, 0, fixtureNS, samplePublisher), "q101"},
		// the list's oldest row, its message's strings cut: no publisher
		{ftxRecord(fbHash("99"), 99, 0, fbOutOfGas(), true, "2000utia", cut), row(99, 0, fixtureNS, ""), "q99"},
	}
}

// fbInsert stores a failed transaction as the collector does.
func fbInsert(t *testing.T, st *store.Store, r failedtx.Record) {
	t.Helper()
	raw, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	if ok, err := st.InsertFailedTx(r, raw); err != nil || !ok {
		t.Fatalf("insert %s: inserted=%v err=%v", r.DedupeKey, ok, err)
	}
}

// fbFixture is the fixture's server, with its own and its store: the blobs
// and, with failures, the failed transactions of fbFailures and the row
// that does not decode.
func fbFixture(t *testing.T, failures bool) (*httptest.Server, *api.Server, *store.Store) {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "observer.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	other := fbOther(t)
	for _, b := range fbBlobs(other) {
		key := fbSampleKey
		if b.pub == other {
			key = fbOtherKey
		}
		settled := ftxAt.Add(time.Duration(b.height) * time.Second)
		pub := scan.Publication{
			SchemaVersion: scan.AttestationSchemaVersion, PromiseHash: b.id, SettlementHeight: b.height, SettlementTxIndex: b.index, SettlementTime: settled,
			SettlementTxHash: txOf(b.id), MustServeUntil: settled.Add(time.Hour), RecordedAt: settled, Signer: samplePublisher,
			Promise: scan.PromiseFields{ChainID: "t", Height: b.height - 1, Commitment: fmt.Sprintf("%064x", b.height), CreationTimestamp: settled, BlobSize: 262144,
				SignerPublicKey: key, Namespace: b.ns},
			Assignment: scan.AssignmentTable{
				ProtocolParams:     scan.ProtocolParamsSnapshot{OriginalRows: 4096, TotalRows: 16384},
				ValidatorSetHeight: b.height - 1, TotalVotingPower: 10, Sigma: 148, Distinct: 148, ValidatorsWithRows: 1,
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
	}
	if failures {
		for _, f := range fbFailures(other) {
			fbInsert(t, st, f.rec)
		}
		// a row a newer build wrote, say, that this one cannot read
		if _, err := st.DB().Exec(`INSERT INTO failed_txs (dedupe_key, tx_hash, height, tx_index, time, code, codespace, raw_json)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?)`, failedtx.Key(105, 0), fbBroken, 105, 0, store.TS(ftxAt.Add(105*time.Second)), 11, "sdk",
			`{"schema_version":1,"tx_hash":`); err != nil {
			t.Fatal(err)
		}
	}
	srv := api.NewWithVantage(st, api.VantageInfo{Name: "test"}, nil)
	t.Cleanup(srv.Close)
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)
	return ts, srv, st
}

// fbList is the list the answers must give of the rows keep selects:
// newest first, by height and then by transaction index.
func fbList(other string, keep func(fbRow) bool) []fbRow {
	var rows []fbRow
	for _, b := range fbBlobs(other) {
		if keep(b) {
			rows = append(rows, b)
		}
	}
	for _, f := range fbFailures(other) {
		if f.row != nil && keep(*f.row) {
			rows = append(rows, *f.row)
		}
	}
	sort.Slice(rows, func(i, j int) bool {
		a, b := rows[i], rows[j]
		return a.height > b.height || (a.height == b.height && a.index > b.index)
	})
	return rows
}

// fbPage is what a page of the list is checked by.
type fbPage struct {
	Blobs []struct {
		Status            string `json:"status"`
		PromiseHash       string `json:"promise_hash"`
		SettlementHeight  int64  `json:"settlement_height"`
		SettlementTxIndex int    `json:"settlement_tx_index"`
	} `json:"blobs"`
	Total             int64  `json:"total"`
	FailedTotal       *int   `json:"failed_total"`
	Truncated         bool   `json:"truncated"`
	NextBeforeHeight  *int64 `json:"next_before_height"`
	NextBeforeTxIndex *int   `json:"next_before_tx_index"`
}

// ids are the page's rows as fbRow names them; a blob without its status
// is marked.
func (p fbPage) ids() string {
	var out []string
	for _, b := range p.Blobs {
		switch b.Status {
		case "failed":
			out = append(out, fmt.Sprintf("f%d:%d", b.SettlementHeight, b.SettlementTxIndex))
		case "success":
			out = append(out, b.PromiseHash)
		default:
			out = append(out, "status "+b.Status+"? "+b.PromiseHash)
		}
	}
	return strings.Join(out, ",")
}

func fbIDs(rows []fbRow) string {
	var out []string
	for _, r := range rows {
		out = append(out, r.id)
	}
	return strings.Join(out, ",")
}

// Every page of every filter, at every offset and several sizes, is the
// stretch of the one list it should be: the blobs and the failed payments
// the filters select, newest first, a failure on a page's edge on that page
// and on no other, total counting both, failed_total the failures, and the
// cursor at the page's last row, a failure's included.
func TestTheFailedPaymentsStandAmongTheBlobsOnEveryPage(t *testing.T) {
	ts, _, _ := fbFixture(t, true)
	other := fbOther(t)
	for _, f := range []struct {
		name, q string
		keep    func(fbRow) bool
	}{
		{"everything", "", func(fbRow) bool { return true }},
		{"a namespace, asked in upper case", "&namespace=" + strings.ToUpper(fbNS), func(r fbRow) bool { return r.ns == fbNS }},
		{"the second publisher", "&publisher=" + other, func(r fbRow) bool { return r.pub == other }},
		{"samplePublisher", "&publisher=" + samplePublisher, func(r fbRow) bool { return r.pub == samplePublisher }},
		{"before b106", "&before_height=106&before_tx_index=1", func(r fbRow) bool { return r.height < 106 || (r.height == 106 && r.index < 1) }},
		{"a namespace before 109", "&namespace=" + fixtureNS + "&before_height=109", func(r fbRow) bool { return r.ns == fixtureNS && r.height < 109 }},
	} {
		want := fbList(other, f.keep)
		failed := 0
		for _, r := range want {
			if r.failed {
				failed++
			}
		}
		if failed == 0 || failed == len(want) {
			t.Fatalf("%s: the fixture must mix blobs and failures: %s", f.name, fbIDs(want))
		}
		for _, limit := range []int{1, 2, 3, 5, len(want)} {
			for offset := 0; offset <= len(want)+1; offset++ {
				path := fmt.Sprintf("/v1/blobs?include_failed=1&limit=%d&offset=%d%s", limit, offset, f.q)
				var p fbPage
				if code := get(t, ts, path, &p); code != 200 {
					t.Fatalf("%s: %d", path, code)
				}
				lo, hi := min(offset, len(want)), min(offset+limit, len(want))
				if got, w := p.ids(), fbIDs(want[lo:hi]); got != w {
					t.Errorf("%s (%s): rows %s, want %s", path, f.name, got, w)
				}
				if p.Total != int64(len(want)) || p.FailedTotal == nil || *p.FailedTotal != failed || p.Truncated != (hi < len(want)) {
					t.Errorf("%s (%s): total %d, failed_total %v, truncated %v; want %d, %d, %v", path, f.name, p.Total, p.FailedTotal, p.Truncated, len(want), failed, hi < len(want))
				}
				switch {
				case p.Truncated && hi > lo:
					last := want[hi-1]
					if p.NextBeforeHeight == nil || p.NextBeforeTxIndex == nil || *p.NextBeforeHeight != last.height || *p.NextBeforeTxIndex != last.index {
						t.Errorf("%s (%s): cursor %v %v, want %s's", path, f.name, p.NextBeforeHeight, p.NextBeforeTxIndex, last.id)
					}
				case !p.Truncated && p.NextBeforeHeight != nil:
					t.Errorf("%s (%s): a cursor on the last page", path, f.name)
				}
			}
		}
	}
	// the cursor a page gives leads to the next: the whole list, a page of
	// three at a time
	want := fbList(other, func(fbRow) bool { return true })
	var walked []string
	path := "/v1/blobs?include_failed=1&limit=3"
	for i := 0; i < len(want); i++ {
		var p fbPage
		if code := get(t, ts, path, &p); code != 200 {
			t.Fatalf("%s: %d", path, code)
		}
		walked = append(walked, p.ids())
		if !p.Truncated {
			break
		}
		path = fmt.Sprintf("/v1/blobs?include_failed=1&limit=3&before_height=%d&before_tx_index=%d", *p.NextBeforeHeight, *p.NextBeforeTxIndex)
	}
	if got := strings.Join(walked, ","); got != fbIDs(want) {
		t.Errorf("by the cursor: %s, want %s", got, fbIDs(want))
	}
}

// fbWantRow is a failed payment's row as its record gives it: the promise's
// hash and namespace, the publisher where the message names one, its place,
// the chain's code, Explain's reason, and whether it is final.
func fbWantRow(r failedtx.Record, promise, ns, pub string) string {
	var b strings.Builder
	fmt.Fprintf(&b, `{"status":"failed","promise_hash":%q,"namespace":%q,`, promise, ns)
	if pub != "" {
		fmt.Fprintf(&b, `"publisher":%q,`, pub)
	}
	fmt.Fprintf(&b, `"settlement_height":%d,"settlement_tx_index":%d,"settlement_tx_hash":%q,"settlement_time":%q,"code":%d,"codespace":%q,`,
		r.Height, r.TxIndex, r.TxHash, store.TS(r.Time), r.Code, r.Codespace)
	if reason := failedtx.Explain(r).Reason; reason != "" {
		fmt.Fprintf(&b, `"reason":%q,`, reason)
	}
	fmt.Fprintf(&b, `"final":%v}`, r.AntePassed)
	return b.String()
}

// A failed payment's row is its record's place, promise and error, and
// nothing a blob has: no commitment, size, signer, fee, endorsement or
// reading. A MsgPayForFibre inside a MsgExec gives its promise as one at
// the top level does; one whose strings were cut names no publisher. Each
// blob's row is the row the list gives without include_failed, with status
// success before it.
func TestAFailedPaymentsRowIsWhatItsRecordSays(t *testing.T) {
	ts, _, _ := fbFixture(t, true)
	other := fbOther(t)
	var rows []json.RawMessage
	if err := json.Unmarshal(ftxAsk(t, ts, "/v1/blobs?include_failed=1&limit=50").keys["blobs"], &rows); err != nil {
		t.Fatal(err)
	}
	var plain []json.RawMessage
	if err := json.Unmarshal(ftxAsk(t, ts, "/v1/blobs?limit=50").keys["blobs"], &plain); err != nil {
		t.Fatal(err)
	}
	byID := map[string]string{}
	for _, raw := range rows {
		var r struct {
			Status            string `json:"status"`
			PromiseHash       string `json:"promise_hash"`
			SettlementHeight  int64  `json:"settlement_height"`
			SettlementTxIndex int    `json:"settlement_tx_index"`
		}
		if err := json.Unmarshal(raw, &r); err != nil {
			t.Fatal(err)
		}
		if r.Status == "failed" {
			byID[fmt.Sprintf("f%d:%d", r.SettlementHeight, r.SettlementTxIndex)] = string(raw)
		} else {
			byID[r.PromiseHash] = string(raw)
		}
	}
	n := 0
	for _, f := range fbFailures(other) {
		if f.row == nil {
			continue
		}
		n++
		if got, want := byID[f.row.id], fbWantRow(f.rec, f.promise, f.row.ns, f.row.pub); got != want {
			t.Errorf("%s:\n got %s\nwant %s", f.row.id, got, want)
		}
	}
	if n != 6 || len(rows) != 16 {
		t.Fatalf("%d failed payments, %d rows; want 6 and 16", n, len(rows))
	}
	for _, raw := range plain {
		var r struct {
			PromiseHash string `json:"promise_hash"`
		}
		if err := json.Unmarshal(raw, &r); err != nil {
			t.Fatal(err)
		}
		if got, want := byID[r.PromiseHash], `{"status":"success",`+string(raw[1:]); got != want {
			t.Errorf("%s:\n got %s\nwant %s", r.PromiseHash, got, want)
		}
	}
}

// Without include_failed, or with 0, the list is the one it was: byte for
// byte what a store with no failure on record answers, headers included. A
// lookup by transaction or commitment answers as it does without
// include_failed, a failure by its hash included (failed_tx), and adds no
// failed row.
func TestTheListWithoutIncludeFailedIsTheListItWas(t *testing.T) {
	with, _, _ := fbFixture(t, true)
	without, _, _ := fbFixture(t, false)
	other := fbOther(t)
	same := func(what string, a, b ftxAnswer) {
		t.Helper()
		if a.status != 200 || a.status != b.status || a.cache != b.cache || !bytes.Equal(a.body, b.body) {
			t.Errorf("%s:\n%d %q %s\n%d %q %s", what, a.status, a.cache, a.body, b.status, b.cache, b.body)
		}
	}
	for _, path := range []string{
		"/v1/blobs", "/v1/blobs?limit=3&offset=2", "/v1/blobs?namespace=" + fbNS, "/v1/blobs?publisher=" + other,
		"/v1/blobs?before_height=106&before_tx_index=1&limit=4", "/v1/blobs?limit=25&offset=0",
	} {
		a := ftxAsk(t, with, path)
		same(path+", with failures and without", a, ftxAsk(t, without, path))
		sep := "?"
		if strings.Contains(path, "?") {
			sep = "&"
		}
		same(path+", and with include_failed=0", a, ftxAsk(t, with, path+sep+"include_failed=0"))
	}
	for _, path := range []string{
		"/v1/blobs?tx=" + txOf("b105"), "/v1/blobs?tx=" + strings.ToUpper(txOf("b103")) + "&limit=5", "/v1/blobs?commitment=" + fmt.Sprintf("%064x", 103),
		"/v1/blobs?tx=" + fbHash("109"), "/v1/blobs?tx=" + fbHash("104"), "/v1/blobs?commitment=" + fmt.Sprintf("%064x", 106) + "&namespace=" + fbNS,
	} {
		a := ftxAsk(t, with, path)
		if a.keys["failed_total"] != nil {
			t.Errorf("%s: failed_total without include_failed", path)
		}
		same(path+", and with include_failed=1", a, ftxAsk(t, with, path+"&include_failed=1"))
	}
	// with no failure on record, include_failed=1 is the blobs, each a success
	var p fbPage
	if code := get(t, without, "/v1/blobs?include_failed=1&limit=50", &p); code != 200 || p.Total != 10 || p.FailedTotal == nil || *p.FailedTotal != 0 ||
		p.ids() != fbIDs(fbList(other, func(r fbRow) bool { return !r.failed })) {
		t.Errorf("no failures: %d %+v", code, p)
	}
	for _, v := range []string{"2", "true", "yes"} {
		if code := get(t, with, "/v1/blobs?include_failed="+v, nil); code != 400 {
			t.Errorf("include_failed=%s: %d, want 400", v, code)
		}
	}
}

// A page that holds a failure that is not final is not cached, as
// /v1/txs/{hash} does not cache one: its transaction could still take
// effect in a later block. Any other page keeps the list's default.
func TestAPageWithAFailureThatIsNotFinalIsNotCached(t *testing.T) {
	ts, _, _ := fbFixture(t, true)
	// the list: f111 b109 f109 b108 b107 b106 f106 b105 f104 b104 b103 b102 b101 f101 b100 f99
	for _, c := range []struct{ path, cache string }{
		{"/v1/blobs?include_failed=1&limit=3", "public, max-age=15"},
		{"/v1/blobs?include_failed=1&limit=1&offset=8", "no-store"},
		{"/v1/blobs?include_failed=1&limit=25", "no-store"},
		{"/v1/blobs?include_failed=1&limit=5&offset=10", "public, max-age=15"},
		{"/v1/blobs?include_failed=1&publisher=" + samplePublisher + "&before_height=104", "public, max-age=15"},
	} {
		if a := ftxAsk(t, ts, c.path); a.status != 200 || a.cache != c.cache {
			t.Errorf("%s: %d %q, want %q", c.path, a.status, a.cache, c.cache)
		}
	}
}

// The failures are read once and then on from the last row read: one
// stored after an answer is in the next. A row whose record does not decode
// is in no answer, which stands (never a 500); it is counted once among the
// undecodable rows /v1/health reports, and only once the list is read with
// it, never by the reading at the start.
func TestTheFailedPaymentsAreReadOnAsTheyAreStored(t *testing.T) {
	ts, srv, st := fbFixture(t, true)
	if n, _, _ := srv.UndecodableRows(); n != 0 {
		t.Fatalf("%d undecodable rows before the list was read", n)
	}
	var first, after, end fbPage
	if code := get(t, ts, "/v1/blobs?include_failed=1&limit=2", &first); code != 200 || first.Total != 16 || first.FailedTotal == nil || *first.FailedTotal != 6 ||
		first.ids() != "f111:0,b109" {
		t.Fatalf("first: %d %+v", code, first)
	}
	if n, last, _ := srv.UndecodableRows(); n != 1 || !strings.Contains(last, "failed transaction of "+fbBroken) {
		t.Fatalf("undecodable rows %d, last %q; want 1, naming the failed transaction", n, last)
	}
	fbInsert(t, st, ftxRecord(fbHash("112"), 112, 3, fbOutOfGas(), true, "2000utia", fbPFF(0, fixtureNS, samplePublisher, "q112")))
	if code := get(t, ts, "/v1/blobs?include_failed=1&limit=2", &after); code != 200 || after.Total != 17 || after.FailedTotal == nil || *after.FailedTotal != 7 ||
		after.ids() != "f112:3,f111:0" {
		t.Fatalf("after a failure stored: %d %+v", code, after)
	}
	if code := get(t, ts, "/v1/blobs?include_failed=1&limit=2&offset=15", &end); code != 200 || end.ids() != "b100,f99:0" || end.Truncated {
		t.Fatalf("the last page: %d %+v", code, end)
	}
	if n, _, _ := srv.UndecodableRows(); n != 1 {
		t.Fatalf("undecodable rows %d after reading the list again; want 1", n)
	}
}
