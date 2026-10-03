package api_test

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cosmos/cosmos-sdk/crypto/keys/secp256k1"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/scan"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/api"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/ingest"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
)

// filterFixture is four settlements: p1 and p2 settle the same commitment
// (a blob paid for twice), p1 with a payment on record from owner and p2
// from before payments were kept, signed with owner's key; p3 is another
// blob paid for by samplePublisher; p4 is signed by a third key and has no
// payment. Every one is submitted by samplePublisher, which pays only for
// p3.
func filterFixture(t *testing.T) (ts *httptest.Server, owner, twice string) {
	t.Helper()
	dir := t.TempDir()
	st, err := store.Open(filepath.Join(dir, "observer.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	ownerKey := secp256k1.GenPrivKey().PubKey().(*secp256k1.PubKey)
	thirdKey := secp256k1.GenPrivKey().PubKey().(*secp256k1.PubKey)
	if owner, err = scan.PublisherOf(hex.EncodeToString(ownerKey.Key)); err != nil {
		t.Fatal(err)
	}
	twice = strings.Repeat("ab", 32)
	now := time.Now().UTC().Truncate(time.Second)
	at := now.Add(-time.Hour)
	for i, p := range []struct {
		hash, commitment string
		key              *secp256k1.PubKey
	}{
		{"p1", twice, ownerKey}, {"p2", twice, ownerKey}, {"p3", strings.Repeat("cd", 32), thirdKey}, {"p4", strings.Repeat("ef", 32), thirdKey},
	} {
		pub := scan.Publication{
			SchemaVersion: scan.AttestationSchemaVersion, PromiseHash: p.hash, SettlementHeight: int64(100 + i), SettlementTime: at.Add(time.Duration(i) * time.Minute),
			SettlementTxHash: txOf(p.hash), MustServeUntil: at.Add(time.Hour), RecordedAt: at, Signer: samplePublisher,
			Promise: scan.PromiseFields{ChainID: "t", Height: int64(99 + i), Commitment: p.commitment, CreationTimestamp: at, BlobSize: 262144,
				SignerPublicKey: hex.EncodeToString(p.key.Key)},
			Assignment: scan.AssignmentTable{
				ProtocolParams:     scan.ProtocolParamsSnapshot{OriginalRows: 4096, TotalRows: 16384},
				ValidatorSetHeight: int64(99 + i), TotalVotingPower: 10, Sigma: 148, Distinct: 148, ValidatorsWithRows: 1,
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
	ps := []scan.Payment{
		{SchemaVersion: 1, DedupeKey: "txp1:0", Kind: "settlement", Height: 100, Time: at, TxHash: "txp1", Publisher: owner, Processor: samplePublisher,
			PromiseHash: "p1", BlobSize: 262144, Denom: "utia", AmountUtia: 695_000},
		{SchemaVersion: 1, DedupeKey: "txp3:0", Kind: "settlement", Height: 102, Time: at.Add(2 * time.Minute), TxHash: "txp3", Publisher: samplePublisher,
			Processor: samplePublisher, PromiseHash: "p3", BlobSize: 262144, Denom: "utia", AmountUtia: 695_000},
	}
	if r, err := ingest.Payments(st, writePayments(t, dir, ps), now); err != nil || r.Inserted != 2 {
		t.Fatalf("ingest payments: inserted=%d err=%v", r.Inserted, err)
	}
	ts = httptest.NewServer(api.NewWithVantage(st, api.VantageInfo{Name: "test"}, nil))
	t.Cleanup(ts.Close)
	return ts, owner, twice
}

// txOf is the settlement transaction hash the fixture gives a promise: 64 hex
// characters, in lower case as the scanner writes it, except p4's, in upper
// case as sentinel-synth writes it.
func txOf(hash string) string {
	h := sha256.Sum256([]byte("tx" + hash))
	if hash == "p4" {
		return strings.ToUpper(hex.EncodeToString(h[:]))
	}
	return hex.EncodeToString(h[:])
}

type filteredPage struct {
	Blobs []struct {
		PromiseHash      string `json:"promise_hash"`
		Commitment       string `json:"commitment"`
		Publisher        string `json:"publisher"`
		SettlementTxHash string `json:"settlement_tx_hash"`
	} `json:"blobs"`
	Total             int64  `json:"total"`
	Truncated         bool   `json:"truncated"`
	NextBeforeHeight  *int64 `json:"next_before_height"`
	NextBeforeTxIndex *int64 `json:"next_before_tx_index"`
	Commitment        string `json:"commitment"`
	Publisher         string `json:"publisher"`
	Tx                string `json:"tx"`
}

func (p filteredPage) hashes() string {
	var hs []string
	for _, b := range p.Blobs {
		hs = append(hs, b.PromiseHash)
	}
	return strings.Join(hs, ",")
}

// A DA team holds the commitment, not the promise hash: ?commitment= lists
// every settlement of that blob, newest first, and says what it filtered on.
func TestBlobsByCommitment(t *testing.T) {
	ts, _, twice := filterFixture(t)
	var page filteredPage
	if code := get(t, ts, "/v1/blobs?commitment="+strings.ToUpper(twice), &page); code != 200 {
		t.Fatalf("by commitment: %d", code)
	}
	if page.hashes() != "p2,p1" || page.Total != 2 || page.Truncated || page.Commitment != twice {
		t.Fatalf("by commitment: %s, total %d, echo %q", page.hashes(), page.Total, page.Commitment)
	}
	for _, b := range page.Blobs {
		if b.Commitment != twice {
			t.Errorf("%s has commitment %s", b.PromiseHash, b.Commitment)
		}
	}
	var none filteredPage
	if code := get(t, ts, "/v1/blobs?commitment="+strings.Repeat("00", 32), &none); code != 200 || len(none.Blobs) != 0 || none.Total != 0 {
		t.Fatalf("unknown commitment: %d %+v", code, none)
	}
	for _, bad := range []string{"zz", strings.Repeat("ab", 31), strings.Repeat("ab", 33), strings.Repeat("zz", 32)} {
		if code := get(t, ts, "/v1/blobs?commitment="+bad, nil); code != 400 {
			t.Errorf("commitment %q: %d, want 400", bad, code)
		}
	}
	// the other filters are not echoed when they were not asked for
	var plain map[string]json.RawMessage
	get(t, ts, "/v1/blobs", &plain)
	if _, ok := plain["commitment"]; ok {
		t.Error("an unfiltered page echoes a commitment")
	}
	if _, ok := plain["publisher"]; ok {
		t.Error("an unfiltered page echoes a publisher")
	}
}

// A developer who submitted a blob holds the settlement's transaction hash,
// as the client returns it: ?tx= finds the blob by it in either case, with
// or without 0x, and says what it filtered on. Every blob row, on the list
// and on the blob's own route, carries its hash in lower case, whatever case
// the store holds it in.
func TestBlobsByTx(t *testing.T) {
	ts, _, twice := filterFixture(t)
	p3 := txOf("p3")
	for _, q := range []string{p3, strings.ToUpper(p3), "0x" + p3, "0X" + strings.ToUpper(p3)} {
		var page filteredPage
		if code := get(t, ts, "/v1/blobs?tx="+q, &page); code != 200 {
			t.Fatalf("tx %s: %d", q, code)
		}
		if page.hashes() != "p3" || page.Total != 1 || page.Truncated || page.Tx != p3 || page.Blobs[0].SettlementTxHash != p3 {
			t.Fatalf("tx %s: %s, total %d, echo %q, row's hash %q", q, page.hashes(), page.Total, page.Tx, page.Blobs[0].SettlementTxHash)
		}
	}
	// a hash the store holds in upper case is found by either and published in lower
	p4 := strings.ToLower(txOf("p4"))
	for _, q := range []string{p4, txOf("p4")} {
		var page filteredPage
		if code := get(t, ts, "/v1/blobs?tx="+q, &page); code != 200 || page.hashes() != "p4" || page.Blobs[0].SettlementTxHash != p4 {
			t.Fatalf("tx stored in upper case, asked as %s: %d %+v", q, code, page)
		}
	}
	var none filteredPage
	if code := get(t, ts, "/v1/blobs?tx="+strings.Repeat("00", 32), &none); code != 200 || len(none.Blobs) != 0 || none.Total != 0 {
		t.Fatalf("unknown tx: %d %+v", code, none)
	}
	for _, bad := range []string{"zz", "txp1", strings.Repeat("ab", 31), strings.Repeat("ab", 33), "0x" + strings.Repeat("zz", 32)} {
		if code := get(t, ts, "/v1/blobs?tx="+bad, nil); code != 400 {
			t.Errorf("tx %q: %d, want 400", bad, code)
		}
	}
	// the filters combine: p1's transaction did settle the commitment paid
	// for twice, p3's did not
	var both, neither filteredPage
	if code := get(t, ts, "/v1/blobs?tx="+txOf("p1")+"&commitment="+twice, &both); code != 200 || both.hashes() != "p1" {
		t.Fatalf("p1's tx and its commitment: %d %s", code, both.hashes())
	}
	if code := get(t, ts, "/v1/blobs?tx="+p3+"&commitment="+twice, &neither); code != 200 || len(neither.Blobs) != 0 {
		t.Fatalf("p3's tx and another commitment: %d %s", code, neither.hashes())
	}
	// the commitment is read the same way: 0x and upper case
	var commit filteredPage
	if code := get(t, ts, "/v1/blobs?commitment=0X"+strings.ToUpper(twice), &commit); code != 200 || commit.hashes() != "p2,p1" || commit.Commitment != twice {
		t.Fatalf("0x commitment: %d %s echo %q", code, commit.hashes(), commit.Commitment)
	}
	// every row carries its own transaction hash
	var all filteredPage
	get(t, ts, "/v1/blobs", &all)
	if len(all.Blobs) != 4 {
		t.Fatalf("unfiltered: %s", all.hashes())
	}
	for _, b := range all.Blobs {
		if want := strings.ToLower(txOf(b.PromiseHash)); b.SettlementTxHash != want {
			t.Errorf("%s carries tx %q, want %q", b.PromiseHash, b.SettlementTxHash, want)
		}
	}
	var one struct {
		Blob struct {
			SettlementTxHash string `json:"settlement_tx_hash"`
		} `json:"blob"`
	}
	if code := get(t, ts, "/v1/blobs/p1", &one); code != 200 || one.Blob.SettlementTxHash != txOf("p1") {
		t.Fatalf("the blob's own route: %d, tx %q, want %q", code, one.Blob.SettlementTxHash, txOf("p1"))
	}
	var plain map[string]json.RawMessage
	get(t, ts, "/v1/blobs", &plain)
	if _, ok := plain["tx"]; ok {
		t.Error("an unfiltered page echoes a tx")
	}
}

// ?publisher= lists the blobs an account paid for, as each blob names its
// publisher: by the payment on record, else by the key that signed the
// promise, and never by who submitted the settlement. It pages like the
// list, by offset or by the cursor.
func TestBlobsByPublisher(t *testing.T) {
	ts, owner, twice := filterFixture(t)
	var page filteredPage
	if code := get(t, ts, "/v1/blobs?publisher="+owner, &page); code != 200 {
		t.Fatalf("by publisher: %d", code)
	}
	if page.hashes() != "p2,p1" || page.Total != 2 || page.Publisher != owner {
		t.Fatalf("owner's blobs: %s, total %d, echo %q", page.hashes(), page.Total, page.Publisher)
	}
	for _, b := range page.Blobs {
		if b.Publisher != owner {
			t.Errorf("%s is listed for %s but names %s", b.PromiseHash, owner, b.Publisher)
		}
	}
	// the same account in upper case, which bech32 allows
	var upper filteredPage
	if code := get(t, ts, "/v1/blobs?publisher="+strings.ToUpper(owner), &upper); code != 200 || upper.hashes() != "p2,p1" || upper.Total != 2 || upper.Publisher != owner {
		t.Fatalf("in upper case: %d %s, total %d, echo %q", code, upper.hashes(), upper.Total, upper.Publisher)
	}
	// the submitter of every settlement paid for one of them
	var sub filteredPage
	if code := get(t, ts, "/v1/blobs?publisher="+samplePublisher, &sub); code != 200 || sub.hashes() != "p3" || sub.Total != 1 {
		t.Fatalf("submitter's blobs: %d %s total %d, want p3 only", code, sub.hashes(), sub.Total)
	}
	var nobody filteredPage
	if code := get(t, ts, "/v1/blobs?publisher="+otherPublisher, &nobody); code != 200 || len(nobody.Blobs) != 0 || nobody.Total != 0 {
		t.Fatalf("an account with nothing paid: %d %+v", code, nobody)
	}

	// one a page: the cursor and the offset reach the same second row
	var first filteredPage
	if code := get(t, ts, "/v1/blobs?limit=1&publisher="+owner, &first); code != 200 || first.hashes() != "p2" || !first.Truncated || first.Total != 2 ||
		first.NextBeforeHeight == nil || first.NextBeforeTxIndex == nil {
		t.Fatalf("first page: %d %+v", code, first)
	}
	var byCursor, byOffset filteredPage
	get(t, ts, "/v1/blobs?limit=1&publisher="+owner+"&before_height="+strconv.FormatInt(*first.NextBeforeHeight, 10)+
		"&before_tx_index="+strconv.FormatInt(*first.NextBeforeTxIndex, 10), &byCursor)
	get(t, ts, "/v1/blobs?limit=1&offset=1&publisher="+owner, &byOffset)
	if byCursor.hashes() != "p1" || byCursor.Truncated || byOffset.hashes() != "p1" {
		t.Fatalf("second page: cursor %s, offset %s", byCursor.hashes(), byOffset.hashes())
	}

	// the filters combine
	var both filteredPage
	if code := get(t, ts, "/v1/blobs?publisher="+samplePublisher+"&commitment="+twice, &both); code != 200 || len(both.Blobs) != 0 {
		t.Fatalf("publisher and commitment: %d %s", code, both.hashes())
	}
	for _, bad := range []string{"nope", "celestiavaloper1yg3zyg3zyg3zyg3zyg3zyg3zyg3zyg3z64pdkn", sampleValidator} {
		if code := get(t, ts, "/v1/blobs?publisher="+bad, nil); code != 400 {
			t.Errorf("publisher %q: %d, want 400", bad, code)
		}
	}
}
