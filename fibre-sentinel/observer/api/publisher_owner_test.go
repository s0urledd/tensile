package api_test

import (
	"encoding/hex"
	"encoding/json"
	"net/http/httptest"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/cosmos/cosmos-sdk/crypto/keys/secp256k1"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/scan"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/api"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/ingest"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
)

// A settlement submitted by someone else (typically an endorsing validator)
// belongs to the escrow owner whose key signed the promise: the blob names
// that account as its publisher, and its publisher page lists the blob,
// whether or not a payment row is on record. The submitter's page does not.
func TestThePublisherIsTheEscrowOwnerNotTheSubmitter(t *testing.T) {
	dir := t.TempDir()
	st, err := store.Open(filepath.Join(dir, "observer.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	key := secp256k1.GenPrivKey().PubKey().(*secp256k1.PubKey)
	owner, err := scan.PublisherOf(hex.EncodeToString(key.Key))
	if err != nil {
		t.Fatal(err)
	}
	submitter := samplePublisher
	if submitter == owner {
		t.Fatal("the fixture needs two different accounts")
	}
	now := time.Now().UTC().Truncate(time.Second)
	at := now.Add(-time.Hour)
	// "paid" has a settlement payment on record; "unpaid" predates payments
	for i, hash := range []string{"paid", "unpaid"} {
		pub := scan.Publication{
			SchemaVersion: scan.AttestationSchemaVersion, PromiseHash: hash, SettlementHeight: int64(100 + i), SettlementTime: at,
			SettlementTxHash: "tx" + hash, MustServeUntil: at.Add(time.Hour), RecordedAt: at, Signer: submitter,
			Promise: scan.PromiseFields{ChainID: "t", Height: int64(99 + i), Commitment: "c" + hash, CreationTimestamp: at, BlobSize: 262144,
				SignerPublicKey: hex.EncodeToString(key.Key)},
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
	ps := []scan.Payment{{SchemaVersion: 1, DedupeKey: "txpaid:0", Kind: "settlement", Height: 100, Time: at,
		TxHash: "txpaid", Publisher: owner, Processor: submitter, PromiseHash: "paid", BlobSize: 262144, Denom: "utia", AmountUtia: 695_000}}
	if r, err := ingest.Payments(st, writePayments(t, dir, ps), now); err != nil || r.Inserted != 1 {
		t.Fatalf("ingest payments: inserted=%d err=%v", r.Inserted, err)
	}
	ts := httptest.NewServer(api.NewWithVantage(st, api.VantageInfo{Name: "test"}, nil))
	t.Cleanup(ts.Close)

	var list struct {
		Blobs []struct {
			PromiseHash string `json:"promise_hash"`
			Signer      string `json:"signer"`
			Publisher   string `json:"publisher"`
		} `json:"blobs"`
	}
	if code := get(t, ts, "/v1/blobs", &list); code != 200 || len(list.Blobs) != 2 {
		t.Fatalf("blobs: %d, %d rows", code, len(list.Blobs))
	}
	for _, b := range list.Blobs {
		if b.Publisher != owner || b.Signer != submitter {
			t.Errorf("%s: publisher %s signer %s, want publisher %s (the escrow owner) and signer %s", b.PromiseHash, b.Publisher, b.Signer, owner, submitter)
		}
	}
	pageOf := func(addr string) []string {
		var p struct {
			Blobs []struct {
				PromiseHash string `json:"promise_hash"`
			} `json:"recent_blobs"`
		}
		if code := get(t, ts, "/v1/publishers/"+addr, &p); code != 200 {
			return nil
		}
		var hs []string
		for _, b := range p.Blobs {
			hs = append(hs, b.PromiseHash)
		}
		sort.Strings(hs)
		return hs
	}
	if got := strings.Join(pageOf(owner), ","); got != "paid,unpaid" {
		t.Errorf("the owner's page lists %q, want paid,unpaid", got)
	}
	if got := pageOf(submitter); len(got) != 0 {
		t.Errorf("the submitter's page lists %v, want none", got)
	}
}
