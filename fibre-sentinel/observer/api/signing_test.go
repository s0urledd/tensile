package api_test

import (
	"encoding/json"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/scan"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/api"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
)

// Three validators of equal power, 40-hex so the detail route accepts them.
const (
	sigV1 = "1111111111111111111111111111111111111111"
	sigV2 = "2222222222222222222222222222222222222222"
	sigV3 = "3333333333333333333333333333333333333333"
)

// signingFixture writes five publications over sigV1..sigV3 (power 10 each,
// 30 in total), settled an hour apart:
//
//	p1  v1 v2 signed      20/30, exactly the quorum
//	p2  v1 v2 v3 signed   30/30
//	p3  v1 signed         10/30, below the quorum as this observer sees it
//	p4  schema 1          recorded before verification: unknown
//	p5  v1 v2 signed      but the transaction failed: not a settled promise
//
// and returns the server and the store, for tests that add probes.
func signingFixture(t *testing.T) (*httptest.Server, *store.Store, time.Time) {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "observer.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	now := time.Now().UTC().Truncate(time.Second)

	add := func(i int, hash string, schema int, code uint32, signed ...string) {
		at := now.Add(-time.Duration(6-i) * time.Hour)
		on := map[string]bool{}
		for _, a := range signed {
			on[a] = true
		}
		var power int64
		vals := []scan.ValidatorAssignment{}
		for _, a := range []string{sigV1, sigV2, sigV3} {
			vals = append(vals, scan.ValidatorAssignment{Address: a, VotingPower: 10, RowCount: 148, Attested: on[a]})
			if on[a] {
				power += 10
			}
		}
		pub := scan.Publication{
			SchemaVersion: schema, PromiseHash: hash, SettlementHeight: int64(100 + i), SettlementTime: at,
			SettlementTxHash: "tx" + hash, SettlementTxCode: code, MustServeUntil: at.Add(time.Hour), RecordedAt: at,
			Signer:                  "celestia1pub",
			Promise:                 scan.PromiseFields{ChainID: "t", Height: int64(99 + i), Commitment: "c" + hash, CreationTimestamp: at, BlobSize: 1024},
			ValidatorSignatureCount: len(signed),
			Assignment: scan.AssignmentTable{
				ProtocolParams:     scan.ProtocolParamsSnapshot{OriginalRows: 4096, TotalRows: 16384},
				ValidatorSetHeight: int64(99 + i), TotalVotingPower: 30, Sigma: 444, Distinct: 444,
				ValidatorsWithRows: 3, AttestedWithRows: len(signed), SignatureEntries: len(signed),
				SignaturesVerified: len(signed), AttestedVotingPower: power, Validators: vals,
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
	add(1, "p1", scan.AttestationSchemaVersion, 0, sigV1, sigV2)
	add(2, "p2", scan.AttestationSchemaVersion, 0, sigV1, sigV2, sigV3)
	add(3, "p3", scan.AttestationSchemaVersion, 0, sigV1)
	add(4, "p4", 1, 0)
	add(5, "p5", scan.AttestationSchemaVersion, 7, sigV1, sigV2)
	if _, err := st.StartRun("collector", "test", "t", now); err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(api.New(st, "test"))
	t.Cleanup(ts.Close)
	return ts, st, now
}

type signingJSON struct {
	Assigned int64 `json:"assigned"`
	Signed   int64 `json:"signed"`
	Unknown  int64 `json:"unknown"`
	Rate     struct {
		Num   int64    `json:"num"`
		Den   int64    `json:"den"`
		Value *float64 `json:"value"`
	} `json:"rate"`
}

// The per-validator rate: assigned = settled promises that gave it rows and
// whose signatures were verified; the pre-verification record is unknown on
// neither side, and a failed transaction is no promise at all.
func TestSigningParticipationPerValidator(t *testing.T) {
	ts, _, _ := signingFixture(t)
	var resp struct {
		Validators []struct {
			Address string      `json:"address"`
			Signing signingJSON `json:"signing"`
		} `json:"validators"`
	}
	if code := get(t, ts, "/v1/validators?window=all", &resp); code != 200 {
		t.Fatalf("validators: %d", code)
	}
	want := map[string][3]int64{sigV1: {3, 3, 1}, sigV2: {3, 2, 1}, sigV3: {3, 1, 1}}
	seen := 0
	for _, v := range resp.Validators {
		w, ok := want[v.Address]
		if !ok {
			continue
		}
		seen++
		s := v.Signing
		if s.Assigned != w[0] || s.Signed != w[1] || s.Unknown != w[2] || s.Rate.Num != w[1] || s.Rate.Den != w[0] {
			t.Errorf("%s: signing = %+v, want assigned %d signed %d unknown %d", v.Address[:4], s, w[0], w[1], w[2])
		}
	}
	if seen != 3 {
		t.Fatalf("saw %d of 3 validators", seen)
	}

	// The detail route carries the same object.
	var det struct {
		Validator struct {
			Signing signingJSON `json:"signing"`
		} `json:"validator"`
	}
	if code := get(t, ts, "/v1/validators/"+sigV3+"?window=all", &det); code != 200 {
		t.Fatalf("detail: %d", code)
	}
	if s := det.Validator.Signing; s.Assigned != 3 || s.Signed != 1 {
		t.Fatalf("detail signing = %+v, want 1/3", s)
	}
}

// A window with nothing assigned is nothing to say: den 0 and a null value,
// never 0%.
func TestSigningEmptyWindowIsNull(t *testing.T) {
	ts, _, _ := signingFixture(t)
	var det struct {
		Validator struct {
			Signing signingJSON `json:"signing"`
		} `json:"validator"`
	}
	// Every publication settled at least an hour before now; the 24h window
	// holds them all, so move the question to one that holds none by asking
	// as of a moment before the first settlement.
	asOf := time.Now().UTC().Add(-7 * time.Hour).Format(time.RFC3339)
	if code := get(t, ts, "/v1/validators/"+sigV1+"?window=24h&as_of="+asOf, &det); code != 200 {
		t.Fatalf("detail: %d", code)
	}
	if s := det.Validator.Signing; s.Assigned != 0 || s.Rate.Den != 0 || s.Rate.Value != nil {
		t.Fatalf("signing = %+v, want an empty rate with a null value", s)
	}
}

// The network distribution: every promise in exactly one bucket, the quorum
// applied as the chain applies it (20 of 30 meets it), and the unknown and
// failed records outside.
func TestSigningDistribution(t *testing.T) {
	ts, _, _ := signingFixture(t)
	var resp struct {
		Promises  int64 `json:"promises"`
		Unknown   int64 `json:"unknown"`
		Threshold struct {
			Num, Den int64
		} `json:"threshold"`
		Meets   struct{ Num, Den int64 } `json:"meets_threshold"`
		Buckets []struct {
			Key            string `json:"key"`
			Count          int64  `json:"count"`
			AboveThreshold bool   `json:"above_threshold"`
		} `json:"buckets"`
		SignersMedian *int64 `json:"signers_median"`
	}
	if code := get(t, ts, "/v1/signing?window=all", &resp); code != 200 {
		t.Fatalf("signing: %d", code)
	}
	if resp.Promises != 3 || resp.Unknown != 1 {
		t.Fatalf("promises %d unknown %d, want 3 and 1", resp.Promises, resp.Unknown)
	}
	if resp.Threshold.Num != 2 || resp.Threshold.Den != 3 {
		t.Fatalf("threshold %+v, want 2/3", resp.Threshold)
	}
	if resp.Meets.Num != 2 || resp.Meets.Den != 3 {
		t.Fatalf("meets_threshold %+v, want 2/3", resp.Meets)
	}
	got := map[string]int64{}
	var sum int64
	for _, b := range resp.Buckets {
		got[b.Key] = b.Count
		sum += b.Count
		if (b.Key == "below") == b.AboveThreshold {
			t.Errorf("bucket %s above_threshold = %v", b.Key, b.AboveThreshold)
		}
	}
	if sum != resp.Promises {
		t.Fatalf("buckets sum to %d, want %d: %v", sum, resp.Promises, got)
	}
	if got["below"] != 1 || got["q_70"] != 1 || got["all"] != 1 {
		t.Fatalf("buckets = %v, want one below, one at the quorum, one full", got)
	}
	if resp.SignersMedian == nil || *resp.SignersMedian != 2 {
		t.Fatalf("signers_median = %v, want 2", resp.SignersMedian)
	}
	if code := get(t, ts, "/v1/signing?window=bogus", nil); code != 400 {
		t.Fatalf("bad window: %d, want 400", code)
	}
}

// A promise that settled while the validator had no Fibre host is one it
// could not sign: it is counted apart, in neither side of the rate, so a
// validator that registered late is rated on the promises it could reach.
// A host the registry could not read (NULL) stays in.
func TestSigningLeavesOutPromisesWithoutAHost(t *testing.T) {
	ts, st, _ := signingFixture(t)
	if _, err := st.DB().Exec(`UPDATE assignments SET host_at_settlement = '' WHERE validator_address = ? AND promise_hash IN ('p1', 'p3')`, sigV3); err != nil {
		t.Fatal(err)
	}
	var det struct {
		Validator struct {
			Signing struct {
				signingJSON
				NoHost int64 `json:"no_host"`
			} `json:"signing"`
		} `json:"validator"`
	}
	if code := get(t, ts, "/v1/validators/"+sigV3+"?window=all", &det); code != 200 {
		t.Fatalf("detail: %d", code)
	}
	if s := det.Validator.Signing; s.Assigned != 1 || s.Signed != 1 || s.NoHost != 2 || s.Rate.Den != 1 {
		t.Fatalf("signing = %+v, want 1/1 with 2 promises without a host", s)
	}
}

// The newest endorsement and the newest assigned promises are read from the
// whole record, whatever the window: a validator that stopped endorsing shows
// a run of zeros that a rate over a long period would hide. p4 (recorded
// before signatures were verified) and p5 (a failed transaction) are in
// neither count.
func TestSigningRecentAndLastEndorsement(t *testing.T) {
	ts, st, _ := signingFixture(t)
	var p2, p3 string
	if err := st.DB().QueryRow(`SELECT settlement_time FROM publications WHERE promise_hash = 'p2'`).Scan(&p2); err != nil {
		t.Fatal(err)
	}
	if err := st.DB().QueryRow(`SELECT settlement_time FROM publications WHERE promise_hash = 'p3'`).Scan(&p3); err != nil {
		t.Fatal(err)
	}
	type recentJSON struct {
		Last   *string `json:"last_endorsed_at"`
		Recent struct {
			Assigned int64 `json:"assigned"`
			Endorsed int64 `json:"endorsed"`
		} `json:"recent"`
	}
	for _, c := range []struct {
		addr     string
		endorsed int64
		last     string
	}{
		{sigV1, 3, p3}, // signed p1, p2, p3
		{sigV3, 1, p2}, // signed p2 only
	} {
		var det struct {
			Validator struct {
				Signing recentJSON `json:"signing"`
			} `json:"validator"`
		}
		if code := get(t, ts, "/v1/validators/"+c.addr+"?window=24h", &det); code != 200 {
			t.Fatalf("detail: %d", code)
		}
		s := det.Validator.Signing
		if s.Recent.Assigned != 3 || s.Recent.Endorsed != c.endorsed {
			t.Errorf("%s recent = %+v, want 3 assigned, %d endorsed", c.addr[:4], s.Recent, c.endorsed)
		}
		if s.Last == nil || *s.Last != c.last {
			t.Errorf("%s last endorsed = %v, want %s", c.addr[:4], s.Last, c.last)
		}
	}
}

// The blob list carries how many validators with rows endorsed each promise,
// and leaves it out for a record from before signatures were verified.
func TestBlobListAttestedWithRows(t *testing.T) {
	ts, _, _ := signingFixture(t)
	var resp struct {
		Blobs []struct {
			PromiseHash      string `json:"promise_hash"`
			AttestedWithRows *int   `json:"attested_with_rows"`
		} `json:"blobs"`
	}
	if code := get(t, ts, "/v1/blobs?window=all", &resp); code != 200 {
		t.Fatalf("blobs: %d", code)
	}
	want := map[string]int{"p1": 2, "p2": 3, "p3": 1}
	seen := 0
	for _, b := range resp.Blobs {
		if b.PromiseHash == "p4" {
			if b.AttestedWithRows != nil {
				t.Errorf("p4: attested_with_rows = %d, want absent", *b.AttestedWithRows)
			}
			continue
		}
		w, ok := want[b.PromiseHash]
		if !ok {
			continue
		}
		seen++
		if b.AttestedWithRows == nil || *b.AttestedWithRows != w {
			t.Errorf("%s: attested_with_rows = %v, want %d", b.PromiseHash, b.AttestedWithRows, w)
		}
	}
	if seen != 3 {
		t.Fatalf("saw %d of 3 blobs", seen)
	}
}
