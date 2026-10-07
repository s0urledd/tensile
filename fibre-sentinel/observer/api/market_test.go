package api_test

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/cosmos/cosmos-sdk/types/bech32"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/scan"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/api"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/ingest"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
	"net/http/httptest"
)

// The sample store's publications: the first one is signed by this account
// and settled in this tx. The payments written here reuse them so the blob
// page can join charge to publication.
const (
	samplePublisher = "celestia1d3mmg652pxj776dyqwlsrc93y64088g6ux8deq"
	otherPublisher  = "celestia1zyg3zyg3zyg3zyg3zyg3zyg3zyg3zyg3shxjgz"
	timeoutOperator = "celestia1yg3zyg3zyg3zyg3zyg3zyg3zyg3zyg3zl2r5q4" // same bytes as timeoutValoper
	timeoutValoper  = "celestiavaloper1yg3zyg3zyg3zyg3zyg3zyg3zyg3zyg3z64pdkn"
	// The first validator of the sample assignment, given an identity whose
	// operator address has timeoutOperator's bytes.
	sampleValidator = "7730f065f885965a04e6c6f41ef9a29d547f1c99"
)

func writePayments(t *testing.T, dir string, ps []scan.Payment) string {
	t.Helper()
	path := filepath.Join(dir, "payments.jsonl")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	for _, p := range ps {
		if err := enc.Encode(p); err != nil {
			t.Fatal(err)
		}
	}
	return path
}

func marketServer(t *testing.T, labels map[string]api.PublisherLabel) (*httptest.Server, *store.Store) {
	t.Helper()
	dir := t.TempDir()
	st, err := store.Open(filepath.Join(dir, "observer.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	now := time.Now()
	if _, err := ingest.Publications(st, filepath.Join(sampleDir, "publications.jsonl"), now); err != nil {
		t.Fatal(err)
	}
	var pubs []scan.Publication
	pubs, err = scan.LoadPublications(filepath.Join(sampleDir, "publications.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	first := pubs[0]
	settle := now.Add(-2 * time.Hour)
	fee := uint64(650_000 + 45_000*1) // 256 KiB → one chunk
	ps := []scan.Payment{
		{SchemaVersion: 1, DedupeKey: "d1", Kind: "deposit", Height: 10, Time: settle.Add(-time.Hour), TxHash: "aa", Publisher: samplePublisher, Denom: "utia", AmountUtia: 6_000_000_000},
		{SchemaVersion: 1, DedupeKey: first.SettlementTxHash + ":0", Kind: "settlement", Height: first.SettlementHeight, Time: settle, TxHash: first.SettlementTxHash,
			Publisher: samplePublisher, Processor: samplePublisher, PromiseHash: first.PromiseHash, BlobSize: 262144, GasUnits: fee, Denom: "utia", AmountUtia: fee},
		{SchemaVersion: 1, DedupeKey: "t1", Kind: "timeout", Height: 12, Time: settle.Add(time.Minute), TxHash: "bb",
			Publisher: otherPublisher, Processor: timeoutOperator, PromiseHash: "deadbeef", BlobSize: 1 << 20, GasUnits: 830_000, Denom: "utia", AmountUtia: 830_000},
		{SchemaVersion: 1, DedupeKey: "s2", Kind: "settlement", Height: 13, Time: settle.Add(2 * time.Minute), TxHash: "cc",
			Publisher: otherPublisher, Processor: otherPublisher, PromiseHash: "cafe", BlobSize: 1 << 20, GasUnits: 830_000, Denom: "utia", AmountUtia: 830_000},
		{SchemaVersion: 1, DedupeKey: "w1", Kind: "withdrawal_request", Height: 14, Time: settle.Add(3 * time.Minute), TxHash: "dd", Publisher: samplePublisher, Denom: "utia", AmountUtia: 1000},
		{SchemaVersion: 1, DedupeKey: "h15:executed:0", Kind: "withdrawal_executed", Height: 15, Time: settle.Add(4 * time.Minute), TxIndex: -1, Publisher: samplePublisher, Denom: "utia", AmountUtia: 1000},
		// A payment ten days old: in 30d and all, not in 24h or 7d.
		{SchemaVersion: 1, DedupeKey: "old", Kind: "settlement", Height: 5, Time: now.Add(-10 * 24 * time.Hour), TxHash: "ee",
			Publisher: otherPublisher, Processor: otherPublisher, PromiseHash: "0ld", BlobSize: 65536, GasUnits: 695_000, Denom: "utia", AmountUtia: 695_000},
	}
	if r, err := ingest.Payments(st, writePayments(t, dir, ps), now); err != nil || r.Inserted != int64(len(ps)) {
		t.Fatalf("ingest payments: inserted=%d err=%v", r.Inserted, err)
	}
	if err := st.UpsertEscrowAccount(scan.Escrow{Signer: samplePublisher, Denom: "utia", BalanceUtia: 5_999_304_000, AvailableUtia: 5_999_304_000, Height: 20, Found: true}, now); err != nil {
		t.Fatal(err)
	}
	if err := st.UpsertEscrowAccount(scan.Escrow{Signer: otherPublisher, Found: false}, now); err != nil {
		t.Fatal(err)
	}
	if _, err := st.UpsertValidatorIdentities([]scan.ValidatorIdentity{{
		ConsAddressHex: sampleValidator, Moniker: "enforcer",
		OperatorAddress: timeoutValoper,
	}}, now); err != nil {
		t.Fatal(err)
	}
	var opts []api.Option
	if labels != nil {
		opts = append(opts, api.WithPublisherLabels(labels))
	}
	ts := httptest.NewServer(api.NewWithVantage(st, api.VantageInfo{Name: "test"}, nil, opts...))
	t.Cleanup(ts.Close)
	return ts, st
}

func TestMarketSummary(t *testing.T) {
	ts, _ := marketServer(t, map[string]api.PublisherLabel{samplePublisher: {Address: samplePublisher, Label: "Sentinel test publisher", Source: "this observer"}})
	var m struct {
		Settlements    int64                       `json:"settlements"`
		Blobs          int64                       `json:"blobs"`
		Fees           int64                       `json:"fees_settled_utia"`
		Bytes          int64                       `json:"bytes"`
		Publishers     int64                       `json:"publishers_active"`
		PerMiB         *float64                    `json:"paid_per_mib_utia"`
		Timeouts       int64                       `json:"timeouts"`
		TimedOut       int64                       `json:"timed_out_utia"`
		Deposits       struct{ Count, Utia int64 } `json:"deposits"`
		EscrowHeld     int64                       `json:"escrow_held_utia"`
		EscrowAccounts int64                       `json:"escrow_accounts"`
		Daily          []map[string]any            `json:"daily"`
		Hourly         []struct {
			Hour        string `json:"hour"`
			Bytes       int64  `json:"bytes"`
			Settlements int64  `json:"settlements"`
			Fees        int64  `json:"fees_utia"`
		} `json:"hourly"`
		ComputedAt string `json:"computed_at"`
	}
	if code := get(t, ts, "/v1/market?window=24h", &m); code != 200 {
		t.Fatalf("market: %d", code)
	}
	// "cafe" has no recorded publication: a blob of its own.
	if m.Settlements != 2 || m.Blobs != 2 || m.Fees != 695_000+830_000 || m.Bytes != 262144+1<<20 || m.Publishers != 2 {
		t.Fatalf("settlements: %+v", m)
	}
	if m.PerMiB == nil || *m.PerMiB < 1_000_000 || *m.PerMiB > 1_300_000 {
		t.Fatalf("paid per MiB: %v", m.PerMiB)
	}
	if m.Timeouts != 1 || m.TimedOut != 830_000 {
		t.Fatalf("timeouts: %+v", m)
	}
	if m.Deposits.Count != 1 || m.Deposits.Utia != 6_000_000_000 {
		t.Fatalf("deposits: %+v", m)
	}
	if m.EscrowHeld != 5_999_304_000 || m.EscrowAccounts != 1 {
		t.Fatalf("escrow: held=%d accounts=%d (a not-found account must not count)", m.EscrowHeld, m.EscrowAccounts)
	}
	if len(m.Daily) == 0 {
		t.Fatal("no daily buckets")
	}
	// a day is charted by the hour: the same settlements, split by UTC hour
	var hb, hs, hf int64
	for _, h := range m.Hourly {
		if len(h.Hour) != 13 {
			t.Fatalf("hour key %q, want YYYY-MM-DDTHH", h.Hour)
		}
		hb, hs, hf = hb+h.Bytes, hs+h.Settlements, hf+h.Fees
	}
	if hs != m.Settlements || hb != m.Bytes || hf != m.Fees {
		t.Fatalf("hourly sums to %d settlements, %d bytes, %d utia; want %d, %d, %d", hs, hb, hf, m.Settlements, m.Bytes, m.Fees)
	}
	// What no page read is not published: the split by publisher (the
	// publishers' own figures are /v1/publishers), the top five and the
	// fold, the largest poster and the withdrawal sums (the queue is
	// withdrawal_queue).
	var whole map[string]json.RawMessage
	get(t, ts, "/v1/market?window=24h", &whole)
	for _, k := range []string{"hourly_by_publisher", "daily_by_publisher", "top_publishers", "other_publishers", "largest_poster",
		"withdrawals_requested", "withdrawals_executed"} {
		if _, ok := whole[k]; ok {
			t.Errorf("/v1/market still carries %s", k)
		}
	}
	if _, ok := whole["readings"]; !ok {
		t.Error("/v1/market carries no readings")
	}
	if m.ComputedAt == "" {
		t.Fatal("no computed_at")
	}
	// every fee here is recomputed with the module's formula, which
	// /v1/params publishes
	var params struct {
		Formula struct {
			BaseGas     uint64 `json:"base_gas"`
			GasPerChunk uint64 `json:"gas_per_chunk"`
			ChunkBytes  uint64 `json:"chunk_bytes"`
			UtiaPerGas  uint64 `json:"utia_per_gas"`
		} `json:"price_formula"`
	}
	if code := get(t, ts, "/v1/params", &params); code != 200 {
		t.Fatalf("params: %d", code)
	}
	if f := params.Formula; f.BaseGas != 650_000 || f.GasPerChunk != 45_000 || f.ChunkBytes != 262144 || f.UtiaPerGas != 1 {
		t.Fatalf("formula: %+v", f)
	}
	// The old settlement is in 30d, not in 24h.
	var m30 struct {
		Settlements int64
		Hourly      []map[string]any `json:"hourly"`
	}
	get(t, ts, "/v1/market?window=30d", &m30)
	if m30.Settlements != 3 {
		t.Fatalf("30d settlements: %d", m30.Settlements)
	}
	if m30.Hourly != nil {
		t.Fatalf("a 30-day window carries hourly buckets: %d", len(m30.Hourly))
	}
	if code := get(t, ts, "/v1/market?window=1y", nil); code != 400 {
		t.Fatalf("bad window: %d", code)
	}
}

// A day is charted by the hour as longer periods are by the day: over the
// same settlements the hours' fees, bytes and settlements add up to the
// days' and to the period's.
func TestHourlySumsToTheDays(t *testing.T) {
	dir := t.TempDir()
	st, err := store.Open(filepath.Join(dir, "observer.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	now := time.Now()
	// Seven publishers, the n-th with n settlements of varied sizes spread
	// over the last day, so the top five by fees are the last five and the
	// first two fold into "other".
	pubs := make([]string, 7)
	for i := range pubs {
		b := make([]byte, 20)
		for j := range b {
			b[j] = byte(i + 1)
		}
		if pubs[i], err = bech32.ConvertAndEncode("celestia", b); err != nil {
			t.Fatal(err)
		}
	}
	var ps []scan.Payment
	for i, pub := range pubs {
		for k := 0; k <= i; k++ {
			n := len(ps)
			size := uint32(65536 * (1 + (i+3*k)%9))
			fee := 650_000 + 45_000*uint64((size+262143)/262144)
			at := now.Add(-time.Duration(1+(5*i+3*k)%22)*time.Hour - time.Duration(7*k)*time.Minute)
			ps = append(ps, scan.Payment{SchemaVersion: 1, DedupeKey: fmt.Sprintf("s%d", n), Kind: "settlement", Height: int64(100 + n), Time: at,
				TxHash: fmt.Sprintf("tx%d", n), Publisher: pub, Processor: pub, PromiseHash: fmt.Sprintf("p%d", n), BlobSize: size, GasUnits: fee, Denom: "utia", AmountUtia: fee})
		}
	}
	// a timeout, in the days' timeouts and in no split; a settlement two days
	// old, in neither
	ps = append(ps,
		scan.Payment{SchemaVersion: 1, DedupeKey: "t", Kind: "timeout", Height: 90, Time: now.Add(-3 * time.Hour), TxHash: "tt",
			Publisher: pubs[6], Processor: pubs[0], PromiseHash: "tp", BlobSize: 1 << 20, GasUnits: 830_000, Denom: "utia", AmountUtia: 830_000},
		scan.Payment{SchemaVersion: 1, DedupeKey: "old", Kind: "settlement", Height: 80, Time: now.Add(-48 * time.Hour), TxHash: "to",
			Publisher: pubs[0], Processor: pubs[0], PromiseHash: "op", BlobSize: 65536, GasUnits: 695_000, Denom: "utia", AmountUtia: 695_000})
	if r, err := ingest.Payments(st, writePayments(t, dir, ps), now); err != nil || r.Inserted != int64(len(ps)) {
		t.Fatalf("ingest payments: inserted=%d err=%v", r.Inserted, err)
	}
	labels := map[string]api.PublisherLabel{pubs[5]: {Address: pubs[5], Label: "Named publisher", Source: "test"}}
	ts := httptest.NewServer(api.NewWithVantage(st, api.VantageInfo{Name: "test"}, nil, api.WithPublisherLabels(labels)))
	t.Cleanup(ts.Close)

	type slice struct {
		Day, Hour, Publisher, Label string
		Fees                        int64 `json:"fees_utia"`
		Bytes, Settlements          int64
	}
	var m struct {
		Fees        int64   `json:"fees_settled_utia"`
		Bytes       int64   `json:"bytes"`
		Settlements int64   `json:"settlements"`
		Daily       []slice `json:"daily"`
		Hourly      []slice `json:"hourly"`
	}
	if code := get(t, ts, "/v1/market?window=24h", &m); code != 200 {
		t.Fatalf("market: %d", code)
	}
	if m.Settlements != 28 {
		t.Fatalf("%d settlements, want 28", m.Settlements)
	}

	// The hours' fees, bytes and settlements are the days'.
	add := func(rows []slice) (f, b, n int64) {
		for _, r := range rows {
			f, b, n = f+r.Fees, b+r.Bytes, n+r.Settlements
		}
		return
	}
	df, db, dn := add(m.Daily)
	hf, hb, hn := add(m.Hourly)
	if hf != df || hb != db || hn != dn || hf != m.Fees || hb != m.Bytes || hn != m.Settlements {
		t.Fatalf("hours sum to %d utia, %d bytes, %d settlements; days to %d, %d, %d; the period %d, %d, %d",
			hf, hb, hn, df, db, dn, m.Fees, m.Bytes, m.Settlements)
	}
}

func TestPublishersListAndDetail(t *testing.T) {
	ts, _ := marketServer(t, nil)
	var list struct {
		Publishers []struct {
			Publisher   string   `json:"publisher"`
			Settlements int64    `json:"settlements"`
			FeesShare   *float64 `json:"fees_share"`
			Timeouts    int64    `json:"timeouts"`
			FirstSeen   string   `json:"first_seen_at"`
			Escrow      *struct {
				Found   bool  `json:"found"`
				Balance int64 `json:"balance_utia"`
			} `json:"escrow"`
		} `json:"publishers"`
		Count int `json:"count"`
	}
	if code := get(t, ts, "/v1/publishers?window=24h", &list); code != 200 {
		t.Fatalf("publishers: %d", code)
	}
	if list.Count != 2 {
		t.Fatalf("want 2 publishers, got %+v", list)
	}
	if list.Publishers[0].Publisher != otherPublisher || list.Publishers[0].Timeouts != 1 {
		t.Fatalf("ordering by fees, timeouts: %+v", list.Publishers[0])
	}
	if list.Publishers[0].Escrow == nil || list.Publishers[0].Escrow.Found {
		t.Fatalf("a polled but absent escrow must be reported as not found: %+v", list.Publishers[0].Escrow)
	}
	if list.Publishers[1].Escrow == nil || list.Publishers[1].Escrow.Balance != 5_999_304_000 {
		t.Fatalf("escrow: %+v", list.Publishers[1].Escrow)
	}
	if list.Publishers[1].FirstSeen == "" {
		t.Fatal("first_seen_at must be filled from the whole history")
	}

	var one struct {
		Publisher struct {
			Publisher string `json:"publisher"`
			Fees      int64  `json:"fees_utia"`
		} `json:"publisher"`
		Windows []struct {
			Window      struct{ Name string } `json:"window"`
			Settlements int64                 `json:"settlements"`
		} `json:"windows"`
		Payments []map[string]any `json:"recent_payments"`
	}
	if code := get(t, ts, "/v1/publishers/"+samplePublisher+"?window=24h", &one); code != 200 {
		t.Fatalf("publisher detail: %d", code)
	}
	if one.Publisher.Fees != 695_000 || len(one.Windows) != 4 || len(one.Payments) != 4 {
		t.Fatalf("detail: %+v", one)
	}
	// the same account in upper case, which bech32 allows
	var upper struct {
		Publisher struct {
			Publisher string `json:"publisher"`
		} `json:"publisher"`
	}
	if code := get(t, ts, "/v1/publishers/"+strings.ToUpper(samplePublisher)+"?window=24h", &upper); code != 200 || upper.Publisher.Publisher != samplePublisher {
		t.Fatalf("in upper case: %d %+v", code, upper)
	}
	for _, w := range one.Windows {
		if w.Window.Name == "24h" && w.Settlements != 1 {
			t.Fatalf("24h span: %+v", w)
		}
	}
	// A payment does not repeat the page's publisher, and the blob it paid
	// for is the blob's own page; the publisher's blobs are
	// /v1/blobs?publisher=, which its page lists them by.
	for _, p := range one.Payments {
		for _, k := range []string{"publisher", "processor", "namespace", "blob_size"} {
			if _, ok := p[k]; ok {
				t.Errorf("a recent payment carries %s: %v", k, p)
			}
		}
	}
	var whole map[string]json.RawMessage
	get(t, ts, "/v1/publishers/"+samplePublisher+"?window=24h", &whole)
	for _, k := range []string{"recent_blobs", "recent_blobs_truncated"} {
		if _, ok := whole[k]; ok {
			t.Errorf("the publisher's page still carries %s", k)
		}
	}
	var blobs struct {
		Blobs []struct {
			Charge *struct {
				Fee int64 `json:"fee_utia"`
			} `json:"charge"`
		} `json:"blobs"`
	}
	if code := get(t, ts, "/v1/blobs?publisher="+samplePublisher, &blobs); code != 200 || len(blobs.Blobs) == 0 ||
		blobs.Blobs[len(blobs.Blobs)-1].Charge == nil || blobs.Blobs[len(blobs.Blobs)-1].Charge.Fee != 695_000 {
		t.Fatalf("the settled sample blob must carry its charge: %d %+v", code, blobs.Blobs)
	}
	// A publisher with history but nothing in the window still resolves.
	var quiet struct {
		Publisher struct{ Settlements int64 } `json:"publisher"`
	}
	if code := get(t, ts, "/v1/publishers/"+otherPublisher+"?window=24h", &quiet); code != 200 {
		t.Fatalf("quiet publisher: %d", code)
	}
	// A publisher with only a deposit has no settlement in any span: every
	// SUM over an empty CASE is NULL in SQLite and must scan as zero.
	dir := t.TempDir()
	st2, err := store.Open(filepath.Join(dir, "o.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st2.Close()
	if _, err := ingest.Payments(st2, writePayments(t, dir, []scan.Payment{{SchemaVersion: 1, DedupeKey: "d", Kind: "deposit", Height: 1, Time: time.Now(), Publisher: samplePublisher, Denom: "utia", AmountUtia: 5}}), time.Now()); err != nil {
		t.Fatal(err)
	}
	ts2 := httptest.NewServer(api.New(st2, "t"))
	defer ts2.Close()
	if code := get(t, ts2, "/v1/publishers/"+samplePublisher, &quiet); code != 200 {
		t.Fatalf("deposit-only publisher: %d", code)
	}
	if code := get(t, ts, "/v1/publishers/celestia1nobody?window=24h", nil); code != 400 {
		t.Fatalf("malformed address: %d", code)
	}
	if code := get(t, ts, "/v1/publishers/"+timeoutOperator+"?window=24h", nil); code != 404 {
		t.Fatalf("unknown publisher: %d", code)
	}
	if code := get(t, ts, "/v1/publishers/"+timeoutValoper, nil); code != 400 {
		t.Fatalf("valoper prefix: %d", code)
	}
}

func TestBlobChargeAndValidatorTimeouts(t *testing.T) {
	ts, _ := marketServer(t, nil)
	pubs, err := scan.LoadPublications(filepath.Join(sampleDir, "publications.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	var blob struct {
		Blob struct {
			// the account the chain charged, where a payment is on record
			Publisher string `json:"publisher"`
			Charge    *struct {
				Fee      int64 `json:"fee_utia"`
				Settled  bool  `json:"settled"`
				TimedOut bool  `json:"timed_out"`
			} `json:"charge"`
		} `json:"blob"`
	}
	if code := get(t, ts, "/v1/blobs/"+pubs[0].PromiseHash, &blob); code != 200 {
		t.Fatalf("blob: %d", code)
	}
	if c := blob.Blob.Charge; c == nil || c.Fee != 695_000 || !c.Settled || c.TimedOut || blob.Blob.Publisher != samplePublisher {
		t.Fatalf("charge: %+v, publisher %s", blob.Blob.Charge, blob.Blob.Publisher)
	}
	if code := get(t, ts, "/v1/blobs/"+pubs[1].PromiseHash, &blob); code != 200 {
		t.Fatalf("blob: %d", code)
	}
	if blob.Blob.Charge != nil {
		t.Fatalf("a publication without a recorded payment must have a null charge, got %+v", blob.Blob.Charge)
	}

	// every listed validator, by the figure its page shows
	var vals struct {
		Validators []struct {
			Address string `json:"address"`
		} `json:"validators"`
	}
	if code := get(t, ts, "/v1/validators?window=24h", &vals); code != 200 {
		t.Fatalf("validators: %d", code)
	}
	found := false
	for _, l := range vals.Validators {
		var det struct {
			Validator struct {
				Address  string `json:"address"`
				Timeouts int64  `json:"timeouts_enforced"`
			} `json:"validator"`
		}
		if code := get(t, ts, "/v1/validators/"+l.Address+"?window=24h", &det); code != 200 {
			t.Fatalf("validator %s: %d", l.Address, code)
		}
		v := det.Validator
		if v.Address == sampleValidator {
			found = true
			if v.Timeouts != 1 {
				t.Fatalf("the operator whose account submitted the timeout must show it: %+v", v)
			}
		} else if v.Timeouts != 0 {
			t.Fatalf("validator %s did not submit a timeout: %+v", v.Address, v)
		}
	}
	if !found {
		t.Fatal("sample validator missing from the table")
	}
}

func TestLoadPublisherLabels(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "publishers.yaml")
	if err := os.WriteFile(path, []byte("publishers:\n  - address: "+samplePublisher+"\n    label: Sentinel\n    source: this observer\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	m, err := api.LoadPublisherLabels(path)
	if err != nil || len(m) != 1 || m[samplePublisher].Label != "Sentinel" {
		t.Fatalf("labels: %v %v", m, err)
	}
	if m, err := api.LoadPublisherLabels(filepath.Join(dir, "missing.yaml")); err != nil || len(m) != 0 {
		t.Fatalf("missing file must be an empty registry: %v %v", m, err)
	}
	if err := os.WriteFile(path, []byte("publishers:\n  - address: nope\n    label: x\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := api.LoadPublisherLabels(path); err == nil {
		t.Fatal("a non-bech32 address must be rejected")
	}
	if err := os.WriteFile(path, []byte("publishers:\n  - address: "+samplePublisher+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := api.LoadPublisherLabels(path); err == nil {
		t.Fatal("a label-less entry must be rejected")
	}
}

// ?as_of= on the market route: pinned, honoured in both directions, and
// never written into the shared snapshot. The cache is keyed on the
// window's name alone, so handing it a pinned window filed a month-old
// answer under "30d" and served it to every later reader, on disk too.
func TestMarketAsOfIsPinnedAndLeavesTheSnapshotAlone(t *testing.T) {
	ts, _ := marketServer(t, nil)
	fetch := func(q string) (settlements int64, fees int64, asOf bool, end time.Time) {
		t.Helper()
		var m struct {
			Settlements int64 `json:"settlements"`
			Fees        int64 `json:"fees_settled_utia"`
			Window      struct {
				AsOf bool      `json:"as_of"`
				End  time.Time `json:"end"`
			} `json:"window"`
		}
		if code := get(t, ts, q, &m); code != 200 {
			t.Fatalf("%s: HTTP %d", q, code)
		}
		return m.Settlements, m.Fees, m.Window.AsOf, m.Window.End
	}

	// Live: the two settlements from two hours ago and the one from ten days ago.
	live, liveFees, asOf, _ := fetch("/v1/market?window=30d")
	if live != 3 || asOf {
		t.Fatalf("live 30d: settlements=%d as_of=%v, want 3/false", live, asOf)
	}

	// Pinned five days back: only the ten-day-old settlement is in range.
	pin := time.Now().UTC().Add(-5 * 24 * time.Hour).Truncate(time.Second)
	n, fees, asOf, end := fetch("/v1/market?window=30d&as_of=" + pin.Format(time.RFC3339))
	if !asOf {
		t.Fatal("a pinned request was answered with an unpinned window")
	}
	if !end.Equal(pin) {
		t.Fatalf("window end = %s, want the pin %s", end, pin)
	}
	if n != 1 || fees != 695_000 {
		t.Fatalf("pinned 30d: settlements=%d fees=%d, want 1/695000 (payments after the pin must be excluded)", n, fees)
	}

	// The live answer is unchanged: the pinned one was never stored.
	after, afterFees, asOf, _ := fetch("/v1/market?window=30d")
	if after != live || afterFees != liveFees || asOf {
		t.Fatalf("after one pinned request the live answer became settlements=%d fees=%d as_of=%v, want %d/%d/false",
			after, afterFees, asOf, live, liveFees)
	}
}

// A pinned market or publisher answer names what it leaves as of now, as
// the validator routes do; an unpinned one has nothing to name.
func TestMarketAndPublisherAsOfNotes(t *testing.T) {
	ts, _ := marketServer(t, nil)
	pin := time.Now().UTC().Add(-time.Hour).Truncate(time.Second).Format(time.RFC3339)
	for path, want := range map[string]string{
		"/v1/market?window=30d":                                          "",
		"/v1/market?window=30d&as_of=" + pin:                             "escrow_held_utia",
		"/v1/publishers?window=30d":                                      "",
		"/v1/publishers?window=30d&as_of=" + pin:                         "pending_withdrawals",
		"/v1/publishers/" + samplePublisher + "?window=30d":              "",
		"/v1/publishers/" + samplePublisher + "?window=30d&as_of=" + pin: "recent_payments",
	} {
		var body struct {
			Note string `json:"as_of_note"`
		}
		if code := get(t, ts, path, &body); code != 200 {
			t.Fatalf("%s: HTTP %d", path, code)
		}
		if (want == "") != (body.Note == "") || !strings.Contains(body.Note, want) {
			t.Errorf("%s: as_of_note %q, want one naming %q", path, body.Note, want)
		}
	}
}

// The escrow total is the module account's balance, read by the collector,
// and it is published beside the sum over known publishers rather than in
// place of it: the two differ exactly by the accounts nobody saw publish.
func TestMarketEscrowTotalIsTheModuleBalance(t *testing.T) {
	ts, st := marketServer(t, nil)
	type body struct {
		Total *int64  `json:"escrow_total_utia"`
		At    *string `json:"escrow_total_at"`
		Held  int64   `json:"escrow_held_utia"`
	}
	// as_of computes the window on the spot, so the answer is not a snapshot
	// taken before the meta row below was written
	pinned := func() string { return "/v1/market?window=30d&as_of=" + time.Now().UTC().Format(time.RFC3339) }
	var before body
	if code := get(t, ts, pinned(), &before); code != 200 {
		t.Fatalf("market: %d", code)
	}
	if before.Total != nil {
		t.Fatalf("no module balance read yet, but a total of %d was published", *before.Total)
	}
	now := time.Now()
	if err := st.SetMeta("escrow_module_utia", "123456789", now); err != nil {
		t.Fatal(err)
	}
	if err := st.SetMeta("escrow_module_polled_at", store.TS(now), now); err != nil {
		t.Fatal(err)
	}
	var after body
	if code := get(t, ts, pinned(), &after); code != 200 {
		t.Fatalf("market: %d", code)
	}
	if after.Total == nil || *after.Total != 123456789 || after.At == nil {
		t.Fatalf("total %v at %v, want 123456789 with its read time", after.Total, after.At)
	}
	if after.Held != before.Held {
		t.Errorf("the known-publisher sum moved (%d -> %d); the total must sit beside it", before.Held, after.Held)
	}
}

// Blobs counts what the settlements paid for, by BlobID (blob_version ||
// commitment): a blob uploaded and paid for twice is one blob and two
// settlements, and the same commitment under another version is another blob.
func TestMarketBlobsByCommitment(t *testing.T) {
	dir := t.TempDir()
	st, err := store.Open(filepath.Join(dir, "observer.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	now := time.Now().UTC().Truncate(time.Second)
	at := now.Add(-time.Hour)
	var ps []scan.Payment
	for i, c := range []struct {
		hash, commitment string
		version          uint32
	}{{"p1", "same", 0}, {"p2", "same", 0}, {"p3", "other", 0}, {"p4", "same", 1}} {
		pub := scan.Publication{
			SchemaVersion: scan.AttestationSchemaVersion, PromiseHash: c.hash, SettlementHeight: int64(100 + i), SettlementTime: at,
			SettlementTxHash: "tx" + c.hash, MustServeUntil: at.Add(time.Hour), RecordedAt: at, Signer: samplePublisher,
			Promise: scan.PromiseFields{ChainID: "t", Height: int64(99 + i), Commitment: c.commitment, BlobVersion: c.version, CreationTimestamp: at, BlobSize: 262144},
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
		ps = append(ps, scan.Payment{SchemaVersion: 1, DedupeKey: "tx" + c.hash + ":0", Kind: "settlement", Height: int64(100 + i), Time: at,
			TxHash: "tx" + c.hash, Publisher: samplePublisher, Processor: samplePublisher, PromiseHash: c.hash, BlobSize: 262144, Denom: "utia", AmountUtia: 695_000})
	}
	if r, err := ingest.Payments(st, writePayments(t, dir, ps), now); err != nil || r.Inserted != int64(len(ps)) {
		t.Fatalf("ingest payments: inserted=%d err=%v", r.Inserted, err)
	}
	ts := httptest.NewServer(api.NewWithVantage(st, api.VantageInfo{Name: "test"}, nil))
	t.Cleanup(ts.Close)
	var m struct {
		Settlements int64 `json:"settlements"`
		Blobs       int64 `json:"blobs"`
	}
	if code := get(t, ts, "/v1/market?window=24h", &m); code != 200 {
		t.Fatalf("market: %d", code)
	}
	if m.Settlements != 4 || m.Blobs != 3 {
		t.Fatalf("settlements %d blobs %d, want 4 and 3", m.Settlements, m.Blobs)
	}
}

// The namespaces the window's settlements used, and all on record.
func TestMarketNamespaces(t *testing.T) {
	dir := t.TempDir()
	st, err := store.Open(filepath.Join(dir, "observer.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	now := time.Now().UTC().Truncate(time.Second)
	var ps []scan.Payment
	for i, c := range []struct {
		hash, ns string
		size     uint32
		ago      time.Duration
	}{{"old", "ns0", 1 << 27, 3 * 24 * time.Hour}, {"a", "ns1", 1 << 20, time.Hour}, {"b", "ns1", 1 << 22, time.Hour}, {"c", "ns2", 1 << 18, time.Hour}} {
		ps = append(ps, scan.Payment{SchemaVersion: 1, DedupeKey: "tx" + c.hash + ":0", Kind: "settlement", Height: int64(100 + i), Time: now.Add(-c.ago),
			TxHash: "tx" + c.hash, Publisher: samplePublisher, Processor: samplePublisher, PromiseHash: c.hash, Namespace: c.ns, BlobSize: c.size, Denom: "utia", AmountUtia: 695_000})
	}
	if r, err := ingest.Payments(st, writePayments(t, dir, ps), now); err != nil || r.Inserted != int64(len(ps)) {
		t.Fatalf("ingest payments: inserted=%d err=%v", r.Inserted, err)
	}
	ts := httptest.NewServer(api.NewWithVantage(st, api.VantageInfo{Name: "test"}, nil))
	t.Cleanup(ts.Close)
	var m struct {
		Namespaces      int64 `json:"namespaces"`
		NamespacesTotal int64 `json:"namespaces_total"`
	}
	if code := get(t, ts, "/v1/market?window=24h", &m); code != 200 {
		t.Fatalf("market: %d", code)
	}
	if m.Namespaces != 2 || m.NamespacesTotal != 3 {
		t.Errorf("namespaces %d of %d on record, want 2 of 3", m.Namespaces, m.NamespacesTotal)
	}
}

// The publisher page shows /v1/market's board and /v1/publishers' table side
// by side. Both come from one snapshot, so they describe the same moment: a
// settlement that lands after it moves neither until the snapshot is
// refreshed, and then both at once.
func TestPublishersAndMarketAreOneSnapshot(t *testing.T) {
	ts, st := marketServer(t, nil)
	type board struct {
		Window     struct{ End time.Time } `json:"window"`
		ComputedAt string                  `json:"computed_at"`
		Fees       int64                   `json:"fees_settled_utia"`
		Top        []struct {
			Publisher string   `json:"publisher"`
			FeesShare *float64 `json:"fees_share"`
		} `json:"top_publishers"`
		Publishers json.RawMessage `json:"publishers"`
	}
	type table struct {
		Window     struct{ End time.Time } `json:"window"`
		ComputedAt string                  `json:"computed_at"`
		Publishers []struct {
			Publisher string   `json:"publisher"`
			FeesUtia  int64    `json:"fees_utia"`
			FeesShare *float64 `json:"fees_share"`
		} `json:"publishers"`
	}
	read := func() (board, table) {
		t.Helper()
		var b board
		var tb table
		if code := get(t, ts, "/v1/market?window=7d", &b); code != 200 {
			t.Fatalf("market: %d", code)
		}
		if code := get(t, ts, "/v1/publishers?window=7d", &tb); code != 200 {
			t.Fatalf("publishers: %d", code)
		}
		return b, tb
	}
	agree := func(b board, tb table) {
		t.Helper()
		if b.Publishers != nil {
			t.Fatalf("/v1/market carries the publisher list: %s", b.Publishers)
		}
		if b.ComputedAt == "" || b.ComputedAt != tb.ComputedAt || !b.Window.End.Equal(tb.Window.End) {
			t.Fatalf("market computed %s over a window ending %s, publishers %s ending %s: not one snapshot",
				b.ComputedAt, b.Window.End, tb.ComputedAt, tb.Window.End)
		}
		var sum int64
		share := map[string]float64{}
		for _, p := range tb.Publishers {
			sum += p.FeesUtia
			if p.FeesShare != nil {
				share[p.Publisher] = *p.FeesShare
			}
		}
		if sum != b.Fees {
			t.Fatalf("the table's fees add up to %d, the board says %d", sum, b.Fees)
		}
		for _, p := range b.Top {
			if p.FeesShare == nil || share[p.Publisher] != *p.FeesShare {
				t.Fatalf("%s: board share %v, table share %v", p.Publisher, p.FeesShare, share[p.Publisher])
			}
		}
	}
	b1, t1 := read()
	agree(b1, t1)

	// A settlement lands after the snapshot: both still show the snapshot.
	settle := time.Now().Add(-time.Minute)
	ps := []scan.Payment{{SchemaVersion: 1, DedupeKey: "late", Kind: "settlement", Height: 99, Time: settle, TxHash: "ff",
		Publisher: samplePublisher, Processor: samplePublisher, PromiseHash: "late", BlobSize: 1 << 20, GasUnits: 830_000, Denom: "utia", AmountUtia: 830_000}}
	if r, err := ingest.Payments(st, writePayments(t, t.TempDir(), ps), time.Now()); err != nil || r.Inserted != 1 {
		t.Fatalf("ingest: %+v %v", r, err)
	}
	b2, t2 := read()
	agree(b2, t2)
	if b2.ComputedAt == b1.ComputedAt && b2.Fees != b1.Fees {
		t.Fatalf("the board moved without its snapshot: %d → %d", b1.Fees, b2.Fees)
	}
}

// A blob row publishes what a page reads: the charge's fee and outcome (its
// gas is the fee at one utia per gas, the timeout's submitter is the
// payment's), not how many validators served it (each assignment says), and
// the promise's creation time only on the blob's own page, where a reader
// checks its deadline.
func TestBlobRowsPublishWhatIsRead(t *testing.T) {
	ts, _ := marketServer(t, nil)
	pubs, err := scan.LoadPublications(filepath.Join(sampleDir, "publications.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	type row = map[string]json.RawMessage
	absent := func(where string, r row, keys ...string) {
		t.Helper()
		for _, k := range keys {
			if _, ok := r[k]; ok {
				t.Errorf("%s carries %s: %s", where, k, r[k])
			}
		}
	}
	var list struct {
		Blobs []row `json:"blobs"`
	}
	if code := get(t, ts, "/v1/blobs", &list); code != 200 || len(list.Blobs) == 0 {
		t.Fatalf("blobs: %d, %d rows", code, len(list.Blobs))
	}
	for _, b := range list.Blobs {
		absent("a listed blob", b, "creation_timestamp")
		var c, rc row
		_ = json.Unmarshal(b["charge"], &c)
		_ = json.Unmarshal(b["reconstructable"], &rc)
		absent("a listed blob's charge", c, "gas_units", "processor")
		absent("a listed blob's reading", rc, "served_by_validators")
	}
	var one struct {
		Blob row `json:"blob"`
	}
	if code := get(t, ts, "/v1/blobs/"+pubs[0].PromiseHash, &one); code != 200 {
		t.Fatalf("blob: %d", code)
	}
	if _, ok := one.Blob["creation_timestamp"]; !ok {
		t.Error("the blob's own page has no creation_timestamp")
	}
	var c row
	_ = json.Unmarshal(one.Blob["charge"], &c)
	absent("the blob's charge", c, "gas_units", "processor")
}
