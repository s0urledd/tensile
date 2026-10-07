package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/cosmos/cosmos-sdk/types/bech32"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/scan"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
)

// /v1/market carries every blob on record by Tensile's reading, network-wide
// and over the whole record, the same on every window: what the overview
// counted by downloading every publisher's row of /v1/publishers?window=all
// once a minute to add up three numbers. It is the publishers' counts summed
// and the blobs no publisher is known for with them, taken with the market
// snapshot and never computed per request.
func TestMarketCarriesTheNetworksReadingTotals(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "observer.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	full := make([]int, 40)
	for i := range full {
		full[i] = i
	}
	pub := func(b byte) string {
		raw := make([]byte, 20)
		for i := range raw {
			raw[i] = b
		}
		a, err := bech32.ConvertAndEncode("celestia", raw)
		if err != nil {
			t.Fatal(err)
		}
		return a
	}
	alice, bob := pub(1), pub(2)
	pay := func(i int, h, who string) {
		t.Helper()
		if _, err := st.UpsertPayment(scan.Payment{SchemaVersion: 1, DedupeKey: fmt.Sprintf("s%d", i), Kind: "settlement", Height: int64(i + 2),
			Time: time.Now(), Publisher: who, PromiseHash: h, Namespace: "ns", BlobSize: 1024, Denom: "utia", AmountUtia: 1}, []byte("{}")); err != nil {
			t.Fatal(err)
		}
	}
	// alice: two Available; bob: one Unavailable; and one no payment and
	// no readable key names a publisher for, its window closed unread.
	for i := 0; i < 2; i++ {
		pay(i, writeBlob(t, st, i, blobCase{needed: 20, total: 80, points: 1, complete: true, over: true,
			vals: []valRows{{addr: fmt.Sprintf("a%d", i), rows: full[:20], attested: 1, served: true}}}), alice)
	}
	pay(2, writeBlob(t, st, 2, blobCase{needed: 30, total: 160, points: 1, over: true, vals: []valRows{
		{addr: "o1", rows: full[:20], attested: 1, served: true},
		{addr: "o2", rows: full[20:30], attested: 1, served: false},
	}}), bob)
	writeBlob(t, st, 3, blobCase{needed: 20, total: 80, over: true})

	srv := New(st, "test")
	ts := httptest.NewServer(srv)
	defer func() { ts.Close(); srv.Close() }()
	type totals struct {
		Available   int64 `json:"available"`
		Unavailable int64 `json:"unavailable"`
		NotRead     int64 `json:"not_read"`
	}
	fetch := func(path string, into any) {
		t.Helper()
		resp, err := http.Get(ts.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != 200 {
			t.Fatalf("%s: %d", path, resp.StatusCode)
		}
		if err := json.NewDecoder(resp.Body).Decode(into); err != nil {
			t.Fatal(err)
		}
	}
	want := totals{Available: 2, Unavailable: 1, NotRead: 1}
	for _, w := range []string{"24h", "7d", "30d", "all"} {
		var m struct {
			Readings *totals `json:"readings"`
		}
		fetch("/v1/market?window="+w, &m)
		if m.Readings == nil || *m.Readings != want {
			t.Errorf("/v1/market?window=%s readings %+v, want %+v", w, m.Readings, want)
		}
	}
	// A pinned answer, computed on demand, carries the window's snapshot's.
	var pinned struct {
		Readings *totals `json:"readings"`
	}
	fetch("/v1/market?window=24h&as_of="+time.Now().UTC().Format(time.RFC3339), &pinned)
	if pinned.Readings == nil || *pinned.Readings != want {
		t.Errorf("/v1/market as_of readings %+v, want %+v", pinned.Readings, want)
	}
	// the publishers' own counts, summed, are the same but for the blob no
	// publisher row can hold
	var list struct {
		Publishers []struct {
			Readings *readingCounts `json:"readings"`
		} `json:"publishers"`
	}
	fetch("/v1/publishers?window=all", &list)
	var sum totals
	for _, p := range list.Publishers {
		if p.Readings != nil {
			sum.Available += p.Readings.Available
			sum.Unavailable += p.Readings.Unavailable
			sum.NotRead += p.Readings.NotRead
		}
	}
	if sum != (totals{Available: 2, Unavailable: 1}) {
		t.Errorf("the publishers' counts sum to %+v", sum)
	}
}
