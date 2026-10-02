package api_test

import (
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/cosmos/cosmos-sdk/types/bech32"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/scan"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/api"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/ingest"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
)

type pubNamespace struct {
	Namespace   string `json:"namespace"`
	Settlements int64  `json:"settlements"`
	Bytes       int64  `json:"bytes"`
}

type pubFields struct {
	Publisher       string          `json:"publisher"`
	FirstSeen       string          `json:"first_seen_at"`
	Namespaces      []pubNamespace  `json:"namespaces"`
	NamespacesTotal int64           `json:"namespaces_total"`
	FirstSettlement json.RawMessage `json:"first_settlement_at"`
	LastSettlement  json.RawMessage `json:"last_settlement_at"`
	Readings        *pubReadings    `json:"readings"`
}

type pubReadings struct {
	Available         int64 `json:"available"`
	Unavailable       int64 `json:"unavailable"`
	InRetentionWindow int64 `json:"in_retention_window"`
	NotRead           int64 `json:"not_read"`
}

// A publisher row names the namespaces its settlements in the period used,
// the most used first and at most ten, with how many there were; its first
// and last settlement span the whole record whatever the period, and are
// null for an account that never settled (where first_seen_at, any escrow
// movement, is not); and it carries Tensile's reading of its blobs. The
// publisher page gets the same row, and each of its spans the namespaces of
// that span.
func TestPublisherRowsNameNamespacesAndSettlements(t *testing.T) {
	dir := t.TempDir()
	st, err := store.Open(filepath.Join(dir, "observer.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	const (
		pubA = samplePublisher
		pubB = otherPublisher
		pubC = timeoutOperator // a deposit, never a settlement
	)
	pubD, err := bech32.ConvertAndEncode("celestia", []byte("dddddddddddddddddddd")) // settled ten days ago only
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Second)
	n := 0
	pay := func(kind, publisher, ns string, size uint32, at time.Time) scan.Payment {
		n++
		return scan.Payment{SchemaVersion: 1, DedupeKey: fmt.Sprintf("p%d", n), Kind: kind, Height: int64(n), Time: at, TxHash: fmt.Sprintf("tx%d", n),
			Publisher: publisher, PromiseHash: fmt.Sprintf("h%d", n), Namespace: ns, BlobSize: size, Denom: "utia", AmountUtia: 1000}
	}
	ps := []scan.Payment{
		pay("deposit", pubA, "", 0, now.Add(-11*24*time.Hour)),
		pay("settlement", pubA, "ns1", 100, now.Add(-10*24*time.Hour)),
		pay("settlement", pubA, "ns1", 200, now.Add(-2*time.Hour)),
		pay("settlement", pubA, "ns2", 300, now.Add(-time.Hour)),
		pay("settlement", pubA, "ns1", 50, now.Add(-30*time.Minute)),
		pay("deposit", pubC, "", 0, now.Add(-time.Hour)),
		pay("settlement", pubD, "nsd", 10, now.Add(-10*24*time.Hour)),
	}
	// twelve namespaces: nsb00 three times, nsb01 twice, the rest once each,
	// the later the namespace the more recently
	for i := 0; i < 12; i++ {
		times := map[int]int{0: 3, 1: 2}[i]
		if times == 0 {
			times = 1
		}
		for k := 0; k < times; k++ {
			ps = append(ps, pay("settlement", pubB, fmt.Sprintf("nsb%02d", i), 100, now.Add(-time.Duration(30-i+k)*time.Minute)))
		}
	}
	if r, err := ingest.Payments(st, writePayments(t, dir, ps), now); err != nil || r.Inserted != int64(len(ps)) {
		t.Fatalf("ingest payments: inserted=%d err=%v", r.Inserted, err)
	}
	ts := httptest.NewServer(api.NewWithVantage(st, api.VantageInfo{Name: "test"}, nil))
	t.Cleanup(ts.Close)
	stamp := func(at time.Time) string { return `"` + store.TS(at) + `"` }

	rowsOf := func(window string) map[string]pubFields {
		t.Helper()
		var list struct {
			Publishers []pubFields `json:"publishers"`
		}
		if code := get(t, ts, "/v1/publishers?window="+window, &list); code != 200 {
			t.Fatalf("publishers %s: %d", window, code)
		}
		out := map[string]pubFields{}
		for _, p := range list.Publishers {
			out[p.Publisher] = p
		}
		return out
	}
	day := rowsOf("24h")
	a := day[pubA]
	if fmt.Sprint(a.Namespaces) != "[{ns1 2 250} {ns2 1 300}]" || a.NamespacesTotal != 2 {
		t.Errorf("24h namespaces of A: %v of %d, want ns1 twice then ns2", a.Namespaces, a.NamespacesTotal)
	}
	if string(a.FirstSettlement) != stamp(now.Add(-10*24*time.Hour)) || string(a.LastSettlement) != stamp(now.Add(-30*time.Minute)) {
		t.Errorf("A's settlements span %s to %s, want the whole record whatever the period", a.FirstSettlement, a.LastSettlement)
	}
	if a.FirstSeen != store.TS(now.Add(-11*24*time.Hour)) {
		t.Errorf("first_seen_at %s, want any escrow movement: the deposit before the first settlement", a.FirstSeen)
	}
	if a.Readings == nil || *a.Readings != (pubReadings{}) {
		t.Errorf("A has no blob on record, so no reading: %+v", a.Readings)
	}
	b := day[pubB]
	var names []string
	for _, ns := range b.Namespaces {
		names = append(names, ns.Namespace)
	}
	if fmt.Sprint(names) != "[nsb00 nsb01 nsb11 nsb10 nsb09 nsb08 nsb07 nsb06 nsb05 nsb04]" || b.NamespacesTotal != 12 {
		t.Errorf("B's namespaces: %v of %d, want the ten most settled, then the most recently used, of 12", names, b.NamespacesTotal)
	}
	c := day[pubC]
	if c.Namespaces == nil || len(c.Namespaces) != 0 || c.NamespacesTotal != 0 || string(c.FirstSettlement) != "null" || string(c.LastSettlement) != "null" || c.Readings == nil {
		t.Errorf("an account that never settled: namespaces %v (%d), settlements %s to %s, readings %v; want [], 0, null, null and a count",
			c.Namespaces, c.NamespacesTotal, c.FirstSettlement, c.LastSettlement, c.Readings)
	}
	if all := rowsOf("all")[pubA]; fmt.Sprint(all.Namespaces) != "[{ns1 3 350} {ns2 1 300}]" {
		t.Errorf("A's namespaces over all: %v", all.Namespaces)
	}

	var one struct {
		Publisher pubFields `json:"publisher"`
		Windows   []struct {
			Window          struct{ Name string } `json:"window"`
			Namespaces      []pubNamespace        `json:"namespaces"`
			NamespacesTotal int64                 `json:"namespaces_total"`
		} `json:"windows"`
	}
	if code := get(t, ts, "/v1/publishers/"+pubA+"?window=24h", &one); code != 200 {
		t.Fatalf("publisher detail: %d", code)
	}
	if fmt.Sprint(one.Publisher.Namespaces) != fmt.Sprint(a.Namespaces) || string(one.Publisher.FirstSettlement) != string(a.FirstSettlement) ||
		one.Publisher.Readings == nil {
		t.Errorf("the publisher page's row is not the list's: %+v", one.Publisher)
	}
	spans := map[string]string{}
	for _, w := range one.Windows {
		spans[w.Window.Name] = fmt.Sprintf("%v/%d", w.Namespaces, w.NamespacesTotal)
	}
	if spans["24h"] != "[{ns1 2 250} {ns2 1 300}]/2" || spans["all"] != "[{ns1 3 350} {ns2 1 300}]/2" {
		t.Errorf("the spans' namespaces: %v", spans)
	}
	// Nothing in the period: the row is the whole record's with the period's
	// figures zeroed, its namespaces among them, and its settlements kept.
	var quiet struct {
		Publisher pubFields `json:"publisher"`
	}
	if code := get(t, ts, "/v1/publishers/"+pubD+"?window=24h", &quiet); code != 200 {
		t.Fatalf("quiet publisher: %d", code)
	}
	if quiet.Publisher.Namespaces == nil || len(quiet.Publisher.Namespaces) != 0 || quiet.Publisher.NamespacesTotal != 0 ||
		string(quiet.Publisher.LastSettlement) != stamp(now.Add(-10*24*time.Hour)) {
		t.Errorf("a publisher quiet in the period: %+v", quiet.Publisher)
	}
}
