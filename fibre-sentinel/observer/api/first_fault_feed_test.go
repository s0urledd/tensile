package api_test

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/probe"
	"github.com/plsgiveup/fibre/fibre-sentinel/internal/scan"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/api"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
)

// The network feed's first FAULT of a validator is its first counted one (a
// blob that could not be reconstructed), whatever else failed beside it:
// not one still settling, none when the first fell before the feed's span,
// and the same probe on a tie however the rows were written.
func TestNetworkFeedFirstFault(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "observer.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	now := time.Now().UTC().Truncate(time.Second)
	addr := func(i int) string { return fmt.Sprintf("%040x", 0xa0+i) }
	// Every blob here needs rows nobody served, so a FAULT on it counts:
	// the blob could not be reconstructed.
	unreadable := func(hash string, at time.Time) {
		pub := scan.Publication{SchemaVersion: scan.AttestationSchemaVersion, PromiseHash: hash, SettlementHeight: 100,
			SettlementTime: at.Add(-3 * time.Hour), MustServeUntil: at.Add(time.Hour), SettlementTxHash: "tx" + hash,
			Promise: scan.PromiseFields{Commitment: "cc", Height: 99, CreationTimestamp: at.Add(-3 * time.Hour)}}
		pub.Assignment.ProtocolParams.OriginalRows, pub.Assignment.ProtocolParams.TotalRows = 4, 16
		raw, _ := json.Marshal(pub)
		if _, err := st.UpsertPublication(pub, raw); err != nil {
			t.Fatal(err)
		}
	}
	fault := func(hash, v string, at time.Time) {
		unreadable(hash, at)
		insertProbe(t, st, hash, v, at, probe.OutcomeNotFound)
	}
	// others fault with v at at: a blob every validator failed
	incident := func(at time.Time, hash string, v string) {
		fault(hash, v, at)
		for i := 10; i < 13; i++ {
			insertProbe(t, st, hash, addr(i), at, probe.OutcomeNotFound)
		}
	}

	// v1: its first FAULT 40 days ago, before the span, and another five
	// days ago: its first is not in the span, so it has no entry.
	v1 := addr(1)
	incident(now.Add(-40*24*time.Hour), "old", v1)
	fault("g1", v1, now.Add(-5*24*time.Hour))
	// v2: 25 FAULTs on blobs every validator failed, then one more: the
	// first of them all is its first.
	v2 := addr(2)
	v2First := now.Add(-10 * 24 * time.Hour)
	for i := 0; i < 25; i++ {
		incident(v2First.Add(time.Duration(i)*time.Hour), fmt.Sprintf("s%02d", i), v2)
	}
	fault("g2", v2, now.Add(-2*24*time.Hour))
	// v3: only a FAULT still settling.
	v3 := addr(3)
	fault("g3", v3, now.Add(-5*time.Minute))
	// v4: two FAULTs at the same moment, the higher hash written first.
	v4 := addr(4)
	v4First := now.Add(-3 * 24 * time.Hour)
	fault("zz", v4, v4First)
	fault("aa", v4, v4First)

	ts := httptest.NewServer(api.New(st, "test"))
	defer ts.Close()
	resp, d := fetchAtom(t, ts, "/v1/feed.atom", nil)
	if resp.StatusCode != 200 {
		t.Fatalf("status %d", resp.StatusCode)
	}
	got := map[string]string{}
	for _, e := range d.Entries {
		if e.Category.Term != "first-fault" {
			continue
		}
		for _, v := range []string{v1, v2, v3, v4, addr(10)} {
			if strings.Contains(e.ID, v) {
				got[v] = e.Updated
			}
		}
	}
	want := map[string]string{
		v2: v2First.Format(time.RFC3339),
		v4: v4First.Format(time.RFC3339),
	}
	for v, at := range want {
		if got[v] != at {
			t.Errorf("%s: first fault at %q, want %q", v, got[v], at)
		}
	}
	if _, ok := got[v3]; ok {
		t.Errorf("a fault still settling was published as the first: %v", got)
	}
	if _, ok := got[v1]; ok {
		t.Errorf("a validator whose first fault fell before the span has an entry: %v", got)
	}

	// The tie goes to the lower hash, in the validator's own feed too (a
	// heartbeat puts the validator on record for it).
	at := store.TS(now.Add(-time.Hour))
	if _, err := st.DB().Exec(`INSERT INTO reachability (dedupe_key, vantage, validator_address, validator_host, height, scheduled_at, started_at,
		dns_ok, tcp_ok, tcp_ms, tls_ok, tls_ms, identity_ok, identity_reason, outcome, raw_error, total_duration_ms, raw_json)
		VALUES ('k', 'test', ?, 'h:7980', 1, ?, ?, 1, 1, 10, 0, 20, 0, '', 'TLS_HANDSHAKE_FAIL', '', 30, '{}')`, v4, at, at); err != nil {
		t.Fatal(err)
	}
	resp, err = ts.Client().Get(ts.URL + "/v1/validators/" + v4 + "/feed.atom")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "hash=aa") || strings.Contains(string(body), "hash=zz") {
		t.Errorf("tie not broken by hash:\n%s", body)
	}
}
