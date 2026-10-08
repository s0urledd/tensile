package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"testing"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
)

// /v1/probes reads no more than it must for a filter no index serves. A
// class this observer does not publish is a 400, not a read of every row
// for it. served=no and class=RETENTION_UNVERIFIED with nothing to narrow
// them read probeScanSpan of readings, which the answer names, and a since
// further back is a 400; a validator, a blob or a reading narrows them, and
// then any span is read. At most probeScanSlots such reads run at once,
// past which the answer is a 429; the narrowed ones and a class the index
// serves are not counted. Before, each of these read the whole table, and
// nothing bounded how many did at once.
func TestProbesReadsNoIndexServesAreBoundedAndRationed(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "observer.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	s := NewWithVantage(st, VantageInfo{Name: "test"}, nil)
	defer s.Close()
	get := func(path string) (int, map[string]any) {
		t.Helper()
		rec := httptest.NewRecorder()
		s.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		var body map[string]any
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatalf("%s: %d %s", path, rec.Code, rec.Body.String())
		}
		return rec.Code, body
	}
	rfc := func(tm time.Time) string { return url.QueryEscape(tm.UTC().Format(time.RFC3339)) }
	v := fmt.Sprintf("%040x", 0xab)
	month := rfc(time.Now().Add(-30 * 24 * time.Hour))

	if code, body := get("/v1/probes?class=ZZZ"); code != 400 {
		t.Errorf("a class this observer does not publish: %d %v", code, body)
	}
	if code, body := get("/v1/probes?class=identity_mismatch"); code != 200 || body["since"] != nil {
		t.Errorf("a class it publishes, served by its index: %d %v", code, body)
	}

	code, body := get("/v1/probes?served=no")
	since, _ := body["since"].(string)
	at, err := time.Parse(time.RFC3339, since)
	if code != 200 || err != nil {
		t.Fatalf("served=no: %d %v", code, body)
	}
	if d := time.Since(at) - probeScanSpan; d < -time.Minute || d > time.Minute {
		t.Errorf("served=no reads from %s, want %s back", since, probeScanSpan)
	}
	before := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	if code, body := get("/v1/probes?class=RETENTION_UNVERIFIED&before=" + rfc(before)); code != 200 || body["since"] != before.Add(-probeScanSpan).Format(time.RFC3339) {
		t.Errorf("class=RETENTION_UNVERIFIED before %s: %d, since %v, want the span back from before", before, code, body["since"])
	}
	for _, path := range []string{"/v1/probes?served=no&since=" + month, "/v1/probes?class=retention_unverified&since=" + month} {
		if code, body := get(path); code != 400 {
			t.Errorf("%s: %d %v, want a 400 for a since past the span", path, code, body)
		}
	}
	if code, body := get("/v1/probes?served=no&validator=" + v + "&since=" + month); code != 200 || body["since"] != nil {
		t.Errorf("served=no for one validator over a month: %d %v", code, body)
	}

	for i := 0; i < probeScanSlots; i++ {
		s.probeScans <- struct{}{}
	}
	if code, body := get("/v1/probes?served=no"); code != 429 {
		t.Errorf("served=no with every slot taken: %d %v", code, body)
	}
	for _, path := range []string{"/v1/probes?served=no&validator=" + v, "/v1/probes?class=FAULT"} {
		if code, body := get(path); code != 200 {
			t.Errorf("%s with every slot taken: %d %v, want it answered: it takes none", path, code, body)
		}
	}
	for i := 0; i < probeScanSlots; i++ {
		<-s.probeScans
	}
	if code, body := get("/v1/probes?served=no"); code != 200 {
		t.Errorf("served=no with the slots free again: %d %v", code, body)
	}
}
