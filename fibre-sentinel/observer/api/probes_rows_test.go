package api_test

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
)

// /v1/probes answers with the readings and leaves out each one's row indices
// and their digest, which are most of its bytes and only a verifier needs;
// ?rows=1 puts them back, with a lower cap on the page. The validator page's
// "Full history" link (limit=1000, no rows) keeps working.
func TestProbesLeaveOutTheRowsUnlessAsked(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "observer.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	t0 := time.Date(2026, 9, 18, 10, 0, 0, 0, time.UTC)
	a := insertPub(t, st, "aaaa", "cc1", t0, map[string][]int{"v1": {0, 1}})
	insertDeferred(t, st, a, "v1", t0.Add(10*time.Minute), []uint32{1, 0}, "shadow_pending: …")
	ts := httptestServer(t, st)

	type page struct {
		Probes []struct {
			Validator  string   `json:"validator_address"`
			RowIndices []uint32 `json:"row_indices"`
		} `json:"probes"`
		RowsIncluded bool `json:"rows_included"`
	}
	var slim page
	if code := get(t, ts, "/v1/probes?blob=aaaa&limit=1000", &slim); code != 200 {
		t.Fatalf("limit=1000, as the validator page's full-history link asks: %d", code)
	}
	if len(slim.Probes) != 1 || slim.Probes[0].Validator != "v1" {
		t.Fatalf("probes: %+v", slim.Probes)
	}
	if slim.RowsIncluded || slim.Probes[0].RowIndices != nil {
		t.Fatalf("the default answer carries the row indices: %+v", slim)
	}

	var full page
	if code := get(t, ts, "/v1/probes?blob=aaaa&rows=1", &full); code != 200 {
		t.Fatalf("rows=1: %d", code)
	}
	if !full.RowsIncluded || len(full.Probes) != 1 || len(full.Probes[0].RowIndices) != 2 {
		t.Fatalf("rows=1 did not carry the row indices: %+v", full)
	}

	if code := get(t, ts, "/v1/probes?rows=1&limit=1000", nil); code != 400 {
		t.Errorf("rows=1 with limit=1000 -> %d, want 400 (its cap is lower)", code)
	}
	if code := get(t, ts, "/v1/probes?rows=maybe", nil); code != 400 {
		t.Errorf("rows=maybe -> %d, want 400", code)
	}
}
