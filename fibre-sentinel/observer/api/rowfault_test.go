package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
)

// One publication whose record does not decode is that publication's fault,
// not the page's: the blob list, the blob's own page and its readings with
// their row indices all answer, the blob marked "unknown" and the readings
// without their indices; the network's and the publishers' batches go on;
// the row is counted and its status kept by nothing. Before, one such row
// answered /v1/blobs with a 500 for everyone (#253's incident) and stopped
// the snapshots that read it.
func TestARowThatDoesNotDecodeIsItsOwnFault(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "observer.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	full := []int{}
	for i := 0; i < 40; i++ {
		full = append(full, i)
	}
	good := writeBlob(t, st, 0, blobCase{needed: 40, total: 160, points: 2, complete: true, vals: []valRows{
		{addr: "a1", rows: full[:20], attested: 1, served: true},
		{addr: "a2", rows: full[20:], attested: 1, served: true},
	}})
	// Overlapping assignments put the bounds either side of the threshold,
	// so the batches read this blob's row lists too (reconstructBatch's
	// fallback), as the blob list always does. Its window is over, so a
	// status drawn from it would be cached for good.
	bad := writeBlob(t, st, 1, blobCase{needed: 20, total: 160, points: 2, complete: true, over: true, vals: []valRows{
		{addr: "i1", rows: full[:20], attested: 1, served: true},
		{addr: "i2", rows: full[:20], attested: 1, served: false},
	}})
	// Its served readings mark their rows as the validator's own assignment
	// (store.RowsAssigned), and the record that gives the assignment is one
	// this build cannot decode: what a newer build's format, or a table
	// entry this one does not know, looks like to it.
	if _, err := st.DB().Exec(`UPDATE probes SET row_indices = ? WHERE promise_hash = ? AND commitment_verified = 1`, store.RowsAssigned, bad); err != nil {
		t.Fatal(err)
	}
	if _, err := st.DB().Exec(`UPDATE publications SET raw_json = x'00ff01' WHERE promise_hash = ?`, bad); err != nil {
		t.Fatal(err)
	}

	srv := New(st, "test")
	ts := httptest.NewServer(srv)
	defer func() { ts.Close(); srv.Close() }()
	fetch := func(path string, into any) {
		t.Helper()
		resp, err := http.Get(ts.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != 200 {
			t.Fatalf("%s: %d, want 200", path, resp.StatusCode)
		}
		if err := json.NewDecoder(resp.Body).Decode(into); err != nil {
			t.Fatal(err)
		}
	}
	type status struct {
		Hash            string `json:"promise_hash"`
		Reconstructable struct {
			Status string `json:"status"`
		} `json:"reconstructable"`
	}

	var list struct {
		Blobs []status `json:"blobs"`
	}
	fetch("/v1/blobs", &list)
	got := map[string]string{}
	for _, b := range list.Blobs {
		got[b.Hash] = b.Reconstructable.Status
	}
	if len(got) != 2 || got[good] != "yes" || got[bad] != "unknown" {
		t.Fatalf("/v1/blobs: %v, want the good blob yes and the undecodable one unknown", got)
	}

	var one struct {
		Blob status `json:"blob"`
	}
	fetch("/v1/blobs/"+bad, &one)
	if one.Blob.Reconstructable.Status != "unknown" {
		t.Errorf("/v1/blobs/{hash}: %q, want unknown", one.Blob.Reconstructable.Status)
	}
	var rows struct {
		Probes []struct {
			Outcome    string   `json:"outcome"`
			RowIndices []uint32 `json:"row_indices"`
		} `json:"probes"`
	}
	fetch("/v1/probes?rows=1&blob="+bad, &rows)
	if len(rows.Probes) == 0 {
		t.Fatal("/v1/probes?rows=1: no readings")
	}
	for _, p := range rows.Probes {
		if p.RowIndices != nil {
			t.Errorf("a reading whose rows could not be read out of the record carries %v", p.RowIndices)
		}
	}

	// The batches the network and the publishers are computed with go on
	// past it, and the memo keeps no status drawn from it.
	ctx := context.Background()
	pin := asOfPin{now: time.Now()}
	batch, err := srv.reconstructBatch(ctx, "", 10, pin)
	if err != nil {
		t.Fatalf("reconstructBatch: %v", err)
	}
	if rc := batch[bad]; rc == nil || rc.Status != "unknown" || !rc.faulted {
		t.Errorf("batch: %+v, want unknown and faulted", rc)
	}
	if rc := batch[good]; rc == nil || rc.Status != "yes" {
		t.Errorf("batch, the good blob: %+v", rc)
	}
	if err := srv.readings.update(ctx, srv, 0); err != nil {
		t.Fatalf("readings: %v", err)
	}
	srv.readings.upd.Lock()
	kept := srv.readings.faulted[bad]
	srv.readings.upd.Unlock()
	if !kept {
		t.Error("the readings memo does not hold the undecodable blob for its next update")
	}
	// Counted under none of the words: its window is over, and "not read"
	// would say Tensile missed a blob whose reading is on record.
	if tot := srv.readings.totals(time.Now()); tot == nil || *tot != (readingTotals{Available: 1}) {
		t.Errorf("reading totals: %+v, want the good blob available and the undecodable one in none", tot)
	}
	srv.blobs.mu.Lock()
	_, cached := srv.blobs.m[bad]
	srv.blobs.mu.Unlock()
	if cached {
		t.Error("a status drawn from an undecodable row was cached")
	}

	// Counted, so it is seen: once per row, however often it is read, and
	// the records check fails while such rows are met, clearing a while
	// after the last.
	n, last, at := srv.UndecodableRows()
	if n != 1 || last == "" {
		t.Errorf("undecodable rows: %d (%q), want 1", n, last)
	}
	if c := srv.recordsCheck(at.Add(time.Minute)); c.Name != "records" || c.OK || !strings.Contains(c.Detail, "since the API started: 1,") {
		t.Errorf("records check a minute after: %+v, want failing on the one row", c)
	}
	if c := srv.recordsCheck(at.Add(rowFaultFresh)); !c.OK {
		t.Errorf("records check %s after the last: %+v, want passing", rowFaultFresh, c)
	}
	if c := (&Server{}).recordsCheck(time.Now()); !c.OK {
		t.Errorf("records check with every row decoded: %+v", c)
	}
	// and /v1/health carries it, so healthwatch says so (it answers 503 while
	// any check fails)
	resp, err := http.Get(ts.URL + "/v1/health")
	if err != nil {
		t.Fatal(err)
	}
	var health struct {
		Checks []healthCheck `json:"checks"`
	}
	err = json.NewDecoder(resp.Body).Decode(&health)
	resp.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	var records *healthCheck
	for i := range health.Checks {
		if health.Checks[i].Name == "records" {
			records = &health.Checks[i]
		}
	}
	if records == nil || records.OK {
		t.Errorf("/v1/health after a row that did not decode: records check %+v, want failing", records)
	}

	// A request that ends is the request's error, not the row's: its blobs
	// are never published unknown.
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	if rc, err := srv.readStatus(cctx, bad, pin); err == nil {
		t.Errorf("a cancelled read was answered %+v rather than failed", rc)
	} else if _, ok := isRowFault(err); ok {
		t.Errorf("a cancelled read was taken for the row's fault: %v", err)
	}
}
