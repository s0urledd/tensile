package api_test

import (
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/observer/api"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
)

// /v1/hosting says how old each IP database is: the collector records the
// file's date on every lookup, and the answer used to leave it out, so a
// file never refreshed (no timer on a new host) drifted with nothing
// saying so.
func TestHostingPublishesEachDatabasesDate(t *testing.T) {
	f := newHostingFixture(t)
	f.enable(t)
	fi, err := os.Stat(filepath.Join(f.dir, "ip2asn-combined.tsv.gz"))
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(api.New(f.st, "test"))
	defer ts.Close()
	var out struct {
		Sources struct {
			ASN struct {
				Modified string `json:"modified"`
			} `json:"asn_db"`
		} `json:"sources"`
	}
	if code := get(t, ts, "/v1/hosting", &out); code != 200 {
		t.Fatalf("hosting: %d", code)
	}
	at, err := time.Parse(time.RFC3339Nano, out.Sources.ASN.Modified)
	if err != nil || !at.Equal(fi.ModTime().UTC()) {
		t.Fatalf("asn_db.modified %q, want the file's %s", out.Sources.ASN.Modified, store.TS(fi.ModTime()))
	}
}
