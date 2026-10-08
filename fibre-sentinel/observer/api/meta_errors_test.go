package api_test

import (
	"encoding/json"
	"io"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/scan"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/api"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
)

// /v1/meta publishes every params range, and never the scanner's RPC error
// in one. A range the scanner could not close carries the error its read of
// a height met, and a check that could not happen the error of the read
// that failed, each as the RPC client wrote it, the node's address in it,
// as a scan gap's last error is (which /v1/meta has left out all along).
// The height the read failed at stays, and the scanner's own words for a
// range too long to read stay as they are.
func TestMetaPublishesNoRPCErrorOfAParamsRange(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "observer.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	now := time.Now().UTC().Truncate(time.Second)
	rpcErr := `abci query params h=150: post failed: Post "http://100.64.7.9:26657": context deadline exceeded`
	tooLong := "the range is 8801 heights, over the 1000 this scanner will read in one pass; nothing was read, so nothing is proven"
	for _, u := range []scan.ParamUncertainty{
		{ID: "t:silent_change:121-180", Kind: scan.UncertaintySilentChange, FromHeight: 121, ToHeight: 180,
			Resolution: scan.ResolutionUnresolvable, ResolvedAt: &now, ResolveError: "params at height 150: " + rpcErr},
		{ID: "t:silent_change:200-9000", Kind: scan.UncertaintySilentChange, FromHeight: 200, ToHeight: 9000,
			Resolution: scan.ResolutionUnresolvable, ResolvedAt: &now, ResolveError: tooLong},
		{ID: "t:check_skipped:300-310", Kind: scan.UncertaintyCheckSkipped, FromHeight: 300, ToHeight: 310, LastError: rpcErr},
	} {
		u.SchemaVersion, u.ChainID, u.DetectedAt, u.IntervalStartKnown = scan.ParamUncertaintySchemaVersion, "t", now, true
		raw, err := json.Marshal(u)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := st.UpsertParamUncertainty(u, raw, now); err != nil {
			t.Fatal(err)
		}
	}
	ts := httptest.NewServer(api.New(st, "test"))
	defer ts.Close()
	resp, err := ts.Client().Get(ts.URL + "/v1/meta")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	for _, leak := range []string{"100.64.7.9", "26657", "post failed", "deadline exceeded", "last_error"} {
		if strings.Contains(string(b), leak) {
			t.Errorf("/v1/meta carries %q: %s", leak, b)
		}
	}
	var meta struct {
		Ranges []struct {
			ID           string `json:"id"`
			ResolveError string `json:"resolve_error"`
		} `json:"param_uncertainty"`
	}
	if err := json.Unmarshal(b, &meta); err != nil {
		t.Fatal(err)
	}
	got := map[string]string{}
	for _, r := range meta.Ranges {
		got[r.ID] = r.ResolveError
	}
	want := map[string]string{
		"t:silent_change:121-180":  "the params could not be read at height 150; nothing is proven",
		"t:silent_change:200-9000": tooLong,
		"t:check_skipped:300-310":  "",
	}
	if len(got) != len(want) {
		t.Fatalf("ranges published: %v, want %d", got, len(want))
	}
	for id, w := range want {
		if got[id] != w {
			t.Errorf("%s: resolve_error %q, want %q", id, got[id], w)
		}
	}
}
