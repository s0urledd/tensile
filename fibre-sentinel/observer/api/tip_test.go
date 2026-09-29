package api_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/observer/api"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
)

type tipBody struct {
	Height      int64      `json:"height"`
	BlockTime   *time.Time `json:"block_time"`
	FibreActive bool       `json:"fibre_active"`
}

func getTip(t *testing.T, srv http.Handler) (tipBody, *httptest.ResponseRecorder) {
	t.Helper()
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, httptest.NewRequest("GET", "/v1/tip", nil))
	var b tipBody
	if err := json.Unmarshal(rec.Body.Bytes(), &b); err != nil {
		t.Fatalf("%d %s", rec.Code, rec.Body.String())
	}
	return b, rec
}

// The ticker reads the scanner's own status file, which moves with every
// block, and says where the block came from and when it was made.
func TestTipReadsTheScannerStatus(t *testing.T) {
	dir := t.TempDir()
	st, err := store.Open(filepath.Join(dir, "observer.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	_ = st.SetMeta("fibre_active", "yes", time.Now())
	_ = st.SetMeta("chain_height", "900", time.Now())
	bt := time.Date(2026, 9, 24, 20, 15, 0, 0, time.UTC)
	if err := os.MkdirAll(filepath.Join(dir, "status"), 0o755); err != nil {
		t.Fatal(err)
	}
	raw := `{"component":"scanner","updated_at":"2026-09-24T20:15:01Z","height":1000,"detail":{"chain_tip":1082620,"tip_block_time":"2026-09-24T20:15:00Z"}}`
	if err := os.WriteFile(filepath.Join(dir, "status", "scanner.json"), []byte(raw), 0o644); err != nil {
		t.Fatal(err)
	}
	srv := api.NewWithVantage(st, api.VantageInfo{Name: "test"}, nil, api.WithDataDir(dir))
	defer srv.Close()
	b, rec := getTip(t, srv)
	if b.Height != 1082620 || b.BlockTime == nil || !b.BlockTime.Equal(bt) || !b.FibreActive {
		t.Errorf("tip %+v", b)
	}
	if cc := rec.Header().Get("Cache-Control"); cc != "no-store" {
		t.Errorf("a ticker answer must not be cached: %q", cc)
	}
}

// With no scanner file the collector's view of the chain stands in.
func TestTipFallsBackToTheCollector(t *testing.T) {
	dir := t.TempDir()
	st, err := store.Open(filepath.Join(dir, "observer.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	_ = st.SetMeta("chain_height", "1065000", time.Now())
	_ = st.SetMeta("chain_tip_time", store.TS(time.Date(2026, 9, 24, 7, 0, 0, 0, time.UTC)), time.Now())
	srv := api.NewWithVantage(st, api.VantageInfo{Name: "test"}, nil, api.WithDataDir(dir))
	defer srv.Close()
	b, _ := getTip(t, srv)
	if b.Height != 1065000 || b.BlockTime == nil || b.FibreActive {
		t.Errorf("tip %+v", b)
	}
}
