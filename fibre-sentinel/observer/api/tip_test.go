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

// With a node to ask, the ticker reads the node's newest committed block, not
// the scanner's file; a node that does not answer leaves the file to answer.
func TestTipAsksTheNode(t *testing.T) {
	dir := t.TempDir()
	st, err := store.Open(filepath.Join(dir, "observer.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if err := os.MkdirAll(filepath.Join(dir, "status"), 0o755); err != nil {
		t.Fatal(err)
	}
	raw := `{"component":"scanner","updated_at":"2026-10-03T17:00:01Z","height":1351480,"detail":{"chain_tip":1351480,"tip_block_time":"2026-10-03T17:00:00Z"}}`
	if err := os.WriteFile(filepath.Join(dir, "status", "scanner.json"), []byte(raw), 0o644); err != nil {
		t.Fatal(err)
	}
	node := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/status" {
			http.NotFound(w, r)
			return
		}
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":-1,"result":{"sync_info":{"latest_block_height":"1351482","latest_block_time":"2026-10-03T17:00:05.943Z"}}}`))
	}))
	defer node.Close()
	srv := api.NewWithVantage(st, api.VantageInfo{Name: "test"}, nil, api.WithDataDir(dir), api.WithTipRPC(node.URL))
	defer srv.Close()
	b, _ := getTip(t, srv)
	want := time.Date(2026, 10, 3, 17, 0, 5, 943000000, time.UTC)
	if b.Height != 1351482 || b.BlockTime == nil || !b.BlockTime.Equal(want) {
		t.Errorf("tip from the node %+v", b)
	}

	down := api.NewWithVantage(st, api.VantageInfo{Name: "test"}, nil, api.WithDataDir(dir), api.WithTipRPC("http://127.0.0.1:1"))
	defer down.Close()
	if b, _ := getTip(t, down); b.Height != 1351480 {
		t.Errorf("a node that does not answer should leave the scanner's file to answer: %+v", b)
	}
}
