package api_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
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

// The tip names the newest blob the store holds, in /v1/blobs' own order
// (height, then position in the block), so a page can read the list only when
// a blob arrives; with none stored the field is left out, which is what a
// page also meets from an API that predates it.
func TestTipNamesTheNewestBlob(t *testing.T) {
	dir := t.TempDir()
	st, err := store.Open(filepath.Join(dir, "observer.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	_ = st.SetMeta("chain_height", "1000", time.Now())
	srv := api.NewWithVantage(st, api.VantageInfo{Name: "test"}, nil, api.WithDataDir(dir))
	defer srv.Close()

	_, rec := getTip(t, srv)
	if strings.Contains(rec.Body.String(), "latest_blob") {
		t.Fatalf("a store without blobs should leave latest_blob out: %s", rec.Body.String())
	}

	// two blobs in block 900, the second later in it, and an older one
	now := time.Now().UTC()
	hash := func(idx int) string { return fmt.Sprintf("%064x", 0x5a0000+idx) }
	insertPublication(t, st, 1, 899, now.Add(-time.Minute), "aa", "alice", 10)
	insertPublication(t, st, 2, 900, now, "aa", "alice", 10)
	insertPublication(t, st, 3, 900, now, "aa", "alice", 10)
	for idx, tx := range map[int]int{2: 4, 3: 1} {
		if _, err := st.DB().Exec(`UPDATE publications SET settlement_tx_index = ? WHERE promise_hash = ?`, tx, hash(idx)); err != nil {
			t.Fatal(err)
		}
	}
	type latest struct {
		LatestBlob *struct {
			PromiseHash      string `json:"promise_hash"`
			SettlementHeight int64  `json:"settlement_height"`
		} `json:"latest_blob"`
	}
	read := func() latest {
		t.Helper()
		_, rec := getTip(t, srv)
		var l latest
		if err := json.Unmarshal(rec.Body.Bytes(), &l); err != nil {
			t.Fatal(err)
		}
		return l
	}
	// the answer is kept for a quarter of a second
	time.Sleep(300 * time.Millisecond)
	if l := read(); l.LatestBlob == nil || l.LatestBlob.PromiseHash != hash(2) || l.LatestBlob.SettlementHeight != 900 {
		t.Fatalf("latest_blob %+v, want %s at 900 (tx index 4 of the block)", l.LatestBlob, hash(2))
	}

	// a blob in the next block takes its place once the cached answer lapses
	insertPublication(t, st, 4, 901, now, "bb", "bob", 10)
	time.Sleep(300 * time.Millisecond)
	if l := read(); l.LatestBlob == nil || l.LatestBlob.PromiseHash != hash(4) || l.LatestBlob.SettlementHeight != 901 {
		t.Fatalf("latest_blob %+v, want %s at 901", l.LatestBlob, hash(4))
	}

	// and it is the first row /v1/blobs serves
	rec = httptest.NewRecorder()
	srv.ServeHTTP(rec, httptest.NewRequest("GET", "/v1/blobs?limit=1", nil))
	var page struct {
		Blobs []struct {
			PromiseHash string `json:"promise_hash"`
		} `json:"blobs"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &page); err != nil || len(page.Blobs) != 1 || page.Blobs[0].PromiseHash != hash(4) {
		t.Fatalf("/v1/blobs' newest is not the tip's: %s", rec.Body.String())
	}
}

// /v1/tip reads the store with its cache's lock held, and every open page
// asks it every second. While every connection of the store's pool is held
// by reads ahead of it, it answers within tipStoreTimeout all the same,
// without the store's figures for that answer, and Fibre's state is the
// last answer's. It used to read through Store.Meta, which takes no
// context: it waited for a connection for as long as those reads ran, and
// every reader of the route waited behind it.
func TestTipAnswersWhileEveryConnectionIsHeld(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "observer.db")) // a pool of one connection
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	_ = st.SetMeta("fibre_active", "yes", time.Now())
	_ = st.SetMeta("chain_height", "1000", time.Now())
	srv := api.NewWithVantage(st, api.VantageInfo{Name: "test"}, nil)
	defer srv.Close()
	if b, _ := getTip(t, srv); !b.FibreActive || b.Height != 1000 {
		t.Fatalf("tip %+v", b)
	}
	tx, err := st.DB().Begin() // holds the one connection
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	time.Sleep(300 * time.Millisecond) // past the answer kept
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		rec := httptest.NewRecorder()
		srv.ServeHTTP(rec, httptest.NewRequest("GET", "/v1/tip", nil))
		done <- rec
	}()
	select {
	case rec := <-done:
		var b tipBody
		if err := json.Unmarshal(rec.Body.Bytes(), &b); err != nil || rec.Code != http.StatusOK || !b.FibreActive {
			t.Errorf("with every connection held: %d %s", rec.Code, rec.Body.String())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("/v1/tip waited for a connection of the store")
	}
}
