package api_test

import (
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	assign "github.com/plsgiveup/fibre/fibre-assign"
	"github.com/plsgiveup/fibre/fibre-sentinel/internal/scan"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/api"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
)

type paramsBody struct {
	Current *struct {
		Height    int64    `json:"effective_from_height"`
		TxIndex   int      `json:"effective_from_tx_index"`
		Time      *string  `json:"effective_from_time"`
		Source    string   `json:"source"`
		WD        int64    `json:"withdrawal_delay_s"`
		Timeout   int64    `json:"payment_promise_timeout_s"`
		Window    int64    `json:"payment_promise_height_window"`
		Retention int64    `json:"shard_retention_s"`
		Budget    int64    `json:"full_stake_storage_budget_bytes"`
		Changed   []string `json:"changed"`
	} `json:"current"`
	Derived *struct {
		MustServe  int64 `json:"must_serve_window_s"`
		Settleable int64 `json:"promise_settleable_s"`
		Processed  int64 `json:"processed_payment_retention_s"`
	} `json:"derived"`
	History []struct {
		Height  int64    `json:"effective_from_height"`
		Time    *string  `json:"effective_from_time"`
		Source  string   `json:"source"`
		Changed []string `json:"changed"`
	} `json:"history"`
	Changes  int `json:"changes"`
	Protocol struct {
		Commit      string `json:"pinned_celestia_app_commit"`
		Rows        int    `json:"original_rows"`
		Parity      int    `json:"parity_rows"`
		Total       int    `json:"total_rows"`
		MaxBlob     int    `json:"max_blob_size_bytes"`
		MinRow      int    `json:"min_row_size_bytes"`
		MinRowsVal  int    `json:"min_rows_per_validator"`
		Liveness    string `json:"liveness_threshold"`
		Safety      string `json:"safety_threshold"`
		Fingerprint string `json:"assignment_fingerprint"`
		Matches     *bool  `json:"fingerprint_matches"`
		Skew        int64  `json:"max_promise_clock_skew_s"`
		Bounds      map[string]struct {
			Min *int64 `json:"min_s"`
			Max int64  `json:"max_s"`
		} `json:"param_bounds"`
	} `json:"protocol"`
	Formula *struct {
		BaseGas     uint64 `json:"base_gas"`
		GasPerChunk uint64 `json:"gas_per_chunk"`
		ChunkBytes  uint64 `json:"chunk_bytes"`
		UtiaPerGas  uint64 `json:"utia_per_gas"`
	} `json:"price_formula"`
}

func paramsServer(t *testing.T) (*httptest.Server, *store.Store) {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "observer.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	ts := httptest.NewServer(api.NewWithVantage(st, api.VantageInfo{Name: "test"}, nil))
	t.Cleanup(ts.Close)
	return ts, st
}

// Before the scanner has seeded any params there is nothing current, and
// the pinned constants are still published: they come from the code.
func TestParamsEmpty(t *testing.T) {
	ts, _ := paramsServer(t)
	var p paramsBody
	if code := get(t, ts, "/v1/params", &p); code != 200 {
		t.Fatalf("params: %d", code)
	}
	if p.Current != nil || p.Derived != nil || p.History == nil || len(p.History) != 0 || p.Changes != 0 {
		t.Fatalf("empty store: %+v", p)
	}
	// The pinned values, cross-checked against the assignment pin so the
	// two sources of the same constants cannot silently diverge.
	pp := assign.ParamsV10BlobV0
	if p.Protocol.Rows != pp.OriginalRows || p.Protocol.Total != pp.TotalRows || p.Protocol.Parity != pp.TotalRows-pp.OriginalRows ||
		p.Protocol.MinRowsVal != pp.MinRowsPerValidator || p.Protocol.Liveness != "1/3" || p.Protocol.Safety != "2/3" {
		t.Fatalf("protocol constants disagree with the assignment pin: %+v", p.Protocol)
	}
	if p.Protocol.MaxBlob != 128<<20 || p.Protocol.MinRow <= 0 || p.Protocol.Commit != assign.PinnedCelestiaAppCommit {
		t.Fatalf("protocol: %+v", p.Protocol)
	}
	if p.Protocol.Fingerprint != pp.Fingerprint() || p.Protocol.Matches != nil {
		t.Fatalf("fingerprint: %q matches=%v", p.Protocol.Fingerprint, p.Protocol.Matches)
	}
	if p.Protocol.Skew != 600 {
		t.Fatalf("max promise clock skew %d", p.Protocol.Skew)
	}
	for name, b := range map[string][2]int64{
		"withdrawal_delay":        {int64((12*time.Hour + 10*time.Minute) / time.Second), int64(7 * 24 * time.Hour / time.Second)},
		"payment_promise_timeout": {600, int64(12 * time.Hour / time.Second)},
		"shard_retention":         {600, int64(7 * 24 * time.Hour / time.Second)},
	} {
		got, ok := p.Protocol.Bounds[name]
		if !ok || got.Min == nil || *got.Min != b[0] || got.Max != b[1] {
			t.Fatalf("bound %s: %+v, want %v", name, got, b)
		}
	}
	// the module's charge, from which every fee is recomputed, is here
	// before any blob settles
	if f := p.Formula; f == nil || f.BaseGas != 650_000 || f.GasPerChunk != 45_000 || f.ChunkBytes != 262144 || f.UtiaPerGas != 1 {
		t.Fatalf("price formula: %+v", f)
	}
}

// A seed and one change: the change names what changed, the current entry
// is the change, both are dated when the header time is known, and the
// derived windows follow the current values.
func TestParamsHistory(t *testing.T) {
	ts, st := paramsServer(t)
	seed := scan.ParamsSnapshot{WithdrawalDelaySeconds: 86400, PaymentPromiseTimeoutSeconds: 3600, PaymentPromiseHeightWindow: 1000,
		ShardRetentionSeconds: 14400, FullStakeStorageBudget: 2 << 40}
	change := seed
	change.ShardRetentionSeconds = 3 * 3600
	change.PaymentPromiseTimeoutSeconds = 4 * 3600
	if err := st.UpsertParams([]scan.ParamEntry{
		{FromHeight: 1000, FromTxIndex: -1, Source: "seed", ParamsJSON: seed},
		{FromHeight: 1200, FromTxIndex: 3, Source: "event", ParamsJSON: change},
	}); err != nil {
		t.Fatal(err)
	}
	at := time.Date(2026, 9, 25, 1, 2, 3, 0, time.UTC)
	if err := st.SetParamTime(1200, at); err != nil {
		t.Fatal(err)
	}
	_ = st.SetMeta("protocol_params_fingerprint", assign.ParamsV10BlobV0.Fingerprint(), time.Now())

	var p paramsBody
	if code := get(t, ts, "/v1/params", &p); code != 200 {
		t.Fatalf("params: %d", code)
	}
	if len(p.History) != 2 || p.Changes != 1 {
		t.Fatalf("history: %+v", p.History)
	}
	if p.History[0].Source != "seed" || p.History[0].Time != nil || len(p.History[0].Changed) != 0 {
		t.Fatalf("seed: %+v", p.History[0])
	}
	c := p.Current
	if c == nil || c.Height != 1200 || c.TxIndex != 3 || c.Source != "event" || c.Time == nil || *c.Time != store.TS(at) {
		t.Fatalf("current: %+v", c)
	}
	if !reflect.DeepEqual(c.Changed, []string{"payment_promise_timeout", "shard_retention"}) {
		t.Fatalf("changed: %v", c.Changed)
	}
	if c.Retention != 10800 || c.Timeout != 14400 || c.WD != 86400 || c.Window != 1000 || c.Budget != 2<<40 {
		t.Fatalf("values: %+v", c)
	}
	// must_serve window = max(timeout, retention): here the timeout.
	if p.Derived == nil || p.Derived.MustServe != 14400 || p.Derived.Settleable != 86400 || p.Derived.Processed != 86400+600 {
		t.Fatalf("derived: %+v", p.Derived)
	}
	if p.Protocol.Matches == nil || !*p.Protocol.Matches {
		t.Fatalf("scanner fingerprint not matched: %v", p.Protocol.Matches)
	}

	// A scanner on another pin is reported, not hidden.
	_ = st.SetMeta("protocol_params_fingerprint", "something-else", time.Now())
	get(t, ts, "/v1/params", &p)
	if p.Protocol.Matches == nil || *p.Protocol.Matches {
		t.Fatalf("mismatched fingerprint reported as a match")
	}
}
