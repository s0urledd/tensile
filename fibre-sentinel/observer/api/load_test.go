package api_test

import (
	"testing"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
)

type loadJSON struct {
	Promises      int64 `json:"promises"`
	Bytes         int64 `json:"bytes"`
	StoredBytes   int64 `json:"stored_bytes"`
	RowsPerBlob   int64 `json:"rows_per_blob"`
	EstIngressBps int64 `json:"est_ingress_bps"` // no longer published
}

// Load counts what a validator committed to store: the rows of the settled
// promises it endorsed, their row data (blob_size / original_rows each), and
// the rows of the newest assignment. A promise recorded before signatures were
// verified and a failed transaction are in neither.
func TestLoadPerValidator(t *testing.T) {
	ts, st, _ := signingFixture(t)
	// v1 endorsed p1, p2 and p3; p4 predates signature verification and p5's
	// transaction failed. Every assignment is 148 rows of a 1024-byte blob with
	// 4096 original rows: 37 bytes of row data.
	var det struct {
		Validator struct {
			Load loadJSON `json:"load"`
		} `json:"validator"`
	}
	if code := get(t, ts, "/v1/validators/"+sigV1+"?window=all", &det); code != 200 {
		t.Fatalf("detail: %d", code)
	}
	l := det.Validator.Load
	if l.Promises != 3 || l.Bytes != 3*37 {
		t.Fatalf("load = %+v, want 3 endorsed promises, 111 bytes", l)
	}
	if rows := loadRows(t, st, sigV1); rows != 3*148 {
		t.Fatalf("load rows = %d, want 444", rows)
	}
	if l.StoredBytes != 0 {
		t.Errorf("stored = %d, want 0: every window in the fixture has ended", l.StoredBytes)
	}
	if l.RowsPerBlob != 148 {
		t.Errorf("rows per blob = %d, want 148", l.RowsPerBlob)
	}
	if l.EstIngressBps != 0 {
		t.Errorf("a mainnet estimate is published: %d", l.EstIngressBps)
	}
}

// Rows a validator was assigned and did not endorse are no duty: v3 endorsed
// only p2, so it committed to one promise's rows, not three.
func TestLoadCountsOnlyEndorsedPromises(t *testing.T) {
	ts, st, _ := signingFixture(t)
	var det struct {
		Validator struct {
			Load loadJSON `json:"load"`
		} `json:"validator"`
	}
	if code := get(t, ts, "/v1/validators/"+sigV3+"?window=all", &det); code != 200 {
		t.Fatalf("detail: %d", code)
	}
	if l := det.Validator.Load; l.Promises != 1 || l.Bytes != 37 || l.RowsPerBlob != 148 {
		t.Fatalf("load = %+v, want 1 endorsed promise, 37 bytes, 148 rows per blob", l)
	}
	if rows := loadRows(t, st, sigV3); rows != 148 {
		t.Fatalf("load rows = %d, want 148", rows)
	}
}

// loadRows is the endorsed rows behind addr's shard data over the whole
// record, which the row keeps and the routes size in bytes.
func loadRows(t *testing.T, st *store.Store, addr string) int64 {
	t.Helper()
	var rows struct {
		Validators []struct {
			Address string `json:"address"`
			Load    struct {
				Rows int64 `json:"rows"`
			} `json:"load"`
		} `json:"validators"`
	}
	rowsOf(t, st, "test", "all", time.Time{}, &rows)
	for _, v := range rows.Validators {
		if v.Address == addr {
			return v.Load.Rows
		}
	}
	t.Fatalf("no row for %s", addr)
	return 0
}
