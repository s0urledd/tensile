package api_test

import "testing"

// Numbered pages: offset walks the same order the cursor does, and total
// counts every publication the filters select.
func TestBlobsPageByOffset(t *testing.T) {
	ts := serverWithSample(t)
	type page struct {
		Blobs []struct {
			PromiseHash string `json:"promise_hash"`
		} `json:"blobs"`
		Total     int64 `json:"total"`
		Offset    int   `json:"offset"`
		Truncated bool  `json:"truncated"`
	}
	var all, first, second page
	if code := get(t, ts, "/v1/blobs", &all); code != 200 || all.Total != 3 || len(all.Blobs) != 3 {
		t.Fatalf("all: %d, total %d, %d rows", code, all.Total, len(all.Blobs))
	}
	if code := get(t, ts, "/v1/blobs?limit=2", &first); code != 200 || first.Total != 3 || len(first.Blobs) != 2 || !first.Truncated {
		t.Fatalf("first page: %d %+v", code, first)
	}
	if code := get(t, ts, "/v1/blobs?limit=2&offset=2", &second); code != 200 || second.Total != 3 || second.Offset != 2 || len(second.Blobs) != 1 || second.Truncated {
		t.Fatalf("second page: %d %+v", code, second)
	}
	if second.Blobs[0].PromiseHash != all.Blobs[2].PromiseHash {
		t.Errorf("offset 2 is %s, the third row is %s", second.Blobs[0].PromiseHash, all.Blobs[2].PromiseHash)
	}
	for _, bad := range []string{"-1", "x", "100001"} {
		if code := get(t, ts, "/v1/blobs?offset="+bad, nil); code != 400 {
			t.Errorf("offset=%s: %d, want 400", bad, code)
		}
	}
}
