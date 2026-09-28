package api_test

import (
	"testing"
)

// Every figure the site prints has to say through which block of the chain
// it was computed, not only when: the record is indexed by height, the
// clock is not, and a reader checking a figure against the chain needs the
// height. The same record_through rides every snapshot response, and it is
// the scanner's own checkpoint — the one /v1/meta reports — so the three
// cannot disagree. /v1/meta also says which of the three kinds of evidence
// each headline figure rests on, with the three defined beside it.
func TestEveryFigureSaysThroughWhichBlockItWasComputed(t *testing.T) {
	ts := serverWithSample(t)
	var meta struct {
		LastScannedHeight string            `json:"last_scanned_height"`
		Evidence          map[string]string `json:"evidence"`
		EvidenceKinds     map[string]string `json:"evidence_kinds"`
	}
	if code := get(t, ts, "/v1/meta", &meta); code != 200 {
		t.Fatalf("meta: %d", code)
	}
	if meta.LastScannedHeight == "" || meta.LastScannedHeight == "0" {
		t.Fatalf("the sample store has no scanner checkpoint: %q", meta.LastScannedHeight)
	}
	type rt struct {
		Height    int64  `json:"height"`
		BlockTime string `json:"block_time"`
	}
	for _, path := range []string{"/v1/network?window=24h", "/v1/validators?window=24h", "/v1/market?window=24h", "/v1/network?window=all"} {
		var resp struct {
			RecordThrough *rt `json:"record_through"`
		}
		if code := get(t, ts, path, &resp); code != 200 {
			t.Fatalf("%s: %d", path, code)
		}
		if resp.RecordThrough == nil {
			t.Fatalf("%s carries no record_through", path)
		}
		if got := resp.RecordThrough.Height; got == 0 || meta.LastScannedHeight != itoa(got) {
			t.Fatalf("%s: record_through.height = %d, meta says %s", path, got, meta.LastScannedHeight)
		}
		// block_time is the scanner's last_scanned_time and is absent on a
		// record written before the scanner kept it (the sample is one);
		// the height is the contract.
	}
	if len(meta.EvidenceKinds) != 3 {
		t.Fatalf("evidence kinds: %v", meta.EvidenceKinds)
	}
	for figure, kind := range meta.Evidence {
		if _, ok := meta.EvidenceKinds[kind]; !ok {
			t.Fatalf("figure %s rests on %q, which evidence_kinds does not define", figure, kind)
		}
	}
	for _, figure := range []string{"obligations", "reconstructable", "reachability", "publications", "signed_shards", "fees_settled", "throughput", "endorsed"} {
		if meta.Evidence[figure] == "" {
			t.Fatalf("headline figure %s has no evidence kind", figure)
		}
	}
	// The three are different claims and the map must keep them apart.
	if meta.Evidence["reachability"] == meta.Evidence["obligations"] || meta.Evidence["obligations"] == meta.Evidence["publications"] {
		t.Fatalf("evidence kinds blurred: %v", meta.Evidence)
	}
}

func itoa(n int64) string {
	if n == 0 {
		return "0"
	}
	var b []byte
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	return string(b)
}
