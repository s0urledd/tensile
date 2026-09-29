package api_test

import (
	"testing"
)

// Every figure the site prints has to say through which block of the chain
// it was computed, not only when: the record is indexed by height, the
// clock is not, and a reader checking a figure against the chain needs the
// height. The same record_through rides every snapshot response, and it is
// the scanner's own checkpoint, so the three cannot disagree.
func TestEveryFigureSaysThroughWhichBlockItWasComputed(t *testing.T) {
	ts, st := serverAndStore(t)
	scanned, err := st.Meta("last_scanned_height")
	if err != nil {
		t.Fatal(err)
	}
	if scanned == "" || scanned == "0" {
		t.Fatalf("the sample store has no scanner checkpoint: %q", scanned)
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
		if got := resp.RecordThrough.Height; got == 0 || scanned != itoa(got) {
			t.Fatalf("%s: record_through.height = %d, the scanner's checkpoint is %s", path, got, scanned)
		}
		// block_time is the scanner's last_scanned_time and is absent on a
		// record written before the scanner kept it (the sample is one);
		// the height is the contract.
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
