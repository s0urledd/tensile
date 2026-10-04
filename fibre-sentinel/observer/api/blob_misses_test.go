package api_test

import (
	"net/http"
	"strings"
	"testing"
)

// A reader looks a blob up by what it holds a moment after submitting it,
// often before the scanner has read its block. A lookup that finds nothing
// must not be held by a cache, or the blob reads "not indexed yet" for 15
// seconds after it is on record: by promise hash a 404, by commitment or by
// transaction an empty list, and both are no-store. A lookup that finds its
// blob, and every list that is not a lookup, keep the usual policy.
func TestALookupThatFindsNothingIsNotCached(t *testing.T) {
	ts, _, twice := filterFixture(t)
	zero := strings.Repeat("00", 32)
	for _, c := range []struct {
		path   string
		status int
		cache  string
	}{
		// hits
		{"/v1/blobs/p1", 200, "public, max-age=15"},
		{"/v1/blobs?commitment=" + twice, 200, "public, max-age=15"},
		{"/v1/blobs?tx=" + txOf("p3"), 200, "public, max-age=15"},
		{"/v1/blobs?tx=" + txOf("p1") + "&commitment=" + twice, 200, "public, max-age=15"},
		// misses
		{"/v1/blobs/" + zero, 404, "no-store"},
		{"/v1/blobs?commitment=" + zero, 200, "no-store"},
		{"/v1/blobs?tx=" + zero, 200, "no-store"},
		{"/v1/blobs?tx=0x" + strings.ToUpper(zero) + "&limit=25", 200, "no-store"},
		// a transaction that settled another commitment: nothing names both
		{"/v1/blobs?tx=" + txOf("p3") + "&commitment=" + twice, 200, "no-store"},
		// lists that are not lookups, empty or not
		{"/v1/blobs", 200, "public, max-age=15"},
		{"/v1/blobs?namespace=" + strings.Repeat("00", 29), 200, "public, max-age=15"},
	} {
		resp, err := http.Get(ts.URL + c.path)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != c.status || resp.Header.Get("Cache-Control") != c.cache {
			t.Errorf("%s: %d %q, want %d %q", c.path, resp.StatusCode, resp.Header.Get("Cache-Control"), c.status, c.cache)
		}
	}
}
