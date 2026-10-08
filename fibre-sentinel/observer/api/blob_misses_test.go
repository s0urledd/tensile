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

// A lookup by the hash of a transaction that failed in a block finds no
// blob, and answers the failure (failed_tx). A final failure cannot change,
// so its answer keeps the usual policy; one that could still be included
// again is a miss, not cached. The failure is answered only to the hash
// asked alone (the page aside): beside a commitment, a namespace, a
// publisher or a cursor, the lookup is a miss like any other, as nothing
// it filters on is held for a failure.
func TestAFailedTransactionIsCachedWhenFinalAndAnsweredToItsHashAlone(t *testing.T) {
	ts, _ := ftxFixture(t, true)
	for _, c := range []struct {
		path   string
		cache  string
		failed bool
	}{
		// the hash alone, as the site asks it
		{"/v1/blobs?tx=" + ftxFinal, "public, max-age=15", true},
		{"/v1/blobs?tx=" + ftxFinal + "&limit=25&offset=0", "public, max-age=15", true},
		{"/v1/blobs?tx=0x" + strings.ToUpper(ftxFinal) + "&limit=25", "public, max-age=15", true},
		// stopped before the ante: it could still be included, so a miss
		{"/v1/blobs?tx=" + ftxOpen, "no-store", true},
		{"/v1/blobs?tx=" + ftxOpen + "&limit=25", "no-store", true},
		// the hash beside another filter
		{"/v1/blobs?tx=" + ftxFinal + "&commitment=" + strings.Repeat("ab", 32), "no-store", false},
		{"/v1/blobs?tx=" + ftxFinal + "&publisher=" + samplePublisher, "no-store", false},
		{"/v1/blobs?tx=" + ftxFinal + "&before_height=1000", "no-store", false},
		// f1's transaction settled it in fixtureNS, and failed once before
		{"/v1/blobs?tx=" + ftxSettled + "&namespace=" + strings.Repeat("00", 29), "no-store", false},
		// a hash nothing carries
		{"/v1/blobs?tx=" + strings.Repeat("00", 32), "no-store", false},
	} {
		a := ftxAsk(t, ts, c.path)
		_, has := a.keys["failed_tx"]
		if a.status != 200 || a.cache != c.cache || has != c.failed || string(a.keys["total"]) != "0" {
			t.Errorf("%s: %d %q failed_tx=%v total %s, want 200 %q failed_tx=%v total 0", c.path, a.status, a.cache, has, a.keys["total"], c.cache, c.failed)
		}
	}
}
