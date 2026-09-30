package api_test

import (
	"bytes"
	"encoding/xml"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/cosmos/cosmos-sdk/types/bech32"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/probe"
	"github.com/plsgiveup/fibre/fibre-sentinel/internal/scan"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/api"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
)

// valoperOf is a celestiavaloper1… for a test validator. Its bytes are not
// the consensus address's: the two keys are unrelated on chain, and an
// answer that merely re-encoded the consensus bytes would be caught.
func valoperOf(t *testing.T, b byte) string {
	t.Helper()
	s, err := bech32.ConvertAndEncode("celestiavaloper", bytes.Repeat([]byte{b}, 20))
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// Every answer that names a validator by the consensus address its rows are
// keyed by also names it by the operator address a reader knows, from the
// staking set, and names none for a validator the staking set does not name.
// The hex address stays where it was: the site keys on it.
func TestAnswersNameValidatorsByOperatorAddress(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "observer.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	now := time.Now().UTC().Truncate(time.Second)
	created, msu := now.Add(-2*time.Hour), now.Add(-30*time.Minute)
	at := msu.Add(-10 * time.Minute)
	insertReading(t, st, "op1", created, msu, 4, probe.EndReadLabel, at, []endVal{
		{addr: selfAddrs["kept"], rows: 4, w: ok},
		{addr: selfAddrs["earlyonly"], rows: 2, w: err500},
		{addr: selfAddrs["silent"], rows: 2, w: refused},
	})
	insertReading(t, st, "op2", created, msu, 2, probe.EndReadLabel, at, []endVal{
		{addr: selfAddrs["broke"], rows: 2, w: gone},
	})
	// earlyonly is not in the staking set this observer read.
	want := map[string]string{
		selfAddrs["kept"]:      valoperOf(t, 0xe1),
		selfAddrs["broke"]:     valoperOf(t, 0xe2),
		selfAddrs["silent"]:    valoperOf(t, 0xe3),
		selfAddrs["earlyonly"]: "",
	}
	var ids []scan.ValidatorIdentity
	for _, k := range []string{"kept", "broke", "silent"} {
		ids = append(ids, scan.ValidatorIdentity{ConsAddressHex: selfAddrs[k], OperatorAddress: want[selfAddrs[k]], Moniker: k,
			Tokens: "1000000", Status: "BOND_STATUS_BONDED"})
	}
	if _, err := st.UpsertValidatorIdentities(ids, now); err != nil {
		t.Fatal(err)
	}
	ts := httptestServer(t, st)

	type named struct {
		Validator string `json:"validator_address"`
		Operator  string `json:"operator_address"`
	}
	check := func(what string, rows []named, n int) {
		t.Helper()
		if len(rows) != n {
			t.Fatalf("%s: %d rows, want %d: %+v", what, len(rows), n, rows)
		}
		for _, r := range rows {
			w, known := want[r.Validator]
			if !known {
				t.Fatalf("%s: a row of %q, which the fixture never wrote", what, r.Validator)
			}
			if r.Operator != w {
				t.Errorf("%s: %s has operator_address %q, want %q", what, r.Validator, r.Operator, w)
			}
		}
	}

	// the raw rows, by blob and by any spelling of the validator
	var probes struct {
		Probes []named `json:"probes"`
	}
	if code := get(t, ts, "/v1/probes?blob=op1", &probes); code != 200 {
		t.Fatalf("probes: %d", code)
	}
	check("/v1/probes?blob=", probes.Probes, 3)
	probes.Probes = nil
	if code := get(t, ts, "/v1/probes?validator="+want[selfAddrs["kept"]], &probes); code != 200 {
		t.Fatalf("probes by operator: %d", code)
	}
	check("/v1/probes?validator=", probes.Probes, 1)

	// a blob's assignments and its readings
	var blob struct {
		Assignments []named `json:"assignments"`
		Probes      []named `json:"probes"`
	}
	if code := get(t, ts, "/v1/blobs/op1", &blob); code != 200 {
		t.Fatalf("blob: %d", code)
	}
	check("assignments", blob.Assignments, 3)
	check("blob readings", blob.Probes, 3)

	// a validator's status, asked for by its operator address
	var status struct {
		Address  string `json:"address"`
		Operator string `json:"operator_address"`
	}
	broke := selfAddrs["broke"]
	if code := get(t, ts, "/v1/validators/"+want[broke]+"/status?window=all", &status); code != 200 {
		t.Fatalf("status: %d", code)
	}
	if status.Address != broke || status.Operator != want[broke] {
		t.Errorf("status names %+v, want %s and %s", status, broke, want[broke])
	}

	// the list and the page
	var list struct {
		Validators []struct {
			Address  string `json:"address"`
			Operator string `json:"operator_address"`
		} `json:"validators"`
	}
	if code := get(t, ts, "/v1/validators?window=all", &list); code != 200 {
		t.Fatalf("validators: %d", code)
	}
	for _, v := range list.Validators {
		if v.Operator != want[v.Address] {
			t.Errorf("list: %s has operator_address %q, want %q", v.Address, v.Operator, want[v.Address])
		}
	}

	// ?exclude= takes the operator address the site shows, and the answer
	// names the validator both ways
	var net struct {
		Excluded  []string          `json:"excluded"`
		Operators map[string]string `json:"excluded_operator_addresses"`
	}
	if code := get(t, ts, "/v1/network?window=all&exclude="+want[broke], &net); code != 200 {
		t.Fatalf("exclude by operator: %d", code)
	}
	if !reflect.DeepEqual(net.Excluded, []string{broke}) || !reflect.DeepEqual(net.Operators, map[string]string{broke: want[broke]}) {
		t.Errorf("exclude by operator: %+v", net)
	}
	net.Excluded, net.Operators = nil, nil
	early := selfAddrs["earlyonly"]
	if code := get(t, ts, "/v1/network?window=all&exclude="+early, &net); code != 200 {
		t.Fatalf("exclude without an operator: %d", code)
	}
	if !reflect.DeepEqual(net.Excluded, []string{early}) || net.Operators != nil {
		t.Errorf("a validator with no operator on record: %+v", net)
	}
	// a real operator address the staking set never named
	if code := get(t, ts, "/v1/network?window=all&exclude="+valoperOf(t, 0x77), nil); code != 404 {
		t.Errorf("exclude of an unknown operator: %d, want 404", code)
	}
}

// atomLinks is a feed as far as its links: the feed's own, and each entry's.
type atomLinks struct {
	Links []struct {
		Rel  string `xml:"rel,attr"`
		Href string `xml:"href,attr"`
	} `xml:"link"`
	Entries []struct {
		ID       string `xml:"id"`
		Category struct {
			Term string `xml:"term,attr"`
		} `xml:"category"`
		Link struct {
			Href string `xml:"href,attr"`
		} `xml:"link"`
	} `xml:"entry"`
}

func fetchAtomLinks(t *testing.T, ts *httptest.Server, path string) atomLinks {
	t.Helper()
	req, _ := http.NewRequest("GET", ts.URL+path, nil)
	req.Header.Set("X-Forwarded-Host", "obs.example")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		t.Fatalf("%s: %d %s", path, resp.StatusCode, body)
	}
	var d atomLinks
	if err := xml.Unmarshal(body, &d); err != nil {
		t.Fatalf("%s: %v", path, err)
	}
	return d
}

// The feeds link a validator's page by its operator address, as the site
// does, and by the consensus address for a validator with none on record.
// Only the links move: an entry's ID is minted from the consensus address
// and stays what it was, or every reader would see every entry again.
func TestFeedsLinkValidatorsByOperatorAddress(t *testing.T) {
	f := newHostingFixture(t)
	before := httptest.NewServer(api.New(f.st, "test"))
	defer before.Close()
	was := fetchAtomLinks(t, before, "/v1/validators/"+f.addrs[1]+"/feed.atom")

	// Alpha and Beta get an operator address; Gamma has none on record.
	ops := [2]string{valoperOf(t, 0xa1), valoperOf(t, 0xa2)}
	ids := []scan.ValidatorIdentity{
		{ConsAddressHex: f.addrs[0], OperatorAddress: ops[0], Moniker: "Alpha <script>", Tokens: "50000000", Status: "BOND_STATUS_BONDED"},
		{ConsAddressHex: f.addrs[1], OperatorAddress: ops[1], Moniker: "Beta", Tokens: "20000000", Status: "BOND_STATUS_BONDED"},
	}
	if _, err := f.st.UpsertValidatorIdentities(ids, time.Now()); err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(api.New(f.st, "test"))
	defer ts.Close()

	pageOf := func(addr string) string { return "/validator/?addr=" + addr }
	for i, op := range ops {
		d := fetchAtomLinks(t, ts, "/v1/validators/"+f.addrs[i]+"/feed.atom")
		if len(d.Entries) == 0 {
			t.Fatalf("validator %d: no entries", i)
		}
		for _, e := range d.Entries {
			if e.Link.Href != pageOf(op) {
				t.Errorf("validator %d: %s links %q, want %q", i, e.Category.Term, e.Link.Href, pageOf(op))
			}
		}
		for _, l := range d.Links {
			if l.Rel == "alternate" && l.Href != pageOf(op) {
				t.Errorf("validator %d: the feed's page is %q, want %q", i, l.Href, pageOf(op))
			}
		}
	}
	// the same entries under the same IDs as before the operator was known
	now := fetchAtomLinks(t, ts, "/v1/validators/"+ops[1]+"/feed.atom")
	if len(now.Entries) != len(was.Entries) {
		t.Fatalf("entries: %d, were %d", len(now.Entries), len(was.Entries))
	}
	for i := range was.Entries {
		if now.Entries[i].ID != was.Entries[i].ID {
			t.Errorf("entry %d: id %s, was %s", i, now.Entries[i].ID, was.Entries[i].ID)
		}
		if was.Entries[i].Link.Href != pageOf(f.addrs[1]) {
			t.Errorf("entry %d linked %q before an operator was known, want the consensus address", i, was.Entries[i].Link.Href)
		}
	}

	// Gamma keeps the consensus address
	for _, e := range fetchAtomLinks(t, ts, "/v1/validators/"+f.addrs[2]+"/feed.atom").Entries {
		if e.Link.Href != pageOf(f.addrs[2]) {
			t.Errorf("Gamma: %s links %q, want %q", e.Category.Term, e.Link.Href, pageOf(f.addrs[2]))
		}
	}

	// the network feed: Beta's host change
	found := false
	for _, e := range fetchAtomLinks(t, ts, "/v1/feed.atom").Entries {
		if e.Category.Term == "host-changed" {
			found = true
			if e.Link.Href != pageOf(ops[1]) {
				t.Errorf("network feed: host change links %q, want %q", e.Link.Href, pageOf(ops[1]))
			}
		}
		if strings.Contains(e.Link.Href, "/validator/") && !strings.HasPrefix(e.Link.Href, "/validator/?addr=celestiavaloper1") &&
			e.Link.Href != pageOf(f.addrs[2]) {
			t.Errorf("network feed: %s links %q", e.Category.Term, e.Link.Href)
		}
	}
	if !found {
		t.Fatal("network feed: no host change entry")
	}
}
