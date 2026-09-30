package api_test

import (
	"bytes"
	"encoding/json"
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

// insertUnassignedReading writes one reading of hash (written before by
// insertReading) from a validator that holds none of its rows, as the
// reading of a validator asked without rows would be.
func insertUnassignedReading(t *testing.T, st *store.Store, hash string, msu, at time.Time, addr string) {
	t.Helper()
	class, reason := probe.Classify(probe.Evidence{Phase: probe.PhaseInWindow, Outcome: probe.OutcomeNotFound})
	m := probe.Measurement{
		SchemaVersion: probe.AttestationSchemaVersion, Vantage: "test",
		PromiseHash: hash, Commitment: "cc" + hash, MustServeUntil: msu, ValidatorSetHeight: 299,
		ValidatorAddress: addr, ValidatorHost: addr + ":443",
		ScheduleLabel: probe.EndReadLabel, ScheduledAt: at, StartedAt: at, FinishedAt: at,
		Phase: probe.PhaseInWindow, Outcome: probe.OutcomeNotFound,
		Classification: class, ClassificationReason: reason, TotalDurationMS: 10,
	}
	m.TCP.OK, m.TLS.OK, m.Identity.OK = true, true, true
	raw, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.InsertProbe(m, raw); err != nil {
		t.Fatal(err)
	}
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
	// asked is a validator the staking set names that holds no rows of op1
	// but was read on it all the same: a reading the assignments' join
	// cannot name, so its operator address must come from elsewhere.
	asked := strings.Repeat("e5", 20)
	insertUnassignedReading(t, st, "op1", msu, at.Add(time.Minute), asked)
	// earlyonly is not in the staking set this observer read.
	want := map[string]string{
		selfAddrs["kept"]:      valoperOf(t, 0xe1),
		selfAddrs["broke"]:     valoperOf(t, 0xe2),
		selfAddrs["silent"]:    valoperOf(t, 0xe3),
		selfAddrs["earlyonly"]: "",
		asked:                  valoperOf(t, 0xe5),
	}
	var ids []scan.ValidatorIdentity
	for _, a := range []string{selfAddrs["kept"], selfAddrs["broke"], selfAddrs["silent"], asked} {
		ids = append(ids, scan.ValidatorIdentity{ConsAddressHex: a, OperatorAddress: want[a], Moniker: "m" + a[:4],
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
	check("/v1/probes?blob=", probes.Probes, 4)
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
	check("blob readings", blob.Probes, 4)
	// the reading of a validator with no assignment on the blob names its
	// operator address too
	for _, a := range blob.Assignments {
		if a.Validator == asked {
			t.Fatalf("assignments: %s holds no rows of op1 but is listed", asked)
		}
	}
	found := false
	for _, p := range blob.Probes {
		found = found || (p.Validator == asked && p.Operator == want[asked])
	}
	if !found {
		t.Errorf("blob readings: no reading of %s with operator_address %s: %+v", asked, want[asked], blob.Probes)
	}

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

// An operator whose validator was removed and created again under a new
// consensus key leaves two identities with one operator address, since the
// table is only ever upserted. The operator address names the newer one:
// the older consensus key carries none anywhere, so every link to its page
// keeps the consensus address and still opens that page, not the newer
// validator's.
func TestSupersededConsensusKeyKeepsItsHexLink(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "observer.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	now := time.Now().UTC().Truncate(time.Second)
	created, msu := now.Add(-2*time.Hour), now.Add(-30*time.Minute)
	at := msu.Add(-10 * time.Minute)
	old, cur := selfAddrs["kept"], selfAddrs["broke"]
	insertReading(t, st, "old1", created, msu, 4, probe.EndReadLabel, at, []endVal{{addr: old, rows: 4, w: ok}})
	insertReading(t, st, "cur1", created, msu, 4, probe.EndReadLabel, at, []endVal{{addr: cur, rows: 4, w: ok}})
	op := valoperOf(t, 0xe1)
	// The old key as the staking set last listed it, then the new one under
	// the same operator; the old row is never touched again.
	if _, err := st.UpsertValidatorIdentities([]scan.ValidatorIdentity{{ConsAddressHex: old, OperatorAddress: op, Moniker: "again",
		Tokens: "1000000", Status: "BOND_STATUS_UNBONDED"}}, now.Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}
	if _, err := st.UpsertValidatorIdentities([]scan.ValidatorIdentity{{ConsAddressHex: cur, OperatorAddress: op, Moniker: "again",
		Tokens: "1000000", Status: "BOND_STATUS_BONDED"}}, now); err != nil {
		t.Fatal(err)
	}
	ts := httptestServer(t, st)
	want := map[string]string{old: "", cur: op}

	type named struct {
		Validator string `json:"validator_address"`
		Operator  string `json:"operator_address"`
	}
	for _, blob := range []string{"old1", "cur1"} {
		var probes struct {
			Probes []named `json:"probes"`
		}
		if code := get(t, ts, "/v1/probes?blob="+blob, &probes); code != 200 || len(probes.Probes) != 1 {
			t.Fatalf("probes of %s: %d, %+v", blob, code, probes)
		}
		var b struct {
			Assignments []named `json:"assignments"`
			Probes      []named `json:"probes"`
		}
		if code := get(t, ts, "/v1/blobs/"+blob, &b); code != 200 || len(b.Assignments) != 1 || len(b.Probes) != 1 {
			t.Fatalf("blob %s: %d, %+v", blob, code, b)
		}
		for what, r := range map[string]named{"probes": probes.Probes[0], "assignment": b.Assignments[0], "blob reading": b.Probes[0]} {
			if r.Operator != want[r.Validator] {
				t.Errorf("%s of %s: %s has operator_address %q, want %q", what, blob, r.Validator, r.Operator, want[r.Validator])
			}
		}
	}

	var list struct {
		Validators []struct {
			Address  string `json:"address"`
			Operator string `json:"operator_address"`
		} `json:"validators"`
	}
	if code := get(t, ts, "/v1/validators?window=all", &list); code != 200 {
		t.Fatalf("validators: %d", code)
	}
	seen := 0
	for _, v := range list.Validators {
		if w, mine := want[v.Address]; mine {
			seen++
			if v.Operator != w {
				t.Errorf("list: %s has operator_address %q, want %q", v.Address, v.Operator, w)
			}
		}
	}
	if seen != 2 {
		t.Fatalf("list: %d of the two validators", seen)
	}

	// the operator address opens the newer validator; the old one's own
	// page names no operator address
	var status struct {
		Address  string `json:"address"`
		Operator string `json:"operator_address"`
	}
	if code := get(t, ts, "/v1/validators/"+op+"/status?window=all", &status); code != 200 || status.Address != cur || status.Operator != op {
		t.Errorf("status by operator: %d, %+v, want %s", code, status, cur)
	}
	status.Address, status.Operator = "", ""
	if code := get(t, ts, "/v1/validators/"+old+"/status?window=all", &status); code != 200 || status.Address != old || status.Operator != "" {
		t.Errorf("status of the old key: %d, %+v, want %s and no operator address", code, status, old)
	}

	// each feed links its own validator's page
	for addr, page := range map[string]string{old: "/validator/?addr=" + old, cur: "/validator/?addr=" + op} {
		found := false
		for _, l := range fetchAtomLinks(t, ts, "/v1/validators/"+addr+"/feed.atom").Links {
			if l.Rel == "alternate" {
				found = true
				if l.Href != page {
					t.Errorf("feed of %s: its page is %q, want %q", addr, l.Href, page)
				}
			}
		}
		if !found {
			t.Errorf("feed of %s: no page link", addr)
		}
	}
}
