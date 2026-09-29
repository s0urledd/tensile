package api_test

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/cosmos/cosmos-sdk/types/bech32"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/probe"
	"github.com/plsgiveup/fibre/fibre-sentinel/internal/scan"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/api"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/hosting"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
)

// hostingFixture is a store with three validators behind an open endpoint:
// two on Hetzner, one on OVH, stakes 50/20/30, plus heartbeats that go
// down for twenty minutes and come back, and a chain registration history.
type hostingFixture struct {
	st      *store.Store
	dir     string
	addrs   [3]string // hex
	bechs   [3]string
	hosts   [3]string
	started time.Time
}

func newHostingFixture(t *testing.T) *hostingFixture {
	t.Helper()
	f := &hostingFixture{dir: t.TempDir()}
	st, err := store.Open(filepath.Join(f.dir, "observer.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	f.st = st
	if err := hosting.EnsureSchema(st.DB()); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Second)
	f.started = now.Add(-3 * time.Hour)
	ips := [3]string{"5.9.1.1", "5.9.2.2", "51.68.3.3"}
	var ids []scan.ValidatorIdentity
	var provs []scan.FibreProvider
	for i := range 3 {
		raw := bytes.Repeat([]byte{byte(0x10 * (i + 1))}, 20)
		f.addrs[i] = hex.EncodeToString(raw)
		f.bechs[i], _ = bech32.ConvertAndEncode("celestiavalcons", raw)
		f.hosts[i] = []string{"a.example:7980", "b.example:7980", "c.example:7980"}[i]
		ids = append(ids, scan.ValidatorIdentity{ConsAddressHex: f.addrs[i], Moniker: []string{"Alpha <script>", "Beta", "Gamma"}[i],
			Tokens: []string{"50000000", "20000000", "30000000"}[i], Status: "BOND_STATUS_BONDED"})
		provs = append(provs, scan.FibreProvider{ConsAddressBech32: f.bechs[i], Host: f.hosts[i]})
	}
	if _, err := st.UpsertValidatorIdentities(ids, now); err != nil {
		t.Fatal(err)
	}
	if _, _, err := st.ObserveEndpoints(context.Background(), provs, 100, f.started); err != nil {
		t.Fatal(err)
	}
	if err := st.SetMeta("chain_id", "mocha-test", now); err != nil {
		t.Fatal(err)
	}
	// Alpha: up, down for four beats (20 min), up again.
	for b := 0; b < 30; b++ {
		at := f.started.Add(time.Duration(b) * 5 * time.Minute)
		for i := range 3 {
			up := !(i == 0 && b >= 10 && b < 14)
			m := probe.Measurement{Vantage: "test", ValidatorAddress: f.addrs[i], ValidatorHost: f.hosts[i], ScheduledAt: at, StartedAt: at,
				DNS: probe.StepResult{Attempted: true, OK: true, Detail: ips[i]}, TCP: probe.StepResult{Attempted: true, OK: up}, Outcome: probe.OutcomeReachable}
			m.TLS.OK, m.Identity.OK = up, up
			if up {
				m.TCP.Detail = "-> " + ips[i] + ":7980"
			} else {
				m.Outcome = probe.OutcomeTCPRefused
			}
			raw, _ := json.Marshal(m)
			if _, err := st.InsertReachability(m, raw); err != nil {
				t.Fatal(err)
			}
		}
	}
	// chain history for Beta: seeded, then an event changing its host
	for _, e := range []scan.HostEvent{
		{HostEntry: scan.HostEntry{FromHeight: 50, ConsAddress: f.addrs[1], Host: "old.example:7980", Source: "seed"}, Time: f.started.Add(-time.Hour)},
		{HostEntry: scan.HostEntry{FromHeight: 90, FromTxIndex: 2, ConsAddress: f.addrs[1], Host: f.hosts[1], Source: "event"}, Time: f.started.Add(-30 * time.Minute)},
	} {
		if _, err := st.ReplayHostEvent(e); err != nil {
			t.Fatal(err)
		}
	}
	return f
}

const testASN = "5.9.0.0\t5.9.255.255\t24940\tDE\tHETZNER-AS\n51.68.0.0\t51.68.255.255\t16276\tFR\tOVH\n"

func (f *hostingFixture) enable(t *testing.T) {
	t.Helper()
	var b bytes.Buffer
	w := gzip.NewWriter(&b)
	w.Write([]byte(testASN))
	w.Close()
	p := filepath.Join(f.dir, "ip2asn-combined.tsv.gz")
	if err := os.WriteFile(p, b.Bytes(), 0o644); err != nil {
		t.Fatal(err)
	}
	r := &hosting.Refresher{DB: f.st.DB(), Cfg: hosting.Config{ASNPath: p}}
	if res, err := r.Run(context.Background(), time.Now()); err != nil || res.WithASN != 3 {
		t.Fatalf("refresh: %+v %v", res, err)
	}
}

func TestHostingOffThenOn(t *testing.T) {
	f := newHostingFixture(t)
	ts := httptest.NewServer(api.New(f.st, "test"))
	defer ts.Close()

	var off struct {
		Sources struct {
			Enabled bool `json:"enabled"`
		} `json:"sources"`
		Summary      *json.RawMessage `json:"summary"`
		ProviderASNs []struct {
			ASN      uint32 `json:"asn"`
			Provider string `json:"provider"`
		} `json:"provider_asns"`
	}
	if code := get(t, ts, "/v1/hosting", &off); code != 200 || off.Sources.Enabled || off.Summary != nil || len(off.ProviderASNs) == 0 {
		t.Fatalf("off: %d %+v", code, off)
	}
	if code := get(t, ts, "/v1/hosting?as_of=2026-01-01T00:00:00Z", nil); code != 400 {
		t.Fatalf("as_of: %d", code)
	}

	f.enable(t)
	// a fresh server, so the validator snapshot is computed after the lookup
	ts2 := httptest.NewServer(api.New(f.st, "test"))
	defer ts2.Close()
	var on struct {
		Sources struct {
			Enabled bool `json:"enabled"`
			ASN     struct {
				License string `json:"license"`
			} `json:"asn_db"`
		} `json:"sources"`
		Summary struct {
			Registered int `json:"registered_hosts"`
			Resolved   int `json:"resolved_hosts"`
			ByProvider []struct {
				Key        string  `json:"key"`
				Hosts      int     `json:"hosts"`
				StakeShare float64 `json:"stake_share"`
			} `json:"by_provider"`
			Nakamoto struct {
				Provider struct {
					Count    *int     `json:"count"`
					Entities []string `json:"entities"`
				} `json:"provider"`
			} `json:"nakamoto_third"`
		} `json:"summary"`
	}
	if code := get(t, ts2, "/v1/hosting", &on); code != 200 || !on.Sources.Enabled || !strings.Contains(on.Sources.ASN.License, "PDDL") {
		t.Fatalf("on: %d %+v", code, on)
	}
	s := on.Summary
	if s.Registered != 3 || s.Resolved != 3 || s.ByProvider[0].Key != "Hetzner" || s.ByProvider[0].Hosts != 2 || s.ByProvider[0].StakeShare < 0.69 || s.ByProvider[0].StakeShare > 0.71 {
		t.Fatalf("summary: %+v", s)
	}
	if s.Nakamoto.Provider.Count == nil || *s.Nakamoto.Provider.Count != 1 {
		t.Fatalf("nakamoto: %+v", s.Nakamoto)
	}

	var vals struct {
		Validators []struct {
			Address string        `json:"address"`
			Hosting *hosting.Info `json:"hosting"`
		} `json:"validators"`
	}
	if code := get(t, ts2, "/v1/validators", &vals); code != 200 {
		t.Fatalf("validators: %d", code)
	}
	seen := 0
	for _, v := range vals.Validators {
		if v.Hosting == nil {
			t.Fatalf("%s: no hosting", v.Address)
		}
		seen++
		if v.Address == f.addrs[2] && (v.Hosting.Provider != "OVH" || v.Hosting.ASN != 16276 || v.Hosting.IP != "51.68.3.3") {
			t.Fatalf("gamma: %+v", v.Hosting)
		}
	}
	if seen != 3 {
		t.Fatalf("rows: %d", seen)
	}
	// a pinned window gets no hosting: it is the current lookup only
	var pinned struct {
		Validators []struct {
			Hosting *hosting.Info `json:"hosting"`
		} `json:"validators"`
	}
	get(t, ts2, "/v1/validators?as_of="+time.Now().UTC().Add(-time.Minute).Format(time.RFC3339), &pinned)
	for _, v := range pinned.Validators {
		if v.Hosting != nil {
			t.Fatal("hosting on a pinned window")
		}
	}
}

type atomDoc struct {
	XMLName xml.Name
	ID      string `xml:"id"`
	Title   string `xml:"title"`
	Links   []struct {
		Rel  string `xml:"rel,attr"`
		Href string `xml:"href,attr"`
	} `xml:"link"`
	Entries []struct {
		ID       string `xml:"id"`
		Title    string `xml:"title"`
		Updated  string `xml:"updated"`
		Category struct {
			Term string `xml:"term,attr"`
		} `xml:"category"`
	} `xml:"entry"`
}

func fetchAtom(t *testing.T, ts *httptest.Server, path string, hdr map[string]string) (*http.Response, atomDoc) {
	t.Helper()
	req, _ := http.NewRequest("GET", ts.URL+path, nil)
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	var d atomDoc
	if resp.StatusCode == 200 {
		if err := xml.Unmarshal(body, &d); err != nil {
			t.Fatalf("%s: not XML: %v\n%s", path, err, body)
		}
	}
	return resp, d
}

func TestValidatorFeed(t *testing.T) {
	f := newHostingFixture(t)
	ts := httptest.NewServer(api.New(f.st, "test"))
	defer ts.Close()

	resp, d := fetchAtom(t, ts, "/v1/validators/"+f.addrs[0]+"/feed.atom", map[string]string{"X-Forwarded-Host": "obs.example"})
	if resp.StatusCode != 200 || !strings.HasPrefix(resp.Header.Get("Content-Type"), "application/atom+xml") {
		t.Fatalf("status %d %s", resp.StatusCode, resp.Header.Get("Content-Type"))
	}
	if resp.Header.Get("Cache-Control") != "public, max-age=300" || resp.Header.Get("ETag") == "" {
		t.Fatalf("cache headers: %v", resp.Header)
	}
	if d.XMLName.Space != "http://www.w3.org/2005/Atom" || !strings.HasPrefix(d.ID, "tag:obs.example,2026:tensile/mocha-test/") {
		t.Fatalf("feed id: %s", d.ID)
	}
	if !strings.Contains(d.Title, "Alpha <script>") {
		t.Fatalf("title: %q", d.Title)
	}
	terms := map[string]string{}
	for _, e := range d.Entries {
		terms[e.Category.Term] = e.ID
	}
	for _, k := range []string{"unreachable", "recovered", "first-reachable", "bonded-joined"} {
		if terms[k] == "" {
			t.Fatalf("no %s entry: %+v", k, d.Entries)
		}
	}
	// unreachable is dated at the first failed beat, and its ID carries that date
	wantAt := f.started.Add(50 * time.Minute).UTC().Format("20060102T150405Z")
	if !strings.HasSuffix(terms["unreachable"], "/unreachable/"+wantAt) {
		t.Fatalf("unreachable id %s, want suffix %s", terms["unreachable"], wantAt)
	}

	// deterministic: a different server (restart) produces the same IDs
	ts2 := httptest.NewServer(api.New(f.st, "test"))
	defer ts2.Close()
	_, d2 := fetchAtom(t, ts2, "/v1/validators/"+f.bechs[0]+"/feed.atom", map[string]string{"X-Forwarded-Host": "obs.example"})
	if len(d2.Entries) != len(d.Entries) {
		t.Fatalf("entries differ: %d vs %d", len(d2.Entries), len(d.Entries))
	}
	for i := range d.Entries {
		if d.Entries[i].ID != d2.Entries[i].ID {
			t.Fatalf("id %d differs: %s vs %s", i, d.Entries[i].ID, d2.Entries[i].ID)
		}
	}

	// conditional GET
	resp3, _ := fetchAtom(t, ts, "/v1/validators/"+f.addrs[0]+"/feed.atom",
		map[string]string{"X-Forwarded-Host": "obs.example", "If-None-Match": resp.Header.Get("ETag")})
	if resp3.StatusCode != http.StatusNotModified {
		t.Fatalf("If-None-Match: %d", resp3.StatusCode)
	}

	// Beta's host change comes from the chain record
	_, db := fetchAtom(t, ts, "/v1/validators/"+f.addrs[1]+"/feed.atom", nil)
	found := false
	for _, e := range db.Entries {
		if e.Category.Term == "host-changed" && strings.Contains(e.Title, "b.example:7980") && strings.HasSuffix(e.ID, "/registration/90-2") {
			found = true
		}
		if e.Category.Term == "unreachable" {
			t.Fatal("Beta never went down")
		}
	}
	if !found {
		t.Fatalf("no host change entry: %+v", db.Entries)
	}

	// unknown and malformed addresses
	if r, _ := fetchAtom(t, ts, "/v1/validators/"+strings.Repeat("ab", 20)+"/feed.atom", nil); r.StatusCode != 404 {
		t.Fatalf("unknown: %d", r.StatusCode)
	}
	if r, _ := fetchAtom(t, ts, "/v1/validators/nope/feed.atom", nil); r.StatusCode != 400 {
		t.Fatalf("malformed: %d", r.StatusCode)
	}
}

func TestNetworkFeed(t *testing.T) {
	f := newHostingFixture(t)
	ts := httptest.NewServer(api.New(f.st, "test"))
	defer ts.Close()
	resp, d := fetchAtom(t, ts, "/v1/feed.atom", nil)
	if resp.StatusCode != 200 {
		t.Fatalf("status %d", resp.StatusCode)
	}
	var changed, joined int
	for _, e := range d.Entries {
		switch e.Category.Term {
		case "host-changed":
			changed++
		case "bonded-joined":
			joined++
		}
	}
	// the host change is there; the endpoints opened by the observer's first
	// poll are not "joins" and stay out of the network feed
	if changed != 1 || joined != 0 {
		t.Fatalf("changed=%d joined=%d: %+v", changed, joined, d.Entries)
	}
	for _, l := range d.Links {
		if l.Rel == "self" && l.Href != "feed.atom" {
			t.Fatalf("self link %q", l.Href)
		}
	}
}
