package hosting

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/cosmos/cosmos-sdk/types/bech32"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/probe"
	"github.com/plsgiveup/fibre/fibre-sentinel/internal/scan"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
)

// A slice of the real iptoasn format: IPv4 and IPv6 ranges, a "Not routed"
// range, and a description with spaces.
const asnTSV = "1.0.0.0\t1.0.0.255\t13335\tUS\tCLOUDFLARENET\n" +
	"1.0.1.0\t1.0.3.255\t0\tNone\tNot routed\n" +
	"5.9.0.0\t5.9.255.255\t24940\tDE\tHETZNER-AS\n" +
	"51.68.0.0\t51.68.255.255\t16276\tFR\tOVH\n" +
	"203.0.113.0\t203.0.113.255\t64500\tNL\tEXAMPLE-NET Example B.V.\n" +
	"2a01:4f8::\t2a01:4f8:ffff:ffff:ffff:ffff:ffff:ffff\t24940\tDE\tHETZNER-AS\n"

// And of DB-IP's: no header, ZZ for unknown.
const countryCSV = "0.0.0.0,0.255.255.255,ZZ\n" +
	"5.9.0.0,5.9.127.255,DE\n" +
	"5.9.128.0,5.9.255.255,FI\n" +
	"51.68.0.0,51.68.255.255,PL\n" +
	"2a01:4f8::,2a01:4f8:ffff:ffff:ffff:ffff:ffff:ffff,DE\n"

func writeFile(t *testing.T, dir, name, body string, gz bool) string {
	t.Helper()
	p := filepath.Join(dir, name)
	data := []byte(body)
	if gz {
		var b bytes.Buffer
		w := gzip.NewWriter(&b)
		_, _ = w.Write(data)
		_ = w.Close()
		data = b.Bytes()
	}
	if err := os.WriteFile(p, data, 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

func addrs(ss ...string) []netip.Addr {
	var out []netip.Addr
	for _, s := range ss {
		out = append(out, netip.MustParseAddr(s))
	}
	return out
}

func TestLookupASN(t *testing.T) {
	dir := t.TempDir()
	for _, gz := range []bool{false, true} {
		p := writeFile(t, dir, "asn.tsv", asnTSV, gz)
		got, err := LookupASN(p, addrs("5.9.10.10", "1.0.2.1", "2a01:4f8:c0c:1::1", "203.0.113.9", "9.9.9.9", "::ffff:51.68.1.1"))
		if err != nil {
			t.Fatal(err)
		}
		want := map[string]ASNRecord{
			"5.9.10.10":         {24940, "HETZNER-AS", "DE"},
			"2a01:4f8:c0c:1::1": {24940, "HETZNER-AS", "DE"},
			"203.0.113.9":       {64500, "EXAMPLE-NET Example B.V.", "NL"},
			"51.68.1.1":         {16276, "OVH", "FR"}, // IPv4-mapped input is unmapped
		}
		if len(got) != len(want) {
			t.Fatalf("gz=%v: got %v", gz, got)
		}
		for a, w := range want {
			if got[netip.MustParseAddr(a)] != w {
				t.Fatalf("gz=%v %s: got %+v want %+v", gz, a, got[netip.MustParseAddr(a)], w)
			}
		}
		// not routed and not covered: absent, never AS 0
		if _, ok := got[netip.MustParseAddr("1.0.2.1")]; ok {
			t.Fatal("a Not routed range must not produce a record")
		}
	}
}

func TestLookupRejectsWrongFile(t *testing.T) {
	p := writeFile(t, t.TempDir(), "x.tsv", "<html>not a database</html>\n", false)
	if _, err := LookupASN(p, addrs("5.9.1.1")); err == nil {
		t.Fatal("a file with no readable range must be an error, not an empty answer")
	}
	// no targets: the file is not even opened
	if got, err := LookupASN("/does/not/exist", nil); err != nil || len(got) != 0 {
		t.Fatalf("empty target set: %v %v", got, err)
	}
}

func TestLookupCountry(t *testing.T) {
	p := writeFile(t, t.TempDir(), "c.csv.gz", countryCSV, true)
	got, err := LookupCountry(p, addrs("5.9.10.10", "5.9.200.1", "0.1.2.3", "2a01:4f8::5", "8.8.8.8"))
	if err != nil {
		t.Fatal(err)
	}
	if got[netip.MustParseAddr("5.9.10.10")] != "DE" || got[netip.MustParseAddr("5.9.200.1")] != "FI" ||
		got[netip.MustParseAddr("2a01:4f8::5")] != "DE" || len(got) != 3 {
		t.Fatalf("got %v", got)
	}
}

func TestProviderFor(t *testing.T) {
	cases := map[uint32]string{24940: ProviderHetzner, 213230: ProviderHetzner, 16276: ProviderOVH, 16509: ProviderAWS,
		396982: ProviderGCP, 8075: ProviderAzure, 14061: ProviderDigitalOcean, 51167: ProviderContabo,
		20473: ProviderVultr, 63949: ProviderAkamaiLinode, 13335: ProviderOther, 0: ProviderUnknown,
		// Amazon's corporate AS is deliberately not AWS
		7224: ProviderOther,
		// the providers Mocha's Fibre hosts actually resolved to after activation
		16125: ProviderCherry, 59642: ProviderCherry, 201814: ProviderMevspace, 12876: ProviderScaleway,
		205544: ProviderLeaseweb, 60781: ProviderLeaseweb, 29066: ProviderVelia, 30083: ProviderVelia,
		63023: ProviderGTHost, 62563: ProviderGTHost, 396356: ProviderLatitude, 20326: ProviderTeraswitch,
		31034: ProviderAruba}
	for asn, want := range cases {
		if got := ProviderFor(asn); got != want {
			t.Errorf("AS%d: %s, want %s", asn, got, want)
		}
	}
	for _, p := range ProviderASNs() {
		if p.Provider == ProviderOther || p.Provider == ProviderUnknown {
			t.Fatalf("remainder bucket in the published list: %+v", p)
		}
	}
}

func TestAddressesFromMeasurement(t *testing.T) {
	mk := func(dns, tcp string) []byte {
		b, _ := json.Marshal(map[string]any{"dns": map[string]any{"detail": dns}, "tcp": map[string]any{"detail": tcp}})
		return b
	}
	cases := []struct {
		dns, tcp  string
		want      []string
		connected string
	}{
		{"5.9.1.1,2a01:4f8::1", "-> 5.9.1.1:7980", []string{"5.9.1.1", "2a01:4f8::1"}, "5.9.1.1"},
		{"5.9.1.1,2a01:4f8::1", "failed 5.9.1.1:7980: refused; -> [2a01:4f8::1]:7980", []string{"5.9.1.1", "2a01:4f8::1"}, "2a01:4f8::1"},
		{"5.9.1.1 (not dialled: 10.0.0.1 (private))", "", []string{"5.9.1.1"}, ""},
		{"literal IP 51.68.3.3", "-> 51.68.3.3:7980", []string{"51.68.3.3"}, "51.68.3.3"},
		{"", "", nil, ""},
		// private addresses are never returned, even if a row carried one
		{"192.168.1.1,127.0.0.1", "", nil, ""},
		// nor shared (carrier-grade NAT, an overlay network) or other
		// special-purpose ones, connected or not
		{"100.64.1.1,5.9.1.1", "failed 100.64.1.1:7980: timeout; -> 5.9.1.1:7980", []string{"5.9.1.1"}, "5.9.1.1"},
		{"100.100.1.1", "-> 100.100.1.1:7980", nil, ""},
		{"198.18.0.1,2001:db8::1,64:ff9b::a00:1", "", nil, ""},
	}
	for i, c := range cases {
		got, conn := AddressesFromMeasurement(mk(c.dns, c.tcp))
		if len(got) != len(c.want) {
			t.Fatalf("%d: got %v want %v", i, got, c.want)
		}
		for j := range got {
			if got[j].String() != c.want[j] {
				t.Fatalf("%d: got %v want %v", i, got, c.want)
			}
		}
		if (c.connected == "") != !conn.IsValid() || (conn.IsValid() && conn.String() != c.connected) {
			t.Fatalf("%d: connected %v want %q", i, conn, c.connected)
		}
	}
	if got, _ := AddressesFromMeasurement([]byte("not json")); got != nil {
		t.Fatal("garbage must yield nothing")
	}
}

func TestConcentrate(t *testing.T) {
	info := func(p string, asn uint32, cc string) *Info {
		return &Info{Status: "ok", IP: "192.0.2.1", Provider: p, ASN: asn, Country: cc, ASOrg: "org"}
	}
	members := []Member{
		{"a", 30, info(ProviderHetzner, 24940, "DE")},
		{"b", 10, info(ProviderHetzner, 213230, "FI")},
		{"c", 25, info(ProviderOther, 64500, "NL")},
		{"d", 20, info(ProviderOVH, 16276, "FR")},
		{"e", 15, nil}, // unresolved: in every denominator
	}
	s := Concentrate(members)
	if s.Registered != 5 || s.Resolved != 4 || s.Unresolved != 1 || s.TotalStake != 100 || s.ResolvedStake != 85 {
		t.Fatalf("totals: %+v", s)
	}
	if s.ByProvider[0].Key != ProviderHetzner || s.ByProvider[0].Stake != 40 || s.ByProvider[0].StakeShare != 0.4 || s.ByProvider[0].Hosts != 2 {
		t.Fatalf("by provider: %+v", s.ByProvider)
	}
	// Hetzner alone is 40% > 1/3: one entity
	if n := s.Nakamoto.Provider; n.Count == nil || *n.Count != 1 || n.Entities[0] != ProviderHetzner {
		t.Fatalf("provider nakamoto: %+v", n)
	}
	// by AS: 30 (AS24940) + 25 (AS64500) = 55 > 33.3 → 2
	if n := s.Nakamoto.ASN; n.Count == nil || *n.Count != 2 {
		t.Fatalf("asn nakamoto: %+v", n)
	}
	// by country: DE 30, NL 25 → 2
	if n := s.Nakamoto.Country; n.Count == nil || *n.Count != 2 || n.Entities[0] != "DE" {
		t.Fatalf("country nakamoto: %+v", n)
	}

	// Other is never an entity: only Other and Unknown hold stake
	s = Concentrate([]Member{{"a", 50, info(ProviderOther, 1, "US")}, {"b", 50, nil}})
	if s.Nakamoto.Provider.Count != nil {
		t.Fatalf("Other/Unknown counted as an entity: %+v", s.Nakamoto.Provider)
	}
	// exactly a third is not more than a third
	s = Concentrate([]Member{{"a", 1, info(ProviderAWS, 16509, "US")}, {"b", 1, info(ProviderGCP, 15169, "US")}, {"c", 1, info(ProviderOVH, 16276, "FR")}})
	if n := s.Nakamoto.Provider; n.Count == nil || *n.Count != 2 {
		t.Fatalf("a third must need a second entity: %+v", n)
	}
	// no stake known: counted over hosts, and says so
	s = Concentrate([]Member{{"a", 0, info(ProviderAWS, 16509, "US")}, {"b", 0, info(ProviderOVH, 16276, "FR")}})
	if s.Basis != "hosts" || s.Nakamoto.Provider.Count == nil || *s.Nakamoto.Provider.Count != 1 {
		t.Fatalf("hosts basis: %+v", s)
	}
}

// ---- the collector pass, against a real store ----

func consBech(t *testing.T, b byte) (string, string) {
	t.Helper()
	raw := bytes.Repeat([]byte{b}, 20)
	s, err := bech32.ConvertAndEncode("celestiavalcons", raw)
	if err != nil {
		t.Fatal(err)
	}
	return s, string(bytes.Repeat([]byte{"0123456789abcdef"[b>>4], "0123456789abcdef"[b&15]}, 20))
}

func beat(t *testing.T, st *store.Store, addrHex, host string, at time.Time, dns, tcp string) {
	t.Helper()
	m := probe.Measurement{Vantage: "t", ValidatorAddress: addrHex, ValidatorHost: host, ScheduledAt: at, StartedAt: at,
		DNS: probe.StepResult{Attempted: true, OK: true, Detail: dns}, TCP: probe.StepResult{Attempted: true, OK: tcp != "", Detail: tcp},
		Outcome: probe.OutcomeReachable}
	raw, _ := json.Marshal(m)
	if _, err := st.InsertReachability(m, raw); err != nil {
		t.Fatal(err)
	}
}

func TestRefresherEndToEnd(t *testing.T) {
	dir := t.TempDir()
	st, err := store.Open(filepath.Join(dir, "observer.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if err := EnsureSchema(st.DB()); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	now := time.Now().UTC()
	b1, h1 := consBech(t, 0x11)
	b2, h2 := consBech(t, 0x22)
	b3, h3 := consBech(t, 0x33)
	b4, h4 := consBech(t, 0x44)
	provs := []scan.FibreProvider{
		{ConsAddressBech32: b1, Host: "fibre.one.example:7980"},
		{ConsAddressBech32: b2, Host: "51.68.3.3:7980"}, // literal, never heartbeat-resolved here
		{ConsAddressBech32: b3, Host: "gone.example:7980"},
		{ConsAddressBech32: b4, Host: "multi.example:7980"},
	}
	if _, _, err := st.ObserveEndpoints(ctx, provs, 100, now.Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}
	beat(t, st, h1, "fibre.one.example:7980", now.Add(-10*time.Minute), "5.9.1.1", "-> 5.9.1.1:7980")
	// an older resolution of another host for the same validator is ignored
	beat(t, st, h1, "old.example:7980", now.Add(-20*time.Minute), "51.68.1.1", "")
	// resolved long ago: outside the lookback, so unresolved now
	beat(t, st, h3, "gone.example:7980", now.Add(-48*time.Hour), "5.9.9.9", "")
	// two networks behind one name; the connected one is primary
	beat(t, st, h4, "multi.example:7980", now.Add(-5*time.Minute), "51.68.7.7,2a01:4f8::7", "failed 51.68.7.7:7980: timeout; -> [2a01:4f8::7]:7980")
	_ = h2

	var logs []string
	r := &Refresher{DB: st.DB(), Logf: func(f string, a ...any) { logs = append(logs, f) }, Record: recordOf(st)}

	// off: no file configured
	res, err := r.Run(ctx, now)
	if err != nil || res.Enabled {
		t.Fatalf("off: %+v %v", res, err)
	}
	if src, _ := ReadSources(ctx, st.DB()); src.Enabled {
		t.Fatal("sources enabled with no file")
	}

	r.Cfg = Config{ASNPath: writeFile(t, dir, DefaultASNFile, asnTSV, true)}
	res, err = r.Run(ctx, now)
	if err != nil {
		t.Fatal(err)
	}
	if res.Hosts != 4 || res.Resolved != 3 || res.WithASN != 3 {
		t.Fatalf("run: %+v", res)
	}
	cur, err := Current(ctx, st.DB())
	if err != nil {
		t.Fatal(err)
	}
	if in := cur[h1]; in.Status != "ok" || in.ASN != 24940 || in.Provider != ProviderHetzner || in.Country != "DE" ||
		in.CountryBasis != "as_registry" || in.IP != "5.9.1.1" || in.ResolvedBy != "heartbeat" || in.Host != "fibre.one.example:7980" {
		t.Fatalf("h1: %+v", in)
	}
	if in := cur[h2]; in.Status != "ok" || in.Provider != ProviderOVH || in.ResolvedBy != "literal" {
		t.Fatalf("h2 literal: %+v", in)
	}
	if in := cur[h3]; in.Status != "unresolved" || in.Provider != ProviderUnknown || in.IP != "" {
		t.Fatalf("h3: %+v", in)
	}
	if in := cur[h4]; in.IP != "2a01:4f8::7" || in.Provider != ProviderHetzner || !in.MixedNetworks || len(in.Addresses) != 2 {
		t.Fatalf("h4: %+v", in)
	}

	// unchanged: skipped
	if res, _ := r.Run(ctx, now.Add(time.Minute)); !res.Skipped {
		t.Fatalf("unchanged run not skipped: %+v", res)
	}

	// add the country file: re-run, geolocation basis
	r.Cfg.CountryPath = writeFile(t, dir, DefaultCountryFile, countryCSV, true)
	if res, err := r.Run(ctx, now.Add(2*time.Minute)); err != nil || res.Skipped {
		t.Fatalf("country added: %+v %v", res, err)
	}
	cur, _ = Current(ctx, st.DB())
	if in := cur[h1]; in.Country != "DE" || in.CountryBasis != "geolocation" {
		t.Fatalf("h1 with country db: %+v", in)
	}
	if in := cur[h2]; in.Country != "PL" || in.CountryBasis != "geolocation" {
		t.Fatalf("h2 with country db: %+v", in)
	}
	src, err := ReadSources(ctx, st.DB())
	if err != nil || !src.Enabled || src.ASN == nil || src.Country == nil || src.Country.Attribution == "" || src.ASN.File != DefaultASNFile {
		t.Fatalf("sources: %+v %v", src, err)
	}

	// the file goes away: feature off, rows cleared
	os.Remove(r.Cfg.ASNPath)
	res, err = r.Run(ctx, now.Add(3*time.Minute))
	if err != nil || res.Enabled || res.Cleared != 4 {
		t.Fatalf("removed file: %+v %v", res, err)
	}
	if cur, _ := Current(ctx, st.DB()); len(cur) != 0 {
		t.Fatalf("rows left after the feature went off: %v", cur)
	}
	if src, _ := ReadSources(ctx, st.DB()); src.Enabled {
		t.Fatal("still enabled after the file went away")
	}
}

func TestCurrentWithoutTable(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "observer.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	cur, err := Current(context.Background(), st.DB())
	if err != nil || len(cur) != 0 {
		t.Fatalf("a database without the table is 'off', not an error: %v %v", cur, err)
	}
}

func TestResolveConfig(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("HOSTING_ASN_DB", "")
	t.Setenv("HOSTING_COUNTRY_DB", "")
	t.Setenv("HOSTING_CITY_DB", "")
	if c := ResolveConfig("", "", "", dir); c.ASNPath != "" || c.CountryPath != "" || c.CityPath != "" {
		t.Fatalf("nothing there: %+v", c)
	}
	os.MkdirAll(filepath.Join(dir, "hosting"), 0o755)
	def := writeFile(t, filepath.Join(dir, "hosting"), DefaultASNFile, asnTSV, true)
	if c := ResolveConfig("", "", "", dir); c.ASNPath != def {
		t.Fatalf("default file: %+v", c)
	}
	t.Setenv("HOSTING_ASN_DB", "/env/asn")
	if c := ResolveConfig("", "", "", dir); c.ASNPath != "/env/asn" {
		t.Fatalf("env: %+v", c)
	}
	if c := ResolveConfig("/flag/asn", "/flag/c", "/flag/city", dir); c.ASNPath != "/flag/asn" || c.CountryPath != "/flag/c" || c.CityPath != "/flag/city" {
		t.Fatalf("flag: %+v", c)
	}
}

// recordOf is the Record the collector gives a Refresher: the store's reading of a reachability row's raw_json.
func recordOf(st *store.Store) func(ctx context.Context, raw []byte) ([]byte, error) {
	return func(ctx context.Context, raw []byte) ([]byte, error) { return st.ReachRecord(ctx, st.DB(), raw) }
}

// A collector that has just started has not loaded the slim tables yet. The store has one connection, and reading a
// slim heartbeat loads them with a query of its own: the refresher must not hold that connection with its rows while
// it reads them, or it waits on itself for good (mocha, 2026-10-07: the collector stopped ingesting after its first
// pass, from a restart until it was restarted again).
func TestRefresherOnAStoreJustOpenedDoesNotWaitOnItself(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "observer.db")
	st, err := store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := EnsureSchema(st.DB()); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	now := time.Now().UTC()
	b1, h1 := consBech(t, 0x11)
	if _, _, err := st.ObserveEndpoints(ctx, []scan.FibreProvider{{ConsAddressBech32: b1, Host: "fibre.one.example:7980"}}, 100, now.Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}
	beat(t, st, h1, "fibre.one.example:7980", now.Add(-10*time.Minute), "5.9.1.1", "-> 5.9.1.1:7980")
	st.Close()

	// the store as a restarted collector opens it: no slim table loaded yet
	st, err = store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	r := &Refresher{DB: st.DB(), Logf: t.Logf, Record: recordOf(st), Cfg: Config{ASNPath: writeFile(t, dir, DefaultASNFile, asnTSV, true)}}
	tctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	res, err := r.Run(tctx, now)
	if err != nil {
		t.Fatalf("run on a store just opened: %v", err)
	}
	if res.Resolved != 1 {
		t.Fatalf("run: %+v", res)
	}
}

// A heartbeat whose record does not decode is that heartbeat's alone: the other endpoints are resolved and the table
// is written, its endpoint goes without addresses until a newer heartbeat, and the log names it once, not every pass.
func TestRefresherSkipsAHeartbeatThatDoesNotDecode(t *testing.T) {
	dir := t.TempDir()
	st, err := store.Open(filepath.Join(dir, "observer.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if err := EnsureSchema(st.DB()); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	now := time.Now().UTC()
	b1, h1 := consBech(t, 0x11)
	b2, h2 := consBech(t, 0x22)
	provs := []scan.FibreProvider{{ConsAddressBech32: b1, Host: "fibre.one.example:7980"}, {ConsAddressBech32: b2, Host: "fibre.two.example:7980"}}
	if _, _, err := st.ObserveEndpoints(ctx, provs, 100, now.Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}
	beat(t, st, h1, "fibre.one.example:7980", now.Add(-10*time.Minute), "5.9.1.1", "-> 5.9.1.1:7980")
	beat(t, st, h2, "fibre.two.example:7980", now.Add(-10*time.Minute), "51.68.1.1", "-> 51.68.1.1:7980")
	// the second heartbeat's slim record, cut short
	var raw []byte
	if err := st.DB().QueryRow(`SELECT raw_json FROM reachability WHERE validator_address = ?`, h2).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	if len(raw) < 2 || raw[0] == '{' {
		t.Fatalf("the heartbeat is kept as %.40q, not in its slim form", raw)
	}
	if _, err := st.DB().Exec(`UPDATE reachability SET raw_json = ? WHERE validator_address = ?`, raw[:len(raw)-1], h2); err != nil {
		t.Fatal(err)
	}

	var logs []string
	r := &Refresher{DB: st.DB(), Logf: func(f string, a ...any) { logs = append(logs, fmt.Sprintf(f, a...)) }, Record: recordOf(st),
		Cfg: Config{ASNPath: writeFile(t, dir, DefaultASNFile, asnTSV, true)}}
	res, err := r.Run(ctx, now)
	if err != nil {
		t.Fatalf("one heartbeat that does not decode failed the run: %v", err)
	}
	if res.Hosts != 2 || res.Resolved != 1 {
		t.Fatalf("run: %+v", res)
	}
	cur, err := Current(ctx, st.DB())
	if err != nil {
		t.Fatal(err)
	}
	if in := cur[h1]; in.Status != "ok" || in.IP != "5.9.1.1" {
		t.Fatalf("the endpoint whose heartbeat decodes: %+v", in)
	}
	if in := cur[h2]; in.Status != "unresolved" || in.IP != "" {
		t.Fatalf("the endpoint whose heartbeat does not decode: %+v", in)
	}
	// the next pass reads the heartbeats again (nothing changed, so it writes nothing) and says nothing more
	if res, err := r.Run(ctx, now.Add(time.Minute)); err != nil || !res.Skipped {
		t.Fatalf("second pass: %+v %v", res, err)
	}
	said := 0
	for _, l := range logs {
		if strings.Contains(l, h2) {
			said++
		}
	}
	if said != 1 {
		t.Fatalf("the heartbeat was named %d times: %q", said, logs)
	}
}
