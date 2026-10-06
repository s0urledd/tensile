package hosting

import (
	"context"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/scan"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
)

// A slice of DB-IP's city format: no header, ZZ for unknown, 0,0 for "no
// coordinates", names quoted when they hold a space, a comma or a quote
// (escaped by doubling), adjacent ranges to test the boundaries, IPv4 and
// IPv6.
const cityCSV = "0.0.0.0,0.255.255.255,ZZ,ZZ,,,0,0\n" +
	"5.9.0.0,5.9.1.255,EU,DE,Bavaria,Nuremberg,49.4478,11.0683\n" +
	"5.9.2.0,5.9.2.255,EU,DE,Saxony,Falkenstein,50.4779,12.3713\n" +
	"5.9.3.0,5.9.3.255,EU,GR,\"Central Macedonia\",\"Ampelokipoi, Thessaloniki\",40.653,22.9262\n" +
	"5.9.4.0,5.9.4.255,EU,DE,,,0,0\n" +
	"51.68.0.0,51.68.255.255,EU,FR,Hauts-de-France,Roubaix,50.6942,3.17456\n" +
	"203.0.113.0,203.0.113.255,EU,NL,\"North Holland\",\"Amsterdam \"\"Zuid\"\"\",52.37,0\n" +
	"198.51.100.0,198.51.100.255,EU,NL,\"North Holland\",Haarlem,0,0\n" +
	"192.0.2.0,192.0.2.255,EU,GB,England,\"London (Paddington)\",51.5176,-0.180429\n" +
	"2a01:4f8::,2a01:4f8:0:ffff:ffff:ffff:ffff:ffff,EU,DE,Saxony,Falkenstein,50.4779,12.3713\n" +
	"2a01:4f8:1::,2a01:4f8:ffff:ffff:ffff:ffff:ffff:ffff,EU,FI,Uusimaa,Helsinki,60.1699,24.9384\n"

func TestLookupCity(t *testing.T) {
	dir := t.TempDir()
	for _, gz := range []bool{false, true} {
		p := writeFile(t, dir, "city.csv", cityCSV, gz)
		got, err := LookupCity(p, addrs(
			"5.9.0.0", "5.9.1.255", "5.9.2.0", "5.9.2.255", "5.9.3.0", "5.9.3.255", // range boundaries
			"5.9.4.1",                    // a range with no city
			"0.1.2.3",                    // ZZ
			"51.67.255.255", "51.69.0.0", // just outside a range, both sides
			"::ffff:51.68.0.0", // IPv4-mapped input
			"203.0.113.9", "198.51.100.1", "192.0.2.7",
			"2a01:4f7:ffff:ffff:ffff:ffff:ffff:ffff", "2a01:4f8::", "2a01:4f8:0:ffff:ffff:ffff:ffff:ffff", "2a01:4f8:1::",
		))
		if err != nil {
			t.Fatal(err)
		}
		type w struct {
			cc, region, city string
			lat, lon         float64
			coords           bool
		}
		want := map[string]w{
			"5.9.0.0":                             {"DE", "Bavaria", "Nuremberg", 49.4478, 11.0683, true},
			"5.9.1.255":                           {"DE", "Bavaria", "Nuremberg", 49.4478, 11.0683, true},
			"5.9.2.0":                             {"DE", "Saxony", "Falkenstein", 50.4779, 12.3713, true},
			"5.9.2.255":                           {"DE", "Saxony", "Falkenstein", 50.4779, 12.3713, true},
			"5.9.3.0":                             {"GR", "Central Macedonia", "Ampelokipoi, Thessaloniki", 40.653, 22.9262, true},
			"5.9.3.255":                           {"GR", "Central Macedonia", "Ampelokipoi, Thessaloniki", 40.653, 22.9262, true},
			"51.68.0.0":                           {"FR", "Hauts-de-France", "Roubaix", 50.6942, 3.17456, true},
			"203.0.113.9":                         {"NL", "North Holland", `Amsterdam "Zuid"`, 52.37, 0, true}, // a zero longitude alone is a real point
			"198.51.100.1":                        {"NL", "North Holland", "Haarlem", 0, 0, false},             // 0,0 is "no point"
			"192.0.2.7":                           {"GB", "England", "London", 51.5176, -0.180429, true},       // district dropped
			"2a01:4f8::":                          {"DE", "Saxony", "Falkenstein", 50.4779, 12.3713, true},
			"2a01:4f8:0:ffff:ffff:ffff:ffff:ffff": {"DE", "Saxony", "Falkenstein", 50.4779, 12.3713, true},
			"2a01:4f8:1::":                        {"FI", "Uusimaa", "Helsinki", 60.1699, 24.9384, true},
		}
		if len(got) != len(want) {
			t.Fatalf("gz=%v: got %d records, want %d: %+v", gz, len(got), len(want), got)
		}
		for a, e := range want {
			r, ok := got[netip.MustParseAddr(a)]
			if !ok || r.Country != e.cc || r.Region != e.region || r.City != e.city || r.HasCoords != e.coords || r.Lat != e.lat || r.Lon != e.lon {
				t.Fatalf("gz=%v %s: got %+v (found %v), want %+v", gz, a, r, ok, e)
			}
		}
	}
}

func TestLookupCityAbsentOrWrongFile(t *testing.T) {
	if _, err := LookupCity(filepath.Join(t.TempDir(), "missing.csv.gz"), addrs("5.9.1.1")); err == nil {
		t.Fatal("a missing file with targets must be an error")
	}
	// the country file is not a city file: its lines have no city field
	p := writeFile(t, t.TempDir(), "c.csv", countryCSV, false)
	if got, err := LookupCity(p, addrs("5.9.1.1")); err != nil || len(got) != 0 {
		t.Fatalf("country file read as city: %v %v", got, err)
	}
	p = writeFile(t, t.TempDir(), "x.csv", "<html>nope</html>\n", false)
	if _, err := LookupCity(p, addrs("5.9.1.1")); err == nil {
		t.Fatal("a file with no readable range must be an error")
	}
}

// The pass with a city file: city fields on the rows, the country file's
// country kept, a city whose country disagrees dropped, and the city file
// standing in for the country file when that one is absent.
func TestRefresherCity(t *testing.T) {
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
	provs := []scan.FibreProvider{
		{ConsAddressBech32: b1, Host: "one.example:7980"},
		{ConsAddressBech32: b2, Host: "51.68.3.3:7980"},
		{ConsAddressBech32: b3, Host: "v6.example:7980"},
	}
	if _, _, err := st.ObserveEndpoints(ctx, provs, 100, now.Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}
	beat(t, st, h1, "one.example:7980", now.Add(-5*time.Minute), "5.9.1.1", "-> 5.9.1.1:7980")
	beat(t, st, h3, "v6.example:7980", now.Add(-5*time.Minute), "2a01:4f8:1::9", "-> [2a01:4f8:1::9]:7980")

	asn := writeFile(t, dir, DefaultASNFile, asnTSV, true)
	missing := filepath.Join(dir, "no-such-city.csv.gz")
	r := &Refresher{DB: st.DB(), Cfg: Config{ASNPath: asn, CityPath: missing}, Record: recordOf(st)}

	// a configured but absent city file: country only, no error, no city
	if _, err := r.Run(ctx, now); err != nil {
		t.Fatal(err)
	}
	cur, _ := Current(ctx, st.DB())
	for k, in := range cur {
		if in.City != "" || in.Lat != nil || in.Lon != nil || in.Region != "" {
			t.Fatalf("%s: city fields without a city file: %+v", k, in)
		}
	}
	if src, _ := ReadSources(ctx, st.DB()); src.City != nil {
		t.Fatalf("city source without a city file: %+v", src.City)
	}
	if s := Concentrate(members(cur, h1, h2, h3)); s.ByCity != nil {
		t.Fatalf("by_city without a city file: %+v", s.ByCity)
	}

	// the city file appears (no country file): the next pass places them,
	// and the city file's country is the geolocated country
	r.Cfg.CityPath = writeFile(t, dir, DefaultCityFile, cityCSV, true)
	res, err := r.Run(ctx, now.Add(time.Minute))
	if err != nil || res.Skipped || res.WithCity != 3 {
		t.Fatalf("city run: %+v %v", res, err)
	}
	cur, _ = Current(ctx, st.DB())
	if in := cur[h1]; in.City != "Nuremberg" || in.Region != "Bavaria" || in.Country != "DE" || in.CountryBasis != "geolocation" ||
		in.Lat == nil || *in.Lat != 49.4478 || *in.Lon != 11.0683 {
		t.Fatalf("h1: %+v", in)
	}
	if in := cur[h3]; in.City != "Helsinki" || in.Country != "FI" {
		t.Fatalf("h3 (IPv6): %+v", in)
	}
	if src, _ := ReadSources(ctx, st.DB()); src.City == nil || src.City.File != DefaultCityFile || src.City.Attribution != "IP Geolocation by DB-IP" {
		t.Fatalf("city source: %+v", src.City)
	}

	// with the country file too: it wins, and where it disagrees with the
	// city file (51.68.x: PL vs FR; 2a01:4f8:1::: DE vs FI) the city is dropped, not
	// mislabelled
	r.Cfg.CountryPath = writeFile(t, dir, DefaultCountryFile, countryCSV, true)
	if res, err := r.Run(ctx, now.Add(2*time.Minute)); err != nil || res.WithCity != 1 {
		t.Fatalf("country+city run: %+v %v", res, err)
	}
	cur, _ = Current(ctx, st.DB())
	if in := cur[h2]; in.Country != "PL" || in.City != "" || in.Lat != nil {
		t.Fatalf("h2 disagreement: %+v", in)
	}
	if in := cur[h1]; in.City != "Nuremberg" {
		t.Fatalf("h1 with both: %+v", in)
	}
	if in := cur[h3]; in.Country != "DE" || in.City != "" {
		t.Fatalf("h3 disagreement: %+v", in)
	}

	s := Concentrate(members(cur, h1, h2, h3))
	if len(s.ByCity) != 2 {
		t.Fatalf("by_city: %+v", s.ByCity)
	}
	last := s.ByCity[len(s.ByCity)-1]
	if last.Key != "" || last.Hosts != 2 || last.Lat != nil || last.City != "" {
		t.Fatalf("the unplaced bucket must come last and carry no place: %+v", last)
	}
	for _, b := range s.ByCity[:1] {
		if b.Key == "" || b.Lat == nil || b.Lon == nil || b.Country == "" || !strings.HasPrefix(b.Key, b.Country+"/") {
			t.Fatalf("city bucket: %+v", b)
		}
	}

	// the city file goes away: the next pass drops the city fields
	os.Remove(r.Cfg.CityPath)
	if _, err := r.Run(ctx, now.Add(3*time.Minute)); err != nil {
		t.Fatal(err)
	}
	cur, _ = Current(ctx, st.DB())
	if in := cur[h1]; in.City != "" || in.Lat != nil {
		t.Fatalf("city kept after its file went away: %+v", in)
	}
}

func members(cur map[string]Info, keys ...string) []Member {
	var out []Member
	for _, k := range keys {
		m := Member{Validator: k, Stake: 10}
		if in, ok := cur[k]; ok {
			m.Info = &in
		}
		out = append(out, m)
	}
	return out
}

func TestConcentrateByCity(t *testing.T) {
	f := func(v float64) *float64 { return &v }
	city := func(cc, region, c string, lat, lon float64) *Info {
		return &Info{Status: "ok", IP: "192.0.2.1", Provider: ProviderOther, ASN: 64500, Country: cc, Region: region, City: c, Lat: f(lat), Lon: f(lon)}
	}
	s := Concentrate([]Member{
		{"b", 30, city("DE", "Saxony", "Falkenstein", 50.48, 12.37)},
		{"a", 30, city("DE", "Saxony", "Falkenstein", 50.47, 12.36)}, // same city, another point: the lower validator's wins
		{"c", 25, city("FI", "Uusimaa", "Helsinki", 60.17, 24.94)},
		{"d", 5, &Info{Status: "ok", IP: "192.0.2.2", ASN: 64500, Country: "US"}}, // no city
		{"e", 10, nil}, // unresolved
	})
	if len(s.ByCity) != 3 {
		t.Fatalf("by_city: %+v", s.ByCity)
	}
	b := s.ByCity[0]
	if b.Key != "DE/Saxony/Falkenstein" || b.Hosts != 2 || b.Stake != 60 || b.StakeShare != 0.6 || b.HostShare != 0.4 ||
		*b.Lat != 50.47 || *b.Lon != 12.36 || b.Country != "DE" || b.City != "Falkenstein" || b.Region != "Saxony" {
		t.Fatalf("Falkenstein: %+v", b)
	}
	if b := s.ByCity[1]; b.Key != "FI/Uusimaa/Helsinki" || b.StakeShare != 0.25 {
		t.Fatalf("Helsinki: %+v", b)
	}
	// the remainder: no city and unresolved, over every registered host
	if b := s.ByCity[2]; b.Key != "" || b.Hosts != 2 || b.Stake != 15 || b.Lat != nil {
		t.Fatalf("remainder: %+v", b)
	}
	if s := Concentrate([]Member{{"a", 1, &Info{Status: "ok", IP: "192.0.2.1", Country: "DE"}}}); s.ByCity != nil {
		t.Fatalf("no city anywhere must leave by_city out: %+v", s.ByCity)
	}
}

// A table created by the collector before the city columns: Current reads
// it (no cities), EnsureSchema adds the columns once, and a second call is
// a no-op.
func TestEnsureSchemaEvolvesOldTable(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "observer.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	db := st.DB()
	old := `CREATE TABLE endpoint_hosting (
		validator_address TEXT NOT NULL, host TEXT NOT NULL, status TEXT NOT NULL,
		ip TEXT NOT NULL DEFAULT '', asn INTEGER NOT NULL DEFAULT 0, as_org TEXT NOT NULL DEFAULT '',
		as_country TEXT NOT NULL DEFAULT '', country TEXT NOT NULL DEFAULT '', country_basis TEXT NOT NULL DEFAULT '',
		provider TEXT NOT NULL DEFAULT '', addresses_json TEXT NOT NULL DEFAULT '[]', resolved_at TEXT NOT NULL DEFAULT '',
		resolved_by TEXT NOT NULL DEFAULT '', looked_up_at TEXT NOT NULL, PRIMARY KEY (validator_address, host))`
	if _, err := db.Exec(old); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO endpoint_hosting (validator_address, host, status, ip, asn, country, looked_up_at)
		VALUES ('aa', 'h:1', 'ok', '5.9.1.1', 24940, 'DE', 'x')`); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	cur, err := Current(ctx, db)
	if err != nil || cur["aa"].ASN != 24940 || cur["aa"].City != "" {
		t.Fatalf("old table: %+v %v", cur, err)
	}
	for range 2 {
		if err := EnsureSchema(db); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.Exec(`UPDATE endpoint_hosting SET city = 'Nuremberg', region = 'Bavaria', lat = 49.4, lon = 11.1`); err != nil {
		t.Fatal(err)
	}
	cur, err = Current(ctx, db)
	if in := cur["aa"]; err != nil || in.City != "Nuremberg" || in.Lat == nil || *in.Lat != 49.4 || in.ASN != 24940 {
		t.Fatalf("evolved table: %+v %v", in, err)
	}
}
