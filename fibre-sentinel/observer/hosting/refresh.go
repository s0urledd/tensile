package hosting

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// Config is where the collector finds the database files. An empty or
// missing ASNPath turns the feature off; CountryPath and CityPath are
// optional.
type Config struct {
	ASNPath     string
	CountryPath string
	// CityPath is DB-IP's IP to City Lite file. Without it hosts are placed
	// by country only and the API carries no city fields.
	CityPath string
	// Lookback is how far back a heartbeat's resolution still counts as
	// the host's current address. The heartbeat runs every five minutes;
	// the default six hours covers a heartbeat outage of that length
	// without dropping every host to "unresolved".
	Lookback time.Duration
	// MaxAge re-runs the lookup at least this often even when nothing
	// changed, so looked_up_at never goes stale on a quiet network.
	MaxAge time.Duration
}

// DefaultPaths are the file names deploy/hosting-db.sh writes under
// <data-dir>/hosting, which the collector uses when no path is configured.
const (
	DefaultASNFile     = "ip2asn-combined.tsv.gz"
	DefaultCountryFile = "dbip-country-lite.csv.gz"
	DefaultCityFile    = "dbip-city-lite.csv.gz"
)

// ResolveConfig fills a Config from a flag value, then an environment
// variable (the systemd units load /etc/fibre-observer/<network>.env into
// the environment, so setting HOSTING_ASN_DB there needs no unit change),
// then the default file under <dataDir>/hosting. The default is only taken
// when the file exists, so a host that never ran the download script keeps
// the feature off without a warning.
func ResolveConfig(flagASN, flagCountry, flagCity, dataDir string) Config {
	pick := func(flagV, env, file string) string {
		if flagV != "" {
			return flagV
		}
		if v := strings.TrimSpace(os.Getenv(env)); v != "" {
			return v
		}
		p := filepath.Join(dataDir, "hosting", file)
		if _, err := os.Stat(p); err == nil {
			return p
		}
		return ""
	}
	return Config{
		ASNPath:     pick(flagASN, "HOSTING_ASN_DB", DefaultASNFile),
		CountryPath: pick(flagCountry, "HOSTING_COUNTRY_DB", DefaultCountryFile),
		CityPath:    pick(flagCity, "HOSTING_CITY_DB", DefaultCityFile),
	}
}

// Refresher is the collector's lookup pass. It keeps only a fingerprint of
// its last run between calls: the targets and the files' size and mtime.
type Refresher struct {
	DB  *sql.DB
	Cfg Config
	// Vantage, when set, is the observer's own: only its heartbeats are
	// read for addresses. Another vantage's copied heartbeats resolve the
	// same host from elsewhere, and a geo-steered name can answer there with
	// addresses this observer never reached. Empty reads every row.
	Vantage string
	Logf    func(string, ...any)
	// Record gives a reachability row's raw_json back as its record line
	// (store.ReachRecord): the column keeps the slim form. Nil reads it as
	// it is, and a slim row is then an error rather than an address read
	// wrong.
	Record func(ctx context.Context, raw []byte) ([]byte, error)

	lastKey string
	lastRun time.Time
}

// Result summarises one Run.
type Result struct {
	Enabled  bool
	Skipped  bool // nothing changed since the last run and it is younger than MaxAge
	Hosts    int
	Resolved int
	WithASN  int
	WithCity int // hosts placed in a city (only with a city file)
	Cleared  int // rows removed because the feature was turned off
}

// target is one open endpoint and the addresses a recent heartbeat saw for it.
type target struct {
	addrHex    string
	host       string
	addrs      []netip.Addr // in the heartbeat's own order (IPv4 first)
	connected  netip.Addr   // the address the TCP connect reached, if any
	resolvedAt string
	resolvedBy string
}

// Run does one pass: work out every open endpoint's addresses, look them up
// in the files, and replace the table's contents. It never touches the
// network. Safe to call every collector pass: when neither the endpoints,
// their addresses nor the files changed, it returns at once.
func (r *Refresher) Run(ctx context.Context, now time.Time) (Result, error) {
	cfg := r.Cfg
	if cfg.Lookback <= 0 {
		cfg.Lookback = 6 * time.Hour
	}
	if cfg.MaxAge <= 0 {
		cfg.MaxAge = 24 * time.Hour
	}
	asnStat, asnErr := statFile(cfg.ASNPath)
	if asnErr != nil {
		// Off. Anything an earlier configuration stored goes: a lookup
		// nobody can reproduce from a file that is no longer there should
		// not keep being served as current.
		n, err := r.clear(ctx, now)
		if err == nil && n > 0 && r.Logf != nil {
			r.Logf("hosting: %v; feature off, %d stored lookup(s) cleared", asnErr, n)
		}
		r.lastKey = ""
		return Result{Cleared: n}, err
	}
	countryStat, countryErr := statFile(cfg.CountryPath)
	if countryErr != nil && cfg.CountryPath != "" && r.Logf != nil && r.lastKey == "" {
		r.Logf("hosting: country file: %v; countries fall back to the AS registry's", countryErr)
	}
	cityStat, cityErr := statFile(cfg.CityPath)
	if cityErr != nil && cfg.CityPath != "" && r.Logf != nil && r.lastKey == "" {
		r.Logf("hosting: city file: %v; hosts are placed by country only", cityErr)
	}

	targets, err := r.targets(ctx, now.Add(-cfg.Lookback))
	if err != nil {
		return Result{Enabled: true}, err
	}
	key := fingerprint(targets, asnStat, countryStat+"\n"+cityStat)
	if key == r.lastKey && now.Sub(r.lastRun) < cfg.MaxAge {
		return Result{Enabled: true, Skipped: true, Hosts: len(targets)}, nil
	}

	var all []netip.Addr
	for _, t := range targets {
		all = append(all, t.addrs...)
	}
	asns, err := LookupASN(cfg.ASNPath, all)
	if err != nil {
		return Result{Enabled: true}, fmt.Errorf("asn db: %w", err)
	}
	var countries map[netip.Addr]string
	if countryStat != "" {
		if countries, err = LookupCountry(cfg.CountryPath, all); err != nil {
			// A broken optional file must not take the ASN half down
			// with it; the rows say which basis each country has.
			if r.Logf != nil {
				r.Logf("hosting: country db: %v; countries fall back to the AS registry's", err)
			}
			countries, countryStat = nil, ""
		}
	}
	var cities map[netip.Addr]CityRecord
	if cityStat != "" {
		if cities, err = LookupCity(cfg.CityPath, all); err != nil {
			// Optional too: a bad city file costs the city fields only.
			if r.Logf != nil {
				r.Logf("hosting: city db: %v; hosts are placed by country only", err)
			}
			cities, cityStat = nil, ""
		}
	}

	res := Result{Enabled: true, Hosts: len(targets)}
	tx, err := r.DB.BeginTx(ctx, nil)
	if err != nil {
		return res, err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `DELETE FROM endpoint_hosting`); err != nil {
		return res, err
	}
	for _, t := range targets {
		row := buildInfo(t, asns, countries, cities)
		if row.Status != "unresolved" {
			res.Resolved++
		}
		if row.Status == "ok" {
			res.WithASN++
		}
		if row.City != "" {
			res.WithCity++
		}
		addrs, _ := json.Marshal(row.Addresses)
		if _, err := tx.ExecContext(ctx, `INSERT INTO endpoint_hosting
			(validator_address, host, status, ip, asn, as_org, as_country, country, country_basis, provider,
			 addresses_json, resolved_at, resolved_by, looked_up_at, city, region, lat, lon)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
			ON CONFLICT(validator_address, host) DO NOTHING`,
			t.addrHex, t.host, row.Status, row.IP, row.ASN, row.ASOrg, asCountryOf(row, asns), row.Country, row.CountryBasis,
			row.Provider, string(addrs), row.ResolvedAt, row.ResolvedBy, ts(now), row.City, row.Region, row.Lat, row.Lon); err != nil {
			return res, err
		}
	}
	meta := map[string]string{
		MetaEnabled:         "yes",
		MetaASNDB:           filepath.Base(cfg.ASNPath),
		MetaASNDBModified:   asnStat[strings.LastIndexByte(asnStat, '|')+1:],
		MetaCountryDB:       "",
		MetaCountryModified: "",
		MetaCityDB:          "",
		MetaCityModified:    "",
		MetaLookedUpAt:      ts(now),
	}
	if countryStat != "" {
		meta[MetaCountryDB] = filepath.Base(cfg.CountryPath)
		meta[MetaCountryModified] = countryStat[strings.LastIndexByte(countryStat, '|')+1:]
	}
	if cityStat != "" {
		meta[MetaCityDB] = filepath.Base(cfg.CityPath)
		meta[MetaCityModified] = cityStat[strings.LastIndexByte(cityStat, '|')+1:]
	}
	for k, v := range meta {
		if err := setMeta(ctx, tx, k, v, now); err != nil {
			return res, err
		}
	}
	if err := tx.Commit(); err != nil {
		return res, err
	}
	r.lastKey, r.lastRun = key, now
	return res, nil
}

// asCountryOf is the AS registry's country for the primary address, kept in
// its own column even when the published country is DB-IP's, so the two can
// be compared later without a second lookup.
func asCountryOf(in Info, asns map[netip.Addr]ASNRecord) string {
	if a, err := netip.ParseAddr(in.IP); err == nil {
		return asns[a.Unmap()].ASCountry
	}
	return ""
}

// buildInfo turns one target and the lookups into the published row.
//
// Country, first match wins: the country file, then the city file's country
// (both DB-IP geolocation estimates), then the AS registry's. The city
// fields are set only when the city file's country is the published
// country, so a row can never read "Frankfurt, FI" when the two monthly
// files disagree about a range.
func buildInfo(t target, asns map[netip.Addr]ASNRecord, countries map[netip.Addr]string, cities map[netip.Addr]CityRecord) Info {
	in := Info{Host: t.host, ResolvedAt: t.resolvedAt, ResolvedBy: t.resolvedBy, Status: "unresolved", Provider: ProviderUnknown}
	if len(t.addrs) == 0 {
		return in
	}
	primary := t.addrs[0]
	if t.connected.IsValid() {
		primary = t.connected
	}
	country := func(a netip.Addr) (string, string) {
		if cc, ok := countries[a]; ok {
			return cc, "geolocation"
		}
		if c, ok := cities[a]; ok {
			return c.Country, "geolocation"
		}
		if rec, ok := asns[a]; ok && rec.ASCountry != "" {
			return rec.ASCountry, "as_registry"
		}
		return "", ""
	}
	for _, a := range t.addrs {
		rec := asns[a]
		cc, _ := country(a)
		in.Addresses = append(in.Addresses, Address{IP: a.String(), ASN: rec.ASN, ASOrg: rec.Org, Country: cc,
			Provider: ProviderFor(rec.ASN), Connected: t.connected.IsValid() && a == t.connected})
	}
	in.IP = primary.String()
	rec, ok := asns[primary]
	in.Country, in.CountryBasis = country(primary)
	if c, found := cities[primary]; found && in.Country == c.Country {
		in.City, in.Region = c.City, c.Region
		if c.HasCoords {
			lat, lon := c.Lat, c.Lon
			in.Lat, in.Lon = &lat, &lon
		}
	}
	if !ok {
		in.Status = "no_asn"
		return in
	}
	in.Status, in.ASN, in.ASOrg, in.Provider = "ok", rec.ASN, rec.Org, ProviderFor(rec.ASN)
	return in
}

// targets lists every open endpoint with the addresses the newest heartbeat
// within the lookback recorded for it. An endpoint the heartbeat has not
// resolved is still listed, with no address, so the API can say
// "unresolved" rather than leave it out of the denominator.
func (r *Refresher) targets(ctx context.Context, since time.Time) ([]target, error) {
	eps, err := r.DB.QueryContext(ctx, `SELECT DISTINCT validator_cons_address, host FROM endpoints WHERE closed_at IS NULL`)
	if err != nil {
		return nil, err
	}
	type key struct{ addr, host string }
	var order []key
	for eps.Next() {
		var bech, host string
		if err := eps.Scan(&bech, &host); err != nil {
			eps.Close()
			return nil, err
		}
		addr, err := consHex(bech)
		if err != nil {
			addr = strings.ToLower(bech) // stored in another form; keep it as is
		}
		order = append(order, key{addr, host})
	}
	eps.Close()
	if err := eps.Err(); err != nil {
		return nil, err
	}

	// The newest heartbeat per (validator, host) that got past DNS and still
	// carries its raw record. Rows are ingested in write order, so the
	// highest rowid is the newest, the same rule the API's reachability
	// queries use.
	latest := map[key]target{}
	vq, vargs := "", []any{ts(since)}
	if r.Vantage != "" {
		vq, vargs = " AND +vantage = ?", append(vargs, r.Vantage)
	}
	rows, err := r.DB.QueryContext(ctx, `SELECT validator_address, validator_host, started_at, raw_json FROM reachability
		WHERE rowid IN (SELECT MAX(rowid) FROM reachability
			WHERE dns_ok = 1 AND raw_json <> '' AND started_at >= ?`+vq+`
			GROUP BY validator_address, validator_host)`, vargs...)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var addr, host, at string
		var raw []byte
		if err := rows.Scan(&addr, &host, &at, &raw); err != nil {
			rows.Close()
			return nil, err
		}
		switch {
		case r.Record != nil:
			line, err := r.Record(ctx, raw)
			if err != nil {
				rows.Close()
				return nil, fmt.Errorf("heartbeat of %s: %w", addr, err)
			}
			raw = line
		case len(raw) > 0 && raw[0] != '{':
			rows.Close()
			return nil, fmt.Errorf("heartbeat of %s: a slim record and no Record to read it", addr)
		}
		addrs, connected := AddressesFromMeasurement(raw)
		if len(addrs) == 0 {
			continue
		}
		latest[key{strings.ToLower(addr), host}] = target{addrs: addrs, connected: connected, resolvedAt: at, resolvedBy: "heartbeat"}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	out := make([]target, 0, len(order))
	for _, k := range order {
		t := latest[k]
		t.addrHex, t.host = k.addr, k.host
		if len(t.addrs) == 0 {
			// A host registered as a literal address needs no resolution:
			// the chain record is the address.
			if a, ok := literalHost(k.host); ok {
				t.addrs, t.resolvedBy, t.resolvedAt = []netip.Addr{a}, "literal", ""
			}
		}
		out = append(out, t)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].addrHex != out[j].addrHex {
			return out[i].addrHex < out[j].addrHex
		}
		return out[i].host < out[j].host
	})
	return out, nil
}

// AddressesFromMeasurement reads the addresses a probe.Measurement's DNS
// step recorded, and the one its TCP step connected to, from the raw JSON
// the reachability row keeps. The formats are the ones probe.Run writes:
//
//	dns.detail  "203.0.113.5,2001:db8::5"                   resolved, in dial order
//	dns.detail  "203.0.113.5 (not dialled: 10.0.0.1 (private))"
//	dns.detail  "literal IP 203.0.113.5"
//	tcp.detail  "failed 2001:db8::5: …; -> 203.0.113.5:7980"
//
// Addresses the observer refused to dial (private, loopback) are not
// returned: they say nothing about hosting and publishing them is exactly
// what the prober avoids.
func AddressesFromMeasurement(raw []byte) (addrs []netip.Addr, connected netip.Addr) {
	var m struct {
		DNS struct {
			Detail string `json:"detail"`
		} `json:"dns"`
		TCP struct {
			Detail string `json:"detail"`
		} `json:"tcp"`
	}
	if json.Unmarshal(raw, &m) != nil {
		return nil, netip.Addr{}
	}
	d := strings.TrimSpace(m.DNS.Detail)
	d = strings.TrimPrefix(d, "literal IP ")
	if i := strings.Index(d, " ("); i >= 0 {
		d = d[:i]
	}
	seen := map[netip.Addr]bool{}
	for _, f := range strings.Split(d, ",") {
		a, err := netip.ParseAddr(strings.TrimSpace(f))
		if err != nil {
			continue
		}
		a = a.Unmap()
		if seen[a] || !publicAddr(a) {
			continue
		}
		seen[a] = true
		addrs = append(addrs, a)
	}
	if i := strings.LastIndex(m.TCP.Detail, "-> "); i >= 0 {
		if ap, err := netip.ParseAddrPort(strings.TrimSpace(m.TCP.Detail[i+3:])); err == nil {
			c := ap.Addr().Unmap()
			if publicAddr(c) {
				connected = c
				if !seen[c] {
					addrs = append(addrs, c)
				}
			}
		}
	}
	return addrs, connected
}

// literalHost reports whether a registered host:port names an address
// directly, and that address when it is a public one.
func literalHost(hostport string) (netip.Addr, bool) {
	h, _, err := net.SplitHostPort(hostport)
	if err != nil {
		h = hostport
	}
	a, err := netip.ParseAddr(strings.Trim(h, "[]"))
	if err != nil || !publicAddr(a.Unmap()) {
		return netip.Addr{}, false
	}
	return a.Unmap(), true
}

// publicAddr is the same line the prober draws (routableIP): nothing
// private, loopback, link-local, multicast or unspecified.
func publicAddr(a netip.Addr) bool {
	return a.IsValid() && a.IsGlobalUnicast() && !a.IsPrivate() && !a.IsLoopback() && !a.IsLinkLocalUnicast()
}

// statFile returns "size|modified" for a readable regular file, which is
// what the fingerprint and the meta record use; an error when the path is
// empty or unusable.
func statFile(path string) (string, error) {
	if path == "" {
		return "", fmt.Errorf("no database file configured")
	}
	fi, err := os.Stat(path)
	if err != nil {
		return "", err
	}
	if !fi.Mode().IsRegular() || fi.Size() == 0 {
		return "", fmt.Errorf("%s: not a non-empty regular file", path)
	}
	return fmt.Sprintf("%d|%s", fi.Size(), ts(fi.ModTime())), nil
}

func fingerprint(tg []target, asnStat, countryStat string) string {
	h := sha256.New()
	fmt.Fprintf(h, "%s\n%s\n", asnStat, countryStat)
	for _, t := range tg {
		fmt.Fprintf(h, "%s|%s|%s|", t.addrHex, t.host, t.connected)
		for _, a := range t.addrs {
			fmt.Fprintf(h, "%s,", a)
		}
		h.Write([]byte{'\n'})
	}
	return hex.EncodeToString(h.Sum(nil))
}

// clear empties the table and marks the feature off. Returns rows removed.
func (r *Refresher) clear(ctx context.Context, now time.Time) (int, error) {
	res, err := r.DB.ExecContext(ctx, `DELETE FROM endpoint_hosting`)
	if err != nil {
		if isNoTable(err) {
			return 0, nil
		}
		return 0, err
	}
	n, _ := res.RowsAffected()
	var cur string
	_ = r.DB.QueryRowContext(ctx, `SELECT value FROM meta WHERE key = ?`, MetaEnabled).Scan(&cur)
	if cur != "no" {
		if err := setMeta(ctx, r.DB, MetaEnabled, "no", now); err != nil {
			return int(n), err
		}
	}
	return int(n), nil
}

type execer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

func setMeta(ctx context.Context, db execer, k, v string, now time.Time) error {
	_, err := db.ExecContext(ctx, `INSERT INTO meta (key, value, updated_at) VALUES (?, ?, ?)
		ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at`, k, v, ts(now))
	return err
}
