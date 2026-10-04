package hosting

import (
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/cosmos/cosmos-sdk/types/bech32"

	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
)

// Schema is the one table this package owns.
//
// It is created with CREATE TABLE IF NOT EXISTS by the collector at start
// (EnsureSchema) rather than as a numbered store migration, on purpose: the
// table is derived state, rebuilt in full by every lookup pass from the
// reachability rows and the operator's database files, so it has no history
// to migrate, nothing in the export or the JSONL record depends on it, and a
// database that lacks it (an older collector, or a fresh one before its first
// start) simply has the feature off. The API treats a missing table exactly
// like an empty one. Keeping it out of the migration list also keeps the
// schema version free for changes that do carry history.
//
// One row per open endpoint (validator, registered host). The columns
// describe the primary address, the one the heartbeat actually connected to
// when it connected, else the first the name resolved to; addresses_json
// carries every resolved address with its own lookup, because a name that
// resolves into two networks is exactly the case a single column would hide.
const Schema = `CREATE TABLE IF NOT EXISTS endpoint_hosting (
	validator_address TEXT NOT NULL,             -- 20-byte consensus address, hex
	host              TEXT NOT NULL,             -- host:port as registered
	status            TEXT NOT NULL,             -- ok | no_asn | unresolved
	ip                TEXT NOT NULL DEFAULT '',  -- the primary address
	asn               INTEGER NOT NULL DEFAULT 0,
	as_org            TEXT NOT NULL DEFAULT '',
	as_country        TEXT NOT NULL DEFAULT '',  -- the AS registry's country
	country           TEXT NOT NULL DEFAULT '',
	country_basis     TEXT NOT NULL DEFAULT '',  -- geolocation | as_registry
	provider          TEXT NOT NULL DEFAULT '',
	addresses_json    TEXT NOT NULL DEFAULT '[]',
	resolved_at       TEXT NOT NULL DEFAULT '',  -- when the heartbeat resolved it
	resolved_by       TEXT NOT NULL DEFAULT '',  -- heartbeat | literal
	looked_up_at      TEXT NOT NULL,
	city              TEXT NOT NULL DEFAULT '',  -- DB-IP city estimate, when a city file is configured
	region            TEXT NOT NULL DEFAULT '',  -- its state / province
	lat               REAL,                      -- the city's approximate coordinates; NULL when unknown
	lon               REAL,
	PRIMARY KEY (validator_address, host)
)`

// addedColumns are the columns added to endpoint_hosting after it first
// shipped, with their definitions. A table created by an older collector
// lacks them; EnsureSchema adds them. Append only.
var addedColumns = [][2]string{
	{"city", "TEXT NOT NULL DEFAULT ''"},
	{"region", "TEXT NOT NULL DEFAULT ''"},
	{"lat", "REAL"},
	{"lon", "REAL"},
}

// EnsureSchema creates the table when it is not there, and adds any column
// a table created by an older collector is missing (SQLite has no ADD
// COLUMN IF NOT EXISTS, so it reads the table's columns first). Idempotent;
// the collector calls it once at start whether or not the feature is
// configured, so the table always has the current shape on a database the
// current collector has opened. The added columns start empty and the next
// lookup pass fills them: the table is rebuilt in full by every pass.
func EnsureSchema(db *sql.DB) error {
	if _, err := db.Exec(Schema); err != nil {
		return err
	}
	rows, err := db.Query(`SELECT name FROM pragma_table_info('endpoint_hosting')`)
	if err != nil {
		return err
	}
	have := map[string]bool{}
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			rows.Close()
			return err
		}
		have[n] = true
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	for _, c := range addedColumns {
		if have[c[0]] {
			continue
		}
		if _, err := db.Exec(`ALTER TABLE endpoint_hosting ADD COLUMN ` + c[0] + ` ` + c[1]); err != nil {
			return fmt.Errorf("add column %s: %w", c[0], err)
		}
	}
	return nil
}

// Meta keys the lookup pass writes. The API reads them to say which files
// the figures came from and how old those files are.
const (
	MetaEnabled         = "hosting_enabled" // yes | no
	MetaASNDB           = "hosting_asn_db"  // file name
	MetaASNDBModified   = "hosting_asn_db_modified"
	MetaCountryDB       = "hosting_country_db"
	MetaCountryModified = "hosting_country_db_modified"
	MetaCityDB          = "hosting_city_db"
	MetaCityModified    = "hosting_city_db_modified"
	MetaLookedUpAt      = "hosting_looked_up_at"
)

// Address is one resolved address with its own lookup.
type Address struct {
	IP        string `json:"ip"`
	ASN       uint32 `json:"asn,omitempty"`
	ASOrg     string `json:"as_org,omitempty"`
	Country   string `json:"country,omitempty"`
	Provider  string `json:"provider"`
	Connected bool   `json:"connected,omitempty"` // the address the heartbeat's TCP connect reached
}

// Info is what the API publishes per validator. Field names are the API's.
type Info struct {
	// Status is ok (an origin AS was found for the primary address),
	// no_asn (the address resolved but lies in no routed range the file
	// knows), or unresolved (no recent heartbeat recorded an address for
	// this host: the name did not resolve, resolved only to addresses the
	// observer does not dial, or the heartbeat has not reached it yet).
	Status string `json:"status"`
	Host   string `json:"host"`
	IP     string `json:"ip,omitempty"`
	ASN    uint32 `json:"asn,omitempty"`
	ASOrg  string `json:"as_org,omitempty"`
	// Country is an ISO 3166-1 alpha-2 code. CountryBasis says what it
	// means: "geolocation" is DB-IP's estimate of where the address is
	// used; "as_registry" is the country the announcing AS is registered in,
	// which for a multinational cloud is its head office, not the machine.
	Country      string `json:"country,omitempty"`
	CountryBasis string `json:"country_basis,omitempty"`
	// City and Region are DB-IP's IP to City Lite estimate for the primary
	// address, and Lat/Lon that city's approximate coordinates (a point for
	// the city, not for the machine). All absent without a city file, or
	// when the city file's country is not Country.
	City   string   `json:"city,omitempty"`
	Region string   `json:"region,omitempty"`
	Lat    *float64 `json:"lat,omitempty"`
	Lon    *float64 `json:"lon,omitempty"`
	// Provider is the normalised bucket for ASN (providers.go).
	Provider string `json:"provider"`
	// Addresses is every address the host resolved to, each looked up on
	// its own. MixedNetworks is set when they fall in more than one AS.
	Addresses     []Address `json:"addresses,omitempty"`
	MixedNetworks bool      `json:"mixed_networks,omitempty"`
	// ResolvedAt is when the heartbeat resolved the name (ResolvedBy
	// "heartbeat"), or empty for a host registered as a literal address
	// (ResolvedBy "literal"). LookedUpAt is when the files were consulted.
	ResolvedAt string `json:"resolved_at,omitempty"`
	ResolvedBy string `json:"resolved_by,omitempty"`
	LookedUpAt string `json:"looked_up_at"`
}

// Current reads the table: one Info per validator (hex address) with an
// open endpoint row this package looked up. A validator with two open
// endpoints (a host change caught between polls) gets the newer resolution.
// A database without the table answers an empty map, not an error: that is
// a collector from before this feature, and the feature is simply off.
//
// A table from before the city columns (the API opened the database before
// the new collector's EnsureSchema ran) reads as having no cities.
func Current(ctx context.Context, db store.Querier) (map[string]Info, error) {
	const base = `SELECT validator_address, host, status, ip, asn, as_org, country, country_basis,
		provider, addresses_json, resolved_at, resolved_by, looked_up_at`
	withCity := true
	rows, err := db.QueryContext(ctx, base+`, city, region, lat, lon FROM endpoint_hosting`)
	if err != nil && strings.Contains(err.Error(), "no such column") {
		withCity = false
		rows, err = db.QueryContext(ctx, base+` FROM endpoint_hosting`)
	}
	if err != nil {
		if isNoTable(err) {
			return map[string]Info{}, nil
		}
		return nil, err
	}
	defer rows.Close()
	out := map[string]Info{}
	for rows.Next() {
		var addr, addrs string
		var in Info
		var asn int64
		var lat, lon sql.NullFloat64
		dest := []any{&addr, &in.Host, &in.Status, &in.IP, &asn, &in.ASOrg, &in.Country, &in.CountryBasis,
			&in.Provider, &addrs, &in.ResolvedAt, &in.ResolvedBy, &in.LookedUpAt}
		if withCity {
			dest = append(dest, &in.City, &in.Region, &lat, &lon)
		}
		if err := rows.Scan(dest...); err != nil {
			return nil, err
		}
		if lat.Valid && lon.Valid {
			in.Lat, in.Lon = &lat.Float64, &lon.Float64
		}
		in.ASN = uint32(asn)
		// The bucket is a function of the AS number and of providers.go, not
		// a fact about the lookup: recompute it on read, so a change to the
		// provider list shows at once instead of after the next lookup pass.
		if in.ASN != 0 {
			in.Provider = ProviderFor(in.ASN)
		}
		_ = json.Unmarshal([]byte(addrs), &in.Addresses)
		for i := range in.Addresses {
			if in.Addresses[i].ASN != 0 {
				in.Addresses[i].Provider = ProviderFor(in.Addresses[i].ASN)
			}
		}
		seen := map[uint32]bool{}
		for _, a := range in.Addresses {
			if a.ASN != 0 {
				seen[a.ASN] = true
			}
		}
		in.MixedNetworks = len(seen) > 1
		if cur, ok := out[addr]; ok && cur.ResolvedAt >= in.ResolvedAt {
			continue
		}
		out[addr] = in
	}
	return out, rows.Err()
}

// isNoTable reports SQLite's "no such table" (the driver has no typed
// error for it).
func isNoTable(err error) bool {
	return err != nil && strings.Contains(err.Error(), "no such table")
}

// Sources is the provenance block the API publishes with every hosting
// figure: whether the feature is on, which files, how old, and under which
// licence. The licence fields are constants of the data sources, not
// configuration, because they are facts about those files.
type Sources struct {
	Enabled    bool      `json:"enabled"`
	ASN        *DBSource `json:"asn_db,omitempty"`
	Country    *DBSource `json:"country_db,omitempty"`
	City       *DBSource `json:"city_db,omitempty"`
	LookedUpAt string    `json:"looked_up_at,omitempty"`
	// Vantage is the caveat, in words, every hosting figure carries.
	Caveat string `json:"caveat"`
}

// DBSource describes one data file.
type DBSource struct {
	File        string `json:"file"`
	Modified    string `json:"modified,omitempty"`
	Name        string `json:"name"`
	URL         string `json:"url"`
	License     string `json:"license"`
	LicenseURL  string `json:"license_url"`
	Attribution string `json:"attribution,omitempty"` // the line the licence asks the page to print
}

// Caveat is the sentence every hosting figure is published under.
const Caveat = "As resolved from this vantage: the addresses are what this observer's DNS returned for each registered host, " +
	"and the network is the origin AS announcing them. GeoDNS, proxies, tunnels and anycast can hide where a host really runs; " +
	"country from a geolocation database is an estimate. A statement about routing, not about any operator."

// ReadSources reads the provenance from the meta table.
func ReadSources(ctx context.Context, db *sql.DB) (Sources, error) {
	s := Sources{Caveat: Caveat}
	meta := map[string]string{}
	rows, err := db.QueryContext(ctx, `SELECT key, value FROM meta WHERE key LIKE 'hosting_%'`)
	if err != nil {
		return s, err
	}
	defer rows.Close()
	for rows.Next() {
		var k, v string
		if err := rows.Scan(&k, &v); err != nil {
			return s, err
		}
		meta[k] = v
	}
	if err := rows.Err(); err != nil {
		return s, err
	}
	s.Enabled = meta[MetaEnabled] == "yes"
	if !s.Enabled {
		return s, nil
	}
	s.LookedUpAt = meta[MetaLookedUpAt]
	if f := meta[MetaASNDB]; f != "" {
		s.ASN = &DBSource{File: f, Modified: meta[MetaASNDBModified],
			Name: "IPtoASN", URL: "https://iptoasn.com/",
			License: "Public Domain (ODC PDDL v1.0)", LicenseURL: "https://opendatacommons.org/licenses/pddl/1-0/"}
	}
	if f := meta[MetaCountryDB]; f != "" {
		s.Country = &DBSource{File: f, Modified: meta[MetaCountryModified],
			Name: "DB-IP IP to Country Lite", URL: "https://db-ip.com/db/download/ip-to-country-lite",
			License: "CC BY 4.0", LicenseURL: "https://creativecommons.org/licenses/by/4.0/",
			Attribution: "IP Geolocation by DB-IP"}
	}
	if f := meta[MetaCityDB]; f != "" {
		s.City = &DBSource{File: f, Modified: meta[MetaCityModified],
			Name: "DB-IP IP to City Lite", URL: "https://db-ip.com/db/download/ip-to-city-lite",
			License: "CC BY 4.0", LicenseURL: "https://creativecommons.org/licenses/by/4.0/",
			Attribution: "IP Geolocation by DB-IP"}
	}
	return s, nil
}

// consHex turns a celestiavalcons1… address into its 20-byte hex form, the
// key reachability rows use. The endpoints table stores the bech32 form.
func consHex(bech string) (string, error) {
	hrp, raw, err := bech32.DecodeAndConvert(bech)
	if err != nil {
		return "", err
	}
	if !strings.HasSuffix(hrp, "valcons") || len(raw) != 20 {
		return "", fmt.Errorf("%q is not a 20-byte consensus address", bech)
	}
	return hex.EncodeToString(raw), nil
}

func ts(t time.Time) string { return store.TS(t) }
