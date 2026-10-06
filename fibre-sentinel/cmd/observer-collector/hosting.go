package main

// The hosting lookup pass: which network and country each registered Fibre
// host resolves into, from local database files (see observer/hosting and
// deploy/hosting-db.sh). It lives in its own file so main.go carries only
// the two lines that wire it in.
//
// Configuration, first match wins:
//
//	-hosting-asn-db / -hosting-country-db / -hosting-city-db     flags
//	HOSTING_ASN_DB / HOSTING_COUNTRY_DB / HOSTING_CITY_DB       environment (the systemd
//	                                                            units load <network>.env)
//	<data-dir>/hosting/ip2asn-combined.tsv.gz                   the files hosting-db.sh
//	<data-dir>/hosting/dbip-country-lite.csv.gz                 writes, when present
//	<data-dir>/hosting/dbip-city-lite.csv.gz
//
// No ASN file means the feature is off and nothing is looked up. The pass
// never makes a network call: addresses come from the heartbeat's own
// reachability rows.

import (
	"context"
	"flag"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/observer/hosting"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
)

// Package-level so the flag block in main() stays untouched; flag.Parse
// there picks them up like any other.
var (
	hostingASNFlag     = flag.String("hosting-asn-db", "", "iptoasn.com ip2asn TSV (gz ok) for the hosting lookup (default $HOSTING_ASN_DB, else <data-dir>/hosting/"+hosting.DefaultASNFile+" if present; none = feature off)")
	hostingCountryFlag = flag.String("hosting-country-db", "", "DB-IP IP-to-Country Lite CSV (gz ok), optional (default $HOSTING_COUNTRY_DB, else <data-dir>/hosting/"+hosting.DefaultCountryFile+" if present)")
	hostingCityFlag    = flag.String("hosting-city-db", "", "DB-IP IP-to-City Lite CSV (gz ok), optional, adds city and coordinates (default $HOSTING_CITY_DB, else <data-dir>/hosting/"+hosting.DefaultCityFile+" if present)")
)

// newHostingPass prepares the table and returns the function the collector
// calls on every endpoint-poll pass. Errors are logged, never fatal: this
// is an annotation on the record, not part of it. vantage is this
// observer's own: addresses are read from its heartbeats only, since another
// vantage may resolve a host differently.
func newHostingPass(st *store.Store, dataDir, vantage string, logf func(string, ...any)) func(ctx context.Context, now time.Time) {
	if err := hosting.EnsureSchema(st.DB()); err != nil {
		logf("hosting: create table: %v; lookups disabled", err)
		return func(context.Context, time.Time) {}
	}
	cfg := hosting.ResolveConfig(*hostingASNFlag, *hostingCountryFlag, *hostingCityFlag, dataDir)
	if cfg.ASNPath == "" {
		logf("hosting: no ASN database configured; provider/country lookup off (deploy/hosting-db.sh enables it)")
	} else {
		logf("hosting: asn db %s, country db %q, city db %q", cfg.ASNPath, cfg.CountryPath, cfg.CityPath)
	}
	r := &hosting.Refresher{DB: st.DB(), Cfg: cfg, Vantage: vantage, Logf: logf,
		Record: func(ctx context.Context, raw []byte) ([]byte, error) { return st.ReachRecord(ctx, st.DB(), raw) }}
	return func(ctx context.Context, now time.Time) {
		// The databases usually arrive after the collector has started
		// (deploy/hosting-db.sh runs once the new binaries are up), and an
		// optional one (the city file) can be added to a host that already
		// has the others. Resolve the paths again every pass, a few stats a
		// minute, so a file that appears is used without a restart. A file
		// that disappears is left to Run, which notices it by stat.
		if c := hosting.ResolveConfig(*hostingASNFlag, *hostingCountryFlag, *hostingCityFlag, dataDir); c.ASNPath != "" &&
			(c.ASNPath != r.Cfg.ASNPath || c.CountryPath != r.Cfg.CountryPath || c.CityPath != r.Cfg.CityPath) {
			if r.Cfg.ASNPath == "" {
				logf("hosting: asn db %s appeared, country db %q, city db %q; lookup on", c.ASNPath, c.CountryPath, c.CityPath)
			} else {
				logf("hosting: databases now asn %s, country %q, city %q", c.ASNPath, c.CountryPath, c.CityPath)
			}
			c.Lookback, c.MaxAge = r.Cfg.Lookback, r.Cfg.MaxAge
			r.Cfg = c
		}
		res, err := r.Run(ctx, now)
		switch {
		case err != nil:
			logf("hosting: %v", err)
		case res.Enabled && !res.Skipped:
			logf("hosting: %d open endpoint(s), %d resolved, %d with an origin AS, %d placed in a city", res.Hosts, res.Resolved, res.WithASN, res.WithCity)
		}
	}
}
