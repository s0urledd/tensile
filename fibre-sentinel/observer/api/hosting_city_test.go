package api_test

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/observer/api"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/hosting"
)

const testCity = "5.9.0.0,5.9.255.255,EU,DE,Saxony,Falkenstein,50.4779,12.3713\n" +
	"51.68.0.0,51.68.255.255,EU,FR,Hauts-de-France,Roubaix,50.6942,3.17456\n"

// Without a city file the city fields and by_city are simply missing; with
// one, every row carries its city and point, and /v1/hosting groups them.
func TestHostingCity(t *testing.T) {
	f := newHostingFixture(t)
	f.enable(t) // ASN only

	ts := httptest.NewServer(api.New(f.st, "test"))
	defer ts.Close()
	var raw map[string]json.RawMessage
	if code := get(t, ts, "/v1/hosting", &raw); code != 200 {
		t.Fatalf("hosting: %d", code)
	}
	if strings.Contains(string(raw["summary"]), "by_city") || strings.Contains(string(raw["sources"]), "city_db") {
		t.Fatalf("city output without a city file: %s %s", raw["summary"], raw["sources"])
	}
	var vraw struct {
		Validators []struct {
			Hosting json.RawMessage `json:"hosting"`
		} `json:"validators"`
	}
	get(t, ts, "/v1/validators", &vraw)
	for _, v := range vraw.Validators {
		for _, k := range []string{`"city"`, `"lat"`, `"lon"`} {
			if strings.Contains(string(v.Hosting), k) {
				t.Fatalf("%s without a city file: %s", k, v.Hosting)
			}
		}
	}

	city := filepath.Join(f.dir, hosting.DefaultCityFile)
	if err := os.WriteFile(city, []byte(testCity), 0o644); err != nil {
		t.Fatal(err)
	}
	r := &hosting.Refresher{DB: f.st.DB(), Cfg: hosting.Config{ASNPath: filepath.Join(f.dir, "ip2asn-combined.tsv.gz"), CityPath: city}}
	if res, err := r.Run(context.Background(), time.Now()); err != nil || res.WithCity != 3 {
		t.Fatalf("refresh with city: %+v %v", res, err)
	}

	ts2 := httptest.NewServer(api.New(f.st, "test"))
	defer ts2.Close()
	var on struct {
		Sources struct {
			City *struct {
				Name        string `json:"name"`
				License     string `json:"license"`
				Attribution string `json:"attribution"`
			} `json:"city_db"`
		} `json:"sources"`
		Summary struct {
			ByCity []struct {
				Key        string   `json:"key"`
				City       string   `json:"city"`
				Region     string   `json:"region"`
				Country    string   `json:"country"`
				Lat        *float64 `json:"lat"`
				Lon        *float64 `json:"lon"`
				Hosts      int      `json:"hosts"`
				HostShare  float64  `json:"host_share"`
				StakeShare float64  `json:"stake_share"`
			} `json:"by_city"`
		} `json:"summary"`
	}
	if code := get(t, ts2, "/v1/hosting", &on); code != 200 {
		t.Fatalf("hosting: %d", code)
	}
	if c := on.Sources.City; c == nil || c.License != "CC BY 4.0" || c.Attribution != "IP Geolocation by DB-IP" || c.Name != "DB-IP IP to City Lite" {
		t.Fatalf("city source: %+v", c)
	}
	bc := on.Summary.ByCity
	if len(bc) != 2 {
		t.Fatalf("by_city: %+v", bc)
	}
	if b := bc[0]; b.Key != "DE/Saxony/Falkenstein" || b.City != "Falkenstein" || b.Country != "DE" || b.Region != "Saxony" ||
		b.Hosts != 2 || b.Lat == nil || *b.Lat != 50.4779 || *b.Lon != 12.3713 || b.StakeShare < 0.69 || b.StakeShare > 0.71 {
		t.Fatalf("Falkenstein: %+v", b)
	}
	if b := bc[1]; b.City != "Roubaix" || b.Hosts != 1 || b.HostShare < 0.33 || b.HostShare > 0.34 {
		t.Fatalf("Roubaix: %+v", b)
	}

	var vals struct {
		Validators []struct {
			Address string        `json:"address"`
			Hosting *hosting.Info `json:"hosting"`
		} `json:"validators"`
	}
	get(t, ts2, "/v1/validators", &vals)
	for _, v := range vals.Validators {
		h := v.Hosting
		if h == nil || h.City == "" || h.Lat == nil || h.Lon == nil {
			t.Fatalf("%s: no city: %+v", v.Address, h)
		}
		if v.Address == f.addrs[2] && (h.City != "Roubaix" || h.Country != "FR" || *h.Lon != 3.17456) {
			t.Fatalf("gamma: %+v", h)
		}
	}
	// the region stays in the row, which /v1/hosting groups cities by
	var rows struct {
		Validators []struct {
			Address string        `json:"address"`
			Hosting *hosting.Info `json:"hosting"`
		} `json:"validators"`
	}
	rowsOf(t, f.st, "test", "24h", time.Time{}, &rows)
	for _, v := range rows.Validators {
		if v.Address == f.addrs[2] && (v.Hosting == nil || v.Hosting.Region != "Hauts-de-France") {
			t.Fatalf("gamma's row: %+v", v.Hosting)
		}
	}
}
