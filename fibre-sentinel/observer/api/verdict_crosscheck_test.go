package api_test

import (
	"encoding/json"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/probe"
	"github.com/plsgiveup/fibre/fibre-sentinel/internal/scan"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/ingest"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/verdict"
)

// rowsFromStore reads every probe row and publication back as the record
// holds them (raw_json), which is what a verifier holding the export has.
func rowsFromStore(t *testing.T, st *store.Store) ([]verdict.Row, map[string]time.Time, verdict.Blobs) {
	t.Helper()
	var rows []verdict.Row
	prs, err := st.DB().Query(`SELECT raw_json FROM probes`)
	if err != nil {
		t.Fatal(err)
	}
	for prs.Next() {
		var raw string
		if err := prs.Scan(&raw); err != nil {
			t.Fatal(err)
		}
		var m probe.Measurement
		if err := json.Unmarshal([]byte(raw), &m); err != nil {
			t.Fatal(err)
		}
		rows = append(rows, verdict.FromMeasurement(m))
	}
	prs.Close()
	settled := map[string]time.Time{}
	blobs := verdict.Blobs{}
	pbs, err := st.DB().Query(`SELECT raw_json FROM publications`)
	if err != nil {
		t.Fatal(err)
	}
	for pbs.Next() {
		var raw string
		if err := pbs.Scan(&raw); err != nil {
			t.Fatal(err)
		}
		var p scan.Publication
		if err := json.Unmarshal([]byte(raw), &p); err != nil {
			t.Fatal(err)
		}
		settled[p.PromiseHash] = p.SettlementTime
		blobs[p.PromiseHash] = verdict.FactsOf(p)
	}
	pbs.Close()
	return rows, settled, blobs
}

type apiObligations struct {
	Network struct {
		Obligations   obligationsJSON `json:"obligations"`
		VantageHealth struct {
			Suspect []struct {
				At     string `json:"at"`
				Reason string `json:"reason"`
			} `json:"suspect"`
		} `json:"vantage_health"`
	}
	Validators []struct {
		Address     string          `json:"address"`
		Obligations obligationsJSON `json:"obligations"`
	}
}

func fetchObligations(t *testing.T, ts *httptest.Server, query string) apiObligations {
	t.Helper()
	var out apiObligations
	get(t, ts, "/v1/network?"+query, &out.Network)
	var vals struct {
		Validators []struct {
			Address     string          `json:"address"`
			Obligations obligationsJSON `json:"obligations"`
		} `json:"validators"`
	}
	get(t, ts, "/v1/validators?"+query, &vals)
	out.Validators = vals.Validators
	return out
}

// same compares every bucket, Total included. Total is the count of
// obligations the buckets partition, so leaving it out of this comparison
// let a bucket appear on one side and not the other without the one test
// that runs both implementations over the same rows noticing: the named
// buckets can agree while the two disagree about how many obligations there
// were.
func same(a obligationsJSON, b verdict.Obligations) bool {
	return a.Total == b.Total && a.Served == b.Served && a.Broken == b.Broken &&
		a.HeldParamUnverified == b.HeldParamUnverified && a.NotCounted == b.NotCounted && a.Pending == b.Pending
}

// The obligation buckets and the suspect points the API computes in SQL
// must equal what the verdict package derives from the same rows in Go,
// on every window, pinned or live: a third party with the export
// reproduces the site's figures.
func TestVerdictPackageMatchesTheSQL(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "observer.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	now := time.Now().UTC().Truncate(time.Second)
	// the obligations fixture's profiles, plus a suspect point, plus a
	// publication still pending, plus the sample record
	created, msu := now.Add(-2*time.Hour), now.Add(-30*time.Minute)
	insertProbeSet(t, st, "x1", created, msu, map[string][]wire{
		"served": {ok, ok, ok, ok}, "endun": {ok, err500, err500, err500}, "broken": {ok, gone, ok, ok},
		"unreach": {refused, refused, refused, refused}, "reach": {err500, err500, err500, err500},
		"backoff": {skipped, skipped, skipped, skipped}, "gaplast": {ok, ok, ok, skipped},
	}, false)
	insertProbeSet(t, st, "x2", created.Add(10*time.Minute), msu, map[string][]wire{
		"a": {refused, ok}, "b": {refused, ok}, "c": {refused, ok}, "d": {ok, ok},
	}, false)
	insertProbeSet(t, st, "x3", now.Add(-20*time.Minute), now.Add(time.Hour), map[string][]wire{
		"a": {ok}, "b": {gone}, "c": {ok},
	}, false)
	for _, f := range []string{"publications.jsonl", "measurements.jsonl"} {
		var err error
		if f == "publications.jsonl" {
			_, err = ingest.Publications(st, filepath.Join(sampleDir, f), now)
		} else {
			_, err = ingest.Measurements(st, filepath.Join(sampleDir, f), now)
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	rows, settled, blobs := rowsFromStore(t, st)
	ts := httptestServer(t, st)

	for _, c := range []struct {
		name  string
		query string
		win   verdict.Window
	}{
		{"all live", "window=all", verdict.Window{All: true, End: now}},
		{"24h live", "window=24h", verdict.Window{Start: now.Add(-24 * time.Hour), End: now}},
		{"24h pinned mid-schedule", "window=24h&as_of=" + created.Add(150*time.Second).Format(time.RFC3339),
			verdict.Window{Start: created.Add(150 * time.Second).Add(-24 * time.Hour), End: created.Add(150 * time.Second)}},
		{"all pinned before x3", "window=all&as_of=" + now.Add(-25*time.Minute).Format(time.RFC3339),
			verdict.Window{All: true, End: now.Add(-25 * time.Minute)}},
	} {
		api := fetchObligations(t, ts, c.query)
		sus := verdict.SuspectPoints(rows, c.win, blobs)
		net, byVal := verdict.ComputeObligations(rows, settled, c.win, sus, blobs)
		if len(sus) != len(api.Network.VantageHealth.Suspect) {
			t.Errorf("%s: suspect points: go %d, sql %d", c.name, len(sus), len(api.Network.VantageHealth.Suspect))
		}
		for i := range sus {
			if i < len(api.Network.VantageHealth.Suspect) && api.Network.VantageHealth.Suspect[i].At != store.TS(sus[i].At) {
				t.Errorf("%s: suspect point %d: go %s, sql %s", c.name, i, store.TS(sus[i].At), api.Network.VantageHealth.Suspect[i].At)
			}
		}
		if !same(api.Network.Obligations, net) {
			t.Errorf("%s: network obligations differ:\nsql %+v\ngo  %+v", c.name, api.Network.Obligations, net)
		}
		seen := 0
		for _, v := range api.Validators {
			g := byVal[v.Address]
			if v.Obligations.Total == 0 && g.Total == 0 {
				continue
			}
			seen++
			if !same(v.Obligations, g) {
				t.Errorf("%s: %s obligations differ:\nsql %+v\ngo  %+v", c.name, v.Address, v.Obligations, g)
			}
		}
		if seen == 0 && net.Total > 0 {
			t.Errorf("%s: no validator carried obligations in the API answer", c.name)
		}
	}
}
