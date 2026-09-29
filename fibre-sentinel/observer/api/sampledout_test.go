package api_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/probe"
	"github.com/plsgiveup/fibre/fibre-sentinel/internal/scan"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/api"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/rollup"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
)

// soFixture is a record with probed and sampled-out publications side by
// side: what the site's figures are drawn from.
type soFixture struct {
	pubs      []scan.Publication
	real      []probe.Measurement
	decisions []probe.SampledOut
}

func soAddr(i int) string { return fmt.Sprintf("%040x", 0xa0+i) }

func soPub(hash string, height int64, settle, msu time.Time) scan.Publication {
	var vals []scan.ValidatorAssignment
	for i := 0; i < 6; i++ {
		// the last one's signature is not among the promise's: unattested
		vals = append(vals, scan.ValidatorAssignment{Address: soAddr(i), VotingPower: 10, RowCount: 2, Rows: []int{2 * i, 2*i + 1}, Attested: i != 5})
	}
	return scan.Publication{
		SchemaVersion: scan.AttestationSchemaVersion, PromiseHash: hash,
		SettlementHeight: height, SettlementTime: settle, MustServeUntil: msu, RecordedAt: settle,
		SettlementTxHash: "tx" + hash, Signer: "celestia1pub",
		Promise:                 scan.PromiseFields{ChainID: "t", Height: height - 1, Commitment: "cc" + hash, CreationTimestamp: settle, BlobSize: 4096},
		ValidatorSignatureCount: 5,
		Assignment: scan.AssignmentTable{
			ProtocolParams:     scan.ProtocolParamsSnapshot{OriginalRows: 4, TotalRows: 16},
			ValidatorSetHeight: height - 1, TotalVotingPower: 60, Sigma: 12, Distinct: 12,
			ValidatorsWithRows: 6, AttestedWithRows: 5, SignatureEntries: 5, SignaturesVerified: 5,
			AttestedVotingPower: 50, Validators: vals,
		},
	}
}

// soRows is every point of pub probed, validator i seeing wires[i] at every
// in-window point (and a served shard in grace and post); at is a hook to
// change one point's wire for every validator.
func soRows(pub scan.Publication, wires []wire, at func(label string, w wire) wire) []probe.Measurement {
	var out []probe.Measurement
	for i, v := range pub.Assignment.Validators {
		for _, pt := range earlierSchedule(pub) {
			w := wires[i]
			ph := probe.PhaseAt(pt.At, pub, probe.ScheduleConfig{})
			if ph != probe.PhaseInWindow {
				w = ok
			}
			if at != nil {
				w = at(pt.Label, w)
			}
			m := probe.Measurement{
				SchemaVersion: probe.AttestationSchemaVersion, Vantage: "test",
				PromiseHash: pub.PromiseHash, Commitment: pub.Promise.Commitment, MustServeUntil: pub.MustServeUntil,
				ValidatorSetHeight: pub.Assignment.ValidatorSetHeight, ValidatorAddress: v.Address, ValidatorHost: "h" + v.Address[36:] + ":7980",
				Assigned: true, Attested: v.Attested, AssignedRowCount: v.RowCount,
				ScheduleLabel: pt.Label, ScheduledAt: pt.At, StartedAt: pt.At.Add(time.Second), FinishedAt: pt.At.Add(2 * time.Second),
				Phase: ph, Outcome: w.outcome, TotalDurationMS: 40 + int64(i),
				Sampling: &probe.SamplingDecision{P: 1, Binding: "none", DayCommitment: "c0ffee"},
			}
			if w.outcome == probe.OutcomeMissed {
				m.Classification, m.ClassificationReason = probe.ClassNotProbed, "scheduled point elapsed before the prober ran it"
			} else {
				m.Classification, m.ClassificationReason = probe.Classify(probe.Evidence{Assigned: true, Attested: v.Attested, Phase: ph, Outcome: w.outcome})
			}
			m.TCP.OK, m.TLS.OK, m.Identity.OK = w.tls, w.tls, w.tls
			if w.outcome == probe.OutcomeServedOK {
				m.Download.OK, m.Download.RowsReturned, m.Download.RowsExpected = true, 2, 2
				m.Download.CommitmentVerified, m.Download.AssignmentVerified = true, true
			}
			out = append(out, m)
		}
	}
	return out
}

func soDecision(pub scan.Publication, p float64) probe.SampledOut {
	d := probe.SampledOut{
		SchemaVersion: probe.SampledOutSchemaVersion, Kind: probe.SampledOutKind, Vantage: "test",
		PromiseHash: pub.PromiseHash, Commitment: pub.Promise.Commitment, SettlementTime: pub.SettlementTime,
		MustServeUntil: pub.MustServeUntil, ValidatorSetHeight: pub.Assignment.ValidatorSetHeight,
		DecidedAt: pub.SettlementTime.Add(9 * time.Second),
		Sampling:  probe.SamplingDecision{P: p, Binding: "validator_bytes_per_day", DayCommitment: "c0ffee"},
		Reason:    fmt.Sprintf("budget:p=%.3f:validator_bytes_per_day:day_commitment=c0ffee", p), Validators: 6,
	}
	for _, pt := range earlierSchedule(pub) {
		d.Points = append(d.Points, probe.SampledOutPoint{Label: pt.Label, At: pt.At, Phase: probe.PhaseAt(pt.At, pub, probe.ScheduleConfig{})})
	}
	return d
}

func sampledOutFixture(now time.Time) soFixture {
	var f soFixture
	missed := wire{probe.OutcomeMissed, false}
	profile := []wire{ok, ok, refused, err500, missed, gone}

	// Two days ago by the calendar, so the rollup below always rolls and
	// prunes their day whatever the hour the test runs at.
	day2 := now.Truncate(24 * time.Hour).Add(-46 * time.Hour)
	old := soPub("probedold", 100, day2, day2.Add(4*time.Hour))
	f.pubs = append(f.pubs, old)
	f.real = append(f.real, soRows(old, profile, func(label string, w wire) wire {
		if label == "w4" && w == ok {
			return gone // the second validator's early prune: a fault at the last in-window point
		}
		return w
	})...)
	outOld := soPub("outold", 101, day2.Add(time.Minute), day2.Add(4*time.Hour+time.Minute))
	f.pubs = append(f.pubs, outOld)
	f.decisions = append(f.decisions, soDecision(outOld, 0.286))

	// Hours ago: a probed publication and a sampled-out one settled in the
	// same block, so their schedule points coincide, and at w2 every
	// validator of the probed one was unreachable.
	recent := soPub("probedrecent", 200, now.Add(-6*time.Hour), now.Add(-2*time.Hour))
	f.pubs = append(f.pubs, recent)
	f.real = append(f.real, soRows(recent, []wire{ok, ok, ok, err500, ok, gone}, func(label string, w wire) wire {
		if label == "w2" {
			return refused
		}
		return w
	})...)
	outRecent := soPub("outrecent", 200, now.Add(-6*time.Hour), now.Add(-2*time.Hour))
	outRecent.SettlementTxIndex = 1
	f.pubs = append(f.pubs, outRecent)
	f.decisions = append(f.decisions, soDecision(outRecent, 0.311))

	// Still under obligation: pending, with points yet to come.
	pending := soPub("outpending", 300, now.Add(-time.Hour), now.Add(3*time.Hour))
	f.pubs = append(f.pubs, pending)
	f.decisions = append(f.decisions, soDecision(pending, 0.29))
	return f
}

// soStore builds a store from the fixture: with each sampled-out
// publication as the rows the prober used to write (asRows) or as its one
// decision.
func soStore(t *testing.T, f soFixture, asRows bool) *store.Store {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "observer.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	byHash := map[string]scan.Publication{}
	for _, p := range f.pubs {
		raw, _ := json.Marshal(p)
		if _, err := st.UpsertPublication(p, raw); err != nil {
			t.Fatal(err)
		}
		byHash[p.PromiseHash] = p
	}
	put := func(m probe.Measurement) {
		raw, _ := json.Marshal(m)
		if ok, err := st.InsertProbe(m, raw); err != nil || !ok {
			t.Fatalf("insert %s: %v %v", m.DedupeKey(), ok, err)
		}
	}
	for _, m := range f.real {
		put(m)
	}
	for _, d := range f.decisions {
		if asRows {
			for _, m := range d.Expand(byHash[d.PromiseHash]) {
				put(m)
			}
			continue
		}
		raw, _ := json.Marshal(d)
		if ok, err := st.InsertSampledOut(d, raw); err != nil || !ok {
			t.Fatalf("insert decision %s: %v %v", d.Key(), ok, err)
		}
	}
	if _, err := st.StartRun("collector", "test", "t", time.Now()); err != nil {
		t.Fatal(err)
	}
	return st
}

// volatile are the response fields that say when or how fast an answer was
// computed; the lists of rows, where a sampled-out publication's rows are
// not listed (TestSampledOutBlobsCountTheRowsTheyStandFor checks those); the
// newest reading row (/v1/meta last_probe_at), which a decision is not; and
// the store's own row counts in /v1/meta, which are how many rows it holds
// and are meant to shrink.
var volatile = map[string]bool{"computed_at": true, "compute_ms": true, "server_time": true,
	"recent_probes": true, "recent_probes_truncated": true, "last_probe_at": true, "counts": true}

func strip(v any) any {
	switch x := v.(type) {
	case map[string]any:
		for k := range x {
			if volatile[k] {
				delete(x, k)
				continue
			}
			x[k] = strip(x[k])
		}
	case []any:
		for i := range x {
			x[i] = strip(x[i])
		}
	}
	return v
}

func fetch(t *testing.T, ts *httptest.Server, path string) any {
	t.Helper()
	resp, err := http.Get(ts.URL + path)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		t.Fatalf("%s: HTTP %d %s", path, resp.StatusCode, b)
	}
	var v any
	if err := json.Unmarshal(b, &v); err != nil {
		t.Fatalf("%s: %v", path, err)
	}
	return strip(v)
}

// firstDiff names where two decoded answers part, for the failure message.
func firstDiff(path string, a, b any) string {
	switch x := a.(type) {
	case map[string]any:
		y, ok := b.(map[string]any)
		if !ok {
			return path
		}
		keys := map[string]bool{}
		for k := range x {
			keys[k] = true
		}
		for k := range y {
			keys[k] = true
		}
		var ks []string
		for k := range keys {
			ks = append(ks, k)
		}
		sort.Strings(ks)
		for _, k := range ks {
			if d := firstDiff(path+"."+k, x[k], y[k]); d != "" {
				return d
			}
		}
		return ""
	case []any:
		y, ok := b.([]any)
		if !ok || len(x) != len(y) {
			return fmt.Sprintf("%s (len %d vs %v)", path, len(x), b)
		}
		for i := range x {
			if d := firstDiff(fmt.Sprintf("%s[%d]", path, i), x[i], y[i]); d != "" {
				return d
			}
		}
		return ""
	}
	if !reflect.DeepEqual(a, b) {
		// The two answers are computed a moment apart, and every window
		// ends at "now": a timestamp that differs by less than that moment
		// is the clock, not the figure.
		sa, oka := a.(string)
		sb, okb := b.(string)
		if oka && okb {
			ta, ea := time.Parse(time.RFC3339Nano, sa)
			tb, eb := time.Parse(time.RFC3339Nano, sb)
			if ea == nil && eb == nil && ta.Sub(tb).Abs() < 5*time.Second {
				return ""
			}
		}
		return fmt.Sprintf("%s: %v vs %v", path, a, b)
	}
	return ""
}

func soPaths() []string {
	paths := []string{"/v1/meta", "/v1/blobs?limit=50", "/v1/sampling?window=7d", "/v1/sampling?window=all"}
	for _, w := range []string{"24h", "7d", "all"} {
		paths = append(paths, "/v1/network?window="+w, "/v1/validators?window="+w)
		for i := 0; i < 6; i++ {
			paths = append(paths, "/v1/validators/"+soAddr(i)+"?window="+w)
		}
	}
	for _, h := range []string{"probedold", "outold", "probedrecent", "outrecent", "outpending"} {
		paths = append(paths, "/v1/blobs/"+h, "/v1/probes?blob="+h+"&class=FAULT")
	}
	return paths
}

func compareStores(t *testing.T, what string, a, b *store.Store) {
	t.Helper()
	ta := httptest.NewServer(api.New(a, "test"))
	defer ta.Close()
	tb := httptest.NewServer(api.New(b, "test"))
	defer tb.Close()
	for _, p := range soPaths() {
		va, vb := fetch(t, ta, p), fetch(t, tb, p)
		if strings.HasPrefix(p, "/v1/blobs/") {
			// The blob page lists rows; a sampled-out blob's are its
			// decision instead (checked below). Every figure on it must
			// still agree.
			delete(va.(map[string]any), "probes")
			delete(vb.(map[string]any), "probes")
		}
		if d := firstDiff("", va, vb); d != "" {
			t.Errorf("%s: %s differs at %s", what, p, d)
		}
	}
	// and what the summary and the rows keep beside what they publish: the
	// reading tallies, attestation, latency
	for _, w := range []string{"24h", "7d", "all"} {
		for name, whole := range map[string]func(*store.Store, string, string, time.Time) ([]byte, error){
			"network": api.NetworkJSON, "validator rows": api.ValidatorRowsJSON,
		} {
			ra, err := whole(a, "test", w, time.Time{})
			if err != nil {
				t.Fatal(err)
			}
			rb, err := whole(b, "test", w, time.Time{})
			if err != nil {
				t.Fatal(err)
			}
			var va, vb any
			if err := json.Unmarshal(ra, &va); err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(rb, &vb); err != nil {
				t.Fatal(err)
			}
			if d := firstDiff("", strip(va), strip(vb)); d != "" {
				t.Errorf("%s: %s %s differs at %s", what, name, w, d)
			}
		}
	}
}

// Every figure the site shows is the same whether a sampled-out publication
// is stored as the NOT_PROBED rows the prober used to write, as the one
// decision the prober writes now, or as the decision the migration makes
// from those rows: the network and validator figures, the obligations, the
// blob list's counts, the sampling audit, and the "all" window once days
// are rolled up and pruned.
func TestSampledOutFiguresUnchanged(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	f := sampledOutFixture(now)
	rows := soStore(t, f, true)
	dec := soStore(t, f, false)
	mig := soStore(t, f, true)
	n, deleted, err := mig.CollapseSampledOut(context.Background())
	if err != nil || n != 3 || deleted != 3*6*6 {
		t.Fatalf("collapse: %d decisions, %d rows, %v", n, deleted, err)
	}
	var stored int64
	_ = dec.DB().QueryRow(`SELECT COUNT(*) FROM probes`).Scan(&stored)
	if stored != int64(len(f.real)) {
		t.Fatalf("decision store holds %d probe rows, want only the %d real ones", stored, len(f.real))
	}

	// The fixture is doing what it is for: sampled-out obligations are not
	// read, so not counted (or pending).
	var net struct {
		Obligations struct {
			NotCounted int64 `json:"not_counted"`
			Pending    int64 `json:"pending"`
		} `json:"obligations"`
	}
	tr := httptest.NewServer(api.New(rows, "test"))
	get(t, tr, "/v1/network?window=7d", &net)
	tr.Close()
	var whole struct {
		Classes map[string]int64 `json:"classes"`
	}
	networkOf(t, rows, "test", "7d", time.Time{}, &whole)
	if net.Obligations.NotCounted < 10 || net.Obligations.Pending < 5 || whole.Classes["NOT_PROBED"] == 0 {
		t.Fatalf("fixture does not exercise the figures: %+v %v", net, whole.Classes)
	}

	compareStores(t, "decision", rows, dec)
	compareStores(t, "migrated", rows, mig)

	// The daily rollups and, once rolled days are pruned, the "all" window
	// built on them.
	cfg := rollup.Config{RollupAfter: time.Nanosecond, RetainRaw: 24 * time.Hour, Batch: 5000}
	for _, st := range []*store.Store{rows, dec, mig} {
		rep, err := rollup.Run(context.Background(), st, now, cfg)
		if err != nil {
			t.Fatal(err)
		}
		if len(rep.RolledDays) == 0 || len(rep.PrunedDays) == 0 {
			t.Fatalf("the rollup did not roll and prune the fixture's first day: %+v", rep)
		}
	}
	// The pruned day took the decision with it, as it took the rows.
	var left int64
	_ = dec.DB().QueryRow(`SELECT COUNT(*) FROM sampling_decisions WHERE promise_hash = 'outold'`).Scan(&left)
	if left != 0 {
		t.Fatal("a pruned day's sampled-out decision is still stored")
	}
	dump := func(st *store.Store, q string) string {
		r, err := st.DB().Query(q)
		if err != nil {
			t.Fatal(err)
		}
		defer r.Close()
		cols, _ := r.Columns()
		var out []string
		for r.Next() {
			v := make([]any, len(cols))
			p := make([]any, len(cols))
			for i := range v {
				p[i] = &v[i]
			}
			if err := r.Scan(p...); err != nil {
				t.Fatal(err)
			}
			out = append(out, fmt.Sprint(v...))
		}
		return strings.Join(out, "\n")
	}
	for _, q := range []string{
		`SELECT day, validator_address, total, served, broken, end_unobserved, held_param_unverified, unobserved_reachable, unobserved_unreachable, unobserved_not_probed, pending FROM obligation_daily ORDER BY 1, 2`,
		`SELECT day, validator_address, probes, gaps, faults, classes_json, attested, unattested, unknown_att FROM probe_daily ORDER BY 1, 2`,
		`SELECT key, value FROM meta WHERE key IN ('rollup_through', 'raw_from') ORDER BY 1`,
	} {
		want := dump(rows, q)
		for name, st := range map[string]*store.Store{"decision": dec, "migrated": mig} {
			if got := dump(st, q); got != want {
				t.Errorf("%s rollup differs for %s:\nrows:\n%s\n%s:\n%s", name, q, want, name, got)
			}
		}
	}
	compareStores(t, "decision after rollup", rows, dec)
	compareStores(t, "migrated after rollup", rows, mig)
}

// A blob the earlier load policy drew out of its sample was not read: its
// counts still stand for the NOT_PROBED rows its one decision replaces, and
// no reading row is listed for it anywhere.
func TestSampledOutBlobsCountTheRowsTheyStandFor(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	f := sampledOutFixture(now)
	ts := httptest.NewServer(api.New(soStore(t, f, false), "test"))
	defer ts.Close()

	var blob struct {
		Blob struct {
			ProbeCount      int64            `json:"probe_count"`
			Classes         map[string]int64 `json:"classes"`
			Reconstructable struct {
				Status string `json:"status"`
			} `json:"reconstructable"`
		} `json:"blob"`
		Probes []any `json:"probes"`
	}
	get(t, ts, "/v1/blobs/outold", &blob)
	if blob.Blob.ProbeCount != 36 || blob.Blob.Classes["NOT_PROBED"] != 36 || len(blob.Probes) != 0 {
		t.Fatalf("blob counts %d %v, %d probe rows listed", blob.Blob.ProbeCount, blob.Blob.Classes, len(blob.Probes))
	}
	if blob.Blob.Reconstructable.Status != "not_read" {
		t.Fatalf("a sampled-out blob whose window closed reads %q, want not_read", blob.Blob.Reconstructable.Status)
	}

	var probes struct {
		Probes []any `json:"probes"`
	}
	get(t, ts, "/v1/probes?blob=outrecent", &probes)
	if len(probes.Probes) != 0 {
		t.Fatalf("/v1/probes?blob=outrecent lists %d rows", len(probes.Probes))
	}
}

// earlierSchedule is the schedule the sampled-out decisions on record were
// made under: four in-window points, a grace point and a post point.
func earlierSchedule(pub scan.Publication) []probe.SchedulePoint {
	span := pub.MustServeUntil.Sub(pub.SettlementTime)
	var pts []probe.SchedulePoint
	for i, f := range []float64{0.12, 0.45, 0.72, 0.92} {
		pts = append(pts, probe.SchedulePoint{At: pub.SettlementTime.Add(time.Duration(float64(span) * f)), Phase: probe.PhaseInWindow, Label: fmt.Sprintf("w%d", i+1)})
	}
	return append(pts,
		probe.SchedulePoint{At: pub.MustServeUntil.Add(30 * time.Second), Phase: probe.PhaseGrace, Label: "grace"},
		probe.SchedulePoint{At: pub.MustServeUntil.Add(210 * time.Second), Phase: probe.PhasePost, Label: "post"})
}
