package api_test

import (
	"encoding/json"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/probe"
	"github.com/plsgiveup/fibre/fibre-sentinel/internal/scan"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/api"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
)

// The stability layers an operator reads, and the unit each is counted in.
//
// These exist because the same fact stated in the wrong unit reads as a
// different, worse fact. An obligation is probed at four schedule points, so
// a probe count of anything held out of the serve rate runs four times the
// number of blobs it actually concerns, and "812 unattested" against a named
// operator is not the same claim as "203 blobs carried no signature from you".

// stabilityFixtureStore builds two publications over two validators. v1 carries a
// verified signature on both; v2 carries none, which is what the two-thirds
// quorum leaves behind. Every validator is probed at all four in-window
// points of both blobs, so probe counts are exactly four times obligation
// counts and a test can tell which unit a figure is in.
//
// v1 serves at w1 and w2 of the second blob and returns nothing at w3 and w4:
// a validator that prunes early, which the pooled rate cannot distinguish
// from one that is uniformly poor.
//
// Reachability heartbeats: ten for each validator, of which v2's last three
// did not complete TLS.
func stabilityFixtureStore(t *testing.T) (*httptest.Server, *store.Store) {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "observer.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })

	now := time.Now().UTC().Truncate(time.Second)
	points := []string{"w1", "w2", "w3", "w4"}

	for bi, hash := range []string{"b001", "b002"} {
		created := now.Add(-time.Duration(60-bi*10) * time.Minute)
		msu := now.Add(time.Duration(30+bi*10) * time.Minute)
		pub := scan.Publication{
			SchemaVersion:    scan.AttestationSchemaVersion,
			PromiseHash:      hash,
			SettlementHeight: int64(100 + bi),
			SettlementTime:   created,
			MustServeUntil:   msu,
			RecordedAt:       now,
			SettlementTxHash: "tx" + hash,
			Signer:           "celestia1pub",
			Promise: scan.PromiseFields{ChainID: "t", Height: 99, Commitment: "cc" + hash,
				CreationTimestamp: created, BlobSize: 1024},
			ValidatorSignatureCount: 1,
			Assignment: scan.AssignmentTable{
				ProtocolParams:      scan.ProtocolParamsSnapshot{OriginalRows: 4, TotalRows: 8},
				ValidatorSetHeight:  99,
				TotalVotingPower:    20,
				Sigma:               4,
				Distinct:            4,
				ValidatorsWithRows:  2,
				AttestedWithRows:    1,
				SignatureEntries:    1,
				SignaturesVerified:  1,
				AttestedVotingPower: 10,
				Validators: []scan.ValidatorAssignment{
					{Address: "v1", VotingPower: 10, RowCount: 2, Rows: []int{0, 1}, Attested: true},
					{Address: "v2", VotingPower: 10, RowCount: 2, Rows: []int{2, 3}, Attested: false},
				},
			},
		}
		raw, err := json.Marshal(pub)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := st.UpsertPublication(pub, raw); err != nil {
			t.Fatal(err)
		}

		for pi, label := range points {
			at := created.Add(time.Duration(pi+1) * time.Minute)
			for _, addr := range []string{"v1", "v2"} {
				attested := addr == "v1"
				outcome := probe.OutcomeServedOK
				// v1 prunes the second blob after the first half of its window.
				if addr == "v1" && bi == 1 && pi >= 2 {
					outcome = probe.OutcomeNotFound
				}
				if addr == "v2" {
					outcome = probe.OutcomeNotFound
				}
				class, reason := probe.Classify(probe.Evidence{
					Assigned: true, Attested: attested, Phase: probe.PhaseInWindow, Outcome: outcome,
				})
				m := probe.Measurement{
					SchemaVersion: probe.AttestationSchemaVersion, Vantage: "test",
					PromiseHash: hash, Commitment: "cc" + hash, MustServeUntil: msu, ValidatorSetHeight: 99,
					ValidatorAddress: addr, ValidatorHost: addr + ":443",
					Assigned: true, Attested: attested, AssignedRowCount: 2,
					ScheduleLabel: label, ScheduledAt: at, StartedAt: at, FinishedAt: at,
					Phase: probe.PhaseInWindow, Outcome: outcome,
					Classification: class, ClassificationReason: reason,
				}
				if outcome == probe.OutcomeServedOK {
					m.Download.OK, m.Download.RowsReturned, m.Download.RowsExpected = true, 2, 2
					m.Download.CommitmentVerified, m.Download.AssignmentVerified = true, true
				}
				raw, err := json.Marshal(m)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := st.InsertProbe(m, raw); err != nil {
					t.Fatal(err)
				}
			}
		}
	}

	for i := 0; i < 10; i++ {
		at := now.Add(-time.Duration(10-i) * 10 * time.Minute)
		for _, addr := range []string{"v1", "v2"} {
			up := addr == "v1" || i < 7
			m := probe.Measurement{
				SchemaVersion: probe.AttestationSchemaVersion, Vantage: "test",
				ValidatorAddress: addr, ValidatorHost: addr + ":443", ValidatorSetHeight: 99,
				ScheduledAt: at, StartedAt: at, FinishedAt: at,
			}
			m.DNS.OK = true
			m.TCP.OK, m.TLS.OK = up, up
			// v2's endpoint answers throughout but its endorsement lapsed for
			// the two heartbeats before it went down, so identity validity and
			// reachability are different measurements of different things.
			m.Identity.OK = up && !(addr == "v2" && i >= 5)
			if up {
				m.Outcome = probe.OutcomeReachable
			} else {
				m.Outcome = probe.OutcomeTCPRefused
			}
			raw, err := json.Marshal(m)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := st.InsertReachability(m, raw); err != nil {
				t.Fatal(err)
			}
		}
	}
	// Both validators advertise a Fibre endpoint. The "answering now" census
	// is taken over the validators that do, so a fixture without endpoints
	// would be a network with nothing registered.
	for _, a := range []string{"v1", "v2"} {
		if _, err := st.DB().Exec(`INSERT INTO endpoints
			(validator_cons_address, host, first_seen_at, first_seen_height, last_seen_at, last_seen_height)
			VALUES (?, ?, ?, ?, ?, ?)`, a, a+":4433", store.TS(now.Add(-2*time.Hour)), 99, store.TS(now), 99); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := st.StartRun("collector", "test", "t", now); err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(api.New(st, "test"))
	t.Cleanup(ts.Close)
	return ts, st
}

type stabilityValidator struct {
	Address     string `json:"address"`
	Attestation struct {
		AttestedBlobs   int64                    `json:"attested_blobs"`
		UnattestedBlobs int64                    `json:"unattested_blobs"`
		UnknownBlobs    int64                    `json:"unknown_blobs"`
		BlobCoverage    struct{ Num, Den int64 } `json:"blob_coverage"`
	} `json:"attestation"`
	Classes       map[string]int64 `json:"classes"`
	Uptime        rateJSON         `json:"reachability_window"`
	IdentityValid rateJSON         `json:"identity_rate_window"`
	LastDown      *string          `json:"last_unreachable_at"`
}

type rateJSON struct {
	Num   int64    `json:"num"`
	Den   int64    `json:"den"`
	Value *float64 `json:"value"`
}

// stabilityValidators reads every validator's reachability from the list,
// and the rest from its row: the fixture's addresses are not ones the
// validator page takes, and the list leaves out the attestation, the last
// failed handshake, the reading tally and the certificate rate.
func stabilityValidators(t *testing.T, ts *httptest.Server, st *store.Store) map[string]stabilityValidator {
	t.Helper()
	var resp struct {
		Validators []struct {
			Address string   `json:"address"`
			Uptime  rateJSON `json:"reachability_window"`
		} `json:"validators"`
	}
	if code := get(t, ts, "/v1/validators?window=all", &resp); code != 200 {
		t.Fatalf("validators: %d", code)
	}
	var rows struct {
		Validators []stabilityValidator `json:"validators"`
	}
	rowsOf(t, st, "test", "all", time.Time{}, &rows)
	row := map[string]stabilityValidator{}
	for _, v := range rows.Validators {
		row[v.Address] = v
	}
	out := map[string]stabilityValidator{}
	for _, l := range resp.Validators {
		v, ok := row[l.Address]
		if !ok {
			t.Fatalf("%s is listed but has no row", l.Address)
		}
		if v.Uptime.Num != l.Uptime.Num || v.Uptime.Den != l.Uptime.Den {
			t.Fatalf("%s: listed reachability %+v, row %+v", l.Address, l.Uptime, v.Uptime)
		}
		out[l.Address] = v
	}
	return out
}

// The figure a page shows a named operator is counted in blobs, not in
// readings: both blobs of v2 are unattested, and each was read four times on
// the earlier schedule.
func TestUnattestedIsCountedPerObligation(t *testing.T) {
	ts, st := stabilityFixtureStore(t)
	vals := stabilityValidators(t, ts, st)
	v2, ok := vals["v2"]
	if !ok {
		t.Fatal("v2 missing from /v1/validators")
	}
	if v2.Attestation.UnattestedBlobs != 2 {
		t.Errorf("unattested_blobs = %d, want 2: v2 is assigned both blobs and proven to hold neither",
			v2.Attestation.UnattestedBlobs)
	}
	if v2.Classes["UNATTESTED"] != 8 {
		t.Errorf("classes.UNATTESTED = %d, want 8: the class tally counts readings", v2.Classes["UNATTESTED"])
	}
	if v2.Attestation.BlobCoverage.Num != 0 || v2.Attestation.BlobCoverage.Den != 2 {
		t.Errorf("blob_coverage = %d/%d, want 0/2", v2.Attestation.BlobCoverage.Num, v2.Attestation.BlobCoverage.Den)
	}

	v1 := vals["v1"]
	if v1.Attestation.AttestedBlobs != 2 || v1.Attestation.UnattestedBlobs != 0 {
		t.Errorf("v1 attestation by blob = %d attested / %d unattested, want 2/0",
			v1.Attestation.AttestedBlobs, v1.Attestation.UnattestedBlobs)
	}
}

// Reachability is sampled for every registered validator on a fixed heartbeat,
// so it says whether the service was up over the window even for a validator
// the publisher never collected a signature from. That is the one stability
// figure whose coverage does not depend on attestation.
func TestReachabilityHistoryIsPublishedPerValidator(t *testing.T) {
	ts, st := stabilityFixtureStore(t)
	vals := stabilityValidators(t, ts, st)

	v1 := vals["v1"]
	if v1.Uptime.Num != 10 || v1.Uptime.Den != 10 {
		t.Errorf("v1 reachability_window = %d/%d, want 10/10", v1.Uptime.Num, v1.Uptime.Den)
	}
	if v1.LastDown != nil {
		t.Errorf("v1 last_unreachable_at = %v, want null: it never failed a heartbeat", *v1.LastDown)
	}

	v2 := vals["v2"]
	if v2.Uptime.Num != 7 || v2.Uptime.Den != 10 {
		t.Errorf("v2 reachability_window = %d/%d, want 7/10", v2.Uptime.Num, v2.Uptime.Den)
	}
	if v2.LastDown == nil {
		t.Error("v2 last_unreachable_at is null, but three of its heartbeats did not complete TLS")
	}
	// Identity validity is measured over the heartbeats that saw a
	// certificate. v2 presented one on seven, and two of those were no longer
	// endorsed, so it is 5/7 — not 5/10, which would report one outage twice.
	if v2.IdentityValid.Num != 5 || v2.IdentityValid.Den != 7 {
		t.Errorf("v2 identity_rate_window = %d/%d, want 5/7: an endpoint that was down presented no certificate to judge",
			v2.IdentityValid.Num, v2.IdentityValid.Den)
	}

	// Network-wide, the same two questions have different answers: one
	// endpoint is down right now, and 17 of 20 heartbeats completed over the
	// window. A census of the present cannot say how the week went.
	var net struct {
		Window rateJSON `json:"reachability_window"`
		Now    rateJSON `json:"reachability"`
	}
	if code := get(t, ts, "/v1/network?window=all", &net); code != 200 {
		t.Fatalf("network: %d", code)
	}
	if net.Window.Num != 17 || net.Window.Den != 20 {
		t.Errorf("network reachability_window = %d/%d, want 17/20", net.Window.Num, net.Window.Den)
	}
	if net.Now.Num != 1 || net.Now.Den != 2 {
		t.Errorf("network reachability = %d/%d, want 1/2: v2 is unreachable as of its newest evidence",
			net.Now.Num, net.Now.Den)
	}
}

// Latency is the fifth thing an operator needs and the one this site measures
// but never published. Two properties have to hold for it to be worth
// publishing at all: it must describe service rather than failure, and it must
// be comparable between validators of different sizes.

// latencyFixture gives two validators the same service quality at different
// sizes, and a third that is genuinely slow. v1 carries four times v2's rows
// and takes four times as long, so their throughput is identical and their
// durations are not. v3 carries v2's rows at a quarter of the speed.
func latencyFixture(t *testing.T) (*httptest.Server, *store.Store) {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "observer.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })

	now := time.Now().UTC().Truncate(time.Second)
	created, msu := now.Add(-time.Hour), now.Add(time.Hour)
	const hash = "lat1"
	rowsOf := map[string]int{"v1": 400, "v2": 100, "v3": 100, "v4": 4}
	// Durations chosen so v1 and v2 land on the same transfer rate over the
	// download step and v3 on a quarter of it. Half of every probe is the
	// download; the other half is dial, handshake and identity check, the
	// fixed cost that a whole-probe figure would amortise over the bigger
	// shard and not the smaller.
	msOf := map[string][]int64{
		"v1": {400, 400, 400, 1200},
		"v2": {100, 100, 100, 300},
		"v3": {400, 400, 400, 1200},
		// v4 carries shards too small for bandwidth to decide the time: it
		// has service times but no throughput figure.
		"v4": {100, 100, 100, 300},
	}

	pub := scan.Publication{
		SchemaVersion: scan.AttestationSchemaVersion, PromiseHash: hash,
		SettlementHeight: 100, SettlementTime: created, MustServeUntil: msu, RecordedAt: now,
		SettlementTxHash: "tx", Signer: "celestia1pub",
		Promise:                 scan.PromiseFields{ChainID: "t", Height: 99, Commitment: "cc", CreationTimestamp: created, BlobSize: 4096},
		ValidatorSignatureCount: 3,
		Assignment: scan.AssignmentTable{
			ProtocolParams:     scan.ProtocolParamsSnapshot{OriginalRows: 600, TotalRows: 2400},
			ValidatorSetHeight: 99, TotalVotingPower: 30, Sigma: 600, Distinct: 600,
			ValidatorsWithRows: 4, AttestedWithRows: 4, SignatureEntries: 4, SignaturesVerified: 4,
			AttestedVotingPower: 30,
			Validators: []scan.ValidatorAssignment{
				{Address: "v1", VotingPower: 10, RowCount: 400, Rows: []int{0}, Attested: true},
				{Address: "v2", VotingPower: 10, RowCount: 100, Rows: []int{1}, Attested: true},
				{Address: "v3", VotingPower: 10, RowCount: 100, Rows: []int{2}, Attested: true},
				{Address: "v4", VotingPower: 10, RowCount: 4, Rows: []int{3}, Attested: true},
			},
		},
	}
	raw, err := json.Marshal(pub)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.UpsertPublication(pub, raw); err != nil {
		t.Fatal(err)
	}

	points := []string{"w1", "w2", "w3", "w4"}
	for addr, list := range msOf {
		for i, ms := range list {
			at := created.Add(time.Duration(i+1) * time.Minute)
			class, reason := probe.Classify(probe.Evidence{
				Assigned: true, Attested: true, Phase: probe.PhaseInWindow, Outcome: probe.OutcomeServedOK,
			})
			m := probe.Measurement{
				SchemaVersion: probe.AttestationSchemaVersion, Vantage: "test",
				PromiseHash: hash, Commitment: "cc", MustServeUntil: msu, ValidatorSetHeight: 99,
				ValidatorAddress: addr, ValidatorHost: addr + ":443",
				Assigned: true, Attested: true, AssignedRowCount: rowsOf[addr],
				ScheduleLabel: points[i], ScheduledAt: at, StartedAt: at, FinishedAt: at.Add(time.Duration(ms) * time.Millisecond),
				Phase: probe.PhaseInWindow, Outcome: probe.OutcomeServedOK,
				Classification: class, ClassificationReason: reason,
				TotalDurationMS: ms,
			}
			m.Download.OK, m.Download.RowsReturned, m.Download.RowsExpected = true, rowsOf[addr], rowsOf[addr]
			m.Download.CommitmentVerified, m.Download.AssignmentVerified = true, true
			m.Download.DurationMS = ms / 2
			// 32 KiB a row, so v1-v3 carry shards of at least 2 MiB and v4
			// one of 128 KiB. One of v3's records predates the byte count,
			// as every record on a store from before schema 8 does: it must
			// fall out of the throughput sample and not read as zero bytes.
			if !(addr == "v3" && i == 0) {
				m.Download.BytesReturned = int64(rowsOf[addr]) * 32768
			}
			raw, err := json.Marshal(m)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := st.InsertProbe(m, raw); err != nil {
				t.Fatal(err)
			}
		}
		// One failure per validator, far slower than any success. It must not
		// reach the percentiles: how long a failure took is not a service time,
		// and letting it in would make the slowest validator look like the one
		// that failed most.
		at := created.Add(5 * time.Minute)
		class, reason := probe.Classify(probe.Evidence{
			Assigned: true, Attested: true, Phase: probe.PhaseInWindow, Outcome: probe.OutcomeNotFound,
		})
		m := probe.Measurement{
			SchemaVersion: probe.AttestationSchemaVersion, Vantage: "test",
			PromiseHash: hash, Commitment: "cc", MustServeUntil: msu, ValidatorSetHeight: 99,
			ValidatorAddress: addr, ValidatorHost: addr + ":443",
			Assigned: true, Attested: true, AssignedRowCount: rowsOf[addr],
			ScheduleLabel: "w4", ScheduledAt: at, StartedAt: at, FinishedAt: at,
			Phase: probe.PhaseInWindow, Outcome: probe.OutcomeNotFound,
			Classification: class, ClassificationReason: reason,
			TotalDurationMS: 60000,
		}
		raw, err := json.Marshal(m)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := st.InsertProbe(m, raw); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := st.StartRun("collector", "test", "t", now); err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(api.New(st, "test"))
	t.Cleanup(ts.Close)
	return ts, st
}

type latencyValidator struct {
	Address  string `json:"address"`
	P50      *int64 `json:"serve_latency_p50_ms"`
	P95      *int64 `json:"serve_latency_p95_ms"`
	Sample   int64  `json:"serve_latency_sample"`
	BytesSec *int64 `json:"serve_bytes_per_second"`
	TSample  int64  `json:"serve_throughput_sample"`
}

func TestLatencyIsServiceTimeAndSizeNormalised(t *testing.T) {
	_, st := latencyFixture(t)
	// Latency is kept in the rows and published nowhere; throughput is on
	// the validator's page, which the fixture's addresses cannot open.
	var resp struct {
		Validators []latencyValidator `json:"validators"`
	}
	rowsOf(t, st, "test", "all", time.Time{}, &resp)
	by := map[string]latencyValidator{}
	for _, v := range resp.Validators {
		by[v.Address] = v
	}

	for _, addr := range []string{"v1", "v2", "v3", "v4"} {
		if by[addr].Sample != 4 {
			t.Errorf("%s latency sample = %d, want 4: the failed probe is not a service time",
				addr, by[addr].Sample)
		}
		if p95 := by[addr].P95; p95 == nil || *p95 >= 60000 {
			t.Errorf("%s p95 = %v, want the slowest SUCCESS rather than the failure's 60s", addr, p95)
		}
	}

	// The whole case for normalising, in three validators.
	//
	// v1 carries four times v2's rows and takes four times as long: the same
	// service at a different size. v3 carries v2's rows at v1's durations: a
	// quarter of the service. So v1 and v3 have IDENTICAL durations and
	// opposite meanings, and a milliseconds column cannot tell them apart at
	// all — it would put both at the bottom and say nothing true about either.
	v1, v2, v3 := by["v1"], by["v2"], by["v3"]
	for _, v := range []latencyValidator{v1, v2, v3} {
		if v.P50 == nil || v.BytesSec == nil {
			t.Fatalf("%s has no latency figures: %+v", v.Address, v)
		}
	}
	if v4 := by["v4"]; v4.P50 == nil || v4.BytesSec != nil || v4.TSample != 0 {
		t.Errorf("v4 = %+v: a small shard's time is round trips, so it has a latency but no throughput figure", v4)
	}
	if v1.TSample != 4 || v3.TSample != 3 {
		t.Errorf("throughput sample = v1 %d, v3 %d, want 4 and 3: a record without a byte count is outside the sample",
			v1.TSample, v3.TSample)
	}
	if *v1.P50 != *v3.P50 {
		t.Errorf("fixture is not exercising the point: v1 p50 %d and v3 p50 %d should be identical",
			*v1.P50, *v3.P50)
	}
	if *v1.BytesSec != *v2.BytesSec {
		t.Errorf("bytes/s = v1 %d, v2 %d: the same service at different sizes must read the same",
			*v1.BytesSec, *v2.BytesSec)
	}
	if *v3.BytesSec >= *v2.BytesSec {
		t.Errorf("bytes/s = v3 %d, v2 %d: at identical durations to v1, v3 carries a quarter of the bytes and must read worse",
			*v3.BytesSec, *v2.BytesSec)
	}
	// Over the download step, not the whole probe: 400 rows × 512 B in 200 ms.
	if want := int64(400*32768) * 1000 / 200; *v1.BytesSec != want {
		t.Errorf("v1 bytes/s = %d, want %d over the download step alone", *v1.BytesSec, want)
	}

	// The same figures network-wide, which the summary keeps.
	var net struct {
		P50    *int64 `json:"serve_latency_p50_ms"`
		P95    *int64 `json:"serve_latency_p95_ms"`
		Sample int64  `json:"serve_latency_sample"`
	}
	networkOf(t, st, "test", "all", time.Time{}, &net)
	if net.Sample != 16 {
		t.Errorf("network latency sample = %d, want 16 successful probes", net.Sample)
	}
	if net.P95 == nil || *net.P95 >= 60000 {
		t.Errorf("network p95 = %v, want a success rather than the 60s failures", net.P95)
	}
}

// The "answering now" census counts validators that advertise a Fibre
// endpoint, not every validator this observer has ever probed. Mocha jails
// and unbonds routinely, and an operator that left the bonded provider list
// keeps its last handshake in the record forever: counting it meant the
// denominator only ever grew, and the numerator was printed on the overview
// beside registered_endpoints — a count from a different population — so the
// tile could read more validators answering than there are endpoints to
// answer from.
func TestReachabilityCensusFollowsTheRegistry(t *testing.T) {
	ts, st := stabilityFixtureStore(t)
	// Pinned, so each read is computed rather than served from the snapshot
	// the previous one filled; the pin also exercises the as_of branch of the
	// registry query, which has its own bounds.
	read := func() (num, den int64, registered int64) {
		t.Helper()
		var net struct {
			Now        rateJSON `json:"reachability"`
			Registered int64    `json:"registered_endpoints"`
		}
		q := "/v1/network?window=all&as_of=" + time.Now().UTC().Format(time.RFC3339)
		if code := get(t, ts, q, &net); code != 200 {
			t.Fatalf("network: %d", code)
		}
		return net.Now.Num, net.Now.Den, net.Registered
	}
	num, den, registered := read()
	if num != 1 || den != 2 || registered != 2 {
		t.Fatalf("both registered: %d/%d over %d endpoints, want 1/2 over 2", num, den, registered)
	}
	if den > registered {
		t.Fatalf("the census (%d) is larger than the registry it is printed against (%d)", den, registered)
	}

	// v2 leaves the bonded provider list. Its last handshake, a failed one,
	// stays in the record and must stop being counted the moment its
	// endpoint closes: the tile says how many registered endpoints answer,
	// and v2 no longer has one.
	if _, err := st.DB().Exec(`UPDATE endpoints SET closed_at = ?, closed_height = ?, closed_reason = ?
		WHERE validator_cons_address = ?`, store.TS(time.Now().UTC().Add(-time.Second)), 120, "left_bonded_provider_list", "v2"); err != nil {
		t.Fatal(err)
	}
	num, den, registered = read()
	if registered != 1 {
		t.Fatalf("registered_endpoints = %d after v2 left, want 1", registered)
	}
	if num != 1 || den != 1 {
		t.Fatalf("census = %d/%d after v2 left, want 1/1: a closed endpoint is not answering and not counted", num, den)
	}
}
