package slim

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	assign "github.com/plsgiveup/fibre/fibre-assign"
	"github.com/plsgiveup/fibre/fibre-sentinel/internal/probe"
	"github.com/plsgiveup/fibre/fibre-sentinel/internal/scan"
)

// A publication over a small validator set, its assignment as fibre-assign computes it, the way the scanner writes it.
func testPublication(t *testing.T, hashByte byte, vals []assign.Validator) scan.Publication {
	t.Helper()
	var c [32]byte
	for i := range c {
		c[i] = byte(i * 7)
	}
	sm, err := assign.Assign(c, vals, assign.ParamsV10BlobV0)
	if err != nil {
		t.Fatal(err)
	}
	created := time.Date(2026, 10, 4, 12, 0, 0, 123456789, time.UTC)
	p := scan.Publication{
		SchemaVersion: 3, PromiseHash: hex.EncodeToString(bytes.Repeat([]byte{hashByte}, 32)),
		SettlementHeight: 1000, SettlementTime: created.Add(9 * time.Second), SettlementTxHash: hex.EncodeToString(bytes.Repeat([]byte{0xab}, 32)),
		Signer: "celestia1xyz", ValidatorSignatureCount: len(vals),
		Promise: scan.PromiseFields{ChainID: "test-1", Height: 999, Namespace: "00" + hex.EncodeToString([]byte("tensile-0000000000000000000")),
			NamespaceVersion: 0, NamespaceID: hex.EncodeToString([]byte("tensile-0000000000000000000")), BlobSize: 1 << 20,
			Commitment: hex.EncodeToString(c[:]), CreationTimestamp: created, SignerPublicKey: hex.EncodeToString(bytes.Repeat([]byte{2}, 33)),
			Signature: hex.EncodeToString(bytes.Repeat([]byte{5}, 64))},
		ParamsAtPublication: scan.ParamsSnapshot{PaymentPromiseTimeout: "1h0m0s", ShardRetention: "4h0m0s", PaymentPromiseTimeoutSeconds: 3600, ShardRetentionSeconds: 14400},
		MustServeUntil:      created.Add(4 * time.Hour), MustServeUntilBasis: "shard_retention",
		RecordedAt: created.Add(10 * time.Second),
	}
	pp := assign.ParamsV10BlobV0
	p.Assignment.ProtocolParams = scan.ProtocolParamsSnapshot{OriginalRows: pp.OriginalRows, TotalRows: pp.TotalRows, MinRowsPerValidator: pp.MinRowsPerValidator,
		LivenessThresholdNum: pp.LivenessThreshold.Numerator, LivenessThresholdDen: pp.LivenessThreshold.Denominator, Fingerprint: pp.Fingerprint(), PinnedCelestiaApp: assign.PinnedCelestiaAppCommit}
	p.Assignment.ValidatorSetHeight = 999
	seen := map[int]int{}
	for i, v := range vals {
		rows := sm[v.Address]
		va := scan.ValidatorAssignment{Address: v.Address.String(), VotingPower: v.VotingPower, RowCount: len(rows), Rows: rows, Attested: i%3 != 2,
			Host: fmt.Sprintf("10.0.0.%d:7980", i+1), HostSource: scan.HostFromEvent}
		p.Assignment.Validators = append(p.Assignment.Validators, va)
		p.Assignment.TotalVotingPower += v.VotingPower
		p.Assignment.Sigma += len(rows)
		for _, r := range rows {
			seen[r]++
		}
		if len(rows) > 0 {
			p.Assignment.ValidatorsWithRows++
			if va.Attested {
				p.Assignment.AttestedWithRows++
				p.Assignment.AttestedVotingPower += v.VotingPower
			}
		}
	}
	p.Assignment.Distinct = len(seen)
	for _, n := range seen {
		if n > 1 {
			p.Assignment.WrapOverlaps++
		}
	}
	return p
}

func testValidators(n int) []assign.Validator {
	var vals []assign.Validator
	for i := 0; i < n; i++ {
		var a assign.Address
		a[0], a[19] = byte(i+1), byte(i*13)
		vals = append(vals, assign.Validator{Address: a, VotingPower: int64(1000 * (n - i))})
	}
	return vals
}

func testReading(p scan.Publication, v scan.ValidatorAssignment, attempt int) probe.Measurement {
	sched := p.MustServeUntil.Add(-10 * time.Minute)
	m := probe.Measurement{SchemaVersion: 2, Vantage: "ut-1", PromiseHash: p.PromiseHash, Commitment: p.Promise.Commitment, MustServeUntil: p.MustServeUntil,
		ValidatorSetHeight: p.Assignment.ValidatorSetHeight, ValidatorAddress: v.Address, ValidatorHost: v.Host, Assigned: v.RowCount > 0, HostSource: "bonded",
		HostAtSettlement: v.Host, Attested: v.Attested, AssignedRowCount: v.RowCount, ScheduleLabel: "full", ScheduledAt: sched,
		StartedAt: sched.Add(1234567 * time.Microsecond), FinishedAt: sched.Add(3 * time.Second), LatenessMS: 1234, Attempt: attempt,
		Outcome: probe.OutcomeServedOK, Classification: probe.ClassHealthy, ClassificationReason: "served", TotalDurationMS: 1766}
	m.TLS = probe.TLSResult{Attempted: true, OK: true, DurationMS: 12, Version: "1.3", PeerCertSHA256: hex.EncodeToString(bytes.Repeat([]byte{9}, 32))}
	m.Download = probe.DownloadResult{Attempted: true, OK: true, DurationMS: 800, RowsReturned: v.RowCount, RowsExpected: v.RowCount,
		RowsSHA256: hex.EncodeToString(bytes.Repeat([]byte{7}, 32)), CommitmentVerified: true, AssignmentVerified: true}
	for _, r := range v.Rows {
		m.Download.RowIndices = append(m.Download.RowIndices, uint32(r))
	}
	return m
}

func line(t *testing.T, x any) []byte {
	t.Helper()
	b, err := json.Marshal(x)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// Every case is written slim, the tables are kept and loaded into a fresh Tables as another process would, and each
// record comes back byte for byte.
func TestRecordsComeBackByteForByte(t *testing.T) {
	vals := testValidators(8)
	pub := testPublication(t, 0x11, vals)
	// another promise over the same blob, settled later over a set without its largest validator: its rows differ
	other := testPublication(t, 0x22, vals[1:])
	other.SettlementHeight += 10
	v := pub.Assignment.Validators[3]
	var otherRows []uint32
	for _, ov := range other.Assignment.Validators {
		if ov.Address == v.Address {
			for _, r := range ov.Rows {
				otherRows = append(otherRows, uint32(r))
			}
		}
	}
	served := testReading(pub, v, 0)
	if fmt.Sprint(otherRows) == fmt.Sprint(served.Download.RowIndices) {
		t.Fatal("the other promise assigns the validator the same rows; the shadowing case would test nothing")
	}
	mut := func(f func(*probe.Measurement)) probe.Measurement { m := testReading(pub, v, 0); f(&m); return m }
	cases := map[string]probe.Measurement{
		"served": served,
		"retried": mut(func(m *probe.Measurement) {
			m.Attempt = 2
			m.Retry = &probe.RetryInfo{Attempts: 2, DelayMS: 90000, FirstOutcome: "TCP_TIMEOUT"}
		}),
		"reversed": mut(func(m *probe.Measurement) {
			r := m.Download.RowIndices
			for i, j := 0, len(r)-1; i < j; i, j = i+1, j-1 {
				r[i], r[j] = r[j], r[i]
			}
		}),
		"half": mut(func(m *probe.Measurement) {
			m.Download.RowIndices = m.Download.RowIndices[:len(m.Download.RowIndices)/2]
			m.Download.RowsReturned /= 2
		}),
		"shadowed": mut(func(m *probe.Measurement) {
			m.Download.RowIndices = otherRows
			m.Download.RowsReturned = len(otherRows)
			m.Download.ShadowedBy = other.PromiseHash
		}),
		"foreign": mut(func(m *probe.Measurement) {
			m.Download.RowIndices = []uint32{16383, 7, 9001}
			m.Download.RowsReturned = 3
		}),
		"host moved": mut(func(m *probe.Measurement) { m.HostAtSettlement = "203.0.113.9:7980" }),
		"unreachable": mut(func(m *probe.Measurement) {
			m.Download = probe.DownloadResult{}
			m.Outcome, m.Classification = probe.Outcome("TCP_TIMEOUT"), probe.ClassUnreachable
		}),
	}
	w := NewTables()
	pubLine, otherLine := line(t, pub), line(t, other)
	pubBody, pubInfo, err := w.EncodePublication(pubLine)
	if err != nil {
		t.Fatal(err)
	}
	otherBody, otherInfo, err := w.EncodePublication(otherLine)
	if err != nil {
		t.Fatal(err)
	}
	lookupW := func(h string) *Pub {
		if h == otherInfo.Hash {
			return otherInfo
		}
		return nil
	}
	bodies := map[string][]byte{}
	for name, m := range cases {
		b, err := w.EncodeMeasurement(line(t, m), pubInfo, lookupW)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		bodies[name] = b
	}
	if len(pubBody) > len(pubLine)/20 {
		t.Errorf("the slim publication is %d bytes of a %d-byte line", len(pubBody), len(pubLine))
	}

	// another process: the tables as the store keeps them
	r := NewTables()
	for _, e := range w.Pending() {
		if err := r.Add(e); err != nil {
			t.Fatal(err)
		}
	}
	got, rPub, err := r.DecodePublication(pubBody)
	if err != nil || !bytes.Equal(got, pubLine) {
		t.Fatalf("publication: %v\n got %.200s\nwant %.200s", err, got, pubLine)
	}
	gotOther, rOther, err := r.DecodePublication(otherBody)
	if err != nil || !bytes.Equal(gotOther, otherLine) {
		t.Fatalf("other publication: %v", err)
	}
	lookupR := func(h string) *Pub {
		if h == rOther.Hash {
			return rOther
		}
		return nil
	}
	for name, m := range cases {
		want := line(t, m)
		got, err := r.DecodeMeasurement(bodies[name], rPub, lookupR)
		if err != nil || !bytes.Equal(got, want) {
			t.Errorf("%s: %v\n got %.300s\nwant %.300s", name, err, got, want)
		}
	}
	if len(bodies["served"]) > 200 {
		t.Errorf("a served reading is %d bytes slim", len(bodies["served"]))
	}
	if len(bodies["shadowed"]) > len(bodies["served"])+80 {
		t.Errorf("a shadowed reading costs %d bytes over a served one: its rows should be a reference", len(bodies["shadowed"])-len(bodies["served"]))
	}
}

// A reader that has not loaded an entry the writer added since says so, and decodes once it has loaded it.
func TestAnEntryNotLoadedYetIsAskedFor(t *testing.T) {
	pub := testPublication(t, 0x33, testValidators(5))
	w := NewTables()
	r := NewTables()
	body, _, err := w.EncodePublication(line(t, pub))
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := r.DecodePublication(body); !errors.Is(err, ErrUnknownEntry) {
		t.Fatalf("decoding before loading: %v, want ErrUnknownEntry", err)
	}
	for _, e := range w.Pending() {
		if err := r.Add(e); err != nil {
			t.Fatal(err)
		}
	}
	if _, _, err := r.DecodePublication(body); err != nil {
		t.Fatal(err)
	}
}

// Entries handed out for a record that was never kept are forgotten, so the next record adds them again under the
// same numbers.
func TestRollbackForgetsWhatWasNotKept(t *testing.T) {
	pub := testPublication(t, 0x44, testValidators(4))
	w := NewTables()
	kept := w.Stored()
	if _, _, err := w.EncodePublication(line(t, pub)); err != nil {
		t.Fatal(err)
	}
	first := w.Pending()
	w.Rollback(kept)
	if _, _, err := w.EncodePublication(line(t, pub)); err != nil {
		t.Fatal(err)
	}
	again := w.Pending()
	if len(first) == 0 || len(first) != len(again) {
		t.Fatalf("%d entries, then %d after the rollback", len(first), len(again))
	}
	for i := range first {
		if first[i].Kind != again[i].Kind || first[i].ID != again[i].ID || !bytes.Equal(first[i].Body, again[i].Body) {
			t.Fatalf("entry %d differs after the rollback", i)
		}
	}
}

// A publication in its line form (one an earlier build stored) gives its readings the same as its slim form.
func TestPubFromLineMatchesTheEncodedOne(t *testing.T) {
	pub := testPublication(t, 0x55, testValidators(6))
	w := NewTables()
	l := line(t, pub)
	_, enc, err := w.EncodePublication(l)
	if err != nil {
		t.Fatal(err)
	}
	fromLine, err := PubFromLine(l)
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range pub.Assignment.Validators {
		a, okA := enc.Rows(v.Address)
		b, okB := fromLine.Rows(v.Address)
		if !okA || !okB || fmt.Sprint(a) != fmt.Sprint(b) || fmt.Sprint(a) != fmt.Sprint(v.Rows) {
			t.Fatalf("%s: rows differ", v.Address)
		}
	}
	m := testReading(pub, pub.Assignment.Validators[0], 0)
	b, err := w.EncodeMeasurement(line(t, m), fromLine, nil)
	if err != nil {
		t.Fatal(err)
	}
	got, err := w.DecodeMeasurement(b, enc, nil)
	if err != nil || !bytes.Equal(got, line(t, m)) {
		t.Fatalf("a reading encoded against the line form and decoded against the slim one: %v", err)
	}
}
