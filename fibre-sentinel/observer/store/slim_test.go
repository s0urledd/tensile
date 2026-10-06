package store_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	assign "github.com/plsgiveup/fibre/fibre-assign"
	"github.com/plsgiveup/fibre/fibre-sentinel/internal/probe"
	"github.com/plsgiveup/fibre/fibre-sentinel/internal/scan"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
)

func slimPublication(t *testing.T, hashByte byte, n int) scan.Publication {
	t.Helper()
	var vals []assign.Validator
	for i := 0; i < n; i++ {
		var a assign.Address
		a[0], a[19] = byte(i+1), byte(i*13)
		vals = append(vals, assign.Validator{Address: a, VotingPower: int64(1000 * (n - i))})
	}
	var c [32]byte
	for i := range c {
		c[i] = byte(i*7) ^ hashByte
	}
	sm, err := assign.Assign(c, vals, assign.ParamsV10BlobV0)
	if err != nil {
		t.Fatal(err)
	}
	created := time.Date(2026, 10, 4, 12, 0, 0, 123456789, time.UTC)
	pp := assign.ParamsV10BlobV0
	p := scan.Publication{SchemaVersion: 3, PromiseHash: hex.EncodeToString(bytes.Repeat([]byte{hashByte}, 32)), SettlementHeight: 1000 + int64(hashByte),
		SettlementTime: created.Add(9 * time.Second), SettlementTxHash: hex.EncodeToString(bytes.Repeat([]byte{hashByte, 0xab}, 16)), Signer: "celestia1xyz",
		Promise: scan.PromiseFields{ChainID: "test-1", Height: 999, Namespace: "00" + hex.EncodeToString([]byte("tensile-0000000000000000000")),
			NamespaceID: hex.EncodeToString([]byte("tensile-0000000000000000000")), BlobSize: 1 << 20, Commitment: hex.EncodeToString(c[:]),
			CreationTimestamp: created, SignerPublicKey: hex.EncodeToString(bytes.Repeat([]byte{2}, 33)), Signature: hex.EncodeToString(bytes.Repeat([]byte{5}, 64))},
		ParamsAtPublication: scan.ParamsSnapshot{PaymentPromiseTimeout: "1h0m0s", ShardRetention: "4h0m0s", PaymentPromiseTimeoutSeconds: 3600, ShardRetentionSeconds: 14400},
		MustServeUntil:      created.Add(4 * time.Hour), MustServeUntilBasis: "shard_retention", RecordedAt: created.Add(10 * time.Second)}
	p.Assignment.ProtocolParams = scan.ProtocolParamsSnapshot{OriginalRows: pp.OriginalRows, TotalRows: pp.TotalRows, MinRowsPerValidator: pp.MinRowsPerValidator,
		LivenessThresholdNum: pp.LivenessThreshold.Numerator, LivenessThresholdDen: pp.LivenessThreshold.Denominator, Fingerprint: pp.Fingerprint(), PinnedCelestiaApp: assign.PinnedCelestiaAppCommit}
	p.Assignment.ValidatorSetHeight = 999
	for _, v := range vals {
		rows := sm[v.Address]
		p.Assignment.Validators = append(p.Assignment.Validators, scan.ValidatorAssignment{Address: v.Address.String(), VotingPower: v.VotingPower,
			RowCount: len(rows), Rows: rows, Attested: true, Host: "10.0.0.1:7980", HostSource: scan.HostFromEvent})
		p.Assignment.TotalVotingPower += v.VotingPower
		p.Assignment.Sigma += len(rows)
	}
	return p
}

func slimReading(p scan.Publication, v scan.ValidatorAssignment, rows []uint32) probe.Measurement {
	sched := p.MustServeUntil.Add(-10 * time.Minute)
	m := probe.Measurement{SchemaVersion: 2, Vantage: "ut-1", PromiseHash: p.PromiseHash, Commitment: p.Promise.Commitment, MustServeUntil: p.MustServeUntil,
		ValidatorSetHeight: 999, ValidatorAddress: v.Address, ValidatorHost: v.Host, Assigned: true, HostAtSettlement: v.Host, Attested: true,
		AssignedRowCount: v.RowCount, ScheduleLabel: "full", ScheduledAt: sched, StartedAt: sched.Add(time.Second), FinishedAt: sched.Add(2 * time.Second),
		LatenessMS: 1000, Phase: probe.PhaseInWindow, Outcome: probe.OutcomeServedOK, Classification: probe.ClassHealthy, TotalDurationMS: 1000}
	m.Download = probe.DownloadResult{Attempted: true, OK: true, RowsReturned: len(rows), RowsExpected: v.RowCount, RowIndices: rows,
		RowsSHA256: hex.EncodeToString(bytes.Repeat([]byte{7}, 32)), CommitmentVerified: true, AssignmentVerified: true}
	return m
}

func u32(xs []int) []uint32 {
	out := make([]uint32, len(xs))
	for i, x := range xs {
		out[i] = uint32(x)
	}
	return out
}

// A store writes the slim record and marks the lists an assignment gives, reads every row back to its line byte for
// byte (in this process and in another one), resolves the marks, and counts each reading's distinct verified rows as
// the rollup's json_each did.
func TestSlimRowsReadBackAsWritten(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "observer.db")
	st, err := store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	pub := slimPublication(t, 0x11, 7)
	pubLine, _ := json.Marshal(pub)
	if ok, err := st.UpsertPublication(pub, pubLine); err != nil || !ok {
		t.Fatalf("publication: %v %v", ok, err)
	}
	var lines [][]byte
	var keys []string
	var union = map[uint32]bool{}
	for i, v := range pub.Assignment.Validators {
		rows := u32(v.Rows)
		if i == 2 { // part of its rows, in another order
			rows = []uint32{rows[3], rows[1], rows[0]}
		}
		if i == 4 { // rows of no promise
			rows = []uint32{16383, 5, 9001}
		}
		m := slimReading(pub, v, rows)
		for _, r := range rows {
			union[r] = true
		}
		l, _ := json.Marshal(m)
		if ok, err := st.InsertProbe(m, l); err != nil || !ok {
			t.Fatalf("probe %d: %v %v", i, ok, err)
		}
		lines, keys = append(lines, l), append(keys, m.DedupeKey())
	}

	check := func(st *store.Store, who string) {
		var raw []byte
		if err := st.DB().QueryRow(`SELECT raw_json FROM publications WHERE promise_hash = ?`, pub.PromiseHash).Scan(&raw); err != nil {
			t.Fatal(err)
		}
		if raw[0] == '{' || len(raw) > len(pubLine)/10 {
			t.Fatalf("%s: the publication row keeps %d bytes, the line is %d", who, len(raw), len(pubLine))
		}
		got, err := st.Record(ctx, st.DB(), raw)
		if err != nil || !bytes.Equal(got, pubLine) {
			t.Fatalf("%s: publication back: %v", who, err)
		}
		for i, k := range keys {
			var raw []byte
			var idx sql.NullString
			if err := st.DB().QueryRow(`SELECT raw_json, row_indices FROM probes WHERE dedupe_key = ?`, k).Scan(&raw, &idx); err != nil {
				t.Fatal(err)
			}
			got, err := st.ProbeRecord(ctx, st.DB(), pub.PromiseHash, raw)
			if err != nil || !bytes.Equal(got, lines[i]) {
				t.Fatalf("%s: probe %d back: %v\n got %.300s\nwant %.300s", who, i, err, got, lines[i])
			}
			wantMark := i != 2 && i != 4
			if (idx.String == store.RowsAssigned) != wantMark {
				t.Fatalf("%s: probe %d row_indices %.40q", who, i, idx.String)
			}
			v := pub.Assignment.Validators[i]
			list, err := st.RowIndices(ctx, st.DB(), pub.PromiseHash, v.Address, idx.String)
			if err != nil {
				t.Fatal(err)
			}
			var m probe.Measurement
			_ = json.Unmarshal(lines[i], &m)
			want, _ := json.Marshal(m.Download.RowIndices)
			if list != string(want) {
				t.Fatalf("%s: probe %d rows resolve to %.60s, want %.60s", who, i, list, want)
			}
		}
		var marked int
		if err := st.DB().QueryRow(`SELECT COUNT(*) FROM assignments WHERE promise_hash = ? AND rows_json = ?`, pub.PromiseHash, store.RowsAssigned).Scan(&marked); err != nil {
			t.Fatal(err)
		}
		if marked != len(pub.Assignment.Validators) {
			t.Fatalf("%s: %d of %d assignment rows marked", who, marked, len(pub.Assignment.Validators))
		}
		var exact, orig, total int
		if err := st.DB().QueryRow(`SELECT exact FROM reading_rows WHERE promise_hash = ?`, pub.PromiseHash).Scan(&exact); err != nil {
			t.Fatal(err)
		}
		if exact != len(union) {
			t.Fatalf("%s: reading_rows.exact %d, the lists hold %d distinct rows", who, exact, len(union))
		}
		if err := st.DB().QueryRow(`SELECT original_rows, total_rows FROM publications WHERE promise_hash = ?`, pub.PromiseHash).Scan(&orig, &total); err != nil {
			t.Fatal(err)
		}
		if orig != assign.ParamsV10BlobV0.OriginalRows || total != assign.ParamsV10BlobV0.TotalRows {
			t.Fatalf("%s: original_rows %d, total_rows %d", who, orig, total)
		}
	}
	check(st, "the writer")
	st.Close()
	// another process: the tables come from the store
	st2, err := store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer st2.Close()
	check(st2, "a reader")
}

// A row an earlier build wrote keeps its line, which is read as it is, and its readings are written against it.
func TestALineRowIsReadAsItIs(t *testing.T) {
	ctx := context.Background()
	st := open(t)
	pub := slimPublication(t, 0x22, 5)
	pubLine, _ := json.Marshal(pub)
	if _, err := st.UpsertPublication(pub, pubLine); err != nil {
		t.Fatal(err)
	}
	// as an earlier build stored it
	if _, err := st.DB().Exec(`UPDATE publications SET raw_json = ? WHERE promise_hash = ?`, string(pubLine), pub.PromiseHash); err != nil {
		t.Fatal(err)
	}
	var raw []byte
	if err := st.DB().QueryRow(`SELECT raw_json FROM publications WHERE promise_hash = ?`, pub.PromiseHash).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	got, err := st.Record(ctx, st.DB(), raw)
	if err != nil || !bytes.Equal(got, pubLine) {
		t.Fatalf("a line row: %v", err)
	}
	// the reading is written against the line form
	v := pub.Assignment.Validators[0]
	m := slimReading(pub, v, u32(v.Rows))
	l, _ := json.Marshal(m)
	if _, err := st.InsertProbe(m, l); err != nil {
		t.Fatal(err)
	}
	var praw []byte
	if err := st.DB().QueryRow(`SELECT raw_json FROM probes WHERE dedupe_key = ?`, m.DedupeKey()).Scan(&praw); err != nil {
		t.Fatal(err)
	}
	back, err := st.ProbeRecord(ctx, st.DB(), pub.PromiseHash, praw)
	if err != nil || !bytes.Equal(back, l) {
		t.Fatalf("probe back: %v", err)
	}
}
