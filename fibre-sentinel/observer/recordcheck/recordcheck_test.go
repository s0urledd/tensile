package recordcheck

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	assign "github.com/plsgiveup/fibre/fibre-assign"
	"github.com/plsgiveup/fibre/fibre-sentinel/internal/probe"
	"github.com/plsgiveup/fibre/fibre-sentinel/internal/scan"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/export"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
)

// fixture is a store holding one publication and a reading per assigned validator, written the way the collector
// writes them (slim rows), and the record files' lines.
type fixture struct {
	st                   *store.Store
	pubLine, other       []byte
	readings, reachLines [][]byte
}

func newFixture(t *testing.T) fixture {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "observer.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	f := fixture{st: st}
	pub := publication(t, 0x21)
	f.pubLine, _ = json.Marshal(pub)
	if ok, err := st.UpsertPublication(pub, f.pubLine); err != nil || !ok {
		t.Fatalf("publication: %v %v", ok, err)
	}
	f.other, _ = json.Marshal(publication(t, 0x22)) // never stored
	for i, v := range pub.Assignment.Validators {
		rows := v.Rows
		if i == 1 { // part of its rows, in another order
			rows = []int{rows[2], rows[0]}
		}
		m := reading(pub, v, rows)
		l, _ := json.Marshal(m)
		if ok, err := st.InsertProbe(m, l); err != nil || !ok {
			t.Fatalf("reading %d: %v %v", i, ok, err)
		}
		f.readings = append(f.readings, l)
		m.Download = probe.DownloadResult{}
		m.PromiseHash, m.Commitment, m.Assigned = "", "", false
		l, _ = json.Marshal(m)
		if ok, err := st.InsertReachability(m, l); err != nil || !ok {
			t.Fatalf("reachability %d: %v %v", i, ok, err)
		}
		f.reachLines = append(f.reachLines, l)
	}
	return f
}

func publication(t *testing.T, b byte) scan.Publication {
	t.Helper()
	var vals []assign.Validator
	for i := 0; i < 4; i++ {
		var a assign.Address
		a[0], a[19] = byte(i+1), b
		vals = append(vals, assign.Validator{Address: a, VotingPower: int64(1000 * (4 - i))})
	}
	var c [32]byte
	for i := range c {
		c[i] = byte(i*7) ^ b
	}
	sm, err := assign.Assign(c, vals, assign.ParamsV10BlobV0)
	if err != nil {
		t.Fatal(err)
	}
	at := time.Date(2026, 10, 5, 12, 0, 0, 123456789, time.UTC)
	pp := assign.ParamsV10BlobV0
	p := scan.Publication{SchemaVersion: 3, PromiseHash: hex.EncodeToString(bytes.Repeat([]byte{b}, 32)), SettlementHeight: 1000 + int64(b),
		SettlementTime: at.Add(9 * time.Second), Signer: "celestia1xyz",
		Promise:             scan.PromiseFields{ChainID: "test-1", Height: 999, BlobSize: 1 << 20, Commitment: hex.EncodeToString(c[:]), CreationTimestamp: at},
		ParamsAtPublication: scan.ParamsSnapshot{ShardRetention: "4h0m0s", ShardRetentionSeconds: 14400},
		MustServeUntil:      at.Add(4 * time.Hour), MustServeUntilBasis: "shard_retention", RecordedAt: at.Add(10 * time.Second)}
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

func reading(p scan.Publication, v scan.ValidatorAssignment, rows []int) probe.Measurement {
	at := p.MustServeUntil.Add(-10 * time.Minute)
	idx := make([]uint32, len(rows))
	for i, r := range rows {
		idx[i] = uint32(r)
	}
	m := probe.Measurement{SchemaVersion: 2, Vantage: "ut-1", PromiseHash: p.PromiseHash, Commitment: p.Promise.Commitment, MustServeUntil: p.MustServeUntil,
		ValidatorSetHeight: 999, ValidatorAddress: v.Address, ValidatorHost: v.Host, Assigned: true, Attested: true, AssignedRowCount: v.RowCount,
		ScheduleLabel: "full", ScheduledAt: at, StartedAt: at.Add(time.Second), FinishedAt: at.Add(2 * time.Second), Phase: probe.PhaseInWindow,
		Outcome: probe.OutcomeServedOK, Classification: probe.ClassHealthy, TotalDurationMS: 1000}
	m.Download = probe.DownloadResult{Attempted: true, OK: true, RowsReturned: len(rows), RowsExpected: v.RowCount, RowIndices: idx,
		CommitmentVerified: true, AssignmentVerified: true}
	return m
}

func file(lines ...[]byte) []byte {
	var b []byte
	for _, l := range lines {
		b = append(append(b, l...), '\n')
	}
	return b
}

func digest(b []byte) string {
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}

// Every line the store holds comes back byte for byte, and the file rebuilt from them has the file's digest.
func TestLinesComeBackByteForByte(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	var raw []byte
	if err := f.st.DB().QueryRow(`SELECT raw_json FROM probes LIMIT 1`).Scan(&raw); err != nil || raw[0] == '{' {
		t.Fatalf("the fixture's readings are not slim rows: %v", err)
	}
	for name, lines := range map[string][][]byte{
		"publications.jsonl": {f.pubLine},
		"measurements.jsonl": f.readings,
		"reachability.jsonl": f.reachLines,
	} {
		src := file(lines...)
		r, err := CheckLines(ctx, f.st, name, bytes.NewReader(src), 1)
		if err != nil {
			t.Fatal(err)
		}
		if !r.Reproducible || r.Identical != int64(len(lines)) || r.RebuiltSHA != digest(src) {
			t.Fatalf("%s: %+v", name, r)
		}
		if name != "reachability.jsonl" && r.SlimRows != r.Identical {
			t.Fatalf("%s: %d of %d lines came from slim rows", name, r.SlimRows, r.Identical)
		}
	}
}

// A line the store does not give back is counted by why, never as identical, and the rebuilt file is not the file.
func TestLinesNotBackAreCountedByWhy(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	changed := bytes.Replace(f.readings[1], []byte(`"full"`), []byte(`"fuII"`), 1)
	src := file(f.readings[0], changed, f.readings[0], f.readings[2], []byte(`{"vantage":`))
	r, err := CheckLines(ctx, f.st, "measurements.jsonl", bytes.NewReader(src), 1)
	if err != nil {
		t.Fatal(err)
	}
	if r.Identical != 2 || r.Different != 1 || r.Repeated != 1 || r.Unreadable != 1 || r.Reproducible {
		t.Fatalf("%+v", r)
	}
	// the changed line's key is the one listed
	var m probe.Measurement
	_ = json.Unmarshal(f.readings[1], &m)
	if !strings.Contains(strings.Join(r.Examples, "\n"), "different: "+m.DedupeKey()) {
		t.Fatalf("examples: %v", r.Examples)
	}

	r, err = CheckLines(ctx, f.st, "publications.jsonl", bytes.NewReader(file(f.pubLine, f.other)), 1)
	if err != nil {
		t.Fatal(err)
	}
	if r.Identical != 1 || r.Missing != 1 || r.Reproducible {
		t.Fatalf("%+v", r)
	}

	// -from-line skips the lines before it
	r, err = CheckLines(ctx, f.st, "publications.jsonl", bytes.NewReader(file(f.other, f.pubLine)), 2)
	if err != nil || r.Lines != 1 || !r.Reproducible {
		t.Fatalf("%+v %v", r, err)
	}
}

// A day's export is checked as a whole: the tarball against the index and the sidecar, the members against the
// manifest, and the checked files against the store.
func TestDay(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	members := map[string][]byte{
		"publications.jsonl": file(f.pubLine),
		"measurements.jsonl": file(f.readings...),
		"reachability.jsonl": file(f.reachLines...),
		"registry.jsonl":     file([]byte(`{"x":1}`)),
	}
	order := []string{"publications.jsonl", "measurements.jsonl", "reachability.jsonl", "registry.jsonl"}
	build := func(dir string, tamper func(map[string][]byte)) export.Entry {
		man := export.Manifest{Vantage: "ut-1", Day: "2026-10-05"}
		for _, n := range order {
			b := members[n]
			man.Files = append(man.Files, export.Member{Name: n, Lines: int64(bytes.Count(b, []byte{'\n'})), Bytes: int64(len(b)), SHA256: digest(b)})
		}
		in := map[string][]byte{}
		for n, b := range members {
			in[n] = b
		}
		if tamper != nil {
			tamper(in)
		}
		mj, _ := json.Marshal(man)
		var buf bytes.Buffer
		gz := gzip.NewWriter(&buf)
		tw := tar.NewWriter(gz)
		for _, n := range append([]string{"manifest.json"}, order...) {
			b := mj
			if n != "manifest.json" {
				b = in[n]
			}
			_ = tw.WriteHeader(&tar.Header{Name: n, Mode: 0o644, Size: int64(len(b))})
			_, _ = tw.Write(b)
		}
		_ = tw.Close()
		_ = gz.Close()
		name := "tensile-ut-1-2026-10-05.tar.gz"
		_ = os.WriteFile(filepath.Join(dir, name), buf.Bytes(), 0o644)
		_ = os.WriteFile(filepath.Join(dir, name+".sha256"), []byte(digest(buf.Bytes())+"  "+name+"\n"), 0o644)
		return export.Entry{Name: name, Bytes: int64(buf.Len()), SHA256: digest(buf.Bytes()), Manifest: man}
	}

	dir := t.TempDir()
	r, err := CheckDay(ctx, f.st, dir, build(dir, nil))
	if err != nil {
		t.Fatal(err)
	}
	if !r.ExportIntact || !r.Reproducible || len(r.Files) != 3 || len(r.NotChecked) != 1 {
		t.Fatalf("%+v", r)
	}

	// a member that is not what the manifest says: the export is not intact
	dir = t.TempDir()
	r, err = CheckDay(ctx, f.st, dir, build(dir, func(m map[string][]byte) { m["registry.jsonl"] = file([]byte(`{"x":2}`)) }))
	if err != nil {
		t.Fatal(err)
	}
	if r.ExportIntact || r.Reproducible || !strings.Contains(strings.Join(r.ExportErrors, "\n"), "registry.jsonl") {
		t.Fatalf("%+v", r)
	}

	// a tarball that is not the one the index names
	dir = t.TempDir()
	e := build(dir, nil)
	e.SHA256 = digest(nil)
	if r, err = CheckDay(ctx, f.st, dir, e); err != nil || r.ExportIntact || r.Reproducible {
		t.Fatalf("%+v %v", r, err)
	}
}
