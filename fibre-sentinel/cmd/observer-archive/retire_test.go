package main

import (
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	assign "github.com/plsgiveup/fibre/fibre-assign"
	"github.com/plsgiveup/fibre/fibre-sentinel/internal/probe"
	"github.com/plsgiveup/fibre/fibre-sentinel/internal/record"
	"github.com/plsgiveup/fibre/fibre-sentinel/internal/scan"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/export"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/recordcheck"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
)

// rotates reports whether record.Archive runs here: it needs flock.
func rotates(t *testing.T) bool {
	t.Helper()
	p := filepath.Join(t.TempDir(), "probe.jsonl")
	if err := os.WriteFile(p, []byte(`{"t":"2026-01-01T00:00:00Z"}`+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := record.Archive(p, record.Options{Cutoff: time.Now(), TimeField: "t", Limit: -1, DryRun: true})
	return !errors.Is(err, record.ErrUnsupported)
}

const vantageFile = "vantages/de-1/reachability.jsonl"

// day0 is the first of the fixture's four days. The first three are
// archived; the fourth stays live.
var day0 = time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)

// retireData is a data directory as the observer leaves it after four
// days, with the store the collector filled from it: a publication a day,
// its payment, a reading and a heartbeat per assigned validator, the same
// heartbeats from a second vantage (de-1), and a sampling decision a day,
// which the store does not keep line by line. Every line is in the store
// but one reading on the second day, which is only in the file. Each day
// is exported, and the remote backup has read back the first two days'
// tarballs.
type retireData struct {
	dir, db string
	files   map[string][]byte // every byte written to each record file
	exports []string          // the tarballs' names, day 0 first
}

func newRetireData(t *testing.T) *retireData {
	t.Helper()
	d := &retireData{dir: t.TempDir(), db: filepath.Join(t.TempDir(), "observer.db"), files: map[string][]byte{}}
	st, err := store.Open(d.db)
	if err != nil {
		t.Fatal(err)
	}
	add := func(name string, l []byte) { d.files[name] = append(append(d.files[name], l...), '\n') }
	must := func(what string, ok bool, err error) {
		t.Helper()
		if err != nil || !ok {
			t.Fatalf("%s: %v %v", what, ok, err)
		}
	}
	for day := 0; day < 4; day++ {
		at := day0.AddDate(0, 0, day).Add(12*time.Hour + 123456789)
		pub := publicationAt(t, byte(0x21+day), at)
		l, _ := json.Marshal(pub)
		ok, err := st.UpsertPublication(pub, l)
		must("publication", ok, err)
		add("publications.jsonl", l)
		pay := scan.Payment{SchemaVersion: 1, DedupeKey: fmt.Sprintf("settlement-%d", day), Kind: "settlement", Height: pub.SettlementHeight,
			Time: pub.SettlementTime, Publisher: "celestia1pub", PromiseHash: pub.PromiseHash, Denom: "utia", AmountUtia: 1000, RecordedAt: pub.RecordedAt}
		l, _ = json.Marshal(pay)
		ok, err = st.UpsertPayment(pay, l)
		must("payment", ok, err)
		add("payments.jsonl", l)
		add(probe.SampledOutFile, []byte(fmt.Sprintf(`{"kind":"sampled_out","vantage":"ut-1","promise_hash":%q,"decided_at":%q}`, pub.PromiseHash, at.Format(time.RFC3339Nano))))
		for _, v := range pub.Assignment.Validators {
			m := readingOf(pub, v)
			l, _ := json.Marshal(m)
			ok, err := st.InsertProbe(m, l)
			must("reading", ok, err)
			add("measurements.jsonl", l)
			m.Download = probe.DownloadResult{}
			m.PromiseHash, m.Commitment, m.Assigned = "", "", false
			l, _ = json.Marshal(m)
			ok, err = st.InsertReachability(m, l)
			must("heartbeat", ok, err)
			add("reachability.jsonl", l)
			m.Vantage = "de-1"
			l, _ = json.Marshal(m)
			ok, err = st.InsertReachability(m, l)
			must("de-1 heartbeat", ok, err)
			add(vantageFile, l)
		}
		if day == 1 {
			m := readingOf(pub, pub.Assignment.Validators[0])
			m.ScheduledAt = m.ScheduledAt.Add(time.Second)
			l, _ := json.Marshal(m)
			add("measurements.jsonl", l)
		}
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	for name, b := range d.files {
		p := filePath(d.dir, name)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, b, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	b := &export.Builder{DataDir: d.dir, Dir: d.expDir(), Vantage: "ut-1", Build: "t", Hour: 3}
	for day := 1; day <= 4; day++ {
		built, err := b.Run(day0.AddDate(0, 0, day).Add(4 * time.Hour))
		if err != nil || len(built) != 1 {
			t.Fatalf("export of day %d: %v %v", day-1, built, err)
		}
		d.exports = append(d.exports, built[0])
	}
	d.remoteOK(t, 0)
	d.remoteOK(t, 1)
	return d
}

func (d *retireData) expDir() string { return filepath.Join(d.dir, "exports") }

// remoteOK appends to remote.jsonl what backup.sh appends once it has read
// day's tarball back from the remote with the digest it has now.
func (d *retireData) remoteOK(t *testing.T, day int) {
	t.Helper()
	sum, _, err := recordcheck.DigestFile(filepath.Join(d.expDir(), d.exports[day]))
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(filepath.Join(d.expDir(), RemoteFile), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	fmt.Fprintf(f, `{"name":%q,"sha256":%q,"checked_at":"2026-10-05T03:36:00Z","ok":true}`+"\n", d.exports[day], sum)
}

// publicationAt is a publication settled at at+9s whose assignment is the
// assignment library's, so that the store's slim row gives it back byte
// for byte (observer/recordcheck's fixture, dated).
func publicationAt(t *testing.T, b byte, at time.Time) scan.Publication {
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

// readingOf is a full reading of v's rows of p, ten minutes before its
// window closes (the same day as its settlement).
func readingOf(p scan.Publication, v scan.ValidatorAssignment) probe.Measurement {
	at := p.MustServeUntil.Add(-10 * time.Minute)
	idx := make([]uint32, len(v.Rows))
	for i, r := range v.Rows {
		idx[i] = uint32(r)
	}
	m := probe.Measurement{SchemaVersion: 2, Vantage: "ut-1", PromiseHash: p.PromiseHash, Commitment: p.Promise.Commitment, MustServeUntil: p.MustServeUntil,
		ValidatorSetHeight: 999, ValidatorAddress: v.Address, ValidatorHost: v.Host, Assigned: true, Attested: true, AssignedRowCount: v.RowCount,
		ScheduleLabel: "full", ScheduledAt: at, StartedAt: at.Add(time.Second), FinishedAt: at.Add(2 * time.Second), Phase: probe.PhaseInWindow,
		Outcome: probe.OutcomeServedOK, Classification: probe.ClassHealthy, TotalDurationMS: 1000}
	m.Download = probe.DownloadResult{Attempted: true, OK: true, RowsReturned: len(v.Rows), RowsExpected: v.RowCount, RowIndices: idx,
		CommitmentVerified: true, AssignmentVerified: true}
	return m
}

func sum256(b []byte) string {
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}

// archiveByHand makes the cut record.Archive makes, where Archive cannot
// run (no flock): the lines of path dated before cutoff by field go into
// the next gzip segment, the index records it and the new generation, and
// the live file keeps the rest.
func archiveByHand(t *testing.T, path, field string, cutoff time.Time) {
	t.Helper()
	live, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	idx, err := record.LoadIndex(path)
	if err != nil {
		t.Fatal(err)
	}
	var base int64
	if n := len(idx.Generations); n > 0 {
		base = idx.Generations[n-1].Base
	}
	var cut, lines int64
	for _, l := range bytes.SplitAfter(live, []byte{'\n'}) {
		var m map[string]json.RawMessage
		var at time.Time
		if json.Unmarshal(l, &m) != nil || json.Unmarshal(m[field], &at) != nil || !at.Before(cutoff) {
			break
		}
		cut += int64(len(l))
		lines++
	}
	if cut == 0 || cut == int64(len(live)) {
		t.Fatalf("%s: a cut at %d of %d bytes", path, cut, len(live))
	}
	var gz bytes.Buffer
	z := gzip.NewWriter(&gz)
	z.Write(live[:cut])
	z.Close()
	head := func(b []byte) string { return sum256(b[:bytes.IndexByte(b, '\n')+1]) }
	sg := record.Segment{Name: fmt.Sprintf("%06d-%s.jsonl.gz", len(idx.Segments)+1, cutoff.Format("2006-01-02")), From: base, To: base + cut, Lines: lines,
		SHA256: sum256(live[:cut]), GzSHA256: sum256(gz.Bytes()), GzBytes: int64(gz.Len()), Cutoff: cutoff, ArchivedAt: cutoff}
	if len(idx.Generations) == 0 {
		idx.Generations = append(idx.Generations, record.Generation{Base: 0, Head: head(live), At: cutoff})
	}
	idx.Version, idx.File, idx.TimeField, idx.LiveSince = 1, filepath.Base(path), field, cutoff
	idx.Segments = append(idx.Segments, sg)
	idx.Generations = append(idx.Generations, record.Generation{Base: base + cut, Head: head(live[cut:]), At: cutoff})
	adir := record.ArchiveDir(path)
	if err := os.MkdirAll(adir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(adir, sg.Name), gz.Bytes(), 0o644); err != nil {
		t.Fatal(err)
	}
	writeIndex(t, path, idx)
	if err := os.WriteFile(path, live[cut:], 0o644); err != nil {
		t.Fatal(err)
	}
}

func writeIndex(t *testing.T, path string, idx *record.Index) {
	t.Helper()
	raw, err := json.MarshalIndent(idx, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(record.ArchiveDir(path), record.IndexFile), raw, 0o644); err != nil {
		t.Fatal(err)
	}
}

// retireByHand stands in for record.Retire where it cannot run (no
// flock): it records r in the index and removes the segment's file, as
// Retire does after its own read back, which is left to the readers here.
func retireByHand(path, segment string, r record.Retired) error {
	idx, err := record.LoadIndex(path)
	if err != nil {
		return err
	}
	found := false
	for i := range idx.Segments {
		if idx.Segments[i].Name == segment {
			found = true
			if idx.Segments[i].Retired == nil {
				idx.Segments[i].Retired = &r
			}
		}
	}
	if !found {
		return fmt.Errorf("%s: no segment %s", path, segment)
	}
	raw, err := json.MarshalIndent(idx, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(record.ArchiveDir(path), record.IndexFile), raw, 0o644); err != nil {
		return err
	}
	if err := os.Remove(filepath.Join(record.ArchiveDir(path), segment)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

func runAt(t *testing.T, at time.Time, args ...string) (int, string, string) {
	t.Helper()
	var out, errb bytes.Buffer
	code := run(args, &out, &errb, at)
	return code, out.String(), errb.String()
}

// retirement is one -retire run: its exit status, output and report.
type retirement struct {
	code      int
	out, errs string
	rep       Report
}

func (d *retireData) retire(t *testing.T, at time.Time) retirement {
	t.Helper()
	code, out, errs := runAt(t, at, "-data-dir", d.dir, "-retire", "-db", d.db)
	r := retirement{code: code, out: out, errs: errs}
	raw, err := os.ReadFile(filepath.Join(d.dir, record.Dir, ReportFile))
	if err != nil {
		t.Fatalf("no report: %v\n%s%s", err, out, errs)
	}
	if err := json.Unmarshal(raw, &r.rep); err != nil {
		t.Fatal(err)
	}
	return r
}

func (r retirement) file(t *testing.T, name string) FileReport {
	t.Helper()
	for _, f := range r.rep.Files {
		if f.Name == name {
			return f
		}
	}
	t.Fatalf("no %s in the report:\n%s", name, r.out)
	return FileReport{}
}

// kept is every kept segment's reason, by file and segment.
func (r retirement) kept() map[string]string {
	out := map[string]string{}
	for _, f := range r.rep.Files {
		for _, k := range f.KeptSegments {
			out[f.Name+" "+k.Segment] = k.Reason
		}
	}
	return out
}

// readsWhole requires every record file to read back, through the archive
// and the exports, exactly as it was written, and every archive to verify.
func (d *retireData) readsWhole(t *testing.T) {
	t.Helper()
	for name, want := range d.files {
		f, err := record.OpenAll(filePath(d.dir, name))
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		got, err := io.ReadAll(f)
		f.Close()
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("%s does not read back byte for byte (%d of %d bytes): %v", name, len(got), len(want), err)
		}
	}
	if code, out, errs := runAt(t, day0, "-data-dir", d.dir, "-verify"); code != 0 {
		t.Fatalf("verify: %d\n%s%s", code, out, errs)
	}
}

// proven are the fixture's files the store keeps line by line, the ones
// whose segments can be retired.
var proven = []string{"publications.jsonl", "payments.jsonl", "measurements.jsonl", "reachability.jsonl", vantageFile}

// testRetire is the whole retirement over the fixture, archived by archive
// with cutoffs at the start of days 1, 2 and 3, so that each file has a
// segment per day of days 0 to 2: it holds -retire to removing exactly the
// segments every export of which is intact, reproduced from the store and
// on the remote, to keeping each other one with its reason, to a dry run
// and a second run that change nothing, to a tampered export proving
// nothing, to failing on a segment lost without being retired, and to a
// record that reads back byte for byte throughout.
func testRetire(t *testing.T, archive func(d *retireData, cutoff time.Time)) {
	d := newRetireData(t)
	for day := 1; day <= 3; day++ {
		archive(d, day0.AddDate(0, 0, day))
	}
	d.readsWhole(t)
	at := day0.AddDate(0, 0, 4).Add(4*time.Hour + 50*time.Minute)

	// A dry run says what would go and writes nothing: no file removed, no
	// ledger, no report.
	before := d.segmentFiles(t)
	code, out, errs := runAt(t, at, "-data-dir", d.dir, "-retire", "-db", d.db, "-dry-run")
	if code != 0 || !strings.Contains(out, "summary| 9 segment(s) would be retired") {
		t.Fatalf("dry run: %d\n%s%s", code, out, errs)
	}
	if after := d.segmentFiles(t); after != before {
		t.Fatalf("the dry run removed segment files: %d, then %d", before, after)
	}
	for _, p := range []string{filepath.Join(d.expDir(), recordcheck.LedgerFile), filepath.Join(d.dir, record.Dir, ReportFile)} {
		if _, err := os.Stat(p); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("the dry run wrote %s: %v", p, err)
		}
	}

	first := d.retire(t, at)
	if first.code != 0 || first.errs != "" {
		t.Fatalf("first run: %d\n%s%s", first.code, first.out, first.errs)
	}
	if len(first.rep.Checked) != 3 {
		t.Fatalf("first run checked %v against the store, want days 0 to 2", first.rep.Checked)
	}
	remote2 := "2026-10-03 not yet proven on the remote"
	for _, name := range proven {
		f := first.file(t, name)
		wantNow := 2
		if name == "measurements.jsonl" {
			wantNow = 1
		}
		if f.Segments != 3 || f.RetiredNow != wantNow || f.RetiredBefore != 0 || f.BytesFreed <= 0 {
			t.Fatalf("%s: %+v\n%s", name, f, first.out)
		}
		if got := first.kept()[name+" 000003-2026-10-04.jsonl.gz"]; got != remote2 {
			t.Fatalf("%s: day 2, not on the remote, kept for %q", name, got)
		}
	}
	missing := first.kept()["measurements.jsonl 000002-2026-10-03.jsonl.gz"]
	if !strings.HasPrefix(missing, "2026-10-02 measurements.jsonl: 1 lines missing (first missing: ut-1|") || !strings.HasSuffix(missing, ", not reproducible from the store") {
		t.Fatalf("day 1's reading the store does not hold: kept for %q", missing)
	}
	if f := first.file(t, probe.SampledOutFile); f.Kept != 3 || f.RetiredNow != 0 ||
		f.KeptSegments[0].Reason != "sampling_decisions.jsonl is not stored line by line, so the store cannot reproduce it" {
		t.Fatalf("sampling decisions: %+v", f)
	}
	if !strings.Contains(first.out, "publications.jsonl: 3 segment(s): 2 retired now (") || !strings.Contains(first.out, "  kept| 000003-2026-10-04.jsonl.gz [") {
		t.Fatalf("output:\n%s", first.out)
	}

	// What a retired segment says of itself, for a top-level file and a
	// vantage's.
	for name, dir := range map[string]string{"publications.jsonl": "../../exports", vantageFile: "../../../../exports"} {
		path := filePath(d.dir, name)
		idx, err := record.LoadIndex(path)
		if err != nil {
			t.Fatal(err)
		}
		r := idx.Segments[1].Retired
		if r == nil || r.Member != name || r.ExportsDir != dir || len(r.Exports) != 1 || r.Exports[0] != d.exports[1] || !r.At.Equal(at) || !strings.Contains(r.Proof, "verified.json: 2026-10-02 checked ") {
			t.Fatalf("%s: segment 2 retired as %+v", name, r)
		}
		if _, err := os.Stat(filepath.Join(record.ArchiveDir(path), idx.Segments[1].Name)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("%s: the retired segment's file is still there: %v", name, err)
		}
		if _, err := os.Stat(filepath.Join(record.ArchiveDir(path), idx.Segments[2].Name)); err != nil {
			t.Fatalf("%s: the kept segment's file: %v", name, err)
		}
	}
	ledger, err := recordcheck.ReadLedger(filepath.Join(d.expDir(), recordcheck.LedgerFile))
	if err != nil || len(ledger) != 3 {
		t.Fatalf("the days checked are not in the ledger: %v %v", ledger, err)
	}
	d.readsWhole(t)

	// A second run finds nothing new, checks nothing again and keeps the
	// same segments for the same reasons.
	second := d.retire(t, at)
	if second.code != 0 || second.rep.RetiredNow != 0 || len(second.rep.Checked) != 0 || fmt.Sprint(second.kept()) != fmt.Sprint(first.kept()) {
		t.Fatalf("second run: %d %+v\n%s%s", second.code, second.rep, second.out, second.errs)
	}
	if f := second.file(t, "publications.jsonl"); f.RetiredBefore != 2 || f.RetiredNow != 0 {
		t.Fatalf("second run, publications: %+v", f)
	}

	// Day 2's tarball now has an ok line on the remote, but its bytes on the
	// disk changed since the ledger's check: it proves nothing and says so.
	d.remoteOK(t, 2)
	tarball := filepath.Join(d.expDir(), d.exports[2])
	orig, err := os.ReadFile(tarball)
	if err != nil {
		t.Fatal(err)
	}
	bad := append([]byte(nil), orig...)
	bad[len(bad)/2] ^= 0xff
	if err := os.WriteFile(tarball, bad, 0o644); err != nil {
		t.Fatal(err)
	}
	third := d.retire(t, at)
	if third.code != 0 || third.rep.RetiredNow != 0 || len(third.rep.Checked) != 1 {
		t.Fatalf("tampered: %d %+v\n%s%s", third.code, third.rep, third.out, third.errs)
	}
	for _, name := range proven {
		if got := third.kept()[name+" 000003-2026-10-04.jsonl.gz"]; !strings.HasPrefix(got, "2026-10-03 export tarball changed since it was verified (tarball: ") {
			t.Fatalf("%s: day 2, tampered, kept for %q", name, got)
		}
	}
	if fmt.Sprint(third.kept()) != fmt.Sprint(d.retire(t, at).kept()) {
		t.Fatal("a run after the tampered one gave other reasons")
	}

	// The tarball as it was: the ledger's answer stands again, and day 2 is
	// retired; day 1's measurements stay.
	if err := os.WriteFile(tarball, orig, 0o644); err != nil {
		t.Fatal(err)
	}
	fourth := d.retire(t, at)
	if fourth.code != 0 || fourth.rep.RetiredNow != len(proven) || len(fourth.rep.Checked) != 0 || fourth.rep.Kept != 4 {
		t.Fatalf("restored: %d %+v\n%s%s", fourth.code, fourth.rep, fourth.out, fourth.errs)
	}
	if got := fourth.kept()["measurements.jsonl 000002-2026-10-03.jsonl.gz"]; got != missing {
		t.Fatalf("day 1's measurements: kept for %q", got)
	}
	d.readsWhole(t)

	// -logical-end is the end of the file as written, archived and retired
	// or not.
	for _, name := range proven {
		code, out, errs := runAt(t, at, "-data-dir", d.dir, "-logical-end", name)
		if end, err := strconv.ParseInt(strings.TrimSpace(out), 10, 64); code != 0 || err != nil || end != int64(len(d.files[name])) {
			t.Fatalf("%s: -logical-end %d %q %s, want %d", name, code, out, errs, len(d.files[name]))
		}
	}
	if code, out, _ := runAt(t, at, "-data-dir", d.dir, "-status", "-files", "publications.jsonl"); code != 0 || !strings.Contains(out, "3 segment(s) (3 retired, read from the exports)") {
		t.Fatalf("status: %d %s", code, out)
	}

	// A segment whose file is gone without being retired is an error: the
	// run says so, fails, and still reports the rest.
	lost := filepath.Join(record.ArchiveDir(filePath(d.dir, probe.SampledOutFile)), "000001-2026-10-02.jsonl.gz")
	if err := os.Remove(lost); err != nil {
		t.Fatal(err)
	}
	broken := d.retire(t, at)
	if broken.code != 1 || broken.rep.Errors != 1 || !strings.Contains(broken.errs, "sampling_decisions.jsonl: FAILED: 000001-2026-10-02.jsonl.gz: its file is missing and it was never retired") ||
		broken.file(t, "publications.jsonl").RetiredBefore != 3 {
		t.Fatalf("a lost segment: %d %+v\n%s%s", broken.code, broken.rep, broken.out, broken.errs)
	}
}

// segmentFiles counts the segment files on the disk.
func (d *retireData) segmentFiles(t *testing.T) int {
	t.Helper()
	n := 0
	err := filepath.WalkDir(d.dir, func(p string, e os.DirEntry, err error) error {
		if err == nil && strings.HasSuffix(p, ".jsonl.gz") {
			n++
		}
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return n
}

// The exports a segment is read back from are the ones whose members hold
// its bytes as one contiguous copy of the file: a quiet day's empty member
// is left out, and a gap, a file exported again from an earlier offset, or
// a member the export left blank lines out of keeps the segment.
func TestCoverNeedsOneContiguousCopy(t *testing.T) {
	const name = "measurements.jsonl"
	entry := func(day string, from, to, bytes int64) export.Entry {
		e := export.Entry{Name: "tensile-v-" + day + ".tar.gz"}
		e.Day = day
		e.Files = []export.Member{{Name: "publications.jsonl", From: 0, To: 500, Bytes: 500}, {Name: name, From: from, To: to, Bytes: bytes}}
		return e
	}
	days := func(es []export.Entry) string {
		var out []string
		for _, e := range es {
			out = append(out, e.Day)
		}
		return strings.Join(out, ",")
	}
	index := []export.Entry{entry("d4", 300, 400, 100), entry("d3", 200, 300, 100), entry("d2", 200, 200, 0), entry("d1", 0, 200, 200)}
	for _, c := range []struct {
		from, to int64
		want     string
	}{{0, 200, "d1"}, {150, 250, "d1,d3"}, {0, 400, "d1,d3,d4"}, {200, 300, "d3"}} {
		if got, why := cover(index, name, c.from, c.to); why != "" || days(got) != c.want {
			t.Fatalf("[%d, %d): %s %q, want %s", c.from, c.to, days(got), why, c.want)
		}
	}
	for _, c := range []struct {
		index    []export.Entry
		from, to int64
		why      string
	}{
		{index, 300, 450, "logical bytes [400, 450) are in no export"},
		{[]export.Entry{entry("d1", 0, 100, 100), entry("d3", 150, 300, 150)}, 0, 200, "logical bytes [100, 150) are in no export"},
		{[]export.Entry{entry("d1", 0, 100, 100), entry("d2", 0, 50, 50)}, 0, 100, "d2 measurements.jsonl starts at logical byte 0, inside the export before it"},
		{[]export.Entry{entry("d1", 0, 100, 98)}, 0, 100, "d1 measurements.jsonl holds 98 bytes for logical bytes [0, 100), so it is not a copy of them"},
	} {
		if got, why := cover(c.index, name, c.from, c.to); got != nil || !strings.HasPrefix(why, c.why) {
			t.Fatalf("[%d, %d): %s %q, want %q", c.from, c.to, days(got), why, c.why)
		}
	}
}

// The newest check of a tarball on the remote is the one that counts, so a
// check that failed takes back one that passed; a line still being
// appended proves nothing yet, and a line that does not read as a check
// fails the run rather than being passed over.
func TestRemoteChecksNewestCounts(t *testing.T) {
	path := filepath.Join(t.TempDir(), RemoteFile)
	lines := `{"name":"a.tar.gz","sha256":"aa","checked_at":"2026-10-05T03:36:00Z","ok":true}` + "\n" +
		`{"name":"b.tar.gz","sha256":"bb","checked_at":"2026-10-05T03:36:00Z","ok":true}` + "\n" +
		`{"name":"a.tar.gz","sha256":"aa","checked_at":"2026-10-06T03:36:00Z","ok":false}` + "\n" +
		`{"name":"c.tar.gz","sha256":"cc","checked_at":"2026-10-06T03:3`
	if err := os.WriteFile(path, []byte(lines), 0o644); err != nil {
		t.Fatal(err)
	}
	got, err := readRemote(path)
	if err != nil || len(got) != 2 {
		t.Fatalf("%+v %v", got, err)
	}
	r := &retirer{remote: got}
	proof := func(name, sum string) *exportProof {
		e := export.Entry{Name: name}
		e.Day = "2026-10-04"
		return &exportProof{e: e, sum: sum}
	}
	for _, c := range []struct{ name, sum, want string }{
		{"a.tar.gz", "aa", "2026-10-04 not proven on the remote: its last check (2026-10-06T03:36:00Z) did not read the tarball back"},
		{"b.tar.gz", "bb", ""},
		{"b.tar.gz", "b2", "2026-10-04 not proven on the remote: its last check (2026-10-05T03:36:00Z) read back sha256 bb, the tarball now is b2"},
		{"c.tar.gz", "cc", "2026-10-04 not yet proven on the remote"},
	} {
		if why := r.onRemote(proof(c.name, c.sum)); why != c.want {
			t.Fatalf("%s %s: %q, want %q", c.name, c.sum, why, c.want)
		}
	}
	if err := os.WriteFile(path, []byte(lines[:40]+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := readRemote(path); err == nil || !strings.Contains(err.Error(), "line 1") {
		t.Fatalf("a broken line: %v", err)
	}
	if got, err := readRemote(filepath.Join(t.TempDir(), RemoteFile)); err != nil || len(got) != 0 {
		t.Fatalf("no remote.jsonl: %+v %v", got, err)
	}
}

// -logical-end is the size of a file never archived, 0 for one not
// written yet, base plus size for one archived, and an error for one whose
// archive holds bytes its missing live file would have followed, and it
// is a run of its own that reads nothing outside the data dir.
func TestLogicalEndOfFilesNotArchived(t *testing.T) {
	dir := t.TempDir()
	p := filePath(dir, vantageFile)
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	end := func(name string) (int, string) {
		t.Helper()
		code, out, errs := runAt(t, day0, "-data-dir", dir, "-logical-end", name)
		return code, strings.TrimSpace(out + errs)
	}
	if code, out := end(vantageFile); code != 0 || out != "0" {
		t.Fatalf("not written yet: %d %s", code, out)
	}
	lines := `{"scheduled_at":"2026-10-01T00:00:00Z"}` + "\n" + `{"scheduled_at":"2026-10-02T00:00:00Z"}` + "\n"
	if err := os.WriteFile(p, []byte(lines), 0o644); err != nil {
		t.Fatal(err)
	}
	if code, out := end(vantageFile); code != 0 || out != strconv.Itoa(len(lines)) {
		t.Fatalf("never archived: %d %s", code, out)
	}
	archiveByHand(t, p, "scheduled_at", day0.AddDate(0, 0, 1))
	if code, out := end(vantageFile); code != 0 || out != strconv.Itoa(len(lines)) {
		t.Fatalf("archived: %d %s", code, out)
	}
	if err := os.Remove(p); err != nil {
		t.Fatal(err)
	}
	if code, out := end(vantageFile); code != 1 || !strings.Contains(out, "holds logical bytes before it") {
		t.Fatalf("missing after archiving: %d %s", code, out)
	}
	if code, out := end("../elsewhere.jsonl"); code != 2 || !strings.Contains(out, "not a path inside the data directory") {
		t.Fatalf("outside the data dir: %d %s", code, out)
	}
	if code, out, errs := runAt(t, day0, "-data-dir", dir, "-retire", "-verify"); code != 2 || !strings.Contains(errs, "separate runs") {
		t.Fatalf("two modes at once: %d %s%s", code, out, errs)
	}
}

// The command end to end: observer-archive archives every file it now
// archives, publications, payments and the other vantage's heartbeats
// among them, and -retire removes what is proven held elsewhere, through
// record.Retire.
func TestRetireEndToEnd(t *testing.T) {
	if !rotates(t) {
		t.Skip("record.Archive and record.Retire need flock, which this platform does not have; this runs on Linux (CI), and TestRetireWithoutRotation runs the same checks here")
	}
	testRetire(t, func(d *retireData, cutoff time.Time) {
		// The archive run after the export at 04:00, keeping a day live.
		at := cutoff.Add(24*time.Hour + 4*time.Hour + 40*time.Minute)
		code, out, errs := runAt(t, at, "-data-dir", d.dir, "-keep", "24h")
		if code != 0 || errs != "" {
			t.Fatalf("archive at %s: %d\n%s%s", at, code, out, errs)
		}
		for _, f := range append(append([]FileSpec(nil), Files...), FileSpec{vantageFile, ""}) {
			if !strings.Contains(out, "\n"+f.Name+": archived ") && !strings.HasPrefix(out, f.Name+": archived ") {
				t.Fatalf("archive at %s did not archive %s's day:\n%s", at, f.Name, out)
			}
		}
	})
}

// Everything -retire decides holds where rotation cannot run as well, with
// the archive cut by hand as record.Archive cuts it and retireByHand
// standing in for record.Retire, and the readers holding the exports each
// retired segment names to its bytes.
func TestRetireWithoutRotation(t *testing.T) {
	retireSegment = retireByHand
	t.Cleanup(func() { retireSegment = record.Retire })
	testRetire(t, func(d *retireData, cutoff time.Time) {
		specs, err := archived(d.dir)
		if err != nil {
			t.Fatal(err)
		}
		if len(specs) != len(Files)+1 || specs[len(specs)-1].Name != vantageFile {
			t.Fatalf("archived files: %+v", specs)
		}
		for _, f := range specs {
			archiveByHand(t, filePath(d.dir, f.Name), f.TimeField, cutoff)
		}
	})
}
