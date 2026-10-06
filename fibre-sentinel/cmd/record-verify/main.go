// Command record-verify checks, for each finished day, that the store gives back the day's record files byte for
// byte: the check that must pass before a day's JSONL may ever leave the disk. It only reads and reports. It deletes
// nothing, writes nothing but its own report, and nothing acts on what it prints.
//
//	record-verify -db observer.db -exports DIR [-day 2026-10-05 ...] [-json report.json]
//	record-verify -db observer.db -file measurements.jsonl [-from-line N]
//
// For each day with an export, today excluded:
//
//   - the export: the tarball's bytes and digest against index.json and its .sha256 sidecar, and each member's
//     bytes, lines and digest against the manifest inside it;
//   - each record file the store keeps one row per line of (publications, measurements, reachability, payments):
//     every line is looked up by its key and written back from the store (store.Record, store.ProbeRecord and
//     store.ReachRecord for a slim row, the stored line otherwise). The lines written back, in the export's order,
//     make the file again, and its digest is compared with the member's.
//
// A line the store cannot give back is counted by why:
//
//   - missing: no row under the line's key;
//   - stripped: the row is there with its line emptied (the raw_json strip retired on 2026-10-04);
//   - sampled out: a NOT_PROBED row of a publication the store keeps as one sampling decision (store/sampledout.go),
//     which stands for the row in every figure but is not the line;
//   - repeated: a key already seen in the file; the store keeps the first line under a key and drops the rest;
//   - different: the line written back is not the line;
//   - unreadable: no key could be read from the line.
//
// The first examples of each are listed with their keys. A day is reproducible when the export is intact and every
// checked file is rebuilt byte for byte. The other record files (registry, runs, host history, sampling secrets and
// decisions, amendments, ranges, corrections) are not stored line by line and are listed as not checked.
//
// With -file, one record file is checked line by line the same way, from line N on: how the first rows a build
// writes are checked before their day is exported.
//
// Exit status: 0 every day checked is reproducible; 1 one is not; 2 bad usage or a read error.
package main

import (
	"archive/tar"
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/probe"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/export"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
)

// kind is a record file the store keeps one row per line of: its table, and how a line's key is read.
type kind struct {
	table string
	// probes are decoded against their publication, so the query reads its hash beside the row
	probes bool
	key    func(line []byte) (key string, sampledOut *probe.Measurement, err error)
}

var kinds = map[string]kind{
	"publications.jsonl": {table: "publications", key: func(l []byte) (string, *probe.Measurement, error) {
		var x struct {
			H string `json:"promise_hash"`
		}
		err := json.Unmarshal(l, &x)
		return x.H, nil, err
	}},
	"measurements.jsonl": {table: "probes", probes: true, key: func(l []byte) (string, *probe.Measurement, error) {
		var m probe.Measurement
		if err := json.Unmarshal(l, &m); err != nil {
			return "", nil, err
		}
		if probe.IsSampledOutRow(m) {
			return m.DedupeKey(), &m, nil
		}
		return m.DedupeKey(), nil, nil
	}},
	"reachability.jsonl": {table: "reachability", key: func(l []byte) (string, *probe.Measurement, error) {
		var m probe.Measurement
		if err := json.Unmarshal(l, &m); err != nil {
			return "", nil, err
		}
		// store.InsertReachability's key
		return m.Vantage + "|" + m.ValidatorAddress + "|" + m.ScheduledAt.UTC().Format(time.RFC3339Nano), nil, nil
	}},
	"payments.jsonl": {table: "payments", key: func(l []byte) (string, *probe.Measurement, error) {
		var x struct {
			K string `json:"dedupe_key"`
		}
		err := json.Unmarshal(l, &x)
		return x.K, nil, err
	}},
}

func (k kind) query() string {
	switch {
	case k.probes:
		return `SELECT raw_json, promise_hash FROM probes WHERE dedupe_key = ?`
	case k.table == "publications":
		return `SELECT raw_json FROM publications WHERE promise_hash = ?`
	}
	return `SELECT raw_json FROM ` + k.table + ` WHERE dedupe_key = ?`
}

// FileReport is one record file of one day.
type FileReport struct {
	Name         string   `json:"name"`
	Lines        int64    `json:"lines"`
	Identical    int64    `json:"identical"`
	SlimRows     int64    `json:"identical_slim_rows"`
	Missing      int64    `json:"missing"`
	Stripped     int64    `json:"stripped"`
	SampledOut   int64    `json:"sampled_out"`
	Repeated     int64    `json:"repeated"`
	Different    int64    `json:"different"`
	Unreadable   int64    `json:"unreadable"`
	SourceSHA256 string   `json:"source_sha256,omitempty"`
	RebuiltSHA   string   `json:"rebuilt_sha256"`
	Reproducible bool     `json:"reproducible"`
	Examples     []string `json:"examples,omitempty"`
}

func (f FileReport) notBack() int64 {
	return f.Missing + f.Stripped + f.SampledOut + f.Repeated + f.Different + f.Unreadable
}

// DayReport is one day: its export and its files.
type DayReport struct {
	Day          string       `json:"day"`
	Export       string       `json:"export"`
	ExportIntact bool         `json:"export_intact"`
	ExportErrors []string     `json:"export_errors,omitempty"`
	Files        []FileReport `json:"files"`
	NotChecked   []string     `json:"not_checked,omitempty"`
	Reproducible bool         `json:"reproducible"`
}

func main() {
	dbPath := flag.String("db", "", "the observer's store, opened read-only")
	dir := flag.String("exports", "", "the exports directory: index.json and the tarballs")
	file := flag.String("file", "", "check this record file against the store instead of the exports")
	fromLine := flag.Int64("from-line", 1, "with -file, the first line to check")
	jsonOut := flag.String("json", "", "write the whole report here as JSON too")
	var days multi
	flag.Var(&days, "day", "a day to check, repeatable (default every finished day with an export)")
	flag.Parse()
	if *dbPath == "" || (*dir == "") == (*file == "") {
		fmt.Fprintln(os.Stderr, "usage: record-verify -db observer.db (-exports DIR [-day D ...] | -file F [-from-line N]) [-json report.json]")
		os.Exit(2)
	}
	st, err := store.OpenReadOnly(*dbPath)
	if err != nil {
		fatal(err)
	}
	defer st.Close()
	ctx := context.Background()

	if *file != "" {
		name := filepath.Base(*file)
		k, ok := kinds[name]
		if !ok {
			fatal(fmt.Errorf("%s: the store does not keep this file line by line", name))
		}
		f, err := os.Open(*file)
		if err != nil {
			fatal(err)
		}
		defer f.Close()
		r, err := checkLines(ctx, st, name, k, f, *fromLine)
		if err != nil {
			fatal(err)
		}
		printFile("", r)
		writeJSON(*jsonOut, r)
		if r.notBack() > 0 {
			os.Exit(1)
		}
		return
	}

	idx, err := export.ReadIndex(*dir)
	if err != nil {
		fatal(err)
	}
	sort.Slice(idx, func(i, j int) bool { return idx[i].Day < idx[j].Day })
	want := map[string]bool{}
	for _, d := range days {
		want[d] = true
	}
	today := time.Now().UTC().Format("2006-01-02")
	reports := []DayReport{}
	for _, e := range idx {
		if e.Day >= today || (len(days) > 0 && !want[e.Day]) {
			continue
		}
		delete(want, e.Day)
		r, err := checkDay(ctx, st, *dir, e)
		if err != nil {
			fatal(err)
		}
		reports = append(reports, r)
		printDay(r)
	}
	for d := range want {
		fmt.Printf("day| %s: no finished export in the index\n", d)
	}
	writeJSON(*jsonOut, reports)
	ok := 0
	for _, r := range reports {
		if r.Reproducible {
			ok++
		}
	}
	fmt.Printf("summary| %d day(s) checked: %d reproducible byte for byte, %d not. Nothing was deleted.\n", len(reports), ok, len(reports)-ok)
	if ok < len(reports) || len(want) > 0 {
		os.Exit(1)
	}
}

type multi []string

func (m *multi) String() string     { return strings.Join(*m, ",") }
func (m *multi) Set(v string) error { *m = append(*m, v); return nil }

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "record-verify:", err)
	os.Exit(2)
}

func writeJSON(path string, v any) {
	if path == "" {
		return
	}
	b, err := json.MarshalIndent(v, "", "  ")
	if err == nil {
		err = os.WriteFile(path, append(b, '\n'), 0o644)
	}
	if err != nil {
		fatal(err)
	}
}

// checkDay reads the day's tarball once, as a stream: each member's digest and counts against the manifest, and each
// checked file's lines against the store as they go by.
func checkDay(ctx context.Context, st *store.Store, dir string, e export.Entry) (DayReport, error) {
	r := DayReport{Day: e.Day, Export: e.Name, ExportIntact: true}
	fail := func(f string, a ...any) {
		r.ExportIntact = false
		r.ExportErrors = append(r.ExportErrors, fmt.Sprintf(f, a...))
	}
	path := filepath.Join(dir, e.Name)

	sum, n, err := digestFile(path)
	if err != nil {
		fail("%v", err)
		return r, nil
	}
	if n != e.Bytes || sum != e.SHA256 {
		fail("tarball: %d bytes, sha256 %s; the index says %d bytes, %s", n, sum, e.Bytes, e.SHA256)
	}
	if side, err := os.ReadFile(path + ".sha256"); err != nil {
		fail("sidecar: %v", err)
	} else if err := (&export.Archive{SHA256: sum}).CheckSidecar(side, e.Name); err != nil {
		fail("sidecar: %v", err)
	}

	want := map[string]export.Member{}
	for _, m := range e.Manifest.Files {
		want[m.Name] = m
	}
	if e.Manifest.State != nil {
		want[e.Manifest.State.Name] = *e.Manifest.State
	}
	f, err := os.Open(path)
	if err != nil {
		fail("%v", err)
		return r, nil
	}
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		fail("gzip: %v", err)
		return r, nil
	}
	tr := tar.NewReader(gz)
	seen := map[string]bool{}
	for {
		h, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			fail("tar: %v", err)
			break
		}
		name := h.Name
		seen[name] = true
		m, named := want[name]
		if !named && name != "manifest.json" {
			fail("%s: in the tarball, not in the manifest", name)
		}
		k, checked := kinds[name]
		digest := sha256.New()
		count := &lineCounter{}
		body := io.TeeReader(tr, io.MultiWriter(digest, count))
		var fr FileReport
		if checked {
			if fr, err = checkLines(ctx, st, name, k, body, 1); err != nil {
				return r, fmt.Errorf("%s %s: %w", e.Name, name, err)
			}
		}
		if _, err := io.Copy(io.Discard, body); err != nil {
			fail("%s: %v", name, err)
		}
		got := hex.EncodeToString(digest.Sum(nil))
		if named && (got != m.SHA256 || count.bytes != m.Bytes || (name != export.StateFile && count.lines != m.Lines)) {
			fail("%s: %d bytes, %d lines, sha256 %s; the manifest says %d, %d, %s", name, count.bytes, count.lines, got, m.Bytes, m.Lines, m.SHA256)
		}
		switch {
		case checked:
			fr.SourceSHA256 = got
			fr.Reproducible = fr.notBack() == 0 && fr.RebuiltSHA == got
			r.Files = append(r.Files, fr)
		case named && name != export.StateFile:
			r.NotChecked = append(r.NotChecked, name)
		}
	}
	for name := range want {
		if !seen[name] {
			fail("%s: in the manifest, not in the tarball", name)
		}
	}
	r.Reproducible = r.ExportIntact
	for _, fr := range r.Files {
		r.Reproducible = r.Reproducible && fr.Reproducible
	}
	return r, nil
}

type lineCounter struct{ bytes, lines int64 }

func (c *lineCounter) Write(p []byte) (int, error) {
	c.bytes += int64(len(p))
	c.lines += int64(bytes.Count(p, []byte{'\n'}))
	return len(p), nil
}

func digestFile(path string) (string, int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", 0, err
	}
	defer f.Close()
	h := sha256.New()
	n, err := io.Copy(h, f)
	return hex.EncodeToString(h.Sum(nil)), n, err
}

// checkLines writes each line of src back from the store and compares. The lines written back, in src's order and
// each with its newline, are the file rebuilt; RebuiltSHA is its digest.
func checkLines(ctx context.Context, st *store.Store, name string, k kind, src io.Reader, from int64) (FileReport, error) {
	fr := FileReport{Name: name}
	stmt, err := st.DB().PrepareContext(ctx, k.query())
	if err != nil {
		return fr, err
	}
	defer stmt.Close()
	decided, err := st.DB().PrepareContext(ctx, `SELECT COUNT(*) FROM sampling_decisions WHERE vantage = ? AND promise_hash = ?`)
	if err != nil {
		return fr, err
	}
	defer decided.Close()
	examples := map[string]int{}
	note := func(why, key string) {
		if examples[why] < 3 {
			examples[why]++
			fr.Examples = append(fr.Examples, why+": "+key)
		}
	}
	rebuilt := sha256.New()
	keys := map[string]bool{}
	sc := bufio.NewScanner(src)
	sc.Buffer(make([]byte, 1<<20), 256<<20)
	var n int64
	for sc.Scan() {
		n++
		if n < from {
			continue
		}
		line := sc.Bytes()
		fr.Lines++
		key, sampledOut, err := k.key(line)
		if err != nil || key == "" {
			fr.Unreadable++
			note("unreadable", fmt.Sprintf("line %d", n))
			continue
		}
		if keys[key] {
			fr.Repeated++
			note("repeated", key)
			continue
		}
		keys[key] = true
		var raw []byte
		var hash string
		if k.probes {
			err = stmt.QueryRowContext(ctx, key).Scan(&raw, &hash)
		} else {
			err = stmt.QueryRowContext(ctx, key).Scan(&raw)
		}
		switch {
		case errors.Is(err, sql.ErrNoRows) && sampledOut != nil:
			var d int
			if err := decided.QueryRowContext(ctx, sampledOut.Vantage, sampledOut.PromiseHash).Scan(&d); err != nil {
				return fr, err
			}
			if d > 0 {
				fr.SampledOut++
				note("sampled out", key)
			} else {
				fr.Missing++
				note("missing", key)
			}
			continue
		case errors.Is(err, sql.ErrNoRows):
			fr.Missing++
			note("missing", key)
			continue
		case err != nil:
			return fr, err
		case len(raw) == 0:
			fr.Stripped++
			note("stripped", key)
			continue
		}
		back, slim := raw, raw[0] != '{'
		if slim {
			switch {
			case k.probes:
				back, err = st.ProbeRecord(ctx, st.DB(), hash, raw)
			case k.table == "reachability":
				back, err = st.ReachRecord(ctx, st.DB(), raw)
			default:
				back, err = st.Record(ctx, st.DB(), raw)
			}
			if err != nil {
				fr.Different++
				note("different (the slim row does not decode: "+err.Error()+")", key)
				continue
			}
		}
		if !bytes.Equal(back, line) {
			fr.Different++
			note("different", key)
			continue
		}
		fr.Identical++
		if slim {
			fr.SlimRows++
		}
		rebuilt.Write(back)
		rebuilt.Write([]byte{'\n'})
	}
	if err := sc.Err(); err != nil {
		return fr, err
	}
	fr.RebuiltSHA = hex.EncodeToString(rebuilt.Sum(nil))
	fr.Reproducible = fr.notBack() == 0
	return fr, nil
}

func printDay(r DayReport) {
	verdict := "reproducible byte for byte"
	if !r.Reproducible {
		verdict = "NOT reproducible"
	}
	intact := "intact"
	if !r.ExportIntact {
		intact = "NOT intact"
	}
	fmt.Printf("day| %s %s: export %s; %s\n", r.Day, r.Export, intact, verdict)
	for _, e := range r.ExportErrors {
		fmt.Printf("  export| %s\n", e)
	}
	for _, f := range r.Files {
		printFile("  ", f)
	}
	if len(r.NotChecked) > 0 {
		fmt.Printf("  not checked| not stored line by line: %s\n", strings.Join(r.NotChecked, ", "))
	}
}

func printFile(indent string, f FileReport) {
	fmt.Printf("%sfile| %-19s %8d lines: %d identical (%d slim); %d missing, %d stripped, %d sampled out, %d repeated, %d different, %d unreadable; rebuilt %s",
		indent, f.Name, f.Lines, f.Identical, f.SlimRows, f.Missing, f.Stripped, f.SampledOut, f.Repeated, f.Different, f.Unreadable, short(f.RebuiltSHA))
	if f.SourceSHA256 != "" {
		fmt.Printf(", source %s", short(f.SourceSHA256))
	}
	fmt.Println()
	for _, x := range f.Examples {
		fmt.Printf("%s  %s\n", indent, x)
	}
}

func short(s string) string {
	if len(s) > 12 {
		return s[:12]
	}
	return s
}
