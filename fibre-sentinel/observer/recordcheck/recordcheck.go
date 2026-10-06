// Package recordcheck checks that the store gives back a day's record files byte for byte: the check that must pass
// before a day's JSONL may ever leave the disk. cmd/record-verify prints it; observer-archive -retire reads what it
// found from the ledger (ledger.go) and checks the days the ledger does not hold yet.
//
// For a day's export (CheckDay): the tarball's bytes and digest against index.json and its .sha256 sidecar, each
// member's bytes, lines and digest against the manifest inside it, and each member the store keeps one row per line
// of (publications, measurements, reachability, payments, and every other vantage's reachability) line by line
// (CheckLines): every line is looked up by its key and written back from the store (store.Record, store.ProbeRecord
// and store.ReachRecord for a slim row, the stored line otherwise). The lines written back, in the export's order,
// make the file again, and its digest is compared with the member's. A line the store cannot give back is counted by
// why (FileReport), never as identical.
//
// It only reads. It deletes nothing and writes nothing but the ledger, and only when asked to.
package recordcheck

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
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
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

// kinds are keyed by the file's base name: another vantage's heartbeats, vantages/<name>/reachability.jsonl in an
// export, are reachability lines like the observer's own, and their keys carry the vantage they were made from.
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

// kindOf is the kind of a member or file name, by its base name.
func kindOf(name string) (kind, bool) {
	k, ok := kinds[path.Base(filepath.ToSlash(name))]
	return k, ok
}

// Checked reports whether the store keeps the file named (a member name or a path) one row per line, so that
// CheckLines can check it. The other record files (registry, runs, host history, sampling secrets and decisions,
// amendments, ranges, corrections) are not stored line by line.
func Checked(name string) bool {
	_, ok := kindOf(name)
	return ok
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

// FileReport is one record file of one day. A line the store cannot give back is counted by why:
//
//   - missing: no row under the line's key;
//   - stripped: the row is there with its line emptied (the raw_json strip retired on 2026-10-04);
//   - sampled out: a NOT_PROBED row of a publication the store keeps as one sampling decision (store/sampledout.go),
//     which stands for the row in every figure but is not the line;
//   - repeated: a key already seen in the file; the store keeps the first line under a key and drops the rest;
//   - different: the line written back is not the line;
//   - unreadable: no key could be read from the line.
//
// The first examples of each are listed with their keys.
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

// NotBack is the number of lines the store did not give back, for whatever reason.
func (f FileReport) NotBack() int64 {
	return f.Missing + f.Stripped + f.SampledOut + f.Repeated + f.Different + f.Unreadable
}

// DayReport is one day: its export and its files. A day is reproducible when the export is intact and every checked
// file is rebuilt byte for byte.
type DayReport struct {
	Day          string       `json:"day"`
	Export       string       `json:"export"`
	ExportIntact bool         `json:"export_intact"`
	ExportErrors []string     `json:"export_errors,omitempty"`
	Files        []FileReport `json:"files"`
	NotChecked   []string     `json:"not_checked,omitempty"`
	Reproducible bool         `json:"reproducible"`

	// tarball is the SHA-256 of the tarball's bytes as they were checked, and checked the members its manifest names
	// that the store keeps line by line: what the ledger records of the day (LedgerDay). Neither is in the report
	// record-verify prints, which says the same through export_intact and its files. tarball is empty when what the
	// check found is not about those bytes alone (CheckDay's unrecorded), so that nothing is recorded.
	tarball string
	checked []string
}

// CheckDay reads the day's tarball once, as a stream: each member's digest and counts against the manifest, and each
// checked file's lines against the store as they go by. dir is the exports directory, e the day's index entry. A
// problem with the export is in the report (ExportIntact, ExportErrors); the error is a failure to read the store.
func CheckDay(ctx context.Context, st *store.Store, dir string, e export.Entry) (DayReport, error) {
	return CheckDayPaced(ctx, st, dir, e, nil)
}

// Pause is called by CheckDayPaced where the check may stop for a while, and may hold it there: before each member of
// the tarball (the first one comes after the tarball was read whole for its digest) and every pauseEvery lines of a
// checked member. The check holds nothing there that another process waits on: each lookup is a statement of its
// own, so no read transaction stays open between two; no lock is taken (the ledger is merged by the caller, after
// the check); and the tarball is only open for reading. A tarball rebuilt while the check waits is found by the
// stream's digest, as one rebuilt while it reads is. observer-archive passes its pace (internal/pace) here, so that a
// day checked during the night's run gives the disk back to the validator it shares it with. An error from it ends
// the check with that error.
type Pause func(ctx context.Context) error

// pauseEvery is how many lines of a checked member go by between two calls of a Pause: often enough that a check
// stops soon after the disk gets busy, and the pressure read at each costs nothing beside the lookups.
var pauseEvery int64 = 10000

// CheckDayPaced is CheckDay, calling pause (nil: never) where the check may stop.
func CheckDayPaced(ctx context.Context, st *store.Store, dir string, e export.Entry, pause Pause) (DayReport, error) {
	r := DayReport{Day: e.Day, Export: e.Name, ExportIntact: true}
	fail := func(f string, a ...any) {
		r.ExportIntact = false
		r.ExportErrors = append(r.ExportErrors, fmt.Sprintf(f, a...))
	}
	// unrecorded is a failure that is not the tarball's bytes: the index entry or the sidecar disagreeing with them,
	// or a file that could not be read. It can be gone by the next run (the builder writes the tarball, then its
	// sidecar, then the index, so a check that read the index before a rebuild sees the new tarball against the old
	// entry), and the ledger keeps an answer for as long as the tarball's digest stays. So the day is not recorded,
	// and the next run checks it again.
	unrecorded := func(f string, a ...any) {
		fail(f, a...)
		r.tarball = ""
	}
	path := filepath.Join(dir, e.Name)

	sum, n, err := DigestFile(path)
	if err != nil {
		unrecorded("%v", err)
		return r, nil
	}
	r.tarball = sum
	if n != e.Bytes || sum != e.SHA256 {
		// The members are checked against this entry's manifest too, so nothing below is about these bytes alone.
		unrecorded("tarball: %d bytes, sha256 %s; the index says %d bytes, %s", n, sum, e.Bytes, e.SHA256)
	}
	if side, err := os.ReadFile(path + ".sha256"); err != nil {
		unrecorded("sidecar: %v", err)
	} else if err := (&export.Archive{SHA256: sum}).CheckSidecar(side, e.Name); err != nil {
		unrecorded("sidecar: %v", err)
	}

	want := map[string]export.Member{}
	for _, m := range e.Manifest.Files {
		want[m.Name] = m
		if Checked(m.Name) {
			r.checked = append(r.checked, m.Name)
		}
	}
	if e.Manifest.State != nil {
		want[e.Manifest.State.Name] = *e.Manifest.State
	}
	f, err := os.Open(path)
	if err != nil {
		unrecorded("%v", err)
		return r, nil
	}
	defer f.Close()
	// The stream is hashed as it is read, so the digest the ledger records is the one of the bytes checked, not of a
	// tarball rebuilt between the two reads. A failure to read the file is told apart from a gzip or tar stream that
	// is broken in it: only the second is about the bytes.
	src := &readFailure{r: f}
	streamed := sha256.New()
	raw := io.TeeReader(src, streamed)
	gz, err := gzip.NewReader(raw)
	if err != nil {
		fail("gzip: %v", err)
		if src.err != nil {
			r.tarball = ""
		}
		return r, nil
	}
	tr := tar.NewReader(gz)
	seen := map[string]bool{}
	for {
		if pause != nil {
			if err := pause(ctx); err != nil {
				return r, fmt.Errorf("%s: %w", e.Name, err)
			}
		}
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
		k, checked := kindOf(name)
		digest := sha256.New()
		count := &lineCounter{}
		body := io.TeeReader(tr, io.MultiWriter(digest, count))
		var fr FileReport
		if checked {
			if fr, err = checkLines(ctx, st, name, k, body, 1, pause); err != nil {
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
			fr.Reproducible = fr.NotBack() == 0 && fr.RebuiltSHA == got
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
	if _, err := io.Copy(io.Discard, raw); err != nil {
		fail("%v", err)
	} else if s := hex.EncodeToString(streamed.Sum(nil)); s != sum {
		unrecorded("tarball: changed while it was checked (sha256 %s, then %s)", sum, s)
	}
	if src.err != nil {
		r.tarball = ""
	}
	r.Reproducible = r.ExportIntact
	for _, fr := range r.Files {
		r.Reproducible = r.Reproducible && fr.Reproducible
	}
	return r, nil
}

// readFailure keeps the first error reading the file itself, the end of it aside.
type readFailure struct {
	r   io.Reader
	err error
}

func (f *readFailure) Read(p []byte) (int, error) {
	n, err := f.r.Read(p)
	if err != nil && !errors.Is(err, io.EOF) && f.err == nil {
		f.err = err
	}
	return n, err
}

type lineCounter struct{ bytes, lines int64 }

func (c *lineCounter) Write(p []byte) (int, error) {
	c.bytes += int64(len(p))
	c.lines += int64(bytes.Count(p, []byte{'\n'}))
	return len(p), nil
}

// DigestFile is the SHA-256 and the length of the file at path: how a tarball's current digest is taken to compare
// with the index, its sidecar and the ledger.
func DigestFile(path string) (string, int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", 0, err
	}
	defer f.Close()
	h := sha256.New()
	n, err := io.Copy(h, f)
	return hex.EncodeToString(h.Sum(nil)), n, err
}

// CheckLines writes each line of src, a record file named name (a member name or a path; its kind is read from its
// base name), back from the store and compares, from line from on. The lines written back, in src's order and each
// with its newline, are the file rebuilt; RebuiltSHA is its digest. A file the store does not keep line by line is an
// error.
func CheckLines(ctx context.Context, st *store.Store, name string, src io.Reader, from int64) (FileReport, error) {
	k, ok := kindOf(name)
	if !ok {
		return FileReport{Name: name}, fmt.Errorf("%s: the store does not keep this file line by line", name)
	}
	return checkLines(ctx, st, name, k, src, from, nil)
}

func checkLines(ctx context.Context, st *store.Store, name string, k kind, src io.Reader, from int64, pause Pause) (FileReport, error) {
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
		if pause != nil && n%pauseEvery == 0 {
			if err := pause(ctx); err != nil {
				return fr, err
			}
		}
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
	fr.Reproducible = fr.NotBack() == 0
	return fr, nil
}
