// Package export builds the daily export: one tarball per UTC day holding
// every record the observer wrote for that day, straight from the JSONL
// files, with a manifest of line counts and digests. The export is what a
// verifier downloads: sentinel-recompute re-derives every verdict and every
// published figure from it, and the digests let two verifiers agree they
// hold the same bytes.
//
// Records are assigned to a day by their own timestamp (a publication's
// settlement time, a probe's start, an endpoint event's time), not by when
// they reached the file. Each file is read from where the previous build
// stopped up to the first record dated after the day; records dated
// before the day that turn up in that range (a late probe row, a straggler
// after a restart) are included and counted as late, so every line lands
// in exactly one export and the manifest says which ones came late.
package export

import (
	"archive/tar"
	"bufio"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/record"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/verdict"
)

// FileSpec names one JSONL file and the field that dates its records.
type FileSpec struct {
	Name      string `json:"name"`
	TimeField string `json:"time_field"`
}

// Files is every record file the observer writes, in export order.
var Files = []FileSpec{
	{"publications.jsonl", "settlement_time"},
	{"payments.jsonl", "time"},
	{"measurements.jsonl", "started_at"},
	// A publication the prober's load policy sampled out, recorded once and
	// dated by the decision, which is the started_at of every NOT_PROBED row
	// it stands for (probe.SampledOut).
	{"sampling_decisions.jsonl", "decided_at"},
	{"reachability.jsonl", "started_at"},
	{"registry.jsonl", "at"},
	{"runs.jsonl", "at"},
	{"sampling-secrets.jsonl", "revealed_at"},
	{"amendments.jsonl", "judged_at"},
	{"host_history.jsonl", "time"},
	// The ranges this observer could not say which x/fibre params were in
	// force over, and the deadlines and verdicts a verified range moved.
	// Both are in the export for the same reason amendments are: without
	// them a third party redrawing the verdicts reaches a different answer
	// and cannot see why.
	{"param_uncertainty.jsonl", "detected_at"},
	{"corrections.jsonl", "judged_at"},
}

// StateFile is the scanner's state, carried in every export as a snapshot
// rather than as a day's lines. It is not a record file: it is the current
// value of the param history, the scan gaps, the host seed and the scan
// frontier, and sentinel-recompute needs all four to redraw a verdict the way
// the observer drew it. Without it the tool reads no gaps, so a late shadow
// verdict that the record defers on a scan gap is redrawn as a decided one,
// and a host it cannot resolve is a difference rather than a known blind
// spot. A few kilobytes, and the piece that makes the rest checkable.
const StateFile = "state.json"

// Member describes one file inside an export.
type Member struct {
	Name      string `json:"name"`
	TimeField string `json:"time_field"`
	Lines     int64  `json:"lines"`
	// LateLines are records dated before the export's day that reached the
	// file after that day's export was built (or, on the first export, any
	// earlier record still unexported). They are in this export.
	LateLines int64  `json:"late_lines"`
	Bytes     int64  `json:"bytes"`
	SHA256    string `json:"sha256"`
	// SkewedLines are records dated more than one day past the export's day:
	// a clock step on the vantage, not a record of the future. They are in
	// this export, with the day's lines, and named here because the
	// alternative is a file that never exports again.
	SkewedLines int64 `json:"skewed_lines,omitempty"`
	// From and To are the byte range of the source file the lines were
	// read from, so a holder of the original file can re-derive the member.
	From int64 `json:"source_from"`
	To   int64 `json:"source_to"`
}

// Manifest is manifest.json inside the tarball and the index entry.
type Manifest struct {
	Vantage     string    `json:"vantage"`
	Day         string    `json:"day"`
	GeneratedAt time.Time `json:"generated_at"`
	Build       string    `json:"build"`
	// Methodology is the verdict.MethodologyVersion the day was recorded under.
	Methodology string   `json:"methodology_version,omitempty"`
	Files       []Member `json:"files"`
	// State is the scanner state snapshot carried beside the day's lines.
	// Absent only on an export built from a data directory that had none.
	State *Member `json:"state,omitempty"`
	Rule  string  `json:"rule"`
}

// Entry is one line of the index the API serves.
type Entry struct {
	Name   string `json:"name"`
	Bytes  int64  `json:"bytes"`
	SHA256 string `json:"sha256"`
	Manifest
	// Signature is the ed25519 signature over the manifest digest, the same
	// object <name>.sig holds (see sign.go). Omitted, not empty, on an
	// export built with no signing key, so an unsigned index is exactly
	// what it was before signing existed.
	Signature *Signature `json:"signature,omitempty"`
}

const rule = "records dated (by time_field, UTC) on this day, plus late records dated earlier, read from each source file between source_from and source_to; every line of every source file is in exactly one export"

// state is exports/state.json: how far each source file has been exported
// and the last day built.
type state struct {
	LastDay string           `json:"last_day"`
	Offsets map[string]int64 `json:"offsets"`
}

// Builder builds exports for one observer.
type Builder struct {
	DataDir string
	Dir     string // where exports and their index live
	Vantage string
	Build   string
	// Hour is the UTC hour after which a day's export may be built (the
	// grace for late rows). 3 means 03:00 the next day.
	Hour int
	Logf func(string, ...any)
	// Signer, when set, signs every export this builder writes (sign.go).
	// Nil builds unsigned exports, exactly as before signing existed.
	Signer *Signer
}

// NamePattern is what an export file name looks like. Exports built before
// the observer was named Tensile carry the old "fibrescope-" prefix; they are
// part of the record and stay downloadable under the name they were published
// with. The optional suffix is the digest sidecar (.sha256) or the signature
// (.sig).
var NamePattern = regexp.MustCompile(`^(?:tensile|fibrescope)-[A-Za-z0-9._-]+-\d{4}-\d{2}-\d{2}\.tar\.gz(\.sha256|\.sig)?$`)

func (b *Builder) name(day string) string {
	v := strings.Map(func(r rune) rune {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '.' || r == '_' {
			return r
		}
		return '-'
	}, b.Vantage)
	if v == "" {
		v = "local"
	}
	return "tensile-" + v + "-" + day + ".tar.gz"
}

// Run builds every export that is due at now and not built yet: from the
// day after the last built one up to yesterday, once the grace hour has
// passed. It returns the names built.
func (b *Builder) Run(now time.Time) ([]string, error) {
	st, err := b.loadState()
	if err != nil {
		return nil, err
	}
	now = now.UTC()
	yesterday := dayOf(now).Add(-24 * time.Hour)
	// A first run builds the newest day whose grace has passed; that export
	// sweeps up everything older as late lines, so the record before the
	// exports began is in an export too.
	first := dayOf(now.Add(-time.Duration(b.Hour) * time.Hour)).Add(-24 * time.Hour)
	if st.LastDay != "" {
		d, err := time.Parse("2006-01-02", st.LastDay)
		if err != nil {
			return nil, fmt.Errorf("exports state: bad last_day %q", st.LastDay)
		}
		first = d.Add(24 * time.Hour)
	}
	var built []string
	for d := first; !d.After(yesterday); d = d.Add(24 * time.Hour) {
		if now.Before(d.Add(24 * time.Hour).Add(time.Duration(b.Hour) * time.Hour)) {
			break // the grace for late rows has not passed
		}
		day := d.Format("2006-01-02")
		if err := b.build(day, st, now); err != nil {
			return built, fmt.Errorf("export %s: %w", day, err)
		}
		built = append(built, b.name(day))
	}
	return built, nil
}

func (b *Builder) loadState() (*state, error) {
	st := &state{Offsets: map[string]int64{}}
	raw, err := os.ReadFile(filepath.Join(b.Dir, "state.json"))
	if errors.Is(err, os.ErrNotExist) {
		return st, nil
	}
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(raw, st); err != nil {
		return nil, fmt.Errorf("exports state: %w", err)
	}
	if st.Offsets == nil {
		st.Offsets = map[string]int64{}
	}
	return st, nil
}

func (b *Builder) saveState(st *state) error {
	raw, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return err
	}
	return atomicWrite(filepath.Join(b.Dir, "state.json"), raw)
}

// build writes one day's export, its digest sidecar, updates the index and
// then the state. The state is written last: a crash before it leaves an
// export that the next run rebuilds from the same offsets, identically.
func (b *Builder) build(day string, st *state, now time.Time) error {
	if err := os.MkdirAll(b.Dir, 0o755); err != nil {
		return err
	}
	man := Manifest{Vantage: b.Vantage, Day: day, GeneratedAt: now.UTC(), Build: b.Build, Methodology: verdict.MethodologyVersion, Rule: rule}
	newOffsets := map[string]int64{}
	// The whole state, as it stands, not the part dated today: it is a
	// snapshot, and every export carries the one current at the time it was
	// built so that any single tarball is enough to redraw the verdicts of
	// the day it holds.
	stateBytes, stateErr := os.ReadFile(filepath.Join(b.DataDir, StateFile))
	if stateErr != nil && !errors.Is(stateErr, fs.ErrNotExist) {
		return stateErr
	}
	var tarBuf bytes.Buffer
	gz := gzip.NewWriter(&tarBuf)
	tw := tar.NewWriter(gz)
	for _, f := range Files {
		from := st.Offsets[f.Name]
		m, data, to, err := collect(filepath.Join(b.DataDir, f.Name), f, day, from)
		if err != nil {
			return err
		}
		newOffsets[f.Name] = to
		man.Files = append(man.Files, m)
		if err := addMember(tw, f.Name, data, now); err != nil {
			return err
		}
	}
	if stateBytes != nil {
		sum := sha256.Sum256(stateBytes)
		man.State = &Member{Name: StateFile, Lines: 1, Bytes: int64(len(stateBytes)), SHA256: hex.EncodeToString(sum[:])}
		if err := addMember(tw, StateFile, stateBytes, now); err != nil {
			return err
		}
	} else if b.Logf != nil {
		b.Logf("WARNING: export %s: no %s in the data directory, so this export cannot be recomputed on its own (scan gaps, the param history and the host seed all live there)", day, StateFile)
	}
	manJSON, err := json.MarshalIndent(man, "", "  ")
	if err != nil {
		return err
	}
	if err := addMember(tw, "manifest.json", manJSON, now); err != nil {
		return err
	}
	if err := tw.Close(); err != nil {
		return err
	}
	if err := gz.Close(); err != nil {
		return err
	}
	name := b.name(day)
	path := filepath.Join(b.Dir, name)
	if err := atomicWrite(path, tarBuf.Bytes()); err != nil {
		return err
	}
	sum := sha256.Sum256(tarBuf.Bytes())
	digest := hex.EncodeToString(sum[:])
	if err := atomicWrite(path+".sha256", []byte(digest+"  "+name+"\n")); err != nil {
		return err
	}
	entry := Entry{Name: name, Bytes: int64(tarBuf.Len()), SHA256: digest, Manifest: man}
	if b.Signer != nil {
		// The digest of manifest.json exactly as the tarball holds it, so a
		// verifier who extracts that one member reproduces it. The key is
		// recorded before the signature is written, and both before the
		// index names the export signed: nothing published ever points at a
		// key or a .sig the directory does not hold. A crash in between
		// leaves a .sig that the next run, rebuilding the day from the same
		// offsets, overwrites along with the tarball.
		ms := sha256.Sum256(manJSON)
		sig := b.Signer.Sign(hex.EncodeToString(ms[:]))
		if err := recordSigningKey(b.Dir, b.Signer.PublicKey(), day); err != nil {
			return fmt.Errorf("signing keys: %w", err)
		}
		sigJSON, err := json.MarshalIndent(sig, "", "  ")
		if err != nil {
			return err
		}
		if err := atomicWrite(path+".sig", append(sigJSON, '\n')); err != nil {
			return err
		}
		entry.Signature = sig
	}
	if err := b.updateIndex(entry); err != nil {
		return err
	}
	st.LastDay = day
	for k, v := range newOffsets {
		st.Offsets[k] = v
	}
	if err := b.saveState(st); err != nil {
		return err
	}
	if b.Logf != nil {
		var lines, late int64
		for _, m := range man.Files {
			lines += m.Lines
			late += m.LateLines
		}
		signed := "unsigned"
		if entry.Signature != nil {
			signed = "signed by " + entry.Signature.KeyFingerprint
		}
		b.Logf("export: %s written (%d lines, %d late, %d bytes, sha256 %s, %s)", name, lines, late, tarBuf.Len(), digest[:12], signed)
	}
	return nil
}

func addMember(tw *tar.Writer, name string, data []byte, now time.Time) error {
	if err := tw.WriteHeader(&tar.Header{Name: name, Mode: 0o644, Size: int64(len(data)), ModTime: now.UTC(), Format: tar.FormatPAX}); err != nil {
		return err
	}
	_, err := tw.Write(data)
	return err
}

// collect reads path from offset `from`, taking every complete line dated
// on or before day and stopping at the first dated after it. It returns
// the member description, the bytes taken, and the offset to resume from.
// A missing file is an empty member.
func collect(path string, f FileSpec, day string, from int64) (Member, []byte, int64, error) {
	m := Member{Name: f.Name, TimeField: f.TimeField, From: from, To: from}
	// Offsets are logical (internal/record): a file whose older lines were
	// archived keeps every byte at the offset it was written at, so
	// source_from and source_to name the same bytes before and after a
	// rotation, and an offset behind the live file's base reads the
	// archived segments.
	s, err := record.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		m.SHA256 = emptySHA
		return m, nil, from, nil
	}
	if err != nil {
		return m, nil, from, err
	}
	defer s.Close()
	if s.End() < from {
		// The file shrank: it was replaced. Start over; the old bytes are
		// in earlier exports and the new ones will be counted late.
		from = 0
		m.From = 0
	}
	src, err := s.ReaderFrom(from)
	if err != nil {
		return m, nil, from, err
	}
	r := bufio.NewReaderSize(src, 1<<20)
	var out bytes.Buffer
	pos := from
	h := sha256.New()
	for {
		raw, err := r.ReadBytes('\n')
		if err != nil {
			if errors.Is(err, io.EOF) {
				break // a partial trailing line waits for the next build
			}
			return m, nil, from, err
		}
		trimmed := bytes.TrimSpace(raw)
		if len(trimmed) == 0 {
			pos += int64(len(raw))
			continue
		}
		d, ok := dayOfLine(trimmed, f.TimeField)
		if !ok {
			// An undated or undecodable line goes with the day being built:
			// it is in the record and must be in exactly one export.
			d = day
		}
		if d > day {
			// The first record of a later day: tomorrow's export starts
			// here — unless it is dated so far ahead that no export will
			// ever reach it. Builds advance one calendar day at a time up
			// to yesterday, so a single line carrying a forward clock step
			// froze this file's offset for good: every later export shipped
			// an empty member for it, with no error and nothing in the
			// manifest but lines: 0, while the record went on growing
			// behind it. A line the clock cannot justify is exported with
			// the day being built, where the record's own rule puts
			// anything undated, and counted so the manifest says it
			// happened.
			if d > nextDay(day) {
				m.SkewedLines++
			} else {
				break
			}
		}
		pos += int64(len(raw))
		out.Write(raw)
		h.Write(raw)
		m.Lines++
		if d < day {
			m.LateLines++
		}
	}
	m.To = pos
	m.Bytes = int64(out.Len())
	m.SHA256 = hex.EncodeToString(h.Sum(nil))
	return m, out.Bytes(), pos, nil
}

// nextDay is the calendar day after a YYYY-MM-DD string. An unparseable day
// yields itself, which makes the look-ahead test above false and keeps the
// old behaviour for anything this cannot reason about.
func nextDay(day string) string {
	t, err := time.Parse("2006-01-02", day)
	if err != nil {
		return day
	}
	return t.AddDate(0, 0, 1).Format("2006-01-02")
}

// emptySHA is SHA-256 of nothing, the digest of an empty member.
const emptySHA = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

// dayOfLine reads the record's timestamp field as a UTC day.
func dayOfLine(line []byte, field string) (string, bool) {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(line, &m); err != nil {
		return "", false
	}
	raw, ok := m[field]
	if !ok {
		return "", false
	}
	var t time.Time
	if err := json.Unmarshal(raw, &t); err != nil || t.IsZero() {
		return "", false
	}
	return t.UTC().Format("2006-01-02"), true
}

func dayOf(t time.Time) time.Time {
	t = t.UTC()
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
}

// updateIndex rewrites index.json with the entry added or replaced.
func (b *Builder) updateIndex(e Entry) error {
	entries, err := ReadIndex(b.Dir)
	if err != nil {
		return err
	}
	kept := entries[:0]
	for _, x := range entries {
		if x.Name != e.Name {
			kept = append(kept, x)
		}
	}
	kept = append(kept, e)
	sort.Slice(kept, func(i, j int) bool { return kept[i].Day > kept[j].Day })
	raw, err := json.MarshalIndent(kept, "", "  ")
	if err != nil {
		return err
	}
	return atomicWrite(filepath.Join(b.Dir, "index.json"), raw)
}

// ReadIndex returns the exports listed in dir's index, newest day first.
// A missing index is an empty list.
func ReadIndex(dir string) ([]Entry, error) {
	raw, err := os.ReadFile(filepath.Join(dir, "index.json"))
	if errors.Is(err, os.ErrNotExist) {
		return []Entry{}, nil
	}
	if err != nil {
		return nil, err
	}
	var out []Entry
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, fmt.Errorf("exports index: %w", err)
	}
	if out == nil {
		out = []Entry{}
	}
	return out, nil
}

func atomicWrite(path string, data []byte) error {
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}
