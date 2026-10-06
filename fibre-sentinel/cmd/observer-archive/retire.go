package main

// -retire: removing an archived segment's file once every byte of it is
// proven to be held in three other places.
//
// A line of a file the store keeps line by line (publications, payments,
// measurements, the heartbeats of every vantage) is on the disk three
// times once its day is exported and archived: in the segment, in the
// daily export tarball, and in the store, which writes it back byte for
// byte. The tarball also goes to the remote backup every night. The
// segment's file, the local JSONL copy, is removed only when, for every
// export whose member of the file holds some of the segment's bytes:
//
//  1. the tarball is the one index.json lists, and the one the checks
//     below were about;
//  2. the ledger (exports/verified.json) holds it under its current digest
//     with the member rebuilt byte for byte from the store. A day the
//     ledger does not hold, or holds under another digest, is checked here
//     first (recordcheck.CheckDay, against the store opened read-only) and
//     recorded;
//  3. exports/remote.jsonl, where deploy/backup.sh records each tarball it
//     read back from the remote, says the remote copy has the same digest;
//
// and then record.Retire reads the segment back from those exports and
// requires its exact bytes before the file goes. Anything not proven is
// kept, and the run says why; a second run retires nothing new and gives
// the same reasons. Files the store does not keep line by line (of those
// archived, the sampling decisions) are never retired.
//
// The run needs no network, and the archive unit has none: what the remote
// holds is what backup.sh, which runs earlier, read back and recorded.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/record"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/export"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/recordcheck"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
)

// RemoteFile, in the exports directory, is where deploy/backup.sh records
// each export tarball it read back from the remote backup: one JSON line
// per check, {"name","sha256","checked_at","ok"}, appended. The newest line
// for a name is the one that counts, so a later check that failed takes
// back an earlier one that passed.
const RemoteFile = "remote.jsonl"

// ReportFile, under <data-dir>/archive/, is what the last -retire run did.
const ReportFile = "retire-report.json"

// retireSegment removes a proven segment's file. It is record.Retire; the
// tests that run where rotation is not supported (no flock) put a stand-in
// here that edits the index the same way, so that everything this file
// decides is tested there too.
var retireSegment = record.Retire

// Report is one -retire run, file by file, as printed and as written to
// ReportFile.
type Report struct {
	At         time.Time `json:"at"`
	DryRun     bool      `json:"dry_run,omitempty"`
	ExportsDir string    `json:"exports_dir"`
	// Checked are the exports this run checked against the store, with what
	// was found; the others' answers came from the ledger.
	Checked    []string     `json:"checked"`
	Files      []FileReport `json:"files"`
	RetiredNow int          `json:"retired_now"`
	BytesFreed int64        `json:"bytes_freed"`
	Kept       int          `json:"kept"`
	Errors     int          `json:"errors"`
}

// FileReport is one archived file: what happened to each of its segments.
type FileReport struct {
	Name string `json:"name"`
	// Segments counts the segments of the archive before the live file.
	Segments      int              `json:"segments"`
	RetiredNow    int              `json:"retired_now"`
	RetiredBefore int              `json:"retired_before"`
	Kept          int              `json:"kept"`
	BytesFreed    int64            `json:"bytes_freed"`
	Retired       []RetiredSegment `json:"retired,omitempty"`
	KeptSegments  []KeptSegment    `json:"kept_segments,omitempty"`
	// Note says why a file has no segment to look at ("no such file",
	// "nothing archived").
	Note   string   `json:"note,omitempty"`
	Errors []string `json:"errors,omitempty"`
}

// RetiredSegment is a segment whose file this run removed (or, with
// -dry-run, would remove).
type RetiredSegment struct {
	Segment string   `json:"segment"`
	From    int64    `json:"from"`
	To      int64    `json:"to"`
	GzBytes int64    `json:"gz_bytes"`
	Exports []string `json:"exports"`
}

// KeptSegment is a segment whose file stays, and why.
type KeptSegment struct {
	Segment string `json:"segment"`
	From    int64  `json:"from"`
	To      int64  `json:"to"`
	Reason  string `json:"reason"`
}

// retirer is one -retire run.
type retirer struct {
	dataDir, expDir, dbPath string
	dry                     bool
	now                     time.Time
	build                   string
	// retire removes a segment's file (retireSegment).
	retire func(path, segment string, r record.Retired) error

	ctx     context.Context
	st      *store.Store
	stErr   error
	index   []export.Entry
	ledger  recordcheck.Ledger
	remote  map[string]remoteCheck
	proofs  map[string]*exportProof
	checked []string
}

// run retires what is proven, prints what it did and, unless -dry-run,
// writes the report. Exit status: 0 when the run worked, whether or not
// segments were kept for a reason; 1 on an error.
func (r *retirer) run(specs []FileSpec, stdout, stderr io.Writer) int {
	rep, err := r.retireAll(specs)
	if err != nil {
		fmt.Fprintln(stderr, "observer-archive: -retire:", err)
		return 1
	}
	printReport(stdout, stderr, rep)
	if !r.dry {
		p, err := writeReport(r.dataDir, rep)
		if err != nil {
			fmt.Fprintln(stderr, "observer-archive: report:", err)
			return 1
		}
		fmt.Fprintf(stdout, "report| %s\n", p)
	}
	if rep.Errors > 0 {
		return 1
	}
	return 0
}

// retireAll reads the exports' index, the ledger and the remote checks
// once, then goes through specs. The error is one that leaves nothing to
// decide on: an index or a ledger or remote.jsonl that cannot be read. An
// error about one file is in its report, and the run goes on to the next.
func (r *retirer) retireAll(specs []FileSpec) (*Report, error) {
	if r.ctx == nil {
		r.ctx = context.Background()
	}
	defer func() {
		if r.st != nil {
			r.st.Close()
			r.st = nil
		}
	}()
	var err error
	if r.index, err = export.ReadIndex(r.expDir); err != nil {
		return nil, err
	}
	if r.ledger, err = recordcheck.ReadLedger(r.ledgerPath()); err != nil {
		return nil, err
	}
	if r.remote, err = readRemote(filepath.Join(r.expDir, RemoteFile)); err != nil {
		return nil, err
	}
	r.proofs = map[string]*exportProof{}
	r.checked = nil
	rep := &Report{At: r.now, DryRun: r.dry, ExportsDir: r.expDir, Files: []FileReport{}}
	for _, f := range specs {
		fr := r.file(f)
		rep.Files = append(rep.Files, fr)
		rep.RetiredNow += fr.RetiredNow
		rep.BytesFreed += fr.BytesFreed
		rep.Kept += fr.Kept
		rep.Errors += len(fr.Errors)
	}
	rep.Checked = append([]string{}, r.checked...)
	return rep, nil
}

func (r *retirer) ledgerPath() string { return filepath.Join(r.expDir, recordcheck.LedgerFile) }

// file decides every segment of one file before its live file.
func (r *retirer) file(f FileSpec) FileReport {
	fr := FileReport{Name: f.Name}
	path := filePath(r.dataDir, f.Name)
	// The live file must be one its index describes. Open reads a live file
	// that matches no generation as if nothing were archived, and every
	// segment would then look like one a crashed run left behind.
	if _, err := record.LogicalEnd(path); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			fr.Note = "no such file"
		} else {
			fr.Errors = append(fr.Errors, err.Error())
		}
		return fr
	}
	s, err := record.Open(path)
	if err != nil {
		fr.Errors = append(fr.Errors, err.Error())
		return fr
	}
	idx, base := s.Index(), s.Base()
	s.Close()
	var segs []record.Segment
	for _, sg := range idx.Segments {
		// A segment past the live file's base was written by a run that
		// never swapped it in; the next archive run drops it.
		if sg.To <= base {
			segs = append(segs, sg)
		}
	}
	fr.Segments = len(segs)
	if len(segs) == 0 {
		fr.Note = "nothing archived"
		return fr
	}
	keep := func(sg record.Segment, why string) {
		fr.Kept++
		fr.KeptSegments = append(fr.KeptSegments, KeptSegment{Segment: sg.Name, From: sg.From, To: sg.To, Reason: why})
	}
	byLine := recordcheck.Checked(f.Name)
	for _, sg := range segs {
		_, err := os.Stat(filepath.Join(record.ArchiveDir(path), sg.Name))
		present := err == nil
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			fr.Errors = append(fr.Errors, err.Error())
			keep(sg, err.Error())
			continue
		}
		switch {
		case sg.Retired != nil && !present:
			fr.RetiredBefore++
		case sg.Retired != nil:
			// Retire saves the index before it removes the file, so a run
			// that stopped in between left both. Finishing it reads the
			// exports back again first; what else was proven was proven
			// before the index was saved.
			r.apply(&fr, path, sg, *sg.Retired, keep)
		case !present:
			why := "its file is missing and it was never retired: nothing proves its bytes are anywhere else"
			fr.Errors = append(fr.Errors, sg.Name+": "+why)
			keep(sg, why)
		case !byLine:
			keep(sg, f.Name+" is not stored line by line, so the store cannot reproduce it")
		default:
			ret, reasons, err := r.plan(f.Name, path, sg)
			switch {
			case err != nil:
				fr.Errors = append(fr.Errors, sg.Name+": "+err.Error())
				keep(sg, "not checked: "+err.Error())
			case len(reasons) > 0:
				keep(sg, strings.Join(reasons, "; "))
			default:
				r.apply(&fr, path, sg, ret, keep)
			}
		}
	}
	return fr
}

// apply removes sg's file (record.Retire), or with -dry-run only counts it.
// A Retire that refuses is an error: everything it checks was checked
// before, so the exports changed during the run or disagree with the
// segment, and someone should look.
func (r *retirer) apply(fr *FileReport, path string, sg record.Segment, ret record.Retired, keep func(record.Segment, string)) {
	if !r.dry {
		if err := r.retire(path, sg.Name, ret); err != nil {
			fr.Errors = append(fr.Errors, err.Error())
			keep(sg, "retiring failed: "+err.Error())
			return
		}
	}
	fr.RetiredNow++
	fr.BytesFreed += sg.GzBytes
	fr.Retired = append(fr.Retired, RetiredSegment{Segment: sg.Name, From: sg.From, To: sg.To, GzBytes: sg.GzBytes, Exports: ret.Exports})
}

// plan finds the exports holding sg's bytes of the file named member and
// what stands in the way of each proving them. With nothing in the way it
// returns the Retired to give sg. The error is one that stopped the
// proving itself: the store could not be opened or read, a tarball could
// not be read.
func (r *retirer) plan(member, path string, sg record.Segment) (record.Retired, []string, error) {
	parts, why := cover(r.index, member, sg.From, sg.To)
	if why != "" {
		return record.Retired{}, []string{why}, nil
	}
	var reasons, names, stored, remote []string
	for _, e := range parts {
		names = append(names, e.Name)
		p, err := r.proof(e)
		if err != nil {
			return record.Retired{}, nil, err
		}
		if why := p.member(member); why != "" {
			reasons = append(reasons, why)
			continue
		}
		if why := r.onRemote(p); why != "" {
			reasons = append(reasons, why)
			continue
		}
		stored = append(stored, fmt.Sprintf("%s checked %s by %s", e.Day, p.day.CheckedAt.UTC().Format(time.RFC3339), p.day.Build))
		remote = append(remote, fmt.Sprintf("%s at %s", e.Day, r.remote[e.Name].CheckedAt))
	}
	if len(reasons) > 0 {
		return record.Retired{}, reasons, nil
	}
	rel, err := exportsDirFrom(path, r.expDir)
	if err != nil {
		return record.Retired{}, nil, err
	}
	return record.Retired{
		At:         r.now,
		Exports:    names,
		Member:     member,
		ExportsDir: rel,
		Proof: fmt.Sprintf("held by the member %s of %s, each tarball the one index.json lists; "+
			"the member rebuilt byte for byte from the store (verified.json: %s); "+
			"the remote backup's copy of each tarball read back to the same sha256 (remote.jsonl: %s); "+
			"the segment's bytes read back from the exports to its sha256 before its file was removed",
			member, strings.Join(names, ", "), strings.Join(stored, "; "), strings.Join(remote, "; ")),
	}, nil, nil
}

// exportsDirFrom is the exports directory relative to path's archive
// directory, which is how a retired segment names it, so that a restored
// copy of the data directory reads its own exports.
func exportsDirFrom(path, expDir string) (string, error) {
	adir, err := filepath.Abs(record.ArchiveDir(path))
	if err != nil {
		return "", err
	}
	edir, err := filepath.Abs(expDir)
	if err != nil {
		return "", err
	}
	rel, err := filepath.Rel(adir, edir)
	if err != nil {
		return "", err
	}
	return filepath.ToSlash(rel), nil
}

// cover is the exports whose member of the file named member holds some of
// logical bytes [from, to), in the order they hold them (oldest first), or
// why they do not hold all of it as a copy of the file's bytes. A member
// that holds nothing (a quiet day) is left out: no byte of the range is in
// its tarball, so nothing about that tarball needs proving.
func cover(index []export.Entry, member string, from, to int64) ([]export.Entry, string) {
	type part struct {
		e export.Entry
		m export.Member
	}
	var parts []part
	for _, e := range index {
		for _, m := range e.Files {
			if m.Name == member && m.To > m.From && m.From < to && m.To > from {
				parts = append(parts, part{e, m})
			}
		}
	}
	sort.SliceStable(parts, func(i, j int) bool { return parts[i].m.From < parts[j].m.From })
	var out []export.Entry
	at := from
	for i, p := range parts {
		switch {
		case p.m.From > at:
			return nil, fmt.Sprintf("logical bytes [%d, %d) are in no export", at, p.m.From)
		case i > 0 && p.m.From < at:
			// The file was exported again from an earlier offset (the
			// export starts over a file that shrank, i.e. was replaced):
			// two exports hold different bytes at the same offsets, and
			// which of them are the archived ones is not for this run to
			// guess.
			return nil, fmt.Sprintf("%s %s starts at logical byte %d, inside the export before it: the file was exported again from an earlier offset", p.e.Day, member, p.m.From)
		case p.m.Bytes != p.m.To-p.m.From:
			// The export leaves a blank line's bytes out of the member.
			return nil, fmt.Sprintf("%s %s holds %d bytes for logical bytes [%d, %d), so it is not a copy of them", p.e.Day, member, p.m.Bytes, p.m.From, p.m.To)
		}
		out = append(out, p.e)
		at = p.m.To
	}
	if at < to {
		return nil, fmt.Sprintf("logical bytes [%d, %d) are in no export", at, to)
	}
	return out, ""
}

// exportProof is what this run found of one export tarball: its digest now
// and the store check that stands for those bytes, or why there is none.
type exportProof struct {
	e   export.Entry
	sum string
	day *recordcheck.LedgerDay
	why string
}

// member is why the export does not show the member reproducible from the
// store; empty when it does.
func (p *exportProof) member(name string) string {
	if p.why != "" {
		return p.why
	}
	m, ok := p.day.Files[name]
	switch {
	case !ok:
		return fmt.Sprintf("%s %s: the store check of the export does not name it", p.e.Day, name)
	case !m.Reproducible:
		return fmt.Sprintf("%s %s: %s, not reproducible from the store", p.e.Day, name, m.Why)
	}
	return ""
}

// proof finds what stands for export e's bytes, once per run: the ledger's
// entry while the tarball is the one it checked, or else a check against
// the store, recorded in the ledger (not with -dry-run). A tarball whose
// check is not about its bytes alone (it disagrees with index.json or its
// sidecar) is not recorded, by recordcheck's rule, and proves nothing.
func (r *retirer) proof(e export.Entry) (*exportProof, error) {
	if p, ok := r.proofs[e.Name]; ok {
		return p, nil
	}
	p := &exportProof{e: e}
	sum, n, err := recordcheck.DigestFile(filepath.Join(r.expDir, e.Name))
	switch {
	case errors.Is(err, os.ErrNotExist):
		p.why = fmt.Sprintf("%s export %s is not in the exports directory", e.Day, e.Name)
		r.proofs[e.Name] = p
		return p, nil
	case err != nil:
		return nil, fmt.Errorf("export %s: %w", e.Name, err)
	}
	p.sum = sum
	if d, ok := r.ledger.Holds(e.Name, sum); ok {
		if sum != e.SHA256 || n != e.Bytes {
			p.why = fmt.Sprintf("%s export tarball is not the one index.json lists (%d bytes, sha256 %s; the index says %d, %s)", e.Day, n, short(sum), e.Bytes, short(e.SHA256))
		} else {
			p.day = &d
		}
		r.proofs[e.Name] = p
		return p, nil
	}
	if r.st == nil && r.stErr == nil {
		r.st, r.stErr = store.OpenReadOnly(r.dbPath)
	}
	if r.stErr != nil {
		return nil, fmt.Errorf("%s is not in the ledger and the store could not be opened to check it: %w", e.Name, r.stErr)
	}
	rep, err := recordcheck.CheckDay(r.ctx, r.st, r.expDir, e)
	if err != nil {
		return nil, fmt.Errorf("checking %s against the store: %w", e.Name, err)
	}
	d, ok := rep.LedgerDay(r.build, r.now)
	if !ok {
		first := "it could not be read"
		if len(rep.ExportErrors) > 0 {
			first = rep.ExportErrors[0]
		}
		if prev, had := r.ledger[e.Name]; had && prev.SHA256 != sum {
			p.why = fmt.Sprintf("%s export tarball changed since it was verified (%s)", e.Day, first)
		} else {
			p.why = fmt.Sprintf("%s export is not intact (%s)", e.Day, first)
		}
		r.checked = append(r.checked, fmt.Sprintf("%s %s: not recorded: %s", e.Day, e.Name, first))
		r.proofs[e.Name] = p
		return p, nil
	}
	if !r.dry {
		// Merged at once, so a long run that stops keeps the days it
		// checked.
		if err := recordcheck.MergeLedger(r.ledgerPath(), map[string]recordcheck.LedgerDay{e.Name: d}); err != nil {
			return nil, err
		}
	}
	r.ledger[e.Name] = d
	p.day = &d
	verdict := "every checked member reproducible byte for byte"
	if !d.Reproducible() {
		var not []string
		for name, m := range d.Files {
			if !m.Reproducible {
				not = append(not, name)
			}
		}
		sort.Strings(not)
		verdict = "no member checked"
		if len(not) > 0 {
			verdict = "NOT reproducible: " + strings.Join(not, ", ")
		}
	}
	r.checked = append(r.checked, fmt.Sprintf("%s %s: %s", e.Day, e.Name, verdict))
	r.proofs[e.Name] = p
	return p, nil
}

// remoteCheck is one line of RemoteFile.
type remoteCheck struct {
	Name      string `json:"name"`
	SHA256    string `json:"sha256"`
	CheckedAt string `json:"checked_at"`
	OK        bool   `json:"ok"`
}

// readRemote reads RemoteFile at path: the newest check of each tarball,
// by name. A missing file is no check yet. A line that does not read as a
// check is an error rather than skipped: skipping it could let an older
// check that passed stand for one that failed. The bytes after the last
// newline are a line backup.sh is still appending, and prove nothing yet.
func readRemote(path string) (map[string]remoteCheck, error) {
	out := map[string]remoteCheck{}
	raw, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return out, nil
	}
	if err != nil {
		return nil, err
	}
	lines := bytes.Split(raw, []byte{'\n'})
	for i, l := range lines[:len(lines)-1] {
		if len(bytes.TrimSpace(l)) == 0 {
			continue
		}
		var c remoteCheck
		if err := json.Unmarshal(l, &c); err != nil {
			return nil, fmt.Errorf("%s line %d: %w", path, i+1, err)
		}
		if c.Name == "" {
			return nil, fmt.Errorf("%s line %d: no name", path, i+1)
		}
		out[c.Name] = c
	}
	return out, nil
}

// onRemote is why the remote backup is not shown to hold the export's
// tarball as it is now; empty when it is.
func (r *retirer) onRemote(p *exportProof) string {
	c, ok := r.remote[p.e.Name]
	switch {
	case !ok:
		return fmt.Sprintf("%s not yet proven on the remote", p.e.Day)
	case !c.OK:
		return fmt.Sprintf("%s not proven on the remote: its last check (%s) did not read the tarball back", p.e.Day, c.CheckedAt)
	case !strings.EqualFold(c.SHA256, p.sum):
		return fmt.Sprintf("%s not proven on the remote: its last check (%s) read back sha256 %s, the tarball now is %s", p.e.Day, c.CheckedAt, short(c.SHA256), short(p.sum))
	}
	return ""
}

func short(s string) string {
	if len(s) > 12 {
		return s[:12]
	}
	return s
}

// printReport prints one line per file, each kept segment with its reason
// and, to stderr, each error.
func printReport(stdout, stderr io.Writer, rep *Report) {
	verb, freed := "retired now", "freed"
	if rep.DryRun {
		verb, freed = "would be retired", "to free"
	}
	for _, c := range rep.Checked {
		fmt.Fprintf(stdout, "checked| %s\n", c)
	}
	for _, f := range rep.Files {
		if f.Note != "" {
			fmt.Fprintf(stdout, "%s: %s\n", f.Name, f.Note)
		} else if f.Segments > 0 {
			fmt.Fprintf(stdout, "%s: %d segment(s): %d %s (%s %s), %d retired before, %d kept\n",
				f.Name, f.Segments, f.RetiredNow, verb, mb(f.BytesFreed), freed, f.RetiredBefore, f.Kept)
		}
		for _, k := range f.KeptSegments {
			fmt.Fprintf(stdout, "  kept| %s [%d, %d): %s\n", k.Segment, k.From, k.To, k.Reason)
		}
		for _, e := range f.Errors {
			fmt.Fprintf(stderr, "%s: FAILED: %s\n", f.Name, e)
		}
	}
	fmt.Fprintf(stdout, "summary| %d segment(s) %s, %s %s; %d kept. Nothing else was deleted.\n", rep.RetiredNow, verb, mb(rep.BytesFreed), freed, rep.Kept)
}

// writeReport writes rep to <data-dir>/archive/retire-report.json
// atomically: a temp file, synced, renamed over the old report. A reader
// sees one whole report or the other.
func writeReport(dataDir string, rep *Report) (string, error) {
	dir := filepath.Join(dataDir, record.Dir)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	raw, err := json.MarshalIndent(rep, "", "  ")
	if err != nil {
		return "", err
	}
	path := filepath.Join(dir, ReportFile)
	tmp, err := os.CreateTemp(dir, ReportFile+".*.tmp")
	if err != nil {
		return "", err
	}
	defer os.Remove(tmp.Name()) // gone already after the rename
	_, err = tmp.Write(append(raw, '\n'))
	if err == nil {
		err = tmp.Chmod(0o644)
	}
	if err == nil {
		err = tmp.Sync()
	}
	if cerr := tmp.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return "", err
	}
	if err := os.Rename(tmp.Name(), path); err != nil {
		return "", err
	}
	// The directory is synced where it can be. A report lost to a power
	// cut is the previous one, and the next run writes it again.
	if d, err := os.Open(dir); err == nil {
		_ = d.Sync()
		d.Close()
	}
	return path, nil
}
