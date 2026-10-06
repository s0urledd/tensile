// Command observer-archive keeps the observer's biggest JSONL files bounded:
// once a day, after the daily export, the lines older than -keep move from
// each live file into a gzip segment under <data-dir>/archive/<file>/, and
// the live file keeps the rest. Nothing leaves the record. Every reader
// reads the segments and the live file as the one file they were written as
// (internal/record), at the same byte offsets.
//
// Archived: publications.jsonl and payments.jsonl (sentinel-scan),
// measurements.jsonl and sampling_decisions.jsonl (sentinel-probe),
// reachability.jsonl (observer-heartbeat), and each other vantage's
// vantages/<name>/reachability.jsonl, whose segments go under
// vantages/<name>/archive/. Their writers follow a rotation: the
// observer's own through record.Appender, deploy/vantage-pull.sh by
// appending under a shared flock on the file from its logical end
// (-logical-end). The other record files are small and their writers hold
// them open without that protocol; they are left alone.
//
// The run is idempotent (a second run the same day finds nothing older than
// the cutoff) and crash-safe (record.Archive): the source bytes stay in the
// live file until the segment holding them is fsynced and has read back to
// the same digest, and a run that stopped half way is undone by the next.
//
// With -retire, a segment's gzip file is removed once every byte of it is
// proven to be in three other places: the daily exports, intact; the store,
// which gives back every line byte for byte; and the remote backup, read
// back to the same digest (retire.go). Anything not proven is kept, and the
// run says why.
//
//	observer-archive -data-dir /var/lib/fibre-observer/mocha            archive
//	observer-archive -data-dir ... -dry-run                             say what would move
//	observer-archive -data-dir ... -verify                              check every segment
//	observer-archive -data-dir ... -status                              one line per file
//	observer-archive -data-dir ... -retire -db .../observer.db          remove what is proven held elsewhere
//	observer-archive -data-dir ... -logical-end vantages/de-1/reachability.jsonl
package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/probe"
	"github.com/plsgiveup/fibre/fibre-sentinel/internal/record"
	"github.com/plsgiveup/fibre/fibre-sentinel/internal/scan"
	"github.com/plsgiveup/fibre/fibre-sentinel/internal/status"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/export"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/recordcheck"
)

// FileSpec is one archived file and the field that dates its lines.
type FileSpec struct {
	Name      string
	TimeField string
}

// Files are the observer's own files this command archives; each other
// vantage's heartbeats are archived too (VantageFiles). The time field of
// the prober's two files is the one its restart horizon reasons about
// (probe.archivedFrom): a measurement's scheduled_at, a decision's
// decided_at. A publication is dated by its settlement, as the export
// dates it, and a payment by its block time.
var Files = []FileSpec{
	{"measurements.jsonl", "scheduled_at"},
	{probe.SampledOutFile, "decided_at"},
	{"reachability.jsonl", "scheduled_at"},
	{"publications.jsonl", "settlement_time"},
	{"payments.jsonl", "time"},
}

// VantageFiles are the other vantages' heartbeat files under dataDir, each
// named as the export names its member and keys its offset
// ("vantages/de-1/reachability.jsonl"), so the cap at the export's offset
// and the exports a retired segment is read back from are found by the
// same name. They are dated like the observer's own heartbeats.
func VantageFiles(dataDir string) ([]FileSpec, error) {
	vs, err := export.VantageFiles(dataDir)
	if err != nil {
		return nil, err
	}
	out := make([]FileSpec, 0, len(vs))
	for _, v := range vs {
		out = append(out, FileSpec{v.Name, "scheduled_at"})
	}
	return out, nil
}

// archived is every file archived under dataDir: Files, then the other
// vantages' files.
func archived(dataDir string) ([]FileSpec, error) {
	vs, err := VantageFiles(dataDir)
	if err != nil {
		return nil, err
	}
	return append(append([]FileSpec(nil), Files...), vs...), nil
}

// filePath is where the file named name (slash-separated, relative to the
// data directory) is.
func filePath(dataDir, name string) string {
	return filepath.Join(dataDir, filepath.FromSlash(name))
}

// DefaultKeep is how much of each file stays live.
const DefaultKeep = 7 * 24 * time.Hour

// minMargin is added to the longest retention window the chain has had to
// make the shortest -keep accepted: a publication's schedule runs from its
// settlement to must_serve_until plus a few minutes, and the prober plans
// it again after a restart only while every row it could have written is
// still live.
const minMargin = 24 * time.Hour

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr, time.Now()))
}

func run(args []string, stdout, stderr io.Writer, now time.Time) int {
	fs := flag.NewFlagSet("observer-archive", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var (
		dataDir   = fs.String("data-dir", "./sentinel-data", "the observer's data directory")
		keep      = fs.Duration("keep", DefaultKeep, "keep lines dated within this long in the live files (the cutoff is the start of that UTC day); at least the longest retention window in state.json plus 24h")
		only      = fs.String("files", "", "comma-separated subset of "+names(Files)+",vantages/<name>/reachability.jsonl (default all)")
		expDir    = fs.String("exports-dir", "", "the daily exports' dir (default <data-dir>/exports); nothing the export has not read yet is archived")
		noExpCap  = fs.Bool("ignore-exports", false, "archive whether or not the daily export has read the lines (it reads the archive either way)")
		dryRun    = fs.Bool("dry-run", false, "report what would move (with -retire: what would be removed); write nothing")
		verify    = fs.Bool("verify", false, "check every segment against its index (digest, length, lines, no gap) and exit")
		statusOut = fs.Bool("status", false, "print each file's base, live size and segments and exit")
		retire    = fs.Bool("retire", false, "archive nothing; remove each archived segment's file whose bytes are proven to be in the intact daily exports, reproduced by the store and on the remote backup, and say why each other one is kept")
		dbPath    = fs.String("db", "", "with -retire: the observer's store (default <data-dir>/observer.db, as the collector's), opened read-only to check the days the exports' ledger ("+recordcheck.LedgerFile+") does not hold yet")
		logical   = fs.String("logical-end", "", "print the logical end of this file (a path under -data-dir, e.g. vantages/de-1/reachability.jsonl) and exit: its live file's base plus its size, where a copier appending to it resumes")
	)
	if err := fs.Parse(args); err != nil {
		return 2
	}
	modes := 0
	for _, on := range []bool{*verify, *statusOut, *retire, *logical != ""} {
		if on {
			modes++
		}
	}
	if modes > 1 {
		fmt.Fprintln(stderr, "observer-archive: -verify, -status, -retire and -logical-end are separate runs; give one")
		return 2
	}
	if *logical != "" {
		return printLogicalEnd(*dataDir, *logical, stdout, stderr)
	}
	specs, err := selectFiles(*dataDir, *only)
	if err != nil {
		fmt.Fprintln(stderr, "observer-archive:", err)
		return 2
	}
	if *verify {
		return verifyAll(*dataDir, specs, stdout, stderr)
	}
	if *statusOut {
		return statusAll(*dataDir, specs, stdout, stderr)
	}
	if *expDir == "" {
		*expDir = filepath.Join(*dataDir, "exports")
	}
	if *retire {
		// Retiring archives nothing: the unit runs it after the archive
		// run, which is given the operator's -keep. A -retire that
		// archived too, with the default -keep, would move lines an
		// operator had chosen to keep live.
		if *dbPath == "" {
			*dbPath = filepath.Join(*dataDir, "observer.db")
		}
		r := &retirer{
			dataDir: *dataDir, expDir: *expDir, dbPath: *dbPath, dry: *dryRun, now: now.UTC(),
			build: status.BuildRevision(), retire: retireSegment,
		}
		return r.run(specs, stdout, stderr)
	}
	minKeep, err := MinKeep(filepath.Join(*dataDir, "state.json"))
	if err != nil {
		fmt.Fprintln(stderr, "observer-archive:", err)
		return 2
	}
	if *keep < minKeep {
		fmt.Fprintf(stderr, "observer-archive: -keep %s is shorter than %s (the longest retention window in state.json plus %s); a restarted prober could not see rows it still needs\n", *keep, minKeep, minMargin)
		return 2
	}
	cutoff := Cutoff(now, *keep)
	var exported map[string]int64
	if !*noExpCap {
		if exported, err = exportOffsets(*expDir); err != nil {
			fmt.Fprintln(stderr, "observer-archive:", err)
			return 2
		}
	}
	verb := "archived"
	if *dryRun {
		verb = "would archive"
	}
	failed := false
	for _, f := range specs {
		limit := int64(-1)
		if exported != nil {
			limit = exported[f.Name] // 0 when the export has not read the file: nothing moves
		}
		res, err := record.Archive(filePath(*dataDir, f.Name), record.Options{
			Cutoff: cutoff, TimeField: f.TimeField, Limit: limit, DryRun: *dryRun, Now: now,
		})
		if err != nil {
			fmt.Fprintf(stderr, "%s: FAILED: %v\n", f.Name, err)
			failed = true
			continue
		}
		if res.Skipped != "" {
			fmt.Fprintf(stdout, "%s: %s; live %s\n", f.Name, res.Skipped, mb(res.Live))
			continue
		}
		gz := ""
		if res.GzBytes > 0 {
			gz = fmt.Sprintf(" into %s (%s gzip)", res.Segment, mb(res.GzBytes))
		}
		fmt.Fprintf(stdout, "%s: %s %d line(s), %s, dated before %s%s; live %s -> %s, base %d -> %d\n",
			f.Name, verb, res.Lines, mb(res.Cut), cutoff.Format("2006-01-02"), gz, mb(res.Live), mb(res.LiveKept), res.Base, res.Base+res.Cut)
	}
	if failed {
		return 1
	}
	return 0
}

// Cutoff is the start of the UTC day keep before now: every line dated on
// or after it stays live.
func Cutoff(now time.Time, keep time.Duration) time.Time {
	t := now.UTC().Add(-keep)
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
}

// MinKeep is the shortest -keep accepted: the longest window any params
// value in state.json has given a publication (the later of the payment
// promise timeout and the shard retention), plus minMargin. A data dir
// without state.json has no publications yet, so the margin alone.
func MinKeep(statePath string) (time.Duration, error) {
	raw, err := os.ReadFile(statePath)
	if errors.Is(err, os.ErrNotExist) {
		return minMargin, nil
	}
	if err != nil {
		return 0, err
	}
	var st scan.PersistState
	if err := json.Unmarshal(raw, &st); err != nil {
		return 0, fmt.Errorf("%s: %w", statePath, err)
	}
	var window time.Duration
	for _, e := range st.ParamHistory {
		for _, s := range []int64{e.ParamsJSON.PaymentPromiseTimeoutSeconds, e.ParamsJSON.ShardRetentionSeconds} {
			if d := time.Duration(s) * time.Second; d > window {
				window = d
			}
		}
	}
	return window + minMargin, nil
}

// exportOffsets reads how far the daily export has read each file
// (exports/state.json); nil when exports were never built, which caps
// nothing.
func exportOffsets(dir string) (map[string]int64, error) {
	raw, err := os.ReadFile(filepath.Join(dir, "state.json"))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var st struct {
		Offsets map[string]int64 `json:"offsets"`
	}
	if err := json.Unmarshal(raw, &st); err != nil {
		return nil, fmt.Errorf("exports state: %w", err)
	}
	if st.Offsets == nil {
		st.Offsets = map[string]int64{}
	}
	return st.Offsets, nil
}

func selectFiles(dataDir, only string) ([]FileSpec, error) {
	all, err := archived(dataDir)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(only) == "" {
		return all, nil
	}
	var out []FileSpec
	for _, n := range strings.Split(only, ",") {
		n = strings.TrimSpace(n)
		found := false
		for _, f := range all {
			if f.Name == n {
				out = append(out, f)
				found = true
			}
		}
		if !found {
			return nil, fmt.Errorf("%q is not an archived file (%s)", n, names(all))
		}
	}
	return out, nil
}

func names(specs []FileSpec) string {
	var n []string
	for _, f := range specs {
		n = append(n, f.Name)
	}
	return strings.Join(n, ",")
}

// printLogicalEnd prints name's logical end (record.LogicalEnd): where
// deploy/vantage-pull.sh resumes appending another host's copy of a file,
// so that a rotation, which shortens the live file, does not make it fetch
// again bytes the record already holds. A file that does not exist and was
// never archived ends at 0: nothing of it is held yet. One that is missing
// while its archive holds segments is an error, since resuming from 0
// would append the archived bytes a second time.
func printLogicalEnd(dataDir, name string, stdout, stderr io.Writer) int {
	if !filepath.IsLocal(filepath.FromSlash(name)) {
		fmt.Fprintf(stderr, "observer-archive: -logical-end %q is not a path inside the data directory\n", name)
		return 2
	}
	path := filePath(dataDir, name)
	end, err := record.LogicalEnd(path)
	if errors.Is(err, os.ErrNotExist) {
		idx, ierr := record.LoadIndex(path)
		switch {
		case ierr != nil:
			err = ierr
		case len(idx.Segments) > 0 || len(idx.Generations) > 0:
			err = fmt.Errorf("%s is missing, but its archive (%s) holds logical bytes before it; nothing to resume from", path, record.ArchiveDir(path))
		default:
			end, err = 0, nil
		}
	}
	if err != nil {
		fmt.Fprintln(stderr, "observer-archive:", err)
		return 1
	}
	fmt.Fprintln(stdout, end)
	return 0
}

func verifyAll(dataDir string, specs []FileSpec, stdout, stderr io.Writer) int {
	failed := false
	for _, f := range specs {
		n, err := record.Verify(filePath(dataDir, f.Name))
		switch {
		case errors.Is(err, os.ErrNotExist):
			fmt.Fprintf(stdout, "%s: no such file\n", f.Name)
		case err != nil:
			fmt.Fprintf(stderr, "%s: FAILED: %v\n", f.Name, err)
			failed = true
		default:
			fmt.Fprintf(stdout, "%s: %d segment(s) verified\n", f.Name, n)
		}
	}
	if failed {
		return 1
	}
	return 0
}

func statusAll(dataDir string, specs []FileSpec, stdout, stderr io.Writer) int {
	for _, f := range specs {
		s, err := record.Open(filePath(dataDir, f.Name))
		if errors.Is(err, os.ErrNotExist) {
			fmt.Fprintf(stdout, "%s: no such file\n", f.Name)
			continue
		}
		if err != nil {
			fmt.Fprintf(stderr, "%s: %v\n", f.Name, err)
			return 1
		}
		idx := s.Index()
		var gz, lines int64
		retired := 0
		for _, sg := range idx.Segments {
			if sg.To > s.Base() {
				continue
			}
			lines += sg.Lines
			// A retired segment's file is gone: its bytes are on disk
			// only in the exports.
			if sg.Retired != nil {
				retired++
			} else {
				gz += sg.GzBytes
			}
		}
		since := "never archived"
		if !idx.LiveSince.IsZero() {
			since = "live since " + idx.LiveSince.Format("2006-01-02")
		}
		ret := ""
		if retired > 0 {
			ret = fmt.Sprintf(" (%d retired, read from the exports)", retired)
		}
		fmt.Fprintf(stdout, "%s: base %d, live %s, end %d; %d segment(s)%s, %d line(s), %s gzip; %s\n",
			f.Name, s.Base(), mb(s.End()-s.Base()), s.End(), len(idx.Segments), ret, lines, mb(gz), since)
		s.Close()
	}
	return 0
}

func mb(n int64) string { return fmt.Sprintf("%.1f MB", float64(n)/1e6) }
