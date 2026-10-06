// Command record-verify checks, for each finished day, that the store gives back the day's record files byte for
// byte: the check that must pass before a day's JSONL may ever leave the disk. It only reads and reports. It deletes
// nothing, and writes nothing but its own report and, with -ledger, the ledger of what it found.
//
//	record-verify -db observer.db -exports DIR [-day 2026-10-05 ...] [-json report.json] [-ledger DIR/verified.json]
//	record-verify -db observer.db -file measurements.jsonl [-from-line N]
//
// For each day with an export, today excluded:
//
//   - the export: the tarball's bytes and digest against index.json and its .sha256 sidecar, and each member's
//     bytes, lines and digest against the manifest inside it;
//   - each record file the store keeps one row per line of (publications, measurements, reachability, payments, and
//     each other vantage's heartbeats, vantages/<name>/reachability.jsonl): every line is looked up by its key and
//     written back from the store (store.Record, store.ProbeRecord and store.ReachRecord for a slim row, the stored
//     line otherwise). The lines written back, in the export's order, make the file again, and its digest is
//     compared with the member's.
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
// decisions, amendments, ranges, corrections) are not stored line by line and are listed as not checked. The check
// itself is observer/recordcheck; this command prints it.
//
// With -ledger, each day checked is merged into the ledger at that path (observer-archive -retire reads the exports
// directory's verified.json): the tarball's digest, when and by which build it was checked, and each checked
// member's answer. A day the ledger already holds under the tarball's current digest is listed from the ledger and
// not checked again; one whose tarball changed since is checked again. A day named with -day is always checked, and
// its entry replaced. A check whose answer is not about the tarball's bytes alone (the index entry or the sidecar
// disagreeing with them, a read error) is not recorded, so the day is checked again on the next run.
//
// With -file, one record file is checked line by line the same way, from line N on: how the first rows a build
// writes are checked before their day is exported.
//
// Exit status: 0 every day checked, or listed from the ledger, is reproducible; 1 one is not; 2 bad usage or a read
// error.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/status"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/export"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/recordcheck"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
)

func main() {
	dbPath := flag.String("db", "", "the observer's store, opened read-only")
	dir := flag.String("exports", "", "the exports directory: index.json and the tarballs")
	file := flag.String("file", "", "check this record file against the store instead of the exports")
	fromLine := flag.Int64("from-line", 1, "with -file, the first line to check")
	jsonOut := flag.String("json", "", "write the whole report here as JSON too")
	ledgerPath := flag.String("ledger", "", "with -exports, merge each day checked into this ledger (the exports dir's "+recordcheck.LedgerFile+
		" is the one observer-archive -retire reads); a day it holds under the tarball's current digest is not checked again unless named with -day")
	var days multi
	flag.Var(&days, "day", "a day to check, repeatable (default every finished day with an export)")
	flag.Parse()
	if *dbPath == "" || (*dir == "") == (*file == "") || (*ledgerPath != "" && *dir == "") {
		fmt.Fprintln(os.Stderr, "usage: record-verify -db observer.db (-exports DIR [-day D ...] [-ledger FILE] | -file F [-from-line N]) [-json report.json]")
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
		if !recordcheck.Checked(name) {
			fatal(fmt.Errorf("%s: the store does not keep this file line by line", name))
		}
		f, err := os.Open(*file)
		if err != nil {
			fatal(err)
		}
		defer f.Close()
		r, err := recordcheck.CheckLines(ctx, st, name, f, *fromLine)
		if err != nil {
			fatal(err)
		}
		printFile(os.Stdout, "", r)
		if err := writeJSON(*jsonOut, r); err != nil {
			fatal(err)
		}
		if r.NotBack() > 0 {
			os.Exit(1)
		}
		return
	}

	_, code, err := verifyDays(ctx, st, *dir, days, *ledgerPath, *jsonOut, time.Now().UTC().Format("2006-01-02"), os.Stdout)
	if err != nil {
		fatal(err)
	}
	if code != 0 {
		os.Exit(code)
	}
}

// verifyDays checks each day of the exports directory dir's index before today, or only the days named when days
// names any, and prints it to out. With a ledger, a day the ledger holds under the tarball's current digest is listed
// from it instead, unless it was named, and each day checked is merged into the ledger. It writes the reports to
// jsonOut when that is set, prints the summary and returns the reports of the days it checked and the exit status; the
// error is a read error, which is exit status 2.
func verifyDays(ctx context.Context, st *store.Store, dir string, days []string, ledgerPath, jsonOut, today string, out io.Writer) ([]recordcheck.DayReport, int, error) {
	idx, err := export.ReadIndex(dir)
	if err != nil {
		return nil, 2, err
	}
	var ledger recordcheck.Ledger
	if ledgerPath != "" {
		if ledger, err = recordcheck.ReadLedger(ledgerPath); err != nil {
			return nil, 2, err
		}
	}
	sort.Slice(idx, func(i, j int) bool { return idx[i].Day < idx[j].Day })
	want := map[string]bool{}
	for _, d := range days {
		want[d] = true
	}
	build := status.BuildRevision()
	reports := []recordcheck.DayReport{}
	held, heldNot := 0, 0
	for _, e := range idx {
		if e.Day >= today || (len(days) > 0 && !want[e.Day]) {
			continue
		}
		delete(want, e.Day)
		if ledger != nil && len(days) == 0 {
			// The ledger's answer stands for as long as the tarball is the one it was about. A tarball that cannot
			// be read is left to the check, which says why.
			if d, ok, err := ledger.Current(dir, e.Name); err == nil && ok {
				held++
				if !d.Reproducible() {
					heldNot++
				}
				printHeld(out, e, d)
				continue
			}
		}
		r, err := recordcheck.CheckDay(ctx, st, dir, e)
		if err != nil {
			return reports, 2, err
		}
		reports = append(reports, r)
		printDay(out, r)
		if ledgerPath != "" {
			// Merged after each day, so a long run that stops keeps the days it finished.
			if d, ok := r.LedgerDay(build, time.Now()); ok {
				if err := recordcheck.MergeLedger(ledgerPath, map[string]recordcheck.LedgerDay{e.Name: d}); err != nil {
					return reports, 2, err
				}
			}
		}
	}
	for d := range want {
		fmt.Fprintf(out, "day| %s: no finished export in the index\n", d)
	}
	if err := writeJSON(jsonOut, reports); err != nil {
		return reports, 2, err
	}
	ok := 0
	for _, r := range reports {
		if r.Reproducible {
			ok++
		}
	}
	if held > 0 {
		fmt.Fprintf(out, "summary| %d day(s) checked: %d reproducible byte for byte, %d not; %d more unchanged since the ledger's check: %d reproducible, %d not. Nothing was deleted.\n",
			len(reports), ok, len(reports)-ok, held, held-heldNot, heldNot)
	} else {
		fmt.Fprintf(out, "summary| %d day(s) checked: %d reproducible byte for byte, %d not. Nothing was deleted.\n", len(reports), ok, len(reports)-ok)
	}
	if ok < len(reports) || heldNot > 0 || len(want) > 0 {
		return reports, 1, nil
	}
	return reports, 0, nil
}

type multi []string

func (m *multi) String() string     { return strings.Join(*m, ",") }
func (m *multi) Set(v string) error { *m = append(*m, v); return nil }

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "record-verify:", err)
	os.Exit(2)
}

func writeJSON(path string, v any) error {
	if path == "" {
		return nil
	}
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(b, '\n'), 0o644)
}

func printDay(w io.Writer, r recordcheck.DayReport) {
	verdict := "reproducible byte for byte"
	if !r.Reproducible {
		verdict = "NOT reproducible"
	}
	intact := "intact"
	if !r.ExportIntact {
		intact = "NOT intact"
	}
	fmt.Fprintf(w, "day| %s %s: export %s; %s\n", r.Day, r.Export, intact, verdict)
	for _, e := range r.ExportErrors {
		fmt.Fprintf(w, "  export| %s\n", e)
	}
	for _, f := range r.Files {
		printFile(w, "  ", f)
	}
	if len(r.NotChecked) > 0 {
		fmt.Fprintf(w, "  not checked| not stored line by line: %s\n", strings.Join(r.NotChecked, ", "))
	}
}

// printHeld prints a day the ledger answers for, the tarball being the one it checked: its verdict, and why each
// member that is not reproducible is not.
func printHeld(w io.Writer, e export.Entry, d recordcheck.LedgerDay) {
	verdict := "reproducible byte for byte"
	if !d.Reproducible() {
		verdict = "NOT reproducible"
	}
	fmt.Fprintf(w, "day| %s %s: unchanged since checked at %s by %s; %s\n", e.Day, e.Name, d.CheckedAt.UTC().Format(time.RFC3339), d.Build, verdict)
	names := make([]string, 0, len(d.Files))
	for n := range d.Files {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		if m := d.Files[n]; !m.Reproducible {
			fmt.Fprintf(w, "  file| %s: %s\n", n, m.Why)
		}
	}
}

func printFile(w io.Writer, indent string, f recordcheck.FileReport) {
	fmt.Fprintf(w, "%sfile| %-19s %8d lines: %d identical (%d slim); %d missing, %d stripped, %d sampled out, %d repeated, %d different, %d unreadable; rebuilt %s",
		indent, f.Name, f.Lines, f.Identical, f.SlimRows, f.Missing, f.Stripped, f.SampledOut, f.Repeated, f.Different, f.Unreadable, short(f.RebuiltSHA))
	if f.SourceSHA256 != "" {
		fmt.Fprintf(w, ", source %s", short(f.SourceSHA256))
	}
	fmt.Fprintln(w)
	for _, x := range f.Examples {
		fmt.Fprintf(w, "%s  %s\n", indent, x)
	}
}

func short(s string) string {
	if len(s) > 12 {
		return s[:12]
	}
	return s
}
