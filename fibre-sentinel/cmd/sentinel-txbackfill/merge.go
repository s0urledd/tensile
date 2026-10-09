package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/failedtx"
	"github.com/plsgiveup/fibre/fibre-sentinel/internal/scan"
	"github.com/plsgiveup/fibre/fibre-sentinel/internal/txcost"
)

// stagedLine is one line of a staged file, without its newline, and the
// place it names.
type stagedLine struct {
	line   []byte
	key    string
	height int64
	index  int
}

// mergeStaging appends the finished staging in outDir to the live files in
// dataDir. It refuses, before writing anything, when the staging is not
// finished, is of another chain, has a line that is not a usable record of
// its range or is out of order, or when a sentinel-scan process of dataDir
// runs (procRoot is /proc). The lines go through the scanner's own store,
// each one byte for byte as it was staged, a key the live file holds being
// left out; then the store is synced and closed. It prints what it did per
// file.
func mergeStaging(outDir, dataDir, procRoot string, out io.Writer) error {
	p, err := loadProgress(outDir)
	if err != nil {
		return err
	}
	if p == nil {
		return fmt.Errorf("%s holds no %s: stage first", outDir, progressFile)
	}
	if !p.complete() {
		return fmt.Errorf("the staging in %s is not finished (staged through %d of %d): run it again first", outDir, p.Next-1, p.last())
	}
	chainID, err := stateChainID(dataDir)
	if err != nil {
		return err
	}
	if chainID != p.ChainID {
		return fmt.Errorf("the staging is of chain %q, the data directory's is %q", p.ChainID, chainID)
	}
	if pid, err := runningScanner(procRoot, dataDir); err != nil {
		return fmt.Errorf("looking for a running scanner: %w", err)
	} else if pid != 0 {
		return fmt.Errorf("sentinel-scan (pid %d) is running on %s: stop it first (systemctl stop fibre-scan@<network>), and start it again after the merge", pid, dataDir)
	}
	failed, err := readStaged(filepath.Join(outDir, failedtx.FileName), p.Failed, p.From, p.FailedTo, checkFailed)
	if err != nil {
		return err
	}
	costs, err := readStaged(filepath.Join(outDir, txcost.FileName), p.Costs, p.From, p.CostsTo, checkCost)
	if err != nil {
		return err
	}

	// The store cuts a torn final line off each file as it opens, as the
	// scanner's start does; only then are the live keys read, every line's.
	st, err := scan.OpenStore(dataDir)
	if err != nil {
		return err
	}
	closed := false
	defer func() {
		if !closed {
			st.Close()
		}
	}()
	appendAll := func(name string, lines []stagedLine, add func(stagedLine) error) error {
		live, err := liveKeys(filepath.Join(dataDir, name))
		if err != nil {
			return err
		}
		added := 0
		for _, l := range lines {
			if live[l.key] {
				continue
			}
			if err := add(l); err != nil {
				return fmt.Errorf("%s: %s: %w (%d appended before it; the merge run again appends the rest)", name, l.key, err, added)
			}
			added++
		}
		fmt.Fprintf(out, "%s: %d staged line(s), %d already on record, %d appended\n", name, len(lines), len(lines)-added, added)
		return nil
	}
	if err := appendAll(failedtx.FileName, failed, func(l stagedLine) error {
		var r failedtx.Record
		if err := json.Unmarshal(l.line, &r); err != nil {
			return err
		}
		return st.AppendFailedTxLine(r, l.line)
	}); err != nil {
		return err
	}
	if err := appendAll(txcost.FileName, costs, func(l stagedLine) error {
		var r txcost.Record
		if err := json.Unmarshal(l.line, &r); err != nil {
			return err
		}
		return st.AppendTxCostLine(r, l.line)
	}); err != nil {
		return err
	}
	if err := st.Sync(); err != nil {
		return err
	}
	closed = true
	return st.Close()
}

// readStaged reads a staged file whole and holds it to its staging: as long
// as progress.json's mark, every line whole and accepted by check, its key
// its own height and index, within [from, to], and each after the one
// before it in (height, tx index) order.
func readStaged(path string, m mark, from, to int64, check func([]byte) (stagedLine, error)) ([]stagedLine, error) {
	b, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) && m.Bytes == 0 {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if int64(len(b)) != m.Bytes {
		return nil, fmt.Errorf("%s holds %d bytes, its staging %d: it changed after the staging", path, len(b), m.Bytes)
	}
	if len(b) == 0 {
		return nil, nil
	}
	if b[len(b)-1] != '\n' {
		return nil, fmt.Errorf("%s does not end on a whole line", path)
	}
	var out []stagedLine
	for n, line := range bytes.Split(b[:len(b)-1], []byte("\n")) {
		l, err := check(line)
		if err != nil {
			return nil, fmt.Errorf("%s line %d: %w", path, n+1, err)
		}
		if l.key != failedtx.Key(l.height, l.index) {
			return nil, fmt.Errorf("%s line %d: dedupe_key %q is not its height and tx index", path, n+1, l.key)
		}
		if l.height < from || l.height > to {
			return nil, fmt.Errorf("%s line %d: height %d is outside the staging's %d..%d", path, n+1, l.height, from, to)
		}
		if k := len(out); k > 0 {
			if prev := out[k-1]; l.height < prev.height || (l.height == prev.height && l.index <= prev.index) {
				return nil, fmt.Errorf("%s line %d: %s after %s", path, n+1, l.key, prev.key)
			}
		}
		l.line = line
		out = append(out, l)
	}
	if int64(len(out)) != m.Lines {
		return nil, fmt.Errorf("%s holds %d lines, its staging %d", path, len(out), m.Lines)
	}
	return out, nil
}

// checkFailed and checkCost accept a line the collector's ingest accepts
// (ingest.FailedTxs, ingest.TxCosts): a merged line it would step over as a
// bad record would be on file and nowhere else.
func checkFailed(line []byte) (stagedLine, error) {
	var r failedtx.Record
	if err := json.Unmarshal(line, &r); err != nil {
		return stagedLine{}, err
	}
	if r.DedupeKey == "" || len(r.TxHash) != 64 || r.Code == 0 || r.Height <= 0 || r.TxIndex < 0 || r.Time.IsZero() {
		return stagedLine{}, errors.New("a failed tx without dedupe_key, tx_hash, code, height, tx_index or time")
	}
	return stagedLine{key: r.DedupeKey, height: r.Height, index: r.TxIndex}, nil
}

func checkCost(line []byte) (stagedLine, error) {
	var r txcost.Record
	if err := json.Unmarshal(line, &r); err != nil {
		return stagedLine{}, err
	}
	if r.DedupeKey == "" || len(r.TxHash) != 64 || r.Height <= 0 || r.TxIndex < 0 || r.Time.IsZero() {
		return stagedLine{}, errors.New("a tx cost without dedupe_key, tx_hash, height, tx_index or time")
	}
	return stagedLine{key: r.DedupeKey, height: r.Height, index: r.TxIndex}, nil
}

// runningScanner is the pid of a sentinel-scan process whose data
// directory is dataDir, read from <procRoot>/<pid>/cmdline, or 0 when there
// is none. The store holds no lock a second writer could see (a write takes
// its file's lock for that write only), and the process list is what says
// the scanner runs. A scanner whose -data-dir is relative is placed by its
// working directory, and counted as running when that cannot be read.
// Without procRoot (not Linux) it finds none.
func runningScanner(procRoot, dataDir string) (int, error) {
	want, err := filepath.Abs(dataDir)
	if err != nil {
		return 0, err
	}
	entries, err := os.ReadDir(procRoot)
	if errors.Is(err, fs.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	for _, e := range entries {
		pid, err := strconv.Atoi(e.Name())
		if err != nil || pid <= 0 {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(procRoot, e.Name(), "cmdline"))
		if err != nil || len(raw) == 0 {
			continue // gone since the listing, or a kernel thread
		}
		args := strings.Split(strings.TrimRight(string(raw), "\x00"), "\x00")
		if filepath.Base(args[0]) != "sentinel-scan" {
			continue
		}
		dir := flagValue(args[1:], "data-dir", "./sentinel-data")
		if !filepath.IsAbs(dir) {
			cwd, err := os.Readlink(filepath.Join(procRoot, e.Name(), "cwd"))
			if err != nil {
				return pid, nil
			}
			dir = filepath.Join(cwd, dir)
		}
		if samePath(dir, want) {
			return pid, nil
		}
	}
	return 0, nil
}

// flagValue is the value the flag package gives name from args (-name v,
// -name=v, with one dash or two; the last one wins), or def.
func flagValue(args []string, name, def string) string {
	v := def
	for i := 0; i < len(args); i++ {
		a := args[i]
		if a == "--" {
			break
		}
		if !strings.HasPrefix(a, "-") {
			continue
		}
		a = strings.TrimPrefix(strings.TrimPrefix(a, "-"), "-")
		switch {
		case a == name && i+1 < len(args):
			v = args[i+1]
			i++
		case strings.HasPrefix(a, name+"="):
			v = strings.TrimPrefix(a, name+"=")
		}
	}
	return v
}

// samePath compares two absolute paths cleaned and, where they exist, with
// their symlinks resolved.
func samePath(a, b string) bool {
	a, b = filepath.Clean(a), filepath.Clean(b)
	if a == b {
		return true
	}
	ra, errA := filepath.EvalSymlinks(a)
	rb, errB := filepath.EvalSymlinks(b)
	return errA == nil && errB == nil && ra == rb
}
