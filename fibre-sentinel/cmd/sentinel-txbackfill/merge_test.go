package main

import (
	"bytes"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/failedtx"
	"github.com/plsgiveup/fibre/fibre-sentinel/internal/scan"
	"github.com/plsgiveup/fibre/fibre-sentinel/internal/txcost"
)

// procWith is a /proc holding one process per cmdline (arguments separated
// by spaces), pids 10, 20 and on, beside a kernel thread (pid 2, an empty
// cmdline) and self.
func procWith(t *testing.T, cmdlines ...string) string {
	t.Helper()
	root := t.TempDir()
	write := func(pid, raw string) {
		t.Helper()
		dir := filepath.Join(root, pid)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "cmdline"), []byte(raw), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write("2", "")
	write("self", "/usr/local/bin/sentinel-txbackfill\x00")
	for k, c := range cmdlines {
		write(strconv.Itoa(10*(k+1)), strings.ReplaceAll(c, " ", "\x00")+"\x00")
	}
	return root
}

// A scanner of the data directory is found by its -data-dir, however the
// flag is written; one of another directory, another program, or no /proc
// at all is none.
func TestRunningScanner(t *testing.T) {
	data := t.TempDir()
	for _, c := range []struct {
		name string
		cmds []string
		want int
	}{
		{"the unit's command", []string{"/usr/bin/bash -c sleep", "/usr/local/bin/sentinel-scan -rpc http://127.0.0.1:26657 -data-dir " + data + " -start-height 0 -skip-heights= -follow -poll 1s"}, 20},
		{"-data-dir=", []string{"sentinel-scan --data-dir=" + data + "/ -follow"}, 10},
		{"the last one wins", []string{"sentinel-scan -data-dir /elsewhere -data-dir " + data}, 10},
		{"another data directory", []string{"/usr/local/bin/sentinel-scan -data-dir /var/lib/fibre-observer/mainnet -follow"}, 0},
		{"another program on it", []string{"/usr/local/bin/observer-collector -data-dir " + data}, 0},
		{"no scanner", []string{"/usr/local/bin/sentinel-txbackfill -merge -data-dir " + data}, 0},
	} {
		got, err := runningScanner(procWith(t, c.cmds...), data)
		if err != nil || got != c.want {
			t.Errorf("%s: pid %d (%v), want %d", c.name, got, err, c.want)
		}
	}
	if got, err := runningScanner(filepath.Join(t.TempDir(), "no-proc"), data); err != nil || got != 0 {
		t.Errorf("no /proc: %d %v", got, err)
	}
}

// stagedFixture is a finished staging of the test chain, and a data
// directory whose files hold newer lines, as the scanner wrote them, and
// one of the staged lines already.
func stagedFixture(t *testing.T) (cfg stageConfig, liveFailed, liveCosts string) {
	t.Helper()
	c := testChain(t)
	local, a := newFakeNode(t, c, 140, 200), newFakeNode(t, c, 1, 200)
	liveFailed = `{"schema_version":1,"dedupe_key":"h601:0","height":601,"time":"2026-10-09T00:00:00Z","app_version":10,"tx_hash":"` + strings.Repeat("ab", 32) + `","tx_index":0,"code":5,"codespace":"sdk","log":"x","gas_wanted":1,"gas_used":1,"ante_passed":false,"messages":[],"recorded_at":"2026-10-09T00:00:01Z"}` + "\n"
	liveCosts = `{"schema_version":1,"dedupe_key":"h602:3","height":602,"tx_index":3,"time":"2026-10-09T00:00:00Z","tx_hash":"` + strings.Repeat("cd", 32) + `","gas_wanted":1,"gas_used":1,"messages":[],"recorded_at":"2026-10-09T00:00:01Z"}` + "\n"
	cfg = config(t, dataDir(t, map[string]string{failedtx.FileName: liveFailed, txcost.FileName: liveCosts}))
	if out, err := stageOnce(t, cfg, local, a); err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
	// the scanner, after the staging, wrote one of its lines itself (a re-scan)
	staged := readFile(t, filepath.Join(cfg.OutDir, txcost.FileName))
	first := staged[:strings.Index(staged, "\n")+1]
	liveCosts += first
	if err := os.WriteFile(filepath.Join(cfg.DataDir, txcost.FileName), []byte(liveCosts), 0o644); err != nil {
		t.Fatal(err)
	}
	return cfg, liveFailed, liveCosts
}

// The merge appends every staged line the live file does not hold, byte for
// byte, after the lines it holds, and prints what it did; run again it
// appends nothing. The scanner's store then opens the files, its seen-sets
// holding the keys above its checkpoint alone.
func TestMergeAppendsTheStagedLines(t *testing.T) {
	cfg, liveFailed, liveCosts := stagedFixture(t)
	proc := procWith(t, "/usr/local/bin/observer-api -data-dir "+cfg.DataDir)
	var out bytes.Buffer
	if err := mergeStaging(cfg.OutDir, cfg.DataDir, proc, &out); err != nil {
		t.Fatalf("%v\n%s", err, out.String())
	}
	stagedFailed := readFile(t, filepath.Join(cfg.OutDir, failedtx.FileName))
	stagedCosts := readFile(t, filepath.Join(cfg.OutDir, txcost.FileName))
	firstCost := stagedCosts[:strings.Index(stagedCosts, "\n")+1]
	failed, costs := readFile(t, filepath.Join(cfg.DataDir, failedtx.FileName)), readFile(t, filepath.Join(cfg.DataDir, txcost.FileName))
	if failed != liveFailed+stagedFailed {
		t.Errorf("failed_txs.jsonl:\n%s\nwant:\n%s", failed, liveFailed+stagedFailed)
	}
	if costs != liveCosts+strings.TrimPrefix(stagedCosts, firstCost) {
		t.Errorf("tx_costs.jsonl:\n%s\nwant:\n%s", costs, liveCosts+strings.TrimPrefix(stagedCosts, firstCost))
	}
	for _, w := range []string{"failed_txs.jsonl: 3 staged line(s), 0 already on record, 3 appended", "tx_costs.jsonl: 4 staged line(s), 1 already on record, 3 appended"} {
		if !strings.Contains(out.String(), w) {
			t.Errorf("not printed: %q in\n%s", w, out.String())
		}
	}

	out.Reset()
	if err := mergeStaging(cfg.OutDir, cfg.DataDir, proc, &out); err != nil {
		t.Fatal(err)
	}
	if readFile(t, filepath.Join(cfg.DataDir, failedtx.FileName)) != failed || readFile(t, filepath.Join(cfg.DataDir, txcost.FileName)) != costs ||
		!strings.Contains(out.String(), "4 staged line(s), 4 already on record, 0 appended") {
		t.Fatalf("the merge run again changed the files, or said:\n%s", out.String())
	}

	st, err := scan.OpenStore(cfg.DataDir)
	if err != nil {
		t.Fatalf("the scanner's store over the merged files: %v", err)
	}
	defer st.Close()
	if !st.FailedTxSeen("h601:0") || !st.TxCostSeen("h602:3") || st.FailedTxSeen("h105:0") || st.TxCostSeen("h101:1") {
		t.Fatal("the store's seen-sets are not the keys above its checkpoint")
	}
}

// The merge refuses, appending nothing, while the scanner runs on the data
// directory, over a staging not finished or of another chain, and over a
// staged line that is not a usable record of its place.
func TestMergeRefuses(t *testing.T) {
	cfg, liveFailed, liveCosts := stagedFixture(t)
	unchanged := func(what string) {
		t.Helper()
		if readFile(t, filepath.Join(cfg.DataDir, failedtx.FileName)) != liveFailed || readFile(t, filepath.Join(cfg.DataDir, txcost.FileName)) != liveCosts {
			t.Fatalf("%s: the live files changed", what)
		}
	}
	none := procWith(t)
	var out bytes.Buffer

	running := procWith(t, "/usr/local/bin/sentinel-scan -rpc http://127.0.0.1:26657 -data-dir "+cfg.DataDir+" -follow")
	if err := mergeStaging(cfg.OutDir, cfg.DataDir, running, &out); err == nil || !strings.Contains(err.Error(), "is running") {
		t.Fatalf("a running scanner: %v", err)
	}
	unchanged("a running scanner")

	// Each bad file is as long as the staged one, so that each is refused
	// for what is wrong in it, not for its length.
	costsPath := filepath.Join(cfg.OutDir, txcost.FileName)
	good := readFile(t, costsPath)
	firstEnd := strings.Index(good, "\n") + 1
	for name, bad := range map[string]string{
		"undecodable":    strings.Replace(good, `"tx_hash":"`, `"tx_hash": `, 1),
		"out of order":   good[strings.Index(good[firstEnd:], "\n")+firstEnd+1:] + good[:strings.Index(good[firstEnd:], "\n")+firstEnd+1],
		"another key":    strings.Replace(good, `"dedupe_key":"h101:1"`, `"dedupe_key":"h101:2"`, 1),
		"outside":        strings.Replace(good, `"dedupe_key":"h101:1","height":101`, `"dedupe_key":"h99:1", "height":99 `, 1),
		"no time":        regexp.MustCompile(`"time":"[^"]*"`).ReplaceAllLiteralString(good[:firstEnd], `"time":"0001-01-01T00:00:00.000000000Z"`) + good[firstEnd:],
		"a cut last one": good[:len(good)-1] + " ",
	} {
		if len(bad) != len(good) || bad == good {
			t.Fatalf("%s: the case is not the staged file's length, or is the file", name)
		}
		if err := os.WriteFile(costsPath, []byte(bad), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := mergeStaging(cfg.OutDir, cfg.DataDir, none, &out); err == nil {
			t.Errorf("%s: merged", name)
		}
		unchanged(name)
	}
	if err := os.WriteFile(costsPath, []byte(good), 0o644); err != nil {
		t.Fatal(err)
	}

	p, _ := loadProgress(cfg.OutDir)
	p.Next = 150
	if err := saveProgress(cfg.OutDir, p); err != nil {
		t.Fatal(err)
	}
	if err := mergeStaging(cfg.OutDir, cfg.DataDir, none, &out); err == nil || !strings.Contains(err.Error(), "not finished") {
		t.Fatalf("an unfinished staging: %v", err)
	}
	p.Next, p.ChainID = 161, "other-1"
	if err := saveProgress(cfg.OutDir, p); err != nil {
		t.Fatal(err)
	}
	if err := mergeStaging(cfg.OutDir, cfg.DataDir, none, &out); err == nil || !strings.Contains(err.Error(), "chain") {
		t.Fatalf("another chain's staging: %v", err)
	}
	unchanged("the progress refused")
}
