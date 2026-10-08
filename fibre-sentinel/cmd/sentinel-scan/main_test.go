package main

import (
	"errors"
	"os"
	"os/exec"
	"strings"
	"testing"
)

// -include-failed is retired: a scanner started with it set stops at once,
// saying where failed Fibre transactions are recorded, before it opens its
// data directory. The stop exits the process, so main runs in a child: this
// test binary again, told by the environment to be the scanner.
func TestIncludeFailedIsRetired(t *testing.T) {
	if dir := os.Getenv("SENTINEL_SCAN_RETIRED_FLAG_DIR"); dir != "" {
		os.Args = []string{"sentinel-scan", "-include-failed=true", "-data-dir", dir, "-rpc", "http://127.0.0.1:1"}
		main()
		return
	}
	dir := t.TempDir()
	cmd := exec.Command(os.Args[0], "-test.run=^TestIncludeFailedIsRetired$")
	cmd.Env = append(os.Environ(), "SENTINEL_SCAN_RETIRED_FLAG_DIR="+dir)
	out, err := cmd.CombinedOutput()
	var ee *exec.ExitError
	if !errors.As(err, &ee) || ee.ExitCode() != 1 {
		t.Fatalf("the scanner did not stop with exit 1: %v\n%s", err, out)
	}
	if !strings.Contains(string(out), "-include-failed is retired") || !strings.Contains(string(out), "failed_txs.jsonl") {
		t.Fatalf("the stop does not say why:\n%s", out)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("the data directory was written before the stop: %d entries", len(entries))
	}
}
