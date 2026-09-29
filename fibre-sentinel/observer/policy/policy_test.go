package policy

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// A policy file written for the budgets and sampling of the earlier model
// still loads, and only its secret file is read.
func TestAnEarlierPolicyFileStillLoads(t *testing.T) {
	path := filepath.Join(t.TempDir(), "policy.yaml")
	old := `caps:
  per_validator:
    min_request_spacing: 2s
  global:
    bytes_per_hour: 53687091200
sampling:
  enabled: false
  master_secret_file: /data/sampling-master.key
  projection_lookback: 1h
budget_state:
  unknown_cooldown: 10m
`
	if err := os.WriteFile(path, []byte(old), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Sampling.MasterSecretFile != "/data/sampling-master.key" {
		t.Fatalf("secret file %q", cfg.Sampling.MasterSecretFile)
	}
	for _, f := range []string{"policy.mocha.yaml", "policy.example.yaml"} {
		if _, err := Load(f); err != nil {
			t.Errorf("%s: %v", f, err)
		}
	}
}

func TestAPolicyNeedsItsSecretFile(t *testing.T) {
	var cfg Config
	if _, err := New(cfg); err == nil {
		t.Fatal("a policy with no master_secret_file was accepted")
	} else if !strings.Contains(err.Error(), "master_secret_file") {
		t.Fatalf("the refusal does not name what to set: %v", err)
	}

	var cfg2 Config
	cfg2.Sampling.MasterSecretFile = filepath.Join(t.TempDir(), "sampling-master.key")
	p2, err := New(cfg2)
	if err != nil {
		t.Fatalf("a configured secret file was refused: %v", err)
	}
	p3, err := New(cfg2)
	if err != nil {
		t.Fatal(err)
	}
	day := time.Date(2026, 9, 21, 0, 0, 0, 0, time.UTC)
	if p2.DayCommitment(day) != p3.DayCommitment(day) {
		t.Fatal("two policies over the same secret file produced different day commitments")
	}
	if p2.DayCommitment(day) == p2.DayCommitment(day.Add(24*time.Hour)) {
		t.Fatal("day commitments must differ")
	}
}
