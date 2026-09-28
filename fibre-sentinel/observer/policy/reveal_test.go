package policy

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// A day's secret is published once its reveal delay has passed, once, and
// what is published hashes to the commitment the rows carried.
func TestRevealDue(t *testing.T) {
	var cfg Config
	cfg.Sampling.AllowEphemeralSecret = true
	p, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), SecretsFile)
	now := time.Date(2026, 9, 18, 12, 0, 0, 0, time.UTC)
	days, err := p.RevealDue(path, now, 7*24*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	// 2026-09-10 ended 7 days and 12 hours ago: the newest due day; the
	// catch-up bound reaches 14 days further back
	if len(days) == 0 || days[len(days)-1] != "2026-09-10" || days[0] != "2026-08-27" {
		t.Fatalf("revealed %v", days)
	}
	for _, d := range days {
		if d > "2026-09-10" {
			t.Errorf("%s revealed before its delay passed", d)
		}
	}
	raw, _ := os.ReadFile(path)
	lines := strings.Split(strings.TrimSpace(string(raw)), "\n")
	if len(lines) != len(days) {
		t.Fatalf("%d lines for %d days", len(lines), len(days))
	}
	var last Reveal
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &last); err != nil {
		t.Fatal(err)
	}
	sec, err := hex.DecodeString(last.Secret)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(sec)
	day, _ := time.Parse("2006-01-02", last.Day)
	if hex.EncodeToString(sum[:]) != last.Commitment || last.Commitment != p.DayCommitment(day) {
		t.Errorf("revealed secret does not hash to the day's commitment: %+v", last)
	}
	if string(sec) != string(p.DaySecret(day)) {
		t.Error("revealed secret is not the day secret the draws used")
	}
	// the next day's reveal adds one line, and nothing is revealed twice
	days, err = p.RevealDue(path, now.Add(24*time.Hour), 7*24*time.Hour)
	if err != nil || len(days) != 1 || days[0] != "2026-09-11" {
		t.Fatalf("next day: %v %v", days, err)
	}
	days, err = p.RevealDue(path, now.Add(24*time.Hour), 7*24*time.Hour)
	if err != nil || len(days) != 0 {
		t.Fatalf("repeat: %v %v", days, err)
	}
	// a disabled delay reveals nothing
	if days, _ := p.RevealDue(path, now.Add(48*time.Hour), 0); len(days) != 0 {
		t.Errorf("after=0 revealed %v", days)
	}
}
