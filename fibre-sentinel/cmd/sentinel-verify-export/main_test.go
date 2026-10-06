package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/observer/export"
)

// An export carrying another vantage's heartbeats, a member named by its
// path under the data dir (vantages/<name>/reachability.jsonl), passes the
// offline check like any other: the nested name is a member, not a stray.
func TestVerifyAcceptsAVantageMember(t *testing.T) {
	data := t.TempDir()
	p := filepath.Join(data, export.VantagesDir, "de-1", "reachability.jsonl")
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte(`{"started_at":"2026-09-10T10:00:00Z","vantage":"de-1"}`+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	b := &export.Builder{DataDir: data, Dir: filepath.Join(data, "exports"), Vantage: "v", Build: "x", Hour: 3}
	built, err := b.Run(time.Date(2026, 9, 11, 4, 0, 0, 0, time.UTC))
	if err != nil || len(built) != 1 {
		t.Fatalf("built %v err %v", built, err)
	}
	path := filepath.Join(data, "exports", built[0])
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if a, err := export.ReadArchive(raw); err != nil || len(a.Members["vantages/de-1/reachability.jsonl"]) == 0 {
		t.Fatalf("the export does not carry the vantage's file: %v", err)
	}
	if code := verify(path, path+".sig", nil, false); code != 0 {
		t.Fatalf("verify exit %d, want 0", code)
	}
}
