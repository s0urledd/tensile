package main

import (
	"os"
	"path/filepath"
	"testing"
)

// Nothing is sampled any more: a prober started with -policy over a data
// directory without the master secret reveals nothing and creates no key.
// policy.New, which it called before, wrote a fresh one, and the reveal
// loop then published a secret a day for days with no draw.
func TestNoMasterSecretIsCreated(t *testing.T) {
	dir := t.TempDir()
	key := filepath.Join(dir, "sampling-master.key")

	p, why, err := revealerFor("", dir)
	if err != nil || p != nil || why != "" {
		t.Fatalf("no -policy: %v %v %q", p, err, why)
	}
	p, why, err = revealerFor("default", dir)
	if err != nil || p != nil || why == "" {
		t.Fatalf("-policy default without a key: revealer %v, err %v, why %q", p, err, why)
	}
	if _, err := os.Stat(key); !os.IsNotExist(err) {
		t.Fatalf("a master secret was created: %v", err)
	}

	// A key already there (the draws of September 2026) is still read.
	if err := os.WriteFile(key, []byte("0123456789abcdef0123456789abcdef"), 0o600); err != nil {
		t.Fatal(err)
	}
	p, why, err = revealerFor("default", dir)
	if err != nil || p == nil || why != "" {
		t.Fatalf("-policy default with a key: revealer %v, err %v, why %q", p, err, why)
	}
}
