//go:build unix

package main

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

// Run as root on a data directory nothing was archived in yet, -retire
// leaves the archive directory it makes for its report to the data's
// owner: the archive unit, running as that owner, must still be able to
// take the lock and archive there.
func TestRetireReportKeepsOwner(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("needs root to hand files to another owner")
	}
	dir := t.TempDir()
	const uid, gid = 65534, 65534
	if err := os.Chown(dir, uid, gid); err != nil {
		t.Fatal(err)
	}
	if code, out, errs := runAt(t, day0, "-data-dir", dir, "-retire", "-db", filepath.Join(dir, "observer.db")); code != 0 {
		t.Fatalf("-retire: %d\n%s%s", code, out, errs)
	}
	info, err := os.Stat(filepath.Join(dir, "archive"))
	if err != nil {
		t.Fatal(err)
	}
	if st := info.Sys().(*syscall.Stat_t); st.Uid != uid || st.Gid != gid {
		t.Fatalf("archive/ is owned by %d:%d, want %d:%d", st.Uid, st.Gid, uid, gid)
	}
}
