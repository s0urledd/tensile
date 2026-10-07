//go:build unix

package scan

import (
	"os"
	"sync"
	"syscall"
	"testing"
)

// holdShared takes the shared flock on path that a writer holds for one
// write, as another process mid-write would, and returns its release (safe
// to call more than once).
func holdShared(t *testing.T, path string) (release func()) {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_SH); err != nil {
		f.Close()
		t.Fatal(err)
	}
	var once sync.Once
	return func() {
		once.Do(func() {
			_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
			f.Close()
		})
	}
}
