//go:build unix

package recordcheck

import (
	"errors"
	"os"
	"syscall"
)

// ledgerLocking says whether MergeLedger keeps its writers apart on this platform.
const ledgerLocking = true

// lockLedger takes an exclusive flock on <path>.lock, a file beside the ledger kept for this alone: the ledger itself
// is replaced by a rename, so a lock on it would be on a file no longer at its name. The lock file is opened
// read-only, which flock allows, so a lock file a run as root left behind is still taken by the service user. Closing
// it, which the returned func does, releases the lock.
func lockLedger(path string) (func(), error) {
	f, err := os.OpenFile(path+".lock", os.O_RDONLY|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	for {
		err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX)
		if !errors.Is(err, syscall.EINTR) {
			break
		}
	}
	if err != nil {
		f.Close()
		return nil, err
	}
	return func() { f.Close() }, nil
}

// syncDir syncs the directory, so a rename in it survives a power loss. A filesystem that cannot sync a directory says
// EINVAL, and there is then nothing more to do.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	if err := d.Sync(); err != nil && !errors.Is(err, syscall.EINVAL) {
		return err
	}
	return nil
}
