package record

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// Retire removes the gzip file of path's archived segment named segment,
// whose bytes the daily exports r names hold, and records r in the index,
// so readers read those bytes from the exports from then on. What else
// must be proven before a local copy may go (the store, the remote backup)
// is the caller's to decide; Retire proves the one thing a reader will
// depend on, that the exports give back the segment's exact bytes:
//
//  1. under archive/.lock, as Archive runs, so the index is not rewritten
//     under a run and the backup never copies half a retirement;
//  2. read the segment back from the exports (every tarball and member
//     digest against the exports index, the range's length, lines and
//     SHA-256 against the segment) and refuse on any difference;
//  3. save the index with Retired set (atomically, as Archive saves it);
//  4. remove the file and fsync the directory.
//
// A crash between 3 and 4 leaves a retired segment whose file is still
// there; readers read the file while it is, and a second Retire checks the
// exports again and removes it. Retire is idempotent: a segment retired
// and gone is left alone, and one already retired keeps the Retired it was
// given first. A segment whose file is missing and that was never retired
// is refused: nothing proved its bytes are anywhere else.
func Retire(path, segment string, r Retired) error {
	if !rotationSupported {
		return ErrUnsupported
	}
	adir := ArchiveDir(path)
	if _, err := os.Stat(adir); err != nil {
		return fmt.Errorf("%s: no archive: %v", path, err)
	}
	lk, err := os.OpenFile(filepath.Join(filepath.Dir(adir), LockFile), os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	defer lk.Close()
	// Owned like the data directory, as Archive leaves it: the service
	// user must still be able to take the lock after a run as root.
	if di, err := os.Stat(filepath.Dir(path)); err == nil {
		if err := ownLike(lk.Name(), di); err != nil {
			return err
		}
	}
	if err := lockExclusive(lk); err != nil {
		return fmt.Errorf("archive lock: %w", err)
	}
	defer unlock(lk)

	idx, err := LoadIndex(path)
	if err != nil {
		return err
	}
	i := -1
	for j, sg := range idx.Segments {
		if sg.Name == segment {
			i = j
		}
	}
	if i < 0 {
		return fmt.Errorf("%s: no archived segment %s", path, segment)
	}
	sg := idx.Segments[i]
	file := filepath.Join(adir, sg.Name)
	if _, err := os.Stat(file); err != nil {
		switch {
		case errors.Is(err, os.ErrNotExist) && sg.Retired != nil:
			return nil
		case errors.Is(err, os.ErrNotExist):
			return fmt.Errorf("%s: segment %s is missing and was never retired; nothing proves its bytes are anywhere else", path, sg.Name)
		default:
			return err
		}
	}
	saved := sg.Retired != nil
	if !saved {
		if r.At.IsZero() {
			r.At = time.Now().UTC()
		}
		sg.Retired = &r
	}
	if err := checkFromExports(path, sg); err != nil {
		return fmt.Errorf("%w; its file is kept", err)
	}
	if !saved {
		idx.Segments[i].Retired = sg.Retired
		if err := idx.save(adir); err != nil {
			return err
		}
	}
	if err := os.Remove(file); err != nil {
		return err
	}
	return syncDir(adir)
}
