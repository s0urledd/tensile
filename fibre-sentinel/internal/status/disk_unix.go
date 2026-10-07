//go:build unix

package status

import "syscall"

// diskSupported is whether diskOf can measure the data disk here.
const diskSupported = true

// diskOf is the free and total bytes of the filesystem holding dir, nil when
// it cannot be read.
func diskOf(dir string) *Disk {
	var st syscall.Statfs_t
	if err := syscall.Statfs(dir, &st); err != nil || st.Blocks == 0 {
		return nil
	}
	bs := uint64(st.Bsize)
	d := &Disk{FreeBytes: st.Bavail * bs, TotalBytes: st.Blocks * bs}
	d.FreeShare = float64(d.FreeBytes) / float64(d.TotalBytes)
	return d
}
