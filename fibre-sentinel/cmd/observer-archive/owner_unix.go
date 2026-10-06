//go:build unix

package main

import (
	"os"
	"syscall"
)

// ownLike gives path the owner and group of like, as internal/record does
// for what it makes. A run as the service user changes nothing; a run as
// root must not leave what it made to root.
func ownLike(path string, like os.FileInfo) error {
	st, ok := like.Sys().(*syscall.Stat_t)
	if !ok || os.Geteuid() != 0 {
		return nil
	}
	return os.Lchown(path, int(st.Uid), int(st.Gid))
}
