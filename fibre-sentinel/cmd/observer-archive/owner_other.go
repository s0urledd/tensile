//go:build !unix

package main

import "os"

// ownLike does nothing where files have no unix owner to hand over.
func ownLike(string, os.FileInfo) error { return nil }
