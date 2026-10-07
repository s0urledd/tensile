//go:build !unix

package status

// Without statfs the data disk is not measured: the report carries no disk
// figures and /v1/health no disk check. The observer runs on Linux; this is
// so the package builds and its tests run elsewhere.
const diskSupported = false

func diskOf(string) *Disk { return nil }
