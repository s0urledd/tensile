//go:build !unix

package scan

import "testing"

// holdShared has no lock to take without flock. The tests that need it skip
// before they get here (rotationSupported): rotation never runs where flock
// is missing.
func holdShared(t *testing.T, path string) (release func()) {
	t.Helper()
	t.Fatalf("no flock on this platform to hold %s with", path)
	return nil
}
