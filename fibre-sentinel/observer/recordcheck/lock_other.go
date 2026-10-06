//go:build !unix

package recordcheck

// Without flock the ledger's writers are not kept apart, and a directory is not synced like a file. The observer runs
// on Linux; this is for building and testing elsewhere, where one writer runs at a time.
const ledgerLocking = false

func lockLedger(string) (func(), error) { return func() {}, nil }

func syncDir(string) error { return nil }
