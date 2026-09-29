package api

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
)

// Derived state kept across restarts.
//
// Two things the snapshots read are derived from the whole record rather
// than from a window: the memo of each publication's original_rows
// (origrows.go) and the ledger of each validator's newest endorsements
// (signing.go). Both are exact and cheap to keep up to date, and both cost,
// to rebuild, time in proportion to everything the store has ever held: the
// memo a JSON parse of every publication's 100 KB record (0.8 ms each, seven
// seconds for the September store's "all"), the ledger a pass over every
// assignment ever stored. A restart rebuilt both inside the first
// computations of the windows, whose timeouts do not grow with the store,
// and a computation cancelled on its timeout started again from nothing.
//
// So both are kept in files beside the snapshots (originalRowsFile,
// endorsementLedgerFile), read back when a computation first needs them and
// caught up from there, as they are while the process runs.
//
// A file is used only for the store it was computed from, and only while
// that store still holds everything the file was computed from. The API
// opens the store query_only and cannot write an identity into it, so the
// identity is what the store keeps anyway (storeIdentity), and each file
// carries, besides:
//
//   - a definition: a digest of the SQL and constants its content is
//     computed with, so a build that computes it differently rebuilds it;
//   - a high-water mark: the rowid of the newest row it was computed from
//     and that row's own key. A store restored from an older backup does not
//     have the row; one whose newest rows were deleted and the rowids used
//     again has another row under it.
//
// and the newest of its entries are looked up again and compared. Any doubt
// is a rebuild: a file that does not parse, is of another format or
// definition, names another store, or whose mark or entries do not match
// what the store holds now is removed, and the memo or ledger is built from
// the store as it was before there were files. A query that fails while a
// file is checked is an error of the computation, which the next one
// retries; it proves nothing about the file.
//
// An older build does not know the files and never reads them, so going
// back costs nothing. Coming forward again, a file written before the
// rollback has an older mark over a store that has only grown since, and is
// caught up like any other.
const (
	originalRowsFile      = "original-rows.json"
	endorsementLedgerFile = "endorsement-ledger.json"
)

// storeIdentity is what names a store without writing to it: the moment it
// was created (the applied_at of schema_migrations' version 1, written once
// by the process that created the file and never again, so a copy keeps it
// and a store built anew gets another) and the chain it records.
type storeIdentity struct {
	Created string `json:"created"`
	ChainID string `json:"chain_id"`
}

func readStoreIdentity(ctx context.Context, db *sql.DB) (storeIdentity, error) {
	var id storeIdentity
	if err := db.QueryRowContext(ctx, `SELECT applied_at FROM schema_migrations WHERE version = 1`).Scan(&id.Created); err != nil {
		return id, err
	}
	err := db.QueryRowContext(ctx, `SELECT value FROM meta WHERE key = 'chain_id'`).Scan(&id.ChainID)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return id, err
	}
	return id, nil
}

// derivedHeader opens every derived file.
type derivedHeader struct {
	Kind       string        `json:"kind"`
	Format     int           `json:"format"`
	Definition string        `json:"definition"`
	Store      storeIdentity `json:"store"`
}

// derivedFormat is the files' layout version.
const derivedFormat = 1

// definitionOf digests what a derived file's content is computed with.
func definitionOf(parts ...string) string {
	h := sha256.New()
	for _, p := range parts {
		h.Write([]byte(p))
		h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))
}

// refusal is why a derived file was not used.
type refusal string

// checkHeader refuses a file that is not kind under definition for the
// store db is.
func checkHeader(ctx context.Context, db *sql.DB, h derivedHeader, kind, definition string) (refusal, error) {
	switch {
	case h.Kind != kind:
		return refusal("kind " + h.Kind + ", not " + kind), nil
	case h.Format != derivedFormat:
		return "another format", nil
	case h.Definition != definition:
		return "computed with another definition", nil
	}
	id, err := readStoreIdentity(ctx, db)
	if err != nil {
		return "", err
	}
	if h.Store != id {
		return refusal("computed from another store (created " + h.Store.Created + ", chain " + h.Store.ChainID + ")"), nil
	}
	return "", nil
}

// readDerived reads path into v. A missing file is ok false with no
// refusal; one that does not parse is refused.
func readDerived(path string, v any) (ok bool, why refusal) {
	b, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, ""
		}
		return false, refusal("unreadable: " + err.Error())
	}
	if err := json.Unmarshal(b, v); err != nil {
		return false, refusal("does not parse: " + err.Error())
	}
	return true, ""
}

// writeDerived writes v to path whole or not at all: a temporary file
// renamed over it, so a reader never meets half a file.
func writeDerived(path string, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}
