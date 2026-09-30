package api

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
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
//   - a digest of its own body (sealDerived), so a file edited, or damaged
//     in a way that still parses, is refused whole rather than believed
//     beyond the entries the load reads again;
//   - a definition: a digest of the SQL and constants its content is
//     computed with and of the version of the Go that computes it
//     (memoVersion, ledgerVersion), so a build that computes it differently
//     rebuilds it;
//   - a high-water mark: the rowid of the newest row it was computed from
//     and that row's own key. A store restored from an older backup does not
//     have the row; one whose newest rows were deleted and the rowids used
//     again has another row under it.
//
// and the newest of its entries are looked up again and compared. Any doubt
// is a rebuild: a file whose digest is not its body's, that does not parse,
// is of another format or definition, names another store or another
// schema, or whose mark or entries do not match what the store holds now is
// refused, and the memo or ledger is built from the store as it was before
// there were files. The file is left for the next write to replace, not
// removed: another process may have written a good one in its place since
// it was read. A query that fails while a file is checked is an error of
// the computation, which the next one retries; it proves nothing about the
// file.
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
// and a store built anew gets another), the chain it records, and the
// schema it is at (the highest schema_migrations version).
//
// The schema is there because a migration may rewrite, for old rows, a
// column a file was computed from: a backfill of assignments.attested, say.
// The load reads again only the rows a file publishes, so rows that a
// migration brought into the population below the file's mark would never
// be folded in. A file from before a migration is refused, and the memo or
// ledger rebuilt once.
//
// For that, a file names the identity the store had when what it holds
// began to be read, not the one the store has when the file is written. A
// process that is running when the collector migrates (the warm-up keeps
// the old API serving for minutes after) holds rows read before the
// migration, and a file it wrote after under the new schema would be
// believed. So the memo and the ledger keep the identity they were built
// under: the file's, when open kept one, or the store's, read before the
// first row, when they were built from nothing. A write that finds the
// store at another identity writes nothing, and they are built again from
// the store as it is now.
type storeIdentity struct {
	Created string `json:"created"`
	ChainID string `json:"chain_id"`
	Schema  int    `json:"schema"`
}

func readStoreIdentity(ctx context.Context, db *sql.DB) (storeIdentity, error) {
	var id storeIdentity
	if err := db.QueryRowContext(ctx, `SELECT applied_at, (SELECT MAX(version) FROM schema_migrations)
			FROM schema_migrations WHERE version = 1`).Scan(&id.Created, &id.Schema); err != nil {
		return id, err
	}
	err := db.QueryRowContext(ctx, `SELECT value FROM meta WHERE key = 'chain_id'`).Scan(&id.ChainID)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return id, err
	}
	return id, nil
}

// storeChange says how a store's identity moved from was to now.
func storeChange(was, now storeIdentity) string {
	if was.Created != now.Created || was.ChainID != now.ChainID {
		return fmt.Sprintf("the store is another one (created %s, chain %s), not the one it was built from (created %s, chain %s)",
			now.Created, now.ChainID, was.Created, was.ChainID)
	}
	return fmt.Sprintf("the store moved from schema version %d to %d", was.Schema, now.Schema)
}

// derivedHeader opens every derived file.
type derivedHeader struct {
	// Digest is the sha256 of the file as it is with this field empty
	// (sealDerived). It is the first field so that it opens the file,
	// where readDerived finds it.
	Digest     string        `json:"digest"`
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
	switch {
	case h.Store.Created != id.Created || h.Store.ChainID != id.ChainID:
		return refusal("computed from another store (created " + h.Store.Created + ", chain " + h.Store.ChainID + ")"), nil
	case h.Store.Schema != id.Schema:
		return refusal(fmt.Sprintf("computed under schema version %d, the store is at %d", h.Store.Schema, id.Schema)), nil
	}
	return "", nil
}

// readDerived reads path into v. A missing file is ok false with no
// refusal; one whose digest is not its body's, or that does not parse, is
// refused.
func readDerived(path string, v any) (ok bool, why refusal) {
	b, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, ""
		}
		return false, refusal("unreadable: " + err.Error())
	}
	if why := unsealDerived(b); why != "" {
		return false, why
	}
	if err := json.Unmarshal(b, v); err != nil {
		return false, refusal("does not parse: " + err.Error())
	}
	return true, ""
}

// writeDerived writes v, whose digest must be empty, to path sealed
// (sealDerived), and whole or not at all: a temporary file of this write's
// own, synced and then renamed over path, so a reader never meets half a
// file. Two writers of one path, two processes over one directory, say,
// never write into each other's temporary file or rename it away: with one
// name for every writer, one truncated the other's while it was being
// renamed, and published half a file or failed.
func writeDerived(path string, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	if b, err = sealDerived(b); err != nil {
		return err
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, filepath.Base(path)+".*.tmp")
	if err != nil {
		return err
	}
	_, err = tmp.Write(b)
	if err == nil {
		err = tmp.Chmod(0o644)
	}
	if err == nil {
		err = tmp.Sync()
	}
	if cerr := tmp.Close(); err == nil {
		err = cerr
	}
	if err == nil {
		err = os.Rename(tmp.Name(), path)
	}
	if err != nil {
		_ = os.Remove(tmp.Name())
	}
	return err
}

// A file's digest covers every byte of it but the digest's own. The load
// reads again only the newest of what a file holds, so a file edited, or
// damaged in a way that still parses, below them would otherwise be
// believed: an entry moved to another value, a validator dropped from the
// ledger. The digest is what lets the load trust the rest; the checks
// against the store are what tell it the store still is the one the file
// was computed from.
const (
	digestOpen  = `{"digest":"`
	digestEmpty = digestOpen + `"`
)

// sealDerived puts in body, a derived file with its digest empty, the
// sha256 of body itself.
func sealDerived(body []byte) ([]byte, error) {
	if !bytes.HasPrefix(body, []byte(digestEmpty)) {
		return nil, errors.New("a derived file must open with its digest, empty")
	}
	sum := sha256.Sum256(body)
	out := make([]byte, 0, len(body)+2*sha256.Size)
	out = append(out, digestOpen...)
	out = hex.AppendEncode(out, sum[:])
	return append(out, body[len(digestOpen):]...), nil
}

// unsealDerived refuses b unless it opens with a digest that is the sha256
// of b with that digest emptied.
func unsealDerived(b []byte) refusal {
	n := len(digestOpen) + 2*sha256.Size
	if len(b) <= n || !bytes.HasPrefix(b, []byte(digestOpen)) || b[n] != '"' {
		return "no digest where the file opens"
	}
	h := sha256.New()
	h.Write([]byte(digestOpen))
	h.Write(b[n:])
	if hex.EncodeToString(h.Sum(nil)) != string(b[len(digestOpen):n]) {
		return "its digest is not its body's"
	}
	return ""
}
