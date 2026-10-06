package recordcheck

// The ledger is what the checks found, kept so that a day is checked once and not on every run: observer-archive
// -retire removes an archived segment's file only when every export holding its bytes is in the ledger, under the
// tarball's current digest, with the segment's file reproducible. A day whose tarball changed since (a rebuild) is
// checked again, because the ledger's answer was about other bytes. A check whose answer is not about the tarball's
// bytes alone (its index entry or sidecar disagreeing with it, a read error) is not recorded at all, so it cannot
// stand for the tarball once that cause is gone.
//
// An answer about the store stands like one about the export: the lines a store did not give back are almost always
// gone from it for good (sampled out, stripped, repeated), and checking every such day again on every run is what the
// ledger is there to spare. A day the store has caught up on since is checked again by naming it (record-verify
// -day), which replaces its entry.

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// LedgerFile is the ledger's name in the exports directory.
const LedgerFile = "verified.json"

// Ledger is <exports-dir>/verified.json: one entry per export tarball, by its name.
type Ledger map[string]LedgerDay

// LedgerDay is what one check of one tarball found.
type LedgerDay struct {
	// SHA256 is the tarball's digest when it was checked; the entry says nothing about a tarball with another.
	SHA256    string    `json:"sha256"`
	CheckedAt time.Time `json:"checked_at"`
	// Build is the revision of the code that checked it.
	Build string                  `json:"build"`
	Files map[string]LedgerMember `json:"files"`
}

// LedgerMember is one checked member of the tarball, by its member name.
type LedgerMember struct {
	Lines         int64  `json:"lines"`
	Identical     int64  `json:"identical"`
	Reproducible  bool   `json:"reproducible"`
	RebuiltSHA256 string `json:"rebuilt_sha256"`
	// Why is empty for a reproducible member, and otherwise says why it is not, in words.
	Why string `json:"why"`
}

// Reproducible reports whether every checked member of the day was rebuilt byte for byte. A day with no member
// recorded is not: nothing about it was shown.
func (d LedgerDay) Reproducible() bool {
	for _, m := range d.Files {
		if !m.Reproducible {
			return false
		}
	}
	return len(d.Files) > 0
}

// Holds is the ledger's entry for the tarball name when it was checked with the digest sha256, the one it has now.
func (l Ledger) Holds(name, sha256 string) (LedgerDay, bool) {
	d, ok := l[name]
	if !ok || sha256 == "" || d.SHA256 != sha256 {
		return LedgerDay{}, false
	}
	return d, true
}

// Current is the ledger's entry for the tarball name in the exports directory dir while the tarball there is still
// the one it checked. ok is false when the ledger has no entry for it or one about other bytes: the day is to be
// checked (again). The error is a tarball that cannot be read.
func (l Ledger) Current(dir, name string) (d LedgerDay, ok bool, err error) {
	sum, _, err := DigestFile(filepath.Join(dir, name))
	if err != nil {
		return LedgerDay{}, false, err
	}
	d, ok = l.Holds(name, sum)
	return d, ok, nil
}

// LedgerDay is the ledger's entry for the day checked, by build at at. ok is false when the answer is not about the
// tarball's bytes alone: they changed while they were read, its index entry or sidecar disagreed with them, or a file
// could not be read. Recorded under the digest, such an answer would outlive its cause for as long as the tarball
// stays; unrecorded, the day is checked again on the next run. Every checked member the manifest names is in the
// entry; on an export that is not intact none is reproducible, whatever its lines did, since the export is what a
// removed segment would be read back from.
func (r DayReport) LedgerDay(build string, at time.Time) (LedgerDay, bool) {
	if r.tarball == "" {
		return LedgerDay{}, false
	}
	d := LedgerDay{SHA256: r.tarball, CheckedAt: at.UTC(), Build: build, Files: map[string]LedgerMember{}}
	broken := ""
	if !r.ExportIntact {
		broken = "the export is not intact"
		if len(r.ExportErrors) > 0 {
			broken += " (" + r.ExportErrors[0] + ")"
		}
	}
	// A member the manifest names that was never reached in the tarball is recorded as not shown, with the reason.
	notRead := "not read from the tarball"
	if broken != "" {
		notRead = broken + "; " + notRead
	}
	for _, name := range r.checked {
		d.Files[name] = LedgerMember{Why: notRead}
	}
	for _, f := range r.Files {
		m := LedgerMember{Lines: f.Lines, Identical: f.Identical, Reproducible: f.Reproducible && broken == "", RebuiltSHA256: f.RebuiltSHA, Why: f.Why()}
		if broken != "" {
			m.Why = strings.TrimSuffix(broken+"; "+m.Why, "; ")
		}
		d.Files[f.Name] = m
	}
	return d, true
}

// Why says why the file is not reproducible: the lines not given back, by why, and the first example; empty when it
// is reproducible.
func (f FileReport) Why() string {
	if f.Reproducible {
		return ""
	}
	var parts []string
	for _, c := range []struct {
		n   int64
		why string
	}{{f.Missing, "missing"}, {f.Stripped, "stripped"}, {f.SampledOut, "sampled out"}, {f.Repeated, "repeated"}, {f.Different, "different"}, {f.Unreadable, "unreadable"}} {
		if c.n > 0 {
			parts = append(parts, fmt.Sprintf("%d lines %s", c.n, c.why))
		}
	}
	if len(parts) == 0 {
		return fmt.Sprintf("the lines written back make sha256 %s, the member is %s", f.RebuiltSHA, f.SourceSHA256)
	}
	why := strings.Join(parts, ", ")
	if len(f.Examples) > 0 {
		why += " (first " + f.Examples[0] + ")"
	}
	return why
}

// ReadLedger reads the ledger at path. A missing ledger is an empty one: no day checked yet.
func ReadLedger(path string) (Ledger, error) {
	raw, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return Ledger{}, nil
	}
	if err != nil {
		return nil, err
	}
	l := Ledger{}
	if err := json.Unmarshal(raw, &l); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	if l == nil {
		l = Ledger{}
	}
	return l, nil
}

// MergeLedger sets the days given in the ledger at path and keeps every other entry. The ledger is read again here,
// not taken from the caller, so a day another run recorded in the meantime is kept, and under an exclusive lock held
// until the new ledger is in place (lockLedger), so that two writers (record-verify -ledger by hand, observer-archive
// -retire on its timer) never both merge into the same old ledger and the later rename drops what the other added. It
// is written whole to a temporary file, synced and renamed over the old one, and the directory synced so the rename
// survives a power loss: a reader sees the old ledger or the new one, never a part.
func MergeLedger(path string, days map[string]LedgerDay) error {
	unlock, err := lockLedger(path)
	if err != nil {
		return err
	}
	defer unlock()
	l, err := ReadLedger(path)
	if err != nil {
		return err
	}
	for name, d := range days {
		l[name] = d
	}
	raw, err := json.MarshalIndent(l, "", "  ")
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), filepath.Base(path)+".*.tmp")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name()) // gone already after the rename
	_, err = tmp.Write(append(raw, '\n'))
	if err == nil {
		err = tmp.Chmod(0o644)
	}
	if err == nil {
		err = tmp.Sync()
	}
	if cerr := tmp.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return err
	}
	if err := os.Rename(tmp.Name(), path); err != nil {
		return err
	}
	return syncDir(filepath.Dir(path))
}
