// Package record keeps the append-only JSONL files bounded without taking a
// line out of the record.
//
// A file's closed, older lines move into gzip segments under
// <data-dir>/archive/<file>/, and the live file keeps only the lines since.
// Every byte keeps its place in one logical file: the segments hold logical
// bytes [0, base) in order, and the live file holds [base, ...). Offsets
// kept by readers (the collector's ingest cursors, the export's
// source_from/source_to) are logical offsets, so they mean the same byte
// before and after a rotation, and a file that was never archived has base
// 0, which is what every offset already meant.
//
// Three parts, one per role:
//
//   - Appender, for a writer. It appends under a shared flock on the file
//     and checks, under that lock, that the path still names the file it
//     holds; when it does not, the file was rotated and it reopens the path.
//   - Open, for a reader: a Stream over the segments and the live file,
//     positioned at any logical offset.
//   - Archive, for the daily job: it moves the lines before a cutoff into a
//     segment, fsyncs and verifies it, and replaces the live file with its
//     own tail under an exclusive flock, so no append can land in between.
//
// index.json, beside the segments, lists them and every generation of the
// live file: the logical offset it starts at and the SHA-256 of its first
// line. A reader finds its base by the first line of the file it actually
// opened, so a reader holding the file from before a rotation and one
// holding the file after it each read the right bytes.
//
// A segment whose bytes are proven to be in the daily exports can later be
// retired (Retire): its gzip file is removed, the index names the exports
// that hold it, and a reader gets the same bytes back from those tarballs,
// held to their digests (exports.go).
package record

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Dir is where archived segments live, under the data dir:
// <data-dir>/archive/<file name>/.
const Dir = "archive"

// IndexFile is the index of one file's segments and generations.
const IndexFile = "index.json"

// Segment is one gzip file of archived lines: logical bytes [From, To) of
// the record, exactly as they were written.
type Segment struct {
	Name string `json:"name"`
	From int64  `json:"from"`
	To   int64  `json:"to"`
	// Lines is the number of lines in the segment.
	Lines int64 `json:"lines"`
	// SHA256 is the digest of the uncompressed bytes, GzSHA256 and GzBytes
	// of the file on disk.
	SHA256   string `json:"sha256"`
	GzSHA256 string `json:"gz_sha256"`
	GzBytes  int64  `json:"gz_bytes"`
	// Cutoff is the time the run archived before: every line in the segment
	// comes before the first line of the file dated at or after it.
	Cutoff     time.Time `json:"cutoff"`
	ArchivedAt time.Time `json:"archived_at"`
	// Retired is set once the segment's file was removed (Retire): its
	// bytes are read back from the daily exports.
	Retired *Retired `json:"retired,omitempty"`
}

// Retired says where a removed segment's bytes are and what was proven
// before its file went.
type Retired struct {
	At time.Time `json:"at"`
	// Exports are the export tarballs, oldest first, whose members of this
	// file together hold [From, To).
	Exports []string `json:"exports"`
	// Member is the file's member name inside them ("publications.jsonl",
	// "vantages/de-1/reachability.jsonl").
	Member string `json:"member"`
	// ExportsDir is the exports directory relative to ArchiveDir(path), so
	// a restored copy of the data directory reads its own exports
	// ("../../exports" for a top-level file, "../../../../exports" for
	// vantages/<n>/<file>).
	ExportsDir string `json:"exports_dir"`
	// Proof is what was checked before the file was removed, in words (for
	// whoever reads the index).
	Proof string `json:"proof"`
}

// Generation is one live file: the logical offset its first byte stands
// at, and the SHA-256 of its first line (newline included), which is how a
// reader tells which generation it opened.
type Generation struct {
	Base int64     `json:"base"`
	Head string    `json:"head_sha256"`
	At   time.Time `json:"at"`
}

// Index is archive/<file>/index.json.
type Index struct {
	Version   int    `json:"version"`
	File      string `json:"file"`
	TimeField string `json:"time_field"`
	// LiveSince is the newest cutoff archived before: every line dated at or
	// after it is in the live file. Zero when nothing was archived.
	LiveSince   time.Time    `json:"live_since"`
	Segments    []Segment    `json:"segments"`
	Generations []Generation `json:"generations"`
}

const indexVersion = 1

// ArchiveDir is the directory holding path's segments and index.
func ArchiveDir(path string) string {
	return filepath.Join(filepath.Dir(path), Dir, filepath.Base(path))
}

// LoadIndex reads path's index. A file that was never archived has none,
// which is an empty index and no error.
func LoadIndex(path string) (*Index, error) {
	raw, err := os.ReadFile(filepath.Join(ArchiveDir(path), IndexFile))
	if errors.Is(err, os.ErrNotExist) {
		return &Index{Version: indexVersion, File: filepath.Base(path)}, nil
	}
	if err != nil {
		return nil, err
	}
	idx := &Index{}
	if err := json.Unmarshal(raw, idx); err != nil {
		return nil, fmt.Errorf("%s: %w", filepath.Join(ArchiveDir(path), IndexFile), err)
	}
	return idx, nil
}

// LiveSince is the time from which every line of path is in the live file;
// zero when nothing was ever archived. A missing or unreadable index is
// zero: nothing archived that this caller can know of. The prober asks
// every cycle, so the answer is kept until the index file changes.
func LiveSince(path string) time.Time {
	ip := filepath.Join(ArchiveDir(path), IndexFile)
	info, err := os.Stat(ip)
	if err != nil {
		return time.Time{}
	}
	liveSinceMu.Lock()
	defer liveSinceMu.Unlock()
	if c, ok := liveSinceCache[ip]; ok && c.mod.Equal(info.ModTime()) && c.size == info.Size() {
		return c.since
	}
	idx, err := LoadIndex(path)
	if err != nil {
		return time.Time{}
	}
	liveSinceCache[ip] = liveSinceEntry{mod: info.ModTime(), size: info.Size(), since: idx.LiveSince}
	return idx.LiveSince
}

type liveSinceEntry struct {
	mod   time.Time
	size  int64
	since time.Time
}

var (
	liveSinceMu    sync.Mutex
	liveSinceCache = map[string]liveSinceEntry{}
)

// Base is the logical offset of the live file's first byte: the base of the
// newest generation whose first line is head, or 0 when none is (a file
// never archived).
func (idx *Index) base(head string) (int64, bool) {
	if head == "" {
		return 0, false
	}
	for i := len(idx.Generations) - 1; i >= 0; i-- {
		if idx.Generations[i].Head == head {
			return idx.Generations[i].Base, true
		}
	}
	return 0, false
}

// save writes the index atomically: a temp file, fsynced, renamed over the
// old one, and the directory fsynced so the rename survives a power loss.
func (idx *Index) save(dir string) error {
	raw, err := json.MarshalIndent(idx, "", "  ")
	if err != nil {
		return err
	}
	return writeSync(filepath.Join(dir, IndexFile), append(raw, '\n'))
}

func writeSync(path string, b []byte) error {
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(b); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	return syncDir(filepath.Dir(path))
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	if err := d.Sync(); err != nil && !isUnsupportedSync(err) {
		return err
	}
	return nil
}

// headOf is the SHA-256 of the first line of f (newline included), read
// with ReadAt so f's offset is left alone. "" when f holds no complete line.
func headOf(f *os.File, at int64) (string, error) {
	h := sha256.New()
	buf := make([]byte, 64<<10)
	for off := at; ; {
		n, err := f.ReadAt(buf, off)
		for i := 0; i < n; i++ {
			if buf[i] == '\n' {
				h.Write(buf[:i+1])
				return hex.EncodeToString(h.Sum(nil)), nil
			}
		}
		h.Write(buf[:n])
		off += int64(n)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return "", nil
			}
			return "", err
		}
	}
}
