package record

// Reading a retired segment back from the daily exports.
//
// Every byte of an archived segment is also in the daily export tarballs:
// the export reads each file from where the previous one stopped, so the
// member of a file in consecutive exports holds consecutive logical bytes
// of it, [source_from, source_to) in exports/index.json. Once a segment's
// gzip file is removed (Retire), its bytes are the parts of those members
// that fall inside [From, To), and the index names the exports to read.
//
// The export package imports this one (its builder reads through Open), so
// the little of its format needed here is parsed here: the index entries
// with their members, and the tarball as gzip over tar.

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
)

// exportsIndexFile lists the exports in the exports directory.
const exportsIndexFile = "index.json"

// exportEntry is what this reads of one entry of exports/index.json: the
// tarball's size and digest and its members.
type exportEntry struct {
	Name   string         `json:"name"`
	Bytes  int64          `json:"bytes"`
	SHA256 string         `json:"sha256"`
	Files  []exportMember `json:"files"`
}

// exportMember is one member as the index lists it: its size and digest,
// and the logical byte range of the source file it was read from.
type exportMember struct {
	Name   string `json:"name"`
	Bytes  int64  `json:"bytes"`
	SHA256 string `json:"sha256"`
	From   int64  `json:"source_from"`
	To     int64  `json:"source_to"`
}

// exportsIndex is exports/index.json by tarball name. A name is listed
// once by the builder; more than one entry under a name is kept so that
// reading that export can refuse it rather than pick one.
type exportsIndex map[string][]exportEntry

func loadExportsIndex(dir string) (exportsIndex, error) {
	p := filepath.Join(dir, exportsIndexFile)
	raw, err := os.ReadFile(p)
	if err != nil {
		return nil, err
	}
	var list []exportEntry
	if err := json.Unmarshal(raw, &list); err != nil {
		return nil, fmt.Errorf("%s: %w", p, err)
	}
	idx := exportsIndex{}
	for _, e := range list {
		idx[e.Name] = append(idx[e.Name], e)
	}
	return idx, nil
}

// check refuses a Retired that cannot name where the bytes are. The
// exports directory must be relative: an absolute one would send a
// restored copy of the data directory to exports it does not hold.
func (r *Retired) check() error {
	switch {
	case len(r.Exports) == 0:
		return errors.New("names no exports")
	case r.Member == "":
		return errors.New("names no member")
	case r.ExportsDir == "" || filepath.IsAbs(r.ExportsDir) || strings.HasPrefix(filepath.ToSlash(r.ExportsDir), "/"):
		return fmt.Errorf("exports_dir %q is not a path relative to the archive directory", r.ExportsDir)
	}
	return nil
}

// exportsDirOf is where the exports holding path's retired segment are.
func exportsDirOf(path string, r *Retired) string {
	return filepath.Join(ArchiveDir(path), filepath.FromSlash(r.ExportsDir))
}

// exportPart is one export a retired segment is read from: the tarball,
// its index entry, and the entry's member of the file.
type exportPart struct {
	path  string
	entry exportEntry
	m     exportMember
}

// planExports resolves the exports sg.Retired names in idx and checks,
// before a byte is read, that their members of the file cover [sg.From,
// sg.To) in order and without a gap, that each adds to it, and that each
// member is a byte-for-byte copy of its source range (the builder drops a
// blank line's bytes from a member, and such a member cannot stand in for
// the file). An empty member that starts where the exports before it end
// adds nothing and is not read: a quiet day leaves a file's member empty,
// and the exports whose member overlaps [From, To) include it whenever it
// sits inside the range.
func planExports(dir string, idx exportsIndex, sg Segment) ([]exportPart, error) {
	r := sg.Retired
	var parts []exportPart
	at := sg.From
	for _, name := range r.Exports {
		if name == "" || name == "." || name == ".." || filepath.Base(name) != name || strings.ContainsAny(name, `/\`) {
			return nil, fmt.Errorf("export %q is not a file name", name)
		}
		es := idx[name]
		switch len(es) {
		case 0:
			return nil, fmt.Errorf("export %s is not in %s", name, filepath.Join(dir, exportsIndexFile))
		case 1:
		default:
			return nil, fmt.Errorf("export %s is listed %d times in %s", name, len(es), filepath.Join(dir, exportsIndexFile))
		}
		e := es[0]
		var m *exportMember
		for i := range e.Files {
			if e.Files[i].Name != r.Member {
				continue
			}
			if m != nil {
				return nil, fmt.Errorf("export %s lists member %s twice", name, r.Member)
			}
			m = &e.Files[i]
		}
		switch {
		case m == nil:
			return nil, fmt.Errorf("export %s has no member %s", name, r.Member)
		case m.To-m.From != m.Bytes:
			return nil, fmt.Errorf("export %s: member %s holds %d bytes for source bytes [%d, %d), so it is not a copy of them", name, r.Member, m.Bytes, m.From, m.To)
		case m.Bytes == 0 && m.From == at:
			// Nothing of the range depends on this tarball, so a reader
			// does not either.
			continue
		case at >= sg.To:
			return nil, fmt.Errorf("export %s is past the segment's end: the exports before it hold all of [%d, %d)", name, sg.From, sg.To)
		case m.From > at:
			return nil, fmt.Errorf("the exports leave logical bytes [%d, %d) out: %s's member %s starts at %d", at, min(m.From, sg.To), name, r.Member, m.From)
		case m.To <= at:
			return nil, fmt.Errorf("export %s holds nothing of [%d, %d): its member %s ends at %d", name, at, sg.To, r.Member, m.To)
		}
		p := filepath.Join(dir, name)
		if _, err := os.Stat(p); err != nil {
			return nil, err
		}
		parts = append(parts, exportPart{path: p, entry: e, m: *m})
		at = min(m.To, sg.To)
	}
	if at < sg.To {
		return nil, fmt.Errorf("the exports end at logical byte %d, the segment at %d", at, sg.To)
	}
	return parts, nil
}

// exportsLabel starts every error about reading path's segment name back
// from the exports.
func exportsLabel(path, name string) string {
	return fmt.Sprintf("%s: segment %s from the exports", path, name)
}

// planFromExports reads the exports index from dir, once per cache (nil:
// every call), and plans reading sg back from the exports it names. Every
// error starts with label and wraps nothing: a missing tarball's
// os.ErrNotExist would tell a caller reading the whole record that the
// record file does not exist, which callers take as no error.
func planFromExports(label, dir string, sg Segment, cache map[string]exportsIndex) ([]exportPart, error) {
	parts, err := func() ([]exportPart, error) {
		if sg.Retired == nil {
			return nil, errors.New("not retired")
		}
		if err := sg.Retired.check(); err != nil {
			return nil, err
		}
		idx, ok := cache[dir]
		if !ok {
			var err error
			if idx, err = loadExportsIndex(dir); err != nil {
				return nil, err
			}
			if cache != nil {
				cache[dir] = idx
			}
		}
		return planExports(dir, idx, sg)
	}()
	if err != nil {
		return nil, fmt.Errorf("%s: %v", label, err)
	}
	return parts, nil
}

// openFromExports reads logical bytes [sg.From, sg.To) of the file back
// from the exports that sg.Retired names, with planFromExports's errors.
//
// No byte comes out before all of them are proven: a reader that takes the
// bytes a line at a time (the ingest's tail stores each line and moves its
// cursor past it) must never be handed a wrong line ahead of the error. A
// day is too large to hold, so the exports are read twice. The first pass
// hands out nothing: every byte of every tarball is hashed against the
// index, every member used is read whole and hashed against it, and the
// range must be exactly To-From bytes and sg.Lines lines with sg.SHA256;
// it keeps the SHA-256 of each proofBlock of the range. The second pass
// reads the exports again and hands out each block only once its digest is
// the first pass's, so a tarball changed between the passes ends the read
// with an error before the first block that differs. Any failure ends the
// reader with the error instead of io.EOF. The second decompression is
// paid only by readers of the record from before the live file (a rebuild
// from zero, recompute), the only ones that reach a retired segment.
func openFromExports(label, dir string, sg Segment, cache map[string]exportsIndex) (io.ReadCloser, error) {
	parts, err := planFromExports(label, dir, sg, cache)
	if err != nil {
		return nil, err
	}
	pr, pw := io.Pipe()
	return &exportsReader{label: label, pr: pr, pw: pw, sg: sg, parts: parts}, nil
}

// exportsReader runs the copy in a goroutine that starts on the first Read:
// a reader over the whole record opens every segment before it reads one,
// and a goroutine per retired segment, each reading its exports, would
// work for no reason. Close stops the copy at its next block.
type exportsReader struct {
	once   sync.Once
	closed atomic.Bool
	label  string
	pr     *io.PipeReader
	pw     *io.PipeWriter
	sg     Segment
	parts  []exportPart
}

func (e *exportsReader) Read(b []byte) (int, error) {
	e.once.Do(func() {
		go func() {
			err := func() error {
				sums, err := proveFromExports(e.sg, e.parts, &e.closed)
				if err != nil {
					return err
				}
				return emitFromExports(e.pw, e.sg, e.parts, sums)
			}()
			if err != nil {
				err = fmt.Errorf("%s: %v", e.label, err)
			}
			e.pw.CloseWithError(err)
		}()
	})
	return e.pr.Read(b)
}

func (e *exportsReader) Close() error {
	e.closed.Store(true)
	return e.pr.Close()
}

// proofBlock is how much of a retired range the second pass holds before
// it hands the bytes out: each block goes only once its digest matches the
// first pass's. A mainnet day of a few hundred MB keeps a few hundred
// digests.
const proofBlock = 1 << 20

// blocker cuts what is written to it into proofBlock-sized blocks, the
// last one shorter, and passes each whole block to fn: both passes cut the
// range at the same offsets, so their blocks pair up.
type blocker struct {
	buf []byte
	fn  func([]byte) error
}

func newBlocker(fn func([]byte) error) *blocker {
	return &blocker{buf: make([]byte, 0, proofBlock), fn: fn}
}

func (b *blocker) write(p []byte) error {
	for len(p) > 0 {
		n := copy(b.buf[len(b.buf):cap(b.buf)], p)
		b.buf, p = b.buf[:len(b.buf)+n], p[n:]
		if len(b.buf) == cap(b.buf) {
			if err := b.flush(); err != nil {
				return err
			}
		}
	}
	return nil
}

func (b *blocker) flush() error {
	if len(b.buf) == 0 {
		return nil
	}
	err := b.fn(b.buf)
	b.buf = b.buf[:0]
	return err
}

// errReaderClosed stops a first pass whose reader was closed: nobody will
// read what it proves.
var errReaderClosed = errors.New("the reader was closed")

// proveFromExports is the first pass: it reads [sg.From, sg.To) from parts
// without handing out a byte, checks it whole against the segment, and
// returns the SHA-256 of each proofBlock of it. A closed (nil: never set)
// stops it early.
func proveFromExports(sg Segment, parts []exportPart, closed *atomic.Bool) ([][sha256.Size]byte, error) {
	h := sha256.New()
	var got, lines int64
	var sums [][sha256.Size]byte
	bl := newBlocker(func(b []byte) error {
		if closed != nil && closed.Load() {
			return errReaderClosed
		}
		h.Write(b)
		lines += int64(bytes.Count(b, []byte{'\n'}))
		got += int64(len(b))
		sums = append(sums, sha256.Sum256(b))
		return nil
	})
	at, err := readParts(sg, parts, bl.write)
	if err != nil {
		return nil, err
	}
	if err := bl.flush(); err != nil {
		return nil, err
	}
	switch total := sg.To - sg.From; {
	case at != sg.To || got != total:
		return nil, fmt.Errorf("the exports gave %d of the segment's %d bytes", got, total)
	case hex.EncodeToString(h.Sum(nil)) != sg.SHA256:
		return nil, errors.New("the bytes differ from the segment's sha256")
	case lines != sg.Lines:
		return nil, fmt.Errorf("%d lines, the index says %d", lines, sg.Lines)
	}
	return sums, nil
}

// emitFromExports is the second pass: it reads [sg.From, sg.To) from parts
// again and writes it to w a block at a time, each block only once its
// SHA-256 is the one the first pass proved for it.
func emitFromExports(w io.Writer, sg Segment, parts []exportPart, sums [][sha256.Size]byte) error {
	k := 0
	bl := newBlocker(func(b []byte) error {
		if k >= len(sums) || sha256.Sum256(b) != sums[k] {
			return fmt.Errorf("the exports changed while they were read: logical bytes from %d differ from the first pass", sg.From+int64(k)*proofBlock)
		}
		k++
		_, err := w.Write(b)
		return err
	})
	if _, err := readParts(sg, parts, bl.write); err != nil {
		return err
	}
	if err := bl.flush(); err != nil {
		return err
	}
	if k != len(sums) {
		return fmt.Errorf("the exports changed while they were read: %d of the range's %d blocks came back", k, len(sums))
	}
	return nil
}

// readParts passes emit the bytes of [sg.From, sg.To) that parts hold, in
// order, and returns the logical offset they reach.
func readParts(sg Segment, parts []exportPart, emit func([]byte) error) (int64, error) {
	at := sg.From
	for _, p := range parts {
		next, err := copyMember(p, at, sg.To, emit)
		if err != nil {
			return at, fmt.Errorf("export %s: %w", p.entry.Name, err)
		}
		at = next
	}
	return at, nil
}

// copyMember reads p's tarball whole and passes emit the part of the
// file's member inside [at, to), which starts at logical byte p.m.From. It
// returns the logical offset emitted up to. The member is read to its end
// and the tarball to its last byte, so both digests are checked against
// the index whatever part of them the range needs.
func copyMember(p exportPart, at, to int64, emit func([]byte) error) (int64, error) {
	f, err := os.Open(p.path)
	if err != nil {
		return at, err
	}
	defer f.Close()
	th := sha256.New()
	cr := &countReader{r: io.TeeReader(f, th)}
	z, err := gzip.NewReader(cr)
	if err != nil {
		return at, fmt.Errorf("not gzip: %w", err)
	}
	tr := tar.NewReader(z)
	found := false
	buf := make([]byte, 256<<10)
	for {
		hd, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return at, fmt.Errorf("tar: %w", err)
		}
		if hd.Name != p.m.Name {
			continue
		}
		if found {
			// Two members under one name: a tool that extracts the
			// tarball and this reader could read different bytes.
			return at, fmt.Errorf("member %s appears twice", p.m.Name)
		}
		found = true
		// '\x00' is the pre-POSIX spelling of a regular file.
		if hd.Typeflag != tar.TypeReg && hd.Typeflag != '\x00' {
			return at, fmt.Errorf("member %s is not a regular file", p.m.Name)
		}
		if hd.Size != p.m.Bytes {
			return at, fmt.Errorf("member %s is %d bytes, the index says %d", p.m.Name, hd.Size, p.m.Bytes)
		}
		mh := sha256.New()
		pos := p.m.From
		for {
			n, err := tr.Read(buf)
			if n > 0 {
				b := buf[:n]
				mh.Write(b)
				if lo, hi := max(at, pos), min(to, pos+int64(n)); lo < hi {
					if err := emit(b[lo-pos : hi-pos]); err != nil {
						return at, err
					}
					at = hi
				}
				pos += int64(n)
			}
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				return at, fmt.Errorf("member %s: %w", p.m.Name, err)
			}
		}
		if n := pos - p.m.From; n != p.m.Bytes {
			return at, fmt.Errorf("member %s: read %d bytes, the index says %d", p.m.Name, n, p.m.Bytes)
		}
		if sum := hex.EncodeToString(mh.Sum(nil)); sum != p.m.SHA256 {
			return at, fmt.Errorf("member %s: sha256 %s, the index says %s", p.m.Name, sum, p.m.SHA256)
		}
	}
	if !found {
		return at, fmt.Errorf("no member %s in the tarball", p.m.Name)
	}
	// The rest of the gzip stream and anything after it, so the tarball's
	// digest covers every byte of the file.
	if _, err := io.Copy(io.Discard, z); err != nil {
		return at, fmt.Errorf("gzip: %w", err)
	}
	if _, err := io.Copy(io.Discard, cr); err != nil {
		return at, err
	}
	if cr.n != p.entry.Bytes {
		return at, fmt.Errorf("tarball is %d bytes, the index says %d", cr.n, p.entry.Bytes)
	}
	if sum := hex.EncodeToString(th.Sum(nil)); sum != p.entry.SHA256 {
		return at, fmt.Errorf("tarball sha256 %s, the index says %s", sum, p.entry.SHA256)
	}
	return at, nil
}

// checkFromExports reads sg back from the exports, whole, and requires its
// length, lines and digest: what Retire proves before it removes a
// segment's file, and what Verify checks once the file is gone. It is the
// reader's first pass alone; nothing needs the bytes.
func checkFromExports(path string, sg Segment) error {
	label := exportsLabel(path, sg.Name)
	if sg.Retired == nil {
		return fmt.Errorf("%s: not retired", label)
	}
	parts, err := planFromExports(label, exportsDirOf(path, sg.Retired), sg, nil)
	if err != nil {
		return err
	}
	if _, err := proveFromExports(sg, parts, nil); err != nil {
		return fmt.Errorf("%s: %v", label, err)
	}
	return nil
}
