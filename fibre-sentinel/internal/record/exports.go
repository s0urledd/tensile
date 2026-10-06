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
// the file).
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

// openFromExports reads logical bytes [sg.From, sg.To) of the file back
// from the exports that sg.Retired names. Their index is read from dir
// once per cache (nil: every call). Every error, returned or ending the
// reader, starts with label and wraps nothing: a missing tarball's
// os.ErrNotExist would tell a caller reading the whole record that the
// record file does not exist, which callers take as no error.
//
// The bytes are streamed, a day at a time being too large to hold, so
// what the reader returns before its end is not yet proven; its end is.
// Every byte of every tarball read is hashed against the index, every
// member used is read whole and hashed against it, and what is emitted
// must be exactly To-From bytes and sg.Lines lines with sg.SHA256. Should
// any of that fail, the reader ends with the error instead of io.EOF, and
// the last byte of the range is held back until all of it has passed: a
// caller that reads exactly To-From bytes (Stream.ReaderFrom does) never
// has the whole range unless it is the segment's.
func openFromExports(label, dir string, sg Segment, cache map[string]exportsIndex) (io.ReadCloser, error) {
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
	pr, pw := io.Pipe()
	return &exportsReader{label: label, pr: pr, pw: pw, sg: sg, parts: parts}, nil
}

// exportsReader runs the copy in a goroutine that starts on the first Read:
// a reader over the whole record opens every segment before it reads one,
// and a goroutine per retired segment, each holding a tarball open, would
// wait for no reason. Close stops the copy at its next write.
type exportsReader struct {
	once  sync.Once
	label string
	pr    *io.PipeReader
	pw    *io.PipeWriter
	sg    Segment
	parts []exportPart
}

func (e *exportsReader) Read(b []byte) (int, error) {
	e.once.Do(func() {
		go func() {
			err := copyFromExports(e.pw, e.sg, e.parts)
			if err != nil {
				err = fmt.Errorf("%s: %v", e.label, err)
			}
			e.pw.CloseWithError(err)
		}()
	})
	return e.pr.Read(b)
}

func (e *exportsReader) Close() error { return e.pr.Close() }

// copyFromExports writes [sg.From, sg.To) to w from parts, the last byte
// only once every check has passed.
func copyFromExports(w io.Writer, sg Segment, parts []exportPart) error {
	total := sg.To - sg.From
	h := sha256.New()
	var got, lines int64
	var held []byte
	emit := func(b []byte) error {
		h.Write(b)
		lines += int64(bytes.Count(b, []byte{'\n'}))
		got += int64(len(b))
		if got == total {
			held = []byte{b[len(b)-1]}
			b = b[:len(b)-1]
		}
		_, err := w.Write(b)
		return err
	}
	at := sg.From
	for _, p := range parts {
		next, err := copyMember(p, at, sg.To, emit)
		if err != nil {
			return fmt.Errorf("export %s: %w", p.entry.Name, err)
		}
		at = next
	}
	switch {
	case at != sg.To || got != total:
		return fmt.Errorf("the exports gave %d of the segment's %d bytes", got, total)
	case hex.EncodeToString(h.Sum(nil)) != sg.SHA256:
		return errors.New("the bytes differ from the segment's sha256")
	case lines != sg.Lines:
		return fmt.Errorf("%d lines, the index says %d", lines, sg.Lines)
	}
	_, err := w.Write(held)
	return err
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
// length and digest: what Retire proves before it removes a segment's
// file, and what Verify checks once the file is gone.
func checkFromExports(path string, sg Segment) error {
	label := exportsLabel(path, sg.Name)
	if sg.Retired == nil {
		return fmt.Errorf("%s: not retired", label)
	}
	r, err := openFromExports(label, exportsDirOf(path, sg.Retired), sg, nil)
	if err != nil {
		return err
	}
	defer r.Close()
	h := sha256.New()
	n, err := io.Copy(h, r)
	if err != nil {
		return err
	}
	if n != sg.To-sg.From {
		return fmt.Errorf("%s: %d bytes, the index says %d", label, n, sg.To-sg.From)
	}
	if hex.EncodeToString(h.Sum(nil)) != sg.SHA256 {
		return fmt.Errorf("%s: the bytes differ from the segment's sha256", label)
	}
	return nil
}
