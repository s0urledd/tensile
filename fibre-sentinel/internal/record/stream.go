package record

import (
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
)

// Stream is one open record file: its archived segments and the live file,
// read as the one logical file they were written as.
type Stream struct {
	path    string
	live    *os.File
	base    int64
	size    int64
	idx     *Index
	closers []io.Closer
	// exports holds the exports indexes read for retired segments, by
	// directory: a reader over the whole record may open dozens of
	// retired segments, whose exports share one index.
	exports map[string]exportsIndex
}

// Open opens path for reading. The live file is opened first and its base
// found from its own first line, so a rotation between the two steps
// cannot pair one generation's bytes with another's base. A missing live
// file is os.ErrNotExist, as os.Open would say.
func Open(path string) (*Stream, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	s := &Stream{path: path, live: f, size: info.Size()}
	idx, err := LoadIndex(path)
	if err != nil {
		f.Close()
		return nil, err
	}
	s.idx = idx
	if len(idx.Generations) > 0 {
		head, err := headOf(f, 0)
		if err != nil {
			f.Close()
			return nil, err
		}
		s.base, _ = idx.base(head)
	}
	return s, nil
}

// Base is the logical offset of the live file's first byte.
func (s *Stream) Base() int64 { return s.base }

// End is the logical offset of the live file's end when it was opened.
func (s *Stream) End() int64 { return s.base + s.size }

// LogicalEnd is the logical offset just past path's last byte: the live
// file's base, found by its first line as Open finds it, plus its size. It
// is where a copier that appends another host's copy of the file resumes
// (deploy/vantage-pull.sh). Open reads a live file that matches no
// generation of a non-empty index as base 0; here that is an error: an
// end counted from 0 falls short of the true one, and a copier resuming
// there would append again bytes the record already holds. A missing live
// file is os.ErrNotExist.
func LogicalEnd(path string) (int64, error) {
	s, err := Open(path)
	if err != nil {
		return 0, err
	}
	defer s.Close()
	if len(s.idx.Generations) > 0 {
		head, err := headOf(s.live, 0)
		if err != nil {
			return 0, err
		}
		if _, ok := s.idx.base(head); !ok {
			return 0, fmt.Errorf("%s: the live file's first line matches no generation in %s", path, filepath.Join(ArchiveDir(path), IndexFile))
		}
	}
	return s.End(), nil
}

// Index is the file's archive index (empty when it was never archived).
func (s *Stream) Index() *Index { return s.idx }

// ReaderFrom reads the logical file from off to the live file's end
// (including whatever is appended while it reads). Bytes before the live
// file's base come from the segments, which must cover [off, base) without
// a gap.
func (s *Stream) ReaderFrom(off int64) (io.Reader, error) {
	if off < 0 {
		return nil, fmt.Errorf("%s: negative offset %d", s.path, off)
	}
	if off >= s.base {
		if _, err := s.live.Seek(off-s.base, io.SeekStart); err != nil {
			return nil, err
		}
		return s.live, nil
	}
	segs := append([]Segment(nil), s.idx.Segments...)
	sort.Slice(segs, func(i, j int) bool { return segs[i].From < segs[j].From })
	var rs []io.Reader
	at := off
	for _, sg := range segs {
		if sg.To <= at || sg.From >= s.base {
			continue
		}
		if sg.From > at {
			return nil, fmt.Errorf("%s: archive has no segment for logical bytes [%d, %d)", s.path, at, sg.From)
		}
		r, err := s.openSegment(sg)
		if err != nil {
			return nil, err
		}
		if skip := at - sg.From; skip > 0 {
			if n, err := io.CopyN(io.Discard, r, skip); err != nil {
				return nil, fmt.Errorf("%s: segment %s ends after %d of the %d bytes to skip: %w", s.path, sg.Name, n, skip, err)
			}
		}
		rs = append(rs, io.LimitReader(r, sg.To-at))
		at = sg.To
	}
	if at != s.base {
		return nil, fmt.Errorf("%s: archive ends at logical byte %d, the live file starts at %d", s.path, at, s.base)
	}
	if _, err := s.live.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	rs = append(rs, s.live)
	return io.MultiReader(rs...), nil
}

// openSegment reads a segment from its gzip file while the file is there,
// retired or not (a Retire that stopped between saving the index and
// removing the file leaves both), and from the daily exports once a
// retired segment's file is gone.
func (s *Stream) openSegment(sg Segment) (io.Reader, error) {
	f, err := os.Open(filepath.Join(ArchiveDir(s.path), sg.Name))
	if errors.Is(err, os.ErrNotExist) {
		return s.openRetired(sg, err)
	}
	if err != nil {
		return nil, err
	}
	s.closers = append(s.closers, f)
	z, err := gzip.NewReader(f)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", sg.Name, err)
	}
	s.closers = append(s.closers, z)
	return z, nil
}

// openRetired reads a segment whose file is gone from the exports its
// index entry names. Retire saves the index before it removes the file, so
// a Stream whose index was loaded before a Retire finds the segment
// retired in the index as it is now; a segment that is gone without being
// retired is an error naming the file. That error does not wrap
// os.ErrNotExist: callers take it from a read of the whole record to mean
// the file does not exist at all, which is no error to them.
func (s *Stream) openRetired(sg Segment, missing error) (io.Reader, error) {
	if sg.Retired == nil {
		if idx, err := LoadIndex(s.path); err == nil {
			for _, now := range idx.Segments {
				if now.Name == sg.Name && now.From == sg.From && now.To == sg.To && now.SHA256 == sg.SHA256 {
					sg = now
				}
			}
		}
	}
	if sg.Retired == nil {
		return nil, fmt.Errorf("%s: segment %s is gone and was never retired: %v", s.path, sg.Name, missing)
	}
	label := exportsLabel(s.path, sg.Name)
	if err := sg.Retired.check(); err != nil {
		return nil, fmt.Errorf("%s: %v", label, err)
	}
	if s.exports == nil {
		s.exports = map[string]exportsIndex{}
	}
	r, err := openFromExports(label, exportsDirOf(s.path, sg.Retired), sg, s.exports)
	if err != nil {
		return nil, err
	}
	s.closers = append(s.closers, r)
	return r, nil
}

// Close closes the live file and every segment opened.
func (s *Stream) Close() error {
	for _, c := range s.closers {
		_ = c.Close()
	}
	s.closers = nil
	return s.live.Close()
}

// OpenAll is a reader over the whole record, segments then live file, for
// the tools that read every line (recompute, measure-check). A file never
// archived reads exactly as os.Open would.
func OpenAll(path string) (io.ReadCloser, error) {
	s, err := Open(path)
	if err != nil {
		return nil, err
	}
	r, err := s.ReaderFrom(0)
	if err != nil {
		s.Close()
		return nil, err
	}
	return struct {
		io.Reader
		io.Closer
	}{r, s}, nil
}
