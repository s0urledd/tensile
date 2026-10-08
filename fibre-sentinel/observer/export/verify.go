package export

// Verifying an export from the outside: what cmd/sentinel-verify-export runs,
// written here so the builder's tests can hold the two to each other. Every
// check reads only the bytes a downloader has — the tarball, its .sha256
// sidecar, its .sig, and a public key obtained separately — and nothing from
// the observer's data directory.

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"strings"
	"unicode"
	"unicode/utf8"
)

// maxMember bounds one member read from an untrusted tarball. Members are
// read as a stream, so this bounds the time a member takes to check, not
// memory. A day's measurements file is tens of megabytes; this is far
// above that.
const maxMember = 4 << 30

// maxManifest bounds manifest.json, the one member always held: a manifest
// is a few kilobytes.
const maxManifest = 16 << 20

// maxHeld bounds the bytes of the other members ReadArchive keeps for its
// caller, all of them together (a var, so a test can lower it). Each member
// is checked from its digest, byte count and line count, taken as it
// streams by; a tarball whose members come to more is checked all the same
// and keeps none of their bytes, so neither a large export nor a
// decompression bomb, any number of members of zeros, can take the
// verifier's memory.
var maxHeld int64 = 256 << 20

// MemberSum is what CheckMembers needs of one member: its digest, its byte
// count and the lines the builder counts in it.
type MemberSum struct {
	SHA256 string
	Bytes  int64
	Lines  int64
}

// Archive is a parsed export tarball.
type Archive struct {
	SHA256 string // of the tarball bytes
	// Sums is every member's MemberSum, manifest.json included.
	Sums map[string]MemberSum
	// Members is every member by name, manifest.json included, with its
	// bytes while Held: while all of them come to at most maxHeld. Past
	// that, every member but manifest.json is named with nil bytes.
	Members        map[string][]byte
	Held           bool
	ManifestRaw    []byte
	Manifest       Manifest
	ManifestSHA256 string
}

// ReadArchive parses an export tarball and digests its manifest.
func ReadArchive(tarball []byte) (*Archive, error) {
	sum := sha256.Sum256(tarball)
	a := &Archive{SHA256: hex.EncodeToString(sum[:]), Sums: map[string]MemberSum{}, Members: map[string][]byte{}, Held: true}
	gz, err := gzip.NewReader(bytes.NewReader(tarball))
	if err != nil {
		return nil, fmt.Errorf("not gzip: %w", err)
	}
	tr := tar.NewReader(gz)
	var held int64
	for {
		h, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("tar: %w", err)
		}
		// '\x00' is the pre-POSIX spelling of a regular file; the reader
		// normalises it, but an old writer's tarball is still a tarball.
		if h.Typeflag != tar.TypeReg && h.Typeflag != '\x00' {
			return nil, fmt.Errorf("tar member %q is not a regular file", h.Name)
		}
		if _, dup := a.Sums[h.Name]; dup {
			// Two members under one name would let a verifier and a tool
			// that extracts to disk read different bytes for the same file.
			return nil, fmt.Errorf("tar member %q appears twice", h.Name)
		}
		limit := int64(maxMember)
		if h.Name == "manifest.json" {
			limit = maxManifest
		}
		// The header's size is the member's (the reader holds a regular
		// file to it), so a member is held only when it fits what is left.
		keep := h.Name == "manifest.json" || a.Held && h.Size >= 0 && held+h.Size <= maxHeld
		s := newSummer()
		var body bytes.Buffer
		w := io.Writer(s)
		if keep {
			body.Grow(int(min(max(h.Size, 0), limit)))
			w = io.MultiWriter(s, &body)
		}
		n, err := io.Copy(w, io.LimitReader(tr, limit+1))
		if err != nil {
			return nil, fmt.Errorf("tar member %q: %w", h.Name, err)
		}
		if n > limit {
			return nil, fmt.Errorf("tar member %q is larger than %d bytes", h.Name, limit)
		}
		a.Sums[h.Name] = s.sum()
		switch {
		case h.Name == "manifest.json":
			a.Members[h.Name] = body.Bytes()
		case keep:
			a.Members[h.Name] = body.Bytes()
			held += n
		default:
			if a.Held {
				a.Held = false
				for name := range a.Members {
					if name != "manifest.json" {
						a.Members[name] = nil
					}
				}
			}
			a.Members[h.Name] = nil
		}
	}
	raw, ok := a.Members["manifest.json"]
	if !ok {
		return nil, errors.New("no manifest.json in the tarball")
	}
	a.ManifestRaw = raw
	a.ManifestSHA256 = a.Sums["manifest.json"].SHA256
	if err := json.Unmarshal(raw, &a.Manifest); err != nil {
		return nil, fmt.Errorf("manifest.json: %w", err)
	}
	return a, nil
}

// CheckMembers holds every member to the manifest: present, the right
// digest, the right byte count, the right number of lines, and nothing in
// the tarball the manifest does not name. It returns every problem, not the
// first.
func (a *Archive) CheckMembers() []string {
	var problems []string
	named := map[string]bool{"manifest.json": true}
	check := func(m Member) {
		named[m.Name] = true
		s, ok := a.Sums[m.Name]
		if !ok {
			problems = append(problems, fmt.Sprintf("%s: in the manifest, not in the tarball", m.Name))
			return
		}
		if s.SHA256 != m.SHA256 {
			problems = append(problems, fmt.Sprintf("%s: sha256 %s, manifest says %s", m.Name, s.SHA256, m.SHA256))
		}
		if s.Bytes != m.Bytes {
			problems = append(problems, fmt.Sprintf("%s: %d bytes, manifest says %d", m.Name, s.Bytes, m.Bytes))
		}
		if m.Name != StateFile && s.Lines != m.Lines {
			problems = append(problems, fmt.Sprintf("%s: %d lines, manifest says %d", m.Name, s.Lines, m.Lines))
		}
	}
	for _, m := range a.Manifest.Files {
		check(m)
	}
	if a.Manifest.State != nil {
		check(*a.Manifest.State)
	}
	for name := range a.Sums {
		if !named[name] {
			problems = append(problems, fmt.Sprintf("%s: in the tarball, not in the manifest", name))
		}
	}
	return problems
}

// summer takes a member's MemberSum as it streams by. Its line count is the
// builder's: collect skips whitespace-only lines without counting them but
// keeps their bytes, so a line counts when it holds a rune that is not
// white space (bytes.TrimSpace leaves something of it).
type summer struct {
	h     hash.Hash
	bytes int64
	lines int64
	// text: the line being read holds a rune that is not white space;
	// carry: the start of a rune the last write ended inside.
	text  bool
	carry []byte
}

func newSummer() *summer { return &summer{h: sha256.New()} }

func (s *summer) Write(p []byte) (int, error) {
	s.h.Write(p)
	s.bytes += int64(len(p))
	b := p
	if len(s.carry) > 0 {
		b = append(s.carry, p...)
		s.carry = nil
	}
	for i := 0; i < len(b); {
		c := b[i]
		switch {
		case c == '\n':
			if s.text {
				s.lines++
			}
			s.text = false
			i++
		case s.text:
			// The rest of the line counts for nothing more.
			j := bytes.IndexByte(b[i:], '\n')
			if j < 0 {
				return len(p), nil
			}
			i += j
		case c < utf8.RuneSelf:
			s.text = !asciiSpace(c)
			i++
		case !utf8.FullRune(b[i:]):
			s.carry = append([]byte(nil), b[i:]...)
			return len(p), nil
		default:
			r, size := utf8.DecodeRune(b[i:])
			s.text = !unicode.IsSpace(r)
			i += size
		}
	}
	return len(p), nil
}

func asciiSpace(c byte) bool {
	return c == ' ' || c == '\t' || c == '\n' || c == '\v' || c == '\f' || c == '\r'
}

// sum ends the member: bytes that end inside a rune are not white space,
// and a last line without a newline counts like any other.
func (s *summer) sum() MemberSum {
	if len(s.carry) > 0 {
		s.text = true
	}
	n := s.lines
	if s.text {
		n++
	}
	return MemberSum{SHA256: hex.EncodeToString(s.h.Sum(nil)), Bytes: s.bytes, Lines: n}
}

// CheckSidecar compares the tarball digest with a .sha256 sidecar
// ("<hex>  <name>\n", the sha256sum format).
func (a *Archive) CheckSidecar(sidecar []byte, name string) error {
	f := strings.Fields(string(sidecar))
	if len(f) == 0 {
		return errors.New("empty sidecar")
	}
	if !strings.EqualFold(f[0], a.SHA256) {
		return fmt.Errorf("sidecar says %s, the tarball is %s", f[0], a.SHA256)
	}
	if name != "" && len(f) > 1 && f[1] != name {
		return fmt.Errorf("sidecar names %s, not %s", f[1], name)
	}
	return nil
}

// CheckSignature verifies a .sig file's contents against the archive's
// manifest and a public key the caller obtained independently.
func (a *Archive) CheckSignature(sigJSON []byte, pub ed25519.PublicKey) (*Signature, error) {
	var sig Signature
	if err := json.Unmarshal(sigJSON, &sig); err != nil {
		return nil, fmt.Errorf(".sig: %w", err)
	}
	return &sig, VerifySignature(&sig, a.ManifestSHA256, pub)
}
