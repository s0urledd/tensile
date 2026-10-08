package export

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func buildOne(t *testing.T, signer *Signer) (dir, name string) {
	t.Helper()
	data := t.TempDir()
	dir = filepath.Join(data, "exports")
	if err := os.WriteFile(filepath.Join(data, "measurements.jsonl"),
		[]byte(line("started_at", "2026-09-10T10:00:00Z", "a")+line("started_at", "2026-09-10T11:00:00Z", "b")), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(data, StateFile), []byte(`{"chain_id":"mocha-4"}`), 0o644); err != nil {
		t.Fatal(err)
	}
	b := &Builder{DataDir: data, Dir: dir, Vantage: "v", Build: "x", Hour: 3, Signer: signer}
	built, err := b.Run(time.Date(2026, 9, 11, 3, 0, 0, 0, time.UTC))
	if err != nil || len(built) != 1 {
		t.Fatalf("built %v err %v", built, err)
	}
	return dir, built[0]
}

func newTestSigner(t *testing.T) *Signer {
	t.Helper()
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return NewSigner(priv)
}

// No key, no change: the index entry carries no signature key at all, no
// .sig is written and no key file appears. An operator who never configures
// a key must see exactly the exports they saw before signing existed.
func TestUnsignedExportIsUnchanged(t *testing.T) {
	dir, name := buildOne(t, nil)
	raw, err := os.ReadFile(filepath.Join(dir, "index.json"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "signature") {
		t.Fatalf("unsigned index mentions a signature:\n%s", raw)
	}
	for _, f := range []string{name + ".sig", SigningKeysFile} {
		if _, err := os.Stat(filepath.Join(dir, f)); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("%s exists on an unsigned build (err %v)", f, err)
		}
	}
}

// A signed export verifies end to end with nothing but the bytes a
// downloader holds and a key obtained separately; every piece that could be
// swapped is caught.
func TestSignedExportVerifies(t *testing.T) {
	signer := newTestSigner(t)
	dir, name := buildOne(t, signer)

	tarball, _ := os.ReadFile(filepath.Join(dir, name))
	sidecar, _ := os.ReadFile(filepath.Join(dir, name+".sha256"))
	sigJSON, err := os.ReadFile(filepath.Join(dir, name+".sig"))
	if err != nil {
		t.Fatalf("no .sig beside the export: %v", err)
	}
	a, err := ReadArchive(tarball)
	if err != nil {
		t.Fatal(err)
	}
	if p := a.CheckMembers(); len(p) != 0 {
		t.Fatalf("members: %v", p)
	}
	if err := a.CheckSidecar(sidecar, name); err != nil {
		t.Fatal(err)
	}
	sig, err := a.CheckSignature(sigJSON, signer.PublicKey())
	if err != nil {
		t.Fatalf("signature: %v", err)
	}
	if sig.Message != SignatureDomain+a.ManifestSHA256 {
		t.Errorf("message = %q", sig.Message)
	}

	// The index carries the same signature.
	idx, err := ReadIndex(dir)
	if err != nil || len(idx) != 1 || idx[0].Signature == nil {
		t.Fatalf("index = %+v err %v", idx, err)
	}
	if *idx[0].Signature != *sig {
		t.Errorf("index signature %+v differs from .sig %+v", idx[0].Signature, sig)
	}

	// The key is published, with the day it signed.
	keys, err := ReadSigningKeys(dir)
	if err != nil || len(keys) != 1 {
		t.Fatalf("keys = %+v err %v", keys, err)
	}
	if keys[0].KeyFingerprint != Fingerprint(signer.PublicKey()) || keys[0].FirstDay != "2026-09-10" || keys[0].LastDay != "2026-09-10" {
		t.Errorf("key entry = %+v", keys[0])
	}
	pub, err := ParsePublicKey([]byte(keys[0].PublicKeyPEM))
	if err != nil || !pub.Equal(signer.PublicKey()) {
		t.Errorf("published PEM does not parse back to the key: %v", err)
	}

	// Another key: refused, even though the .sig carries a public key of
	// its own that it would verify against.
	other := newTestSigner(t)
	if _, err := a.CheckSignature(sigJSON, other.PublicKey()); err == nil {
		t.Error("a signature verified against a key that did not make it")
	}
	// A forged signature under the right fingerprint.
	var forged Signature
	_ = json.Unmarshal(sigJSON, &forged)
	bad, _ := base64.StdEncoding.DecodeString(forged.Signature)
	bad[0] ^= 1
	forged.Signature = base64.StdEncoding.EncodeToString(bad)
	if err := VerifySignature(&forged, a.ManifestSHA256, signer.PublicKey()); err == nil {
		t.Error("a flipped signature bit verified")
	}
	// The signature moved onto another manifest.
	if err := VerifySignature(sig, strings.Repeat("0", 64), signer.PublicKey()); err == nil {
		t.Error("a signature verified against a manifest it does not cover")
	}
}

// A tarball whose member was changed, or that carries a member the
// manifest does not name, fails the member check even when the manifest
// itself (and so the signature) is untouched.
func TestCheckMembersCatchesTampering(t *testing.T) {
	dir, name := buildOne(t, newTestSigner(t))
	tarball, _ := os.ReadFile(filepath.Join(dir, name))
	// The same tarball with one byte of a member flipped and a member added.
	gz, err := gzip.NewReader(bytes.NewReader(tarball))
	if err != nil {
		t.Fatal(err)
	}
	tr := tar.NewReader(gz)
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	tw := tar.NewWriter(zw)
	for {
		h, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		b, err := io.ReadAll(tr)
		if err != nil {
			t.Fatal(err)
		}
		if h.Name == "measurements.jsonl" {
			b[2] ^= 1
		}
		if err := addMember(tw, h.Name, b, time.Now()); err != nil {
			t.Fatal(err)
		}
	}
	if err := addMember(tw, "extra.jsonl", []byte("{}\n"), time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	a, err := ReadArchive(buf.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	p := a.CheckMembers()
	if len(p) != 2 || !strings.Contains(p[0]+p[1], "measurements.jsonl: sha256") || !strings.Contains(p[0]+p[1], "extra.jsonl") {
		t.Fatalf("problems = %v", p)
	}
	if err := a.CheckSidecar([]byte(strings.Repeat("a", 64)+"  "+name+"\n"), name); err == nil {
		t.Error("a wrong sidecar digest passed")
	}
}

// A tarball carrying two members under one name is refused: an extracting
// tool would keep the last and a streaming reader might check the first.
func TestReadArchiveRefusesDuplicateMembers(t *testing.T) {
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	for _, body := range []string{"{}", "{}"} {
		_ = addMember(tw, "manifest.json", []byte(body), time.Now())
	}
	_ = tw.Close()
	_ = gz.Close()
	if _, err := ReadArchive(buf.Bytes()); err == nil || !strings.Contains(err.Error(), "twice") {
		t.Fatalf("err = %v", err)
	}
}

func TestKeyFilesRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "export-signing.pem")
	fp, err := GenerateKey(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := GenerateKey(path); err == nil {
		t.Error("GenerateKey overwrote an existing key")
	}
	s, err := LoadSigner(path)
	if err != nil {
		t.Fatal(err)
	}
	if Fingerprint(s.PublicKey()) != fp {
		t.Error("the loaded key is not the generated one")
	}
	pubPEM, _ := os.ReadFile(path + ".pub")
	pub, err := ParsePublicKey(pubPEM)
	if err != nil || !pub.Equal(s.PublicKey()) {
		t.Fatalf("pub file: %v", err)
	}
	for _, form := range []string{hex.EncodeToString(pub), base64.StdEncoding.EncodeToString(pub),
		`{"current":{"public_key":"` + base64.StdEncoding.EncodeToString(pub) + `"}}`} {
		got, err := ParsePublicKey([]byte(form))
		if err != nil || !got.Equal(pub) {
			t.Errorf("ParsePublicKey(%q) = %v", form, err)
		}
	}
	// A public key where the private key belongs is an error, not unsigned.
	if _, err := LoadSigner(path + ".pub"); err == nil {
		t.Error("LoadSigner accepted a public key file")
	}
}

func TestNamePatternAdmitsSignatures(t *testing.T) {
	if !NamePattern.MatchString("tensile-v-2026-09-10.tar.gz.sig") {
		t.Error(".sig must be servable")
	}
	for _, bad := range []string{"tensile-v-2026-09-10.tar.gz.sig.sig", "tensile-v-2026-09-10.sig", SigningKeysFile} {
		if NamePattern.MatchString(bad) {
			t.Errorf("%q must not match", bad)
		}
	}
}

// The anchor payload is canonical (same export, same bytes) and binds the
// signature only to the manifest it was made over.
func TestAnchorPayload(t *testing.T) {
	signer := newTestSigner(t)
	dir, name := buildOne(t, signer)
	tarball, _ := os.ReadFile(filepath.Join(dir, name))
	a, _ := ReadArchive(tarball)
	sigJSON, _ := os.ReadFile(filepath.Join(dir, name+".sig"))
	sig, err := a.CheckSignature(sigJSON, signer.PublicKey())
	if err != nil {
		t.Fatal(err)
	}
	p, err := NewAnchorPayload(name, a, sig)
	if err != nil {
		t.Fatal(err)
	}
	b1, _ := p.Bytes()
	b2, _ := p.Bytes()
	if !bytes.Equal(b1, b2) || !strings.HasPrefix(string(b1), `{"type":"`+AnchorPayloadType+`","vantage":"v","day":"2026-09-10"`) {
		t.Fatalf("payload = %s", b1)
	}
	if !strings.Contains(string(b1), a.ManifestSHA256) || !strings.Contains(string(b1), sig.KeyFingerprint) {
		t.Errorf("payload does not name the digest and key: %s", b1)
	}
	if len(b1) > 1024 {
		t.Errorf("payload is %d bytes; it is meant to be a share or two", len(b1))
	}
	other := *sig
	other.ManifestSHA256 = strings.Repeat("0", 64)
	if _, err := NewAnchorPayload(name, a, &other); err == nil {
		t.Error("a signature over another manifest was anchored")
	}
	unsigned, err := NewAnchorPayload(name, a, nil)
	if err != nil || unsigned.Signature != "" {
		t.Errorf("unsigned anchor = %+v err %v", unsigned, err)
	}
}

// pubkeyJSON is /v1/exports/pubkey's key fields over dir's record.
func pubkeyJSON(t *testing.T, dir string) []byte {
	t.Helper()
	keys, err := ReadSigningKeys(dir)
	if err != nil || len(keys) == 0 {
		t.Fatalf("keys = %+v err %v", keys, err)
	}
	raw, err := json.Marshal(map[string]any{"current": keys[0], "keys": keys})
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

// verifyWith checks the export name in dir against the keys a reader holds,
// picking the one its .sig names.
func verifyWith(t *testing.T, dir, name string, pubs []ed25519.PublicKey) error {
	t.Helper()
	tarball, _ := os.ReadFile(filepath.Join(dir, name))
	sigJSON, err := os.ReadFile(filepath.Join(dir, name+".sig"))
	if err != nil {
		t.Fatal(err)
	}
	a, err := ReadArchive(tarball)
	if err != nil {
		t.Fatal(err)
	}
	var named Signature
	_ = json.Unmarshal(sigJSON, &named)
	_, err = a.CheckSignature(sigJSON, KeyFor(pubs, &named))
	return err
}

// After a rotation the new key is current from its first export on, and an
// export the old key signed still verifies against the key record a reader
// fetched after the rotation.
func TestARotationKeepsOldExportsVerifiable(t *testing.T) {
	k1, k2 := newTestSigner(t), newTestSigner(t)
	dir, old := buildOne(t, k1)
	data := filepath.Dir(dir)
	if err := os.WriteFile(filepath.Join(data, "measurements.jsonl"), []byte(line("started_at", "2026-09-11T10:00:00Z", "c")), 0o644); err != nil {
		t.Fatal(err)
	}
	b := &Builder{DataDir: data, Dir: dir, Vantage: "v", Build: "x", Hour: 3, Signer: k2}
	built, err := b.Run(time.Date(2026, 9, 12, 3, 0, 0, 0, time.UTC))
	if err != nil || len(built) != 1 {
		t.Fatalf("built %v err %v", built, err)
	}
	keys, err := ParsePublicKeys(pubkeyJSON(t, dir))
	if err != nil || len(keys) != 2 || !keys[0].Equal(k2.PublicKey()) {
		t.Fatalf("keys after the rotation: %d err %v, want the new one first of two", len(keys), err)
	}
	if err := verifyWith(t, dir, built[0], keys); err != nil {
		t.Errorf("the new export: %v", err)
	}
	if err := verifyWith(t, dir, old, keys); err != nil {
		t.Errorf("the export signed before the rotation: %v", err)
	}
}

// A new key re-signing a day the old key had signed (a crash before the
// state was saved, then the rotation) is current at once: the pubkey route
// must not name the old key beside a .sig and an index entry naming the new.
func TestARotationOnARebuiltDayMakesTheNewKeyCurrent(t *testing.T) {
	k1, k2 := newTestSigner(t), newTestSigner(t)
	dir, name := buildOne(t, k1)
	if err := os.Remove(filepath.Join(dir, "state.json")); err != nil {
		t.Fatal(err)
	}
	b := &Builder{DataDir: filepath.Dir(dir), Dir: dir, Vantage: "v", Build: "x", Hour: 3, Signer: k2}
	if built, err := b.Run(time.Date(2026, 9, 11, 3, 0, 0, 0, time.UTC)); err != nil || len(built) != 1 || built[0] != name {
		t.Fatalf("rebuilt %v err %v", built, err)
	}
	cur, err := ParsePublicKey(pubkeyJSON(t, dir))
	if err != nil || !cur.Equal(k2.PublicKey()) {
		t.Fatalf("current key is not the one that signed the newest export (err %v)", err)
	}
	if err := verifyWith(t, dir, name, []ed25519.PublicKey{cur}); err != nil {
		t.Errorf("the newest export against the current key: %v", err)
	}
}
