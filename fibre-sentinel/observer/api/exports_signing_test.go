package api_test

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/observer/api"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/export"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
)

// An export built with a key is served with its .sig, the index names the
// key by its fingerprint, and /v1/exports/pubkey hands out the key that
// verifies it. Without a key the pubkey route is a JSON 404. The list does
// not repeat the key block: /v1/exports/pubkey is where it is.
func TestSignedExportsAreServedWithTheirKey(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "observer.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	dataDir := t.TempDir()

	// Before any signed export: unsigned, and the route says so.
	ts := httptestServerWith(t, st, api.WithDataDir(dataDir))
	var list map[string]json.RawMessage
	if code := get(t, ts, "/v1/exports", &list); code != 200 {
		t.Fatalf("exports before signing: %d", code)
	}
	if _, ok := list["signing"]; ok {
		t.Fatalf("the list carries the signing block: %s", list["signing"])
	}
	if code := get(t, ts, "/v1/exports/pubkey", nil); code != 404 {
		t.Fatalf("pubkey with no signed export: %d, want 404", code)
	}

	// One signed export.
	if err := os.WriteFile(filepath.Join(dataDir, "measurements.jsonl"), []byte(`{"started_at":"2026-09-10T10:00:00Z"}`+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	_, priv, _ := ed25519.GenerateKey(rand.Reader)
	signer := export.NewSigner(priv)
	b := &export.Builder{DataDir: dataDir, Dir: filepath.Join(dataDir, "exports"), Vantage: "test", Build: "x", Hour: 3, Signer: signer}
	built, err := b.Run(time.Date(2026, 9, 11, 3, 0, 0, 0, time.UTC))
	if err != nil || len(built) != 1 {
		t.Fatalf("built %v err %v", built, err)
	}
	name := built[0]

	var key struct {
		Signed  bool `json:"signed"`
		Current struct {
			KeyFingerprint string `json:"key_fingerprint"`
			PublicKeyPEM   string `json:"public_key_pem"`
		} `json:"current"`
		VerifyCommand string `json:"verify_command"`
	}
	if code := get(t, ts, "/v1/exports/pubkey", &key); code != 200 {
		t.Fatalf("pubkey: %d", code)
	}
	pub, err := export.ParsePublicKey([]byte(key.Current.PublicKeyPEM))
	if err != nil || !pub.Equal(signer.PublicKey()) || key.Current.KeyFingerprint != export.Fingerprint(pub) || key.VerifyCommand == "" {
		t.Fatalf("pubkey answer = %+v (err %v)", key, err)
	}

	// The .sig and the tarball as a downloader gets them verify against
	// the key the route served.
	fetch := func(path string) ([]byte, string) {
		r, err := http.Get(ts.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		defer r.Body.Close()
		if r.StatusCode != 200 {
			t.Fatalf("%s: %d", path, r.StatusCode)
		}
		body, _ := io.ReadAll(r.Body)
		return body, r.Header.Get("Content-Type")
	}
	tarball, _ := fetch("/v1/exports/" + name)
	sigJSON, ct := fetch("/v1/exports/" + name + ".sig")
	if ct != "application/json; charset=utf-8" {
		t.Errorf(".sig content type %q", ct)
	}
	a, err := export.ReadArchive(tarball)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.CheckSignature(sigJSON, pub); err != nil {
		t.Fatalf("served export does not verify against the served key: %v", err)
	}

	var idx struct {
		Exports []struct {
			Signature *struct {
				KeyFingerprint string `json:"key_fingerprint"`
				ManifestSHA256 string `json:"manifest_sha256"`
			} `json:"signature"`
		} `json:"exports"`
	}
	if code := get(t, ts, "/v1/exports", &idx); code != 200 || len(idx.Exports) != 1 || idx.Exports[0].Signature == nil ||
		idx.Exports[0].Signature.KeyFingerprint != export.Fingerprint(pub) || idx.Exports[0].Signature.ManifestSHA256 != a.ManifestSHA256 {
		t.Fatalf("index entry does not carry the signature: %d %+v", code, idx)
	}
	// The key record itself is not reachable as an export file.
	if code := get(t, ts, "/v1/exports/"+export.SigningKeysFile, nil); code != 404 {
		t.Errorf("%s served as an export: %d", export.SigningKeysFile, code)
	}
}
