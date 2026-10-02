package api

import (
	"bytes"
	"image"
	"image/color"
	"image/jpeg"
	"image/png"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/scan"
)

// ?s=72 serves the picture as a 72 px square, smaller than the original and of the
// same kind; any other side, or none, serves the original as stored.
func TestAvatarThumbnail(t *testing.T) {
	ts, st := serverAndStore(t)
	now := time.Now()
	if _, err := st.UpsertValidatorIdentities([]scan.ValidatorIdentity{{ConsAddressHex: "bb", Moniker: "y", Identity: "A1B2C3D4E5F60718", Status: "BOND_STATUS_BONDED"}}, now); err != nil {
		t.Fatal(err)
	}
	src := image.NewRGBA(image.Rect(0, 0, 400, 300))
	for y := 0; y < 300; y++ {
		for x := 0; x < 400; x++ {
			src.SetRGBA(x, y, color.RGBA{uint8(x), uint8(y), uint8(x ^ y), 255})
		}
	}
	var orig bytes.Buffer
	if err := jpeg.Encode(&orig, src, &jpeg.Options{Quality: 95}); err != nil {
		t.Fatal(err)
	}
	if err := st.PutAvatar("A1B2C3D4E5F60718", "ok", "https://x/pic.jpg", "image/jpeg", orig.Bytes(), now); err != nil {
		t.Fatal(err)
	}
	get := func(q string) (string, []byte) {
		r, err := http.Get(ts.URL + "/v1/avatars/a1b2c3d4e5f60718" + q)
		if err != nil {
			t.Fatal(err)
		}
		defer r.Body.Close()
		body, _ := io.ReadAll(r.Body)
		if r.StatusCode != 200 {
			t.Fatalf("%s: %d", q, r.StatusCode)
		}
		return r.Header.Get("Content-Type"), body
	}
	for i := 0; i < 2; i++ { // the second answer comes from the kept square
		ct, body := get("?s=72")
		cfg, format, err := image.DecodeConfig(bytes.NewReader(body))
		if err != nil || ct != "image/jpeg" || format != "jpeg" || cfg.Width != 72 || cfg.Height != 72 || len(body) >= orig.Len() {
			t.Fatalf("thumb %d: %q %q %dx%d %d of %d bytes (%v)", i, ct, format, cfg.Width, cfg.Height, len(body), orig.Len(), err)
		}
	}
	for _, q := range []string{"", "?s=999", "?s=x"} {
		if ct, body := get(q); ct != "image/jpeg" || !bytes.Equal(body, orig.Bytes()) {
			t.Errorf("%q: not the original (%q, %d bytes)", q, ct, len(body))
		}
	}
	// a transparent PNG keeps its transparency as a PNG square
	pngSrc := image.NewNRGBA(image.Rect(0, 0, 200, 200))
	var pbuf bytes.Buffer
	if err := png.Encode(&pbuf, pngSrc); err != nil {
		t.Fatal(err)
	}
	if ct, data := avatarThumb("P", "image/png", pbuf.Bytes(), now, 72); ct != "image/png" {
		t.Errorf("png square: %q", ct)
	} else if img, err := png.Decode(bytes.NewReader(data)); err != nil || img.Bounds().Dx() != 72 {
		// a fully transparent 200 px PNG compresses to less than its square: the original comes back
		if !bytes.Equal(data, pbuf.Bytes()) {
			t.Errorf("png square: %v", err)
		}
	}
}
