package api

import (
	"bytes"
	"image"
	"image/color"
	"image/png"
	"testing"
	"time"
)

// A PNG, which can be transparent, comes back as a PNG square that keeps its
// transparency; an original already that small comes back as it is.
func TestAvatarThumbPNG(t *testing.T) {
	src := image.NewNRGBA(image.Rect(0, 0, 240, 180))
	for y := 0; y < 180; y++ {
		for x := 0; x < 240; x++ {
			a := uint8(255)
			if x < 120 {
				a = 0
			}
			src.SetNRGBA(x, y, color.NRGBA{uint8(x), uint8(y), uint8(x ^ y), a})
		}
	}
	var orig bytes.Buffer
	if err := png.Encode(&orig, src); err != nil {
		t.Fatal(err)
	}
	ct, data := avatarThumb("PNGTEST", "image/png", orig.Bytes(), time.Unix(1, 0), 72)
	img, err := png.Decode(bytes.NewReader(data))
	if err != nil || ct != "image/png" || img.Bounds().Dx() != 72 || img.Bounds().Dy() != 72 || len(data) >= orig.Len() {
		t.Fatalf("png square: %q %v %d of %d bytes", ct, err, len(data), orig.Len())
	}
	// the left of the centred square was transparent in the original, and stays so
	if _, _, _, a := img.At(2, 36).RGBA(); a != 0 {
		t.Errorf("transparency lost: alpha %d", a)
	}
	var small bytes.Buffer
	if err := png.Encode(&small, image.NewNRGBA(image.Rect(0, 0, 48, 48))); err != nil {
		t.Fatal(err)
	}
	if _, d := avatarThumb("SMALL", "image/png", small.Bytes(), time.Unix(1, 0), 72); !bytes.Equal(d, small.Bytes()) {
		t.Error("an original smaller than the square was not served as it is")
	}
}
