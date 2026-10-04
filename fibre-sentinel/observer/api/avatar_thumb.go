package api

import (
	"bytes"
	"image"
	"image/color"
	_ "image/gif" // decodes a GIF original; the square comes back as a PNG
	"image/jpeg"
	"image/png"
	"strconv"
	"sync"
	"time"
)

// The site draws a validator's picture at 24 to 36 px, and the Keybase
// originals run to 40 KB each: a page of validators fetched two megabytes of
// them. ?s= asks for the picture as a small square of one of these sides,
// made once from the stored original and kept while that original is the
// same one.
var thumbSides = map[int]bool{72: true}

// thumbMaxPixels bounds the original a square is made from: a picture whose
// header claims more is served as it is rather than decoded.
const thumbMaxPixels = 4096 * 4096

type thumbKey struct {
	id   string
	side int
}

type thumbVal struct {
	checked time.Time
	ct      string
	data    []byte
}

var thumbs sync.Map // thumbKey -> thumbVal

// thumbSide is the side ?s= asks for, or 0 for the original.
func thumbSide(q string) int {
	n, err := strconv.Atoi(q)
	if err != nil || !thumbSides[n] {
		return 0
	}
	return n
}

// avatarThumb is the picture as a side x side square, centred and averaged
// down, with its content type: a JPEG stays a JPEG, and a PNG or GIF, which
// can be transparent, becomes a PNG. The original comes back when it is
// already that small, does not decode, or would not get smaller.
func avatarThumb(id, ct string, data []byte, checked time.Time, side int) (string, []byte) {
	k := thumbKey{id, side}
	if v, ok := thumbs.Load(k); ok {
		if t := v.(thumbVal); t.checked.Equal(checked) {
			return t.ct, t.data
		}
	}
	cfg, _, err := image.DecodeConfig(bytes.NewReader(data))
	if err != nil || cfg.Width*cfg.Height > thumbMaxPixels {
		return ct, data
	}
	src, format, err := image.Decode(bytes.NewReader(data))
	if err != nil {
		return ct, data
	}
	b := src.Bounds()
	if b.Dx() <= side && b.Dy() <= side {
		return ct, data
	}
	sq := b.Dx()
	if b.Dy() < sq {
		sq = b.Dy()
	}
	x0, y0 := b.Min.X+(b.Dx()-sq)/2, b.Min.Y+(b.Dy()-sq)/2
	dst := image.NewRGBA(image.Rect(0, 0, side, side))
	for oy := 0; oy < side; oy++ {
		sy0, sy1 := y0+oy*sq/side, y0+(oy+1)*sq/side
		if sy1 <= sy0 {
			sy1 = sy0 + 1
		}
		for ox := 0; ox < side; ox++ {
			sx0, sx1 := x0+ox*sq/side, x0+(ox+1)*sq/side
			if sx1 <= sx0 {
				sx1 = sx0 + 1
			}
			// every source pixel under this one, averaged: premultiplied, as RGBA() gives them
			var r, g, bl, a, n uint64
			for y := sy0; y < sy1; y++ {
				for x := sx0; x < sx1; x++ {
					cr, cg, cb, ca := src.At(x, y).RGBA()
					r, g, bl, a, n = r+uint64(cr), g+uint64(cg), bl+uint64(cb), a+uint64(ca), n+1
				}
			}
			dst.SetRGBA(ox, oy, color.RGBA{uint8(r / n >> 8), uint8(g / n >> 8), uint8(bl / n >> 8), uint8(a / n >> 8)})
		}
	}
	var buf bytes.Buffer
	outCT := "image/png"
	if format == "jpeg" {
		outCT = "image/jpeg"
		err = jpeg.Encode(&buf, dst, &jpeg.Options{Quality: 85})
	} else {
		err = png.Encode(&buf, dst)
	}
	if err != nil || buf.Len() >= len(data) {
		return ct, data
	}
	t := thumbVal{checked: checked, ct: outCT, data: buf.Bytes()}
	thumbs.Store(k, t)
	return t.ct, t.data
}
