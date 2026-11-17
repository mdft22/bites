// Package zstd is the zstd content coding for bites' response compression (ADR 0061), a
// module of its own so that the core takes no third-party dependency (ADR 0006, ADR 0070). It
// compresses with github.com/klauspost/compress/zstd at its fastest level:
//
//	app := bites.New(bites.WithCompression(bites.Compression{
//		Encoders: []bites.Encoder{brotli.Encoder(), zstd.Encoder(), bites.GzipEncoder()},
//	}))
package zstd

import (
	"bytes"
	"io"
	"sync"

	kzstd "github.com/klauspost/compress/zstd"
	"github.com/nevindra/bites"
)

// Window is the window of every frame: 1 MiB. RFC 9659 lets a zstd content coding use at most
// 8 MiB, and a smaller window is what keeps an encoder state at about 1.5 MB instead of 9 MB
// (docs/research/encoders); an answer longer than the window is still compressed, its matches
// reaching back at most 1 MiB.
const Window = 1 << 20

// encoder is the module's Encoder: a pool of zstd encoders, each used by one answer at a time.
type encoder struct{ pool sync.Pool }

// the one encoder of the process, so that every app shares its pool.
var shared = &encoder{pool: sync.Pool{New: func() any {
	// The options are constants that NewWriter accepts, so the error is nil.
	e, _ := kzstd.NewWriter(nil,
		kzstd.WithEncoderLevel(kzstd.SpeedFastest),
		kzstd.WithEncoderConcurrency(1),
		kzstd.WithWindowSize(Window),
		kzstd.WithLowerEncoderMem(true))
	return e
}}}

// Encoder is the zstd Encoder for bites.Compression: SpeedFastest, a 1 MiB window, a content
// checksum, and one frame per answer written with EncodeAll. Its states are pooled and never
// reach user code (ADR 0008); about GOMAXPROCS of them are live at once, because compression
// never blocks, and the garbage collector drops the ones left idle.
func Encoder() bites.Encoder { return shared }

// Coding is "zstd".
func (*encoder) Coding() string { return "zstd" }

// Encode appends one zstd frame holding src to dst.
func (e *encoder) Encode(dst, src []byte) []byte {
	z := e.pool.Get().(*kzstd.Encoder)
	dst = z.EncodeAll(src, dst)
	e.pool.Put(z)
	return dst
}

// decoder is the module's Decoder: a pool of zstd readers, each used by one request at a time.
type decoder struct{ pool sync.Pool }

var sharedDecoder = &decoder{}

// Decoder is the zstd Decoder for bites.Decompress and bites.WithDecompression: it decodes a
// request body sent with Content-Encoding: zstd, stopping at the route's MaxBody of decoded
// bytes. Its readers are pooled and never reach user code (ADR 0008). The decoder's memory is
// held under the window the frame names, and a frame that names a window over 8 MiB, which RFC
// 9659 forbids for a content coding, is refused.
func Decoder() bites.Decoder { return sharedDecoder }

// Coding is "zstd".
func (*decoder) Coding() string { return "zstd" }

// Decode appends the decoded form of the zstd frame src to dst, and returns bites.ErrDecodedTooLarge
// when it would hold more than max bytes.
func (d *decoder) Decode(dst, src []byte, max int64) ([]byte, error) {
	r := bytes.NewReader(src)
	z, _ := d.pool.Get().(*kzstd.Decoder)
	if z == nil {
		var err error
		// The options are constants that NewReader accepts. A window over 8 MiB is refused as
		// RFC 9659 says, which bounds what one frame can ask the decoder to hold.
		if z, err = kzstd.NewReader(r, kzstd.WithDecoderConcurrency(1), kzstd.WithDecoderMaxWindow(8<<20)); err != nil {
			return dst, err
		}
	} else if err := z.Reset(r); err != nil {
		return dst, err
	}
	defer d.pool.Put(z)
	buf := bytes.NewBuffer(dst)
	// One byte more than max tells a body that is exactly max from one that is over it.
	if _, err := buf.ReadFrom(io.LimitReader(z, max+1)); err != nil {
		return dst, err
	}
	if int64(buf.Len()-len(dst)) > max {
		return dst, bites.ErrDecodedTooLarge
	}
	return buf.Bytes(), nil
}
