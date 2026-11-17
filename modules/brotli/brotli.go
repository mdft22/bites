// Package brotli is the br content coding for bites' response compression (ADR 0061), a module
// of its own so that the core takes no third-party dependency (ADR 0006, ADR 0070). It compresses
// with github.com/andybalholm/brotli's matchfinder writer at level 1:
//
//	app := bites.New(bites.WithCompression(bites.Compression{
//		Encoders: []bites.Encoder{brotli.Encoder(), zstd.Encoder(), bites.GzipEncoder()},
//	}))
package brotli

import (
	"bytes"
	"io"
	"sync"

	abrotli "github.com/andybalholm/brotli"
	"github.com/andybalholm/brotli/matchfinder"
	"github.com/nevindra/bites"
)

// Level is the brotli level: 1, of the matchfinder writer (NewWriterV2). It took about the CPU of
// gzip at BestSpeed and made answers 14 to 42% smaller than gzip's on the JSON of
// docs/research/encoders; level 4 took 4 to 8 times the CPU of gzip.
const Level = 1

// encoder is the module's Encoder: a pool of writers, each used by one answer at a time.
type encoder struct{ pool sync.Pool }

// state is one pooled writer and the sink its output is appended to.
type state struct {
	w   *matchfinder.Writer
	out sink
}

// sink is the io.Writer the compressor appends to: the caller's dst.
type sink struct{ b []byte }

func (s *sink) Write(p []byte) (int, error) { s.b = append(s.b, p...); return len(p), nil }

// the one encoder of the process, so that every app shares its pool.
var shared = &encoder{pool: sync.Pool{New: func() any {
	st := &state{}
	st.w = abrotli.NewWriterV2(&st.out, Level)
	return st
}}}

// Encoder is the brotli Encoder for bites.Compression: level 1 of the matchfinder writer, a
// whole answer per stream. Its writers are pooled and never reach user code (ADR 0008); about
// GOMAXPROCS of them are live at once, because compression never blocks, and the garbage
// collector drops the ones left idle.
func Encoder() bites.Encoder { return shared }

// Coding is "br".
func (*encoder) Coding() string { return "br" }

// Encode appends one brotli stream holding src to dst.
func (e *encoder) Encode(dst, src []byte) []byte {
	st := e.pool.Get().(*state)
	st.out.b = dst
	st.w.Reset(&st.out)
	st.w.Write(src)
	st.w.Close()
	dst = st.out.b
	st.out.b = nil // the pooled writer keeps no reference to the caller's buffer
	e.pool.Put(st)
	return dst
}

// decoder is the module's Decoder.
type decoder struct{}

// Decoder is the brotli Decoder for bites.Decompress and bites.WithDecompression: it decodes a
// request body sent with Content-Encoding: br, stopping at the route's MaxBody of decoded bytes.
func Decoder() bites.Decoder { return decoder{} }

// Coding is "br".
func (decoder) Coding() string { return "br" }

// Decode appends the decoded form of the brotli stream src to dst, and returns
// bites.ErrDecodedTooLarge when it would hold more than max bytes.
func (decoder) Decode(dst, src []byte, max int64) ([]byte, error) {
	buf := bytes.NewBuffer(dst)
	// One byte more than max tells a body that is exactly max from one that is over it.
	if _, err := buf.ReadFrom(io.LimitReader(abrotli.NewReader(bytes.NewReader(src)), max+1)); err != nil {
		return dst, err
	}
	if int64(buf.Len()-len(dst)) > max {
		return dst, bites.ErrDecodedTooLarge
	}
	return buf.Bytes(), nil
}
