package bites

import (
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/nevindra/bites/internal/engine"
	"github.com/nevindra/bites/internal/h1"
)

// Decoder is one content coding the framework can decode a request body from, the counterpart of
// Encoder: gzip from the core (GzipDecoder), zstd and brotli from the modules
// github.com/nevindra/bites/modules/zstd and github.com/nevindra/bites/modules/brotli. A route
// reads it from its Content-Encoding header when the route, a group or the app opts in with
// Decompress or WithDecompression (ADR 0044). An application rarely writes one.
//
// Coding is the token Content-Encoding names it by ("gzip", "zstd", "br"), a lowercase token read
// once at Registration. Decode appends the decoded form of src to dst and returns it, and must
// stop as soon as the decoded body would be over max bytes, returning ErrDecodedTooLarge: the
// framework counts the decoded size against MaxBody and never the compressed one, so that a small
// body that expands a thousandfold is a 413 at the limit and not an allocation of its own size.
// Any other error means src is not valid in this coding, and is a 400. It is called from many
// connections at once, and must keep neither src nor dst past its return, because src is the
// request's own buffer (ADR 0008). A panic in Decode is recovered and answered as a 400.
type Decoder interface {
	Coding() string
	Decode(dst, src []byte, max int64) ([]byte, error)
}

// ErrDecodedTooLarge is what a Decoder returns when the decoded body would be over the max it was
// given. The framework answers it with a 413 naming MaxBody.
var ErrDecodedTooLarge = errors.New("bites: the decoded body is over the limit")

// GzipDecoder is the core's gzip Decoder: compress/gzip, its readers pooled. A body of several
// gzip members is decoded as one, as RFC 1952 says.
func GzipDecoder() Decoder { return gzipDec{} }

type gzipDec struct{}

var gzipReaders sync.Pool // *gzip.Reader

func (gzipDec) Coding() string { return "gzip" }

func (gzipDec) Decode(dst, src []byte, max int64) ([]byte, error) {
	r := bytes.NewReader(src)
	zr, _ := gzipReaders.Get().(*gzip.Reader)
	if zr == nil {
		var err error
		if zr, err = gzip.NewReader(r); err != nil {
			return dst, err
		}
	} else if err := zr.Reset(r); err != nil {
		return dst, err
	}
	defer gzipReaders.Put(zr)
	buf := bytes.NewBuffer(dst)
	if max < int64(len(src))*4 {
		buf.Grow(int(max))
	} else {
		buf.Grow(len(src) * 4)
	}
	// One byte more than max tells a body that is exactly max from one that is over it.
	if _, err := buf.ReadFrom(io.LimitReader(zr, max+1)); err != nil {
		return dst, err
	}
	if int64(buf.Len()-len(dst)) > max {
		return dst, ErrDecodedTooLarge
	}
	return buf.Bytes(), nil
}

// Decompress makes this route (or every route of this group) decode a request body that arrives
// with Content-Encoding, with the Decoders it lists, or gzip alone when it lists none (ADR 0044).
// The decoded body is what the handler's Body section reads, and its size is counted against
// MaxBody, never the compressed size, so a body that expands past the limit is a 413 at the limit.
// A body sent in a coding the route does not list is a 415 naming the coding it may use, and an
// invalid one is a 400. Without Decompress, WithDecompression or a group's, a request with a
// Content-Encoding other than identity is a 415 that says the route does not decode it: the
// bytes are never handed to a JSON decoder compressed. It covers a JSON, form, Raw and []byte
// Body; a Multipart, Upload or BodyReader body is a stream the framework does not decode, and
// Decompress on its route is a Registration panic. The innermost option wins, so a route can
// list other codings than its group. It makes the route panic at Registration for a nil Decoder,
// a Coding that is not a lowercase token (or is "identity" or "*") and one listed twice.
func Decompress(decoders ...Decoder) RouteOption {
	return func(c *routeConfig) {
		list, err := checkDecoders(decoders)
		if err != nil {
			c.refuse("Decompress: %v", err)
			return
		}
		c.decoders, c.decodersSet = list, true
	}
}

// WithDecompression is Decompress for every route of the app that reads a JSON, form, Raw or
// []byte Body; a route or a group may give its own Decompress to list other codings. It panics at
// New for what Decompress refuses, and when given twice. A route with a streamed Body (Multipart,
// Upload, BodyReader) is not decoded and is not refused.
func WithDecompression(decoders ...Decoder) Option {
	where := callerWhere()
	return func(a *App) {
		list, err := checkDecoders(decoders)
		if err == nil && a.decoders != nil {
			err = errors.New("it is given twice: give one WithDecompression that lists every Decoder")
		}
		if err != nil {
			panic(&RegistrationError{msg: "WithDecompression: " + err.Error() + "\n\tregistered at " + where})
		}
		a.decoders = list
		a.custom["decompression"] = true
	}
}

func checkDecoders(decoders []Decoder) ([]Decoder, error) {
	list := append([]Decoder(nil), decoders...)
	if len(list) == 0 {
		list = []Decoder{GzipDecoder()}
	}
	seen := map[string]int{}
	for i, d := range list {
		if d == nil {
			return nil, fmt.Errorf("Decoders[%d] is nil", i)
		}
		cd := d.Coding()
		if !isToken(cd) || strings.ToLower(cd) != cd || cd == "identity" || cd == "*" {
			return nil, fmt.Errorf("Decoders[%d] has the coding %q, which is not a lowercase content coding token such as \"gzip\", \"zstd\" or \"br\"", i, cd)
		}
		if j, ok := seen[cd]; ok {
			return nil, fmt.Errorf("Decoders[%d] has the coding %q, as Decoders[%d] has: list each coding once", i, cd, j)
		}
		seen[cd] = i
	}
	return list, nil
}

// codings is the Content-Encoding of head as a list in the order it was applied, without
// identity, and whether the header is present at all.
func codings(head *h1.Request) (list []string, present bool) {
	for i := range head.Headers {
		h := &head.Headers[i]
		if !equalFold(h.Name, "content-encoding") {
			continue
		}
		present = true
		for _, tok := range strings.Split(string(h.Value), ",") {
			if tok = strings.ToLower(strings.TrimSpace(tok)); tok != "" && tok != "identity" {
				list = append(list, tok)
			}
		}
	}
	return list, present
}

// decoded turns the body ReadBody returned into what the client meant, by the route's Decoders,
// and reports false when it has answered. A body with no Content-Encoding, or identity, is
// returned as it is.
func decoded(ex *engine.Exchange, body []byte, cfg *routeConfig) ([]byte, bool) {
	list, _ := codings(ex.Head())
	if len(list) == 0 || len(body) == 0 {
		return body, true
	}
	// The codings are listed in the order they were applied, so they are undone last first.
	for i := len(list) - 1; i >= 0; i-- {
		var d Decoder
		for _, c := range cfg.decoders {
			if c.Coding() == list[i] {
				d = c
				break
			}
		}
		if d == nil {
			offered := make([]string, len(cfg.decoders))
			for j, c := range cfg.decoders {
				offered[j] = c.Coding()
			}
			if len(offered) == 0 {
				problem(ex, 415, "this route does not decode a body sent with Content-Encoding: "+list[i]+"; send it uncompressed")
			} else {
				problem(ex, 415, "this route does not decode Content-Encoding: "+list[i]+"; it accepts "+strings.Join(offered, ", ")+", or an uncompressed body").Header("Accept-Encoding", strings.Join(offered, ", "))
			}
			return nil, false
		}
		out, err := runDecoder(d, body, cfg.maxBody)
		switch {
		case errors.Is(err, ErrDecodedTooLarge):
			problem(ex, 413, fmt.Sprintf("the body is over the %d bytes this route reads once decoded from %s", cfg.maxBody, list[i]))
			return nil, false
		case err != nil:
			problem(ex, 400, "the body is not valid "+list[i])
			return nil, false
		}
		body = out
	}
	return body, true
}

// runDecoder calls d with a recovered panic as an error, so a Decoder that fails is a 400 and
// not a dead connection.
func runDecoder(d Decoder, src []byte, max int64) (out []byte, err error) {
	defer func() {
		if r := recover(); r != nil {
			out, err = nil, fmt.Errorf("the %s Decoder panicked: %v", d.Coding(), r)
		}
	}()
	return d.Decode(nil, src, max)
}

// WebSocketCompression configures permessage-deflate for the app's WebSockets (RFC 7692, ADR 0040).
type WebSocketCompression struct {
	// MinSize is the smallest message payload, in bytes, that is compressed: a smaller message is
	// sent as it is, because compressing a few dozen bytes costs more CPU than it saves bytes.
	// Zero is the default of 256 bytes. It must not be negative.
	MinSize int
}

// DefaultWebSocketCompressionMinSize is the MinSize of a WebSocketCompression that leaves it zero.
const DefaultWebSocketCompressionMinSize = 256

// WithWebSocketCompression offers permessage-deflate to every client that asks for it (browsers
// do), so that the JSON frames of a chatty protocol (presence, collaborative operations, streamed
// text) go compressed (ADR 0040). It is off without this option. It negotiates the one mode that
// keeps no memory for a socket: server_no_context_takeover and client_no_context_takeover, so
// every message is compressed and inflated on its own, with a pooled state held only while it
// runs, and an idle socket costs what it cost before. A message of a Topic is compressed once for
// all its Sockets. A message under MinSize, and one that compressing does not make smaller, is
// sent as it is, which the RFC allows.
//
// What a client sends is held to the same limits as a plain message: the route's MaxMessage is
// counted on the inflated message, so a message that inflates past it closes the socket with 1009,
// data that is not valid deflate and a text message that is not valid UTF-8 close it with 1007.
// A client that does not offer the extension, or offers it with a window it cannot meet
// (server_max_window_bits other than 15), gets plain frames. It panics at New for a negative
// MinSize and when given twice.
func WithWebSocketCompression(c WebSocketCompression) Option {
	where := callerWhere()
	return func(a *App) {
		fail := func(format string, args ...any) {
			panic(&RegistrationError{msg: "WithWebSocketCompression: " + fmt.Sprintf(format, args...) + "\n\tregistered at " + where})
		}
		switch {
		case a.wsDeflate.On:
			fail("it is given twice: give one WebSocketCompression")
		case c.MinSize < 0:
			fail("MinSize is %d, which is not a size: give a number of bytes, or zero for the default of %d", c.MinSize, DefaultWebSocketCompressionMinSize)
		}
		if c.MinSize == 0 {
			c.MinSize = DefaultWebSocketCompressionMinSize
		}
		a.wsDeflate = engine.WSDeflate{On: true, MinSize: c.MinSize}
		a.custom["wsCompression"] = true
	}
}
