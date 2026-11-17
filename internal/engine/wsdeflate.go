package engine

import (
	"bytes"
	"compress/flate"
	"errors"
	"io"
	"sync"

	"github.com/nevindra/bites/internal/h1"
	"github.com/nevindra/bites/internal/ws"
)

// permessage-deflate (RFC 7692), in the one mode that keeps no state for a socket: every message
// is compressed and inflated on its own, with server_no_context_takeover and
// client_no_context_takeover agreed in the handshake, so a socket holds a bool and a message holds
// a pooled flate state only while it is being compressed or inflated (ADR 0040).

// WSDeflate configures the app's permessage-deflate. The zero value is off.
type WSDeflate struct {
	// On offers the extension to every client that asks for it.
	On bool
	// MinSize is the smallest message payload, in bytes, that is compressed: a smaller one is sent
	// as it is, which RFC 7692 allows on a compressed connection. At least 1 is the framework's.
	MinSize int
}

const wsDeflateResponse = "permessage-deflate; server_no_context_takeover; client_no_context_takeover"

// deflateTail is what a sync-flushed deflate stream ends with, which RFC 7692 section 7.2.1 has the
// sender remove and section 7.2.2 has the receiver append.
var deflateTail = []byte{0x00, 0x00, 0xff, 0xff}

// negotiateDeflate reports whether the request's Sec-WebSocket-Extensions holds an offer of
// permessage-deflate this server can accept. It reads every offer in order and takes the first
// whose parameters it knows: client_no_context_takeover, server_no_context_takeover and
// client_max_window_bits (a value or none), which it need not answer, and server_max_window_bits
// only as 15, the window flate has. An offer with another parameter, one given twice or with a
// value it cannot meet is declined (RFC 7692 section 5.1), and the next is tried.
func negotiateDeflate(head *h1.Request) bool {
	for i := range head.Headers {
		h := &head.Headers[i]
		if !equalFoldASCII(h.Name, "sec-websocket-extensions") {
			continue
		}
		for _, offer := range bytes.Split(h.Value, []byte(",")) {
			if acceptableOffer(offer) {
				return true
			}
		}
	}
	return false
}

func acceptableOffer(offer []byte) bool {
	parts := bytes.Split(offer, []byte(";"))
	if !equalFoldASCII(bytes.TrimSpace(parts[0]), "permessage-deflate") {
		return false
	}
	seen := map[string]bool{}
	for _, p := range parts[1:] {
		name, val, hasVal := bytes.Cut(bytes.TrimSpace(p), []byte("="))
		n := string(bytes.ToLower(bytes.TrimSpace(name)))
		if seen[n] {
			return false
		}
		seen[n] = true
		val = bytes.Trim(bytes.TrimSpace(val), `"`)
		switch n {
		case "client_no_context_takeover", "server_no_context_takeover":
			if hasVal {
				return false
			}
		case "client_max_window_bits":
		case "server_max_window_bits":
			if !hasVal || string(val) != "15" {
				return false
			}
		default:
			return false
		}
	}
	return true
}

func equalFoldASCII(b []byte, s string) bool {
	if len(b) != len(s) {
		return false
	}
	for i := range b {
		c := b[i]
		if 'A' <= c && c <= 'Z' {
			c += 'a' - 'A'
		}
		if c != s[i] {
			return false
		}
	}
	return true
}

var flateWriters sync.Pool // *flateWriter

type flateWriter struct {
	w   *flate.Writer
	out bytes.Buffer
}

// deflateFrame returns the frame as a compressed message frame, and whether it did: frame is one
// complete unfragmented text or binary frame as Socket.Send builds them, whose payload is at least
// min bytes and which compressing makes smaller. The frame it returns is the caller's and
// separate from frame, which is unchanged. A frame it cannot read, or that does not shrink, is
// not compressed, and is sent as it is.
func deflateFrame(frame []byte, min int) ([]byte, bool) {
	if len(frame) < 2 || frame[0]&0xf0 != 0x80 {
		return nil, false
	}
	op := frame[0] & 0x0f
	if op != byte(ws.OpText) && op != byte(ws.OpBinary) {
		return nil, false
	}
	hl := 2 // a server frame is never masked: the length is in the second byte, or after it
	switch frame[1] & 0x7f {
	case 126:
		hl = 4
	case 127:
		hl = 10
	}
	if len(frame) < hl {
		return nil, false
	}
	payload := frame[hl:]
	if len(payload) == 0 || len(payload) < min {
		return nil, false
	}
	fw, _ := flateWriters.Get().(*flateWriter)
	if fw == nil {
		w, _ := flate.NewWriter(io.Discard, flate.BestSpeed) // the level is a constant it accepts
		fw = &flateWriter{w: w}
	}
	defer flateWriters.Put(fw)
	fw.out.Reset()
	fw.w.Reset(&fw.out)
	fw.w.Write(payload)
	if fw.w.Flush() != nil {
		return nil, false
	}
	c := fw.out.Bytes()
	if !bytes.HasSuffix(c, deflateTail) {
		return nil, false
	}
	c = c[:len(c)-len(deflateTail)]
	if len(c) >= len(payload) {
		return nil, false
	}
	out := make([]byte, 0, ws.HeaderLen(len(c))+len(c))
	out = ws.AppendHeader(out, true, ws.Opcode(op), len(c))
	out[0] |= 0x40 // RSV1: the message is compressed
	return append(out, c...), true
}

var flateReaders sync.Pool // io.ReadCloser that is a flate.Resetter

var errInflateTooBig = errors.New("ws: the inflated message is over the limit")

// inflate returns the message that the deflate data of a compressed message holds, at most max
// bytes of it. errInflateTooBig says it would be more, and any other error that data is not
// valid deflate.
func inflate(data []byte, max int) ([]byte, error) {
	src := io.MultiReader(bytes.NewReader(data), bytes.NewReader(deflateTail))
	var fr io.ReadCloser
	if v := flateReaders.Get(); v != nil {
		fr = v.(io.ReadCloser)
		if err := fr.(flate.Resetter).Reset(src, nil); err != nil {
			return nil, err
		}
	} else {
		fr = flate.NewReader(src)
	}
	defer flateReaders.Put(fr)
	// One byte more than max tells a message of exactly max from one over it.
	var buf bytes.Buffer
	buf.Grow(min(max, len(data)*4+64))
	if _, err := buf.ReadFrom(io.LimitReader(fr, int64(max)+1)); err != nil && !errors.Is(err, io.ErrUnexpectedEOF) {
		return nil, err
	}
	if buf.Len() > max {
		return nil, errInflateTooBig
	}
	return buf.Bytes(), nil
}
