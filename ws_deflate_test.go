package bites_test

import (
	"bytes"
	"compress/flate"
	"context"
	"encoding/binary"
	"io"
	"strings"
	"testing"

	"github.com/nevindra/bites"
	"github.com/nevindra/bites/bitetest"
	"github.com/nevindra/bites/internal/ws"
)

// permessage-deflate (RFC 7692) in the mode with no context takeover (ADR 0040).

const offerDeflate = "Sec-WebSocket-Extensions: permessage-deflate; client_max_window_bits"

// compressMessage is data as a client compresses a message: deflate with a sync flush, the four
// bytes of the flush taken off.
func compressMessage(t *testing.T, data string) []byte {
	t.Helper()
	var b bytes.Buffer
	fw, _ := flate.NewWriter(&b, flate.BestSpeed)
	fw.Write([]byte(data))
	fw.Flush()
	return bytes.TrimSuffix(b.Bytes(), []byte{0, 0, 0xff, 0xff})
}

// inflateMessage is what a client does to a compressed message it received.
func inflateMessage(t *testing.T, p []byte) string {
	t.Helper()
	r := flate.NewReader(io.MultiReader(bytes.NewReader(p), bytes.NewReader([]byte{0, 0, 0xff, 0xff})))
	out, err := io.ReadAll(r)
	if err != nil && err.Error() != "unexpected EOF" {
		t.Fatalf("the server's compressed message does not inflate: %v", err)
	}
	return string(out)
}

// writeCompressed sends payload as the first frame of a compressed message: RSV1 set.
func (w *wsc) writeCompressed(op ws.Opcode, payload []byte) {
	w.t.Helper()
	f := ws.AppendClientFrame(nil, true, op, payload, [4]byte{9, 8, 7, 6})
	f[0] |= 0x40
	if _, err := w.c.Write(f); err != nil {
		w.t.Fatal(err)
	}
}

// nextRaw reads one frame with its first byte, which has RSV1.
func (w *wsc) nextRaw() (b0 byte, op ws.Opcode, p []byte) {
	w.t.Helper()
	var h [2]byte
	if _, err := io.ReadFull(w.br, h[:]); err != nil {
		w.t.Fatalf("the connection ended: %v", err)
	}
	n := int(h[1] & 0x7f)
	switch n {
	case 126:
		var b [2]byte
		io.ReadFull(w.br, b[:])
		n = int(binary.BigEndian.Uint16(b[:]))
	case 127:
		var b [8]byte
		io.ReadFull(w.br, b[:])
		n = int(binary.BigEndian.Uint64(b[:]))
	}
	p = make([]byte, n)
	if _, err := io.ReadFull(w.br, p); err != nil {
		w.t.Fatal(err)
	}
	return h[0], ws.Opcode(h[0] & 0x0f), p
}

// echoRoute echoes each message and keeps no channel a test must drain, so a test may open many.
func echoRoute(_ context.Context, _ wsIn, s *bites.Socket[wsOut]) (bites.Inbox[wsMsg], error) {
	return bites.Inbox[wsMsg]{OnMessage: func(_ context.Context, m wsMsg) error { return s.Send(wsOut{Text: m.Text}) }}, nil
}

func deflateApp(t *testing.T, opts ...bites.Option) *bitetest.Server {
	srv, _, _ := startWS(t, func(app *bites.App) { app.WebSocket("/rooms/{id}", echoRoute) },
		append([]bites.Option{bites.WithWebSocketCompression(bites.WebSocketCompression{MinSize: 64})}, opts...)...)
	return srv
}

func TestTheHandshakeAcceptsAnOfferOfPermessageDeflateWithNoContextTakeoverAndOnlyWhenTheAppOptedIn(t *testing.T) {
	srv := deflateApp(t)
	w := memWS(t, srv, "/rooms/1", offerDeflate)
	if got := w.hdr["sec-websocket-extensions"]; got != "permessage-deflate; server_no_context_takeover; client_no_context_takeover" {
		t.Fatalf("the answer to an offer: %q", got)
	}
	if w = memWS(t, srv, "/rooms/1"); w.hdr["sec-websocket-extensions"] != "" {
		t.Fatalf("an answer with no offer: %q", w.hdr["sec-websocket-extensions"])
	}
	for _, bad := range []string{
		"Sec-WebSocket-Extensions: permessage-deflate; server_max_window_bits=10",
		"Sec-WebSocket-Extensions: permessage-deflate; unknown_param",
		"Sec-WebSocket-Extensions: permessage-deflate; server_no_context_takeover; server_no_context_takeover",
		"Sec-WebSocket-Extensions: x-webkit-deflate-frame",
	} {
		if w = memWS(t, srv, "/rooms/1", bad); w.hdr["sec-websocket-extensions"] != "" {
			t.Errorf("%q was accepted: %q", bad, w.hdr["sec-websocket-extensions"])
		}
	}
	// A second offer the server can meet is taken when the first it cannot.
	w = memWS(t, srv, "/rooms/1", "Sec-WebSocket-Extensions: permessage-deflate; server_max_window_bits=10, permessage-deflate")
	if w.hdr["sec-websocket-extensions"] == "" {
		t.Error("the second offer was not taken")
	}
	off, _, _ := startWS(t, func(app *bites.App) { app.WebSocket("/rooms/{id}", echoRoute) })
	if w = memWS(t, off, "/rooms/1", offerDeflate); w.hdr["sec-websocket-extensions"] != "" {
		t.Fatalf("an app that did not opt in answered %q", w.hdr["sec-websocket-extensions"])
	}
}

func TestAMessageOverMinSizeIsSentCompressedWithRSV1AndASmallOneAsItIs(t *testing.T) {
	srv := deflateApp(t)
	w := memWS(t, srv, "/rooms/1", offerDeflate)
	big := strings.Repeat("hello ", 100)
	w.text(`{"text":"` + big + `"}`)
	b0, op, p := w.nextRaw()
	if op != ws.OpText || b0&0x40 == 0 || b0&0x80 == 0 {
		t.Fatalf("the first byte is %08b, opcode %d: want FIN and RSV1 on a text frame", b0, op)
	}
	if want := `{"text":"` + big + `"}`; inflateMessage(t, p) != want || len(p) >= len(want)/2 {
		t.Fatalf("the message inflated to %q from %d bytes", inflateMessage(t, p), len(p))
	}
	w.text(`{"text":"hi"}`)
	b0, _, p = w.nextRaw()
	if b0&0x40 != 0 || string(p) != `{"text":"hi"}` {
		t.Fatalf("a small message: first byte %08b, %q", b0, p)
	}
	plain := memWS(t, srv, "/rooms/1")
	plain.text(`{"text":"` + big + `"}`)
	if b0, _, p = plain.nextRaw(); b0&0x40 != 0 || len(p) < len(big) {
		t.Fatalf("a client that did not negotiate got a compressed frame: %08b", b0)
	}
}

func TestACompressedMessageFromTheClientIsInflatedBeforeTheHandlerReadsIt(t *testing.T) {
	srv := deflateApp(t)
	w := memWS(t, srv, "/rooms/1", offerDeflate)
	msg := `{"text":"` + strings.Repeat("ab", 500) + `"}`
	w.writeCompressed(ws.OpText, compressMessage(t, msg))
	_, _, p := w.nextRaw()
	if got := inflateMessage(t, p); got != msg && string(p) != msg {
		t.Fatalf("the echo of a compressed message is %q", got)
	}
	// Two messages in a row each stand alone: no context is kept between them.
	w.writeCompressed(ws.OpText, compressMessage(t, msg))
	if _, _, p = w.nextRaw(); inflateMessage(t, p) != msg && string(p) != msg {
		t.Fatal("the second compressed message was not read on its own")
	}
	// A compressed message sent in fragments is assembled and inflated whole.
	c := compressMessage(t, msg)
	half := len(c) / 2
	f := ws.AppendClientFrame(nil, false, ws.OpText, c[:half], [4]byte{1, 2, 3, 4})
	f[0] |= 0x40
	w.c.Write(f)
	w.write(true, ws.OpContinuation, c[half:])
	if _, _, p = w.nextRaw(); inflateMessage(t, p) != msg && string(p) != msg {
		t.Fatal("a fragmented compressed message was not read")
	}
}

func TestAMessageThatInflatesPastMaxMessageIsClosedWith1009AndInvalidDataWith1007(t *testing.T) {
	srv := deflateApp(t, bites.WithLimits(bites.Limits{MaxMessage: 4096}))
	w := memWS(t, srv, "/rooms/1", offerDeflate)
	bomb := compressMessage(t, `{"text":"`+strings.Repeat("a", 200000)+`"}`)
	if len(bomb) >= 4096 {
		t.Fatalf("the test message is %d bytes compressed", len(bomb))
	}
	w.writeCompressed(ws.OpText, bomb)
	w.expectClose(1009)

	w = memWS(t, srv, "/rooms/1", offerDeflate)
	w.writeCompressed(ws.OpText, []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff})
	w.expectClose(1007)

	w = memWS(t, srv, "/rooms/1", offerDeflate)
	w.writeCompressed(ws.OpText, compressMessage(t, "\xff\xfe not utf-8"))
	w.expectClose(1007)
}

func TestRSV1IsRefusedWithoutTheExtensionOnAContinuationAndOnAControlFrame(t *testing.T) {
	srv := deflateApp(t)
	w := memWS(t, srv, "/rooms/1") // no offer: RSV1 is not allowed
	w.writeCompressed(ws.OpText, []byte("x"))
	w.expectClose(1002)

	w = memWS(t, srv, "/rooms/1", offerDeflate)
	f := ws.AppendClientFrame(nil, false, ws.OpText, []byte("{"), [4]byte{1, 2, 3, 4})
	w.c.Write(f)
	f = ws.AppendClientFrame(nil, true, ws.OpContinuation, []byte("}"), [4]byte{1, 2, 3, 4})
	f[0] |= 0x40 // RSV1 on a continuation
	w.c.Write(f)
	w.expectClose(1002)

	w = memWS(t, srv, "/rooms/1", offerDeflate)
	f = ws.AppendClientFrame(nil, true, ws.OpPing, nil, [4]byte{1, 2, 3, 4})
	f[0] |= 0x40 // RSV1 on a control frame
	w.c.Write(f)
	w.expectClose(1002)

	w = memWS(t, srv, "/rooms/1", offerDeflate)
	f = ws.AppendClientFrame(nil, true, ws.OpText, []byte("x"), [4]byte{1, 2, 3, 4})
	f[0] |= 0x20 // RSV2 is never allowed
	w.c.Write(f)
	w.expectClose(1002)
}

func TestATopicMessageIsCompressedOnceForTheSocketsThatNegotiatedAndSentPlainToTheOthers(t *testing.T) {
	var topic bites.Topic[wsOut]
	srv, _, _ := startWS(t, func(app *bites.App) {
		app.WebSocket("/t", func(_ context.Context, _ struct{}, s *bites.Socket[wsOut]) (bites.Inbox[wsMsg], error) {
			topic.Subscribe(s)
			return bites.Inbox[wsMsg]{}, nil
		})
	}, bites.WithWebSocketCompression(bites.WebSocketCompression{MinSize: 64}))
	a, b := memWS(t, srv, "/t", offerDeflate), memWS(t, srv, "/t")
	waitFor(t, "both sockets to subscribe", func() bool { return topic.Len() == 2 })
	text := strings.Repeat("broadcast ", 50)
	topic.Publish(wsOut{Text: text})
	b0, _, p := a.nextRaw()
	if b0&0x40 == 0 || !strings.Contains(inflateMessage(t, p), text) {
		t.Fatalf("the socket that negotiated got %08b %q", b0, p)
	}
	if b0, _, p = b.nextRaw(); b0&0x40 != 0 || !strings.Contains(string(p), text) {
		t.Fatalf("the socket that did not got %08b", b0)
	}
}

func TestWithWebSocketCompressionRefusesANegativeMinSizeAndBeingGivenTwice(t *testing.T) {
	for name, f := range map[string]func(){
		"a negative MinSize": func() { bites.New(bites.WithWebSocketCompression(bites.WebSocketCompression{MinSize: -1})) },
		"twice": func() {
			bites.New(bites.WithWebSocketCompression(bites.WebSocketCompression{}), bites.WithWebSocketCompression(bites.WebSocketCompression{}))
		},
	} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("%s was not refused", name)
				}
			}()
			f()
		}()
	}
}
