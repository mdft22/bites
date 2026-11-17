package engine

import (
	"bytes"
	"strings"
	"testing"

	"github.com/nevindra/bites/internal/ws"
)

// payloadOf is the payload of a server frame: after the header its length code needs.
func payloadOf(f []byte) []byte {
	switch f[1] & 0x7f {
	case 126:
		return f[4:]
	case 127:
		return f[10:]
	}
	return f[2:]
}

func jsonFrame(n int) []byte {
	p := []byte(`{"type":"presence","users":[` + strings.Repeat(`{"id":"u-1234","name":"Ada Lovelace","status":"online"},`, n) + `{}]}`)
	return ws.AppendFrame(nil, ws.OpText, p)
}

func TestADeflatedFrameInflatesToItsPayloadAndALongOneUsesAnExtendedLength(t *testing.T) {
	for _, n := range []int{5, 40, 2000} { // 7-bit, 16-bit and 64-bit length headers
		f := jsonFrame(n)
		cf, ok := deflateFrame(f, 64)
		if !ok {
			t.Fatalf("%d bytes were not compressed", len(f))
		}
		if cf[0] != 0xC1 || len(cf) >= len(f) {
			t.Fatalf("first byte %#x, %d bytes from %d", cf[0], len(cf), len(f))
		}
		payload := payloadOf(cf)
		got, err := inflate(payload, 1<<24)
		if err != nil || !bytes.Equal(got, f[len(f)-len(got):]) {
			t.Fatalf("inflate: %v, %d bytes want the payload of %d", err, len(got), len(f))
		}
	}
}

func TestAFrameUnderTheFloorOrThatDoesNotShrinkOrIsNotADataFrameIsNotCompressed(t *testing.T) {
	if _, ok := deflateFrame(ws.AppendFrame(nil, ws.OpText, []byte(`{"a":1}`)), 64); ok {
		t.Fatal("a payload under the floor was compressed")
	}
	random := make([]byte, 200)
	for i := range random {
		random[i] = byte(i*131 + i*i*17)
	}
	if _, ok := deflateFrame(ws.AppendFrame(nil, ws.OpBinary, random), 64); ok {
		t.Fatal("a frame that does not shrink was compressed")
	}
	ping, _ := ws.AppendPing(nil, bytes.Repeat([]byte("x"), 100))
	if _, ok := deflateFrame(ping, 1); ok {
		t.Fatal("a control frame was compressed")
	}
}

func TestInflateStopsAtTheLimitAndRefusesWhatIsNotDeflate(t *testing.T) {
	f := jsonFrame(2000)
	cf, _ := deflateFrame(f, 64)
	if _, err := inflate(payloadOf(cf), 1000); err != errInflateTooBig {
		t.Fatalf("over the limit: %v", err)
	}
	if _, err := inflate([]byte{0xff, 0xff, 0xff}, 1000); err == nil {
		t.Fatal("garbage inflated")
	}
}

func BenchmarkDeflateAFrameOf4KiB(b *testing.B) {
	f := jsonFrame(80)
	b.SetBytes(int64(len(f)))
	b.ReportAllocs()
	for b.Loop() {
		deflateFrame(f, 256)
	}
}

func BenchmarkInflateAMessageOf4KiB(b *testing.B) {
	f := jsonFrame(80)
	cf, _ := deflateFrame(f, 256)
	payload := payloadOf(cf)
	b.SetBytes(int64(len(f)))
	b.ReportAllocs()
	for b.Loop() {
		inflate(payload, 64<<10)
	}
}
