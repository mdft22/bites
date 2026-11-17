package etaghash

import (
	"crypto/sha256"
	"hash/crc32"
	"hash/crc64"
	"hash/fnv"
	"testing"
)

var sizes = []int{256, 4096, 65536, 1 << 20}
var sink uint64
var s32 [32]byte

func bufs(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i * 7)
	}
	return b
}
func BenchmarkSHA256(b *testing.B) {
	for _, n := range sizes {
		d := bufs(n)
		b.Run(itoa(n), func(b *testing.B) {
			b.SetBytes(int64(n))
			for i := 0; i < b.N; i++ {
				s32 = sha256.Sum256(d)
			}
		})
	}
}
func BenchmarkFNV64a(b *testing.B) {
	for _, n := range sizes {
		d := bufs(n)
		b.Run(itoa(n), func(b *testing.B) {
			b.SetBytes(int64(n))
			for i := 0; i < b.N; i++ {
				h := fnv.New64a()
				h.Write(d)
				sink = h.Sum64()
			}
		})
	}
}

var ct = crc32.MakeTable(crc32.Castagnoli)

func BenchmarkCRC32C(b *testing.B) {
	for _, n := range sizes {
		d := bufs(n)
		b.Run(itoa(n), func(b *testing.B) {
			b.SetBytes(int64(n))
			for i := 0; i < b.N; i++ {
				sink = uint64(crc32.Checksum(d, ct))
			}
		})
	}
}

var c64 = crc64.MakeTable(crc64.ECMA)

func BenchmarkCRC64(b *testing.B) {
	for _, n := range sizes {
		d := bufs(n)
		b.Run(itoa(n), func(b *testing.B) {
			b.SetBytes(int64(n))
			for i := 0; i < b.N; i++ {
				sink = crc64.Checksum(d, c64)
			}
		})
	}
}
func itoa(n int) string {
	s := ""
	if n == 0 {
		return "0"
	}
	for n > 0 {
		s = string(rune('0'+n%10)) + s
		n /= 10
	}
	return s
}
