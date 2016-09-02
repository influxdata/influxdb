package murmur3_test

import (
	"fmt"
	"testing"

	"github.com/influxdata/influxdb/pkg/murmur3"
)

var data = []struct {
	h32 uint32
	s   string
}{
	{0x00000000, ""},
	{0x248bfa47, "hello"},
	{0x149bbb7f, "hello, world"},
	{0xe31e8a70, "19 Jan 2038 at 3:14:07 AM"},
	{0xd5c48bfc, "The quick brown fox jumps over the lazy dog."},
}

func TestRef(t *testing.T) {
	for _, elem := range data {
		var h32 murmur3.Hash32
		h32.Write([]byte(elem.s))
		if v := h32.Sum32(); v != elem.h32 {
			t.Errorf("'%s': 0x%x (want 0x%x)", elem.s, v, elem.h32)
		}

		var h32_byte murmur3.Hash32
		h32_byte.Write([]byte(elem.s))
		target := fmt.Sprintf("%08x", elem.h32)
		if p := fmt.Sprintf("%x", h32_byte.Sum(nil)); p != target {
			t.Errorf("'%s': %s (want %s)", elem.s, p, target)
		}

		if v := murmur3.Sum32([]byte(elem.s)); v != elem.h32 {
			t.Errorf("'%s': 0x%x (want 0x%x)", elem.s, v, elem.h32)
		}
	}
}

func TestIncremental(t *testing.T) {
	for _, elem := range data {
		var h32 murmur3.Hash32
		for i, j, k := 0, 0, len(elem.s); i < k; i = j {
			j = 2*i + 3
			if j > k {
				j = k
			}
			s := elem.s[i:j]
			print(s + "|")
			h32.Write([]byte(s))
		}
		println()
		if v := h32.Sum32(); v != elem.h32 {
			t.Errorf("'%s': 0x%x (want 0x%x)", elem.s, v, elem.h32)
		}
	}
}

//---

func bench32(b *testing.B, length int) {
	buf := make([]byte, length)
	b.SetBytes(int64(length))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		murmur3.Sum32(buf)
	}
}

func Benchmark32_1(b *testing.B)    { bench32(b, 1) }
func Benchmark32_2(b *testing.B)    { bench32(b, 2) }
func Benchmark32_4(b *testing.B)    { bench32(b, 4) }
func Benchmark32_8(b *testing.B)    { bench32(b, 8) }
func Benchmark32_16(b *testing.B)   { bench32(b, 16) }
func Benchmark32_32(b *testing.B)   { bench32(b, 32) }
func Benchmark32_64(b *testing.B)   { bench32(b, 64) }
func Benchmark32_128(b *testing.B)  { bench32(b, 128) }
func Benchmark32_256(b *testing.B)  { bench32(b, 256) }
func Benchmark32_512(b *testing.B)  { bench32(b, 512) }
func Benchmark32_1024(b *testing.B) { bench32(b, 1024) }
func Benchmark32_2048(b *testing.B) { bench32(b, 2048) }
func Benchmark32_4096(b *testing.B) { bench32(b, 4096) }
func Benchmark32_8192(b *testing.B) { bench32(b, 8192) }

func benchPartial32(b *testing.B, length int) {
	buf := make([]byte, length)
	b.SetBytes(int64(length))

	start := (32 / 8) / 2
	chunks := 7
	k := length / chunks
	tail := (length - start) % k

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var h murmur3.Hash32
		h.Write(buf[0:start])

		for j := start; j+k <= length; j += k {
			h.Write(buf[j : j+k])
		}

		h.Write(buf[length-tail:])
		h.Sum32()
	}
}

func BenchmarkPartial32_8(b *testing.B)   { benchPartial32(b, 8) }
func BenchmarkPartial32_16(b *testing.B)  { benchPartial32(b, 16) }
func BenchmarkPartial32_32(b *testing.B)  { benchPartial32(b, 32) }
func BenchmarkPartial32_64(b *testing.B)  { benchPartial32(b, 64) }
func BenchmarkPartial32_128(b *testing.B) { benchPartial32(b, 128) }
