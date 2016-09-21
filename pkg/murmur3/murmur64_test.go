package murmur3_test

import (
	"crypto/rand"

	"github.com/cespare/xxhash"

	// "github.com/OneOfOne/xxhash"
	// "github.com/spaolacci/murmur3"

	"testing"
)

func benchmark(b *testing.B, hashFn func([]byte) uint64, n int) {
	// generate random data.
	var buf = make([]byte, n)
	if m, err := rand.Read(buf); err != nil {
		b.Fatal(err)
	} else if m != n {
		b.Fatalf("only wrote %d bytes to buffer of size %d", m, n)
	}

	b.ReportAllocs()
	b.ResetTimer()
	var bv uint64
	for i := 0; i < b.N; i++ {
		bv += hashFn(buf)
	}
}

var (
	// hashFn = xxhash.Checksum64
	hashFn = xxhash.Sum64
	// hashFn = murmur3.Sum64
)

func BenchmarkHash_5(b *testing.B)   { benchmark(b, hashFn, 5) }
func BenchmarkHash_10(b *testing.B)  { benchmark(b, hashFn, 10) }
func BenchmarkHash_15(b *testing.B)  { benchmark(b, hashFn, 15) }
func BenchmarkHash_20(b *testing.B)  { benchmark(b, hashFn, 20) }
func BenchmarkHash_25(b *testing.B)  { benchmark(b, hashFn, 25) }
func BenchmarkHash_30(b *testing.B)  { benchmark(b, hashFn, 30) }
func BenchmarkHash_35(b *testing.B)  { benchmark(b, hashFn, 35) }
func BenchmarkHash_40(b *testing.B)  { benchmark(b, hashFn, 40) }
func BenchmarkHash_45(b *testing.B)  { benchmark(b, hashFn, 45) }
func BenchmarkHash_50(b *testing.B)  { benchmark(b, hashFn, 50) }
func BenchmarkHash_100(b *testing.B) { benchmark(b, hashFn, 100) }
func BenchmarkHash_250(b *testing.B) { benchmark(b, hashFn, 250) }
func BenchmarkHash_500(b *testing.B) { benchmark(b, hashFn, 500) }
