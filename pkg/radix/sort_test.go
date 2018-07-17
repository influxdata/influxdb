package radix

import (
	"math/rand"
	"testing"
)

func benchmarkSort(b *testing.B, size int) {
	orig := make([]uint64, size)
	for i := range orig {
		orig[i] = uint64(rand.Int63())
	}
	data := make([]uint64, size)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		copy(data, orig)
		SortUint64s(data)
	}
}

func BenchmarkSort_64(b *testing.B)  { benchmarkSort(b, 64) }
func BenchmarkSort_128(b *testing.B) { benchmarkSort(b, 128) }
func BenchmarkSort_256(b *testing.B) { benchmarkSort(b, 256) }
func BenchmarkSort_12K(b *testing.B) { benchmarkSort(b, 12*1024) }
