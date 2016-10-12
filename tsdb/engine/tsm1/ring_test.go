package tsm1

import (
	"fmt"
	"testing"
)

var strSliceRes []string

func benchmarkRingkeys(b *testing.B, r *ring, keys int) {
	// Add some keys
	for i := 0; i < keys; i++ {
		r.add(fmt.Sprintf("cpu,host=server-%d value=1", i), nil)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		strSliceRes = r.keys(false)
	}
}

func BenchmarkRing_keys_100(b *testing.B)    { benchmarkRingkeys(b, MustNewRing(256), 100) }
func BenchmarkRing_keys_1000(b *testing.B)   { benchmarkRingkeys(b, MustNewRing(256), 1000) }
func BenchmarkRing_keys_10000(b *testing.B)  { benchmarkRingkeys(b, MustNewRing(256), 10000) }
func BenchmarkRing_keys_100000(b *testing.B) { benchmarkRingkeys(b, MustNewRing(256), 100000) }

func MustNewRing(n int) *ring {
	r, err := newring(n)
	if err != nil {
		panic(err)
	}
	return r
}
