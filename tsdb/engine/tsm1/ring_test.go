package tsm1

import (
	"fmt"
	"runtime"
	"sync"
	"testing"
)

func TestRing_newRing(t *testing.T) {
	examples := []struct {
		n         int
		returnErr bool
	}{
		{n: 1}, {n: 2}, {n: 4}, {n: 8}, {n: 16}, {n: 32}, {n: 64}, {n: 128}, {n: 256},
		{n: 0, returnErr: true}, {n: 3, returnErr: true}, {n: 512, returnErr: true},
	}

	for i, example := range examples {
		r, err := newring(example.n)
		if err != nil {
			if example.returnErr {
				continue // expecting an error.
			}
			t.Fatal(err)
		}

		if got, exp := len(r.partitions), example.n; got != exp {
			t.Fatalf("[Example %d] got %v, expected %v", i, got, exp)
		}

		// Check partitions distributed correctly
		partitions := make([]*partition, 0)
		for i, partition := range r.continuum {
			if i == 0 || partition != partitions[len(partitions)-1] {
				partitions = append(partitions, partition)
			}
		}

		if got, exp := len(partitions), example.n; got != exp {
			t.Fatalf("[Example %d] got %v, expected %v", i, got, exp)
		}
	}
}

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

func benchmarkRingWrite(b *testing.B, r *ring, n int) {
	for i := 0; i < b.N; i++ {
		var wg sync.WaitGroup
		for i := 0; i < runtime.GOMAXPROCS(0); i++ {
			errC := make(chan error)
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < n; j++ {
					if err := r.write(fmt.Sprintf("cpu,host=server-%d value=1", j), Values{}); err != nil {
						errC <- err
					}
				}
			}()

			go func() {
				wg.Wait()
				close(errC)
			}()

			for err := range errC {
				if err != nil {
					b.Error(err)
				}
			}
		}
	}
}

func BenchmarkRing_write_1_100(b *testing.B)      { benchmarkRingWrite(b, MustNewRing(1), 100) }
func BenchmarkRing_write_1_1000(b *testing.B)     { benchmarkRingWrite(b, MustNewRing(1), 1000) }
func BenchmarkRing_write_1_10000(b *testing.B)    { benchmarkRingWrite(b, MustNewRing(1), 10000) }
func BenchmarkRing_write_1_100000(b *testing.B)   { benchmarkRingWrite(b, MustNewRing(1), 100000) }
func BenchmarkRing_write_4_100(b *testing.B)      { benchmarkRingWrite(b, MustNewRing(4), 100) }
func BenchmarkRing_write_4_1000(b *testing.B)     { benchmarkRingWrite(b, MustNewRing(4), 1000) }
func BenchmarkRing_write_4_10000(b *testing.B)    { benchmarkRingWrite(b, MustNewRing(4), 10000) }
func BenchmarkRing_write_4_100000(b *testing.B)   { benchmarkRingWrite(b, MustNewRing(4), 100000) }
func BenchmarkRing_write_32_100(b *testing.B)     { benchmarkRingWrite(b, MustNewRing(32), 100) }
func BenchmarkRing_write_32_1000(b *testing.B)    { benchmarkRingWrite(b, MustNewRing(32), 1000) }
func BenchmarkRing_write_32_10000(b *testing.B)   { benchmarkRingWrite(b, MustNewRing(32), 10000) }
func BenchmarkRing_write_32_100000(b *testing.B)  { benchmarkRingWrite(b, MustNewRing(32), 100000) }
func BenchmarkRing_write_128_100(b *testing.B)    { benchmarkRingWrite(b, MustNewRing(128), 100) }
func BenchmarkRing_write_128_1000(b *testing.B)   { benchmarkRingWrite(b, MustNewRing(128), 1000) }
func BenchmarkRing_write_128_10000(b *testing.B)  { benchmarkRingWrite(b, MustNewRing(128), 10000) }
func BenchmarkRing_write_128_100000(b *testing.B) { benchmarkRingWrite(b, MustNewRing(256), 100000) }
func BenchmarkRing_write_256_100(b *testing.B)    { benchmarkRingWrite(b, MustNewRing(256), 100) }
func BenchmarkRing_write_256_1000(b *testing.B)   { benchmarkRingWrite(b, MustNewRing(256), 1000) }
func BenchmarkRing_write_256_10000(b *testing.B)  { benchmarkRingWrite(b, MustNewRing(256), 10000) }
func BenchmarkRing_write_256_100000(b *testing.B) { benchmarkRingWrite(b, MustNewRing(256), 100000) }

func MustNewRing(n int) *ring {
	r, err := newring(n)
	if err != nil {
		panic(err)
	}
	return r
}
