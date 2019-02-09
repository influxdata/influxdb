package tsm1

import (
	"fmt"
	"runtime"
	"sync"
	"testing"
)

func TestRing_newRing(t *testing.T) {
	r := newring()

	if got, exp := len(r.partitions), partitions; got != exp {
		t.Fatalf("got %v, expected %v", got, exp)
	}

	// Check partitions distributed correctly
	ps := make([]*partition, 0)
	for i, partition := range r.partitions {
		if i == 0 || partition != ps[len(ps)-1] {
			ps = append(ps, partition)
		}
	}

	if got, exp := len(ps), partitions; got != exp {
		t.Fatalf("got %v, expected %v", got, exp)
	}
}

var strSliceRes [][]byte

func benchmarkRingkeys(b *testing.B, r *ring, keys int) {
	// Add some keys
	for i := 0; i < keys; i++ {
		r.add([]byte(fmt.Sprintf("cpu,host=server-%d value=1", i)), new(entry))
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		strSliceRes = r.keys(false)
	}
}

func BenchmarkRing_keys_100(b *testing.B)    { benchmarkRingkeys(b, newring(), 100) }
func BenchmarkRing_keys_1000(b *testing.B)   { benchmarkRingkeys(b, newring(), 1000) }
func BenchmarkRing_keys_10000(b *testing.B)  { benchmarkRingkeys(b, newring(), 10000) }
func BenchmarkRing_keys_100000(b *testing.B) { benchmarkRingkeys(b, newring(), 100000) }

func benchmarkRingGetPartition(b *testing.B, r *ring, keys int) {
	vals := make([][]byte, keys)

	// Add some keys
	for i := 0; i < keys; i++ {
		vals[i] = []byte(fmt.Sprintf("cpu,host=server-%d field1=value1,field2=value2,field4=value4,field5=value5,field6=value6,field7=value7,field8=value1,field9=value2,field10=value4,field11=value5,field12=value6,field13=value7", i))
		r.add(vals[i], new(entry))
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r.getPartition(vals[i%keys])
	}
}

func BenchmarkRing_getPartition_100(b *testing.B)  { benchmarkRingGetPartition(b, newring(), 100) }
func BenchmarkRing_getPartition_1000(b *testing.B) { benchmarkRingGetPartition(b, newring(), 1000) }

func benchmarkRingWrite(b *testing.B, r *ring, n int) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var wg sync.WaitGroup
		for i := 0; i < runtime.GOMAXPROCS(0); i++ {
			errC := make(chan error)
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < n; j++ {
					if _, err := r.write([]byte(fmt.Sprintf("cpu,host=server-%d value=1", j)), Values{}); err != nil {
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

func BenchmarkRing_write_100(b *testing.B)    { benchmarkRingWrite(b, newring(), 100) }
func BenchmarkRing_write_1000(b *testing.B)   { benchmarkRingWrite(b, newring(), 1000) }
func BenchmarkRing_write_10000(b *testing.B)  { benchmarkRingWrite(b, newring(), 10000) }
func BenchmarkRing_write_100000(b *testing.B) { benchmarkRingWrite(b, newring(), 100000) }
