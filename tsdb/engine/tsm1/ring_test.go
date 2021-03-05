package tsm1

import (
	"fmt"
	"runtime"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRing_newRing(t *testing.T) {
	examples := []struct {
		n         int
		returnErr bool
	}{
		{n: 1},
		{n: 2},
		{n: 4},
		{n: 8},
		{n: 16},
		{n: 32, returnErr: true},
		{n: 0, returnErr: true},
		{n: 3, returnErr: true},
	}

	for _, example := range examples {
		t.Run(fmt.Sprintf("ring n %d", example.n), func(t *testing.T) {
			r, err := newring(example.n)
			if example.returnErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, example.n, len(r.partitions))

			// Check partitions distributed correctly
			partitions := make([]*partition, 0)
			for i, partition := range r.partitions {
				if i == 0 || partition != partitions[len(partitions)-1] {
					partitions = append(partitions, partition)
				}
			}
			require.Equal(t, example.n, len(partitions))
		})
	}
}

var strSliceRes [][]byte

func benchmarkRingkeys(b *testing.B, r *ring, keys int) {
	// Add some keys
	for i := 0; i < keys; i++ {
		r.write([]byte(fmt.Sprintf("cpu,host=server-%d value=1", i)), Values([]Value{
			IntegerValue{
				unixnano: 1,
				value:    int64(i),
			},
		}))
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		strSliceRes = r.keys(false)
	}
}

func BenchmarkRing_keys_100(b *testing.B)    { benchmarkRingkeys(b, MustNewRing(16), 100) }
func BenchmarkRing_keys_1000(b *testing.B)   { benchmarkRingkeys(b, MustNewRing(16), 1000) }
func BenchmarkRing_keys_10000(b *testing.B)  { benchmarkRingkeys(b, MustNewRing(16), 10000) }
func BenchmarkRing_keys_100000(b *testing.B) { benchmarkRingkeys(b, MustNewRing(16), 100000) }

func benchmarkRingGetPartition(b *testing.B, r *ring, keys int) {
	vals := make([][]byte, keys)

	// Add some keys
	for i := 0; i < keys; i++ {
		vals[i] = []byte(fmt.Sprintf("cpu,host=server-%d field1=value1,field2=value2,field4=value4,field5=value5,field6=value6,field7=value7,field8=value1,field9=value2,field10=value4,field11=value5,field12=value6,field13=value7", i))
		r.write([]byte(fmt.Sprintf("cpu,host=server-%d value=1", i)), Values([]Value{
			IntegerValue{
				unixnano: 1,
				value:    int64(i),
			},
		}))
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r.getPartition(vals[i%keys])
	}
}

func BenchmarkRing_getPartition_100(b *testing.B) {
	benchmarkRingGetPartition(b, MustNewRing(16), 100)
}
func BenchmarkRing_getPartition_1000(b *testing.B) {
	benchmarkRingGetPartition(b, MustNewRing(16), 1000)
}

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

func BenchmarkRing_write_1_100(b *testing.B)     { benchmarkRingWrite(b, MustNewRing(1), 100) }
func BenchmarkRing_write_1_1000(b *testing.B)    { benchmarkRingWrite(b, MustNewRing(1), 1000) }
func BenchmarkRing_write_1_10000(b *testing.B)   { benchmarkRingWrite(b, MustNewRing(1), 10000) }
func BenchmarkRing_write_1_100000(b *testing.B)  { benchmarkRingWrite(b, MustNewRing(1), 100000) }
func BenchmarkRing_write_4_100(b *testing.B)     { benchmarkRingWrite(b, MustNewRing(4), 100) }
func BenchmarkRing_write_4_1000(b *testing.B)    { benchmarkRingWrite(b, MustNewRing(4), 1000) }
func BenchmarkRing_write_4_10000(b *testing.B)   { benchmarkRingWrite(b, MustNewRing(4), 10000) }
func BenchmarkRing_write_4_100000(b *testing.B)  { benchmarkRingWrite(b, MustNewRing(4), 100000) }
func BenchmarkRing_write_16_100(b *testing.B)    { benchmarkRingWrite(b, MustNewRing(16), 100) }
func BenchmarkRing_write_16_1000(b *testing.B)   { benchmarkRingWrite(b, MustNewRing(16), 1000) }
func BenchmarkRing_write_16_10000(b *testing.B)  { benchmarkRingWrite(b, MustNewRing(16), 10000) }
func BenchmarkRing_write_16_100000(b *testing.B) { benchmarkRingWrite(b, MustNewRing(16), 100000) }

func MustNewRing(n int) *ring {
	r, err := newring(n)
	if err != nil {
		panic(err)
	}
	return r
}
