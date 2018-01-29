package bloom_test

import (
	"encoding/binary"
	"fmt"
	"os"
	"testing"

	"github.com/influxdata/influxdb/pkg/bloom"
)

// Ensure filter can insert values and verify they exist.
func TestFilter_InsertContains(t *testing.T) {
	if testing.Short() || os.Getenv("GORACE") != "" || os.Getenv("APPVEYOR") != "" {
		t.Skip("Skipping test in short, race and appveyor mode.")
	}

	// Short, less comprehensive test.
	testShortFilter_InsertContains(t)

	if testing.Short() {
		return // Just run the above short test
	}

	// More comprehensive test for the xxhash based Bloom Filter.

	// These parameters will result, for 10M entries, with a bloom filter
	// with 0.001 false positive rate (1 in 1000 values will be incorrectly
	// identified as being present in the set).
	filter := bloom.NewFilter(143775876, 10)
	v := make([]byte, 4)
	for i := 0; i < 10000000; i++ {
		binary.BigEndian.PutUint32(v, uint32(i))
		filter.Insert(v)
	}

	// None of the values inserted should ever be considered "not possibly in
	// the filter".
	t.Run("100M", func(t *testing.T) {
		for i := 0; i < 10000000; i++ {
			binary.BigEndian.PutUint32(v, uint32(i))
			if !filter.Contains(v) {
				t.Fatalf("got false for value %q, expected true", v)
			}
		}

		// If we check for 100,000,000 values that we know are not present in the
		// filter then we might expect around 100,000 of them to be false positives.
		var fp int
		for i := 10000000; i < 110000000; i++ {
			binary.BigEndian.PutUint32(v, uint32(i))
			if filter.Contains(v) {
				fp++
			}
		}

		if fp > 1000000 {
			// If we're an order of magnitude off, then it's arguable that there
			// is a bug in the bloom filter.
			t.Fatalf("got %d false positives which is an error rate of %f, expected error rate <=0.001", fp, float64(fp)/100000000)
		}
		t.Logf("Bloom false positive error rate was %f", float64(fp)/100000000)
	})
}

func testShortFilter_InsertContains(t *testing.T) {
	t.Run("short", func(t *testing.T) {
		f := bloom.NewFilter(1000, 4)

		// Insert value and validate.
		f.Insert([]byte("Bess"))
		if !f.Contains([]byte("Bess")) {
			t.Fatal("expected true")
		}

		// Insert another value and test.
		f.Insert([]byte("Emma"))
		if !f.Contains([]byte("Emma")) {
			t.Fatal("expected true")
		}

		// Validate that a non-existent value doesn't exist.
		if f.Contains([]byte("Jane")) {
			t.Fatal("expected false")
		}
	})
}

var benchCases = []struct {
	m, k uint64
	n    int
}{
	{m: 100, k: 4, n: 1000},
	{m: 1000, k: 4, n: 1000},
	{m: 10000, k: 4, n: 1000},
	{m: 100000, k: 4, n: 1000},
	{m: 100, k: 8, n: 1000},
	{m: 1000, k: 8, n: 1000},
	{m: 10000, k: 8, n: 1000},
	{m: 100000, k: 8, n: 1000},
	{m: 100, k: 20, n: 1000},
	{m: 1000, k: 20, n: 1000},
	{m: 10000, k: 20, n: 1000},
	{m: 100000, k: 20, n: 1000},
}

func BenchmarkFilter_Insert(b *testing.B) {
	for _, c := range benchCases {
		data := make([][]byte, 0, c.n)
		for i := 0; i < c.n; i++ {
			data = append(data, []byte(fmt.Sprintf("%d", i)))
		}

		filter := bloom.NewFilter(c.m, c.k)
		b.Run(fmt.Sprintf("m=%d_k=%d_n=%d", c.m, c.k, c.n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				for _, v := range data {
					filter.Insert(v)
				}
			}
		})

	}
}

var okResult bool

func BenchmarkFilter_Contains(b *testing.B) {
	for _, c := range benchCases {
		data := make([][]byte, 0, c.n)
		notData := make([][]byte, 0, c.n)
		for i := 0; i < c.n; i++ {
			data = append(data, []byte(fmt.Sprintf("%d", i)))
			notData = append(notData, []byte(fmt.Sprintf("%d", c.n+i)))
		}

		filter := bloom.NewFilter(c.m, c.k)
		for _, v := range data {
			filter.Insert(v)
		}

		b.Run(fmt.Sprintf("m=%d_k=%d_n=%d", c.m, c.k, c.n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				for _, v := range data {
					okResult = filter.Contains(v)
					if !okResult {
						b.Fatalf("Filter returned negative for value %q in set", v)
					}
				}

				// And now a bunch of values that don't exist.
				for _, v := range notData {
					okResult = filter.Contains(v)
				}
			}
		})
	}
}

func BenchmarkFilter_Merge(b *testing.B) {
	for _, c := range benchCases {
		data1 := make([][]byte, 0, c.n)
		data2 := make([][]byte, 0, c.n)
		for i := 0; i < c.n; i++ {
			data1 = append(data1, []byte(fmt.Sprintf("%d", i)))
			data2 = append(data2, []byte(fmt.Sprintf("%d", c.n+i)))
		}

		filter1 := bloom.NewFilter(c.m, c.k)
		filter2 := bloom.NewFilter(c.m, c.k)
		for i := 0; i < c.n; i++ {
			filter1.Insert(data1[i])
			filter2.Insert(data2[i])
		}

		b.Run(fmt.Sprintf("m=%d_k=%d_n=%d", c.m, c.k, c.n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				other, err := bloom.NewFilterBuffer(filter1.Bytes(), filter1.K())
				if err != nil {
					b.Fatal(err)
				}
				other.Merge(filter2)
			}
		})
	}
}
