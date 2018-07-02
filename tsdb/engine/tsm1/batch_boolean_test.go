package tsm1_test

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb/tsdb/engine/tsm1"
)

func Test_BooleanBatchDecodeAll_Single(t *testing.T) {
	enc := tsm1.NewBooleanEncoder(1)
	exp := true
	enc.Write(exp)
	b, err := enc.Bytes()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	got, _ := tsm1.BooleanBatchDecodeAll(b, nil)
	if len(got) != 1 {
		t.Fatalf("expected 1 value")
	}
	if got := got[0]; got != exp {
		t.Fatalf("unexpected value -got/+exp\n%s", cmp.Diff(got, exp))
	}
}

func Test_BooleanBatchDecodeAll_Multi_Compressed(t *testing.T) {
	cases := []struct {
		n int
		p float64 // probability of a true value
	}{
		{10, 0.33},
		{100, 0.55},
		{1000, 0.68},
	}

	for _, tc := range cases {
		t.Run(fmt.Sprintf("%d_%0.2f", tc.n, tc.p), func(t *testing.T) {
			rand.Seed(int64(tc.n * tc.n))

			enc := tsm1.NewBooleanEncoder(tc.n)
			values := make([]bool, tc.n)
			for i := range values {
				values[i] = rand.Float64() < tc.p
				enc.Write(values[i])
			}

			b, err := enc.Bytes()
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			got, err := tsm1.BooleanBatchDecodeAll(b, nil)
			if err != nil {
				t.Fatalf("unexpected error %q", err.Error())
			}

			if !cmp.Equal(got, values) {
				t.Fatalf("unexpected values, -got/+exp\n%s", cmp.Diff(got, values))
			}
		})
	}
}

func Test_BooleanBatchDecoder_Corrupt(t *testing.T) {
	cases := []struct {
		name string
		d    string
	}{
		{"empty", ""},
		{"invalid count", "\x10\x90"},
		{"count greater than remaining bits, multiple bytes expected", "\x10\x7f"},
		{"count greater than remaining bits, one byte expected", "\x10\x01"},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			dst, _ := tsm1.BooleanBatchDecodeAll([]byte(c.d), nil)
			if len(dst) != 0 {
				t.Fatalf("unexpected result -got/+want\n%s", cmp.Diff(dst, nil))
			}
		})
	}
}

func BenchmarkBooleanBatchDecodeAll(b *testing.B) {
	benchmarks := []struct {
		n int
	}{
		{1},
		{55},
		{555},
		{1000},
	}
	for _, bm := range benchmarks {
		b.Run(fmt.Sprintf("%d", bm.n), func(b *testing.B) {
			size := bm.n
			e := tsm1.NewBooleanEncoder(size)
			for i := 0; i < size; i++ {
				e.Write(i&1 == 1)
			}
			bytes, err := e.Bytes()
			if err != nil {
				b.Fatalf("unexpected error: %v", err)
			}

			b.SetBytes(int64(len(bytes)))
			b.ResetTimer()

			dst := make([]bool, size)
			for i := 0; i < b.N; i++ {
				res, _ := tsm1.BooleanBatchDecodeAll(bytes, dst)
				if len(res) != size {
					b.Fatalf("expected to read %d booleans, but read %d", size, len(res))
				}
			}
		})
	}
}
