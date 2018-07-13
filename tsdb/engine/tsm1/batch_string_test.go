package tsm1

import (
	"fmt"
	"math/rand"
	"testing"
	"testing/quick"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb/internal/testutil"
)

func TestStringBatchDecodeAll_NoValues(t *testing.T) {
	enc := NewStringEncoder(1024)
	b, err := enc.Bytes()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	got, err := StringBatchDecodeAll(b, nil)
	if err != nil {
		t.Fatalf("unexpected error creating string decoder: %v", err)
	}

	exp := []string{}
	if !cmp.Equal(got, exp) {
		t.Fatalf("unexpected value: -got/+exp\n%s", cmp.Diff(got, exp))
	}
}

func TestStringBatchDecodeAll_Single(t *testing.T) {
	enc := NewStringEncoder(1024)
	v1 := "v1"
	enc.Write(v1)
	b, err := enc.Bytes()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	got, err := StringBatchDecodeAll(b, nil)
	if err != nil {
		t.Fatalf("unexpected error creating string decoder: %v", err)
	}

	exp := []string{"v1"}
	if !cmp.Equal(got, exp) {
		t.Fatalf("unexpected value: -got/+exp\n%s", cmp.Diff(got, exp))
	}
}

func TestStringBatchDecodeAll_Multi_Compressed(t *testing.T) {
	enc := NewStringEncoder(1024)

	exp := make([]string, 10)
	for i := range exp {
		exp[i] = fmt.Sprintf("value %d", i)
		enc.Write(exp[i])
	}

	b, err := enc.Bytes()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if b[0]>>4 != stringCompressedSnappy {
		t.Fatalf("unexpected encoding: got %v, exp %v", b[0], stringCompressedSnappy)
	}

	if exp := 51; len(b) != exp {
		t.Fatalf("unexpected length: got %v, exp %v", len(b), exp)
	}

	got, err := StringBatchDecodeAll(b, nil)
	if err != nil {
		t.Fatalf("unexpected error creating string decoder: %v", err)
	}

	if !cmp.Equal(got, exp) {
		t.Fatalf("unexpected value: -got/+exp\n%s", cmp.Diff(got, exp))
	}
}

func TestStringBatchDecodeAll_Quick(t *testing.T) {
	quick.Check(func(values []string) bool {
		exp := values
		if values == nil {
			exp = []string{}
		}
		// Write values to encoder.
		enc := NewStringEncoder(1024)
		for _, v := range values {
			enc.Write(v)
		}

		// Retrieve encoded bytes from encoder.
		buf, err := enc.Bytes()
		if err != nil {
			t.Fatal(err)
		}

		// Read values out of decoder.
		got, err := StringBatchDecodeAll(buf, nil)
		if err != nil {
			t.Fatalf("unexpected error creating string decoder: %v", err)
		}

		if !cmp.Equal(got, exp) {
			t.Fatalf("unexpected value: -got/+exp\n%s", cmp.Diff(got, exp))
		}

		return true
	}, nil)
}

func TestStringBatchDecodeAll_Empty(t *testing.T) {
	got, err := StringBatchDecodeAll([]byte{}, nil)
	if err != nil {
		t.Fatalf("unexpected error creating string decoder: %v", err)
	}

	exp := []string{}
	if !cmp.Equal(got, exp) {
		t.Fatalf("unexpected value: -got/+exp\n%s", cmp.Diff(got, exp))
	}
}

func TestStringBatchDecodeAll_CorruptBytes(t *testing.T) {
	cases := []string{
		"\x10\x03\b\x03Hi", // Higher length than actual data
		"\x10\x1dp\x9c\x90\x90\x90\x90\x90\x90\x90\x90\x90length overflow----",
		"0t\x00\x01\x000\x00\x01\x000\x00\x01\x000\x00\x01\x000\x00\x01" +
			"\x000\x00\x01\x000\x00\x01\x000\x00\x00\x00\xff:\x01\x00\x01\x00\x01" +
			"\x00\x01\x00\x01\x00\x01\x00\x010\x010\x000\x010\x010\x010\x01" +
			"0\x010\x010\x010\x010\x010\x010\x010\x010\x010\x010", // Upper slice bounds overflows negative
	}

	for _, c := range cases {
		t.Run(fmt.Sprintf("%q", c), func(t *testing.T) {
			got, err := StringBatchDecodeAll([]byte(c), nil)
			if err == nil {
				t.Fatal("exp an err, got nil")
			}

			exp := []string{}
			if !cmp.Equal(got, exp) {
				t.Fatalf("unexpected value: -got/+exp\n%s", cmp.Diff(got, exp))
			}
		})
	}
}

func BenchmarkStringBatchDecodeAll(b *testing.B) {
	benchmarks := []struct {
		n int
		w int
	}{
		{1, 10},
		{55, 10},
		{550, 10},
		{1000, 10},
	}
	for _, bm := range benchmarks {
		rand.Seed(int64(bm.n * 1e3))

		s := NewStringEncoder(bm.n)
		for c := 0; c < bm.n; c++ {
			s.Write(testutil.MakeSentence(bm.w))
		}
		s.Flush()
		bytes, err := s.Bytes()
		if err != nil {
			b.Fatalf("unexpected error: %v", err)
		}

		b.Run(fmt.Sprintf("%d", bm.n), func(b *testing.B) {
			b.SetBytes(int64(len(bytes)))
			b.ReportAllocs()

			dst := make([]string, bm.n)
			for i := 0; i < b.N; i++ {
				got, err := StringBatchDecodeAll(bytes, dst)
				if err != nil {
					b.Fatalf("unexpected length -got/+exp\n%s", cmp.Diff(len(dst), bm.n))
				}
				if len(got) != bm.n {
					b.Fatalf("unexpected length -got/+exp\n%s", cmp.Diff(len(dst), bm.n))
				}
			}
		})
	}
}
