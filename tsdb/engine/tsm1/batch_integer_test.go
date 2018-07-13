package tsm1

import (
	"fmt"
	"math"
	"math/rand"
	"testing"
	"testing/quick"

	"github.com/google/go-cmp/cmp"
)

func TestIntegerBatchDecodeAll_NegativeUncompressed(t *testing.T) {
	exp := []int64{
		-2352281900722994752, 1438442655375607923, -4110452567888190110,
		-1221292455668011702, -1941700286034261841, -2836753127140407751,
		1432686216250034552, 3663244026151507025, -3068113732684750258,
		-1949953187327444488, 3713374280993588804, 3226153669854871355,
		-2093273755080502606, 1006087192578600616, -2272122301622271655,
		2533238229511593671, -4450454445568858273, 2647789901083530435,
		2761419461769776844, -1324397441074946198, -680758138988210958,
		94468846694902125, -2394093124890745254, -2682139311758778198,
	}
	enc := NewIntegerEncoder(256)
	for _, v := range exp {
		enc.Write(v)
	}

	b, err := enc.Bytes()
	if err != nil {
		t.Fatalf("expected error: %v", err)
	}

	if got := b[0] >> 4; intUncompressed != got {
		t.Fatalf("encoding type mismatch: exp uncompressed, got %v", got)
	}

	got, err := IntegerBatchDecodeAll(b, nil)
	if err != nil {
		t.Fatalf("unexpected decode error %q", err)
	}

	if !cmp.Equal(got, exp) {
		t.Fatalf("unexpected values: -got/+exp\n%s", cmp.Diff(got, exp))
	}
}

func TestIntegerBatchDecodeAll_AllNegative(t *testing.T) {
	enc := NewIntegerEncoder(3)
	exp := []int64{
		-10, -5, -1,
	}

	for _, v := range exp {
		enc.Write(v)
	}

	b, err := enc.Bytes()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got := b[0] >> 4; intCompressedSimple != got {
		t.Fatalf("encoding type mismatch: exp uncompressed, got %v", got)
	}

	got, err := IntegerBatchDecodeAll(b, nil)
	if err != nil {
		t.Fatalf("unexpected decode error %q", err)
	}

	if !cmp.Equal(got, exp) {
		t.Fatalf("unexpected values: -got/+exp\n%s", cmp.Diff(got, exp))
	}
}

func TestIntegerBatchDecodeAll_CounterPacked(t *testing.T) {
	enc := NewIntegerEncoder(16)
	exp := []int64{
		1e15, 1e15 + 1, 1e15 + 2, 1e15 + 3, 1e15 + 4, 1e15 + 6,
	}

	for _, v := range exp {
		enc.Write(v)
	}

	b, err := enc.Bytes()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if b[0]>>4 != intCompressedSimple {
		t.Fatalf("unexpected encoding format: expected simple, got %v", b[0]>>4)
	}

	// Should use 1 header byte + 2, 8 byte words if delta-encoding is used based on
	// values sizes.  Without delta-encoding, we'd get 49 bytes.
	if exp := 17; len(b) != exp {
		t.Fatalf("encoded length mismatch: got %v, exp %v", len(b), exp)
	}

	got, err := IntegerBatchDecodeAll(b, nil)
	if err != nil {
		t.Fatalf("unexpected decode error %q", err)
	}

	if !cmp.Equal(got, exp) {
		t.Fatalf("unexpected values: -got/+exp\n%s", cmp.Diff(got, exp))
	}
}

func TestIntegerBatchDecodeAll_CounterRLE(t *testing.T) {
	enc := NewIntegerEncoder(16)
	exp := []int64{
		1e15, 1e15 + 1, 1e15 + 2, 1e15 + 3, 1e15 + 4, 1e15 + 5,
	}

	for _, v := range exp {
		enc.Write(v)
	}

	b, err := enc.Bytes()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if b[0]>>4 != intCompressedRLE {
		t.Fatalf("unexpected encoding format: expected RLE, got %v", b[0]>>4)
	}

	// Should use 1 header byte, 8 byte first value, 1 var-byte for delta and 1 var-byte for
	// count of deltas in this particular RLE.
	if exp := 11; len(b) != exp {
		t.Fatalf("encoded length mismatch: got %v, exp %v", len(b), exp)
	}

	got, err := IntegerBatchDecodeAll(b, nil)
	if err != nil {
		t.Fatalf("unexpected decode error %q", err)
	}

	if !cmp.Equal(got, exp) {
		t.Fatalf("unexpected values: -got/+exp\n%s", cmp.Diff(got, exp))
	}
}

func TestIntegerBatchDecodeAll_Descending(t *testing.T) {
	enc := NewIntegerEncoder(16)
	exp := []int64{
		7094, 4472, 1850,
	}

	for _, v := range exp {
		enc.Write(v)
	}

	b, err := enc.Bytes()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if b[0]>>4 != intCompressedRLE {
		t.Fatalf("unexpected encoding format: expected simple, got %v", b[0]>>4)
	}

	// Should use 1 header byte, 8 byte first value, 1 var-byte for delta and 1 var-byte for
	// count of deltas in this particular RLE.
	if exp := 12; len(b) != exp {
		t.Fatalf("encoded length mismatch: got %v, exp %v", len(b), exp)
	}

	got, err := IntegerBatchDecodeAll(b, nil)
	if err != nil {
		t.Fatalf("unexpected decode error %q", err)
	}

	if !cmp.Equal(got, exp) {
		t.Fatalf("unexpected values: -got/+exp\n%s", cmp.Diff(got, exp))
	}
}

func TestIntegerBatchDecodeAll_Flat(t *testing.T) {
	enc := NewIntegerEncoder(16)
	exp := []int64{
		1, 1, 1, 1,
	}

	for _, v := range exp {
		enc.Write(v)
	}

	b, err := enc.Bytes()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if b[0]>>4 != intCompressedRLE {
		t.Fatalf("unexpected encoding format: expected simple, got %v", b[0]>>4)
	}

	// Should use 1 header byte, 8 byte first value, 1 var-byte for delta and 1 var-byte for
	// count of deltas in this particular RLE.
	if exp := 11; len(b) != exp {
		t.Fatalf("encoded length mismatch: got %v, exp %v", len(b), exp)
	}

	got, err := IntegerBatchDecodeAll(b, nil)
	if err != nil {
		t.Fatalf("unexpected decode error %q", err)
	}

	if !cmp.Equal(got, exp) {
		t.Fatalf("unexpected values: -got/+exp\n%s", cmp.Diff(got, exp))
	}
}

func TestIntegerBatchDecodeAll_MinMax(t *testing.T) {
	enc := NewIntegerEncoder(2)
	exp := []int64{
		math.MinInt64, math.MaxInt64,
	}

	for _, v := range exp {
		enc.Write(v)
	}

	b, err := enc.Bytes()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if b[0]>>4 != intUncompressed {
		t.Fatalf("unexpected encoding format: expected simple, got %v", b[0]>>4)
	}

	if exp := 17; len(b) != exp {
		t.Fatalf("encoded length mismatch: got %v, exp %v", len(b), exp)
	}

	got, err := IntegerBatchDecodeAll(b, nil)
	if err != nil {
		t.Fatalf("unexpected decode error %q", err)
	}

	if !cmp.Equal(got, exp) {
		t.Fatalf("unexpected values: -got/+exp\n%s", cmp.Diff(got, exp))
	}
}

func TestIntegerBatchDecodeAll_Quick(t *testing.T) {
	quick.Check(func(values []int64) bool {
		exp := values
		if values == nil {
			exp = []int64{} // is this really expected?
		}

		// Write values to encoder.
		enc := NewIntegerEncoder(1024)
		for _, v := range values {
			enc.Write(v)
		}

		// Retrieve encoded bytes from encoder.
		buf, err := enc.Bytes()
		if err != nil {
			t.Fatal(err)
		}

		// Read values out of decoder.
		got, err := IntegerBatchDecodeAll(buf, nil)
		if err != nil {
			t.Fatalf("unexpected decode error %q", err)
		}

		if !cmp.Equal(got, exp) {
			t.Fatalf("unexpected values: -got/+exp\n%s", cmp.Diff(got, exp))
		}

		return true
	}, nil)
}

func BenchmarkIntegerBatchDecodeAllUncompressed(b *testing.B) {
	benchmarks := []int{
		5,
		55,
		555,
		1000,
	}

	values := []int64{
		-2352281900722994752, 1438442655375607923, -4110452567888190110,
		-1221292455668011702, -1941700286034261841, -2836753127140407751,
		1432686216250034552, 3663244026151507025, -3068113732684750258,
		-1949953187327444488, 3713374280993588804, 3226153669854871355,
		-2093273755080502606, 1006087192578600616, -2272122301622271655,
		2533238229511593671, -4450454445568858273, 2647789901083530435,
		2761419461769776844, -1324397441074946198, -680758138988210958,
		94468846694902125, -2394093124890745254, -2682139311758778198,
	}

	for _, size := range benchmarks {
		rand.Seed(int64(size * 1e3))

		enc := NewIntegerEncoder(size)
		for i := 0; i < size; i++ {
			enc.Write(values[rand.Int()%len(values)])
		}
		bytes, _ := enc.Bytes()

		b.Run(fmt.Sprintf("%d", size), func(b *testing.B) {
			b.SetBytes(int64(len(bytes)))
			b.ReportAllocs()

			dst := make([]int64, size)
			for i := 0; i < b.N; i++ {
				dst, _ = IntegerBatchDecodeAll(bytes, dst)
			}
		})
	}
}

func BenchmarkIntegerBatchDecodeAllPackedSimple(b *testing.B) {
	benchmarks := []int{
		5,
		55,
		555,
		1000,
	}
	for _, size := range benchmarks {
		rand.Seed(int64(size * 1e3))

		enc := NewIntegerEncoder(size)
		for i := 0; i < size; i++ {
			// Small amount of randomness prevents RLE from being used
			enc.Write(int64(i) + int64(rand.Intn(10)))
		}
		bytes, _ := enc.Bytes()

		b.Run(fmt.Sprintf("%d", size), func(b *testing.B) {
			b.SetBytes(int64(len(bytes)))
			b.ReportAllocs()

			dst := make([]int64, size)
			for i := 0; i < b.N; i++ {
				IntegerBatchDecodeAll(bytes, dst)
			}
		})
	}
}

func BenchmarkIntegerBatchDecodeAllRLE(b *testing.B) {
	benchmarks := []struct {
		n     int
		delta int64
	}{
		{5, 1},
		{55, 1},
		{555, 1},
		{1000, 1},
		{1000, 0},
	}
	for _, bm := range benchmarks {
		rand.Seed(int64(bm.n * 1e3))

		enc := NewIntegerEncoder(bm.n)
		acc := int64(0)
		for i := 0; i < bm.n; i++ {
			enc.Write(acc)
			acc += bm.delta
		}
		bytes, _ := enc.Bytes()

		b.Run(fmt.Sprintf("%d_delta_%d", bm.n, bm.delta), func(b *testing.B) {
			b.SetBytes(int64(len(bytes)))
			b.ReportAllocs()

			dst := make([]int64, bm.n)
			for i := 0; i < b.N; i++ {
				IntegerBatchDecodeAll(bytes, dst)
			}
		})
	}
}
