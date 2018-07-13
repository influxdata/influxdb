package tsm1

import (
	"fmt"
	"math/rand"
	"testing"
	"testing/quick"
	"time"

	"github.com/google/go-cmp/cmp"
)

func TestTimeBatchDecodeAll_NoValues(t *testing.T) {
	enc := NewTimeEncoder(0)
	b, err := enc.Bytes()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	got, err := TimeBatchDecodeAll(b, nil)
	if err != nil {
		t.Fatalf("unexpected decode error %q", err)
	}

	exp := []int64{}
	if !cmp.Equal(got, exp) {
		t.Fatalf("unexpected values: -got/+exp\n%s", cmp.Diff(got, exp))
	}
}

func TestTimeBatchDecodeAll_One(t *testing.T) {
	enc := NewTimeEncoder(1)
	exp := []int64{0}
	for _, v := range exp {
		enc.Write(v)
	}
	b, err := enc.Bytes()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got := b[0] >> 4; got != timeCompressedPackedSimple {
		t.Fatalf("Wrong encoding used: expected uncompressed, got %v", got)
	}

	got, err := TimeBatchDecodeAll(b, nil)
	if err != nil {
		t.Fatalf("unexpected decode error %q", err)
	}

	if !cmp.Equal(got, exp) {
		t.Fatalf("unexpected values: -got/+exp\n%s", cmp.Diff(got, exp))
	}
}

func TestTimeBatchDecodeAll_Two(t *testing.T) {
	enc := NewTimeEncoder(2)
	exp := []int64{0, 1}
	for _, v := range exp {
		enc.Write(v)
	}

	b, err := enc.Bytes()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got := b[0] >> 4; got != timeCompressedRLE {
		t.Fatalf("Wrong encoding used: expected rle, got %v", got)
	}

	got, err := TimeBatchDecodeAll(b, nil)
	if err != nil {
		t.Fatalf("unexpected decode error %q", err)
	}

	if !cmp.Equal(got, exp) {
		t.Fatalf("unexpected values: -got/+exp\n%s", cmp.Diff(got, exp))
	}
}

func TestTimeBatchDecodeAll_Three(t *testing.T) {
	enc := NewTimeEncoder(3)
	exp := []int64{0, 1, 3}
	for _, v := range exp {
		enc.Write(v)
	}

	b, err := enc.Bytes()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got := b[0] >> 4; got != timeCompressedPackedSimple {
		t.Fatalf("Wrong encoding used: expected rle, got %v", got)
	}

	got, err := TimeBatchDecodeAll(b, nil)
	if err != nil {
		t.Fatalf("unexpected decode error %q", err)
	}

	if !cmp.Equal(got, exp) {
		t.Fatalf("unexpected values: -got/+exp\n%s", cmp.Diff(got, exp))
	}
}

func TestTimeBatchDecodeAll_Large_Range(t *testing.T) {
	enc := NewTimeEncoder(2)
	exp := []int64{1442369134000000000, 1442369135000000000}
	for _, v := range exp {
		enc.Write(v)
	}
	b, err := enc.Bytes()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got := b[0] >> 4; got != timeCompressedRLE {
		t.Fatalf("Wrong encoding used: expected rle, got %v", got)
	}

	got, err := TimeBatchDecodeAll(b, nil)
	if err != nil {
		t.Fatalf("unexpected decode error %q", err)
	}

	if !cmp.Equal(got, exp) {
		t.Fatalf("unexpected values: -got/+exp\n%s", cmp.Diff(got, exp))
	}
}

func TestTimeBatchDecodeAll_Uncompressed(t *testing.T) {
	enc := NewTimeEncoder(3)
	exp := []int64{
		time.Unix(0, 0).UnixNano(),
		time.Unix(1, 0).UnixNano(),
		// about 36.5yrs in NS resolution is max range for compressed format
		// This should cause the encoding to fallback to raw points
		time.Unix(2, 2<<59).UnixNano(),
	}
	for _, v := range exp {
		enc.Write(v)
	}

	b, err := enc.Bytes()
	if err != nil {
		t.Fatalf("expected error: %v", err)
	}

	if exp := 25; len(b) != exp {
		t.Fatalf("length mismatch: got %v, exp %v", len(b), exp)
	}

	if got := b[0] >> 4; got != timeUncompressed {
		t.Fatalf("Wrong encoding used: expected uncompressed, got %v", got)
	}

	got, err := TimeBatchDecodeAll(b, nil)
	if err != nil {
		t.Fatalf("unexpected decode error %q", err)
	}

	if !cmp.Equal(got, exp) {
		t.Fatalf("unexpected values: -got/+exp\n%s", cmp.Diff(got, exp))
	}
}

func TestTimeBatchDecodeAll_RLE(t *testing.T) {
	enc := NewTimeEncoder(512)
	var exp []int64
	for i := 0; i < 500; i++ {
		exp = append(exp, int64(i))
	}

	for _, v := range exp {
		enc.Write(v)
	}

	b, err := enc.Bytes()
	if exp := 12; len(b) != exp {
		t.Fatalf("length mismatch: got %v, exp %v", len(b), exp)
	}

	if got := b[0] >> 4; got != timeCompressedRLE {
		t.Fatalf("Wrong encoding used: expected uncompressed, got %v", got)
	}

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	got, err := TimeBatchDecodeAll(b, nil)
	if err != nil {
		t.Fatalf("unexpected decode error %q", err)
	}

	if !cmp.Equal(got, exp) {
		t.Fatalf("unexpected values: -got/+exp\n%s", cmp.Diff(got, exp))
	}
}

func TestTimeBatchDecodeAll_Reverse(t *testing.T) {
	enc := NewTimeEncoder(3)
	exp := []int64{
		int64(3),
		int64(2),
		int64(0),
	}

	for _, v := range exp {
		enc.Write(v)
	}

	b, err := enc.Bytes()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got := b[0] >> 4; got != timeUncompressed {
		t.Fatalf("Wrong encoding used: expected uncompressed, got %v", got)
	}

	got, err := TimeBatchDecodeAll(b, nil)
	if err != nil {
		t.Fatalf("unexpected decode error %q", err)
	}

	if !cmp.Equal(got, exp) {
		t.Fatalf("unexpected values: -got/+exp\n%s", cmp.Diff(got, exp))
	}
}

func TestTimeBatchDecodeAll_Negative(t *testing.T) {
	enc := NewTimeEncoder(3)
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

	for _, v := range exp {
		enc.Write(v)
	}

	b, err := enc.Bytes()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got := b[0] >> 4; got != timeUncompressed {
		t.Fatalf("Wrong encoding used: expected uncompressed, got %v", got)
	}

	got, err := TimeBatchDecodeAll(b, nil)
	if err != nil {
		t.Fatalf("unexpected decode error %q", err)
	}

	if !cmp.Equal(got, exp) {
		t.Fatalf("unexpected values: -got/+exp\n%s", cmp.Diff(got, exp))
	}
}

func TestTimeBatchDecodeAll_220SecondDelta(t *testing.T) {
	enc := NewTimeEncoder(256)
	var exp []int64
	now := time.Now()
	for i := 0; i < 220; i++ {
		exp = append(exp, now.Add(time.Duration(i*60)*time.Second).UnixNano())
	}

	for _, v := range exp {
		enc.Write(v)
	}

	b, err := enc.Bytes()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Using RLE, should get 12 bytes
	if exp := 12; len(b) != exp {
		t.Fatalf("unexpected length: got %v, exp %v", len(b), exp)
	}

	if got := b[0] >> 4; got != timeCompressedRLE {
		t.Fatalf("Wrong encoding used: expected uncompressed, got %v", got)
	}

	got, err := TimeBatchDecodeAll(b, nil)
	if err != nil {
		t.Fatalf("unexpected decode error %q", err)
	}

	if !cmp.Equal(got, exp) {
		t.Fatalf("unexpected values: -got/+exp\n%s", cmp.Diff(got, exp))
	}
}

func TestTimeBatchDecodeAll_Quick(t *testing.T) {
	quick.Check(func(values []int64) bool {
		// Write values to encoder.
		enc := NewTimeEncoder(1024)
		exp := make([]int64, len(values))
		for i, v := range values {
			exp[i] = int64(v)
			enc.Write(exp[i])
		}

		// Retrieve encoded bytes from encoder.
		buf, err := enc.Bytes()
		if err != nil {
			t.Fatal(err)
		}

		got, err := TimeBatchDecodeAll(buf, nil)
		if err != nil {
			t.Fatalf("unexpected decode error %q", err)
		}

		if !cmp.Equal(got, exp) {
			t.Fatalf("unexpected values: -got/+exp\n%s", cmp.Diff(got, exp))
		}

		return true
	}, nil)
}

func TestTimeBatchDecodeAll_RLESeconds(t *testing.T) {
	enc := NewTimeEncoder(6)
	exp := make([]int64, 6)

	exp[0] = int64(1444448158000000000)
	exp[1] = int64(1444448168000000000)
	exp[2] = int64(1444448178000000000)
	exp[3] = int64(1444448188000000000)
	exp[4] = int64(1444448198000000000)
	exp[5] = int64(1444448208000000000)

	for _, v := range exp {
		enc.Write(v)
	}

	b, err := enc.Bytes()
	if got := b[0] >> 4; got != timeCompressedRLE {
		t.Fatalf("Wrong encoding used: expected rle, got %v", got)
	}

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	got, err := TimeBatchDecodeAll(b, nil)
	if err != nil {
		t.Fatalf("unexpected decode error %q", err)
	}

	if !cmp.Equal(got, exp) {
		t.Fatalf("unexpected values: -got/+exp\n%s", cmp.Diff(got, exp))
	}
}

func TestTimeBatchDecodeAll_Corrupt(t *testing.T) {
	cases := []string{
		"\x10\x14",         // Packed: not enough data
		"\x20\x00",         // RLE: not enough data for starting timestamp
		"\x2012345678\x90", // RLE: initial timestamp but invalid uvarint encoding
		"\x2012345678\x7f", // RLE: timestamp, RLE but invalid repeat
		"\x00123",          // Raw: data length not multiple of 8
	}

	for _, c := range cases {
		t.Run(fmt.Sprintf("%q", c), func(t *testing.T) {
			got, err := TimeBatchDecodeAll([]byte(c), nil)
			if err == nil {
				t.Fatal("exp an err, got nil")
			}

			exp := []int64{}
			if !cmp.Equal(got, exp) {
				t.Fatalf("unexpected value: -got/+exp\n%s", cmp.Diff(got, exp))
			}
		})
	}
}

func BenchmarkTimeBatchDecodeAllUncompressed(b *testing.B) {
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

		enc := NewTimeEncoder(size)
		for i := 0; i < size; i++ {
			enc.Write(values[rand.Int()%len(values)])
		}
		bytes, _ := enc.Bytes()

		b.Run(fmt.Sprintf("%d", size), func(b *testing.B) {
			b.SetBytes(int64(len(bytes)))
			b.ReportAllocs()

			dst := make([]int64, size)
			for i := 0; i < b.N; i++ {
				dst, _ = TimeBatchDecodeAll(bytes, dst)
			}
		})
	}
}

func BenchmarkTimeBatchDecodeAllPackedSimple(b *testing.B) {
	benchmarks := []int{
		5,
		55,
		555,
		1000,
	}
	for _, size := range benchmarks {
		rand.Seed(int64(size * 1e3))

		enc := NewTimeEncoder(size)
		for i := 0; i < size; i++ {
			// Small amount of randomness prevents RLE from being used
			enc.Write(int64(i*1000) + int64(rand.Intn(10)))
		}
		bytes, _ := enc.Bytes()

		b.Run(fmt.Sprintf("%d", size), func(b *testing.B) {
			b.SetBytes(int64(len(bytes)))
			b.ReportAllocs()

			dst := make([]int64, size)
			for i := 0; i < b.N; i++ {
				dst, _ = TimeBatchDecodeAll(bytes, dst)
			}
		})
	}
}

func BenchmarkTimeBatchDecodeAllRLE(b *testing.B) {
	benchmarks := []struct {
		n     int
		delta int64
	}{
		{5, 10},
		{55, 10},
		{555, 10},
		{1000, 10},
	}
	for _, bm := range benchmarks {
		enc := NewTimeEncoder(bm.n)
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
				dst, _ = TimeBatchDecodeAll(bytes, dst)
			}
		})
	}
}
