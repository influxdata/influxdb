package tsm1_test

import (
	"bytes"
	"fmt"
	"io"
	"math"
	"math/rand"
	"reflect"
	"testing"
	"testing/quick"

	"github.com/dgryski/go-bitstream"
	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/platform/tsdb/tsm1"
)

var fullBlockFloat64Ones []float64

func init() {
	for i := 0; i < 1000; i++ {
		fullBlockFloat64Ones = append(fullBlockFloat64Ones, 1.0)
	}
}
func TestFloatArrayEncodeAll(t *testing.T) {
	examples := [][]float64{
		{12, 12, 24, 13, 24, 24, 24, 24}, // From example paper.
		{-3.8970913068231994e+307, -9.036931257783943e+307, 1.7173073833490201e+308,
			-9.312369166661538e+307, -2.2435523083555231e+307, 1.4779121287289644e+307,
			1.771273431601434e+308, 8.140360378221364e+307, 4.783405048208089e+307,
			-2.8044680049605344e+307, 4.412915337205696e+307, -1.2779380602005046e+308,
			1.6235802318921885e+308, -1.3402901846299688e+307, 1.6961015582104055e+308,
			-1.067980796435633e+308, -3.02868987458268e+307, 1.7641793640790284e+308,
			1.6587191845856813e+307, -1.786073304985983e+308, 1.0694549382051123e+308,
			3.5635180996210295e+307}, // Failed during early development
		{6.00065e+06, 6.000656e+06, 6.000657e+06, 6.000659e+06, 6.000661e+06}, // Similar values.
		twoHoursData,
		fullBlockFloat64Ones,
		{},
	}

	for _, example := range examples {
		src := example
		var buf []byte
		buf, err := tsm1.FloatArrayEncodeAll(src, buf)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		result, err := tsm1.FloatArrayDecodeAll(buf, nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if got, exp := result, src; !reflect.DeepEqual(got, exp) {
			t.Fatalf("got result %v, expected %v", got, exp)
		}
	}
}

func TestFloatArrayEncode_Compare(t *testing.T) {
	// generate random values
	input := make([]float64, 1000)
	for i := 0; i < len(input); i++ {
		input[i] = (rand.Float64() * math.MaxFloat64) - math.MaxFloat32
	}

	s := tsm1.NewFloatEncoder()
	for _, v := range input {
		s.Write(v)
	}
	s.Flush()

	buf1, err := s.Bytes()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var buf2 []byte
	buf2, err = tsm1.FloatArrayEncodeAll(input, buf2)
	if err != nil {
		t.Fatalf("unexpected error: %v\nbuf: %db %x", err, len(buf2), buf2)
	}

	result, err := tsm1.FloatArrayDecodeAll(buf2, nil)
	if err != nil {
		dumpBufs(buf1, buf2)
		t.Fatalf("unexpected error: %v\nbuf: %db %x", err, len(buf2), buf2)
	}

	if got, exp := result, input; !reflect.DeepEqual(got, exp) {
		t.Fatalf("got result %v, expected %v", got, exp)
	}

	// Check that the encoders are byte for byte the same...
	if !bytes.Equal(buf1, buf2) {
		dumpBufs(buf1, buf2)
		t.Fatalf("Raw bytes differ for encoders")
	}
}

func dumpBufs(a, b []byte) {
	longest := len(a)
	if len(b) > longest {
		longest = len(b)
	}

	for i := 0; i < longest; i++ {
		var as, bs string
		if i < len(a) {
			as = fmt.Sprintf("%08b", a[i])
		}
		if i < len(b) {
			bs = fmt.Sprintf("%08b", b[i])
		}

		same := as == bs
		fmt.Printf("%d (%d) %s - %s :: %v\n", i, i*8, as, bs, same)
	}
	fmt.Println()
}

func dumpBuf(b []byte) {
	for i, v := range b {
		fmt.Printf("%d %08b\n", i, v)
	}
	fmt.Println()
}

func TestFloatArrayEncodeAll_NaN(t *testing.T) {
	examples := [][]float64{
		{1.0, math.NaN(), 2.0},
		{1.22, math.NaN()},
		{math.NaN(), math.NaN()},
		{math.NaN()},
	}

	for _, example := range examples {
		var buf []byte
		_, err := tsm1.FloatArrayEncodeAll(example, buf)
		if err == nil {
			t.Fatalf("expected error. got nil")
		}
	}
}

func Test_FloatArrayEncodeAll_Quick(t *testing.T) {
	quick.Check(func(values []float64) bool {
		src := values
		if src == nil {
			src = []float64{}
		}

		for i, v := range src {
			if math.IsNaN(v) {
				src[i] = 1.0 // Remove invalid values
			}
		}

		s := tsm1.NewFloatEncoder()
		for _, p := range src {
			s.Write(p)
		}
		s.Flush()

		buf1, err := s.Bytes()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		var buf2 []byte
		buf2, err = tsm1.FloatArrayEncodeAll(src, buf2)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		result, err := tsm1.FloatArrayDecodeAll(buf2, nil)
		if err != nil {
			dumpBufs(buf1, buf2)
			fmt.Println(src)
			t.Fatalf("unexpected error: %v", err)
		}

		if got, exp := result, src[:len(src)]; !reflect.DeepEqual(got, exp) {
			t.Fatalf("got result %v, expected %v", got, exp)
		}
		return true
	}, nil)
}

func TestDecodeFloatArrayAll_Empty(t *testing.T) {
	s := tsm1.NewFloatEncoder()
	s.Flush()

	b, err := s.Bytes()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var got []float64
	if _, err := tsm1.FloatArrayDecodeAll(b, got); err != nil {
		t.Fatal(err)
	}

}

func TestFloatArrayDecodeAll_Simple(t *testing.T) {
	// Example from the paper
	s := tsm1.NewFloatEncoder()

	exp := []float64{
		12,
		12,
		24,

		// extra tests

		// floating point masking/shifting bug
		13,
		24,

		// delta-of-delta sizes
		24,
		24,
		24,
	}

	for _, f := range exp {
		s.Write(f)
	}
	s.Flush()

	b, err := s.Bytes()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	buf := make([]float64, 8)
	got, err := tsm1.FloatArrayDecodeAll(b, buf)
	if err != nil {
		t.Fatalf("unexpected decode error %q", err)
	}

	if !cmp.Equal(got, exp) {
		t.Fatalf("unexpected values -got/+exp\n%s", cmp.Diff(got, exp))
	}
}

func TestFloatArrayDecodeAll_Empty(t *testing.T) {
	s := tsm1.NewFloatEncoder()
	s.Flush()

	b, err := s.Bytes()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	buf := make([]float64, 8)
	got, err := tsm1.FloatArrayDecodeAll(b, buf)
	if err != nil {
		t.Fatalf("unexpected decode error %q", err)
	}

	if exp := []float64{}; !cmp.Equal(got, exp) {
		t.Fatalf("unexpected values -got/+exp\n%s", cmp.Diff(got, exp))
	}
}

func TestBatchBitStreamEOF(t *testing.T) {
	br := tsm1.NewBatchBitReader([]byte("0"))

	b := br.ReadBits(8)
	if br.Err() != nil {
		t.Fatal(br.Err())
	}
	if b != '0' {
		t.Error("ReadBits(8) didn't return first byte")
	}

	br.ReadBits(8)
	if br.Err() != io.EOF {
		t.Error("ReadBits(8) on empty string didn't return EOF")
	}

	// 0 = 0b00110000
	br = tsm1.NewBatchBitReader([]byte("0"))

	buf := bytes.NewBuffer(nil)
	bw := bitstream.NewWriter(buf)

	for i := 0; i < 4; i++ {
		bit := br.ReadBit()
		if br.Err() == io.EOF {
			break
		}
		if br.Err() != nil {
			t.Error("GetBit returned error err=", br.Err().Error())
			return
		}
		bw.WriteBit(bitstream.Bit(bit))
	}

	bw.Flush(bitstream.One)

	err := bw.WriteByte(0xAA)
	if err != nil {
		t.Error("unable to WriteByte")
	}

	c := buf.Bytes()

	if len(c) != 2 || c[1] != 0xAA || c[0] != 0x3f {
		t.Error("bad return from 4 read bytes")
	}

	br = tsm1.NewBatchBitReader([]byte(""))
	br.ReadBit()
	if br.Err() != io.EOF {
		t.Error("ReadBit on empty string didn't return EOF")
	}
}

func TestBatchBitStream(t *testing.T) {
	buf := bytes.NewBuffer(nil)
	br := tsm1.NewBatchBitReader([]byte("hello"))
	bw := bitstream.NewWriter(buf)

	for {
		bit := br.ReadBit()
		if br.Err() == io.EOF {
			break
		}
		if br.Err() != nil {
			t.Error("GetBit returned error err=", br.Err().Error())
			return
		}
		bw.WriteBit(bitstream.Bit(bit))
	}

	s := buf.String()

	if s != "hello" {
		t.Error("expected 'hello', got=", []byte(s))
	}
}

func TestBatchByteStream(t *testing.T) {
	buf := bytes.NewBuffer(nil)
	br := tsm1.NewBatchBitReader([]byte("hello"))
	bw := bitstream.NewWriter(buf)

	for i := 0; i < 3; i++ {
		bit := br.ReadBit()
		if br.Err() == io.EOF {
			break
		}
		if br.Err() != nil {
			t.Error("GetBit returned error err=", br.Err().Error())
			return
		}
		bw.WriteBit(bitstream.Bit(bit))
	}

	for i := 0; i < 3; i++ {
		byt := br.ReadBits(8)
		if br.Err() == io.EOF {
			break
		}
		if br.Err() != nil {
			t.Error("ReadBits(8) returned error err=", br.Err().Error())
			return
		}
		bw.WriteByte(byte(byt))
	}

	u := br.ReadBits(13)

	if br.Err() != nil {
		t.Error("ReadBits returned error err=", br.Err().Error())
		return
	}

	bw.WriteBits(u, 13)

	bw.WriteBits(('!'<<12)|('.'<<4)|0x02, 20)
	// 0x2f == '/'
	bw.Flush(bitstream.One)

	s := buf.String()

	if s != "hello!./" {
		t.Errorf("expected 'hello!./', got=%x", []byte(s))
	}
}

// Ensure bit reader can read random bits written to a stream.
func TestBatchBitReader_Quick(t *testing.T) {
	if err := quick.Check(func(values []uint64, nbits []uint) bool {
		// Limit nbits to 64.
		for i := 0; i < len(values) && i < len(nbits); i++ {
			nbits[i] = (nbits[i] % 64) + 1
			values[i] = values[i] & (math.MaxUint64 >> (64 - nbits[i]))
		}

		// Write bits to a buffer.
		var buf bytes.Buffer
		w := bitstream.NewWriter(&buf)
		for i := 0; i < len(values) && i < len(nbits); i++ {
			w.WriteBits(values[i], int(nbits[i]))
		}
		w.Flush(bitstream.Zero)

		// Read bits from the buffer.
		r := tsm1.NewBatchBitReader(buf.Bytes())
		for i := 0; i < len(values) && i < len(nbits); i++ {
			v := r.ReadBits(nbits[i])
			if r.Err() != nil {
				t.Errorf("unexpected error(%d): %s", i, r.Err())
				return false
			} else if v != values[i] {
				t.Errorf("value mismatch(%d): got=%d, exp=%d (nbits=%d)", i, v, values[i], nbits[i])
				return false
			}
		}

		return true
	}, &quick.Config{
		Values: func(a []reflect.Value, rand *rand.Rand) {
			a[0], _ = quick.Value(reflect.TypeOf([]uint64{}), rand)
			a[1], _ = quick.Value(reflect.TypeOf([]uint{}), rand)
		},
	}); err != nil {
		t.Fatal(err)
	}
}

var bufResult []byte

func BenchmarkEncodeFloats(b *testing.B) {
	var err error
	cases := []int{10, 100, 1000}
	enc := tsm1.NewFloatEncoder()

	for _, n := range cases {
		b.Run(fmt.Sprintf("%d_seq", n), func(b *testing.B) {
			input := make([]float64, n)
			for i := 0; i < n; i++ {
				input[i] = float64(i)
			}

			b.Run("itr", func(b *testing.B) {
				b.ReportAllocs()
				enc.Reset()
				b.ResetTimer()
				for n := 0; n < b.N; n++ {
					enc.Reset()
					for _, x := range input {
						enc.Write(x)
					}
					enc.Flush()
					if bufResult, err = enc.Bytes(); err != nil {
						b.Fatal(err)
					}
				}
			})

			b.Run("batch", func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for n := 0; n < b.N; n++ {
					if bufResult, err = tsm1.FloatArrayEncodeAll(input, bufResult); err != nil {
						b.Fatal(err)
					}
				}
			})

		})

		b.Run(fmt.Sprintf("%d_ran", n), func(b *testing.B) {
			input := make([]float64, n)
			for i := 0; i < n; i++ {
				input[i] = rand.Float64() * 100.0
			}

			b.Run("itr", func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for n := 0; n < b.N; n++ {
					enc.Reset()
					for _, x := range input {
						enc.Write(x)
					}
					enc.Flush()
					if bufResult, err = enc.Bytes(); err != nil {
						b.Fatal(err)
					}
				}
			})

			b.Run("batch", func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for n := 0; n < b.N; n++ {
					if bufResult, err = tsm1.FloatArrayEncodeAll(input, bufResult); err != nil {
						b.Fatal(err)
					}
				}
			})
		})
	}
}

func BenchmarkFloatArrayDecodeAll(b *testing.B) {
	benchmarks := []int{
		1,
		55,
		550,
		1000,
	}
	for _, size := range benchmarks {
		s := tsm1.NewFloatEncoder()
		for c := 0; c < size; c++ {
			s.Write(twoHoursData[c%len(twoHoursData)])
		}
		s.Flush()
		bytes, err := s.Bytes()
		if err != nil {
			b.Fatalf("unexpected error: %v", err)
		}

		b.Run(fmt.Sprintf("%d", size), func(b *testing.B) {
			b.SetBytes(int64(len(bytes)))
			b.ResetTimer()

			dst := make([]float64, size)
			for i := 0; i < b.N; i++ {

				got, err := tsm1.FloatArrayDecodeAll(bytes, dst)
				if err != nil {
					b.Fatalf("unexpected length -got/+exp\n%s", cmp.Diff(len(dst), size))
				}
				if len(got) != size {
					b.Fatalf("unexpected length -got/+exp\n%s", cmp.Diff(len(dst), size))
				}
			}
		})
	}
}
