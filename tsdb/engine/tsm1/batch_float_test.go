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
	"github.com/influxdata/influxdb/tsdb/engine/tsm1"
)

func TestFloatBatchDecodeAll_Simple(t *testing.T) {
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
	got, err := tsm1.FloatBatchDecodeAll(b, buf)
	if err != nil {
		t.Fatalf("unexpected decode error %q", err)
	}

	if !cmp.Equal(got, exp) {
		t.Fatalf("unexpected values -got/+exp\n%s", cmp.Diff(got, exp))
	}
}

func TestFloatBatchDecodeAll_Empty(t *testing.T) {
	s := tsm1.NewFloatEncoder()
	s.Flush()

	b, err := s.Bytes()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	buf := make([]float64, 8)
	got, err := tsm1.FloatBatchDecodeAll(b, buf)
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

func BenchmarkFloatBatchDecodeAll(b *testing.B) {
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

				got, err := tsm1.FloatBatchDecodeAll(bytes, dst)
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
