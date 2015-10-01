package tsm1_test

import (
	"math"
	"testing"

	"github.com/influxdb/influxdb/tsdb/engine/tsm1"
)

func Test_Int64Encoder_NoValues(t *testing.T) {
	enc := tsm1.NewInt64Encoder()
	b, err := enc.Bytes()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	dec := tsm1.NewInt64Decoder(b)
	if dec.Next() {
		t.Fatalf("unexpected next value: got true, exp false")
	}
}

func Test_Int64Encoder_One(t *testing.T) {
	enc := tsm1.NewInt64Encoder()
	v1 := int64(1)

	enc.Write(1)
	b, err := enc.Bytes()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	dec := tsm1.NewInt64Decoder(b)
	if !dec.Next() {
		t.Fatalf("unexpected next value: got true, exp false")
	}

	if v1 != dec.Read() {
		t.Fatalf("read value mismatch: got %v, exp %v", dec.Read(), v1)
	}
}

func Test_Int64Encoder_Two(t *testing.T) {
	enc := tsm1.NewInt64Encoder()
	var v1, v2 int64 = 1, 2

	enc.Write(v1)
	enc.Write(v2)

	b, err := enc.Bytes()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	dec := tsm1.NewInt64Decoder(b)
	if !dec.Next() {
		t.Fatalf("unexpected next value: got true, exp false")
	}

	if v1 != dec.Read() {
		t.Fatalf("read value mismatch: got %v, exp %v", dec.Read(), v1)
	}

	if !dec.Next() {
		t.Fatalf("unexpected next value: got true, exp false")
	}

	if v2 != dec.Read() {
		t.Fatalf("read value mismatch: got %v, exp %v", dec.Read(), v2)
	}
}

func Test_Int64Encoder_Negative(t *testing.T) {
	enc := tsm1.NewInt64Encoder()
	var v1, v2, v3 int64 = -2, 0, 1

	enc.Write(v1)
	enc.Write(v2)
	enc.Write(v3)

	b, err := enc.Bytes()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	dec := tsm1.NewInt64Decoder(b)
	if !dec.Next() {
		t.Fatalf("unexpected next value: got true, exp false")
	}

	if v1 != dec.Read() {
		t.Fatalf("read value mismatch: got %v, exp %v", dec.Read(), v1)
	}

	if !dec.Next() {
		t.Fatalf("unexpected next value: got true, exp false")
	}

	if v2 != dec.Read() {
		t.Fatalf("read value mismatch: got %v, exp %v", dec.Read(), v2)
	}

	if !dec.Next() {
		t.Fatalf("unexpected next value: got true, exp false")
	}

	if v3 != dec.Read() {
		t.Fatalf("read value mismatch: got %v, exp %v", dec.Read(), v3)
	}
}

func Test_Int64Encoder_Large_Range(t *testing.T) {
	enc := tsm1.NewInt64Encoder()
	var v1, v2 int64 = math.MinInt64, math.MaxInt64
	enc.Write(v1)
	enc.Write(v2)
	b, err := enc.Bytes()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	dec := tsm1.NewInt64Decoder(b)
	if !dec.Next() {
		t.Fatalf("unexpected next value: got true, exp false")
	}

	if v1 != dec.Read() {
		t.Fatalf("read value mismatch: got %v, exp %v", dec.Read(), v1)
	}

	if !dec.Next() {
		t.Fatalf("unexpected next value: got true, exp false")
	}

	if v2 != dec.Read() {
		t.Fatalf("read value mismatch: got %v, exp %v", dec.Read(), v2)
	}
}

func Test_Int64Encoder_Uncompressed(t *testing.T) {
	enc := tsm1.NewInt64Encoder()
	var v1, v2, v3 int64 = 0, 1, 1 << 60

	enc.Write(v1)
	enc.Write(v2)
	enc.Write(v3)

	b, err := enc.Bytes()
	if err != nil {
		t.Fatalf("expected error: %v", err)
	}

	// 1 byte header + 3 * 8 byte values
	if exp := 25; len(b) != exp {
		t.Fatalf("length mismatch: got %v, exp %v", len(b), exp)
	}

	dec := tsm1.NewInt64Decoder(b)
	if !dec.Next() {
		t.Fatalf("unexpected next value: got true, exp false")
	}

	if v1 != dec.Read() {
		t.Fatalf("read value mismatch: got %v, exp %v", dec.Read(), v1)
	}

	if !dec.Next() {
		t.Fatalf("unexpected next value: got true, exp false")
	}

	if v2 != dec.Read() {
		t.Fatalf("read value mismatch: got %v, exp %v", dec.Read(), v2)
	}

	if !dec.Next() {
		t.Fatalf("unexpected next value: got true, exp false")
	}

	if v3 != dec.Read() {
		t.Fatalf("read value mismatch: got %v, exp %v", dec.Read(), v3)
	}
}

func Test_Int64Encoder_AllNegative(t *testing.T) {
	enc := tsm1.NewInt64Encoder()
	values := []int64{
		-10, -5, -1,
	}

	for _, v := range values {
		enc.Write(v)
	}

	b, err := enc.Bytes()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	dec := tsm1.NewInt64Decoder(b)
	i := 0
	for dec.Next() {
		if i > len(values) {
			t.Fatalf("read too many values: got %v, exp %v", i, len(values))
		}

		if values[i] != dec.Read() {
			t.Fatalf("read value %d mismatch: got %v, exp %v", i, dec.Read(), values[i])
		}
		i += 1
	}
}

func BenchmarkInt64Encoder(b *testing.B) {
	enc := tsm1.NewInt64Encoder()
	x := make([]int64, 1024)
	for i := 0; i < len(x); i++ {
		x[i] = int64(i)
		enc.Write(x[i])
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		enc.Bytes()
	}
}

type byteSetter interface {
	SetBytes(b []byte)
}

func BenchmarkInt64Decoder(b *testing.B) {
	x := make([]int64, 1024)
	enc := tsm1.NewInt64Encoder()
	for i := 0; i < len(x); i++ {
		x[i] = int64(i)
		enc.Write(x[i])
	}
	bytes, _ := enc.Bytes()

	b.ResetTimer()

	dec := tsm1.NewInt64Decoder(bytes)

	for i := 0; i < b.N; i++ {
		dec.(byteSetter).SetBytes(bytes)
		for dec.Next() {
		}
	}
}
