package tsm1_test

import (
	"fmt"
	"testing"

	"github.com/influxdb/influxdb/tsdb/engine/tsm1"
)

func Test_StringEncoder_NoValues(t *testing.T) {
	enc := tsm1.NewStringEncoder()
	b, err := enc.Bytes()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	dec := tsm1.NewStringDecoder(b)
	if dec.Next() {
		t.Fatalf("unexpected next value: got true, exp false")
	}
}

func Test_StringEncoder_Single(t *testing.T) {
	enc := tsm1.NewStringEncoder()
	v1 := "v1"
	enc.Write(v1)
	b, err := enc.Bytes()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	dec := tsm1.NewStringDecoder(b)
	if !dec.Next() {
		t.Fatalf("unexpected next value: got false, exp true")
	}

	if v1 != dec.Read() {
		t.Fatalf("unexpected value: got %v, exp %v", dec.Read(), v1)
	}
}

func Test_StringEncoder_Multi_Compressed(t *testing.T) {
	enc := tsm1.NewStringEncoder()

	values := make([]string, 10)
	for i := range values {
		values[i] = fmt.Sprintf("value %d", i)
		enc.Write(values[i])
	}

	b, err := enc.Bytes()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if b[0]>>4 != tsm1.EncodingSnappy {
		t.Fatalf("unexpected encoding: got %v, exp %v", b[0], tsm1.EncodingSnappy)
	}

	if exp := 47; len(b) != exp {
		t.Fatalf("unexpected length: got %v, exp %v", len(b), exp)
	}

	dec := tsm1.NewStringDecoder(b)

	for i, v := range values {
		if !dec.Next() {
			t.Fatalf("unexpected next value: got false, exp true")
		}
		if v != dec.Read() {
			t.Fatalf("unexpected value at pos %d: got %v, exp %v", i, dec.Read(), v)
		}
	}

	if dec.Next() {
		t.Fatalf("unexpected next value: got true, exp false")
	}
}
