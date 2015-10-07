package tsm1_test

import (
	// "math/rand"

	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/influxdb/influxdb/tsdb/engine/tsm1"
)

func TestEncoding_FloatBlock(t *testing.T) {
	valueCount := 1000
	times := getTimes(valueCount, 60, time.Second)
	values := make(tsm1.Values, len(times))
	for i, t := range times {
		values[i] = tsm1.NewValue(t, float64(i))
	}

	b, err := values.Encode(nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	decodedValues := values.DecodeSameTypeBlock(b)

	if !reflect.DeepEqual(decodedValues, values) {
		t.Fatalf("unexpected results:\n\tgot: %v\n\texp: %v\n", decodedValues, values)
	}
}

func TestEncoding_FloatBlock_ZeroTime(t *testing.T) {
	values := make(tsm1.Values, 3)
	for i := 0; i < 3; i++ {
		values[i] = tsm1.NewValue(time.Unix(0, 0), float64(i))
	}

	b, err := values.Encode(nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	decodedValues := values.DecodeSameTypeBlock(b)

	if !reflect.DeepEqual(decodedValues, values) {
		t.Fatalf("unexpected results:\n\tgot: %v\n\texp: %v\n", decodedValues, values)
	}
}

func TestEncoding_FloatBlock_SimilarFloats(t *testing.T) {
	values := make(tsm1.Values, 5)
	values[0] = tsm1.NewValue(time.Unix(0, 1444238178437870000), 6.00065e+06)
	values[1] = tsm1.NewValue(time.Unix(0, 1444238185286830000), 6.000656e+06)
	values[2] = tsm1.NewValue(time.Unix(0, 1444238188441501000), 6.000657e+06)
	values[3] = tsm1.NewValue(time.Unix(0, 1444238195286811000), 6.000659e+06)
	values[4] = tsm1.NewValue(time.Unix(0, 1444238198439917000), 6.000661e+06)

	b, err := values.Encode(nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	decodedValues := values.DecodeSameTypeBlock(b)

	if !reflect.DeepEqual(decodedValues, values) {
		t.Fatalf("unexpected results:\n\tgot: %v\n\texp: %v\n", decodedValues, values)
	}
}

func TestEncoding_IntBlock_Basic(t *testing.T) {
	valueCount := 1000
	times := getTimes(valueCount, 60, time.Second)
	values := make(tsm1.Values, len(times))
	for i, t := range times {
		values[i] = tsm1.NewValue(t, int64(i))
	}

	b, err := values.Encode(nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	decodedValues := values.DecodeSameTypeBlock(b)

	if len(decodedValues) != len(values) {
		t.Fatalf("unexpected results length:\n\tgot: %v\n\texp: %v\n", len(decodedValues), len(values))
	}

	for i := 0; i < len(decodedValues); i++ {

		if decodedValues[i].Time() != values[i].Time() {
			t.Fatalf("unexpected results:\n\tgot: %v\n\texp: %v\n", decodedValues[i].Time(), values[i].Time())
		}

		if decodedValues[i].Value() != values[i].Value() {
			t.Fatalf("unexpected results:\n\tgot: %v\n\texp: %v\n", decodedValues[i].Value(), values[i].Value())
		}
	}
}

func TestEncoding_IntBlock_Negatives(t *testing.T) {
	valueCount := 1000
	times := getTimes(valueCount, 60, time.Second)
	values := make(tsm1.Values, len(times))
	for i, t := range times {
		v := int64(i)
		if i%2 == 0 {
			v = -v
		}
		values[i] = tsm1.NewValue(t, int64(v))
	}

	b, err := values.Encode(nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	decodedValues := values.DecodeSameTypeBlock(b)

	if !reflect.DeepEqual(decodedValues, values) {
		t.Fatalf("unexpected results:\n\tgot: %v\n\texp: %v\n", decodedValues, values)
	}
}

func TestEncoding_BoolBlock_Basic(t *testing.T) {
	valueCount := 1000
	times := getTimes(valueCount, 60, time.Second)
	values := make(tsm1.Values, len(times))
	for i, t := range times {
		v := true
		if i%2 == 0 {
			v = false
		}
		values[i] = tsm1.NewValue(t, v)
	}

	b, err := values.Encode(nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	decodedValues := values.DecodeSameTypeBlock(b)

	if !reflect.DeepEqual(decodedValues, values) {
		t.Fatalf("unexpected results:\n\tgot: %v\n\texp: %v\n", decodedValues, values)
	}
}

func TestEncoding_StringBlock_Basic(t *testing.T) {
	valueCount := 1000
	times := getTimes(valueCount, 60, time.Second)
	values := make(tsm1.Values, len(times))
	for i, t := range times {
		values[i] = tsm1.NewValue(t, fmt.Sprintf("value %d", i))
	}

	b, err := values.Encode(nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	decodedValues := values.DecodeSameTypeBlock(b)

	if !reflect.DeepEqual(decodedValues, values) {
		t.Fatalf("unexpected results:\n\tgot: %v\n\texp: %v\n", decodedValues, values)
	}
}

func getTimes(n, step int, precision time.Duration) []time.Time {
	t := time.Now().Round(precision)
	a := make([]time.Time, n)
	for i := 0; i < n; i++ {
		a[i] = t.Add(time.Duration(i*60) * precision)
	}
	return a
}
