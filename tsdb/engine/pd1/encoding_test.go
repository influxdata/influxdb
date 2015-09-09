package pd1_test

import (
	// "math/rand"
	// "reflect"
	"testing"
	"time"

	// "github.com/influxdb/influxdb/tsdb/engine/pd1"
)

func TestEncoding_FloatBlock(t *testing.T) {
	// valueCount := 100
	// times := getTimes(valueCount, 60, time.Second)
	// values := make([]Value, len(times))
	// for i, t := range times {
	// 	values[i] = pd1.NewValue(t, rand.Float64())
	// }

	// b := pd1.EncodeFloatBlock(nil, values)

	// decodedValues, err := pd1.DecodeFloatBlock(b)
	// if err != nil {
	// 	t.Fatalf("error decoding: %s", err.Error)
	// }

	// if !reflect.DeepEqual(decodedValues, values) {
	// 	t.Fatalf("unexpected results:\n\tgot: %v\n\texp: %v\n", decodedValues, values)
	// }
}

func getTimes(n, step int, precision time.Duration) []time.Time {
	t := time.Now().Round(precision)
	a := make([]time.Time, n)
	for i := 0; i < n; i++ {
		a[i] = t.Add(60 * precision)
	}
	return a
}
