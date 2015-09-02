package pd1_test

import (
	"math/rand"
	"reflect"
	"testing"
	"time"

	"github.com/influxdb/influxdb/tsdb/engine/pd1"
)

func TestEncoding_FloatBlock(t *testing.T) {
	valueCount := 100
	times := getTimes(valueCount, 60, time.Second)
	values := make([]pd1.FloatValue, len(times))
	for i, t := range times {
		values[i] = pd1.FloatValue{Time: t, Value: rand.Float64()}
	}

	b := pd1.EncodeFloatBlock(nil, values)

	decodedValues, err := pd1.DecodeFloatBlock(b)
	if err != nil {
		t.Fatalf("error decoding: %s", err.Error)
	}
	if !reflect.DeepEqual(decodedValues, values) {
		t.Fatalf("values not equal:\n\tgot: %s\n\texp: %s", values, decodedValues)
	}
}

func getTimes(n, step int, precision time.Duration) []int64 {
	t := time.Now().Round(precision)
	a := make([]int64, n)
	for i := 0; i < n; i++ {
		a[i] = t.Add(60 * precision).UnixNano()
	}
	return a
}
