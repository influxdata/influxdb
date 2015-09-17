package pd1_test

import (
	// "math/rand"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/influxdb/influxdb/tsdb/engine/pd1"
)

func TestEncoding_FloatBlock(t *testing.T) {
	valueCount := 1000
	times := getTimes(valueCount, 60, time.Second)
	values := make(pd1.Values, len(times))
	for i, t := range times {
		values[i] = pd1.NewValue(t, float64(i))
	}

	b := values.Encode(nil)
	fmt.Println("**** ", len(b))

	decodedValues := values.DecodeSameTypeBlock(b)

	if !reflect.DeepEqual(decodedValues, values) {
		t.Fatalf("unexpected results:\n\tgot: %v\n\texp: %v\n", decodedValues, values)
	}
}

func getTimes(n, step int, precision time.Duration) []time.Time {
	t := time.Now().Round(precision)
	a := make([]time.Time, n)
	for i := 0; i < n; i++ {
		a[i] = t.Add(60 * precision)
	}
	return a
}
