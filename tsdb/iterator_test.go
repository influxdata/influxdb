package tsdb_test

import (
	"math/rand"
	"reflect"
	"testing"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/influxdb/influxdb/tsdb"
)

// Ensure that a MinIterator can calculate minimum points within each interval.
func TestFloatMinIterator(t *testing.T) {
	itr := tsdb.NewFloatMinIterator([]tsdb.FloatIterator{
		&FloatIterator{Values: []tsdb.FloatValue{
			{Time: time.Unix(0, 0).UTC(), Value: 15},
			{Time: time.Unix(1, 0).UTC(), Value: 10},
			{Time: time.Unix(5, 0).UTC(), Value: 20},
		}},
		&FloatIterator{Values: []tsdb.FloatValue{
			{Time: time.Unix(1, 0).UTC(), Value: 11},
			{Time: time.Unix(2, 0).UTC(), Value: 10},
			{Time: time.Unix(23, 0).UTC(), Value: 8},
		}},
	})
	itr.Interval = 5 * time.Second

	if v := itr.Next(); !reflect.DeepEqual(v, &tsdb.FloatValue{Time: time.Unix(1, 0).UTC(), Value: 10}) {
		t.Fatalf("unexpected value: %s", spew.Sdump(v))
	}
	if v := itr.Next(); !reflect.DeepEqual(v, &tsdb.FloatValue{Time: time.Unix(5, 0).UTC(), Value: 20}) {
		t.Fatalf("unexpected value: %s", spew.Sdump(v))
	}
	if v := itr.Next(); !reflect.DeepEqual(v, &tsdb.FloatValue{Time: time.Unix(23, 0).UTC(), Value: 8}) {
		t.Fatalf("unexpected value: %s", spew.Sdump(v))
	}
}

func BenchmarkFloatMinIterator_1m(b *testing.B)  { benchmarkFloatMinIterator(b, 1*time.Minute) }
func BenchmarkFloatMinIterator_10m(b *testing.B) { benchmarkFloatMinIterator(b, 10*time.Minute) }
func BenchmarkFloatMinIterator_1h(b *testing.B)  { benchmarkFloatMinIterator(b, 1*time.Hour) }
func BenchmarkFloatMinIterator_1d(b *testing.B)  { benchmarkFloatMinIterator(b, 24*time.Hour) }

func benchmarkFloatMinIterator(b *testing.B, interval time.Duration) {
	// Generate inputs.
	const inputN = 10
	var inputs []tsdb.FloatIterator
	for i := 0; i < inputN; i++ {
		values := make([]tsdb.FloatValue, b.N/inputN)
		for j := range values {
			values[j] = tsdb.FloatValue{
				Time:  time.Unix(int64(time.Duration(j)*(10*time.Second)), 0).UTC(),
				Value: rand.Float64(),
			}
		}
		inputs = append(inputs, &FloatIterator{Values: values})
	}
	b.ResetTimer()
	b.ReportAllocs()

	itr := tsdb.NewFloatMinIterator(inputs)
	itr.Interval = interval
	for {
		if v := itr.Next(); v == nil {
			break
		}
	}
}

// Test implementation of tsdb.FloatIterator.
type FloatIterator struct {
	Values []tsdb.FloatValue
}

// Next returns the next value and shifts it off the beginning of the Values slice.
func (itr *FloatIterator) Next() *tsdb.FloatValue {
	if len(itr.Values) == 0 {
		return nil
	}

	v := &itr.Values[0]
	itr.Values = itr.Values[1:]
	return v
}
