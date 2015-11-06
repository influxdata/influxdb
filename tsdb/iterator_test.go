package tsdb_test

import (
	"fmt"
	"math"
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

// TestJoin ensures that a set of iterators can be combined together and output synced iterators.
func TestJoin(t *testing.T) {
	inputs := []tsdb.Iterator{
		&FloatIterator{Values: []tsdb.FloatValue{
			{Time: time.Unix(0, 0).UTC(), Value: 1},
			{Time: time.Unix(1, 0).UTC(), Value: 2},
		}},
		&FloatIterator{Values: []tsdb.FloatValue{
			{Time: time.Unix(1, 0).UTC(), Value: 4},
			{Time: time.Unix(4, 0).UTC(), Value: 5},
		}},
	}

	outputs := Iterators(tsdb.Join(inputs))
	if a := outputs.Next(); !tsdb.Values(a).Equals(tsdb.Values{
		&tsdb.FloatValue{Time: time.Unix(0, 0).UTC(), Value: 1},
		&tsdb.FloatValue{Time: time.Unix(0, 0).UTC(), Value: math.NaN()},
	}) {
		t.Fatalf("unexpected values: %s", spew.Sdump(a))
	}

	if a := outputs.Next(); !tsdb.Values(a).Equals(tsdb.Values{
		&tsdb.FloatValue{Time: time.Unix(1, 0).UTC(), Value: 2},
		&tsdb.FloatValue{Time: time.Unix(1, 0).UTC(), Value: 4},
	}) {
		t.Fatalf("unexpected values: %s", spew.Sdump(a))
	}

	if a := outputs.Next(); !tsdb.Values(a).Equals(tsdb.Values{
		&tsdb.FloatValue{Time: time.Unix(4, 0).UTC(), Value: math.NaN()},
		&tsdb.FloatValue{Time: time.Unix(4, 0).UTC(), Value: 5},
	}) {
		t.Fatalf("unexpected values: %s", spew.Sdump(a))
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

func BenchmarkJoin(b *testing.B) {
	rand := rand.New(rand.NewSource(0))

	// Generate inputs.
	inputs := make([]tsdb.Iterator, 10)
	for i := range inputs {
		inputs[i] = GenerateFloatIterator(rand, b.N/len(inputs))
	}
	b.ResetTimer()
	b.ReportAllocs()

	// Join inputs together and continuously read all outputs until a nil is returned.
	outputs := tsdb.Join(inputs)
	for {
		var done bool
		for _, output := range outputs {
			if v := output.(tsdb.FloatIterator).Next(); v == nil {
				done = true
			}
		}

		if done {
			break
		}
	}
}

// Iterators represents a helper wrapper for working with multiple generic iterators.
type Iterators []tsdb.Iterator

// Next returns the next value from each iterator.
func (itrs Iterators) Next() []tsdb.Value {
	a := make([]tsdb.Value, len(itrs))
	for i, itr := range itrs {
		switch itr := itr.(type) {
		case tsdb.FloatIterator:
			a[i] = itr.Next()
		default:
			panic(fmt.Sprintf("iterator type not supported: %T", itr))
		}
	}
	return a
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

// GenerateFloatIterator creates a FloatIterator with random data.
func GenerateFloatIterator(rand *rand.Rand, valueN int) *FloatIterator {
	const interval = 10 * time.Second

	itr := &FloatIterator{
		Values: make([]tsdb.FloatValue, valueN),
	}

	for i := 0; i < valueN; i++ {
		// Generate incrementing timestamp with some jitter (1s).
		jitter := (rand.Int63n(2) * int64(time.Second))
		timestamp := int64(i)*int64(10*time.Second) + jitter

		itr.Values[i] = tsdb.FloatValue{
			Time:  time.Unix(timestamp, 0).UTC(),
			Value: rand.Float64(),
		}
	}

	return itr
}
