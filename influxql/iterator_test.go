package influxql_test

import (
	"fmt"
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/influxdb/influxdb/influxql"
	"github.com/influxdb/influxdb/pkg/deep"
)

// Ensure that a set of iterators can be combined together and output synced iterators.
func TestJoin(t *testing.T) {
	inputs := []influxql.Iterator{
		&FloatIterator{Points: []influxql.FloatPoint{
			{Time: 0, Value: 1},
			{Time: 1, Value: 2},
		}},
		&FloatIterator{Points: []influxql.FloatPoint{
			{Time: 1, Value: 4},
			{Time: 4, Value: 5},
		}},
	}

	if a := Iterators(influxql.Join(inputs)).ReadAll(); !deep.Equal(a, [][]influxql.Point{
		{
			&influxql.FloatPoint{Time: 0, Value: 1},
			&influxql.FloatPoint{Time: 0, Value: math.NaN()},
		},
		{
			&influxql.FloatPoint{Time: 1, Value: 2},
			&influxql.FloatPoint{Time: 1, Value: 4},
		},
		{
			&influxql.FloatPoint{Time: 4, Value: math.NaN()},
			&influxql.FloatPoint{Time: 4, Value: 5},
		},
	}) {
		t.Fatalf("unexpected points: %s", spew.Sdump(a))
	}
}

// Ensure auxilary iterators can be created for auxilary fields.
func TestFloatAuxIterator(t *testing.T) {
	itr := influxql.NewAuxIterator(
		&FloatIterator{Points: []influxql.FloatPoint{
			{Time: 0, Value: 1, Aux: []interface{}{float64(100), float64(200)}},
			{Time: 1, Value: 2, Aux: []interface{}{float64(500), math.NaN()}},
		}},
		influxql.IteratorOptions{Aux: []string{"f0", "f1"}},
	)

	itrs := []influxql.Iterator{
		itr,
		itr.Iterator("f0"),
		itr.Iterator("f1"),
		itr.Iterator("f0"),
	}

	if a := Iterators(itrs).ReadAll(); !deep.Equal(a, [][]influxql.Point{
		{
			&influxql.FloatPoint{Time: 0, Value: 1, Aux: []interface{}{float64(100), float64(200)}},
			&influxql.FloatPoint{Time: 0, Value: float64(100)},
			&influxql.FloatPoint{Time: 0, Value: float64(200)},
			&influxql.FloatPoint{Time: 0, Value: float64(100)},
		},
		{
			&influxql.FloatPoint{Time: 1, Value: 2, Aux: []interface{}{float64(500), math.NaN()}},
			&influxql.FloatPoint{Time: 1, Value: float64(500)},
			&influxql.FloatPoint{Time: 1, Value: math.NaN()},
			&influxql.FloatPoint{Time: 1, Value: float64(500)},
		},
	}) {
		t.Fatalf("unexpected points: %s", spew.Sdump(a))
	}
}

func BenchmarkJoin(b *testing.B) {
	// Generate inputs.
	rand := rand.New(rand.NewSource(0))
	inputs := make([]influxql.Iterator, 10)
	for i := range inputs {
		inputs[i] = GenerateFloatIterator(rand, b.N/len(inputs))
	}
	b.ResetTimer()
	b.ReportAllocs()

	// Join inputs together and continuously read all outputs until a nil is returned.
	outputs := influxql.Join(inputs)
	for {
		var done bool
		for _, output := range outputs {
			if v := output.(influxql.FloatIterator).Next(); v == nil {
				done = true
			}
		}

		if done {
			break
		}
	}
}

// Iterators is a test wrapper for iterators.
type Iterators []influxql.Iterator

// Next returns the next value from each iterator.
// Returns nil if any iterator returns a nil.
func (itrs Iterators) Next() []influxql.Point {
	a := make([]influxql.Point, len(itrs))
	for i, itr := range itrs {
		switch itr := itr.(type) {
		case influxql.FloatIterator:
			fp := itr.Next()
			if fp == nil {
				return nil
			}
			a[i] = fp
		default:
			panic(fmt.Sprintf("iterator type not supported: %T", itr))
		}
	}
	return a
}

// ReadAll reads all points from all iterators.
func (itrs Iterators) ReadAll() [][]influxql.Point {
	var a [][]influxql.Point

	// Read from every iterator until a nil is encountered.
	for {
		points := itrs.Next()
		if points == nil {
			break
		}
		a = append(a, points)
	}

	// Close all iterators.
	influxql.Iterators(itrs).Close()

	return a
}

// IteratorCreator is a mockable implementation of SelectStatementExecutor.IteratorCreator.
type IteratorCreator struct {
	CreateIteratorFn  func(opt influxql.IteratorOptions) (influxql.Iterator, error)
	FieldDimensionsFn func(sources influxql.Sources) (fields, dimensions map[string]struct{}, err error)
}

func (ic *IteratorCreator) CreateIterator(opt influxql.IteratorOptions) (influxql.Iterator, error) {
	return ic.CreateIteratorFn(opt)
}

func (ic *IteratorCreator) FieldDimensions(sources influxql.Sources) (fields, dimensions map[string]struct{}, err error) {
	return ic.FieldDimensionsFn(sources)
}

// Test implementation of influxql.FloatIterator.
type FloatIterator struct {
	Points []influxql.FloatPoint
}

// Close is a no-op.
func (itr *FloatIterator) Close() error { return nil }

// Next returns the next value and shifts it off the beginning of the points slice.
func (itr *FloatIterator) Next() *influxql.FloatPoint {
	if len(itr.Points) == 0 {
		return nil
	}

	v := &itr.Points[0]
	itr.Points = itr.Points[1:]
	return v
}

// GenerateFloatIterator creates a FloatIterator with random data.
func GenerateFloatIterator(rand *rand.Rand, valueN int) *FloatIterator {
	const interval = 10 * time.Second

	itr := &FloatIterator{
		Points: make([]influxql.FloatPoint, valueN),
	}

	for i := 0; i < valueN; i++ {
		// Generate incrementing timestamp with some jitter (1s).
		jitter := (rand.Int63n(2) * int64(time.Second))
		timestamp := int64(i)*int64(10*time.Second) + jitter

		itr.Points[i] = influxql.FloatPoint{
			Time:  timestamp,
			Value: rand.Float64(),
		}
	}

	return itr
}
