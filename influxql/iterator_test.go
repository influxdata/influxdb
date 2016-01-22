package influxql_test

import (
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"testing"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/influxdb/influxdb/influxql"
	"github.com/influxdb/influxdb/pkg/deep"
)

//go:generate tmpl -data=@tmpldata iterator.gen_test.go.tmpl

// Ensure that a set of iterators can be merged together, sorted by window and name/tag.
func TestMergeIterator_Float(t *testing.T) {
	test := TestFloatIterator{
		Inputs: []*FloatIterator{
			{Points: []influxql.FloatPoint{
				{Name: "cpu", Tags: ParseTags("host=A"), Time: 0, Value: 1},
				{Name: "cpu", Tags: ParseTags("host=B"), Time: 1, Value: 2},
				{Name: "cpu", Tags: ParseTags("host=A"), Time: 12, Value: 3},
				{Name: "cpu", Tags: ParseTags("host=A"), Time: 30, Value: 4},
			}},
			{Points: []influxql.FloatPoint{
				{Name: "cpu", Tags: ParseTags("host=B"), Time: 11, Value: 5},
				{Name: "cpu", Tags: ParseTags("host=B"), Time: 13, Value: 6},
				{Name: "cpu", Tags: ParseTags("host=A"), Time: 20, Value: 7},
			}},
			{Points: []influxql.FloatPoint{}},
		},
		IteratorFn: func(itrs []influxql.Iterator) influxql.Iterator {
			return influxql.NewMergeIterator(itrs, influxql.IteratorOptions{
				Interval: influxql.Interval{
					Duration: 10 * time.Nanosecond,
				},
				Ascending: true,
			})
		},
		Points: []influxql.FloatPoint{
			{Name: "cpu", Tags: ParseTags("host=A"), Time: 0, Value: 1},
			{Name: "cpu", Tags: ParseTags("host=B"), Time: 1, Value: 2},
			{Name: "cpu", Tags: ParseTags("host=A"), Time: 12, Value: 3},
			{Name: "cpu", Tags: ParseTags("host=B"), Time: 11, Value: 5},
			{Name: "cpu", Tags: ParseTags("host=B"), Time: 13, Value: 6},
			{Name: "cpu", Tags: ParseTags("host=A"), Time: 20, Value: 7},
			{Name: "cpu", Tags: ParseTags("host=A"), Time: 30, Value: 4},
		},
	}
	test.run(t)
}

// Ensure that a set of iterators can be merged together, sorted by window and name/tag.
func TestMergeIterator_Integer(t *testing.T) {
	test := TestIntegerIterator{
		Inputs: []*IntegerIterator{
			{Points: []influxql.IntegerPoint{
				{Name: "cpu", Tags: ParseTags("host=A"), Time: 0, Value: 1},
				{Name: "cpu", Tags: ParseTags("host=B"), Time: 1, Value: 2},
				{Name: "cpu", Tags: ParseTags("host=A"), Time: 12, Value: 3},
				{Name: "cpu", Tags: ParseTags("host=A"), Time: 30, Value: 4},
			}},
			{Points: []influxql.IntegerPoint{
				{Name: "cpu", Tags: ParseTags("host=B"), Time: 11, Value: 5},
				{Name: "cpu", Tags: ParseTags("host=B"), Time: 13, Value: 6},
				{Name: "cpu", Tags: ParseTags("host=A"), Time: 20, Value: 7},
			}},
			{Points: []influxql.IntegerPoint{}},
		},
		IteratorFn: func(itrs []influxql.Iterator) influxql.Iterator {
			return influxql.NewMergeIterator(itrs, influxql.IteratorOptions{
				Interval: influxql.Interval{
					Duration: 10 * time.Nanosecond,
				},
				Ascending: true,
			})
		},
		Points: []influxql.IntegerPoint{
			{Name: "cpu", Tags: ParseTags("host=A"), Time: 0, Value: 1},
			{Name: "cpu", Tags: ParseTags("host=B"), Time: 1, Value: 2},
			{Name: "cpu", Tags: ParseTags("host=A"), Time: 12, Value: 3},
			{Name: "cpu", Tags: ParseTags("host=B"), Time: 11, Value: 5},
			{Name: "cpu", Tags: ParseTags("host=B"), Time: 13, Value: 6},
			{Name: "cpu", Tags: ParseTags("host=A"), Time: 20, Value: 7},
			{Name: "cpu", Tags: ParseTags("host=A"), Time: 30, Value: 4},
		},
	}
	test.run(t)
}

// Ensure that a set of iterators can be merged together, sorted by window and name/tag.
func TestMergeIterator_String(t *testing.T) {
	test := TestStringIterator{
		Inputs: []*StringIterator{
			{Points: []influxql.StringPoint{
				{Name: "cpu", Tags: ParseTags("host=A"), Time: 0, Value: "a"},
				{Name: "cpu", Tags: ParseTags("host=B"), Time: 1, Value: "b"},
				{Name: "cpu", Tags: ParseTags("host=A"), Time: 12, Value: "c"},
				{Name: "cpu", Tags: ParseTags("host=A"), Time: 30, Value: "d"},
			}},
			{Points: []influxql.StringPoint{
				{Name: "cpu", Tags: ParseTags("host=B"), Time: 11, Value: "e"},
				{Name: "cpu", Tags: ParseTags("host=B"), Time: 13, Value: "f"},
				{Name: "cpu", Tags: ParseTags("host=A"), Time: 20, Value: "g"},
			}},
			{Points: []influxql.StringPoint{}},
		},
		IteratorFn: func(itrs []influxql.Iterator) influxql.Iterator {
			return influxql.NewMergeIterator(itrs, influxql.IteratorOptions{
				Interval: influxql.Interval{
					Duration: 10 * time.Nanosecond,
				},
				Ascending: true,
			})
		},
		Points: []influxql.StringPoint{
			{Name: "cpu", Tags: ParseTags("host=A"), Time: 0, Value: "a"},
			{Name: "cpu", Tags: ParseTags("host=B"), Time: 1, Value: "b"},
			{Name: "cpu", Tags: ParseTags("host=A"), Time: 12, Value: "c"},
			{Name: "cpu", Tags: ParseTags("host=B"), Time: 11, Value: "e"},
			{Name: "cpu", Tags: ParseTags("host=B"), Time: 13, Value: "f"},
			{Name: "cpu", Tags: ParseTags("host=A"), Time: 20, Value: "g"},
			{Name: "cpu", Tags: ParseTags("host=A"), Time: 30, Value: "d"},
		},
	}
	test.run(t)
}

// Ensure that a set of iterators can be merged together, sorted by window and name/tag.
func TestMergeIterator_Boolean(t *testing.T) {
	test := TestBooleanIterator{
		Inputs: []*BooleanIterator{
			{Points: []influxql.BooleanPoint{
				{Name: "cpu", Tags: ParseTags("host=A"), Time: 0, Value: true},
				{Name: "cpu", Tags: ParseTags("host=B"), Time: 1, Value: false},
				{Name: "cpu", Tags: ParseTags("host=A"), Time: 12, Value: true},
				{Name: "cpu", Tags: ParseTags("host=A"), Time: 30, Value: false},
			}},
			{Points: []influxql.BooleanPoint{
				{Name: "cpu", Tags: ParseTags("host=B"), Time: 11, Value: true},
				{Name: "cpu", Tags: ParseTags("host=B"), Time: 13, Value: false},
				{Name: "cpu", Tags: ParseTags("host=A"), Time: 20, Value: true},
			}},
			{Points: []influxql.BooleanPoint{}},
		},
		IteratorFn: func(itrs []influxql.Iterator) influxql.Iterator {
			return influxql.NewMergeIterator(itrs, influxql.IteratorOptions{
				Interval: influxql.Interval{
					Duration: 10 * time.Nanosecond,
				},
				Ascending: true,
			})
		},
		Points: []influxql.BooleanPoint{
			{Name: "cpu", Tags: ParseTags("host=A"), Time: 0, Value: true},
			{Name: "cpu", Tags: ParseTags("host=B"), Time: 1, Value: false},
			{Name: "cpu", Tags: ParseTags("host=A"), Time: 12, Value: true},
			{Name: "cpu", Tags: ParseTags("host=B"), Time: 11, Value: true},
			{Name: "cpu", Tags: ParseTags("host=B"), Time: 13, Value: false},
			{Name: "cpu", Tags: ParseTags("host=A"), Time: 20, Value: true},
			{Name: "cpu", Tags: ParseTags("host=A"), Time: 30, Value: false},
		},
	}
	test.run(t)
}

func TestMergeIterator_Nil(t *testing.T) {
	itr := influxql.NewMergeIterator([]influxql.Iterator{nil}, influxql.IteratorOptions{}).(influxql.FloatIterator)
	if p := itr.Next(); p != nil {
		t.Fatalf("unexpected point: %#v", p)
	}
}

// Ensure that a set of iterators can be merged together, sorted by name/tag.
func TestSortedMergeIterator_Float(t *testing.T) {
	test := TestFloatIterator{
		Inputs: []*FloatIterator{
			{Points: []influxql.FloatPoint{
				{Name: "cpu", Tags: ParseTags("host=A"), Time: 0, Value: 1},
				{Name: "cpu", Tags: ParseTags("host=A"), Time: 12, Value: 3},
				{Name: "cpu", Tags: ParseTags("host=A"), Time: 30, Value: 4},
				{Name: "cpu", Tags: ParseTags("host=B"), Time: 1, Value: 2},
			}},
			{Points: []influxql.FloatPoint{
				{Name: "cpu", Tags: ParseTags("host=A"), Time: 20, Value: 7},
				{Name: "cpu", Tags: ParseTags("host=B"), Time: 11, Value: 5},
				{Name: "cpu", Tags: ParseTags("host=B"), Time: 13, Value: 6},
			}},
			{Points: []influxql.FloatPoint{}},
		},
		IteratorFn: func(itrs []influxql.Iterator) influxql.Iterator {
			return influxql.NewSortedMergeIterator(itrs,
				influxql.IteratorOptions{
					Interval: influxql.Interval{
						Duration: 10 * time.Nanosecond,
					},
					Ascending: true,
				})
		},
		Points: []influxql.FloatPoint{
			{Name: "cpu", Tags: ParseTags("host=A"), Time: 0, Value: 1},
			{Name: "cpu", Tags: ParseTags("host=A"), Time: 12, Value: 3},
			{Name: "cpu", Tags: ParseTags("host=A"), Time: 20, Value: 7},
			{Name: "cpu", Tags: ParseTags("host=A"), Time: 30, Value: 4},
			{Name: "cpu", Tags: ParseTags("host=B"), Time: 1, Value: 2},
			{Name: "cpu", Tags: ParseTags("host=B"), Time: 11, Value: 5},
			{Name: "cpu", Tags: ParseTags("host=B"), Time: 13, Value: 6},
		},
	}
	test.run(t)
}

// Ensure that a set of iterators can be merged together, sorted by name/tag.
func TestSortedMergeIterator_Integer(t *testing.T) {
	test := TestIntegerIterator{
		Inputs: []*IntegerIterator{
			{Points: []influxql.IntegerPoint{
				{Name: "cpu", Tags: ParseTags("host=A"), Time: 0, Value: 1},
				{Name: "cpu", Tags: ParseTags("host=A"), Time: 12, Value: 3},
				{Name: "cpu", Tags: ParseTags("host=A"), Time: 30, Value: 4},
				{Name: "cpu", Tags: ParseTags("host=B"), Time: 1, Value: 2},
			}},
			{Points: []influxql.IntegerPoint{
				{Name: "cpu", Tags: ParseTags("host=A"), Time: 20, Value: 7},
				{Name: "cpu", Tags: ParseTags("host=B"), Time: 11, Value: 5},
				{Name: "cpu", Tags: ParseTags("host=B"), Time: 13, Value: 6},
			}},
			{Points: []influxql.IntegerPoint{}},
		},
		IteratorFn: func(itrs []influxql.Iterator) influxql.Iterator {
			return influxql.NewSortedMergeIterator(itrs, influxql.IteratorOptions{
				Interval: influxql.Interval{
					Duration: 10 * time.Nanosecond,
				},
				Ascending: true,
			})
		},
		Points: []influxql.IntegerPoint{
			{Name: "cpu", Tags: ParseTags("host=A"), Time: 0, Value: 1},
			{Name: "cpu", Tags: ParseTags("host=A"), Time: 12, Value: 3},
			{Name: "cpu", Tags: ParseTags("host=A"), Time: 20, Value: 7},
			{Name: "cpu", Tags: ParseTags("host=A"), Time: 30, Value: 4},
			{Name: "cpu", Tags: ParseTags("host=B"), Time: 1, Value: 2},
			{Name: "cpu", Tags: ParseTags("host=B"), Time: 11, Value: 5},
			{Name: "cpu", Tags: ParseTags("host=B"), Time: 13, Value: 6},
		},
	}
	test.run(t)
}

// Ensure that a set of iterators can be merged together, sorted by name/tag.
func TestSortedMergeIterator_String(t *testing.T) {
	test := TestStringIterator{
		Inputs: []*StringIterator{
			{Points: []influxql.StringPoint{
				{Name: "cpu", Tags: ParseTags("host=A"), Time: 0, Value: "a"},
				{Name: "cpu", Tags: ParseTags("host=A"), Time: 12, Value: "c"},
				{Name: "cpu", Tags: ParseTags("host=A"), Time: 30, Value: "d"},
				{Name: "cpu", Tags: ParseTags("host=B"), Time: 1, Value: "b"},
			}},
			{Points: []influxql.StringPoint{
				{Name: "cpu", Tags: ParseTags("host=A"), Time: 20, Value: "g"},
				{Name: "cpu", Tags: ParseTags("host=B"), Time: 11, Value: "e"},
				{Name: "cpu", Tags: ParseTags("host=B"), Time: 13, Value: "f"},
			}},
			{Points: []influxql.StringPoint{}},
		},
		IteratorFn: func(itrs []influxql.Iterator) influxql.Iterator {
			return influxql.NewSortedMergeIterator(itrs, influxql.IteratorOptions{
				Interval: influxql.Interval{
					Duration: 10 * time.Nanosecond,
				},
				Ascending: true,
			})
		},
		Points: []influxql.StringPoint{
			{Name: "cpu", Tags: ParseTags("host=A"), Time: 0, Value: "a"},
			{Name: "cpu", Tags: ParseTags("host=A"), Time: 12, Value: "c"},
			{Name: "cpu", Tags: ParseTags("host=A"), Time: 20, Value: "g"},
			{Name: "cpu", Tags: ParseTags("host=A"), Time: 30, Value: "d"},
			{Name: "cpu", Tags: ParseTags("host=B"), Time: 1, Value: "b"},
			{Name: "cpu", Tags: ParseTags("host=B"), Time: 11, Value: "e"},
			{Name: "cpu", Tags: ParseTags("host=B"), Time: 13, Value: "f"},
		},
	}
	test.run(t)
}

// Ensure that a set of iterators can be merged together, sorted by name/tag.
func TestSortedMergeIterator_Boolean(t *testing.T) {
	test := TestBooleanIterator{
		Inputs: []*BooleanIterator{
			{Points: []influxql.BooleanPoint{
				{Name: "cpu", Tags: ParseTags("host=A"), Time: 0, Value: true},
				{Name: "cpu", Tags: ParseTags("host=A"), Time: 12, Value: true},
				{Name: "cpu", Tags: ParseTags("host=A"), Time: 30, Value: false},
				{Name: "cpu", Tags: ParseTags("host=B"), Time: 1, Value: false},
			}},
			{Points: []influxql.BooleanPoint{
				{Name: "cpu", Tags: ParseTags("host=A"), Time: 20, Value: true},
				{Name: "cpu", Tags: ParseTags("host=B"), Time: 11, Value: true},
				{Name: "cpu", Tags: ParseTags("host=B"), Time: 13, Value: false},
			}},
			{Points: []influxql.BooleanPoint{}},
		},
		IteratorFn: func(itrs []influxql.Iterator) influxql.Iterator {
			return influxql.NewSortedMergeIterator(itrs, influxql.IteratorOptions{
				Interval: influxql.Interval{
					Duration: 10 * time.Nanosecond,
				},
				Ascending: true,
			})
		},
		Points: []influxql.BooleanPoint{
			{Name: "cpu", Tags: ParseTags("host=A"), Time: 0, Value: true},
			{Name: "cpu", Tags: ParseTags("host=A"), Time: 12, Value: true},
			{Name: "cpu", Tags: ParseTags("host=A"), Time: 20, Value: true},
			{Name: "cpu", Tags: ParseTags("host=A"), Time: 30, Value: false},
			{Name: "cpu", Tags: ParseTags("host=B"), Time: 1, Value: false},
			{Name: "cpu", Tags: ParseTags("host=B"), Time: 11, Value: true},
			{Name: "cpu", Tags: ParseTags("host=B"), Time: 13, Value: false},
		},
	}
	test.run(t)
}

func TestSortedMergeIterator(t *testing.T) {
	itr := influxql.NewSortedMergeIterator([]influxql.Iterator{
		&FloatIterator{Points: []influxql.FloatPoint{
			{Name: "cpu", Tags: ParseTags("host=A"), Time: 0, Value: 1},
			{Name: "cpu", Tags: ParseTags("host=A"), Time: 12, Value: 3},
			{Name: "cpu", Tags: ParseTags("host=A"), Time: 30, Value: 4},
			{Name: "cpu", Tags: ParseTags("host=B"), Time: 1, Value: 2},
		}},
		&FloatIterator{Points: []influxql.FloatPoint{
			{Name: "cpu", Tags: ParseTags("host=A"), Time: 20, Value: 7},
			{Name: "cpu", Tags: ParseTags("host=B"), Time: 11, Value: 5},
			{Name: "cpu", Tags: ParseTags("host=B"), Time: 13, Value: 6},
		}},
	}, influxql.IteratorOptions{
		Interval: influxql.Interval{
			Duration: 10 * time.Nanosecond,
		},
		Ascending: true,
	}).(influxql.FloatIterator)

	if p := itr.Next(); !reflect.DeepEqual(p, &influxql.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 0, Value: 1}) {
		t.Fatalf("unexpected point: %#v", p)
	} else if p = itr.Next(); !reflect.DeepEqual(p, &influxql.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 12, Value: 3}) {
		t.Fatalf("unexpected point: %#v", p)
	}
	if p := itr.Next(); !reflect.DeepEqual(p, &influxql.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 20, Value: 7}) {
		t.Fatalf("unexpected point: %#v", p)
	} else if p = itr.Next(); !reflect.DeepEqual(p, &influxql.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 30, Value: 4}) {
		t.Fatalf("unexpected point: %#v", p)
	} else if p := itr.Next(); !reflect.DeepEqual(p, &influxql.FloatPoint{Name: "cpu", Tags: ParseTags("host=B"), Time: 1, Value: 2}) {
		t.Fatalf("unexpected point: %#v", p)
	}
	if p := itr.Next(); !reflect.DeepEqual(p, &influxql.FloatPoint{Name: "cpu", Tags: ParseTags("host=B"), Time: 11, Value: 5}) {
		t.Fatalf("unexpected point: %#v", p)
	}
	if p := itr.Next(); !reflect.DeepEqual(p, &influxql.FloatPoint{Name: "cpu", Tags: ParseTags("host=B"), Time: 13, Value: 6}) {
		t.Fatalf("unexpected point: %#v", p)
	}
}

func TestSortedMergeIterator_Nil(t *testing.T) {
	itr := influxql.NewSortedMergeIterator([]influxql.Iterator{nil}, influxql.IteratorOptions{}).(influxql.FloatIterator)
	if p := itr.Next(); p != nil {
		t.Fatalf("unexpected point: %#v", p)
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

// Ensure limit iterator returns a subset of points.
func TestLimitIterator(t *testing.T) {
	itr := influxql.NewLimitIterator(
		&FloatIterator{Points: []influxql.FloatPoint{
			{Time: 0, Value: 0},
			{Time: 1, Value: 1},
			{Time: 2, Value: 2},
			{Time: 3, Value: 3},
		}},
		influxql.IteratorOptions{
			Limit:     2,
			Offset:    1,
			StartTime: influxql.MinTime,
			EndTime:   influxql.MaxTime,
		},
	)

	if a := (Iterators{itr}).ReadAll(); !deep.Equal(a, [][]influxql.Point{
		{&influxql.FloatPoint{Time: 1, Value: 1}},
		{&influxql.FloatPoint{Time: 2, Value: 2}},
	}) {
		t.Fatalf("unexpected points: %s", spew.Sdump(a))
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

func TestIteratorOptions_Window_Interval(t *testing.T) {
	opt := influxql.IteratorOptions{
		Interval: influxql.Interval{
			Duration: 10,
		},
	}

	start, end := opt.Window(4)
	if start != 0 {
		t.Errorf("expected start to be 0, got %d", start)
	}
	if end != 10 {
		t.Errorf("expected end to be 10, got %d", end)
	}
}

func TestIteratorOptions_Window_Offset(t *testing.T) {
	opt := influxql.IteratorOptions{
		Interval: influxql.Interval{
			Duration: 10,
			Offset:   8,
		},
	}

	start, end := opt.Window(14)
	if start != 8 {
		t.Errorf("expected start to be 8, got %d", start)
	}
	if end != 18 {
		t.Errorf("expected end to be 18, got %d", end)
	}
}

func TestIteratorOptions_Window_Default(t *testing.T) {
	opt := influxql.IteratorOptions{
		StartTime: 0,
		EndTime:   60,
	}

	start, end := opt.Window(34)
	if start != 0 {
		t.Errorf("expected start to be 0, got %d", start)
	}
	if end != 60 {
		t.Errorf("expected end to be 60, got %d", end)
	}
}

func TestIteratorOptions_SeekTime_Ascending(t *testing.T) {
	opt := influxql.IteratorOptions{
		StartTime: 30,
		EndTime:   60,
		Ascending: true,
	}

	time := opt.SeekTime()
	if time != 30 {
		t.Errorf("expected time to be 30, got %d", time)
	}
}

func TestIteratorOptions_SeekTime_Descending(t *testing.T) {
	opt := influxql.IteratorOptions{
		StartTime: 30,
		EndTime:   60,
		Ascending: false,
	}

	time := opt.SeekTime()
	if time != 60 {
		t.Errorf("expected time to be 60, got %d", time)
	}
}

func TestIteratorOptions_MergeSorted(t *testing.T) {
	opt := influxql.IteratorOptions{}
	sorted := opt.MergeSorted()
	if !sorted {
		t.Error("expected no expression to be sorted, got unsorted")
	}

	opt.Expr = &influxql.VarRef{}
	sorted = opt.MergeSorted()
	if !sorted {
		t.Error("expected expression with varref to be sorted, got unsorted")
	}

	opt.Expr = &influxql.Call{}
	sorted = opt.MergeSorted()
	if sorted {
		t.Error("expected expression without varref to be unsorted, got sorted")
	}
}

func TestIteratorOptions_DerivativeInterval_Default(t *testing.T) {
	opt := influxql.IteratorOptions{}
	expected := influxql.Interval{Duration: time.Second}
	actual := opt.DerivativeInterval()
	if actual != expected {
		t.Errorf("expected derivative interval to be %v, got %v", expected, actual)
	}
}

func TestIteratorOptions_DerivativeInterval_GroupBy(t *testing.T) {
	opt := influxql.IteratorOptions{
		Interval: influxql.Interval{
			Duration: 10,
			Offset:   2,
		},
	}
	expected := influxql.Interval{Duration: 10}
	actual := opt.DerivativeInterval()
	if actual != expected {
		t.Errorf("expected derivative interval to be %v, got %v", expected, actual)
	}
}

func TestIteratorOptions_DerivativeInterval_Call(t *testing.T) {
	opt := influxql.IteratorOptions{
		Expr: &influxql.Call{
			Name: "mean",
			Args: []influxql.Expr{
				&influxql.VarRef{Val: "value"},
				&influxql.DurationLiteral{Val: 2 * time.Second},
			},
		},
		Interval: influxql.Interval{
			Duration: 10,
			Offset:   2,
		},
	}
	expected := influxql.Interval{Duration: 2 * time.Second}
	actual := opt.DerivativeInterval()
	if actual != expected {
		t.Errorf("expected derivative interval to be %v, got %v", expected, actual)
	}
}
