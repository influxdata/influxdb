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

// Ensure that a set of iterators can be merged together, sorted by window and name/tag.
func TestMergeIterator_Float(t *testing.T) {
	inputs := []*FloatIterator{
		{Points: []influxql.FloatPoint{
			{Name: "cpu", Tags: ParseTags("host=A"), Time: 0, Value: 1},
			{Name: "cpu", Tags: ParseTags("host=B"), Time: 1, Value: 2},
			{Name: "mem", Tags: ParseTags("host=B"), Time: 11, Value: 8},
			{Name: "cpu", Tags: ParseTags("host=A"), Time: 12, Value: 3},
			{Name: "cpu", Tags: ParseTags("host=A"), Time: 30, Value: 4},
		}},
		{Points: []influxql.FloatPoint{
			{Name: "cpu", Tags: ParseTags("host=B"), Time: 11, Value: 5},
			{Name: "cpu", Tags: ParseTags("host=B"), Time: 13, Value: 6},
			{Name: "cpu", Tags: ParseTags("host=A"), Time: 20, Value: 7},
			{Name: "mem", Tags: ParseTags("host=A"), Time: 25, Value: 9},
		}},
		{Points: []influxql.FloatPoint{}},
		{Points: []influxql.FloatPoint{}},
	}

	itr := influxql.NewMergeIterator(FloatIterators(inputs), influxql.IteratorOptions{
		Interval: influxql.Interval{
			Duration: 10 * time.Nanosecond,
		},
		Ascending: true,
	})
	if a, ok := CompareFloatIterator(itr, []influxql.FloatPoint{
		{Name: "cpu", Tags: ParseTags("host=A"), Time: 0, Value: 1},
		{Name: "cpu", Tags: ParseTags("host=B"), Time: 1, Value: 2},
		{Name: "cpu", Tags: ParseTags("host=B"), Time: 11, Value: 5},
		{Name: "cpu", Tags: ParseTags("host=B"), Time: 13, Value: 6},
		{Name: "mem", Tags: ParseTags("host=B"), Time: 11, Value: 8},
		{Name: "cpu", Tags: ParseTags("host=A"), Time: 12, Value: 3},
		{Name: "cpu", Tags: ParseTags("host=A"), Time: 20, Value: 7},
		{Name: "mem", Tags: ParseTags("host=A"), Time: 25, Value: 9},
		{Name: "cpu", Tags: ParseTags("host=A"), Time: 30, Value: 4},
	}); !ok {
		t.Errorf("unexpected points: %s", spew.Sdump(a))
	}

	for i, input := range inputs {
		if !input.Closed {
			t.Errorf("iterator %d not closed", i)
		}
	}
}

// Ensure that a set of iterators can be merged together, sorted by window and name/tag.
func TestMergeIterator_Integer(t *testing.T) {
	inputs := []*IntegerIterator{
		{Points: []influxql.IntegerPoint{
			{Name: "cpu", Tags: ParseTags("host=A"), Time: 0, Value: 1},
			{Name: "cpu", Tags: ParseTags("host=B"), Time: 1, Value: 2},
			{Name: "mem", Tags: ParseTags("host=B"), Time: 11, Value: 8},
			{Name: "cpu", Tags: ParseTags("host=A"), Time: 12, Value: 3},
			{Name: "cpu", Tags: ParseTags("host=A"), Time: 30, Value: 4},
		}},
		{Points: []influxql.IntegerPoint{
			{Name: "cpu", Tags: ParseTags("host=B"), Time: 11, Value: 5},
			{Name: "cpu", Tags: ParseTags("host=B"), Time: 13, Value: 6},
			{Name: "cpu", Tags: ParseTags("host=A"), Time: 20, Value: 7},
			{Name: "mem", Tags: ParseTags("host=A"), Time: 25, Value: 9},
		}},
		{Points: []influxql.IntegerPoint{}},
	}
	itr := influxql.NewMergeIterator(IntegerIterators(inputs), influxql.IteratorOptions{
		Interval: influxql.Interval{
			Duration: 10 * time.Nanosecond,
		},
		Ascending: true,
	})

	if a, ok := CompareIntegerIterator(itr, []influxql.IntegerPoint{
		{Name: "cpu", Tags: ParseTags("host=A"), Time: 0, Value: 1},
		{Name: "cpu", Tags: ParseTags("host=B"), Time: 1, Value: 2},
		{Name: "cpu", Tags: ParseTags("host=B"), Time: 11, Value: 5},
		{Name: "cpu", Tags: ParseTags("host=B"), Time: 13, Value: 6},
		{Name: "mem", Tags: ParseTags("host=B"), Time: 11, Value: 8},
		{Name: "cpu", Tags: ParseTags("host=A"), Time: 12, Value: 3},
		{Name: "cpu", Tags: ParseTags("host=A"), Time: 20, Value: 7},
		{Name: "mem", Tags: ParseTags("host=A"), Time: 25, Value: 9},
		{Name: "cpu", Tags: ParseTags("host=A"), Time: 30, Value: 4},
	}); !ok {
		t.Errorf("unexpected points: %s", spew.Sdump(a))
	}

	for i, input := range inputs {
		if !input.Closed {
			t.Errorf("iterator %d not closed", i)
		}
	}
}

// Ensure that a set of iterators can be merged together, sorted by window and name/tag.
func TestMergeIterator_String(t *testing.T) {
	inputs := []*StringIterator{
		{Points: []influxql.StringPoint{
			{Name: "cpu", Tags: ParseTags("host=A"), Time: 0, Value: "a"},
			{Name: "cpu", Tags: ParseTags("host=B"), Time: 1, Value: "b"},
			{Name: "mem", Tags: ParseTags("host=B"), Time: 11, Value: "h"},
			{Name: "cpu", Tags: ParseTags("host=A"), Time: 12, Value: "c"},
			{Name: "cpu", Tags: ParseTags("host=A"), Time: 30, Value: "d"},
		}},
		{Points: []influxql.StringPoint{
			{Name: "cpu", Tags: ParseTags("host=B"), Time: 11, Value: "e"},
			{Name: "cpu", Tags: ParseTags("host=B"), Time: 13, Value: "f"},
			{Name: "cpu", Tags: ParseTags("host=A"), Time: 20, Value: "g"},
			{Name: "mem", Tags: ParseTags("host=A"), Time: 25, Value: "i"},
		}},
		{Points: []influxql.StringPoint{}},
	}
	itr := influxql.NewMergeIterator(StringIterators(inputs), influxql.IteratorOptions{
		Interval: influxql.Interval{
			Duration: 10 * time.Nanosecond,
		},
		Ascending: true,
	})
	if a, ok := CompareStringIterator(itr, []influxql.StringPoint{
		{Name: "cpu", Tags: ParseTags("host=A"), Time: 0, Value: "a"},
		{Name: "cpu", Tags: ParseTags("host=B"), Time: 1, Value: "b"},
		{Name: "cpu", Tags: ParseTags("host=B"), Time: 11, Value: "e"},
		{Name: "cpu", Tags: ParseTags("host=B"), Time: 13, Value: "f"},
		{Name: "mem", Tags: ParseTags("host=B"), Time: 11, Value: "h"},
		{Name: "cpu", Tags: ParseTags("host=A"), Time: 12, Value: "c"},
		{Name: "cpu", Tags: ParseTags("host=A"), Time: 20, Value: "g"},
		{Name: "mem", Tags: ParseTags("host=A"), Time: 25, Value: "i"},
		{Name: "cpu", Tags: ParseTags("host=A"), Time: 30, Value: "d"},
	}); !ok {
		t.Errorf("unexpected points: %s", spew.Sdump(a))
	}

	for i, input := range inputs {
		if !input.Closed {
			t.Errorf("iterator %d not closed", i)
		}
	}
}

// Ensure that a set of iterators can be merged together, sorted by window and name/tag.
func TestMergeIterator_Boolean(t *testing.T) {
	inputs := []*BooleanIterator{
		{Points: []influxql.BooleanPoint{
			{Name: "cpu", Tags: ParseTags("host=A"), Time: 0, Value: true},
			{Name: "cpu", Tags: ParseTags("host=B"), Time: 1, Value: false},
			{Name: "mem", Tags: ParseTags("host=B"), Time: 11, Value: true},
			{Name: "cpu", Tags: ParseTags("host=A"), Time: 12, Value: true},
			{Name: "cpu", Tags: ParseTags("host=A"), Time: 30, Value: false},
		}},
		{Points: []influxql.BooleanPoint{
			{Name: "cpu", Tags: ParseTags("host=B"), Time: 11, Value: true},
			{Name: "cpu", Tags: ParseTags("host=B"), Time: 13, Value: false},
			{Name: "cpu", Tags: ParseTags("host=A"), Time: 20, Value: true},
			{Name: "mem", Tags: ParseTags("host=A"), Time: 25, Value: false},
		}},
		{Points: []influxql.BooleanPoint{}},
	}
	itr := influxql.NewMergeIterator(BooleanIterators(inputs), influxql.IteratorOptions{
		Interval: influxql.Interval{
			Duration: 10 * time.Nanosecond,
		},
		Ascending: true,
	})
	if a, ok := CompareBooleanIterator(itr, []influxql.BooleanPoint{
		{Name: "cpu", Tags: ParseTags("host=A"), Time: 0, Value: true},
		{Name: "cpu", Tags: ParseTags("host=B"), Time: 1, Value: false},
		{Name: "cpu", Tags: ParseTags("host=B"), Time: 11, Value: true},
		{Name: "cpu", Tags: ParseTags("host=B"), Time: 13, Value: false},
		{Name: "mem", Tags: ParseTags("host=B"), Time: 11, Value: true},
		{Name: "cpu", Tags: ParseTags("host=A"), Time: 12, Value: true},
		{Name: "cpu", Tags: ParseTags("host=A"), Time: 20, Value: true},
		{Name: "mem", Tags: ParseTags("host=A"), Time: 25, Value: false},
		{Name: "cpu", Tags: ParseTags("host=A"), Time: 30, Value: false},
	}); !ok {
		t.Errorf("unexpected points: %s", spew.Sdump(a))
	}

	for i, input := range inputs {
		if !input.Closed {
			t.Errorf("iterator %d not closed", i)
		}
	}
}

func TestMergeIterator_Nil(t *testing.T) {
	itr := influxql.NewMergeIterator([]influxql.Iterator{nil}, influxql.IteratorOptions{}).(influxql.FloatIterator)
	if p := itr.Next(); p != nil {
		t.Fatalf("unexpected point: %#v", p)
	}
	itr.Close()
}

// Ensure that a set of iterators can be merged together, sorted by name/tag.
func TestSortedMergeIterator_Float(t *testing.T) {
	inputs := []*FloatIterator{
		{Points: []influxql.FloatPoint{
			{Name: "cpu", Tags: ParseTags("host=A"), Time: 0, Value: 1},
			{Name: "cpu", Tags: ParseTags("host=A"), Time: 12, Value: 3},
			{Name: "cpu", Tags: ParseTags("host=A"), Time: 30, Value: 4},
			{Name: "cpu", Tags: ParseTags("host=B"), Time: 1, Value: 2},
			{Name: "mem", Tags: ParseTags("host=B"), Time: 4, Value: 8},
		}},
		{Points: []influxql.FloatPoint{
			{Name: "cpu", Tags: ParseTags("host=A"), Time: 20, Value: 7},
			{Name: "cpu", Tags: ParseTags("host=B"), Time: 11, Value: 5},
			{Name: "cpu", Tags: ParseTags("host=B"), Time: 13, Value: 6},
			{Name: "mem", Tags: ParseTags("host=A"), Time: 25, Value: 9},
		}},
		{Points: []influxql.FloatPoint{}},
	}
	itr := influxql.NewSortedMergeIterator(FloatIterators(inputs), influxql.IteratorOptions{
		Interval: influxql.Interval{
			Duration: 10 * time.Nanosecond,
		},
		Ascending: true,
	})
	if a, ok := CompareFloatIterator(itr, []influxql.FloatPoint{
		{Name: "cpu", Tags: ParseTags("host=A"), Time: 0, Value: 1},
		{Name: "cpu", Tags: ParseTags("host=A"), Time: 12, Value: 3},
		{Name: "cpu", Tags: ParseTags("host=A"), Time: 20, Value: 7},
		{Name: "cpu", Tags: ParseTags("host=A"), Time: 30, Value: 4},
		{Name: "cpu", Tags: ParseTags("host=B"), Time: 1, Value: 2},
		{Name: "cpu", Tags: ParseTags("host=B"), Time: 11, Value: 5},
		{Name: "cpu", Tags: ParseTags("host=B"), Time: 13, Value: 6},
		{Name: "mem", Tags: ParseTags("host=A"), Time: 25, Value: 9},
		{Name: "mem", Tags: ParseTags("host=B"), Time: 4, Value: 8},
	}); !ok {
		t.Errorf("unexpected points: %s", spew.Sdump(a))
	}

	for i, input := range inputs {
		if !input.Closed {
			t.Errorf("iterator %d not closed", i)
		}
	}
}

// Ensure that a set of iterators can be merged together, sorted by name/tag.
func TestSortedMergeIterator_Integer(t *testing.T) {
	inputs := []*IntegerIterator{
		{Points: []influxql.IntegerPoint{
			{Name: "cpu", Tags: ParseTags("host=A"), Time: 0, Value: 1},
			{Name: "cpu", Tags: ParseTags("host=A"), Time: 12, Value: 3},
			{Name: "cpu", Tags: ParseTags("host=A"), Time: 30, Value: 4},
			{Name: "cpu", Tags: ParseTags("host=B"), Time: 1, Value: 2},
			{Name: "mem", Tags: ParseTags("host=B"), Time: 4, Value: 8},
		}},
		{Points: []influxql.IntegerPoint{
			{Name: "cpu", Tags: ParseTags("host=A"), Time: 20, Value: 7},
			{Name: "cpu", Tags: ParseTags("host=B"), Time: 11, Value: 5},
			{Name: "cpu", Tags: ParseTags("host=B"), Time: 13, Value: 6},
			{Name: "mem", Tags: ParseTags("host=A"), Time: 25, Value: 9},
		}},
		{Points: []influxql.IntegerPoint{}},
	}
	itr := influxql.NewSortedMergeIterator(IntegerIterators(inputs), influxql.IteratorOptions{
		Interval: influxql.Interval{
			Duration: 10 * time.Nanosecond,
		},
		Ascending: true,
	})
	if a, ok := CompareIntegerIterator(itr, []influxql.IntegerPoint{
		{Name: "cpu", Tags: ParseTags("host=A"), Time: 0, Value: 1},
		{Name: "cpu", Tags: ParseTags("host=A"), Time: 12, Value: 3},
		{Name: "cpu", Tags: ParseTags("host=A"), Time: 20, Value: 7},
		{Name: "cpu", Tags: ParseTags("host=A"), Time: 30, Value: 4},
		{Name: "cpu", Tags: ParseTags("host=B"), Time: 1, Value: 2},
		{Name: "cpu", Tags: ParseTags("host=B"), Time: 11, Value: 5},
		{Name: "cpu", Tags: ParseTags("host=B"), Time: 13, Value: 6},
		{Name: "mem", Tags: ParseTags("host=A"), Time: 25, Value: 9},
		{Name: "mem", Tags: ParseTags("host=B"), Time: 4, Value: 8},
	}); !ok {
		t.Errorf("unexpected points: %s", spew.Sdump(a))
	}

	for i, input := range inputs {
		if !input.Closed {
			t.Errorf("iterator %d not closed", i)
		}
	}
}

// Ensure that a set of iterators can be merged together, sorted by name/tag.
func TestSortedMergeIterator_String(t *testing.T) {
	inputs := []*StringIterator{
		{Points: []influxql.StringPoint{
			{Name: "cpu", Tags: ParseTags("host=A"), Time: 0, Value: "a"},
			{Name: "cpu", Tags: ParseTags("host=A"), Time: 12, Value: "c"},
			{Name: "cpu", Tags: ParseTags("host=A"), Time: 30, Value: "d"},
			{Name: "cpu", Tags: ParseTags("host=B"), Time: 1, Value: "b"},
			{Name: "mem", Tags: ParseTags("host=B"), Time: 4, Value: "h"},
		}},
		{Points: []influxql.StringPoint{
			{Name: "cpu", Tags: ParseTags("host=A"), Time: 20, Value: "g"},
			{Name: "cpu", Tags: ParseTags("host=B"), Time: 11, Value: "e"},
			{Name: "cpu", Tags: ParseTags("host=B"), Time: 13, Value: "f"},
			{Name: "mem", Tags: ParseTags("host=A"), Time: 25, Value: "i"},
		}},
		{Points: []influxql.StringPoint{}},
	}
	itr := influxql.NewSortedMergeIterator(StringIterators(inputs), influxql.IteratorOptions{
		Interval: influxql.Interval{
			Duration: 10 * time.Nanosecond,
		},
		Ascending: true,
	})
	if a, ok := CompareStringIterator(itr, []influxql.StringPoint{
		{Name: "cpu", Tags: ParseTags("host=A"), Time: 0, Value: "a"},
		{Name: "cpu", Tags: ParseTags("host=A"), Time: 12, Value: "c"},
		{Name: "cpu", Tags: ParseTags("host=A"), Time: 20, Value: "g"},
		{Name: "cpu", Tags: ParseTags("host=A"), Time: 30, Value: "d"},
		{Name: "cpu", Tags: ParseTags("host=B"), Time: 1, Value: "b"},
		{Name: "cpu", Tags: ParseTags("host=B"), Time: 11, Value: "e"},
		{Name: "cpu", Tags: ParseTags("host=B"), Time: 13, Value: "f"},
		{Name: "mem", Tags: ParseTags("host=A"), Time: 25, Value: "i"},
		{Name: "mem", Tags: ParseTags("host=B"), Time: 4, Value: "h"},
	}); !ok {
		t.Errorf("unexpected points: %s", spew.Sdump(a))
	}

	for i, input := range inputs {
		if !input.Closed {
			t.Errorf("iterator %d not closed", i)
		}
	}
}

// Ensure that a set of iterators can be merged together, sorted by name/tag.
func TestSortedMergeIterator_Boolean(t *testing.T) {
	inputs := []*BooleanIterator{
		{Points: []influxql.BooleanPoint{
			{Name: "cpu", Tags: ParseTags("host=A"), Time: 0, Value: true},
			{Name: "cpu", Tags: ParseTags("host=A"), Time: 12, Value: true},
			{Name: "cpu", Tags: ParseTags("host=A"), Time: 30, Value: false},
			{Name: "cpu", Tags: ParseTags("host=B"), Time: 1, Value: false},
			{Name: "mem", Tags: ParseTags("host=B"), Time: 4, Value: true},
		}},
		{Points: []influxql.BooleanPoint{
			{Name: "cpu", Tags: ParseTags("host=A"), Time: 20, Value: true},
			{Name: "cpu", Tags: ParseTags("host=B"), Time: 11, Value: true},
			{Name: "cpu", Tags: ParseTags("host=B"), Time: 13, Value: false},
			{Name: "mem", Tags: ParseTags("host=A"), Time: 25, Value: true},
		}},
		{Points: []influxql.BooleanPoint{}},
	}
	itr := influxql.NewSortedMergeIterator(BooleanIterators(inputs), influxql.IteratorOptions{
		Interval: influxql.Interval{
			Duration: 10 * time.Nanosecond,
		},
		Ascending: true,
	})
	if a, ok := CompareBooleanIterator(itr, []influxql.BooleanPoint{
		{Name: "cpu", Tags: ParseTags("host=A"), Time: 0, Value: true},
		{Name: "cpu", Tags: ParseTags("host=A"), Time: 12, Value: true},
		{Name: "cpu", Tags: ParseTags("host=A"), Time: 20, Value: true},
		{Name: "cpu", Tags: ParseTags("host=A"), Time: 30, Value: false},
		{Name: "cpu", Tags: ParseTags("host=B"), Time: 1, Value: false},
		{Name: "cpu", Tags: ParseTags("host=B"), Time: 11, Value: true},
		{Name: "cpu", Tags: ParseTags("host=B"), Time: 13, Value: false},
		{Name: "mem", Tags: ParseTags("host=A"), Time: 25, Value: true},
		{Name: "mem", Tags: ParseTags("host=B"), Time: 4, Value: true},
	}); !ok {
		t.Errorf("unexpected points: %s", spew.Sdump(a))
	}

	for i, input := range inputs {
		if !input.Closed {
			t.Errorf("iterator %d not closed", i)
		}
	}
}

func TestSortedMergeIterator_Nil(t *testing.T) {
	itr := influxql.NewSortedMergeIterator([]influxql.Iterator{nil}, influxql.IteratorOptions{}).(influxql.FloatIterator)
	if p := itr.Next(); p != nil {
		t.Fatalf("unexpected point: %#v", p)
	}
	itr.Close()
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

// Test implementation of influxql.FloatIterator
type FloatIterator struct {
	Points []influxql.FloatPoint
	Closed bool
}

// Close is a no-op.
func (itr *FloatIterator) Close() error { itr.Closed = true; return nil }

// Next returns the next value and shifts it off the beginning of the points slice.
func (itr *FloatIterator) Next() *influxql.FloatPoint {
	if len(itr.Points) == 0 || itr.Closed {
		return nil
	}

	v := &itr.Points[0]
	itr.Points = itr.Points[1:]
	return v
}

func FloatIterators(inputs []*FloatIterator) []influxql.Iterator {
	itrs := make([]influxql.Iterator, len(inputs))
	for i := range itrs {
		itrs[i] = influxql.Iterator(inputs[i])
	}
	return itrs
}

func CompareFloatIterator(input influxql.Iterator, expected []influxql.FloatPoint) ([]influxql.FloatPoint, bool) {
	itr := input.(influxql.FloatIterator)
	points := make([]influxql.FloatPoint, 0, len(expected))
	for p := itr.Next(); p != nil; p = itr.Next() {
		points = append(points, *p)
	}
	itr.Close()
	return points, deep.Equal(points, expected)
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

// Test implementation of influxql.IntegerIterator
type IntegerIterator struct {
	Points []influxql.IntegerPoint
	Closed bool
}

// Close is a no-op.
func (itr *IntegerIterator) Close() error { itr.Closed = true; return nil }

// Next returns the next value and shifts it off the beginning of the points slice.
func (itr *IntegerIterator) Next() *influxql.IntegerPoint {
	if len(itr.Points) == 0 || itr.Closed {
		return nil
	}

	v := &itr.Points[0]
	itr.Points = itr.Points[1:]
	return v
}

func IntegerIterators(inputs []*IntegerIterator) []influxql.Iterator {
	itrs := make([]influxql.Iterator, len(inputs))
	for i := range itrs {
		itrs[i] = influxql.Iterator(inputs[i])
	}
	return itrs
}

func CompareIntegerIterator(input influxql.Iterator, expected []influxql.IntegerPoint) ([]influxql.IntegerPoint, bool) {
	itr := input.(influxql.IntegerIterator)
	points := make([]influxql.IntegerPoint, 0, len(expected))
	for p := itr.Next(); p != nil; p = itr.Next() {
		points = append(points, *p)
	}
	itr.Close()
	return points, deep.Equal(points, expected)
}

// Test implementation of influxql.StringIterator
type StringIterator struct {
	Points []influxql.StringPoint
	Closed bool
}

// Close is a no-op.
func (itr *StringIterator) Close() error { itr.Closed = true; return nil }

// Next returns the next value and shifts it off the beginning of the points slice.
func (itr *StringIterator) Next() *influxql.StringPoint {
	if len(itr.Points) == 0 || itr.Closed {
		return nil
	}

	v := &itr.Points[0]
	itr.Points = itr.Points[1:]
	return v
}

func StringIterators(inputs []*StringIterator) []influxql.Iterator {
	itrs := make([]influxql.Iterator, len(inputs))
	for i := range itrs {
		itrs[i] = influxql.Iterator(inputs[i])
	}
	return itrs
}

func CompareStringIterator(input influxql.Iterator, expected []influxql.StringPoint) ([]influxql.StringPoint, bool) {
	itr := input.(influxql.StringIterator)
	points := make([]influxql.StringPoint, 0, len(expected))
	for p := itr.Next(); p != nil; p = itr.Next() {
		points = append(points, *p)
	}
	itr.Close()
	return points, deep.Equal(points, expected)
}

// Test implementation of influxql.BooleanIterator
type BooleanIterator struct {
	Points []influxql.BooleanPoint
	Closed bool
}

// Close is a no-op.
func (itr *BooleanIterator) Close() error { itr.Closed = true; return nil }

// Next returns the next value and shifts it off the beginning of the points slice.
func (itr *BooleanIterator) Next() *influxql.BooleanPoint {
	if len(itr.Points) == 0 || itr.Closed {
		return nil
	}

	v := &itr.Points[0]
	itr.Points = itr.Points[1:]
	return v
}

func BooleanIterators(inputs []*BooleanIterator) []influxql.Iterator {
	itrs := make([]influxql.Iterator, len(inputs))
	for i := range itrs {
		itrs[i] = influxql.Iterator(inputs[i])
	}
	return itrs
}

func CompareBooleanIterator(input influxql.Iterator, expected []influxql.BooleanPoint) ([]influxql.BooleanPoint, bool) {
	itr := input.(influxql.BooleanIterator)
	points := make([]influxql.BooleanPoint, 0, len(expected))
	for p := itr.Next(); p != nil; p = itr.Next() {
		points = append(points, *p)
	}
	itr.Close()
	return points, deep.Equal(points, expected)
}
