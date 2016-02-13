package influxql_test

import (
	"math/rand"
	"testing"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/influxdb/pkg/deep"
)

// Ensure that a float iterator can be created for a count() call.
func TestCallIterator_Count_Float(t *testing.T) {
	itr := influxql.NewCallIterator(
		&FloatIterator{Points: []influxql.FloatPoint{
			{Name: "cpu", Time: 0, Value: 15, Tags: ParseTags("region=us-east,host=hostA")},
			{Name: "cpu", Time: 1, Value: 11, Tags: ParseTags("region=us-west,host=hostB")},
			{Name: "cpu", Time: 2, Value: 10, Tags: ParseTags("region=us-east,host=hostA")},
			{Name: "cpu", Time: 1, Value: 10, Tags: ParseTags("region=us-west,host=hostA")},

			{Name: "cpu", Time: 5, Value: 20, Tags: ParseTags("region=us-east,host=hostA")},

			{Name: "cpu", Time: 23, Value: 8, Tags: ParseTags("region=us-west,host=hostB")},
			{Name: "mem", Time: 23, Value: 10, Tags: ParseTags("region=us-west,host=hostB")},
		}},
		influxql.IteratorOptions{
			Expr:       MustParseExpr(`count("value")`),
			Dimensions: []string{"host"},
			Interval:   influxql.Interval{Duration: 5 * time.Nanosecond},
		},
	)

	if a := Iterators([]influxql.Iterator{itr}).ReadAll(); !deep.Equal(a, [][]influxql.Point{
		{&influxql.FloatPoint{Name: "cpu", Time: 0, Value: 3, Tags: ParseTags("host=hostA")}},
		{&influxql.FloatPoint{Name: "cpu", Time: 0, Value: 1, Tags: ParseTags("host=hostB")}},
		{&influxql.FloatPoint{Name: "cpu", Time: 5, Value: 1, Tags: ParseTags("host=hostA")}},
		{&influxql.FloatPoint{Name: "cpu", Time: 20, Value: 1, Tags: ParseTags("host=hostB")}},
		{&influxql.FloatPoint{Name: "mem", Time: 20, Value: 1, Tags: ParseTags("host=hostB")}},
	}) {
		t.Fatalf("unexpected points: %s", spew.Sdump(a))
	}
}

// Ensure that an integer iterator can be created for a count() call.
func TestCallIterator_Count_Integer(t *testing.T) {
	itr := influxql.NewCallIterator(
		&IntegerIterator{Points: []influxql.IntegerPoint{
			{Name: "cpu", Time: 0, Value: 15, Tags: ParseTags("region=us-east,host=hostA")},
			{Name: "cpu", Time: 1, Value: 11, Tags: ParseTags("region=us-west,host=hostB")},
			{Name: "cpu", Time: 2, Value: 10, Tags: ParseTags("region=us-east,host=hostA")},
			{Name: "cpu", Time: 1, Value: 10, Tags: ParseTags("region=us-west,host=hostA")},

			{Name: "cpu", Time: 5, Value: 20, Tags: ParseTags("region=us-east,host=hostA")},

			{Name: "cpu", Time: 23, Value: 8, Tags: ParseTags("region=us-west,host=hostB")},
			{Name: "mem", Time: 23, Value: 10, Tags: ParseTags("region=us-west,host=hostB")},
		}},
		influxql.IteratorOptions{
			Expr:       MustParseExpr(`count("value")`),
			Dimensions: []string{"host"},
			Interval:   influxql.Interval{Duration: 5 * time.Nanosecond},
		},
	)

	if a := Iterators([]influxql.Iterator{itr}).ReadAll(); !deep.Equal(a, [][]influxql.Point{
		{&influxql.IntegerPoint{Name: "cpu", Time: 0, Value: 3, Tags: ParseTags("host=hostA")}},
		{&influxql.IntegerPoint{Name: "cpu", Time: 0, Value: 1, Tags: ParseTags("host=hostB")}},
		{&influxql.IntegerPoint{Name: "cpu", Time: 5, Value: 1, Tags: ParseTags("host=hostA")}},
		{&influxql.IntegerPoint{Name: "cpu", Time: 20, Value: 1, Tags: ParseTags("host=hostB")}},
		{&influxql.IntegerPoint{Name: "mem", Time: 20, Value: 1, Tags: ParseTags("host=hostB")}},
	}) {
		t.Fatalf("unexpected points: %s", spew.Sdump(a))
	}
}

// Ensure that a float iterator can be created for a min() call.
func TestCallIterator_Min_Float(t *testing.T) {
	itr := influxql.NewCallIterator(
		&FloatIterator{Points: []influxql.FloatPoint{
			{Time: 0, Value: 15, Tags: ParseTags("region=us-east,host=hostA")},
			{Time: 1, Value: 11, Tags: ParseTags("region=us-west,host=hostB")},
			{Time: 2, Value: 10, Tags: ParseTags("region=us-east,host=hostA")},
			{Time: 4, Value: 12, Tags: ParseTags("region=us-east,host=hostA")},
			{Time: 1, Value: 10, Tags: ParseTags("region=us-west,host=hostA")},

			{Time: 5, Value: 20, Tags: ParseTags("region=us-east,host=hostA")},

			{Time: 23, Value: 8, Tags: ParseTags("region=us-west,host=hostB")},
		}},
		influxql.IteratorOptions{
			Expr:       MustParseExpr(`min("value")`),
			Dimensions: []string{"host"},
			Interval:   influxql.Interval{Duration: 5 * time.Nanosecond},
		},
	)

	if a, ok := CompareFloatIterator(itr, []influxql.FloatPoint{
		{Time: 0, Value: 10, Tags: ParseTags("host=hostA")},
		{Time: 0, Value: 11, Tags: ParseTags("host=hostB")},
		{Time: 5, Value: 20, Tags: ParseTags("host=hostA")},
		{Time: 20, Value: 8, Tags: ParseTags("host=hostB")},
	}); !ok {
		t.Fatalf("unexpected points: %s", spew.Sdump(a))
	}
}

// Ensure that a integer iterator can be created for a min() call.
func TestCallIterator_Min_Integer(t *testing.T) {
	itr := influxql.NewCallIterator(
		&IntegerIterator{Points: []influxql.IntegerPoint{
			{Time: 0, Value: 15, Tags: ParseTags("region=us-east,host=hostA")},
			{Time: 1, Value: 11, Tags: ParseTags("region=us-west,host=hostB")},
			{Time: 2, Value: 10, Tags: ParseTags("region=us-east,host=hostA")},
			{Time: 4, Value: 12, Tags: ParseTags("region=us-east,host=hostA")},
			{Time: 1, Value: 10, Tags: ParseTags("region=us-west,host=hostA")},

			{Time: 5, Value: 20, Tags: ParseTags("region=us-east,host=hostA")},

			{Time: 23, Value: 8, Tags: ParseTags("region=us-west,host=hostB")},
		}},
		influxql.IteratorOptions{
			Expr:       MustParseExpr(`min("value")`),
			Dimensions: []string{"host"},
			Interval:   influxql.Interval{Duration: 5 * time.Nanosecond},
		},
	)

	if a, ok := CompareIntegerIterator(itr, []influxql.IntegerPoint{
		{Time: 0, Value: 10, Tags: ParseTags("host=hostA")},
		{Time: 0, Value: 11, Tags: ParseTags("host=hostB")},
		{Time: 5, Value: 20, Tags: ParseTags("host=hostA")},
		{Time: 20, Value: 8, Tags: ParseTags("host=hostB")},
	}); !ok {
		t.Fatalf("unexpected points: %s", spew.Sdump(a))
	}
}

// Ensure that a float iterator can be created for a max() call.
func TestCallIterator_Max_Float(t *testing.T) {
	itr := influxql.NewCallIterator(
		&FloatIterator{Points: []influxql.FloatPoint{
			{Time: 0, Value: 15, Tags: ParseTags("region=us-east,host=hostA")},
			{Time: 1, Value: 11, Tags: ParseTags("region=us-west,host=hostB")},
			{Time: 2, Value: 10, Tags: ParseTags("region=us-east,host=hostA")},
			{Time: 1, Value: 10, Tags: ParseTags("region=us-west,host=hostA")},

			{Time: 5, Value: 20, Tags: ParseTags("region=us-east,host=hostA")},

			{Time: 23, Value: 8, Tags: ParseTags("region=us-west,host=hostB")},
		}},
		influxql.IteratorOptions{
			Expr:       MustParseExpr(`max("value")`),
			Dimensions: []string{"host"},
			Interval:   influxql.Interval{Duration: 5 * time.Nanosecond},
		},
	)

	if a, ok := CompareFloatIterator(itr, []influxql.FloatPoint{
		{Time: 0, Value: 15, Tags: ParseTags("host=hostA")},
		{Time: 0, Value: 11, Tags: ParseTags("host=hostB")},
		{Time: 5, Value: 20, Tags: ParseTags("host=hostA")},
		{Time: 20, Value: 8, Tags: ParseTags("host=hostB")},
	}); !ok {
		t.Fatalf("unexpected points: %s", spew.Sdump(a))
	}
}

// Ensure that a integer iterator can be created for a max() call.
func TestCallIterator_Max_Integer(t *testing.T) {
	itr := influxql.NewCallIterator(
		&IntegerIterator{Points: []influxql.IntegerPoint{
			{Time: 0, Value: 15, Tags: ParseTags("region=us-east,host=hostA")},
			{Time: 1, Value: 11, Tags: ParseTags("region=us-west,host=hostB")},
			{Time: 2, Value: 10, Tags: ParseTags("region=us-east,host=hostA")},
			{Time: 1, Value: 10, Tags: ParseTags("region=us-west,host=hostA")},

			{Time: 5, Value: 20, Tags: ParseTags("region=us-east,host=hostA")},

			{Time: 23, Value: 8, Tags: ParseTags("region=us-west,host=hostB")},
		}},
		influxql.IteratorOptions{
			Expr:       MustParseExpr(`max("value")`),
			Dimensions: []string{"host"},
			Interval:   influxql.Interval{Duration: 5 * time.Nanosecond},
		},
	)

	if a, ok := CompareIntegerIterator(itr, []influxql.IntegerPoint{
		{Time: 0, Value: 15, Tags: ParseTags("host=hostA")},
		{Time: 0, Value: 11, Tags: ParseTags("host=hostB")},
		{Time: 5, Value: 20, Tags: ParseTags("host=hostA")},
		{Time: 20, Value: 8, Tags: ParseTags("host=hostB")},
	}); !ok {
		t.Fatalf("unexpected points: %s", spew.Sdump(a))
	}
}

// Ensure that a float iterator can be created for a sum() call.
func TestCallIterator_Sum_Float(t *testing.T) {
	itr := influxql.NewCallIterator(
		&FloatIterator{Points: []influxql.FloatPoint{
			{Time: 0, Value: 15, Tags: ParseTags("region=us-east,host=hostA")},
			{Time: 1, Value: 11, Tags: ParseTags("region=us-west,host=hostB")},
			{Time: 2, Value: 10, Tags: ParseTags("region=us-east,host=hostA")},
			{Time: 1, Value: 10, Tags: ParseTags("region=us-west,host=hostA")},

			{Time: 5, Value: 20, Tags: ParseTags("region=us-east,host=hostA")},

			{Time: 23, Value: 8, Tags: ParseTags("region=us-west,host=hostB")},
		}},
		influxql.IteratorOptions{
			Expr:       MustParseExpr(`sum("value")`),
			Dimensions: []string{"host"},
			Interval:   influxql.Interval{Duration: 5 * time.Nanosecond},
		},
	)

	if a, ok := CompareFloatIterator(itr, []influxql.FloatPoint{
		{Time: 0, Value: 35, Tags: ParseTags("host=hostA")},
		{Time: 0, Value: 11, Tags: ParseTags("host=hostB")},
		{Time: 5, Value: 20, Tags: ParseTags("host=hostA")},
		{Time: 20, Value: 8, Tags: ParseTags("host=hostB")},
	}); !ok {
		t.Fatalf("unexpected points: %s", spew.Sdump(a))
	}
}

// Ensure that an integer iterator can be created for a sum() call.
func TestCallIterator_Sum_Integer(t *testing.T) {
	itr := influxql.NewCallIterator(
		&IntegerIterator{Points: []influxql.IntegerPoint{
			{Time: 0, Value: 15, Tags: ParseTags("region=us-east,host=hostA")},
			{Time: 1, Value: 11, Tags: ParseTags("region=us-west,host=hostB")},
			{Time: 2, Value: 10, Tags: ParseTags("region=us-east,host=hostA")},
			{Time: 1, Value: 10, Tags: ParseTags("region=us-west,host=hostA")},

			{Time: 5, Value: 20, Tags: ParseTags("region=us-east,host=hostA")},

			{Time: 23, Value: 8, Tags: ParseTags("region=us-west,host=hostB")},
		}},
		influxql.IteratorOptions{
			Expr:       MustParseExpr(`sum("value")`),
			Dimensions: []string{"host"},
			Interval:   influxql.Interval{Duration: 5 * time.Nanosecond},
		},
	)

	if a, ok := CompareIntegerIterator(itr, []influxql.IntegerPoint{
		{Time: 0, Value: 35, Tags: ParseTags("host=hostA")},
		{Time: 0, Value: 11, Tags: ParseTags("host=hostB")},
		{Time: 5, Value: 20, Tags: ParseTags("host=hostA")},
		{Time: 20, Value: 8, Tags: ParseTags("host=hostB")},
	}); !ok {
		t.Fatalf("unexpected points: %s", spew.Sdump(a))
	}
}

// Ensure that a float iterator can be created for a first() call.
func TestCallIterator_First_Float(t *testing.T) {
	itr := influxql.NewCallIterator(
		&FloatIterator{Points: []influxql.FloatPoint{
			{Time: 1, Value: 11, Tags: ParseTags("region=us-west,host=hostB")},
			{Time: 2, Value: 10, Tags: ParseTags("region=us-east,host=hostA")},
			{Time: 0, Value: 15, Tags: ParseTags("region=us-east,host=hostA")},
			{Time: 1, Value: 10, Tags: ParseTags("region=us-west,host=hostA")},

			{Time: 6, Value: 20, Tags: ParseTags("region=us-east,host=hostA")},

			{Time: 23, Value: 8, Tags: ParseTags("region=us-west,host=hostB")},
		}},
		influxql.IteratorOptions{
			Expr:       MustParseExpr(`first("value")`),
			Dimensions: []string{"host"},
			Interval:   influxql.Interval{Duration: 5 * time.Nanosecond},
		},
	)

	if a, ok := CompareFloatIterator(itr, []influxql.FloatPoint{
		{Time: 0, Value: 15, Tags: ParseTags("host=hostA")},
		{Time: 0, Value: 11, Tags: ParseTags("host=hostB")},
		{Time: 5, Value: 20, Tags: ParseTags("host=hostA")},
		{Time: 20, Value: 8, Tags: ParseTags("host=hostB")},
	}); !ok {
		t.Fatalf("unexpected points: %s", spew.Sdump(a))
	}
}

// Ensure that an integer iterator can be created for a first() call.
func TestCallIterator_First_Integer(t *testing.T) {
	itr := influxql.NewCallIterator(
		&IntegerIterator{Points: []influxql.IntegerPoint{
			{Time: 1, Value: 11, Tags: ParseTags("region=us-west,host=hostB")},
			{Time: 2, Value: 10, Tags: ParseTags("region=us-east,host=hostA")},
			{Time: 0, Value: 15, Tags: ParseTags("region=us-east,host=hostA")},
			{Time: 1, Value: 10, Tags: ParseTags("region=us-west,host=hostA")},

			{Time: 6, Value: 20, Tags: ParseTags("region=us-east,host=hostA")},

			{Time: 23, Value: 8, Tags: ParseTags("region=us-west,host=hostB")},
		}},
		influxql.IteratorOptions{
			Expr:       MustParseExpr(`first("value")`),
			Dimensions: []string{"host"},
			Interval:   influxql.Interval{Duration: 5 * time.Nanosecond},
		},
	)

	if a, ok := CompareIntegerIterator(itr, []influxql.IntegerPoint{
		{Time: 0, Value: 15, Tags: ParseTags("host=hostA")},
		{Time: 0, Value: 11, Tags: ParseTags("host=hostB")},
		{Time: 5, Value: 20, Tags: ParseTags("host=hostA")},
		{Time: 20, Value: 8, Tags: ParseTags("host=hostB")},
	}); !ok {
		t.Fatalf("unexpected points: %s", spew.Sdump(a))
	}
}

// Ensure that a float iterator can be created for a last() call.
func TestCallIterator_Last_Float(t *testing.T) {
	itr := influxql.NewCallIterator(
		&FloatIterator{Points: []influxql.FloatPoint{
			{Time: 1, Value: 11, Tags: ParseTags("region=us-west,host=hostB")},
			{Time: 2, Value: 10, Tags: ParseTags("region=us-east,host=hostA")},
			{Time: 0, Value: 15, Tags: ParseTags("region=us-east,host=hostA")},
			{Time: 1, Value: 10, Tags: ParseTags("region=us-west,host=hostA")},

			{Time: 6, Value: 20, Tags: ParseTags("region=us-east,host=hostA")},

			{Time: 23, Value: 8, Tags: ParseTags("region=us-west,host=hostB")},
		}},
		influxql.IteratorOptions{
			Expr:       MustParseExpr(`last("value")`),
			Dimensions: []string{"host"},
			Interval:   influxql.Interval{Duration: 5 * time.Nanosecond},
		},
	)

	if a, ok := CompareFloatIterator(itr, []influxql.FloatPoint{
		{Time: 0, Value: 10, Tags: ParseTags("host=hostA")},
		{Time: 0, Value: 11, Tags: ParseTags("host=hostB")},
		{Time: 5, Value: 20, Tags: ParseTags("host=hostA")},
		{Time: 20, Value: 8, Tags: ParseTags("host=hostB")},
	}); !ok {
		t.Fatalf("unexpected points: %s", spew.Sdump(a))
	}
}

// Ensure that an integer iterator can be created for a last() call.
func TestCallIterator_Last_Integer(t *testing.T) {
	itr := influxql.NewCallIterator(
		&IntegerIterator{Points: []influxql.IntegerPoint{
			{Time: 1, Value: 11, Tags: ParseTags("region=us-west,host=hostB")},
			{Time: 2, Value: 10, Tags: ParseTags("region=us-east,host=hostA")},
			{Time: 0, Value: 15, Tags: ParseTags("region=us-east,host=hostA")},
			{Time: 1, Value: 10, Tags: ParseTags("region=us-west,host=hostA")},

			{Time: 6, Value: 20, Tags: ParseTags("region=us-east,host=hostA")},

			{Time: 23, Value: 8, Tags: ParseTags("region=us-west,host=hostB")},
		}},
		influxql.IteratorOptions{
			Expr:       MustParseExpr(`last("value")`),
			Dimensions: []string{"host"},
			Interval:   influxql.Interval{Duration: 5 * time.Nanosecond},
		},
	)

	if a, ok := CompareIntegerIterator(itr, []influxql.IntegerPoint{
		{Time: 0, Value: 10, Tags: ParseTags("host=hostA")},
		{Time: 0, Value: 11, Tags: ParseTags("host=hostB")},
		{Time: 5, Value: 20, Tags: ParseTags("host=hostA")},
		{Time: 20, Value: 8, Tags: ParseTags("host=hostB")},
	}); !ok {
		t.Fatalf("unexpected points: %s", spew.Sdump(a))
	}
}

func BenchmarkCallIterator_Min_Float(b *testing.B) {
	input := GenerateFloatIterator(rand.New(rand.NewSource(0)), b.N)
	b.ResetTimer()
	b.ReportAllocs()

	itr := influxql.NewCallIterator(input, influxql.IteratorOptions{
		Expr:     MustParseExpr("min(value)"),
		Interval: influxql.Interval{Duration: 1 * time.Hour},
	}).(influxql.FloatIterator)
	for {
		if p := itr.Next(); p == nil {
			break
		}
	}
}
