package influxql_test

import (
	"math/rand"
	"testing"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/influxdb/influxdb/influxql"
	"github.com/influxdb/influxdb/pkg/deep"
)

// Ensure that an iterator can be created for a count() call.
func TestCallIterator_Count_Float(t *testing.T) {
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
			Expr:       MustParseExpr(`count("value")`),
			Dimensions: []string{"host"},
			Interval:   influxql.Interval{Duration: 5 * time.Nanosecond},
		},
	)

	if a := (Iterators{itr}).ReadAll(); !deep.Equal(a, [][]influxql.Point{
		{&influxql.FloatPoint{Time: 0, Value: 3, Tags: ParseTags("host=hostA")}},
		{&influxql.FloatPoint{Time: 0, Value: 1, Tags: ParseTags("host=hostB")}},
		{&influxql.FloatPoint{Time: 5, Value: 1, Tags: ParseTags("host=hostA")}},
		{&influxql.FloatPoint{Time: 20, Value: 1, Tags: ParseTags("host=hostB")}},
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

	if a := (Iterators{itr}).ReadAll(); !deep.Equal(a, [][]influxql.Point{
		{&influxql.FloatPoint{Time: 0, Value: 10, Tags: ParseTags("host=hostA")}},
		{&influxql.FloatPoint{Time: 0, Value: 11, Tags: ParseTags("host=hostB")}},
		{&influxql.FloatPoint{Time: 5, Value: 20, Tags: ParseTags("host=hostA")}},
		{&influxql.FloatPoint{Time: 20, Value: 8, Tags: ParseTags("host=hostB")}},
	}) {
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

	if a := (Iterators{itr}).ReadAll(); !deep.Equal(a, [][]influxql.Point{
		{&influxql.FloatPoint{Time: 0, Value: 15, Tags: ParseTags("host=hostA")}},
		{&influxql.FloatPoint{Time: 0, Value: 11, Tags: ParseTags("host=hostB")}},
		{&influxql.FloatPoint{Time: 5, Value: 20, Tags: ParseTags("host=hostA")}},
		{&influxql.FloatPoint{Time: 20, Value: 8, Tags: ParseTags("host=hostB")}},
	}) {
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

	if a := (Iterators{itr}).ReadAll(); !deep.Equal(a, [][]influxql.Point{
		{&influxql.FloatPoint{Time: 0, Value: 35, Tags: ParseTags("host=hostA")}},
		{&influxql.FloatPoint{Time: 0, Value: 11, Tags: ParseTags("host=hostB")}},
		{&influxql.FloatPoint{Time: 5, Value: 20, Tags: ParseTags("host=hostA")}},
		{&influxql.FloatPoint{Time: 20, Value: 8, Tags: ParseTags("host=hostB")}},
	}) {
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

	if a := (Iterators{itr}).ReadAll(); !deep.Equal(a, [][]influxql.Point{
		{&influxql.FloatPoint{Time: 0, Value: 15, Tags: ParseTags("host=hostA")}},
		{&influxql.FloatPoint{Time: 1, Value: 11, Tags: ParseTags("host=hostB")}},
		{&influxql.FloatPoint{Time: 6, Value: 20, Tags: ParseTags("host=hostA")}},
		{&influxql.FloatPoint{Time: 23, Value: 8, Tags: ParseTags("host=hostB")}},
	}) {
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

	if a := (Iterators{itr}).ReadAll(); !deep.Equal(a, [][]influxql.Point{
		{&influxql.FloatPoint{Time: 2, Value: 10, Tags: ParseTags("host=hostA")}},
		{&influxql.FloatPoint{Time: 1, Value: 11, Tags: ParseTags("host=hostB")}},
		{&influxql.FloatPoint{Time: 6, Value: 20, Tags: ParseTags("host=hostA")}},
		{&influxql.FloatPoint{Time: 23, Value: 8, Tags: ParseTags("host=hostB")}},
	}) {
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
