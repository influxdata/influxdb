package influxql_test

import (
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/influxdb/influxdb/influxql"
	"github.com/influxdb/influxdb/pkg/deep"
)

// Second represents a helper for type converting durations.
const Second = int64(time.Second)

// Ensure a SELECT min() query can be executed.
func TestSelect_Min(t *testing.T) {
	var ic IteratorCreator
	ic.CreateIteratorFn = func(opt influxql.IteratorOptions) (influxql.Iterator, error) {
		if !reflect.DeepEqual(opt.Expr, MustParseExpr(`min(value)`)) {
			t.Fatalf("unexpected expr: %s", spew.Sdump(opt.Expr))
		}

		return influxql.NewCallIterator(&FloatIterator{Points: []influxql.FloatPoint{
			{Name: "cpu", Tags: ParseTags("region=west,host=A"), Time: 0 * Second, Value: 20},
			{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 5 * Second, Value: 10},
			{Name: "cpu", Tags: ParseTags("region=east,host=A"), Time: 9 * Second, Value: 19},
			{Name: "cpu", Tags: ParseTags("region=east,host=A"), Time: 10 * Second, Value: 2},
			{Name: "cpu", Tags: ParseTags("region=west,host=A"), Time: 11 * Second, Value: 3},
			{Name: "cpu", Tags: ParseTags("region=west,host=A"), Time: 31 * Second, Value: 100},
		}}, opt), nil
	}

	// Execute selection.
	itrs, err := influxql.Select(MustParseSelectStatement(`SELECT min(value) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY time(10s), host fill(none)`), &ic)
	if err != nil {
		t.Fatal(err)
	} else if a := Iterators(itrs).ReadAll(); !deep.Equal(a, [][]influxql.Point{
		{&influxql.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 0 * Second, Value: 19}},
		{&influxql.FloatPoint{Name: "cpu", Tags: ParseTags("host=B"), Time: 0 * Second, Value: 10}},
		{&influxql.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 10 * Second, Value: 2}},
		{&influxql.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 30 * Second, Value: 100}},
	}) {
		t.Fatalf("unexpected points: %s", spew.Sdump(a))
	}
}

// Ensure a SELECT distinct() query can be executed.
func TestSelect_Distinct(t *testing.T) {
	var ic IteratorCreator
	ic.CreateIteratorFn = func(opt influxql.IteratorOptions) (influxql.Iterator, error) {
		return &FloatIterator{Points: []influxql.FloatPoint{
			{Name: "cpu", Tags: ParseTags("region=west,host=A"), Time: 0 * Second, Value: 20},
			{Name: "cpu", Tags: ParseTags("region=west,host=A"), Time: 1 * Second, Value: 19},
			{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 5 * Second, Value: 10},
			{Name: "cpu", Tags: ParseTags("region=east,host=A"), Time: 9 * Second, Value: 19},
			{Name: "cpu", Tags: ParseTags("region=east,host=A"), Time: 10 * Second, Value: 2},
			{Name: "cpu", Tags: ParseTags("region=east,host=A"), Time: 11 * Second, Value: 2},
			{Name: "cpu", Tags: ParseTags("region=east,host=A"), Time: 12 * Second, Value: 2},
		}}, nil
	}

	// Execute selection.
	itrs, err := influxql.Select(MustParseSelectStatement(`SELECT distinct(value) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY time(10s), host fill(none)`), &ic)
	if err != nil {
		t.Fatal(err)
	} else if a := Iterators(itrs).ReadAll(); !deep.Equal(a, [][]influxql.Point{
		{&influxql.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 0 * Second, Value: 20}},
		{&influxql.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 1 * Second, Value: 19}},
		{&influxql.FloatPoint{Name: "cpu", Tags: ParseTags("host=B"), Time: 5 * Second, Value: 10}},
		{&influxql.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 10 * Second, Value: 2}},
	}) {
		t.Fatalf("unexpected points: %s", spew.Sdump(a))
	}
}

// Ensure a SELECT mean() query can be executed.
func TestSelect_Mean_Float(t *testing.T) {
	var ic IteratorCreator
	ic.CreateIteratorFn = func(opt influxql.IteratorOptions) (influxql.Iterator, error) {
		return &FloatIterator{Points: []influxql.FloatPoint{
			{Name: "cpu", Tags: ParseTags("region=west,host=A"), Time: 0 * Second, Value: 20},
			{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 5 * Second, Value: 10},
			{Name: "cpu", Tags: ParseTags("region=east,host=A"), Time: 9 * Second, Value: 19},
			{Name: "cpu", Tags: ParseTags("region=east,host=A"), Time: 10 * Second, Value: 2},
			{Name: "cpu", Tags: ParseTags("region=west,host=A"), Time: 11 * Second, Value: 3},
			{Name: "cpu", Tags: ParseTags("region=west,host=A"), Time: 31 * Second, Value: 100},

			{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 50 * Second, Value: 1},
			{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 51 * Second, Value: 2},
			{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 52 * Second, Value: 4},
			{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 53 * Second, Value: 4},
			{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 53 * Second, Value: 5},
		}}, nil
	}

	// Execute selection.
	itrs, err := influxql.Select(MustParseSelectStatement(`SELECT mean(value) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY time(10s), host fill(none)`), &ic)
	if err != nil {
		t.Fatal(err)
	} else if a := Iterators(itrs).ReadAll(); !deep.Equal(a, [][]influxql.Point{
		{&influxql.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 0 * Second, Value: 19.5}},
		{&influxql.FloatPoint{Name: "cpu", Tags: ParseTags("host=B"), Time: 0 * Second, Value: 10}},
		{&influxql.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 10 * Second, Value: 2.5}},
		{&influxql.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 30 * Second, Value: 100}},
		{&influxql.FloatPoint{Name: "cpu", Tags: ParseTags("host=B"), Time: 50 * Second, Value: 3.2}},
	}) {
		t.Fatalf("unexpected points: %s", spew.Sdump(a))
	}
}

// Ensure a SELECT mean() query can be executed.
func TestSelect_Mean_Integer(t *testing.T) {
	var ic IteratorCreator
	ic.CreateIteratorFn = func(opt influxql.IteratorOptions) (influxql.Iterator, error) {
		return &IntegerIterator{Points: []influxql.IntegerPoint{
			{Name: "cpu", Tags: ParseTags("region=west,host=A"), Time: 0 * Second, Value: 20},
			{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 5 * Second, Value: 10},
			{Name: "cpu", Tags: ParseTags("region=east,host=A"), Time: 9 * Second, Value: 19},
			{Name: "cpu", Tags: ParseTags("region=east,host=A"), Time: 10 * Second, Value: 2},
			{Name: "cpu", Tags: ParseTags("region=west,host=A"), Time: 11 * Second, Value: 3},
			{Name: "cpu", Tags: ParseTags("region=west,host=A"), Time: 31 * Second, Value: 100},

			{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 50 * Second, Value: 1},
			{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 51 * Second, Value: 2},
			{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 52 * Second, Value: 4},
			{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 53 * Second, Value: 4},
			{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 53 * Second, Value: 5},
		}}, nil
	}

	// Execute selection.
	itrs, err := influxql.Select(MustParseSelectStatement(`SELECT mean(value) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY time(10s), host fill(none)`), &ic)
	if err != nil {
		t.Fatal(err)
	} else if a := Iterators(itrs).ReadAll(); !deep.Equal(a, [][]influxql.Point{
		{&influxql.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 0 * Second, Value: 19.5}},
		{&influxql.FloatPoint{Name: "cpu", Tags: ParseTags("host=B"), Time: 0 * Second, Value: 10}},
		{&influxql.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 10 * Second, Value: 2.5}},
		{&influxql.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 30 * Second, Value: 100}},
		{&influxql.FloatPoint{Name: "cpu", Tags: ParseTags("host=B"), Time: 50 * Second, Value: 3.2}},
	}) {
		t.Fatalf("unexpected points: %s", spew.Sdump(a))
	}
}

// Ensure a SELECT median() query can be executed.
func TestSelect_Median_Float(t *testing.T) {
	var ic IteratorCreator
	ic.CreateIteratorFn = func(opt influxql.IteratorOptions) (influxql.Iterator, error) {
		return &FloatIterator{Points: []influxql.FloatPoint{
			{Name: "cpu", Tags: ParseTags("region=west,host=A"), Time: 0 * Second, Value: 20},
			{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 5 * Second, Value: 10},
			{Name: "cpu", Tags: ParseTags("region=east,host=A"), Time: 9 * Second, Value: 19},
			{Name: "cpu", Tags: ParseTags("region=east,host=A"), Time: 10 * Second, Value: 2},
			{Name: "cpu", Tags: ParseTags("region=west,host=A"), Time: 11 * Second, Value: 3},
			{Name: "cpu", Tags: ParseTags("region=west,host=A"), Time: 31 * Second, Value: 100},

			{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 50 * Second, Value: 1},
			{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 51 * Second, Value: 2},
			{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 52 * Second, Value: 3},
			{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 53 * Second, Value: 4},
			{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 53 * Second, Value: 5},
		}}, nil
	}

	// Execute selection.
	itrs, err := influxql.Select(MustParseSelectStatement(`SELECT median(value) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY time(10s), host fill(none)`), &ic)
	if err != nil {
		t.Fatal(err)
	} else if a := Iterators(itrs).ReadAll(); !deep.Equal(a, [][]influxql.Point{
		{&influxql.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 0 * Second, Value: 19.5}},
		{&influxql.FloatPoint{Name: "cpu", Tags: ParseTags("host=B"), Time: 0 * Second, Value: 10}},
		{&influxql.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 10 * Second, Value: 2.5}},
		{&influxql.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 30 * Second, Value: 100}},
		{&influxql.FloatPoint{Name: "cpu", Tags: ParseTags("host=B"), Time: 50 * Second, Value: 3}},
	}) {
		t.Fatalf("unexpected points: %s", spew.Sdump(a))
	}
}

// Ensure a SELECT median() query can be executed.
func TestSelect_Median_Integer(t *testing.T) {
	var ic IteratorCreator
	ic.CreateIteratorFn = func(opt influxql.IteratorOptions) (influxql.Iterator, error) {
		return &IntegerIterator{Points: []influxql.IntegerPoint{
			{Name: "cpu", Tags: ParseTags("region=west,host=A"), Time: 0 * Second, Value: 20},
			{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 5 * Second, Value: 10},
			{Name: "cpu", Tags: ParseTags("region=east,host=A"), Time: 9 * Second, Value: 19},
			{Name: "cpu", Tags: ParseTags("region=east,host=A"), Time: 10 * Second, Value: 2},
			{Name: "cpu", Tags: ParseTags("region=west,host=A"), Time: 11 * Second, Value: 3},
			{Name: "cpu", Tags: ParseTags("region=west,host=A"), Time: 31 * Second, Value: 100},

			{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 50 * Second, Value: 1},
			{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 51 * Second, Value: 2},
			{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 52 * Second, Value: 3},
			{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 53 * Second, Value: 4},
			{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 53 * Second, Value: 5},
		}}, nil
	}

	// Execute selection.
	itrs, err := influxql.Select(MustParseSelectStatement(`SELECT median(value) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY time(10s), host fill(none)`), &ic)
	if err != nil {
		t.Fatal(err)
	} else if a := Iterators(itrs).ReadAll(); !deep.Equal(a, [][]influxql.Point{
		{&influxql.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 0 * Second, Value: 19.5}},
		{&influxql.FloatPoint{Name: "cpu", Tags: ParseTags("host=B"), Time: 0 * Second, Value: 10}},
		{&influxql.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 10 * Second, Value: 2.5}},
		{&influxql.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 30 * Second, Value: 100}},
		{&influxql.FloatPoint{Name: "cpu", Tags: ParseTags("host=B"), Time: 50 * Second, Value: 3}},
	}) {
		t.Fatalf("expected 1 iterator, got %d", len(itrs))
	}
}

// Ensure a SELECT stddev() query can be executed.
func TestSelect_Stddev(t *testing.T) {
	var ic IteratorCreator
	ic.CreateIteratorFn = func(opt influxql.IteratorOptions) (influxql.Iterator, error) {
		return &FloatIterator{Points: []influxql.FloatPoint{
			{Name: "cpu", Tags: ParseTags("region=west,host=A"), Time: 0 * Second, Value: 20},
			{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 5 * Second, Value: 10},
			{Name: "cpu", Tags: ParseTags("region=east,host=A"), Time: 9 * Second, Value: 19},
			{Name: "cpu", Tags: ParseTags("region=east,host=A"), Time: 10 * Second, Value: 2},
			{Name: "cpu", Tags: ParseTags("region=west,host=A"), Time: 11 * Second, Value: 3},
			{Name: "cpu", Tags: ParseTags("region=west,host=A"), Time: 31 * Second, Value: 100},

			{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 50 * Second, Value: 1},
			{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 51 * Second, Value: 2},
			{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 52 * Second, Value: 3},
			{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 53 * Second, Value: 4},
			{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 53 * Second, Value: 5},
		}}, nil
	}

	// Execute selection.
	itrs, err := influxql.Select(MustParseSelectStatement(`SELECT stddev(value) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY time(10s), host fill(none)`), &ic)
	if err != nil {
		t.Fatal(err)
	} else if a := Iterators(itrs).ReadAll(); !deep.Equal(a, [][]influxql.Point{
		{&influxql.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 0 * Second, Value: 0.7071067811865476}},
		{&influxql.FloatPoint{Name: "cpu", Tags: ParseTags("host=B"), Time: 0 * Second, Value: math.NaN()}},
		{&influxql.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 10 * Second, Value: 0.7071067811865476}},
		{&influxql.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 30 * Second, Value: math.NaN()}},
		{&influxql.FloatPoint{Name: "cpu", Tags: ParseTags("host=B"), Time: 50 * Second, Value: 1.5811388300841898}},
	}) {
		t.Fatalf("unexpected points: %s", spew.Sdump(a))
	}
}

// Ensure a SELECT spread() query can be executed.
func TestSelect_Spread(t *testing.T) {
	var ic IteratorCreator
	ic.CreateIteratorFn = func(opt influxql.IteratorOptions) (influxql.Iterator, error) {
		return &FloatIterator{Points: []influxql.FloatPoint{
			{Name: "cpu", Tags: ParseTags("region=west,host=A"), Time: 0 * Second, Value: 20},
			{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 5 * Second, Value: 10},
			{Name: "cpu", Tags: ParseTags("region=east,host=A"), Time: 9 * Second, Value: 19},
			{Name: "cpu", Tags: ParseTags("region=east,host=A"), Time: 10 * Second, Value: 2},
			{Name: "cpu", Tags: ParseTags("region=west,host=A"), Time: 11 * Second, Value: 3},
			{Name: "cpu", Tags: ParseTags("region=west,host=A"), Time: 31 * Second, Value: 100},

			{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 50 * Second, Value: 1},
			{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 51 * Second, Value: 2},
			{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 52 * Second, Value: 3},
			{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 53 * Second, Value: 4},
			{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 53 * Second, Value: 5},
		}}, nil
	}

	// Execute selection.
	itrs, err := influxql.Select(MustParseSelectStatement(`SELECT spread(value) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY time(10s), host fill(none)`), &ic)
	if err != nil {
		t.Fatal(err)
	} else if a := Iterators(itrs).ReadAll(); !deep.Equal(a, [][]influxql.Point{
		{&influxql.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 0 * Second, Value: 1}},
		{&influxql.FloatPoint{Name: "cpu", Tags: ParseTags("host=B"), Time: 0 * Second, Value: 0}},
		{&influxql.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 10 * Second, Value: 1}},
		{&influxql.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 30 * Second, Value: 0}},
		{&influxql.FloatPoint{Name: "cpu", Tags: ParseTags("host=B"), Time: 50 * Second, Value: 4}},
	}) {
		t.Fatalf("unexpected points: %s", spew.Sdump(a))
	}
}

// Ensure a SELECT percentile() query can be executed.
func TestSelect_Percentile(t *testing.T) {
	var ic IteratorCreator
	ic.CreateIteratorFn = func(opt influxql.IteratorOptions) (influxql.Iterator, error) {
		return &FloatIterator{Points: []influxql.FloatPoint{
			{Name: "cpu", Tags: ParseTags("region=west,host=A"), Time: 0 * Second, Value: 20},
			{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 5 * Second, Value: 10},
			{Name: "cpu", Tags: ParseTags("region=east,host=A"), Time: 9 * Second, Value: 19},
			{Name: "cpu", Tags: ParseTags("region=east,host=A"), Time: 10 * Second, Value: 2},
			{Name: "cpu", Tags: ParseTags("region=west,host=A"), Time: 11 * Second, Value: 3},
			{Name: "cpu", Tags: ParseTags("region=west,host=A"), Time: 31 * Second, Value: 100},

			{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 50 * Second, Value: 10},
			{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 51 * Second, Value: 9},
			{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 52 * Second, Value: 8},
			{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 53 * Second, Value: 7},
			{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 54 * Second, Value: 6},
			{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 55 * Second, Value: 5},
			{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 56 * Second, Value: 4},
			{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 57 * Second, Value: 3},
			{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 58 * Second, Value: 2},
			{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 59 * Second, Value: 1},
		}}, nil
	}

	// Execute selection.
	itrs, err := influxql.Select(MustParseSelectStatement(`SELECT percentile(value, 90) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY time(10s), host fill(none)`), &ic)
	if err != nil {
		t.Fatal(err)
	} else if a := Iterators(itrs).ReadAll(); !deep.Equal(a, [][]influxql.Point{
		{&influxql.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 0 * Second, Value: 20}},
		{&influxql.FloatPoint{Name: "cpu", Tags: ParseTags("host=B"), Time: 0 * Second, Value: 10}},
		{&influxql.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 10 * Second, Value: 3}},
		{&influxql.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 30 * Second, Value: 100}},
		{&influxql.FloatPoint{Name: "cpu", Tags: ParseTags("host=B"), Time: 50 * Second, Value: 9}},
	}) {
		t.Fatalf("unexpected points: %s", spew.Sdump(a))
	}
}

// Ensure a simple raw SELECT statement can be executed.
func TestSelect_Raw(t *testing.T) {
	// Mock two iterators -- one for each value in the query.
	var ic IteratorCreator
	ic.CreateIteratorFn = func(opt influxql.IteratorOptions) (influxql.Iterator, error) {
		if !reflect.DeepEqual(opt.Aux, []string{"v1", "v2"}) {
			t.Fatalf("unexpected options: %s", spew.Sdump(opt.Expr))

		}
		return &FloatIterator{Points: []influxql.FloatPoint{
			{Time: 0, Aux: []interface{}{float64(1), math.NaN()}},
			{Time: 1, Aux: []interface{}{math.NaN(), float64(2)}},
			{Time: 5, Aux: []interface{}{float64(3), float64(4)}},
		}}, nil
	}

	// Execute selection.
	itrs, err := influxql.Select(MustParseSelectStatement(`SELECT v1, v2 FROM cpu`), &ic)
	if err != nil {
		t.Fatal(err)
	} else if a := Iterators(itrs).ReadAll(); !deep.Equal(a, [][]influxql.Point{
		{
			&influxql.FloatPoint{Time: 0, Value: 1},
			&influxql.FloatPoint{Time: 0, Value: math.NaN()},
		},
		{
			&influxql.FloatPoint{Time: 1, Value: math.NaN()},
			&influxql.FloatPoint{Time: 1, Value: 2},
		},
		{
			&influxql.FloatPoint{Time: 5, Value: 3},
			&influxql.FloatPoint{Time: 5, Value: 4},
		},
	}) {
		t.Fatalf("unexpected points: %s", spew.Sdump(a))
	}
}

// Ensure a SELECT binary expr add query can be executed.
func TestSelect_BinaryExpr_Add_RHS_Float(t *testing.T) {
	var ic IteratorCreator
	ic.CreateIteratorFn = func(opt influxql.IteratorOptions) (influxql.Iterator, error) {
		return &FloatIterator{Points: []influxql.FloatPoint{
			{Name: "cpu", Time: 0 * Second, Value: 20, Aux: []interface{}{float64(20)}},
			{Name: "cpu", Time: 5 * Second, Value: 10, Aux: []interface{}{float64(10)}},
			{Name: "cpu", Time: 9 * Second, Value: 19, Aux: []interface{}{float64(19)}},
		}}, nil
	}

	// Execute selection.
	itrs, err := influxql.Select(MustParseSelectStatement(`SELECT value + 2 FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z'`), &ic)
	if err != nil {
		t.Fatal(err)
	} else if a := Iterators(itrs).ReadAll(); !deep.Equal(a, [][]influxql.Point{
		{&influxql.FloatPoint{Name: "cpu", Time: 0 * Second, Value: 22}},
		{&influxql.FloatPoint{Name: "cpu", Time: 5 * Second, Value: 12}},
		{&influxql.FloatPoint{Name: "cpu", Time: 9 * Second, Value: 21}},
	}) {
		t.Fatalf("unexpected points: %s", spew.Sdump(a))
	}
}

// Ensure a SELECT binary expr add query can be executed.
func TestSelect_BinaryExpr_Add_LHS_Float(t *testing.T) {
	var ic IteratorCreator
	ic.CreateIteratorFn = func(opt influxql.IteratorOptions) (influxql.Iterator, error) {
		return &FloatIterator{Points: []influxql.FloatPoint{
			{Name: "cpu", Time: 0 * Second, Value: 20, Aux: []interface{}{float64(20)}},
			{Name: "cpu", Time: 5 * Second, Value: 10, Aux: []interface{}{float64(10)}},
			{Name: "cpu", Time: 9 * Second, Value: 19, Aux: []interface{}{float64(19)}},
		}}, nil
	}

	// Execute selection.
	itrs, err := influxql.Select(MustParseSelectStatement(`SELECT 2 + value FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z'`), &ic)
	if err != nil {
		t.Fatal(err)
	} else if a := Iterators(itrs).ReadAll(); !deep.Equal(a, [][]influxql.Point{
		{&influxql.FloatPoint{Name: "cpu", Time: 0 * Second, Value: 22}},
		{&influxql.FloatPoint{Name: "cpu", Time: 5 * Second, Value: 12}},
		{&influxql.FloatPoint{Name: "cpu", Time: 9 * Second, Value: 21}},
	}) {
		t.Fatalf("unexpected points: %s", spew.Sdump(a))
	}
}
