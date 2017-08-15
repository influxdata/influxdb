package query_test

import (
	"fmt"
	"math/rand"
	"reflect"
	"testing"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/influxdb/query"
)

// Second represents a helper for type converting durations.
const Second = int64(time.Second)

// Ensure a SELECT min() query can be executed.
func TestSelect_Min(t *testing.T) {
	var ic IteratorCreator
	ic.CreateIteratorFn = func(m *influxql.Measurement, opt query.IteratorOptions) (query.Iterator, error) {
		if m.Name != "cpu" {
			t.Fatalf("unexpected source: %s", m.Name)
		}
		if !reflect.DeepEqual(opt.Expr, MustParseExpr(`min(value)`)) {
			t.Fatalf("unexpected expr: %s", spew.Sdump(opt.Expr))
		}

		input, err := query.Iterators{
			&FloatIterator{Points: []query.FloatPoint{
				{Name: "cpu", Tags: ParseTags("region=west,host=A"), Time: 0 * Second, Value: 20},
				{Name: "cpu", Tags: ParseTags("region=west,host=A"), Time: 11 * Second, Value: 3},
				{Name: "cpu", Tags: ParseTags("region=west,host=A"), Time: 31 * Second, Value: 100},
			}},
			&FloatIterator{Points: []query.FloatPoint{
				{Name: "cpu", Tags: ParseTags("region=east,host=A"), Time: 9 * Second, Value: 19},
				{Name: "cpu", Tags: ParseTags("region=east,host=A"), Time: 10 * Second, Value: 2},
			}},
			&FloatIterator{Points: []query.FloatPoint{
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 5 * Second, Value: 10},
			}},
		}.Merge(opt)
		if err != nil {
			return nil, err
		}
		return query.NewCallIterator(input, opt)
	}

	// Execute selection.
	itrs, err := query.Select(MustParseSelectStatement(`SELECT min(value) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY time(10s), host fill(none)`), &ic, nil)
	if err != nil {
		t.Fatal(err)
	} else if a, err := Iterators(itrs).ReadAll(); err != nil {
		t.Fatalf("unexpected point: %s", err)
	} else if diff := cmp.Diff(a, [][]query.Point{
		{&query.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 0 * Second, Value: 19, Aggregated: 2}},
		{&query.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 10 * Second, Value: 2, Aggregated: 2}},
		{&query.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 30 * Second, Value: 100, Aggregated: 1}},
		{&query.FloatPoint{Name: "cpu", Tags: ParseTags("host=B"), Time: 0 * Second, Value: 10, Aggregated: 1}},
	}); diff != "" {
		t.Fatalf("unexpected points:\n%s", diff)
	}
}

// Ensure a SELECT distinct() query can be executed.
func TestSelect_Distinct_Float(t *testing.T) {
	var ic IteratorCreator
	ic.CreateIteratorFn = func(m *influxql.Measurement, opt query.IteratorOptions) (query.Iterator, error) {
		if m.Name != "cpu" {
			t.Fatalf("unexpected source: %s", m.Name)
		}
		return query.Iterators{
			&FloatIterator{Points: []query.FloatPoint{
				{Name: "cpu", Tags: ParseTags("region=west,host=A"), Time: 0 * Second, Value: 20},
				{Name: "cpu", Tags: ParseTags("region=west,host=A"), Time: 1 * Second, Value: 19},
			}},
			&FloatIterator{Points: []query.FloatPoint{
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 5 * Second, Value: 10},
			}},
			&FloatIterator{Points: []query.FloatPoint{
				{Name: "cpu", Tags: ParseTags("region=east,host=A"), Time: 9 * Second, Value: 19},
				{Name: "cpu", Tags: ParseTags("region=east,host=A"), Time: 10 * Second, Value: 2},
				{Name: "cpu", Tags: ParseTags("region=east,host=A"), Time: 11 * Second, Value: 2},
				{Name: "cpu", Tags: ParseTags("region=east,host=A"), Time: 12 * Second, Value: 2},
			}},
		}.Merge(opt)
	}

	// Execute selection.
	itrs, err := query.Select(MustParseSelectStatement(`SELECT distinct(value) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY time(10s), host fill(none)`), &ic, nil)
	if err != nil {
		t.Fatal(err)
	} else if a, err := Iterators(itrs).ReadAll(); err != nil {
		t.Fatalf("unexpected point: %s", err)
	} else if diff := cmp.Diff(a, [][]query.Point{
		{&query.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 0 * Second, Value: 20}},
		{&query.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 0 * Second, Value: 19}},
		{&query.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 10 * Second, Value: 2}},
		{&query.FloatPoint{Name: "cpu", Tags: ParseTags("host=B"), Time: 0 * Second, Value: 10}},
	}); diff != "" {
		t.Fatalf("unexpected points:\n%s", diff)
	}
}

// Ensure a SELECT distinct() query can be executed.
func TestSelect_Distinct_Integer(t *testing.T) {
	var ic IteratorCreator
	ic.CreateIteratorFn = func(m *influxql.Measurement, opt query.IteratorOptions) (query.Iterator, error) {
		if m.Name != "cpu" {
			t.Fatalf("unexpected source: %s", m.Name)
		}
		return query.Iterators{
			&IntegerIterator{Points: []query.IntegerPoint{
				{Name: "cpu", Tags: ParseTags("region=west,host=A"), Time: 0 * Second, Value: 20},
				{Name: "cpu", Tags: ParseTags("region=west,host=A"), Time: 1 * Second, Value: 19},
			}},
			&IntegerIterator{Points: []query.IntegerPoint{
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 5 * Second, Value: 10},
			}},
			&IntegerIterator{Points: []query.IntegerPoint{
				{Name: "cpu", Tags: ParseTags("region=east,host=A"), Time: 9 * Second, Value: 19},
				{Name: "cpu", Tags: ParseTags("region=east,host=A"), Time: 10 * Second, Value: 2},
				{Name: "cpu", Tags: ParseTags("region=east,host=A"), Time: 11 * Second, Value: 2},
				{Name: "cpu", Tags: ParseTags("region=east,host=A"), Time: 12 * Second, Value: 2},
			}},
		}.Merge(opt)
	}

	// Execute selection.
	itrs, err := query.Select(MustParseSelectStatement(`SELECT distinct(value) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY time(10s), host fill(none)`), &ic, nil)
	if err != nil {
		t.Fatal(err)
	} else if a, err := Iterators(itrs).ReadAll(); err != nil {
		t.Fatalf("unexpected point: %s", err)
	} else if diff := cmp.Diff(a, [][]query.Point{
		{&query.IntegerPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 0 * Second, Value: 20}},
		{&query.IntegerPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 0 * Second, Value: 19}},
		{&query.IntegerPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 10 * Second, Value: 2}},
		{&query.IntegerPoint{Name: "cpu", Tags: ParseTags("host=B"), Time: 0 * Second, Value: 10}},
	}); diff != "" {
		t.Fatalf("unexpected points:\n%s", diff)
	}
}

// Ensure a SELECT distinct() query can be executed.
func TestSelect_Distinct_String(t *testing.T) {
	var ic IteratorCreator
	ic.CreateIteratorFn = func(m *influxql.Measurement, opt query.IteratorOptions) (query.Iterator, error) {
		if m.Name != "cpu" {
			t.Fatalf("unexpected source: %s", m.Name)
		}
		return query.Iterators{
			&StringIterator{Points: []query.StringPoint{
				{Name: "cpu", Tags: ParseTags("region=west,host=A"), Time: 0 * Second, Value: "a"},
				{Name: "cpu", Tags: ParseTags("region=west,host=A"), Time: 1 * Second, Value: "b"},
			}},
			&StringIterator{Points: []query.StringPoint{
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 5 * Second, Value: "c"},
			}},
			&StringIterator{Points: []query.StringPoint{
				{Name: "cpu", Tags: ParseTags("region=east,host=A"), Time: 9 * Second, Value: "b"},
				{Name: "cpu", Tags: ParseTags("region=east,host=A"), Time: 10 * Second, Value: "d"},
				{Name: "cpu", Tags: ParseTags("region=east,host=A"), Time: 11 * Second, Value: "d"},
				{Name: "cpu", Tags: ParseTags("region=east,host=A"), Time: 12 * Second, Value: "d"},
			}},
		}.Merge(opt)
	}

	// Execute selection.
	itrs, err := query.Select(MustParseSelectStatement(`SELECT distinct(value) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY time(10s), host fill(none)`), &ic, nil)
	if err != nil {
		t.Fatal(err)
	} else if a, err := Iterators(itrs).ReadAll(); err != nil {
		t.Fatalf("unexpected point: %s", err)
	} else if diff := cmp.Diff(a, [][]query.Point{
		{&query.StringPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 0 * Second, Value: "a"}},
		{&query.StringPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 0 * Second, Value: "b"}},
		{&query.StringPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 10 * Second, Value: "d"}},
		{&query.StringPoint{Name: "cpu", Tags: ParseTags("host=B"), Time: 0 * Second, Value: "c"}},
	}); diff != "" {
		t.Fatalf("unexpected points:\n%s", diff)
	}
}

// Ensure a SELECT distinct() query can be executed.
func TestSelect_Distinct_Boolean(t *testing.T) {
	var ic IteratorCreator
	ic.CreateIteratorFn = func(m *influxql.Measurement, opt query.IteratorOptions) (query.Iterator, error) {
		if m.Name != "cpu" {
			t.Fatalf("unexpected source: %s", m.Name)
		}
		return query.Iterators{
			&BooleanIterator{Points: []query.BooleanPoint{
				{Name: "cpu", Tags: ParseTags("region=west,host=A"), Time: 0 * Second, Value: true},
				{Name: "cpu", Tags: ParseTags("region=west,host=A"), Time: 1 * Second, Value: false},
			}},
			&BooleanIterator{Points: []query.BooleanPoint{
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 5 * Second, Value: false},
			}},
			&BooleanIterator{Points: []query.BooleanPoint{
				{Name: "cpu", Tags: ParseTags("region=east,host=A"), Time: 9 * Second, Value: true},
				{Name: "cpu", Tags: ParseTags("region=east,host=A"), Time: 10 * Second, Value: false},
				{Name: "cpu", Tags: ParseTags("region=east,host=A"), Time: 11 * Second, Value: false},
				{Name: "cpu", Tags: ParseTags("region=east,host=A"), Time: 12 * Second, Value: true},
			}},
		}.Merge(opt)
	}

	// Execute selection.
	itrs, err := query.Select(MustParseSelectStatement(`SELECT distinct(value) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY time(10s), host fill(none)`), &ic, nil)
	if err != nil {
		t.Fatal(err)
	} else if a, err := Iterators(itrs).ReadAll(); err != nil {
		t.Fatalf("unexpected point: %s", err)
	} else if diff := cmp.Diff(a, [][]query.Point{
		{&query.BooleanPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 0 * Second, Value: true}},
		{&query.BooleanPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 0 * Second, Value: false}},
		{&query.BooleanPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 10 * Second, Value: false}},
		{&query.BooleanPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 10 * Second, Value: true}},
		{&query.BooleanPoint{Name: "cpu", Tags: ParseTags("host=B"), Time: 0 * Second, Value: false}},
	}); diff != "" {
		t.Errorf("unexpected points:\n%s", diff)
	}
}

// Ensure a SELECT mean() query can be executed.
func TestSelect_Mean_Float(t *testing.T) {
	var ic IteratorCreator
	ic.CreateIteratorFn = func(m *influxql.Measurement, opt query.IteratorOptions) (query.Iterator, error) {
		if m.Name != "cpu" {
			t.Fatalf("unexpected source: %s", m.Name)
		}
		input, err := query.Iterators{
			&FloatIterator{Points: []query.FloatPoint{
				{Name: "cpu", Tags: ParseTags("region=west,host=A"), Time: 0 * Second, Value: 20},
				{Name: "cpu", Tags: ParseTags("region=west,host=A"), Time: 11 * Second, Value: 3},
				{Name: "cpu", Tags: ParseTags("region=west,host=A"), Time: 31 * Second, Value: 100},
			}},
			&FloatIterator{Points: []query.FloatPoint{
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 5 * Second, Value: 10},
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 50 * Second, Value: 1},
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 51 * Second, Value: 2},
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 52 * Second, Value: 4},
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 53 * Second, Value: 4},
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 53 * Second, Value: 5},
			}},
			&FloatIterator{Points: []query.FloatPoint{
				{Name: "cpu", Tags: ParseTags("region=east,host=A"), Time: 9 * Second, Value: 19},
				{Name: "cpu", Tags: ParseTags("region=east,host=A"), Time: 10 * Second, Value: 2},
			}},
		}.Merge(opt)
		if err != nil {
			return nil, err
		}
		return query.NewCallIterator(input, opt)
	}

	// Execute selection.
	itrs, err := query.Select(MustParseSelectStatement(`SELECT mean(value) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY time(10s), host fill(none)`), &ic, nil)
	if err != nil {
		t.Fatal(err)
	} else if a, err := Iterators(itrs).ReadAll(); err != nil {
		t.Fatalf("unexpected point: %s", err)
	} else if diff := cmp.Diff(a, [][]query.Point{
		{&query.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 0 * Second, Value: 19.5, Aggregated: 2}},
		{&query.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 10 * Second, Value: 2.5, Aggregated: 2}},
		{&query.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 30 * Second, Value: 100, Aggregated: 1}},
		{&query.FloatPoint{Name: "cpu", Tags: ParseTags("host=B"), Time: 0 * Second, Value: 10, Aggregated: 1}},
		{&query.FloatPoint{Name: "cpu", Tags: ParseTags("host=B"), Time: 50 * Second, Value: 3.2, Aggregated: 5}},
	}); diff != "" {
		t.Fatalf("unexpected points:\n%s", diff)
	}
}

// Ensure a SELECT mean() query can be executed.
func TestSelect_Mean_Integer(t *testing.T) {
	var ic IteratorCreator
	ic.CreateIteratorFn = func(m *influxql.Measurement, opt query.IteratorOptions) (query.Iterator, error) {
		if m.Name != "cpu" {
			t.Fatalf("unexpected source: %s", m.Name)
		}
		input, err := query.Iterators{
			&IntegerIterator{Points: []query.IntegerPoint{
				{Name: "cpu", Tags: ParseTags("region=west,host=A"), Time: 0 * Second, Value: 20},
				{Name: "cpu", Tags: ParseTags("region=west,host=A"), Time: 11 * Second, Value: 3},
				{Name: "cpu", Tags: ParseTags("region=west,host=A"), Time: 31 * Second, Value: 100},
			}},
			&IntegerIterator{Points: []query.IntegerPoint{
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 5 * Second, Value: 10},
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 50 * Second, Value: 1},
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 51 * Second, Value: 2},
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 52 * Second, Value: 4},
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 53 * Second, Value: 4},
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 53 * Second, Value: 5},
			}},
			&IntegerIterator{Points: []query.IntegerPoint{
				{Name: "cpu", Tags: ParseTags("region=east,host=A"), Time: 9 * Second, Value: 19},
				{Name: "cpu", Tags: ParseTags("region=east,host=A"), Time: 10 * Second, Value: 2},
			}},
		}.Merge(opt)
		if err != nil {
			return nil, err
		}
		return query.NewCallIterator(input, opt)
	}

	// Execute selection.
	itrs, err := query.Select(MustParseSelectStatement(`SELECT mean(value) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY time(10s), host fill(none)`), &ic, nil)
	if err != nil {
		t.Fatal(err)
	} else if a, err := Iterators(itrs).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if diff := cmp.Diff(a, [][]query.Point{
		{&query.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 0 * Second, Value: 19.5, Aggregated: 2}},
		{&query.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 10 * Second, Value: 2.5, Aggregated: 2}},
		{&query.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 30 * Second, Value: 100, Aggregated: 1}},
		{&query.FloatPoint{Name: "cpu", Tags: ParseTags("host=B"), Time: 0 * Second, Value: 10, Aggregated: 1}},
		{&query.FloatPoint{Name: "cpu", Tags: ParseTags("host=B"), Time: 50 * Second, Value: 3.2, Aggregated: 5}},
	}); diff != "" {
		t.Fatalf("unexpected points:\n%s", diff)
	}
}

// Ensure a SELECT mean() query cannot be executed on strings.
func TestSelect_Mean_String(t *testing.T) {
	var ic IteratorCreator
	ic.CreateIteratorFn = func(m *influxql.Measurement, opt query.IteratorOptions) (query.Iterator, error) {
		if m.Name != "cpu" {
			t.Fatalf("unexpected source: %s", m.Name)
		}
		return query.NewCallIterator(&StringIterator{}, opt)
	}

	// Execute selection.
	itrs, err := query.Select(MustParseSelectStatement(`SELECT mean(value) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY time(10s), host fill(none)`), &ic, nil)
	if err == nil || err.Error() != "unsupported mean iterator type: *query_test.StringIterator" {
		t.Errorf("unexpected error: %s", err)
	}

	if itrs != nil {
		query.Iterators(itrs).Close()
	}
}

// Ensure a SELECT mean() query cannot be executed on booleans.
func TestSelect_Mean_Boolean(t *testing.T) {
	var ic IteratorCreator
	ic.CreateIteratorFn = func(m *influxql.Measurement, opt query.IteratorOptions) (query.Iterator, error) {
		if m.Name != "cpu" {
			t.Fatalf("unexpected source: %s", m.Name)
		}
		return query.NewCallIterator(&BooleanIterator{}, opt)
	}

	// Execute selection.
	itrs, err := query.Select(MustParseSelectStatement(`SELECT mean(value) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY time(10s), host fill(none)`), &ic, nil)
	if err == nil || err.Error() != "unsupported mean iterator type: *query_test.BooleanIterator" {
		t.Errorf("unexpected error: %s", err)
	}

	if itrs != nil {
		query.Iterators(itrs).Close()
	}
}

// Ensure a SELECT median() query can be executed.
func TestSelect_Median_Float(t *testing.T) {
	var ic IteratorCreator
	ic.CreateIteratorFn = func(m *influxql.Measurement, opt query.IteratorOptions) (query.Iterator, error) {
		if m.Name != "cpu" {
			t.Fatalf("unexpected source: %s", m.Name)
		}
		return query.Iterators{
			&FloatIterator{Points: []query.FloatPoint{
				{Name: "cpu", Tags: ParseTags("region=west,host=A"), Time: 0 * Second, Value: 20},
				{Name: "cpu", Tags: ParseTags("region=west,host=A"), Time: 11 * Second, Value: 3},
				{Name: "cpu", Tags: ParseTags("region=west,host=A"), Time: 31 * Second, Value: 100},
			}},
			&FloatIterator{Points: []query.FloatPoint{
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 5 * Second, Value: 10},
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 50 * Second, Value: 1},
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 51 * Second, Value: 2},
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 52 * Second, Value: 3},
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 53 * Second, Value: 4},
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 53 * Second, Value: 5},
			}},
			&FloatIterator{Points: []query.FloatPoint{
				{Name: "cpu", Tags: ParseTags("region=east,host=A"), Time: 9 * Second, Value: 19},
				{Name: "cpu", Tags: ParseTags("region=east,host=A"), Time: 10 * Second, Value: 2},
			}},
		}.Merge(opt)
	}

	// Execute selection.
	itrs, err := query.Select(MustParseSelectStatement(`SELECT median(value) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY time(10s), host fill(none)`), &ic, nil)
	if err != nil {
		t.Fatal(err)
	} else if a, err := Iterators(itrs).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if diff := cmp.Diff(a, [][]query.Point{
		{&query.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 0 * Second, Value: 19.5}},
		{&query.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 10 * Second, Value: 2.5}},
		{&query.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 30 * Second, Value: 100}},
		{&query.FloatPoint{Name: "cpu", Tags: ParseTags("host=B"), Time: 0 * Second, Value: 10}},
		{&query.FloatPoint{Name: "cpu", Tags: ParseTags("host=B"), Time: 50 * Second, Value: 3}},
	}); diff != "" {
		t.Fatalf("unexpected points: %s", diff)
	}
}

// Ensure a SELECT median() query can be executed.
func TestSelect_Median_Integer(t *testing.T) {
	var ic IteratorCreator
	ic.CreateIteratorFn = func(m *influxql.Measurement, opt query.IteratorOptions) (query.Iterator, error) {
		if m.Name != "cpu" {
			t.Fatalf("unexpected source: %s", m.Name)
		}
		return query.Iterators{
			&IntegerIterator{Points: []query.IntegerPoint{
				{Name: "cpu", Tags: ParseTags("region=west,host=A"), Time: 0 * Second, Value: 20},
				{Name: "cpu", Tags: ParseTags("region=west,host=A"), Time: 11 * Second, Value: 3},
				{Name: "cpu", Tags: ParseTags("region=west,host=A"), Time: 31 * Second, Value: 100},
			}},
			&IntegerIterator{Points: []query.IntegerPoint{
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 5 * Second, Value: 10},
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 50 * Second, Value: 1},
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 51 * Second, Value: 2},
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 52 * Second, Value: 3},
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 53 * Second, Value: 4},
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 53 * Second, Value: 5},
			}},
			&IntegerIterator{Points: []query.IntegerPoint{
				{Name: "cpu", Tags: ParseTags("region=east,host=A"), Time: 9 * Second, Value: 19},
				{Name: "cpu", Tags: ParseTags("region=east,host=A"), Time: 10 * Second, Value: 2},
			}},
		}.Merge(opt)
	}

	// Execute selection.
	itrs, err := query.Select(MustParseSelectStatement(`SELECT median(value) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY time(10s), host fill(none)`), &ic, nil)
	if err != nil {
		t.Fatal(err)
	} else if a, err := Iterators(itrs).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if diff := cmp.Diff(a, [][]query.Point{
		{&query.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 0 * Second, Value: 19.5}},
		{&query.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 10 * Second, Value: 2.5}},
		{&query.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 30 * Second, Value: 100}},
		{&query.FloatPoint{Name: "cpu", Tags: ParseTags("host=B"), Time: 0 * Second, Value: 10}},
		{&query.FloatPoint{Name: "cpu", Tags: ParseTags("host=B"), Time: 50 * Second, Value: 3}},
	}); diff != "" {
		t.Fatalf("unexpected points: %s", diff)
	}
}

// Ensure a SELECT median() query cannot be executed on strings.
func TestSelect_Median_String(t *testing.T) {
	var ic IteratorCreator
	ic.CreateIteratorFn = func(m *influxql.Measurement, opt query.IteratorOptions) (query.Iterator, error) {
		if m.Name != "cpu" {
			t.Fatalf("unexpected source: %s", m.Name)
		}
		return &StringIterator{}, nil
	}

	// Execute selection.
	itrs, err := query.Select(MustParseSelectStatement(`SELECT median(value) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY time(10s), host fill(none)`), &ic, nil)
	if err == nil || err.Error() != "unsupported median iterator type: *query_test.StringIterator" {
		t.Errorf("unexpected error: %s", err)
	}

	if itrs != nil {
		query.Iterators(itrs).Close()
	}
}

// Ensure a SELECT median() query cannot be executed on booleans.
func TestSelect_Median_Boolean(t *testing.T) {
	var ic IteratorCreator
	ic.CreateIteratorFn = func(m *influxql.Measurement, opt query.IteratorOptions) (query.Iterator, error) {
		if m.Name != "cpu" {
			t.Fatalf("unexpected source: %s", m.Name)
		}
		return &BooleanIterator{}, nil
	}

	// Execute selection.
	itrs, err := query.Select(MustParseSelectStatement(`SELECT median(value) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY time(10s), host fill(none)`), &ic, nil)
	if err == nil || err.Error() != "unsupported median iterator type: *query_test.BooleanIterator" {
		t.Errorf("unexpected error: %s", err)
	}

	if itrs != nil {
		query.Iterators(itrs).Close()
	}
}

// Ensure a SELECT mode() query can be executed.
func TestSelect_Mode_Float(t *testing.T) {
	var ic IteratorCreator
	ic.CreateIteratorFn = func(m *influxql.Measurement, opt query.IteratorOptions) (query.Iterator, error) {
		if m.Name != "cpu" {
			t.Fatalf("unexpected source: %s", m.Name)
		}
		return query.Iterators{
			&FloatIterator{Points: []query.FloatPoint{
				{Name: "cpu", Tags: ParseTags("region=west,host=A"), Time: 0 * Second, Value: 10},
				{Name: "cpu", Tags: ParseTags("region=west,host=A"), Time: 11 * Second, Value: 2},
				{Name: "cpu", Tags: ParseTags("region=west,host=A"), Time: 31 * Second, Value: 100},
			}},
			&FloatIterator{Points: []query.FloatPoint{
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 5 * Second, Value: 10},
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 50 * Second, Value: 1},
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 51 * Second, Value: 2},
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 52 * Second, Value: 3},
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 53 * Second, Value: 4},
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 53 * Second, Value: 5},
			}},
			&FloatIterator{Points: []query.FloatPoint{
				{Name: "cpu", Tags: ParseTags("region=east,host=A"), Time: 9 * Second, Value: 19},
				{Name: "cpu", Tags: ParseTags("region=east,host=A"), Time: 10 * Second, Value: 2},
			}},
		}.Merge(opt)
	}

	// Execute selection.
	itrs, err := query.Select(MustParseSelectStatement(`SELECT mode(value) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY time(10s), host fill(none)`), &ic, nil)
	if err != nil {
		t.Fatal(err)
	} else if a, err := Iterators(itrs).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if diff := cmp.Diff(a, [][]query.Point{
		{&query.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 0 * Second, Value: 10}},
		{&query.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 10 * Second, Value: 2}},
		{&query.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 30 * Second, Value: 100}},
		{&query.FloatPoint{Name: "cpu", Tags: ParseTags("host=B"), Time: 0 * Second, Value: 10}},
		{&query.FloatPoint{Name: "cpu", Tags: ParseTags("host=B"), Time: 50 * Second, Value: 1}},
	}); diff != "" {
		t.Fatalf("unexpected points:\n%s", diff)
	}
}

// Ensure a SELECT mode() query can be executed.
func TestSelect_Mode_Integer(t *testing.T) {
	var ic IteratorCreator
	ic.CreateIteratorFn = func(m *influxql.Measurement, opt query.IteratorOptions) (query.Iterator, error) {
		if m.Name != "cpu" {
			t.Fatalf("unexpected source: %s", m.Name)
		}
		return query.Iterators{
			&IntegerIterator{Points: []query.IntegerPoint{
				{Name: "cpu", Tags: ParseTags("region=west,host=A"), Time: 0 * Second, Value: 10},
				{Name: "cpu", Tags: ParseTags("region=west,host=A"), Time: 11 * Second, Value: 2},
				{Name: "cpu", Tags: ParseTags("region=west,host=A"), Time: 31 * Second, Value: 100},
			}},
			&IntegerIterator{Points: []query.IntegerPoint{
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 5 * Second, Value: 10},
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 50 * Second, Value: 1},
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 51 * Second, Value: 2},
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 52 * Second, Value: 3},
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 53 * Second, Value: 4},
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 54 * Second, Value: 5},
			}},
			&IntegerIterator{Points: []query.IntegerPoint{
				{Name: "cpu", Tags: ParseTags("region=east,host=A"), Time: 9 * Second, Value: 19},
				{Name: "cpu", Tags: ParseTags("region=east,host=A"), Time: 10 * Second, Value: 2},
			}},
		}.Merge(opt)
	}

	// Execute selection.
	itrs, err := query.Select(MustParseSelectStatement(`SELECT mode(value) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY time(10s), host fill(none)`), &ic, nil)
	if err != nil {
		t.Fatal(err)
	} else if a, err := Iterators(itrs).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if diff := cmp.Diff(a, [][]query.Point{
		{&query.IntegerPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 0 * Second, Value: 10}},
		{&query.IntegerPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 10 * Second, Value: 2}},
		{&query.IntegerPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 30 * Second, Value: 100}},
		{&query.IntegerPoint{Name: "cpu", Tags: ParseTags("host=B"), Time: 0 * Second, Value: 10}},
		{&query.IntegerPoint{Name: "cpu", Tags: ParseTags("host=B"), Time: 50 * Second, Value: 1}},
	}); diff != "" {
		t.Fatalf("unexpected points: %s", diff)
	}
}

// Ensure a SELECT mode() query cannot be executed on strings.
func TestSelect_Mode_String(t *testing.T) {
	var ic IteratorCreator
	ic.CreateIteratorFn = func(m *influxql.Measurement, opt query.IteratorOptions) (query.Iterator, error) {
		if m.Name != "cpu" {
			t.Fatalf("unexpected source: %s", m.Name)
		}
		return query.Iterators{
			&StringIterator{Points: []query.StringPoint{
				{Name: "cpu", Tags: ParseTags("region=west,host=A"), Time: 0 * Second, Value: "a"},
				{Name: "cpu", Tags: ParseTags("region=west,host=A"), Time: 1 * Second, Value: "a"},
			}},
			&StringIterator{Points: []query.StringPoint{
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 5 * Second, Value: "cxxx"},
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 6 * Second, Value: "zzzz"},
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 7 * Second, Value: "zzzz"},
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 8 * Second, Value: "zxxx"},
			}},
			&StringIterator{Points: []query.StringPoint{
				{Name: "cpu", Tags: ParseTags("region=east,host=A"), Time: 9 * Second, Value: "b"},
				{Name: "cpu", Tags: ParseTags("region=east,host=A"), Time: 10 * Second, Value: "d"},
				{Name: "cpu", Tags: ParseTags("region=east,host=A"), Time: 11 * Second, Value: "d"},
				{Name: "cpu", Tags: ParseTags("region=east,host=A"), Time: 12 * Second, Value: "d"},
			}},
		}.Merge(opt)
	}

	// Execute selection.
	itrs, err := query.Select(MustParseSelectStatement(`SELECT mode(value) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY time(10s), host fill(none)`), &ic, nil)
	if err != nil {
		t.Fatal(err)
	} else if a, err := Iterators(itrs).ReadAll(); err != nil {
		t.Fatalf("unexpected point: %s", err)
	} else if diff := cmp.Diff(a, [][]query.Point{
		{&query.StringPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 0 * Second, Value: "a"}},
		{&query.StringPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 10 * Second, Value: "d"}},
		{&query.StringPoint{Name: "cpu", Tags: ParseTags("host=B"), Time: 0 * Second, Value: "zzzz"}},
	}); diff != "" {
		t.Fatalf("unexpected points:\n%s", diff)
	}
}

// Ensure a SELECT mode() query cannot be executed on booleans.
func TestSelect_Mode_Boolean(t *testing.T) {
	var ic IteratorCreator
	ic.CreateIteratorFn = func(m *influxql.Measurement, opt query.IteratorOptions) (query.Iterator, error) {
		if m.Name != "cpu" {
			t.Fatalf("unexpected source: %s", m.Name)
		}
		return query.Iterators{
			&BooleanIterator{Points: []query.BooleanPoint{
				{Name: "cpu", Tags: ParseTags("region=west,host=A"), Time: 0 * Second, Value: true},
				{Name: "cpu", Tags: ParseTags("region=west,host=A"), Time: 1 * Second, Value: false},
				{Name: "cpu", Tags: ParseTags("region=west,host=A"), Time: 2 * Second, Value: false},
			}},
			&BooleanIterator{Points: []query.BooleanPoint{
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 5 * Second, Value: true},
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 6 * Second, Value: false},
			}},
			&BooleanIterator{Points: []query.BooleanPoint{
				{Name: "cpu", Tags: ParseTags("region=east,host=A"), Time: 9 * Second, Value: false},
				{Name: "cpu", Tags: ParseTags("region=east,host=A"), Time: 10 * Second, Value: true},
				{Name: "cpu", Tags: ParseTags("region=east,host=A"), Time: 11 * Second, Value: false},
				{Name: "cpu", Tags: ParseTags("region=east,host=A"), Time: 12 * Second, Value: true},
			}},
		}.Merge(opt)
	}

	// Execute selection.
	itrs, err := query.Select(MustParseSelectStatement(`SELECT mode(value) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY time(10s), host fill(none)`), &ic, nil)
	if err != nil {
		t.Fatal(err)
	} else if a, err := Iterators(itrs).ReadAll(); err != nil {
		t.Fatalf("unexpected point: %s", err)
	} else if diff := cmp.Diff(a, [][]query.Point{
		{&query.BooleanPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 0 * Second, Value: false}},
		{&query.BooleanPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 10 * Second, Value: true}},
		{&query.BooleanPoint{Name: "cpu", Tags: ParseTags("host=B"), Time: 0 * Second, Value: true}},
	}); diff != "" {
		t.Errorf("unexpected points:\n%s", diff)
	}
}

// Ensure a SELECT top() query can be executed.
func TestSelect_Top_NoTags_Float(t *testing.T) {
	var ic IteratorCreator
	ic.CreateIteratorFn = func(m *influxql.Measurement, opt query.IteratorOptions) (query.Iterator, error) {
		if m.Name != "cpu" {
			t.Fatalf("unexpected source: %s", m.Name)
		}
		return query.Iterators{
			&FloatIterator{Points: []query.FloatPoint{
				{Name: "cpu", Tags: ParseTags("region=west,host=A"), Time: 0 * Second, Value: 20},
				{Name: "cpu", Tags: ParseTags("region=west,host=A"), Time: 11 * Second, Value: 3},
				{Name: "cpu", Tags: ParseTags("region=west,host=A"), Time: 31 * Second, Value: 100},
			}},
			&FloatIterator{Points: []query.FloatPoint{
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 5 * Second, Value: 10},
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 50 * Second, Value: 1},
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 51 * Second, Value: 2},
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 52 * Second, Value: 3},
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 53 * Second, Value: 4},
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 53 * Second, Value: 5},
			}},
			&FloatIterator{Points: []query.FloatPoint{
				{Name: "cpu", Tags: ParseTags("region=east,host=A"), Time: 9 * Second, Value: 19},
				{Name: "cpu", Tags: ParseTags("region=east,host=A"), Time: 10 * Second, Value: 2},
			}},
		}.Merge(opt)
	}

	// Execute selection.
	itrs, err := query.Select(MustParseSelectStatement(`SELECT top(value, 2) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY time(30s), host fill(none)`), &ic, nil)
	if err != nil {
		t.Fatal(err)
	} else if a, err := Iterators(itrs).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if diff := cmp.Diff(a, [][]query.Point{
		{&query.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 0 * Second, Value: 20}},
		{&query.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 9 * Second, Value: 19}},
		{&query.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 31 * Second, Value: 100}},
		{&query.FloatPoint{Name: "cpu", Tags: ParseTags("host=B"), Time: 5 * Second, Value: 10}},
		{&query.FloatPoint{Name: "cpu", Tags: ParseTags("host=B"), Time: 53 * Second, Value: 5}},
		{&query.FloatPoint{Name: "cpu", Tags: ParseTags("host=B"), Time: 53 * Second, Value: 4}},
	}); diff != "" {
		t.Fatalf("unexpected points:\n%s", diff)
	}
}

// Ensure a SELECT top() query can be executed.
func TestSelect_Top_NoTags_Integer(t *testing.T) {
	var ic IteratorCreator
	ic.CreateIteratorFn = func(m *influxql.Measurement, opt query.IteratorOptions) (query.Iterator, error) {
		if m.Name != "cpu" {
			t.Fatalf("unexpected source: %s", m.Name)
		}
		return query.Iterators{
			&IntegerIterator{Points: []query.IntegerPoint{
				{Name: "cpu", Tags: ParseTags("region=west,host=A"), Time: 0 * Second, Value: 20},
				{Name: "cpu", Tags: ParseTags("region=west,host=A"), Time: 11 * Second, Value: 3},
				{Name: "cpu", Tags: ParseTags("region=west,host=A"), Time: 31 * Second, Value: 100},
			}},
			&IntegerIterator{Points: []query.IntegerPoint{
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 5 * Second, Value: 10},
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 50 * Second, Value: 1},
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 51 * Second, Value: 2},
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 52 * Second, Value: 3},
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 53 * Second, Value: 4},
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 53 * Second, Value: 5},
			}},
			&IntegerIterator{Points: []query.IntegerPoint{
				{Name: "cpu", Tags: ParseTags("region=east,host=A"), Time: 9 * Second, Value: 19},
				{Name: "cpu", Tags: ParseTags("region=east,host=A"), Time: 10 * Second, Value: 2},
			}},
		}.Merge(opt)
	}

	// Execute selection.
	itrs, err := query.Select(MustParseSelectStatement(`SELECT top(value, 2) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY time(30s), host fill(none)`), &ic, nil)
	if err != nil {
		t.Fatal(err)
	} else if a, err := Iterators(itrs).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if diff := cmp.Diff(a, [][]query.Point{
		{&query.IntegerPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 0 * Second, Value: 20}},
		{&query.IntegerPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 9 * Second, Value: 19}},
		{&query.IntegerPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 31 * Second, Value: 100}},
		{&query.IntegerPoint{Name: "cpu", Tags: ParseTags("host=B"), Time: 5 * Second, Value: 10}},
		{&query.IntegerPoint{Name: "cpu", Tags: ParseTags("host=B"), Time: 53 * Second, Value: 5}},
		{&query.IntegerPoint{Name: "cpu", Tags: ParseTags("host=B"), Time: 53 * Second, Value: 4}},
	}); diff != "" {
		t.Fatalf("unexpected points:\n%s", diff)
	}
}

// Ensure a SELECT top() query can be executed with tags.
func TestSelect_Top_Tags_Float(t *testing.T) {
	var ic IteratorCreator
	ic.CreateIteratorFn = func(m *influxql.Measurement, opt query.IteratorOptions) (query.Iterator, error) {
		if m.Name != "cpu" {
			t.Fatalf("unexpected source: %s", m.Name)
		}
		if !reflect.DeepEqual(opt.Expr, MustParseExpr(`max(value::float)`)) {
			t.Fatalf("unexpected expr: %s", spew.Sdump(opt.Expr))
		}
		return query.Iterators{
			MustCallIterator(&FloatIterator{Points: []query.FloatPoint{
				{Name: "cpu", Tags: ParseTags("region=west,host=A"), Time: 0 * Second, Value: 20, Aux: []interface{}{"A"}},
				{Name: "cpu", Tags: ParseTags("region=west,host=A"), Time: 11 * Second, Value: 3, Aux: []interface{}{"A"}},
				{Name: "cpu", Tags: ParseTags("region=west,host=A"), Time: 31 * Second, Value: 100, Aux: []interface{}{"A"}},
			}}, opt),
			MustCallIterator(&FloatIterator{Points: []query.FloatPoint{
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 5 * Second, Value: 10, Aux: []interface{}{"B"}},
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 50 * Second, Value: 1, Aux: []interface{}{"B"}},
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 51 * Second, Value: 2, Aux: []interface{}{"B"}},
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 52 * Second, Value: 3, Aux: []interface{}{"B"}},
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 53 * Second, Value: 4, Aux: []interface{}{"B"}},
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 53 * Second, Value: 5, Aux: []interface{}{"B"}},
			}}, opt),
			MustCallIterator(&FloatIterator{Points: []query.FloatPoint{
				{Name: "cpu", Tags: ParseTags("region=east,host=A"), Time: 9 * Second, Value: 19, Aux: []interface{}{"A"}},
				{Name: "cpu", Tags: ParseTags("region=east,host=A"), Time: 10 * Second, Value: 2, Aux: []interface{}{"A"}},
			}}, opt),
		}.Merge(opt)
	}

	// Execute selection.
	itrs, err := query.Select(MustParseSelectStatement(`SELECT top(value::float, host::tag, 2) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY time(30s) fill(none)`), &ic, nil)
	if err != nil {
		t.Fatal(err)
	} else if a, err := Iterators(itrs).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if diff := cmp.Diff(a, [][]query.Point{
		{
			&query.FloatPoint{Name: "cpu", Time: 0 * Second, Value: 20, Aux: []interface{}{"A"}},
			&query.StringPoint{Name: "cpu", Time: 0 * Second, Value: "A"},
		},
		{
			&query.FloatPoint{Name: "cpu", Time: 5 * Second, Value: 10, Aux: []interface{}{"B"}},
			&query.StringPoint{Name: "cpu", Time: 5 * Second, Value: "B"},
		},
		{
			&query.FloatPoint{Name: "cpu", Time: 31 * Second, Value: 100, Aux: []interface{}{"A"}},
			&query.StringPoint{Name: "cpu", Time: 31 * Second, Value: "A"},
		},
		{
			&query.FloatPoint{Name: "cpu", Time: 53 * Second, Value: 5, Aux: []interface{}{"B"}},
			&query.StringPoint{Name: "cpu", Time: 53 * Second, Value: "B"},
		},
	}); diff != "" {
		t.Fatalf("unexpected points:\n%s", diff)
	}
}

// Ensure a SELECT top() query can be executed with tags.
func TestSelect_Top_Tags_Integer(t *testing.T) {
	var ic IteratorCreator
	ic.CreateIteratorFn = func(m *influxql.Measurement, opt query.IteratorOptions) (query.Iterator, error) {
		if m.Name != "cpu" {
			t.Fatalf("unexpected source: %s", m.Name)
		}
		return query.Iterators{
			&IntegerIterator{Points: []query.IntegerPoint{
				{Name: "cpu", Tags: ParseTags("region=west,host=A"), Time: 0 * Second, Value: 20, Aux: []interface{}{"A"}},
				{Name: "cpu", Tags: ParseTags("region=west,host=A"), Time: 11 * Second, Value: 3, Aux: []interface{}{"A"}},
				{Name: "cpu", Tags: ParseTags("region=west,host=A"), Time: 31 * Second, Value: 100, Aux: []interface{}{"A"}},
			}},
			&IntegerIterator{Points: []query.IntegerPoint{
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 5 * Second, Value: 10, Aux: []interface{}{"B"}},
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 50 * Second, Value: 1, Aux: []interface{}{"B"}},
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 51 * Second, Value: 2, Aux: []interface{}{"B"}},
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 52 * Second, Value: 3, Aux: []interface{}{"B"}},
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 53 * Second, Value: 4, Aux: []interface{}{"B"}},
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 53 * Second, Value: 5, Aux: []interface{}{"B"}},
			}},
			&IntegerIterator{Points: []query.IntegerPoint{
				{Name: "cpu", Tags: ParseTags("region=east,host=A"), Time: 9 * Second, Value: 19, Aux: []interface{}{"A"}},
				{Name: "cpu", Tags: ParseTags("region=east,host=A"), Time: 10 * Second, Value: 2, Aux: []interface{}{"A"}},
			}},
		}.Merge(opt)
	}

	// Execute selection.
	itrs, err := query.Select(MustParseSelectStatement(`SELECT top(value::integer, host::tag, 2) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY time(30s) fill(none)`), &ic, nil)
	if err != nil {
		t.Fatal(err)
	} else if a, err := Iterators(itrs).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if diff := cmp.Diff(a, [][]query.Point{
		{
			&query.IntegerPoint{Name: "cpu", Time: 0 * Second, Value: 20, Aux: []interface{}{"A"}},
			&query.StringPoint{Name: "cpu", Time: 0 * Second, Value: "A"},
		},
		{
			&query.IntegerPoint{Name: "cpu", Time: 5 * Second, Value: 10, Aux: []interface{}{"B"}},
			&query.StringPoint{Name: "cpu", Time: 5 * Second, Value: "B"},
		},
		{
			&query.IntegerPoint{Name: "cpu", Time: 31 * Second, Value: 100, Aux: []interface{}{"A"}},
			&query.StringPoint{Name: "cpu", Time: 31 * Second, Value: "A"},
		},
		{
			&query.IntegerPoint{Name: "cpu", Time: 53 * Second, Value: 5, Aux: []interface{}{"B"}},
			&query.StringPoint{Name: "cpu", Time: 53 * Second, Value: "B"},
		},
	}); diff != "" {
		t.Fatalf("unexpected points:\n%s", diff)
	}
}

// Ensure a SELECT top() query can be executed with tags and group by.
func TestSelect_Top_GroupByTags_Float(t *testing.T) {
	var ic IteratorCreator
	ic.CreateIteratorFn = func(m *influxql.Measurement, opt query.IteratorOptions) (query.Iterator, error) {
		if m.Name != "cpu" {
			t.Fatalf("unexpected source: %s", m.Name)
		}
		if !reflect.DeepEqual(opt.Expr, MustParseExpr(`max(value::float)`)) {
			t.Fatalf("unexpected expr: %s", spew.Sdump(opt.Expr))
		}
		return query.Iterators{
			MustCallIterator(&FloatIterator{Points: []query.FloatPoint{
				{Name: "cpu", Tags: ParseTags("region=west,host=A"), Time: 0 * Second, Value: 20, Aux: []interface{}{"A"}},
				{Name: "cpu", Tags: ParseTags("region=west,host=A"), Time: 11 * Second, Value: 3, Aux: []interface{}{"A"}},
				{Name: "cpu", Tags: ParseTags("region=west,host=A"), Time: 31 * Second, Value: 100, Aux: []interface{}{"A"}},
			}}, opt),
			MustCallIterator(&FloatIterator{Points: []query.FloatPoint{
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 5 * Second, Value: 10, Aux: []interface{}{"B"}},
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 50 * Second, Value: 1, Aux: []interface{}{"B"}},
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 51 * Second, Value: 2, Aux: []interface{}{"B"}},
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 52 * Second, Value: 3, Aux: []interface{}{"B"}},
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 53 * Second, Value: 4, Aux: []interface{}{"B"}},
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 53 * Second, Value: 5, Aux: []interface{}{"B"}},
			}}, opt),
			MustCallIterator(&FloatIterator{Points: []query.FloatPoint{
				{Name: "cpu", Tags: ParseTags("region=east,host=A"), Time: 9 * Second, Value: 19, Aux: []interface{}{"A"}},
				{Name: "cpu", Tags: ParseTags("region=east,host=A"), Time: 10 * Second, Value: 2, Aux: []interface{}{"A"}},
			}}, opt),
		}.Merge(opt)
	}

	// Execute selection.
	itrs, err := query.Select(MustParseSelectStatement(`SELECT top(value::float, host::tag, 1) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY region, time(30s) fill(none)`), &ic, nil)
	if err != nil {
		t.Fatal(err)
	} else if a, err := Iterators(itrs).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if diff := cmp.Diff(a, [][]query.Point{
		{
			&query.FloatPoint{Name: "cpu", Tags: ParseTags("region=east"), Time: 9 * Second, Value: 19, Aux: []interface{}{"A"}},
			&query.StringPoint{Name: "cpu", Tags: ParseTags("region=east"), Time: 9 * Second, Value: "A"},
		},
		{
			&query.FloatPoint{Name: "cpu", Tags: ParseTags("region=west"), Time: 0 * Second, Value: 20, Aux: []interface{}{"A"}},
			&query.StringPoint{Name: "cpu", Tags: ParseTags("region=west"), Time: 0 * Second, Value: "A"},
		},
		{
			&query.FloatPoint{Name: "cpu", Tags: ParseTags("region=west"), Time: 31 * Second, Value: 100, Aux: []interface{}{"A"}},
			&query.StringPoint{Name: "cpu", Tags: ParseTags("region=west"), Time: 31 * Second, Value: "A"},
		},
	}); diff != "" {
		t.Fatalf("unexpected points:\n%s", diff)
	}
}

// Ensure a SELECT top() query can be executed with tags and group by.
func TestSelect_Top_GroupByTags_Integer(t *testing.T) {
	var ic IteratorCreator
	ic.CreateIteratorFn = func(m *influxql.Measurement, opt query.IteratorOptions) (query.Iterator, error) {
		if m.Name != "cpu" {
			t.Fatalf("unexpected source: %s", m.Name)
		}
		if !reflect.DeepEqual(opt.Expr, MustParseExpr(`max(value::integer)`)) {
			t.Fatalf("unexpected expr: %s", spew.Sdump(opt.Expr))
		}
		return query.Iterators{
			MustCallIterator(&IntegerIterator{Points: []query.IntegerPoint{
				{Name: "cpu", Tags: ParseTags("region=west,host=A"), Time: 0 * Second, Value: 20, Aux: []interface{}{"A"}},
				{Name: "cpu", Tags: ParseTags("region=west,host=A"), Time: 11 * Second, Value: 3, Aux: []interface{}{"A"}},
				{Name: "cpu", Tags: ParseTags("region=west,host=A"), Time: 31 * Second, Value: 100, Aux: []interface{}{"A"}},
			}}, opt),
			MustCallIterator(&IntegerIterator{Points: []query.IntegerPoint{
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 5 * Second, Value: 10, Aux: []interface{}{"B"}},
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 50 * Second, Value: 1, Aux: []interface{}{"B"}},
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 51 * Second, Value: 2, Aux: []interface{}{"B"}},
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 52 * Second, Value: 3, Aux: []interface{}{"B"}},
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 53 * Second, Value: 4, Aux: []interface{}{"B"}},
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 53 * Second, Value: 5, Aux: []interface{}{"B"}},
			}}, opt),
			MustCallIterator(&IntegerIterator{Points: []query.IntegerPoint{
				{Name: "cpu", Tags: ParseTags("region=east,host=A"), Time: 9 * Second, Value: 19, Aux: []interface{}{"A"}},
				{Name: "cpu", Tags: ParseTags("region=east,host=A"), Time: 10 * Second, Value: 2, Aux: []interface{}{"A"}},
			}}, opt),
		}.Merge(opt)
	}

	// Execute selection.
	itrs, err := query.Select(MustParseSelectStatement(`SELECT top(value::integer, host::tag, 1) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY region, time(30s) fill(none)`), &ic, nil)
	if err != nil {
		t.Fatal(err)
	} else if a, err := Iterators(itrs).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if diff := cmp.Diff(a, [][]query.Point{
		{
			&query.IntegerPoint{Name: "cpu", Tags: ParseTags("region=east"), Time: 9 * Second, Value: 19, Aux: []interface{}{"A"}},
			&query.StringPoint{Name: "cpu", Tags: ParseTags("region=east"), Time: 9 * Second, Value: "A"},
		},
		{
			&query.IntegerPoint{Name: "cpu", Tags: ParseTags("region=west"), Time: 0 * Second, Value: 20, Aux: []interface{}{"A"}},
			&query.StringPoint{Name: "cpu", Tags: ParseTags("region=west"), Time: 0 * Second, Value: "A"},
		},
		{
			&query.IntegerPoint{Name: "cpu", Tags: ParseTags("region=west"), Time: 31 * Second, Value: 100, Aux: []interface{}{"A"}},
			&query.StringPoint{Name: "cpu", Tags: ParseTags("region=west"), Time: 31 * Second, Value: "A"},
		},
	}); diff != "" {
		t.Fatalf("unexpected points:\n%s", diff)
	}
}

// Ensure a SELECT bottom() query can be executed.
func TestSelect_Bottom_NoTags_Float(t *testing.T) {
	var ic IteratorCreator
	ic.CreateIteratorFn = func(m *influxql.Measurement, opt query.IteratorOptions) (query.Iterator, error) {
		if m.Name != "cpu" {
			t.Fatalf("unexpected source: %s", m.Name)
		}
		return query.Iterators{
			&FloatIterator{Points: []query.FloatPoint{
				{Name: "cpu", Tags: ParseTags("region=west,host=A"), Time: 0 * Second, Value: 20},
				{Name: "cpu", Tags: ParseTags("region=west,host=A"), Time: 11 * Second, Value: 3},
				{Name: "cpu", Tags: ParseTags("region=west,host=A"), Time: 31 * Second, Value: 100},
			}},
			&FloatIterator{Points: []query.FloatPoint{
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 5 * Second, Value: 10},
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 50 * Second, Value: 1},
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 51 * Second, Value: 2},
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 52 * Second, Value: 3},
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 53 * Second, Value: 4},
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 53 * Second, Value: 5},
			}},
			&FloatIterator{Points: []query.FloatPoint{
				{Name: "cpu", Tags: ParseTags("region=east,host=A"), Time: 9 * Second, Value: 19},
				{Name: "cpu", Tags: ParseTags("region=east,host=A"), Time: 10 * Second, Value: 2},
			}},
		}.Merge(opt)
	}

	// Execute selection.
	itrs, err := query.Select(MustParseSelectStatement(`SELECT bottom(value::float, 2) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY time(30s), host fill(none)`), &ic, nil)
	if err != nil {
		t.Fatal(err)
	} else if a, err := Iterators(itrs).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if diff := cmp.Diff(a, [][]query.Point{
		{&query.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 10 * Second, Value: 2}},
		{&query.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 11 * Second, Value: 3}},
		{&query.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 31 * Second, Value: 100}},
		{&query.FloatPoint{Name: "cpu", Tags: ParseTags("host=B"), Time: 5 * Second, Value: 10}},
		{&query.FloatPoint{Name: "cpu", Tags: ParseTags("host=B"), Time: 50 * Second, Value: 1}},
		{&query.FloatPoint{Name: "cpu", Tags: ParseTags("host=B"), Time: 51 * Second, Value: 2}},
	}); diff != "" {
		t.Fatalf("unexpected points:\n%s", diff)
	}
}

// Ensure a SELECT bottom() query can be executed.
func TestSelect_Bottom_NoTags_Integer(t *testing.T) {
	var ic IteratorCreator
	ic.CreateIteratorFn = func(m *influxql.Measurement, opt query.IteratorOptions) (query.Iterator, error) {
		if m.Name != "cpu" {
			t.Fatalf("unexpected source: %s", m.Name)
		}
		return query.Iterators{
			&IntegerIterator{Points: []query.IntegerPoint{
				{Name: "cpu", Tags: ParseTags("region=west,host=A"), Time: 0 * Second, Value: 20},
				{Name: "cpu", Tags: ParseTags("region=west,host=A"), Time: 11 * Second, Value: 3},
				{Name: "cpu", Tags: ParseTags("region=west,host=A"), Time: 31 * Second, Value: 100},
			}},
			&IntegerIterator{Points: []query.IntegerPoint{
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 5 * Second, Value: 10},
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 50 * Second, Value: 1},
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 51 * Second, Value: 2},
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 52 * Second, Value: 3},
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 53 * Second, Value: 4},
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 53 * Second, Value: 5},
			}},
			&IntegerIterator{Points: []query.IntegerPoint{
				{Name: "cpu", Tags: ParseTags("region=east,host=A"), Time: 9 * Second, Value: 19},
				{Name: "cpu", Tags: ParseTags("region=east,host=A"), Time: 10 * Second, Value: 2},
			}},
		}.Merge(opt)
	}

	// Execute selection.
	itrs, err := query.Select(MustParseSelectStatement(`SELECT bottom(value::integer, 2) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY time(30s), host fill(none)`), &ic, nil)
	if err != nil {
		t.Fatal(err)
	} else if a, err := Iterators(itrs).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if diff := cmp.Diff(a, [][]query.Point{
		{&query.IntegerPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 10 * Second, Value: 2}},
		{&query.IntegerPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 11 * Second, Value: 3}},
		{&query.IntegerPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 31 * Second, Value: 100}},
		{&query.IntegerPoint{Name: "cpu", Tags: ParseTags("host=B"), Time: 5 * Second, Value: 10}},
		{&query.IntegerPoint{Name: "cpu", Tags: ParseTags("host=B"), Time: 50 * Second, Value: 1}},
		{&query.IntegerPoint{Name: "cpu", Tags: ParseTags("host=B"), Time: 51 * Second, Value: 2}},
	}); diff != "" {
		t.Fatalf("unexpected points:\n%s", diff)
	}
}

// Ensure a SELECT bottom() query can be executed with tags.
func TestSelect_Bottom_Tags_Float(t *testing.T) {
	var ic IteratorCreator
	ic.CreateIteratorFn = func(m *influxql.Measurement, opt query.IteratorOptions) (query.Iterator, error) {
		if m.Name != "cpu" {
			t.Fatalf("unexpected source: %s", m.Name)
		}
		if !reflect.DeepEqual(opt.Expr, MustParseExpr(`min(value::float)`)) {
			t.Fatalf("unexpected expr: %s", spew.Sdump(opt.Expr))
		}
		return query.Iterators{
			MustCallIterator(&FloatIterator{Points: []query.FloatPoint{
				{Name: "cpu", Tags: ParseTags("region=west,host=A"), Time: 0 * Second, Value: 20, Aux: []interface{}{"A"}},
				{Name: "cpu", Tags: ParseTags("region=west,host=A"), Time: 11 * Second, Value: 3, Aux: []interface{}{"A"}},
				{Name: "cpu", Tags: ParseTags("region=west,host=A"), Time: 31 * Second, Value: 100, Aux: []interface{}{"A"}},
			}}, opt),
			MustCallIterator(&FloatIterator{Points: []query.FloatPoint{
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 5 * Second, Value: 10, Aux: []interface{}{"B"}},
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 50 * Second, Value: 1, Aux: []interface{}{"B"}},
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 51 * Second, Value: 2, Aux: []interface{}{"B"}},
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 52 * Second, Value: 3, Aux: []interface{}{"B"}},
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 53 * Second, Value: 4, Aux: []interface{}{"B"}},
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 53 * Second, Value: 5, Aux: []interface{}{"B"}},
			}}, opt),
			MustCallIterator(&FloatIterator{Points: []query.FloatPoint{
				{Name: "cpu", Tags: ParseTags("region=east,host=A"), Time: 9 * Second, Value: 19, Aux: []interface{}{"A"}},
				{Name: "cpu", Tags: ParseTags("region=east,host=A"), Time: 10 * Second, Value: 2, Aux: []interface{}{"A"}},
			}}, opt),
		}.Merge(opt)
	}

	// Execute selection.
	itrs, err := query.Select(MustParseSelectStatement(`SELECT bottom(value::float, host::tag, 2) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY time(30s) fill(none)`), &ic, nil)
	if err != nil {
		t.Fatal(err)
	} else if a, err := Iterators(itrs).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if diff := cmp.Diff(a, [][]query.Point{
		{
			&query.FloatPoint{Name: "cpu", Time: 5 * Second, Value: 10, Aux: []interface{}{"B"}},
			&query.StringPoint{Name: "cpu", Time: 5 * Second, Value: "B"},
		},
		{
			&query.FloatPoint{Name: "cpu", Time: 10 * Second, Value: 2, Aux: []interface{}{"A"}},
			&query.StringPoint{Name: "cpu", Time: 10 * Second, Value: "A"},
		},
		{
			&query.FloatPoint{Name: "cpu", Time: 31 * Second, Value: 100, Aux: []interface{}{"A"}},
			&query.StringPoint{Name: "cpu", Time: 31 * Second, Value: "A"},
		},
		{
			&query.FloatPoint{Name: "cpu", Time: 50 * Second, Value: 1, Aux: []interface{}{"B"}},
			&query.StringPoint{Name: "cpu", Time: 50 * Second, Value: "B"},
		},
	}); diff != "" {
		t.Fatalf("unexpected points:\n%s", diff)
	}
}

// Ensure a SELECT bottom() query can be executed with tags.
func TestSelect_Bottom_Tags_Integer(t *testing.T) {
	var ic IteratorCreator
	ic.CreateIteratorFn = func(m *influxql.Measurement, opt query.IteratorOptions) (query.Iterator, error) {
		if m.Name != "cpu" {
			t.Fatalf("unexpected source: %s", m.Name)
		}
		if !reflect.DeepEqual(opt.Expr, MustParseExpr(`min(value::integer)`)) {
			t.Fatalf("unexpected expr: %s", spew.Sdump(opt.Expr))
		}
		return query.Iterators{
			MustCallIterator(&IntegerIterator{Points: []query.IntegerPoint{
				{Name: "cpu", Tags: ParseTags("region=west,host=A"), Time: 0 * Second, Value: 20, Aux: []interface{}{"A"}},
				{Name: "cpu", Tags: ParseTags("region=west,host=A"), Time: 11 * Second, Value: 3, Aux: []interface{}{"A"}},
				{Name: "cpu", Tags: ParseTags("region=west,host=A"), Time: 31 * Second, Value: 100, Aux: []interface{}{"A"}},
			}}, opt),
			MustCallIterator(&IntegerIterator{Points: []query.IntegerPoint{
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 5 * Second, Value: 10, Aux: []interface{}{"B"}},
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 50 * Second, Value: 1, Aux: []interface{}{"B"}},
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 51 * Second, Value: 2, Aux: []interface{}{"B"}},
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 52 * Second, Value: 3, Aux: []interface{}{"B"}},
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 53 * Second, Value: 4, Aux: []interface{}{"B"}},
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 53 * Second, Value: 5, Aux: []interface{}{"B"}},
			}}, opt),
			MustCallIterator(&IntegerIterator{Points: []query.IntegerPoint{
				{Name: "cpu", Tags: ParseTags("region=east,host=A"), Time: 9 * Second, Value: 19, Aux: []interface{}{"A"}},
				{Name: "cpu", Tags: ParseTags("region=east,host=A"), Time: 10 * Second, Value: 2, Aux: []interface{}{"A"}},
			}}, opt),
		}.Merge(opt)
	}

	// Execute selection.
	itrs, err := query.Select(MustParseSelectStatement(`SELECT bottom(value::integer, host::tag, 2) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY time(30s) fill(none)`), &ic, nil)
	if err != nil {
		t.Fatal(err)
	} else if a, err := Iterators(itrs).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if diff := cmp.Diff(a, [][]query.Point{
		{
			&query.IntegerPoint{Name: "cpu", Time: 5 * Second, Value: 10, Aux: []interface{}{"B"}},
			&query.StringPoint{Name: "cpu", Time: 5 * Second, Value: "B"},
		},
		{
			&query.IntegerPoint{Name: "cpu", Time: 10 * Second, Value: 2, Aux: []interface{}{"A"}},
			&query.StringPoint{Name: "cpu", Time: 10 * Second, Value: "A"},
		},
		{
			&query.IntegerPoint{Name: "cpu", Time: 31 * Second, Value: 100, Aux: []interface{}{"A"}},
			&query.StringPoint{Name: "cpu", Time: 31 * Second, Value: "A"},
		},
		{
			&query.IntegerPoint{Name: "cpu", Time: 50 * Second, Value: 1, Aux: []interface{}{"B"}},
			&query.StringPoint{Name: "cpu", Time: 50 * Second, Value: "B"},
		},
	}); diff != "" {
		t.Fatalf("unexpected points:\n%s", diff)
	}
}

// Ensure a SELECT bottom() query can be executed with tags and group by.
func TestSelect_Bottom_GroupByTags_Float(t *testing.T) {
	var ic IteratorCreator
	ic.CreateIteratorFn = func(m *influxql.Measurement, opt query.IteratorOptions) (query.Iterator, error) {
		if m.Name != "cpu" {
			t.Fatalf("unexpected source: %s", m.Name)
		}
		if !reflect.DeepEqual(opt.Expr, MustParseExpr(`min(value::float)`)) {
			t.Fatalf("unexpected expr: %s", spew.Sdump(opt.Expr))
		}
		return query.Iterators{
			MustCallIterator(&FloatIterator{Points: []query.FloatPoint{
				{Name: "cpu", Tags: ParseTags("region=west,host=A"), Time: 0 * Second, Value: 20, Aux: []interface{}{"A"}},
				{Name: "cpu", Tags: ParseTags("region=west,host=A"), Time: 11 * Second, Value: 3, Aux: []interface{}{"A"}},
				{Name: "cpu", Tags: ParseTags("region=west,host=A"), Time: 31 * Second, Value: 100, Aux: []interface{}{"A"}},
			}}, opt),
			MustCallIterator(&FloatIterator{Points: []query.FloatPoint{
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 5 * Second, Value: 10, Aux: []interface{}{"B"}},
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 50 * Second, Value: 1, Aux: []interface{}{"B"}},
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 51 * Second, Value: 2, Aux: []interface{}{"B"}},
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 52 * Second, Value: 3, Aux: []interface{}{"B"}},
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 53 * Second, Value: 4, Aux: []interface{}{"B"}},
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 53 * Second, Value: 5, Aux: []interface{}{"B"}},
			}}, opt),
			MustCallIterator(&FloatIterator{Points: []query.FloatPoint{
				{Name: "cpu", Tags: ParseTags("region=east,host=A"), Time: 9 * Second, Value: 19, Aux: []interface{}{"A"}},
				{Name: "cpu", Tags: ParseTags("region=east,host=A"), Time: 10 * Second, Value: 2, Aux: []interface{}{"A"}},
			}}, opt),
		}.Merge(opt)
	}

	// Execute selection.
	itrs, err := query.Select(MustParseSelectStatement(`SELECT bottom(value::float, host::tag, 1) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY region, time(30s) fill(none)`), &ic, nil)
	if err != nil {
		t.Fatal(err)
	} else if a, err := Iterators(itrs).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if diff := cmp.Diff(a, [][]query.Point{
		{
			&query.FloatPoint{Name: "cpu", Tags: ParseTags("region=east"), Time: 10 * Second, Value: 2, Aux: []interface{}{"A"}},
			&query.StringPoint{Name: "cpu", Tags: ParseTags("region=east"), Time: 10 * Second, Value: "A"},
		},
		{
			&query.FloatPoint{Name: "cpu", Tags: ParseTags("region=west"), Time: 11 * Second, Value: 3, Aux: []interface{}{"A"}},
			&query.StringPoint{Name: "cpu", Tags: ParseTags("region=west"), Time: 11 * Second, Value: "A"},
		},
		{
			&query.FloatPoint{Name: "cpu", Tags: ParseTags("region=west"), Time: 50 * Second, Value: 1, Aux: []interface{}{"B"}},
			&query.StringPoint{Name: "cpu", Tags: ParseTags("region=west"), Time: 50 * Second, Value: "B"},
		},
	}); diff != "" {
		t.Fatalf("unexpected points: %s", diff)
	}
}

// Ensure a SELECT bottom() query can be executed with tags and group by.
func TestSelect_Bottom_GroupByTags_Integer(t *testing.T) {
	var ic IteratorCreator
	ic.CreateIteratorFn = func(m *influxql.Measurement, opt query.IteratorOptions) (query.Iterator, error) {
		if m.Name != "cpu" {
			t.Fatalf("unexpected source: %s", m.Name)
		}
		if !reflect.DeepEqual(opt.Expr, MustParseExpr(`min(value::float)`)) {
			t.Fatalf("unexpected expr: %s", spew.Sdump(opt.Expr))
		}
		return query.Iterators{
			MustCallIterator(&IntegerIterator{Points: []query.IntegerPoint{
				{Name: "cpu", Tags: ParseTags("region=west,host=A"), Time: 0 * Second, Value: 20, Aux: []interface{}{"A"}},
				{Name: "cpu", Tags: ParseTags("region=west,host=A"), Time: 11 * Second, Value: 3, Aux: []interface{}{"A"}},
				{Name: "cpu", Tags: ParseTags("region=west,host=A"), Time: 31 * Second, Value: 100, Aux: []interface{}{"A"}},
			}}, opt),
			MustCallIterator(&IntegerIterator{Points: []query.IntegerPoint{
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 5 * Second, Value: 10, Aux: []interface{}{"B"}},
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 50 * Second, Value: 1, Aux: []interface{}{"B"}},
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 51 * Second, Value: 2, Aux: []interface{}{"B"}},
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 52 * Second, Value: 3, Aux: []interface{}{"B"}},
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 53 * Second, Value: 4, Aux: []interface{}{"B"}},
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 53 * Second, Value: 5, Aux: []interface{}{"B"}},
			}}, opt),
			MustCallIterator(&IntegerIterator{Points: []query.IntegerPoint{
				{Name: "cpu", Tags: ParseTags("region=east,host=A"), Time: 9 * Second, Value: 19, Aux: []interface{}{"A"}},
				{Name: "cpu", Tags: ParseTags("region=east,host=A"), Time: 10 * Second, Value: 2, Aux: []interface{}{"A"}},
			}}, opt),
		}.Merge(opt)
	}

	// Execute selection.
	itrs, err := query.Select(MustParseSelectStatement(`SELECT bottom(value::float, host::tag, 1) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY region, time(30s) fill(none)`), &ic, nil)
	if err != nil {
		t.Fatal(err)
	} else if a, err := Iterators(itrs).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if diff := cmp.Diff(a, [][]query.Point{
		{
			&query.IntegerPoint{Name: "cpu", Tags: ParseTags("region=east"), Time: 10 * Second, Value: 2, Aux: []interface{}{"A"}},
			&query.StringPoint{Name: "cpu", Tags: ParseTags("region=east"), Time: 10 * Second, Value: "A"},
		},
		{
			&query.IntegerPoint{Name: "cpu", Tags: ParseTags("region=west"), Time: 11 * Second, Value: 3, Aux: []interface{}{"A"}},
			&query.StringPoint{Name: "cpu", Tags: ParseTags("region=west"), Time: 11 * Second, Value: "A"},
		},
		{
			&query.IntegerPoint{Name: "cpu", Tags: ParseTags("region=west"), Time: 50 * Second, Value: 1, Aux: []interface{}{"B"}},
			&query.StringPoint{Name: "cpu", Tags: ParseTags("region=west"), Time: 50 * Second, Value: "B"},
		},
	}); diff != "" {
		t.Fatalf("unexpected points:\n%s", diff)
	}
}

// Ensure a SELECT query with a fill(null) statement can be executed.
func TestSelect_Fill_Null_Float(t *testing.T) {
	var ic IteratorCreator
	ic.CreateIteratorFn = func(m *influxql.Measurement, opt query.IteratorOptions) (query.Iterator, error) {
		if m.Name != "cpu" {
			t.Fatalf("unexpected source: %s", m.Name)
		}
		return query.NewCallIterator(&FloatIterator{Points: []query.FloatPoint{
			{Name: "cpu", Tags: ParseTags("host=A"), Time: 12 * Second, Value: 2},
		}}, opt)
	}

	// Execute selection.
	itrs, err := query.Select(MustParseSelectStatement(`SELECT mean(value) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:01:00Z' GROUP BY host, time(10s) fill(null)`), &ic, nil)
	if err != nil {
		t.Fatal(err)
	} else if a, err := Iterators(itrs).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if diff := cmp.Diff(a, [][]query.Point{
		{&query.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 0 * Second, Nil: true}},
		{&query.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 10 * Second, Value: 2, Aggregated: 1}},
		{&query.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 20 * Second, Nil: true}},
		{&query.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 30 * Second, Nil: true}},
		{&query.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 40 * Second, Nil: true}},
		{&query.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 50 * Second, Nil: true}},
	}); diff != "" {
		t.Fatalf("unexpected points:\n%s", diff)
	}
}

// Ensure a SELECT query with a fill(<number>) statement can be executed.
func TestSelect_Fill_Number_Float(t *testing.T) {
	var ic IteratorCreator
	ic.CreateIteratorFn = func(m *influxql.Measurement, opt query.IteratorOptions) (query.Iterator, error) {
		if m.Name != "cpu" {
			t.Fatalf("unexpected source: %s", m.Name)
		}
		return query.NewCallIterator(&FloatIterator{Points: []query.FloatPoint{
			{Name: "cpu", Tags: ParseTags("host=A"), Time: 12 * Second, Value: 2},
		}}, opt)
	}

	// Execute selection.
	itrs, err := query.Select(MustParseSelectStatement(`SELECT mean(value) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:01:00Z' GROUP BY host, time(10s) fill(1)`), &ic, nil)
	if err != nil {
		t.Fatal(err)
	} else if a, err := Iterators(itrs).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if diff := cmp.Diff(a, [][]query.Point{
		{&query.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 0 * Second, Value: 1}},
		{&query.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 10 * Second, Value: 2, Aggregated: 1}},
		{&query.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 20 * Second, Value: 1}},
		{&query.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 30 * Second, Value: 1}},
		{&query.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 40 * Second, Value: 1}},
		{&query.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 50 * Second, Value: 1}},
	}); diff != "" {
		t.Fatalf("unexpected points:\n%s", diff)
	}
}

// Ensure a SELECT query with a fill(previous) statement can be executed.
func TestSelect_Fill_Previous_Float(t *testing.T) {
	var ic IteratorCreator
	ic.CreateIteratorFn = func(m *influxql.Measurement, opt query.IteratorOptions) (query.Iterator, error) {
		if m.Name != "cpu" {
			t.Fatalf("unexpected source: %s", m.Name)
		}
		return query.NewCallIterator(&FloatIterator{Points: []query.FloatPoint{
			{Name: "cpu", Tags: ParseTags("host=A"), Time: 12 * Second, Value: 2},
		}}, opt)
	}

	// Execute selection.
	itrs, err := query.Select(MustParseSelectStatement(`SELECT mean(value) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:01:00Z' GROUP BY host, time(10s) fill(previous)`), &ic, nil)
	if err != nil {
		t.Fatal(err)
	} else if a, err := Iterators(itrs).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if diff := cmp.Diff(a, [][]query.Point{
		{&query.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 0 * Second, Nil: true}},
		{&query.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 10 * Second, Value: 2, Aggregated: 1}},
		{&query.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 20 * Second, Value: 2}},
		{&query.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 30 * Second, Value: 2}},
		{&query.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 40 * Second, Value: 2}},
		{&query.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 50 * Second, Value: 2}},
	}); diff != "" {
		t.Fatalf("unexpected points:\n%s", diff)
	}
}

// Ensure a SELECT query with a fill(linear) statement can be executed.
func TestSelect_Fill_Linear_Float_One(t *testing.T) {
	var ic IteratorCreator
	ic.CreateIteratorFn = func(m *influxql.Measurement, opt query.IteratorOptions) (query.Iterator, error) {
		if m.Name != "cpu" {
			t.Fatalf("unexpected source: %s", m.Name)
		}
		return query.NewCallIterator(&FloatIterator{Points: []query.FloatPoint{
			{Name: "cpu", Tags: ParseTags("host=A"), Time: 12 * Second, Value: 2},
			{Name: "cpu", Tags: ParseTags("host=A"), Time: 32 * Second, Value: 4},
		}}, opt)
	}

	// Execute selection.
	itrs, err := query.Select(MustParseSelectStatement(`SELECT mean(value) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:01:00Z' GROUP BY host, time(10s) fill(linear)`), &ic, nil)
	if err != nil {
		t.Fatal(err)
	} else if a, err := Iterators(itrs).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if diff := cmp.Diff(a, [][]query.Point{
		{&query.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 0 * Second, Nil: true}},
		{&query.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 10 * Second, Value: 2, Aggregated: 1}},
		{&query.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 20 * Second, Value: 3}},
		{&query.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 30 * Second, Value: 4, Aggregated: 1}},
		{&query.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 40 * Second, Nil: true}},
		{&query.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 50 * Second, Nil: true}},
	}); diff != "" {
		t.Fatalf("unexpected points:\n%s", diff)
	}
}

func TestSelect_Fill_Linear_Float_Many(t *testing.T) {
	var ic IteratorCreator
	ic.CreateIteratorFn = func(m *influxql.Measurement, opt query.IteratorOptions) (query.Iterator, error) {
		if m.Name != "cpu" {
			t.Fatalf("unexpected source: %s", m.Name)
		}
		return query.NewCallIterator(&FloatIterator{Points: []query.FloatPoint{
			{Name: "cpu", Tags: ParseTags("host=A"), Time: 12 * Second, Value: 2},
			{Name: "cpu", Tags: ParseTags("host=A"), Time: 62 * Second, Value: 7},
		}}, opt)
	}

	// Execute selection.
	itrs, err := query.Select(MustParseSelectStatement(`SELECT mean(value) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:01:00Z' GROUP BY host, time(10s) fill(linear)`), &ic, nil)
	if err != nil {
		t.Fatal(err)
	} else if a, err := Iterators(itrs).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if diff := cmp.Diff(a, [][]query.Point{
		{&query.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 0 * Second, Nil: true}},
		{&query.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 10 * Second, Value: 2, Aggregated: 1}},
		{&query.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 20 * Second, Value: 3}},
		{&query.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 30 * Second, Value: 4}},
		{&query.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 40 * Second, Value: 5}},
		{&query.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 50 * Second, Value: 6}},
		{&query.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 60 * Second, Value: 7, Aggregated: 1}},
	}); diff != "" {
		t.Fatalf("unexpected points:\n%s", diff)
	}
}

func TestSelect_Fill_Linear_Float_MultipleSeries(t *testing.T) {
	var ic IteratorCreator
	ic.CreateIteratorFn = func(m *influxql.Measurement, opt query.IteratorOptions) (query.Iterator, error) {
		if m.Name != "cpu" {
			t.Fatalf("unexpected source: %s", m.Name)
		}
		return query.NewCallIterator(&FloatIterator{Points: []query.FloatPoint{
			{Name: "cpu", Tags: ParseTags("host=A"), Time: 12 * Second, Value: 2},
			{Name: "cpu", Tags: ParseTags("host=B"), Time: 32 * Second, Value: 4},
		}}, opt)
	}

	// Execute selection.
	itrs, err := query.Select(MustParseSelectStatement(`SELECT mean(value) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:01:00Z' GROUP BY host, time(10s) fill(linear)`), &ic, nil)
	if err != nil {
		t.Fatal(err)
	} else if a, err := Iterators(itrs).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if diff := cmp.Diff(a, [][]query.Point{
		{&query.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 0 * Second, Nil: true}},
		{&query.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 10 * Second, Value: 2, Aggregated: 1}},
		{&query.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 20 * Second, Nil: true}},
		{&query.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 30 * Second, Nil: true}},
		{&query.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 40 * Second, Nil: true}},
		{&query.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 50 * Second, Nil: true}},
		{&query.FloatPoint{Name: "cpu", Tags: ParseTags("host=B"), Time: 0 * Second, Nil: true}},
		{&query.FloatPoint{Name: "cpu", Tags: ParseTags("host=B"), Time: 10 * Second, Nil: true}},
		{&query.FloatPoint{Name: "cpu", Tags: ParseTags("host=B"), Time: 20 * Second, Nil: true}},
		{&query.FloatPoint{Name: "cpu", Tags: ParseTags("host=B"), Time: 30 * Second, Value: 4, Aggregated: 1}},
		{&query.FloatPoint{Name: "cpu", Tags: ParseTags("host=B"), Time: 40 * Second, Nil: true}},
		{&query.FloatPoint{Name: "cpu", Tags: ParseTags("host=B"), Time: 50 * Second, Nil: true}},
	}); diff != "" {
		t.Fatalf("unexpected points:\n%s", diff)
	}
}

// Ensure a SELECT query with a fill(linear) statement can be executed for integers.
func TestSelect_Fill_Linear_Integer_One(t *testing.T) {
	var ic IteratorCreator
	ic.CreateIteratorFn = func(m *influxql.Measurement, opt query.IteratorOptions) (query.Iterator, error) {
		if m.Name != "cpu" {
			t.Fatalf("unexpected source: %s", m.Name)
		}
		return query.NewCallIterator(&IntegerIterator{Points: []query.IntegerPoint{
			{Name: "cpu", Tags: ParseTags("host=A"), Time: 12 * Second, Value: 1},
			{Name: "cpu", Tags: ParseTags("host=A"), Time: 32 * Second, Value: 4},
		}}, opt)
	}

	// Execute selection.
	itrs, err := query.Select(MustParseSelectStatement(`SELECT max(value) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:01:00Z' GROUP BY host, time(10s) fill(linear)`), &ic, nil)
	if err != nil {
		t.Fatal(err)
	} else if a, err := Iterators(itrs).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if diff := cmp.Diff(a, [][]query.Point{
		{&query.IntegerPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 0 * Second, Nil: true}},
		{&query.IntegerPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 10 * Second, Value: 1, Aggregated: 1}},
		{&query.IntegerPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 20 * Second, Value: 2}},
		{&query.IntegerPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 30 * Second, Value: 4, Aggregated: 1}},
		{&query.IntegerPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 40 * Second, Nil: true}},
		{&query.IntegerPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 50 * Second, Nil: true}},
	}); diff != "" {
		t.Fatalf("unexpected points:\n%s", diff)
	}
}

func TestSelect_Fill_Linear_Integer_Many(t *testing.T) {
	var ic IteratorCreator
	ic.CreateIteratorFn = func(m *influxql.Measurement, opt query.IteratorOptions) (query.Iterator, error) {
		if m.Name != "cpu" {
			t.Fatalf("unexpected source: %s", m.Name)
		}
		return query.NewCallIterator(&IntegerIterator{Points: []query.IntegerPoint{
			{Name: "cpu", Tags: ParseTags("host=A"), Time: 12 * Second, Value: 1},
			{Name: "cpu", Tags: ParseTags("host=A"), Time: 72 * Second, Value: 10},
		}}, opt)
	}

	// Execute selection.
	itrs, err := query.Select(MustParseSelectStatement(`SELECT max(value) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:01:20Z' GROUP BY host, time(10s) fill(linear)`), &ic, nil)
	if err != nil {
		t.Fatal(err)
	} else if a, err := Iterators(itrs).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if diff := cmp.Diff(a, [][]query.Point{
		{&query.IntegerPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 0 * Second, Nil: true}},
		{&query.IntegerPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 10 * Second, Value: 1, Aggregated: 1}},
		{&query.IntegerPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 20 * Second, Value: 2}},
		{&query.IntegerPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 30 * Second, Value: 4}},
		{&query.IntegerPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 40 * Second, Value: 5}},
		{&query.IntegerPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 50 * Second, Value: 7}},
		{&query.IntegerPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 60 * Second, Value: 8}},
		{&query.IntegerPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 70 * Second, Value: 10, Aggregated: 1}},
	}); diff != "" {
		t.Fatalf("unexpected points:\n%s", diff)
	}
}

func TestSelect_Fill_Linear_Integer_MultipleSeries(t *testing.T) {
	var ic IteratorCreator
	ic.CreateIteratorFn = func(m *influxql.Measurement, opt query.IteratorOptions) (query.Iterator, error) {
		if m.Name != "cpu" {
			t.Fatalf("unexpected source: %s", m.Name)
		}
		return query.NewCallIterator(&IntegerIterator{Points: []query.IntegerPoint{
			{Name: "cpu", Tags: ParseTags("host=A"), Time: 12 * Second, Value: 2},
			{Name: "cpu", Tags: ParseTags("host=B"), Time: 32 * Second, Value: 4},
		}}, opt)
	}

	// Execute selection.
	itrs, err := query.Select(MustParseSelectStatement(`SELECT max(value) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:01:00Z' GROUP BY host, time(10s) fill(linear)`), &ic, nil)
	if err != nil {
		t.Fatal(err)
	} else if a, err := Iterators(itrs).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if diff := cmp.Diff(a, [][]query.Point{
		{&query.IntegerPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 0 * Second, Nil: true}},
		{&query.IntegerPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 10 * Second, Value: 2, Aggregated: 1}},
		{&query.IntegerPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 20 * Second, Nil: true}},
		{&query.IntegerPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 30 * Second, Nil: true}},
		{&query.IntegerPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 40 * Second, Nil: true}},
		{&query.IntegerPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 50 * Second, Nil: true}},
		{&query.IntegerPoint{Name: "cpu", Tags: ParseTags("host=B"), Time: 0 * Second, Nil: true}},
		{&query.IntegerPoint{Name: "cpu", Tags: ParseTags("host=B"), Time: 10 * Second, Nil: true}},
		{&query.IntegerPoint{Name: "cpu", Tags: ParseTags("host=B"), Time: 20 * Second, Nil: true}},
		{&query.IntegerPoint{Name: "cpu", Tags: ParseTags("host=B"), Time: 30 * Second, Value: 4, Aggregated: 1}},
		{&query.IntegerPoint{Name: "cpu", Tags: ParseTags("host=B"), Time: 40 * Second, Nil: true}},
		{&query.IntegerPoint{Name: "cpu", Tags: ParseTags("host=B"), Time: 50 * Second, Nil: true}},
	}); diff != "" {
		t.Fatalf("unexpected points:\n%s", diff)
	}
}

// Ensure a SELECT stddev() query can be executed.
func TestSelect_Stddev_Float(t *testing.T) {
	var ic IteratorCreator
	ic.CreateIteratorFn = func(m *influxql.Measurement, opt query.IteratorOptions) (query.Iterator, error) {
		if m.Name != "cpu" {
			t.Fatalf("unexpected source: %s", m.Name)
		}
		return query.Iterators{
			&FloatIterator{Points: []query.FloatPoint{
				{Name: "cpu", Tags: ParseTags("region=west,host=A"), Time: 0 * Second, Value: 20},
				{Name: "cpu", Tags: ParseTags("region=west,host=A"), Time: 11 * Second, Value: 3},
				{Name: "cpu", Tags: ParseTags("region=west,host=A"), Time: 31 * Second, Value: 100},
			}},
			&FloatIterator{Points: []query.FloatPoint{
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 5 * Second, Value: 10},
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 50 * Second, Value: 1},
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 51 * Second, Value: 2},
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 52 * Second, Value: 3},
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 53 * Second, Value: 4},
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 53 * Second, Value: 5},
			}},
			&FloatIterator{Points: []query.FloatPoint{
				{Name: "cpu", Tags: ParseTags("region=east,host=A"), Time: 9 * Second, Value: 19},
				{Name: "cpu", Tags: ParseTags("region=east,host=A"), Time: 10 * Second, Value: 2},
			}},
		}.Merge(opt)
	}

	// Execute selection.
	itrs, err := query.Select(MustParseSelectStatement(`SELECT stddev(value) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY time(10s), host fill(none)`), &ic, nil)
	if err != nil {
		t.Fatal(err)
	} else if a, err := Iterators(itrs).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if diff := cmp.Diff(a, [][]query.Point{
		{&query.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 0 * Second, Value: 0.7071067811865476}},
		{&query.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 10 * Second, Value: 0.7071067811865476}},
		{&query.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 30 * Second, Nil: true}},
		{&query.FloatPoint{Name: "cpu", Tags: ParseTags("host=B"), Time: 0 * Second, Nil: true}},
		{&query.FloatPoint{Name: "cpu", Tags: ParseTags("host=B"), Time: 50 * Second, Value: 1.5811388300841898}},
	}); diff != "" {
		t.Fatalf("unexpected points:\n%s", diff)
	}
}

// Ensure a SELECT stddev() query can be executed.
func TestSelect_Stddev_Integer(t *testing.T) {
	var ic IteratorCreator
	ic.CreateIteratorFn = func(m *influxql.Measurement, opt query.IteratorOptions) (query.Iterator, error) {
		if m.Name != "cpu" {
			t.Fatalf("unexpected source: %s", m.Name)
		}
		return query.Iterators{
			&IntegerIterator{Points: []query.IntegerPoint{
				{Name: "cpu", Tags: ParseTags("region=west,host=A"), Time: 0 * Second, Value: 20},
				{Name: "cpu", Tags: ParseTags("region=west,host=A"), Time: 11 * Second, Value: 3},
				{Name: "cpu", Tags: ParseTags("region=west,host=A"), Time: 31 * Second, Value: 100},
			}},
			&IntegerIterator{Points: []query.IntegerPoint{
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 5 * Second, Value: 10},
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 50 * Second, Value: 1},
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 51 * Second, Value: 2},
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 52 * Second, Value: 3},
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 53 * Second, Value: 4},
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 53 * Second, Value: 5},
			}},
			&IntegerIterator{Points: []query.IntegerPoint{
				{Name: "cpu", Tags: ParseTags("region=east,host=A"), Time: 9 * Second, Value: 19},
				{Name: "cpu", Tags: ParseTags("region=east,host=A"), Time: 10 * Second, Value: 2},
			}},
		}.Merge(opt)
	}

	// Execute selection.
	itrs, err := query.Select(MustParseSelectStatement(`SELECT stddev(value) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY time(10s), host fill(none)`), &ic, nil)
	if err != nil {
		t.Fatal(err)
	} else if a, err := Iterators(itrs).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if diff := cmp.Diff(a, [][]query.Point{
		{&query.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 0 * Second, Value: 0.7071067811865476}},
		{&query.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 10 * Second, Value: 0.7071067811865476}},
		{&query.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 30 * Second, Nil: true}},
		{&query.FloatPoint{Name: "cpu", Tags: ParseTags("host=B"), Time: 0 * Second, Nil: true}},
		{&query.FloatPoint{Name: "cpu", Tags: ParseTags("host=B"), Time: 50 * Second, Value: 1.5811388300841898}},
	}); diff != "" {
		t.Fatalf("unexpected points:\n%s", diff)
	}
}

// Ensure a SELECT spread() query can be executed.
func TestSelect_Spread_Float(t *testing.T) {
	var ic IteratorCreator
	ic.CreateIteratorFn = func(m *influxql.Measurement, opt query.IteratorOptions) (query.Iterator, error) {
		if m.Name != "cpu" {
			t.Fatalf("unexpected source: %s", m.Name)
		}
		return query.Iterators{
			&FloatIterator{Points: []query.FloatPoint{
				{Name: "cpu", Tags: ParseTags("region=west,host=A"), Time: 0 * Second, Value: 20},
				{Name: "cpu", Tags: ParseTags("region=west,host=A"), Time: 11 * Second, Value: 3},
				{Name: "cpu", Tags: ParseTags("region=west,host=A"), Time: 31 * Second, Value: 100},
			}},
			&FloatIterator{Points: []query.FloatPoint{
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 5 * Second, Value: 10},
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 50 * Second, Value: 1},
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 51 * Second, Value: 2},
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 52 * Second, Value: 3},
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 53 * Second, Value: 4},
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 53 * Second, Value: 5},
			}},
			&FloatIterator{Points: []query.FloatPoint{
				{Name: "cpu", Tags: ParseTags("region=east,host=A"), Time: 9 * Second, Value: 19},
				{Name: "cpu", Tags: ParseTags("region=east,host=A"), Time: 10 * Second, Value: 2},
			}},
		}.Merge(opt)
	}

	// Execute selection.
	itrs, err := query.Select(MustParseSelectStatement(`SELECT spread(value) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY time(10s), host fill(none)`), &ic, nil)
	if err != nil {
		t.Fatal(err)
	} else if a, err := Iterators(itrs).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if diff := cmp.Diff(a, [][]query.Point{
		{&query.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 0 * Second, Value: 1}},
		{&query.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 10 * Second, Value: 1}},
		{&query.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 30 * Second, Value: 0}},
		{&query.FloatPoint{Name: "cpu", Tags: ParseTags("host=B"), Time: 0 * Second, Value: 0}},
		{&query.FloatPoint{Name: "cpu", Tags: ParseTags("host=B"), Time: 50 * Second, Value: 4}},
	}); diff != "" {
		t.Fatalf("unexpected points:\n%s", diff)
	}
}

// Ensure a SELECT spread() query can be executed.
func TestSelect_Spread_Integer(t *testing.T) {
	var ic IteratorCreator
	ic.CreateIteratorFn = func(m *influxql.Measurement, opt query.IteratorOptions) (query.Iterator, error) {
		if m.Name != "cpu" {
			t.Fatalf("unexpected source: %s", m.Name)
		}
		return query.Iterators{
			&IntegerIterator{Points: []query.IntegerPoint{
				{Name: "cpu", Tags: ParseTags("region=west,host=A"), Time: 0 * Second, Value: 20},
				{Name: "cpu", Tags: ParseTags("region=west,host=A"), Time: 11 * Second, Value: 3},
				{Name: "cpu", Tags: ParseTags("region=west,host=A"), Time: 31 * Second, Value: 100},
			}},
			&IntegerIterator{Points: []query.IntegerPoint{
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 5 * Second, Value: 10},
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 50 * Second, Value: 1},
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 51 * Second, Value: 2},
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 52 * Second, Value: 3},
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 53 * Second, Value: 4},
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 53 * Second, Value: 5},
			}},
			&IntegerIterator{Points: []query.IntegerPoint{
				{Name: "cpu", Tags: ParseTags("region=east,host=A"), Time: 9 * Second, Value: 19},
				{Name: "cpu", Tags: ParseTags("region=east,host=A"), Time: 10 * Second, Value: 2},
			}},
		}.Merge(opt)
	}

	// Execute selection.
	itrs, err := query.Select(MustParseSelectStatement(`SELECT spread(value) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY time(10s), host fill(none)`), &ic, nil)
	if err != nil {
		t.Fatal(err)
	} else if a, err := Iterators(itrs).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if diff := cmp.Diff(a, [][]query.Point{
		{&query.IntegerPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 0 * Second, Value: 1}},
		{&query.IntegerPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 10 * Second, Value: 1}},
		{&query.IntegerPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 30 * Second, Value: 0}},
		{&query.IntegerPoint{Name: "cpu", Tags: ParseTags("host=B"), Time: 0 * Second, Value: 0}},
		{&query.IntegerPoint{Name: "cpu", Tags: ParseTags("host=B"), Time: 50 * Second, Value: 4}},
	}); diff != "" {
		t.Fatalf("unexpected points:\n%s", diff)
	}
}

// Ensure a SELECT percentile() query can be executed.
func TestSelect_Percentile_Float(t *testing.T) {
	var ic IteratorCreator
	ic.CreateIteratorFn = func(m *influxql.Measurement, opt query.IteratorOptions) (query.Iterator, error) {
		if m.Name != "cpu" {
			t.Fatalf("unexpected source: %s", m.Name)
		}
		return query.Iterators{
			&FloatIterator{Points: []query.FloatPoint{
				{Name: "cpu", Tags: ParseTags("region=west,host=A"), Time: 0 * Second, Value: 20},
				{Name: "cpu", Tags: ParseTags("region=west,host=A"), Time: 11 * Second, Value: 3},
				{Name: "cpu", Tags: ParseTags("region=west,host=A"), Time: 31 * Second, Value: 100},
			}},
			&FloatIterator{Points: []query.FloatPoint{
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 5 * Second, Value: 10},
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
			}},
			&FloatIterator{Points: []query.FloatPoint{
				{Name: "cpu", Tags: ParseTags("region=east,host=A"), Time: 9 * Second, Value: 19},
				{Name: "cpu", Tags: ParseTags("region=east,host=A"), Time: 10 * Second, Value: 2},
			}},
		}.Merge(opt)
	}

	// Execute selection.
	itrs, err := query.Select(MustParseSelectStatement(`SELECT percentile(value, 90) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY time(10s), host fill(none)`), &ic, nil)
	if err != nil {
		t.Fatal(err)
	} else if a, err := Iterators(itrs).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if diff := cmp.Diff(a, [][]query.Point{
		{&query.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 0 * Second, Value: 20}},
		{&query.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 10 * Second, Value: 3}},
		{&query.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 30 * Second, Value: 100}},
		{&query.FloatPoint{Name: "cpu", Tags: ParseTags("host=B"), Time: 0 * Second, Value: 10}},
		{&query.FloatPoint{Name: "cpu", Tags: ParseTags("host=B"), Time: 50 * Second, Value: 9}},
	}); diff != "" {
		t.Fatalf("unexpected points:\n%s", diff)
	}
}

// Ensure a SELECT percentile() query can be executed.
func TestSelect_Percentile_Integer(t *testing.T) {
	var ic IteratorCreator
	ic.CreateIteratorFn = func(m *influxql.Measurement, opt query.IteratorOptions) (query.Iterator, error) {
		if m.Name != "cpu" {
			t.Fatalf("unexpected source: %s", m.Name)
		}
		return query.Iterators{
			&IntegerIterator{Points: []query.IntegerPoint{
				{Name: "cpu", Tags: ParseTags("region=west,host=A"), Time: 0 * Second, Value: 20},
				{Name: "cpu", Tags: ParseTags("region=west,host=A"), Time: 11 * Second, Value: 3},
				{Name: "cpu", Tags: ParseTags("region=west,host=A"), Time: 31 * Second, Value: 100},
			}},
			&IntegerIterator{Points: []query.IntegerPoint{
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 5 * Second, Value: 10},
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
			}},
			&IntegerIterator{Points: []query.IntegerPoint{
				{Name: "cpu", Tags: ParseTags("region=east,host=A"), Time: 9 * Second, Value: 19},
				{Name: "cpu", Tags: ParseTags("region=east,host=A"), Time: 10 * Second, Value: 2},
			}},
		}.Merge(opt)
	}

	// Execute selection.
	itrs, err := query.Select(MustParseSelectStatement(`SELECT percentile(value, 90) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY time(10s), host fill(none)`), &ic, nil)
	if err != nil {
		t.Fatal(err)
	} else if a, err := Iterators(itrs).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if diff := cmp.Diff(a, [][]query.Point{
		{&query.IntegerPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 0 * Second, Value: 20}},
		{&query.IntegerPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 10 * Second, Value: 3}},
		{&query.IntegerPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 30 * Second, Value: 100}},
		{&query.IntegerPoint{Name: "cpu", Tags: ParseTags("host=B"), Time: 0 * Second, Value: 10}},
		{&query.IntegerPoint{Name: "cpu", Tags: ParseTags("host=B"), Time: 50 * Second, Value: 9}},
	}); diff != "" {
		t.Fatalf("unexpected points:\n%s", diff)
	}
}

// Ensure a SELECT sample() query can be executed.
func TestSelect_Sample_Float(t *testing.T) {
	var ic IteratorCreator
	ic.CreateIteratorFn = func(m *influxql.Measurement, opt query.IteratorOptions) (query.Iterator, error) {
		if m.Name != "cpu" {
			t.Fatalf("unexpected source: %s", m.Name)
		}
		return query.Iterators{
			&FloatIterator{Points: []query.FloatPoint{
				{Name: "cpu", Tags: ParseTags("region=west,host=A"), Time: 0 * Second, Value: 20},
				{Name: "cpu", Tags: ParseTags("region=west,host=A"), Time: 5 * Second, Value: 10},
			}},
			&FloatIterator{Points: []query.FloatPoint{
				{Name: "cpu", Tags: ParseTags("region=east,host=B"), Time: 10 * Second, Value: 19},
				{Name: "cpu", Tags: ParseTags("region=east,host=B"), Time: 15 * Second, Value: 2},
			}},
		}.Merge(opt)
	}

	// Execute selection.
	itrs, err := query.Select(MustParseSelectStatement(`SELECT sample(value, 2) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY time(10s), host fill(none)`), &ic, nil)
	if err != nil {
		t.Fatal(err)
	} else if a, err := Iterators(itrs).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if diff := cmp.Diff(a, [][]query.Point{
		{&query.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 0 * Second, Value: 20}},
		{&query.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 5 * Second, Value: 10}},
		{&query.FloatPoint{Name: "cpu", Tags: ParseTags("host=B"), Time: 10 * Second, Value: 19}},
		{&query.FloatPoint{Name: "cpu", Tags: ParseTags("host=B"), Time: 15 * Second, Value: 2}},
	}); diff != "" {
		t.Fatalf("unexpected points:\n%s", diff)
	}
}

// Ensure a SELECT sample() query can be executed.
func TestSelect_Sample_Integer(t *testing.T) {
	var ic IteratorCreator
	ic.CreateIteratorFn = func(m *influxql.Measurement, opt query.IteratorOptions) (query.Iterator, error) {
		if m.Name != "cpu" {
			t.Fatalf("unexpected source: %s", m.Name)
		}
		return query.Iterators{
			&IntegerIterator{Points: []query.IntegerPoint{
				{Name: "cpu", Tags: ParseTags("region=west,host=A"), Time: 0 * Second, Value: 20},
				{Name: "cpu", Tags: ParseTags("region=west,host=A"), Time: 5 * Second, Value: 10},
			}},
			&IntegerIterator{Points: []query.IntegerPoint{
				{Name: "cpu", Tags: ParseTags("region=east,host=B"), Time: 10 * Second, Value: 19},
				{Name: "cpu", Tags: ParseTags("region=east,host=B"), Time: 15 * Second, Value: 2},
			}},
		}.Merge(opt)
	}

	// Execute selection.
	itrs, err := query.Select(MustParseSelectStatement(`SELECT sample(value, 2) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY time(10s), host fill(none)`), &ic, nil)
	if err != nil {
		t.Fatal(err)
	} else if a, err := Iterators(itrs).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if diff := cmp.Diff(a, [][]query.Point{
		{&query.IntegerPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 0 * Second, Value: 20}},
		{&query.IntegerPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 5 * Second, Value: 10}},
		{&query.IntegerPoint{Name: "cpu", Tags: ParseTags("host=B"), Time: 10 * Second, Value: 19}},
		{&query.IntegerPoint{Name: "cpu", Tags: ParseTags("host=B"), Time: 15 * Second, Value: 2}},
	}); diff != "" {
		t.Fatalf("unexpected points:\n%s", diff)
	}
}

// Ensure a SELECT sample() query can be executed.
func TestSelect_Sample_Boolean(t *testing.T) {
	var ic IteratorCreator
	ic.CreateIteratorFn = func(m *influxql.Measurement, opt query.IteratorOptions) (query.Iterator, error) {
		if m.Name != "cpu" {
			t.Fatalf("unexpected source: %s", m.Name)
		}
		return query.Iterators{
			&BooleanIterator{Points: []query.BooleanPoint{
				{Name: "cpu", Tags: ParseTags("region=west,host=A"), Time: 0 * Second, Value: true},
				{Name: "cpu", Tags: ParseTags("region=west,host=A"), Time: 5 * Second, Value: false},
			}},
			&BooleanIterator{Points: []query.BooleanPoint{
				{Name: "cpu", Tags: ParseTags("region=east,host=B"), Time: 10 * Second, Value: false},
				{Name: "cpu", Tags: ParseTags("region=east,host=B"), Time: 15 * Second, Value: true},
			}},
		}.Merge(opt)
	}

	// Execute selection.
	itrs, err := query.Select(MustParseSelectStatement(`SELECT sample(value, 2) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY time(10s), host fill(none)`), &ic, nil)
	if err != nil {
		t.Fatal(err)
	} else if a, err := Iterators(itrs).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if diff := cmp.Diff(a, [][]query.Point{
		{&query.BooleanPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 0 * Second, Value: true}},
		{&query.BooleanPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 5 * Second, Value: false}},
		{&query.BooleanPoint{Name: "cpu", Tags: ParseTags("host=B"), Time: 10 * Second, Value: false}},
		{&query.BooleanPoint{Name: "cpu", Tags: ParseTags("host=B"), Time: 15 * Second, Value: true}},
	}); diff != "" {
		t.Fatalf("unexpected points:\n%s", diff)
	}
}

// Ensure a SELECT sample() query can be executed.
func TestSelect_Sample_String(t *testing.T) {
	var ic IteratorCreator
	ic.CreateIteratorFn = func(m *influxql.Measurement, opt query.IteratorOptions) (query.Iterator, error) {
		if m.Name != "cpu" {
			t.Fatalf("unexpected source: %s", m.Name)
		}
		return query.Iterators{
			&StringIterator{Points: []query.StringPoint{
				{Name: "cpu", Tags: ParseTags("region=west,host=A"), Time: 0 * Second, Value: "a"},
				{Name: "cpu", Tags: ParseTags("region=west,host=A"), Time: 5 * Second, Value: "b"},
			}},
			&StringIterator{Points: []query.StringPoint{
				{Name: "cpu", Tags: ParseTags("region=east,host=B"), Time: 10 * Second, Value: "c"},
				{Name: "cpu", Tags: ParseTags("region=east,host=B"), Time: 15 * Second, Value: "d"},
			}},
		}.Merge(opt)
	}

	// Execute selection.
	itrs, err := query.Select(MustParseSelectStatement(`SELECT sample(value, 2) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY time(10s), host fill(none)`), &ic, nil)
	if err != nil {
		t.Fatal(err)
	} else if a, err := Iterators(itrs).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if diff := cmp.Diff(a, [][]query.Point{
		{&query.StringPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 0 * Second, Value: "a"}},
		{&query.StringPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 5 * Second, Value: "b"}},
		{&query.StringPoint{Name: "cpu", Tags: ParseTags("host=B"), Time: 10 * Second, Value: "c"}},
		{&query.StringPoint{Name: "cpu", Tags: ParseTags("host=B"), Time: 15 * Second, Value: "d"}},
	}); diff != "" {
		t.Fatalf("unexpected points:\n%s", diff)
	}
}

// Ensure a simple raw SELECT statement can be executed.
func TestSelect_Raw(t *testing.T) {
	// Mock two iterators -- one for each value in the query.
	var ic IteratorCreator
	ic.CreateIteratorFn = func(m *influxql.Measurement, opt query.IteratorOptions) (query.Iterator, error) {
		if m.Name != "cpu" {
			t.Fatalf("unexpected source: %s", m.Name)
		}
		if !reflect.DeepEqual(opt.Aux, []influxql.VarRef{{Val: "v1", Type: influxql.Float}, {Val: "v2", Type: influxql.Float}}) {
			t.Fatalf("unexpected options: %s", spew.Sdump(opt.Expr))

		}
		return &FloatIterator{Points: []query.FloatPoint{
			{Time: 0, Aux: []interface{}{float64(1), nil}},
			{Time: 1, Aux: []interface{}{nil, float64(2)}},
			{Time: 5, Aux: []interface{}{float64(3), float64(4)}},
		}}, nil
	}

	// Execute selection.
	itrs, err := query.Select(MustParseSelectStatement(`SELECT v1::float, v2::float FROM cpu`), &ic, nil)
	if err != nil {
		t.Fatal(err)
	} else if a, err := Iterators(itrs).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if diff := cmp.Diff(a, [][]query.Point{
		{
			&query.FloatPoint{Time: 0, Value: 1},
			&query.FloatPoint{Time: 0, Nil: true},
		},
		{
			&query.FloatPoint{Time: 1, Nil: true},
			&query.FloatPoint{Time: 1, Value: 2},
		},
		{
			&query.FloatPoint{Time: 5, Value: 3},
			&query.FloatPoint{Time: 5, Value: 4},
		},
	}); diff != "" {
		t.Fatalf("unexpected points:\n%s", diff)
	}
}

// Ensure a SELECT binary expr queries can be executed as floats.
func TestSelect_BinaryExpr_Float(t *testing.T) {
	var ic IteratorCreator
	ic.CreateIteratorFn = func(m *influxql.Measurement, opt query.IteratorOptions) (query.Iterator, error) {
		if m.Name != "cpu" {
			t.Fatalf("unexpected source: %s", m.Name)
		}
		makeAuxFields := func(value float64) []interface{} {
			aux := make([]interface{}, len(opt.Aux))
			for i := range aux {
				aux[i] = value
			}
			return aux
		}
		return &FloatIterator{Points: []query.FloatPoint{
			{Name: "cpu", Time: 0 * Second, Value: 20, Aux: makeAuxFields(20)},
			{Name: "cpu", Time: 5 * Second, Value: 10, Aux: makeAuxFields(10)},
			{Name: "cpu", Time: 9 * Second, Value: 19, Aux: makeAuxFields(19)},
		}}, nil
	}
	ic.FieldDimensionsFn = func(m *influxql.Measurement) (map[string]influxql.DataType, map[string]struct{}, error) {
		if m.Name != "cpu" {
			t.Fatalf("unexpected source: %s", m.Name)
		}
		return map[string]influxql.DataType{"value": influxql.Float}, nil, nil
	}

	for _, test := range []struct {
		Name      string
		Statement string
		Points    [][]query.Point
	}{
		{
			Name:      "rhs binary add number",
			Statement: `SELECT value + 2.0 FROM cpu`,
			Points: [][]query.Point{
				{&query.FloatPoint{Name: "cpu", Time: 0 * Second, Value: 22}},
				{&query.FloatPoint{Name: "cpu", Time: 5 * Second, Value: 12}},
				{&query.FloatPoint{Name: "cpu", Time: 9 * Second, Value: 21}},
			},
		},
		{
			Name:      "rhs binary add integer",
			Statement: `SELECT value + 2 FROM cpu`,
			Points: [][]query.Point{
				{&query.FloatPoint{Name: "cpu", Time: 0 * Second, Value: 22}},
				{&query.FloatPoint{Name: "cpu", Time: 5 * Second, Value: 12}},
				{&query.FloatPoint{Name: "cpu", Time: 9 * Second, Value: 21}},
			},
		},
		{
			Name:      "lhs binary add number",
			Statement: `SELECT 2.0 + value FROM cpu`,
			Points: [][]query.Point{
				{&query.FloatPoint{Name: "cpu", Time: 0 * Second, Value: 22}},
				{&query.FloatPoint{Name: "cpu", Time: 5 * Second, Value: 12}},
				{&query.FloatPoint{Name: "cpu", Time: 9 * Second, Value: 21}},
			},
		},
		{
			Name:      "lhs binary add integer",
			Statement: `SELECT 2 + value FROM cpu`,
			Points: [][]query.Point{
				{&query.FloatPoint{Name: "cpu", Time: 0 * Second, Value: 22}},
				{&query.FloatPoint{Name: "cpu", Time: 5 * Second, Value: 12}},
				{&query.FloatPoint{Name: "cpu", Time: 9 * Second, Value: 21}},
			},
		},
		{
			Name:      "two variable binary add",
			Statement: `SELECT value + value FROM cpu`,
			Points: [][]query.Point{
				{&query.FloatPoint{Name: "cpu", Time: 0 * Second, Value: 40}},
				{&query.FloatPoint{Name: "cpu", Time: 5 * Second, Value: 20}},
				{&query.FloatPoint{Name: "cpu", Time: 9 * Second, Value: 38}},
			},
		},
		{
			Name:      "rhs binary multiply number",
			Statement: `SELECT value * 2.0 FROM cpu`,
			Points: [][]query.Point{
				{&query.FloatPoint{Name: "cpu", Time: 0 * Second, Value: 40}},
				{&query.FloatPoint{Name: "cpu", Time: 5 * Second, Value: 20}},
				{&query.FloatPoint{Name: "cpu", Time: 9 * Second, Value: 38}},
			},
		},
		{
			Name:      "rhs binary multiply integer",
			Statement: `SELECT value * 2 FROM cpu`,
			Points: [][]query.Point{
				{&query.FloatPoint{Name: "cpu", Time: 0 * Second, Value: 40}},
				{&query.FloatPoint{Name: "cpu", Time: 5 * Second, Value: 20}},
				{&query.FloatPoint{Name: "cpu", Time: 9 * Second, Value: 38}},
			},
		},
		{
			Name:      "lhs binary multiply number",
			Statement: `SELECT 2.0 * value FROM cpu`,
			Points: [][]query.Point{
				{&query.FloatPoint{Name: "cpu", Time: 0 * Second, Value: 40}},
				{&query.FloatPoint{Name: "cpu", Time: 5 * Second, Value: 20}},
				{&query.FloatPoint{Name: "cpu", Time: 9 * Second, Value: 38}},
			},
		},
		{
			Name:      "lhs binary multiply integer",
			Statement: `SELECT 2 * value FROM cpu`,
			Points: [][]query.Point{
				{&query.FloatPoint{Name: "cpu", Time: 0 * Second, Value: 40}},
				{&query.FloatPoint{Name: "cpu", Time: 5 * Second, Value: 20}},
				{&query.FloatPoint{Name: "cpu", Time: 9 * Second, Value: 38}},
			},
		},
		{
			Name:      "two variable binary multiply",
			Statement: `SELECT value * value FROM cpu`,
			Points: [][]query.Point{
				{&query.FloatPoint{Name: "cpu", Time: 0 * Second, Value: 400}},
				{&query.FloatPoint{Name: "cpu", Time: 5 * Second, Value: 100}},
				{&query.FloatPoint{Name: "cpu", Time: 9 * Second, Value: 361}},
			},
		},
		{
			Name:      "rhs binary subtract number",
			Statement: `SELECT value - 2.0 FROM cpu`,
			Points: [][]query.Point{
				{&query.FloatPoint{Name: "cpu", Time: 0 * Second, Value: 18}},
				{&query.FloatPoint{Name: "cpu", Time: 5 * Second, Value: 8}},
				{&query.FloatPoint{Name: "cpu", Time: 9 * Second, Value: 17}},
			},
		},
		{
			Name:      "rhs binary subtract integer",
			Statement: `SELECT value - 2 FROM cpu`,
			Points: [][]query.Point{
				{&query.FloatPoint{Name: "cpu", Time: 0 * Second, Value: 18}},
				{&query.FloatPoint{Name: "cpu", Time: 5 * Second, Value: 8}},
				{&query.FloatPoint{Name: "cpu", Time: 9 * Second, Value: 17}},
			},
		},
		{
			Name:      "lhs binary subtract number",
			Statement: `SELECT 2.0 - value FROM cpu`,
			Points: [][]query.Point{
				{&query.FloatPoint{Name: "cpu", Time: 0 * Second, Value: -18}},
				{&query.FloatPoint{Name: "cpu", Time: 5 * Second, Value: -8}},
				{&query.FloatPoint{Name: "cpu", Time: 9 * Second, Value: -17}},
			},
		},
		{
			Name:      "lhs binary subtract integer",
			Statement: `SELECT 2 - value FROM cpu`,
			Points: [][]query.Point{
				{&query.FloatPoint{Name: "cpu", Time: 0 * Second, Value: -18}},
				{&query.FloatPoint{Name: "cpu", Time: 5 * Second, Value: -8}},
				{&query.FloatPoint{Name: "cpu", Time: 9 * Second, Value: -17}},
			},
		},
		{
			Name:      "two variable binary subtract",
			Statement: `SELECT value - value FROM cpu`,
			Points: [][]query.Point{
				{&query.FloatPoint{Name: "cpu", Time: 0 * Second, Value: 0}},
				{&query.FloatPoint{Name: "cpu", Time: 5 * Second, Value: 0}},
				{&query.FloatPoint{Name: "cpu", Time: 9 * Second, Value: 0}},
			},
		},
		{
			Name:      "rhs binary division number",
			Statement: `SELECT value / 2.0 FROM cpu`,
			Points: [][]query.Point{
				{&query.FloatPoint{Name: "cpu", Time: 0 * Second, Value: 10}},
				{&query.FloatPoint{Name: "cpu", Time: 5 * Second, Value: 5}},
				{&query.FloatPoint{Name: "cpu", Time: 9 * Second, Value: float64(19) / 2}},
			},
		},
		{
			Name:      "rhs binary division integer",
			Statement: `SELECT value / 2 FROM cpu`,
			Points: [][]query.Point{
				{&query.FloatPoint{Name: "cpu", Time: 0 * Second, Value: 10}},
				{&query.FloatPoint{Name: "cpu", Time: 5 * Second, Value: 5}},
				{&query.FloatPoint{Name: "cpu", Time: 9 * Second, Value: float64(19) / 2}},
			},
		},
		{
			Name:      "lhs binary division number",
			Statement: `SELECT 38.0 / value FROM cpu`,
			Points: [][]query.Point{
				{&query.FloatPoint{Name: "cpu", Time: 0 * Second, Value: 1.9}},
				{&query.FloatPoint{Name: "cpu", Time: 5 * Second, Value: 3.8}},
				{&query.FloatPoint{Name: "cpu", Time: 9 * Second, Value: 2}},
			},
		},
		{
			Name:      "lhs binary division integer",
			Statement: `SELECT 38 / value FROM cpu`,
			Points: [][]query.Point{
				{&query.FloatPoint{Name: "cpu", Time: 0 * Second, Value: 1.9}},
				{&query.FloatPoint{Name: "cpu", Time: 5 * Second, Value: 3.8}},
				{&query.FloatPoint{Name: "cpu", Time: 9 * Second, Value: 2}},
			},
		},
		{
			Name:      "two variable binary division",
			Statement: `SELECT value / value FROM cpu`,
			Points: [][]query.Point{
				{&query.FloatPoint{Name: "cpu", Time: 0 * Second, Value: 1}},
				{&query.FloatPoint{Name: "cpu", Time: 5 * Second, Value: 1}},
				{&query.FloatPoint{Name: "cpu", Time: 9 * Second, Value: 1}},
			},
		},
	} {
		stmt, err := MustParseSelectStatement(test.Statement).RewriteFields(&ic)
		if err != nil {
			t.Errorf("%s: rewrite error: %s", test.Name, err)
		}

		itrs, err := query.Select(stmt, &ic, nil)
		if err != nil {
			t.Errorf("%s: parse error: %s", test.Name, err)
		} else if a, err := Iterators(itrs).ReadAll(); err != nil {
			t.Fatalf("%s: unexpected error: %s", test.Name, err)
		} else if diff := cmp.Diff(a, test.Points); diff != "" {
			t.Errorf("%s: unexpected points:\n%s", test.Name, diff)
		}
	}
}

// Ensure a SELECT binary expr queries can be executed as integers.
func TestSelect_BinaryExpr_Integer(t *testing.T) {
	var ic IteratorCreator
	ic.CreateIteratorFn = func(m *influxql.Measurement, opt query.IteratorOptions) (query.Iterator, error) {
		if m.Name != "cpu" {
			t.Fatalf("unexpected source: %s", m.Name)
		}
		makeAuxFields := func(value int64) []interface{} {
			aux := make([]interface{}, len(opt.Aux))
			for i := range aux {
				aux[i] = value
			}
			return aux
		}
		return &IntegerIterator{Points: []query.IntegerPoint{
			{Name: "cpu", Time: 0 * Second, Value: 20, Aux: makeAuxFields(20)},
			{Name: "cpu", Time: 5 * Second, Value: 10, Aux: makeAuxFields(10)},
			{Name: "cpu", Time: 9 * Second, Value: 19, Aux: makeAuxFields(19)},
		}}, nil
	}
	ic.FieldDimensionsFn = func(m *influxql.Measurement) (map[string]influxql.DataType, map[string]struct{}, error) {
		if m.Name != "cpu" {
			t.Fatalf("unexpected source: %s", m.Name)
		}
		return map[string]influxql.DataType{"value": influxql.Integer}, nil, nil
	}

	for _, test := range []struct {
		Name      string
		Statement string
		Points    [][]query.Point
	}{
		{
			Name:      "rhs binary add number",
			Statement: `SELECT value + 2.0 FROM cpu`,
			Points: [][]query.Point{
				{&query.FloatPoint{Name: "cpu", Time: 0 * Second, Value: 22}},
				{&query.FloatPoint{Name: "cpu", Time: 5 * Second, Value: 12}},
				{&query.FloatPoint{Name: "cpu", Time: 9 * Second, Value: 21}},
			},
		},
		{
			Name:      "rhs binary add integer",
			Statement: `SELECT value + 2 FROM cpu`,
			Points: [][]query.Point{
				{&query.IntegerPoint{Name: "cpu", Time: 0 * Second, Value: 22}},
				{&query.IntegerPoint{Name: "cpu", Time: 5 * Second, Value: 12}},
				{&query.IntegerPoint{Name: "cpu", Time: 9 * Second, Value: 21}},
			},
		},
		{
			Name:      "lhs binary add number",
			Statement: `SELECT 2.0 + value FROM cpu`,
			Points: [][]query.Point{
				{&query.FloatPoint{Name: "cpu", Time: 0 * Second, Value: 22}},
				{&query.FloatPoint{Name: "cpu", Time: 5 * Second, Value: 12}},
				{&query.FloatPoint{Name: "cpu", Time: 9 * Second, Value: 21}},
			},
		},
		{
			Name:      "lhs binary add integer",
			Statement: `SELECT 2 + value FROM cpu`,
			Points: [][]query.Point{
				{&query.IntegerPoint{Name: "cpu", Time: 0 * Second, Value: 22}},
				{&query.IntegerPoint{Name: "cpu", Time: 5 * Second, Value: 12}},
				{&query.IntegerPoint{Name: "cpu", Time: 9 * Second, Value: 21}},
			},
		},
		{
			Name:      "two variable binary add",
			Statement: `SELECT value + value FROM cpu`,
			Points: [][]query.Point{
				{&query.IntegerPoint{Name: "cpu", Time: 0 * Second, Value: 40}},
				{&query.IntegerPoint{Name: "cpu", Time: 5 * Second, Value: 20}},
				{&query.IntegerPoint{Name: "cpu", Time: 9 * Second, Value: 38}},
			},
		},
		{
			Name:      "rhs binary multiply number",
			Statement: `SELECT value * 2.0 FROM cpu`,
			Points: [][]query.Point{
				{&query.FloatPoint{Name: "cpu", Time: 0 * Second, Value: 40}},
				{&query.FloatPoint{Name: "cpu", Time: 5 * Second, Value: 20}},
				{&query.FloatPoint{Name: "cpu", Time: 9 * Second, Value: 38}},
			},
		},
		{
			Name:      "rhs binary multiply integer",
			Statement: `SELECT value * 2 FROM cpu`,
			Points: [][]query.Point{
				{&query.IntegerPoint{Name: "cpu", Time: 0 * Second, Value: 40}},
				{&query.IntegerPoint{Name: "cpu", Time: 5 * Second, Value: 20}},
				{&query.IntegerPoint{Name: "cpu", Time: 9 * Second, Value: 38}},
			},
		},
		{
			Name:      "lhs binary multiply number",
			Statement: `SELECT 2.0 * value FROM cpu`,
			Points: [][]query.Point{
				{&query.FloatPoint{Name: "cpu", Time: 0 * Second, Value: 40}},
				{&query.FloatPoint{Name: "cpu", Time: 5 * Second, Value: 20}},
				{&query.FloatPoint{Name: "cpu", Time: 9 * Second, Value: 38}},
			},
		},
		{
			Name:      "lhs binary multiply integer",
			Statement: `SELECT 2 * value FROM cpu`,
			Points: [][]query.Point{
				{&query.IntegerPoint{Name: "cpu", Time: 0 * Second, Value: 40}},
				{&query.IntegerPoint{Name: "cpu", Time: 5 * Second, Value: 20}},
				{&query.IntegerPoint{Name: "cpu", Time: 9 * Second, Value: 38}},
			},
		},
		{
			Name:      "two variable binary multiply",
			Statement: `SELECT value * value FROM cpu`,
			Points: [][]query.Point{
				{&query.IntegerPoint{Name: "cpu", Time: 0 * Second, Value: 400}},
				{&query.IntegerPoint{Name: "cpu", Time: 5 * Second, Value: 100}},
				{&query.IntegerPoint{Name: "cpu", Time: 9 * Second, Value: 361}},
			},
		},
		{
			Name:      "rhs binary subtract number",
			Statement: `SELECT value - 2.0 FROM cpu`,
			Points: [][]query.Point{
				{&query.FloatPoint{Name: "cpu", Time: 0 * Second, Value: 18}},
				{&query.FloatPoint{Name: "cpu", Time: 5 * Second, Value: 8}},
				{&query.FloatPoint{Name: "cpu", Time: 9 * Second, Value: 17}},
			},
		},
		{
			Name:      "rhs binary subtract integer",
			Statement: `SELECT value - 2 FROM cpu`,
			Points: [][]query.Point{
				{&query.IntegerPoint{Name: "cpu", Time: 0 * Second, Value: 18}},
				{&query.IntegerPoint{Name: "cpu", Time: 5 * Second, Value: 8}},
				{&query.IntegerPoint{Name: "cpu", Time: 9 * Second, Value: 17}},
			},
		},
		{
			Name:      "lhs binary subtract number",
			Statement: `SELECT 2.0 - value FROM cpu`,
			Points: [][]query.Point{
				{&query.FloatPoint{Name: "cpu", Time: 0 * Second, Value: -18}},
				{&query.FloatPoint{Name: "cpu", Time: 5 * Second, Value: -8}},
				{&query.FloatPoint{Name: "cpu", Time: 9 * Second, Value: -17}},
			},
		},
		{
			Name:      "lhs binary subtract integer",
			Statement: `SELECT 2 - value FROM cpu`,
			Points: [][]query.Point{
				{&query.IntegerPoint{Name: "cpu", Time: 0 * Second, Value: -18}},
				{&query.IntegerPoint{Name: "cpu", Time: 5 * Second, Value: -8}},
				{&query.IntegerPoint{Name: "cpu", Time: 9 * Second, Value: -17}},
			},
		},
		{
			Name:      "two variable binary subtract",
			Statement: `SELECT value - value FROM cpu`,
			Points: [][]query.Point{
				{&query.IntegerPoint{Name: "cpu", Time: 0 * Second, Value: 0}},
				{&query.IntegerPoint{Name: "cpu", Time: 5 * Second, Value: 0}},
				{&query.IntegerPoint{Name: "cpu", Time: 9 * Second, Value: 0}},
			},
		},
		{
			Name:      "rhs binary division number",
			Statement: `SELECT value / 2.0 FROM cpu`,
			Points: [][]query.Point{
				{&query.FloatPoint{Name: "cpu", Time: 0 * Second, Value: 10}},
				{&query.FloatPoint{Name: "cpu", Time: 5 * Second, Value: 5}},
				{&query.FloatPoint{Name: "cpu", Time: 9 * Second, Value: 9.5}},
			},
		},
		{
			Name:      "rhs binary division integer",
			Statement: `SELECT value / 2 FROM cpu`,
			Points: [][]query.Point{
				{&query.FloatPoint{Name: "cpu", Time: 0 * Second, Value: 10}},
				{&query.FloatPoint{Name: "cpu", Time: 5 * Second, Value: 5}},
				{&query.FloatPoint{Name: "cpu", Time: 9 * Second, Value: float64(19) / 2}},
			},
		},
		{
			Name:      "lhs binary division number",
			Statement: `SELECT 38.0 / value FROM cpu`,
			Points: [][]query.Point{
				{&query.FloatPoint{Name: "cpu", Time: 0 * Second, Value: 1.9}},
				{&query.FloatPoint{Name: "cpu", Time: 5 * Second, Value: 3.8}},
				{&query.FloatPoint{Name: "cpu", Time: 9 * Second, Value: 2.0}},
			},
		},
		{
			Name:      "lhs binary division integer",
			Statement: `SELECT 38 / value FROM cpu`,
			Points: [][]query.Point{
				{&query.FloatPoint{Name: "cpu", Time: 0 * Second, Value: 1.9}},
				{&query.FloatPoint{Name: "cpu", Time: 5 * Second, Value: 3.8}},
				{&query.FloatPoint{Name: "cpu", Time: 9 * Second, Value: 2}},
			},
		},
		{
			Name:      "two variable binary division",
			Statement: `SELECT value / value FROM cpu`,
			Points: [][]query.Point{
				{&query.FloatPoint{Name: "cpu", Time: 0 * Second, Value: 1}},
				{&query.FloatPoint{Name: "cpu", Time: 5 * Second, Value: 1}},
				{&query.FloatPoint{Name: "cpu", Time: 9 * Second, Value: 1}},
			},
		},
		{
			Name:      "rhs binary bitwise-and integer",
			Statement: `SELECT value & 254 FROM cpu`,
			Points: [][]query.Point{
				{&query.IntegerPoint{Name: "cpu", Time: 0 * Second, Value: 20}},
				{&query.IntegerPoint{Name: "cpu", Time: 5 * Second, Value: 10}},
				{&query.IntegerPoint{Name: "cpu", Time: 9 * Second, Value: 18}},
			},
		},
		{
			Name:      "lhs binary bitwise-or integer",
			Statement: `SELECT 4 | value FROM cpu`,
			Points: [][]query.Point{
				{&query.IntegerPoint{Name: "cpu", Time: 0 * Second, Value: 20}},
				{&query.IntegerPoint{Name: "cpu", Time: 5 * Second, Value: 14}},
				{&query.IntegerPoint{Name: "cpu", Time: 9 * Second, Value: 23}},
			},
		},
		{
			Name:      "two variable binary bitwise-xor",
			Statement: `SELECT value ^ value FROM cpu`,
			Points: [][]query.Point{
				{&query.IntegerPoint{Name: "cpu", Time: 0 * Second, Value: 0}},
				{&query.IntegerPoint{Name: "cpu", Time: 5 * Second, Value: 0}},
				{&query.IntegerPoint{Name: "cpu", Time: 9 * Second, Value: 0}},
			},
		},
	} {
		stmt, err := MustParseSelectStatement(test.Statement).RewriteFields(&ic)
		if err != nil {
			t.Errorf("%s: rewrite error: %s", test.Name, err)
		}

		itrs, err := query.Select(stmt, &ic, nil)
		if err != nil {
			t.Errorf("%s: parse error: %s", test.Name, err)
		} else if a, err := Iterators(itrs).ReadAll(); err != nil {
			t.Fatalf("%s: unexpected error: %s", test.Name, err)
		} else if diff := cmp.Diff(a, test.Points); diff != "" {
			t.Errorf("%s: unexpected points:\n%s", test.Name, diff)
		}
	}
}

// Ensure a SELECT binary expr queries can be executed on mixed iterators.
func TestSelect_BinaryExpr_Mixed(t *testing.T) {
	var ic IteratorCreator
	ic.CreateIteratorFn = func(m *influxql.Measurement, opt query.IteratorOptions) (query.Iterator, error) {
		if m.Name != "cpu" {
			t.Fatalf("unexpected source: %s", m.Name)
		}
		return &IntegerIterator{Points: []query.IntegerPoint{
			{Name: "cpu", Time: 0 * Second, Value: 20, Aux: []interface{}{float64(20), int64(10)}},
			{Name: "cpu", Time: 5 * Second, Value: 10, Aux: []interface{}{float64(10), int64(15)}},
			{Name: "cpu", Time: 9 * Second, Value: 19, Aux: []interface{}{float64(19), int64(5)}},
		}}, nil
	}
	ic.FieldDimensionsFn = func(m *influxql.Measurement) (map[string]influxql.DataType, map[string]struct{}, error) {
		if m.Name != "cpu" {
			t.Fatalf("unexpected source: %s", m.Name)
		}
		return map[string]influxql.DataType{
			"total": influxql.Float,
			"value": influxql.Integer,
		}, nil, nil
	}

	for _, test := range []struct {
		Name      string
		Statement string
		Points    [][]query.Point
	}{
		{
			Name:      "mixed binary add",
			Statement: `SELECT total + value FROM cpu`,
			Points: [][]query.Point{
				{&query.FloatPoint{Name: "cpu", Time: 0 * Second, Value: 30}},
				{&query.FloatPoint{Name: "cpu", Time: 5 * Second, Value: 25}},
				{&query.FloatPoint{Name: "cpu", Time: 9 * Second, Value: 24}},
			},
		},
		{
			Name:      "mixed binary subtract",
			Statement: `SELECT total - value FROM cpu`,
			Points: [][]query.Point{
				{&query.FloatPoint{Name: "cpu", Time: 0 * Second, Value: 10}},
				{&query.FloatPoint{Name: "cpu", Time: 5 * Second, Value: -5}},
				{&query.FloatPoint{Name: "cpu", Time: 9 * Second, Value: 14}},
			},
		},
		{
			Name:      "mixed binary multiply",
			Statement: `SELECT total * value FROM cpu`,
			Points: [][]query.Point{
				{&query.FloatPoint{Name: "cpu", Time: 0 * Second, Value: 200}},
				{&query.FloatPoint{Name: "cpu", Time: 5 * Second, Value: 150}},
				{&query.FloatPoint{Name: "cpu", Time: 9 * Second, Value: 95}},
			},
		},
		{
			Name:      "mixed binary division",
			Statement: `SELECT total / value FROM cpu`,
			Points: [][]query.Point{
				{&query.FloatPoint{Name: "cpu", Time: 0 * Second, Value: 2}},
				{&query.FloatPoint{Name: "cpu", Time: 5 * Second, Value: float64(10) / float64(15)}},
				{&query.FloatPoint{Name: "cpu", Time: 9 * Second, Value: float64(19) / float64(5)}},
			},
		},
	} {
		stmt, err := MustParseSelectStatement(test.Statement).RewriteFields(&ic)
		if err != nil {
			t.Errorf("%s: rewrite error: %s", test.Name, err)
		}

		itrs, err := query.Select(stmt, &ic, nil)
		if err != nil {
			t.Errorf("%s: parse error: %s", test.Name, err)
		} else if a, err := Iterators(itrs).ReadAll(); err != nil {
			t.Fatalf("%s: unexpected error: %s", test.Name, err)
		} else if diff := cmp.Diff(a, test.Points); diff != "" {
			t.Errorf("%s: unexpected points:\n%s", test.Name, diff)
		}
	}
}

// Ensure a SELECT binary expr queries can be executed as booleans.
func TestSelect_BinaryExpr_Boolean(t *testing.T) {
	var ic IteratorCreator
	ic.CreateIteratorFn = func(m *influxql.Measurement, opt query.IteratorOptions) (query.Iterator, error) {
		if m.Name != "cpu" {
			t.Fatalf("unexpected source: %s", m.Name)
		}
		makeAuxFields := func(value bool) []interface{} {
			aux := make([]interface{}, len(opt.Aux))
			for i := range aux {
				aux[i] = value
			}
			return aux
		}
		return &BooleanIterator{Points: []query.BooleanPoint{
			{Name: "cpu", Time: 0 * Second, Value: true, Aux: makeAuxFields(true)},
			{Name: "cpu", Time: 5 * Second, Value: false, Aux: makeAuxFields(false)},
			{Name: "cpu", Time: 9 * Second, Value: true, Aux: makeAuxFields(true)},
		}}, nil
	}
	ic.FieldDimensionsFn = func(m *influxql.Measurement) (map[string]influxql.DataType, map[string]struct{}, error) {
		if m.Name != "cpu" {
			t.Fatalf("unexpected source: %s", m.Name)
		}
		return map[string]influxql.DataType{
			"one": influxql.Boolean,
			"two": influxql.Boolean,
		}, nil, nil
	}

	for _, test := range []struct {
		Name      string
		Statement string
		Points    [][]query.Point
	}{
		{
			Name:      "rhs binary bitwise-xor",
			Statement: `SELECT one ^ true FROM cpu`,
			Points: [][]query.Point{
				{&query.BooleanPoint{Name: "cpu", Time: 0 * Second, Value: false}},
				{&query.BooleanPoint{Name: "cpu", Time: 5 * Second, Value: true}},
				{&query.BooleanPoint{Name: "cpu", Time: 9 * Second, Value: false}},
			},
		},
		{
			Name:      "lhs binary or",
			Statement: `SELECT true | two FROM cpu`,
			Points: [][]query.Point{
				{&query.BooleanPoint{Name: "cpu", Time: 0 * Second, Value: true}},
				{&query.BooleanPoint{Name: "cpu", Time: 5 * Second, Value: true}},
				{&query.BooleanPoint{Name: "cpu", Time: 9 * Second, Value: true}},
			},
		},
		{
			Name:      "two series bitwise-and",
			Statement: `SELECT one & two FROM cpu`,
			Points: [][]query.Point{
				{&query.BooleanPoint{Name: "cpu", Time: 0 * Second, Value: true}},
				{&query.BooleanPoint{Name: "cpu", Time: 5 * Second, Value: false}},
				{&query.BooleanPoint{Name: "cpu", Time: 9 * Second, Value: true}},
			},
		},
	} {
		stmt, err := MustParseSelectStatement(test.Statement).RewriteFields(&ic)
		if err != nil {
			t.Errorf("%s: rewrite error: %s", test.Name, err)
		}

		itrs, err := query.Select(stmt, &ic, nil)
		if err != nil {
			t.Errorf("%s: parse error: %s", test.Name, err)
		} else if a, err := Iterators(itrs).ReadAll(); err != nil {
			t.Fatalf("%s: unexpected error: %s", test.Name, err)
		} else if diff := cmp.Diff(a, test.Points); diff != "" {
			t.Errorf("%s: unexpected points:\n%s", test.Name, diff)
		}
	}
}

// Ensure a SELECT binary expr with nil values can be executed.
// Nil values may be present when a field is missing from one iterator,
// but not the other.
func TestSelect_BinaryExpr_NilValues(t *testing.T) {
	var ic IteratorCreator
	ic.CreateIteratorFn = func(m *influxql.Measurement, opt query.IteratorOptions) (query.Iterator, error) {
		if m.Name != "cpu" {
			t.Fatalf("unexpected source: %s", m.Name)
		}
		return &FloatIterator{Points: []query.FloatPoint{
			{Name: "cpu", Time: 0 * Second, Value: 20, Aux: []interface{}{float64(20), nil}},
			{Name: "cpu", Time: 5 * Second, Value: 10, Aux: []interface{}{float64(10), float64(15)}},
			{Name: "cpu", Time: 9 * Second, Value: 19, Aux: []interface{}{nil, float64(5)}},
		}}, nil
	}
	ic.FieldDimensionsFn = func(m *influxql.Measurement) (map[string]influxql.DataType, map[string]struct{}, error) {
		if m.Name != "cpu" {
			t.Fatalf("unexpected source: %s", m.Name)
		}
		return map[string]influxql.DataType{
			"total": influxql.Float,
			"value": influxql.Float,
		}, nil, nil
	}

	for _, test := range []struct {
		Name      string
		Statement string
		Points    [][]query.Point
	}{
		{
			Name:      "nil binary add",
			Statement: `SELECT total + value FROM cpu`,
			Points: [][]query.Point{
				{&query.FloatPoint{Name: "cpu", Time: 0 * Second, Nil: true}},
				{&query.FloatPoint{Name: "cpu", Time: 5 * Second, Value: 25}},
				{&query.FloatPoint{Name: "cpu", Time: 9 * Second, Nil: true}},
			},
		},
		{
			Name:      "nil binary subtract",
			Statement: `SELECT total - value FROM cpu`,
			Points: [][]query.Point{
				{&query.FloatPoint{Name: "cpu", Time: 0 * Second, Nil: true}},
				{&query.FloatPoint{Name: "cpu", Time: 5 * Second, Value: -5}},
				{&query.FloatPoint{Name: "cpu", Time: 9 * Second, Nil: true}},
			},
		},
		{
			Name:      "nil binary multiply",
			Statement: `SELECT total * value FROM cpu`,
			Points: [][]query.Point{
				{&query.FloatPoint{Name: "cpu", Time: 0 * Second, Nil: true}},
				{&query.FloatPoint{Name: "cpu", Time: 5 * Second, Value: 150}},
				{&query.FloatPoint{Name: "cpu", Time: 9 * Second, Nil: true}},
			},
		},
		{
			Name:      "nil binary division",
			Statement: `SELECT total / value FROM cpu`,
			Points: [][]query.Point{
				{&query.FloatPoint{Name: "cpu", Time: 0 * Second, Nil: true}},
				{&query.FloatPoint{Name: "cpu", Time: 5 * Second, Value: float64(10) / float64(15)}},
				{&query.FloatPoint{Name: "cpu", Time: 9 * Second, Nil: true}},
			},
		},
	} {
		stmt, err := MustParseSelectStatement(test.Statement).RewriteFields(&ic)
		if err != nil {
			t.Errorf("%s: rewrite error: %s", test.Name, err)
		}

		itrs, err := query.Select(stmt, &ic, nil)
		if err != nil {
			t.Errorf("%s: parse error: %s", test.Name, err)
		} else if a, err := Iterators(itrs).ReadAll(); err != nil {
			t.Fatalf("%s: unexpected error: %s", test.Name, err)
		} else if diff := cmp.Diff(a, test.Points); diff != "" {
			t.Errorf("%s: unexpected points:\n%s", test.Name, diff)
		}
	}
}

// Ensure a SELECT (...) query can be executed.
func TestSelect_ParenExpr(t *testing.T) {
	var ic IteratorCreator
	ic.CreateIteratorFn = func(m *influxql.Measurement, opt query.IteratorOptions) (query.Iterator, error) {
		if m.Name != "cpu" {
			t.Fatalf("unexpected source: %s", m.Name)
		}
		if !reflect.DeepEqual(opt.Expr, MustParseExpr(`min(value)`)) {
			t.Fatalf("unexpected expr: %s", spew.Sdump(opt.Expr))
		}

		input, err := query.Iterators{
			&FloatIterator{Points: []query.FloatPoint{
				{Name: "cpu", Tags: ParseTags("region=west,host=A"), Time: 0 * Second, Value: 20},
				{Name: "cpu", Tags: ParseTags("region=west,host=A"), Time: 11 * Second, Value: 3},
				{Name: "cpu", Tags: ParseTags("region=west,host=A"), Time: 31 * Second, Value: 100},
			}},
			&FloatIterator{Points: []query.FloatPoint{
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 5 * Second, Value: 10},
			}},
			&FloatIterator{Points: []query.FloatPoint{
				{Name: "cpu", Tags: ParseTags("region=east,host=A"), Time: 9 * Second, Value: 19},
				{Name: "cpu", Tags: ParseTags("region=east,host=A"), Time: 10 * Second, Value: 2},
			}},
		}.Merge(opt)
		if err != nil {
			return nil, err
		}
		return query.NewCallIterator(input, opt)
	}

	// Execute selection.
	itrs, err := query.Select(MustParseSelectStatement(`SELECT (min(value)) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY time(10s), host fill(none)`), &ic, nil)
	if err != nil {
		t.Fatal(err)
	} else if a, err := Iterators(itrs).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if diff := cmp.Diff(a, [][]query.Point{
		{&query.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 0 * Second, Value: 19, Aggregated: 2}},
		{&query.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 10 * Second, Value: 2, Aggregated: 2}},
		{&query.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 30 * Second, Value: 100, Aggregated: 1}},
		{&query.FloatPoint{Name: "cpu", Tags: ParseTags("host=B"), Time: 0 * Second, Value: 10, Aggregated: 1}},
	}); diff != "" {
		t.Fatalf("unexpected points:\n%s", diff)
	}

	ic.CreateIteratorFn = func(m *influxql.Measurement, opt query.IteratorOptions) (query.Iterator, error) {
		if m.Name != "cpu" {
			t.Fatalf("unexpected source: %s", m.Name)
		}
		return query.Iterators{
			&FloatIterator{Points: []query.FloatPoint{
				{Name: "cpu", Tags: ParseTags("region=west,host=A"), Time: 0 * Second, Value: 20},
				{Name: "cpu", Tags: ParseTags("region=west,host=A"), Time: 1 * Second, Value: 19},
			}},
			&FloatIterator{Points: []query.FloatPoint{
				{Name: "cpu", Tags: ParseTags("region=west,host=B"), Time: 5 * Second, Value: 10},
			}},
			&FloatIterator{Points: []query.FloatPoint{
				{Name: "cpu", Tags: ParseTags("region=east,host=A"), Time: 9 * Second, Value: 19},
				{Name: "cpu", Tags: ParseTags("region=east,host=A"), Time: 10 * Second, Value: 2},
				{Name: "cpu", Tags: ParseTags("region=east,host=A"), Time: 11 * Second, Value: 2},
				{Name: "cpu", Tags: ParseTags("region=east,host=A"), Time: 12 * Second, Value: 2},
			}},
		}.Merge(opt)
	}

	// Execute selection.
	itrs, err = query.Select(MustParseSelectStatement(`SELECT (distinct(value)) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY time(10s), host fill(none)`), &ic, nil)
	if err != nil {
		t.Fatal(err)
	} else if a, err := Iterators(itrs).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if diff := cmp.Diff(a, [][]query.Point{
		{&query.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 0 * Second, Value: 20}},
		{&query.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 0 * Second, Value: 19}},
		{&query.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 10 * Second, Value: 2}},
		{&query.FloatPoint{Name: "cpu", Tags: ParseTags("host=B"), Time: 0 * Second, Value: 10}},
	}); diff != "" {
		t.Fatalf("unexpected points:\n%s", diff)
	}
}

func TestSelect_Derivative_Float(t *testing.T) {
	var ic IteratorCreator
	ic.CreateIteratorFn = func(m *influxql.Measurement, opt query.IteratorOptions) (query.Iterator, error) {
		if m.Name != "cpu" {
			t.Fatalf("unexpected source: %s", m.Name)
		}
		return &FloatIterator{Points: []query.FloatPoint{
			{Name: "cpu", Time: 0 * Second, Value: 20},
			{Name: "cpu", Time: 4 * Second, Value: 10},
			{Name: "cpu", Time: 8 * Second, Value: 19},
			{Name: "cpu", Time: 12 * Second, Value: 3},
		}}, nil
	}

	// Execute selection.
	itrs, err := query.Select(MustParseSelectStatement(`SELECT derivative(value, 1s) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:00:16Z'`), &ic, nil)
	if err != nil {
		t.Fatal(err)
	} else if a, err := Iterators(itrs).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if diff := cmp.Diff(a, [][]query.Point{
		{&query.FloatPoint{Name: "cpu", Time: 4 * Second, Value: -2.5}},
		{&query.FloatPoint{Name: "cpu", Time: 8 * Second, Value: 2.25}},
		{&query.FloatPoint{Name: "cpu", Time: 12 * Second, Value: -4}},
	}); diff != "" {
		t.Fatalf("unexpected points:\n%s", diff)
	}
}

func TestSelect_Derivative_Integer(t *testing.T) {
	var ic IteratorCreator
	ic.CreateIteratorFn = func(m *influxql.Measurement, opt query.IteratorOptions) (query.Iterator, error) {
		if m.Name != "cpu" {
			t.Fatalf("unexpected source: %s", m.Name)
		}
		return &IntegerIterator{Points: []query.IntegerPoint{
			{Name: "cpu", Time: 0 * Second, Value: 20},
			{Name: "cpu", Time: 4 * Second, Value: 10},
			{Name: "cpu", Time: 8 * Second, Value: 19},
			{Name: "cpu", Time: 12 * Second, Value: 3},
		}}, nil
	}

	// Execute selection.
	itrs, err := query.Select(MustParseSelectStatement(`SELECT derivative(value, 1s) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:00:16Z'`), &ic, nil)
	if err != nil {
		t.Fatal(err)
	} else if a, err := Iterators(itrs).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if diff := cmp.Diff(a, [][]query.Point{
		{&query.FloatPoint{Name: "cpu", Time: 4 * Second, Value: -2.5}},
		{&query.FloatPoint{Name: "cpu", Time: 8 * Second, Value: 2.25}},
		{&query.FloatPoint{Name: "cpu", Time: 12 * Second, Value: -4}},
	}); diff != "" {
		t.Fatalf("unexpected points:\n%s", diff)
	}
}

func TestSelect_Derivative_Desc_Float(t *testing.T) {
	var ic IteratorCreator
	ic.CreateIteratorFn = func(m *influxql.Measurement, opt query.IteratorOptions) (query.Iterator, error) {
		if m.Name != "cpu" {
			t.Fatalf("unexpected source: %s", m.Name)
		}
		return &FloatIterator{Points: []query.FloatPoint{
			{Name: "cpu", Time: 12 * Second, Value: 3},
			{Name: "cpu", Time: 8 * Second, Value: 19},
			{Name: "cpu", Time: 4 * Second, Value: 10},
			{Name: "cpu", Time: 0 * Second, Value: 20},
		}}, nil
	}

	// Execute selection.
	itrs, err := query.Select(MustParseSelectStatement(`SELECT derivative(value, 1s) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:00:16Z' ORDER BY desc`), &ic, nil)
	if err != nil {
		t.Fatal(err)
	} else if a, err := Iterators(itrs).ReadAll(); err != nil {
		t.Errorf("unexpected error: %s", err)
	} else if diff := cmp.Diff(a, [][]query.Point{
		{&query.FloatPoint{Name: "cpu", Time: 8 * Second, Value: 4}},
		{&query.FloatPoint{Name: "cpu", Time: 4 * Second, Value: -2.25}},
		{&query.FloatPoint{Name: "cpu", Time: 0 * Second, Value: 2.5}},
	}); diff != "" {
		t.Fatalf("unexpected points:\n%s", diff)
	}
}

func TestSelect_Derivative_Desc_Integer(t *testing.T) {
	var ic IteratorCreator
	ic.CreateIteratorFn = func(m *influxql.Measurement, opt query.IteratorOptions) (query.Iterator, error) {
		if m.Name != "cpu" {
			t.Fatalf("unexpected source: %s", m.Name)
		}
		return &IntegerIterator{Points: []query.IntegerPoint{
			{Name: "cpu", Time: 12 * Second, Value: 3},
			{Name: "cpu", Time: 8 * Second, Value: 19},
			{Name: "cpu", Time: 4 * Second, Value: 10},
			{Name: "cpu", Time: 0 * Second, Value: 20},
		}}, nil
	}

	// Execute selection.
	itrs, err := query.Select(MustParseSelectStatement(`SELECT derivative(value, 1s) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:00:16Z' ORDER BY desc`), &ic, nil)
	if err != nil {
		t.Fatal(err)
	} else if a, err := Iterators(itrs).ReadAll(); err != nil {
		t.Errorf("unexpected error: %s", err)
	} else if diff := cmp.Diff(a, [][]query.Point{
		{&query.FloatPoint{Name: "cpu", Time: 8 * Second, Value: 4}},
		{&query.FloatPoint{Name: "cpu", Time: 4 * Second, Value: -2.25}},
		{&query.FloatPoint{Name: "cpu", Time: 0 * Second, Value: 2.5}},
	}); diff != "" {
		t.Fatalf("unexpected points:\n%s", diff)
	}
}

func TestSelect_Derivative_Duplicate_Float(t *testing.T) {
	var ic IteratorCreator
	ic.CreateIteratorFn = func(m *influxql.Measurement, opt query.IteratorOptions) (query.Iterator, error) {
		if m.Name != "cpu" {
			t.Fatalf("unexpected source: %s", m.Name)
		}
		return &FloatIterator{Points: []query.FloatPoint{
			{Name: "cpu", Time: 0 * Second, Value: 20},
			{Name: "cpu", Time: 0 * Second, Value: 19},
			{Name: "cpu", Time: 4 * Second, Value: 10},
			{Name: "cpu", Time: 4 * Second, Value: 3},
		}}, nil
	}

	// Execute selection.
	itrs, err := query.Select(MustParseSelectStatement(`SELECT derivative(value, 1s) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:00:16Z'`), &ic, nil)
	if err != nil {
		t.Fatal(err)
	} else if a, err := Iterators(itrs).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if diff := cmp.Diff(a, [][]query.Point{
		{&query.FloatPoint{Name: "cpu", Time: 4 * Second, Value: -2.5}},
	}); diff != "" {
		t.Fatalf("unexpected points:\n%s", diff)
	}
}

func TestSelect_Derivative_Duplicate_Integer(t *testing.T) {
	var ic IteratorCreator
	ic.CreateIteratorFn = func(m *influxql.Measurement, opt query.IteratorOptions) (query.Iterator, error) {
		if m.Name != "cpu" {
			t.Fatalf("unexpected source: %s", m.Name)
		}
		return &IntegerIterator{Points: []query.IntegerPoint{
			{Name: "cpu", Time: 0 * Second, Value: 20},
			{Name: "cpu", Time: 0 * Second, Value: 19},
			{Name: "cpu", Time: 4 * Second, Value: 10},
			{Name: "cpu", Time: 4 * Second, Value: 3},
		}}, nil
	}

	// Execute selection.
	itrs, err := query.Select(MustParseSelectStatement(`SELECT derivative(value, 1s) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:00:16Z'`), &ic, nil)
	if err != nil {
		t.Fatal(err)
	} else if a, err := Iterators(itrs).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if diff := cmp.Diff(a, [][]query.Point{
		{&query.FloatPoint{Name: "cpu", Time: 4 * Second, Value: -2.5}},
	}); diff != "" {
		t.Fatalf("unexpected points:\n%s", diff)
	}
}

func TestSelect_Difference_Float(t *testing.T) {
	var ic IteratorCreator
	ic.CreateIteratorFn = func(m *influxql.Measurement, opt query.IteratorOptions) (query.Iterator, error) {
		if m.Name != "cpu" {
			t.Fatalf("unexpected source: %s", m.Name)
		}
		return &FloatIterator{Points: []query.FloatPoint{
			{Name: "cpu", Time: 0 * Second, Value: 20},
			{Name: "cpu", Time: 4 * Second, Value: 10},
			{Name: "cpu", Time: 8 * Second, Value: 19},
			{Name: "cpu", Time: 12 * Second, Value: 3},
		}}, nil
	}

	// Execute selection.
	itrs, err := query.Select(MustParseSelectStatement(`SELECT difference(value) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:00:16Z'`), &ic, nil)
	if err != nil {
		t.Fatal(err)
	} else if a, err := Iterators(itrs).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if diff := cmp.Diff(a, [][]query.Point{
		{&query.FloatPoint{Name: "cpu", Time: 4 * Second, Value: -10}},
		{&query.FloatPoint{Name: "cpu", Time: 8 * Second, Value: 9}},
		{&query.FloatPoint{Name: "cpu", Time: 12 * Second, Value: -16}},
	}); diff != "" {
		t.Fatalf("unexpected points: %s", diff)
	}
}

func TestSelect_Difference_Integer(t *testing.T) {
	var ic IteratorCreator
	ic.CreateIteratorFn = func(m *influxql.Measurement, opt query.IteratorOptions) (query.Iterator, error) {
		if m.Name != "cpu" {
			t.Fatalf("unexpected source: %s", m.Name)
		}
		return &IntegerIterator{Points: []query.IntegerPoint{
			{Name: "cpu", Time: 0 * Second, Value: 20},
			{Name: "cpu", Time: 4 * Second, Value: 10},
			{Name: "cpu", Time: 8 * Second, Value: 19},
			{Name: "cpu", Time: 12 * Second, Value: 3},
		}}, nil
	}

	// Execute selection.
	itrs, err := query.Select(MustParseSelectStatement(`SELECT difference(value) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:00:16Z'`), &ic, nil)
	if err != nil {
		t.Fatal(err)
	} else if a, err := Iterators(itrs).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if diff := cmp.Diff(a, [][]query.Point{
		{&query.IntegerPoint{Name: "cpu", Time: 4 * Second, Value: -10}},
		{&query.IntegerPoint{Name: "cpu", Time: 8 * Second, Value: 9}},
		{&query.IntegerPoint{Name: "cpu", Time: 12 * Second, Value: -16}},
	}); diff != "" {
		t.Fatalf("unexpected points:\n%s", diff)
	}
}

func TestSelect_Difference_Duplicate_Float(t *testing.T) {
	var ic IteratorCreator
	ic.CreateIteratorFn = func(m *influxql.Measurement, opt query.IteratorOptions) (query.Iterator, error) {
		if m.Name != "cpu" {
			t.Fatalf("unexpected source: %s", m.Name)
		}
		return &FloatIterator{Points: []query.FloatPoint{
			{Name: "cpu", Time: 0 * Second, Value: 20},
			{Name: "cpu", Time: 0 * Second, Value: 19},
			{Name: "cpu", Time: 4 * Second, Value: 10},
			{Name: "cpu", Time: 4 * Second, Value: 3},
		}}, nil
	}

	// Execute selection.
	itrs, err := query.Select(MustParseSelectStatement(`SELECT difference(value) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:00:16Z'`), &ic, nil)
	if err != nil {
		t.Fatal(err)
	} else if a, err := Iterators(itrs).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if diff := cmp.Diff(a, [][]query.Point{
		{&query.FloatPoint{Name: "cpu", Time: 4 * Second, Value: -10}},
	}); diff != "" {
		t.Fatalf("unexpected points: %s", diff)
	}
}

func TestSelect_Difference_Duplicate_Integer(t *testing.T) {
	var ic IteratorCreator
	ic.CreateIteratorFn = func(m *influxql.Measurement, opt query.IteratorOptions) (query.Iterator, error) {
		if m.Name != "cpu" {
			t.Fatalf("unexpected source: %s", m.Name)
		}
		return &IntegerIterator{Points: []query.IntegerPoint{
			{Name: "cpu", Time: 0 * Second, Value: 20},
			{Name: "cpu", Time: 0 * Second, Value: 19},
			{Name: "cpu", Time: 4 * Second, Value: 10},
			{Name: "cpu", Time: 4 * Second, Value: 3},
		}}, nil
	}

	// Execute selection.
	itrs, err := query.Select(MustParseSelectStatement(`SELECT difference(value) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:00:16Z'`), &ic, nil)
	if err != nil {
		t.Fatal(err)
	} else if a, err := Iterators(itrs).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if diff := cmp.Diff(a, [][]query.Point{
		{&query.IntegerPoint{Name: "cpu", Time: 4 * Second, Value: -10}},
	}); diff != "" {
		t.Fatalf("unexpected points:\n%s", diff)
	}
}

func TestSelect_Non_Negative_Difference_Float(t *testing.T) {
	var ic IteratorCreator
	ic.CreateIteratorFn = func(m *influxql.Measurement, opt query.IteratorOptions) (query.Iterator, error) {
		if m.Name != "cpu" {
			t.Fatalf("unexpected source: %s", m.Name)
		}
		return &FloatIterator{Points: []query.FloatPoint{
			{Name: "cpu", Time: 0 * Second, Value: 20},
			{Name: "cpu", Time: 4 * Second, Value: 10},
			{Name: "cpu", Time: 8 * Second, Value: 29},
			{Name: "cpu", Time: 12 * Second, Value: 3},
			{Name: "cpu", Time: 16 * Second, Value: 39},
		}}, nil
	}

	// Execute selection.
	itrs, err := query.Select(MustParseSelectStatement(`SELECT non_negative_difference(value) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:00:16Z'`), &ic, nil)
	if err != nil {
		t.Fatal(err)
	} else if a, err := Iterators(itrs).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if diff := cmp.Diff(a, [][]query.Point{
		{&query.FloatPoint{Name: "cpu", Time: 8 * Second, Value: 19}},
		{&query.FloatPoint{Name: "cpu", Time: 16 * Second, Value: 36}},
	}); diff != "" {
		t.Fatalf("unexpected points:\n%s", diff)
	}
}

func TestSelect_Non_Negative_Difference_Integer(t *testing.T) {
	var ic IteratorCreator
	ic.CreateIteratorFn = func(m *influxql.Measurement, opt query.IteratorOptions) (query.Iterator, error) {
		if m.Name != "cpu" {
			t.Fatalf("unexpected source: %s", m.Name)
		}
		return &IntegerIterator{Points: []query.IntegerPoint{
			{Name: "cpu", Time: 0 * Second, Value: 20},
			{Name: "cpu", Time: 4 * Second, Value: 10},
			{Name: "cpu", Time: 8 * Second, Value: 21},
			{Name: "cpu", Time: 12 * Second, Value: 3},
		}}, nil
	}

	// Execute selection.
	itrs, err := query.Select(MustParseSelectStatement(`SELECT non_negative_difference(value) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:00:16Z'`), &ic, nil)
	if err != nil {
		t.Fatal(err)
	} else if a, err := Iterators(itrs).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if diff := cmp.Diff(a, [][]query.Point{
		{&query.IntegerPoint{Name: "cpu", Time: 8 * Second, Value: 11}},
	}); diff != "" {
		t.Fatalf("unexpected points:\n%s", diff)
	}
}

func TestSelect_Non_Negative_Difference_Duplicate_Float(t *testing.T) {
	var ic IteratorCreator
	ic.CreateIteratorFn = func(m *influxql.Measurement, opt query.IteratorOptions) (query.Iterator, error) {
		if m.Name != "cpu" {
			t.Fatalf("unexpected source: %s", m.Name)
		}
		return &FloatIterator{Points: []query.FloatPoint{
			{Name: "cpu", Time: 0 * Second, Value: 20},
			{Name: "cpu", Time: 0 * Second, Value: 19},
			{Name: "cpu", Time: 4 * Second, Value: 10},
			{Name: "cpu", Time: 4 * Second, Value: 3},
			{Name: "cpu", Time: 8 * Second, Value: 30},
			{Name: "cpu", Time: 8 * Second, Value: 19},
			{Name: "cpu", Time: 12 * Second, Value: 10},
			{Name: "cpu", Time: 12 * Second, Value: 3},
			{Name: "cpu", Time: 16 * Second, Value: 40},
			{Name: "cpu", Time: 16 * Second, Value: 3},
		}}, nil
	}

	// Execute selection.
	itrs, err := query.Select(MustParseSelectStatement(`SELECT non_negative_difference(value) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:00:16Z'`), &ic, nil)
	if err != nil {
		t.Fatal(err)
	} else if a, err := Iterators(itrs).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if diff := cmp.Diff(a, [][]query.Point{
		{&query.FloatPoint{Name: "cpu", Time: 8 * Second, Value: 20}},
		{&query.FloatPoint{Name: "cpu", Time: 16 * Second, Value: 30}},
	}); diff != "" {
		t.Fatalf("unexpected points:\n%s", diff)
	}
}

func TestSelect_Non_Negative_Difference_Duplicate_Integer(t *testing.T) {
	var ic IteratorCreator
	ic.CreateIteratorFn = func(m *influxql.Measurement, opt query.IteratorOptions) (query.Iterator, error) {
		if m.Name != "cpu" {
			t.Fatalf("unexpected source: %s", m.Name)
		}
		return &IntegerIterator{Points: []query.IntegerPoint{
			{Name: "cpu", Time: 0 * Second, Value: 20},
			{Name: "cpu", Time: 0 * Second, Value: 19},
			{Name: "cpu", Time: 4 * Second, Value: 10},
			{Name: "cpu", Time: 4 * Second, Value: 3},
			{Name: "cpu", Time: 8 * Second, Value: 30},
			{Name: "cpu", Time: 8 * Second, Value: 19},
			{Name: "cpu", Time: 12 * Second, Value: 10},
			{Name: "cpu", Time: 12 * Second, Value: 3},
			{Name: "cpu", Time: 16 * Second, Value: 40},
			{Name: "cpu", Time: 16 * Second, Value: 3},
		}}, nil
	}

	// Execute selection.
	itrs, err := query.Select(MustParseSelectStatement(`SELECT non_negative_difference(value) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:00:16Z'`), &ic, nil)
	if err != nil {
		t.Fatal(err)
	} else if a, err := Iterators(itrs).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if diff := cmp.Diff(a, [][]query.Point{
		{&query.IntegerPoint{Name: "cpu", Time: 8 * Second, Value: 20}},
		{&query.IntegerPoint{Name: "cpu", Time: 16 * Second, Value: 30}},
	}); diff != "" {
		t.Fatalf("unexpected points:\n%s", diff)
	}
}

func TestSelect_Elapsed_Float(t *testing.T) {
	var ic IteratorCreator
	ic.CreateIteratorFn = func(m *influxql.Measurement, opt query.IteratorOptions) (query.Iterator, error) {
		if m.Name != "cpu" {
			t.Fatalf("unexpected source: %s", m.Name)
		}
		return &FloatIterator{Points: []query.FloatPoint{
			{Name: "cpu", Time: 0 * Second, Value: 20},
			{Name: "cpu", Time: 4 * Second, Value: 10},
			{Name: "cpu", Time: 8 * Second, Value: 19},
			{Name: "cpu", Time: 11 * Second, Value: 3},
		}}, nil
	}

	// Execute selection.
	itrs, err := query.Select(MustParseSelectStatement(`SELECT elapsed(value, 1s) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:00:16Z'`), &ic, nil)
	if err != nil {
		t.Fatal(err)
	} else if a, err := Iterators(itrs).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if diff := cmp.Diff(a, [][]query.Point{
		{&query.IntegerPoint{Name: "cpu", Time: 4 * Second, Value: 4}},
		{&query.IntegerPoint{Name: "cpu", Time: 8 * Second, Value: 4}},
		{&query.IntegerPoint{Name: "cpu", Time: 11 * Second, Value: 3}},
	}); diff != "" {
		t.Fatalf("unexpected points:\n%s", diff)
	}
}

func TestSelect_Elapsed_Integer(t *testing.T) {
	var ic IteratorCreator
	ic.CreateIteratorFn = func(m *influxql.Measurement, opt query.IteratorOptions) (query.Iterator, error) {
		if m.Name != "cpu" {
			t.Fatalf("unexpected source: %s", m.Name)
		}
		return &IntegerIterator{Points: []query.IntegerPoint{
			{Name: "cpu", Time: 0 * Second, Value: 20},
			{Name: "cpu", Time: 4 * Second, Value: 10},
			{Name: "cpu", Time: 8 * Second, Value: 19},
			{Name: "cpu", Time: 11 * Second, Value: 3},
		}}, nil
	}

	// Execute selection.
	itrs, err := query.Select(MustParseSelectStatement(`SELECT elapsed(value, 1s) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:00:16Z'`), &ic, nil)
	if err != nil {
		t.Fatal(err)
	} else if a, err := Iterators(itrs).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if diff := cmp.Diff(a, [][]query.Point{
		{&query.IntegerPoint{Name: "cpu", Time: 4 * Second, Value: 4}},
		{&query.IntegerPoint{Name: "cpu", Time: 8 * Second, Value: 4}},
		{&query.IntegerPoint{Name: "cpu", Time: 11 * Second, Value: 3}},
	}); diff != "" {
		t.Fatalf("unexpected points:\n%s", diff)
	}
}

func TestSelect_Elapsed_String(t *testing.T) {
	var ic IteratorCreator
	ic.CreateIteratorFn = func(m *influxql.Measurement, opt query.IteratorOptions) (query.Iterator, error) {
		if m.Name != "cpu" {
			t.Fatalf("unexpected source: %s", m.Name)
		}
		return &StringIterator{Points: []query.StringPoint{
			{Name: "cpu", Time: 0 * Second, Value: "a"},
			{Name: "cpu", Time: 4 * Second, Value: "b"},
			{Name: "cpu", Time: 8 * Second, Value: "c"},
			{Name: "cpu", Time: 11 * Second, Value: "d"},
		}}, nil
	}

	// Execute selection.
	itrs, err := query.Select(MustParseSelectStatement(`SELECT elapsed(value, 1s) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:00:16Z'`), &ic, nil)
	if err != nil {
		t.Fatal(err)
	} else if a, err := Iterators(itrs).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if diff := cmp.Diff(a, [][]query.Point{
		{&query.IntegerPoint{Name: "cpu", Time: 4 * Second, Value: 4}},
		{&query.IntegerPoint{Name: "cpu", Time: 8 * Second, Value: 4}},
		{&query.IntegerPoint{Name: "cpu", Time: 11 * Second, Value: 3}},
	}); diff != "" {
		t.Fatalf("unexpected points:\n%s", diff)
	}
}

func TestSelect_Elapsed_Boolean(t *testing.T) {
	var ic IteratorCreator
	ic.CreateIteratorFn = func(m *influxql.Measurement, opt query.IteratorOptions) (query.Iterator, error) {
		if m.Name != "cpu" {
			t.Fatalf("unexpected source: %s", m.Name)
		}
		return &BooleanIterator{Points: []query.BooleanPoint{
			{Name: "cpu", Time: 0 * Second, Value: true},
			{Name: "cpu", Time: 4 * Second, Value: false},
			{Name: "cpu", Time: 8 * Second, Value: false},
			{Name: "cpu", Time: 11 * Second, Value: true},
		}}, nil
	}

	// Execute selection.
	itrs, err := query.Select(MustParseSelectStatement(`SELECT elapsed(value, 1s) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:00:16Z'`), &ic, nil)
	if err != nil {
		t.Fatal(err)
	} else if a, err := Iterators(itrs).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if diff := cmp.Diff(a, [][]query.Point{
		{&query.IntegerPoint{Name: "cpu", Time: 4 * Second, Value: 4}},
		{&query.IntegerPoint{Name: "cpu", Time: 8 * Second, Value: 4}},
		{&query.IntegerPoint{Name: "cpu", Time: 11 * Second, Value: 3}},
	}); diff != "" {
		t.Fatalf("unexpected points:\n%s", diff)
	}
}

func TestSelect_Integral_Float(t *testing.T) {
	var ic IteratorCreator
	ic.CreateIteratorFn = func(m *influxql.Measurement, opt query.IteratorOptions) (query.Iterator, error) {
		if m.Name != "cpu" {
			t.Fatalf("unexpected source: %s", m.Name)
		}
		return &FloatIterator{Points: []query.FloatPoint{
			{Name: "cpu", Time: 10 * Second, Value: 20},
			{Name: "cpu", Time: 15 * Second, Value: 10},
			{Name: "cpu", Time: 20 * Second, Value: 0},
			{Name: "cpu", Time: 30 * Second, Value: -10},
		}}, nil
	}

	itrs, err := query.Select(MustParseSelectStatement(`SELECT integral(value) FROM cpu`), &ic, nil)
	if err != nil {
		t.Fatal(err)
	} else if a, err := Iterators(itrs).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if diff := cmp.Diff(a, [][]query.Point{
		{&query.FloatPoint{Name: "cpu", Time: 0, Value: 50}},
	}); diff != "" {
		t.Fatalf("unexpected points:\n%s", diff)
	}
}

func TestSelect_Integral_Float_GroupByTime(t *testing.T) {
	var ic IteratorCreator
	ic.CreateIteratorFn = func(m *influxql.Measurement, opt query.IteratorOptions) (query.Iterator, error) {
		if m.Name != "cpu" {
			t.Fatalf("unexpected source: %s", m.Name)
		}
		return &FloatIterator{Points: []query.FloatPoint{
			{Name: "cpu", Time: 10 * Second, Value: 20},
			{Name: "cpu", Time: 15 * Second, Value: 10},
			{Name: "cpu", Time: 20 * Second, Value: 0},
			{Name: "cpu", Time: 30 * Second, Value: -10},
		}}, nil
	}

	itrs, err := query.Select(MustParseSelectStatement(`SELECT integral(value) FROM cpu WHERE time > 0s AND time < 60s GROUP BY time(20s)`), &ic, nil)
	if err != nil {
		t.Fatal(err)
	} else if a, err := Iterators(itrs).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if diff := cmp.Diff(a, [][]query.Point{
		{&query.FloatPoint{Name: "cpu", Time: 0, Value: 100}},
		{&query.FloatPoint{Name: "cpu", Time: 20 * Second, Value: -50}},
	}); diff != "" {
		t.Fatalf("unexpected points:\n%s", diff)
	}
}

func TestSelect_Integral_Float_InterpolateGroupByTime(t *testing.T) {
	var ic IteratorCreator
	ic.CreateIteratorFn = func(m *influxql.Measurement, opt query.IteratorOptions) (query.Iterator, error) {
		if m.Name != "cpu" {
			t.Fatalf("unexpected source: %s", m.Name)
		}
		return &FloatIterator{Points: []query.FloatPoint{
			{Name: "cpu", Time: 10 * Second, Value: 20},
			{Name: "cpu", Time: 15 * Second, Value: 10},
			{Name: "cpu", Time: 25 * Second, Value: 0},
			{Name: "cpu", Time: 30 * Second, Value: -10},
		}}, nil
	}

	itrs, err := query.Select(MustParseSelectStatement(`SELECT integral(value) FROM cpu WHERE time > 0s AND time < 60s GROUP BY time(20s)`), &ic, nil)
	if err != nil {
		t.Fatal(err)
	} else if a, err := Iterators(itrs).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if diff := cmp.Diff(a, [][]query.Point{
		{&query.FloatPoint{Name: "cpu", Time: 0, Value: 112.5}},
		{&query.FloatPoint{Name: "cpu", Time: 20 * Second, Value: -12.5}},
	}); diff != "" {
		t.Fatalf("unexpected points:\n%s", diff)
	}
}

func TestSelect_Integral_Integer(t *testing.T) {
	var ic IteratorCreator
	ic.CreateIteratorFn = func(m *influxql.Measurement, opt query.IteratorOptions) (query.Iterator, error) {
		if m.Name != "cpu" {
			t.Fatalf("unexpected source: %s", m.Name)
		}
		return &IntegerIterator{Points: []query.IntegerPoint{
			{Name: "cpu", Time: 0 * Second, Value: 20},
			{Name: "cpu", Time: 5 * Second, Value: 10},
			{Name: "cpu", Time: 10 * Second, Value: 0},
			{Name: "cpu", Time: 20 * Second, Value: -10},
		}}, nil
	}

	itrs, err := query.Select(MustParseSelectStatement(`SELECT integral(value) FROM cpu`), &ic, nil)
	if err != nil {
		t.Fatal(err)
	} else if a, err := Iterators(itrs).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if diff := cmp.Diff(a, [][]query.Point{
		{&query.FloatPoint{Name: "cpu", Time: 0, Value: 50}},
	}); diff != "" {
		t.Fatalf("unexpected points:\n%s", diff)
	}
}

func TestSelect_Integral_Duplicate_Float(t *testing.T) {
	var ic IteratorCreator
	ic.CreateIteratorFn = func(m *influxql.Measurement, opt query.IteratorOptions) (query.Iterator, error) {
		if m.Name != "cpu" {
			t.Fatalf("unexpected source: %s", m.Name)
		}
		return &FloatIterator{Points: []query.FloatPoint{
			{Name: "cpu", Time: 0 * Second, Value: 20},
			{Name: "cpu", Time: 5 * Second, Value: 10},
			{Name: "cpu", Time: 5 * Second, Value: 30},
			{Name: "cpu", Time: 10 * Second, Value: 40},
		}}, nil
	}

	itrs, err := query.Select(MustParseSelectStatement(`SELECT integral(value) FROM cpu`), &ic, nil)
	if err != nil {
		t.Fatal(err)
	} else if a, err := Iterators(itrs).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if diff := cmp.Diff(a, [][]query.Point{
		{&query.FloatPoint{Name: "cpu", Time: 0, Value: 250}},
	}); diff != "" {
		t.Fatalf("unexpected points:\n%s", diff)
	}
}

func TestSelect_Integral_Duplicate_Integer(t *testing.T) {
	var ic IteratorCreator
	ic.CreateIteratorFn = func(m *influxql.Measurement, opt query.IteratorOptions) (query.Iterator, error) {
		if m.Name != "cpu" {
			t.Fatalf("unexpected source: %s", m.Name)
		}
		return &IntegerIterator{Points: []query.IntegerPoint{
			{Name: "cpu", Time: 0 * Second, Value: 20},
			{Name: "cpu", Time: 5 * Second, Value: 10},
			{Name: "cpu", Time: 5 * Second, Value: 30},
			{Name: "cpu", Time: 10 * Second, Value: 40},
		}}, nil
	}

	itrs, err := query.Select(MustParseSelectStatement(`SELECT integral(value, 2s) FROM cpu`), &ic, nil)
	if err != nil {
		t.Fatal(err)
	} else if a, err := Iterators(itrs).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if diff := cmp.Diff(a, [][]query.Point{
		{&query.FloatPoint{Name: "cpu", Time: 0, Value: 125}},
	}); diff != "" {
		t.Fatalf("unexpected points:\n%s", diff)
	}
}

func TestSelect_MovingAverage_Float(t *testing.T) {
	var ic IteratorCreator
	ic.CreateIteratorFn = func(m *influxql.Measurement, opt query.IteratorOptions) (query.Iterator, error) {
		if m.Name != "cpu" {
			t.Fatalf("unexpected source: %s", m.Name)
		}
		return &FloatIterator{Points: []query.FloatPoint{
			{Name: "cpu", Time: 0 * Second, Value: 20},
			{Name: "cpu", Time: 4 * Second, Value: 10},
			{Name: "cpu", Time: 8 * Second, Value: 19},
			{Name: "cpu", Time: 12 * Second, Value: 3},
		}}, nil
	}

	// Execute selection.
	itrs, err := query.Select(MustParseSelectStatement(`SELECT moving_average(value, 2) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:00:16Z'`), &ic, nil)
	if err != nil {
		t.Fatal(err)
	} else if a, err := Iterators(itrs).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if diff := cmp.Diff(a, [][]query.Point{
		{&query.FloatPoint{Name: "cpu", Time: 4 * Second, Value: 15, Aggregated: 2}},
		{&query.FloatPoint{Name: "cpu", Time: 8 * Second, Value: 14.5, Aggregated: 2}},
		{&query.FloatPoint{Name: "cpu", Time: 12 * Second, Value: 11, Aggregated: 2}},
	}); diff != "" {
		t.Fatalf("unexpected points:\n%s", diff)
	}
}

func TestSelect_MovingAverage_Integer(t *testing.T) {
	var ic IteratorCreator
	ic.CreateIteratorFn = func(m *influxql.Measurement, opt query.IteratorOptions) (query.Iterator, error) {
		if m.Name != "cpu" {
			t.Fatalf("unexpected source: %s", m.Name)
		}
		return &IntegerIterator{Points: []query.IntegerPoint{
			{Name: "cpu", Time: 0 * Second, Value: 20},
			{Name: "cpu", Time: 4 * Second, Value: 10},
			{Name: "cpu", Time: 8 * Second, Value: 19},
			{Name: "cpu", Time: 12 * Second, Value: 3},
		}}, nil
	}

	// Execute selection.
	itrs, err := query.Select(MustParseSelectStatement(`SELECT moving_average(value, 2) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:00:16Z'`), &ic, nil)
	if err != nil {
		t.Fatal(err)
	} else if a, err := Iterators(itrs).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if diff := cmp.Diff(a, [][]query.Point{
		{&query.FloatPoint{Name: "cpu", Time: 4 * Second, Value: 15, Aggregated: 2}},
		{&query.FloatPoint{Name: "cpu", Time: 8 * Second, Value: 14.5, Aggregated: 2}},
		{&query.FloatPoint{Name: "cpu", Time: 12 * Second, Value: 11, Aggregated: 2}},
	}); diff != "" {
		t.Fatalf("unexpected points:\n%s", diff)
	}
}

func TestSelect_CumulativeSum_Float(t *testing.T) {
	var ic IteratorCreator
	ic.CreateIteratorFn = func(m *influxql.Measurement, opt query.IteratorOptions) (query.Iterator, error) {
		if m.Name != "cpu" {
			t.Fatalf("unexpected source: %s", m.Name)
		}
		return &FloatIterator{Points: []query.FloatPoint{
			{Name: "cpu", Time: 0 * Second, Value: 20},
			{Name: "cpu", Time: 4 * Second, Value: 10},
			{Name: "cpu", Time: 8 * Second, Value: 19},
			{Name: "cpu", Time: 12 * Second, Value: 3},
		}}, nil
	}

	// Execute selection.
	itrs, err := query.Select(MustParseSelectStatement(`SELECT cumulative_sum(value) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:00:16Z'`), &ic, nil)
	if err != nil {
		t.Fatal(err)
	} else if a, err := Iterators(itrs).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if diff := cmp.Diff(a, [][]query.Point{
		{&query.FloatPoint{Name: "cpu", Time: 0 * Second, Value: 20}},
		{&query.FloatPoint{Name: "cpu", Time: 4 * Second, Value: 30}},
		{&query.FloatPoint{Name: "cpu", Time: 8 * Second, Value: 49}},
		{&query.FloatPoint{Name: "cpu", Time: 12 * Second, Value: 52}},
	}); diff != "" {
		t.Fatalf("unexpected points:\n%s", diff)
	}
}

func TestSelect_CumulativeSum_Integer(t *testing.T) {
	var ic IteratorCreator
	ic.CreateIteratorFn = func(m *influxql.Measurement, opt query.IteratorOptions) (query.Iterator, error) {
		if m.Name != "cpu" {
			t.Fatalf("unexpected source: %s", m.Name)
		}
		return &IntegerIterator{Points: []query.IntegerPoint{
			{Name: "cpu", Time: 0 * Second, Value: 20},
			{Name: "cpu", Time: 4 * Second, Value: 10},
			{Name: "cpu", Time: 8 * Second, Value: 19},
			{Name: "cpu", Time: 12 * Second, Value: 3},
		}}, nil
	}

	// Execute selection.
	itrs, err := query.Select(MustParseSelectStatement(`SELECT cumulative_sum(value) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:00:16Z'`), &ic, nil)
	if err != nil {
		t.Fatal(err)
	} else if a, err := Iterators(itrs).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if diff := cmp.Diff(a, [][]query.Point{
		{&query.IntegerPoint{Name: "cpu", Time: 0 * Second, Value: 20}},
		{&query.IntegerPoint{Name: "cpu", Time: 4 * Second, Value: 30}},
		{&query.IntegerPoint{Name: "cpu", Time: 8 * Second, Value: 49}},
		{&query.IntegerPoint{Name: "cpu", Time: 12 * Second, Value: 52}},
	}); diff != "" {
		t.Fatalf("unexpected points:\n%s", diff)
	}
}

func TestSelect_CumulativeSum_Duplicate_Float(t *testing.T) {
	var ic IteratorCreator
	ic.CreateIteratorFn = func(m *influxql.Measurement, opt query.IteratorOptions) (query.Iterator, error) {
		if m.Name != "cpu" {
			t.Fatalf("unexpected source: %s", m.Name)
		}
		return &FloatIterator{Points: []query.FloatPoint{
			{Name: "cpu", Time: 0 * Second, Value: 20},
			{Name: "cpu", Time: 0 * Second, Value: 19},
			{Name: "cpu", Time: 4 * Second, Value: 10},
			{Name: "cpu", Time: 4 * Second, Value: 3},
		}}, nil
	}

	// Execute selection.
	itrs, err := query.Select(MustParseSelectStatement(`SELECT cumulative_sum(value) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:00:16Z'`), &ic, nil)
	if err != nil {
		t.Fatal(err)
	} else if a, err := Iterators(itrs).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if diff := cmp.Diff(a, [][]query.Point{
		{&query.FloatPoint{Name: "cpu", Time: 0 * Second, Value: 20}},
		{&query.FloatPoint{Name: "cpu", Time: 0 * Second, Value: 39}},
		{&query.FloatPoint{Name: "cpu", Time: 4 * Second, Value: 49}},
		{&query.FloatPoint{Name: "cpu", Time: 4 * Second, Value: 52}},
	}); diff != "" {
		t.Fatalf("unexpected points:\n%s", diff)
	}
}

func TestSelect_CumulativeSum_Duplicate_Integer(t *testing.T) {
	var ic IteratorCreator
	ic.CreateIteratorFn = func(m *influxql.Measurement, opt query.IteratorOptions) (query.Iterator, error) {
		if m.Name != "cpu" {
			t.Fatalf("unexpected source: %s", m.Name)
		}
		return &IntegerIterator{Points: []query.IntegerPoint{
			{Name: "cpu", Time: 0 * Second, Value: 20},
			{Name: "cpu", Time: 0 * Second, Value: 19},
			{Name: "cpu", Time: 4 * Second, Value: 10},
			{Name: "cpu", Time: 4 * Second, Value: 3},
		}}, nil
	}

	// Execute selection.
	itrs, err := query.Select(MustParseSelectStatement(`SELECT cumulative_sum(value) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:00:16Z'`), &ic, nil)
	if err != nil {
		t.Fatal(err)
	} else if a, err := Iterators(itrs).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if diff := cmp.Diff(a, [][]query.Point{
		{&query.IntegerPoint{Name: "cpu", Time: 0 * Second, Value: 20}},
		{&query.IntegerPoint{Name: "cpu", Time: 0 * Second, Value: 39}},
		{&query.IntegerPoint{Name: "cpu", Time: 4 * Second, Value: 49}},
		{&query.IntegerPoint{Name: "cpu", Time: 4 * Second, Value: 52}},
	}); diff != "" {
		t.Fatalf("unexpected points:\n%s", diff)
	}
}

func TestSelect_HoltWinters_GroupBy_Agg(t *testing.T) {
	var ic IteratorCreator
	ic.CreateIteratorFn = func(m *influxql.Measurement, opt query.IteratorOptions) (query.Iterator, error) {
		if m.Name != "cpu" {
			t.Fatalf("unexpected source: %s", m.Name)
		}
		return query.NewCallIterator(&FloatIterator{Points: []query.FloatPoint{
			{Name: "cpu", Time: 10 * Second, Value: 4},
			{Name: "cpu", Time: 11 * Second, Value: 6},

			{Name: "cpu", Time: 12 * Second, Value: 9},
			{Name: "cpu", Time: 13 * Second, Value: 11},

			{Name: "cpu", Time: 14 * Second, Value: 5},
			{Name: "cpu", Time: 15 * Second, Value: 7},

			{Name: "cpu", Time: 16 * Second, Value: 10},
			{Name: "cpu", Time: 17 * Second, Value: 12},

			{Name: "cpu", Time: 18 * Second, Value: 6},
			{Name: "cpu", Time: 19 * Second, Value: 8},
		}}, opt)
	}

	// Execute selection.
	itrs, err := query.Select(MustParseSelectStatement(`SELECT holt_winters(mean(value), 2, 2) FROM cpu WHERE time >= '1970-01-01T00:00:10Z' AND time < '1970-01-01T00:00:20Z' GROUP BY time(2s)`), &ic, nil)
	if err != nil {
		t.Fatal(err)
	} else if a, err := Iterators(itrs).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if diff := cmp.Diff(a, [][]query.Point{
		{&query.FloatPoint{Name: "cpu", Time: 20 * Second, Value: 11.960623419918432}},
		{&query.FloatPoint{Name: "cpu", Time: 22 * Second, Value: 7.953140268154609}},
	}); diff != "" {
		t.Fatalf("unexpected points:\n%s", diff)
	}
}

func TestSelect_UnsupportedCall(t *testing.T) {
	var ic IteratorCreator
	ic.CreateIteratorFn = func(m *influxql.Measurement, opt query.IteratorOptions) (query.Iterator, error) {
		if m.Name != "cpu" {
			t.Fatalf("unexpected source: %s", m.Name)
		}
		return &FloatIterator{}, nil
	}

	_, err := query.Select(MustParseSelectStatement(`SELECT foobar(value) FROM cpu`), &ic, nil)
	if err == nil || err.Error() != "unsupported call: foobar" {
		t.Errorf("unexpected error: %s", err)
	}
}

func TestSelect_InvalidQueries(t *testing.T) {
	var ic IteratorCreator
	ic.CreateIteratorFn = func(m *influxql.Measurement, opt query.IteratorOptions) (query.Iterator, error) {
		if m.Name != "cpu" {
			t.Fatalf("unexpected source: %s", m.Name)
		}
		return &FloatIterator{}, nil
	}

	tests := []struct {
		q   string
		err string
	}{
		{
			q:   `SELECT foobar(value) FROM cpu`,
			err: `unsupported call: foobar`,
		},
		{
			q:   `SELECT 'value' FROM cpu`,
			err: `invalid expression type: *influxql.StringLiteral`,
		},
		{
			q:   `SELECT 'value', value FROM cpu`,
			err: `invalid expression type: *influxql.StringLiteral`,
		},
	}

	for i, tt := range tests {
		itrs, err := query.Select(MustParseSelectStatement(tt.q), &ic, nil)
		if err == nil || err.Error() != tt.err {
			t.Errorf("%d. expected error '%s', got '%s'", i, tt.err, err)
		}
		query.Iterators(itrs).Close()
	}
}

func BenchmarkSelect_Raw_1K(b *testing.B)   { benchmarkSelectRaw(b, 1000) }
func BenchmarkSelect_Raw_100K(b *testing.B) { benchmarkSelectRaw(b, 1000000) }

func benchmarkSelectRaw(b *testing.B, pointN int) {
	benchmarkSelect(b, MustParseSelectStatement(`SELECT fval FROM cpu`), NewRawBenchmarkIteratorCreator(pointN))
}

func benchmarkSelect(b *testing.B, stmt *influxql.SelectStatement, ic query.IteratorCreator) {
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		itrs, err := query.Select(stmt, ic, nil)
		if err != nil {
			b.Fatal(err)
		}
		query.DrainIterators(itrs)
	}
}

// NewRawBenchmarkIteratorCreator returns a new mock iterator creator with generated fields.
func NewRawBenchmarkIteratorCreator(pointN int) *IteratorCreator {
	var ic IteratorCreator
	ic.CreateIteratorFn = func(m *influxql.Measurement, opt query.IteratorOptions) (query.Iterator, error) {
		if opt.Expr != nil {
			panic("unexpected expression")
		}

		p := query.FloatPoint{
			Name: "cpu",
			Aux:  make([]interface{}, len(opt.Aux)),
		}

		for i := range opt.Aux {
			switch opt.Aux[i].Val {
			case "fval":
				p.Aux[i] = float64(100)
			default:
				panic("unknown iterator expr: " + opt.Expr.String())
			}
		}

		return &FloatPointGenerator{N: pointN, Fn: func(i int) *query.FloatPoint {
			p.Time = int64(time.Duration(i) * (10 * time.Second))
			return &p
		}}, nil
	}
	return &ic
}

func benchmarkSelectDedupe(b *testing.B, seriesN, pointsPerSeries int) {
	stmt := MustParseSelectStatement(`SELECT sval::string FROM cpu`)
	stmt.Dedupe = true

	var ic IteratorCreator
	ic.CreateIteratorFn = func(m *influxql.Measurement, opt query.IteratorOptions) (query.Iterator, error) {
		if opt.Expr != nil {
			panic("unexpected expression")
		}

		p := query.FloatPoint{
			Name: "tags",
			Aux:  []interface{}{nil},
		}

		return &FloatPointGenerator{N: seriesN * pointsPerSeries, Fn: func(i int) *query.FloatPoint {
			p.Aux[0] = fmt.Sprintf("server%d", i%seriesN)
			return &p
		}}, nil
	}

	b.ResetTimer()
	benchmarkSelect(b, stmt, &ic)
}

func BenchmarkSelect_Dedupe_1K(b *testing.B) { benchmarkSelectDedupe(b, 1000, 100) }

func benchmarkSelectTop(b *testing.B, seriesN, pointsPerSeries int) {
	stmt := MustParseSelectStatement(`SELECT top(sval, 10) FROM cpu`)

	var ic IteratorCreator
	ic.CreateIteratorFn = func(m *influxql.Measurement, opt query.IteratorOptions) (query.Iterator, error) {
		if m.Name != "cpu" {
			b.Fatalf("unexpected source: %s", m.Name)
		}
		if !reflect.DeepEqual(opt.Expr, MustParseExpr(`sval`)) {
			b.Fatalf("unexpected expr: %s", spew.Sdump(opt.Expr))
		}

		p := query.FloatPoint{
			Name: "cpu",
		}

		return &FloatPointGenerator{N: seriesN * pointsPerSeries, Fn: func(i int) *query.FloatPoint {
			p.Value = float64(rand.Int63())
			p.Time = int64(time.Duration(i) * (10 * time.Second))
			return &p
		}}, nil
	}

	b.ResetTimer()
	benchmarkSelect(b, stmt, &ic)
}

func BenchmarkSelect_Top_1K(b *testing.B) { benchmarkSelectTop(b, 1000, 1000) }
