package influxdb_test

import (
	"reflect"
	"testing"
	"time"

	"github.com/influxdb/influxdb"
)

// Ensure the planner can generate an appropriate executor.
func TestPlanner(t *testing.T) {
	// Create a planner to a mock database with multiple series:
	//
	//   1. "cpu"    - cpu usage
	//   2. "visits" - page view tracking
	//   3. "errors" - system errors
	//
	// Each series has a "host" tag.
	var p influxdb.Planner
	p.NewIteratorFunc = func(name string, tags map[string]string) []influxdb.Iterator {
		switch name {
		case "cpu":
			return []influxdb.Iterator{&sliceIterator{Points: []influxdb.Point{
				&point{"timestamp": mustParseTime("2000-01-01T00:00:00Z"), "value": float64(10)},
				&point{"timestamp": mustParseTime("2000-01-01T00:00:00Z"), "value": float64(60)},
				&point{"timestamp": mustParseTime("2000-01-01T00:01:30Z"), "value": float64(50)},
			}}}
		case "visits":
			return []influxdb.Iterator{&sliceIterator{Points: []influxdb.Point{
				&point{"timestamp": mustParseTime("2000-01-01T00:00:00Z"), "path": "/", "user_id": 123},
				&point{"timestamp": mustParseTime("2000-01-01T00:01:00Z"), "path": "/signup", "user_id": 456},
				&point{"timestamp": mustParseTime("2000-01-01T00:01:00Z"), "path": "/login", "user_id": 123},
			}}}
		case "errors":
		}
		panic("series not found: " + name)
	}

	// Set up a list of example queries with their expected result set.
	var tests = []struct {
		q   string
		res [][]interface{}
	}{
		// 0. Retrieve raw data.
		{
			q:   `SELECT value FROM cpu`,
			res: [][]interface{}{{float64(10)}, {float64(60)}, {float64(50)}},
		},

		// 1. Simple count.
		{
			q:   `SELECT count() FROM cpu`,
			res: [][]interface{}{{3}},
		},

		// 2. Sum grouped by time.
		{
			q:   `SELECT sum(value) FROM cpu GROUP BY time(1m)`,
			res: [][]interface{}{{-1}},
		},
	}

	// Iterate over each test, parse the query, plan & execute the statement.
	// Retrieve all the result rows and compare with the expected result.
	for i, tt := range tests {
		// Plan and execute.
		q := mustParseSelectStatement(tt.q)
		ch, err := p.Plan(q).Execute()
		if err != nil {
			t.Errorf("%d. %q: execute error: %s", i, tt.q, err)
			continue
		}

		// Collect all the results.
		var res [][]interface{}
		for row := range ch {
			res = append(res, row)
		}

		// Compare the results to what is expected.
		if !reflect.DeepEqual(tt.res, res) {
			t.Errorf("%d. %q: result mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.q, tt.res, res)
			continue
		}
	}
}

// sliceIterator iterates over a slice of points.
type sliceIterator struct {
	Points []influxdb.Point
	Index  int
}

// Next returns the next point in the iterator.
func (i *sliceIterator) Next() (p influxdb.Point) {
	if i.Index < len(i.Points) {
		p = i.Points[i.Index]
		i.Index++
	}
	return
}

// point represents a single timeseries data point.
// The "timestamp" key is reserved for the timestamp.
type point map[string]interface{}

// Timestamp returns the time on the point in nanoseconds since epoch.
// Panic if the "timestamp" key is not a time.
func (p point) Timestamp() int64 {
	return p["timestamp"].(time.Time).UnixNano()
}

// Value returns a value by name.
func (p point) Value(name string) interface{} { return p[name] }
