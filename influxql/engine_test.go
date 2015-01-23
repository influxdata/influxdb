package influxql_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/influxdb/influxdb/influxql"
)

// Ensure the planner can plan and execute a simple count query.
func TestPlanner_Plan_Count(t *testing.T) {
	ic := &IteratorCreator{
		CreateIteratorsFunc: func(stmt *influxql.SelectStatement) ([]influxql.Iterator, error) {
			min, max, interval := MustTimeRangeAndInterval(stmt)
			return []influxql.Iterator{
				NewIterator(min, max, interval, nil, []Point{
					{"2000-01-01T00:00:00Z", float64(100)},
					{"2000-01-01T00:00:10Z", float64(90)},
					{"2000-01-01T00:00:20Z", float64(80)},
				}),
				NewIterator(min, max, interval, nil, []Point{
					{"2000-01-01T00:00:30Z", float64(70)},
					{"2000-01-01T00:00:40Z", float64(60)},
					{"2000-01-01T00:00:50Z", float64(50)},
				})}, nil
		},
	}

	// Expected resultset.
	exp := minify(`[{"name":"cpu","columns":["time","count"],"values":[[978307200000000,6]]}]`)

	// Execute and compare.
	rs := MustPlanAndExecute(ic, `2000-01-01T12:00:00Z`,
		`SELECT count(value) FROM cpu WHERE time >= '2001-01-01'`)
	if act := minify(jsonify(rs)); exp != act {
		t.Fatalf("unexpected resultset: %s", act)
	}
}

// Ensure the planner can plan and execute a count query grouped by hour.
func TestPlanner_Plan_GroupByInterval(t *testing.T) {
	ic := &IteratorCreator{
		CreateIteratorsFunc: func(stmt *influxql.SelectStatement) ([]influxql.Iterator, error) {
			min, max, interval := MustTimeRangeAndInterval(stmt)
			return []influxql.Iterator{
				NewIterator(min, max, interval, nil, []Point{
					{"2000-01-01T09:00:00Z", float64(100)},
					{"2000-01-01T09:00:00Z", float64(90)},
					{"2000-01-01T09:30:00Z", float64(80)},
				}),
				NewIterator(min, max, interval, nil, []Point{
					{"2000-01-01T11:00:00Z", float64(70)},
					{"2000-01-01T11:00:00Z", float64(60)},
					{"2000-01-01T11:30:00Z", float64(50)},
				})}, nil
		},
	}

	// Expected resultset.
	exp := minify(`[{
		"name":"cpu",
		"columns":["time","sum"],
		"values":[
			[946717200000000,190],
			[946719000000000,80],
			[946720800000000,0],
			[946722600000000,0],
			[946724400000000,130],
			[946726200000000,50]
		]
	}]`)

	// Query for data since 3 hours ago until now, grouped every 30 minutes.
	rs := MustPlanAndExecute(ic, "2000-01-01T12:00:00Z", `
		SELECT sum(value)
		FROM cpu
		WHERE time >= now() - 3h
		GROUP BY time(30m)`)

	// Compare resultsets.
	if act := jsonify(rs); exp != act {
		t.Fatalf("unexpected resultset: %s", indent(act))
	}
}

// Ensure the planner can plan and execute a query grouped by interval and tag.
func TestPlanner_Plan_GroupByIntervalAndTag(t *testing.T) {
	ic := &IteratorCreator{
		CreateIteratorsFunc: func(stmt *influxql.SelectStatement) ([]influxql.Iterator, error) {
			min, max, interval := MustTimeRangeAndInterval(stmt)
			return []influxql.Iterator{
				NewIterator(min, max, interval, []string{"servera"}, []Point{
					{"2000-01-01T09:00:00Z", float64(10)},
					{"2000-01-01T09:30:00Z", float64(20)},
					{"2000-01-01T11:00:00Z", float64(30)},
					{"2000-01-01T11:30:00Z", float64(40)},
				}),
				NewIterator(min, max, interval, []string{"serverb"}, []Point{
					{"2000-01-01T09:00:00Z", float64(1)},
					{"2000-01-01T11:00:00Z", float64(2)},
				})}, nil
		},
	}

	// Query for data since 3 hours ago until now, grouped every 30 minutes.
	rs := MustPlanAndExecute(ic, "2000-01-01T12:00:00Z", `
		SELECT sum(value)
		FROM cpu
		WHERE time >= now() - 3h
		GROUP BY time(1h), host`)

	// Expected resultset.
	exp := minify(`[{
		"name":"cpu",
		"tags":{"host":"servera"},
		"columns":["time","sum"],
		"values":[
			[946717200000000,30],
			[946720800000000,0],
			[946724400000000,70]
		]
	},{
		"name":"cpu",
		"tags":{"host":"serverb"},
		"columns":["time","sum"],
		"values":[
			[946717200000000,1],
			[946720800000000,0],
			[946724400000000,2]
		]
	}]`)

	// Compare resultsets.
	if act := jsonify(rs); exp != act {
		t.Fatalf("unexpected resultset:\n\nexp=%s\n\ngot=%s\n\n", exp, act)
	}
}

// Ensure the planner sends the correct simplified statements to the iterator creator.
func TestPlanner_CreateIterators(t *testing.T) {
	var flag0, flag1 bool
	ic := &IteratorCreator{
		CreateIteratorsFunc: func(stmt *influxql.SelectStatement) ([]influxql.Iterator, error) {
			switch stmt.String() {
			case `SELECT cpu.0.value FROM cpu.0 GROUP BY time(10s)`:
				flag0 = true
			case `SELECT cpu.1.value FROM cpu.1 GROUP BY time(10s)`:
				flag1 = true
			default:
				t.Fatalf("unexpected stmt passed to iterator creator: %s", stmt.String())
			}
			return nil, nil
		},
	}

	stmt := MustParseSelectStatement(`
		SELECT sum(cpu.0.value) + sum(cpu.1.value) AS sum
		FROM JOIN(cpu.0, cpu.1)
		WHERE time >= '2000-01-01 00:00:00' AND time < '2000-01-01 00:01:00'
		GROUP BY time(10s)`)

	p := influxql.NewPlanner(ic)
	p.Now = func() time.Time { return mustParseTime("2000-01-01T12:00:00Z") }
	if _, err := p.Plan(stmt); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	// Verify correct statements were passed through.
	if !flag0 {
		t.Error("cpu.0 substatement not passed in")
	}
	if !flag1 {
		t.Error("cpu.1 substatement not passed in")
	}
}

// IteratorCreator represents a mockable iterator creator.
type IteratorCreator struct {
	CreateIteratorsFunc func(stmt *influxql.SelectStatement) ([]influxql.Iterator, error)
}

// CreateIterators returns a list of iterators for a simple SELECT statement.
func (ic *IteratorCreator) CreateIterators(stmt *influxql.SelectStatement) ([]influxql.Iterator, error) {
	return ic.CreateIteratorsFunc(stmt)
}

// Iterator represents an implementation of Iterator.
type Iterator struct {
	min, max int64 // time range
	interval int64 // interval duration

	tags   []string
	points []Point

	index      int   // current point index
	imin, imax int64 // current interval time range
}

// NewIterator returns a new iterator.
func NewIterator(min, max time.Time, interval time.Duration, tags []string, points []Point) *Iterator {
	i := &Iterator{interval: int64(interval), tags: tags, points: points}
	if !min.IsZero() {
		i.min = min.UnixNano()
	}
	if !max.IsZero() {
		i.max = max.UnixNano()
	}
	return i
}

func (i *Iterator) Open() error  { i.imin = -1; return nil }
func (i *Iterator) Close() error { return nil }

// NextIterval moves the iterator to the next available interval.
// Returns true if another iterval is available.
func (i *Iterator) NextIterval() bool {
	// Initialize interval start time if not set.
	// If there's no duration then there's only one interval.
	// Otherwise increment it by the interval.
	if i.imin == -1 {
		i.imin = i.min
	} else if i.interval == 0 {
		return false
	} else {
		// Update interval start time if it's before iterator end time.
		// Otherwise return false.
		if imin := i.imin + i.interval; i.max == 0 || imin < i.max {
			i.imin = imin
		} else {
			return false
		}
	}

	// Interval end time should be the start time plus interval duration.
	// If the end time is beyond the iterator end time then shorten it.
	i.imax = i.imin + i.interval
	if i.imax > i.max {
		i.imax = i.max
	}

	return true
}

// Next returns the next point's timestamp and field value.
func (i *Iterator) Next() (timestamp int64, value interface{}) {
	for {
		// If index is beyond points range then return nil.
		if i.index > len(i.points)-1 {
			return 0, nil
		}

		// Retrieve point and extract value.
		p := i.points[i.index]

		// Move cursor forward.
		i.index++

		// If timestamp is beyond bucket time range then move index back and exit.
		timestamp := p.Time()
		if timestamp >= i.imax && i.imax != 0 {
			i.index--
			return 0, nil
		}

		return p.Time(), p.Value
	}
}

// Key returns the interval start time and tag values encoded as a byte slice.
func (i *Iterator) Key() []byte { return influxql.MarshalKey(i.imin, i.tags) }

// Point represents a single value at a given time.
type Point struct {
	Timestamp string // ISO-8601 formatted timestamp.
	Value     interface{}
}

// Time returns the Timestamp as nanoseconds since epoch.
func (p Point) Time() int64 { return mustParseTime(p.Timestamp).UnixNano() }

// PlanAndExecute plans, executes, and retrieves all rows.
func PlanAndExecute(ic influxql.IteratorCreator, now string, querystring string) ([]*influxql.Row, error) {
	// Plan statement.
	p := influxql.NewPlanner(ic)
	p.Now = func() time.Time { return mustParseTime(now) }
	e, err := p.Plan(MustParseSelectStatement(querystring))
	if err != nil {
		return nil, err
	}

	// Execute plan.
	ch, err := e.Execute()
	if err != nil {
		return nil, err
	}

	// Collect resultset.
	var rs []*influxql.Row
	for row := range ch {
		rs = append(rs, row)
	}

	return rs, nil
}

// MustPlanAndExecute plans, executes, and retrieves all rows. Panic on error.
func MustPlanAndExecute(ic influxql.IteratorCreator, now string, querystring string) []*influxql.Row {
	rs, err := PlanAndExecute(ic, now, querystring)
	if err != nil {
		panic(err.Error())
	}
	return rs
}

// MustTimeRangeAndInterval returns the time range & interval of the query. Panic on error.
func MustTimeRangeAndInterval(stmt *influxql.SelectStatement) (time.Time, time.Time, time.Duration) {
	min, max := influxql.TimeRange(stmt.Condition)
	interval, _, err := stmt.Dimensions.Normalize()
	if err != nil {
		panic(err.Error())
	}
	return min, max, interval
}

func warn(v ...interface{})              { fmt.Fprintln(os.Stderr, v...) }
func warnf(msg string, v ...interface{}) { fmt.Fprintf(os.Stderr, msg+"\n", v...) }

// mustParseTime parses an IS0-8601 string. Panic on error.
func mustParseTime(s string) time.Time {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		panic(err.Error())
	}
	return t
}

// mustMarshalJSON encodes a value to JSON.
func mustMarshalJSON(v interface{}) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic("marshal json: " + err.Error())
	}
	return b
}

// jsonify encodes a value to JSON and returns as a string.
func jsonify(v interface{}) string { return string(mustMarshalJSON(v)) }

// ident indents a JSON string.
func indent(s string) string {
	var buf bytes.Buffer
	json.Indent(&buf, []byte(s), "", "  ")
	return buf.String()
}

// minify removes tabs and newlines.
func minify(s string) string { return strings.NewReplacer("\n", "", "\t", "").Replace(s) }
