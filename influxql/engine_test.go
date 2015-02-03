package influxql_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/influxdb/influxdb/influxql"
)

// Ensure a mapper can process intervals across multiple iterators.
func TestMapper_Map(t *testing.T) {
	m := influxql.NewMapper(influxql.MapSum,
		NewIterator([]string{"foo"}, []Point{
			{"2000-01-01T00:00:00Z", float64(10)}, // first minute
			{"2000-01-01T00:00:30Z", float64(20)},
			{"2000-01-01T00:01:00Z", float64(30)}, // second minute
			{"2000-01-01T00:01:30Z", float64(40)},
			{"2000-01-01T00:03:00Z", float64(50)}, // fourth minute (skip third)
		}),
		1*time.Minute)

	ch := m.Map().C()
	if data := <-ch; !reflect.DeepEqual(data, map[influxql.Key]interface{}{influxql.Key{Timestamp: 946684800000000000, Values: "\x00\x03foo"}: float64(30)}) {
		t.Fatalf("unexpected data(0/foo): %#v", data)
	}
	if data := <-ch; !reflect.DeepEqual(data, map[influxql.Key]interface{}{influxql.Key{Timestamp: 946684860000000000, Values: "\x00\x03foo"}: float64(70)}) {
		t.Fatalf("unexpected data(1/foo): %#v", data)
	}
	if data := <-ch; !reflect.DeepEqual(data, map[influxql.Key]interface{}{influxql.Key{Timestamp: 946684920000000000, Values: "\x00\x03foo"}: float64(0)}) {
		t.Fatalf("unexpected data(2/foo): %#v", data)
	}
	if data := <-ch; !reflect.DeepEqual(data, map[influxql.Key]interface{}{influxql.Key{Timestamp: 946684980000000000, Values: "\x00\x03foo"}: float64(50)}) {
		t.Fatalf("unexpected data(3/foo): %#v", data)
	}
}

// Ensure a reducer can combine data received from a mapper.
func TestReducer_Reduce(t *testing.T) {
	m := []*influxql.Mapper{
		influxql.NewMapper(influxql.MapSum,
			NewIterator([]string{"foo"}, []Point{
				{"2000-01-01T00:00:00Z", float64(10)},
				{"2000-01-01T00:01:00Z", float64(20)},
			}), 1*time.Minute),
		influxql.NewMapper(influxql.MapSum,
			NewIterator([]string{"bar"}, []Point{
				{"2000-01-01T00:00:00Z", float64(100)},
				{"2000-01-01T00:01:00Z", float64(200)},
			}), 1*time.Minute),
		influxql.NewMapper(influxql.MapSum, NewIterator([]string{"foo"}, []Point{
			{"2000-01-01T00:00:00Z", float64(1000)},
			{"2000-01-01T00:01:00Z", float64(2000)},
		}), 1*time.Minute)}

	r := influxql.NewReducer(influxql.ReduceSum, m)
	ch := r.Reduce().C()
	if data := <-ch; !reflect.DeepEqual(data, map[influxql.Key]interface{}{influxql.Key{Timestamp: 946684800000000000, Values: "\x00\x03bar"}: float64(100)}) {
		t.Fatalf("unexpected data(0/bar): %#v", data)
	} else if data = <-ch; !reflect.DeepEqual(data, map[influxql.Key]interface{}{influxql.Key{Timestamp: 946684800000000000, Values: "\x00\x03foo"}: float64(1010)}) {
		t.Fatalf("unexpected data(0/foo): %#v", data)
	}
	if data := <-ch; !reflect.DeepEqual(data, map[influxql.Key]interface{}{influxql.Key{Timestamp: 946684860000000000, Values: "\x00\x03bar"}: float64(200)}) {
		t.Fatalf("unexpected data(1/bar): %#v", data)
	} else if data = <-ch; !reflect.DeepEqual(data, map[influxql.Key]interface{}{influxql.Key{Timestamp: 946684860000000000, Values: "\x00\x03foo"}: float64(2020)}) {
		t.Fatalf("unexpected data(1/foo): %#v", data)
	}
	if data, ok := <-ch; data != nil {
		t.Fatalf("unexpected data(end): %#v", data)
	} else if ok {
		t.Fatalf("expected channel close")
	}
}

// Ensure the planner can plan and execute a simple count query.
func TestPlanner_Plan_Count(t *testing.T) {
	tx := NewTx()
	tx.CreateIteratorsFunc = func(stmt *influxql.SelectStatement) ([]influxql.Iterator, error) {
		return []influxql.Iterator{
			NewIterator(nil, []Point{
				{"2000-01-01T00:00:00Z", float64(100)},
				{"2000-01-01T00:00:10Z", float64(90)},
				{"2000-01-01T00:00:20Z", float64(80)},
			}),
			NewIterator(nil, []Point{
				{"2000-01-01T00:00:30Z", float64(70)},
				{"2000-01-01T00:00:40Z", float64(60)},
				{"2000-01-01T00:00:50Z", float64(50)},
			})}, nil
	}

	// Expected resultset.
	exp := minify(`[{"name":"cpu","columns":["time","count"],"values":[[0,6]]}]`)

	// Execute and compare.
	rs := MustPlanAndExecute(NewDB(tx), `2000-01-01T12:00:00Z`,
		`SELECT count(value) FROM cpu WHERE time >= '2000-01-01'`)
	if act := minify(jsonify(rs)); exp != act {
		t.Fatalf("unexpected resultset: %s", act)
	}
}

// Ensure the planner can plan and execute a mean query
func TestPlanner_Plan_Mean(t *testing.T) {
	tx := NewTx()
	tx.CreateIteratorsFunc = func(stmt *influxql.SelectStatement) ([]influxql.Iterator, error) {
		return []influxql.Iterator{
			NewIterator(nil, []Point{
				{"2000-01-01T00:00:00Z", float64(100)},
				{"2000-01-01T00:00:10Z", float64(90)},
				{"2000-01-01T00:00:20Z", float64(80)},
			}),
			NewIterator(nil, []Point{
				{"2000-01-01T00:00:00Z", float64(80)},
				{"2000-01-01T00:00:10Z", float64(80)},
				{"2000-01-01T00:00:20Z", float64(50)},
			}),
			NewIterator(nil, []Point{
				{"2000-01-01T00:01:30Z", float64(70)},
				{"2000-01-01T00:01:40Z", float64(60)},
				{"2000-01-01T00:01:50Z", float64(50)},
			})}, nil
	}

	// Expected resultset.
	exp := minify(`[{"name":"cpu","columns":["time","mean"],"values":[[946684800000000000,80],[946684860000000000,60]]}]`)

	// Execute and compare.
	rs := MustPlanAndExecute(NewDB(tx), `2000-01-01T12:00:00Z`,
		`SELECT mean(value) FROM cpu WHERE time >= '2000-01-01' GROUP BY time(1m)`)
	if act := minify(jsonify(rs)); exp != act {
		t.Fatalf("unexpected resultset: %s", act)
	}
}

// Ensure the planner can plan and execute a percentile query
func TestPlanner_Plan_Percentile(t *testing.T) {
	tx := NewTx()
	tx.CreateIteratorsFunc = func(stmt *influxql.SelectStatement) ([]influxql.Iterator, error) {
		return []influxql.Iterator{
			NewIterator(nil, []Point{
				{"2000-01-01T00:00:00Z", float64(100)},
				{"2000-01-01T00:00:10Z", float64(90)},
				{"2000-01-01T00:00:20Z", float64(80)},
			}),
			NewIterator(nil, []Point{
				{"2000-01-01T00:00:00Z", float64(80)},
				{"2000-01-01T00:00:10Z", float64(80)},
				{"2000-01-01T00:00:20Z", float64(50)},
			}),
			NewIterator(nil, []Point{
				{"2000-01-01T00:01:30Z", float64(70)},
				{"2000-01-01T00:01:40Z", float64(60)},
				{"2000-01-01T00:01:50Z", float64(50)},
			})}, nil
	}

	// Expected resultset.
	exp := minify(`[{"name":"cpu","columns":["time","percentile"],"values":[[946684800000000000,80],[946684860000000000,60]]}]`)

	// Execute and compare.
	rs := MustPlanAndExecute(NewDB(tx), `2000-01-01T12:00:00Z`,
		`SELECT percentile(value, 60) FROM cpu WHERE time >= '2000-01-01' GROUP BY time(1m)`)
	if act := minify(jsonify(rs)); exp != act {
		t.Fatalf("unexpected resultset: %s", act)
	}

	exp = minify(`[{"name":"cpu","columns":["time","percentile"],"values":[[946684800000000000,100],[946684860000000000,70]]}]`)
	// Execute and compare.
	rs = MustPlanAndExecute(NewDB(tx), `2000-01-01T12:00:00Z`,
		`SELECT percentile(value, 99.9) FROM cpu WHERE time >= '2000-01-01' GROUP BY time(1m)`)
	if act := minify(jsonify(rs)); exp != act {
		t.Fatalf("unexpected resultset: %s", act)
	}
}

// Ensure the planner can plan and execute a query that returns raw data points
func TestPlanner_Plan_RawData(t *testing.T) {
	tx := NewTx()
	tx.CreateIteratorsFunc = func(stmt *influxql.SelectStatement) ([]influxql.Iterator, error) {
		return []influxql.Iterator{
			NewIterator(nil, []Point{
				{"2000-01-01T00:00:00Z", float64(100)},
				{"2000-01-01T00:00:10Z", float64(90)},
				{"2000-01-01T00:00:20Z", float64(80)},
			}),
			NewIterator(nil, []Point{
				{"2000-01-01T00:00:00Z", float64(70)},
				{"2000-01-01T00:00:10Z", float64(60)},
				{"2000-01-01T00:00:24Z", float64(50)},
			}),
			NewIterator(nil, []Point{
				{"2000-01-01T00:00:00Z", float64(40)},
				{"2000-01-01T00:00:10Z", float64(30)},
				{"2000-01-01T00:00:22Z", float64(20)},
			}),
			NewIterator(nil, []Point{
				{"2000-01-01T00:01:30Z", float64(10)},
				{"2000-01-01T00:01:40Z", float64(9)},
				{"2000-01-01T00:01:50Z", float64(8)},
			})}, nil
	}

	// Expected resultset.
	exp := minify(`[{"name":"cpu","columns":["time","value"],"values":[[946684800000000000,40],[946684810000000000,30],[946684820000000000,80],[946684822000000000,20],[946684824000000000,50],[946684890000000000,10],[946684900000000000,9],[946684910000000000,8]]}]`)

	// Execute and compare.
	rs := MustPlanAndExecute(NewDB(tx), `2000-01-01T12:00:00Z`,
		`SELECT value FROM cpu WHERE time >= '2000-01-01T00:00:11Z'`)
	if act := minify(jsonify(rs)); exp != act {
		t.Fatalf("unexpected resultset: %s", act)
	}
}

// Ensure the planner can plan and execute a count query grouped by hour.
func TestPlanner_Plan_GroupByInterval(t *testing.T) {
	tx := NewTx()
	tx.CreateIteratorsFunc = func(stmt *influxql.SelectStatement) ([]influxql.Iterator, error) {
		//min, max, interval := MustTimeRangeAndInterval(stmt, `2000-01-01T12:00:00Z`)
		return []influxql.Iterator{
			NewIterator(nil, []Point{
				{"2000-01-01T09:00:00Z", float64(100)},
				{"2000-01-01T09:00:00Z", float64(90)},
				{"2000-01-01T09:30:00Z", float64(80)},
			}),
			NewIterator(nil, []Point{
				{"2000-01-01T11:00:00Z", float64(70)},
				{"2000-01-01T11:00:00Z", float64(60)},
				{"2000-01-01T11:30:00Z", float64(50)},
			})}, nil
	}

	// Expected resultset.
	exp := minify(`[{
		"name":"cpu",
		"columns":["time","sum"],
		"values":[
			[946717200000000000,190],
			[946719000000000000,80],
			[946724400000000000,130],
			[946726200000000000,50]
		]
	}]`)

	// Query for data since 3 hours ago until now, grouped every 30 minutes.
	rs := MustPlanAndExecute(NewDB(tx), "2000-01-01T12:00:00Z", `
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
	tx := NewTx()
	tx.CreateIteratorsFunc = func(stmt *influxql.SelectStatement) ([]influxql.Iterator, error) {
		//min, max, interval := MustTimeRangeAndInterval(stmt, `2000-01-01T12:00:00Z`)
		return []influxql.Iterator{
			NewIterator([]string{"servera"}, []Point{
				{"2000-01-01T09:00:00Z", float64(10)},
				{"2000-01-01T09:30:00Z", float64(20)},
				{"2000-01-01T11:00:00Z", float64(30)},
				{"2000-01-01T11:30:00Z", float64(40)},
			}),
			NewIterator([]string{"serverb"}, []Point{
				{"2000-01-01T09:00:00Z", float64(1)},
				{"2000-01-01T11:00:00Z", float64(2)},
			})}, nil
	}

	// Query for data since 3 hours ago until now, grouped every 30 minutes.
	rs := MustPlanAndExecute(NewDB(tx), "2000-01-01T12:00:00Z", `
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
			[946717200000000000,30],
			[946720800000000000,0],
			[946724400000000000,70]
		]
	},{
		"name":"cpu",
		"tags":{"host":"serverb"},
		"columns":["time","sum"],
		"values":[
			[946717200000000000,1],
			[946720800000000000,0],
			[946724400000000000,2]
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
	tx := NewTx()
	tx.CreateIteratorsFunc = func(stmt *influxql.SelectStatement) ([]influxql.Iterator, error) {
		switch stmt.String() {
		case `SELECT cpu.0.value FROM cpu.0 GROUP BY time(10s)`:
			flag0 = true
		case `SELECT cpu.1.value FROM cpu.1 GROUP BY time(10s)`:
			flag1 = true
		default:
			t.Fatalf("unexpected stmt passed to iterator creator: %s", stmt.String())
		}
		return nil, nil
	}

	stmt := MustParseSelectStatement(`
		SELECT sum(cpu.0.value) + sum(cpu.1.value) AS sum
		FROM JOIN(cpu.0, cpu.1)
		WHERE time >= '2000-01-01 00:00:00' AND time < '2000-01-01 00:01:00'
		GROUP BY time(10s)`)

	p := influxql.NewPlanner(NewDB(tx))
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

// DB represents a mockable database.
type DB struct {
	BeginFunc func() (influxql.Tx, error)
}

// NewDB returns a mock database that returns a transaction for Begin().
func NewDB(tx influxql.Tx) *DB {
	return &DB{
		BeginFunc: func() (influxql.Tx, error) { return tx, nil },
	}
}

func (db *DB) Begin() (influxql.Tx, error) { return db.BeginFunc() }

// Tx represents a mockable transaction.
type Tx struct {
	OpenFunc   func() error
	CloseFunc  func() error
	SetNowFunc func(time.Time)

	CreateIteratorsFunc func(*influxql.SelectStatement) ([]influxql.Iterator, error)
}

// NewTx returns a new mock Tx.
func NewTx() *Tx {
	return &Tx{
		OpenFunc:   func() error { return nil },
		CloseFunc:  func() error { return nil },
		SetNowFunc: func(_ time.Time) {},
	}
}

func (tx *Tx) Open() error          { return tx.OpenFunc() }
func (tx *Tx) Close() error         { return tx.CloseFunc() }
func (tx *Tx) SetNow(now time.Time) { tx.SetNowFunc(now) }

func (tx *Tx) CreateIterators(stmt *influxql.SelectStatement) ([]influxql.Iterator, error) {
	return tx.CreateIteratorsFunc(stmt)
}

// Iterator represents an implementation of Iterator.
type Iterator struct {
	tags   string  // encoded dimensional tag values
	points []Point // underlying point data
	index  int     // current point index
}

// NewIterator returns a new iterator.
func NewIterator(tags []string, points []Point) *Iterator {
	return &Iterator{tags: string(influxql.MarshalStrings(tags)), points: points}
}

// Tags returns the encoded dimensional tag values.
func (i *Iterator) Tags() string { return i.tags }

// Next returns the next point's timestamp and field value.
func (i *Iterator) Next() (key int64, value interface{}) {
	// If index is beyond points range then return nil.
	if i.index > len(i.points)-1 {
		return 0, nil
	}

	// Retrieve point and extract value.
	p := i.points[i.index]
	i.index++
	return p.Time(), p.Value
}

// Point represents a single value at a given time.
type Point struct {
	Timestamp string // ISO-8601 formatted timestamp.
	Value     interface{}
}

// Time returns the Timestamp as nanoseconds since epoch.
func (p Point) Time() int64 { return mustParseTime(p.Timestamp).UnixNano() }

// PlanAndExecute plans, executes, and retrieves all rows.
func PlanAndExecute(db influxql.DB, now string, querystring string) ([]*influxql.Row, error) {
	// Plan statement.
	p := influxql.NewPlanner(db)
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
func MustPlanAndExecute(db influxql.DB, now string, querystring string) []*influxql.Row {
	rs, err := PlanAndExecute(db, now, querystring)
	if err != nil {
		panic(err.Error())
	}
	return rs
}

// MustTimeRangeAndInterval returns the time range & interval of the query.
// Set max to 2000-01-01 if zero. Panic on error.
func MustTimeRangeAndInterval(stmt *influxql.SelectStatement, defaultMax string) (time.Time, time.Time, time.Duration) {
	min, max := influxql.TimeRange(stmt.Condition)
	interval, _, err := stmt.Dimensions.Normalize()
	if err != nil {
		panic(err.Error())
	}
	if max.IsZero() {
		max = mustParseTime(defaultMax)
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

func unix(nsec int64) time.Time { return time.Unix(0, nsec).UTC() }
