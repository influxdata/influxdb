package influxql_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/influxdb/influxdb/influxql"
)

// Ensure the planner can plan and execute a simple count query.
func TestPlanner_Plan_Count(t *testing.T) {
	db := NewDB("2000-01-01T12:00:00Z")
	db.WriteSeries("cpu", map[string]string{}, "2000-01-01T00:00:00Z", map[string]interface{}{"value": float64(100)})
	db.WriteSeries("cpu", map[string]string{}, "2000-01-01T00:00:10Z", map[string]interface{}{"value": float64(90)})
	db.WriteSeries("cpu", map[string]string{}, "2000-01-01T00:00:20Z", map[string]interface{}{"value": float64(80)})
	db.WriteSeries("cpu", map[string]string{}, "2000-01-01T00:00:30Z", map[string]interface{}{"value": float64(70)})
	db.WriteSeries("cpu", map[string]string{}, "2000-01-01T00:00:40Z", map[string]interface{}{"value": float64(60)})
	db.WriteSeries("cpu", map[string]string{}, "2000-01-01T00:00:50Z", map[string]interface{}{"value": float64(50)})

	// Expected resultset.
	exp := minify(`[{"name":"cpu","columns":["time","count"],"values":[[0,6]]}]`)

	// Execute and compare.
	rs := db.MustPlanAndExecute(`SELECT count(value) FROM cpu`)
	if act := minify(jsonify(rs)); exp != act {
		t.Fatalf("unexpected resultset: %s", act)
	}
}

// Ensure the planner can plan and execute a count query across multiple series.
func TestPlanner_Plan_Count_Multiseries(t *testing.T) {
	db := NewDB("2000-01-01T12:00:00Z")
	db.WriteSeries("cpu", map[string]string{"host": "servera"}, "2000-01-01T00:00:00Z", map[string]interface{}{"value": float64(100)})
	db.WriteSeries("cpu", map[string]string{"host": "serverb"}, "2000-01-01T00:00:10Z", map[string]interface{}{"value": float64(90)})
	db.WriteSeries("cpu", map[string]string{"host": "serverb"}, "2000-01-01T00:00:20Z", map[string]interface{}{"value": float64(80)})
	db.WriteSeries("cpu", map[string]string{"host": "servera"}, "2000-01-01T00:00:30Z", map[string]interface{}{"value": float64(70)})
	db.WriteSeries("cpu", map[string]string{"host": "serverb"}, "2000-01-01T00:00:40Z", map[string]interface{}{"value": float64(60)})
	db.WriteSeries("cpu", map[string]string{"host": "servera", "region": "us-west"}, "2000-01-01T00:00:50Z", map[string]interface{}{"value": float64(50)})

	// Expected resultset.
	exp := minify(`[{"name":"cpu","columns":["time","count"],"values":[[0,6]]}]`)

	// Execute and compare.
	rs := db.MustPlanAndExecute(`SELECT count(value) FROM cpu`)
	if act := minify(jsonify(rs)); exp != act {
		t.Fatalf("unexpected resultset: %s", act)
	}
}

// Ensure the planner can plan and execute a count query grouped by hour.
func TestPlanner_Plan_GroupByInterval(t *testing.T) {
	db := NewDB("2000-01-01T12:00:00Z")
	db.WriteSeries("cpu", map[string]string{}, "2000-01-01T09:00:00Z", map[string]interface{}{"value": float64(100)})
	db.WriteSeries("cpu", map[string]string{}, "2000-01-01T09:00:00Z", map[string]interface{}{"value": float64(90)})
	db.WriteSeries("cpu", map[string]string{}, "2000-01-01T09:30:00Z", map[string]interface{}{"value": float64(80)})
	db.WriteSeries("cpu", map[string]string{}, "2000-01-01T11:00:00Z", map[string]interface{}{"value": float64(70)})
	db.WriteSeries("cpu", map[string]string{}, "2000-01-01T11:00:00Z", map[string]interface{}{"value": float64(60)})
	db.WriteSeries("cpu", map[string]string{}, "2000-01-01T11:30:00Z", map[string]interface{}{"value": float64(50)})

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
	rs := db.MustPlanAndExecute(`
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
	db := NewDB("2000-01-01T12:00:00Z")
	db.WriteSeries("cpu", map[string]string{"host": "servera"}, "2000-01-01T09:00:00Z", map[string]interface{}{"value": float64(10)})
	db.WriteSeries("cpu", map[string]string{"host": "servera"}, "2000-01-01T09:30:00Z", map[string]interface{}{"value": float64(20)})
	db.WriteSeries("cpu", map[string]string{"host": "servera"}, "2000-01-01T11:00:00Z", map[string]interface{}{"value": float64(30)})
	db.WriteSeries("cpu", map[string]string{"host": "servera"}, "2000-01-01T11:30:00Z", map[string]interface{}{"value": float64(40)})

	db.WriteSeries("cpu", map[string]string{"host": "serverb"}, "2000-01-01T09:00:00Z", map[string]interface{}{"value": float64(1)})
	db.WriteSeries("cpu", map[string]string{"host": "serverb"}, "2000-01-01T11:00:00Z", map[string]interface{}{"value": float64(2)})

	// Query for data since 3 hours ago until now, grouped every 30 minutes.
	rs := db.MustPlanAndExecute(`
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
		t.Fatalf("unexpected resultset: \n\n%s\n\n%s\n\n", exp, act)
	}
}

// Ensure the planner can plan and execute a query filtered by tag.
func TestPlanner_Plan_FilterByTag(t *testing.T) {
	db := NewDB("2000-01-01T12:00:00Z")
	db.WriteSeries("cpu", map[string]string{"host": "servera", "region": "us-west"}, "2000-01-01T09:00:00Z", map[string]interface{}{"value": float64(1)})
	db.WriteSeries("cpu", map[string]string{"host": "servera", "region": "us-west"}, "2000-01-01T09:30:00Z", map[string]interface{}{"value": float64(2)})
	db.WriteSeries("cpu", map[string]string{"host": "servera", "region": "us-west"}, "2000-01-01T11:00:00Z", map[string]interface{}{"value": float64(3)})
	db.WriteSeries("cpu", map[string]string{"host": "servera", "region": "us-west"}, "2000-01-01T11:30:00Z", map[string]interface{}{"value": float64(4)})

	db.WriteSeries("cpu", map[string]string{"host": "serverb", "region": "us-east"}, "2000-01-01T09:00:00Z", map[string]interface{}{"value": float64(10)})
	db.WriteSeries("cpu", map[string]string{"host": "serverb", "region": "us-east"}, "2000-01-01T11:00:00Z", map[string]interface{}{"value": float64(20)})

	db.WriteSeries("cpu", map[string]string{"host": "serverc", "region": "us-west"}, "2000-01-01T09:00:00Z", map[string]interface{}{"value": float64(100)})
	db.WriteSeries("cpu", map[string]string{"host": "serverc", "region": "us-west"}, "2000-01-01T11:00:00Z", map[string]interface{}{"value": float64(200)})

	// Query for data since 3 hours ago until now, grouped every 30 minutes.
	rs := db.MustPlanAndExecute(`
		SELECT sum(value)
		FROM cpu
		WHERE time >= now() - 3h AND region = 'us-west'
		GROUP BY time(1h), host`)

	// Expected resultset.
	exp := minify(`[{
		"name":"cpu",
		"tags":{"host":"servera"},
		"columns":["time","sum"],
		"values":[
			[946717200000000,3],
			[946720800000000,0],
			[946724400000000,7]
		]
	},{
		"name":"cpu",
		"tags":{"host":"serverc"},
		"columns":["time","sum"],
		"values":[
			[946717200000000,100],
			[946720800000000,0],
			[946724400000000,200]
		]
	}]`)

	// Compare resultsets.
	if act := jsonify(rs); exp != act {
		t.Fatalf("unexpected resultset: %s", indent(act))
	}
}

// Ensure the planner can plan and execute a joined query.
func TestPlanner_Plan_Join(t *testing.T) {
	db := NewDB("2000-01-01T12:00:00Z")
	db.WriteSeries("cpu.0", map[string]string{}, "2000-01-01T00:00:00Z", map[string]interface{}{"value": float64(1)})
	db.WriteSeries("cpu.0", map[string]string{}, "2000-01-01T00:00:10Z", map[string]interface{}{"value": float64(2)})
	db.WriteSeries("cpu.0", map[string]string{}, "2000-01-01T00:00:20Z", map[string]interface{}{"value": float64(3)})
	db.WriteSeries("cpu.0", map[string]string{}, "2000-01-01T00:00:30Z", map[string]interface{}{"value": float64(4)})

	db.WriteSeries("cpu.1", map[string]string{}, "2000-01-01T00:00:00Z", map[string]interface{}{"value": float64(10)})
	db.WriteSeries("cpu.1", map[string]string{}, "2000-01-01T00:00:10Z", map[string]interface{}{"value": float64(20)})
	db.WriteSeries("cpu.1", map[string]string{}, "2000-01-01T00:00:30Z", map[string]interface{}{"value": float64(40)})

	// Query must join the series and sum the values.
	rs := db.MustPlanAndExecute(`
		SELECT sum(cpu.0.value) + sum(cpu.1.value) AS "sum"
		FROM JOIN(cpu.0, cpu.1)
		WHERE time >= "2000-01-01 00:00:00" AND time < "2000-01-01 00:01:00"
		GROUP BY time(10s)`)

	// Expected resultset.
	exp := minify(`[{
		"columns":["time","sum"],
		"values":[
			[946684800000000,11],
			[946684810000000,22],
			[946684820000000,3],
			[946684830000000,44],
			[946684840000000,0],
			[946684850000000,0]
		]
	}]`)

	// Compare resultsets.
	if act := jsonify(rs); exp != act {
		t.Fatalf("unexpected resultset: %s", indent(act))
	}
}

// DB represents an in-memory test database that implements methods for Planner.
type DB struct {
	measurements map[string]*Measurement
	series       map[uint32]*Series
	maxSeriesID  uint32

	Now time.Time
}

// NewDB returns a new instance of DB at a given time.
func NewDB(now string) *DB {
	return &DB{
		measurements: make(map[string]*Measurement),
		series:       make(map[uint32]*Series),
		Now:          mustParseTime(now),
	}
}

// PlanAndExecute plans, executes, and retrieves all rows.
func (db *DB) PlanAndExecute(querystring string) ([]*influxql.Row, error) {
	// Plan statement.
	p := influxql.NewPlanner(db)
	p.Now = func() time.Time { return db.Now }
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
func (db *DB) MustPlanAndExecute(querystring string) []*influxql.Row {
	rs, err := db.PlanAndExecute(querystring)
	if err != nil {
		panic(err.Error())
	}
	return rs
}

// WriteSeries writes a series
func (db *DB) WriteSeries(name string, tags map[string]string, timestamp string, values map[string]interface{}) {
	// Find or create measurement & series.
	m, s := db.CreateSeriesIfNotExists(name, tags)

	// Create point.
	p := &point{
		timestamp: mustParseTime(timestamp).UTC().UnixNano(),
		values:    make(map[uint8]interface{}),
	}

	// Map field names to field ids.
	for k, v := range values {
		f := m.CreateFieldIfNotExists(k, influxql.InspectDataType(v))
		p.values[f.id] = v
	}

	// Add point to series.
	s.points = append(s.points, p)

	// Sort series points by time.
	sort.Sort(points(s.points))
}

// CreateSeriesIfNotExists returns a measurement & series by name/tagset.
// Creates them if they don't exist.
func (db *DB) CreateSeriesIfNotExists(name string, tags map[string]string) (*Measurement, *Series) {
	// Find or create meaurement
	m := db.measurements[name]
	if m == nil {
		m = NewMeasurement(name)
		db.measurements[name] = m
	}

	// Normalize tags and try to match against existing series.
	if tags == nil {
		tags = make(map[string]string)
	}
	for _, s := range m.series {
		if reflect.DeepEqual(s.tags, tags) {
			return m, s
		}
	}

	// Create new series.
	db.maxSeriesID++
	s := &Series{id: db.maxSeriesID, tags: tags}

	// Add series to DB and measurement.
	db.series[s.id] = s
	m.series[s.id] = s

	return m, s
}

// MatchSeries returns the series ids that match a name and tagset.
func (db *DB) MatchSeries(name string, tags map[string]string) []uint32 {
	// Find measurement.
	m := db.measurements[name]
	if m == nil {
		return nil
	}

	// Compare tagsets against each series.
	var ids []uint32
	for _, s := range m.series {
		// Check that each tag value matches the series' tag values.
		matched := true
		for k, v := range tags {
			if s.tags[k] != v {
				matched = false
				break
			}
		}

		// Append series if all tags match.
		if matched {
			ids = append(ids, s.id)
		}
	}

	return ids
}

// SeriesTagValues returns a slice of tag values for a given series and tag keys.
func (db *DB) SeriesTagValues(seriesID uint32, keys []string) (values []string) {
	values = make([]string, len(keys))

	// Find series.
	s := db.series[seriesID]
	if s == nil {
		return
	}

	// Loop over keys and set values.
	for i, key := range keys {
		values[i] = s.tags[key]
	}
	return
}

// FieldID returns the field identifier for a given measurement name and field name.
func (db *DB) Field(name, field string) (fieldID uint8, typ influxql.DataType) {
	// Find measurement.
	m := db.measurements[name]
	if m == nil {
		return
	}

	// Find field.
	f := m.fields[field]
	if f == nil {
		return
	}

	return f.id, f.typ
}

// CreateIterator returns a new iterator for a given field.
func (db *DB) CreateIterator(seriesID uint32, fieldID uint8, typ influxql.DataType, min, max time.Time, interval time.Duration) influxql.Iterator {
	s := db.series[seriesID]
	if s == nil {
		panic(fmt.Sprintf("series not found: %d", seriesID))
	}

	// Create iterator.
	i := &iterator{
		points:   s.points,
		fieldID:  fieldID,
		typ:      typ,
		imin:     -1,
		interval: int64(interval),
	}

	if !min.IsZero() {
		i.min = min.UnixNano()
	}
	if !max.IsZero() {
		i.max = max.UnixNano()
	}

	return i
}

// iterator represents an implementation of Iterator over a set of points.
type iterator struct {
	fieldID uint8
	typ     influxql.DataType

	index  int
	points points

	min, max   int64 // time range
	imin, imax int64 // interval time range
	interval   int64 // interval duration
}

// NextIterval moves the iterator to the next available interval.
// Returns true if another iterval is available.
func (i *iterator) NextIterval() bool {
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
	if max := i.max; i.imax > max {
		i.imax = max
	}

	return true
}

// Next returns the next point's timestamp and field value.
func (i *iterator) Next() (timestamp int64, value interface{}) {
	for {
		// If index is beyond points range then return nil.
		if i.index > len(i.points)-1 {
			return 0, nil
		}

		// Retrieve point and extract value.
		p := i.points[i.index]
		v := p.values[i.fieldID]

		// Move cursor forward.
		i.index++

		// If timestamp is beyond bucket time range then move index back and exit.
		timestamp := p.timestamp
		if timestamp >= i.imax && i.imax != 0 {
			i.index--
			return 0, nil
		}

		// Return value if it is non-nil.
		// Otherwise loop again and try the next point.
		if v != nil {
			return p.timestamp, v
		}
	}
}

// Time returns start time of the current interval.
func (i *iterator) Time() int64 { return i.imin }

// Interval returns the group by duration.
func (i *iterator) Interval() time.Duration { return time.Duration(i.interval) }

type Measurement struct {
	name string

	maxFieldID uint8
	fields     map[string]*Field

	series map[uint32]*Series
}

// NewMeasurement returns a new instance of Measurement.
func NewMeasurement(name string) *Measurement {
	return &Measurement{
		name:   name,
		fields: make(map[string]*Field),
		series: make(map[uint32]*Series),
	}
}

// CreateFieldIfNotExists returns a field by name & data type.
// Creates it if it doesn't exist.
func (m *Measurement) CreateFieldIfNotExists(name string, typ influxql.DataType) *Field {
	f := m.fields[name]
	if f != nil && f.typ != typ {
		panic("field data type mismatch: " + name)
	} else if f == nil {
		m.maxFieldID++
		f = &Field{id: m.maxFieldID, name: name, typ: typ}
		m.fields[name] = f
	}
	return f
}

type Series struct {
	id     uint32
	tags   map[string]string
	points points
}

type Field struct {
	id   uint8
	name string
	typ  influxql.DataType
}

type point struct {
	timestamp int64
	values    map[uint8]interface{}
}

type points []*point

func (p points) Len() int           { return len(p) }
func (p points) Less(i, j int) bool { return p[i].timestamp < p[j].timestamp }
func (p points) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

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
