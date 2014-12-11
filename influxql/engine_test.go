package influxql_test

import (
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/influxdb/influxdb/influxql"
)

// Ensure the planner can plan and execute a query.
func TestPlanner_Plan(t *testing.T) {
	db := NewDB()
	db.WriteSeries("cpu", map[string]string{}, "2000-01-01T00:00:00Z", map[string]interface{}{"value": float64(100)})
	db.WriteSeries("cpu", map[string]string{}, "2000-01-01T00:00:10Z", map[string]interface{}{"value": float64(90)})
	db.WriteSeries("cpu", map[string]string{}, "2000-01-01T00:00:20Z", map[string]interface{}{"value": float64(80)})
	db.WriteSeries("cpu", map[string]string{}, "2000-01-01T00:00:30Z", map[string]interface{}{"value": float64(70)})
	db.WriteSeries("cpu", map[string]string{}, "2000-01-01T00:00:40Z", map[string]interface{}{"value": float64(60)})
	db.WriteSeries("cpu", map[string]string{}, "2000-01-01T00:00:50Z", map[string]interface{}{"value": float64(50)})

	for i, tt := range []struct {
		q  string          // querystring
		rs []*influxql.Row // resultset
	}{
		// 0. Simple count
		{
			q: `SELECT count(value) FROM cpu`,
			rs: []*influxql.Row{
				{
					Name:    "cpu",
					Tags:    map[string]string{},
					Columns: []string{"count"},
					Values: [][]interface{}{
						{6},
					},
				},
			},
		},
	} {
		// Plan statement.
		var p = influxql.NewPlanner(db)
		e, err := p.Plan(MustParseSelectStatement(tt.q))
		if err != nil {
			t.Errorf("%d. %s: plan error: %s", i, tt.q, err)
			continue
		}

		// Execute plan.
		ch, err := e.Execute()
		if err != nil {
			t.Errorf("%d. %s: execute error: %s", i, tt.q, err)
			continue
		}

		// Collect resultset.
		var rs []*influxql.Row
		for row := range ch {
			rs = append(rs, row)
		}

		// Compare resultset.
		if b0, b1 := mustMarshalJSON(tt.rs), mustMarshalJSON(rs); string(b0) != string(b1) {
			t.Errorf("%d. resultset mismatch:\n\n%s\n\nexp=%s\n\ngot=%s\n\n", i, tt.q, b0, b1)
			continue
		}
	}
}

// DB represents an in-memory test database that implements methods for Planner.
type DB struct {
	measurements map[string]*Measurement
	series       map[uint32]*Series
}

// NewDB returns a new instance of DB.
func NewDB() *DB {
	return &DB{
		measurements: make(map[string]*Measurement),
		series:       make(map[uint32]*Series),
	}
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
	m.maxSeriesID++
	s := &Series{id: m.maxSeriesID, tags: tags}

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
func (db *DB) CreateIterator(seriesID uint32, fieldID uint8, typ influxql.DataType) influxql.Iterator {
	s := db.series[seriesID]
	if s == nil {
		panic(fmt.Sprintf("series not found: %d", seriesID))
	}

	// Return iterator.
	return &iterator{
		points:   s.points,
		fieldID:  fieldID,
		typ:      typ,
		time:     time.Unix(0, 0).UTC(),
		duration: 0,
	}
}

// iterator represents an implementation of Iterator over a set of points.
type iterator struct {
	fieldID uint8
	typ     influxql.DataType

	index  int
	points points

	time     time.Time     // bucket start time
	duration time.Duration // bucket duration
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

		// Return value if it is non-nil.
		// Otherwise loop again and try the next point.
		if v != nil {
			return p.timestamp, v
		}
	}
}

// Time returns start time of the current interval.
func (i *iterator) Time() int64 { return i.time.UnixNano() }

// Duration returns the group by duration.
func (i *iterator) Duration() time.Duration { return i.duration }

type Measurement struct {
	name string

	maxFieldID uint8
	fields     map[string]*Field

	maxSeriesID uint32
	series      map[uint32]*Series
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
