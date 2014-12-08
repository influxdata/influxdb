package influxql_test

import (
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/influxdb/influxdb/influxql"
)

func TestPlanner_Plan(t *testing.T) {
	db := NewDB()
	db.WriteSeries("cpu", map[string]string{}, "2000-01-01T00:00:00Z", map[string]interface{}{"value": float64(100)})

	for i, tt := range []struct {
		q  string          // querystring
		rs []*influxql.Row // resultset
	}{
		// 0. Simple count
		{
			q: `SELECT count(value) FROM cpu`,
			rs: []*influxql.Row{
				{
					Name: "cpu",
					Tags: map[string]string{},
					Values: []map[string]interface{}{
						{"timestamp": mustParseTime("2000-01-01T00:00:00Z"), "count": 1},
					},
				},
			},
		},
	} {
		// Plan statement.
		var p = influxql.Planner{DB: db}
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
		if !reflect.DeepEqual(tt.rs, rs) {
			t.Errorf("%d. %s: resultset mismatch:\n\nexp=%+v\n\ngot=%+v\n\n", i, tt.q, tt.rs, rs)
			continue
		}
	}
}

// DB represents an in-memory test database that implements methods for Planner.
type DB struct {
	series map[string]*Series
	data   map[uint32]*SeriesData
}

// NewDB returns a new instance of DB.
func NewDB() *DB {
	return &DB{}
}

// WriteSeries writes a series
func (db *DB) WriteSeries(name string, tags map[string]string, timestamp string, values map[string]interface{}) {

}

// SeriesData returns the series data identifier for a given name & tag set.
func (db *DB) SeriesData(name string, tags map[string]string) uint32 {
	// Find series.
	series := db.series[name]
	if series == nil {
		return 0
	}

	// Normalize tags
	if tags == nil {
		tags = map[string]string{}
	}

	// Find series data.
	for _, data := range series.data {
		if reflect.DeepEqual(data.tags, tags) {
			return data.id
		}
	}

	return 0
}

// FieldID returns the field identifier for a given series name and field name.
func (db *DB) Field(series, field string) (fieldID uint8, typ influxql.DataType) {
	// Find series.
	s := db.series[series]
	if s == nil {
		return
	}

	// Find field.
	f := s.fields[field]
	if f == nil {
		return
	}

	return f.id, f.typ
}

// CreateIterator returns a new iterator for a given field.
func (db *DB) CreateIterator(seriesDataID uint32, fieldID uint8) influxql.Iterator { panic("TODO") }

type Series struct {
	name   string
	fields map[string]*Field
	data   map[uint32]*SeriesData
}

type SeriesData struct {
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
