// Package diagnostics provides the diagnostics type so that
// other packages can provide diagnostics without depending on the monitor package.
package diagnostics // import "github.com/influxdata/influxdb/monitor/diagnostics"

import (
	"sort"
	"time"

	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxdb/toml"
)

// Client is the interface modules implement if they register diagnostics with monitor.
type Client interface {
	Diagnostics() (*Diagnostics, error)
}

// The ClientFunc type is an adapter to allow the use of
// ordinary functions as Diagnostics clients.
type ClientFunc func() (*Diagnostics, error)

// Diagnostics calls f().
func (f ClientFunc) Diagnostics() (*Diagnostics, error) {
	return f()
}

// Diagnostics represents a table of diagnostic information. The first value
// is the name of the columns, the second is a slice of interface slices containing
// the values for each column, by row. This information is never written to an InfluxDB
// system and is display-only. An example showing, say, connections follows:
//
//     source_ip    source_port       dest_ip     dest_port
//     182.1.0.2    2890              127.0.0.1   38901
//     174.33.1.2   2924              127.0.0.1   38902
type Diagnostics struct {
	Columns []query.Column
	Rows    [][]interface{}
}

// NewDiagnostic initialises a new Diagnostics with the specified columns.
func NewDiagnostics(columns []query.Column) *Diagnostics {
	return &Diagnostics{
		Columns: columns,
		Rows:    make([][]interface{}, 0),
	}
}

// AddRow appends the provided row to the Diagnostics' rows.
func (d *Diagnostics) AddRow(r []interface{}) {
	d.Rows = append(d.Rows, r)
}

// RowFromMap returns a new one-row Diagnostics from a map.
func RowFromMap(m map[string]interface{}) *Diagnostics {
	// Display columns in deterministic order.
	columns := make([]query.Column, 0, len(m))
	for k, v := range m {
		col := query.Column{Name: k}
		switch v.(type) {
		case float64:
			col.Type = influxql.Float
		case int, int64:
			col.Type = influxql.Integer
		case uint64:
			col.Type = influxql.Unsigned
		case string:
			col.Type = influxql.String
		case bool:
			col.Type = influxql.Boolean
		case toml.Duration, time.Duration:
			col.Type = influxql.Duration
		}
		columns = append(columns, col)
	}
	sort.Sort(query.Columns(columns))

	d := NewDiagnostics(columns)
	row := make([]interface{}, len(columns))
	for i, k := range columns {
		row[i] = m[k.Name]
	}
	d.AddRow(row)

	return d
}
