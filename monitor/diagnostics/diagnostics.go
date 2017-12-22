// Package diagnostics provides the diagnostics type so that
// other packages can provide diagnostics without depending on the monitor package.
package diagnostics // import "github.com/influxdata/influxdb/monitor/diagnostics"

import "sort"

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
	Columns []string
	Rows    [][]interface{}
}

// NewDiagnostics initialises a new Diagnostics with the specified columns.
func NewDiagnostics(columns []string) *Diagnostics {
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
	sortedKeys := make([]string, 0, len(m))
	for k := range m {
		sortedKeys = append(sortedKeys, k)
	}
	sort.Strings(sortedKeys)

	d := NewDiagnostics(sortedKeys)
	row := make([]interface{}, len(sortedKeys))
	for i, k := range sortedKeys {
		row[i] = m[k]
	}
	d.AddRow(row)

	return d
}
