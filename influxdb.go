package influxdb

import (
	"bytes"
	"fmt"
	"runtime"

	log "code.google.com/p/log4go"
	"github.com/influxdb/influxdb/parser"
)

// Series represents a series of timeseries points.
type Series struct {
	ID     uint64 `json:"id,omitempty"`
	Name   string `json:"name,omitempty"`
	Fields Fields `json:"fields,omitempty"`
}

// Field represents a series field.
type Field struct {
	ID   uint64 `json:"id,omitempty"`
	Name string `json:"name,omitempty"`
}

// String returns a string representation of the field.
func (f *Field) String() string {
	return fmt.Sprintf("Name: %s, ID: %d", f.Name, f.ID)
}

// Fields represents a list of fields.
type Fields []*Field

// Names returns a list of all field names.
func (a Fields) Names() []string {
	names := make([]string, len(a))
	for i, f := range a {
		names[i] = f.Name
	}
	return names
}

// recoverFunc handles recovery in the event of a panic.
func recoverFunc(database, query string, cleanup func(err interface{})) {
	if err := recover(); err != nil {
		buf := make([]byte, 1024)
		n := runtime.Stack(buf, false)
		b := bytes.NewBufferString("")
		fmt.Fprintf(b, "********************************BUG********************************\n")
		fmt.Fprintf(b, "Database: %s\n", database)
		fmt.Fprintf(b, "Query: [%s]\n", query)
		fmt.Fprintf(b, "Error: %s. Stacktrace: %s\n", err, string(buf[:n]))
		log.Error(b.String())
		err = parser.NewQueryError(parser.InternalError, "Internal Error: %s", err)
		if cleanup != nil {
			cleanup(err)
		}
	}
}
