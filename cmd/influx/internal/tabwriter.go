package internal

import (
	"fmt"
	"io"
	"strings"
	"text/tabwriter"

	platform "github.com/influxdata/influxdb/v2"
)

// TabWriter wraps tab writer headers logic.
type TabWriter struct {
	writer      *tabwriter.Writer
	headers     []string
	hideHeaders bool
}

// NewTabWriter creates a new tab writer.
func NewTabWriter(w io.Writer) *TabWriter {
	return &TabWriter{
		writer: tabwriter.NewWriter(w, 0, 8, 1, '\t', 0),
	}
}

// HideHeaders will set the hideHeaders flag.
func (w *TabWriter) HideHeaders(b bool) {
	w.hideHeaders = b
}

// WriteHeaders will write headers.
func (w *TabWriter) WriteHeaders(h ...string) {
	w.headers = h
	if !w.hideHeaders {
		fmt.Fprintln(w.writer, strings.Join(h, "\t"))
	}
}

// Write will write the map into embed tab writer.
func (w *TabWriter) Write(m map[string]interface{}) {
	body := make([]interface{}, len(w.headers))
	types := make([]string, len(w.headers))
	for i, h := range w.headers {
		v := m[h]
		body[i] = v
		types[i] = formatStringType(v)
	}

	formatString := strings.Join(types, "\t")
	fmt.Fprintf(w.writer, formatString+"\n", body...)
}

// Flush should be called after the last call to Write to ensure
// that any data buffered in the Writer is written to output. Any
// incomplete escape sequence at the end is considered
// complete for formatting purposes.
func (w *TabWriter) Flush() {
	w.writer.Flush()
}

func formatStringType(i interface{}) string {
	switch i.(type) {
	case int:
		return "%d"
	case platform.ID, string:
		return "%s"
	}

	return "%v"
}
