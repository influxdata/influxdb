package internal

import (
	"fmt"
	"io"
	"strings"
	"text/tabwriter"

	platform "github.com/influxdata/influxdb"
)

type tabWriter struct {
	writer  *tabwriter.Writer
	headers []string
}

func NewTabWriter(w io.Writer) *tabWriter {
	return &tabWriter{
		writer: tabwriter.NewWriter(w, 0, 8, 1, '\t', 0),
	}
}

func (w *tabWriter) WriteHeaders(h ...string) {
	w.headers = h
	fmt.Fprintln(w.writer, strings.Join(h, "\t"))
}

func (w *tabWriter) Write(m map[string]interface{}) {
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

func (w *tabWriter) Flush() {
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
