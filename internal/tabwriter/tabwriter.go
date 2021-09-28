package tabwriter

import (
	"fmt"
	"io"
	"strings"
	"text/tabwriter"
)

// TabWriter wraps tab writer headers logic.
type TabWriter struct {
	writer      *tabwriter.Writer
	headers     []string
	hideHeaders bool
}

// NewTabWriter creates a new tab writer.
func NewTabWriter(w io.Writer, hideHeaders bool) *TabWriter {
	return &TabWriter{
		writer:      tabwriter.NewWriter(w, 0, 8, 1, '\t', 0),
		hideHeaders: hideHeaders,
	}
}

// WriteHeaders will Write headers.
func (w *TabWriter) WriteHeaders(h ...string) error {
	w.headers = h
	if !w.hideHeaders {
		if _, err := fmt.Fprintln(w.writer, strings.Join(h, "\t")); err != nil {
			return err
		}
	}
	return nil
}

// Write will Write the map into embed tab writer.
func (w *TabWriter) Write(m map[string]interface{}) error {
	body := make([]interface{}, len(w.headers))
	types := make([]string, len(w.headers))
	for i, h := range w.headers {
		v := m[h]
		body[i] = v
		types[i] = formatStringType(v)
	}
	formatString := strings.Join(types, "\t")
	if _, err := fmt.Fprintf(w.writer, formatString+"\n", body...); err != nil {
		return err
	}
	return nil
}

// Flush should be called after the last call to Write to ensure
// that any data buffered in the Writer is written to output. Any
// incomplete escape sequence at the end is considered
// complete for formatting purposes.
func (w *TabWriter) Flush() error {
	return w.writer.Flush()
}

func formatStringType(v interface{}) string {
	switch v.(type) {
	case int:
		return "%d"
	case string:
		return "%s"
	}
	return "%v"
}
