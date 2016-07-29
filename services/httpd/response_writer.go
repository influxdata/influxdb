package httpd

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"

	"github.com/influxdata/influxdb/influxql"
)

// ResponseWriter is an interface for writing a response.
type ResponseWriter interface {
	// WriteResponse writes a response.
	WriteResponse(resp Response) (int, error)

	http.ResponseWriter
}

// NewResponseWriter creates a new ResponseWriter based on the Content-Type of the request
// that wraps the ResponseWriter.
func NewResponseWriter(w http.ResponseWriter, r *http.Request) ResponseWriter {
	pretty := r.URL.Query().Get("pretty") == "true"
	switch r.Header.Get("Content-Type") {
	case "application/json":
		fallthrough
	case "text/csv", "application/csv":
		w.Header().Add("Content-Type", "text/csv")
		return &csvResponseWriter{ResponseWriter: w, Writer: csv.NewWriter(w)}
	default:
		w.Header().Add("Content-Type", "application/json")
		return &jsonResponseWriter{Pretty: pretty, ResponseWriter: w}
	}
}

// WriteError is a convenience function for writing an error response to the ResponseWriter.
func WriteError(w ResponseWriter, err error) (int, error) {
	return w.WriteResponse(Response{Err: err})
}

type jsonResponseWriter struct {
	Pretty bool
	http.ResponseWriter
}

func (w *jsonResponseWriter) WriteResponse(resp Response) (n int, err error) {
	var b []byte
	if w.Pretty {
		b, err = json.MarshalIndent(resp, "", "    ")
	} else {
		b, err = json.Marshal(resp)
	}

	if err != nil {
		n, err = io.WriteString(w, err.Error())
	} else {
		n, err = w.Write(b)
	}

	if !w.Pretty {
		w.Write([]byte("\n"))
		n++
	}
	return n, err
}

// Flush flushes the ResponseWriter if it has a Flush() method.
func (w *jsonResponseWriter) Flush() {
	if w, ok := w.ResponseWriter.(http.Flusher); ok {
		w.Flush()
	}
}

type csvResponseWriter struct {
	http.ResponseWriter
	*csv.Writer
}

func (c *csvResponseWriter) Write(b []byte) (int, error) {
	out := strings.Split(string(b), "\t")
	return len(out), c.Writer.Write(out)
}

func (w *csvResponseWriter) WriteResponse(resp Response) (int, error) {
	var n int
	for _, result := range resp.Results {
		rows := w.formatResults(result, "csv", "\t")
		for _, r := range rows {
			n1, err := w.Write([]byte(r))
			n += n1
			if err != nil {
				return n, err
			}
		}
	}
	return n, nil
}

// Flush flushes the ResponseWriter if it has a Flush() method.
func (w *csvResponseWriter) Flush() {
	w.Writer.Flush()
	if w, ok := w.ResponseWriter.(http.Flusher); ok {
		w.Flush()
	}
}

func (w *csvResponseWriter) formatResults(result *influxql.Result, format, separator string) []string {
	rows := []string{}
	// Create a tabbed writer for each result a they won't always line up
	for _, row := range result.Series {
		// gather tags
		tags := []string{}
		for k, v := range row.Tags {
			tags = append(tags, fmt.Sprintf("%s=%s", k, v))
			sort.Strings(tags)
		}

		columnNames := []string{}

		if len(tags) > 0 {
			columnNames = append([]string{"tags"}, columnNames...)
		}

		if row.Name != "" {
			columnNames = append([]string{"name"}, columnNames...)
		}

		for _, column := range row.Columns {
			columnNames = append(columnNames, column)
		}

		rows = append(rows, strings.Join(columnNames, separator))

		for _, v := range row.Values {
			var values []string
			if format == "csv" {
				if row.Name != "" {
					values = append(values, row.Name)
				}
				if len(tags) > 0 {
					values = append(values, strings.Join(tags, ","))
				}
			}

			for _, vv := range v {
				values = append(values, interfaceToString(vv))
			}
			rows = append(rows, strings.Join(values, separator))
		}
	}
	return rows
}

func interfaceToString(v interface{}) string {
	switch t := v.(type) {
	case nil:
		return ""
	case bool:
		return fmt.Sprintf("%v", v)
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, uintptr:
		return fmt.Sprintf("%d", t)
	case float32, float64:
		return fmt.Sprintf("%v", t)
	default:
		return fmt.Sprintf("%v", t)
	}
}
