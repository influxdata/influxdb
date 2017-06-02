package httpd

import (
	"encoding/csv"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/influxdata/influxdb/models"
	"github.com/tinylib/msgp/msgp"
)

//go:generate msgp

// ResponseWriter is an interface for writing a response.
type ResponseWriter interface {
	// WriteResponse writes a response.
	WriteResponse(resp Response) (int, error)

	http.ResponseWriter
}

// NewResponseWriter creates a new ResponseWriter based on the Accept header
// in the request that wraps the ResponseWriter.
func NewResponseWriter(w http.ResponseWriter, r *http.Request) ResponseWriter {
	pretty := r.URL.Query().Get("pretty") == "true"
	rw := &responseWriter{ResponseWriter: w}
	switch r.Header.Get("Accept") {
	case "application/csv", "text/csv":
		w.Header().Add("Content-Type", "text/csv")
		rw.formatter = &csvFormatter{statementID: -1, Writer: w}
	case "application/x-msgpack":
		w.Header().Add("Content-Type", "application/x-msgpack")
		rw.formatter = newMsgpackFormatter(w)
	case "application/json":
		fallthrough
	default:
		w.Header().Add("Content-Type", "application/json")
		rw.formatter = &jsonFormatter{Pretty: pretty, Writer: w}
	}
	return rw
}

// WriteError is a convenience function for writing an error response to the ResponseWriter.
func WriteError(w ResponseWriter, err error) (int, error) {
	return w.WriteResponse(Response{Err: err})
}

// responseWriter is an implementation of ResponseWriter.
type responseWriter struct {
	formatter interface {
		WriteResponse(resp Response) (int, error)
	}
	http.ResponseWriter
}

// WriteResponse writes the response using the formatter.
func (w *responseWriter) WriteResponse(resp Response) (int, error) {
	return w.formatter.WriteResponse(resp)
}

// Flush flushes the ResponseWriter if it has a Flush() method.
func (w *responseWriter) Flush() {
	if w, ok := w.ResponseWriter.(http.Flusher); ok {
		w.Flush()
	}
}

// CloseNotify calls CloseNotify on the underlying http.ResponseWriter if it
// exists. Otherwise, it returns a nil channel that will never notify.
func (w *responseWriter) CloseNotify() <-chan bool {
	if notifier, ok := w.ResponseWriter.(http.CloseNotifier); ok {
		return notifier.CloseNotify()
	}
	return nil
}

type jsonFormatter struct {
	io.Writer
	Pretty bool
}

func (w *jsonFormatter) WriteResponse(resp Response) (n int, err error) {
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

	w.Write([]byte("\n"))
	n++
	return n, err
}

type csvFormatter struct {
	io.Writer
	statementID int
	columns     []string
}

func (w *csvFormatter) WriteResponse(resp Response) (n int, err error) {
	csv := csv.NewWriter(writer{Writer: w, n: &n})
	defer csv.Flush()
	for _, result := range resp.Results {
		if result.StatementID != w.statementID {
			// If there are no series in the result, skip past this result.
			if len(result.Series) == 0 {
				continue
			}

			// Set the statement id and print out a newline if this is not the first statement.
			if w.statementID >= 0 {
				// Flush the csv writer and write a newline.
				csv.Flush()
				if err := csv.Error(); err != nil {
					return n, err
				}

				out, err := io.WriteString(w, "\n")
				if err != nil {
					return n, err
				}
				n += out
			}
			w.statementID = result.StatementID

			// Print out the column headers from the first series.
			w.columns = make([]string, 2+len(result.Series[0].Columns))
			w.columns[0] = "name"
			w.columns[1] = "tags"
			copy(w.columns[2:], result.Series[0].Columns)
			if err := csv.Write(w.columns); err != nil {
				return n, err
			}
		}

		for _, row := range result.Series {
			w.columns[0] = row.Name
			if len(row.Tags) > 0 {
				w.columns[1] = string(models.NewTags(row.Tags).HashKey()[1:])
			} else {
				w.columns[1] = ""
			}
			for _, values := range row.Values {
				for i, value := range values {
					if value == nil {
						w.columns[i+2] = ""
						continue
					}

					switch v := value.(type) {
					case float64:
						w.columns[i+2] = strconv.FormatFloat(v, 'f', -1, 64)
					case int64:
						w.columns[i+2] = strconv.FormatInt(v, 10)
					case string:
						w.columns[i+2] = v
					case bool:
						if v {
							w.columns[i+2] = "true"
						} else {
							w.columns[i+2] = "false"
						}
					case time.Time:
						w.columns[i+2] = strconv.FormatInt(v.UnixNano(), 10)
					case *float64, *int64, *string, *bool:
						w.columns[i+2] = ""
					}
				}
				csv.Write(w.columns)
			}
		}
	}
	csv.Flush()
	if err := csv.Error(); err != nil {
		return n, err
	}
	return n, nil
}

type msgpackFormatter struct {
	enc *msgp.Writer
	n   int
}

func newMsgpackFormatter(w io.Writer) *msgpackFormatter {
	mf := &msgpackFormatter{}
	mf.enc = msgp.NewWriter(writer{Writer: w, n: &mf.n})
	return mf
}

func (w *msgpackFormatter) WriteResponse(resp Response) (n int, err error) {
	w.n = 0

	w.enc.WriteMapHeader(1)
	if resp.Err != nil {
		w.enc.WriteString("error")
		w.enc.WriteString(resp.Err.Error())
	} else {
		w.enc.WriteString("results")
		w.enc.WriteArrayHeader(uint32(len(resp.Results)))
		for _, result := range resp.Results {
			if result.Err != nil {
				w.enc.WriteMapHeader(1)
				w.enc.WriteString("error")
				w.enc.WriteString(result.Err.Error())
				continue
			}

			elements := 2
			if len(result.Messages) > 0 {
				elements++
			}
			if result.Partial {
				elements++
			}
			w.enc.WriteMapHeader(uint32(elements))
			w.enc.WriteString("statement_id")
			w.enc.WriteInt(result.StatementID)

			w.enc.WriteString("series")
			w.enc.WriteArrayHeader(uint32(len(result.Series)))
			for _, row := range result.Series {
				err = row.EncodeMsg(w.enc)
				if err != nil {
					return 0, err
				}
			}

			if len(result.Messages) > 0 {
				w.enc.WriteString("messages")
				w.enc.WriteArrayHeader(uint32(len(result.Messages)))
				for _, m := range result.Messages {
					w.enc.WriteMapHeader(2)
					w.enc.WriteString("level")
					w.enc.WriteString(m.Level)
					w.enc.WriteString("text")
					w.enc.WriteString(m.Text)
				}
			}

			if result.Partial {
				w.enc.WriteString("partial")
				w.enc.WriteBool(true)
			}
		}
	}
	err = w.enc.Flush()
	return w.n, err
}

type writer struct {
	io.Writer
	n *int
}

func (w writer) Write(data []byte) (n int, err error) {
	n, err = w.Writer.Write(data)
	*w.n += n
	return n, err
}
