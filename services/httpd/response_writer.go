package httpd

import (
	"encoding/json"
	"io"
	"net/http"
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
