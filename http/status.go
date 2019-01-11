package http

import "net/http"

type statusResponseWriter struct {
	statusCode int
	http.ResponseWriter
}

func newStatusResponseWriter(w http.ResponseWriter) *statusResponseWriter {
	return &statusResponseWriter{
		ResponseWriter: w,
	}
}

// WriteHeader writes the header and captures the status code.
func (w *statusResponseWriter) WriteHeader(statusCode int) {
	w.statusCode = statusCode
	w.ResponseWriter.WriteHeader(statusCode)
}

func (w *statusResponseWriter) code() int {
	code := w.statusCode
	if code == 0 {
		// When statusCode is 0 then WriteHeader was never called and we can assume that
		// the ResponseWriter wrote an http.StatusOK.
		code = http.StatusOK
	}
	return code
}
func (w *statusResponseWriter) statusCodeClass() string {
	class := "XXX"
	switch w.code() / 100 {
	case 1:
		class = "1XX"
	case 2:
		class = "2XX"
	case 3:
		class = "3XX"
	case 4:
		class = "4XX"
	case 5:
		class = "5XX"
	}
	return class
}
