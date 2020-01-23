package http

import "net/http"

type StatusResponseWriter struct {
	statusCode    int
	responseBytes int
	http.ResponseWriter
}

func NewStatusResponseWriter(w http.ResponseWriter) *StatusResponseWriter {
	return &StatusResponseWriter{
		ResponseWriter: w,
	}
}

func (w *StatusResponseWriter) Write(b []byte) (int, error) {
	n, err := w.ResponseWriter.Write(b)
	w.responseBytes += n
	return n, err
}

// WriteHeader writes the header and captures the status code.
func (w *StatusResponseWriter) WriteHeader(statusCode int) {
	w.statusCode = statusCode
	w.ResponseWriter.WriteHeader(statusCode)
}

func (w *StatusResponseWriter) Code() int {
	code := w.statusCode
	if code == 0 {
		// When statusCode is 0 then WriteHeader was never called and we can assume that
		// the ResponseWriter wrote an http.StatusOK.
		code = http.StatusOK
	}
	return code
}

func (w *StatusResponseWriter) ResponseBytes() int {
	return w.responseBytes
}

func (w *StatusResponseWriter) StatusCodeClass() string {
	class := "XXX"
	switch w.Code() / 100 {
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
