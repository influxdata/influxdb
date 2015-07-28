package http

import (
	"compress/gzip"
	"io"
	libhttp "net/http"
)

type gzipResponseWriter struct {
	io.Writer
	libhttp.ResponseWriter
}

func (w gzipResponseWriter) Write(b []byte) (int, error) {
	return w.Writer.Write(b)
}

func (w gzipResponseWriter) Flush() {
	w.Writer.(*gzip.Writer).Flush()
}

func (w gzipResponseWriter) CloseNotify() <-chan bool {
	return w.ResponseWriter.(libhttp.CloseNotifier).CloseNotify()
}
