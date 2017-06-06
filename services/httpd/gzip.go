package httpd

import (
	"compress/gzip"
	"io"
	"net/http"
	"strings"
	"sync"
)

type lazyGzipResponseWriter struct {
	io.Writer
	http.ResponseWriter
	http.Flusher
	http.CloseNotifier
	wroteHeader bool
}

// gzipFilter determines if the client can accept compressed responses, and encodes accordingly.
func gzipFilter(inner http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
			inner.ServeHTTP(w, r)
			return
		}

		gw := &lazyGzipResponseWriter{ResponseWriter: w, Writer: w}

		if f, ok := w.(http.Flusher); ok {
			gw.Flusher = f
		}

		if cn, ok := w.(http.CloseNotifier); ok {
			gw.CloseNotifier = cn
		}

		defer gw.Close()

		inner.ServeHTTP(gw, r)
	})
}

func (w *lazyGzipResponseWriter) WriteHeader(code int) {
	if w.wroteHeader {
		return
	}

	w.wroteHeader = true
	if code == http.StatusOK {
		w.Header().Set("Content-Encoding", "gzip")
		// Add gzip compressor
		if _, ok := w.Writer.(*gzip.Writer); !ok {
			w.Writer = getGzipWriter(w.Writer)
		}
	}

	w.ResponseWriter.WriteHeader(code)
}

func (w *lazyGzipResponseWriter) Write(p []byte) (int, error) {
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}
	return w.Writer.Write(p)
}

func (w *lazyGzipResponseWriter) Flush() {
	// Flush writer, if supported
	if f, ok := w.Writer.(interface {
		Flush()
	}); ok {
		f.Flush()
	}

	// Flush the HTTP response
	if w.Flusher != nil {
		w.Flusher.Flush()
	}
}

func (w *lazyGzipResponseWriter) Close() error {
	if gw, ok := w.Writer.(*gzip.Writer); ok {
		putGzipWriter(gw)
	}

	return nil
}

var gzipWriterPool = sync.Pool{
	New: func() interface{} {
		return gzip.NewWriter(nil)
	},
}

func getGzipWriter(w io.Writer) *gzip.Writer {
	gz := gzipWriterPool.Get().(*gzip.Writer)
	gz.Reset(w)
	return gz
}

func putGzipWriter(gz *gzip.Writer) {
	gz.Close()
	gzipWriterPool.Put(gz)
}
