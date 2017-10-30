package server

import (
	"net/http"
	"net/url"
	"path"
	"strings"
)

type interceptingResponseWriter struct {
	http.ResponseWriter
	Flusher http.Flusher
	Prefix  string
}

func (i *interceptingResponseWriter) WriteHeader(status int) {
	if status >= 300 && status < 400 {
		location := i.ResponseWriter.Header().Get("Location")
		if u, err := url.Parse(location); err == nil && !u.IsAbs() {
			hasPrefix := strings.HasPrefix(u.Path, i.Prefix)
			if !hasPrefix || (hasPrefix && !strings.HasPrefix(u.Path[len(i.Prefix):], i.Prefix)) {
				i.ResponseWriter.Header().Set("Location", path.Join(i.Prefix, location)+"/")
			}
		}
	}
	i.ResponseWriter.WriteHeader(status)
}

// Flush is here because the underlying HTTP chunked transfer response writer
// to implement http.Flusher.  Without it data is silently buffered.  This
// was discovered when proxying kapacitor chunked logs.
func (i *interceptingResponseWriter) Flush() {
	if i.Flusher != nil {
		i.Flusher.Flush()
	}
}

// PrefixedRedirect alters the Location header of downstream http.Handlers
// to include a specified prefix
func PrefixedRedirect(prefix string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		iw := &interceptingResponseWriter{
			ResponseWriter: w,
			Prefix:         prefix,
		}
		if flusher, ok := w.(http.Flusher); ok {
			iw.Flusher = flusher
		}
		next.ServeHTTP(iw, r)
	})
}
