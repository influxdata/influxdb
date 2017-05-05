package server

import (
	"net/http"
	"net/url"
	"path"
	"strings"
)

type interceptingResponseWriter struct {
	http.ResponseWriter
	Prefix string
}

func (i *interceptingResponseWriter) WriteHeader(status int) {
	if status >= 300 && status < 400 {
		location := i.ResponseWriter.Header().Get("Location")
		if u, err := url.Parse(location); err == nil && !u.IsAbs() {
			if !strings.HasPrefix(location, i.Prefix) {
				i.ResponseWriter.Header().Set("Location", path.Join(i.Prefix, location)+"/")
			}
		}
	}
	i.ResponseWriter.WriteHeader(status)
}

// PrefixingRedirector alters the Location header of downstream http.Handlers
// to include a specified prefix
func PrefixedRedirect(prefix string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		iw := &interceptingResponseWriter{w, prefix}
		next.ServeHTTP(iw, r)
	})
}
