// +build OMIT
package server

import (
	"net/http"
)

// Version handler adds X-Chronograf-Version header to responses
func Version(version string, h http.Handler) http.Handler {
	fn := func(w http.ResponseWriter, r *http.Request) {
		w.Header().Add("X-Chronograf-Version", version)
		h.ServeHTTP(w, r)
	}
	return http.HandlerFunc(fn)
}
