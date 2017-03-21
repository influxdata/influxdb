// +build OMIT
package server

import "net/http"

// HSTS add HTTP Strict Transport Security header with a max-age of two years
// Inspired from https://blog.bracebin.com/achieving-perfect-ssl-labs-score-with-go
func HSTS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Add("Strict-Transport-Security", "max-age=63072000; includeSubDomains")
		next.ServeHTTP(w, r)
	})
}
