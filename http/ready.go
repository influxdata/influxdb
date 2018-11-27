package http

import (
	"fmt"
	"net/http"
)

// ReadyHandler is a default readiness handler. The default behavior is always ready.
func ReadyHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	fmt.Fprintln(w, `{"status": "ready"}`)
}
