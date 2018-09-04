package http

import (
	"fmt"
	"net/http"
)

// HealthzHandler returns the status of the process.
func HealthzHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	fmt.Fprintln(w, `{"message": "howdy y'all", "status": "healthy"}`)
}
