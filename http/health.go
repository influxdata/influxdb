package http

import (
	"fmt"
	"net/http"
)

// HealthHandler returns the status of the process.
func HealthHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	fmt.Fprintln(w, `{"message": "howdy y'all", "status": "healthy"}`)
}
