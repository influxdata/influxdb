package server

import (
	"net/http"

	"github.com/influxdata/chronograf"
)

type getRoutesResponse struct {
	Layouts  string `json:"layouts"`  // Location of the layouts endpoint
	Mappings string `json:"mappings"` // Location of the application mappings endpoint
	Sources  string `json:"sources"`  // Location of the sources endpoint
	Users    string `json:"users"`    // Location of the users endpoint
	Me       string `json:"me"`       // Location of the me endpoint
}

// AllRoutes returns all top level routes within chronograf
func AllRoutes(logger chronograf.Logger) http.HandlerFunc {
	routes := getRoutesResponse{
		Sources:  "/chronograf/v1/sources",
		Layouts:  "/chronograf/v1/layouts",
		Users:    "/chronograf/v1/users",
		Me:       "/chronograf/v1/me",
		Mappings: "/chronograf/v1/mappings",
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		encodeJSON(w, http.StatusOK, routes, logger)
		return
	})
}
