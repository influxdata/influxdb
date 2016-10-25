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
}

func AllRoutes(logger chronograf.Logger) http.HandlerFunc {
	routes := getRoutesResponse{
		Sources:  httpAPISrcs,
		Layouts:  httpAPILayouts,
		Users:    httpAPIUsrs,
		Mappings: httpAPIMappings,
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		encodeJSON(w, http.StatusOK, routes, logger)
		return
	})
}
