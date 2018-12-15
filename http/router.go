package http

import (
	"net/http"

	"github.com/influxdata/platform"
	"github.com/julienschmidt/httprouter"
)

// NewRouter returns a new router with a 404 handler that returns a JSON response.
func NewRouter() *httprouter.Router {
	router := httprouter.New()
	router.NotFound = http.HandlerFunc(notFoundHandler)
	return router
}

// notFoundHandler represents a 404 handler that return a JSON response
func notFoundHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	pe := &platform.Error{
		Code: platform.ENotFound,
		Msg:  "path not found",
	}

	EncodeError(ctx, pe, w)
}
