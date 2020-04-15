package http

import "net/http"

// ResourceHandler is an HTTP handler for a resource. The prefix
// describes the url path prefix that relates to the handler
// endpoints.
type ResourceHandler interface {
	Prefix() string
	http.Handler
}
