package server

import (
	"net/http"
	libpath "path"

	"github.com/influxdata/platform/chronograf"
)

var _ chronograf.Router = &MountableRouter{}

// MountableRouter is an implementation of a chronograf.Router which supports
// prefixing each route of a Delegated chronograf.Router with a prefix.
type MountableRouter struct {
	Prefix   string
	Delegate chronograf.Router
}

// DELETE defines a route responding to a DELETE request that will be prefixed
// with the configured route prefix
func (mr *MountableRouter) DELETE(path string, handler http.HandlerFunc) {
	mr.Delegate.DELETE(libpath.Join(mr.Prefix, path), handler)
}

// GET defines a route responding to a GET request that will be prefixed
// with the configured route prefix
func (mr *MountableRouter) GET(path string, handler http.HandlerFunc) {
	mr.Delegate.GET(libpath.Join(mr.Prefix, path), handler)
}

// POST defines a route responding to a POST request that will be prefixed
// with the configured route prefix
func (mr *MountableRouter) POST(path string, handler http.HandlerFunc) {
	mr.Delegate.POST(libpath.Join(mr.Prefix, path), handler)
}

// PUT defines a route responding to a PUT request that will be prefixed
// with the configured route prefix
func (mr *MountableRouter) PUT(path string, handler http.HandlerFunc) {
	mr.Delegate.PUT(libpath.Join(mr.Prefix, path), handler)
}

// PATCH defines a route responding to a PATCH request that will be prefixed
// with the configured route prefix
func (mr *MountableRouter) PATCH(path string, handler http.HandlerFunc) {
	mr.Delegate.PATCH(libpath.Join(mr.Prefix, path), handler)
}

// Handler defines a prefixed route responding to a request type specified in
// the method parameter
func (mr *MountableRouter) Handler(method string, path string, handler http.Handler) {
	mr.Delegate.Handler(method, libpath.Join(mr.Prefix, path), handler)
}

// ServeHTTP is an implementation of http.Handler which delegates to the
// configured Delegate's implementation of http.Handler
func (mr *MountableRouter) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	mr.Delegate.ServeHTTP(rw, r)
}
