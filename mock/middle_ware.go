package mock

import (
	"net/http"

	"github.com/influxdata/influxdb"
	platcontext "github.com/influxdata/influxdb/context"
)

// NewMiddlewareHandler create a mocked middleware handler.
func NewMiddlewareHandler(handler http.Handler, auth influxdb.Authorizer) http.Handler {
	return &middlewareHandler{
		handler: handler,
		auth:    auth,
	}
}

type middlewareHandler struct {
	handler http.Handler
	auth    influxdb.Authorizer
}

func (m *middlewareHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	r = r.WithContext(platcontext.SetAuthorizer(ctx, m.auth))
	m.handler.ServeHTTP(w, r)
}
