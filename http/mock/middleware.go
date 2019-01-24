package mock

import (
	"net/http"

	"github.com/influxdata/influxdb"
	platcontext "github.com/influxdata/influxdb/context"
)

// NewAuthMiddlewareHandler create a mocked middleware handler.
func NewAuthMiddlewareHandler(handler http.Handler, auth influxdb.Authorizer) http.Handler {
	return &authMiddlewareHandler{
		handler: handler,
		auth:    auth,
	}
}

type authMiddlewareHandler struct {
	handler http.Handler
	auth    influxdb.Authorizer
}

func (m *authMiddlewareHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	r = r.WithContext(platcontext.SetAuthorizer(ctx, m.auth))
	m.handler.ServeHTTP(w, r)
}
