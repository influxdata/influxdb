package server

import (
	"context"
	"net/http"

	"github.com/influxdata/chronograf"
	"github.com/influxdata/chronograf/oauth2"
)

// AuthorizedToken extracts the token and validates; if valid the next handler
// will be run.  The principal will be sent to the next handler via the request's
// Context.  It is up to the next handler to determine if the principal has access.
// On failure, will return http.StatusForbidden.
func AuthorizedToken(auth oauth2.Authenticator, logger chronograf.Logger, next http.Handler) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		log := logger.
			WithField("component", "auth").
			WithField("remote_addr", r.RemoteAddr).
			WithField("method", r.Method).
			WithField("url", r.URL)

		ctx := r.Context()
		// We do not check the validity of the principal.  Those
		// served further down the chain should do so.
		principal, err := auth.Validate(ctx, r)
		if err != nil {
			log.Error("Invalid principal")
			w.WriteHeader(http.StatusForbidden)
			return
		}

		// Send the principal to the next handler
		ctx = context.WithValue(ctx, oauth2.PrincipalKey, principal)
		next.ServeHTTP(w, r.WithContext(ctx))
		return
	})
}
