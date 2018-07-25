package server

import (
	"net/http"

	"github.com/bouk/httprouter"
	"github.com/influxdata/platform/chronograf"
)

// RouteMatchesPrincipal checks that the organization on context matches the organization
// in the route.
func RouteMatchesPrincipal(
	store DataStore,
	useAuth bool,
	logger chronograf.Logger,
	next http.HandlerFunc,
) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		if !useAuth {
			next(w, r)
			return
		}

		log := logger.
			WithField("component", "org_match").
			WithField("remote_addr", r.RemoteAddr).
			WithField("method", r.Method).
			WithField("url", r.URL)

		orgID := httprouter.GetParamFromContext(ctx, "oid")
		p, err := getValidPrincipal(ctx)
		if err != nil {
			log.Error("Failed to retrieve principal from context")
			Error(w, http.StatusForbidden, "User is not authorized", logger)
			return
		}

		if p.Organization == "" {
			defaultOrg, err := store.Organizations(ctx).DefaultOrganization(ctx)
			if err != nil {
				log.Error("Failed to look up default organization")
				Error(w, http.StatusForbidden, "User is not authorized", logger)
				return
			}
			p.Organization = defaultOrg.ID
		}

		if orgID != p.Organization {
			log.Error("Route organization does not match the organization on principal")
			Error(w, http.StatusForbidden, "User is not authorized", logger)
			return
		}

		next(w, r)
	}
}
