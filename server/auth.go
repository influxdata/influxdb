package server

import (
	"context"
	"fmt"
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
		// We do not check the authorization of the principal.  Those
		// served further down the chain should do so.
		principal, err := auth.Validate(ctx, r)
		if err != nil {
			log.Error("Invalid principal")
			w.WriteHeader(http.StatusForbidden)
			return
		}

		// If the principal is valid we will extend its lifespan
		// into the future
		principal, err = auth.Extend(ctx, w, principal)
		if err != nil {
			log.Error("Unable to extend principal")
			w.WriteHeader(http.StatusForbidden)
			return
		}

		// Send the principal to the next handler
		ctx = context.WithValue(ctx, oauth2.PrincipalKey, principal)
		next.ServeHTTP(w, r.WithContext(ctx))
		return
	})
}

// AuthorizedUser extracts the user name and provider from context. If the user and provider can be found on the
// context, we look up the user by their name and provider. If the user is found, we verify that the user has at
// at least the role supplied.
func AuthorizedUser(store chronograf.UsersStore, useAuth bool, role string, logger chronograf.Logger, next http.HandlerFunc) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !useAuth {
			next(w, r)
			return
		}

		log := logger.
			WithField("component", "role_auth").
			WithField("remote_addr", r.RemoteAddr).
			WithField("method", r.Method).
			WithField("url", r.URL)

		ctx := r.Context()

		username, err := getUsername(ctx)
		if err != nil {
			log.Error("Failed to retrieve username from context")
			Error(w, http.StatusUnauthorized, fmt.Sprintf("User is not authorized"), logger)
			return
		}
		provider, err := getProvider(ctx)
		if err != nil {
			log.Error("Failed to retrieve provider from context")
			Error(w, http.StatusUnauthorized, fmt.Sprintf("User %s is not authorized", username), logger)
			return
		}

		u, err := store.Get(ctx, chronograf.UserQuery{Name: &username, Provider: &provider})
		if err != nil {
			log.Error("Error to retrieving user")
			Error(w, http.StatusUnauthorized, fmt.Sprintf("User %s is not authorized", username), logger)
			return
		}

		if hasPrivelege(u, role) {
			next(w, r)
			return
		}

		Error(w, http.StatusUnauthorized, fmt.Sprintf("User %s is not authorized", username), logger)
		return

	})
}

func hasPrivelege(u *chronograf.User, role string) bool {
	if u == nil {
		return false
	}

	switch role {
	case ViewerRoleName:
		for _, r := range u.Roles {
			switch r.Name {
			case ViewerRoleName, EditorRoleName, AdminRoleName:
				return true
			default:
				return false
			}
		}
	case EditorRoleName:
		for _, r := range u.Roles {
			switch r.Name {
			case EditorRoleName, AdminRoleName:
				return true
			default:
				return false
			}
		}
	case AdminRoleName:
		for _, r := range u.Roles {
			switch r.Name {
			case AdminRoleName:
				return true
			default:
				return false
			}
		}
	default:
		return false
	}

	return false
}
