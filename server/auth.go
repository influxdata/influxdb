package server

import (
	"context"
	"fmt"
	"net/http"

	"github.com/influxdata/chronograf"
	"github.com/influxdata/chronograf/oauth2"
	"github.com/influxdata/chronograf/organizations"
	"github.com/influxdata/chronograf/roles"
)

// AuthorizedToken extracts the token and validates; if valid the next handler
// will be run.  The principal will be sent to the next handler via the request's
// Context.  It is up to the next handler to determine if the principal has access.
// On failure, will return http.StatusForbidden.
func AuthorizedToken(auth oauth2.Authenticator, logger chronograf.Logger, next http.Handler) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		log := logger.
			WithField("component", "token_auth").
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

// AuthorizedUser extracts the user name and provider from context. If the
// user and provider can be found on the context, we look up the user by their
// name and provider. If the user is found, we verify that the user has at at
// least the role supplied.
func AuthorizedUser(
	store DataStore,
	useAuth bool,
	role string,
	logger chronograf.Logger,
	next http.HandlerFunc,
) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !useAuth {
			ctx := r.Context()
			// If there is no auth, then give the user raw access to the DataStore
			r = r.WithContext(serverContext(ctx))
			next(w, r)
			return
		}

		log := logger.
			WithField("component", "role_auth").
			WithField("remote_addr", r.RemoteAddr).
			WithField("method", r.Method).
			WithField("url", r.URL)

		ctx := r.Context()
		serverCtx := serverContext(ctx)

		p, err := getValidPrincipal(ctx)
		if err != nil {
			log.Error("Failed to retrieve principal from context")
			Error(w, http.StatusUnauthorized, "User is not authorized", logger)
			return
		}
		scheme, err := getScheme(ctx)
		if err != nil {
			log.Error("Failed to retrieve scheme from context")
			Error(w, http.StatusUnauthorized, "User is not authorized", logger)
			return
		}

		// This is as if the user was logged into the default organization
		if p.Organization == "" {
			defaultOrg, err := store.Organizations(serverCtx).DefaultOrganization(serverCtx)
			if err != nil {
				unknownErrorWithMessage(w, err, logger)
				return
			}
			p.Organization = fmt.Sprintf("%d", defaultOrg.ID)
		}

		// validate that the organization exists
		orgID, err := parseOrganizationID(p.Organization)
		if err != nil {
			log.Error("Failed to validate organization on context")
			Error(w, http.StatusUnauthorized, "User is not authorized", logger)
			return
		}
		_, err = store.Organizations(serverCtx).Get(serverCtx, chronograf.OrganizationQuery{ID: &orgID})
		if err != nil {
			log.Error(fmt.Sprintf("Failed to retrieve organization %d from organizations store", orgID))
			Error(w, http.StatusUnauthorized, "User is not authorized", logger)
			return
		}
		ctx = context.WithValue(ctx, organizations.ContextKey, p.Organization)

		// TODO: seems silly to look up a user twice
		u, err := store.Users(serverCtx).Get(serverCtx, chronograf.UserQuery{
			Name:     &p.Subject,
			Provider: &p.Issuer,
			Scheme:   &scheme,
		})

		if err != nil {
			log.Error("Failed to retrieve user")
			Error(w, http.StatusUnauthorized, "User is not authorized", logger)
			return
		}
		// In particular this is used by sever/users.go so that we know when and when not to
		// allow users to make someone a super admin
		ctx = context.WithValue(ctx, UserContextKey, u)

		if u.SuperAdmin {
			// To access resources (servers, sources, databases, layouts) within a DataStore,
			// an organization and a role are required even if you are a super admin or are
			// not using auth. Every user's current organization is set on context to filter
			// the resources accessed within a DataStore, including for super admin or when
			// not using auth. In this way, a DataStore can treat all requests the same,
			// including those from a super admin and when not using auth.
			//
			// As for roles, in the case of super admin or when not using auth, the user's
			// role on context (though not on their JWT or user) is set to be admin. In order
			// to access all resources belonging to their current organization.
			ctx = context.WithValue(ctx, roles.ContextKey, roles.AdminRoleName)
			r = r.WithContext(ctx)
			next(w, r)
			return
		}

		u, err = store.Users(ctx).Get(ctx, chronograf.UserQuery{
			Name:     &p.Subject,
			Provider: &p.Issuer,
			Scheme:   &scheme,
		})
		if err != nil {
			log.Error("Failed to retrieve user")
			Error(w, http.StatusUnauthorized, "User is not authorized", logger)
			return
		}

		if hasAuthorizedRole(u, role) {
			if len(u.Roles) != 1 {
				msg := `User %d has too many role in organization. User: %#v.Please report this log at https://github.com/influxdata/chronograf/issues/new"`
				log.Error(fmt.Sprint(msg, u.ID, u))
				unknownErrorWithMessage(w, fmt.Errorf("please have administrator check logs and report error"), logger)
				return
			}
			// use the first role, since there should only ever be one
			// for any particular organization and hasAuthorizedRole
			// should ensure that at least one role for the org exists
			ctx = context.WithValue(ctx, roles.ContextKey, u.Roles[0].Name)
			r = r.WithContext(ctx)
			next(w, r)
			return
		}

		Error(w, http.StatusUnauthorized, "User is not authorized", logger)
		return
	})
}

func hasAuthorizedRole(u *chronograf.User, role string) bool {
	if u == nil {
		return false
	}

	switch role {
	case roles.ViewerRoleName:
		for _, r := range u.Roles {
			switch r.Name {
			case roles.ViewerRoleName, roles.EditorRoleName, roles.AdminRoleName:
				return true
			}
		}
	case roles.EditorRoleName:
		for _, r := range u.Roles {
			switch r.Name {
			case roles.EditorRoleName, roles.AdminRoleName:
				return true
			}
		}
	case roles.AdminRoleName:
		for _, r := range u.Roles {
			switch r.Name {
			case roles.AdminRoleName:
				return true
			}
		}
	case roles.SuperAdminStatus:
		// SuperAdmins should have been authorized before this.
		// This is only meant to restrict access for non-superadmins.
		return false
	}

	return false
}
