package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sort"

	"golang.org/x/net/context"

	"github.com/influxdata/platform/chronograf"
	"github.com/influxdata/platform/chronograf/oauth2"
	"github.com/influxdata/platform/chronograf/organizations"
)

type meLinks struct {
	Self string `json:"self"` // Self link mapping to this resource
}

type meResponse struct {
	*chronograf.User
	Links               meLinks                   `json:"links"`
	Organizations       []chronograf.Organization `json:"organizations"`
	CurrentOrganization *chronograf.Organization  `json:"currentOrganization,omitempty"`
}

type noAuthMeResponse struct {
	Links meLinks `json:"links"`
}

func newNoAuthMeResponse() noAuthMeResponse {
	return noAuthMeResponse{
		Links: meLinks{
			Self: "/chronograf/v1/me",
		},
	}
}

// If new user response is nil, return an empty meResponse because it
// indicates authentication is not needed
func newMeResponse(usr *chronograf.User, org string) meResponse {
	base := "/chronograf/v1"
	name := "me"
	if usr != nil {
		base = fmt.Sprintf("/chronograf/v1/organizations/%s/users", org)
		name = PathEscape(fmt.Sprintf("%d", usr.ID))
	}

	return meResponse{
		User: usr,
		Links: meLinks{
			Self: fmt.Sprintf("%s/%s", base, name),
		},
	}
}

// TODO: This Scheme value is hard-coded temporarily since we only currently
// support OAuth2. This hard-coding should be removed whenever we add
// support for other authentication schemes.
func getScheme(ctx context.Context) (string, error) {
	return "oauth2", nil
}

func getPrincipal(ctx context.Context) (oauth2.Principal, error) {
	principal, ok := ctx.Value(oauth2.PrincipalKey).(oauth2.Principal)
	if !ok {
		return oauth2.Principal{}, fmt.Errorf("Token not found")
	}

	return principal, nil
}

func getValidPrincipal(ctx context.Context) (oauth2.Principal, error) {
	p, err := getPrincipal(ctx)
	if err != nil {
		return p, err
	}
	if p.Subject == "" {
		return oauth2.Principal{}, fmt.Errorf("Token not found")
	}
	if p.Issuer == "" {
		return oauth2.Principal{}, fmt.Errorf("Token not found")
	}
	return p, nil
}

type meRequest struct {
	// Organization is the OrganizationID
	Organization string `json:"organization"`
}

// UpdateMe changes the user's current organization on the JWT and responds
// with the same semantics as Me
func (s *Service) UpdateMe(auth oauth2.Authenticator) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		serverCtx := serverContext(ctx)
		principal, err := auth.Validate(ctx, r)
		if err != nil {
			s.Logger.Error(fmt.Sprintf("Invalid principal: %v", err))
			Error(w, http.StatusForbidden, "invalid principal", s.Logger)
			return
		}
		var req meRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			invalidJSON(w, s.Logger)
			return
		}

		// validate that the organization exists
		org, err := s.Store.Organizations(serverCtx).Get(serverCtx, chronograf.OrganizationQuery{ID: &req.Organization})
		if err != nil {
			Error(w, http.StatusBadRequest, err.Error(), s.Logger)
			return
		}

		// validate that user belongs to organization
		ctx = context.WithValue(ctx, organizations.ContextKey, req.Organization)

		p, err := getValidPrincipal(ctx)
		if err != nil {
			invalidData(w, err, s.Logger)
			return
		}
		if p.Organization == "" {
			defaultOrg, err := s.Store.Organizations(serverCtx).DefaultOrganization(serverCtx)
			if err != nil {
				unknownErrorWithMessage(w, err, s.Logger)
				return
			}
			p.Organization = defaultOrg.ID
		}
		scheme, err := getScheme(ctx)
		if err != nil {
			invalidData(w, err, s.Logger)
			return
		}
		_, err = s.Store.Users(ctx).Get(ctx, chronograf.UserQuery{
			Name:     &p.Subject,
			Provider: &p.Issuer,
			Scheme:   &scheme,
		})
		if err == chronograf.ErrUserNotFound {
			// If the user was not found, check to see if they are a super admin. If
			// they are, add them to the organization.
			u, err := s.Store.Users(serverCtx).Get(serverCtx, chronograf.UserQuery{
				Name:     &p.Subject,
				Provider: &p.Issuer,
				Scheme:   &scheme,
			})
			if err != nil {
				Error(w, http.StatusForbidden, err.Error(), s.Logger)
				return
			}

			if u.SuperAdmin == false {
				// Since a user is not a part of this organization and not a super admin,
				// we should tell them that they are Forbidden (403) from accessing this resource
				Error(w, http.StatusForbidden, chronograf.ErrUserNotFound.Error(), s.Logger)
				return
			}

			// If the user is a super admin give them an admin role in the
			// requested organization.
			u.Roles = append(u.Roles, chronograf.Role{
				Organization: org.ID,
				Name:         org.DefaultRole,
			})
			if err := s.Store.Users(serverCtx).Update(serverCtx, u); err != nil {
				unknownErrorWithMessage(w, err, s.Logger)
				return
			}
		} else if err != nil {
			Error(w, http.StatusBadRequest, err.Error(), s.Logger)
			return
		}

		// TODO: change to principal.CurrentOrganization
		principal.Organization = req.Organization

		if err := auth.Authorize(ctx, w, principal); err != nil {
			Error(w, http.StatusInternalServerError, err.Error(), s.Logger)
			return
		}

		ctx = context.WithValue(ctx, oauth2.PrincipalKey, principal)

		s.Me(w, r.WithContext(ctx))
	}
}

// Me does a findOrCreate based on the username in the context
func (s *Service) Me(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	if !s.UseAuth {
		// If there's no authentication, return an empty user
		res := newNoAuthMeResponse()
		encodeJSON(w, http.StatusOK, res, s.Logger)
		return
	}

	p, err := getValidPrincipal(ctx)
	if err != nil {
		invalidData(w, err, s.Logger)
		return
	}
	scheme, err := getScheme(ctx)
	if err != nil {
		invalidData(w, err, s.Logger)
		return
	}

	ctx = context.WithValue(ctx, organizations.ContextKey, p.Organization)
	serverCtx := serverContext(ctx)

	defaultOrg, err := s.Store.Organizations(serverCtx).DefaultOrganization(serverCtx)
	if err != nil {
		unknownErrorWithMessage(w, err, s.Logger)
		return
	}

	if p.Organization == "" {
		p.Organization = defaultOrg.ID
	}

	usr, err := s.Store.Users(serverCtx).Get(serverCtx, chronograf.UserQuery{
		Name:     &p.Subject,
		Provider: &p.Issuer,
		Scheme:   &scheme,
	})
	if err != nil && err != chronograf.ErrUserNotFound {
		unknownErrorWithMessage(w, err, s.Logger)
		return
	}

	// user exists
	if usr != nil {
		superAdmin := s.mapPrincipalToSuperAdmin(p)
		if superAdmin && !usr.SuperAdmin {
			usr.SuperAdmin = superAdmin
			err := s.Store.Users(serverCtx).Update(serverCtx, usr)
			if err != nil {
				unknownErrorWithMessage(w, err, s.Logger)
				return
			}
		}

		currentOrg, err := s.Store.Organizations(serverCtx).Get(serverCtx, chronograf.OrganizationQuery{ID: &p.Organization})
		if err == chronograf.ErrOrganizationNotFound {
			// The intent is to force a the user to go through another auth flow
			Error(w, http.StatusForbidden, "user's current organization was not found", s.Logger)
			return
		}
		if err != nil {
			unknownErrorWithMessage(w, err, s.Logger)
			return
		}

		orgs, err := s.usersOrganizations(serverCtx, usr)
		if err != nil {
			unknownErrorWithMessage(w, err, s.Logger)
			return
		}

		res := newMeResponse(usr, currentOrg.ID)
		res.Organizations = orgs
		res.CurrentOrganization = currentOrg
		encodeJSON(w, http.StatusOK, res, s.Logger)
		return
	}

	// Because we didnt find a user, making a new one
	user := &chronograf.User{
		Name:     p.Subject,
		Provider: p.Issuer,
		// TODO: This Scheme value is hard-coded temporarily since we only currently
		// support OAuth2. This hard-coding should be removed whenever we add
		// support for other authentication schemes.
		Scheme: scheme,
		// TODO(desa): this needs a better name
		SuperAdmin: s.newUsersAreSuperAdmin(),
	}

	superAdmin := s.mapPrincipalToSuperAdmin(p)
	if superAdmin {
		user.SuperAdmin = superAdmin
	}

	roles, err := s.mapPrincipalToRoles(serverCtx, p)
	if err != nil {
		Error(w, http.StatusInternalServerError, err.Error(), s.Logger)
		return
	}

	if !superAdmin && len(roles) == 0 {
		Error(w, http.StatusForbidden, "This Chronograf is private. To gain access, you must be explicitly added by an administrator.", s.Logger)
		return
	}

	// If the user is a superadmin, give them a role in the default organization
	if user.SuperAdmin {
		hasDefaultOrgRole := false
		for _, role := range roles {
			if role.Organization == defaultOrg.ID {
				hasDefaultOrgRole = true
				break
			}
		}
		if !hasDefaultOrgRole {
			roles = append(roles, chronograf.Role{
				Name:         defaultOrg.DefaultRole,
				Organization: defaultOrg.ID,
			})
		}
	}

	user.Roles = roles

	newUser, err := s.Store.Users(serverCtx).Add(serverCtx, user)
	if err != nil {
		msg := fmt.Errorf("error storing user %s: %v", user.Name, err)
		unknownErrorWithMessage(w, msg, s.Logger)
		return
	}

	orgs, err := s.usersOrganizations(serverCtx, newUser)
	if err != nil {
		unknownErrorWithMessage(w, err, s.Logger)
		return
	}
	currentOrg, err := s.Store.Organizations(serverCtx).Get(serverCtx, chronograf.OrganizationQuery{ID: &p.Organization})
	if err != nil {
		unknownErrorWithMessage(w, err, s.Logger)
		return
	}
	res := newMeResponse(newUser, currentOrg.ID)
	res.Organizations = orgs
	res.CurrentOrganization = currentOrg
	encodeJSON(w, http.StatusOK, res, s.Logger)
}

func (s *Service) firstUser() bool {
	serverCtx := serverContext(context.Background())
	numUsers, err := s.Store.Users(serverCtx).Num(serverCtx)
	if err != nil {
		return false
	}

	return numUsers == 0
}
func (s *Service) newUsersAreSuperAdmin() bool {
	// It's not necessary to enforce that the first user is superAdmin here, since
	// superAdminNewUsers defaults to true, but there's nothing else in the
	// application that dictates that it must be true.
	// So for that reason, we kept this here for now. We've discussed the
	// future possibility of allowing users to override default values via CLI and
	// this case could possibly happen then.
	if s.firstUser() {
		return true
	}
	serverCtx := serverContext(context.Background())
	cfg, err := s.Store.Config(serverCtx).Get(serverCtx)
	if err != nil {
		return false
	}
	return cfg.Auth.SuperAdminNewUsers
}

func (s *Service) usersOrganizations(ctx context.Context, u *chronograf.User) ([]chronograf.Organization, error) {
	if u == nil {
		// TODO(desa): better error
		return nil, fmt.Errorf("user was nil")
	}

	orgIDs := map[string]bool{}
	for _, role := range u.Roles {
		orgIDs[role.Organization] = true
	}

	orgs := []chronograf.Organization{}
	for orgID, _ := range orgIDs {
		org, err := s.Store.Organizations(ctx).Get(ctx, chronograf.OrganizationQuery{ID: &orgID})

		// There can be race conditions between deleting a organization and the me query
		if err == chronograf.ErrOrganizationNotFound {
			continue
		}

		// Any other error should cause an error to be returned
		if err != nil {
			return nil, err
		}
		orgs = append(orgs, *org)
	}

	sort.Slice(orgs, func(i, j int) bool {
		return orgs[i].ID < orgs[j].ID
	})

	return orgs, nil
}

func hasRoleInDefaultOrganization(u *chronograf.User, orgID string) bool {
	for _, role := range u.Roles {
		if role.Organization == orgID {
			return true
		}
	}

	return false
}
