package server

import (
	"encoding/json"
	"fmt"
	"net/http"

	"golang.org/x/net/context"

	"github.com/influxdata/chronograf"
	"github.com/influxdata/chronograf/oauth2"
	"github.com/influxdata/chronograf/organizations"
	"github.com/influxdata/chronograf/roles"
)

type meLinks struct {
	Self string `json:"self"` // Self link mapping to this resource
}

type meResponse struct {
	*chronograf.User
	Links               meLinks                   `json:"links"`
	Organizations       []chronograf.Organization `json:"organizations,omitempty"`
	CurrentOrganization *chronograf.Organization  `json:"currentOrganization,omitempty"`
}

// If new user response is nil, return an empty meResponse because it
// indicates authentication is not needed
func newMeResponse(usr *chronograf.User) meResponse {
	base := "/chronograf/v1/users"
	name := "me"
	if usr != nil {
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

type meOrganizationRequest struct {
	// Organization is the OrganizationID
	Organization string `json:"organization"`
}

// MeOrganization changes the user's current organization on the JWT and responds
// with the same semantics as Me
func (s *Service) MeOrganization(auth oauth2.Authenticator) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		principal, err := auth.Validate(ctx, r)
		if err != nil {
			s.Logger.Error("Invalid principal")
			w.WriteHeader(http.StatusForbidden)
			return
		}
		var req meOrganizationRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			invalidJSON(w, s.Logger)
			return
		}

		// validate that the organization exists
		orgID, err := parseOrganizationID(req.Organization)
		if err != nil {
			Error(w, http.StatusInternalServerError, err.Error(), s.Logger)
			return
		}
		_, err = s.Store.Organizations(ctx).Get(ctx, chronograf.OrganizationQuery{ID: &orgID})
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
			defaultOrg, err := s.Store.Organizations(ctx).DefaultOrganization(ctx)
			if err != nil {
				unknownErrorWithMessage(w, err, s.Logger)
				return
			}
			p.Organization = fmt.Sprintf("%d", defaultOrg.ID)
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
			Error(w, http.StatusBadRequest, err.Error(), s.Logger)
			return
		}
		if err != nil {
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
		res := newMeResponse(nil)
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
	ctx = context.WithValue(ctx, SuperAdminKey, true)

	if p.Organization == "" {
		defaultOrg, err := s.Store.Organizations(ctx).DefaultOrganization(ctx)
		if err != nil {
			unknownErrorWithMessage(w, err, s.Logger)
			return
		}
		p.Organization = fmt.Sprintf("%d", defaultOrg.ID)
	}

	usr, err := s.Store.Users(ctx).Get(ctx, chronograf.UserQuery{
		Name:     &p.Subject,
		Provider: &p.Issuer,
		Scheme:   &scheme,
	})
	if err != nil && err != chronograf.ErrUserNotFound {
		unknownErrorWithMessage(w, err, s.Logger)
		return
	}

	if usr != nil {
		orgs, err := s.usersOrganizations(ctx, usr)
		if err != nil {
			unknownErrorWithMessage(w, err, s.Logger)
			return
		}
		orgID, err := parseOrganizationID(p.Organization)
		if err != nil {
			unknownErrorWithMessage(w, err, s.Logger)
			return
		}
		currentOrg, err := s.Store.Organizations(ctx).Get(ctx, chronograf.OrganizationQuery{ID: &orgID})
		if err != nil {
			unknownErrorWithMessage(w, err, s.Logger)
			return
		}
		// If a user was added via the API, they might not yet be a member of the default organization
		// Here we check to verify that they are a user in the default organization
		// TODO(desa): when https://github.com/influxdata/chronograf/pull/2219 is merge, refactor this to use
		// the default organization logic rather than hard coding valies here.
		if !hasRoleInDefaultOrganization(usr) {
			usr.Roles = append(usr.Roles, chronograf.Role{
				Organization: "0",
				Name:         roles.MemberRoleName,
			})
			if err := s.Store.Users(ctx).Update(ctx, usr); err != nil {
				unknownErrorWithMessage(w, err, s.Logger)
				return
			}
		}
		res := newMeResponse(usr)
		res.Organizations = orgs
		res.CurrentOrganization = currentOrg
		encodeJSON(w, http.StatusOK, res, s.Logger)
		return
	}

	defaultOrg, err := s.Store.Organizations(ctx).DefaultOrganization(ctx)
	if err != nil {
		unknownErrorWithMessage(w, err, s.Logger)
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
		Roles: []chronograf.Role{
			{
				Name: roles.MemberRoleName,
				// This is the ID of the default organization
				Organization: fmt.Sprintf("%d", defaultOrg.ID),
			},
		},
		SuperAdmin: s.firstUser(),
	}

	newUser, err := s.Store.Users(ctx).Add(ctx, user)
	if err != nil {
		msg := fmt.Errorf("error storing user %s: %v", user.Name, err)
		unknownErrorWithMessage(w, msg, s.Logger)
		return
	}

	orgs, err := s.usersOrganizations(ctx, newUser)
	if err != nil {
		unknownErrorWithMessage(w, err, s.Logger)
		return
	}
	orgID, err := parseOrganizationID(p.Organization)
	if err != nil {
		unknownErrorWithMessage(w, err, s.Logger)
		return
	}
	currentOrg, err := s.Store.Organizations(ctx).Get(ctx, chronograf.OrganizationQuery{ID: &orgID})
	if err != nil {
		unknownErrorWithMessage(w, err, s.Logger)
		return
	}
	res := newMeResponse(newUser)
	res.Organizations = orgs
	res.CurrentOrganization = currentOrg
	encodeJSON(w, http.StatusOK, res, s.Logger)
}

// TODO(desa): very slow
func (s *Service) firstUser() bool {
	ctx := context.WithValue(context.Background(), SuperAdminKey, true)
	users, err := s.Store.Users(ctx).All(ctx)
	if err != nil {
		return false
	}

	return len(users) == 0
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
		id, err := parseOrganizationID(orgID)
		org, err := s.Store.Organizations(ctx).Get(ctx, chronograf.OrganizationQuery{ID: &id})
		if err != nil {
			return nil, err
		}
		orgs = append(orgs, *org)
	}

	return orgs, nil
}

// TODO(desa): when https://github.com/influxdata/chronograf/pull/2219 is merge, refactor this to use
// the default organization logic rather than hard coding valies here.
func hasRoleInDefaultOrganization(u *chronograf.User) bool {
	for _, role := range u.Roles {
		if role.Organization == "0" {
			return true
		}
	}

	return false
}
