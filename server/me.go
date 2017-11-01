package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"

	"golang.org/x/net/context"

	"github.com/influxdata/chronograf"
	"github.com/influxdata/chronograf/oauth2"
	"github.com/influxdata/chronograf/organizations"
)

type meLinks struct {
	Self string `json:"self"` // Self link mapping to this resource
}

type meResponse struct {
	*chronograf.User
	Links meLinks `json:"links"`
}

// If new user response is nil, return an empty meResponse because it
// indicates authentication is not needed
func newMeResponse(usr *chronograf.User) meResponse {
	base := "/chronograf/v1/users"
	name := "me"
	if usr != nil {
		name = PathEscape(usr.Name)
	}

	return meResponse{
		User: usr,
		Links: meLinks{
			Self: fmt.Sprintf("%s/%s", base, name),
		},
	}
}

// getUsername not currently used
func getUsername(ctx context.Context) (string, error) {
	principal, err := getPrincipal(ctx)
	if err != nil {
		return "", err
	}
	if principal.Subject == "" {
		return "", fmt.Errorf("Token not found")
	}
	return principal.Subject, nil
}

// getProvider not currently used
func getProvider(ctx context.Context) (string, error) {
	principal, err := getPrincipal(ctx)
	if err != nil {
		return "", err
	}
	if principal.Issuer == "" {
		return "", fmt.Errorf("Token not found")
	}
	return principal.Issuer, nil
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
	OrganizationID string `json:"currentOrganization"`
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
		orgID, err := strconv.ParseUint(req.OrganizationID, 10, 64)
		if err != nil {
			Error(w, http.StatusInternalServerError, err.Error(), s.Logger)
			return
		}
		_, err = s.Store.Organizations(ctx).Get(ctx, chronograf.OrganizationQuery{ID: &orgID})
		if err != nil {
			Error(w, http.StatusBadRequest, err.Error(), s.Logger)
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
		// validate that user belongs to organization
		ctx = context.WithValue(ctx, organizations.ContextKey, req.OrganizationID)
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
		principal.Organization = req.OrganizationID

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
		usr.CurrentOrganization = p.Organization
		res := newMeResponse(usr)
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
		Roles: []chronograf.Role{
			{
				Name: MemberRoleName,
				// This is the ID of the default organization
				Organization: "0",
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

	newUser.CurrentOrganization = p.Organization
	res := newMeResponse(newUser)
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
