package server

import (
	"encoding/json"
	"fmt"
	"net/http"

	"golang.org/x/net/context"

	"github.com/influxdata/chronograf"
	"github.com/influxdata/chronograf/oauth2"
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
	return "OAuth2", nil
}

func getPrincipal(ctx context.Context) (oauth2.Principal, error) {
	principal, ok := ctx.Value(oauth2.PrincipalKey).(oauth2.Principal)
	if !ok {
		return oauth2.Principal{}, fmt.Errorf("Token not found")
	}

	return principal, nil
}

// This is the user's current chronograf organization and is not related to any
// concept of a OAuth organization.
func getOrganization(ctx context.Context) (string, error) {
	principal, err := getPrincipal(ctx)
	if err != nil {
		return "", err
	}
	return principal.Organization, nil
}

type meOrganizationRequest struct {
	OrganizationID string `json:"organization"`
}

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

		// TODO: add logic for validating that the org exists and user belongs to that org

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

	username, err := getUsername(ctx)
	if err != nil {
		invalidData(w, err, s.Logger)
		return
	}
	provider, err := getProvider(ctx)
	if err != nil {
		invalidData(w, err, s.Logger)
		return
	}
	scheme, err := getScheme(ctx)
	if err != nil {
		invalidData(w, err, s.Logger)
		return
	}
	organization, err := getOrganization(ctx)
	if err != nil {
		invalidData(w, err, s.Logger)
		return
	}

	// TODO: add real implementation
	ctx = context.WithValue(ctx, "organizationID", organization)

	usr, err := s.UsersStore.Get(ctx, chronograf.UserQuery{
		Name:     &username,
		Provider: &provider,
		Scheme:   &scheme,
	})
	if err != nil && err != chronograf.ErrUserNotFound {
		unknownErrorWithMessage(w, err, s.Logger)
		return
	}

	if usr != nil {
		usr.CurrentOrganization = organization
		res := newMeResponse(usr)
		encodeJSON(w, http.StatusOK, res, s.Logger)
		return
	}

	// Because we didnt find a user, making a new one
	user := &chronograf.User{
		Name:     username,
		Provider: provider,
		// TODO: This Scheme value is hard-coded temporarily since we only currently
		// support OAuth2. This hard-coding should be removed whenever we add
		// support for other authentication schemes.
		Scheme: "OAuth2",
	}

	newUser, err := s.UsersStore.Add(ctx, user)
	if err != nil {
		msg := fmt.Errorf("error storing user %s: %v", user.Name, err)
		unknownErrorWithMessage(w, msg, s.Logger)
		return
	}

	newUser.CurrentOrganization = organization
	res := newMeResponse(newUser)
	encodeJSON(w, http.StatusOK, res, s.Logger)
}
