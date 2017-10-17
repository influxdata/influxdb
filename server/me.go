package server

import (
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

func getPrincipal(ctx context.Context) (oauth2.Principal, error) {
	principal, ok := ctx.Value(oauth2.PrincipalKey).(oauth2.Principal)
	if !ok {
		return oauth2.Principal{}, fmt.Errorf("Token not found")
	}

	return principal, nil
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

	usrs, err := s.UsersStore.All(ctx)
	if err != nil {
		msg := fmt.Errorf("error retrieving user with Username: %s, Provider: %s: %v", username, provider, err)
		unknownErrorWithMessage(w, msg, s.Logger)
		return
	}

	var usr *chronograf.User
	for _, u := range usrs {
		if u.Name == username && u.Provider == provider {
			usr = &u
			break
		}
	}

	if usr != nil {
		res := newMeResponse(usr)
		encodeJSON(w, http.StatusOK, res, s.Logger)
		return
	}

	// Because we didnt find a user, making a new one
	user := &chronograf.User{
		Name: username,
	}

	newUser, err := s.UsersStore.Add(ctx, user)
	if err != nil {
		msg := fmt.Errorf("error storing user %s: %v", user.Name, err)
		unknownErrorWithMessage(w, msg, s.Logger)
		return
	}

	res := newMeResponse(newUser)
	encodeJSON(w, http.StatusOK, res, s.Logger)
}
