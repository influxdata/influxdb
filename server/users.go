package server

import (
	"fmt"
	"net/http"
	"net/url"

	"golang.org/x/net/context"

	"github.com/influxdata/chronograf"
)

type userLinks struct {
	Self string `json:"self"` // Self link mapping to this resource
}

type userResponse struct {
	*chronograf.User
	Links userLinks `json:"links"`
}

func newUserResponse(usr *chronograf.User) userResponse {
	base := "/chronograf/v1/users"
	// TODO: Change to usrl.PathEscape for go 1.8
	u := &url.URL{Path: usr.Name}
	encodedUser := u.String()
	return userResponse{
		User: usr,
		Links: userLinks{
			Self: fmt.Sprintf("%s/%s", base, encodedUser),
		},
	}
}

func getPrincipal(ctx context.Context) (string, error) {
	principal := ctx.Value(chronograf.PrincipalKey).(chronograf.Principal)
	if principal == "" {
		return "", fmt.Errorf("Token not found")
	}
	return string(principal), nil
}

// Me does a findOrCreate based on the email in the context
func (h *Service) Me(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	if !h.UseAuth {
		// Using status code to signal no need for authentication
		w.WriteHeader(http.StatusTeapot)
		return
	}
	principal, err := getPrincipal(ctx)
	if err != nil {
		invalidData(w, err, h.Logger)
		return
	}

	usr, err := h.UsersStore.Get(ctx, principal)
	if err == nil {
		res := newUserResponse(usr)
		encodeJSON(w, http.StatusOK, res, h.Logger)
		return
	}

	// Because we didnt find a user, making a new one
	user := &chronograf.User{
		Name: principal,
	}
	user, err = h.UsersStore.Add(ctx, user)
	if err != nil {
		msg := fmt.Errorf("error storing user %v: %v", user, err)
		unknownErrorWithMessage(w, msg, h.Logger)
		return
	}

	res := newUserResponse(user)
	encodeJSON(w, http.StatusOK, res, h.Logger)
}
