package server

import (
	"encoding/json"
	"fmt"
	"net/http"

	"golang.org/x/net/context"

	"github.com/influxdata/chronograf"
	"github.com/influxdata/chronograf/oauth2"
)

type userLinks struct {
	Self string `json:"self"` // Self link mapping to this resource
}

type userResponse struct {
	*chronograf.User
	Links userLinks `json:"links"`
}

// If new user response is nil, return an empty userResponse because it
// indicates authentication is not needed
func newUserResponse(usr *chronograf.User) userResponse {
	base := "/chronograf/v1/users"
	if usr != nil {
		return userResponse{
			User: usr,
			Links: userLinks{
				Self: fmt.Sprintf("%s/%d", base, usr.ID),
			},
		}
	}
	return userResponse{}
}

// NewUser adds a new valid user to the store
func (h *Service) NewUser(w http.ResponseWriter, r *http.Request) {
	var usr *chronograf.User
	if err := json.NewDecoder(r.Body).Decode(usr); err != nil {
		invalidJSON(w, h.Logger)
		return
	}
	if err := ValidUserRequest(usr); err != nil {
		invalidData(w, err, h.Logger)
		return
	}

	var err error
	if usr, err = h.UsersStore.Add(r.Context(), usr); err != nil {
		msg := fmt.Errorf("error storing user %v: %v", *usr, err)
		unknownErrorWithMessage(w, msg, h.Logger)
		return
	}

	res := newUserResponse(usr)
	w.Header().Add("Location", res.Links.Self)
	encodeJSON(w, http.StatusCreated, res, h.Logger)
}

// UserID retrieves a user from the store
func (h *Service) UserID(w http.ResponseWriter, r *http.Request) {
	id, err := paramID("id", r)
	if err != nil {
		Error(w, http.StatusUnprocessableEntity, err.Error(), h.Logger)
		return
	}

	ctx := r.Context()
	usr, err := h.UsersStore.Get(ctx, chronograf.UserID(id))
	if err != nil {
		notFound(w, id, h.Logger)
		return
	}

	res := newUserResponse(usr)
	encodeJSON(w, http.StatusOK, res, h.Logger)
}

// RemoveUser deletes the user from the store
func (h *Service) RemoveUser(w http.ResponseWriter, r *http.Request) {
	id, err := paramID("id", r)
	if err != nil {
		Error(w, http.StatusUnprocessableEntity, err.Error(), h.Logger)
		return
	}

	usr := &chronograf.User{ID: chronograf.UserID(id)}
	ctx := r.Context()
	if err = h.UsersStore.Delete(ctx, usr); err != nil {
		unknownErrorWithMessage(w, err, h.Logger)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// UpdateUser handles incremental updates of a data user
func (h *Service) UpdateUser(w http.ResponseWriter, r *http.Request) {
	id, err := paramID("id", r)
	if err != nil {
		Error(w, http.StatusUnprocessableEntity, err.Error(), h.Logger)
		return
	}

	ctx := r.Context()
	usr, err := h.UsersStore.Get(ctx, chronograf.UserID(id))
	if err != nil {
		notFound(w, id, h.Logger)
		return
	}

	var req chronograf.User
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		invalidJSON(w, h.Logger)
		return
	}

	usr.Email = req.Email
	if err := ValidUserRequest(usr); err != nil {
		invalidData(w, err, h.Logger)
		return
	}

	if err := h.UsersStore.Update(ctx, usr); err != nil {
		msg := fmt.Sprintf("Error updating user ID %d", id)
		Error(w, http.StatusInternalServerError, msg, h.Logger)
		return
	}
	encodeJSON(w, http.StatusOK, newUserResponse(usr), h.Logger)
}

// ValidUserRequest checks if email is nonempty
func ValidUserRequest(s *chronograf.User) error {
	// email is required
	if s.Email == "" {
		return fmt.Errorf("Email required")
	}
	return nil
}

func getEmail(ctx context.Context) (string, error) {
	principal, err := getPrincipal(ctx)
	if err != nil {
		return "", err
	}
	if principal.Subject == "" {
		return "", fmt.Errorf("Token not found")
	}
	return principal.Subject, nil
}

func getPrincipal(ctx context.Context) (oauth2.Principal, error) {
	principal, ok := ctx.Value(oauth2.PrincipalKey).(oauth2.Principal)
	if !ok {
		return oauth2.Principal{}, fmt.Errorf("Token not found")
	}

	return principal, nil
}

// Me does a findOrCreate based on the email in the context
func (h *Service) Me(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	if !h.UseAuth {
		// If there's no authentication, return an empty user
		res := newUserResponse(nil)
		encodeJSON(w, http.StatusOK, res, h.Logger)
		return
	}
	email, err := getEmail(ctx)
	if err != nil {
		invalidData(w, err, h.Logger)
		return
	}
	usr, err := h.UsersStore.FindByEmail(ctx, email)
	if err == nil {
		res := newUserResponse(usr)
		encodeJSON(w, http.StatusOK, res, h.Logger)
		return
	}

	// Because we didnt find a user, making a new one
	user := &chronograf.User{
		Email: email,
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
