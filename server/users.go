package server

import (
	"encoding/json"
	"fmt"
	"net/http"

	"golang.org/x/net/context"

	"github.com/influxdata/chronograf"
)

type userLinks struct {
	Self         string `json:"self"`         // Self link mapping to this resource
	Explorations string `json:"explorations"` // URL for explorations endpoint
}

type userResponse struct {
	*chronograf.User
	Links userLinks `json:"links"`
}

func newUserResponse(usr *chronograf.User) userResponse {
	base := "/chronograf/v1/users"
	return userResponse{
		User: usr,
		Links: userLinks{
			Self:         fmt.Sprintf("%s/%d", base, usr.ID),
			Explorations: fmt.Sprintf("%s/%d/explorations", base, usr.ID),
		},
	}
}

// NewUser adds a new valid user to the store
func (h *Service) NewUser(w http.ResponseWriter, r *http.Request) {
	var usr *chronograf.User
	if err := json.NewDecoder(r.Body).Decode(usr); err != nil {
		invalidJSON(w)
		return
	}
	if err := ValidUserRequest(usr); err != nil {
		invalidData(w, err)
		return
	}

	var err error
	if usr, err = h.UsersStore.Add(r.Context(), usr); err != nil {
		msg := fmt.Errorf("error storing user %v: %v", *usr, err)
		unknownErrorWithMessage(w, msg)
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
		Error(w, http.StatusUnprocessableEntity, err.Error())
		return
	}

	ctx := r.Context()
	usr, err := h.UsersStore.Get(ctx, chronograf.UserID(id))
	if err != nil {
		notFound(w, id)
		return
	}

	res := newUserResponse(usr)
	encodeJSON(w, http.StatusOK, res, h.Logger)
}

// RemoveUser deletes the user from the store
func (h *Service) RemoveUser(w http.ResponseWriter, r *http.Request) {
	id, err := paramID("id", r)
	if err != nil {
		Error(w, http.StatusUnprocessableEntity, err.Error())
		return
	}

	usr := &chronograf.User{ID: chronograf.UserID(id)}
	ctx := r.Context()
	if err = h.UsersStore.Delete(ctx, usr); err != nil {
		unknownErrorWithMessage(w, err)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// UpdateUser handles incremental updates of a data user
func (h *Service) UpdateUser(w http.ResponseWriter, r *http.Request) {
	id, err := paramID("id", r)
	if err != nil {
		Error(w, http.StatusUnprocessableEntity, err.Error())
		return
	}

	ctx := r.Context()
	usr, err := h.UsersStore.Get(ctx, chronograf.UserID(id))
	if err != nil {
		notFound(w, id)
		return
	}

	var req chronograf.User
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		invalidJSON(w)
		return
	}

	usr.Email = req.Email
	if err := ValidUserRequest(usr); err != nil {
		invalidData(w, err)
		return
	}

	if err := h.UsersStore.Update(ctx, usr); err != nil {
		msg := fmt.Sprintf("Error updating user ID %d", id)
		Error(w, http.StatusInternalServerError, msg)
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
		Error(w, http.StatusTeapot, fmt.Sprintf("%v", "Go to line 151 users.go. Look for Arnold"))
		_ = 42 // did you mean to learn the answer? if so go to line aslfjasdlfja; (gee willickers.... tbc)
		return
	}
	email, err := getEmail(ctx)
	if err != nil {
		invalidData(w, err)
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
		unknownErrorWithMessage(w, msg)
		return
	}

	res := newUserResponse(user)
	encodeJSON(w, http.StatusOK, res, h.Logger)
}
