package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"

	"github.com/bouk/httprouter"
	"github.com/influxdata/chronograf"
)

type newSourceUserRequest struct {
	Username string `json:"username,omitempty"` // Username for new account
	Password string `json:"password,omitempty"` // Passwor for new account
}

func (r *newSourceUserRequest) Valid() error {
	if r.Username == "" {
		return fmt.Errorf("Username required")
	}
	if r.Password == "" {
		return fmt.Errorf("Password required")
	}
	return nil
}

type sourceUser struct {
	Username string          `json:"username,omitempty"` // Username for new account
	Links    sourceUserLinks `json:"links"`              // Links are URI locations related to user
}

func NewSourceUser(srcID int, name string) sourceUser {
	u := &url.URL{Path: name}
	encodedUser := u.String()
	httpAPISrcs := "/chronograf/v1/sources"
	return sourceUser{
		Username: name,
		Links: sourceUserLinks{
			Self: fmt.Sprintf("%s/%d/users/%s", httpAPISrcs, srcID, encodedUser),
		},
	}
}

type sourceUserLinks struct {
	Self string `json:"self"` // Self link mapping to this resource
}

// NewSourceUser adds user to source
func (h *Service) NewSourceUser(w http.ResponseWriter, r *http.Request) {
	var req newSourceUserRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		invalidJSON(w, h.Logger)
		return
	}
	if err := req.Valid(); err != nil {
		invalidData(w, err, h.Logger)
		return
	}

	ctx := r.Context()

	srcID, store, err := h.sourceUsersStore(ctx, w, r)
	if err != nil {
		return
	}

	user := &chronograf.User{
		Name:   req.Username,
		Passwd: req.Password,
	}

	res, err := store.Add(ctx, user)
	if err != nil {
		Error(w, http.StatusUnprocessableEntity, err.Error(), h.Logger)
	}

	su := NewSourceUser(srcID, res.Name)
	w.Header().Add("Location", su.Links.Self)
	encodeJSON(w, http.StatusCreated, su, h.Logger)
}

type sourceUsers struct {
	Users []sourceUser `json:"users"`
}

// SourceUsers retrieves all users from source.
func (h *Service) SourceUsers(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	srcID, store, err := h.sourceUsersStore(ctx, w, r)
	if err != nil {
		return
	}

	users, err := store.All(ctx)
	if err != nil {
		Error(w, http.StatusBadRequest, err.Error(), h.Logger)
		return
	}

	su := []sourceUser{}
	for _, u := range users {
		su = append(su, NewSourceUser(srcID, u.Name))
	}

	res := sourceUsers{
		Users: su,
	}

	encodeJSON(w, http.StatusOK, res, h.Logger)
}

// SourceUserID retrieves a user with ID from store.
func (h *Service) SourceUserID(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	uid := httprouter.GetParamFromContext(ctx, "uid")

	srcID, store, err := h.sourceUsersStore(ctx, w, r)
	if err != nil {
		return
	}

	u, err := store.Get(ctx, uid)
	if err != nil {
		Error(w, http.StatusBadRequest, err.Error(), h.Logger)
		return
	}

	res := NewSourceUser(srcID, u.Name)
	encodeJSON(w, http.StatusOK, res, h.Logger)
}

func (h *Service) RemoveSourceUser(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	uid := httprouter.GetParamFromContext(ctx, "uid")

	_, store, err := h.sourceUsersStore(ctx, w, r)
	if err != nil {
		return
	}

	if err := store.Delete(ctx, &chronograf.User{Name: uid}); err != nil {
		Error(w, http.StatusBadRequest, err.Error(), h.Logger)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (h *Service) sourceUsersStore(ctx context.Context, w http.ResponseWriter, r *http.Request) (int, chronograf.UsersStore, error) {
	srcID, err := paramID("id", r)
	if err != nil {
		Error(w, http.StatusUnprocessableEntity, err.Error(), h.Logger)
		return 0, nil, err
	}

	src, err := h.SourcesStore.Get(ctx, srcID)
	if err != nil {
		notFound(w, srcID, h.Logger)
		return 0, nil, err
	}

	if err = h.TimeSeries.Connect(ctx, &src); err != nil {
		msg := fmt.Sprintf("Unable to connect to source %d", srcID)
		Error(w, http.StatusBadRequest, msg, h.Logger)
		return 0, nil, err
	}

	store := h.TimeSeries.Users(ctx)
	return srcID, store, nil
}
