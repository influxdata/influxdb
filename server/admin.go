package server

import (
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
	srcID, err := paramID("id", r)
	if err != nil {
		Error(w, http.StatusUnprocessableEntity, err.Error(), h.Logger)
		return
	}

	ctx := r.Context()
	src, err := h.SourcesStore.Get(ctx, srcID)
	if err != nil {
		notFound(w, srcID, h.Logger)
		return
	}

	var req newSourceUserRequest
	if err = json.NewDecoder(r.Body).Decode(&req); err != nil {
		invalidJSON(w, h.Logger)
		return
	}
	if err := req.Valid(); err != nil {
		invalidData(w, err, h.Logger)
		return
	}

	if err = h.TimeSeries.Connect(ctx, &src); err != nil {
		msg := fmt.Sprintf("Unable to connect to source %d", srcID)
		Error(w, http.StatusBadRequest, msg, h.Logger)
		return
	}

	store := h.TimeSeries.Users(ctx)
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

// SourceUserID retrieves a user with ID from store.
func (h *Service) SourceUserID(w http.ResponseWriter, r *http.Request) {
	srcID, err := paramID("id", r)
	if err != nil {
		Error(w, http.StatusUnprocessableEntity, err.Error(), h.Logger)
		return
	}

	ctx := r.Context()
	src, err := h.SourcesStore.Get(ctx, srcID)
	if err != nil {
		notFound(w, srcID, h.Logger)
		return
	}

	if err = h.TimeSeries.Connect(ctx, &src); err != nil {
		msg := fmt.Sprintf("Unable to connect to source %d", srcID)
		Error(w, http.StatusBadRequest, msg, h.Logger)
		return
	}

	uid := httprouter.GetParamFromContext(ctx, "uid")
	store := h.TimeSeries.Users(ctx)
	u, err := store.Get(ctx, uid)

	res := NewSourceUser(srcID, u.Name)
	encodeJSON(w, http.StatusOK, res, h.Logger)
}
