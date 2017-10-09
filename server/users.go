package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/bouk/httprouter"
	"github.com/influxdata/chronograf"
)

type userRequest struct {
	ID       uint64 `json:"id"`
	Name     string `json:"name"`
	Provider string `json:"provider,omitempty"`
	Scheme   string `json:"scheme,omitempty"`
}

func (r *userRequest) ValidCreate() error {
	if r.Name == "" {
		return errors.New("Name required on Chronograf User request body")
	}
	if r.Provider == "" {
		return errors.New("Provider required on Chronograf User request body")
	}
	if r.Scheme == "" {
		return errors.New("Scheme required on Chronograf User request body")
	}
	return nil
}

func (r *userRequest) ValidUpdate() error {
	if r.Name == "" && r.Provider == "" && r.Scheme == "" {
		return errors.New("No fields to update")
	}
	return nil
}

type userResponse struct {
	Links    selfLinks `json:"links"`
	ID       uint64    `json:"id"`
	Name     string    `json:"name,omitempty"`
	Provider string    `json:"provider,omitempty"`
	Scheme   string    `json:"scheme,omitempty"`
}

func newUserResponse(u *chronograf.User) *userResponse {
	return &userResponse{
		ID:       u.ID,
		Name:     u.Name,
		Provider: u.Provider,
		Scheme:   u.Scheme,
		Links: selfLinks{
			Self: fmt.Sprintf("/chronograf/v1/users/%v", u.ID),
		},
	}
}

type usersResponse struct {
	Links selfLinks       `json:"links"`
	Users []*userResponse `json:"users"`
}

func newUsersResponse(users []chronograf.User) *usersResponse {
	usersResp := make([]*userResponse, len(users))
	for i, user := range users {
		usersResp[i] = newUserResponse(&user)
	}
	return &usersResponse{
		Users: usersResp,
		Links: selfLinks{
			Self: "/chronograf/v1/users",
		},
	}
}

func (s *Service) UserID(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	id := httprouter.GetParamFromContext(ctx, "id")
	user, err := s.UsersStore.Get(ctx, id)
	if err != nil {
		Error(w, http.StatusBadRequest, err.Error(), s.Logger)
		return
	}

	res := newUserResponse(user)
	encodeJSON(w, http.StatusOK, res, s.Logger)
}

func (s *Service) NewUser(w http.ResponseWriter, r *http.Request) {
	var req userRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		invalidJSON(w, s.Logger)
		return
	}

	if err := req.ValidCreate(); err != nil {
		invalidData(w, err, s.Logger)
		return
	}

	ctx := r.Context()
	user := &chronograf.User{
		Name:     req.Name,
		Provider: req.Provider,
		Scheme:   req.Scheme,
	}

	res, err := s.UsersStore.Add(ctx, user)
	if err != nil {
		Error(w, http.StatusBadRequest, err.Error(), s.Logger)
		return
	}

	cu := newUserResponse(res)
	w.Header().Add("Location", cu.Links.Self)
	encodeJSON(w, http.StatusCreated, cu, s.Logger)
}

func (s *Service) RemoveUser(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	id := httprouter.GetParamFromContext(ctx, "id")

	u, err := s.UsersStore.Get(ctx, id)
	if err != nil {
		Error(w, http.StatusNotFound, err.Error(), s.Logger)
	}
	if err := s.UsersStore.Delete(ctx, u); err != nil {
		Error(w, http.StatusBadRequest, err.Error(), s.Logger)
	}

	w.WriteHeader(http.StatusNoContent)
}

func (s *Service) UpdateUser(w http.ResponseWriter, r *http.Request) {
	var req userRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		invalidJSON(w, s.Logger)
		return
	}

	if err := req.ValidUpdate(); err != nil {
		invalidData(w, err, s.Logger)
		return
	}

	ctx := r.Context()
	id := httprouter.GetParamFromContext(ctx, "id")

	u, err := s.UsersStore.Get(ctx, id)
	if err != nil {
		Error(w, http.StatusNotFound, err.Error(), s.Logger)
	}

	// TODO: should we diff and only set non-nil fields?
	u.Name = req.Name
	u.Provider = req.Provider
	u.Scheme = req.Scheme

	err = s.UsersStore.Update(ctx, u)
	if err != nil {
		Error(w, http.StatusBadRequest, err.Error(), s.Logger)
		return
	}

	cu := newUserResponse(u)
	w.Header().Add("Location", cu.Links.Self)
	encodeJSON(w, http.StatusOK, cu, s.Logger)
}

func (s *Service) Users(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	users, err := s.UsersStore.All(ctx)
	if err != nil {
		Error(w, http.StatusBadRequest, err.Error(), s.Logger)
		return
	}

	res := newUsersResponse(users)
	encodeJSON(w, http.StatusOK, res, s.Logger)
}
