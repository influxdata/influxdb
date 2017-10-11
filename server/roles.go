package server

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/bouk/httprouter"
	"github.com/influxdata/chronograf"
)

// NewRole adds role to source
func (s *Service) NewRole(w http.ResponseWriter, r *http.Request) {
	var req sourceRoleRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		invalidJSON(w, s.Logger)
		return
	}

	if err := req.ValidCreate(); err != nil {
		invalidData(w, err, s.Logger)
		return
	}

	ctx := r.Context()
	srcID, ts, err := s.sourcesSeries(ctx, w, r)
	if err != nil {
		return
	}

	roles, ok := s.hasRoles(ctx, ts)
	if !ok {
		Error(w, http.StatusNotFound, fmt.Sprintf("Source %d does not have role capability", srcID), s.Logger)
		return
	}

	if _, err := roles.Get(ctx, req.Name); err == nil {
		Error(w, http.StatusBadRequest, fmt.Sprintf("Source %d already has role %s", srcID, req.Name), s.Logger)
		return
	}

	res, err := roles.Add(ctx, &req.Role)
	if err != nil {
		Error(w, http.StatusBadRequest, err.Error(), s.Logger)
		return
	}

	rr := newRoleResponse(srcID, res)
	location(w, rr.Links.Self)
	encodeJSON(w, http.StatusCreated, rr, s.Logger)
}

// UpdateRole changes the permissions or users of a role
func (s *Service) UpdateRole(w http.ResponseWriter, r *http.Request) {
	var req sourceRoleRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		invalidJSON(w, s.Logger)
		return
	}
	if err := req.ValidUpdate(); err != nil {
		invalidData(w, err, s.Logger)
		return
	}

	ctx := r.Context()
	srcID, ts, err := s.sourcesSeries(ctx, w, r)
	if err != nil {
		return
	}

	roles, ok := s.hasRoles(ctx, ts)
	if !ok {
		Error(w, http.StatusNotFound, fmt.Sprintf("Source %d does not have role capability", srcID), s.Logger)
		return
	}

	rid := httprouter.GetParamFromContext(ctx, "rid")
	req.Name = rid

	if err := roles.Update(ctx, &req.Role); err != nil {
		Error(w, http.StatusBadRequest, err.Error(), s.Logger)
		return
	}

	role, err := roles.Get(ctx, req.Name)
	if err != nil {
		Error(w, http.StatusBadRequest, err.Error(), s.Logger)
		return
	}
	rr := newRoleResponse(srcID, role)
	location(w, rr.Links.Self)
	encodeJSON(w, http.StatusOK, rr, s.Logger)
}

// RoleID retrieves a role with ID from store.
func (s *Service) RoleID(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	srcID, ts, err := s.sourcesSeries(ctx, w, r)
	if err != nil {
		return
	}

	roles, ok := s.hasRoles(ctx, ts)
	if !ok {
		Error(w, http.StatusNotFound, fmt.Sprintf("Source %d does not have role capability", srcID), s.Logger)
		return
	}

	rid := httprouter.GetParamFromContext(ctx, "rid")
	role, err := roles.Get(ctx, rid)
	if err != nil {
		Error(w, http.StatusBadRequest, err.Error(), s.Logger)
		return
	}
	rr := newRoleResponse(srcID, role)
	encodeJSON(w, http.StatusOK, rr, s.Logger)
}

// Roles retrieves all roles from the store
func (s *Service) Roles(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	srcID, ts, err := s.sourcesSeries(ctx, w, r)
	if err != nil {
		return
	}

	store, ok := s.hasRoles(ctx, ts)
	if !ok {
		Error(w, http.StatusNotFound, fmt.Sprintf("Source %d does not have role capability", srcID), s.Logger)
		return
	}

	roles, err := store.All(ctx)
	if err != nil {
		Error(w, http.StatusBadRequest, err.Error(), s.Logger)
		return
	}

	rr := make([]roleResponse, len(roles))
	for i, role := range roles {
		rr[i] = newRoleResponse(srcID, &role)
	}

	res := struct {
		Roles []roleResponse `json:"roles"`
	}{rr}
	encodeJSON(w, http.StatusOK, res, s.Logger)
}

// RemoveRole removes role from data source.
func (s *Service) RemoveRole(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	srcID, ts, err := s.sourcesSeries(ctx, w, r)
	if err != nil {
		return
	}

	roles, ok := s.hasRoles(ctx, ts)
	if !ok {
		Error(w, http.StatusNotFound, fmt.Sprintf("Source %d does not have role capability", srcID), s.Logger)
		return
	}

	rid := httprouter.GetParamFromContext(ctx, "rid")
	if err := roles.Delete(ctx, &chronograf.Role{Name: rid}); err != nil {
		Error(w, http.StatusBadRequest, err.Error(), s.Logger)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// sourceRoleRequest is the format used for both creating and updating roles
type sourceRoleRequest struct {
	chronograf.Role
}

func (r *sourceRoleRequest) ValidCreate() error {
	if r.Name == "" || len(r.Name) > 254 {
		return fmt.Errorf("Name is required for a role")
	}
	for _, user := range r.Users {
		if user.Name == "" {
			return fmt.Errorf("Username required")
		}
	}
	return validPermissions(&r.Permissions)
}

func (r *sourceRoleRequest) ValidUpdate() error {
	if len(r.Name) > 254 {
		return fmt.Errorf("Username too long; must be less than 254 characters")
	}
	for _, user := range r.Users {
		if user.Name == "" {
			return fmt.Errorf("Username required")
		}
	}
	return validPermissions(&r.Permissions)
}

type roleResponse struct {
	Users       []*sourceUserResponse  `json:"users"`
	Name        string                 `json:"name"`
	Permissions chronograf.Permissions `json:"permissions"`
	Links       selfLinks              `json:"links"`
}

func newRoleResponse(srcID int, res *chronograf.Role) roleResponse {
	su := make([]*sourceUserResponse, len(res.Users))
	for i := range res.Users {
		name := res.Users[i].Name
		su[i] = newSourceUserResponse(srcID, name)
	}

	if res.Permissions == nil {
		res.Permissions = make(chronograf.Permissions, 0)
	}
	return roleResponse{
		Name:        res.Name,
		Permissions: res.Permissions,
		Users:       su,
		Links:       newSelfLinks(srcID, "roles", res.Name),
	}
}
