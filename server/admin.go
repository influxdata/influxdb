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

func validPermissions(perms *chronograf.Permissions) error {
	if perms == nil {
		return nil
	}
	for _, perm := range *perms {
		if perm.Scope != chronograf.AllScope && perm.Scope != chronograf.DBScope {
			return fmt.Errorf("Invalid permission scope")
		}
		if perm.Scope == chronograf.DBScope && perm.Name == "" {
			return fmt.Errorf("Database scoped permission requires a name")
		}
	}
	return nil
}

type sourceUserRequest struct {
	Username    string                 `json:"name,omitempty"`        // Username for new account
	Password    string                 `json:"password,omitempty"`    // Password for new account
	Permissions chronograf.Permissions `json:"permissions,omitempty"` // Optional permissions
}

func (r *sourceUserRequest) ValidCreate() error {
	if r.Username == "" {
		return fmt.Errorf("Username required")
	}
	if r.Password == "" {
		return fmt.Errorf("Password required")
	}
	return validPermissions(&r.Permissions)
}

func (r *sourceUserRequest) ValidUpdate() error {
	if r.Password == "" && len(r.Permissions) == 0 {
		return fmt.Errorf("No fields to update")
	}
	return validPermissions(&r.Permissions)
}

type sourceUser struct {
	Username    string                 `json:"name,omitempty"`        // Username for new account
	Permissions chronograf.Permissions `json:"permissions,omitempty"` // Account's permissions
	Roles       []roleResponse         `json:"roles,omitempty"`       // Roles if source uses them
	Links       selfLinks              `json:"links"`                 // Links are URI locations related to user
}

type selfLinks struct {
	Self string `json:"self"` // Self link mapping to this resource
}

func newSelfLinks(id int, parent, resource string) selfLinks {
	httpAPISrcs := "/chronograf/v1/sources"
	u := &url.URL{Path: resource}
	encodedResource := u.String()
	return selfLinks{
		Self: fmt.Sprintf("%s/%d/%s/%s", httpAPISrcs, id, parent, encodedResource),
	}
}

// NewSourceUser adds user to source
func (h *Service) NewSourceUser(w http.ResponseWriter, r *http.Request) {
	var req sourceUserRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		invalidJSON(w, h.Logger)
		return
	}
	if err := req.ValidCreate(); err != nil {
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
		Error(w, http.StatusBadRequest, err.Error(), h.Logger)
		return
	}

	su := sourceUser{
		Username:    res.Name,
		Permissions: req.Permissions,
		Links:       newSelfLinks(srcID, "users", res.Name),
	}
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
		res := sourceUser{
			Username:    u.Name,
			Permissions: u.Permissions,
			Links:       newSelfLinks(srcID, "users", u.Name),
		}
		if len(u.Roles) > 0 {
			rr := make([]roleResponse, len(u.Roles))
			for i, role := range u.Roles {
				rr[i] = newRoleResponse(srcID, &role)
			}
			res.Roles = rr
		}
		su = append(su, res)
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

	res := sourceUser{
		Username:    u.Name,
		Permissions: u.Permissions,
		Links:       newSelfLinks(srcID, "users", u.Name),
	}
	if len(u.Roles) > 0 {
		rr := make([]roleResponse, len(u.Roles))
		for i, role := range u.Roles {
			rr[i] = newRoleResponse(srcID, &role)
		}
		res.Roles = rr
	}
	encodeJSON(w, http.StatusOK, res, h.Logger)
}

// RemoveSourceUser removes the user from the InfluxDB source
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

// UpdateSourceUser changes the password or permissions of a source user
func (h *Service) UpdateSourceUser(w http.ResponseWriter, r *http.Request) {
	var req sourceUserRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		invalidJSON(w, h.Logger)
		return
	}
	if err := req.ValidUpdate(); err != nil {
		invalidData(w, err, h.Logger)
		return
	}

	ctx := r.Context()
	uid := httprouter.GetParamFromContext(ctx, "uid")
	srcID, store, err := h.sourceUsersStore(ctx, w, r)
	if err != nil {
		return
	}

	user := &chronograf.User{
		Name:        uid,
		Passwd:      req.Password,
		Permissions: req.Permissions,
	}

	if err := store.Update(ctx, user); err != nil {
		Error(w, http.StatusBadRequest, err.Error(), h.Logger)
		return
	}

	su := sourceUser{
		Username:    user.Name,
		Permissions: user.Permissions,
		Links:       newSelfLinks(srcID, "users", user.Name),
	}
	w.Header().Add("Location", su.Links.Self)
	encodeJSON(w, http.StatusOK, su, h.Logger)
}

func (h *Service) sourcesSeries(ctx context.Context, w http.ResponseWriter, r *http.Request) (int, chronograf.TimeSeries, error) {
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

	ts, err := h.TimeSeries(src)
	if err != nil {
		msg := fmt.Sprintf("Unable to connect to source %d: %v", srcID, err)
		Error(w, http.StatusBadRequest, msg, h.Logger)
		return 0, nil, err
	}

	if err = ts.Connect(ctx, &src); err != nil {
		msg := fmt.Sprintf("Unable to connect to source %d: %v", srcID, err)
		Error(w, http.StatusBadRequest, msg, h.Logger)
		return 0, nil, err
	}
	return srcID, ts, nil
}

func (h *Service) sourceUsersStore(ctx context.Context, w http.ResponseWriter, r *http.Request) (int, chronograf.UsersStore, error) {
	srcID, ts, err := h.sourcesSeries(ctx, w, r)
	if err != nil {
		return 0, nil, err
	}

	store := ts.Users(ctx)
	return srcID, store, nil
}

// hasRoles checks if the influx source has roles or not
func (h *Service) hasRoles(ctx context.Context, ts chronograf.TimeSeries) (chronograf.RolesStore, bool) {
	store, err := ts.Roles(ctx)
	if err != nil {
		return nil, false
	}
	return store, true
}

// Permissions returns all possible permissions for this source.
func (h *Service) Permissions(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	srcID, err := paramID("id", r)
	if err != nil {
		Error(w, http.StatusUnprocessableEntity, err.Error(), h.Logger)
		return
	}

	src, err := h.SourcesStore.Get(ctx, srcID)
	if err != nil {
		notFound(w, srcID, h.Logger)
		return
	}

	ts, err := h.TimeSeries(src)
	if err != nil {
		msg := fmt.Sprintf("Unable to connect to source %d: %v", srcID, err)
		Error(w, http.StatusBadRequest, msg, h.Logger)
		return
	}

	if err = ts.Connect(ctx, &src); err != nil {
		msg := fmt.Sprintf("Unable to connect to source %d: %v", srcID, err)
		Error(w, http.StatusBadRequest, msg, h.Logger)
		return
	}

	perms := ts.Permissions(ctx)
	if err != nil {
		Error(w, http.StatusBadRequest, err.Error(), h.Logger)
		return
	}
	httpAPISrcs := "/chronograf/v1/sources"
	res := struct {
		Permissions chronograf.Permissions `json:"permissions"`
		Links       map[string]string      `json:"links"` // Links are URI locations related to user
	}{
		Permissions: perms,
		Links: map[string]string{
			"self":   fmt.Sprintf("%s/%d/permissions", httpAPISrcs, srcID),
			"source": fmt.Sprintf("%s/%d", httpAPISrcs, srcID),
		},
	}
	encodeJSON(w, http.StatusOK, res, h.Logger)
}

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
	Users       []sourceUser           `json:"users,omitempty"`
	Name        string                 `json:"name"`
	Permissions chronograf.Permissions `json:"permissions"`
	Links       selfLinks              `json:"links"`
}

func newRoleResponse(srcID int, res *chronograf.Role) roleResponse {
	su := make([]sourceUser, len(res.Users))
	for i := range res.Users {
		name := res.Users[i].Name
		su[i] = sourceUser{
			Username: name,
			Links:    newSelfLinks(srcID, "users", name),
		}
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

// NewRole adds role to source
func (h *Service) NewRole(w http.ResponseWriter, r *http.Request) {
	var req sourceRoleRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		invalidJSON(w, h.Logger)
		return
	}

	if err := req.ValidCreate(); err != nil {
		invalidData(w, err, h.Logger)
		return
	}

	ctx := r.Context()
	srcID, ts, err := h.sourcesSeries(ctx, w, r)
	if err != nil {
		return
	}

	roles, ok := h.hasRoles(ctx, ts)
	if !ok {
		Error(w, http.StatusNotFound, fmt.Sprintf("Source %d does not have role capability", srcID), h.Logger)
		return
	}

	res, err := roles.Add(ctx, &req.Role)
	if err != nil {
		Error(w, http.StatusBadRequest, err.Error(), h.Logger)
		return
	}

	rr := newRoleResponse(srcID, res)
	w.Header().Add("Location", rr.Links.Self)
	encodeJSON(w, http.StatusCreated, rr, h.Logger)
}

// UpdateRole changes the permissions or users of a role
func (h *Service) UpdateRole(w http.ResponseWriter, r *http.Request) {
	var req sourceRoleRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		invalidJSON(w, h.Logger)
		return
	}
	if err := req.ValidUpdate(); err != nil {
		invalidData(w, err, h.Logger)
		return
	}

	ctx := r.Context()
	srcID, ts, err := h.sourcesSeries(ctx, w, r)
	if err != nil {
		return
	}

	roles, ok := h.hasRoles(ctx, ts)
	if !ok {
		Error(w, http.StatusNotFound, fmt.Sprintf("Source %d does not have role capability", srcID), h.Logger)
		return
	}

	rid := httprouter.GetParamFromContext(ctx, "rid")
	req.Name = rid

	if err := roles.Update(ctx, &req.Role); err != nil {
		Error(w, http.StatusBadRequest, err.Error(), h.Logger)
		return
	}

	role, err := roles.Get(ctx, req.Name)
	if err != nil {
		Error(w, http.StatusBadRequest, err.Error(), h.Logger)
		return
	}
	rr := newRoleResponse(srcID, role)
	w.Header().Add("Location", rr.Links.Self)
	encodeJSON(w, http.StatusOK, rr, h.Logger)
}

// RoleID retrieves a role with ID from store.
func (h *Service) RoleID(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	srcID, ts, err := h.sourcesSeries(ctx, w, r)
	if err != nil {
		return
	}

	roles, ok := h.hasRoles(ctx, ts)
	if !ok {
		Error(w, http.StatusNotFound, fmt.Sprintf("Source %d does not have role capability", srcID), h.Logger)
		return
	}

	rid := httprouter.GetParamFromContext(ctx, "rid")
	role, err := roles.Get(ctx, rid)
	if err != nil {
		Error(w, http.StatusBadRequest, err.Error(), h.Logger)
		return
	}
	rr := newRoleResponse(srcID, role)
	encodeJSON(w, http.StatusOK, rr, h.Logger)
}

// Roles retrieves all roles from the store
func (h *Service) Roles(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	srcID, ts, err := h.sourcesSeries(ctx, w, r)
	if err != nil {
		return
	}

	store, ok := h.hasRoles(ctx, ts)
	if !ok {
		Error(w, http.StatusNotFound, fmt.Sprintf("Source %d does not have role capability", srcID), h.Logger)
		return
	}

	roles, err := store.All(ctx)
	if err != nil {
		Error(w, http.StatusBadRequest, err.Error(), h.Logger)
		return
	}

	rr := make([]roleResponse, len(roles))
	for i, role := range roles {
		rr[i] = newRoleResponse(srcID, &role)
	}

	res := struct {
		Roles []roleResponse `json:"roles"`
	}{rr}
	encodeJSON(w, http.StatusOK, res, h.Logger)
}

// RemoveRole removes role from data source.
func (h *Service) RemoveRole(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	srcID, ts, err := h.sourcesSeries(ctx, w, r)
	if err != nil {
		return
	}

	roles, ok := h.hasRoles(ctx, ts)
	if !ok {
		Error(w, http.StatusNotFound, fmt.Sprintf("Source %d does not have role capability", srcID), h.Logger)
		return
	}

	rid := httprouter.GetParamFromContext(ctx, "rid")
	if err := roles.Delete(ctx, &chronograf.Role{Name: rid}); err != nil {
		Error(w, http.StatusBadRequest, err.Error(), h.Logger)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
