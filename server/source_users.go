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
	srcID, ts, err := h.sourcesSeries(ctx, w, r)
	if err != nil {
		return
	}

	store := ts.Users(ctx)
	user := &chronograf.User{
		Name:        req.Username,
		Passwd:      req.Password,
		Permissions: req.Permissions,
		Roles:       req.Roles,
	}

	res, err := store.Add(ctx, user)
	if err != nil {
		Error(w, http.StatusBadRequest, err.Error(), h.Logger)
		return
	}

	if err != nil {
		Error(w, http.StatusBadRequest, err.Error(), h.Logger)
		return
	}

	su := newSourceUserResponse(srcID, res.Name).WithPermissions(res.Permissions)
	if _, hasRoles := h.hasRoles(ctx, ts); hasRoles {
		su.WithRoles(srcID, res.Roles)
	}
	w.Header().Add("Location", su.Links.Self)
	encodeJSON(w, http.StatusCreated, su, h.Logger)
}

// SourceUsers retrieves all users from source.
func (h *Service) SourceUsers(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	srcID, ts, err := h.sourcesSeries(ctx, w, r)
	if err != nil {
		return
	}

	store := ts.Users(ctx)
	users, err := store.All(ctx)
	if err != nil {
		Error(w, http.StatusBadRequest, err.Error(), h.Logger)
		return
	}

	_, hasRoles := h.hasRoles(ctx, ts)
	ur := make([]sourceUserResponse, len(users))
	for i, u := range users {
		usr := newSourceUserResponse(srcID, u.Name).WithPermissions(u.Permissions)
		if hasRoles {
			usr.WithRoles(srcID, u.Roles)
		}
		ur[i] = *usr
	}

	res := usersResponse{
		Users: ur,
	}

	encodeJSON(w, http.StatusOK, res, h.Logger)
}

// SourceUserID retrieves a user with ID from store.
func (h *Service) SourceUserID(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	uid := httprouter.GetParamFromContext(ctx, "uid")

	srcID, ts, err := h.sourcesSeries(ctx, w, r)
	if err != nil {
		return
	}
	store := ts.Users(ctx)
	u, err := store.Get(ctx, uid)
	if err != nil {
		Error(w, http.StatusBadRequest, err.Error(), h.Logger)
		return
	}

	res := newSourceUserResponse(srcID, u.Name).WithPermissions(u.Permissions)
	if _, hasRoles := h.hasRoles(ctx, ts); hasRoles {
		res.WithRoles(srcID, u.Roles)
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
	srcID, ts, err := h.sourcesSeries(ctx, w, r)
	if err != nil {
		return
	}

	user := &chronograf.User{
		Name:        uid,
		Passwd:      req.Password,
		Permissions: req.Permissions,
		Roles:       req.Roles,
	}
	store := ts.Users(ctx)

	if err := store.Update(ctx, user); err != nil {
		Error(w, http.StatusBadRequest, err.Error(), h.Logger)
		return
	}

	u, err := store.Get(ctx, uid)
	if err != nil {
		Error(w, http.StatusBadRequest, err.Error(), h.Logger)
		return
	}

	res := newSourceUserResponse(srcID, u.Name).WithPermissions(u.Permissions)
	if _, hasRoles := h.hasRoles(ctx, ts); hasRoles {
		res.WithRoles(srcID, u.Roles)
	}
	w.Header().Add("Location", res.Links.Self)
	encodeJSON(w, http.StatusOK, res, h.Logger)
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

type sourceUserRequest struct {
	Username    string                 `json:"name,omitempty"`        // Username for new account
	Password    string                 `json:"password,omitempty"`    // Password for new account
	Permissions chronograf.Permissions `json:"permissions,omitempty"` // Optional permissions
	Roles       []chronograf.Role      `json:"roles,omitempty"`       // Optional roles
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

type usersResponse struct {
	Users []sourceUserResponse `json:"users"`
}

func (r *sourceUserRequest) ValidUpdate() error {
	if r.Password == "" && len(r.Permissions) == 0 && len(r.Roles) == 0 {
		return fmt.Errorf("No fields to update")
	}
	return validPermissions(&r.Permissions)
}

type sourceUserResponse struct {
	Name           string                 // Username for new account
	Permissions    chronograf.Permissions // Account's permissions
	Roles          []roleResponse         // Roles if source uses them
	Links          selfLinks              // Links are URI locations related to user
	hasPermissions bool
	hasRoles       bool
}

func (u *sourceUserResponse) MarshalJSON() ([]byte, error) {
	res := map[string]interface{}{
		"name":  u.Name,
		"links": u.Links,
	}
	if u.hasRoles {
		res["roles"] = u.Roles
	}
	if u.hasPermissions {
		res["permissions"] = u.Permissions
	}
	return json.Marshal(res)
}

// newSourceUserResponse creates an HTTP JSON response for a user w/o roles
func newSourceUserResponse(srcID int, name string) *sourceUserResponse {
	self := newSelfLinks(srcID, "users", name)
	return &sourceUserResponse{
		Name:  name,
		Links: self,
	}
}

func (u *sourceUserResponse) WithPermissions(perms chronograf.Permissions) *sourceUserResponse {
	u.hasPermissions = true
	if perms == nil {
		perms = make(chronograf.Permissions, 0)
	}
	u.Permissions = perms
	return u
}

// WithRoles adds roles to the HTTP JSON response for a user
func (u *sourceUserResponse) WithRoles(srcID int, roles []chronograf.Role) *sourceUserResponse {
	u.hasRoles = true
	rr := make([]roleResponse, len(roles))
	for i, role := range roles {
		rr[i] = newRoleResponse(srcID, &role)
	}
	u.Roles = rr
	return u
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
