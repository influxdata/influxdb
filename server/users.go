package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strconv"

	"github.com/bouk/httprouter"
	"github.com/influxdata/chronograf"
	"github.com/influxdata/chronograf/roles"
)

type userRequest struct {
	ID         uint64            `json:"id,string"`
	Name       string            `json:"name"`
	Provider   string            `json:"provider"`
	Scheme     string            `json:"scheme"`
	SuperAdmin bool              `json:"superAdmin"`
	Roles      []chronograf.Role `json:"roles"`
}

func (r *userRequest) ValidCreate() error {
	if r.Name == "" {
		return fmt.Errorf("Name required on Chronograf User request body")
	}
	if r.Provider == "" {
		return fmt.Errorf("Provider required on Chronograf User request body")
	}
	if r.Scheme == "" {
		return fmt.Errorf("Scheme required on Chronograf User request body")
	}

	// TODO: This Scheme value is hard-coded temporarily since we only currently
	// support OAuth2. This hard-coding should be removed whenever we add
	// support for other authentication schemes.
	r.Scheme = "oauth2"
	return r.ValidRoles()
}

func (r *userRequest) ValidUpdate() error {
	if len(r.Roles) == 0 {
		return fmt.Errorf("No Roles to update")
	}
	return r.ValidRoles()
}

func (r *userRequest) ValidRoles() error {
	orgs := map[string]bool{}
	if len(r.Roles) > 0 {
		for _, r := range r.Roles {
			if r.Organization == "" {
				return fmt.Errorf("no organization was provided")
			}
			if _, ok := orgs[r.Organization]; ok {
				return fmt.Errorf("duplicate organization %q in roles", r.Organization)
			}
			orgs[r.Organization] = true
			switch r.Name {
			case roles.MemberRoleName, roles.ViewerRoleName, roles.EditorRoleName, roles.AdminRoleName:
				continue
			default:
				return fmt.Errorf("Unknown role %s. Valid roles are 'member', 'viewer', 'editor', and 'admin'", r.Name)
			}
		}
	}
	return nil
}

type userResponse struct {
	Links      selfLinks         `json:"links"`
	ID         uint64            `json:"id,string"`
	Name       string            `json:"name"`
	Provider   string            `json:"provider"`
	Scheme     string            `json:"scheme"`
	SuperAdmin bool              `json:"superAdmin"`
	Roles      []chronograf.Role `json:"roles"`
}

func newUserResponse(u *chronograf.User) *userResponse {
	// This ensures that any user response with no roles returns an empty array instead of
	// null when marshaled into JSON. That way, JavaScript doesn't need any guard on the
	// key existing and it can simply be iterated over.
	if u.Roles == nil {
		u.Roles = []chronograf.Role{}
	}
	return &userResponse{
		ID:         u.ID,
		Name:       u.Name,
		Provider:   u.Provider,
		Scheme:     u.Scheme,
		Roles:      u.Roles,
		SuperAdmin: u.SuperAdmin,
		Links: selfLinks{
			Self: fmt.Sprintf("/chronograf/v1/users/%d", u.ID),
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
	sort.Slice(usersResp, func(i, j int) bool {
		return usersResp[i].ID < usersResp[j].ID
	})
	return &usersResponse{
		Users: usersResp,
		Links: selfLinks{
			Self: "/chronograf/v1/users",
		},
	}
}

// UserID retrieves a Chronograf user with ID from store
func (s *Service) UserID(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	idStr := httprouter.GetParamFromContext(ctx, "id")
	id, err := strconv.ParseUint(idStr, 10, 64)
	if err != nil {
		Error(w, http.StatusBadRequest, fmt.Sprintf("invalid user id: %s", err.Error()), s.Logger)
		return
	}
	user, err := s.Store.Users(ctx).Get(ctx, chronograf.UserQuery{ID: &id})
	if err != nil {
		Error(w, http.StatusBadRequest, err.Error(), s.Logger)
		return
	}

	res := newUserResponse(user)
	encodeJSON(w, http.StatusOK, res, s.Logger)
}

// NewUser adds a new Chronograf user to store
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

	serverCtx := serverContext(ctx)
	cfg, err := s.Store.Config(serverCtx).Get(serverCtx)
	if err != nil {
		Error(w, http.StatusInternalServerError, err.Error(), s.Logger)
		return
	}

	user := &chronograf.User{
		Name:     req.Name,
		Provider: req.Provider,
		Scheme:   req.Scheme,
		Roles:    req.Roles,
	}

	if cfg.Auth.SuperAdminNewUsers {
		req.SuperAdmin = true
	}

	if err := setSuperAdmin(ctx, req, user); err != nil {
		Error(w, http.StatusUnauthorized, err.Error(), s.Logger)
		return
	}

	res, err := s.Store.Users(ctx).Add(ctx, user)
	if err != nil {
		Error(w, http.StatusBadRequest, err.Error(), s.Logger)
		return
	}

	cu := newUserResponse(res)
	location(w, cu.Links.Self)
	encodeJSON(w, http.StatusCreated, cu, s.Logger)
}

// RemoveUser deletes a Chronograf user from store
func (s *Service) RemoveUser(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	idStr := httprouter.GetParamFromContext(ctx, "id")
	id, err := strconv.ParseUint(idStr, 10, 64)
	if err != nil {
		Error(w, http.StatusBadRequest, fmt.Sprintf("invalid user id: %s", err.Error()), s.Logger)
		return
	}

	u, err := s.Store.Users(ctx).Get(ctx, chronograf.UserQuery{ID: &id})
	if err != nil {
		Error(w, http.StatusNotFound, err.Error(), s.Logger)
		return
	}
	ctxUser, ok := hasUserContext(ctx)
	if !ok {
		Error(w, http.StatusBadRequest, "failed to retrieve user from context", s.Logger)
		return
	}
	if ctxUser.ID == u.ID {
		Error(w, http.StatusForbidden, "user cannot delete themselves", s.Logger)
		return
	}
	if err := s.Store.Users(ctx).Delete(ctx, u); err != nil {
		Error(w, http.StatusBadRequest, err.Error(), s.Logger)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// UpdateUser updates a Chronograf user in store
func (s *Service) UpdateUser(w http.ResponseWriter, r *http.Request) {
	var req userRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		invalidJSON(w, s.Logger)
		return
	}

	ctx := r.Context()
	idStr := httprouter.GetParamFromContext(ctx, "id")
	id, err := strconv.ParseUint(idStr, 10, 64)
	if err != nil {
		Error(w, http.StatusBadRequest, fmt.Sprintf("invalid user id: %s", err.Error()), s.Logger)
		return
	}

	if err := req.ValidUpdate(); err != nil {
		invalidData(w, err, s.Logger)
		return
	}

	u, err := s.Store.Users(ctx).Get(ctx, chronograf.UserQuery{ID: &id})
	if err != nil {
		Error(w, http.StatusNotFound, err.Error(), s.Logger)
		return
	}

	// ValidUpdate should ensure that req.Roles is not nil
	u.Roles = req.Roles

	// If the request contains a name, it must be the same as the
	// one on the user. This is particularly useful to the front-end
	// because they would like to provide the whole user object,
	// including the name, provider, and scheme in update requests.
	// But currently, it is not possible to change name, provider, or
	// scheme via the API.
	if req.Name != "" && req.Name != u.Name {
		err := fmt.Errorf("Cannot update Name")
		invalidData(w, err, s.Logger)
		return
	}
	if req.Provider != "" && req.Provider != u.Provider {
		err := fmt.Errorf("Cannot update Provider")
		invalidData(w, err, s.Logger)
		return
	}
	if req.Scheme != "" && req.Scheme != u.Scheme {
		err := fmt.Errorf("Cannot update Scheme")
		invalidData(w, err, s.Logger)
		return
	}

	// Don't allow SuperAdmins to modify their own SuperAdmin status.
	// Allowing them to do so could result in an application where there
	// are no super admins.
	ctxUser, ok := hasUserContext(ctx)
	if !ok {
		Error(w, http.StatusInternalServerError, "failed to retrieve user from context", s.Logger)
		return
	}
	// If the user being updated is the user making the request and they are
	// changing their SuperAdmin status, return an unauthorized error
	if ctxUser.ID == u.ID && req.SuperAdmin != u.SuperAdmin {
		Error(w, http.StatusUnauthorized, "user cannot modify their own SuperAdmin status", s.Logger)
		return
	}

	if err := setSuperAdmin(ctx, req, u); err != nil {
		Error(w, http.StatusUnauthorized, err.Error(), s.Logger)
		return
	}

	err = s.Store.Users(ctx).Update(ctx, u)
	if err != nil {
		Error(w, http.StatusBadRequest, err.Error(), s.Logger)
		return
	}

	cu := newUserResponse(u)
	location(w, cu.Links.Self)
	encodeJSON(w, http.StatusOK, cu, s.Logger)
}

// Users retrieves all Chronograf users from store
func (s *Service) Users(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	users, err := s.Store.Users(ctx).All(ctx)
	if err != nil {
		Error(w, http.StatusBadRequest, err.Error(), s.Logger)
		return
	}

	res := newUsersResponse(users)
	encodeJSON(w, http.StatusOK, res, s.Logger)
}

func setSuperAdmin(ctx context.Context, req userRequest, user *chronograf.User) error {
	// At a high level, this function checks the following
	//   1. Is the user making the request a SuperAdmin.
	//      If they are, allow them to make whatever changes they please.
	//
	//   2. Is the user making the request trying to change the SuperAdmin
	//      status. If so, return an error.
	//
	//   3. If none of the above are the case, let the user make whichever
	//      changes were requested.

	// Only allow users to set SuperAdmin if they have the superadmin context
	// TODO(desa): Refactor this https://github.com/influxdata/chronograf/issues/2207
	if isSuperAdmin := hasSuperAdminContext(ctx); isSuperAdmin {
		user.SuperAdmin = req.SuperAdmin
	} else if !isSuperAdmin && (user.SuperAdmin != req.SuperAdmin) {
		// If req.SuperAdmin has been set, and the request was not made with the SuperAdmin
		// context, return error
		return fmt.Errorf("User does not have authorization required to set SuperAdmin status. See https://github.com/influxdata/chronograf/issues/2601 for more information.")
	}

	return nil
}
