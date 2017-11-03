package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strconv"

	"github.com/bouk/httprouter"
	"github.com/influxdata/chronograf"
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
	if r.Name != "" {
		return fmt.Errorf("Cannot update Name")
	}
	if r.Provider != "" {
		return fmt.Errorf("Cannot update Provider")
	}
	if r.Scheme != "" {
		return fmt.Errorf("Cannot update Scheme")
	}
	if len(r.Roles) == 0 {
		return fmt.Errorf("No Roles to update")
	}
	return r.ValidRoles()
}

func (r *userRequest) ValidRoles() error {
	orgs := map[string]bool{}
	if len(r.Roles) > 0 {
		for _, r := range r.Roles {
			if _, ok := orgs[r.Organization]; ok {
				return fmt.Errorf("duplicate organization %q in roles", r.Organization)
			}
			orgs[r.Organization] = true
			switch r.Name {
			case MemberRoleName, ViewerRoleName, EditorRoleName, AdminRoleName:
				continue
			default:
				return fmt.Errorf("Unknown role %s. Valid roles are 'viewer', 'editor', 'admin', and 'superadmin'", r.Name)
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

// Chronograf User Roles
const (
	MemberRoleName     = "member"
	ViewerRoleName     = "viewer"
	EditorRoleName     = "editor"
	AdminRoleName      = "admin"
	SuperAdminRoleName = "superadmin"
)

var (
	// MemberRole is the role for a user who can only perform No operations.
	MemberRole = chronograf.Role{
		Name: MemberRoleName,
	}

	// ViewerRole is the role for a user who can only perform READ operations on Dashboards, Rules, and Sources
	ViewerRole = chronograf.Role{
		Name: ViewerRoleName,
	}

	// EditorRole is the role for a user who can perform READ and WRITE operations on Dashboards, Rules, and Sources
	EditorRole = chronograf.Role{
		Name: EditorRoleName,
	}

	// AdminRole is the role for a user who can perform READ and WRITE operations on Dashboards, Rules, Sources, and Users
	AdminRole = chronograf.Role{
		Name: AdminRoleName,
	}
)

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
	user := &chronograf.User{
		Name:     req.Name,
		Provider: req.Provider,
		Scheme:   req.Scheme,
		Roles:    req.Roles,
	}

	// If req.SuperAdmin has been set, verify that it was done with the superadmin context.
	// Even though req.SuperAdmin == true is logically equivalent to req.SuperAdmin it is
	// more clear that this code should only be ran in the case that a user is trying to
	// set the SuperAdmin field.
	if req.SuperAdmin == true {
		// Only allow users to set SuperAdmin if they have the superadmin context
		if isSuperAdmin := hasSuperAdminContext(ctx); !isSuperAdmin {
			Error(w, http.StatusBadRequest, "Cannot set SuperAdmin", s.Logger)
			return
		}
		user.SuperAdmin = true
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

	if err := req.ValidUpdate(); err != nil {
		invalidData(w, err, s.Logger)
		return
	}

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

	// ValidUpdate should ensure that req.Roles is not nil
	u.Roles = req.Roles

	// If req.SuperAdmin has been set, verify that it was done with the superadmin context.
	// Even though req.SuperAdmin == true is logically equivalent to req.SuperAdmin it is
	// more clear that this code should only be ran in the case that a user is trying to
	// set the SuperAdmin field.
	if req.SuperAdmin == true {
		// Only allow users to set SuperAdmin if they have the superadmin context
		if isSuperAdmin := hasSuperAdminContext(ctx); !isSuperAdmin {
			Error(w, http.StatusBadRequest, "Cannot set SuperAdmin", s.Logger)
			return
		}
		u.SuperAdmin = true
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
