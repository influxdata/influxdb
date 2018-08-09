package http

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"path"

	"github.com/influxdata/platform"
	kerrors "github.com/influxdata/platform/kit/errors"
	"github.com/julienschmidt/httprouter"
)

// UserHandler represents an HTTP API handler for users.
type UserHandler struct {
	*httprouter.Router
	UserService platform.UserService
}

// NewUserHandler returns a new instance of UserHandler.
func NewUserHandler() *UserHandler {
	h := &UserHandler{
		Router: httprouter.New(),
	}

	h.HandlerFunc("POST", "/v2/users", h.handlePostUser)
	h.HandlerFunc("GET", "/v2/users", h.handleGetUsers)
	h.HandlerFunc("GET", "/v2/users/:id", h.handleGetUser)
	h.HandlerFunc("PATCH", "/v2/users/:id", h.handlePatchUser)
	h.HandlerFunc("DELETE", "/v2/users/:id", h.handleDeleteUser)
	return h
}

// handlePostUser is the HTTP handler for the POST /v2/users route.
func (h *UserHandler) handlePostUser(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	req, err := decodePostUserRequest(ctx, r)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	if err := h.UserService.CreateUser(ctx, req.User); err != nil {
		EncodeError(ctx, err, w)
		return
	}

	if err := encodeResponse(ctx, w, http.StatusCreated, req.User); err != nil {
		EncodeError(ctx, err, w)
		return
	}
}

type postUserRequest struct {
	User *platform.User
}

func decodePostUserRequest(ctx context.Context, r *http.Request) (*postUserRequest, error) {
	b := &platform.User{}
	if err := json.NewDecoder(r.Body).Decode(b); err != nil {
		return nil, err
	}

	return &postUserRequest{
		User: b,
	}, nil
}

// handleGetUser is the HTTP handler for the GET /v2/users/:id route.
func (h *UserHandler) handleGetUser(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	req, err := decodeGetUserRequest(ctx, r)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	b, err := h.UserService.FindUserByID(ctx, req.UserID)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	if err := encodeResponse(ctx, w, http.StatusOK, b); err != nil {
		EncodeError(ctx, err, w)
		return
	}
}

type getUserRequest struct {
	UserID platform.ID
}

func decodeGetUserRequest(ctx context.Context, r *http.Request) (*getUserRequest, error) {
	params := httprouter.ParamsFromContext(ctx)
	id := params.ByName("id")
	if id == "" {
		return nil, kerrors.InvalidDataf("url missing id")
	}

	var i platform.ID
	if err := i.DecodeFromString(id); err != nil {
		return nil, err
	}

	req := &getUserRequest{
		UserID: i,
	}

	return req, nil
}

// handleDeleteUser is the HTTP handler for the DELETE /v2/users/:id route.
func (h *UserHandler) handleDeleteUser(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	req, err := decodeDeleteUserRequest(ctx, r)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	if err := h.UserService.DeleteUser(ctx, req.UserID); err != nil {
		EncodeError(ctx, err, w)
		return
	}

	w.WriteHeader(http.StatusAccepted)
}

type deleteUserRequest struct {
	UserID platform.ID
}

func decodeDeleteUserRequest(ctx context.Context, r *http.Request) (*deleteUserRequest, error) {
	params := httprouter.ParamsFromContext(ctx)
	id := params.ByName("id")
	if id == "" {
		return nil, kerrors.InvalidDataf("url missing id")
	}

	var i platform.ID
	if err := i.DecodeFromString(id); err != nil {
		return nil, err
	}

	return &deleteUserRequest{
		UserID: i,
	}, nil
}

type usersResponse struct {
	Links map[string]string `json:"links"`
	Users []*userResponse   `json:"users"`
}

func (us usersResponse) ToPlatform() []*platform.User {
	users := make([]*platform.User, len(us.Users))
	for i := range us.Users {
		users[i] = &us.Users[i].User
	}
	return users
}

func newUsersResponse(users []*platform.User) *usersResponse {
	res := usersResponse{
		Links: map[string]string{
			"self": "/v2/users",
		},
		Users: []*userResponse{},
	}
	for _, user := range users {
		res.Users = append(res.Users, newUserResponse(user))
	}
	return &res
}

type userResponse struct {
	Links map[string]string `json:"links"`
	platform.User
}

func newUserResponse(u *platform.User) *userResponse {
	return &userResponse{
		Links: map[string]string{
			"self": fmt.Sprintf("/v2/users/%s", u.ID),
		},
		User: *u,
	}
}

// handleGetUsers is the HTTP handler for the GET /v2/users route.
func (h *UserHandler) handleGetUsers(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	req, err := decodeGetUsersRequest(ctx, r)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	users, _, err := h.UserService.FindUsers(ctx, req.filter)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	err = encodeResponse(ctx, w, http.StatusOK, newUsersResponse(users))
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}
}

type getUsersRequest struct {
	filter platform.UserFilter
}

func decodeGetUsersRequest(ctx context.Context, r *http.Request) (*getUsersRequest, error) {
	qp := r.URL.Query()
	req := &getUsersRequest{}

	if id := qp.Get("id"); id != "" {
		req.filter.ID = &platform.ID{}
		if err := req.filter.ID.DecodeFromString(id); err != nil {
			return nil, err
		}
	}

	if name := qp.Get("name"); name != "" {
		req.filter.Name = &name
	}

	return req, nil
}

// handlePatchUser is the HTTP handler for the PATCH /v2/users/:id route.
func (h *UserHandler) handlePatchUser(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	req, err := decodePatchUserRequest(ctx, r)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	b, err := h.UserService.UpdateUser(ctx, req.UserID, req.Update)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	if err := encodeResponse(ctx, w, http.StatusOK, b); err != nil {
		EncodeError(ctx, err, w)
		return
	}
}

type patchUserRequest struct {
	Update platform.UserUpdate
	UserID platform.ID
}

func decodePatchUserRequest(ctx context.Context, r *http.Request) (*patchUserRequest, error) {
	params := httprouter.ParamsFromContext(ctx)
	id := params.ByName("id")
	if id == "" {
		return nil, kerrors.InvalidDataf("url missing id")
	}

	var i platform.ID
	if err := i.DecodeFromString(id); err != nil {
		return nil, err
	}

	var upd platform.UserUpdate
	if err := json.NewDecoder(r.Body).Decode(&upd); err != nil {
		return nil, err
	}

	return &patchUserRequest{
		Update: upd,
		UserID: i,
	}, nil
}

// UserService connects to Influx via HTTP using tokens to manage users
type UserService struct {
	Addr               string
	Token              string
	InsecureSkipVerify bool
}

// FindUserByID returns a single user by ID.
func (s *UserService) FindUserByID(ctx context.Context, id platform.ID) (*platform.User, error) {
	url, err := newURL(s.Addr, userIDPath(id))
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest("GET", url.String(), nil)
	if err != nil {
		return nil, err
	}
	SetToken(s.Token, req)

	hc := newClient(url.Scheme, s.InsecureSkipVerify)
	resp, err := hc.Do(req)
	if err != nil {
		return nil, err
	}

	if err := CheckError(resp); err != nil {
		return nil, err
	}

	var b platform.User
	if err := json.NewDecoder(resp.Body).Decode(&b); err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return &b, nil
}

// FindUser returns the first user that matches filter.
func (s *UserService) FindUser(ctx context.Context, filter platform.UserFilter) (*platform.User, error) {
	users, n, err := s.FindUsers(ctx, filter)
	if err != nil {
		return nil, err
	}

	if n == 0 {
		return nil, ErrNotFound
	}

	return users[0], nil
}

// FindUsers returns a list of users that match filter and the total count of matching users.
// Additional options provide pagination & sorting.
func (s *UserService) FindUsers(ctx context.Context, filter platform.UserFilter, opt ...platform.FindOptions) ([]*platform.User, int, error) {
	url, err := newURL(s.Addr, userPath)
	if err != nil {
		return nil, 0, err
	}

	query := url.Query()

	req, err := http.NewRequest("GET", url.String(), nil)
	if err != nil {
		return nil, 0, err
	}
	if filter.ID != nil {
		query.Add("id", filter.ID.String())
	}
	if filter.Name != nil {
		query.Add("name", *filter.Name)
	}

	req.URL.RawQuery = query.Encode()
	SetToken(s.Token, req)

	hc := newClient(url.Scheme, s.InsecureSkipVerify)
	resp, err := hc.Do(req)
	if err != nil {
		return nil, 0, err
	}

	if err := CheckError(resp); err != nil {
		return nil, 0, err
	}

	var r usersResponse
	if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
		return nil, 0, err
	}

	us := r.ToPlatform()
	return us, len(us), nil
}

const (
	userPath = "/v2/users"
)

// CreateUser creates a new user and sets u.ID with the new identifier.
func (s *UserService) CreateUser(ctx context.Context, u *platform.User) error {
	url, err := newURL(s.Addr, userPath)
	if err != nil {
		return err
	}

	octets, err := json.Marshal(u)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("POST", url.String(), bytes.NewReader(octets))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")
	SetToken(s.Token, req)

	hc := newClient(url.Scheme, s.InsecureSkipVerify)

	resp, err := hc.Do(req)
	if err != nil {
		return err
	}

	// TODO(jsternberg): Should this check for a 201 explicitly?
	if err := CheckError(resp); err != nil {
		return err
	}

	if err := json.NewDecoder(resp.Body).Decode(u); err != nil {
		return err
	}

	return nil
}

// UpdateUser updates a single user with changeset.
// Returns the new user state after update.
func (s *UserService) UpdateUser(ctx context.Context, id platform.ID, upd platform.UserUpdate) (*platform.User, error) {
	url, err := newURL(s.Addr, userIDPath(id))
	if err != nil {
		return nil, err
	}

	octets, err := json.Marshal(upd)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest("PATCH", url.String(), bytes.NewReader(octets))
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/json")
	SetToken(s.Token, req)

	hc := newClient(url.Scheme, s.InsecureSkipVerify)

	resp, err := hc.Do(req)
	if err != nil {
		return nil, err
	}

	if err := CheckError(resp); err != nil {
		return nil, err
	}

	var u platform.User
	if err := json.NewDecoder(resp.Body).Decode(&u); err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return &u, nil
}

// DeleteUser removes a user by ID.
func (s *UserService) DeleteUser(ctx context.Context, id platform.ID) error {
	url, err := newURL(s.Addr, userIDPath(id))
	if err != nil {
		return err
	}

	req, err := http.NewRequest("DELETE", url.String(), nil)
	if err != nil {
		return err
	}
	SetToken(s.Token, req)

	hc := newClient(url.Scheme, s.InsecureSkipVerify)
	resp, err := hc.Do(req)
	if err != nil {
		return err
	}
	return CheckError(resp)
}

func userIDPath(id platform.ID) string {
	return path.Join(userPath, id.String())
}
