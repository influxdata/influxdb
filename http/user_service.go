package http

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/influxdata/platform"
	"github.com/influxdata/platform/kit/errors"
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

	h.HandlerFunc("POST", "/v1/users", h.handlePostUser)
	h.HandlerFunc("GET", "/v1/users", h.handleGetUsers)
	h.HandlerFunc("GET", "/v1/users/:id", h.handleGetUser)
	h.HandlerFunc("PATCH", "/v1/users/:id", h.handlePatchUser)
	return h
}

// handlePostUser is the HTTP handler for the POST /v1/users route.
func (h *UserHandler) handlePostUser(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	req, err := decodePostUserRequest(ctx, r)
	if err != nil {
		errors.EncodeHTTP(ctx, err, w)
		return
	}

	if err := h.UserService.CreateUser(ctx, req.User); err != nil {
		errors.EncodeHTTP(ctx, err, w)
		return
	}

	if err := encodeResponse(ctx, w, http.StatusCreated, req.User); err != nil {
		errors.EncodeHTTP(ctx, err, w)
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

// handleGetUser is the HTTP handler for the GET /v1/users/:id route.
func (h *UserHandler) handleGetUser(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	req, err := decodeGetUserRequest(ctx, r)
	if err != nil {
		errors.EncodeHTTP(ctx, err, w)
		return
	}

	b, err := h.UserService.FindUserByID(ctx, req.UserID)
	if err != nil {
		errors.EncodeHTTP(ctx, err, w)
		return
	}

	if err := encodeResponse(ctx, w, http.StatusOK, b); err != nil {
		errors.EncodeHTTP(ctx, err, w)
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
		return nil, errors.InvalidDataf("url missing id")
	}

	var i platform.ID
	if err := (&i).Decode([]byte(id)); err != nil {
		return nil, err
	}

	req := &getUserRequest{
		UserID: i,
	}

	return req, nil
}

// handleGetUsers is the HTTP handler for the GET /v1/users route.
func (h *UserHandler) handleGetUsers(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	f := platform.UserFilter{}
	users, _, err := h.UserService.FindUsers(ctx, f)
	if err != nil {
		errors.EncodeHTTP(ctx, err, w)
		return
	}

	if err := encodeResponse(ctx, w, http.StatusOK, users); err != nil {
		errors.EncodeHTTP(ctx, err, w)
		return
	}
}

// handlePatchUser is the HTTP handler for the PATH /v1/users route.
func (h *UserHandler) handlePatchUser(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	req, err := decodePatchUserRequest(ctx, r)
	if err != nil {
		errors.EncodeHTTP(ctx, err, w)
		return
	}

	b, err := h.UserService.UpdateUser(ctx, req.UserID, req.Update)
	if err != nil {
		errors.EncodeHTTP(ctx, err, w)
		return
	}

	if err := encodeResponse(ctx, w, http.StatusOK, b); err != nil {
		errors.EncodeHTTP(ctx, err, w)
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
		return nil, errors.InvalidDataf("url missing id")
	}

	var i platform.ID
	if err := (&i).Decode([]byte(id)); err != nil {
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
	req.Header.Set("Authorization", s.Token)

	hc := newClient(url.Scheme, s.InsecureSkipVerify)
	resp, err := hc.Do(req)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()
	dec := json.NewDecoder(resp.Body)
	if resp.StatusCode != http.StatusNoContent {
		var reqErr errors.Error
		if err := dec.Decode(&reqErr); err != nil {
			return nil, err
		}
		return nil, reqErr
	}

	var b platform.User
	if err := dec.Decode(&b); err != nil {
		return nil, err
	}

	return &b, nil
}

// FindUser returns the first user that matches filter.
func (s *UserService) FindUser(ctx context.Context, filter platform.UserFilter) (*platform.User, error) {
	users, n, err := s.FindUsers(ctx, filter)
	if err != nil {
		return nil, err
	}

	if n == 0 {
		return nil, fmt.Errorf("found no matching user")
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

	req.URL.RawQuery = query.Encode()
	req.Header.Set("Authorization", s.Token)

	hc := newClient(url.Scheme, s.InsecureSkipVerify)
	resp, err := hc.Do(req)
	if err != nil {
		return nil, 0, err
	}

	defer resp.Body.Close()
	dec := json.NewDecoder(resp.Body)
	if resp.StatusCode != http.StatusOK {
		var reqErr errors.Error
		if err := dec.Decode(&reqErr); err != nil {
			return nil, 0, err
		}
		return nil, 0, reqErr
	}

	var bs []*platform.User
	if err := dec.Decode(&bs); err != nil {
		return nil, 0, err
	}

	return bs, len(bs), nil
}

const (
	userPath = "/v1/users"
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
	req.Header.Set("Authorization", s.Token)

	hc := newClient(url.Scheme, s.InsecureSkipVerify)

	resp, err := hc.Do(req)
	if err != nil {
		return err
	}

	// TODO: this should really check the error from the headers
	if resp.StatusCode != http.StatusCreated {
		defer resp.Body.Close()
		dec := json.NewDecoder(resp.Body)
		var reqErr errors.Error
		if err := dec.Decode(&reqErr); err != nil {
			return err
		}
		return reqErr
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
	req.Header.Set("Authorization", s.Token)

	hc := newClient(url.Scheme, s.InsecureSkipVerify)

	resp, err := hc.Do(req)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()
	dec := json.NewDecoder(resp.Body)

	if resp.StatusCode != http.StatusCreated {
		var reqErr errors.Error
		if err := dec.Decode(&reqErr); err != nil {
			return nil, err
		}
		return nil, reqErr
	}

	var u platform.User
	if err := dec.Decode(&u); err != nil {
		return nil, err
	}

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
	req.Header.Set("Authorization", s.Token)

	hc := newClient(url.Scheme, s.InsecureSkipVerify)
	resp, err := hc.Do(req)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusNoContent {
		defer resp.Body.Close()
		dec := json.NewDecoder(resp.Body)
		var reqErr errors.Error
		if err := dec.Decode(&reqErr); err != nil {
			return err
		}
		return reqErr
	}

	return nil
}

func userIDPath(id platform.ID) string {
	return userPath + "/" + id.String()
}
