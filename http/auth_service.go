package http

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"path"

	"go.uber.org/zap"

	"github.com/influxdata/platform"
	kerrors "github.com/influxdata/platform/kit/errors"
	"github.com/julienschmidt/httprouter"
)

// AuthorizationHandler represents an HTTP API handler for authorizations.
type AuthorizationHandler struct {
	*httprouter.Router
	Logger *zap.Logger

	AuthorizationService platform.AuthorizationService
}

// NewAuthorizationHandler returns a new instance of AuthorizationHandler.
func NewAuthorizationHandler() *AuthorizationHandler {
	h := &AuthorizationHandler{
		Router: httprouter.New(),
	}

	h.HandlerFunc("POST", "/v1/authorizations", h.handlePostAuthorization)
	h.HandlerFunc("GET", "/v1/authorizations", h.handleGetAuthorizations)
	h.HandlerFunc("GET", "/v1/authorizations/:id", h.handleGetAuthorization)
	h.HandlerFunc("DELETE", "/v1/authorizations/:id", h.handleDeleteAuthorization)
	return h
}

// handlePostAuthorization is the HTTP handler for the POST /v1/authorizations route.
func (h *AuthorizationHandler) handlePostAuthorization(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	req, err := decodePostAuthorizationRequest(ctx, r)
	if err != nil {
		h.Logger.Info("failed to decode request", zap.String("handler", "postAuthorization"), zap.Error(err))
		EncodeError(ctx, err, w)
		return
	}

	// TODO: Need to do some validation of req.Authorization.Permissions

	if err := h.AuthorizationService.CreateAuthorization(ctx, req.Authorization); err != nil {
		// Don't log here, it should already be handled by the service
		EncodeError(ctx, err, w)
		return
	}

	if err := encodeResponse(ctx, w, http.StatusCreated, req.Authorization); err != nil {
		h.Logger.Info("failed to encode response", zap.String("handler", "postAuthorization"), zap.Error(err))
		EncodeError(ctx, err, w)
		return
	}
}

type postAuthorizationRequest struct {
	Authorization *platform.Authorization
}

func decodePostAuthorizationRequest(ctx context.Context, r *http.Request) (*postAuthorizationRequest, error) {
	a := &platform.Authorization{}
	if err := json.NewDecoder(r.Body).Decode(a); err != nil {
		return nil, err
	}

	return &postAuthorizationRequest{
		Authorization: a,
	}, nil
}

// handleGetAuthorizations is the HTTP handler for the GET /v1/authorizations route.
func (h *AuthorizationHandler) handleGetAuthorizations(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	req, err := decodeGetAuthorizationsRequest(ctx, r)
	if err != nil {
		h.Logger.Info("failed to decode request", zap.String("handler", "getAuthorizations"), zap.Error(err))
		EncodeError(ctx, err, w)
		return
	}

	as, _, err := h.AuthorizationService.FindAuthorizations(ctx, req.filter)
	if err != nil {
		// Don't log here, it should already be handled by the service
		EncodeError(ctx, err, w)
		return
	}

	if err := encodeResponse(ctx, w, http.StatusOK, as); err != nil {
		h.Logger.Info("failed to encode response", zap.String("handler", "getAuthorizations"), zap.Error(err))
		EncodeError(ctx, err, w)
		return
	}
}

type getAuthorizationsRequest struct {
	filter platform.AuthorizationFilter
}

func decodeGetAuthorizationsRequest(ctx context.Context, r *http.Request) (*getAuthorizationsRequest, error) {
	qp := r.URL.Query()

	req := &getAuthorizationsRequest{}

	userID := qp.Get("userID")
	if userID != "" {
		req.filter.UserID = &platform.ID{}
		if err := req.filter.UserID.DecodeFromString(userID); err != nil {
			return nil, err
		}
	}

	user := qp.Get("user")
	if user != "" {
		req.filter.User = &user
	}

	authID := qp.Get("id")
	if authID != "" {
		req.filter.ID = &platform.ID{}
		if err := req.filter.ID.DecodeFromString(authID); err != nil {
			return nil, err
		}
	}

	return req, nil
}

// handleGetAuthorization is the HTTP handler for the GET /v1/authorizations/:id route.
func (h *AuthorizationHandler) handleGetAuthorization(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	req, err := decodeGetAuthorizationRequest(ctx, r)
	if err != nil {
		h.Logger.Info("failed to decode request", zap.String("handler", "getAuthorization"), zap.Error(err))
		EncodeError(ctx, err, w)
		return
	}

	a, err := h.AuthorizationService.FindAuthorizationByID(ctx, req.ID)
	if err != nil {
		// Don't log here, it should already be handled by the service
		EncodeError(ctx, err, w)
		return
	}

	if err := encodeResponse(ctx, w, http.StatusOK, a); err != nil {
		h.Logger.Info("failed to encode response", zap.String("handler", "getAuthorization"), zap.Error(err))
		EncodeError(ctx, err, w)
		return
	}
}

type getAuthorizationRequest struct {
	ID platform.ID
}

func decodeGetAuthorizationRequest(ctx context.Context, r *http.Request) (*getAuthorizationRequest, error) {
	params := httprouter.ParamsFromContext(ctx)
	id := params.ByName("id")
	if id == "" {
		return nil, kerrors.InvalidDataf("url missing id")
	}

	var i platform.ID
	if err := i.DecodeFromString(id); err != nil {
		return nil, err
	}

	return &getAuthorizationRequest{
		ID: i,
	}, nil
}

// handleDeleteAuthorization is the HTTP handler for the DELETE /v1/authorizations/:id route.
func (h *AuthorizationHandler) handleDeleteAuthorization(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	req, err := decodeDeleteAuthorizationRequest(ctx, r)
	if err != nil {
		h.Logger.Info("failed to decode request", zap.String("handler", "deleteAuthorization"), zap.Error(err))
		EncodeError(ctx, err, w)
		return
	}

	if err := h.AuthorizationService.DeleteAuthorization(ctx, req.ID); err != nil {
		// Don't log here, it should already be handled by the service
		EncodeError(ctx, err, w)
		return
	}

	w.WriteHeader(http.StatusAccepted)
}

type deleteAuthorizationRequest struct {
	ID platform.ID
}

func decodeDeleteAuthorizationRequest(ctx context.Context, r *http.Request) (*deleteAuthorizationRequest, error) {
	params := httprouter.ParamsFromContext(ctx)
	id := params.ByName("id")
	if id == "" {
		return nil, kerrors.InvalidDataf("url missing id")
	}

	var i platform.ID
	if err := i.DecodeFromString(id); err != nil {
		return nil, err
	}

	return &deleteAuthorizationRequest{
		ID: i,
	}, nil
}

// AuthorizationService connects to Influx via HTTP using tokens to manage authorizations
type AuthorizationService struct {
	Addr               string
	Token              string
	InsecureSkipVerify bool
}

var _ platform.AuthorizationService = (*AuthorizationService)(nil)

func (s *AuthorizationService) FindAuthorizationByID(ctx context.Context, id platform.ID) (*platform.Authorization, error) {
	u, err := newURL(s.Addr, authorizationIDPath(id))
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", s.Token)

	hc := newClient(u.Scheme, s.InsecureSkipVerify)
	resp, err := hc.Do(req)
	if err != nil {
		return nil, err
	}

	if err := CheckError(resp); err != nil {
		return nil, err
	}

	var b platform.Authorization
	if err := json.NewDecoder(resp.Body).Decode(&b); err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return &b, nil
}

// FindAuthorizationByToken returns a single authorization by Token.
func (s *AuthorizationService) FindAuthorizationByToken(ctx context.Context, token string) (*platform.Authorization, error) {
	return nil, errors.New("not supported in HTTP authorization service")
}

// FindAuthorizations returns a list of authorizations that match filter and the total count of matching authorizations.
// Additional options provide pagination & sorting.
func (s *AuthorizationService) FindAuthorizations(ctx context.Context, filter platform.AuthorizationFilter, opt ...platform.FindOptions) ([]*platform.Authorization, int, error) {
	u, err := newURL(s.Addr, authorizationPath)
	if err != nil {
		return nil, 0, err
	}

	query := u.Query()

	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return nil, 0, err
	}

	if filter.ID != nil {
		query.Add("id", filter.ID.String())
	}

	if filter.UserID != nil {
		query.Add("userID", filter.UserID.String())
	}

	if filter.User != nil {
		query.Add("user", *filter.User)
	}

	req.URL.RawQuery = query.Encode()
	req.Header.Set("Authorization", s.Token)

	hc := newClient(u.Scheme, s.InsecureSkipVerify)
	resp, err := hc.Do(req)
	if err != nil {
		return nil, 0, err
	}

	if err := CheckError(resp); err != nil {
		return nil, 0, err
	}

	var bs []*platform.Authorization
	if err := json.NewDecoder(resp.Body).Decode(&bs); err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()

	return bs, len(bs), nil
}

const (
	authorizationPath = "/v1/authorizations"
)

// CreateAuthorization creates a new authorization and sets b.ID with the new identifier.
func (s *AuthorizationService) CreateAuthorization(ctx context.Context, a *platform.Authorization) error {
	u, err := newURL(s.Addr, authorizationPath)
	if err != nil {
		return err
	}

	octets, err := json.Marshal(a)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("POST", u.String(), bytes.NewReader(octets))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", s.Token)

	hc := newClient(u.Scheme, s.InsecureSkipVerify)

	resp, err := hc.Do(req)
	if err != nil {
		return err
	}

	// TODO(jsternberg): Should this check for a 201 explicitly?
	if err := CheckError(resp); err != nil {
		return err
	}

	if err := json.NewDecoder(resp.Body).Decode(a); err != nil {
		return err
	}

	return nil
}

// DeleteAuthorization removes a authorization by id.
func (s *AuthorizationService) DeleteAuthorization(ctx context.Context, id platform.ID) error {
	u, err := newURL(s.Addr, authorizationIDPath(id))
	if err != nil {
		return err
	}

	req, err := http.NewRequest("DELETE", u.String(), nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", s.Token)

	hc := newClient(u.Scheme, s.InsecureSkipVerify)
	resp, err := hc.Do(req)
	if err != nil {
		return err
	}
	return CheckError(resp)
}

func authorizationIDPath(id platform.ID) string {
	return path.Join(authorizationPath, id.String())
}
