package http

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"go.uber.org/zap"

	"github.com/influxdata/platform"
	"github.com/influxdata/platform/kit/errors"
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

	// TODO(desa): Remove this when we get the all clear
	h.HandlerFunc("POST", "/influx/v1/authorizations", h.handlePostAuthorization)
	h.HandlerFunc("GET", "/influx/v1/authorizations", h.handleGetAuthorizations)
	return h
}

// handlePostAuthorization is the HTTP handler for the POST /v1/authorizations route.
func (h *AuthorizationHandler) handlePostAuthorization(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	req, err := decodePostAuthorizationRequest(ctx, r)
	if err != nil {
		h.Logger.Info("failed to decode request", zap.String("handler", "postAuthorization"), zap.Error(err))
		errors.EncodeHTTP(ctx, err, w)
		return
	}

	// TODO: Need to do some validation of req.Authorization.Permissions

	if err := h.AuthorizationService.CreateAuthorization(ctx, req.Authorization); err != nil {
		// Don't log here, it should already be handled by the service
		errors.EncodeHTTP(ctx, err, w)
		return
	}

	if err := encodeResponse(ctx, w, http.StatusCreated, req.Authorization); err != nil {
		h.Logger.Info("failed to encode response", zap.String("handler", "postAuthorization"), zap.Error(err))
		errors.EncodeHTTP(ctx, err, w)
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
		errors.EncodeHTTP(ctx, err, w)
		return
	}

	as, _, err := h.AuthorizationService.FindAuthorizations(ctx, req.filter)
	if err != nil {
		// Don't log here, it should already be handled by the service
		errors.EncodeHTTP(ctx, err, w)
		return
	}

	if err := encodeResponse(ctx, w, http.StatusOK, as); err != nil {
		h.Logger.Info("failed to encode response", zap.String("handler", "getAuthorizations"), zap.Error(err))
		errors.EncodeHTTP(ctx, err, w)
		return
	}
}

type getAuthorizationsRequest struct {
	filter platform.AuthorizationFilter
}

func decodeGetAuthorizationsRequest(ctx context.Context, r *http.Request) (*getAuthorizationsRequest, error) {
	qp := r.URL.Query()
	userID := qp.Get("userID")

	req := &getAuthorizationsRequest{}

	if userID != "" {
		var id platform.ID
		if err := (&id).Decode([]byte(userID)); err != nil {
			return nil, err
		}

		req.filter.UserID = &id
	}

	return req, nil
}

// AuthorizationService connects to Influx via HTTP using tokens to manage authorizations
type AuthorizationService struct {
	Addr               string
	Token              string
	InsecureSkipVerify bool
}

var _ platform.AuthorizationService = (*AuthorizationService)(nil)

// FindAuthorizationByToken returns a single authorization by Token.
func (s *AuthorizationService) FindAuthorizationByToken(ctx context.Context, token string) (*platform.Authorization, error) {
	u, err := newURL(s.Addr, authorizationTokenPath(token))
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

	defer resp.Body.Close()
	dec := json.NewDecoder(resp.Body)
	if resp.StatusCode != http.StatusNoContent {
		var reqErr errors.Error
		if err := dec.Decode(&reqErr); err != nil {
			return nil, err
		}
		return nil, reqErr
	}

	var b platform.Authorization
	if err := dec.Decode(&b); err != nil {
		return nil, err
	}

	return &b, nil
}

// FindAuthorization returns the first authorization that matches filter.
func (s *AuthorizationService) FindAuthorization(ctx context.Context, filter platform.AuthorizationFilter) (*platform.Authorization, error) {
	authorizations, n, err := s.FindAuthorizations(ctx, filter)
	if err != nil {
		return nil, err
	}

	if n == 0 {
		return nil, fmt.Errorf("found no matching authorization")
	}

	return authorizations[0], nil
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

	req.URL.RawQuery = query.Encode()
	req.Header.Set("Authorization", s.Token)

	hc := newClient(u.Scheme, s.InsecureSkipVerify)
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

	var bs []*platform.Authorization
	if err := dec.Decode(&bs); err != nil {
		return nil, 0, err
	}

	return bs, len(bs), nil
}

const (
	authorizationPath = "/v1/authorizations"
)

// CreateAuthorization creates a new authorization and sets b.ID with the new identifier.
func (s *AuthorizationService) CreateAuthorization(ctx context.Context, b *platform.Authorization) error {
	u, err := newURL(s.Addr, authorizationPath)
	if err != nil {
		return err
	}

	octets, err := json.Marshal(b)
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

	if err := json.NewDecoder(resp.Body).Decode(b); err != nil {
		return err
	}

	return nil
}

// DeleteAuthorization removes a authorization by token.
func (s *AuthorizationService) DeleteAuthorization(ctx context.Context, token string) error {
	u, err := newURL(s.Addr, authorizationTokenPath(token))
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

func authorizationTokenPath(token string) string {
	return authorizationPath + "/" + token
}
