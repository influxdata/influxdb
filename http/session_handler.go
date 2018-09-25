package http

import (
	"context"
	"fmt"
	"net/http"

	"github.com/influxdata/platform"
	"github.com/julienschmidt/httprouter"
	"go.uber.org/zap"
)

// BasicAuthHandler represents an HTTP API handler for authorizations.
type BasicAuthHandler struct {
	*httprouter.Router
	Logger *zap.Logger

	BasicAuthService platform.BasicAuthService
	SessionService   platform.SessionService
}

// NewBasicAuthHandler returns a new instance of BasicAuthHandler.
func NewBasicAuthHandler() *BasicAuthHandler {
	h := &BasicAuthHandler{
		Router: httprouter.New(),
	}

	h.HandlerFunc("POST", "/signin", h.handleSignin)
	h.HandlerFunc("POST", "/signout", h.handleSignout)
	return h
}

// handleSignin is the HTTP handler for the POST /signin route.
func (h *BasicAuthHandler) handleSignin(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	req, err := decodeSigninRequest(ctx, r)
	if err != nil {
		h.Logger.Info("failed to decode request", zap.String("handler", "basicAuth"), zap.Error(err))
		EncodeError(ctx, err, w)
		return
	}

	if err := h.BasicAuthService.ComparePassword(ctx, req.Username, req.Password); err != nil {
		// Don't log here, it should already be handled by the service
		EncodeError(ctx, err, w)
		return
	}

	s, err := h.SessionService.CreateSession(ctx, req.Username)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	encodeCookieSession(w, s)
	w.WriteHeader(http.StatusNoContent)
}

type signinRequest struct {
	Username string
	Password string
}

func decodeSigninRequest(ctx context.Context, r *http.Request) (*signinRequest, error) {
	u, p, ok := r.BasicAuth()
	if !ok {
		return nil, fmt.Errorf("invalid basic auth")
	}

	return &signinRequest{
		Username: u,
		Password: p,
	}, nil
}

// handleSignout is the HTTP handler for the POST /signout route.
func (h *BasicAuthHandler) handleSignout(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	req, err := decodeSignoutRequest(ctx, r)
	if err != nil {
		h.Logger.Info("failed to decode request", zap.String("handler", "basicAuth"), zap.Error(err))
		EncodeError(ctx, err, w)
		return
	}

	if err := h.SessionService.ExpireSession(ctx, req.Key); err != nil {
		EncodeError(ctx, err, w)
		return
	}

	// TODO(desa): not sure what to do here maybe redirect?
	w.WriteHeader(http.StatusNoContent)
}

type signoutRequest struct {
	Key string
}

func decodeSignoutRequest(ctx context.Context, r *http.Request) (*signoutRequest, error) {
	key, err := decodeCookieSession(ctx, r)
	if err != nil {
		return nil, err
	}
	return &signoutRequest{
		Key: key,
	}, nil
}

const cookieSessionName = "session"

func encodeCookieSession(w http.ResponseWriter, s *platform.Session) {
	c := &http.Cookie{
		Name:  cookieSessionName,
		Value: s.Key,
	}

	http.SetCookie(w, c)
}
func decodeCookieSession(ctx context.Context, r *http.Request) (string, error) {
	c, err := r.Cookie(cookieSessionName)
	if err != nil {
		return "", err
	}
	return c.Value, nil
}
