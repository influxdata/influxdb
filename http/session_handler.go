package http

import (
	"context"
	"fmt"
	"net/http"

	"github.com/influxdata/platform"
	"github.com/julienschmidt/httprouter"
	"go.uber.org/zap"
)

// SessionHandler represents an HTTP API handler for authorizations.
type SessionHandler struct {
	*httprouter.Router
	Logger *zap.Logger

	BasicAuthService platform.BasicAuthService
	SessionService   platform.SessionService
}

// NewSessionHandler returns a new instance of SessionHandler.
func NewSessionHandler() *SessionHandler {
	h := &SessionHandler{
		Router: httprouter.New(),
	}

	h.HandlerFunc("POST", "/api/v2/signin", h.handleSignin)
	h.HandlerFunc("POST", "/api/v2/signout", h.handleSignout)
	return h
}

// handleSignin is the HTTP handler for the POST /signin route.
func (h *SessionHandler) handleSignin(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	req, err := decodeSigninRequest(ctx, r)
	if err != nil {
		h.Logger.Info("failed to decode request", zap.Error(err))
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
func (h *SessionHandler) handleSignout(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	req, err := decodeSignoutRequest(ctx, r)
	if err != nil {
		h.Logger.Info("failed to decode request", zap.Error(err))
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

// SetCookieSession adds a cookie for the session to an http request
func SetCookieSession(key string, r *http.Request) {
	c := &http.Cookie{
		Name:  cookieSessionName,
		Value: key,
	}

	r.AddCookie(c)
}
