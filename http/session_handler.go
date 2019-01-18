package http

import (
	"context"
	"net/http"

	platform "github.com/influxdata/influxdb"
	"github.com/julienschmidt/httprouter"
	"go.uber.org/zap"
)

// SessionBackend is all services and associated parameters required to construct
// the SessionHandler.
type SessionBackend struct {
	Logger *zap.Logger

	BasicAuthService platform.BasicAuthService
	SessionService   platform.SessionService
}

func NewSessionBackend(b *APIBackend) *SessionBackend {
	return &SessionBackend{
		Logger: b.Logger.With(zap.String("handler", "session")),

		BasicAuthService: b.BasicAuthService,
		SessionService:   b.SessionService,
	}
}

// SessionHandler represents an HTTP API handler for authorizations.
type SessionHandler struct {
	*httprouter.Router
	Logger *zap.Logger

	BasicAuthService platform.BasicAuthService
	SessionService   platform.SessionService
}

// NewSessionHandler returns a new instance of SessionHandler.
func NewSessionHandler(b *SessionBackend) *SessionHandler {
	h := &SessionHandler{
		Router: NewRouter(),
		Logger: b.Logger,

		BasicAuthService: b.BasicAuthService,
		SessionService:   b.SessionService,
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
		UnauthorizedError(ctx, w)
		return
	}

	if err := h.BasicAuthService.ComparePassword(ctx, req.Username, req.Password); err != nil {
		// Don't log here, it should already be handled by the service
		UnauthorizedError(ctx, w)
		return
	}

	s, e := h.SessionService.CreateSession(ctx, req.Username)
	if e != nil {
		UnauthorizedError(ctx, w)
		return
	}

	encodeCookieSession(w, s)
	w.WriteHeader(http.StatusNoContent)
}

type signinRequest struct {
	Username string
	Password string
}

func decodeSigninRequest(ctx context.Context, r *http.Request) (*signinRequest, *platform.Error) {
	u, p, ok := r.BasicAuth()
	if !ok {
		return nil, &platform.Error{
			Code: platform.EInvalid,
			Msg:  "invalid basic auth",
		}
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
		UnauthorizedError(ctx, w)
		return
	}

	if err := h.SessionService.ExpireSession(ctx, req.Key); err != nil {
		UnauthorizedError(ctx, w)
		return
	}

	// TODO(desa): not sure what to do here maybe redirect?
	w.WriteHeader(http.StatusNoContent)
}

type signoutRequest struct {
	Key string
}

func decodeSignoutRequest(ctx context.Context, r *http.Request) (*signoutRequest, *platform.Error) {
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
func decodeCookieSession(ctx context.Context, r *http.Request) (string, *platform.Error) {
	c, err := r.Cookie(cookieSessionName)
	if err != nil {
		return "", &platform.Error{
			Err:  err,
			Code: platform.EInvalid,
		}
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
