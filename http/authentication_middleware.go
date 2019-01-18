package http

import (
	"context"
	"fmt"
	"net/http"
	"time"

	platform "github.com/influxdata/influxdb"
	platcontext "github.com/influxdata/influxdb/context"
	"github.com/julienschmidt/httprouter"
	"go.uber.org/zap"
)

// AuthenticationHandler is a middleware for authenticating incoming requests.
type AuthenticationHandler struct {
	Logger *zap.Logger

	AuthorizationService platform.AuthorizationService
	SessionService       platform.SessionService

	// This is only really used for it's lookup method the specific http
	// hanlder used to register routes does not matter.
	noAuthRouter *httprouter.Router

	Handler http.Handler
}

// NewAuthenticationHandler creates an authentication handler.
func NewAuthenticationHandler() *AuthenticationHandler {
	return &AuthenticationHandler{
		Logger:       zap.NewNop(),
		Handler:      http.DefaultServeMux,
		noAuthRouter: httprouter.New(),
	}
}

// RegisterNoAuthRoute excludes routes from needing authentication.
func (h *AuthenticationHandler) RegisterNoAuthRoute(method, path string) {
	// the handler specified here does not matter.
	h.noAuthRouter.HandlerFunc(method, path, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
}

const (
	tokenAuthScheme   = "token"
	sessionAuthScheme = "session"
)

// ProbeAuthScheme probes the http request for the requests for token or cookie session.
func ProbeAuthScheme(r *http.Request) (string, error) {
	_, tokenErr := GetToken(r)
	_, sessErr := decodeCookieSession(r.Context(), r)

	if tokenErr != nil && sessErr != nil {
		return "", fmt.Errorf("token required")
	}

	if tokenErr == nil {
		return tokenAuthScheme, nil
	}

	return sessionAuthScheme, nil
}

// ServeHTTP extracts the session or token from the http request and places the resulting authorizer on the request context.
func (h *AuthenticationHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if handler, _, _ := h.noAuthRouter.Lookup(r.Method, r.URL.Path); handler != nil {
		h.Handler.ServeHTTP(w, r)
		return
	}

	ctx := r.Context()
	scheme, err := ProbeAuthScheme(r)
	if err != nil {
		UnauthorizedError(ctx, w)
		return
	}

	switch scheme {
	case tokenAuthScheme:
		ctx, err = h.extractAuthorization(ctx, r)
		if err != nil {
			break
		}
		r = r.WithContext(ctx)
		h.Handler.ServeHTTP(w, r)
		return
	case sessionAuthScheme:
		ctx, err = h.extractSession(ctx, r)
		if err != nil {
			break
		}
		r = r.WithContext(ctx)
		h.Handler.ServeHTTP(w, r)
		return
	}

	UnauthorizedError(ctx, w)
}

func (h *AuthenticationHandler) extractAuthorization(ctx context.Context, r *http.Request) (context.Context, error) {
	t, err := GetToken(r)
	if err != nil {
		return ctx, err
	}

	a, err := h.AuthorizationService.FindAuthorizationByToken(ctx, t)
	if err != nil {
		return ctx, err
	}

	return platcontext.SetAuthorizer(ctx, a), nil
}

func (h *AuthenticationHandler) extractSession(ctx context.Context, r *http.Request) (context.Context, error) {
	k, err := decodeCookieSession(ctx, r)
	if err != nil {
		return ctx, err
	}

	s, e := h.SessionService.FindSession(ctx, k)
	if e != nil {
		return ctx, e
	}

	// if the session is not expired, renew the session
	e = h.SessionService.RenewSession(ctx, s, time.Now().Add(platform.RenewSessionTime))
	if e != nil {
		return ctx, e
	}

	return platcontext.SetAuthorizer(ctx, s), nil
}
