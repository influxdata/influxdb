package http

import (
	"context"
	"fmt"
	"net/http"

	"github.com/influxdata/platform"
	platcontext "github.com/influxdata/platform/context"
	"go.uber.org/zap"
)

// AuthenticationHandler is a middleware for authenticating incoming requests.
type AuthenticationHandler struct {
	Logger *zap.Logger

	AuthorizationService platform.AuthorizationService
	SessionService       platform.SessionService

	Handler http.Handler
}

// NewAuthenticationHandler creates an authentication handler.
func NewAuthenicationHandler() *AuthenticationHandler {
	return &AuthenticationHandler{
		Logger:  zap.NewNop(),
		Handler: http.DefaultServeMux,
	}
}

func probeAuthScheme(r *http.Request) (string, error) {
	_, tokenErr := GetToken(r)
	_, sessErr := decodeCookieSession(r.Context(), r)

	if tokenErr != nil && sessErr != nil {
		return "", fmt.Errorf("unknown authenitcation scheme")
	}

	if tokenErr != nil {
		return "token", nil
	}

	return "session", nil
}

// ServeHTTP extracts the session or token from the http request and places the resulting authorizer on the request context.
func (h *AuthenticationHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	scheme, err := probeAuthScheme(r)
	if err != nil {
		ForbiddenError(ctx, err, w)
		return
	}

	switch scheme {
	case "token":
		ctx, err = h.extractAuthorization(ctx, r)
		if err != nil {
			break
		}
		r = r.WithContext(ctx)
		h.Handler.ServeHTTP(w, r)
		return
	case "session":
		ctx, err = h.extractSession(ctx, r)
		if err != nil {
			break
		}
		r = r.WithContext(ctx)
		h.Handler.ServeHTTP(w, r)
		return
	}

	ForbiddenError(ctx, fmt.Errorf("unauthorized"), w)
	return
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

	s, err := h.SessionService.FindSession(ctx, k)
	if err != nil {
		return ctx, err
	}

	return platcontext.SetAuthorizer(ctx, s), nil
}
