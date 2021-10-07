package http

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/influxdata/httprouter"
	platform "github.com/influxdata/influxdb/v2"
	platcontext "github.com/influxdata/influxdb/v2/context"
	"github.com/influxdata/influxdb/v2/jsonweb"
	errors2 "github.com/influxdata/influxdb/v2/kit/platform/errors"
	"github.com/influxdata/influxdb/v2/session"
	"github.com/opentracing/opentracing-go"
	"go.uber.org/zap"
)

// AuthenticationHandler is a middleware for authenticating incoming requests.
type AuthenticationHandler struct {
	errors2.HTTPErrorHandler
	log *zap.Logger

	AuthorizationService platform.AuthorizationService
	SessionService       platform.SessionService
	UserService          platform.UserService
	TokenParser          *jsonweb.TokenParser
	SessionRenewDisabled bool

	// This is only really used for it's lookup method the specific http
	// handler used to register routes does not matter.
	noAuthRouter *httprouter.Router

	Handler http.Handler
}

// NewAuthenticationHandler creates an authentication handler.
func NewAuthenticationHandler(log *zap.Logger, h errors2.HTTPErrorHandler) *AuthenticationHandler {
	return &AuthenticationHandler{
		log:              log,
		HTTPErrorHandler: h,
		Handler:          http.NotFoundHandler(),
		TokenParser:      jsonweb.NewTokenParser(jsonweb.EmptyKeyStore),
		noAuthRouter:     httprouter.New(),
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
	_, sessErr := session.DecodeCookieSession(r.Context(), r)

	if tokenErr != nil && sessErr != nil {
		return "", fmt.Errorf("token required")
	}

	if tokenErr == nil {
		return tokenAuthScheme, nil
	}

	return sessionAuthScheme, nil
}

func (h *AuthenticationHandler) unauthorized(ctx context.Context, w http.ResponseWriter, err error) {
	h.log.Info("Unauthorized", zap.Error(err))
	UnauthorizedError(ctx, h, w)
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
		h.unauthorized(ctx, w, err)
		return
	}

	var auth platform.Authorizer
	switch scheme {
	case tokenAuthScheme:
		auth, err = h.extractAuthorization(ctx, r)
	case sessionAuthScheme:
		auth, err = h.extractSession(ctx, r)
	default:
		// TODO: this error will be nil if it gets here, this should be remedied with some
		//  sentinel error I'm thinking
		err = errors.New("invalid auth scheme")
	}
	if err != nil {
		h.unauthorized(ctx, w, err)
		return
	}

	// jwt based auth is permission based rather than identity based
	// and therefor has no associated user. if the user ID is invalid
	// disregard the user active check
	if auth.GetUserID().Valid() {
		if err = h.isUserActive(ctx, auth); err != nil {
			InactiveUserError(ctx, h, w)
			return
		}
	}

	ctx = platcontext.SetAuthorizer(ctx, auth)

	if span := opentracing.SpanFromContext(ctx); span != nil {
		span.SetTag("user_id", auth.GetUserID().String())
	}

	h.Handler.ServeHTTP(w, r.WithContext(ctx))
}

func (h *AuthenticationHandler) isUserActive(ctx context.Context, auth platform.Authorizer) error {
	u, err := h.UserService.FindUserByID(ctx, auth.GetUserID())
	if err != nil {
		return err
	}

	if u.Status != "inactive" {
		return nil
	}

	return &errors2.Error{Code: errors2.EForbidden, Msg: "User is inactive"}
}

func (h *AuthenticationHandler) extractAuthorization(ctx context.Context, r *http.Request) (platform.Authorizer, error) {
	t, err := GetToken(r)
	if err != nil {
		return nil, err
	}

	token, err := h.TokenParser.Parse(t)
	if err == nil {
		return token, nil
	}

	// if the error returned signifies ths token is
	// not a well formed JWT then use it as a lookup
	// key for its associated authorization
	// otherwise return the error
	if !jsonweb.IsMalformedError(err) {
		return nil, err
	}

	return h.AuthorizationService.FindAuthorizationByToken(ctx, t)
}

func (h *AuthenticationHandler) extractSession(ctx context.Context, r *http.Request) (*platform.Session, error) {
	k, err := session.DecodeCookieSession(ctx, r)
	if err != nil {
		return nil, err
	}

	s, err := h.SessionService.FindSession(ctx, k)
	if err != nil {
		return nil, err
	}

	if !h.SessionRenewDisabled {
		// if the session is not expired, renew the session
		err = h.SessionService.RenewSession(ctx, s, time.Now().Add(platform.RenewSessionTime))
		if err != nil {
			return nil, err
		}
	}

	return s, err
}
