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

// ErrInactiveUser classifies active-user-check failures so callers can answer
// 403 rather than 401. See isUserActive for what it covers and why.
var ErrInactiveUser = errors.New("user is not active")

// CredentialResolver resolves the credential carried by a request into an
// Authorizer. Implementations write nothing to the response and log nothing,
// leaving callers free to decide what a failure means for them:
// AuthenticationHandler turns it into a 401 or 403, while HealthReadyHandler
// merely withholds check detail.
type CredentialResolver interface {
	// Authorize resolves the credential on r. A non-nil Authorizer returned
	// alongside a non-nil error means the credential was resolved but its
	// owning user was rejected; callers that record the authorizer for
	// logging should do so before acting on the error.
	Authorize(r *http.Request) (platform.Authorizer, error)
}

var _ CredentialResolver = (*AuthenticationHandler)(nil)

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

// Authorize implements CredentialResolver. It probes the request for a token
// or session cookie, resolves it, and verifies that the owning user is active.
//
// It deliberately does not consult the no-auth route table, write to the
// response, or log: ServeHTTP retains all three. Keeping the log out of here
// matters because HealthReadyHandler calls this on every credentialed probe,
// and an Info line per probe would be unbounded noise.
func (h *AuthenticationHandler) Authorize(r *http.Request) (platform.Authorizer, error) {
	ctx := r.Context()
	scheme, err := ProbeAuthScheme(r)
	if err != nil {
		return nil, err
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
		// Return an untyped nil rather than auth: extractSession's concrete
		// *platform.Session would otherwise make the interface non-nil, and
		// callers test the Authorizer for nil to decide whether the caller
		// was identified.
		return nil, err
	}

	// JWT-based auth is permission-based rather than identity-based and therefore
	// has no associated user. If the user ID is invalid, disregard the user active
	// check.
	if !auth.GetUserID().Valid() {
		return auth, nil
	}

	// Hand back the authorizer alongside any error: the caller has been
	// identified even though it may be rejected, and ServeHTTP records that
	// identity for the request log before returning 403.
	return auth, h.isUserActive(ctx, auth)
}

// ServeHTTP extracts the session or token from the http request and places the resulting authorizer on the request context.
func (h *AuthenticationHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if handler, _, _ := h.noAuthRouter.Lookup(r.Method, r.URL.Path); handler != nil {
		h.Handler.ServeHTTP(w, r)
		return
	}

	ctx := r.Context()

	auth, err := h.Authorize(r)
	if auth != nil {
		// Set the Authorizer pointer for use in logging high up the call stack.
		// This happens before the error is acted on so that a caller rejected
		// for being inactive is still attributable in the request log.
		platcontext.StoreAuthorizer(ctx, auth)
	}
	if err != nil {
		if errors.Is(err, ErrInactiveUser) {
			InactiveUserError(ctx, h, w)
			return
		}
		h.unauthorized(ctx, w, err)
		return
	}

	ctx = platcontext.SetAuthorizer(ctx, auth)

	if span := opentracing.SpanFromContext(ctx); span != nil {
		span.SetTag("user_id", auth.GetUserID().String())
	}

	h.Handler.ServeHTTP(w, r.WithContext(ctx))
}

// isUserActive reports whether the authorizer's owning user may proceed. Both
// failure modes — the lookup failing and the user being inactive — are wrapped
// in ErrInactiveUser because ServeHTTP has always answered 403 for both, and
// TestAuthenticationHandler pins that. Callers today only classify the error;
// the cause is preserved in the chain for logging and future inspection.
func (h *AuthenticationHandler) isUserActive(ctx context.Context, auth platform.Authorizer) error {
	u, err := h.UserService.FindUserByID(ctx, auth.GetUserID())
	if err != nil {
		return fmt.Errorf("%w: %w", ErrInactiveUser, err)
	}

	if u.Status != "inactive" {
		return nil
	}

	return fmt.Errorf("%w: %w", ErrInactiveUser, &errors2.Error{Code: errors2.EForbidden, Msg: "User is inactive"})
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
	// A handler may be constructed without a session service, in which case
	// cookie-bearing callers are unresolvable and token auth still works. Note
	// the trade: this turns what would be a panic on a genuinely misconfigured
	// handler into a quiet 401 for cookie callers.
	if h.SessionService == nil {
		return nil, errors.New("session service not available")
	}

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
