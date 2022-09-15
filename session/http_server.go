package session

import (
	"context"
	"net/http"

	"github.com/go-chi/chi"
	"github.com/go-chi/chi/middleware"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"
	kithttp "github.com/influxdata/influxdb/v2/kit/transport/http"
	"go.uber.org/zap"
)

const (
	prefixSignIn  = "/api/v2/signin"
	prefixSignOut = "/api/v2/signout"
)

// SessionHandler represents an HTTP API handler for authorizations.
type SessionHandler struct {
	chi.Router
	api *kithttp.API
	log *zap.Logger

	sessionSvc influxdb.SessionService
	passSvc    influxdb.PasswordsService
	userSvc    influxdb.UserService
}

// NewSessionHandler returns a new instance of SessionHandler.
func NewSessionHandler(log *zap.Logger, sessionSvc influxdb.SessionService, userSvc influxdb.UserService, passwordsSvc influxdb.PasswordsService) *SessionHandler {
	svr := &SessionHandler{
		api: kithttp.NewAPI(kithttp.WithLog(log)),
		log: log,

		passSvc:    passwordsSvc,
		sessionSvc: sessionSvc,
		userSvc:    userSvc,
	}

	return svr
}

type resourceHandler struct {
	prefix string
	*SessionHandler
}

// Prefix is necessary to mount the router as a resource handler
func (r resourceHandler) Prefix() string { return r.prefix }

// SignInResourceHandler allows us to return 2 different resource handler
// for the appropriate mounting location
func (h SessionHandler) SignInResourceHandler() *resourceHandler {
	h.Router = chi.NewRouter()
	h.Router.Use(
		middleware.Recoverer,
		middleware.RequestID,
		middleware.RealIP,
	)
	h.Router.Post("/", h.handleSignin)
	return &resourceHandler{prefix: prefixSignIn, SessionHandler: &h}
}

// SignOutResourceHandler allows us to return 2 different resource handler
// for the appropriate mounting location
func (h SessionHandler) SignOutResourceHandler() *resourceHandler {
	h.Router = chi.NewRouter()
	h.Router.Use(
		middleware.Recoverer,
		middleware.RequestID,
		middleware.RealIP,
	)
	h.Router.Post("/", h.handleSignout)
	return &resourceHandler{prefix: prefixSignOut, SessionHandler: &h}
}

// handleSignin is the HTTP handler for the POST /signin route.
func (h *SessionHandler) handleSignin(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	req, decErr := decodeSigninRequest(ctx, r)
	if decErr != nil {
		h.api.Err(w, r, ErrUnauthorized)
		return
	}

	u, err := h.userSvc.FindUser(ctx, influxdb.UserFilter{
		Name: &req.Username,
	})
	if err != nil {
		h.api.Err(w, r, ErrUnauthorized)
		return
	}

	if err := h.passSvc.ComparePassword(ctx, u.ID, req.Password); err != nil {
		h.api.Err(w, r, ErrUnauthorized)
		return
	}

	s, e := h.sessionSvc.CreateSession(ctx, req.Username)
	if e != nil {
		h.api.Err(w, r, ErrUnauthorized)
		return
	}

	encodeCookieSession(w, s)
	w.WriteHeader(http.StatusNoContent)
}

type signinRequest struct {
	Username string
	Password string
}

func decodeSigninRequest(ctx context.Context, r *http.Request) (*signinRequest, *errors.Error) {
	u, p, ok := r.BasicAuth()
	if !ok {
		return nil, &errors.Error{
			Code: errors.EInvalid,
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
		h.api.Err(w, r, ErrUnauthorized)
		return
	}

	if err := h.sessionSvc.ExpireSession(ctx, req.Key); err != nil {
		h.api.Err(w, r, ErrUnauthorized)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

type signoutRequest struct {
	Key string
}

func decodeSignoutRequest(ctx context.Context, r *http.Request) (*signoutRequest, error) {
	key, err := DecodeCookieSession(ctx, r)
	if err != nil {
		return nil, err
	}
	return &signoutRequest{
		Key: key,
	}, nil
}

const cookieSessionName = "influxdb-oss-session"

func encodeCookieSession(w http.ResponseWriter, s *influxdb.Session) {
	// We only need the session cookie for accesses to "/api/...", so limit
	// it to that using "Path".
	//
	// Since the cookie is limited to "/api/..." and we don't expect any
	// links directly into /api/..., use SameSite=Strict as a hardening
	// measure. This works because external links into the UI have the form
	// https://<url>/orgs/<origid>/..., https://<url>/signin, etc and don't
	// need the cookie sent. By the time the UI itself calls out to
	// /api/..., the location bar matches the cookie's domain and
	// everything is 1st party and Strict's restriction work fine.
	//
	// SameSite=Lax would also be safe to use (modern browser's default if
	// unset) since it only sends the cookie with GET (and other safe HTTP
	// methods like HEAD and OPTIONS as defined in RFC6264) requests when
	// the location bar matches the domain of the cookie and we know that
	// our APIs do not perform state-changing actions with GET and other
	// safe methods. Using SameSite=Strict helps future-proof us against
	// that changing (ie, we add a state-changing GET API).
	//
	// Note: it's generally recommended that SameSite should not be relied
	// upon (particularly Lax) because:
	// a) SameSite doesn't work with (cookie-less) Basic Auth. We don't
	//    share browser session BasicAuth with accesses to to /api/... so
	//    this isn't a problem
	// b) SameSite=lax allows GET (and other safe HTTP methods) and some
	//    services might allow state-changing requests via GET. Our API
	//    doesn't support state-changing GETs and SameSite=strict doesn't
	//    allow GETs from 3rd party sites at all, so this isn't a problem
	// c) similar to 'b', some frameworks will accept HTTP methods for
	//    other handlers. Eg, the application is designed for POST but it
	//    will accept requests converted to the GET method. Golang does not
	//    do this itself and our route mounts explicitly map the HTTP
	//    method to the specific handler and thus we are not susceptible to
	//    this
	// d) SameSite could be bypassed if the attacker were able to
	//    manipulate the location bar in the browser (a serious browser
	//    bug; it is reasonable for us to expect browsers to enforce their
	//    SameSite restrictions)
	c := &http.Cookie{
		Name:     cookieSessionName,
		Value:    s.Key,
		Path:     "/api/", // since UI doesn't need it, limit cookie usage to API requests
		Expires:  s.ExpiresAt,
		SameSite: http.SameSiteStrictMode,
	}

	http.SetCookie(w, c)
}

func DecodeCookieSession(ctx context.Context, r *http.Request) (string, error) {
	c, err := r.Cookie(cookieSessionName)
	if err != nil {
		return "", &errors.Error{
			Code: errors.EInvalid,
			Err:  err,
		}
	}
	return c.Value, nil
}

// SetCookieSession adds a cookie for the session to an http request
func SetCookieSession(key string, r *http.Request) {
	c := &http.Cookie{
		Name:     cookieSessionName,
		Value:    key,
		Secure:   true,
		SameSite: http.SameSiteStrictMode,
	}

	r.AddCookie(c)
}
