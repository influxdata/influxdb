package session

import (
	"context"
	"net/http"

	"github.com/go-chi/chi"
	"github.com/go-chi/chi/middleware"
	"github.com/influxdata/influxdb/v2"
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

// SignInResourceHandler allows us to return 2 different rousource handler
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

// SignOutResourceHandler allows us to return 2 different rousource handler
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
		h.api.Err(w, ErrUnauthorized)
		return
	}

	u, err := h.userSvc.FindUser(ctx, influxdb.UserFilter{
		Name: &req.Username,
	})
	if err != nil {
		h.api.Err(w, ErrUnauthorized)
		return
	}

	if err := h.passSvc.ComparePassword(ctx, u.ID, req.Password); err != nil {
		h.api.Err(w, ErrUnauthorized)
		return
	}

	s, e := h.sessionSvc.CreateSession(ctx, req.Username)
	if e != nil {
		h.api.Err(w, ErrUnauthorized)
		return
	}

	encodeCookieSession(w, s)
	w.WriteHeader(http.StatusNoContent)
}

type signinRequest struct {
	Username string
	Password string
}

func decodeSigninRequest(ctx context.Context, r *http.Request) (*signinRequest, *influxdb.Error) {
	u, p, ok := r.BasicAuth()
	if !ok {
		return nil, &influxdb.Error{
			Code: influxdb.EInvalid,
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
		h.api.Err(w, ErrUnauthorized)
		return
	}

	if err := h.sessionSvc.ExpireSession(ctx, req.Key); err != nil {
		h.api.Err(w, ErrUnauthorized)
		return
	}

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

func encodeCookieSession(w http.ResponseWriter, s *influxdb.Session) {
	c := &http.Cookie{
		Name:  cookieSessionName,
		Value: s.Key,
	}

	http.SetCookie(w, c)
}

func decodeCookieSession(ctx context.Context, r *http.Request) (string, error) {
	c, err := r.Cookie(cookieSessionName)
	if err != nil {
		return "", &influxdb.Error{
			Code: influxdb.EInvalid,
			Err:  err,
		}
	}
	return c.Value, nil
}

// SetCookieSession adds a cookie for the session to an http request
func SetCookieSession(key string, r *http.Request) {
	c := &http.Cookie{
		Name:   cookieSessionName,
		Value:  key,
		Secure: true,
	}

	r.AddCookie(c)
}
