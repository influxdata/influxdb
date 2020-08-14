package http

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/influxdata/influxdb/v2"
	platcontext "github.com/influxdata/influxdb/v2/context"
	"github.com/opentracing/opentracing-go"
)

type Influx1xAuthenticationHandler struct {
	influxdb.HTTPErrorHandler
	next http.Handler
	auth influxdb.AuthorizationService
	user influxdb.UserService
}

// NewInflux1xAuthenticationHandler creates an authentication handler to process
// InfluxDB 1.x authentication requests.
func NewInflux1xAuthenticationHandler(next http.Handler, auth influxdb.AuthorizationService, user influxdb.UserService, h influxdb.HTTPErrorHandler) *Influx1xAuthenticationHandler {
	return &Influx1xAuthenticationHandler{
		HTTPErrorHandler: h,
		next:             next,
		auth:             auth,
		user:             user,
	}
}

// ServeHTTP extracts the session or token from the http request and places the resulting authorizer on the request context.
func (h *Influx1xAuthenticationHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// The ping endpoint does not need authorization
	if r.URL.Path == "/ping" {
		h.next.ServeHTTP(w, r)
		return
	}
	ctx := r.Context()

	creds, err := h.parseCredentials(r)
	if err != nil {
		UnauthorizedError(ctx, h, w)
		return
	}

	auth, err := h.auth.FindAuthorizationByToken(ctx, creds.Token)
	if err != nil {
		UnauthorizedError(ctx, h, w)
		return
	}

	var user *influxdb.User
	if creds.Username != "" {
		user, err = h.user.FindUser(ctx, influxdb.UserFilter{Name: &creds.Username})
		if err != nil {
			UnauthorizedError(ctx, h, w)
			return
		}

		if user.ID != auth.UserID {
			h.HandleHTTPError(ctx, &influxdb.Error{
				Code: influxdb.EForbidden,
				Msg:  "Username and Token do not match",
			}, w)
			return
		}
	} else {
		user, err = h.user.FindUserByID(ctx, auth.UserID)
		if err != nil {
			UnauthorizedError(ctx, h, w)
			return
		}
	}

	if err = h.isUserActive(user); err != nil {
		InactiveUserError(ctx, h, w)
		return
	}

	ctx = platcontext.SetAuthorizer(ctx, auth)

	if span := opentracing.SpanFromContext(ctx); span != nil {
		span.SetTag("user_id", auth.GetUserID().String())
	}

	h.next.ServeHTTP(w, r.WithContext(ctx))
}

func (h *Influx1xAuthenticationHandler) isUserActive(u *influxdb.User) error {
	if u.Status != "inactive" {
		return nil
	}

	return &influxdb.Error{Code: influxdb.EForbidden, Msg: "User is inactive"}
}

type credentials struct {
	Username string
	Token    string
}

func parseToken(token string) (user, pass string, ok bool) {
	s := strings.IndexByte(token, ':')
	if s < 0 {
		// Token <token>
		return "", token, true
	}

	// Token <username>:<token>
	return token[:s], token[s+1:], true
}

// parseCredentials parses a request and returns the authentication credentials.
// The credentials may be present as URL query params, or as a Basic
// Authentication header.
// As params: http://127.0.0.1/query?u=username&p=token
// As basic auth: http://username:token@127.0.0.1
// As Token in Authorization header: Token <username:token>
func (h *Influx1xAuthenticationHandler) parseCredentials(r *http.Request) (*credentials, error) {
	q := r.URL.Query()

	// Check for username and password in URL params.
	if u, p := q.Get("u"), q.Get("p"); u != "" && p != "" {
		return &credentials{
			Username: u,
			Token:    p,
		}, nil
	}

	// Check for the HTTP Authorization header.
	if s := r.Header.Get("Authorization"); s != "" {
		// Check for Bearer token.
		strs := strings.Split(s, " ")
		if len(strs) == 2 {
			switch strs[0] {
			case "Token":
				if u, p, ok := parseToken(strs[1]); ok {
					return &credentials{
						Username: u,
						Token:    p,
					}, nil
				}

				// fallback to only a token
			}
		}

		// Check for basic auth.
		if u, p, ok := r.BasicAuth(); ok {
			return &credentials{
				Username: u,
				Token:    p,
			}, nil
		}
	}

	return nil, fmt.Errorf("unable to parse authentication credentials")
}
