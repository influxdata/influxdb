package legacy

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"

	errors2 "github.com/influxdata/influxdb/v2/kit/platform/errors"

	"github.com/influxdata/influxdb/v2"
	platcontext "github.com/influxdata/influxdb/v2/context"
	"github.com/opentracing/opentracing-go"
)

type Authorizer interface {
	Authorize(ctx context.Context, c influxdb.CredentialsV1) (*influxdb.Authorization, error)
}

type Influx1xAuthenticationHandler struct {
	errors2.HTTPErrorHandler
	next http.Handler
	auth Authorizer
}

// NewInflux1xAuthenticationHandler creates an authentication handler to process
// InfluxDB 1.x authentication requests.
func NewInflux1xAuthenticationHandler(next http.Handler, auth Authorizer, h errors2.HTTPErrorHandler) *Influx1xAuthenticationHandler {
	return &Influx1xAuthenticationHandler{
		HTTPErrorHandler: h,
		next:             next,
		auth:             auth,
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
		unauthorizedError(ctx, h, w)
		return
	}

	auth, err := h.auth.Authorize(ctx, creds)
	if err != nil {
		var erri *errors2.Error
		if errors.As(err, &erri) {
			switch erri.Code {
			case errors2.EForbidden, errors2.EUnauthorized:
				h.HandleHTTPError(ctx, erri, w)
				return
			}
		}
		unauthorizedError(ctx, h, w)
		return
	}

	ctx = platcontext.SetAuthorizer(ctx, auth)

	if span := opentracing.SpanFromContext(ctx); span != nil {
		span.SetTag("user_id", auth.GetUserID().String())
	}

	h.next.ServeHTTP(w, r.WithContext(ctx))
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
func (h *Influx1xAuthenticationHandler) parseCredentials(r *http.Request) (influxdb.CredentialsV1, error) {
	q := r.URL.Query()

	// Check for username and password in URL params.
	if u, p := q.Get("u"), q.Get("p"); u != "" && p != "" {
		return influxdb.CredentialsV1{
			Scheme:   influxdb.SchemeV1URL,
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
					return influxdb.CredentialsV1{
						Scheme:   influxdb.SchemeV1Token,
						Username: u,
						Token:    p,
					}, nil
				}

				// fallback to only a token
			}
		}

		// Check for basic auth.
		if u, p, ok := r.BasicAuth(); ok {
			return influxdb.CredentialsV1{
				Scheme:   influxdb.SchemeV1Basic,
				Username: u,
				Token:    p,
			}, nil
		}
	}

	return influxdb.CredentialsV1{}, fmt.Errorf("unable to parse authentication credentials")
}

// unauthorizedError encodes a error message and status code for unauthorized access.
func unauthorizedError(ctx context.Context, h errors2.HTTPErrorHandler, w http.ResponseWriter) {
	h.HandleHTTPError(ctx, &errors2.Error{
		Code: errors2.EUnauthorized,
		Msg:  "unauthorized access",
	}, w)
}
