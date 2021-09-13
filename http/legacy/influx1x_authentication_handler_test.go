package legacy

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"
	kithttp "github.com/influxdata/influxdb/v2/kit/transport/http"
	"github.com/influxdata/influxdb/v2/mock"
	itesting "github.com/influxdata/influxdb/v2/testing"
	"go.uber.org/zap/zaptest"
)

const tokenScheme = "Token " // TODO(goller): I'd like this to be Bearer

func setToken(token string, req *http.Request) {
	req.Header.Set("Authorization", fmt.Sprintf("%s%s", tokenScheme, token))
}

func TestInflux1xAuthenticationHandler(t *testing.T) {
	var userID = itesting.MustIDBase16("0000000000001010")

	type fields struct {
		AuthorizeFn func(ctx context.Context, c influxdb.CredentialsV1) (*influxdb.Authorization, error)
	}

	type exp struct {
		code int
	}

	basic := func(u, p string) func(r *http.Request) {
		return func(r *http.Request) {
			r.SetBasicAuth(u, p)
		}
	}

	token := func(u, p string) func(r *http.Request) {
		return func(r *http.Request) {
			if u == "" {
				setToken(p, r)
			} else {
				setToken(u+":"+p, r)
			}
		}
	}

	query := func(u, p string) func(r *http.Request) {
		return func(r *http.Request) {
			v := r.URL.Query()
			v.Add("u", u)
			v.Add("p", p)
			r.URL.RawQuery = v.Encode()
		}
	}

	const (
		User  = "sydney"
		Token = "my-token"
	)

	tests := []struct {
		name   string
		fields fields
		auth   func(r *http.Request)
		exp    exp
	}{
		// successful requests
		{
			name:   "basic auth",
			fields: fields{},
			auth:   basic(User, Token),
			exp: exp{
				code: http.StatusOK,
			},
		},
		{
			name:   "query string",
			fields: fields{},
			auth:   query(User, Token),
			exp: exp{
				code: http.StatusOK,
			},
		},
		{
			name:   "Token as user:token",
			fields: fields{},
			auth:   token(User, Token),
			exp: exp{
				code: http.StatusOK,
			},
		},
		{
			name:   "Token as token",
			fields: fields{},
			auth:   token("", Token),
			exp: exp{
				code: http.StatusOK,
			},
		},
		{
			name: "token does not exist",
			fields: fields{
				AuthorizeFn: func(ctx context.Context, c influxdb.CredentialsV1) (*influxdb.Authorization, error) {
					return nil, &errors.Error{Code: errors.EUnauthorized}
				},
			},
			exp: exp{
				code: http.StatusUnauthorized,
			},
		},
		{
			name: "authorize returns error EForbidden",
			fields: fields{
				AuthorizeFn: func(ctx context.Context, c influxdb.CredentialsV1) (*influxdb.Authorization, error) {
					return nil, &errors.Error{Code: errors.EForbidden}
				},
			},
			auth: basic(User, Token),
			exp: exp{
				code: http.StatusForbidden,
			},
		},
		{
			name: "authorize returns error EUnauthorized",
			fields: fields{
				AuthorizeFn: func(ctx context.Context, c influxdb.CredentialsV1) (*influxdb.Authorization, error) {
					return nil, &errors.Error{Code: errors.EUnauthorized}
				},
			},
			auth: basic(User, Token),
			exp: exp{
				code: http.StatusUnauthorized,
			},
		},
		{
			name: "authorize returns error other",
			fields: fields{
				AuthorizeFn: func(ctx context.Context, c influxdb.CredentialsV1) (*influxdb.Authorization, error) {
					return nil, &errors.Error{Code: errors.EInvalid}
				},
			},
			auth: basic(User, Token),
			exp: exp{
				code: http.StatusUnauthorized,
			},
		},
		{
			name:   "no auth provided",
			fields: fields{},
			exp: exp{
				code: http.StatusUnauthorized,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			t.Cleanup(ctrl.Finish)

			var h *Influx1xAuthenticationHandler
			{
				auth := &mock.AuthorizerV1{AuthorizeFn: tt.fields.AuthorizeFn}
				if auth.AuthorizeFn == nil {
					auth.AuthorizeFn = func(ctx context.Context, c influxdb.CredentialsV1) (*influxdb.Authorization, error) {
						return &influxdb.Authorization{UserID: userID}, nil
					}
				}
				next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(http.StatusOK)
				})

				h = NewInflux1xAuthenticationHandler(next, auth, kithttp.NewErrorHandler(zaptest.NewLogger(t)))
			}

			w := httptest.NewRecorder()
			r := httptest.NewRequest("POST", "http://any.url", nil)
			if tt.auth != nil {
				tt.auth(r)
			}
			h.ServeHTTP(w, r)

			if got, want := w.Code, tt.exp.code; got != want {
				t.Errorf("expected status code to be %d got %d", want, got)
			}
		})
	}
}
