package http

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/influxdata/influxdb/v2"
	kithttp "github.com/influxdata/influxdb/v2/kit/transport/http"
	"github.com/influxdata/influxdb/v2/mock"
)

func TestInflux1xAuthenticationHandler(t *testing.T) {
	var one = influxdb.ID(1)

	type fields struct {
		FindAuthorizationByTokenFn func(context.Context, string) (*influxdb.Authorization, error)
		FindUserFn                 func(context.Context, influxdb.UserFilter) (*influxdb.User, error)
		FindUserByIDFn             func(context.Context, influxdb.ID) (*influxdb.User, error)
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
				SetToken(p, r)
			} else {
				SetToken(u+":"+p, r)
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
				FindAuthorizationByTokenFn: func(ctx context.Context, token string) (*influxdb.Authorization, error) {
					return nil, fmt.Errorf("authorization not found")
				},
			},
			exp: exp{
				code: http.StatusUnauthorized,
			},
		},
		{
			name: "user is inactive",
			fields: fields{
				FindAuthorizationByTokenFn: func(ctx context.Context, token string) (*influxdb.Authorization, error) {
					return &influxdb.Authorization{UserID: one}, nil
				},
				FindUserFn: func(ctx context.Context, f influxdb.UserFilter) (*influxdb.User, error) {
					return &influxdb.User{ID: one, Status: "inactive"}, nil
				},
			},
			auth: basic(User, Token),
			exp: exp{
				code: http.StatusForbidden,
			},
		},
		{
			name: "username and token mismatch",
			fields: fields{
				FindAuthorizationByTokenFn: func(ctx context.Context, token string) (*influxdb.Authorization, error) {
					return &influxdb.Authorization{UserID: one}, nil
				},
				FindUserFn: func(ctx context.Context, f influxdb.UserFilter) (*influxdb.User, error) {
					return &influxdb.User{ID: influxdb.ID(2)}, nil
				},
			},
			auth: basic(User, Token),
			exp: exp{
				code: http.StatusForbidden,
			},
		},
		{
			name: "no auth provided",
			fields: fields{
				FindAuthorizationByTokenFn: func(ctx context.Context, token string) (*influxdb.Authorization, error) {
					return &influxdb.Authorization{}, nil
				},
			},
			exp: exp{
				code: http.StatusUnauthorized,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var h *Influx1xAuthenticationHandler
			{
				auth := &mock.AuthorizationService{FindAuthorizationByTokenFn: tt.fields.FindAuthorizationByTokenFn}
				if auth.FindAuthorizationByTokenFn == nil {
					auth.FindAuthorizationByTokenFn = func(ctx context.Context, token string) (*influxdb.Authorization, error) {
						return &influxdb.Authorization{UserID: one}, nil
					}
				}

				user := &mock.UserService{FindUserFn: tt.fields.FindUserFn, FindUserByIDFn: tt.fields.FindUserByIDFn}
				if user.FindUserFn == nil {
					user.FindUserFn = func(context.Context, influxdb.UserFilter) (*influxdb.User, error) {
						return &influxdb.User{ID: one}, nil
					}
				}
				if user.FindUserByIDFn == nil {
					user.FindUserByIDFn = func(_ context.Context, id influxdb.ID) (*influxdb.User, error) {
						return &influxdb.User{ID: id}, nil
					}
				}
				next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(http.StatusOK)
				})

				h = NewInflux1xAuthenticationHandler(next, auth, user, kithttp.ErrorHandler(0))
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
