package http_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/influxdata/influxdb/v2"
	platformhttp "github.com/influxdata/influxdb/v2/http"
	"github.com/influxdata/influxdb/v2/jsonweb"
	"github.com/influxdata/influxdb/v2/kit/platform"
	kithttp "github.com/influxdata/influxdb/v2/kit/transport/http"
	"github.com/influxdata/influxdb/v2/mock"
	"github.com/influxdata/influxdb/v2/session"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

const token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJjbG91ZDIuaW5mbHV4ZGF0YS5jb20iLCJhdWQiOiJnYXRld2F5LmluZmx1eGRhdGEuY29tIiwiaWF0IjoxNTY4NjI4OTgwLCJraWQiOiJzb21lLWtleSIsInBlcm1pc3Npb25zIjpbeyJhY3Rpb24iOiJ3cml0ZSIsInJlc291cmNlIjp7InR5cGUiOiJidWNrZXRzIiwiaWQiOiIwMDAwMDAwMDAwMDAwMDAxIiwib3JnSUQiOiIwMDAwMDAwMDAwMDAwMDAyIn19XX0.74vjbExiOd702VSIMmQWaDT_GFvUI0-_P-SfQ_OOHB0"

var one = platform.ID(1)

func TestAuthenticationHandler(t *testing.T) {
	type fields struct {
		AuthorizationService influxdb.AuthorizationService
		SessionService       influxdb.SessionService
		UserService          influxdb.UserService
		TokenParser          *jsonweb.TokenParser
	}
	type args struct {
		token   string
		session string
	}
	type wants struct {
		code int
	}

	tests := []struct {
		name   string
		fields fields
		args   args
		wants  wants
	}{
		{
			name: "session provided",
			fields: fields{
				AuthorizationService: mock.NewAuthorizationService(),
				SessionService: &mock.SessionService{
					FindSessionFn: func(ctx context.Context, key string) (*influxdb.Session, error) {
						return &influxdb.Session{}, nil
					},
					RenewSessionFn: func(ctx context.Context, session *influxdb.Session, expiredAt time.Time) error {
						return nil
					},
				},
			},
			args: args{
				session: "abc123",
			},
			wants: wants{
				code: http.StatusOK,
			},
		},
		{
			name: "session does not exist",
			fields: fields{
				AuthorizationService: mock.NewAuthorizationService(),
				SessionService: &mock.SessionService{
					FindSessionFn: func(ctx context.Context, key string) (*influxdb.Session, error) {
						return nil, fmt.Errorf("session not found")
					},
				},
			},
			args: args{
				session: "abc123",
			},
			wants: wants{
				code: http.StatusUnauthorized,
			},
		},
		{
			name: "token provided",
			fields: fields{
				AuthorizationService: &mock.AuthorizationService{
					FindAuthorizationByTokenFn: func(ctx context.Context, token string) (*influxdb.Authorization, error) {
						return &influxdb.Authorization{}, nil
					},
				},
				SessionService: mock.NewSessionService(),
			},
			args: args{
				token: "abc123",
			},
			wants: wants{
				code: http.StatusOK,
			},
		},
		{
			name: "token does not exist",
			fields: fields{
				AuthorizationService: &mock.AuthorizationService{
					FindAuthorizationByTokenFn: func(ctx context.Context, token string) (*influxdb.Authorization, error) {
						return nil, fmt.Errorf("authorization not found")
					},
				},
				SessionService: mock.NewSessionService(),
			},
			args: args{
				token: "abc123",
			},
			wants: wants{
				code: http.StatusUnauthorized,
			},
		},
		{
			name: "associated user is inactive",
			fields: fields{
				AuthorizationService: &mock.AuthorizationService{
					FindAuthorizationByTokenFn: func(ctx context.Context, token string) (*influxdb.Authorization, error) {
						return &influxdb.Authorization{UserID: one}, nil
					},
				},
				SessionService: mock.NewSessionService(),
				UserService: &mock.UserService{
					FindUserByIDFn: func(ctx context.Context, id platform.ID) (*influxdb.User, error) {
						if !id.Valid() {
							panic("user service should only be called with valid user ID")
						}

						return &influxdb.User{Status: "inactive"}, nil
					},
				},
			},
			args: args{
				token: "abc123",
			},
			wants: wants{
				code: http.StatusForbidden,
			},
		},
		{
			// The active-user check reports 403 for BOTH a user whose status is
			// inactive and any UserService lookup failure. Only the former was
			// covered; this pins the latter so the two cannot drift apart.
			name: "associated user cannot be looked up",
			fields: fields{
				AuthorizationService: &mock.AuthorizationService{
					FindAuthorizationByTokenFn: func(ctx context.Context, token string) (*influxdb.Authorization, error) {
						return &influxdb.Authorization{UserID: one}, nil
					},
				},
				SessionService: mock.NewSessionService(),
				UserService: &mock.UserService{
					FindUserByIDFn: func(context.Context, platform.ID) (*influxdb.User, error) {
						return nil, errors.New("user not found")
					},
				},
			},
			args: args{
				token: "abc123",
			},
			wants: wants{
				code: http.StatusForbidden,
			},
		},
		{
			name: "no auth provided",
			fields: fields{
				AuthorizationService: mock.NewAuthorizationService(),
				SessionService:       mock.NewSessionService(),
			},
			args: args{},
			wants: wants{
				code: http.StatusUnauthorized,
			},
		},
		{
			name: "jwt provided",
			fields: fields{
				AuthorizationService: &mock.AuthorizationService{
					FindAuthorizationByTokenFn: func(ctx context.Context, token string) (*influxdb.Authorization, error) {
						return nil, fmt.Errorf("authorization not found")
					},
				},
				SessionService: mock.NewSessionService(),
				UserService: &mock.UserService{
					FindUserByIDFn: func(ctx context.Context, id platform.ID) (*influxdb.User, error) {
						// ensure that this is not reached as jwt token authorizer produces
						// invalid user id
						if !id.Valid() {
							panic("user service should only be called with valid user ID")
						}

						return nil, errors.New("user not found")
					},
				},
				TokenParser: jsonweb.NewTokenParser(jsonweb.KeyStoreFunc(func(string) ([]byte, error) {
					return []byte("correct-key"), nil
				})),
			},
			args: args{
				token: token,
			},
			wants: wants{
				code: http.StatusOK,
			},
		},
		{
			name: "jwt provided - bad signature",
			fields: fields{
				AuthorizationService: &mock.AuthorizationService{
					FindAuthorizationByTokenFn: func(ctx context.Context, token string) (*influxdb.Authorization, error) {
						panic("token lookup attempted")
					},
				},
				SessionService: mock.NewSessionService(),
				TokenParser: jsonweb.NewTokenParser(jsonweb.KeyStoreFunc(func(string) ([]byte, error) {
					return []byte("incorrect-key"), nil
				})),
			},
			args: args{
				token: token,
			},
			wants: wants{
				code: http.StatusUnauthorized,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
			})

			h := platformhttp.NewAuthenticationHandler(zaptest.NewLogger(t), kithttp.NewErrorHandler(zaptest.NewLogger(t)))
			h.AuthorizationService = tt.fields.AuthorizationService
			h.SessionService = tt.fields.SessionService
			h.UserService = &mock.UserService{
				FindUserByIDFn: func(ctx context.Context, id platform.ID) (*influxdb.User, error) {
					return &influxdb.User{}, nil
				},
			}

			if tt.fields.UserService != nil {
				h.UserService = tt.fields.UserService
			}

			if tt.fields.TokenParser != nil {
				h.TokenParser = tt.fields.TokenParser
			}

			h.Handler = handler

			w := httptest.NewRecorder()
			r := httptest.NewRequest("POST", "http://any.url", nil)

			if tt.args.session != "" {
				session.SetCookieSession(tt.args.session, r)
			}

			if tt.args.token != "" {
				platformhttp.SetToken(tt.args.token, r)
			}

			h.ServeHTTP(w, r)

			if got, want := w.Code, tt.wants.code; got != want {
				t.Errorf("expected status code to be %d got %d", want, got)
			}
		})
	}
}

// TestAuthenticationHandler_Authorize pins the (Authorizer, error) contract that
// HealthReadyHandler depends on. The distinction that matters is between "the
// caller could not be identified" (nil, err) and "the caller was identified but
// rejected" (auth, err) -- ServeHTTP relies on the latter to record the
// authorizer for the request log before answering 403.
func TestAuthenticationHandler_Authorize(t *testing.T) {
	activeUser := func(context.Context, platform.ID) (*influxdb.User, error) {
		return &influxdb.User{}, nil
	}

	tests := []struct {
		name           string
		token          string
		session        string
		authSvc        influxdb.AuthorizationService
		sessionSvc     influxdb.SessionService
		findUserByID   func(context.Context, platform.ID) (*influxdb.User, error)
		wantAuthorizer bool
		wantErr        bool
		wantInactive   bool
	}{
		{
			name:       "no credentials",
			authSvc:    mock.NewAuthorizationService(),
			sessionSvc: mock.NewSessionService(),
			wantErr:    true,
		},
		{
			name:  "token does not resolve",
			token: "abc123",
			authSvc: &mock.AuthorizationService{
				FindAuthorizationByTokenFn: func(context.Context, string) (*influxdb.Authorization, error) {
					return nil, errors.New("authorization not found")
				},
			},
			sessionSvc: mock.NewSessionService(),
			wantErr:    true,
		},
		{
			name:  "token resolves",
			token: "abc123",
			authSvc: &mock.AuthorizationService{
				FindAuthorizationByTokenFn: func(context.Context, string) (*influxdb.Authorization, error) {
					return &influxdb.Authorization{}, nil
				},
			},
			sessionSvc:     mock.NewSessionService(),
			wantAuthorizer: true,
		},
		{
			name:    "session resolves",
			session: "abc123",
			authSvc: mock.NewAuthorizationService(),
			sessionSvc: &mock.SessionService{
				FindSessionFn: func(context.Context, string) (*influxdb.Session, error) {
					return &influxdb.Session{}, nil
				},
				RenewSessionFn: func(context.Context, *influxdb.Session, time.Time) error {
					return nil
				},
			},
			wantAuthorizer: true,
		},
		{
			name:    "cookie presented but no session service",
			session: "abc123",
			authSvc: mock.NewAuthorizationService(),
			wantErr: true,
		},
		{
			name:  "user is inactive",
			token: "abc123",
			authSvc: &mock.AuthorizationService{
				FindAuthorizationByTokenFn: func(context.Context, string) (*influxdb.Authorization, error) {
					return &influxdb.Authorization{UserID: one}, nil
				},
			},
			sessionSvc: mock.NewSessionService(),
			findUserByID: func(context.Context, platform.ID) (*influxdb.User, error) {
				return &influxdb.User{Status: "inactive"}, nil
			},
			wantAuthorizer: true,
			wantErr:        true,
			wantInactive:   true,
		},
		{
			name:  "user lookup fails",
			token: "abc123",
			authSvc: &mock.AuthorizationService{
				FindAuthorizationByTokenFn: func(context.Context, string) (*influxdb.Authorization, error) {
					return &influxdb.Authorization{UserID: one}, nil
				},
			},
			sessionSvc: mock.NewSessionService(),
			findUserByID: func(context.Context, platform.ID) (*influxdb.User, error) {
				return nil, errors.New("user not found")
			},
			wantAuthorizer: true,
			wantErr:        true,
			wantInactive:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := platformhttp.NewAuthenticationHandler(zaptest.NewLogger(t), kithttp.NewErrorHandler(zaptest.NewLogger(t)))
			h.AuthorizationService = tt.authSvc
			h.SessionService = tt.sessionSvc
			findUser := activeUser
			if tt.findUserByID != nil {
				findUser = tt.findUserByID
			}
			h.UserService = &mock.UserService{FindUserByIDFn: findUser}

			r := httptest.NewRequest("GET", "http://any.url", nil)
			if tt.session != "" {
				session.SetCookieSession(tt.session, r)
			}
			if tt.token != "" {
				platformhttp.SetToken(tt.token, r)
			}

			auth, err := h.Authorize(r)

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			if tt.wantAuthorizer {
				assert.NotNil(t, auth, "expected the caller to be identified")
			} else {
				assert.Nil(t, auth, "expected the caller to be unidentifiable")
			}
			assert.Equal(t, tt.wantInactive, errors.Is(err, platformhttp.ErrInactiveUser),
				"ErrInactiveUser classification drives the 403-vs-401 split")
		})
	}
}

// TestAuthenticationHandler_Authorize_SessionRenewDisabled pins that a resolver
// configured with SessionRenewDisabled never extends the session. The health
// resolver relies on this: without it a browser polling /health would keep its
// session alive indefinitely, defeating --session-length.
func TestAuthenticationHandler_Authorize_SessionRenewDisabled(t *testing.T) {
	h := platformhttp.NewAuthenticationHandler(zaptest.NewLogger(t), kithttp.NewErrorHandler(zaptest.NewLogger(t)))
	h.AuthorizationService = mock.NewAuthorizationService()
	h.SessionRenewDisabled = true
	h.SessionService = &mock.SessionService{
		FindSessionFn: func(context.Context, string) (*influxdb.Session, error) {
			return &influxdb.Session{}, nil
		},
		RenewSessionFn: func(context.Context, *influxdb.Session, time.Time) error {
			t.Error("RenewSession must not be called when SessionRenewDisabled is set")
			return nil
		},
	}
	h.UserService = &mock.UserService{
		FindUserByIDFn: func(context.Context, platform.ID) (*influxdb.User, error) {
			return &influxdb.User{}, nil
		},
	}

	r := httptest.NewRequest("GET", "http://any.url", nil)
	session.SetCookieSession("abc123", r)

	auth, err := h.Authorize(r)
	require.NoError(t, err)
	require.NotNil(t, auth)
}

func TestProbeAuthScheme(t *testing.T) {
	type args struct {
		token   string
		session string
	}
	type wants struct {
		scheme string
		err    error
	}

	tests := []struct {
		name  string
		args  args
		wants wants
	}{
		{
			name: "session provided",
			args: args{
				session: "abc123",
			},
			wants: wants{
				scheme: "session",
			},
		},
		{
			name: "token provided",
			args: args{
				token: "abc123",
			},
			wants: wants{
				scheme: "token",
			},
		},
		{
			name: "no auth provided",
			args: args{},
			wants: wants{
				err: fmt.Errorf("unknown authentication scheme"),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := httptest.NewRequest("POST", "http://any.url", nil)

			if tt.args.session != "" {
				session.SetCookieSession(tt.args.session, r)
			}

			if tt.args.token != "" {
				platformhttp.SetToken(tt.args.token, r)
			}

			scheme, err := platformhttp.ProbeAuthScheme(r)
			if (err != nil) != (tt.wants.err != nil) {
				t.Errorf("unexpected error got %v want %v", err, tt.wants.err)
				return
			}

			if got, want := scheme, tt.wants.scheme; got != want {
				t.Errorf("expected scheme to be %s got %s", want, got)
			}
		})
	}
}

func TestAuthenticationHandler_NoAuthRoutes(t *testing.T) {
	type route struct {
		method string
		path   string
	}
	type fields struct {
		excludedRoutes []route
	}
	type args struct {
		method string
		path   string
	}
	type wants struct {
		code int
	}

	tests := []struct {
		name   string
		fields fields
		args   args
		wants  wants
	}{
		{
			name: "route is no auth route",
			fields: fields{
				excludedRoutes: []route{
					{
						method: "GET",
						path:   "/debug/pprof",
					},
				},
			},
			args: args{
				method: "GET",
				path:   "/debug/pprof",
			},
			wants: wants{
				code: http.StatusOK,
			},
		},
		{
			name: "route is an auth route",
			fields: fields{
				excludedRoutes: []route{},
			},
			args: args{
				method: "POST",
				path:   "/api/v2/write",
			},
			wants: wants{
				code: http.StatusUnauthorized,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
			})

			h := platformhttp.NewAuthenticationHandler(zaptest.NewLogger(t), kithttp.NewErrorHandler(zaptest.NewLogger(t)))
			h.AuthorizationService = mock.NewAuthorizationService()
			h.SessionService = mock.NewSessionService()
			h.Handler = handler

			for _, rte := range tt.fields.excludedRoutes {
				h.RegisterNoAuthRoute(rte.method, rte.path)
			}

			w := httptest.NewRecorder()
			r := httptest.NewRequest(tt.args.method, tt.args.path, nil)

			h.ServeHTTP(w, r)

			if got, want := w.Code, tt.wants.code; got != want {
				t.Errorf("expected status code to be %d got %d", want, got)
			}
		})
	}
}
