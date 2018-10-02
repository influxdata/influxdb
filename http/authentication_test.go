package http_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/influxdata/platform"
	platformhttp "github.com/influxdata/platform/http"
	"github.com/influxdata/platform/mock"
)

func TestAuthenticationHandler(t *testing.T) {
	type fields struct {
		AuthorizationService platform.AuthorizationService
		SessionService       platform.SessionService
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
					FindSessionFn: func(ctx context.Context, key string) (*platform.Session, error) {
						return &platform.Session{}, nil
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
					FindSessionFn: func(ctx context.Context, key string) (*platform.Session, error) {
						return nil, fmt.Errorf("session not found")
					},
				},
			},
			args: args{
				session: "abc123",
			},
			wants: wants{
				code: http.StatusForbidden,
			},
		},
		{
			name: "token provided",
			fields: fields{
				AuthorizationService: &mock.AuthorizationService{
					FindAuthorizationByTokenFn: func(ctx context.Context, token string) (*platform.Authorization, error) {
						return &platform.Authorization{}, nil
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
					FindAuthorizationByTokenFn: func(ctx context.Context, token string) (*platform.Authorization, error) {
						return nil, fmt.Errorf("authorization not found")
					},
				},
				SessionService: mock.NewSessionService(),
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
				code: http.StatusForbidden,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
			})

			h := platformhttp.NewAuthenticationHandler()
			h.AuthorizationService = tt.fields.AuthorizationService
			h.SessionService = tt.fields.SessionService
			h.Handler = handler

			w := httptest.NewRecorder()
			r := httptest.NewRequest("POST", "http://any.url", nil)

			if tt.args.session != "" {
				platformhttp.SetCookieSession(tt.args.session, r)
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
				platformhttp.SetCookieSession(tt.args.session, r)
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
				code: http.StatusForbidden,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
			})

			h := platformhttp.NewAuthenticationHandler()
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
