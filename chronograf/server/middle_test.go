package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/bouk/httprouter"
	"github.com/influxdata/influxdb/chronograf"
	"github.com/influxdata/influxdb/chronograf/mocks"
	"github.com/influxdata/influxdb/chronograf/oauth2"
)

func TestRouteMatchesPrincipal(t *testing.T) {
	type fields struct {
		OrganizationsStore chronograf.OrganizationsStore
		Logger             chronograf.Logger
	}
	type args struct {
		useAuth      bool
		principal    *oauth2.Principal
		routerParams *httprouter.Params
	}
	type wants struct {
		matches bool
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		wants  wants
	}{
		{
			name: "route matches request params",
			fields: fields{
				Logger: &chronograf.NoopLogger{},
				OrganizationsStore: &mocks.OrganizationsStore{
					DefaultOrganizationF: func(ctx context.Context) (*chronograf.Organization, error) {
						return &chronograf.Organization{
							ID: "default",
						}, nil
					},
				},
			},
			args: args{
				useAuth: true,
				principal: &oauth2.Principal{
					Subject:      "user",
					Issuer:       "github",
					Organization: "default",
				},
				routerParams: &httprouter.Params{
					{
						Key:   "oid",
						Value: "default",
					},
				},
			},
			wants: wants{
				matches: true,
			},
		},
		{
			name: "route does not match request params",
			fields: fields{
				Logger: &chronograf.NoopLogger{},
				OrganizationsStore: &mocks.OrganizationsStore{
					DefaultOrganizationF: func(ctx context.Context) (*chronograf.Organization, error) {
						return &chronograf.Organization{
							ID: "default",
						}, nil
					},
				},
			},
			args: args{
				useAuth: true,
				principal: &oauth2.Principal{
					Subject:      "user",
					Issuer:       "github",
					Organization: "default",
				},
				routerParams: &httprouter.Params{
					{
						Key:   "oid",
						Value: "other",
					},
				},
			},
			wants: wants{
				matches: false,
			},
		},
		{
			name: "missing principal",
			fields: fields{
				Logger: &chronograf.NoopLogger{},
				OrganizationsStore: &mocks.OrganizationsStore{
					DefaultOrganizationF: func(ctx context.Context) (*chronograf.Organization, error) {
						return &chronograf.Organization{
							ID: "default",
						}, nil
					},
				},
			},
			args: args{
				useAuth:   true,
				principal: nil,
				routerParams: &httprouter.Params{
					{
						Key:   "oid",
						Value: "other",
					},
				},
			},
			wants: wants{
				matches: false,
			},
		},
		{
			name: "not using auth",
			fields: fields{
				Logger: &chronograf.NoopLogger{},
				OrganizationsStore: &mocks.OrganizationsStore{
					DefaultOrganizationF: func(ctx context.Context) (*chronograf.Organization, error) {
						return &chronograf.Organization{
							ID: "default",
						}, nil
					},
				},
			},
			args: args{
				useAuth: false,
				principal: &oauth2.Principal{
					Subject:      "user",
					Issuer:       "github",
					Organization: "default",
				},
				routerParams: &httprouter.Params{
					{
						Key:   "oid",
						Value: "other",
					},
				},
			},
			wants: wants{
				matches: true,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := &mocks.Store{
				OrganizationsStore: tt.fields.OrganizationsStore,
			}
			var matches bool
			next := func(w http.ResponseWriter, r *http.Request) {
				matches = true
			}
			fn := RouteMatchesPrincipal(
				store,
				tt.args.useAuth,
				tt.fields.Logger,
				next,
			)

			w := httptest.NewRecorder()
			url := "http://any.url"
			r := httptest.NewRequest(
				"GET",
				url,
				nil,
			)
			if tt.args.routerParams != nil {
				r = r.WithContext(httprouter.WithParams(r.Context(), *tt.args.routerParams))
			}
			if tt.args.principal == nil {
				r = r.WithContext(context.WithValue(r.Context(), oauth2.PrincipalKey, nil))
			} else {
				r = r.WithContext(context.WithValue(r.Context(), oauth2.PrincipalKey, *tt.args.principal))
			}
			fn(w, r)

			if matches != tt.wants.matches {
				t.Errorf("%q. RouteMatchesPrincipal() = %v, expected %v", tt.name, matches, tt.wants.matches)
			}

			if !matches && w.Code != http.StatusForbidden {
				t.Errorf("%q. RouteMatchesPrincipal() Status Code = %v, expected %v", tt.name, w.Code, http.StatusForbidden)
			}

		})
	}
}
