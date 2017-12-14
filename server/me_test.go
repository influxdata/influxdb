package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/influxdata/chronograf"
	"github.com/influxdata/chronograf/log"
	"github.com/influxdata/chronograf/mocks"
	"github.com/influxdata/chronograf/oauth2"
	"github.com/influxdata/chronograf/roles"
)

type MockUsers struct{}

func TestService_Me(t *testing.T) {
	type fields struct {
		UsersStore              chronograf.UsersStore
		OrganizationsStore      chronograf.OrganizationsStore
		Logger                  chronograf.Logger
		UseAuth                 bool
		SuperAdminFirstUserOnly bool
	}
	type args struct {
		w *httptest.ResponseRecorder
		r *http.Request
	}
	tests := []struct {
		name            string
		fields          fields
		args            args
		principal       oauth2.Principal
		wantStatus      int
		wantContentType string
		wantBody        string
	}{
		{
			name: "Existing user - not member of any organization",
			args: args{
				w: httptest.NewRecorder(),
				r: httptest.NewRequest("GET", "http://example.com/foo", nil),
			},
			fields: fields{
				UseAuth:                 true,
				SuperAdminFirstUserOnly: true,
				Logger:                  log.New(log.DebugLevel),
				OrganizationsStore: &mocks.OrganizationsStore{
					DefaultOrganizationF: func(ctx context.Context) (*chronograf.Organization, error) {
						return &chronograf.Organization{
							ID:          0,
							Name:        "Default",
							DefaultRole: roles.ViewerRoleName,
							Public:      false,
						}, nil
					},
					GetF: func(ctx context.Context, q chronograf.OrganizationQuery) (*chronograf.Organization, error) {
						switch *q.ID {
						case 0:
							return &chronograf.Organization{
								ID:          0,
								Name:        "Default",
								DefaultRole: roles.ViewerRoleName,
								Public:      false,
							}, nil
						case 1:
							return &chronograf.Organization{
								ID:     1,
								Name:   "The Bad Place",
								Public: false,
							}, nil
						}
						return nil, nil
					},
				},
				UsersStore: &mocks.UsersStore{
					NumF: func(ctx context.Context) (int, error) {
						// This function gets to verify that there is at least one first user
						return 1, nil
					},
					GetF: func(ctx context.Context, q chronograf.UserQuery) (*chronograf.User, error) {
						if q.Name == nil || q.Provider == nil || q.Scheme == nil {
							return nil, fmt.Errorf("Invalid user query: missing Name, Provider, and/or Scheme")
						}
						return &chronograf.User{
							Name:     "me",
							Provider: "github",
							Scheme:   "oauth2",
						}, nil
					},
					UpdateF: func(ctx context.Context, u *chronograf.User) error {
						return nil
					},
				},
			},
			principal: oauth2.Principal{
				Subject: "me",
				Issuer:  "github",
			},
			wantStatus:      http.StatusForbidden,
			wantContentType: "application/json",
			wantBody:        `{"code":403,"message":"This organization is private. To gain access, you must be explicitly added by an administrator."}`,
		},
		{
			name: "Existing user - private default org and user is a super admin",
			args: args{
				w: httptest.NewRecorder(),
				r: httptest.NewRequest("GET", "http://example.com/foo", nil),
			},
			fields: fields{
				UseAuth: true,
				Logger:  log.New(log.DebugLevel),
				OrganizationsStore: &mocks.OrganizationsStore{
					DefaultOrganizationF: func(ctx context.Context) (*chronograf.Organization, error) {
						return &chronograf.Organization{
							ID:          0,
							Name:        "Default",
							DefaultRole: roles.ViewerRoleName,
							Public:      false,
						}, nil
					},
					GetF: func(ctx context.Context, q chronograf.OrganizationQuery) (*chronograf.Organization, error) {
						switch *q.ID {
						case 0:
							return &chronograf.Organization{
								ID:          0,
								Name:        "Default",
								DefaultRole: roles.ViewerRoleName,
								Public:      true,
							}, nil
						case 1:
							return &chronograf.Organization{
								ID:     1,
								Name:   "The Bad Place",
								Public: true,
							}, nil
						}
						return nil, nil
					},
				},
				UsersStore: &mocks.UsersStore{
					NumF: func(ctx context.Context) (int, error) {
						// This function gets to verify that there is at least one first user
						return 1, nil
					},
					GetF: func(ctx context.Context, q chronograf.UserQuery) (*chronograf.User, error) {
						if q.Name == nil || q.Provider == nil || q.Scheme == nil {
							return nil, fmt.Errorf("Invalid user query: missing Name, Provider, and/or Scheme")
						}
						return &chronograf.User{
							Name:       "me",
							Provider:   "github",
							Scheme:     "oauth2",
							SuperAdmin: true,
						}, nil
					},
					UpdateF: func(ctx context.Context, u *chronograf.User) error {
						return nil
					},
				},
			},
			principal: oauth2.Principal{
				Subject: "me",
				Issuer:  "github",
			},
			wantStatus:      http.StatusOK,
			wantContentType: "application/json",
			wantBody:        `{"name":"me","roles":[{"name":"viewer","organization":"0"}],"provider":"github","scheme":"oauth2","superAdmin":true,"links":{"self":"/chronograf/v1/users/0"},"organizations":[{"id":"0","name":"Default","defaultRole":"viewer","public":true}],"currentOrganization":{"id":"0","name":"Default","defaultRole":"viewer","public":true}}`,
		},
		{
			name: "Existing user - private default org",
			args: args{
				w: httptest.NewRecorder(),
				r: httptest.NewRequest("GET", "http://example.com/foo", nil),
			},
			fields: fields{
				UseAuth: true,
				Logger:  log.New(log.DebugLevel),
				OrganizationsStore: &mocks.OrganizationsStore{
					DefaultOrganizationF: func(ctx context.Context) (*chronograf.Organization, error) {
						return &chronograf.Organization{
							ID:          0,
							Name:        "Default",
							DefaultRole: roles.ViewerRoleName,
							Public:      false,
						}, nil
					},
					GetF: func(ctx context.Context, q chronograf.OrganizationQuery) (*chronograf.Organization, error) {
						switch *q.ID {
						case 0:
							return &chronograf.Organization{
								ID:          0,
								Name:        "Default",
								DefaultRole: roles.ViewerRoleName,
								Public:      true,
							}, nil
						case 1:
							return &chronograf.Organization{
								ID:     1,
								Name:   "The Bad Place",
								Public: true,
							}, nil
						}
						return nil, nil
					},
				},
				UsersStore: &mocks.UsersStore{
					NumF: func(ctx context.Context) (int, error) {
						// This function gets to verify that there is at least one first user
						return 1, nil
					},
					GetF: func(ctx context.Context, q chronograf.UserQuery) (*chronograf.User, error) {
						if q.Name == nil || q.Provider == nil || q.Scheme == nil {
							return nil, fmt.Errorf("Invalid user query: missing Name, Provider, and/or Scheme")
						}
						return &chronograf.User{
							Name:     "me",
							Provider: "github",
							Scheme:   "oauth2",
						}, nil
					},
					UpdateF: func(ctx context.Context, u *chronograf.User) error {
						return nil
					},
				},
			},
			principal: oauth2.Principal{
				Subject: "me",
				Issuer:  "github",
			},
			wantStatus:      http.StatusForbidden,
			wantContentType: "application/json",
			wantBody:        `{"code":403,"message":"This organization is private. To gain access, you must be explicitly added by an administrator."}`,
		},
		{
			name: "Existing user - default org public",
			args: args{
				w: httptest.NewRecorder(),
				r: httptest.NewRequest("GET", "http://example.com/foo", nil),
			},
			fields: fields{
				UseAuth: true,
				Logger:  log.New(log.DebugLevel),
				OrganizationsStore: &mocks.OrganizationsStore{
					DefaultOrganizationF: func(ctx context.Context) (*chronograf.Organization, error) {
						return &chronograf.Organization{
							ID:          0,
							Name:        "Default",
							DefaultRole: roles.ViewerRoleName,
							Public:      true,
						}, nil
					},
					GetF: func(ctx context.Context, q chronograf.OrganizationQuery) (*chronograf.Organization, error) {
						switch *q.ID {
						case 0:
							return &chronograf.Organization{
								ID:          0,
								Name:        "Default",
								DefaultRole: roles.ViewerRoleName,
								Public:      true,
							}, nil
						case 1:
							return &chronograf.Organization{
								ID:     1,
								Name:   "The Bad Place",
								Public: true,
							}, nil
						}
						return nil, nil
					},
				},
				UsersStore: &mocks.UsersStore{
					NumF: func(ctx context.Context) (int, error) {
						// This function gets to verify that there is at least one first user
						return 1, nil
					},
					GetF: func(ctx context.Context, q chronograf.UserQuery) (*chronograf.User, error) {
						if q.Name == nil || q.Provider == nil || q.Scheme == nil {
							return nil, fmt.Errorf("Invalid user query: missing Name, Provider, and/or Scheme")
						}
						return &chronograf.User{
							Name:     "me",
							Provider: "github",
							Scheme:   "oauth2",
						}, nil
					},
					UpdateF: func(ctx context.Context, u *chronograf.User) error {
						return nil
					},
				},
			},
			principal: oauth2.Principal{
				Subject: "me",
				Issuer:  "github",
			},
			wantStatus:      http.StatusOK,
			wantContentType: "application/json",
			wantBody:        `{"name":"me","roles":[{"name":"viewer","organization":"0"}],"provider":"github","scheme":"oauth2","links":{"self":"/chronograf/v1/users/0"},"organizations":[{"id":"0","name":"Default","defaultRole":"viewer","public":true}],"currentOrganization":{"id":"0","name":"Default","defaultRole":"viewer","public":true}}`,
		},
		{
			name: "Existing user - organization doesn't exist",
			args: args{
				w: httptest.NewRecorder(),
				r: httptest.NewRequest("GET", "http://example.com/foo", nil),
			},
			fields: fields{
				UseAuth: true,
				Logger:  log.New(log.DebugLevel),
				OrganizationsStore: &mocks.OrganizationsStore{
					DefaultOrganizationF: func(ctx context.Context) (*chronograf.Organization, error) {
						return &chronograf.Organization{
							ID:          0,
							Name:        "Default",
							DefaultRole: roles.ViewerRoleName,
							Public:      true,
						}, nil
					},
					GetF: func(ctx context.Context, q chronograf.OrganizationQuery) (*chronograf.Organization, error) {
						switch *q.ID {
						case 0:
							return &chronograf.Organization{
								ID:          0,
								Name:        "Default",
								DefaultRole: roles.ViewerRoleName,
								Public:      true,
							}, nil
						}
						return nil, chronograf.ErrOrganizationNotFound
					},
				},
				UsersStore: &mocks.UsersStore{
					GetF: func(ctx context.Context, q chronograf.UserQuery) (*chronograf.User, error) {
						if q.Name == nil || q.Provider == nil || q.Scheme == nil {
							return nil, fmt.Errorf("Invalid user query: missing Name, Provider, and/or Scheme")
						}
						return &chronograf.User{
							Name:     "me",
							Provider: "github",
							Scheme:   "oauth2",
						}, nil
					},
					UpdateF: func(ctx context.Context, u *chronograf.User) error {
						return nil
					},
				},
			},
			principal: oauth2.Principal{
				Subject:      "me",
				Issuer:       "github",
				Organization: "1",
			},
			wantStatus:      http.StatusForbidden,
			wantContentType: "application/json",
			wantBody:        `{"code":403,"message":"user's current organization was not found"}`,
		},
		{
			name: "new user - default org is public",
			args: args{
				w: httptest.NewRecorder(),
				r: httptest.NewRequest("GET", "http://example.com/foo", nil),
			},
			fields: fields{
				UseAuth:                 true,
				SuperAdminFirstUserOnly: false,
				Logger:                  log.New(log.DebugLevel),
				OrganizationsStore: &mocks.OrganizationsStore{
					DefaultOrganizationF: func(ctx context.Context) (*chronograf.Organization, error) {
						return &chronograf.Organization{
							ID:          0,
							Name:        "The Gnarly Default",
							DefaultRole: roles.ViewerRoleName,
							Public:      true,
						}, nil
					},
					GetF: func(ctx context.Context, q chronograf.OrganizationQuery) (*chronograf.Organization, error) {
						return &chronograf.Organization{
							ID:          0,
							Name:        "The Gnarly Default",
							DefaultRole: roles.ViewerRoleName,
							Public:      true,
						}, nil
					},
				},
				UsersStore: &mocks.UsersStore{
					NumF: func(ctx context.Context) (int, error) {
						// This function gets to verify that there is at least one first user
						return 1, nil
					},
					GetF: func(ctx context.Context, q chronograf.UserQuery) (*chronograf.User, error) {
						if q.Name == nil || q.Provider == nil || q.Scheme == nil {
							return nil, fmt.Errorf("Invalid user query: missing Name, Provider, and/or Scheme")
						}
						return nil, chronograf.ErrUserNotFound
					},
					AddF: func(ctx context.Context, u *chronograf.User) (*chronograf.User, error) {
						return u, nil
					},
					UpdateF: func(ctx context.Context, u *chronograf.User) error {
						return nil
					},
				},
			},
			principal: oauth2.Principal{
				Subject: "secret",
				Issuer:  "auth0",
			},
			wantStatus:      http.StatusOK,
			wantContentType: "application/json",
			wantBody: `{"name":"secret","superAdmin":true,"roles":[{"name":"viewer","organization":"0"}],"provider":"auth0","scheme":"oauth2","links":{"self":"/chronograf/v1/users/0"},"organizations":[{"id":"0","name":"The Gnarly Default","defaultRole":"viewer","public":true}],"currentOrganization":{"id":"0","name":"The Gnarly Default","defaultRole":"viewer","public":true}}
`,
		},
		{
			name: "New user - New users not super admin, not first user",
			args: args{
				w: httptest.NewRecorder(),
				r: httptest.NewRequest("GET", "http://example.com/foo", nil),
			},
			fields: fields{
				UseAuth:                 true,
				SuperAdminFirstUserOnly: true,
				Logger:                  log.New(log.DebugLevel),
				OrganizationsStore: &mocks.OrganizationsStore{
					DefaultOrganizationF: func(ctx context.Context) (*chronograf.Organization, error) {
						return &chronograf.Organization{
							ID:          0,
							Name:        "The Gnarly Default",
							DefaultRole: roles.ViewerRoleName,
							Public:      true,
						}, nil
					},
					GetF: func(ctx context.Context, q chronograf.OrganizationQuery) (*chronograf.Organization, error) {
						return &chronograf.Organization{
							ID:          0,
							Name:        "The Gnarly Default",
							DefaultRole: roles.ViewerRoleName,
							Public:      true,
						}, nil
					},
				},
				UsersStore: &mocks.UsersStore{
					NumF: func(ctx context.Context) (int, error) {
						// This function gets to verify that there is at least one first user
						return 1, nil
					},
					GetF: func(ctx context.Context, q chronograf.UserQuery) (*chronograf.User, error) {
						if q.Name == nil || q.Provider == nil || q.Scheme == nil {
							return nil, fmt.Errorf("Invalid user query: missing Name, Provider, and/or Scheme")
						}
						return nil, chronograf.ErrUserNotFound
					},
					AddF: func(ctx context.Context, u *chronograf.User) (*chronograf.User, error) {
						return u, nil
					},
					UpdateF: func(ctx context.Context, u *chronograf.User) error {
						return nil
					},
				},
			},
			principal: oauth2.Principal{
				Subject: "secret",
				Issuer:  "auth0",
			},
			wantStatus:      http.StatusOK,
			wantContentType: "application/json",
			wantBody: `{"name":"secret","roles":[{"name":"viewer","organization":"0"}],"provider":"auth0","scheme":"oauth2","links":{"self":"/chronograf/v1/users/0"},"organizations":[{"id":"0","name":"The Gnarly Default","public":true,"defaultRole":"viewer"}],"currentOrganization":{"id":"0","name":"The Gnarly Default","public":true,"defaultRole":"viewer"}}
`,
		},
		{
			name: "New user - New users not super admin, first user",
			args: args{
				w: httptest.NewRecorder(),
				r: httptest.NewRequest("GET", "http://example.com/foo", nil),
			},
			fields: fields{
				UseAuth:                 true,
				SuperAdminFirstUserOnly: true,
				Logger:                  log.New(log.DebugLevel),
				OrganizationsStore: &mocks.OrganizationsStore{
					DefaultOrganizationF: func(ctx context.Context) (*chronograf.Organization, error) {
						return &chronograf.Organization{
							ID:          0,
							Name:        "The Gnarly Default",
							DefaultRole: roles.ViewerRoleName,
							Public:      true,
						}, nil
					},
					GetF: func(ctx context.Context, q chronograf.OrganizationQuery) (*chronograf.Organization, error) {
						return &chronograf.Organization{
							ID:          0,
							Name:        "The Gnarly Default",
							DefaultRole: roles.ViewerRoleName,
							Public:      true,
						}, nil
					},
				},
				UsersStore: &mocks.UsersStore{
					NumF: func(ctx context.Context) (int, error) {
						// This function gets to verify that there is at least one first user
						return 0, nil
					},
					GetF: func(ctx context.Context, q chronograf.UserQuery) (*chronograf.User, error) {
						if q.Name == nil || q.Provider == nil || q.Scheme == nil {
							return nil, fmt.Errorf("Invalid user query: missing Name, Provider, and/or Scheme")
						}
						return nil, chronograf.ErrUserNotFound
					},
					AddF: func(ctx context.Context, u *chronograf.User) (*chronograf.User, error) {
						return u, nil
					},
					UpdateF: func(ctx context.Context, u *chronograf.User) error {
						return nil
					},
				},
			},
			principal: oauth2.Principal{
				Subject: "secret",
				Issuer:  "auth0",
			},
			wantStatus:      http.StatusOK,
			wantContentType: "application/json",
			wantBody: `{"name":"secret","superAdmin":true,"roles":[{"name":"viewer","organization":"0"}],"provider":"auth0","scheme":"oauth2","links":{"self":"/chronograf/v1/users/0"},"organizations":[{"id":"0","name":"The Gnarly Default","public":true,"defaultRole":"viewer"}],"currentOrganization":{"id":"0","name":"The Gnarly Default","public":true,"defaultRole":"viewer"}}
`,
		},
		{
			name: "Error adding user",
			args: args{
				w: httptest.NewRecorder(),
				r: httptest.NewRequest("GET", "http://example.com/foo", nil),
			},
			fields: fields{
				UseAuth:                 true,
				SuperAdminFirstUserOnly: true,
				OrganizationsStore: &mocks.OrganizationsStore{
					DefaultOrganizationF: func(ctx context.Context) (*chronograf.Organization, error) {
						return &chronograf.Organization{
							ID:     0,
							Public: true,
						}, nil
					},
					GetF: func(ctx context.Context, q chronograf.OrganizationQuery) (*chronograf.Organization, error) {
						return &chronograf.Organization{
							ID:     0,
							Name:   "The Bad Place",
							Public: true,
						}, nil
					},
				},
				UsersStore: &mocks.UsersStore{
					NumF: func(ctx context.Context) (int, error) {
						// This function gets to verify that there is at least one first user
						return 1, nil
					},
					GetF: func(ctx context.Context, q chronograf.UserQuery) (*chronograf.User, error) {
						return nil, chronograf.ErrUserNotFound
					},
					AddF: func(ctx context.Context, u *chronograf.User) (*chronograf.User, error) {
						return nil, fmt.Errorf("Why Heavy?")
					},
					UpdateF: func(ctx context.Context, u *chronograf.User) error {
						return nil
					},
				},
				Logger: log.New(log.DebugLevel),
			},
			principal: oauth2.Principal{
				Subject: "secret",
				Issuer:  "heroku",
			},
			wantStatus:      http.StatusInternalServerError,
			wantContentType: "application/json",
			wantBody:        `{"code":500,"message":"Unknown error: error storing user secret: Why Heavy?"}`,
		},
		{
			name: "No Auth",
			args: args{
				w: httptest.NewRecorder(),
				r: httptest.NewRequest("GET", "http://example.com/foo", nil),
			},
			fields: fields{
				UseAuth:                 false,
				SuperAdminFirstUserOnly: true,
				Logger:                  log.New(log.DebugLevel),
			},
			wantStatus:      http.StatusOK,
			wantContentType: "application/json",
			wantBody: `{"links":{"self":"/chronograf/v1/users/me"}}
`,
		},
		{
			name: "Empty Principal",
			args: args{
				w: httptest.NewRecorder(),
				r: httptest.NewRequest("GET", "http://example.com/foo", nil),
			},
			fields: fields{
				UseAuth:                 true,
				SuperAdminFirstUserOnly: true,
				Logger:                  log.New(log.DebugLevel),
			},
			wantStatus: http.StatusUnprocessableEntity,
			principal: oauth2.Principal{
				Subject: "",
				Issuer:  "",
			},
		},
		{
			name: "new user - default org is private",
			args: args{
				w: httptest.NewRecorder(),
				r: httptest.NewRequest("GET", "http://example.com/foo", nil),
			},
			fields: fields{
				UseAuth: true,
				Logger:  log.New(log.DebugLevel),
				OrganizationsStore: &mocks.OrganizationsStore{
					DefaultOrganizationF: func(ctx context.Context) (*chronograf.Organization, error) {
						return &chronograf.Organization{
							ID:          0,
							Name:        "The Bad Place",
							DefaultRole: roles.MemberRoleName,
							Public:      false,
						}, nil
					},
				},
				UsersStore: &mocks.UsersStore{
					NumF: func(ctx context.Context) (int, error) {
						// This function gets to verify that there is at least one first user
						return 1, nil
					},
					GetF: func(ctx context.Context, q chronograf.UserQuery) (*chronograf.User, error) {
						if q.Name == nil || q.Provider == nil || q.Scheme == nil {
							return nil, fmt.Errorf("Invalid user query: missing Name, Provider, and/or Scheme")
						}
						return nil, chronograf.ErrUserNotFound
					},
					AddF: func(ctx context.Context, u *chronograf.User) (*chronograf.User, error) {
						return u, nil
					},
					UpdateF: func(ctx context.Context, u *chronograf.User) error {
						return nil
					},
				},
			},
			principal: oauth2.Principal{
				Subject: "secret",
				Issuer:  "auth0",
			},
			wantStatus:      http.StatusForbidden,
			wantContentType: "application/json",
			wantBody:        `{"code":403,"message":"This organization is private. To gain access, you must be explicitly added by an administrator."}`,
		},
	}
	for _, tt := range tests {
		tt.args.r = tt.args.r.WithContext(context.WithValue(context.Background(), oauth2.PrincipalKey, tt.principal))
		s := &Service{
			Store: &mocks.Store{
				UsersStore:         tt.fields.UsersStore,
				OrganizationsStore: tt.fields.OrganizationsStore,
			},
			Logger:                  tt.fields.Logger,
			UseAuth:                 tt.fields.UseAuth,
			SuperAdminFirstUserOnly: tt.fields.SuperAdminFirstUserOnly,
		}

		s.Me(tt.args.w, tt.args.r)

		resp := tt.args.w.Result()
		content := resp.Header.Get("Content-Type")
		body, _ := ioutil.ReadAll(resp.Body)

		if resp.StatusCode != tt.wantStatus {
			t.Errorf("%q. Me() = %v, want %v", tt.name, resp.StatusCode, tt.wantStatus)
		}
		if tt.wantContentType != "" && content != tt.wantContentType {
			t.Errorf("%q. Me() = %v, want %v", tt.name, content, tt.wantContentType)
		}
		if tt.wantBody == "" {
			continue
		}
		if eq, err := jsonEqual(tt.wantBody, string(body)); err != nil || !eq {
			t.Errorf("%q. Me() = \n***%v***\n,\nwant\n***%v***", tt.name, string(body), tt.wantBody)
		}
	}
}

func TestService_UpdateMe(t *testing.T) {
	type fields struct {
		UsersStore         chronograf.UsersStore
		OrganizationsStore chronograf.OrganizationsStore
		Logger             chronograf.Logger
		UseAuth            bool
	}
	type args struct {
		w         *httptest.ResponseRecorder
		r         *http.Request
		meRequest *meRequest
		auth      mocks.Authenticator
	}
	tests := []struct {
		name            string
		fields          fields
		args            args
		principal       oauth2.Principal
		wantStatus      int
		wantContentType string
		wantBody        string
	}{
		{
			name: "Set the current User's organization",
			args: args{
				w: httptest.NewRecorder(),
				r: httptest.NewRequest("GET", "http://example.com/foo", nil),
				meRequest: &meRequest{
					Organization: "1337",
				},
				auth: mocks.Authenticator{},
			},
			fields: fields{
				UseAuth: true,
				Logger:  log.New(log.DebugLevel),
				UsersStore: &mocks.UsersStore{
					GetF: func(ctx context.Context, q chronograf.UserQuery) (*chronograf.User, error) {
						if q.Name == nil || q.Provider == nil || q.Scheme == nil {
							return nil, fmt.Errorf("Invalid user query: missing Name, Provider, and/or Scheme")
						}
						return &chronograf.User{
							Name:     "me",
							Provider: "github",
							Scheme:   "oauth2",
							Roles: []chronograf.Role{
								{
									Name:         roles.AdminRoleName,
									Organization: "1337",
								},
							},
						}, nil
					},
					UpdateF: func(ctx context.Context, u *chronograf.User) error {
						return nil
					},
				},
				OrganizationsStore: &mocks.OrganizationsStore{
					DefaultOrganizationF: func(ctx context.Context) (*chronograf.Organization, error) {
						return &chronograf.Organization{
							ID:          0,
							Name:        "Default",
							DefaultRole: roles.AdminRoleName,
							Public:      true,
						}, nil
					},
					GetF: func(ctx context.Context, q chronograf.OrganizationQuery) (*chronograf.Organization, error) {
						if q.ID == nil {
							return nil, fmt.Errorf("Invalid organization query: missing ID")
						}
						switch *q.ID {
						case 0:
							return &chronograf.Organization{
								ID:          0,
								Name:        "Default",
								DefaultRole: roles.AdminRoleName,
								Public:      true,
							}, nil
						case 1337:
							return &chronograf.Organization{
								ID:     1337,
								Name:   "The ShillBillThrilliettas",
								Public: true,
							}, nil
						}
						return nil, nil
					},
				},
			},
			principal: oauth2.Principal{
				Subject: "me",
				Issuer:  "github",
			},
			wantStatus:      http.StatusOK,
			wantContentType: "application/json",
			wantBody:        `{"name":"me","roles":[{"name":"admin","organization":"1337"},{"name":"admin","organization":"0"}],"provider":"github","scheme":"oauth2","links":{"self":"/chronograf/v1/users/0"},"organizations":[{"id":"0","name":"Default","defaultRole":"admin","public":true},{"id":"1337","name":"The ShillBillThrilliettas","public":true}],"currentOrganization":{"id":"1337","name":"The ShillBillThrilliettas","public":true}}`,
		},
		{
			name: "Change the current User's organization",
			args: args{
				w: httptest.NewRecorder(),
				r: httptest.NewRequest("GET", "http://example.com/foo", nil),
				meRequest: &meRequest{
					Organization: "1337",
				},
				auth: mocks.Authenticator{},
			},
			fields: fields{
				UseAuth: true,
				Logger:  log.New(log.DebugLevel),
				UsersStore: &mocks.UsersStore{
					GetF: func(ctx context.Context, q chronograf.UserQuery) (*chronograf.User, error) {
						if q.Name == nil || q.Provider == nil || q.Scheme == nil {
							return nil, fmt.Errorf("Invalid user query: missing Name, Provider, and/or Scheme")
						}
						return &chronograf.User{
							Name:     "me",
							Provider: "github",
							Scheme:   "oauth2",
							Roles: []chronograf.Role{
								{
									Name:         roles.AdminRoleName,
									Organization: "1337",
								},
							},
						}, nil
					},
					UpdateF: func(ctx context.Context, u *chronograf.User) error {
						return nil
					},
				},
				OrganizationsStore: &mocks.OrganizationsStore{
					DefaultOrganizationF: func(ctx context.Context) (*chronograf.Organization, error) {
						return &chronograf.Organization{
							ID:          0,
							Name:        "Default",
							DefaultRole: roles.EditorRoleName,
							Public:      true,
						}, nil
					},
					GetF: func(ctx context.Context, q chronograf.OrganizationQuery) (*chronograf.Organization, error) {
						if q.ID == nil {
							return nil, fmt.Errorf("Invalid organization query: missing ID")
						}
						switch *q.ID {
						case 1337:
							return &chronograf.Organization{
								ID:     1337,
								Name:   "The ThrillShilliettos",
								Public: false,
							}, nil
						case 0:
							return &chronograf.Organization{
								ID:          0,
								Name:        "Default",
								DefaultRole: roles.EditorRoleName,
								Public:      true,
							}, nil
						}
						return nil, nil
					},
				},
			},
			principal: oauth2.Principal{
				Subject:      "me",
				Issuer:       "github",
				Organization: "1338",
			},
			wantStatus:      http.StatusOK,
			wantContentType: "application/json",
			wantBody:        `{"name":"me","roles":[{"name":"admin","organization":"1337"},{"name":"editor","organization":"0"}],"provider":"github","scheme":"oauth2","links":{"self":"/chronograf/v1/users/0"},"organizations":[{"id":"0","name":"Default","defaultRole":"editor","public":true},{"id":"1337","name":"The ThrillShilliettos","public":false}],"currentOrganization":{"id":"1337","name":"The ThrillShilliettos","public":false}}`,
		},
		{
			name: "Unable to find requested user in valid organization",
			args: args{
				w: httptest.NewRecorder(),
				r: httptest.NewRequest("GET", "http://example.com/foo", nil),
				meRequest: &meRequest{
					Organization: "1337",
				},
				auth: mocks.Authenticator{},
			},
			fields: fields{
				UseAuth: true,
				Logger:  log.New(log.DebugLevel),
				UsersStore: &mocks.UsersStore{
					GetF: func(ctx context.Context, q chronograf.UserQuery) (*chronograf.User, error) {
						if q.Name == nil || q.Provider == nil || q.Scheme == nil {
							return nil, fmt.Errorf("Invalid user query: missing Name, Provider, and/or Scheme")
						}
						return &chronograf.User{
							Name:     "me",
							Provider: "github",
							Scheme:   "oauth2",
							Roles: []chronograf.Role{
								{
									Name:         roles.AdminRoleName,
									Organization: "1338",
								},
							},
						}, nil
					},
					UpdateF: func(ctx context.Context, u *chronograf.User) error {
						return nil
					},
				},
				OrganizationsStore: &mocks.OrganizationsStore{
					DefaultOrganizationF: func(ctx context.Context) (*chronograf.Organization, error) {
						return &chronograf.Organization{
							ID: 0,
						}, nil
					},
					GetF: func(ctx context.Context, q chronograf.OrganizationQuery) (*chronograf.Organization, error) {
						if q.ID == nil {
							return nil, fmt.Errorf("Invalid organization query: missing ID")
						}
						return &chronograf.Organization{
							ID:     1337,
							Name:   "The ShillBillThrilliettas",
							Public: true,
						}, nil
					},
				},
			},
			principal: oauth2.Principal{
				Subject:      "me",
				Issuer:       "github",
				Organization: "1338",
			},
			wantStatus:      http.StatusForbidden,
			wantContentType: "application/json",
			wantBody:        `{"code":403,"message":"user not found"}`,
		},
		{
			name: "Unable to find requested organization",
			args: args{
				w: httptest.NewRecorder(),
				r: httptest.NewRequest("GET", "http://example.com/foo", nil),
				meRequest: &meRequest{
					Organization: "1337",
				},
				auth: mocks.Authenticator{},
			},
			fields: fields{
				UseAuth: true,
				Logger:  log.New(log.DebugLevel),
				UsersStore: &mocks.UsersStore{
					GetF: func(ctx context.Context, q chronograf.UserQuery) (*chronograf.User, error) {
						if q.Name == nil || q.Provider == nil || q.Scheme == nil {
							return nil, fmt.Errorf("Invalid user query: missing Name, Provider, and/or Scheme")
						}
						return &chronograf.User{
							Name:     "me",
							Provider: "github",
							Scheme:   "oauth2",
							Roles: []chronograf.Role{
								{
									Name:         roles.AdminRoleName,
									Organization: "1337",
								},
							},
						}, nil
					},
					UpdateF: func(ctx context.Context, u *chronograf.User) error {
						return nil
					},
				},
				OrganizationsStore: &mocks.OrganizationsStore{
					DefaultOrganizationF: func(ctx context.Context) (*chronograf.Organization, error) {
						return &chronograf.Organization{
							ID: 0,
						}, nil
					},
					GetF: func(ctx context.Context, q chronograf.OrganizationQuery) (*chronograf.Organization, error) {
						return nil, chronograf.ErrOrganizationNotFound
					},
				},
			},
			principal: oauth2.Principal{
				Subject:      "me",
				Issuer:       "github",
				Organization: "1338",
			},
			wantStatus:      http.StatusBadRequest,
			wantContentType: "application/json",
			wantBody:        `{"code":400,"message":"organization not found"}`,
		},
	}
	for _, tt := range tests {
		tt.args.r = tt.args.r.WithContext(context.WithValue(context.Background(), oauth2.PrincipalKey, tt.principal))
		s := &Service{
			Store: &Store{
				UsersStore:         tt.fields.UsersStore,
				OrganizationsStore: tt.fields.OrganizationsStore,
			},
			Logger:  tt.fields.Logger,
			UseAuth: tt.fields.UseAuth,
		}

		buf, _ := json.Marshal(tt.args.meRequest)
		tt.args.r.Body = ioutil.NopCloser(bytes.NewReader(buf))
		tt.args.auth.Principal = tt.principal

		s.UpdateMe(&tt.args.auth)(tt.args.w, tt.args.r)

		resp := tt.args.w.Result()
		content := resp.Header.Get("Content-Type")
		body, _ := ioutil.ReadAll(resp.Body)

		if resp.StatusCode != tt.wantStatus {
			t.Errorf("%q. UpdateMe() = %v, want %v", tt.name, resp.StatusCode, tt.wantStatus)
		}
		if tt.wantContentType != "" && content != tt.wantContentType {
			t.Errorf("%q. UpdateMe() = %v, want %v", tt.name, content, tt.wantContentType)
		}
		if eq, err := jsonEqual(tt.wantBody, string(body)); err != nil || !eq {
			t.Errorf("%q. UpdateMe() = \n***%v***\n,\nwant\n***%v***", tt.name, string(body), tt.wantBody)
		}
	}
}
