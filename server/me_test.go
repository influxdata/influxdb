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
)

type MockUsers struct{}

func TestService_Me(t *testing.T) {
	type fields struct {
		UsersStore         chronograf.UsersStore
		OrganizationsStore chronograf.OrganizationsStore
		Logger             chronograf.Logger
		UseAuth            bool
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
			name: "Existing user",
			args: args{
				w: httptest.NewRecorder(),
				r: httptest.NewRequest("GET", "http://example.com/foo", nil),
			},
			fields: fields{
				UseAuth: true,
				Logger:  log.New(log.DebugLevel),
				OrganizationsStore: &mocks.OrganizationsStore{
					GetF: func(ctx context.Context, q chronograf.OrganizationQuery) (*chronograf.Organization, error) {
						return &chronograf.Organization{
							ID:   0,
							Name: "The Bad Place",
						}, nil
					},
				},
				UsersStore: &mocks.UsersStore{
					AllF: func(ctx context.Context) ([]chronograf.User, error) {
						// This function gets to verify that there is at least one first user
						return []chronograf.User{{}}, nil
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
				},
			},
			principal: oauth2.Principal{
				Subject: "me",
				Issuer:  "github",
			},
			wantStatus:      http.StatusOK,
			wantContentType: "application/json",
			wantBody: `{"name":"me","provider":"github","scheme":"oauth2","links":{"self":"/chronograf/v1/users/0"},"currentOrganization":{"id":"0","name":"The Bad Place"}}
`,
		},
		{
			name: "New user",
			args: args{
				w: httptest.NewRecorder(),
				r: httptest.NewRequest("GET", "http://example.com/foo", nil),
			},
			fields: fields{
				UseAuth: true,
				Logger:  log.New(log.DebugLevel),
				OrganizationsStore: &mocks.OrganizationsStore{
					GetF: func(ctx context.Context, q chronograf.OrganizationQuery) (*chronograf.Organization, error) {
						return &chronograf.Organization{
							ID:   0,
							Name: "The Bad Place",
						}, nil
					},
				},
				UsersStore: &mocks.UsersStore{
					AllF: func(ctx context.Context) ([]chronograf.User, error) {
						// This function gets to verify that there is at least one first user
						return []chronograf.User{{}}, nil
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
				},
			},
			principal: oauth2.Principal{
				Subject: "secret",
				Issuer:  "auth0",
			},
			wantStatus:      http.StatusOK,
			wantContentType: "application/json",
			wantBody: `{"name":"secret","roles":[{"name":"member","organization":"0"}],"provider":"auth0","scheme":"oauth2","links":{"self":"/chronograf/v1/users/0"},"organizations":[{"id":"0","name":"The Bad Place"}],"currentOrganization":{"id":"0","name":"The Bad Place"}}
`,
		},
		{
			name: "Error adding user",
			args: args{
				w: httptest.NewRecorder(),
				r: httptest.NewRequest("GET", "http://example.com/foo", nil),
			},
			fields: fields{
				UseAuth: true,
				OrganizationsStore: &mocks.OrganizationsStore{
					GetF: func(ctx context.Context, q chronograf.OrganizationQuery) (*chronograf.Organization, error) {
						return &chronograf.Organization{
							ID:   0,
							Name: "The Bad Place",
						}, nil
					},
				},
				UsersStore: &mocks.UsersStore{
					AllF: func(ctx context.Context) ([]chronograf.User, error) {
						// This function gets to verify that there is at least one first user
						return []chronograf.User{{}}, nil
					},
					GetF: func(ctx context.Context, q chronograf.UserQuery) (*chronograf.User, error) {
						return nil, chronograf.ErrUserNotFound
					},
					AddF: func(ctx context.Context, u *chronograf.User) (*chronograf.User, error) {
						return nil, fmt.Errorf("Why Heavy?")
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
				UseAuth: false,
				Logger:  log.New(log.DebugLevel),
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
				UseAuth: true,
				Logger:  log.New(log.DebugLevel),
			},
			wantStatus: http.StatusUnprocessableEntity,
			principal: oauth2.Principal{
				Subject: "",
				Issuer:  "",
			},
		},
	}
	for _, tt := range tests {
		tt.args.r = tt.args.r.WithContext(context.WithValue(context.Background(), oauth2.PrincipalKey, tt.principal))
		s := &Service{
			Store: &mocks.Store{
				UsersStore:         tt.fields.UsersStore,
				OrganizationsStore: tt.fields.OrganizationsStore,
			},
			Logger:  tt.fields.Logger,
			UseAuth: tt.fields.UseAuth,
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
		if tt.wantBody != "" && string(body) != tt.wantBody {
			t.Errorf("%q. Me() = \n***%v***\n,\nwant\n***%v***", tt.name, string(body), tt.wantBody)
		}
	}
}

func TestService_MeOrganizations(t *testing.T) {
	type fields struct {
		UsersStore         chronograf.UsersStore
		OrganizationsStore chronograf.OrganizationsStore
		Logger             chronograf.Logger
		UseAuth            bool
	}
	type args struct {
		w          *httptest.ResponseRecorder
		r          *http.Request
		orgRequest *meOrganizationRequest
		auth       mocks.Authenticator
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
				orgRequest: &meOrganizationRequest{
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
									Name:         AdminRoleName,
									Organization: "1337",
								},
							},
						}, nil
					},
				},
				OrganizationsStore: &mocks.OrganizationsStore{
					GetF: func(ctx context.Context, q chronograf.OrganizationQuery) (*chronograf.Organization, error) {
						if q.ID == nil {
							return nil, fmt.Errorf("Invalid organization query: missing ID")
						}
						return &chronograf.Organization{
							ID:   1337,
							Name: "The ShillBillThrilliettas",
						}, nil
					},
				},
			},
			principal: oauth2.Principal{
				Subject: "me",
				Issuer:  "github",
			},
			wantStatus:      http.StatusOK,
			wantContentType: "application/json",
			wantBody:        `{"name":"me","roles":[{"name":"admin","organization":"1337"}],"provider":"github","scheme":"oauth2","links":{"self":"/chronograf/v1/users/0"},"organizations":[{"id":"1337","name":"The ShillBillThrilliettas"}],"currentOrganization":{"id":"1337","name":"The ShillBillThrilliettas"}}`,
		},
		{
			name: "Change the current User's organization",
			args: args{
				w: httptest.NewRecorder(),
				r: httptest.NewRequest("GET", "http://example.com/foo", nil),
				orgRequest: &meOrganizationRequest{
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
									Name:         AdminRoleName,
									Organization: "1337",
								},
							},
						}, nil
					},
				},
				OrganizationsStore: &mocks.OrganizationsStore{
					GetF: func(ctx context.Context, q chronograf.OrganizationQuery) (*chronograf.Organization, error) {
						if q.ID == nil {
							return nil, fmt.Errorf("Invalid organization query: missing ID")
						}
						return &chronograf.Organization{
							ID:   1337,
							Name: "The ThrillShilliettos",
						}, nil
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
			wantBody: `{"name":"me","roles":[{"name":"admin","organization":"1337"}],"provider":"github","scheme":"oauth2","links":{"self":"/chronograf/v1/users/0"},"organizations":[{"id":"1337","name":"The ThrillShilliettos"}],"currentOrganization":{"id":"1337","name":"The ThrillShilliettos"}}
`,
		},
		{
			name: "Unable to find requested user in valid organization",
			args: args{
				w: httptest.NewRecorder(),
				r: httptest.NewRequest("GET", "http://example.com/foo", nil),
				orgRequest: &meOrganizationRequest{
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
						return nil, chronograf.ErrUserNotFound
					},
				},
				OrganizationsStore: &mocks.OrganizationsStore{
					GetF: func(ctx context.Context, q chronograf.OrganizationQuery) (*chronograf.Organization, error) {
						if q.ID == nil {
							return nil, fmt.Errorf("Invalid organization query: missing ID")
						}
						return &chronograf.Organization{
							ID:   1337,
							Name: "The ShillBillThrilliettas",
						}, nil
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
			wantBody:        `{"code":400,"message":"user not found"}`,
		},
		{
			name: "Unable to find requested organization",
			args: args{
				w: httptest.NewRecorder(),
				r: httptest.NewRequest("GET", "http://example.com/foo", nil),
				orgRequest: &meOrganizationRequest{
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
									Name:         AdminRoleName,
									Organization: "1337",
								},
							},
						}, nil
					},
				},
				OrganizationsStore: &mocks.OrganizationsStore{
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
			//UsersStore:             tt.fields.UsersStore,
			//OrganizationUsersStore: tt.fields.OrganizationUsersStore,
			Logger:  tt.fields.Logger,
			UseAuth: tt.fields.UseAuth,
		}

		buf, _ := json.Marshal(tt.args.orgRequest)
		tt.args.r.Body = ioutil.NopCloser(bytes.NewReader(buf))
		tt.args.auth.Principal = tt.principal

		s.MeOrganization(&tt.args.auth)(tt.args.w, tt.args.r)

		resp := tt.args.w.Result()
		content := resp.Header.Get("Content-Type")
		body, _ := ioutil.ReadAll(resp.Body)

		if resp.StatusCode != tt.wantStatus {
			t.Errorf("%q. Me() = %v, want %v", tt.name, resp.StatusCode, tt.wantStatus)
		}
		if tt.wantContentType != "" && content != tt.wantContentType {
			t.Errorf("%q. Me() = %v, want %v", tt.name, content, tt.wantContentType)
		}
		if eq, err := jsonEqual(tt.wantBody, string(body)); err != nil || !eq {
			t.Errorf("%q. Me() = \n***%v***\n,\nwant\n***%v***", tt.name, string(body), tt.wantBody)
		}
	}
}
