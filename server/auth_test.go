package server_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/influxdata/chronograf"
	clog "github.com/influxdata/chronograf/log"
	"github.com/influxdata/chronograf/mocks"
	"github.com/influxdata/chronograf/oauth2"
	"github.com/influxdata/chronograf/server"
)

func TestAuthorizedToken(t *testing.T) {
	var tests = []struct {
		Desc        string
		Code        int
		Principal   oauth2.Principal
		ValidateErr error
		Expected    string
	}{
		{
			Desc:        "Error in validate",
			Code:        http.StatusForbidden,
			ValidateErr: errors.New("error"),
		},
		{
			Desc: "Authorized ok",
			Code: http.StatusOK,
			Principal: oauth2.Principal{
				Subject: "Principal Strickland",
			},
			Expected: "Principal Strickland",
		},
	}
	for _, test := range tests {
		// next is a sentinel StatusOK and
		// principal recorder.
		var principal oauth2.Principal
		next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			principal = r.Context().Value(oauth2.PrincipalKey).(oauth2.Principal)
		})
		req, _ := http.NewRequest("GET", "", nil)
		w := httptest.NewRecorder()

		a := &mocks.Authenticator{
			Principal:   test.Principal,
			ValidateErr: test.ValidateErr,
		}

		logger := clog.New(clog.DebugLevel)
		handler := server.AuthorizedToken(a, logger, next)
		handler.ServeHTTP(w, req)
		if w.Code != test.Code {
			t.Errorf("Status code expected: %d actual %d", test.Code, w.Code)
		} else if principal != test.Principal {
			t.Errorf("Principal mismatch expected: %s actual %s", test.Principal, principal)
		}
	}
}

func TestAuthorizedUser(t *testing.T) {
	type fields struct {
		UsersStore         chronograf.UsersStore
		OrganizationsStore chronograf.OrganizationsStore
		Logger             chronograf.Logger
	}
	type args struct {
		principal *oauth2.Principal
		scheme    string
		useAuth   bool
		role      string
	}
	tests := []struct {
		name       string
		fields     fields
		args       args
		authorized bool
	}{
		{
			name: "Not using auth",
			fields: fields{
				UsersStore:         &mocks.UsersStore{},
				OrganizationsStore: &mocks.OrganizationsStore{},
				Logger:             clog.New(clog.DebugLevel),
			},
			args: args{
				useAuth: false,
			},
			authorized: true,
		},
		{
			name: "User with viewer role is viewer authorized",
			fields: fields{
				UsersStore: &mocks.UsersStore{
					GetF: func(ctx context.Context, q chronograf.UserQuery) (*chronograf.User, error) {
						if q.Name == nil || q.Provider == nil || q.Scheme == nil {
							return nil, fmt.Errorf("Invalid user query: missing Name, Provider, and/or Scheme")
						}
						return &chronograf.User{
							ID:       1337,
							Name:     "billysteve",
							Provider: "google",
							Scheme:   "oauth2",
							Roles: []chronograf.Role{
								{
									Name:         server.ViewerRoleName,
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
				Logger: clog.New(clog.DebugLevel),
			},
			args: args{
				principal: &oauth2.Principal{
					Subject:      "billysteve",
					Issuer:       "google",
					Organization: "1337",
				},
				scheme:  "oauth2",
				role:    "viewer",
				useAuth: true,
			},
			authorized: true,
		},
		{
			name: "User with editor role is viewer authorized",
			fields: fields{
				UsersStore: &mocks.UsersStore{
					GetF: func(ctx context.Context, q chronograf.UserQuery) (*chronograf.User, error) {
						if q.Name == nil || q.Provider == nil || q.Scheme == nil {
							return nil, fmt.Errorf("Invalid user query: missing Name, Provider, and/or Scheme")
						}
						return &chronograf.User{
							ID:       1337,
							Name:     "billysteve",
							Provider: "google",
							Scheme:   "oauth2",
							Roles: []chronograf.Role{
								{
									Name:         server.EditorRoleName,
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
				Logger: clog.New(clog.DebugLevel),
			},
			args: args{
				principal: &oauth2.Principal{
					Subject:      "billysteve",
					Issuer:       "google",
					Organization: "1337",
				},
				scheme:  "oauth2",
				role:    "viewer",
				useAuth: true,
			},
			authorized: true,
		},
		{
			name: "User with admin role is viewer authorized",
			fields: fields{
				UsersStore: &mocks.UsersStore{
					GetF: func(ctx context.Context, q chronograf.UserQuery) (*chronograf.User, error) {
						if q.Name == nil || q.Provider == nil || q.Scheme == nil {
							return nil, fmt.Errorf("Invalid user query: missing Name, Provider, and/or Scheme")
						}
						return &chronograf.User{
							ID:       1337,
							Name:     "billysteve",
							Provider: "google",
							Scheme:   "oauth2",
							Roles: []chronograf.Role{
								{
									Name:         server.AdminRoleName,
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
				Logger: clog.New(clog.DebugLevel),
			},
			args: args{
				principal: &oauth2.Principal{
					Subject:      "billysteve",
					Issuer:       "google",
					Organization: "1337",
				},
				scheme:  "oauth2",
				role:    "viewer",
				useAuth: true,
			},
			authorized: true,
		},
		{
			name: "User with viewer role is editor unauthorized",
			fields: fields{
				UsersStore: &mocks.UsersStore{
					GetF: func(ctx context.Context, q chronograf.UserQuery) (*chronograf.User, error) {
						if q.Name == nil || q.Provider == nil || q.Scheme == nil {
							return nil, fmt.Errorf("Invalid user query: missing Name, Provider, and/or Scheme")
						}
						return &chronograf.User{
							ID:       1337,
							Name:     "billysteve",
							Provider: "google",
							Scheme:   "oauth2",
							Roles: []chronograf.Role{
								{
									Name:         server.ViewerRoleName,
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
				Logger: clog.New(clog.DebugLevel),
			},
			args: args{
				principal: &oauth2.Principal{
					Subject:      "billysteve",
					Issuer:       "google",
					Organization: "1337",
				},
				scheme:  "oauth2",
				role:    "editor",
				useAuth: true,
			},
			authorized: false,
		},
		{
			name: "User with editor role is editor authorized",
			fields: fields{
				UsersStore: &mocks.UsersStore{
					GetF: func(ctx context.Context, q chronograf.UserQuery) (*chronograf.User, error) {
						if q.Name == nil || q.Provider == nil || q.Scheme == nil {
							return nil, fmt.Errorf("Invalid user query: missing Name, Provider, and/or Scheme")
						}
						return &chronograf.User{
							ID:       1337,
							Name:     "billysteve",
							Provider: "google",
							Scheme:   "oauth2",
							Roles: []chronograf.Role{
								{
									Name:         server.EditorRoleName,
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
				Logger: clog.New(clog.DebugLevel),
			},
			args: args{
				principal: &oauth2.Principal{
					Subject:      "billysteve",
					Issuer:       "google",
					Organization: "1337",
				},
				scheme:  "oauth2",
				role:    "editor",
				useAuth: true,
			},
			authorized: true,
		},
		{
			name: "User with admin role is editor authorized",
			fields: fields{
				UsersStore: &mocks.UsersStore{
					GetF: func(ctx context.Context, q chronograf.UserQuery) (*chronograf.User, error) {
						if q.Name == nil || q.Provider == nil || q.Scheme == nil {
							return nil, fmt.Errorf("Invalid user query: missing Name, Provider, and/or Scheme")
						}
						return &chronograf.User{
							ID:       1337,
							Name:     "billysteve",
							Provider: "google",
							Scheme:   "oauth2",
							Roles: []chronograf.Role{
								{
									Name:         server.AdminRoleName,
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
				Logger: clog.New(clog.DebugLevel),
			},
			args: args{
				principal: &oauth2.Principal{
					Subject:      "billysteve",
					Issuer:       "google",
					Organization: "1337",
				},
				scheme:  "oauth2",
				role:    "editor",
				useAuth: true,
			},
			authorized: true,
		},
		{
			name: "User with viewer role is admin unauthorized",
			fields: fields{
				UsersStore: &mocks.UsersStore{
					GetF: func(ctx context.Context, q chronograf.UserQuery) (*chronograf.User, error) {
						if q.Name == nil || q.Provider == nil || q.Scheme == nil {
							return nil, fmt.Errorf("Invalid user query: missing Name, Provider, and/or Scheme")
						}
						return &chronograf.User{
							ID:       1337,
							Name:     "billysteve",
							Provider: "google",
							Scheme:   "oauth2",
							Roles: []chronograf.Role{
								{
									Name:         server.ViewerRoleName,
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
				Logger: clog.New(clog.DebugLevel),
			},
			args: args{
				principal: &oauth2.Principal{
					Subject:      "billysteve",
					Issuer:       "google",
					Organization: "1337",
				},
				scheme:  "oauth2",
				role:    "admin",
				useAuth: true,
			},
			authorized: false,
		},
		{
			name: "User with editor role is admin unauthorized",
			fields: fields{
				UsersStore: &mocks.UsersStore{
					GetF: func(ctx context.Context, q chronograf.UserQuery) (*chronograf.User, error) {
						if q.Name == nil || q.Provider == nil || q.Scheme == nil {
							return nil, fmt.Errorf("Invalid user query: missing Name, Provider, and/or Scheme")
						}
						return &chronograf.User{
							ID:       1337,
							Name:     "billysteve",
							Provider: "google",
							Scheme:   "oauth2",
							Roles: []chronograf.Role{
								{
									Name:         server.EditorRoleName,
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
				Logger: clog.New(clog.DebugLevel),
			},
			args: args{
				principal: &oauth2.Principal{
					Subject:      "billysteve",
					Issuer:       "google",
					Organization: "1337",
				},
				scheme:  "oauth2",
				role:    "admin",
				useAuth: true,
			},
			authorized: false,
		},
		{
			name: "User with admin role is admin authorized",
			fields: fields{
				UsersStore: &mocks.UsersStore{
					GetF: func(ctx context.Context, q chronograf.UserQuery) (*chronograf.User, error) {
						if q.Name == nil || q.Provider == nil || q.Scheme == nil {
							return nil, fmt.Errorf("Invalid user query: missing Name, Provider, and/or Scheme")
						}
						return &chronograf.User{
							ID:       1337,
							Name:     "billysteve",
							Provider: "google",
							Scheme:   "oauth2",
							Roles: []chronograf.Role{
								{
									Name:         server.AdminRoleName,
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
				Logger: clog.New(clog.DebugLevel),
			},
			args: args{
				principal: &oauth2.Principal{
					Subject:      "billysteve",
					Issuer:       "google",
					Organization: "1337",
				},
				scheme:  "oauth2",
				role:    "admin",
				useAuth: true,
			},
			authorized: true,
		},
		{
			name: "User with no role is viewer unauthorized",
			fields: fields{
				UsersStore: &mocks.UsersStore{
					GetF: func(ctx context.Context, q chronograf.UserQuery) (*chronograf.User, error) {
						if q.Name == nil || q.Provider == nil || q.Scheme == nil {
							return nil, fmt.Errorf("Invalid user query: missing Name, Provider, and/or Scheme")
						}
						return &chronograf.User{
							ID:       1337,
							Name:     "billysteve",
							Provider: "google",
							Scheme:   "oauth2",
							Roles:    []chronograf.Role{},
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
				Logger: clog.New(clog.DebugLevel),
			},
			args: args{
				principal: &oauth2.Principal{
					Subject:      "billysteve",
					Issuer:       "google",
					Organization: "1337",
				},
				scheme:  "oauth2",
				role:    "view",
				useAuth: true,
			},
			authorized: false,
		},
		{
			name: "User with no role is editor unauthorized",
			fields: fields{
				UsersStore: &mocks.UsersStore{
					GetF: func(ctx context.Context, q chronograf.UserQuery) (*chronograf.User, error) {
						if q.Name == nil || q.Provider == nil || q.Scheme == nil {
							return nil, fmt.Errorf("Invalid user query: missing Name, Provider, and/or Scheme")
						}
						return &chronograf.User{
							ID:       1337,
							Name:     "billysteve",
							Provider: "google",
							Scheme:   "oauth2",
							Roles:    []chronograf.Role{},
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
				Logger: clog.New(clog.DebugLevel),
			},
			args: args{
				principal: &oauth2.Principal{
					Subject:      "billysteve",
					Issuer:       "google",
					Organization: "1337",
				},
				scheme:  "oauth2",
				role:    "editor",
				useAuth: true,
			},
			authorized: false,
		},
		{
			name: "User with no role is admin unauthorized",
			fields: fields{
				UsersStore: &mocks.UsersStore{
					GetF: func(ctx context.Context, q chronograf.UserQuery) (*chronograf.User, error) {
						if q.Name == nil || q.Provider == nil || q.Scheme == nil {
							return nil, fmt.Errorf("Invalid user query: missing Name, Provider, and/or Scheme")
						}
						return &chronograf.User{
							ID:       1337,
							Name:     "billysteve",
							Provider: "google",
							Scheme:   "oauth2",
							Roles:    []chronograf.Role{},
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
				Logger: clog.New(clog.DebugLevel),
			},
			args: args{
				principal: &oauth2.Principal{
					Subject:      "billysteve",
					Issuer:       "google",
					Organization: "1337",
				},
				scheme:  "oauth2",
				role:    "admin",
				useAuth: true,
			},
			authorized: false,
		},
		{
			name: "User with unknown role is viewer unauthorized",
			fields: fields{
				UsersStore: &mocks.UsersStore{
					GetF: func(ctx context.Context, q chronograf.UserQuery) (*chronograf.User, error) {
						if q.Name == nil || q.Provider == nil || q.Scheme == nil {
							return nil, fmt.Errorf("Invalid user query: missing Name, Provider, and/or Scheme")
						}
						return &chronograf.User{
							ID:       1337,
							Name:     "billysteve",
							Provider: "google",
							Scheme:   "oauth2",
							Roles: []chronograf.Role{
								{
									Name: "sweet_role",
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
				Logger: clog.New(clog.DebugLevel),
			},
			args: args{
				principal: &oauth2.Principal{
					Subject:      "billysteve",
					Issuer:       "google",
					Organization: "1337",
				},
				scheme:  "oauth2",
				role:    "viewer",
				useAuth: true,
			},
			authorized: false,
		},
		{
			name: "User with unknown role is editor unauthorized",
			fields: fields{
				UsersStore: &mocks.UsersStore{
					GetF: func(ctx context.Context, q chronograf.UserQuery) (*chronograf.User, error) {
						if q.Name == nil || q.Provider == nil || q.Scheme == nil {
							return nil, fmt.Errorf("Invalid user query: missing Name, Provider, and/or Scheme")
						}
						return &chronograf.User{
							ID:       1337,
							Name:     "billysteve",
							Provider: "google",
							Scheme:   "oauth2",
							Roles: []chronograf.Role{
								{
									Name: "sweet_role",
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
				Logger: clog.New(clog.DebugLevel),
			},
			args: args{
				principal: &oauth2.Principal{
					Subject:      "billysteve",
					Issuer:       "google",
					Organization: "1337",
				},
				scheme:  "oauth2",
				role:    "editor",
				useAuth: true,
			},
			authorized: false,
		},
		{
			name: "User with unknown role is admin unauthorized",
			fields: fields{
				UsersStore: &mocks.UsersStore{
					GetF: func(ctx context.Context, q chronograf.UserQuery) (*chronograf.User, error) {
						if q.Name == nil || q.Provider == nil || q.Scheme == nil {
							return nil, fmt.Errorf("Invalid user query: missing Name, Provider, and/or Scheme")
						}
						return &chronograf.User{
							ID:       1337,
							Name:     "billysteve",
							Provider: "google",
							Scheme:   "oauth2",
							Roles: []chronograf.Role{
								{
									Name: "sweet_role",
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
				Logger: clog.New(clog.DebugLevel),
			},
			args: args{
				principal: &oauth2.Principal{
					Subject:      "billysteve",
					Issuer:       "google",
					Organization: "1337",
				},
				scheme:  "oauth2",
				role:    "admin",
				useAuth: true,
			},
			authorized: false,
		},
		{
			name: "User with viewer role is SuperAdmin unauthorized",
			fields: fields{
				UsersStore: &mocks.UsersStore{
					GetF: func(ctx context.Context, q chronograf.UserQuery) (*chronograf.User, error) {
						if q.Name == nil || q.Provider == nil || q.Scheme == nil {
							return nil, fmt.Errorf("Invalid user query: missing Name, Provider, and/or Scheme")
						}
						return &chronograf.User{
							ID:       1337,
							Name:     "billysteve",
							Provider: "google",
							Scheme:   "oauth2",
							Roles: []chronograf.Role{
								{
									Name:         server.ViewerRoleName,
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
				Logger: clog.New(clog.DebugLevel),
			},
			args: args{
				principal: &oauth2.Principal{
					Subject:      "billysteve",
					Issuer:       "google",
					Organization: "1337",
				},
				scheme:  "oauth2",
				role:    "superadmin",
				useAuth: true,
			},
			authorized: false,
		},
		{
			name: "User with editor role is SuperAdmin unauthorized",
			fields: fields{
				UsersStore: &mocks.UsersStore{
					GetF: func(ctx context.Context, q chronograf.UserQuery) (*chronograf.User, error) {
						if q.Name == nil || q.Provider == nil || q.Scheme == nil {
							return nil, fmt.Errorf("Invalid user query: missing Name, Provider, and/or Scheme")
						}
						return &chronograf.User{
							ID:       1337,
							Name:     "billysteve",
							Provider: "google",
							Scheme:   "oauth2",
							Roles: []chronograf.Role{
								{
									Name:         server.EditorRoleName,
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
				Logger: clog.New(clog.DebugLevel),
			},
			args: args{
				principal: &oauth2.Principal{
					Subject:      "billysteve",
					Issuer:       "google",
					Organization: "1337",
				},
				scheme:  "oauth2",
				role:    "superadmin",
				useAuth: true,
			},
			authorized: false,
		},
		{
			name: "User with admin role is SuperAdmin unauthorized",
			fields: fields{
				UsersStore: &mocks.UsersStore{
					GetF: func(ctx context.Context, q chronograf.UserQuery) (*chronograf.User, error) {
						if q.Name == nil || q.Provider == nil || q.Scheme == nil {
							return nil, fmt.Errorf("Invalid user query: missing Name, Provider, and/or Scheme")
						}
						return &chronograf.User{
							ID:       1337,
							Name:     "billysteve",
							Provider: "google",
							Scheme:   "oauth2",
							Roles: []chronograf.Role{
								{
									Name:         server.AdminRoleName,
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
				Logger: clog.New(clog.DebugLevel),
			},
			args: args{
				principal: &oauth2.Principal{
					Subject:      "billysteve",
					Issuer:       "google",
					Organization: "1337",
				},
				scheme:  "oauth2",
				role:    "superadmin",
				useAuth: true,
			},
			authorized: false,
		},
		{
			name: "SuperAdmin is Viewer authorized",
			fields: fields{
				UsersStore: &mocks.UsersStore{
					GetF: func(ctx context.Context, q chronograf.UserQuery) (*chronograf.User, error) {
						if q.Name == nil || q.Provider == nil || q.Scheme == nil {
							return nil, fmt.Errorf("Invalid user query: missing Name, Provider, and/or Scheme")
						}
						return &chronograf.User{
							ID:         1337,
							Name:       "billysteve",
							Provider:   "google",
							Scheme:     "oauth2",
							SuperAdmin: true,
							Roles: []chronograf.Role{
								{
									Name:         server.MemberRoleName,
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
				Logger: clog.New(clog.DebugLevel),
			},
			args: args{
				principal: &oauth2.Principal{
					Subject:      "billysteve",
					Issuer:       "google",
					Organization: "1337",
				},
				scheme:  "oauth2",
				role:    "viewer",
				useAuth: true,
			},
			authorized: true,
		},
		{
			name: "SuperAdmin is Editor authorized",
			fields: fields{
				UsersStore: &mocks.UsersStore{
					GetF: func(ctx context.Context, q chronograf.UserQuery) (*chronograf.User, error) {
						if q.Name == nil || q.Provider == nil || q.Scheme == nil {
							return nil, fmt.Errorf("Invalid user query: missing Name, Provider, and/or Scheme")
						}
						return &chronograf.User{
							ID:         1337,
							Name:       "billysteve",
							Provider:   "google",
							Scheme:     "oauth2",
							SuperAdmin: true,
							Roles: []chronograf.Role{
								{
									Name:         server.MemberRoleName,
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
				Logger: clog.New(clog.DebugLevel),
			},
			args: args{
				principal: &oauth2.Principal{
					Subject:      "billysteve",
					Issuer:       "google",
					Organization: "1337",
				},
				scheme:  "oauth2",
				role:    "editor",
				useAuth: true,
			},
			authorized: true,
		},
		{
			name: "SuperAdmin is Admin authorized",
			fields: fields{
				UsersStore: &mocks.UsersStore{
					GetF: func(ctx context.Context, q chronograf.UserQuery) (*chronograf.User, error) {
						if q.Name == nil || q.Provider == nil || q.Scheme == nil {
							return nil, fmt.Errorf("Invalid user query: missing Name, Provider, and/or Scheme")
						}
						return &chronograf.User{
							ID:         1337,
							Name:       "billysteve",
							Provider:   "google",
							Scheme:     "oauth2",
							SuperAdmin: true,
							Roles: []chronograf.Role{
								{
									Name:         server.MemberRoleName,
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
				Logger: clog.New(clog.DebugLevel),
			},
			args: args{
				principal: &oauth2.Principal{
					Subject:      "billysteve",
					Issuer:       "google",
					Organization: "1337",
				},
				scheme:  "oauth2",
				role:    "admin",
				useAuth: true,
			},
			authorized: true,
		},
		{
			name: "SuperAdmin is SuperAdmin authorized",
			fields: fields{
				UsersStore: &mocks.UsersStore{
					GetF: func(ctx context.Context, q chronograf.UserQuery) (*chronograf.User, error) {
						if q.Name == nil || q.Provider == nil || q.Scheme == nil {
							return nil, fmt.Errorf("Invalid user query: missing Name, Provider, and/or Scheme")
						}
						return &chronograf.User{
							ID:         1337,
							Name:       "billysteve",
							Provider:   "google",
							Scheme:     "oauth2",
							SuperAdmin: true,
							Roles: []chronograf.Role{
								{
									Name:         server.MemberRoleName,
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
				Logger: clog.New(clog.DebugLevel),
			},
			args: args{
				principal: &oauth2.Principal{
					Subject:      "billysteve",
					Issuer:       "google",
					Organization: "1337",
				},
				scheme:  "oauth2",
				role:    "superadmin",
				useAuth: true,
			},
			authorized: true,
		},
		{
			name: "Invalid principal – principal is nil",
			fields: fields{
				UsersStore: &mocks.UsersStore{
					GetF: func(ctx context.Context, q chronograf.UserQuery) (*chronograf.User, error) {
						if q.Name == nil || q.Provider == nil || q.Scheme == nil {
							return nil, fmt.Errorf("Invalid user query: missing Name, Provider, and/or Scheme")
						}
						return &chronograf.User{
							ID:       1337,
							Name:     "billysteve",
							Provider: "google",
							Scheme:   "oauth2",
							Roles: []chronograf.Role{
								{
									Name:         server.AdminRoleName,
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
				Logger: clog.New(clog.DebugLevel),
			},
			args: args{
				principal: nil,
				scheme:    "oauth2",
				role:      "admin",
				useAuth:   true,
			},
			authorized: false,
		},
		{
			name: "Invalid principal - missing organization",
			fields: fields{
				UsersStore: &mocks.UsersStore{
					GetF: func(ctx context.Context, q chronograf.UserQuery) (*chronograf.User, error) {
						if q.Name == nil || q.Provider == nil || q.Scheme == nil {
							return nil, fmt.Errorf("Invalid user query: missing Name, Provider, and/or Scheme")
						}
						return &chronograf.User{
							ID:       1337,
							Name:     "billysteve",
							Provider: "google",
							Scheme:   "oauth2",
							Roles: []chronograf.Role{
								{
									Name:         server.AdminRoleName,
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
				Logger: clog.New(clog.DebugLevel),
			},
			args: args{
				principal: &oauth2.Principal{
					Subject: "billysteve",
					Issuer:  "google",
				},
				scheme:  "oauth2",
				role:    "admin",
				useAuth: true,
			},
			authorized: false,
		},
		{
			name: "Invalid principal - organization id not uint64",
			fields: fields{
				UsersStore: &mocks.UsersStore{
					GetF: func(ctx context.Context, q chronograf.UserQuery) (*chronograf.User, error) {
						if q.Name == nil || q.Provider == nil || q.Scheme == nil {
							return nil, fmt.Errorf("Invalid user query: missing Name, Provider, and/or Scheme")
						}
						return &chronograf.User{
							ID:       1337,
							Name:     "billysteve",
							Provider: "google",
							Scheme:   "oauth2",
							Roles: []chronograf.Role{
								{
									Name:         server.AdminRoleName,
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
				Logger: clog.New(clog.DebugLevel),
			},
			args: args{
				principal: &oauth2.Principal{
					Subject:      "billysteve",
					Issuer:       "google",
					Organization: "1ee7",
				},
				scheme:  "oauth2",
				role:    "admin",
				useAuth: true,
			},
			authorized: false,
		},
		{
			name: "Failed to retrieve organization",
			fields: fields{
				UsersStore: &mocks.UsersStore{
					GetF: func(ctx context.Context, q chronograf.UserQuery) (*chronograf.User, error) {
						if q.Name == nil || q.Provider == nil || q.Scheme == nil {
							return nil, fmt.Errorf("Invalid user query: missing Name, Provider, and/or Scheme")
						}
						return &chronograf.User{
							ID:       1337,
							Name:     "billysteve",
							Provider: "google",
							Scheme:   "oauth2",
							Roles: []chronograf.Role{
								{
									Name:         server.AdminRoleName,
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
						switch *q.ID {
						case 1338:
							return &chronograf.Organization{
								ID:   1338,
								Name: "The ShillBillThrilliettas",
							}, nil
						default:
							return nil, chronograf.ErrOrganizationNotFound
						}
					},
				},
				Logger: clog.New(clog.DebugLevel),
			},
			args: args{
				principal: &oauth2.Principal{
					Subject:      "billysteve",
					Issuer:       "google",
					Organization: "1337",
				},
				scheme:  "oauth2",
				role:    "admin",
				useAuth: true,
			},
			authorized: false,
		},
		{
			name: "Failed to retrieve user",
			fields: fields{
				UsersStore: &mocks.UsersStore{
					GetF: func(ctx context.Context, q chronograf.UserQuery) (*chronograf.User, error) {
						if q.Name == nil || q.Provider == nil || q.Scheme == nil {
							return nil, fmt.Errorf("Invalid user query: missing Name, Provider, and/or Scheme")
						}
						switch *q.Name {
						case "billysteve":
							return &chronograf.User{
								ID:       1337,
								Name:     "billysteve",
								Provider: "google",
								Scheme:   "oauth2",
								Roles: []chronograf.Role{
									{
										Name:         server.AdminRoleName,
										Organization: "1337",
									},
								},
							}, nil
						default:
							return nil, chronograf.ErrUserNotFound
						}
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
				Logger: clog.New(clog.DebugLevel),
			},
			args: args{
				principal: &oauth2.Principal{
					Subject:      "billietta",
					Issuer:       "google",
					Organization: "1337",
				},
				scheme:  "oauth2",
				role:    "admin",
				useAuth: true,
			},
			authorized: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var authorized bool
			next := func(w http.ResponseWriter, r *http.Request) {
				authorized = true
			}
			fn := server.AuthorizedUser(
				&server.Store{
					UsersStore:         tt.fields.UsersStore,
					OrganizationsStore: tt.fields.OrganizationsStore,
				},
				tt.args.useAuth,
				tt.args.role,
				tt.fields.Logger,
				next,
			)

			w := httptest.NewRecorder()
			r := httptest.NewRequest(
				"GET",
				"http://any.url", // can be any valid URL as we are bypassing mux
				nil,
			)
			if tt.args.principal == nil {
				r = r.WithContext(context.WithValue(r.Context(), oauth2.PrincipalKey, nil))
			} else {
				r = r.WithContext(context.WithValue(r.Context(), oauth2.PrincipalKey, *tt.args.principal))
			}
			fn(w, r)

			if authorized != tt.authorized {
				t.Errorf("%q. AuthorizedUser() = %v, expected %v", tt.name, authorized, tt.authorized)
			}

		})
	}
}
