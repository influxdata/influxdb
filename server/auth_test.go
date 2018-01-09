package server

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
	"github.com/influxdata/chronograf/roles"
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
		handler := AuthorizedToken(a, logger, next)
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
		name                   string
		fields                 fields
		args                   args
		hasOrganizationContext bool
		hasSuperAdminContext   bool
		hasRoleContext         bool
		hasServerContext       bool
		authorized             bool
	}{
		{
			name: "Not using auth",
			fields: fields{
				UsersStore: &mocks.UsersStore{},
				OrganizationsStore: &mocks.OrganizationsStore{
					DefaultOrganizationF: func(ctx context.Context) (*chronograf.Organization, error) {
						return &chronograf.Organization{
							ID: "0",
						}, nil
					},
				},
				Logger: clog.New(clog.DebugLevel),
			},
			args: args{
				useAuth: false,
			},
			hasOrganizationContext: false,
			hasSuperAdminContext:   false,
			hasRoleContext:         false,
			hasServerContext:       true,
			authorized:             true,
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
									Name:         roles.ViewerRoleName,
									Organization: "1337",
								},
							},
						}, nil
					},
				},
				OrganizationsStore: &mocks.OrganizationsStore{
					DefaultOrganizationF: func(ctx context.Context) (*chronograf.Organization, error) {
						return &chronograf.Organization{
							ID: "0",
						}, nil
					},
					GetF: func(ctx context.Context, q chronograf.OrganizationQuery) (*chronograf.Organization, error) {
						if q.ID == nil {
							return nil, fmt.Errorf("Invalid organization query: missing ID")
						}
						return &chronograf.Organization{
							ID:   "1337",
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
			authorized:             true,
			hasOrganizationContext: true,
			hasSuperAdminContext:   false,
			hasRoleContext:         true,
			hasServerContext:       false,
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
									Name:         roles.EditorRoleName,
									Organization: "1337",
								},
							},
						}, nil
					},
				},
				OrganizationsStore: &mocks.OrganizationsStore{
					DefaultOrganizationF: func(ctx context.Context) (*chronograf.Organization, error) {
						return &chronograf.Organization{
							ID: "0",
						}, nil
					},
					GetF: func(ctx context.Context, q chronograf.OrganizationQuery) (*chronograf.Organization, error) {
						if q.ID == nil {
							return nil, fmt.Errorf("Invalid organization query: missing ID")
						}
						return &chronograf.Organization{
							ID:   "1337",
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
			authorized:             true,
			hasOrganizationContext: true,
			hasSuperAdminContext:   false,
			hasRoleContext:         true,
			hasServerContext:       false,
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
									Name:         roles.AdminRoleName,
									Organization: "1337",
								},
							},
						}, nil
					},
				},
				OrganizationsStore: &mocks.OrganizationsStore{
					DefaultOrganizationF: func(ctx context.Context) (*chronograf.Organization, error) {
						return &chronograf.Organization{
							ID: "0",
						}, nil
					},
					GetF: func(ctx context.Context, q chronograf.OrganizationQuery) (*chronograf.Organization, error) {
						if q.ID == nil {
							return nil, fmt.Errorf("Invalid organization query: missing ID")
						}
						return &chronograf.Organization{
							ID:   "1337",
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
			authorized:             true,
			hasOrganizationContext: true,
			hasSuperAdminContext:   false,
			hasRoleContext:         true,
			hasServerContext:       false,
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
									Name:         roles.ViewerRoleName,
									Organization: "1337",
								},
							},
						}, nil
					},
				},
				OrganizationsStore: &mocks.OrganizationsStore{
					DefaultOrganizationF: func(ctx context.Context) (*chronograf.Organization, error) {
						return &chronograf.Organization{
							ID: "0",
						}, nil
					},
					GetF: func(ctx context.Context, q chronograf.OrganizationQuery) (*chronograf.Organization, error) {
						if q.ID == nil {
							return nil, fmt.Errorf("Invalid organization query: missing ID")
						}
						return &chronograf.Organization{
							ID:   "1337",
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
									Name:         roles.EditorRoleName,
									Organization: "1337",
								},
							},
						}, nil
					},
				},
				OrganizationsStore: &mocks.OrganizationsStore{
					DefaultOrganizationF: func(ctx context.Context) (*chronograf.Organization, error) {
						return &chronograf.Organization{
							ID: "0",
						}, nil
					},
					GetF: func(ctx context.Context, q chronograf.OrganizationQuery) (*chronograf.Organization, error) {
						if q.ID == nil {
							return nil, fmt.Errorf("Invalid organization query: missing ID")
						}
						return &chronograf.Organization{
							ID:   "1337",
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
			authorized:             true,
			hasOrganizationContext: true,
			hasSuperAdminContext:   false,
			hasRoleContext:         true,
			hasServerContext:       false,
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
									Name:         roles.AdminRoleName,
									Organization: "1337",
								},
							},
						}, nil
					},
				},
				OrganizationsStore: &mocks.OrganizationsStore{
					DefaultOrganizationF: func(ctx context.Context) (*chronograf.Organization, error) {
						return &chronograf.Organization{
							ID: "0",
						}, nil
					},
					GetF: func(ctx context.Context, q chronograf.OrganizationQuery) (*chronograf.Organization, error) {
						if q.ID == nil {
							return nil, fmt.Errorf("Invalid organization query: missing ID")
						}
						return &chronograf.Organization{
							ID:   "1337",
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
			authorized:             true,
			hasOrganizationContext: true,
			hasSuperAdminContext:   false,
			hasRoleContext:         true,
			hasServerContext:       false,
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
									Name:         roles.ViewerRoleName,
									Organization: "1337",
								},
							},
						}, nil
					},
				},
				OrganizationsStore: &mocks.OrganizationsStore{
					DefaultOrganizationF: func(ctx context.Context) (*chronograf.Organization, error) {
						return &chronograf.Organization{
							ID: "0",
						}, nil
					},
					GetF: func(ctx context.Context, q chronograf.OrganizationQuery) (*chronograf.Organization, error) {
						if q.ID == nil {
							return nil, fmt.Errorf("Invalid organization query: missing ID")
						}
						return &chronograf.Organization{
							ID:   "1337",
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
									Name:         roles.EditorRoleName,
									Organization: "1337",
								},
							},
						}, nil
					},
				},
				OrganizationsStore: &mocks.OrganizationsStore{
					DefaultOrganizationF: func(ctx context.Context) (*chronograf.Organization, error) {
						return &chronograf.Organization{
							ID: "0",
						}, nil
					},
					GetF: func(ctx context.Context, q chronograf.OrganizationQuery) (*chronograf.Organization, error) {
						if q.ID == nil {
							return nil, fmt.Errorf("Invalid organization query: missing ID")
						}
						return &chronograf.Organization{
							ID:   "1337",
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
									Name:         roles.AdminRoleName,
									Organization: "1337",
								},
							},
						}, nil
					},
				},
				OrganizationsStore: &mocks.OrganizationsStore{
					DefaultOrganizationF: func(ctx context.Context) (*chronograf.Organization, error) {
						return &chronograf.Organization{
							ID: "0",
						}, nil
					},
					GetF: func(ctx context.Context, q chronograf.OrganizationQuery) (*chronograf.Organization, error) {
						if q.ID == nil {
							return nil, fmt.Errorf("Invalid organization query: missing ID")
						}
						return &chronograf.Organization{
							ID:   "1337",
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
			authorized:             true,
			hasOrganizationContext: true,
			hasSuperAdminContext:   false,
			hasRoleContext:         true,
			hasServerContext:       false,
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
					DefaultOrganizationF: func(ctx context.Context) (*chronograf.Organization, error) {
						return &chronograf.Organization{
							ID: "0",
						}, nil
					},
					GetF: func(ctx context.Context, q chronograf.OrganizationQuery) (*chronograf.Organization, error) {
						if q.ID == nil {
							return nil, fmt.Errorf("Invalid organization query: missing ID")
						}
						return &chronograf.Organization{
							ID:   "1337",
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
					DefaultOrganizationF: func(ctx context.Context) (*chronograf.Organization, error) {
						return &chronograf.Organization{
							ID: "0",
						}, nil
					},
					GetF: func(ctx context.Context, q chronograf.OrganizationQuery) (*chronograf.Organization, error) {
						if q.ID == nil {
							return nil, fmt.Errorf("Invalid organization query: missing ID")
						}
						return &chronograf.Organization{
							ID:   "1337",
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
					DefaultOrganizationF: func(ctx context.Context) (*chronograf.Organization, error) {
						return &chronograf.Organization{
							ID: "0",
						}, nil
					},
					GetF: func(ctx context.Context, q chronograf.OrganizationQuery) (*chronograf.Organization, error) {
						if q.ID == nil {
							return nil, fmt.Errorf("Invalid organization query: missing ID")
						}
						return &chronograf.Organization{
							ID:   "1337",
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
					DefaultOrganizationF: func(ctx context.Context) (*chronograf.Organization, error) {
						return &chronograf.Organization{
							ID: "0",
						}, nil
					},
					GetF: func(ctx context.Context, q chronograf.OrganizationQuery) (*chronograf.Organization, error) {
						if q.ID == nil {
							return nil, fmt.Errorf("Invalid organization query: missing ID")
						}
						return &chronograf.Organization{
							ID:   "1337",
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
							ID:   "1337",
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
					DefaultOrganizationF: func(ctx context.Context) (*chronograf.Organization, error) {
						return &chronograf.Organization{
							ID: "0",
						}, nil
					},
					GetF: func(ctx context.Context, q chronograf.OrganizationQuery) (*chronograf.Organization, error) {
						if q.ID == nil {
							return nil, fmt.Errorf("Invalid organization query: missing ID")
						}
						return &chronograf.Organization{
							ID:   "1337",
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
									Name:         roles.ViewerRoleName,
									Organization: "1337",
								},
							},
						}, nil
					},
				},
				OrganizationsStore: &mocks.OrganizationsStore{
					DefaultOrganizationF: func(ctx context.Context) (*chronograf.Organization, error) {
						return &chronograf.Organization{
							ID: "0",
						}, nil
					},
					GetF: func(ctx context.Context, q chronograf.OrganizationQuery) (*chronograf.Organization, error) {
						if q.ID == nil {
							return nil, fmt.Errorf("Invalid organization query: missing ID")
						}
						return &chronograf.Organization{
							ID:   "1337",
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
									Name:         roles.EditorRoleName,
									Organization: "1337",
								},
							},
						}, nil
					},
				},
				OrganizationsStore: &mocks.OrganizationsStore{
					DefaultOrganizationF: func(ctx context.Context) (*chronograf.Organization, error) {
						return &chronograf.Organization{
							ID: "0",
						}, nil
					},
					GetF: func(ctx context.Context, q chronograf.OrganizationQuery) (*chronograf.Organization, error) {
						if q.ID == nil {
							return nil, fmt.Errorf("Invalid organization query: missing ID")
						}
						return &chronograf.Organization{
							ID:   "1337",
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
									Name:         roles.AdminRoleName,
									Organization: "1337",
								},
							},
						}, nil
					},
				},
				OrganizationsStore: &mocks.OrganizationsStore{
					DefaultOrganizationF: func(ctx context.Context) (*chronograf.Organization, error) {
						return &chronograf.Organization{
							ID: "0",
						}, nil
					},
					GetF: func(ctx context.Context, q chronograf.OrganizationQuery) (*chronograf.Organization, error) {
						if q.ID == nil {
							return nil, fmt.Errorf("Invalid organization query: missing ID")
						}
						return &chronograf.Organization{
							ID:   "1337",
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
									Name:         roles.MemberRoleName,
									Organization: "1337",
								},
							},
						}, nil
					},
				},
				OrganizationsStore: &mocks.OrganizationsStore{
					DefaultOrganizationF: func(ctx context.Context) (*chronograf.Organization, error) {
						return &chronograf.Organization{
							ID: "0",
						}, nil
					},
					GetF: func(ctx context.Context, q chronograf.OrganizationQuery) (*chronograf.Organization, error) {
						if q.ID == nil {
							return nil, fmt.Errorf("Invalid organization query: missing ID")
						}
						return &chronograf.Organization{
							ID:   "1337",
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
			authorized:             true,
			hasOrganizationContext: true,
			hasSuperAdminContext:   true,
			hasRoleContext:         true,
			hasServerContext:       false,
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
									Name:         roles.MemberRoleName,
									Organization: "1337",
								},
							},
						}, nil
					},
				},
				OrganizationsStore: &mocks.OrganizationsStore{
					DefaultOrganizationF: func(ctx context.Context) (*chronograf.Organization, error) {
						return &chronograf.Organization{
							ID: "0",
						}, nil
					},
					GetF: func(ctx context.Context, q chronograf.OrganizationQuery) (*chronograf.Organization, error) {
						if q.ID == nil {
							return nil, fmt.Errorf("Invalid organization query: missing ID")
						}
						return &chronograf.Organization{
							ID:   "1337",
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
			authorized:             true,
			hasOrganizationContext: true,
			hasSuperAdminContext:   true,
			hasRoleContext:         true,
			hasServerContext:       false,
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
									Name:         roles.MemberRoleName,
									Organization: "1337",
								},
							},
						}, nil
					},
				},
				OrganizationsStore: &mocks.OrganizationsStore{
					DefaultOrganizationF: func(ctx context.Context) (*chronograf.Organization, error) {
						return &chronograf.Organization{
							ID: "0",
						}, nil
					},
					GetF: func(ctx context.Context, q chronograf.OrganizationQuery) (*chronograf.Organization, error) {
						if q.ID == nil {
							return nil, fmt.Errorf("Invalid organization query: missing ID")
						}
						return &chronograf.Organization{
							ID:   "1337",
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
			authorized:             true,
			hasOrganizationContext: true,
			hasSuperAdminContext:   true,
			hasRoleContext:         true,
			hasServerContext:       false,
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
									Name:         roles.MemberRoleName,
									Organization: "1337",
								},
							},
						}, nil
					},
				},
				OrganizationsStore: &mocks.OrganizationsStore{
					DefaultOrganizationF: func(ctx context.Context) (*chronograf.Organization, error) {
						return &chronograf.Organization{
							ID: "0",
						}, nil
					},
					GetF: func(ctx context.Context, q chronograf.OrganizationQuery) (*chronograf.Organization, error) {
						if q.ID == nil {
							return nil, fmt.Errorf("Invalid organization query: missing ID")
						}
						return &chronograf.Organization{
							ID:   "1337",
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
			authorized:             true,
			hasOrganizationContext: true,
			hasSuperAdminContext:   true,
			hasRoleContext:         true,
			hasServerContext:       false,
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
									Name:         roles.AdminRoleName,
									Organization: "1337",
								},
							},
						}, nil
					},
				},
				OrganizationsStore: &mocks.OrganizationsStore{
					DefaultOrganizationF: func(ctx context.Context) (*chronograf.Organization, error) {
						return &chronograf.Organization{
							ID: "0",
						}, nil
					},
					GetF: func(ctx context.Context, q chronograf.OrganizationQuery) (*chronograf.Organization, error) {
						if q.ID == nil {
							return nil, fmt.Errorf("Invalid organization query: missing ID")
						}
						return &chronograf.Organization{
							ID:   "1337",
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
									Name:         roles.AdminRoleName,
									Organization: "1337",
								},
							},
						}, nil
					},
				},
				OrganizationsStore: &mocks.OrganizationsStore{
					DefaultOrganizationF: func(ctx context.Context) (*chronograf.Organization, error) {
						return &chronograf.Organization{
							ID: "0",
						}, nil
					},
					GetF: func(ctx context.Context, q chronograf.OrganizationQuery) (*chronograf.Organization, error) {
						if q.ID == nil {
							return nil, fmt.Errorf("Invalid organization query: missing ID")
						}
						return &chronograf.Organization{
							ID:   "1337",
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
									Name:         roles.AdminRoleName,
									Organization: "1337",
								},
							},
						}, nil
					},
				},
				OrganizationsStore: &mocks.OrganizationsStore{
					DefaultOrganizationF: func(ctx context.Context) (*chronograf.Organization, error) {
						return &chronograf.Organization{
							ID: "0",
						}, nil
					},
					GetF: func(ctx context.Context, q chronograf.OrganizationQuery) (*chronograf.Organization, error) {
						if q.ID == nil {
							return nil, fmt.Errorf("Invalid organization query: missing ID")
						}
						return &chronograf.Organization{
							ID:   "1337",
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
									Name:         roles.AdminRoleName,
									Organization: "1337",
								},
							},
						}, nil
					},
				},
				OrganizationsStore: &mocks.OrganizationsStore{
					DefaultOrganizationF: func(ctx context.Context) (*chronograf.Organization, error) {
						return &chronograf.Organization{
							ID: "0",
						}, nil
					},
					GetF: func(ctx context.Context, q chronograf.OrganizationQuery) (*chronograf.Organization, error) {
						if q.ID == nil {
							return nil, fmt.Errorf("Invalid organization query: missing ID")
						}
						switch *q.ID {
						case "1338":
							return &chronograf.Organization{
								ID:   "1338",
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
										Name:         roles.AdminRoleName,
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
					DefaultOrganizationF: func(ctx context.Context) (*chronograf.Organization, error) {
						return &chronograf.Organization{
							ID: "0",
						}, nil
					},
					GetF: func(ctx context.Context, q chronograf.OrganizationQuery) (*chronograf.Organization, error) {
						if q.ID == nil {
							return nil, fmt.Errorf("Invalid organization query: missing ID")
						}
						return &chronograf.Organization{
							ID:   "1337",
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
			var hasServerCtx bool
			var hasSuperAdminCtx bool
			var hasOrganizationCtx bool
			var hasRoleCtx bool
			next := func(w http.ResponseWriter, r *http.Request) {
				ctx := r.Context()
				hasServerCtx = hasServerContext(ctx)
				hasSuperAdminCtx = hasSuperAdminContext(ctx)
				_, hasOrganizationCtx = hasOrganizationContext(ctx)
				_, hasRoleCtx = hasRoleContext(ctx)
				authorized = true
			}
			fn := AuthorizedUser(
				&Store{
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

			if !authorized && w.Code != http.StatusForbidden {
				t.Errorf("%q. AuthorizedUser() Status Code = %v, expected %v", tt.name, w.Code, http.StatusForbidden)
			}

			if hasServerCtx != tt.hasServerContext {
				t.Errorf("%q. AuthorizedUser().Context().Server = %v, expected %v", tt.name, hasServerCtx, tt.hasServerContext)
			}

			if hasSuperAdminCtx != tt.hasSuperAdminContext {
				t.Errorf("%q. AuthorizedUser().Context().SuperAdmin = %v, expected %v", tt.name, hasSuperAdminCtx, tt.hasSuperAdminContext)
			}

			if hasOrganizationCtx != tt.hasOrganizationContext {
				t.Errorf("%q. AuthorizedUser.Context().Organization = %v, expected %v", tt.name, hasOrganizationCtx, tt.hasOrganizationContext)
			}

			if hasRoleCtx != tt.hasRoleContext {
				t.Errorf("%q. AuthorizedUser().Context().Role = %v, expected %v", tt.name, hasRoleCtx, tt.hasRoleContext)
			}

		})
	}
}

func TestCheckForRawQuery(t *testing.T) {
	type fields struct {
		Logger chronograf.Logger
	}
	type args struct {
		principal     *oauth2.Principal
		serverContext bool
		user          *chronograf.User
		raw           bool
	}
	type wants struct {
		authorized       bool
		hasServerContext bool
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		wants  wants
	}{
		{
			name: "middleware already has server context with raw",
			fields: fields{
				Logger: clog.New(clog.DebugLevel),
			},
			args: args{
				serverContext: true,
				raw:           true,
			},
			wants: wants{
				authorized:       true,
				hasServerContext: true,
			},
		},
		{
			name: "middleware already has server context without raw",
			fields: fields{
				Logger: clog.New(clog.DebugLevel),
			},
			args: args{
				serverContext: true,
				raw:           false,
			},
			wants: wants{
				authorized:       true,
				hasServerContext: true,
			},
		},
		{
			name: "user on context is a SuperAdmin with raw",
			fields: fields{
				Logger: clog.New(clog.DebugLevel),
			},
			args: args{
				user: &chronograf.User{
					SuperAdmin: true,
				},
				raw: true,
			},
			wants: wants{
				authorized:       true,
				hasServerContext: true,
			},
		},
		{
			name: "user on context is a not SuperAdmin with raw",
			fields: fields{
				Logger: clog.New(clog.DebugLevel),
			},
			args: args{
				user: &chronograf.User{
					SuperAdmin: false,
				},
				raw: true,
			},
			wants: wants{
				authorized:       false,
				hasServerContext: false,
			},
		},
		{
			name: "user on context is a SuperAdmin without raw",
			fields: fields{
				Logger: clog.New(clog.DebugLevel),
			},
			args: args{
				user: &chronograf.User{
					SuperAdmin: true,
				},
				raw: false,
			},
			wants: wants{
				authorized:       true,
				hasServerContext: false,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var authorized bool
			var hasServerCtx bool
			next := func(w http.ResponseWriter, r *http.Request) {
				ctx := r.Context()
				hasServerCtx = hasServerContext(ctx)
				authorized = true
			}
			fn := CheckForRawQuery(
				tt.fields.Logger,
				next,
			)

			w := httptest.NewRecorder()
			url := "http://any.url"
			if tt.args.raw {
				url = fmt.Sprintf("%s?raw=true", url)
			}
			r := httptest.NewRequest(
				"GET",
				url,
				nil,
			)
			if tt.args.principal == nil {
				r = r.WithContext(context.WithValue(r.Context(), oauth2.PrincipalKey, nil))
			} else {
				r = r.WithContext(context.WithValue(r.Context(), oauth2.PrincipalKey, *tt.args.principal))
			}

			if tt.args.serverContext {
				r = r.WithContext(serverContext(r.Context()))
			}
			if tt.args.user != nil {
				r = r.WithContext(context.WithValue(r.Context(), UserContextKey, tt.args.user))
			}
			fn(w, r)

			if authorized != tt.wants.authorized {
				t.Errorf("%q. CheckForRawQuery() = %v, expected %v", tt.name, authorized, tt.wants.authorized)
			}

			if !authorized && w.Code != http.StatusForbidden {
				t.Errorf("%q. CheckForRawQuery() Status Code = %v, expected %v", tt.name, w.Code, http.StatusForbidden)
			}

			if hasServerCtx != tt.wants.hasServerContext {
				t.Errorf("%q. CheckForRawQuery().Context().Server = %v, expected %v", tt.name, hasServerCtx, tt.wants.hasServerContext)
			}

		})
	}
}
