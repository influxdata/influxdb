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

type MockAuthenticator struct {
	Principal   oauth2.Principal
	ValidateErr error
	ExtendErr   error
	Serialized  string
}

func (m *MockAuthenticator) Validate(context.Context, *http.Request) (oauth2.Principal, error) {
	return m.Principal, m.ValidateErr
}

func (m *MockAuthenticator) Extend(ctx context.Context, w http.ResponseWriter, p oauth2.Principal) (oauth2.Principal, error) {
	cookie := http.Cookie{}

	http.SetCookie(w, &cookie)
	return m.Principal, m.ExtendErr
}

func (m *MockAuthenticator) Authorize(ctx context.Context, w http.ResponseWriter, p oauth2.Principal) error {
	cookie := http.Cookie{}

	http.SetCookie(w, &cookie)
	return nil
}

func (m *MockAuthenticator) Expire(http.ResponseWriter) {}

func (m *MockAuthenticator) ValidAuthorization(ctx context.Context, serializedAuthorization string) (oauth2.Principal, error) {
	return oauth2.Principal{}, nil
}
func (m *MockAuthenticator) Serialize(context.Context, oauth2.Principal) (string, error) {
	return m.Serialized, nil
}

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

		a := &MockAuthenticator{
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
		UsersStore chronograf.UsersStore
		Logger     chronograf.Logger
	}
	type args struct {
		username string
		provider string
		scheme   string
		useAuth  bool
		role     string
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
				UsersStore: &mocks.UsersStore{},
				Logger:     clog.New(clog.DebugLevel),
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
								server.ViewerRole,
							},
						}, nil
					},
				},
				Logger: clog.New(clog.DebugLevel),
			},
			args: args{
				username: "billysteve",
				provider: "google",
				scheme:   "oauth2",
				role:     "viewer",
				useAuth:  true,
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
								server.EditorRole,
							},
						}, nil
					},
				},
				Logger: clog.New(clog.DebugLevel),
			},
			args: args{
				username: "billysteve",
				provider: "google",
				scheme:   "oauth2",
				role:     "viewer",
				useAuth:  true,
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
								server.AdminRole,
							},
						}, nil
					},
				},
				Logger: clog.New(clog.DebugLevel),
			},
			args: args{
				username: "billysteve",
				provider: "google",
				scheme:   "oauth2",
				role:     "viewer",
				useAuth:  true,
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
								server.ViewerRole,
							},
						}, nil
					},
				},
				Logger: clog.New(clog.DebugLevel),
			},
			args: args{
				username: "billysteve",
				provider: "google",
				scheme:   "oauth2",
				role:     "editor",
				useAuth:  true,
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
								server.EditorRole,
							},
						}, nil
					},
				},
				Logger: clog.New(clog.DebugLevel),
			},
			args: args{
				username: "billysteve",
				provider: "google",
				scheme:   "oauth2",
				role:     "editor",
				useAuth:  true,
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
								server.AdminRole,
							},
						}, nil
					},
				},
				Logger: clog.New(clog.DebugLevel),
			},
			args: args{
				username: "billysteve",
				provider: "google",
				scheme:   "oauth2",
				role:     "editor",
				useAuth:  true,
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
								server.ViewerRole,
							},
						}, nil
					},
				},
				Logger: clog.New(clog.DebugLevel),
			},
			args: args{
				username: "billysteve",
				provider: "google",
				scheme:   "oauth2",
				role:     "admin",
				useAuth:  true,
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
								server.EditorRole,
							},
						}, nil
					},
				},
				Logger: clog.New(clog.DebugLevel),
			},
			args: args{
				username: "billysteve",
				provider: "google",
				scheme:   "oauth2",
				role:     "admin",
				useAuth:  true,
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
								server.AdminRole,
							},
						}, nil
					},
				},
				Logger: clog.New(clog.DebugLevel),
			},
			args: args{
				username: "billysteve",
				provider: "google",
				scheme:   "oauth2",
				role:     "admin",
				useAuth:  true,
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
				Logger: clog.New(clog.DebugLevel),
			},
			args: args{
				username: "billysteve",
				provider: "google",
				scheme:   "oauth2",
				role:     "view",
				useAuth:  true,
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
				Logger: clog.New(clog.DebugLevel),
			},
			args: args{
				username: "billysteve",
				provider: "google",
				scheme:   "oauth2",
				role:     "editor",
				useAuth:  true,
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
				Logger: clog.New(clog.DebugLevel),
			},
			args: args{
				username: "billysteve",
				provider: "google",
				scheme:   "oauth2",
				role:     "admin",
				useAuth:  true,
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
				Logger: clog.New(clog.DebugLevel),
			},
			args: args{
				username: "billysteve",
				provider: "google",
				scheme:   "oauth2",
				role:     "viewer",
				useAuth:  true,
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
				Logger: clog.New(clog.DebugLevel),
			},
			args: args{
				username: "billysteve",
				provider: "google",
				scheme:   "oauth2",
				role:     "editor",
				useAuth:  true,
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
				Logger: clog.New(clog.DebugLevel),
			},
			args: args{
				username: "billysteve",
				provider: "google",
				scheme:   "oauth2",
				role:     "admin",
				useAuth:  true,
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
			fn := server.AuthorizedUser(tt.fields.UsersStore, tt.args.useAuth, tt.args.role, tt.fields.Logger, next)

			w := httptest.NewRecorder()
			r := httptest.NewRequest(
				"GET",
				"http://any.url", // can be any valid URL as we are bypassing mux
				nil,
			)
			r = r.WithContext(context.WithValue(r.Context(), oauth2.PrincipalKey, oauth2.Principal{
				Subject: tt.args.username,
				Issuer:  tt.args.provider,
			}))
			fn(w, r)

			if authorized != tt.authorized {
				t.Errorf("%q. AuthorizedUser() = %v, expected %v", tt.name, authorized, tt.authorized)
			}

		})
	}
}
