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

	"github.com/bouk/httprouter"
	"github.com/influxdata/chronograf"
	"github.com/influxdata/chronograf/log"
	"github.com/influxdata/chronograf/mocks"
)

func TestService_UserID(t *testing.T) {
	type fields struct {
		UsersStore chronograf.UsersStore
		Logger     chronograf.Logger
	}
	type args struct {
		w *httptest.ResponseRecorder
		r *http.Request
	}
	tests := []struct {
		name            string
		fields          fields
		args            args
		id              string
		wantStatus      int
		wantContentType string
		wantBody        string
	}{
		{
			name: "Get Single Chronograf User",
			args: args{
				w: httptest.NewRecorder(),
				r: httptest.NewRequest(
					"GET",
					"http://any.url", // can be any valid URL as we are bypassing mux
					nil,
				),
			},
			fields: fields{
				Logger: log.New(log.DebugLevel),
				UsersStore: &mocks.UsersStore{
					GetF: func(ctx context.Context, q chronograf.UserQuery) (*chronograf.User, error) {
						switch *q.ID {
						case 1337:
							return &chronograf.User{
								ID:       1337,
								Name:     "billysteve",
								Provider: "google",
								Scheme:   "oauth2",
								Roles: []chronograf.Role{
									ViewerRole,
								},
							}, nil
						default:
							return nil, fmt.Errorf("User with ID %d not found", *q.ID)
						}
					},
				},
			},
			id:              "1337",
			wantStatus:      http.StatusOK,
			wantContentType: "application/json",
			wantBody:        `{"id":"1337","superAdmin":false,"name":"billysteve","provider":"google","scheme":"oauth2","links":{"self":"/chronograf/v1/users/1337"},"roles":[{"name":"viewer"}]}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Service{
				Store: &mocks.Store{
					UsersStore: tt.fields.UsersStore,
				},
				Logger: tt.fields.Logger,
			}

			tt.args.r = tt.args.r.WithContext(httprouter.WithParams(
				context.Background(),
				httprouter.Params{
					{
						Key:   "id",
						Value: tt.id,
					},
				}))

			s.UserID(tt.args.w, tt.args.r)

			resp := tt.args.w.Result()
			content := resp.Header.Get("Content-Type")
			body, _ := ioutil.ReadAll(resp.Body)

			if resp.StatusCode != tt.wantStatus {
				t.Errorf("%q. UserID() = %v, want %v", tt.name, resp.StatusCode, tt.wantStatus)
			}
			if tt.wantContentType != "" && content != tt.wantContentType {
				t.Errorf("%q. UserID() = %v, want %v", tt.name, content, tt.wantContentType)
			}
			if eq, _ := jsonEqual(string(body), tt.wantBody); tt.wantBody != "" && !eq {
				t.Errorf("%q. UserID() = \n***%v***\n,\nwant\n***%v***", tt.name, string(body), tt.wantBody)
			}
		})
	}
}

func TestService_NewUser(t *testing.T) {
	type fields struct {
		UsersStore chronograf.UsersStore
		Logger     chronograf.Logger
	}
	type args struct {
		w    *httptest.ResponseRecorder
		r    *http.Request
		user *userRequest
	}
	tests := []struct {
		name            string
		fields          fields
		args            args
		wantStatus      int
		wantContentType string
		wantBody        string
	}{
		{
			name: "Create a new Chronograf User",
			args: args{
				w: httptest.NewRecorder(),
				r: httptest.NewRequest(
					"POST",
					"http://any.url",
					nil,
				),
				user: &userRequest{
					Name:     "bob",
					Provider: "github",
					Scheme:   "oauth2",
				},
			},
			fields: fields{
				Logger: log.New(log.DebugLevel),
				UsersStore: &mocks.UsersStore{
					AddF: func(ctx context.Context, user *chronograf.User) (*chronograf.User, error) {
						return &chronograf.User{
							ID:       1338,
							Name:     "bob",
							Provider: "github",
							Scheme:   "oauth2",
							Roles:    []chronograf.Role{},
						}, nil
					},
				},
			},
			wantStatus:      http.StatusCreated,
			wantContentType: "application/json",
			wantBody:        `{"id":"1338","superAdmin":false,"name":"bob","provider":"github","scheme":"oauth2","roles":[],"links":{"self":"/chronograf/v1/users/1338"}}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Service{
				Store: &mocks.Store{
					UsersStore: tt.fields.UsersStore,
				},
				Logger: tt.fields.Logger,
			}

			buf, _ := json.Marshal(tt.args.user)
			tt.args.r.Body = ioutil.NopCloser(bytes.NewReader(buf))

			s.NewUser(tt.args.w, tt.args.r)

			resp := tt.args.w.Result()
			content := resp.Header.Get("Content-Type")
			body, _ := ioutil.ReadAll(resp.Body)

			if resp.StatusCode != tt.wantStatus {
				t.Errorf("%q. UserID() = %v, want %v", tt.name, resp.StatusCode, tt.wantStatus)
			}
			if tt.wantContentType != "" && content != tt.wantContentType {
				t.Errorf("%q. UserID() = %v, want %v", tt.name, content, tt.wantContentType)
			}
			if eq, _ := jsonEqual(string(body), tt.wantBody); tt.wantBody != "" && !eq {
				t.Errorf("%q. UserID() = \n***%v***\n,\nwant\n***%v***", tt.name, string(body), tt.wantBody)
			}
		})
	}
}

func TestService_RemoveUser(t *testing.T) {
	type fields struct {
		UsersStore chronograf.UsersStore
		Logger     chronograf.Logger
	}
	type args struct {
		w    *httptest.ResponseRecorder
		r    *http.Request
		user *userRequest
	}
	tests := []struct {
		name       string
		fields     fields
		args       args
		user       *chronograf.User
		id         string
		wantStatus int
	}{
		{
			name: "Delete a Chronograf User",
			fields: fields{
				Logger: log.New(log.DebugLevel),
				UsersStore: &mocks.UsersStore{
					GetF: func(ctx context.Context, q chronograf.UserQuery) (*chronograf.User, error) {
						switch *q.ID {
						case 1339:
							return &chronograf.User{
								ID:       1339,
								Name:     "helena",
								Provider: "heroku",
								Scheme:   "oauth2",
							}, nil
						default:
							return nil, fmt.Errorf("User with ID %d not found", *q.ID)
						}
					},
					DeleteF: func(ctx context.Context, user *chronograf.User) error {
						return nil
					},
				},
			},
			args: args{
				w: httptest.NewRecorder(),
				r: httptest.NewRequest(
					"DELETE",
					"http://any.url",
					nil,
				),
				user: &userRequest{
					ID:       1339,
					Name:     "helena",
					Provider: "heroku",
					Scheme:   "oauth2",
				},
			},
			id:         "1339",
			wantStatus: http.StatusNoContent,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Service{
				Store: &mocks.Store{
					UsersStore: tt.fields.UsersStore,
				},
				Logger: tt.fields.Logger,
			}

			tt.args.r = tt.args.r.WithContext(httprouter.WithParams(
				context.Background(),
				httprouter.Params{
					{
						Key:   "id",
						Value: tt.id,
					},
				},
			))

			s.RemoveUser(tt.args.w, tt.args.r)

			resp := tt.args.w.Result()

			if resp.StatusCode != tt.wantStatus {
				t.Errorf("%q. RemoveUser() = %v, want %v", tt.name, resp.StatusCode, tt.wantStatus)
			}
		})
	}
}

func TestService_UpdateUser(t *testing.T) {
	type fields struct {
		UsersStore chronograf.UsersStore
		Logger     chronograf.Logger
	}
	type args struct {
		w    *httptest.ResponseRecorder
		r    *http.Request
		user *userRequest
	}
	tests := []struct {
		name            string
		fields          fields
		args            args
		id              string
		wantStatus      int
		wantContentType string
		wantBody        string
	}{
		{
			name: "Update a Chronograf user",
			fields: fields{
				Logger: log.New(log.DebugLevel),
				UsersStore: &mocks.UsersStore{
					UpdateF: func(ctx context.Context, user *chronograf.User) error {
						return nil
					},
					GetF: func(ctx context.Context, q chronograf.UserQuery) (*chronograf.User, error) {
						switch *q.ID {
						case 1336:
							return &chronograf.User{
								ID:       1336,
								Name:     "bobbetta",
								Provider: "github",
								Scheme:   "oauth2",
								Roles: []chronograf.Role{
									EditorRole,
								},
							}, nil
						default:
							return nil, fmt.Errorf("User with ID %d not found", *q.ID)
						}
					},
				},
			},
			args: args{
				w: httptest.NewRecorder(),
				r: httptest.NewRequest(
					"PATCH",
					"http://any.url",
					nil,
				),
				user: &userRequest{
					ID: 1336,
					Roles: []chronograf.Role{
						AdminRole,
					},
				},
			},
			id:              "1336",
			wantStatus:      http.StatusOK,
			wantContentType: "application/json",
			wantBody:        `{"id":"1336","superAdmin":false,"name":"bobbetta","provider":"github","scheme":"oauth2","links":{"self":"/chronograf/v1/users/1336"},"roles":[{"name":"admin"}]}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Service{
				Store: &mocks.Store{
					UsersStore: tt.fields.UsersStore,
				},
				Logger: tt.fields.Logger,
			}

			tt.args.r = tt.args.r.WithContext(httprouter.WithParams(context.Background(),
				httprouter.Params{
					{
						Key:   "id",
						Value: tt.id,
					},
				}))
			buf, _ := json.Marshal(tt.args.user)
			tt.args.r.Body = ioutil.NopCloser(bytes.NewReader(buf))

			s.UpdateUser(tt.args.w, tt.args.r)

			resp := tt.args.w.Result()
			content := resp.Header.Get("Content-Type")
			body, _ := ioutil.ReadAll(resp.Body)

			if resp.StatusCode != tt.wantStatus {
				t.Errorf("%q. UpdateUser() = %v, want %v", tt.name, resp.StatusCode, tt.wantStatus)
			}
			if tt.wantContentType != "" && content != tt.wantContentType {
				t.Errorf("%q. UpdateUser() = %v, want %v", tt.name, content, tt.wantContentType)
			}
			if eq, _ := jsonEqual(string(body), tt.wantBody); tt.wantBody != "" && !eq {
				t.Errorf("%q. UpdateUser()\ngot:%v\n,\nwant:%v", tt.name, string(body), tt.wantBody)
			}
		})
	}
}

func TestService_Users(t *testing.T) {
	type fields struct {
		UsersStore chronograf.UsersStore
		Logger     chronograf.Logger
	}
	type args struct {
		w *httptest.ResponseRecorder
		r *http.Request
	}
	tests := []struct {
		name            string
		fields          fields
		args            args
		wantStatus      int
		wantContentType string
		wantBody        string
	}{
		{
			name: "Get all Chronograf users",
			fields: fields{
				Logger: log.New(log.DebugLevel),
				UsersStore: &mocks.UsersStore{
					AllF: func(ctx context.Context) ([]chronograf.User, error) {
						return []chronograf.User{
							{
								ID:       1337,
								Name:     "billysteve",
								Provider: "google",
								Scheme:   "oauth2",
								Roles: []chronograf.Role{
									EditorRole,
								},
							},
							{
								ID:       1338,
								Name:     "bobbettastuhvetta",
								Provider: "auth0",
								Scheme:   "oauth2",
							},
						}, nil
					},
				},
			},
			args: args{
				w: httptest.NewRecorder(),
				r: httptest.NewRequest(
					"GET",
					"http://any.url", // can be any valid URL as we are bypassing mux
					nil,
				),
			},
			wantStatus:      http.StatusOK,
			wantContentType: "application/json",
			wantBody:        `{"users":[{"id":"1337","superAdmin":false,"name":"billysteve","provider":"google","scheme":"oauth2","roles":[{"name":"editor"}],"links":{"self":"/chronograf/v1/users/1337"}},{"id":"1338","superAdmin":false,"name":"bobbettastuhvetta","provider":"auth0","scheme":"oauth2","roles":[],"links":{"self":"/chronograf/v1/users/1338"}}],"links":{"self":"/chronograf/v1/users"}}`,
		},
		{
			name: "Get all Chronograf users, ensuring order of users in response",
			fields: fields{
				Logger: log.New(log.DebugLevel),
				UsersStore: &mocks.UsersStore{
					AllF: func(ctx context.Context) ([]chronograf.User, error) {
						return []chronograf.User{
							{
								ID:       1338,
								Name:     "bobbettastuhvetta",
								Provider: "auth0",
								Scheme:   "oauth2",
							},
							{
								ID:       1337,
								Name:     "billysteve",
								Provider: "google",
								Scheme:   "oauth2",
								Roles: []chronograf.Role{
									EditorRole,
								},
							},
						}, nil
					},
				},
			},
			args: args{
				w: httptest.NewRecorder(),
				r: httptest.NewRequest(
					"GET",
					"http://any.url", // can be any valid URL as we are bypassing mux
					nil,
				),
			},
			wantStatus:      http.StatusOK,
			wantContentType: "application/json",
			wantBody:        `{"users":[{"id":"1337","superAdmin":false,"name":"billysteve","provider":"google","scheme":"oauth2","roles":[{"name":"editor"}],"links":{"self":"/chronograf/v1/users/1337"}},{"id":"1338","superAdmin":false,"name":"bobbettastuhvetta","provider":"auth0","scheme":"oauth2","roles":[],"links":{"self":"/chronograf/v1/users/1338"}}],"links":{"self":"/chronograf/v1/users"}}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Service{
				Store: &mocks.Store{
					UsersStore: tt.fields.UsersStore,
				},
				Logger: tt.fields.Logger,
			}

			s.Users(tt.args.w, tt.args.r)

			resp := tt.args.w.Result()
			content := resp.Header.Get("Content-Type")
			body, _ := ioutil.ReadAll(resp.Body)

			if resp.StatusCode != tt.wantStatus {
				t.Errorf("%q. Users() = %v, want %v", tt.name, resp.StatusCode, tt.wantStatus)
			}
			if tt.wantContentType != "" && content != tt.wantContentType {
				t.Errorf("%q. Users() = %v, want %v", tt.name, content, tt.wantContentType)
			}
			if eq, _ := jsonEqual(string(body), tt.wantBody); tt.wantBody != "" && !eq {
				t.Errorf("%q. Users() = \n***%v***\n,\nwant\n***%v***", tt.name, string(body), tt.wantBody)
			}
		})
	}
}

func TestUserRequest_ValidCreate(t *testing.T) {
	type args struct {
		u *userRequest
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
		err     error
	}{
		{
			name: "Valid",
			args: args{
				u: &userRequest{
					ID:       1337,
					Name:     "billietta",
					Provider: "auth0",
					Scheme:   "oauth2",
					Roles: []chronograf.Role{
						EditorRole,
					},
				},
			},
			wantErr: false,
			err:     nil,
		},
		{
			name: "Invalid – Name missing",
			args: args{
				u: &userRequest{
					ID:       1337,
					Provider: "auth0",
					Scheme:   "oauth2",
					Roles: []chronograf.Role{
						EditorRole,
					},
				},
			},
			wantErr: true,
			err:     fmt.Errorf("Name required on Chronograf User request body"),
		},
		{
			name: "Invalid – Provider missing",
			args: args{
				u: &userRequest{
					ID:     1337,
					Name:   "billietta",
					Scheme: "oauth2",
					Roles: []chronograf.Role{
						EditorRole,
					},
				},
			},
			wantErr: true,
			err:     fmt.Errorf("Provider required on Chronograf User request body"),
		},
		{
			name: "Invalid – Scheme missing",
			args: args{
				u: &userRequest{
					ID:       1337,
					Name:     "billietta",
					Provider: "auth0",
					Roles: []chronograf.Role{
						EditorRole,
					},
				},
			},
			wantErr: true,
			err:     fmt.Errorf("Scheme required on Chronograf User request body"),
		},
		{
			name: "Invalid roles",
			args: args{
				u: &userRequest{
					ID:       1337,
					Name:     "billietta",
					Provider: "auth0",
					Scheme:   "oauth2",
					Roles: []chronograf.Role{
						{
							Name: "BilliettaSpecialRole",
						},
					},
				},
			},
			wantErr: true,
			err:     fmt.Errorf("Unknown role BilliettaSpecialRole. Valid roles are 'viewer', 'editor', 'admin', and 'superadmin'"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.args.u.ValidCreate()

			if tt.wantErr {
				if err == nil || err.Error() != tt.err.Error() {
					t.Errorf("%q. ValidCreate(): wantErr %v,\nwant %v,\ngot %v", tt.name, tt.wantErr, tt.err, err)
				}
			} else {
				if err != nil {
					t.Errorf("%q. ValidCreate(): wantErr %v,\nwant %v,\ngot %v", tt.name, tt.wantErr, tt.err, err)
				}
			}
		})
	}
}

func TestUserRequest_ValidUpdate(t *testing.T) {
	type args struct {
		u *userRequest
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
		err     error
	}{
		{
			name: "Valid",
			args: args{
				u: &userRequest{
					ID: 1337,
					Roles: []chronograf.Role{
						EditorRole,
					},
				},
			},
			wantErr: false,
			err:     nil,
		},
		{
			name: "Invalid – roles missing",
			args: args{
				u: &userRequest{},
			},
			wantErr: true,
			err:     fmt.Errorf("No Roles to update"),
		},
		{
			name: "Invalid: field missing",
			args: args{
				u: &userRequest{},
			},
			wantErr: true,
			err:     fmt.Errorf("No Roles to update"),
		},
		{
			name: "Invalid: Name attempted",
			args: args{
				u: &userRequest{
					Name: "bob",
				},
			},
			wantErr: true,
			err:     fmt.Errorf("Cannot update Name"),
		},
		{
			name: "Invalid: Provider attempted",
			args: args{
				u: &userRequest{
					Provider: "Goggles",
				},
			},
			wantErr: true,
			err:     fmt.Errorf("Cannot update Provider"),
		},
		{
			name: "Invalid: Scheme attempted",
			args: args{
				u: &userRequest{
					Scheme: "leDAP",
				},
			},
			wantErr: true,
			err:     fmt.Errorf("Cannot update Scheme"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.args.u.ValidUpdate()

			if tt.wantErr {
				if err == nil || err.Error() != tt.err.Error() {
					t.Errorf("%q. ValidUpdate(): wantErr %v,\nwant %v,\ngot %v", tt.name, tt.wantErr, tt.err, err)
				}
			} else {
				if err != nil {
					t.Errorf("%q. ValidUpdate(): wantErr %v,\nwant %v,\ngot %v", tt.name, tt.wantErr, tt.err, err)
				}
			}
		})
	}
}
