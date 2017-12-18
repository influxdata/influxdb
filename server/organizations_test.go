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
	"github.com/influxdata/chronograf/roles"
)

func TestService_OrganizationID(t *testing.T) {
	type fields struct {
		OrganizationsStore chronograf.OrganizationsStore
		Logger             chronograf.Logger
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
			name: "Get Single Organization",
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
				OrganizationsStore: &mocks.OrganizationsStore{
					GetF: func(ctx context.Context, q chronograf.OrganizationQuery) (*chronograf.Organization, error) {
						switch *q.ID {
						case "1337":
							return &chronograf.Organization{
								ID:     "1337",
								Name:   "The Good Place",
								Public: false,
							}, nil
						default:
							return nil, fmt.Errorf("Organization with ID %s not found", *q.ID)
						}
					},
				},
			},
			id:              "1337",
			wantStatus:      http.StatusOK,
			wantContentType: "application/json",
			wantBody:        `{"links":{"self":"/chronograf/v1/organizations/1337"},"id":"1337","name":"The Good Place","public":false}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Service{
				Store: &mocks.Store{
					OrganizationsStore: tt.fields.OrganizationsStore,
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

			s.OrganizationID(tt.args.w, tt.args.r)

			resp := tt.args.w.Result()
			content := resp.Header.Get("Content-Type")
			body, _ := ioutil.ReadAll(resp.Body)

			if resp.StatusCode != tt.wantStatus {
				t.Errorf("%q. OrganizationID() = %v, want %v", tt.name, resp.StatusCode, tt.wantStatus)
			}
			if tt.wantContentType != "" && content != tt.wantContentType {
				t.Errorf("%q. OrganizationID() = %v, want %v", tt.name, content, tt.wantContentType)
			}
			if eq, _ := jsonEqual(string(body), tt.wantBody); tt.wantBody != "" && !eq {
				t.Errorf("%q. OrganizationID() = \n***%v***\n,\nwant\n***%v***", tt.name, string(body), tt.wantBody)
			}
		})
	}
}

func TestService_Organizations(t *testing.T) {
	type fields struct {
		OrganizationsStore chronograf.OrganizationsStore
		Logger             chronograf.Logger
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
			name: "Get Single Organization",
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
				OrganizationsStore: &mocks.OrganizationsStore{
					AllF: func(ctx context.Context) ([]chronograf.Organization, error) {
						return []chronograf.Organization{
							chronograf.Organization{
								ID:     "1337",
								Name:   "The Good Place",
								Public: false,
							},
							chronograf.Organization{
								ID:     "100",
								Name:   "The Bad Place",
								Public: false,
							},
						}, nil
					},
				},
			},
			wantStatus:      http.StatusOK,
			wantContentType: "application/json",
			wantBody:        `{"links":{"self":"/chronograf/v1/organizations"},"organizations":[{"links":{"self":"/chronograf/v1/organizations/1337"},"id":"1337","name":"The Good Place","public":false},{"links":{"self":"/chronograf/v1/organizations/100"},"id":"100","name":"The Bad Place","public":false}]}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Service{
				Store: &mocks.Store{
					OrganizationsStore: tt.fields.OrganizationsStore,
				},
				Logger: tt.fields.Logger,
			}

			s.Organizations(tt.args.w, tt.args.r)

			resp := tt.args.w.Result()
			content := resp.Header.Get("Content-Type")
			body, _ := ioutil.ReadAll(resp.Body)

			if resp.StatusCode != tt.wantStatus {
				t.Errorf("%q. Organizations() = %v, want %v", tt.name, resp.StatusCode, tt.wantStatus)
			}
			if tt.wantContentType != "" && content != tt.wantContentType {
				t.Errorf("%q. Organizations() = %v, want %v", tt.name, content, tt.wantContentType)
			}
			if eq, _ := jsonEqual(string(body), tt.wantBody); tt.wantBody != "" && !eq {
				t.Errorf("%q. Organizations() = \n***%v***\n,\nwant\n***%v***", tt.name, string(body), tt.wantBody)
			}
		})
	}
}

func TestService_UpdateOrganization(t *testing.T) {
	type fields struct {
		OrganizationsStore chronograf.OrganizationsStore
		Logger             chronograf.Logger
	}
	type args struct {
		w      *httptest.ResponseRecorder
		r      *http.Request
		org    *organizationRequest
		public bool
		setPtr bool
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
			name: "Update Organization name",
			args: args{
				w: httptest.NewRecorder(),
				r: httptest.NewRequest(
					"GET",
					"http://any.url", // can be any valid URL as we are bypassing mux
					nil,
				),
				org: &organizationRequest{
					Name: "The Bad Place",
				},
			},
			fields: fields{
				Logger: log.New(log.DebugLevel),
				OrganizationsStore: &mocks.OrganizationsStore{
					UpdateF: func(ctx context.Context, o *chronograf.Organization) error {
						return nil
					},
					GetF: func(ctx context.Context, q chronograf.OrganizationQuery) (*chronograf.Organization, error) {
						return &chronograf.Organization{
							ID:          "1337",
							Name:        "The Good Place",
							DefaultRole: roles.ViewerRoleName,
							Public:      false,
						}, nil
					},
				},
			},
			id:              "1337",
			wantStatus:      http.StatusOK,
			wantContentType: "application/json",
			wantBody:        `{"id":"1337","name":"The Bad Place","defaultRole":"viewer","links":{"self":"/chronograf/v1/organizations/1337"},"public":false}`,
		},
		{
			name: "Update Organization public",
			args: args{
				w: httptest.NewRecorder(),
				r: httptest.NewRequest(
					"GET",
					"http://any.url", // can be any valid URL as we are bypassing mux
					nil,
				),
				org:    &organizationRequest{},
				public: false,
				setPtr: true,
			},
			fields: fields{
				Logger: log.New(log.DebugLevel),
				OrganizationsStore: &mocks.OrganizationsStore{
					UpdateF: func(ctx context.Context, o *chronograf.Organization) error {
						return nil
					},
					GetF: func(ctx context.Context, q chronograf.OrganizationQuery) (*chronograf.Organization, error) {
						return &chronograf.Organization{
							ID:          "0",
							Name:        "The Good Place",
							DefaultRole: roles.ViewerRoleName,
							Public:      true,
						}, nil
					},
				},
			},
			id:              "0",
			wantStatus:      http.StatusOK,
			wantContentType: "application/json",
			wantBody:        `{"id":"0","name":"The Good Place","defaultRole":"viewer","public":false,"links":{"self":"/chronograf/v1/organizations/0"}}`,
		},
		{
			name: "Update Organization - nothing to update",
			args: args{
				w: httptest.NewRecorder(),
				r: httptest.NewRequest(
					"GET",
					"http://any.url", // can be any valid URL as we are bypassing mux
					nil,
				),
				org: &organizationRequest{},
			},
			fields: fields{
				Logger: log.New(log.DebugLevel),
				OrganizationsStore: &mocks.OrganizationsStore{
					UpdateF: func(ctx context.Context, o *chronograf.Organization) error {
						return nil
					},
					GetF: func(ctx context.Context, q chronograf.OrganizationQuery) (*chronograf.Organization, error) {
						return &chronograf.Organization{
							ID:          "1337",
							Name:        "The Good Place",
							DefaultRole: roles.ViewerRoleName,
							Public:      true,
						}, nil
					},
				},
			},
			id:              "1337",
			wantStatus:      http.StatusUnprocessableEntity,
			wantContentType: "application/json",
			wantBody:        `{"code":422,"message":"No fields to update"}`,
		},
		{
			name: "Update Organization default role",
			args: args{
				w: httptest.NewRecorder(),
				r: httptest.NewRequest(
					"GET",
					"http://any.url", // can be any valid URL as we are bypassing mux
					nil,
				),
				org: &organizationRequest{
					DefaultRole: roles.ViewerRoleName,
				},
			},
			fields: fields{
				Logger: log.New(log.DebugLevel),
				OrganizationsStore: &mocks.OrganizationsStore{
					UpdateF: func(ctx context.Context, o *chronograf.Organization) error {
						return nil
					},
					GetF: func(ctx context.Context, q chronograf.OrganizationQuery) (*chronograf.Organization, error) {
						return &chronograf.Organization{
							ID:          "1337",
							Name:        "The Good Place",
							DefaultRole: roles.MemberRoleName,
							Public:      false,
						}, nil
					},
				},
			},
			id:              "1337",
			wantStatus:      http.StatusOK,
			wantContentType: "application/json",
			wantBody:        `{"links":{"self":"/chronograf/v1/organizations/1337"},"id":"1337","name":"The Good Place","defaultRole":"viewer","public":false}`,
		},
		{
			name: "Update Organization - invalid update",
			args: args{
				w: httptest.NewRecorder(),
				r: httptest.NewRequest(
					"GET",
					"http://any.url", // can be any valid URL as we are bypassing mux
					nil,
				),
				org: &organizationRequest{},
			},
			fields: fields{
				Logger: log.New(log.DebugLevel),
				OrganizationsStore: &mocks.OrganizationsStore{
					UpdateF: func(ctx context.Context, o *chronograf.Organization) error {
						return nil
					},
					GetF: func(ctx context.Context, q chronograf.OrganizationQuery) (*chronograf.Organization, error) {
						return nil, nil
					},
				},
			},
			id:              "1337",
			wantStatus:      http.StatusUnprocessableEntity,
			wantContentType: "application/json",
			wantBody:        `{"code":422,"message":"No fields to update"}`,
		},
		{
			name: "Update Organization - invalid role",
			args: args{
				w: httptest.NewRecorder(),
				r: httptest.NewRequest(
					"GET",
					"http://any.url", // can be any valid URL as we are bypassing mux
					nil,
				),
				org: &organizationRequest{
					DefaultRole: "sillyrole",
				},
			},
			fields: fields{
				Logger: log.New(log.DebugLevel),
				OrganizationsStore: &mocks.OrganizationsStore{
					UpdateF: func(ctx context.Context, o *chronograf.Organization) error {
						return nil
					},
					GetF: func(ctx context.Context, q chronograf.OrganizationQuery) (*chronograf.Organization, error) {
						return nil, nil
					},
				},
			},
			id:              "1337",
			wantStatus:      http.StatusUnprocessableEntity,
			wantContentType: "application/json",
			wantBody:        `{"code":422,"message":"default role must be member, viewer, editor, or admin"}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Service{
				Store: &mocks.Store{
					OrganizationsStore: tt.fields.OrganizationsStore,
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

			if tt.args.setPtr {
				tt.args.org.Public = &tt.args.public
			}

			buf, _ := json.Marshal(tt.args.org)
			tt.args.r.Body = ioutil.NopCloser(bytes.NewReader(buf))
			s.UpdateOrganization(tt.args.w, tt.args.r)

			resp := tt.args.w.Result()
			content := resp.Header.Get("Content-Type")
			body, _ := ioutil.ReadAll(resp.Body)

			if resp.StatusCode != tt.wantStatus {
				t.Errorf("%q. NewOrganization() = %v, want %v", tt.name, resp.StatusCode, tt.wantStatus)
			}
			if tt.wantContentType != "" && content != tt.wantContentType {
				t.Errorf("%q. NewOrganization() = %v, want %v", tt.name, content, tt.wantContentType)
			}
			if eq, _ := jsonEqual(string(body), tt.wantBody); tt.wantBody != "" && !eq {
				t.Errorf("%q. NewOrganization() = \n***%v***\n,\nwant\n***%v***", tt.name, string(body), tt.wantBody)
			}
		})
	}
}

func TestService_RemoveOrganization(t *testing.T) {
	type fields struct {
		OrganizationsStore chronograf.OrganizationsStore
		Logger             chronograf.Logger
	}
	type args struct {
		w *httptest.ResponseRecorder
		r *http.Request
	}
	tests := []struct {
		name       string
		fields     fields
		args       args
		id         string
		wantStatus int
	}{
		{
			name: "Update Organization name",
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
				OrganizationsStore: &mocks.OrganizationsStore{
					DeleteF: func(ctx context.Context, o *chronograf.Organization) error {
						return nil
					},
					GetF: func(ctx context.Context, q chronograf.OrganizationQuery) (*chronograf.Organization, error) {
						switch *q.ID {
						case "1337":
							return &chronograf.Organization{
								ID:   "1337",
								Name: "The Good Place",
							}, nil
						default:
							return nil, fmt.Errorf("Organization with ID %s not found", *q.ID)
						}
					},
				},
			},
			id:         "1337",
			wantStatus: http.StatusNoContent,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Service{
				Store: &mocks.Store{
					OrganizationsStore: tt.fields.OrganizationsStore,
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
			s.RemoveOrganization(tt.args.w, tt.args.r)

			resp := tt.args.w.Result()

			if resp.StatusCode != tt.wantStatus {
				t.Errorf("%q. NewOrganization() = %v, want %v", tt.name, resp.StatusCode, tt.wantStatus)
			}
		})
	}
}

func TestService_NewOrganization(t *testing.T) {
	type fields struct {
		OrganizationsStore chronograf.OrganizationsStore
		UsersStore         chronograf.UsersStore
		Logger             chronograf.Logger
	}
	type args struct {
		w    *httptest.ResponseRecorder
		r    *http.Request
		org  *organizationRequest
		user *chronograf.User
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
			name: "Create Organization",
			args: args{
				w: httptest.NewRecorder(),
				r: httptest.NewRequest(
					"GET",
					"http://any.url", // can be any valid URL as we are bypassing mux
					nil,
				),
				user: &chronograf.User{
					ID:       1,
					Name:     "bobetta",
					Provider: "github",
					Scheme:   "oauth2",
				},
				org: &organizationRequest{
					Name: "The Good Place",
				},
			},
			fields: fields{
				Logger: log.New(log.DebugLevel),
				UsersStore: &mocks.UsersStore{
					AddF: func(ctx context.Context, u *chronograf.User) (*chronograf.User, error) {
						return &chronograf.User{
							ID:       1,
							Name:     "bobetta",
							Provider: "github",
							Scheme:   "oauth2",
						}, nil
					},
				},
				OrganizationsStore: &mocks.OrganizationsStore{
					AddF: func(ctx context.Context, o *chronograf.Organization) (*chronograf.Organization, error) {
						return &chronograf.Organization{
							ID:     "1337",
							Name:   "The Good Place",
							Public: false,
						}, nil
					},
				},
			},
			wantStatus:      http.StatusCreated,
			wantContentType: "application/json",
			wantBody:        `{"id":"1337","public":false,"name":"The Good Place","links":{"self":"/chronograf/v1/organizations/1337"}}`,
		},
		{
			name: "Create Organization - no user on context",
			args: args{
				w: httptest.NewRecorder(),
				r: httptest.NewRequest(
					"GET",
					"http://any.url", // can be any valid URL as we are bypassing mux
					nil,
				),
				org: &organizationRequest{
					Name: "The Good Place",
				},
			},
			fields: fields{
				Logger: log.New(log.DebugLevel),
				UsersStore: &mocks.UsersStore{
					AddF: func(ctx context.Context, u *chronograf.User) (*chronograf.User, error) {
						return &chronograf.User{
							ID:       1,
							Name:     "bobetta",
							Provider: "github",
							Scheme:   "oauth2",
						}, nil
					},
				},
				OrganizationsStore: &mocks.OrganizationsStore{
					AddF: func(ctx context.Context, o *chronograf.Organization) (*chronograf.Organization, error) {
						return &chronograf.Organization{
							ID:   "1337",
							Name: "The Good Place",
						}, nil
					},
					DeleteF: func(ctx context.Context, o *chronograf.Organization) error {
						return nil
					},
				},
			},
			wantStatus:      http.StatusInternalServerError,
			wantContentType: "application/json",
			wantBody:        `{"code":500,"message":"failed to retrieve user from context"}`,
		},
		{
			name: "Create Organization - failed to add user to organization",
			args: args{
				w: httptest.NewRecorder(),
				r: httptest.NewRequest(
					"GET",
					"http://any.url", // can be any valid URL as we are bypassing mux
					nil,
				),
				org: &organizationRequest{
					Name: "The Good Place",
				},
				user: &chronograf.User{
					ID:       1,
					Name:     "bobetta",
					Provider: "github",
					Scheme:   "oauth2",
				},
			},
			fields: fields{
				Logger: log.New(log.DebugLevel),
				UsersStore: &mocks.UsersStore{
					AddF: func(ctx context.Context, u *chronograf.User) (*chronograf.User, error) {
						return nil, fmt.Errorf("failed to add user to org")
					},
				},
				OrganizationsStore: &mocks.OrganizationsStore{
					AddF: func(ctx context.Context, o *chronograf.Organization) (*chronograf.Organization, error) {
						return &chronograf.Organization{
							ID:   "1337",
							Name: "The Good Place",
						}, nil
					},
					DeleteF: func(ctx context.Context, o *chronograf.Organization) error {
						return nil
					},
				},
			},
			wantStatus:      http.StatusInternalServerError,
			wantContentType: "application/json",
			wantBody:        `{"code":500,"message":"failed to add user to organization"}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Service{
				Store: &mocks.Store{
					OrganizationsStore: tt.fields.OrganizationsStore,
					UsersStore:         tt.fields.UsersStore,
				},
				Logger: tt.fields.Logger,
			}

			ctx := tt.args.r.Context()
			ctx = context.WithValue(ctx, UserContextKey, tt.args.user)
			tt.args.r = tt.args.r.WithContext(ctx)

			buf, _ := json.Marshal(tt.args.org)
			tt.args.r.Body = ioutil.NopCloser(bytes.NewReader(buf))
			s.NewOrganization(tt.args.w, tt.args.r)

			resp := tt.args.w.Result()
			content := resp.Header.Get("Content-Type")
			body, _ := ioutil.ReadAll(resp.Body)

			if resp.StatusCode != tt.wantStatus {
				t.Errorf("%q. NewOrganization() = %v, want %v", tt.name, resp.StatusCode, tt.wantStatus)
			}
			if tt.wantContentType != "" && content != tt.wantContentType {
				t.Errorf("%q. NewOrganization() = %v, want %v", tt.name, content, tt.wantContentType)
			}
			if eq, _ := jsonEqual(string(body), tt.wantBody); tt.wantBody != "" && !eq {
				t.Errorf("%q. NewOrganization() = \n***%v***\n,\nwant\n***%v***", tt.name, string(body), tt.wantBody)
			}
		})
	}
}
