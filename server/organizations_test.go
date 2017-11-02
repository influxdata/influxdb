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
						case 1337:
							return &chronograf.Organization{
								ID:   1337,
								Name: "The Good Place",
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
			wantBody:        `{"id":"1337","name":"The Good Place","links":{"self":"/chronograf/v1/organizations/1337"}}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Service{
				Store: &Store{
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
								ID:   1337,
								Name: "The Good Place",
							},
							chronograf.Organization{
								ID:   100,
								Name: "The Bad Place",
							},
						}, nil
					},
				},
			},
			wantStatus:      http.StatusOK,
			wantContentType: "application/json",
			wantBody:        `{"organizations":[{"id":"1337","name":"The Good Place","links":{"self":"/chronograf/v1/organizations/1337"}},{"id":"100","name":"The Bad Place","links":{"self":"/chronograf/v1/organizations/100"}}],"links":{"self":"/chronograf/v1/organizations"}}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Service{
				Store: &Store{
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
		w   *httptest.ResponseRecorder
		r   *http.Request
		org *organizationRequest
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
							ID:   1337,
							Name: "The Good Place",
						}, nil
					},
				},
			},
			id:              "1337",
			wantStatus:      http.StatusOK,
			wantContentType: "application/json",
			wantBody:        `{"id":"1337","name":"The Bad Place","links":{"self":"/chronograf/v1/organizations/1337"}}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Service{
				Store: &Store{
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
						case 1337:
							return &chronograf.Organization{
								ID:   1337,
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
				Store: &Store{
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
		Logger             chronograf.Logger
	}
	type args struct {
		w   *httptest.ResponseRecorder
		r   *http.Request
		org *organizationRequest
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
				org: &organizationRequest{
					Name: "The Good Place",
				},
			},
			fields: fields{
				Logger: log.New(log.DebugLevel),
				OrganizationsStore: &mocks.OrganizationsStore{
					AddF: func(ctx context.Context, o *chronograf.Organization) (*chronograf.Organization, error) {
						return &chronograf.Organization{
							ID:   1337,
							Name: "The Good Place",
						}, nil
					},
				},
			},
			wantStatus:      http.StatusCreated,
			wantContentType: "application/json",
			wantBody:        `{"id":"1337","name":"The Good Place","links":{"self":"/chronograf/v1/organizations/1337"}}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Service{
				Store: &Store{
					OrganizationsStore: tt.fields.OrganizationsStore,
				},
				Logger: tt.fields.Logger,
			}

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
