package server

import (
	"bytes"
	"context"
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

func TestService_NewSourceRole(t *testing.T) {
	type fields struct {
		SourcesStore chronograf.SourcesStore
		TimeSeries   TimeSeriesClient
		Logger       chronograf.Logger
	}
	type args struct {
		w *httptest.ResponseRecorder
		r *http.Request
	}
	tests := []struct {
		name            string
		fields          fields
		args            args
		ID              string
		wantStatus      int
		wantContentType string
		wantBody        string
	}{
		{
			name: "Bad JSON",
			args: args{
				w: httptest.NewRecorder(),
				r: httptest.NewRequest(
					"POST",
					"http://server.local/chronograf/v1/sources/1/roles",
					ioutil.NopCloser(
						bytes.NewReader([]byte(`{BAD}`)))),
			},
			fields: fields{
				Logger: log.New(log.DebugLevel),
			},
			wantStatus:      http.StatusBadRequest,
			wantContentType: "application/json",
			wantBody:        `{"code":400,"message":"Unparsable JSON"}`,
		},
		{
			name: "Invalid request",
			args: args{
				w: httptest.NewRecorder(),
				r: httptest.NewRequest(
					"POST",
					"http://server.local/chronograf/v1/sources/1/roles",
					ioutil.NopCloser(
						bytes.NewReader([]byte(`{"name": ""}`)))),
			},
			fields: fields{
				Logger: log.New(log.DebugLevel),
			},
			ID:              "1",
			wantStatus:      http.StatusUnprocessableEntity,
			wantContentType: "application/json",
			wantBody:        `{"code":422,"message":"Name is required for a role"}`,
		},
		{
			name: "Invalid source ID",
			args: args{
				w: httptest.NewRecorder(),
				r: httptest.NewRequest(
					"POST",
					"http://server.local/chronograf/v1/sources/1/roles",
					ioutil.NopCloser(
						bytes.NewReader([]byte(`{"name": "newrole"}`)))),
			},
			fields: fields{
				Logger: log.New(log.DebugLevel),
			},
			ID:              "BADROLE",
			wantStatus:      http.StatusUnprocessableEntity,
			wantContentType: "application/json",
			wantBody:        `{"code":422,"message":"Error converting ID BADROLE"}`,
		},
		{
			name: "Source doesn't support roles",
			args: args{
				w: httptest.NewRecorder(),
				r: httptest.NewRequest(
					"POST",
					"http://server.local/chronograf/v1/sources/1/roles",
					ioutil.NopCloser(
						bytes.NewReader([]byte(`{"name": "role"}`)))),
			},
			fields: fields{
				Logger: log.New(log.DebugLevel),
				SourcesStore: &mocks.SourcesStore{
					GetF: func(ctx context.Context, ID int) (chronograf.Source, error) {
						return chronograf.Source{
							ID:       1,
							Name:     "muh source",
							Username: "name",
							Password: "hunter2",
							URL:      "http://localhost:8086",
						}, nil
					},
				},
				TimeSeries: &mocks.TimeSeries{
					ConnectF: func(ctx context.Context, src *chronograf.Source) error {
						return nil
					},
					RolesF: func(ctx context.Context) (chronograf.RolesStore, error) {
						return nil, fmt.Errorf("roles not supported")
					},
				},
			},
			ID:              "1",
			wantStatus:      http.StatusNotFound,
			wantContentType: "application/json",
			wantBody:        `{"code":404,"message":"Source 1 does not have role capability"}`,
		},
		{
			name: "Unable to add role to server",
			args: args{
				w: httptest.NewRecorder(),
				r: httptest.NewRequest(
					"POST",
					"http://server.local/chronograf/v1/sources/1/roles",
					ioutil.NopCloser(
						bytes.NewReader([]byte(`{"name": "role"}`)))),
			},
			fields: fields{
				Logger: log.New(log.DebugLevel),
				SourcesStore: &mocks.SourcesStore{
					GetF: func(ctx context.Context, ID int) (chronograf.Source, error) {
						return chronograf.Source{
							ID:       1,
							Name:     "muh source",
							Username: "name",
							Password: "hunter2",
							URL:      "http://localhost:8086",
						}, nil
					},
				},
				TimeSeries: &mocks.TimeSeries{
					ConnectF: func(ctx context.Context, src *chronograf.Source) error {
						return nil
					},
					RolesF: func(ctx context.Context) (chronograf.RolesStore, error) {
						return &mocks.RolesStore{
							AddF: func(ctx context.Context, u *chronograf.Role) (*chronograf.Role, error) {
								return nil, fmt.Errorf("server had and issue")
							},
							GetF: func(ctx context.Context, name string) (*chronograf.Role, error) {
								return nil, fmt.Errorf("No such role")
							},
						}, nil
					},
				},
			},
			ID:              "1",
			wantStatus:      http.StatusBadRequest,
			wantContentType: "application/json",
			wantBody:        `{"code":400,"message":"server had and issue"}`,
		},
		{
			name: "New role for data source",
			args: args{
				w: httptest.NewRecorder(),
				r: httptest.NewRequest(
					"POST",
					"http://server.local/chronograf/v1/sources/1/roles",
					ioutil.NopCloser(
						bytes.NewReader([]byte(`{"name": "biffsgang","users": [{"name": "match"},{"name": "skinhead"},{"name": "3-d"}]}`)))),
			},
			fields: fields{
				Logger: log.New(log.DebugLevel),
				SourcesStore: &mocks.SourcesStore{
					GetF: func(ctx context.Context, ID int) (chronograf.Source, error) {
						return chronograf.Source{
							ID:       1,
							Name:     "muh source",
							Username: "name",
							Password: "hunter2",
							URL:      "http://localhost:8086",
						}, nil
					},
				},
				TimeSeries: &mocks.TimeSeries{
					ConnectF: func(ctx context.Context, src *chronograf.Source) error {
						return nil
					},
					RolesF: func(ctx context.Context) (chronograf.RolesStore, error) {
						return &mocks.RolesStore{
							AddF: func(ctx context.Context, u *chronograf.Role) (*chronograf.Role, error) {
								return u, nil
							},
							GetF: func(ctx context.Context, name string) (*chronograf.Role, error) {
								return nil, fmt.Errorf("no such role")
							},
						}, nil
					},
				},
			},
			ID:              "1",
			wantStatus:      http.StatusCreated,
			wantContentType: "application/json",
			wantBody: `{"users":[{"links":{"self":"/chronograf/v1/sources/1/users/match"},"name":"match"},{"links":{"self":"/chronograf/v1/sources/1/users/skinhead"},"name":"skinhead"},{"links":{"self":"/chronograf/v1/sources/1/users/3-d"},"name":"3-d"}],"name":"biffsgang","permissions":[],"links":{"self":"/chronograf/v1/sources/1/roles/biffsgang"}}
`,
		},
	}
	for _, tt := range tests {
		h := &Service{
			SourcesStore:     tt.fields.SourcesStore,
			TimeSeriesClient: tt.fields.TimeSeries,
			Logger:           tt.fields.Logger,
		}
		tt.args.r = tt.args.r.WithContext(httprouter.WithParams(
			context.Background(),
			httprouter.Params{
				{
					Key:   "id",
					Value: tt.ID,
				},
			}))

		h.NewRole(tt.args.w, tt.args.r)

		resp := tt.args.w.Result()
		content := resp.Header.Get("Content-Type")
		body, _ := ioutil.ReadAll(resp.Body)

		if resp.StatusCode != tt.wantStatus {
			t.Errorf("%q. NewRole() = %v, want %v", tt.name, resp.StatusCode, tt.wantStatus)
		}
		if tt.wantContentType != "" && content != tt.wantContentType {
			t.Errorf("%q. NewRole() = %v, want %v", tt.name, content, tt.wantContentType)
		}
		if tt.wantBody != "" && string(body) != tt.wantBody {
			t.Errorf("%q. NewRole() = \n***%v***\n,\nwant\n***%v***", tt.name, string(body), tt.wantBody)
		}
	}
}

func TestService_UpdateRole(t *testing.T) {
	type fields struct {
		SourcesStore chronograf.SourcesStore
		TimeSeries   TimeSeriesClient
		Logger       chronograf.Logger
	}
	type args struct {
		w *httptest.ResponseRecorder
		r *http.Request
	}
	tests := []struct {
		name            string
		fields          fields
		args            args
		ID              string
		RoleID          string
		wantStatus      int
		wantContentType string
		wantBody        string
	}{
		{
			name: "Update role for data source",
			args: args{
				w: httptest.NewRecorder(),
				r: httptest.NewRequest(
					"POST",
					"http://server.local/chronograf/v1/sources/1/roles",
					ioutil.NopCloser(
						bytes.NewReader([]byte(`{"name": "biffsgang","users": [{"name": "match"},{"name": "skinhead"},{"name": "3-d"}]}`)))),
			},
			fields: fields{
				Logger: log.New(log.DebugLevel),
				SourcesStore: &mocks.SourcesStore{
					GetF: func(ctx context.Context, ID int) (chronograf.Source, error) {
						return chronograf.Source{
							ID:       1,
							Name:     "muh source",
							Username: "name",
							Password: "hunter2",
							URL:      "http://localhost:8086",
						}, nil
					},
				},
				TimeSeries: &mocks.TimeSeries{
					ConnectF: func(ctx context.Context, src *chronograf.Source) error {
						return nil
					},
					RolesF: func(ctx context.Context) (chronograf.RolesStore, error) {
						return &mocks.RolesStore{
							UpdateF: func(ctx context.Context, u *chronograf.Role) error {
								return nil
							},
							GetF: func(ctx context.Context, name string) (*chronograf.Role, error) {
								return &chronograf.Role{
									Name: "biffsgang",
									Users: []chronograf.User{
										{
											Name: "match",
										},
										{
											Name: "skinhead",
										},
										{
											Name: "3-d",
										},
									},
								}, nil
							},
						}, nil
					},
				},
			},
			ID:              "1",
			RoleID:          "biffsgang",
			wantStatus:      http.StatusOK,
			wantContentType: "application/json",
			wantBody: `{"users":[{"links":{"self":"/chronograf/v1/sources/1/users/match"},"name":"match"},{"links":{"self":"/chronograf/v1/sources/1/users/skinhead"},"name":"skinhead"},{"links":{"self":"/chronograf/v1/sources/1/users/3-d"},"name":"3-d"}],"name":"biffsgang","permissions":[],"links":{"self":"/chronograf/v1/sources/1/roles/biffsgang"}}
`,
		},
	}
	for _, tt := range tests {
		h := &Service{
			SourcesStore:     tt.fields.SourcesStore,
			TimeSeriesClient: tt.fields.TimeSeries,
			Logger:           tt.fields.Logger,
		}

		tt.args.r = tt.args.r.WithContext(httprouter.WithParams(
			context.Background(),
			httprouter.Params{
				{
					Key:   "id",
					Value: tt.ID,
				},
				{
					Key:   "rid",
					Value: tt.RoleID,
				},
			}))

		h.UpdateRole(tt.args.w, tt.args.r)

		resp := tt.args.w.Result()
		content := resp.Header.Get("Content-Type")
		body, _ := ioutil.ReadAll(resp.Body)

		if resp.StatusCode != tt.wantStatus {
			t.Errorf("%q. NewRole() = %v, want %v", tt.name, resp.StatusCode, tt.wantStatus)
		}
		if tt.wantContentType != "" && content != tt.wantContentType {
			t.Errorf("%q. NewRole() = %v, want %v", tt.name, content, tt.wantContentType)
		}
		if tt.wantBody != "" && string(body) != tt.wantBody {
			t.Errorf("%q. NewRole() = \n***%v***\n,\nwant\n***%v***", tt.name, string(body), tt.wantBody)
		}
	}
}

func TestService_RoleID(t *testing.T) {
	type fields struct {
		SourcesStore chronograf.SourcesStore
		TimeSeries   TimeSeriesClient
		Logger       chronograf.Logger
	}
	type args struct {
		w *httptest.ResponseRecorder
		r *http.Request
	}
	tests := []struct {
		name            string
		fields          fields
		args            args
		ID              string
		RoleID          string
		wantStatus      int
		wantContentType string
		wantBody        string
	}{
		{
			name: "Get role for data source",
			args: args{
				w: httptest.NewRecorder(),
				r: httptest.NewRequest(
					"GET",
					"http://server.local/chronograf/v1/sources/1/roles/biffsgang",
					nil),
			},
			fields: fields{
				Logger: log.New(log.DebugLevel),
				SourcesStore: &mocks.SourcesStore{
					GetF: func(ctx context.Context, ID int) (chronograf.Source, error) {
						return chronograf.Source{
							ID:       1,
							Name:     "muh source",
							Username: "name",
							Password: "hunter2",
							URL:      "http://localhost:8086",
						}, nil
					},
				},
				TimeSeries: &mocks.TimeSeries{
					ConnectF: func(ctx context.Context, src *chronograf.Source) error {
						return nil
					},
					RolesF: func(ctx context.Context) (chronograf.RolesStore, error) {
						return &mocks.RolesStore{
							GetF: func(ctx context.Context, name string) (*chronograf.Role, error) {
								return &chronograf.Role{
									Name: "biffsgang",
									Permissions: chronograf.Permissions{
										{
											Name:  "grays_sports_almanac",
											Scope: "DBScope",
											Allowed: chronograf.Allowances{
												"ReadData",
											},
										},
									},
									Users: []chronograf.User{
										{
											Name: "match",
										},
										{
											Name: "skinhead",
										},
										{
											Name: "3-d",
										},
									},
								}, nil
							},
						}, nil
					},
				},
			},
			ID:              "1",
			RoleID:          "biffsgang",
			wantStatus:      http.StatusOK,
			wantContentType: "application/json",
			wantBody: `{"users":[{"links":{"self":"/chronograf/v1/sources/1/users/match"},"name":"match"},{"links":{"self":"/chronograf/v1/sources/1/users/skinhead"},"name":"skinhead"},{"links":{"self":"/chronograf/v1/sources/1/users/3-d"},"name":"3-d"}],"name":"biffsgang","permissions":[{"scope":"DBScope","name":"grays_sports_almanac","allowed":["ReadData"]}],"links":{"self":"/chronograf/v1/sources/1/roles/biffsgang"}}
`,
		},
	}
	for _, tt := range tests {
		h := &Service{
			SourcesStore:     tt.fields.SourcesStore,
			TimeSeriesClient: tt.fields.TimeSeries,
			Logger:           tt.fields.Logger,
		}

		tt.args.r = tt.args.r.WithContext(httprouter.WithParams(
			context.Background(),
			httprouter.Params{
				{
					Key:   "id",
					Value: tt.ID,
				},
				{
					Key:   "rid",
					Value: tt.RoleID,
				},
			}))

		h.RoleID(tt.args.w, tt.args.r)

		resp := tt.args.w.Result()
		content := resp.Header.Get("Content-Type")
		body, _ := ioutil.ReadAll(resp.Body)

		if resp.StatusCode != tt.wantStatus {
			t.Errorf("%q. RoleID() = %v, want %v", tt.name, resp.StatusCode, tt.wantStatus)
		}
		if tt.wantContentType != "" && content != tt.wantContentType {
			t.Errorf("%q. RoleID() = %v, want %v", tt.name, content, tt.wantContentType)
		}
		if tt.wantBody != "" && string(body) != tt.wantBody {
			t.Errorf("%q. RoleID() = \n***%v***\n,\nwant\n***%v***", tt.name, string(body), tt.wantBody)
		}
	}
}

func TestService_RemoveRole(t *testing.T) {
	type fields struct {
		SourcesStore chronograf.SourcesStore
		TimeSeries   TimeSeriesClient
		Logger       chronograf.Logger
	}
	type args struct {
		w *httptest.ResponseRecorder
		r *http.Request
	}
	tests := []struct {
		name       string
		fields     fields
		args       args
		ID         string
		RoleID     string
		wantStatus int
	}{
		{
			name: "remove role for data source",
			args: args{
				w: httptest.NewRecorder(),
				r: httptest.NewRequest(
					"GET",
					"http://server.local/chronograf/v1/sources/1/roles/biffsgang",
					nil),
			},
			fields: fields{
				Logger: log.New(log.DebugLevel),
				SourcesStore: &mocks.SourcesStore{
					GetF: func(ctx context.Context, ID int) (chronograf.Source, error) {
						return chronograf.Source{
							ID:       1,
							Name:     "muh source",
							Username: "name",
							Password: "hunter2",
							URL:      "http://localhost:8086",
						}, nil
					},
				},
				TimeSeries: &mocks.TimeSeries{
					ConnectF: func(ctx context.Context, src *chronograf.Source) error {
						return nil
					},
					RolesF: func(ctx context.Context) (chronograf.RolesStore, error) {
						return &mocks.RolesStore{
							DeleteF: func(context.Context, *chronograf.Role) error {
								return nil
							},
						}, nil
					},
				},
			},
			ID:         "1",
			RoleID:     "biffsgang",
			wantStatus: http.StatusNoContent,
		},
	}
	for _, tt := range tests {
		h := &Service{
			SourcesStore:     tt.fields.SourcesStore,
			TimeSeriesClient: tt.fields.TimeSeries,
			Logger:           tt.fields.Logger,
		}

		tt.args.r = tt.args.r.WithContext(httprouter.WithParams(
			context.Background(),
			httprouter.Params{
				{
					Key:   "id",
					Value: tt.ID,
				},
				{
					Key:   "rid",
					Value: tt.RoleID,
				},
			}))

		h.RemoveRole(tt.args.w, tt.args.r)

		resp := tt.args.w.Result()
		if resp.StatusCode != tt.wantStatus {
			t.Errorf("%q. RemoveRole() = %v, want %v", tt.name, resp.StatusCode, tt.wantStatus)
		}
	}
}

func TestService_Roles(t *testing.T) {
	type fields struct {
		SourcesStore chronograf.SourcesStore
		TimeSeries   TimeSeriesClient
		Logger       chronograf.Logger
	}
	type args struct {
		w *httptest.ResponseRecorder
		r *http.Request
	}
	tests := []struct {
		name            string
		fields          fields
		args            args
		ID              string
		RoleID          string
		wantStatus      int
		wantContentType string
		wantBody        string
	}{
		{
			name: "Get roles for data source",
			args: args{
				w: httptest.NewRecorder(),
				r: httptest.NewRequest(
					"GET",
					"http://server.local/chronograf/v1/sources/1/roles",
					nil),
			},
			fields: fields{
				Logger: log.New(log.DebugLevel),
				SourcesStore: &mocks.SourcesStore{
					GetF: func(ctx context.Context, ID int) (chronograf.Source, error) {
						return chronograf.Source{
							ID: 1,
						}, nil
					},
				},
				TimeSeries: &mocks.TimeSeries{
					ConnectF: func(ctx context.Context, src *chronograf.Source) error {
						return nil
					},
					RolesF: func(ctx context.Context) (chronograf.RolesStore, error) {
						return &mocks.RolesStore{
							AllF: func(ctx context.Context) ([]chronograf.Role, error) {
								return []chronograf.Role{
									chronograf.Role{
										Name: "biffsgang",
										Permissions: chronograf.Permissions{
											{
												Name:  "grays_sports_almanac",
												Scope: "DBScope",
												Allowed: chronograf.Allowances{
													"ReadData",
												},
											},
										},
										Users: []chronograf.User{
											{
												Name: "match",
											},
											{
												Name: "skinhead",
											},
											{
												Name: "3-d",
											},
										},
									},
								}, nil
							},
						}, nil
					},
				},
			},
			ID:              "1",
			RoleID:          "biffsgang",
			wantStatus:      http.StatusOK,
			wantContentType: "application/json",
			wantBody: `{"roles":[{"users":[{"links":{"self":"/chronograf/v1/sources/1/users/match"},"name":"match"},{"links":{"self":"/chronograf/v1/sources/1/users/skinhead"},"name":"skinhead"},{"links":{"self":"/chronograf/v1/sources/1/users/3-d"},"name":"3-d"}],"name":"biffsgang","permissions":[{"scope":"DBScope","name":"grays_sports_almanac","allowed":["ReadData"]}],"links":{"self":"/chronograf/v1/sources/1/roles/biffsgang"}}]}
`,
		},
	}
	for _, tt := range tests {
		h := &Service{
			SourcesStore:     tt.fields.SourcesStore,
			TimeSeriesClient: tt.fields.TimeSeries,
			Logger:           tt.fields.Logger,
		}

		tt.args.r = tt.args.r.WithContext(httprouter.WithParams(
			context.Background(),
			httprouter.Params{
				{
					Key:   "id",
					Value: tt.ID,
				},
				{
					Key:   "rid",
					Value: tt.RoleID,
				},
			}))

		h.Roles(tt.args.w, tt.args.r)

		resp := tt.args.w.Result()
		content := resp.Header.Get("Content-Type")
		body, _ := ioutil.ReadAll(resp.Body)

		if resp.StatusCode != tt.wantStatus {
			t.Errorf("%q. RoleID() = %v, want %v", tt.name, resp.StatusCode, tt.wantStatus)
		}
		if tt.wantContentType != "" && content != tt.wantContentType {
			t.Errorf("%q. RoleID() = %v, want %v", tt.name, content, tt.wantContentType)
		}
		if tt.wantBody != "" && string(body) != tt.wantBody {
			t.Errorf("%q. RoleID() = \n***%v***\n,\nwant\n***%v***", tt.name, string(body), tt.wantBody)
		}
	}
}
