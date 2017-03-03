package server_test

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
	"github.com/influxdata/chronograf/server"
)

func TestService_NewSourceUser(t *testing.T) {
	type fields struct {
		SourcesStore chronograf.SourcesStore
		TimeSeries   server.TimeSeriesClient
		Logger       chronograf.Logger
		UseAuth      bool
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
			name: "New user for data source",
			args: args{
				w: httptest.NewRecorder(),
				r: httptest.NewRequest(
					"POST",
					"http://server.local/chronograf/v1/sources/1",
					ioutil.NopCloser(
						bytes.NewReader([]byte(`{"name": "marty", "password": "the_lake"}`)))),
			},
			fields: fields{
				UseAuth: true,
				Logger:  log.New(log.DebugLevel),
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
					UsersF: func(ctx context.Context) chronograf.UsersStore {
						return &mocks.UsersStore{
							AddF: func(ctx context.Context, u *chronograf.User) (*chronograf.User, error) {
								return u, nil
							},
						}
					},
				},
			},
			ID:              "1",
			wantStatus:      http.StatusCreated,
			wantContentType: "application/json",
			wantBody: `{"name":"marty","links":{"self":"/chronograf/v1/sources/1/users/marty"}}
`,
		},
		{
			name: "Error adding user",
			args: args{
				w: httptest.NewRecorder(),
				r: httptest.NewRequest(
					"POST",
					"http://server.local/chronograf/v1/sources/1",
					ioutil.NopCloser(
						bytes.NewReader([]byte(`{"name": "marty", "password": "the_lake"}`)))),
			},
			fields: fields{
				UseAuth: true,
				Logger:  log.New(log.DebugLevel),
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
					UsersF: func(ctx context.Context) chronograf.UsersStore {
						return &mocks.UsersStore{
							AddF: func(ctx context.Context, u *chronograf.User) (*chronograf.User, error) {
								return nil, fmt.Errorf("Weight Has Nothing to Do With It")
							},
						}
					},
				},
			},
			ID:              "1",
			wantStatus:      http.StatusBadRequest,
			wantContentType: "application/json",
			wantBody:        `{"code":400,"message":"Weight Has Nothing to Do With It"}`,
		},
		{
			name: "Failure connecting to user store",
			args: args{
				w: httptest.NewRecorder(),
				r: httptest.NewRequest(
					"POST",
					"http://server.local/chronograf/v1/sources/1",
					ioutil.NopCloser(
						bytes.NewReader([]byte(`{"name": "marty", "password": "the_lake"}`)))),
			},
			fields: fields{
				UseAuth: true,
				Logger:  log.New(log.DebugLevel),
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
						return fmt.Errorf("Biff just happens to be my supervisor")
					},
				},
			},
			ID:              "1",
			wantStatus:      http.StatusBadRequest,
			wantContentType: "application/json",
			wantBody:        `{"code":400,"message":"Unable to connect to source 1: Biff just happens to be my supervisor"}`,
		},
		{
			name: "Failure getting source",
			args: args{
				w: httptest.NewRecorder(),
				r: httptest.NewRequest(
					"POST",
					"http://server.local/chronograf/v1/sources/1",
					ioutil.NopCloser(
						bytes.NewReader([]byte(`{"name": "marty", "password": "the_lake"}`)))),
			},
			fields: fields{
				UseAuth: true,
				Logger:  log.New(log.DebugLevel),
				SourcesStore: &mocks.SourcesStore{
					GetF: func(ctx context.Context, ID int) (chronograf.Source, error) {
						return chronograf.Source{}, fmt.Errorf("No McFly ever amounted to anything in the history of Hill Valley")
					},
				},
			},
			ID:              "1",
			wantStatus:      http.StatusNotFound,
			wantContentType: "application/json",
			wantBody:        `{"code":404,"message":"ID 1 not found"}`,
		},
		{
			name: "Bad ID",
			args: args{
				w: httptest.NewRecorder(),
				r: httptest.NewRequest(
					"POST",
					"http://server.local/chronograf/v1/sources/1",
					ioutil.NopCloser(
						bytes.NewReader([]byte(`{"name": "marty", "password": "the_lake"}`)))),
			},
			fields: fields{
				UseAuth: true,
				Logger:  log.New(log.DebugLevel),
			},
			ID:              "BAD",
			wantStatus:      http.StatusUnprocessableEntity,
			wantContentType: "application/json",
			wantBody:        `{"code":422,"message":"Error converting ID BAD"}`,
		},
		{
			name: "Bad name",
			args: args{
				w: httptest.NewRecorder(),
				r: httptest.NewRequest(
					"POST",
					"http://server.local/chronograf/v1/sources/1",
					ioutil.NopCloser(
						bytes.NewReader([]byte(`{"password": "the_lake"}`)))),
			},
			fields: fields{
				UseAuth: true,
				Logger:  log.New(log.DebugLevel),
			},
			ID:              "BAD",
			wantStatus:      http.StatusUnprocessableEntity,
			wantContentType: "application/json",
			wantBody:        `{"code":422,"message":"Username required"}`,
		},
		{
			name: "Bad JSON",
			args: args{
				w: httptest.NewRecorder(),
				r: httptest.NewRequest(
					"POST",
					"http://server.local/chronograf/v1/sources/1",
					ioutil.NopCloser(
						bytes.NewReader([]byte(`{password}`)))),
			},
			fields: fields{
				UseAuth: true,
				Logger:  log.New(log.DebugLevel),
			},
			ID:              "BAD",
			wantStatus:      http.StatusBadRequest,
			wantContentType: "application/json",
			wantBody:        `{"code":400,"message":"Unparsable JSON"}`,
		},
	}
	for _, tt := range tests {
		tt.args.r = tt.args.r.WithContext(httprouter.WithParams(
			context.Background(),
			httprouter.Params{
				{
					Key:   "id",
					Value: tt.ID,
				},
			}))

		h := &server.Service{
			SourcesStore:     tt.fields.SourcesStore,
			TimeSeriesClient: tt.fields.TimeSeries,
			Logger:           tt.fields.Logger,
			UseAuth:          tt.fields.UseAuth,
		}

		h.NewSourceUser(tt.args.w, tt.args.r)

		resp := tt.args.w.Result()
		content := resp.Header.Get("Content-Type")
		body, _ := ioutil.ReadAll(resp.Body)

		if resp.StatusCode != tt.wantStatus {
			t.Errorf("%q. NewSourceUser() = %v, want %v", tt.name, resp.StatusCode, tt.wantStatus)
		}
		if tt.wantContentType != "" && content != tt.wantContentType {
			t.Errorf("%q. NewSourceUser() = %v, want %v", tt.name, content, tt.wantContentType)
		}
		if tt.wantBody != "" && string(body) != tt.wantBody {
			t.Errorf("%q. NewSourceUser() = \n***%v***\n,\nwant\n***%v***", tt.name, string(body), tt.wantBody)
		}
	}
}

func TestService_SourceUsers(t *testing.T) {
	type fields struct {
		SourcesStore chronograf.SourcesStore
		TimeSeries   server.TimeSeriesClient
		Logger       chronograf.Logger
		UseAuth      bool
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
			name: "All users for data source",
			args: args{
				w: httptest.NewRecorder(),
				r: httptest.NewRequest(
					"GET",
					"http://server.local/chronograf/v1/sources/1",
					nil),
			},
			fields: fields{
				UseAuth: true,
				Logger:  log.New(log.DebugLevel),
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
						return nil, fmt.Errorf("No role store for this test")
					},
					UsersF: func(ctx context.Context) chronograf.UsersStore {
						return &mocks.UsersStore{
							AllF: func(ctx context.Context) ([]chronograf.User, error) {
								return []chronograf.User{
									{
										Name:   "strickland",
										Passwd: "discipline",
										Permissions: chronograf.Permissions{
											{
												Scope:   chronograf.AllScope,
												Allowed: chronograf.Allowances{"READ"},
											},
										},
									},
								}, nil
							},
						}
					},
				},
			},
			ID:              "1",
			wantStatus:      http.StatusOK,
			wantContentType: "application/json",
			wantBody: `{"users":[{"name":"strickland","permissions":[{"scope":"all","allowed":["READ"]}],"links":{"self":"/chronograf/v1/sources/1/users/strickland"}}]}
`,
		},
	}
	for _, tt := range tests {
		tt.args.r = tt.args.r.WithContext(httprouter.WithParams(
			context.Background(),
			httprouter.Params{
				{
					Key:   "id",
					Value: tt.ID,
				},
			}))
		h := &server.Service{
			SourcesStore:     tt.fields.SourcesStore,
			TimeSeriesClient: tt.fields.TimeSeries,
			Logger:           tt.fields.Logger,
			UseAuth:          tt.fields.UseAuth,
		}

		h.SourceUsers(tt.args.w, tt.args.r)
		resp := tt.args.w.Result()
		content := resp.Header.Get("Content-Type")
		body, _ := ioutil.ReadAll(resp.Body)

		if resp.StatusCode != tt.wantStatus {
			t.Errorf("%q. SourceUsers() = %v, want %v", tt.name, resp.StatusCode, tt.wantStatus)
		}
		if tt.wantContentType != "" && content != tt.wantContentType {
			t.Errorf("%q. SourceUsers() = %v, want %v", tt.name, content, tt.wantContentType)
		}
		if tt.wantBody != "" && string(body) != tt.wantBody {
			t.Errorf("%q. SourceUsers() = \n***%v***\n,\nwant\n***%v***", tt.name, string(body), tt.wantBody)
		}
	}
}

func TestService_SourceUserID(t *testing.T) {
	type fields struct {
		SourcesStore chronograf.SourcesStore
		TimeSeries   server.TimeSeriesClient
		Logger       chronograf.Logger
		UseAuth      bool
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
		UID             string
		wantStatus      int
		wantContentType string
		wantBody        string
	}{
		{
			name: "Single user for data source",
			args: args{
				w: httptest.NewRecorder(),
				r: httptest.NewRequest(
					"GET",
					"http://server.local/chronograf/v1/sources/1",
					nil),
			},
			fields: fields{
				UseAuth: true,
				Logger:  log.New(log.DebugLevel),
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
						return nil, fmt.Errorf("No role store for this test")
					},
					UsersF: func(ctx context.Context) chronograf.UsersStore {
						return &mocks.UsersStore{
							GetF: func(ctx context.Context, uid string) (*chronograf.User, error) {
								return &chronograf.User{
									Name:   "strickland",
									Passwd: "discipline",
									Permissions: chronograf.Permissions{
										{
											Scope:   chronograf.AllScope,
											Allowed: chronograf.Allowances{"READ"},
										},
									},
								}, nil
							},
						}
					},
				},
			},
			ID:              "1",
			UID:             "strickland",
			wantStatus:      http.StatusOK,
			wantContentType: "application/json",
			wantBody: `{"name":"strickland","permissions":[{"scope":"all","allowed":["READ"]}],"links":{"self":"/chronograf/v1/sources/1/users/strickland"}}
`,
		},
	}
	for _, tt := range tests {
		tt.args.r = tt.args.r.WithContext(httprouter.WithParams(
			context.Background(),
			httprouter.Params{
				{
					Key:   "id",
					Value: tt.ID,
				},
			}))
		h := &server.Service{
			SourcesStore:     tt.fields.SourcesStore,
			TimeSeriesClient: tt.fields.TimeSeries,
			Logger:           tt.fields.Logger,
			UseAuth:          tt.fields.UseAuth,
		}

		h.SourceUserID(tt.args.w, tt.args.r)
		resp := tt.args.w.Result()
		content := resp.Header.Get("Content-Type")
		body, _ := ioutil.ReadAll(resp.Body)

		if resp.StatusCode != tt.wantStatus {
			t.Errorf("%q. SourceUserID() = %v, want %v", tt.name, resp.StatusCode, tt.wantStatus)
		}
		if tt.wantContentType != "" && content != tt.wantContentType {
			t.Errorf("%q. SourceUserID() = %v, want %v", tt.name, content, tt.wantContentType)
		}
		if tt.wantBody != "" && string(body) != tt.wantBody {
			t.Errorf("%q. SourceUserID() = \n***%v***\n,\nwant\n***%v***", tt.name, string(body), tt.wantBody)
		}
	}
}

func TestService_RemoveSourceUser(t *testing.T) {
	type fields struct {
		SourcesStore chronograf.SourcesStore
		TimeSeries   server.TimeSeriesClient
		Logger       chronograf.Logger
		UseAuth      bool
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
		UID             string
		wantStatus      int
		wantContentType string
		wantBody        string
	}{
		{
			name: "Delete user for data source",
			args: args{
				w: httptest.NewRecorder(),
				r: httptest.NewRequest(
					"GET",
					"http://server.local/chronograf/v1/sources/1",
					nil),
			},
			fields: fields{
				UseAuth: true,
				Logger:  log.New(log.DebugLevel),
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
					UsersF: func(ctx context.Context) chronograf.UsersStore {
						return &mocks.UsersStore{
							DeleteF: func(ctx context.Context, u *chronograf.User) error {
								return nil
							},
						}
					},
				},
			},
			ID:         "1",
			UID:        "strickland",
			wantStatus: http.StatusNoContent,
		},
	}
	for _, tt := range tests {
		tt.args.r = tt.args.r.WithContext(httprouter.WithParams(
			context.Background(),
			httprouter.Params{
				{
					Key:   "id",
					Value: tt.ID,
				},
			}))
		h := &server.Service{
			SourcesStore:     tt.fields.SourcesStore,
			TimeSeriesClient: tt.fields.TimeSeries,
			Logger:           tt.fields.Logger,
			UseAuth:          tt.fields.UseAuth,
		}
		h.RemoveSourceUser(tt.args.w, tt.args.r)
		resp := tt.args.w.Result()
		content := resp.Header.Get("Content-Type")
		body, _ := ioutil.ReadAll(resp.Body)

		if resp.StatusCode != tt.wantStatus {
			t.Errorf("%q. RemoveSourceUser() = %v, want %v", tt.name, resp.StatusCode, tt.wantStatus)
		}
		if tt.wantContentType != "" && content != tt.wantContentType {
			t.Errorf("%q. RemoveSourceUser() = %v, want %v", tt.name, content, tt.wantContentType)
		}
		if tt.wantBody != "" && string(body) != tt.wantBody {
			t.Errorf("%q. RemoveSourceUser() = \n***%v***\n,\nwant\n***%v***", tt.name, string(body), tt.wantBody)
		}
	}
}

func TestService_UpdateSourceUser(t *testing.T) {
	type fields struct {
		SourcesStore chronograf.SourcesStore
		TimeSeries   server.TimeSeriesClient
		Logger       chronograf.Logger
		UseAuth      bool
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
		UID             string
		wantStatus      int
		wantContentType string
		wantBody        string
	}{
		{
			name: "Update user password for data source",
			args: args{
				w: httptest.NewRecorder(),
				r: httptest.NewRequest(
					"POST",
					"http://server.local/chronograf/v1/sources/1",
					ioutil.NopCloser(
						bytes.NewReader([]byte(`{"name": "marty", "password": "the_lake"}`)))),
			},
			fields: fields{
				UseAuth: true,
				Logger:  log.New(log.DebugLevel),
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
					UsersF: func(ctx context.Context) chronograf.UsersStore {
						return &mocks.UsersStore{
							UpdateF: func(ctx context.Context, u *chronograf.User) error {
								return nil
							},
						}
					},
				},
			},
			ID:              "1",
			UID:             "marty",
			wantStatus:      http.StatusOK,
			wantContentType: "application/json",
			wantBody: `{"name":"marty","links":{"self":"/chronograf/v1/sources/1/users/marty"}}
`,
		},
		{
			name: "Invalid update JSON",
			args: args{
				w: httptest.NewRecorder(),
				r: httptest.NewRequest(
					"POST",
					"http://server.local/chronograf/v1/sources/1",
					ioutil.NopCloser(
						bytes.NewReader([]byte(`{"name": "marty"}`)))),
			},
			fields: fields{
				UseAuth: true,
				Logger:  log.New(log.DebugLevel),
			},
			ID:              "1",
			UID:             "marty",
			wantStatus:      http.StatusUnprocessableEntity,
			wantContentType: "application/json",
			wantBody:        `{"code":422,"message":"No fields to update"}`,
		},
	}
	for _, tt := range tests {
		tt.args.r = tt.args.r.WithContext(httprouter.WithParams(
			context.Background(),
			httprouter.Params{
				{
					Key:   "id",
					Value: tt.ID,
				},
				{
					Key:   "uid",
					Value: tt.UID,
				},
			}))
		h := &server.Service{
			SourcesStore:     tt.fields.SourcesStore,
			TimeSeriesClient: tt.fields.TimeSeries,
			Logger:           tt.fields.Logger,
			UseAuth:          tt.fields.UseAuth,
		}
		h.UpdateSourceUser(tt.args.w, tt.args.r)
		resp := tt.args.w.Result()
		content := resp.Header.Get("Content-Type")
		body, _ := ioutil.ReadAll(resp.Body)

		if resp.StatusCode != tt.wantStatus {
			t.Errorf("%q. UpdateSourceUser() = %v, want %v", tt.name, resp.StatusCode, tt.wantStatus)
		}
		if tt.wantContentType != "" && content != tt.wantContentType {
			t.Errorf("%q. UpdateSourceUser() = %v, want %v", tt.name, content, tt.wantContentType)
		}
		if tt.wantBody != "" && string(body) != tt.wantBody {
			t.Errorf("%q. UpdateSourceUser() = \n***%v***\n,\nwant\n***%v***", tt.name, string(body), tt.wantBody)
		}
	}
}

func TestService_Permissions(t *testing.T) {
	type fields struct {
		SourcesStore chronograf.SourcesStore
		TimeSeries   server.TimeSeriesClient
		Logger       chronograf.Logger
		UseAuth      bool
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
			name: "New user for data source",
			args: args{
				w: httptest.NewRecorder(),
				r: httptest.NewRequest(
					"POST",
					"http://server.local/chronograf/v1/sources/1",
					ioutil.NopCloser(
						bytes.NewReader([]byte(`{"name": "marty", "password": "the_lake"}`)))),
			},
			fields: fields{
				UseAuth: true,
				Logger:  log.New(log.DebugLevel),
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
					PermissionsF: func(ctx context.Context) chronograf.Permissions {
						return chronograf.Permissions{
							{
								Scope:   chronograf.AllScope,
								Allowed: chronograf.Allowances{"READ", "WRITE"},
							},
						}
					},
				},
			},
			ID:              "1",
			wantStatus:      http.StatusOK,
			wantContentType: "application/json",
			wantBody: `{"permissions":[{"scope":"all","allowed":["READ","WRITE"]}],"links":{"self":"/chronograf/v1/sources/1/permissions","source":"/chronograf/v1/sources/1"}}
`,
		},
	}
	for _, tt := range tests {
		tt.args.r = tt.args.r.WithContext(httprouter.WithParams(
			context.Background(),
			httprouter.Params{
				{
					Key:   "id",
					Value: tt.ID,
				},
			}))
		h := &server.Service{
			SourcesStore:     tt.fields.SourcesStore,
			TimeSeriesClient: tt.fields.TimeSeries,
			Logger:           tt.fields.Logger,
			UseAuth:          tt.fields.UseAuth,
		}
		h.Permissions(tt.args.w, tt.args.r)
		resp := tt.args.w.Result()
		content := resp.Header.Get("Content-Type")
		body, _ := ioutil.ReadAll(resp.Body)

		if resp.StatusCode != tt.wantStatus {
			t.Errorf("%q. Permissions() = %v, want %v", tt.name, resp.StatusCode, tt.wantStatus)
		}
		if tt.wantContentType != "" && content != tt.wantContentType {
			t.Errorf("%q. Permissions() = %v, want %v", tt.name, content, tt.wantContentType)
		}
		if tt.wantBody != "" && string(body) != tt.wantBody {
			t.Errorf("%q. Permissions() = \n***%v***\n,\nwant\n***%v***", tt.name, string(body), tt.wantBody)
		}
	}
}

func TestService_NewSourceRole(t *testing.T) {
	type fields struct {
		SourcesStore chronograf.SourcesStore
		TimeSeries   server.TimeSeriesClient
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
						}, nil
					},
				},
			},
			ID:              "1",
			wantStatus:      http.StatusCreated,
			wantContentType: "application/json",
			wantBody: `{"users":[{"name":"match","links":{"self":"/chronograf/v1/sources/1/users/match"}},{"name":"skinhead","links":{"self":"/chronograf/v1/sources/1/users/skinhead"}},{"name":"3-d","links":{"self":"/chronograf/v1/sources/1/users/3-d"}}],"name":"biffsgang","permissions":[],"links":{"self":"/chronograf/v1/sources/1/roles/biffsgang"}}
`,
		},
	}
	for _, tt := range tests {
		h := &server.Service{
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
		TimeSeries   server.TimeSeriesClient
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
			wantBody: `{"users":[{"name":"match","links":{"self":"/chronograf/v1/sources/1/users/match"}},{"name":"skinhead","links":{"self":"/chronograf/v1/sources/1/users/skinhead"}},{"name":"3-d","links":{"self":"/chronograf/v1/sources/1/users/3-d"}}],"name":"biffsgang","permissions":[],"links":{"self":"/chronograf/v1/sources/1/roles/biffsgang"}}
`,
		},
	}
	for _, tt := range tests {
		h := &server.Service{
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
		TimeSeries   server.TimeSeriesClient
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
			wantBody: `{"users":[{"name":"match","links":{"self":"/chronograf/v1/sources/1/users/match"}},{"name":"skinhead","links":{"self":"/chronograf/v1/sources/1/users/skinhead"}},{"name":"3-d","links":{"self":"/chronograf/v1/sources/1/users/3-d"}}],"name":"biffsgang","permissions":[{"scope":"DBScope","name":"grays_sports_almanac","allowed":["ReadData"]}],"links":{"self":"/chronograf/v1/sources/1/roles/biffsgang"}}
`,
		},
	}
	for _, tt := range tests {
		h := &server.Service{
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
		TimeSeries   server.TimeSeriesClient
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
		h := &server.Service{
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
		TimeSeries   server.TimeSeriesClient
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
			wantBody: `{"roles":[{"users":[{"name":"match","links":{"self":"/chronograf/v1/sources/1/users/match"}},{"name":"skinhead","links":{"self":"/chronograf/v1/sources/1/users/skinhead"}},{"name":"3-d","links":{"self":"/chronograf/v1/sources/1/users/3-d"}}],"name":"biffsgang","permissions":[{"scope":"DBScope","name":"grays_sports_almanac","allowed":["ReadData"]}],"links":{"self":"/chronograf/v1/sources/1/roles/biffsgang"}}]}
`,
		},
	}
	for _, tt := range tests {
		h := &server.Service{
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
