package server

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/influxdata/platform/chronograf"
	"github.com/influxdata/platform/chronograf/log"
	"github.com/influxdata/platform/chronograf/mocks"
	"github.com/julienschmidt/httprouter"
)

func TestService_GetDatabases(t *testing.T) {
	type fields struct {
		SourcesStore     chronograf.SourcesStore
		ServersStore     chronograf.ServersStore
		LayoutsStore     chronograf.LayoutsStore
		UsersStore       chronograf.UsersStore
		DashboardsStore  chronograf.DashboardsStore
		TimeSeriesClient TimeSeriesClient
		Logger           chronograf.Logger
		UseAuth          bool
		Databases        chronograf.Databases
	}
	type args struct {
		w http.ResponseWriter
		r *http.Request
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := &Service{
				Store: &Store{
					SourcesStore:    tt.fields.SourcesStore,
					ServersStore:    tt.fields.ServersStore,
					LayoutsStore:    tt.fields.LayoutsStore,
					UsersStore:      tt.fields.UsersStore,
					DashboardsStore: tt.fields.DashboardsStore,
				},
				TimeSeriesClient: tt.fields.TimeSeriesClient,
				Logger:           tt.fields.Logger,
				UseAuth:          tt.fields.UseAuth,
				Databases:        tt.fields.Databases,
			}
			h.GetDatabases(tt.args.w, tt.args.r)
		})
	}
}

func TestService_NewDatabase(t *testing.T) {
	type fields struct {
		SourcesStore     chronograf.SourcesStore
		ServersStore     chronograf.ServersStore
		LayoutsStore     chronograf.LayoutsStore
		UsersStore       chronograf.UsersStore
		DashboardsStore  chronograf.DashboardsStore
		TimeSeriesClient TimeSeriesClient
		Logger           chronograf.Logger
		UseAuth          bool
		Databases        chronograf.Databases
	}
	type args struct {
		w http.ResponseWriter
		r *http.Request
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := &Service{
				Store: &Store{
					SourcesStore:    tt.fields.SourcesStore,
					ServersStore:    tt.fields.ServersStore,
					LayoutsStore:    tt.fields.LayoutsStore,
					UsersStore:      tt.fields.UsersStore,
					DashboardsStore: tt.fields.DashboardsStore,
				},
				TimeSeriesClient: tt.fields.TimeSeriesClient,
				Logger:           tt.fields.Logger,
				UseAuth:          tt.fields.UseAuth,
				Databases:        tt.fields.Databases,
			}
			h.NewDatabase(tt.args.w, tt.args.r)
		})
	}
}

func TestService_DropDatabase(t *testing.T) {
	type fields struct {
		SourcesStore     chronograf.SourcesStore
		ServersStore     chronograf.ServersStore
		LayoutsStore     chronograf.LayoutsStore
		UsersStore       chronograf.UsersStore
		DashboardsStore  chronograf.DashboardsStore
		TimeSeriesClient TimeSeriesClient
		Logger           chronograf.Logger
		UseAuth          bool
		Databases        chronograf.Databases
	}
	type args struct {
		w http.ResponseWriter
		r *http.Request
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := &Service{
				Store: &Store{
					SourcesStore:    tt.fields.SourcesStore,
					ServersStore:    tt.fields.ServersStore,
					LayoutsStore:    tt.fields.LayoutsStore,
					UsersStore:      tt.fields.UsersStore,
					DashboardsStore: tt.fields.DashboardsStore,
				},
				TimeSeriesClient: tt.fields.TimeSeriesClient,
				Logger:           tt.fields.Logger,
				UseAuth:          tt.fields.UseAuth,
				Databases:        tt.fields.Databases,
			}
			h.DropDatabase(tt.args.w, tt.args.r)
		})
	}
}

func TestService_RetentionPolicies(t *testing.T) {
	type fields struct {
		SourcesStore     chronograf.SourcesStore
		ServersStore     chronograf.ServersStore
		LayoutsStore     chronograf.LayoutsStore
		UsersStore       chronograf.UsersStore
		DashboardsStore  chronograf.DashboardsStore
		TimeSeriesClient TimeSeriesClient
		Logger           chronograf.Logger
		UseAuth          bool
		Databases        chronograf.Databases
	}
	type args struct {
		w http.ResponseWriter
		r *http.Request
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := &Service{
				Store: &Store{
					SourcesStore:    tt.fields.SourcesStore,
					ServersStore:    tt.fields.ServersStore,
					LayoutsStore:    tt.fields.LayoutsStore,
					UsersStore:      tt.fields.UsersStore,
					DashboardsStore: tt.fields.DashboardsStore,
				},
				TimeSeriesClient: tt.fields.TimeSeriesClient,
				Logger:           tt.fields.Logger,
				UseAuth:          tt.fields.UseAuth,
				Databases:        tt.fields.Databases,
			}
			h.RetentionPolicies(tt.args.w, tt.args.r)
		})
	}
}

func TestService_NewRetentionPolicy(t *testing.T) {
	type fields struct {
		SourcesStore     chronograf.SourcesStore
		ServersStore     chronograf.ServersStore
		LayoutsStore     chronograf.LayoutsStore
		UsersStore       chronograf.UsersStore
		DashboardsStore  chronograf.DashboardsStore
		TimeSeriesClient TimeSeriesClient
		Logger           chronograf.Logger
		UseAuth          bool
		Databases        chronograf.Databases
	}
	type args struct {
		w http.ResponseWriter
		r *http.Request
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := &Service{
				Store: &Store{
					SourcesStore:    tt.fields.SourcesStore,
					ServersStore:    tt.fields.ServersStore,
					LayoutsStore:    tt.fields.LayoutsStore,
					UsersStore:      tt.fields.UsersStore,
					DashboardsStore: tt.fields.DashboardsStore,
				},
				TimeSeriesClient: tt.fields.TimeSeriesClient,
				Logger:           tt.fields.Logger,
				UseAuth:          tt.fields.UseAuth,
				Databases:        tt.fields.Databases,
			}
			h.NewRetentionPolicy(tt.args.w, tt.args.r)
		})
	}
}

func TestService_UpdateRetentionPolicy(t *testing.T) {
	type fields struct {
		SourcesStore     chronograf.SourcesStore
		ServersStore     chronograf.ServersStore
		LayoutsStore     chronograf.LayoutsStore
		UsersStore       chronograf.UsersStore
		DashboardsStore  chronograf.DashboardsStore
		TimeSeriesClient TimeSeriesClient
		Logger           chronograf.Logger
		UseAuth          bool
		Databases        chronograf.Databases
	}
	type args struct {
		w http.ResponseWriter
		r *http.Request
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := &Service{
				Store: &Store{
					SourcesStore:    tt.fields.SourcesStore,
					ServersStore:    tt.fields.ServersStore,
					LayoutsStore:    tt.fields.LayoutsStore,
					UsersStore:      tt.fields.UsersStore,
					DashboardsStore: tt.fields.DashboardsStore,
				},
				TimeSeriesClient: tt.fields.TimeSeriesClient,
				Logger:           tt.fields.Logger,
				UseAuth:          tt.fields.UseAuth,
				Databases:        tt.fields.Databases,
			}
			h.UpdateRetentionPolicy(tt.args.w, tt.args.r)
		})
	}
}

func TestService_DropRetentionPolicy(t *testing.T) {
	type fields struct {
		SourcesStore     chronograf.SourcesStore
		ServersStore     chronograf.ServersStore
		LayoutsStore     chronograf.LayoutsStore
		UsersStore       chronograf.UsersStore
		DashboardsStore  chronograf.DashboardsStore
		TimeSeriesClient TimeSeriesClient
		Logger           chronograf.Logger
		UseAuth          bool
		Databases        chronograf.Databases
	}
	type args struct {
		w http.ResponseWriter
		r *http.Request
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := &Service{
				Store: &Store{
					SourcesStore:    tt.fields.SourcesStore,
					ServersStore:    tt.fields.ServersStore,
					LayoutsStore:    tt.fields.LayoutsStore,
					UsersStore:      tt.fields.UsersStore,
					DashboardsStore: tt.fields.DashboardsStore,
				},
				TimeSeriesClient: tt.fields.TimeSeriesClient,
				Logger:           tt.fields.Logger,
				UseAuth:          tt.fields.UseAuth,
				Databases:        tt.fields.Databases,
			}
			h.DropRetentionPolicy(tt.args.w, tt.args.r)
		})
	}
}

func TestService_Measurements(t *testing.T) {
	type fields struct {
		SourcesStore chronograf.SourcesStore
		Logger       chronograf.Logger
		Databases    chronograf.Databases
	}
	type args struct {
		queryParams map[string]string
	}
	type wants struct {
		statusCode int
		body       string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		wants  wants
	}{
		{
			name: "Gets 100 measurements when no limit or offset provided",
			fields: fields{
				SourcesStore: &mocks.SourcesStore{
					GetF: func(ctx context.Context, srcID int) (chronograf.Source, error) {
						return chronograf.Source{
							ID: 0,
						}, nil
					},
				},
				Databases: &mocks.Databases{
					ConnectF: func(context.Context, *chronograf.Source) error {
						return nil
					},
					GetMeasurementsF: func(ctx context.Context, db string, limit, offset int) ([]chronograf.Measurement, error) {
						return []chronograf.Measurement{
							{
								Name: "pineapple",
							},
							{
								Name: "cubeapple",
							},
							{
								Name: "pinecube",
							},
						}, nil
					},
				},
			},
			args: args{
				queryParams: map[string]string{},
			},
			wants: wants{
				statusCode: 200,
				body: `{"measurements":[{"name":"pineapple"},{"name":"cubeapple"},{"name":"pinecube"}],"links":{"self":"/chronograf/v1/sources/0/dbs/pineapples/measurements?limit=100\u0026offset=0","first":"/chronograf/v1/sources/0/dbs/pineapples/measurements?limit=100\u0026offset=0","next":"/chronograf/v1/sources/0/dbs/pineapples/measurements?limit=100\u0026offset=100"}}
`,
			},
		},
		{
			name: "Fails when invalid limit value provided",
			fields: fields{
				SourcesStore: &mocks.SourcesStore{
					GetF: func(ctx context.Context, srcID int) (chronograf.Source, error) {
						return chronograf.Source{
							ID: 0,
						}, nil
					},
				},
			},
			args: args{
				queryParams: map[string]string{
					"limit": "joe",
				},
			},
			wants: wants{
				statusCode: 422,
				body:       `{"code":422,"message":"strconv.Atoi: parsing \"joe\": invalid syntax"}`,
			},
		},
		{
			name: "Fails when invalid offset value provided",
			fields: fields{
				SourcesStore: &mocks.SourcesStore{
					GetF: func(ctx context.Context, srcID int) (chronograf.Source, error) {
						return chronograf.Source{
							ID: 0,
						}, nil
					},
				},
			},
			args: args{
				queryParams: map[string]string{
					"offset": "bob",
				},
			},
			wants: wants{
				statusCode: 422,
				body:       `{"code":422,"message":"strconv.Atoi: parsing \"bob\": invalid syntax"}`,
			},
		},
		{
			name: "Overrides limit less than or equal to 0 with limit 100",
			fields: fields{
				SourcesStore: &mocks.SourcesStore{
					GetF: func(ctx context.Context, srcID int) (chronograf.Source, error) {
						return chronograf.Source{
							ID: 0,
						}, nil
					},
				},
				Databases: &mocks.Databases{
					ConnectF: func(context.Context, *chronograf.Source) error {
						return nil
					},
					GetMeasurementsF: func(ctx context.Context, db string, limit, offset int) ([]chronograf.Measurement, error) {
						return []chronograf.Measurement{
							{
								Name: "pineapple",
							},
							{
								Name: "cubeapple",
							},
							{
								Name: "pinecube",
							},
						}, nil
					},
				},
			},
			args: args{
				queryParams: map[string]string{
					"limit": "0",
				},
			},
			wants: wants{
				statusCode: 200,
				body: `{"measurements":[{"name":"pineapple"},{"name":"cubeapple"},{"name":"pinecube"}],"links":{"self":"/chronograf/v1/sources/0/dbs/pineapples/measurements?limit=100\u0026offset=0","first":"/chronograf/v1/sources/0/dbs/pineapples/measurements?limit=100\u0026offset=0","next":"/chronograf/v1/sources/0/dbs/pineapples/measurements?limit=100\u0026offset=100"}}
`,
			},
		},
		{
			name: "Overrides offset less than 0 with offset 0",
			fields: fields{
				SourcesStore: &mocks.SourcesStore{
					GetF: func(ctx context.Context, srcID int) (chronograf.Source, error) {
						return chronograf.Source{
							ID: 0,
						}, nil
					},
				},
				Databases: &mocks.Databases{
					ConnectF: func(context.Context, *chronograf.Source) error {
						return nil
					},
					GetMeasurementsF: func(ctx context.Context, db string, limit, offset int) ([]chronograf.Measurement, error) {
						return []chronograf.Measurement{
							{
								Name: "pineapple",
							},
							{
								Name: "cubeapple",
							},
							{
								Name: "pinecube",
							},
						}, nil
					},
				},
			},
			args: args{
				queryParams: map[string]string{
					"offset": "-1337",
				},
			},
			wants: wants{
				statusCode: 200,
				body: `{"measurements":[{"name":"pineapple"},{"name":"cubeapple"},{"name":"pinecube"}],"links":{"self":"/chronograf/v1/sources/0/dbs/pineapples/measurements?limit=100\u0026offset=0","first":"/chronograf/v1/sources/0/dbs/pineapples/measurements?limit=100\u0026offset=0","next":"/chronograf/v1/sources/0/dbs/pineapples/measurements?limit=100\u0026offset=100"}}
`,
			},
		},
		{
			name: "Provides a prev link when offset exceeds limit",
			fields: fields{
				SourcesStore: &mocks.SourcesStore{
					GetF: func(ctx context.Context, srcID int) (chronograf.Source, error) {
						return chronograf.Source{
							ID: 0,
						}, nil
					},
				},
				Databases: &mocks.Databases{
					ConnectF: func(context.Context, *chronograf.Source) error {
						return nil
					},
					GetMeasurementsF: func(ctx context.Context, db string, limit, offset int) ([]chronograf.Measurement, error) {
						return []chronograf.Measurement{
							{
								Name: "pineapple",
							},
							{
								Name: "cubeapple",
							},
							{
								Name: "pinecube",
							},
							{
								Name: "billietta",
							},
							{
								Name: "bobbetta",
							},
							{
								Name: "bobcube",
							},
						}, nil
					},
				},
			},
			args: args{
				queryParams: map[string]string{
					"limit":  "2",
					"offset": "4",
				},
			},
			wants: wants{
				statusCode: 200,
				body: `{"measurements":[{"name":"pineapple"},{"name":"cubeapple"},{"name":"pinecube"},{"name":"billietta"},{"name":"bobbetta"},{"name":"bobcube"}],"links":{"self":"/chronograf/v1/sources/0/dbs/pineapples/measurements?limit=2\u0026offset=4","first":"/chronograf/v1/sources/0/dbs/pineapples/measurements?limit=2\u0026offset=0","next":"/chronograf/v1/sources/0/dbs/pineapples/measurements?limit=2\u0026offset=6","prev":"/chronograf/v1/sources/0/dbs/pineapples/measurements?limit=2\u0026offset=2"}}
`,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger := log.New(log.DebugLevel)
			h := &Service{
				Store: &mocks.Store{
					SourcesStore: tt.fields.SourcesStore,
				},
				Logger:    logger,
				Databases: tt.fields.Databases,
			}

			w := httptest.NewRecorder()
			r := httptest.NewRequest(
				"GET",
				"http://any.url",
				nil,
			)
			r = r.WithContext(context.WithValue(
				context.TODO(),
				httprouter.ParamsKey,
				httprouter.Params{
					{
						Key:   "id",
						Value: "0",
					},
					{
						Key:   "db",
						Value: "pineapples",
					},
				}))

			q := r.URL.Query()
			for key, value := range tt.args.queryParams {
				q.Add(key, value)
			}
			r.URL.RawQuery = q.Encode()

			h.Measurements(w, r)

			resp := w.Result()
			body, err := ioutil.ReadAll(resp.Body)
			defer resp.Body.Close()

			if err != nil {
				t.Error("TestService_Measurements not able to retrieve body")
			}

			var msmts measurementsResponse
			if err := json.Unmarshal(body, &msmts); err != nil {
				t.Error("TestService_Measurements not able to unmarshal JSON response")
			}

			if tt.wants.statusCode != resp.StatusCode {
				t.Errorf("%q. StatusCode:\nwant\n%v\ngot\n%v", tt.name, tt.wants.statusCode, resp.StatusCode)
			}

			if tt.wants.body != string(body) {
				t.Errorf("%q. Body:\nwant\n*%s*\ngot\n*%s*", tt.name, tt.wants.body, string(body))
			}
		})
	}
}

func TestValidDatabaseRequest(t *testing.T) {
	type args struct {
		d *chronograf.Database
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := ValidDatabaseRequest(tt.args.d); (err != nil) != tt.wantErr {
				t.Errorf("ValidDatabaseRequest() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestValidRetentionPolicyRequest(t *testing.T) {
	type args struct {
		rp *chronograf.RetentionPolicy
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := ValidRetentionPolicyRequest(tt.args.rp); (err != nil) != tt.wantErr {
				t.Errorf("ValidRetentionPolicyRequest() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
