package server

import (
	"bytes"
	"context"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/influxdata/platform/chronograf"
	"github.com/influxdata/platform/chronograf/log"
	"github.com/influxdata/platform/chronograf/mocks"
	"github.com/julienschmidt/httprouter"
)

func TestService_Permissions(t *testing.T) {
	type fields struct {
		SourcesStore chronograf.SourcesStore
		TimeSeries   TimeSeriesClient
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
		tt.args.r = tt.args.r.WithContext(context.WithValue(
			context.TODO(),
			httprouter.ParamsKey,
			httprouter.Params{
				{
					Key:   "id",
					Value: tt.ID,
				},
			}))
		h := &Service{
			Store: &mocks.Store{
				SourcesStore: tt.fields.SourcesStore,
			},
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
