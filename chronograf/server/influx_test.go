package server

import (
	"bytes"
	"context"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/julienschmidt/httprouter"

	"github.com/influxdata/platform/chronograf/mocks"

	"github.com/influxdata/platform/chronograf"
)

func TestService_Influx(t *testing.T) {
	type fields struct {
		SourcesStore chronograf.SourcesStore
		TimeSeries   TimeSeriesClient
	}
	type args struct {
		w *httptest.ResponseRecorder
		r *http.Request
	}
	type want struct {
		StatusCode  int
		ContentType string
		Body        string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		ID     string
		want   want
	}{
		{
			name: "Proxies request to Influxdb",
			fields: fields{
				SourcesStore: &mocks.SourcesStore{
					GetF: func(ctx context.Context, ID int) (chronograf.Source, error) {
						return chronograf.Source{
							ID:  1337,
							URL: "http://any.url",
						}, nil
					},
				},
				TimeSeries: &mocks.TimeSeries{
					ConnectF: func(ctx context.Context, src *chronograf.Source) error {
						return nil
					},
					QueryF: func(ctx context.Context, query chronograf.Query) (chronograf.Response, error) {
						return mocks.NewResponse(
								`{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["key","value"],"values":[["cpu","cpu-total"],["cpu","cpu0"],["cpu","cpu1"],["cpu","cpu2"],["cpu","cpu3"],["host","pineapples-MBP"],["host","pineapples-MacBook-Pro.local"]]}]}]}`,
								nil,
							),
							nil
					},
				},
			},
			args: args{
				w: httptest.NewRecorder(),
				r: httptest.NewRequest(
					"POST",
					"http://any.url",
					ioutil.NopCloser(
						bytes.NewReader([]byte(
							`{"db":"bob", "rp":"joe", "query":"SELECT mean(\"usage_user\") FROM cpu WHERE \"cpu\" = 'cpu-total' AND time > now() - 10m GROUP BY host;"}`,
						)),
					),
				),
			},
			ID: "1",
			want: want{
				StatusCode:  http.StatusOK,
				ContentType: "application/json",
				Body: `{"results":{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["key","value"],"values":[["cpu","cpu-total"],["cpu","cpu0"],["cpu","cpu1"],["cpu","cpu2"],["cpu","cpu3"],["host","pineapples-MBP"],["host","pineapples-MacBook-Pro.local"]]}]}]}}
`,
			},
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
		}
		h.Influx(tt.args.w, tt.args.r)

		resp := tt.args.w.Result()
		contentType := resp.Header.Get("Content-Type")
		body, _ := ioutil.ReadAll(resp.Body)

		if resp.StatusCode != tt.want.StatusCode {
			t.Errorf("%q. Influx() = got %v, want %v", tt.name, resp.StatusCode, tt.want.StatusCode)
		}
		if contentType != tt.want.ContentType {
			t.Errorf("%q. Influx() = got %v, want %v", tt.name, contentType, tt.want.ContentType)
		}
		if string(body) != tt.want.Body {
			t.Errorf("%q. Influx() =\ngot  ***%v***\nwant ***%v***\n", tt.name, string(body), tt.want.Body)
		}

	}
}
