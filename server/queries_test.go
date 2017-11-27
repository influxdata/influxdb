package server

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/bouk/httprouter"
	"github.com/influxdata/chronograf"
	"github.com/influxdata/chronograf/mocks"
)

func TestService_Queries(t *testing.T) {
	tests := []struct {
		name         string
		SourcesStore chronograf.SourcesStore
		ID           string
		w            *httptest.ResponseRecorder
		r            *http.Request
		want         string
	}{
		{
			name: "bad json",
			SourcesStore: &mocks.SourcesStore{
				GetF: func(ctx context.Context, ID int) (chronograf.Source, error) {
					return chronograf.Source{
						ID: ID,
					}, nil
				},
			},
			ID:   "1",
			w:    httptest.NewRecorder(),
			r:    httptest.NewRequest("POST", "/queries", bytes.NewReader([]byte(`howdy`))),
			want: `{"code":400,"message":"Unparsable JSON"}`,
		},
		{
			name: "bad id",
			ID:   "howdy",
			w:    httptest.NewRecorder(),
			r:    httptest.NewRequest("POST", "/queries", bytes.NewReader([]byte{})),
			want: `{"code":422,"message":"Error converting ID howdy"}`,
		},
		{
			name: "query with no template vars",
			SourcesStore: &mocks.SourcesStore{
				GetF: func(ctx context.Context, ID int) (chronograf.Source, error) {
					return chronograf.Source{
						ID: ID,
					}, nil
				},
			},
			ID: "1",
			w:  httptest.NewRecorder(),
			r: httptest.NewRequest("POST", "/queries", bytes.NewReader([]byte(`{
					"queries": [
					  {
						"query": "SELECT \"pingReq\" FROM db.\"monitor\".\"httpd\" WHERE time > now() - 1m",
						"id": "82b60d37-251e-4afe-ac93-ca20a3642b11"
					  }
					]}`))),
			want: `{"queries":[{"id":"82b60d37-251e-4afe-ac93-ca20a3642b11","query":"SELECT \"pingReq\" FROM db.\"monitor\".\"httpd\" WHERE time \u003e now() - 1m","queryConfig":{"id":"82b60d37-251e-4afe-ac93-ca20a3642b11","database":"db","measurement":"httpd","retentionPolicy":"monitor","fields":[{"value":"pingReq","type":"field","alias":""}],"tags":{},"groupBy":{"time":"","tags":[]},"areTagsAccepted":false,"rawText":null,"range":{"upper":"","lower":"now() - 1m"},"shifts":[]},"queryAST":{"condition":{"expr":"binary","op":"\u003e","lhs":{"expr":"reference","val":"time"},"rhs":{"expr":"binary","op":"-","lhs":{"expr":"call","name":"now"},"rhs":{"expr":"literal","val":"1m","type":"duration"}}},"fields":[{"column":{"expr":"reference","val":"pingReq"}}],"sources":[{"database":"db","retentionPolicy":"monitor","name":"httpd","type":"measurement"}]}}]}
`,
		},
		{
			name: "query with unparsable query",
			SourcesStore: &mocks.SourcesStore{
				GetF: func(ctx context.Context, ID int) (chronograf.Source, error) {
					return chronograf.Source{
						ID: ID,
					}, nil
				},
			},
			ID: "1",
			w:  httptest.NewRecorder(),
			r: httptest.NewRequest("POST", "/queries", bytes.NewReader([]byte(`{
					"queries": [
					  {
						"query": "SHOW DATABASES",
						"id": "82b60d37-251e-4afe-ac93-ca20a3642b11"
					  }
					]}`))),
			want: `{"queries":[{"id":"82b60d37-251e-4afe-ac93-ca20a3642b11","query":"SHOW DATABASES","queryConfig":{"id":"82b60d37-251e-4afe-ac93-ca20a3642b11","database":"","measurement":"","retentionPolicy":"","fields":[],"tags":{},"groupBy":{"time":"","tags":[]},"areTagsAccepted":false,"rawText":"SHOW DATABASES","range":null,"shifts":[]}}]}
`,
		},
		{
			name: "query with template vars",
			SourcesStore: &mocks.SourcesStore{
				GetF: func(ctx context.Context, ID int) (chronograf.Source, error) {
					return chronograf.Source{
						ID: ID,
					}, nil
				},
			},
			ID: "1",
			w:  httptest.NewRecorder(),
			r: httptest.NewRequest("POST", "/queries", bytes.NewReader([]byte(`{
					"queries": [
					  {
						"query": "SELECT \"pingReq\" FROM :dbs:.\"monitor\".\"httpd\" WHERE time > :dashboardTime: AND time < :upperDashboardTime: GROUP BY :interval:",
						"id": "82b60d37-251e-4afe-ac93-ca20a3642b11"
					  }
					],
					"tempVars": [
					  {
						"tempVar": ":dbs:",
						"values": [
						  {
							"value": "_internal",
							"type": "database",
							"selected": true
						  }
						],
						"id": "792eda0d-2bb2-4de6-a86f-1f652889b044",
						"type": "databases",
						"label": "",
						"query": {
						  "influxql": "SHOW DATABASES",
						  "measurement": "",
						  "tagKey": "",
						  "fieldKey": ""
						},
						"links": {
						  "self": "/chronograf/v1/dashboards/1/templates/792eda0d-2bb2-4de6-a86f-1f652889b044"
						}
					  },
					  {
						"id": "dashtime",
						"tempVar": ":dashboardTime:",
						"type": "constant",
						"values": [
						  {
							"value": "now() - 15m",
							"type": "constant",
							"selected": true
						  }
						]
					  },
					  {
						"id": "upperdashtime",
						"tempVar": ":upperDashboardTime:",
						"type": "constant",
						"values": [
						  {
							"value": "now()",
							"type": "constant",
							"selected": true
						  }
						]
					  },
					  {
						"id": "interval",
						"type": "constant",
						"tempVar": ":interval:",
						"values": [
							{
									"value": "1000",
									"type":  "resolution"
							},
							{
									"value": "3",
									"type":  "pointsPerPixel"
							}
					]
					  }
					]
				  }`))),
			want: `{"queries":[{"id":"82b60d37-251e-4afe-ac93-ca20a3642b11","query":"SELECT \"pingReq\" FROM :dbs:.\"monitor\".\"httpd\" WHERE time \u003e :dashboardTime: AND time \u003c :upperDashboardTime: GROUP BY :interval:","queryConfig":{"id":"82b60d37-251e-4afe-ac93-ca20a3642b11","database":"","measurement":"","retentionPolicy":"","fields":[],"tags":{},"groupBy":{"time":"","tags":[]},"areTagsAccepted":false,"rawText":"SELECT \"pingReq\" FROM :dbs:.\"monitor\".\"httpd\" WHERE time \u003e :dashboardTime: AND time \u003c :upperDashboardTime: GROUP BY :interval:","range":null,"shifts":[]},"queryTemplated":"SELECT \"pingReq\" FROM \"_internal\".\"monitor\".\"httpd\" WHERE time \u003e now() - 15m AND time \u003c now() GROUP BY time(2s)","tempVars":[{"tempVar":":upperDashboardTime:","values":[{"value":"now()","type":"constant","selected":true}]},{"tempVar":":dashboardTime:","values":[{"value":"now() - 15m","type":"constant","selected":true}]},{"tempVar":":dbs:","values":[{"value":"_internal","type":"database","selected":true}]},{"tempVar":":interval:","values":[{"value":"1000","type":"resolution","selected":false},{"value":"3","type":"pointsPerPixel","selected":false}]}]}]}
`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.r = tt.r.WithContext(httprouter.WithParams(
				context.Background(),
				httprouter.Params{
					{
						Key:   "id",
						Value: tt.ID,
					},
				}))
			s := &Service{
				SourcesStore: tt.SourcesStore,
				Logger:       &mocks.TestLogger{},
			}
			s.Queries(tt.w, tt.r)
			got := tt.w.Body.String()
			if got != tt.want {
				t.Errorf("got:\n%s\nwant:\n%s\n", got, tt.want)
			}
		})
	}
}
