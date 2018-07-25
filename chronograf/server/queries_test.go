package server

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/influxdata/platform/chronograf"
	"github.com/influxdata/platform/chronograf/mocks"
	"github.com/julienschmidt/httprouter"
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
			want: `{"queries":[{"durationMs":59999,"id":"82b60d37-251e-4afe-ac93-ca20a3642b11","query":"SELECT \"pingReq\" FROM db.\"monitor\".\"httpd\" WHERE time \u003e now() - 1m","queryConfig":{"id":"82b60d37-251e-4afe-ac93-ca20a3642b11","database":"db","measurement":"httpd","retentionPolicy":"monitor","fields":[{"value":"pingReq","type":"field","alias":""}],"tags":{},"groupBy":{"time":"","tags":[]},"areTagsAccepted":false,"rawText":null,"range":{"upper":"","lower":"now() - 1m"},"shifts":[]},"queryAST":{"condition":{"expr":"binary","op":"\u003e","lhs":{"expr":"reference","val":"time"},"rhs":{"expr":"binary","op":"-","lhs":{"expr":"call","name":"now"},"rhs":{"expr":"literal","val":"1m","type":"duration"}}},"fields":[{"column":{"expr":"reference","val":"pingReq"}}],"sources":[{"database":"db","retentionPolicy":"monitor","name":"httpd","type":"measurement"}]}}]}
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
			want: `{"queries":[{"durationMs":0,"id":"82b60d37-251e-4afe-ac93-ca20a3642b11","query":"SHOW DATABASES","queryConfig":{"id":"82b60d37-251e-4afe-ac93-ca20a3642b11","database":"","measurement":"","retentionPolicy":"","fields":[],"tags":{},"groupBy":{"time":"","tags":[]},"areTagsAccepted":false,"rawText":"SHOW DATABASES","range":null,"shifts":[]}}]}
`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.r = tt.r.WithContext(context.WithValue(
				context.TODO(),
				httprouter.ParamsKey,
				httprouter.Params{
					{
						Key:   "id",
						Value: tt.ID,
					},
				}))
			s := &Service{
				Store: &mocks.Store{
					SourcesStore: tt.SourcesStore,
				},
				Logger: &mocks.TestLogger{},
			}
			s.Queries(tt.w, tt.r)
			got := tt.w.Body.String()
			if got != tt.want {
				t.Errorf("got:\n%s\nwant:\n%s\n", got, tt.want)
			}
		})
	}
}
