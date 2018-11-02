package http

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"regexp"
	"testing"
	"time"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/csv"
	"github.com/influxdata/flux/lang"
	"github.com/influxdata/platform"
	"github.com/influxdata/platform/query"
)

func TestFluxService_Query(t *testing.T) {
	tests := []struct {
		name    string
		token   string
		ctx     context.Context
		r       *query.ProxyRequest
		status  int
		want    int64
		wantW   string
		wantErr bool
	}{
		{
			name:  "query",
			ctx:   context.Background(),
			token: "mytoken",
			r: &query.ProxyRequest{
				Request: query.Request{
					Compiler: lang.FluxCompiler{
						Query: "from()",
					},
				},
				Dialect: csv.DefaultDialect(),
			},
			status: http.StatusOK,
			want:   6,
			wantW:  "howdy\n",
		},
		{
			name:  "error status",
			token: "mytoken",
			ctx:   context.Background(),
			r: &query.ProxyRequest{
				Request: query.Request{
					Compiler: lang.FluxCompiler{
						Query: "from()",
					},
				},
				Dialect: csv.DefaultDialect(),
			},
			status:  http.StatusUnauthorized,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tt.status)
				fmt.Fprintln(w, "howdy")
			}))
			defer ts.Close()
			s := &FluxService{
				Addr:  ts.URL,
				Token: tt.token,
			}

			w := &bytes.Buffer{}
			got, err := s.Query(tt.ctx, w, tt.r)
			if (err != nil) != tt.wantErr {
				t.Errorf("FluxService.Query() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("FluxService.Query() = %v, want %v", got, tt.want)
			}
			if gotW := w.String(); gotW != tt.wantW {
				t.Errorf("FluxService.Query() = %v, want %v", gotW, tt.wantW)
			}
		})
	}
}

func TestFluxQueryService_Query(t *testing.T) {
	var orgID platform.ID
	orgID.DecodeFromString("aaaaaaaaaaaaaaaa")
	tests := []struct {
		name    string
		token   string
		ctx     context.Context
		r       *query.Request
		csv     string
		status  int
		want    string
		wantErr bool
	}{
		{
			name:  "error status",
			token: "mytoken",
			ctx:   context.Background(),
			r: &query.Request{
				OrganizationID: orgID,
				Compiler: lang.FluxCompiler{
					Query: "from()",
				},
			},
			status:  http.StatusUnauthorized,
			wantErr: true,
		},
		{
			name:  "returns csv",
			token: "mytoken",
			ctx:   context.Background(),
			r: &query.Request{
				OrganizationID: orgID,
				Compiler: lang.FluxCompiler{
					Query: "from()",
				},
			},
			status: http.StatusOK,
			csv: `#datatype,string,long,dateTime:RFC3339,double,long,string,boolean,string,string,string
#group,false,false,false,false,false,false,false,true,true,true
#default,0,,,,,,,,,
,result,table,_time,usage_user,test,mystr,this,cpu,host,_measurement
,,0,2018-08-29T13:08:47Z,10.2,10,yay,true,cpu-total,a,cpui
`,
			want: toCRLF(`,,,2018-08-29T13:08:47Z,10.2,10,yay,true,cpu-total,a,cpui

`),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var orgIDStr string
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				orgIDStr = r.URL.Query().Get(OrgID)
				w.WriteHeader(tt.status)
				fmt.Fprintln(w, tt.csv)
			}))
			s := &FluxQueryService{
				Addr:  ts.URL,
				Token: tt.token,
			}
			res, err := s.Query(tt.ctx, tt.r)
			if (err != nil) != tt.wantErr {
				t.Errorf("FluxQueryService.Query() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if res != nil && res.Err() != nil {
				t.Errorf("FluxQueryService.Query() result error = %v", res.Err())
				return
			}
			if tt.wantErr {
				return
			}
			defer res.Cancel()

			enc := csv.NewMultiResultEncoder(csv.ResultEncoderConfig{
				NoHeader:  true,
				Delimiter: ',',
			})
			b := bytes.Buffer{}
			n, err := enc.Encode(&b, res)
			if err != nil {
				t.Errorf("FluxQueryService.Query() encode error = %v", err)
				return
			}
			if n != int64(len(tt.want)) {
				t.Errorf("FluxQueryService.Query() encode result = %d, want %d", n, len(tt.want))
			}
			if orgIDStr == "" {
				t.Error("FluxQueryService.Query() encoded orgID is empty")
			}
			if got, want := orgIDStr, tt.r.OrganizationID.String(); got != want {
				t.Errorf("FluxQueryService.Query() encoded orgID = %s, want %s", got, want)
			}

			got := b.String()
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("FluxQueryService.Query() =\n%s\n%s", got, tt.want)
			}
		})
	}
}

func TestFluxHandler_postFluxAST(t *testing.T) {
	tests := []struct {
		name   string
		w      *httptest.ResponseRecorder
		r      *http.Request
		want   string
		status int
	}{
		{
			name: "get ast from()",
			w:    httptest.NewRecorder(),
			r:    httptest.NewRequest("POST", "/api/v2/query/ast", bytes.NewBufferString(`{"query": "from()"}`)),
			want: `{"ast":{"type":"Program","location":{"start":{"line":1,"column":1},"end":{"line":1,"column":7},"source":"from()"},"body":[{"type":"ExpressionStatement","location":{"start":{"line":1,"column":1},"end":{"line":1,"column":7},"source":"from()"},"expression":{"type":"CallExpression","location":{"start":{"line":1,"column":1},"end":{"line":1,"column":7},"source":"from()"},"callee":{"type":"Identifier","location":{"start":{"line":1,"column":1},"end":{"line":1,"column":5},"source":"from"},"name":"from"}}}]}}
`,
			status: http.StatusOK,
		},
		{
			name:   "error from bad json",
			w:      httptest.NewRecorder(),
			r:      httptest.NewRequest("POST", "/api/v2/query/ast", bytes.NewBufferString(`error!`)),
			status: http.StatusBadRequest,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := &FluxHandler{}
			h.postFluxAST(tt.w, tt.r)
			if got := tt.w.Body.String(); got != tt.want {
				t.Errorf("http.postFluxAST = got\n%vwant\n%v", got, tt.want)
			}
			if got := tt.w.Code; got != tt.status {
				t.Errorf("http.postFluxAST = got %d\nwant %d", got, tt.status)
			}
		})
	}
}

func TestFluxHandler_postFluxSpec(t *testing.T) {
	tests := []struct {
		name   string
		w      *httptest.ResponseRecorder
		r      *http.Request
		now    func() time.Time
		want   string
		status int
	}{
		{
			name: "get spec from()",
			w:    httptest.NewRecorder(),
			r:    httptest.NewRequest("POST", "/api/v2/query/spec", bytes.NewBufferString(`{"query": "from(bucket: \"telegraf\")"}`)),
			now:  func() time.Time { return time.Unix(0, 0).UTC() },
			want: `{"spec":{"operations":[{"kind":"from","id":"from0","spec":{"bucket":"telegraf"}}],"edges":null,"resources":{"priority":"high","concurrency_quota":0,"memory_bytes_quota":0},"now":"1970-01-01T00:00:00Z"}}
`,
			status: http.StatusOK,
		},
		{
			name:   "error from bad json",
			w:      httptest.NewRecorder(),
			r:      httptest.NewRequest("POST", "/api/v2/query/spec", bytes.NewBufferString(`error!`)),
			status: http.StatusBadRequest,
		},
		{
			name:   "error from incomplete spec",
			w:      httptest.NewRecorder(),
			r:      httptest.NewRequest("POST", "/api/v2/query/spec", bytes.NewBufferString(`{"query": "from()"}`)),
			now:    func() time.Time { return time.Unix(0, 0).UTC() },
			status: http.StatusUnprocessableEntity,
		},
		{
			name: "get spec with range and last",
			w:    httptest.NewRecorder(),
			r:    httptest.NewRequest("POST", "/api/v2/query/spec", bytes.NewBufferString(`{"query": "from(bucket:\"demo-bucket-in-1\") |> range(start:-2s) |> last()"}`)),
			now:  func() time.Time { return time.Unix(0, 0).UTC() },
			want: `{"spec":{"operations":[{"kind":"from","id":"from0","spec":{"bucket":"demo-bucket-in-1"}},{"kind":"range","id":"range1","spec":{"start":"-2s","stop":"now","timeCol":"_time","startCol":"_start","stopCol":"_stop"}},{"kind":"last","id":"last2","spec":{"column":""}}],"edges":[{"parent":"from0","child":"range1"},{"parent":"range1","child":"last2"}],"resources":{"priority":"high","concurrency_quota":0,"memory_bytes_quota":0},"now":"1970-01-01T00:00:00Z"}}
`,
			status: http.StatusOK,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := &FluxHandler{
				Now: tt.now,
			}
			h.postFluxSpec(tt.w, tt.r)
			if got := tt.w.Body.String(); got != tt.want {
				t.Errorf("http.postFluxSpec = got %s\nwant %s", got, tt.want)
			}

			if got := tt.w.Code; got != tt.status {
				t.Errorf("http.postFluxSpec = got %d\nwant %d", got, tt.status)
				t.Log(tt.w.HeaderMap)
			}
		})
	}
}

func TestFluxHandler_postFluxPlan(t *testing.T) {
	tests := []struct {
		name   string
		w      *httptest.ResponseRecorder
		r      *http.Request
		now    func() time.Time
		want   string
		status int
	}{
		{
			name: "get plan from()",
			w:    httptest.NewRecorder(),
			r:    httptest.NewRequest("POST", "/api/v2/query/plan", bytes.NewBufferString(`{"query": "from(bucket:\"telegraf\")|> range(start: -5000h)|> filter(fn: (r) => r._measurement == \"mem\" AND r._field == \"used_percent\")|> group(by:[\"host\"])|> mean()"}`)),
			now:  func() time.Time { return time.Unix(0, 0).UTC() },
			want: `{"spec":{"operations":[{"kind":"from","id":"from0","spec":{"bucket":"telegraf"}},{"kind":"range","id":"range1","spec":{"start":"-5000h0m0s","stop":"now","timeCol":"_time","startCol":"_start","stopCol":"_stop"}},{"kind":"filter","id":"filter2","spec":{"fn":{"type":"FunctionExpression","block":{"type":"FunctionBlock","parameters":{"type":"FunctionParameters","list":[{"type":"FunctionParameter","key":{"type":"Identifier","name":"r"}}],"pipe":null},"body":{"type":"LogicalExpression","operator":"and","left":{"type":"BinaryExpression","operator":"==","left":{"type":"MemberExpression","object":{"type":"IdentifierExpression","name":"r"},"property":"_measurement"},"right":{"type":"StringLiteral","value":"mem"}},"right":{"type":"BinaryExpression","operator":"==","left":{"type":"MemberExpression","object":{"type":"IdentifierExpression","name":"r"},"property":"_field"},"right":{"type":"StringLiteral","value":"used_percent"}}}}}}},{"kind":"group","id":"group3","spec":{"by":["host"],"except":null,"all":false,"none":false}},{"kind":"mean","id":"mean4","spec":{"columns":["_value"]}}],"edges":[{"parent":"from0","child":"range1"},{"parent":"range1","child":"filter2"},{"parent":"filter2","child":"group3"},{"parent":"group3","child":"mean4"}],"resources":{"priority":"high","concurrency_quota":0,"memory_bytes_quota":0},"now":"1970-01-01T00:00:00Z"},"logical":{"roots":[{"Spec":{"Name":"_result"}}],"resources":{"priority":"high","concurrency_quota":1,"memory_bytes_quota":9223372036854775807},"now":"1970-01-01T00:00:00Z"},"physical":{"roots":[{"Spec":{"Name":"_result"}}],"resources":{"priority":"high","concurrency_quota":1,"memory_bytes_quota":9223372036854775807},"now":"1970-01-01T00:00:00Z"}}
`,
			status: http.StatusOK,
		},
		{
			name:   "error from bad json",
			w:      httptest.NewRecorder(),
			r:      httptest.NewRequest("POST", "/api/v2/query/plan", bytes.NewBufferString(`error!`)),
			status: http.StatusBadRequest,
		},
		{
			name:   "error from incomplete plan",
			w:      httptest.NewRecorder(),
			r:      httptest.NewRequest("POST", "/api/v2/query/plan", bytes.NewBufferString(`{"query": "from()"}`)),
			now:    func() time.Time { return time.Unix(0, 0).UTC() },
			status: http.StatusUnprocessableEntity,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := &FluxHandler{
				Now: tt.now,
			}
			h.postFluxPlan(tt.w, tt.r)
			if got := tt.w.Body.String(); got != tt.want {
				t.Errorf("http.postFluxPlan = got %s\nwant %s", got, tt.want)
			}

			if got := tt.w.Code; got != tt.status {
				t.Errorf("http.postFluxPlan = got %d\nwant %d", got, tt.status)
				t.Log(tt.w.HeaderMap)
			}
		})
	}
}

var crlfPattern = regexp.MustCompile(`\r?\n`)

func toCRLF(data string) string {
	return crlfPattern.ReplaceAllString(data, "\r\n")
}

func Test_postPlanRequest_Valid(t *testing.T) {
	type fields struct {
		Query string
		Spec  *flux.Spec
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		{
			name:    "no query nor spec is an error",
			fields:  fields{},
			wantErr: true,
		},
		{
			name: "both query and spec is an error",
			fields: fields{
				Query: "from()|>last()",
				Spec:  &flux.Spec{},
			},
			wantErr: true,
		},
		{

			name: "request with query is valid",
			fields: fields{
				Query: `from(bucket:"telegraf")|> range(start: -5000h)|> filter(fn: (r) => r._measurement == "mem" AND r._field == "used_percent")|> group(by:["host"])|> mean()`,
			},
		},
		{

			name: "request with spec is valid",
			fields: fields{
				Spec: &flux.Spec{},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &postPlanRequest{
				Query: tt.fields.Query,
				Spec:  tt.fields.Spec,
			}
			if err := p.Valid(); (err != nil) != tt.wantErr {
				t.Errorf("postPlanRequest.Valid() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
