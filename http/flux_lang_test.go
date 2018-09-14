package http

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestFluxLangHandler_getFlux(t *testing.T) {
	tests := []struct {
		name string
		w    *httptest.ResponseRecorder
		r    *http.Request
		want string
	}{
		{
			name: "get links",
			w:    httptest.NewRecorder(),
			r:    httptest.NewRequest("GET", "/v2/flux", nil),
			want: `{"links":{"self":"/v2/flux","suggestions":"/v2/flux/suggestions","ast":"/v2/flux/ast"}}
`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := &FluxLangHandler{}
			h.getFlux(tt.w, tt.r)
			if got := tt.w.Body.String(); got != tt.want {
				t.Errorf("http.getFlux = got %s\nwant %s", got, tt.want)
			}
		})
	}
}

func TestFluxLangHandler_postFluxAST(t *testing.T) {
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
			r:    httptest.NewRequest("GET", "/v2/flux/ast", bytes.NewBufferString(`{"query": "from()"}`)),
			want: `{"ast":{"type":"Program","location":{"start":{"line":1,"column":1},"end":{"line":1,"column":7},"source":"from()"},"body":[{"type":"ExpressionStatement","location":{"start":{"line":1,"column":1},"end":{"line":1,"column":7},"source":"from()"},"expression":{"type":"CallExpression","location":{"start":{"line":1,"column":1},"end":{"line":1,"column":7},"source":"from()"},"callee":{"type":"Identifier","location":{"start":{"line":1,"column":1},"end":{"line":1,"column":5},"source":"from"},"name":"from"}}}]}}
`,
			status: http.StatusOK,
		},
		{
			name:   "error from bad json",
			w:      httptest.NewRecorder(),
			r:      httptest.NewRequest("GET", "/v2/flux/ast", bytes.NewBufferString(`error!`)),
			status: http.StatusBadRequest,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := &FluxLangHandler{}
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

func TestFluxLangHandler_postFluxSpec(t *testing.T) {
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
			r:    httptest.NewRequest("GET", "/v2/flux/spec", bytes.NewBufferString(`{"query": "from(bucket: \"telegraf\")"}`)),
			now:  func() time.Time { return time.Unix(0, 0).UTC() },
			want: `{"spec":{"operations":[{"kind":"from","id":"from0","spec":{"bucket":"telegraf"}}],"edges":null,"resources":{"priority":"high","concurrency_quota":0,"memory_bytes_quota":0},"now":"1970-01-01T00:00:00Z"}}
`,
			status: http.StatusOK,
		},
		{
			name:   "error from bad json",
			w:      httptest.NewRecorder(),
			r:      httptest.NewRequest("GET", "/v2/flux/spec", bytes.NewBufferString(`error!`)),
			status: http.StatusBadRequest,
		},
		{
			name:   "error from incomplete spec",
			w:      httptest.NewRecorder(),
			r:      httptest.NewRequest("GET", "/v2/flux/spec", bytes.NewBufferString(`{"query": "from()"}`)),
			now:    func() time.Time { return time.Unix(0, 0).UTC() },
			status: http.StatusUnprocessableEntity,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := &FluxLangHandler{
				Now: tt.now,
			}
			h.postFluxSpec(tt.w, tt.r)
			if got := tt.w.Body.String(); got != tt.want {
				t.Errorf("http.postFluxSpec = got %s\nwant %s", got, tt.want)
			}

			if got := tt.w.Code; got != tt.status {
				t.Errorf("http.postFluxSpec = got %d\nwant %d", got, tt.status)
			}
		})
	}
}
