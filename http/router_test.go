package http

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	kithttp "github.com/influxdata/influxdb/v2/kit/transport/http"
	"go.uber.org/zap/zaptest"
)

func TestRouter_NotFound(t *testing.T) {
	type fields struct {
		method    string
		path      string
		handlerFn http.HandlerFunc
	}
	type args struct {
		method string
		path   string
	}
	type wants struct {
		statusCode  int
		contentType string
		body        string
	}

	tests := []struct {
		name   string
		fields fields
		args   args
		wants  wants
	}{
		{
			name: "path not found",
			fields: fields{
				method: "GET",
				path:   "/ping",
				handlerFn: func(w http.ResponseWriter, r *http.Request) {
					encodeResponse(r.Context(), w, http.StatusOK, map[string]string{"message": "pong"})
				},
			},
			args: args{
				method: "GET",
				path:   "/404",
			},
			wants: wants{
				statusCode:  http.StatusNotFound,
				contentType: "application/json; charset=utf-8",
				body: `
{
  "code": "not found",
  "message": "path not found"
}`,
			},
		},
		{
			name: "path found",
			fields: fields{
				method: "GET",
				path:   "/ping",
				handlerFn: func(w http.ResponseWriter, r *http.Request) {
					encodeResponse(r.Context(), w, http.StatusOK, map[string]string{"message": "pong"})
				},
			},
			args: args{
				method: "GET",
				path:   "/ping",
			},
			wants: wants{
				statusCode:  http.StatusOK,
				contentType: "application/json; charset=utf-8",
				body: `
{
  "message": "pong"
}
`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			router := NewRouter(kithttp.NewErrorHandler(zaptest.NewLogger(t)))
			router.HandlerFunc(tt.fields.method, tt.fields.path, tt.fields.handlerFn)

			r := httptest.NewRequest(tt.args.method, tt.args.path, nil)
			w := httptest.NewRecorder()
			router.ServeHTTP(w, r)

			res := w.Result()
			content := res.Header.Get("Content-Type")
			body, _ := io.ReadAll(res.Body)

			if res.StatusCode != tt.wants.statusCode {
				t.Errorf("%q. get %v, want %v", tt.name, res.StatusCode, tt.wants.statusCode)
			}
			if tt.wants.contentType != "" && content != tt.wants.contentType {
				t.Errorf("%q. get %v, want %v", tt.name, content, tt.wants.contentType)
			}
			if eq, diff, _ := jsonEqual(string(body), tt.wants.body); tt.wants.body != "" && !eq {
				t.Errorf("%q. get ***%s***", tt.name, diff)
			}
		})
	}
}

func TestRouter_Panic(t *testing.T) {
	type fields struct {
		method    string
		path      string
		handlerFn http.HandlerFunc
	}
	type args struct {
		method string
		path   string
	}
	type wants struct {
		statusCode  int
		contentType string
		body        string
		logged      bool
	}

	tests := []struct {
		name   string
		fields fields
		args   args
		wants  wants
	}{
		{
			name: "no panic",
			fields: fields{
				method: "GET",
				path:   "/ping",
				handlerFn: func(w http.ResponseWriter, r *http.Request) {
					encodeResponse(r.Context(), w, http.StatusOK, map[string]string{"message": "pong"})
				},
			},
			args: args{
				method: "GET",
				path:   "/ping",
			},
			wants: wants{
				statusCode:  http.StatusOK,
				contentType: "application/json; charset=utf-8",
				logged:      false,
				body: `
{
  "message": "pong"
}
`,
			},
		},
		{
			name: "panic",
			fields: fields{
				method: "GET",
				path:   "/ping",
				handlerFn: func(w http.ResponseWriter, r *http.Request) {
					panic("not implemented")
				},
			},
			args: args{
				method: "GET",
				path:   "/ping",
			},
			wants: wants{
				statusCode:  http.StatusInternalServerError,
				contentType: "application/json; charset=utf-8",
				logged:      true,
				body: `
{
  "code": "internal error",
  "message": "a panic has occurred: /ping: not implemented"
}`,
			},
		},
	}

	for _, tt := range tests[1:] {
		t.Run(tt.name, func(t *testing.T) {
			logger := getPanicLogger()
			defer func() {
				panicLogger = logger
			}()

			tw := newTestLogWriter(t)
			panicLogger = zaptest.NewLogger(tw)

			router := NewRouter(kithttp.NewErrorHandler(zaptest.NewLogger(t)))
			router.HandlerFunc(tt.fields.method, tt.fields.path, tt.fields.handlerFn)

			r := httptest.NewRequest(tt.args.method, tt.args.path, nil)
			w := httptest.NewRecorder()
			router.ServeHTTP(w, r)

			res := w.Result()
			content := res.Header.Get("Content-Type")
			body, _ := io.ReadAll(res.Body)

			if res.StatusCode != tt.wants.statusCode {
				t.Errorf("%q. get %v, want %v", tt.name, res.StatusCode, tt.wants.statusCode)
			}
			if tt.wants.contentType != "" && content != tt.wants.contentType {
				t.Errorf("%q. get %v, want %v", tt.name, content, tt.wants.contentType)
			}
			if eq, diff, _ := jsonEqual(tt.wants.body, string(body)); tt.wants.body != "" && !eq {
				t.Errorf("%q. get ***%s***", tt.name, diff)
			}
			if tt.wants.logged != tw.Logged() {
				t.Errorf("%q. get %v, want %v", tt.name, tt.wants.logged, tw.Logged())
			}
		})
	}
}

func TestRouter_MethodNotAllowed(t *testing.T) {
	type fields struct {
		method    string
		path      string
		handlerFn http.HandlerFunc
	}
	type args struct {
		method string
		path   string
	}
	type wants struct {
		statusCode  int
		contentType string
		body        string
	}

	tests := []struct {
		name   string
		fields fields
		args   args
		wants  wants
	}{
		{
			name: "method not allowed",
			fields: fields{
				method: "GET",
				path:   "/ping",
				handlerFn: func(w http.ResponseWriter, r *http.Request) {
					encodeResponse(r.Context(), w, http.StatusOK, map[string]string{"message": "pong"})
				},
			},
			args: args{
				method: "POST",
				path:   "/ping",
			},
			wants: wants{
				statusCode:  http.StatusMethodNotAllowed,
				contentType: "application/json; charset=utf-8",
				body: `
{
  "code": "method not allowed",
  "message": "allow: GET, OPTIONS"
}`,
			},
		},
		{
			name: "method allowed",
			fields: fields{
				method: "GET",
				path:   "/ping",
				handlerFn: func(w http.ResponseWriter, r *http.Request) {
					encodeResponse(r.Context(), w, http.StatusOK, map[string]string{"message": "pong"})
				},
			},
			args: args{
				method: "GET",
				path:   "/ping",
			},
			wants: wants{
				statusCode:  http.StatusOK,
				contentType: "application/json; charset=utf-8",
				body: `
{
  "message": "pong"
}
`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			router := NewRouter(kithttp.NewErrorHandler(zaptest.NewLogger(t)))
			router.HandlerFunc(tt.fields.method, tt.fields.path, tt.fields.handlerFn)

			r := httptest.NewRequest(tt.args.method, tt.args.path, nil)
			w := httptest.NewRecorder()
			router.ServeHTTP(w, r)

			res := w.Result()
			content := res.Header.Get("Content-Type")
			body, _ := io.ReadAll(res.Body)

			if res.StatusCode != tt.wants.statusCode {
				t.Errorf("%q. get %v, want %v", tt.name, res.StatusCode, tt.wants.statusCode)
			}
			if tt.wants.contentType != "" && content != tt.wants.contentType {
				t.Errorf("%q. get %v, want %v", tt.name, content, tt.wants.contentType)
			}
			if eq, diff, _ := jsonEqual(string(body), tt.wants.body); tt.wants.body != "" && !eq {
				t.Errorf("%q. get ***%s***", tt.name, diff)
			}

		})
	}
}

// testLogWriter is a zaptest.TestingT that captures logged messages.
type testLogWriter struct {
	*testing.T
	Messages []string
}

func newTestLogWriter(t *testing.T) *testLogWriter {
	return &testLogWriter{T: t}
}

func (t *testLogWriter) Logf(format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	t.Messages = append(t.Messages, msg)
	t.T.Log(msg)
}

func (t *testLogWriter) Logged() bool {
	return len(t.Messages) > 0
}
