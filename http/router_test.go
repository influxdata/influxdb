package http

import (
	"bytes"
	"io/ioutil"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
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
			router := NewRouter()
			router.HandlerFunc(tt.fields.method, tt.fields.path, tt.fields.handlerFn)

			r := httptest.NewRequest(tt.args.method, tt.args.path, nil)
			w := httptest.NewRecorder()
			router.ServeHTTP(w, r)

			res := w.Result()
			content := res.Header.Get("Content-Type")
			body, _ := ioutil.ReadAll(res.Body)

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
  "message": "a panic has occurred",
  "error": "not implemented"
}`,
			},
		},
	}

	for _, tt := range tests[1:] {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			log.SetOutput(&buf)
			defer func() {
				log.SetOutput(os.Stderr)
			}()

			router := NewRouter()
			router.HandlerFunc(tt.fields.method, tt.fields.path, tt.fields.handlerFn)

			r := httptest.NewRequest(tt.args.method, tt.args.path, nil)
			w := httptest.NewRecorder()
			router.ServeHTTP(w, r)

			res := w.Result()
			content := res.Header.Get("Content-Type")
			body, _ := ioutil.ReadAll(res.Body)

			if res.StatusCode != tt.wants.statusCode {
				t.Errorf("%q. get %v, want %v", tt.name, res.StatusCode, tt.wants.statusCode)
			}
			if tt.wants.contentType != "" && content != tt.wants.contentType {
				t.Errorf("%q. get %v, want %v", tt.name, content, tt.wants.contentType)
			}
			if got, want := buf.String() != "", tt.wants.logged; got != want {
				t.Errorf("%q. get %v, want %v", tt.name, got, want)
			}
			if eq, diff, _ := jsonEqual(string(body), tt.wants.body); tt.wants.body != "" && !eq {
				t.Errorf("%q. get ***%s***", tt.name, diff)
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
			router := NewRouter()
			router.HandlerFunc(tt.fields.method, tt.fields.path, tt.fields.handlerFn)

			r := httptest.NewRequest(tt.args.method, tt.args.path, nil)
			w := httptest.NewRecorder()
			router.ServeHTTP(w, r)

			res := w.Result()
			content := res.Header.Get("Content-Type")
			body, _ := ioutil.ReadAll(res.Body)

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
