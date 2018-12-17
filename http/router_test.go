package http

import (
	"io/ioutil"
	"net/http"
	"net/http/httptest"
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
			name:   "path not found",
			fields: fields{},
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
  "msg": "path not found"
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

	for _, tt := range tests[1:] {
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
			if eq, _ := jsonEqual(string(body), tt.wants.body); tt.wants.body != "" && !eq {
				t.Errorf("%q. get\n***%v***\n,\nwant\n***%v***", tt.name, string(body), tt.wants.body)
			}

		})
	}
}
