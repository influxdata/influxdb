package http

import (
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"

	kithttp "github.com/influxdata/influxdb/v2/kit/transport/http"
	"go.uber.org/zap/zaptest"
)

func TestAPIHandler_NotFound(t *testing.T) {
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
		name  string
		args  args
		wants wants
	}{
		{
			name: "path not found",
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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			r := httptest.NewRequest(tt.args.method, tt.args.path, nil)
			w := httptest.NewRecorder()

			b := &APIBackend{
				HTTPErrorHandler: kithttp.ErrorHandler(0),
				Logger:           zaptest.NewLogger(t),
			}

			h := NewAPIHandler(b)
			h.ServeHTTP(w, r)

			res := w.Result()
			content := res.Header.Get("Content-Type")
			body, _ := ioutil.ReadAll(res.Body)

			if res.StatusCode != tt.wants.statusCode {
				t.Errorf("%q. get %v, want %v", tt.name, res.StatusCode, tt.wants.statusCode)
			}
			if tt.wants.contentType != "" && content != tt.wants.contentType {
				t.Errorf("%q. get %v, want %v", tt.name, content, tt.wants.contentType)
			}
			if eq, diff, err := jsonEqual(string(body), tt.wants.body); err != nil {
				t.Errorf("%q, error unmarshaling json %v", tt.name, err)
			} else if tt.wants.body != "" && !eq {
				t.Errorf("%q. ***%s***", tt.name, diff)
			}
		})
	}
}
